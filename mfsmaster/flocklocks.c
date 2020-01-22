/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
 * 
 * This file is part of MooseFS.
 * 
 * MooseFS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 (only).
 * 
 * MooseFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <inttypes.h>

#include "MFSCommunication.h"

#include "matoclserv.h"
#include "openfiles.h"
#include "metadata.h"
#include "main.h"
#include "changelog.h"
#include "datapack.h"
#include "bio.h"
#include "slogger.h"
#include "massert.h"

#define MODE_CORRECT 0
#define MODE_BSD 1
#define MODE_LINUX 2

#define STATE_WAITING 0
#define STATE_ACTIVE 1

#define LTYPE_READER 0
#define LTYPE_WRITER 1

typedef struct _instance {
	uint32_t msgid;
	uint32_t reqid; // for interruption only (valid in waiting_* queues)
	struct _instance *next;
} instance;

typedef struct _lock {
	uint64_t owner;
	uint32_t sessionid;
	uint8_t state;
	uint8_t ltype;
	instance *lock_instances;
	struct _inodelocks *parent;
	struct _lock *next,**prev;
} lock;

typedef struct _inodelocks {
	uint32_t inode;
	lock *active;
	lock *waiting_head,**waiting_tail;
	struct _inodelocks *next;
} inodelocks;

#define FLOCK_INODE_HASHSIZE 1024

#define FLOCK_INODE_HASH(inode) (((inode)*0x738A2379)%(FLOCK_INODE_HASHSIZE))

static inodelocks **inodehash;

static uint8_t FlocksMode;

#if 0
static inline void flock_dump(void) {
	uint32_t h;
	inodelocks *il;
	lock *l,**lptr;
	instance *i;
	syslog(LOG_NOTICE,"flock dump:");
	for (h=0 ; h<FLOCK_INODE_HASHSIZE ; h++) {
		for (il = inodehash[h] ; il ; il=il->next) {
			syslog(LOG_NOTICE,"  inode: %"PRIu32" (active:%s,waiting:%s)",il->inode,il->active?"yes":"no",il->waiting_head?"yes":"no");
			lptr = &(il->active);
			while ((l=*lptr)) {
				syslog(LOG_NOTICE,"    active lock: session:%"PRIu32",owner:%"PRIu64",type:%s",l->sessionid,l->owner,l->ltype==LTYPE_READER?"R":"W");
				if (l->state!=STATE_ACTIVE) {
					syslog(LOG_WARNING,"      wrong state !!!");
				}
				if (l->prev != lptr) {
					syslog(LOG_WARNING,"      wrong prev pointer !!!");
				}
				if (l->lock_instances) {
					syslog(LOG_WARNING,"      active lock with waiting processes !!!");
				}
				lptr = &(l->next);
			}
			lptr = &(il->waiting_head);
			while ((l=*lptr)) {
				syslog(LOG_NOTICE,"    waiting lock: session:%"PRIu32",owner:%"PRIu64",type:%s",l->sessionid,l->owner,l->ltype==LTYPE_READER?"R":"W");
				if (l->state!=STATE_WAITING) {
					syslog(LOG_WARNING,"      wrong state !!!");
				}
				if (l->prev != lptr) {
					syslog(LOG_WARNING,"      wrong prev pointer !!!");
				}
				for (i = l->lock_instances ; i ; i=i->next) {
					syslog(LOG_NOTICE,"      waiting process reqid: %"PRIu32,i->reqid);
				}
				lptr = &(l->next);
			}
			if (il->waiting_tail != lptr) {
				syslog(LOG_WARNING,"    wrong tail pointer !!!");
			}
		}
	}
}
#endif

static inline inodelocks* flock_inode_find(uint32_t inode) {
	inodelocks *il;

	for (il = inodehash[FLOCK_INODE_HASH(inode)] ; il ; il=il->next) {
		if (il->inode==inode) {
			return il;
		}
	}
	return NULL;
}

static inline inodelocks* flock_inode_new(uint32_t inode) {
	inodelocks *il;
	uint32_t hash;

	il = malloc(sizeof(inodelocks));
	passert(il);
	il->inode = inode;
	il->active = NULL;
	il->waiting_head = NULL;
	il->waiting_tail = &(il->waiting_head);
	hash = FLOCK_INODE_HASH(inode);
	il->next = inodehash[hash];
	inodehash[hash] = il;
	return il;
}

static inline void flock_inode_remove(uint32_t inode) {
	inodelocks *il,**ilp;
	uint32_t hash;

	hash = FLOCK_INODE_HASH(inode);
	ilp = inodehash + hash;
	while ((il=*ilp)) {
		if (il->inode==inode) {
			massert(il->active==NULL && il->waiting_head==NULL,"inode flock record not empty !!!");
			*ilp = il->next;
			free(il);
		} else {
			ilp = &(il->next);
		}
	}
}

static inline void flock_lock_inode_detach(lock *l) {
	if (l->next) {
		l->next->prev = l->prev;
	} else {
		if (l->state==STATE_WAITING) {
			l->parent->waiting_tail = l->prev;
		}
	}
	*(l->prev) = l->next;
}

static inline void flock_do_lock_inode_attach(lock *l) {
	if (l->state==STATE_WAITING) {
		l->next = NULL;
		l->prev = l->parent->waiting_tail;
		*(l->parent->waiting_tail) = l;
		l->parent->waiting_tail = &(l->next);
	} else {
		l->next = l->parent->active;
		if (l->next) {
			l->next->prev = &(l->next);
		}
		l->prev = &(l->parent->active);
		l->parent->active = l;
	}
}

static inline void flock_lock_inode_attach(lock *l) {
	if (l->state==STATE_ACTIVE) {
		changelog("%"PRIu32"|FLOCK(%"PRIu32",%"PRIu32",%"PRIu64",%c)",main_time(),l->parent->inode,l->sessionid,l->owner,l->ltype==LTYPE_READER?'R':'W');
	}
	flock_do_lock_inode_attach(l);
}

static inline void flock_lock_wake_up_one(lock *l,uint32_t reqid,uint8_t status) {
	instance *i,**iptr;
	iptr = &(l->lock_instances);
	while ((i=*iptr)) {
		if (i->reqid==reqid) {
			matoclserv_fuse_flock_wake_up(l->sessionid,i->msgid,status);
			*iptr = i->next;
			free(i);
		} else {
			iptr = &(i->next);
		}
	}
}

static inline void flock_lock_wake_up_all(lock *l,uint8_t status) {
	instance *i,*ni;
	i = l->lock_instances;
	while (i) {
		ni = i->next;
		matoclserv_fuse_flock_wake_up(l->sessionid,i->msgid,status);
		free(i);
		i = ni;
	}
	l->lock_instances=NULL;
}

static inline void flock_lock_append_req(lock *l,uint32_t msgid,uint32_t reqid) {
	instance *i;

	for (i = l->lock_instances ; i ; i=i->next) {
		if (i->reqid==reqid) {
			i->msgid = msgid;
			return;
		}
	}
	i = malloc(sizeof(instance));
	passert(i);
	i->msgid = msgid;
	i->reqid = reqid;
	i->next = l->lock_instances;
	l->lock_instances = i;
}

static inline void flock_do_lock_remove(lock *l) {
	instance *i,*ni;
	i=l->lock_instances;
	while (i) {
		ni = i->next;
		free(i);
		i = ni;
	}
	if (l->next) {
		l->next->prev = l->prev;
	} else {
		if (l->state==STATE_WAITING) {
			l->parent->waiting_tail = l->prev;
		}
	}
	*(l->prev) = l->next;
	free(l);
}

static inline void flock_lock_remove(lock *l) {
	if (l->state==STATE_ACTIVE) {
		changelog("%"PRIu32"|FLOCK(%"PRIu32",%"PRIu32",%"PRIu64",U)",main_time(),l->parent->inode,l->sessionid,l->owner);
	}
	flock_do_lock_remove(l);
}

static inline uint8_t flock_check(inodelocks *il,uint8_t ltype) {
	if (ltype==LTYPE_READER) {
		if (il->active!=NULL && il->active->ltype==LTYPE_WRITER) {
			return 1;
		}
		if (FlocksMode==MODE_CORRECT) {
// additional condition for classic readers/writers algorithm (not seen in any
// tested OS) - reader should wait when there are other waiting lock's (even if
// currently acquired lock(s) is a reader lock) - it avoids writer starvation
			if (il->waiting_head!=NULL) {
				return 1;
			}
		}
	} else {
		if (il->active!=NULL) {
			return 1;
		}
	}
	return 0;
}

static inline uint8_t flock_lock_new(inodelocks *il,uint8_t ltype,uint32_t sessionid,uint32_t msgid,uint32_t reqid,uint64_t owner) {
	lock *l;
	l = malloc(sizeof(lock));
	l->owner = owner;
	l->sessionid = sessionid;
	l->state = STATE_ACTIVE;
	l->ltype = ltype;
	l->lock_instances = NULL;
	l->parent = il;
	l->next = NULL;
	l->prev = NULL;
	if (flock_check(il,ltype)) {
		l->state = STATE_WAITING;
		flock_lock_append_req(l,msgid,reqid);
		flock_lock_inode_attach(l);
		return MFS_ERROR_WAITING;
	}
	flock_lock_inode_attach(l);
	return MFS_STATUS_OK;
}

static inline void flock_lock_check_waiting(inodelocks *il) {
	lock *l,*nl;
	l = il->waiting_head;
	if (l==NULL) {
		return;
	}
	if (il->active==NULL && l->ltype==LTYPE_WRITER) {
		flock_lock_inode_detach(l);
		l->state = STATE_ACTIVE;
		flock_lock_inode_attach(l);
		flock_lock_wake_up_all(l,MFS_STATUS_OK);
	}
	if (il->active==NULL || il->active->ltype==LTYPE_READER) {
		if (FlocksMode==MODE_LINUX) {
			while (l) {
				nl = l->next;
				if (l->ltype==LTYPE_READER) {
					flock_lock_inode_detach(l);
					l->state = STATE_ACTIVE;
					flock_lock_inode_attach(l);
					flock_lock_wake_up_all(l,MFS_STATUS_OK);
				}
				l = nl;
			}
		} else { // FreeBSD, OSX, Classic readers/writers algorithm
			while (l && l->ltype==LTYPE_READER) {
				nl = l->next;
				flock_lock_inode_detach(l);
				l->state = STATE_ACTIVE;
				flock_lock_inode_attach(l);
				flock_lock_wake_up_all(l,MFS_STATUS_OK);
				l = nl;
			}
		}
	}
}

static inline void flock_lock_unlock(inodelocks *il,lock *l) {
	massert(il==l->parent,"flock data structures mismatch");

	flock_lock_remove(l);

	if (il->active==NULL && il->waiting_head!=NULL) {
		flock_lock_check_waiting(il);
	}
}

uint8_t flock_locks_cmd(uint32_t sessionid,uint32_t msgid,uint32_t reqid,uint32_t inode,uint64_t owner,uint8_t op) {
	inodelocks *il;
	lock *l,*nl;
	uint8_t ltype;

//	flock_dump();
//	syslog(LOG_NOTICE,"flock op: sessionid:%"PRIu32",msgid:%"PRIu32",reqid:%"PRIu32",inode:%"PRIu32",owner:%"PRIu64",op:%u",sessionid,msgid,reqid,inode,owner,op);

	if (op!=FLOCK_INTERRUPT && op!=FLOCK_RELEASE) {
		if (of_checknode(sessionid,inode)==0) {
			return MFS_ERROR_NOTOPENED;
		}
	}
	il = flock_inode_find(inode);
	if (il==NULL) {
		if (op==FLOCK_UNLOCK || op==FLOCK_INTERRUPT || op==FLOCK_RELEASE) {
			return MFS_STATUS_OK;
		}
		il = flock_inode_new(inode);
	}
	if (op==FLOCK_INTERRUPT) {
		l = il->waiting_head;
		while (l) {
			nl = l->next;
			if (l->sessionid==sessionid && l->owner==owner) {
				flock_lock_wake_up_one(l,reqid,MFS_ERROR_EINTR);
				if (l->lock_instances==NULL) { // remove
					flock_lock_remove(l);
				}
			}
			l = nl;
		}
		return MFS_STATUS_OK;
	}
	for (l=il->active ; l ; l=l->next) {
		if (l->sessionid==sessionid && l->owner==owner) {
			if (op==FLOCK_UNLOCK || op==FLOCK_RELEASE) {
				flock_lock_unlock(il,l);
				if (il->waiting_head==NULL && il->active==NULL) {
					flock_inode_remove(il->inode);
				}
				return MFS_STATUS_OK;
			} else if (op==FLOCK_TRY_SHARED) {
				if (l->ltype==LTYPE_READER) {
					return MFS_STATUS_OK;
				} else { // l->ltype==LTYPE_WRITER
					l->ltype=LTYPE_READER;
					flock_lock_check_waiting(il);
					return MFS_STATUS_OK;
				}
			} else if (op==FLOCK_LOCK_SHARED) {
				if (l->ltype==LTYPE_READER) {
					return MFS_STATUS_OK;
				} else { // l->ltype==LTYPE_WRITER
					flock_lock_unlock(il,l);
					return flock_lock_new(il,LTYPE_READER,sessionid,msgid,reqid,owner);
				}
			} else if (op==FLOCK_TRY_EXCLUSIVE) {
				if (l->ltype==LTYPE_WRITER) {
					return MFS_STATUS_OK;
				} else { // l->ltype==LTYPE_READER
					if (il->active->next==NULL) { // this lock is the only one
						l->ltype=LTYPE_WRITER;
						return MFS_STATUS_OK;
					}
					return MFS_ERROR_EAGAIN;
				}
			} else if (op==FLOCK_LOCK_EXCLUSIVE) {
				if (l->ltype==LTYPE_WRITER) {
					return MFS_STATUS_OK;
				} else { // l->ltype==LTYPE_READER
					flock_lock_unlock(il,l);
					return flock_lock_new(il,LTYPE_WRITER,sessionid,msgid,reqid,owner);
				}
			}
			return MFS_ERROR_EINVAL;
		}	
	}
	for (l=il->waiting_head ; l ; l=l->next) {
		if (l->sessionid==sessionid && l->owner==owner) {
			if (op==FLOCK_RELEASE) {
				flock_lock_wake_up_all(l,MFS_ERROR_ECANCELED);
				flock_lock_remove(l);
				return MFS_STATUS_OK;
			} else if (op==FLOCK_UNLOCK) {
				if (FlocksMode==MODE_CORRECT) {
					// logically this call should do this:
					flock_lock_wake_up_all(l,MFS_ERROR_ECANCELED);
					flock_lock_remove(l);
				}
				// but in all tested kernels it was just ignored
				return MFS_STATUS_OK;
			} else if (op==FLOCK_TRY_SHARED || op==FLOCK_TRY_EXCLUSIVE) {
				return MFS_ERROR_EAGAIN;
			} else if (op==FLOCK_LOCK_SHARED) {
				if (l->ltype==LTYPE_READER) {
					flock_lock_append_req(l,msgid,reqid);
					return MFS_ERROR_WAITING;
				} else {
//					return MFS_ERROR_EINVAL;
					flock_lock_wake_up_all(l,MFS_ERROR_ECANCELED);
					l->ltype=LTYPE_READER;
					flock_lock_append_req(l,msgid,reqid);
					return MFS_ERROR_WAITING;
				}
			} else if (op==FLOCK_LOCK_EXCLUSIVE) {
				if (l->ltype==LTYPE_WRITER) {
					flock_lock_append_req(l,msgid,reqid);
					return MFS_ERROR_WAITING;
				} else {
//					return MFS_ERROR_EINVAL;
					flock_lock_wake_up_all(l,MFS_ERROR_ECANCELED);
					l->ltype=LTYPE_WRITER;
					flock_lock_append_req(l,msgid,reqid);
					return MFS_ERROR_WAITING;
				}
			}
			return MFS_ERROR_EINVAL;
		}
	}
	if (op==FLOCK_UNLOCK || op==FLOCK_RELEASE) {
		return MFS_STATUS_OK;
	}
	ltype = (op==FLOCK_TRY_SHARED || op==FLOCK_LOCK_SHARED)?LTYPE_READER:LTYPE_WRITER;
	if (op==FLOCK_TRY_SHARED || op==FLOCK_TRY_EXCLUSIVE) {
		if (flock_check(il,ltype)) {
			return MFS_ERROR_EAGAIN;
		}
	}
	return flock_lock_new(il,ltype,sessionid,msgid,reqid,owner);
}

void flock_file_closed(uint32_t sessionid,uint32_t inode) {
	inodelocks *il;
	lock *l,*nl;

	il = flock_inode_find(inode);
	if (il==NULL) {
		return;
	}

	l = il->waiting_head;
	while (l) {
		nl = l->next;
		if (l->sessionid==sessionid) {
			flock_lock_remove(l);
		}
		l = nl;
	}

	l = il->active;
	while (l) {
		nl = l->next;
		if (l->sessionid==sessionid) {
			flock_lock_unlock(il,l);
		}
		l = nl;
	}
	if (il->waiting_head==NULL && il->active==NULL) {
		flock_inode_remove(il->inode);
	}
}

uint32_t flock_list(uint32_t inode,uint8_t *buff) {
	inodelocks *il;
	lock *l;
	uint32_t h;
	uint32_t ret=0;

	if (inode==0) {
		for (h=0 ; h<FLOCK_INODE_HASHSIZE ; h++) {
			for (il = inodehash[h] ; il ; il=il->next) {
				for (l=il->active ; l ; l=l->next) {
					if (buff==NULL) {
						ret+=37;
					} else {
						put32bit(&buff,il->inode);
						put32bit(&buff,l->sessionid);
						put64bit(&buff,l->owner);
						memset(buff,0,20); // pid,start,end
						buff+=20;
						switch (l->ltype) {
							case LTYPE_READER:
								put8bit(&buff,1);
								break;
							case LTYPE_WRITER:
								put8bit(&buff,2);
								break;
							default:
								put8bit(&buff,0);
						}
					}
				}
			}
		}
	} else {
		il = flock_inode_find(inode);
		if (il!=NULL) {
			for (l=il->active ; l ; l=l->next) {
				if (buff==NULL) {
					ret+=33;
				} else {
					put32bit(&buff,l->sessionid);
					put64bit(&buff,l->owner);
					memset(buff,0,20); // pid,start,end
					buff+=20;
					switch (l->ltype) {
						case LTYPE_READER:
							put8bit(&buff,1);
							break;
						case LTYPE_WRITER:
							put8bit(&buff,2);
							break;
						default:
							put8bit(&buff,0);
					}
				}
			}
		}
	}
	return ret;
}

uint8_t flock_mr_change(uint32_t inode,uint32_t sessionid,uint64_t owner,char cmd) {
	inodelocks *il;
	lock *l,*nl;
	uint8_t ltype;

	if (cmd=='U' || cmd=='u') {
		il = flock_inode_find(inode);
		if (il==NULL) {
			return MFS_ERROR_MISMATCH;
		}
		l=il->active;
		while (l) {
			nl = l->next;
			if (l->sessionid==sessionid && l->owner==owner) {
				flock_do_lock_remove(l);
				meta_version_inc();
			}
			l = nl;
		}
		if (il->waiting_head==NULL && il->active==NULL) {
			flock_inode_remove(il->inode);
		}
		return MFS_STATUS_OK;
	} else if (cmd=='R' || cmd=='r' || cmd=='S' || cmd=='s') {
		ltype = LTYPE_READER;
	} else if (cmd=='W' || cmd=='w' || cmd=='E' || cmd=='e') {
		ltype = LTYPE_WRITER;
	} else {
		return MFS_ERROR_EINVAL;
	}
	il = flock_inode_find(inode);
	if (il==NULL) {
		il = flock_inode_new(inode);
	}
	if (il->active!=NULL && (il->active->ltype==LTYPE_WRITER || ltype==LTYPE_WRITER)) {
		return MFS_ERROR_MISMATCH;
	}
	l = malloc(sizeof(lock));
	l->owner = owner;
	l->sessionid = sessionid;
	l->state = STATE_ACTIVE;
	l->ltype = ltype;
	l->lock_instances = NULL;
	l->parent = il;
	l->next = NULL;
	l->prev = NULL;
	flock_do_lock_inode_attach(l);
	meta_version_inc();
	return MFS_STATUS_OK;
}

#define FLOCK_REC_SIZE 17

uint8_t flock_store(bio *fd) {
	uint8_t storebuff[FLOCK_REC_SIZE];
	uint8_t *ptr;
	uint32_t h;
	inodelocks *il;
	lock *l;

	if (fd==NULL) {
		return 0x10;
	}
	for (h=0 ; h<FLOCK_INODE_HASHSIZE ; h++) {
		for (il = inodehash[h] ; il ; il=il->next) {
			for (l=il->active ; l ; l=l->next) {
				ptr = storebuff;
				put32bit(&ptr,il->inode);
				put64bit(&ptr,l->owner);
				put32bit(&ptr,l->sessionid);
				put8bit(&ptr,l->ltype);
				if (bio_write(fd,storebuff,FLOCK_REC_SIZE)!=FLOCK_REC_SIZE) {
					return 0xFF;
				}
			}
		}
	}
	memset(storebuff,0,FLOCK_REC_SIZE);
	if (bio_write(fd,storebuff,FLOCK_REC_SIZE)!=FLOCK_REC_SIZE) {
		return 0xFF;
	}
	return 0;
}

int flock_load(bio *fd,uint8_t mver,uint8_t ignoreflag) {
	uint8_t loadbuff[FLOCK_REC_SIZE];
	const uint8_t *ptr;
	int32_t r;
	uint32_t inode,sessionid;
	uint64_t owner;
	uint8_t ltype;
	inodelocks *il;
	lock *l;

	if (mver!=0x10) {
		return -1;
	}

	for (;;) {
		r = bio_read(fd,loadbuff,FLOCK_REC_SIZE);
		if (r!=FLOCK_REC_SIZE) {
			return -1;
		}
		ptr = loadbuff;
		inode = get32bit(&ptr);
		owner = get64bit(&ptr);
		sessionid = get32bit(&ptr);
		ltype = get8bit(&ptr);
		if (inode==0 && owner==0 && sessionid==0) {
			return 0;
		}
		if (of_checknode(sessionid,inode)==0) {
			if (ignoreflag) {
				mfs_syslog(LOG_ERR,"loading flock_locks: lock on closed file !!! (ignoring)");
				continue;
			} else {
				mfs_syslog(LOG_ERR,"loading flock_locks: lock on closed file !!!");
				return -1;
			}
		}
		// add lock
		il = flock_inode_find(inode);
		if (il==NULL) {
			il = flock_inode_new(inode);
		}
		if (il->active!=NULL && (il->active->ltype==LTYPE_WRITER || ltype==LTYPE_WRITER)) {
			if (ignoreflag) {
				mfs_syslog(LOG_ERR,"loading flock_locks: wrong lock !!! (ignoring)");
				continue;
			} else {
				mfs_syslog(LOG_ERR,"loading flock_locks: wrong lock !!!");
				return -1;
			}
		}
		l = malloc(sizeof(lock));
		l->owner = owner;
		l->sessionid = sessionid;
		l->state = STATE_ACTIVE;
		l->ltype = ltype;
		l->lock_instances = NULL;
		l->parent = il;
		l->next = NULL;
		l->prev = NULL;
		flock_do_lock_inode_attach(l);
	}
	return 0; // unreachable
}

void flock_cleanup(void) {
	uint32_t h,j;
	inodelocks *il,*nil;
	lock *l,*nl;
	instance *i,*ni;
	for (h=0 ; h<FLOCK_INODE_HASHSIZE ; h++) {
		il = inodehash[h];
		while (il) {
			nil = il->next;
			for (j=0 ; j<2 ; j++) {
				l = j?il->active:il->waiting_head;
				while (l) {
					nl = l->next;
					i = l->lock_instances;
					while (i) {
						ni = i->next;
						free(i);
						i = ni;
					}
					free(l);
					l = nl;
				}
			}
			free(il);
			il = nil;
		}
		inodehash[h] = NULL;
	}
}

int flock_init(void) {
	uint32_t i;
	inodehash = malloc(sizeof(inodelocks*)*FLOCK_INODE_HASHSIZE);
	for (i=0 ; i<FLOCK_INODE_HASHSIZE ; i++) {
		inodehash[i] = NULL;
	}
	FlocksMode = 0;
	return 0;
}
