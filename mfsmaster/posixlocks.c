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
#include "massert.h"

#ifndef MFSTEST

#include "matoclserv.h"
#include "openfiles.h"
#include "metadata.h"
#include "main.h"
#include "changelog.h"
#include "datapack.h"
#include "bio.h"
#include "slogger.h"

#endif

// ranges are closed-open: <start,end)

typedef struct _range {
	uint64_t start;
	uint64_t end;
	uint8_t type;
	struct _range *next;
} range;

#ifndef MFSTEST

typedef struct _alock {
	uint64_t owner;
	uint32_t sessionid;
	uint32_t pid;
	range *ranges;
	struct _alock *next;
} alock;

typedef struct _wlock {
	uint64_t owner;
	uint32_t sessionid;
	uint32_t pid;
	uint32_t msgid;
	uint32_t reqid;
	uint64_t start;
	uint64_t end;
	uint8_t type;
	struct _wlock *next,**prev;
} wlock;

typedef struct _inodelocks {
	uint32_t inode;
	alock *active;
	wlock *waiting_head,**waiting_tail;
	struct _inodelocks *next;
} inodelocks;

#define POSIX_LOCK_INODE_HASHSIZE 1024

#define POSIX_LOCK_INODE_HASH(inode) (((inode)*0x738A2379)%(POSIX_LOCK_INODE_HASHSIZE))

static inodelocks **inodehash;

#if 0
static inline void posix_lock_dump(void) {
	uint32_t h;
	inodelocks *il;
	alock *al;
	wlock *wl,**wlptr;
	range *r;
	syslog(LOG_NOTICE,"posix lock dump:");
	for (h=0 ; h<POSIX_LOCK_INODE_HASHSIZE ; h++) {
		for (il = inodehash[h] ; il ; il=il->next) {
			syslog(LOG_NOTICE,"  inode: %"PRIu32" (active:%s,waiting:%s)",il->inode,il->active?"yes":"no",il->waiting_head?"yes":"no");
			for (al = il->active ; al ; al=al->next) {
				syslog(LOG_NOTICE,"    active lock: session:%"PRIu32",owner:%"PRIu64",pid:%"PRIu32,al->sessionid,al->owner,al->pid);
				if (al->ranges==NULL) {
					syslog(LOG_WARNING,"      no lock ranges !!!");
				}
				for (r = al->ranges ; r ; r=r->next) {
					syslog(LOG_NOTICE,"      range: start:%"PRIu64",end:%"PRIu64",type:%c",r->start,r->end,(r->type==POSIX_LOCK_RDLCK)?'R':(r->type==POSIX_LOCK_WRLCK)?'W':'?');
				}
			}
			wlptr = &(il->waiting_head);
			for (wl = il->waiting_head ; wl ; wl=wl->next) {
				syslog(LOG_NOTICE,"    waiting lock: session:%"PRIu32",owner:%"PRIu64",pid:%"PRIu32",start:%"PRIu64",end:%"PRIu64",type:%c",wl->sessionid,wl->owner,wl->pid,wl->start,wl->end,wl->type);
				wlptr = &(wl->next);
			}
			if (il->waiting_tail != wlptr) {
				syslog(LOG_WARNING,"    wrong tail pointer !!!");
			}
		}
	}
}
#endif

#endif

static inline int posix_lock_test_wlock(range *r,uint8_t *type,uint64_t *start,uint64_t *end) {
	while (r) {
		if (*type==POSIX_LOCK_WRLCK || r->type==POSIX_LOCK_WRLCK) {
			if (*end > r->start && *start < r->end) { // ranges intersects
				*type = r->type;
				*start = r->start;
				*end = r->end;
				return 1;
			}
		}
		r = r->next;
	}
	return 0;
}

static inline void posix_lock_apply_range(range **rptr,uint8_t type,uint64_t start,uint64_t end) {
	range *nr,*r;
	uint8_t added;

	added = 0;
	while (added==0 && (r=*rptr)) {
		if (r->end < start) {
			// wl:      |-----|
			// r:  |--|
#ifdef MFSTEST
			printf("case 1\n");
#endif
			rptr = &(r->next);
		} else if (r->start > end) {
			// wl: |-----|
			// r:          |--|
			if (type!=POSIX_LOCK_UNLCK) {
#ifdef MFSTEST
				printf("case 2a\n");
				printf("malloc\n");
#endif
				nr = malloc(sizeof(range));
				passert(nr);
				nr->start = start;
				nr->end = end;
				nr->type = type;
				nr->next = *rptr;
				*rptr = nr;
#ifdef MFSTEST
			} else {
				printf("case 2b\n");
#endif
			}
			added = 1;
		} else if (start <= r->start && end >= r->end) {
			// wl: |-----|   |-----|
			// r:    |--|    |-----|
#ifdef MFSTEST
			printf("case 3\n");
			printf("free\n");
#endif
			*rptr = r->next;
			free(r);
		} else if (r->start < start && r->end <= end) {
			// wl:   |-----|     |-----|
			// r:  |---|       |-------|
			if (r->type == type) {
#ifdef MFSTEST
				printf("case 4a\n");
				printf("free\n");
#endif
				start = r->start;
				*rptr = r->next;
				free(r);
			} else {
#ifdef MFSTEST
				printf("case 4b\n");
#endif
				r->end = start;
				rptr = &(r->next);
			}
		} else if (r->start >= start && r->end > end) {
			// wl:  |-----|        |-----|
			// r:       |---|      |-------|
			if (r->type == type) {
#ifdef MFSTEST
				printf("case 5a\n");
#endif
				r->start = start;
				added = 1;
			} else {
				r->start = end;
				if (type!=POSIX_LOCK_UNLCK) {
#ifdef MFSTEST
					printf("case 5b\n");
					printf("malloc\n");
#endif
					nr = malloc(sizeof(range));
					passert(nr);
					nr->start = start;
					nr->end = end;
					nr->type = type;
					nr->next = r;
					*rptr = nr;
#ifdef MFSTEST
				} else {
					printf("case 5c\n");
#endif
				}
				added = 1;
			}
		} else {
			// wl:   |-----|
			// r:  |---------|
			if (r->type != type) {
				nr = malloc(sizeof(range));
				passert(nr);
				nr->start = end;
				nr->end = r->end;
				nr->type = r->type;
				nr->next = r->next;
				r->next = nr;
				if (type!=POSIX_LOCK_UNLCK) {
#ifdef MFSTEST
					printf("case 6a\n");
					printf("malloc\n");
					printf("malloc\n");
#endif
					nr = malloc(sizeof(range));
					passert(nr);
					nr->start = start;
					nr->end = end;
					nr->type = type;
					nr->next = r->next;
					r->next = nr;
#ifdef MFSTEST
				} else {
					printf("case 6b\n");
					printf("malloc\n");
#endif
				}
				r->end = start;
#ifdef MFSTEST
			} else {
				printf("case 6c\n");
#endif
			}
			added = 1;
		}
	}
	if (added==0 && type!=POSIX_LOCK_UNLCK) {
#ifdef MFSTEST
		printf("case 7\n");
		printf("malloc\n");
#endif
		nr = malloc(sizeof(range));
		passert(nr);
		nr->start = start;
		nr->end = end;
		nr->type = type;
		nr->next = NULL;
		*rptr = nr;
	}
}

#ifndef MFSTEST

static inline inodelocks* posix_lock_inode_find(uint32_t inode) {
	inodelocks *il;

	for (il = inodehash[POSIX_LOCK_INODE_HASH(inode)] ; il ; il=il->next) {
		if (il->inode==inode) {
			return il;
		}
	}
	return NULL;
}

static inline inodelocks* posix_lock_inode_new(uint32_t inode) {
	inodelocks *il;
	uint32_t hash;

	il = malloc(sizeof(inodelocks));
	passert(il);
	il->inode = inode;
	il->active = NULL;
	il->waiting_head = NULL;
	il->waiting_tail = &(il->waiting_head);
	hash = POSIX_LOCK_INODE_HASH(inode);
	il->next = inodehash[hash];
	inodehash[hash] = il;
	return il;
}

static inline void posix_lock_inode_remove(uint32_t inode) {
	inodelocks *il,**ilp;
	uint32_t hash;

	hash = POSIX_LOCK_INODE_HASH(inode);
	ilp = inodehash + hash;
	while ((il=*ilp)) {
		if (il->inode==inode) {
			massert(il->active==NULL && il->waiting_head==NULL,"inode posix lock record not empty !!!");
			*ilp = il->next;
			free(il);
		} else {
			ilp = &(il->next);
		}
	}
}

static inline void posix_lock_remove_lock(inodelocks *il,wlock *wl) {
	if (wl->next==NULL) {
		il->waiting_tail = wl->prev;
	} else {
		wl->next->prev = wl->prev;
	}
	*(wl->prev) = wl->next;
	free(wl);
}

static inline int posix_lock_get_offensive_lock(inodelocks *il,uint32_t sessionid,uint64_t owner,uint8_t *type,uint64_t *start,uint64_t *end,uint32_t *pid) {
	alock *al;
	for (al=il->active ; al ; al=al->next) {
		if (al->owner!=owner || al->sessionid!=sessionid) {
			if (posix_lock_test_wlock(al->ranges,type,start,end)) {
				if (sessionid==al->sessionid) {
					*pid = al->pid;
				} else {
					*pid = 0;
				}
				return 1;
			}
		}
	}
	return 0;
}

static inline int posix_lock_find_offensive_lock(inodelocks *il,uint32_t sessionid,uint64_t owner,uint8_t type,uint64_t start,uint64_t end) {
	alock *al;
	for (al=il->active ; al ; al=al->next) {
		if (al->owner!=owner || al->sessionid!=sessionid) {
			if (posix_lock_test_wlock(al->ranges,&type,&start,&end)) {
				return 1;
			}
		}
	}
	return 0;
}

static inline void posix_lock_do_apply_lock(inodelocks *il,uint32_t sessionid,uint64_t owner,uint8_t type,uint64_t start,uint64_t end,uint32_t pid) {
	alock *al,**alptr;
	alptr = &(il->active);
	while ((al=*alptr)) {
		if (al->owner==owner && al->sessionid==sessionid) {
			posix_lock_apply_range(&(al->ranges),type,start,end);
			if (al->ranges==NULL) {
				*alptr = al->next;
				free(al);
			}
			return;
		}
		alptr = &(al->next);
	}
	if (type==POSIX_LOCK_UNLCK) {
		return;
	}
	al = malloc(sizeof(alock));
	passert(al);
	al->owner = owner;
	al->sessionid = sessionid;
	al->pid = pid;
	al->ranges = NULL;
	al->next = NULL;
	*alptr = al;
	posix_lock_apply_range(&(al->ranges),type,start,end);
}

static inline void posix_lock_apply_lock(inodelocks *il,uint32_t sessionid,uint64_t owner,uint8_t type,uint64_t start,uint64_t end,uint32_t pid) {
	changelog("%"PRIu32"|POSIXLOCK(%"PRIu32",%"PRIu32",%"PRIu64",%c,%"PRIu64",%"PRIu64",%"PRIu32")",main_time(),il->inode,sessionid,owner,(type==POSIX_LOCK_RDLCK)?'R':(type==POSIX_LOCK_WRLCK)?'W':'U',start,end,pid);
	posix_lock_do_apply_lock(il,sessionid,owner,type,start,end,pid);
}

static inline void posix_lock_append_lock(inodelocks *il,uint32_t sessionid,uint32_t msgid,uint32_t reqid,uint64_t owner,uint8_t type,uint64_t start,uint64_t end,uint32_t pid) {
	wlock *wl;
	wl = malloc(sizeof(wlock));
	passert(wl);
	wl->owner = owner;
	wl->sessionid = sessionid;
	wl->pid = pid;
	wl->msgid = msgid;
	wl->reqid = reqid;
	wl->start = start;
	wl->end = end;
	wl->type = type;
	wl->next = NULL;
	wl->prev = il->waiting_tail;
	*(il->waiting_tail) = wl;
	il->waiting_tail = &(wl->next);
}

static inline void posix_lock_interrupt(inodelocks *il,uint32_t sessionid,uint32_t reqid) {
	wlock *wl;
	for (wl=il->waiting_head ; wl ; wl=wl->next) {
		if (wl->sessionid==sessionid && wl->reqid==reqid) {
			matoclserv_fuse_posix_lock_wake_up(sessionid,wl->msgid,MFS_ERROR_EINTR);
			posix_lock_remove_lock(il,wl);
			return;
		}
	}
}

static inline void posix_lock_check_waiting(inodelocks *il) {
	wlock *wl,*nwl;
	if (il->active==NULL && il->waiting_head==NULL) {
		posix_lock_inode_remove(il->inode);
		return;
	}
	wl = il->waiting_head;
	while (wl) {
		nwl = wl->next;
		if (posix_lock_find_offensive_lock(il,wl->sessionid,wl->owner,wl->type,wl->start,wl->end)==0) {
			posix_lock_apply_lock(il,wl->sessionid,wl->owner,wl->type,wl->start,wl->end,wl->pid);
			matoclserv_fuse_posix_lock_wake_up(wl->sessionid,wl->msgid,MFS_STATUS_OK);
			posix_lock_remove_lock(il,wl);
		}
		wl = nwl;
	}
}

uint8_t posix_lock_cmd(uint32_t sessionid,uint32_t msgid,uint32_t reqid,uint32_t inode,uint64_t owner,uint8_t op,uint8_t *type,uint64_t *start,uint64_t *end,uint32_t *pid) {
	inodelocks *il;
	uint8_t i_type;
	uint64_t i_start;
	uint64_t i_end;
	uint32_t i_pid;

	i_type = *type;
	i_start = *start;
	i_end = *end;
	i_pid = *pid;

//	posix_lock_dump();
//	syslog(LOG_NOTICE,"new lock cmd: sessionid:%"PRIu32",msgid:%"PRIu32",reqid:%"PRIu32",inode:%"PRIu32",owner:%"PRIX64",op:%c,type:%c,start:%"PRIu64",end:%"PRIu64",pid:%"PRIu32,sessionid,msgid,reqid,inode,owner,(op==POSIX_LOCK_CMD_INT)?'I':(op==POSIX_LOCK_CMD_GET)?'G':(op==POSIX_LOCK_CMD_SET)?'S':(op==POSIX_LOCK_CMD_TRY)?'T':'?',(i_type==POSIX_LOCK_RDLCK)?'R':(i_type==POSIX_LOCK_WRLCK)?'W':(i_type==POSIX_LOCK_UNLCK)?'U':'?',i_start,i_end,i_pid);

	if ((op==POSIX_LOCK_CMD_SET || op==POSIX_LOCK_CMD_TRY) && i_type!=POSIX_LOCK_UNLCK) {
		if (of_checknode(sessionid,inode)==0) {
			return MFS_ERROR_NOTOPENED;
		}
	}

	il = posix_lock_inode_find(inode);

	if (op==POSIX_LOCK_CMD_INT) {
		if (il==NULL) {
			return MFS_STATUS_OK;
		}
		posix_lock_interrupt(il,sessionid,reqid);
		return MFS_STATUS_OK;
	}
	if (op==POSIX_LOCK_CMD_GET) {
		if (il!=NULL && i_type!=POSIX_LOCK_UNLCK) {
			if (posix_lock_get_offensive_lock(il,sessionid,owner,type,start,end,pid)) {
				return MFS_STATUS_OK;
			}
		}
		*type = POSIX_LOCK_UNLCK;
		*start = 0;
		*end = 0;
		*pid = 0;
		return MFS_STATUS_OK;
	}
	if (il!=NULL && i_type!=POSIX_LOCK_UNLCK) {
		if (posix_lock_find_offensive_lock(il,sessionid,owner,i_type,i_start,i_end)) {
			if (op==POSIX_LOCK_CMD_TRY) {
				return MFS_ERROR_EAGAIN;
			} else {
				posix_lock_append_lock(il,sessionid,msgid,reqid,owner,i_type,i_start,i_end,i_pid);
				return MFS_ERROR_WAITING;
			}
		}
	}
	if (i_type==POSIX_LOCK_UNLCK) {
		if (il==NULL) {
			return MFS_STATUS_OK;
		}
		posix_lock_apply_lock(il,sessionid,owner,i_type,i_start,i_end,i_pid);
		posix_lock_check_waiting(il);
		return MFS_STATUS_OK;
	}
	if (il==NULL) {
		il = posix_lock_inode_new(inode);
	}
	if (posix_lock_find_offensive_lock(il,sessionid,owner,i_type,i_start,i_end)) {
		posix_lock_append_lock(il,sessionid,msgid,reqid,owner,i_type,i_start,i_end,i_pid);
		return MFS_ERROR_WAITING;
	}
	posix_lock_apply_lock(il,sessionid,owner,i_type,i_start,i_end,i_pid);
	posix_lock_check_waiting(il);
	return MFS_STATUS_OK;
}

void posix_lock_file_closed(uint32_t sessionid,uint32_t inode) {
	inodelocks *il;
	wlock *wl,*nwl;
	alock *al,**alptr;
	uint8_t changed;

	il = posix_lock_inode_find(inode);
	if (il==NULL) {
		return;
	}

	wl = il->waiting_head;
	while (wl) {
		nwl = wl->next;
		if (wl->sessionid==sessionid) {
			posix_lock_remove_lock(il,wl);
		}
		wl = nwl;
	}

	changed = 0;
	alptr = &(il->active);
	while ((al=*alptr)) {
		if (al->sessionid==sessionid) {
			posix_lock_apply_range(&(al->ranges),POSIX_LOCK_UNLCK,0,UINT64_MAX);
			massert(al->ranges==NULL,"locks axists after unlocking everything !!!");
			*alptr = al->next;
			free(al);
			changed = 1;
		} else {
			alptr = &(al->next);
		}
	}

	if (changed) {
		posix_lock_check_waiting(il);
	} else if (il->active==NULL && il->waiting_head==NULL) {
		posix_lock_inode_remove(il->inode);
	}
}

uint32_t posix_lock_list(uint32_t inode,uint8_t *buff) {
	inodelocks *il;
	alock *al;
	range *r;
	uint32_t h;
	uint32_t ret=0;

	if (inode==0) {
		for (h=0 ; h<POSIX_LOCK_INODE_HASHSIZE ; h++) {
			for (il = inodehash[h] ; il ; il=il->next) {
				for (al=il->active ; al ; al=al->next) {
					for (r=al->ranges ; r ; r=r->next) {
						if (buff==NULL) {
							ret+=37;
						} else {
							put32bit(&buff,il->inode);
							put32bit(&buff,al->sessionid);
							put64bit(&buff,al->owner);
							put32bit(&buff,al->pid);
							put64bit(&buff,r->start);
							put64bit(&buff,r->end);
							switch (r->type) {
								case POSIX_LOCK_RDLCK:
									put8bit(&buff,1);
									break;
								case POSIX_LOCK_WRLCK:
									put8bit(&buff,2);
									break;
								default:
									put8bit(&buff,0);
							}
						}
					}
				}
			}
		}
	} else {
		il = posix_lock_inode_find(inode);
		if (il!=NULL) {
			for (al=il->active ; al ; al=al->next) {
				for (r=al->ranges ; r ; r=r->next) {
					if (buff==NULL) {
						ret+=33;
					} else {
						put32bit(&buff,al->sessionid);
						put64bit(&buff,al->owner);
						put32bit(&buff,al->pid);
						put64bit(&buff,r->start);
						put64bit(&buff,r->end);
						switch (r->type) {
							case POSIX_LOCK_RDLCK:
								put8bit(&buff,1);
								break;
							case POSIX_LOCK_WRLCK:
								put8bit(&buff,2);
								break;
							default:
								put8bit(&buff,0);
						}
					}
				}
			}
		}
	}
	return ret;
}

uint8_t posix_lock_mr_change(uint32_t inode,uint32_t sessionid,uint64_t owner,char cmd,uint64_t start,uint64_t end,uint32_t pid) {
	inodelocks *il;
	uint8_t type;

	if (cmd=='U' || cmd=='u') {
		il = posix_lock_inode_find(inode);
		if (il==NULL) {
			return MFS_ERROR_MISMATCH;
		}
		type = POSIX_LOCK_UNLCK;
	} else if (cmd=='R' || cmd=='r' || cmd=='S' || cmd=='s') {
		il = posix_lock_inode_find(inode);
		if (il==NULL) {
			il = posix_lock_inode_new(inode);
		}
		type = POSIX_LOCK_RDLCK;
	} else if (cmd=='W' || cmd=='w' || cmd=='E' || cmd=='e') {
		il = posix_lock_inode_find(inode);
		if (il==NULL) {
			il = posix_lock_inode_new(inode);
		}
		type = POSIX_LOCK_WRLCK;
	} else {
		return MFS_ERROR_EINVAL;
	}
	if (type!=POSIX_LOCK_UNLCK && posix_lock_find_offensive_lock(il,sessionid,owner,type,start,end)) {
		return MFS_ERROR_MISMATCH;
	}
	posix_lock_do_apply_lock(il,sessionid,owner,type,start,end,pid);
	meta_version_inc();
	return MFS_STATUS_OK;
}

#define POSIX_LOCK_REC_SIZE 37

uint8_t posix_lock_store(bio *fd) {
	uint8_t storebuff[POSIX_LOCK_REC_SIZE];
	uint8_t *ptr;
	uint32_t h;
	inodelocks *il;
	alock *al;
	range *r;

	if (fd==NULL) {
		return 0x10;
	}
	for (h=0 ; h<POSIX_LOCK_INODE_HASHSIZE ; h++) {
		for (il = inodehash[h] ; il ; il=il->next) {
			for (al=il->active ; al ; al=al->next) {
				for (r=al->ranges ; r ; r=r->next) {
					ptr = storebuff;
					put32bit(&ptr,il->inode);
					put64bit(&ptr,al->owner);
					put32bit(&ptr,al->sessionid);
					put32bit(&ptr,al->pid);
					put64bit(&ptr,r->start);
					put64bit(&ptr,r->end);
					put8bit(&ptr,r->type);
					if (bio_write(fd,storebuff,POSIX_LOCK_REC_SIZE)!=POSIX_LOCK_REC_SIZE) {
						return 0xFF;
					}
				}
			}
		}
	}
	memset(storebuff,0,POSIX_LOCK_REC_SIZE);
	if (bio_write(fd,storebuff,POSIX_LOCK_REC_SIZE)!=POSIX_LOCK_REC_SIZE) {
		return 0xFF;
	}
	return 0;
}

int posix_lock_load(bio *fd,uint8_t mver,uint8_t ignoreflag) {
	uint8_t loadbuff[POSIX_LOCK_REC_SIZE];
	const uint8_t *ptr;
	int32_t l;
	uint32_t inode,lastinode,sessionid,lastsessionid,pid;
	uint64_t owner,lastowner,start,end,lastend;
	uint8_t type,lasttype;
	uint8_t fino,fses;
	inodelocks *il;
	alock *al,**altail;
	range *r,**rtail;

	if (mver!=0x10) {
		return -1;
	}

	fino = 1;
	fses = 1;
	lastinode = 0;
	lastsessionid = 0;
	lastowner = 0;
	lasttype = 0; // make gcc happy
	lastend = 0; // make gcc happy
	il = NULL; // make gcc happy
	al = NULL; // make gcc happy
	r = NULL; // make gcc happy
	altail = NULL; // make gcc happy
	rtail = NULL; // make gcc happy
	for (;;) {
		l = bio_read(fd,loadbuff,POSIX_LOCK_REC_SIZE);
		if (l!=POSIX_LOCK_REC_SIZE) {
			return -1;
		}
		ptr = loadbuff;
		inode = get32bit(&ptr);
		owner = get64bit(&ptr);
		sessionid = get32bit(&ptr);
		pid = get32bit(&ptr);
		start = get64bit(&ptr);
		end = get64bit(&ptr);
		type = get8bit(&ptr);
		if (inode==0 && owner==0 && sessionid==0) {
			return 0;
		}
		if (inode!=lastinode || sessionid!=lastsessionid || fino || fses) {
			if (of_checknode(sessionid,inode)==0) {
				if (ignoreflag) {
					mfs_syslog(LOG_ERR,"loading posix_locks: lock on closed file !!! (ignoring)");
					continue;
				} else {
					mfs_syslog(LOG_ERR,"loading posix_locks: lock on closed file !!!");
					return -1;
				}
			}
		}
		// add lock
		if (inode!=lastinode || fino) {
			lastinode = inode;
			lastsessionid = 0;
			lastowner = 0;
			fses = 1;
			il = posix_lock_inode_find(inode);
			if (il==NULL) {
				il = posix_lock_inode_new(inode);
			}
			altail = &(il->active);
			fino = 0;
		}
		if (sessionid!=lastsessionid || owner!=lastowner || fses) {
			lastsessionid = sessionid;
			lastowner = owner;
			lastend = 0;
			lasttype = POSIX_LOCK_UNLCK;
			al = malloc(sizeof(alock));
			passert(al);
			al->owner = owner;
			al->sessionid = sessionid;
			al->pid = pid;
			al->ranges = NULL;
			al->next = NULL;
			*altail = al;
			altail = &(al->next);
			rtail = &(al->ranges);
			fses = 0;
		}
		if (lasttype!=POSIX_LOCK_UNLCK) {
			if (start<lastend) {
				if (ignoreflag) {
					mfs_syslog(LOG_ERR,"loading posix_locks: lock range not in order !!! (ignoring)");
					continue;
				} else {
					mfs_syslog(LOG_ERR,"loading posix_locks: lock range not in order !!!");
					return -1;
				}
			}
			if (type==lasttype && start==lastend) {
				if (ignoreflag) {
					mfs_syslog(LOG_ERR,"loading posix_locks: lock range not connected !!! (ignoring)");
					continue;
				} else {
					mfs_syslog(LOG_ERR,"loading posix_locks: lock range not connected !!!");
					return -1;
				}
			}
		}
		r = malloc(sizeof(range));
		passert(r);
		r->start = start;
		r->end = end;
		r->type = type;
		r->next = NULL;
		*rtail = r;
		rtail = &(r->next);
		lastend = end;
		lasttype = type;
	}
	return 0; // unreachable
}

void posix_lock_cleanup(void) {
	uint32_t h;
	inodelocks *il,*nil;
	wlock *wl,*nwl;
	alock *al,*nal;
	range *r,*nr;

	for (h=0 ; h<POSIX_LOCK_INODE_HASHSIZE ; h++) {
		il = inodehash[h];
		while (il) {
			nil = il->next;
			wl = il->waiting_head;
			while (wl) {
				nwl = wl->next;
				free(wl);
				wl = nwl;
			}
			al = il->active;
			while (al) {
				nal = al->next;
				r = al->ranges;
				while (r) {
					nr = r->next;
					free(r);
					r = nr;
				}
				free(al);
				al = nal;
			}
			free(il);
			il = nil;
		}
		inodehash[h] = NULL;
	}
}

int posix_lock_init(void) {
	uint32_t i;
	inodehash = malloc(sizeof(inodelocks*)*POSIX_LOCK_INODE_HASHSIZE);
	passert(inodehash);
	for (i=0 ; i<POSIX_LOCK_INODE_HASHSIZE ; i++) {
		inodehash[i] = NULL;
	}
	return 0;
}

#endif

#ifdef MFSTEST

#include <stdio.h>

void posix_lock_print_ranges(range *r) {
	uint64_t pos;
	range *rm;
	if (r) {
		rm = r;
		while (r) {
			printf("%c:<%"PRIu64",%"PRIu64")%s",(r->type==POSIX_LOCK_RDLCK)?'R':(r->type==POSIX_LOCK_WRLCK)?'W':'?',r->start,r->end,(r->next!=NULL)?" ; ":"\n");
			r = r->next;
		}
		r = rm;
		for (pos=0 ; pos<260 ; pos++) {
			while (r!=NULL && pos>=r->end) {
				r = r->next;
			}
			if (r==NULL || pos<r->start) {
				printf(".");
			} else {
				printf("%c",(r->type==POSIX_LOCK_RDLCK)?'o':(r->type==POSIX_LOCK_WRLCK)?'O':'?');
			}
		}
		printf("\n");
	} else {
		printf("empty\n");
	}
}

void posix_lock_verbose_apply_range(range **rptr,uint8_t type,uint64_t start,uint64_t end) {
	uint64_t pos;
	printf(" + %c:<%"PRIu64",%"PRIu64")\n",(type==POSIX_LOCK_RDLCK)?'R':(type==POSIX_LOCK_WRLCK)?'W':(type==POSIX_LOCK_UNLCK)?'U':'?',start,end);
	for (pos=0 ; pos<260 ; pos++) {
		if (pos<start || pos>=end) {
			printf("-");
		} else {
			printf("%c",(type==POSIX_LOCK_RDLCK)?'o':(type==POSIX_LOCK_WRLCK)?'O':'.');
		}
	}
	printf("\n");
	posix_lock_apply_range(rptr,type,start,end);
}

int main(int argc,char **argv) {
	range *r;
	r = NULL;

	if (argc<=1) {
		printf("usage: %s 1|2\n",argv[0]);
		return 1;
	}
	if (argv[1][0]=='1') {
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,20,25);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,30,35);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,10,15);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,19,26);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,18,25);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,20,27);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,20,25);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,11,34);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_WRLCK,20,25);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_UNLCK,15,20);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_UNLCK,25,30);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_WRLCK,15,20);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_WRLCK,25,30);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,15,20);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,25,30);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_RDLCK,20,25);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_UNLCK,25,30);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_UNLCK,15,20);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_UNLCK,0,5);
		posix_lock_print_ranges(r);
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_UNLCK,0,UINT64_MAX);
		posix_lock_print_ranges(r);
	}
	if (argv[1][0]=='2') {
		uint16_t x,start,end;
		uint8_t type;
		uint32_t i;
		for (i=0 ; i<1000 ; i++) {
			do {
				start = random()%250;
				end = random()%250;
			} while (start==end);
			if (start>end) {
				x = start;
				start = end;
				end = x;
			}
			switch (random()&3) {
				case 0:
					type = POSIX_LOCK_RDLCK;
					break;
				case 1:
					type = POSIX_LOCK_WRLCK;
					break;
				case 2:
					if (r==NULL) {
						type = POSIX_LOCK_RDLCK;
					} else {
						type = POSIX_LOCK_UNLCK;
					}
					break;
				case 3:
					if (r==NULL) {
						type = POSIX_LOCK_WRLCK;
					} else {
						type = POSIX_LOCK_UNLCK;
					}
					break;
			}
			posix_lock_verbose_apply_range(&r,type,start,end);
			posix_lock_print_ranges(r);
		}
		posix_lock_verbose_apply_range(&r,POSIX_LOCK_UNLCK,0,UINT64_MAX);
		posix_lock_print_ranges(r);
	}
}
#endif
