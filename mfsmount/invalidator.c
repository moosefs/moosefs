/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <fuse_lowlevel.h>

#if (FUSE_VERSION >= 28) && defined(__linux__)

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "massert.h"
#include "clocks.h"
#include "portable.h"
#include "main.h"
#include "mastercomm.h"

#define INODEHASHSIZE 256
#define INODEHASH(ino) ((ino)%INODEHASHSIZE)

typedef struct _direntry {
	uint32_t parent;
	char *name;
	uint8_t nleng;
	struct _direntry *next;
} direntry;

typedef struct _invalentry {
	uint32_t inode;
	uint8_t notifycnt;
	uint8_t added;
	uint64_t nlookup;
	double next_notify;
	direntry *head;
	struct _invalentry *next;
} invalentry;

static invalentry *hashtab[INODEHASHSIZE];
static pthread_mutex_t hashlock[INODEHASHSIZE];
static struct fuse_chan *fusech;

#ifndef HAVE___SYNC_FETCH_AND_OP
static pthread_mutex_t glock;
#endif
static pthread_t clthread;
static uint8_t term,on;

void invalidator_insert(uint32_t parent,const char *name,uint8_t nleng,uint32_t inode,double timeout) {
	invalentry *ie;
	direntry *de;
	double next_notify;
	uint32_t h;

#ifdef HAVE___SYNC_FETCH_AND_OP
	if (__sync_fetch_and_or(&on,0)==0) {
		return;
	}
#else
	zassert(pthread_mutex_lock(&glock));
	if (on==0) {
		zassert(pthread_mutex_unlock(&glock));
		return;
	}
	zassert(pthread_mutex_unlock(&glock));
#endif

	next_notify = monotonic_seconds() + timeout;
	h = INODEHASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	for (ie = hashtab[h] ; ie ; ie = ie->next) {
		if (ie->inode == inode) {
			ie->nlookup++;
			if (next_notify > ie->next_notify) {
				ie->next_notify = next_notify;
			}
			for (de = ie->head ; de ; de = de->next) {
				if (de->parent == parent && de->nleng == nleng && memcmp(de->name,name,nleng)==0) {
					break;
				}
			}
			break;
		}
	}
	if (ie==NULL) {
		ie = malloc(sizeof(invalentry));
		passert(ie);
		ie->inode = inode;
		ie->notifycnt = 0;
		ie->added = 0;
		ie->nlookup = 1;
		ie->next_notify = next_notify;
		ie->head = NULL;
		de = ie->head;
		ie->next = hashtab[h];
		hashtab[h] = ie;
	}
	if (de==NULL) {
		de = malloc(sizeof(direntry));
		passert(de);
		de->parent = parent;
		de->name = malloc(nleng);
		passert(de->name);
		memcpy(de->name,name,nleng);
		de->nleng = nleng;
		de->next = ie->head;
		ie->head = de;
	}
	zassert(pthread_mutex_unlock(hashlock+h));
}

void invalidator_forget(uint32_t inode,uint64_t nlookup) {
	invalentry *ie,**iep;
	direntry *de,*den;
	uint32_t h;

#ifdef HAVE___SYNC_FETCH_AND_OP
	if (__sync_fetch_and_or(&on,0)==0) {
		return;
	}
#else
	zassert(pthread_mutex_lock(&glock));
	if (on==0) {
		zassert(pthread_mutex_unlock(&glock));
		return;
	}
	zassert(pthread_mutex_unlock(&glock));
#endif

	h = INODEHASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	iep = hashtab + h;
	while ((ie = *iep)) {
		if (ie->inode == inode) {
			if (nlookup <= ie->nlookup) {
				ie->nlookup -= nlookup;
			} else {
				ie->nlookup = 0;
			}
			if (ie->nlookup==0) {
				for (de = ie->head ; de ; de = den) {
					den = de->next;
					free(de->name);
					free(de);
				}
				if (ie->added) {
					fs_forget_entry(inode);
				}
				*iep = ie->next;
				free(ie);
			}
			break;
		} else {
			iep = &(ie->next);
		}
	}
	zassert(pthread_mutex_unlock(hashlock+h));
}

#define MAXNOTIFY 256

void* invalidator_main_thread(void *arg) {
	invalentry *ie;
	direntry *de;
	uint32_t *parent;
	uint8_t *nleng;
	char **name;
	uint32_t no;
	uint32_t h;
	double now;

	parent = malloc(sizeof(uint32_t)*MAXNOTIFY);
	passert(parent);
	nleng = malloc(sizeof(uint8_t)*MAXNOTIFY);
	passert(nleng);
	name = malloc(sizeof(char*)*MAXNOTIFY);
	passert(name);
	for (no = 0 ; no < MAXNOTIFY ; no++) {
		name[no] = malloc(255);
		passert(name[no]);
	}

	h = 0;
	while (1) {
		now = monotonic_seconds();
		zassert(pthread_mutex_lock(hashlock+h));
		no = 0;
		for (ie = hashtab[h] ; ie && no < MAXNOTIFY ; ie = ie->next) {
			if (ie->next_notify < now) {
				for (de = ie->head ; de && no < MAXNOTIFY ; de = de -> next) {
					// can't call fuse_lowlevel_notify_inval_entry here - cause deadlocks
					parent[no] = de->parent;
					nleng[no] = de->nleng;
					memcpy(name[no],de->name,de->nleng);
					no++;
				}
				if (no < MAXNOTIFY) {
					if (ie->added == 0) {
						if (ie->notifycnt < 5) {
							ie->notifycnt++;
						} else {
							fs_add_entry(ie->inode);
							ie->added = 1;
						}
					}
				}
				ie->next_notify = now + 10.0;
			}
		}
		zassert(pthread_mutex_unlock(hashlock+h));
		h++;
		h%=INODEHASHSIZE;
		if (no>10) {
			printf("no: %u\n",no);
		}
		while (no > 0) {
			no--;
			fuse_lowlevel_notify_inval_entry(fusech,parent[no],name[no],nleng[no]);
		}
		portable_usleep(1000);
#ifdef HAVE___SYNC_FETCH_AND_OP
		if (__sync_fetch_and_or(&term,0)==1) {
#else
		zassert(pthread_mutex_lock(&glock));
		if (term==1) {
			zassert(pthread_mutex_unlock(&glock));
#endif
			for (no = 0 ; no < MAXNOTIFY ; no++) {
				free(name[no]);
			}
			free(name);
			free(nleng);
			free(parent);
			return arg;
		}
#ifndef HAVE___SYNC_FETCH_AND_OP
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
	return NULL;
}

void invalidator_term(void) {
	uint32_t h;
	uint8_t _on;
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_or(&term,1);
	_on = __sync_fetch_and_or(&on,0);
#else
	zassert(pthread_mutex_lock(&glock));
	term = 1;
	_on = on;
	zassert(pthread_mutex_unlock(&glock));
#endif
	if (_on) {
		pthread_join(clthread,NULL);
	}
	for (h=0 ; h<INODEHASHSIZE ; h++) {
		zassert(pthread_mutex_destroy(hashlock+h));
	}
#ifndef HAVE___SYNC_FETCH_AND_OP
	zassert(pthread_mutex_destroy(&glock));
#endif
}

void invalidator_on(void) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_or(&on,1);
#else
	zassert(pthread_mutex_lock(&glock));
	on = 1;
	zassert(pthread_mutex_unlock(&glock));
#endif
	main_minthread_create(&clthread,0,invalidator_main_thread,NULL);
}

void invalidator_init(struct fuse_chan *ch) {
	uint32_t h;
	fusech = ch;
	for (h=0 ; h<INODEHASHSIZE ; h++) {
		hashtab[h] = NULL;
		zassert(pthread_mutex_init(hashlock+h,NULL));
	}
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_and(&term,0);
	__sync_fetch_and_and(&on,0);
#else
	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_mutex_lock(&glock));
	term = 0;
	on = 0;
	zassert(pthread_mutex_unlock(&glock));
#endif
}

#else /* FUSE_VERSION >= 28 */

void invalidator_insert(uint32_t parent,const char *name,uint8_t nleng,uint32_t inode,double timeout) {
	(void)parent;
	(void)name;
	(void)nleng;
	(void)inode;
	(void)timeout;
}

void invalidator_forget(uint32_t inode,uint64_t nlookup) {
	(void)inode;
	(void)nlookup;
}

void invalidator_term(void) {
}

void invalidator_on(void) {
}

void invalidator_init(struct fuse_chan *ch) {
	(void)ch;
}

#endif
