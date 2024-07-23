/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
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

/* common */
#include <inttypes.h>
#include <stdlib.h>
#include <pthread.h>

#include "massert.h"
#include "portable.h"
#include "lwthread.h"
#include "clocks.h"

#define MAX_LIST_LENGTH 20

#define HASH_SIZE 16384

typedef struct _sinoparent {
	uint32_t inode;
	uint32_t parent;
	uint32_t validtime;
	struct _sinoparent *next;
} sinoparent;

static sinoparent* sparents_hash[HASH_SIZE];
static pthread_mutex_t sparents_lock[HASH_SIZE];

#ifndef HAVE___SYNC_FETCH_AND_OP
static pthread_mutex_t glock;
#endif
static pthread_t clthread;
static uint8_t term;

static inline uint32_t sparents_hashfn(uint32_t inode) {
	return (inode%HASH_SIZE);
}

void sparents_add(uint32_t inode,uint32_t parent,uint32_t timeout) {
	uint32_t hash;
	uint32_t leng;
	sinoparent *oldest;
	sinoparent *sip;

	hash = sparents_hashfn(inode);
	zassert(pthread_mutex_lock(sparents_lock+hash));
	sip = sparents_hash[hash];
	oldest = NULL;
	leng = 0;
	while (sip!=NULL) {
		if (sip->inode==inode) {
			sip->parent = parent;
			sip->validtime = monotonic_seconds() + timeout;
			zassert(pthread_mutex_unlock(sparents_lock+hash));
			return;
		}
		if (oldest==NULL || oldest->validtime > sip->validtime) {
			oldest = sip;
		}
		leng++;
		sip = sip->next;
	}
	if (leng>=MAX_LIST_LENGTH && oldest!=NULL) { // second condition practically not needed unless somebody define MAX_LIST_LENGTH == 0
		sip = oldest;
	} else {
		sip = malloc(sizeof(sinoparent));
		sip->next = sparents_hash[hash];
		sparents_hash[hash] = sip;
	}
	sip->inode = inode;
	sip->parent = parent;
	sip->validtime = monotonic_seconds() + timeout;
	zassert(pthread_mutex_unlock(sparents_lock+hash));
}

uint32_t sparents_get(uint32_t inode) {
	uint32_t hash;
	uint32_t parent;
	sinoparent *sip;

	parent = 0;
	hash = sparents_hashfn(inode);
	zassert(pthread_mutex_lock(sparents_lock+hash));
	sip = sparents_hash[hash];
	while (sip!=NULL) {
		if (sip->inode==inode) {
			parent = sip->parent;
			break;
		}
		sip = sip->next;
	}
	zassert(pthread_mutex_unlock(sparents_lock+hash));
	return parent;
}

static void sparents_cleanup(uint32_t hash,uint32_t current_time) {
	sinoparent *sip,**sipp;

	zassert(pthread_mutex_lock(sparents_lock+hash));
	sipp = sparents_hash + hash;
	while ((sip=*sipp)!=NULL) {
		if (current_time==UINT32_C(0xFFFFFFFF) || sip->validtime < current_time) {
			*sipp = sip->next;
			free(sip);
		} else {
			sipp = &(sip->next);
		}
	}
	zassert(pthread_mutex_unlock(sparents_lock+hash));
}

static void* sparents_cleanupthread(void *arg) {
	uint32_t cuhashpos;
	uint32_t current_time;
	uint32_t i;
	(void)arg;

	cuhashpos = 0;
	while (1) {
		current_time = monotonic_seconds();
		for (i=0 ; i<(HASH_SIZE/(10*30)) ; i++) {
			sparents_cleanup(cuhashpos,current_time);
			cuhashpos = (cuhashpos+1)%HASH_SIZE;
		}
		portable_usleep(100000);
#ifdef HAVE___SYNC_FETCH_AND_OP
		if (__sync_fetch_and_or(&term,0)==1) {
			return NULL;
		}
#else
		zassert(pthread_mutex_lock(&glock));
		if (term==1) {
			zassert(pthread_mutex_unlock(&glock));
			return NULL;
		}
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
	return NULL;
}

void sparents_term(void) {
	uint32_t hash;
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_or(&term,1);
#else
	zassert(pthread_mutex_lock(&glock));
	term = 1;
	zassert(pthread_mutex_unlock(&glock));
#endif
	pthread_join(clthread,NULL);
	for (hash=0 ; hash<HASH_SIZE ; hash++) {
		sparents_cleanup(hash,UINT32_C(0xFFFFFFFF));
		massert(sparents_hash[hash]==NULL,"structure hasn't been cleaned up");
		zassert(pthread_mutex_destroy(sparents_lock+hash));
	}

#ifndef HAVE___SYNC_FETCH_AND_OP
	zassert(pthread_mutex_destroy(&glock));
#endif
}

void sparents_init(void) {
	uint32_t hash;
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_and(&term,0);
#else
	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_mutex_lock(&glock));
	term = 0;
	zassert(pthread_mutex_unlock(&glock));
#endif
	for (hash=0 ; hash<HASH_SIZE ; hash++) {
		zassert(pthread_mutex_init(sparents_lock+hash,NULL));
		sparents_hash[hash] = NULL;
	}
	lwt_minthread_create(&clthread,0,sparents_cleanupthread,NULL);
}
