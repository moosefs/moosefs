/*
 * Copyright (C) 2019 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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


#ifdef HAVE_ATOMICS
#include <stdatomic.h>
#undef HAVE_ATOMICS
#define HAVE_ATOMICS 1
#else
#define HAVE_ATOMICS 0
#endif

#if defined(HAVE___SYNC_OP_AND_FETCH) && defined(HAVE___SYNC_BOOL_COMPARE_AND_SWAP)
#define HAVE_SYNCS 1
#else
#define HAVE_SYNCS 0
#endif


#define BUCKETS_MT_MMAP_ALLOC 1


#include <stdlib.h>
#include <inttypes.h>
#include <pthread.h>

#include "massert.h"
#include "buckets_mt.h"


#define INOLENG_HASHSIZE 1024
#define INOLENG_HASH(inode) ((inode)%INOLENG_HASHSIZE)

typedef struct _ileng {
	uint32_t inode;
#if HAVE_ATOMICS
	_Atomic uint32_t refcnt;
	_Atomic uint64_t fleng;
#elif HAVE_SYNCS
	volatile uint32_t refcnt;
	volatile uint64_t fleng;
#else
	volatile uint32_t refcnt;
	volatile uint64_t fleng;
#define USE_LOCK 1
	pthread_mutex_t lock;
#endif
	struct _ileng *next;
} ileng;

CREATE_BUCKET_MT_ALLOCATOR(ileng,ileng,500)

static ileng *inolenghashtab[INOLENG_HASHSIZE];
static pthread_mutex_t hashlock[INOLENG_HASHSIZE];

void* inoleng_acquire(uint32_t inode) {
	uint32_t h;
	ileng *ilptr;

	h = INOLENG_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	for (ilptr = inolenghashtab[h] ; ilptr!=NULL ; ilptr=ilptr->next) {
		if (ilptr->inode==inode) {
#if HAVE_ATOMICS
			atomic_fetch_add(&(ilptr->refcnt),1);
#elif HAVE_SYNCS
			__sync_add_and_fetch(&(ilptr->refcnt),1);
#else
			zassert(pthread_mutex_lock(&(ilptr->lock)));
			ilptr->refcnt++;
			zassert(pthread_mutex_unlock(&(ilptr->lock)));
#endif
			zassert(pthread_mutex_unlock(hashlock+h));
			return (void*)ilptr;
		}
	}
	ilptr = ileng_malloc();
	ilptr->inode = inode;
#if HAVE_ATOMICS
	atomic_init(&(ilptr->refcnt),1);
	atomic_init(&(ilptr->fleng),0);
#else
	ilptr->refcnt = 1;
	ilptr->fleng = 0;
#ifdef USE_LOCK
	zassert(pthread_mutex_init(&(ilptr->lock),NULL));
#endif
#endif
	ilptr->next = inolenghashtab[h];
	inolenghashtab[h] = ilptr;
	zassert(pthread_mutex_unlock(hashlock+h));
	return (void*)ilptr;
}

void inoleng_release(void *ptr) {
	uint32_t h;
	ileng *ilptr,**ilpptr;
	ileng *il = (ileng*)ptr;

#if HAVE_ATOMICS
	if (atomic_fetch_sub(&(il->refcnt),1)==1) { // returns value held previously
#elif HAVE_SYNCS
	if (__sync_sub_and_fetch(&(il->refcnt),1)==0) {
#else
	zassert(pthread_mutex_lock(&(il->lock)));
	il->refcnt--;
	if (il->refcnt==0) {
		zassert(pthread_mutex_unlock(&(il->lock)));
#endif
		h = INOLENG_HASH(il->inode);
		zassert(pthread_mutex_lock(hashlock+h));
#if HAVE_ATOMICS
		if (atomic_load(&(il->refcnt))==0) { // still zero after lock
#elif HAVE_SYNCS
		if (__sync_add_and_fetch(&(il->refcnt),0)==0) {
#else
		zassert(pthread_mutex_lock(&(il->lock)));
		if (il->refcnt==0) {
			zassert(pthread_mutex_unlock(&(il->lock)));
#endif
			ilpptr = inolenghashtab + h;
			while ((ilptr=*ilpptr)!=NULL) {
				if (il==ilptr) {
					*ilpptr = ilptr->next;
#ifdef USE_LOCK
					zassert(pthread_mutex_destroy(&(ilptr->lock)));
#endif
					ileng_free(ilptr);
				} else {
					ilpptr = &(ilptr->next);
				}
			}
#if HAVE_ATOMICS || HAVE_SYNCS
		}
#else
		} else {
			zassert(pthread_mutex_unlock(&(il->lock)));
		}
#endif
		zassert(pthread_mutex_unlock(hashlock+h));
	}
}

uint64_t inoleng_getfleng(void *ptr) {
	ileng *il = (ileng*)ptr;
#if HAVE_ATOMICS
	return atomic_load_explicit(&(il->fleng),memory_order_relaxed);
#elif HAVE_SYNCS
	return __sync_add_and_fetch(&(il->fleng),0);
#else
	uint64_t ret;
	zassert(pthread_mutex_lock(&(il->lock)));
	ret = il->fleng;
	zassert(pthread_mutex_unlock(&(il->lock)));
	return ret;
#endif
}

void inoleng_setfleng(void *ptr,uint64_t fleng) {
	ileng *il = (ileng*)ptr;
#if HAVE_ATOMICS
	atomic_store_explicit(&(il->fleng),fleng,memory_order_relaxed);
#elif HAVE_SYNCS
	for (;;) {
		uint64_t ofleng = __sync_add_and_fetch(&(il->fleng),0);
		if (__sync_bool_compare_and_swap(&(il->fleng),ofleng,fleng)) {
			return;
		}
	}
#else
	zassert(pthread_mutex_lock(&(il->lock)));
	il->fleng = fleng;
	zassert(pthread_mutex_unlock(&(il->lock)));
#endif
}

void inoleng_update_fleng(uint32_t inode,uint64_t fleng) {
	uint32_t h;
	ileng *ilptr;

	h = INOLENG_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	for (ilptr = inolenghashtab[h] ; ilptr!=NULL ; ilptr=ilptr->next) {
		if (ilptr->inode==inode) {
#if HAVE_ATOMICS
			atomic_store_explicit(&(ilptr->fleng),fleng,memory_order_relaxed);
#elif HAVE_SYNCS
			for (;;) {
				uint64_t ofleng = __sync_add_and_fetch(&(ilptr->fleng),0);
				if (__sync_bool_compare_and_swap(&(ilptr->fleng),ofleng,fleng)) {
					break;
				}
			}
#else
			zassert(pthread_mutex_lock(&(ilptr->lock)));
			ilptr->fleng = fleng;
			zassert(pthread_mutex_unlock(&(ilptr->lock)));
#endif
		}
	}
	zassert(pthread_mutex_unlock(hashlock+h));
}

void inoleng_term(void) {
	ileng *ilptr,*ilnptr;
	uint32_t refcnt;
	uint32_t h;

	for (h=0 ; h<INOLENG_HASHSIZE ; h++) {
		zassert(pthread_mutex_lock(hashlock+h));
		for (ilptr = inolenghashtab[h] ; ilptr!=NULL ; ilptr=ilnptr) {
			ilnptr = ilptr->next;
#if HAVE_ATOMICS
			refcnt = atomic_load(&(ilptr->refcnt));
#elif HAVE_SYNCS
			refcnt = __sync_add_and_fetch(&(ilptr->refcnt),0);
#else
			zassert(pthread_mutex_lock(&(ilptr->lock)));
			refcnt = ilptr->refcnt;
			zassert(pthread_mutex_unlock(&(ilptr->lock)));
#endif
			syslog(LOG_WARNING,"inode fleng data structure leftovers (ino: %"PRIu32" ; refcnt: %"PRIu32")",ilptr->inode,refcnt);
			ileng_free(ilptr);
		}
		
		zassert(pthread_mutex_unlock(hashlock+h));
		zassert(pthread_mutex_destroy(hashlock+h));
	}
	ileng_free_all();
}

void inoleng_init(void) {
	uint32_t h;

	(void)ileng_getusage; // functions that are defined by CREATE_BUCKET_MT_ALLOCATOR macro but not used

	for (h=0 ; h<INOLENG_HASHSIZE ; h++) {
		inolenghashtab[h] = NULL;
		zassert(pthread_mutex_init(hashlock+h,NULL));
	}
}
