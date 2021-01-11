/*
 * Copyright (C) 2021 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
#include <string.h>
#include <inttypes.h>
#include <pthread.h>

#include "stats.h"
#include "clocks.h"
#include "massert.h"

#define HASH_FUNCTIONS 4
#define HASH_BUCKET_SIZE 16
#define HASH_BUCKETS 6257

// entries in cache = HASH_FUNCTIONS*HASH_BUCKET_SIZE*HASH_BUCKETS
// // 4 * 16 * 6257 = 400448
// // Symlink cache capacity can be easly changed by altering HASH_BUCKETS value.
// // Any number should work but it is better to use prime numers here.

typedef struct _hashbucket {
	uint32_t inode[HASH_BUCKET_SIZE];
	uint8_t nleng[HASH_BUCKET_SIZE];
	uint8_t *name[HASH_BUCKET_SIZE];
	double time[HASH_BUCKET_SIZE];
} hashbucket;


static hashbucket *negentryhash = NULL;
static double lastvalidentry = 0.0;
static double NegEntryTimeOut = 1.0;
static pthread_mutex_t necachelock = PTHREAD_MUTEX_INITIALIZER;

enum {
	INSERTS = 0,
	REMOVALS,
	SEARCH_HITS,
	SEARCH_MISSES,
	ENTRIES,
	STATNODES
};

static void *statsptr[STATNODES];

static inline void negentry_cache_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"negentry_cache",0,0);
	statsptr[INSERTS] = stats_get_subnode(s,"inserts",0,1);
	statsptr[REMOVALS] = stats_get_subnode(s,"removals",0,1);
	statsptr[SEARCH_HITS] = stats_get_subnode(s,"search_hits",0,1);
	statsptr[SEARCH_MISSES] = stats_get_subnode(s,"search_misses",0,1);
	statsptr[ENTRIES] = stats_get_subnode(s,"#entries",1,1);
}

static inline void negentry_cache_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_counter_inc(statsptr[id]);
	}
}

static inline void negentry_cache_stats_dec(uint8_t id) {
	if (id<STATNODES) {
		stats_counter_dec(statsptr[id]);
	}
}

static inline uint32_t negentry_cache_hash(uint8_t n,uint32_t inode,uint8_t nleng,const uint8_t *name) {
	static const uint32_t primes[HASH_FUNCTIONS] = {1072573589U,3465827623U,2848548977U,748191707U};
	uint32_t hash;
	uint8_t i;

	hash = inode;
	hash *= primes[n];
	hash += nleng;
#if defined(FAST_DATAPACK)
	for (i=0 ; i<nleng/4 ; i++) {
		hash *= primes[n];
		hash += ((const uint32_t*)name)[i];
	}
	for (i=0 ; i<nleng%4 ; i++) {
		hash *= primes[n];
		hash += name[(nleng&~3)+i];
	}
#else
	for (i=0 ; i<nleng ; i++) {
		hash *= primes[n];
		hash += name[i];
	}
#endif
	return hash;
}

void negentry_cache_insert(uint32_t inode,uint8_t nleng,const uint8_t *name) {
	hashbucket *hb,*fhb;
	uint8_t h,i,fi;
	double t;
	double mint;

	if (NegEntryTimeOut<=0.0) {
		return;
	}
	t = monotonic_seconds();
	mint = t;
	fi = 0;
	fhb = NULL;

	negentry_cache_stats_inc(INSERTS);
	zassert(pthread_mutex_lock(&necachelock));
	for (h=0 ; h<HASH_FUNCTIONS ; h++) {
		hb = negentryhash + (negentry_cache_hash(h,inode,nleng,name)%HASH_BUCKETS);
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->inode[i]==inode && hb->nleng[i]==nleng && memcmp(hb->name[i],name,nleng)==0) {
				hb->time[i] = t;
				zassert(pthread_mutex_unlock(&necachelock));
				return;
			}
			if (hb->time[i]<mint) {
				fhb = hb;
				fi = i;
				mint = hb->time[i];
			}
		}
	}
	if (fhb) {
		if (fhb->time[fi]==0.0) {
			negentry_cache_stats_inc(ENTRIES);
		}
		fhb->inode[fi]=inode;
		fhb->nleng[fi]=nleng;
		if (fhb->name[fi]) {
			free(fhb->name[fi]);
		}
		fhb->name[fi] = malloc(nleng);
		passert(fhb->name[fi]);
		memcpy(fhb->name[fi],name,nleng);
		fhb->time[fi]=t;
	}
	zassert(pthread_mutex_unlock(&necachelock));
}

void negentry_cache_remove(uint32_t inode,uint8_t nleng,const uint8_t *name) {
	hashbucket *hb;
	uint8_t h,i;
	uint8_t f;
	double t;

	if (NegEntryTimeOut<=0.0) {
		return;
	}
	t = monotonic_seconds();

	negentry_cache_stats_inc(REMOVALS);
	zassert(pthread_mutex_lock(&necachelock));
	for (h=0 ; h<HASH_FUNCTIONS ; h++) {
		f = 0;
		hb = negentryhash + (negentry_cache_hash(h,inode,nleng,name)%HASH_BUCKETS);
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->inode[i]==inode && hb->nleng[i]==nleng && memcmp(hb->name[i],name,nleng)==0) {
				f = 2;
			}
			if (hb->time[i]>0.0 && (hb->time[i] + NegEntryTimeOut < t || hb->time[i] < lastvalidentry || f==2)) {
				hb->inode[i] = 0;
				hb->nleng[i] = 0;
				if (hb->name[i]) {
					free(hb->name[i]);
				}
				hb->name[i] = NULL;
				hb->time[i] = 0.0;
				negentry_cache_stats_dec(ENTRIES);
			}
			if (f==2) {
				f = 1;
			}
		}
		if (f) {
			zassert(pthread_mutex_unlock(&necachelock));
			return;
		}
	}
	zassert(pthread_mutex_unlock(&necachelock));
	return;
}

uint8_t negentry_cache_search(uint32_t inode,uint8_t nleng,const uint8_t *name) {
	hashbucket *hb;
	uint8_t h,i;
	uint8_t f;
	double t;

	if (NegEntryTimeOut<=0.0) {
		return 0;
	}
	t = monotonic_seconds();

	zassert(pthread_mutex_lock(&necachelock));
	for (h=0 ; h<HASH_FUNCTIONS ; h++) {
		f = 0;
		hb = negentryhash + (negentry_cache_hash(h,inode,nleng,name)%HASH_BUCKETS);
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->time[i]>0.0 && (hb->time[i] + NegEntryTimeOut < t || hb->time[i] < lastvalidentry)) {
				hb->inode[i] = 0;
				hb->nleng[i] = 0;
				if (hb->name[i]) {
					free(hb->name[i]);
				}
				hb->name[i] = NULL;
				hb->time[i] = 0.0;
				negentry_cache_stats_dec(ENTRIES);
			} else if (hb->inode[i]==inode && hb->nleng[i]==nleng && memcmp(hb->name[i],name,nleng)==0) {
				f = 1;
			}
		}
		if (f) {
			zassert(pthread_mutex_unlock(&necachelock));
			negentry_cache_stats_inc(SEARCH_HITS);
			return 1;
		}
	}
	zassert(pthread_mutex_unlock(&necachelock));
	negentry_cache_stats_inc(SEARCH_MISSES);
	return 0;
}

void negentry_cache_clear(void) {
	double t = monotonic_seconds();
	zassert(pthread_mutex_lock(&necachelock));
	lastvalidentry = t;
	zassert(pthread_mutex_unlock(&necachelock));
}

void negentry_cache_init(double to) {
	hashbucket *hb;
	uint8_t i;
	uint32_t hi;

	NegEntryTimeOut = to;
	if (to<=0.0) {
		return;
	}
	negentryhash = malloc(sizeof(hashbucket)*HASH_BUCKETS);
	passert(negentryhash);
	for (hi=0 ; hi<HASH_BUCKETS ; hi++) {
		hb = negentryhash + hi;
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			hb->inode[i] = 0;
			hb->nleng[i] = 0;
			hb->name[i] = NULL;
			hb->time[i] = 0.0;
		}
	}
	negentry_cache_statsptr_init();
}

void negentry_cache_term(void) {
	hashbucket *hb;
	uint8_t i;
	uint32_t hi;

	if (NegEntryTimeOut<=0.0 || negentryhash==NULL) {
		return;
	}
	pthread_mutex_lock(&necachelock);
	for (hi=0 ; hi<HASH_BUCKETS ; hi++) {
		hb = negentryhash + hi;
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->name[i]) {
				free(hb->name[i]);
			}
			hb->name[i]=NULL;
		}
	}
	free(negentryhash);
	negentryhash = NULL;
	pthread_mutex_unlock(&necachelock);
}
