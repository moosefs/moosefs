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

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>

#include "stats.h"
#include "MFSCommunication.h"

#define HASH_FUNCTIONS 4
#define HASH_BUCKET_SIZE 16
#define HASH_BUCKETS 6257


// cache:
// (inode,pos) -> (chunkid,chunkversion,N*[ip,port])



// entries in cache = HASH_FUNCTIONS*HASH_BUCKET_SIZE*HASH_BUCKETS
// 4 * 16 * 6257 = 400448
// Symlink cache capacity can be easly changed by altering HASH_BUCKETS value.
// Any number should work but it is better to use prime numers here.

typedef struct _hashbucket {
	// key
	uint32_t inode[HASH_BUCKET_SIZE];
	uint32_t pos[HASH_BUCKET_SIZE];
	// timestamp
	uint32_t time[HASH_BUCKET_SIZE];
	// values
	uint64_t chunkid[HASH_BUCKET_SIZE];
	uint32_t chunkversion[HASH_BUCKET_SIZE];
	uint8_t csdatasize[HASH_BUCKET_SIZE];
	uint8_t *csdata[HASH_BUCKET_SIZE];
} hashbucket;

static hashbucket *chunklochash = NULL;
static pthread_mutex_t clcachelock = PTHREAD_MUTEX_INITIALIZER;

/*
enum {
	HITS_CORRECT = 0,
	HITS_WRONG,
	MISSES,
	STATNODES
};

static uint64_t *statsptr[STATNODES];

static inline void chunkloc_cache_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"chunkloc_cache_test",0);
	statsptr[HITS_CORRECT] = stats_get_counterptr(stats_get_subnode(s,"hits_correct",0));
	statsptr[HITS_WRONG] = stats_get_counterptr(stats_get_subnode(s,"hits_wrong",0));
	statsptr[MISSES] = stats_get_counterptr(stats_get_subnode(s,"misses",0));
}

static inline void chunkloc_cache_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_lock();
		(*statsptr[id])++;
		stats_unlock();
	}
}
*/

//static uint32_t stats_hit_correct = 0;
//static uint32_t stats_hit_wrong = 0;
//static uint32_t stats_miss = 0;


/*
static inline int chunkloc_compare(const uint8_t *l1,const uint8_t *l2,uint8_t s) {
	uint8_t mul1,sum1,xor1;
	uint8_t mul2,sum2,xor2;
	mul1=mul2=1;
	sum1=sum2=xor1=xor2=0;
	while (s>0) {
		s--;
		mul1*=l1[s];
		mul2*=l2[s];
		sum1+=l1[s];
		sum2+=l2[s];
		xor1^=l1[s];
		xor2^=l2[s];
	}
	return (mul1==mul2&&sum1==sum2&&xor1==xor2)?1:0;
}
*/

void chunkloc_cache_insert(uint32_t inode,uint32_t pos,uint64_t chunkid,uint32_t chunkversion,uint8_t csdatasize,const uint8_t *csdata) {
	uint32_t primes[HASH_FUNCTIONS] = {1072573589U,3465827623U,2848548977U,748191707U};
	hashbucket *hb,*fhb;
	uint8_t h,i,fi;
	uint32_t now;
	uint32_t mints;

	now = time(NULL);
	mints = UINT32_MAX;
	fi = 0;
	fhb = NULL;

	pthread_mutex_lock(&clcachelock);
	for (h=0 ; h<HASH_FUNCTIONS ; h++) {
		hb = chunklochash + ((inode*primes[h]+pos*primes[HASH_FUNCTIONS-1-h])%HASH_BUCKETS);
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->inode[i]==inode && hb->pos[i]==pos) {
//				if (hb->chunkid[i]!=chunkid || hb->chunkversion[i]!=chunkversion || hb->csdatasize[i]!=csdatasize || chunkloc_compare(hb->csdata[i],csdata,csdatasize)==0) {
//					chunkloc_cache_stats_inc(HITS_WRONG);
//				} else {
//					chunkloc_cache_stats_inc(HITS_CORRECT);
//				}
				if (hb->csdata[i]) {
					free(hb->csdata[i]);
				}
				hb->chunkid[i] = chunkid;
				hb->chunkversion[i] = chunkversion;
				hb->csdatasize[i] = csdatasize;
				if (csdatasize>0) {
					hb->csdata[i] = (uint8_t*)malloc(csdatasize);
					memcpy(hb->csdata[i],csdata,csdatasize);
				} else {
					hb->csdata[i] = NULL;
				}
				hb->time[i]=now;
				pthread_mutex_unlock(&clcachelock);
				return;
			}
			if (hb->time[i]<mints) {
				fhb = hb;
				fi = i;
				mints = hb->time[i];
			}
		}
	}
//	chunkloc_cache_stats_inc(MISSES);
	if (fhb) {	// just sanity check
		fhb->inode[fi] = inode;
		fhb->pos[fi] = pos;
		if (fhb->csdata[fi]) {
			free(fhb->csdata[fi]);
		}
		fhb->chunkid[fi] = chunkid;
		fhb->chunkversion[fi] = chunkversion;
		fhb->csdatasize[fi] = csdatasize;
		if (csdatasize>0) {
			fhb->csdata[fi] = (uint8_t*)malloc(csdatasize);
			memcpy(fhb->csdata[fi],csdata,csdatasize);
		} else {
			fhb->csdata[fi] = NULL;
		}
		fhb->time[fi]=now;
	}
	pthread_mutex_unlock(&clcachelock);
}

int chunkloc_cache_search(uint32_t inode,uint32_t pos,uint64_t *chunkid,uint32_t *chunkversion,uint8_t *csdatasize,const uint8_t **csdata) {
	uint32_t primes[HASH_FUNCTIONS] = {1072573589U,3465827623U,2848548977U,748191707U};
	hashbucket *hb;
	uint8_t h,i;
//	uint32_t now;

//	now = time(NULL);

	pthread_mutex_lock(&clcachelock);
	for (h=0 ; h<HASH_FUNCTIONS ; h++) {
		hb = chunklochash + ((inode*primes[h]+pos*primes[HASH_FUNCTIONS-1-h])%HASH_BUCKETS);
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->inode[i]==inode && hb->pos[i]==pos) {
				*chunkid = hb->chunkid[i];
				*chunkversion = hb->chunkversion[i];
				*csdatasize = hb->csdatasize[i];
				*csdata = hb->csdata[i];
				pthread_mutex_unlock(&clcachelock);
				return 1;
			}
		}
	}
	pthread_mutex_unlock(&clcachelock);
	return 0;
}

void chunkloc_cache_init(void) {
	chunklochash = malloc(sizeof(hashbucket)*HASH_BUCKETS);
	memset(chunklochash,0,sizeof(hashbucket)*HASH_BUCKETS);
//	chunkloc_cache_statsptr_init();
}

void chunkloc_cache_term(void) {
	hashbucket *hb;
	uint8_t i;
	uint32_t hi;

	pthread_mutex_lock(&clcachelock);
	for (hi=0 ; hi<HASH_BUCKETS ; hi++) {
		hb = chunklochash + hi;
		for (i=0 ; i<HASH_BUCKET_SIZE ; i++) {
			if (hb->csdata[i]) {
				free(hb->csdata[i]);
			}
		}
	}
	free(chunklochash);
	pthread_mutex_unlock(&clcachelock);
}
