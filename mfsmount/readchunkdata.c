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
#include <signal.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "mastercomm.h"
#include "massert.h"
#include "pcqueue.h"
#include "csorder.h"
#include "clocks.h"

#include "readchunkdata.h"

#define SUSTAIN_WORKERS 10
#define MAX_WORKERS 25

#define MREQ_HASH_SIZE 0x20000

#define MREQ_TIMEOUT 3600.0

#define MREQ_MAX_CHAIN_LENG 5

enum {MR_INIT,MR_READY,MR_REFRESH,MR_INVALID};

typedef struct mreqcache_s {
	double time;
	uint64_t mfleng;
	uint32_t inode;
	uint32_t chindx;
	uint64_t chunkid;
	uint32_t version;
	uint8_t csdataver;
	uint32_t csdatasize;
	uint8_t *csdata;
	uint8_t canmodatime;
	uint8_t reqwaiting;
	uint8_t status;
	uint8_t state;
	struct mreqcache_s *next;
	pthread_cond_t reqcond;
} mreqcache;

typedef struct worker_s {
	pthread_t thread_id;
} worker;

static mreqcache **mreq_cache;
static pthread_mutex_t mreq_cache_lock;

static pthread_mutex_t glock;
static uint32_t workers_avail;
static uint32_t workers_total;
static uint32_t worker_term_waiting;
static pthread_cond_t worker_term_cond;
static pthread_attr_t worker_thattr;

static void *jqueue;

void* read_chunkdata_worker(void*);

/* glock:LOCKED */
static inline void read_chunkdata_spawn_worker(void) {
	sigset_t oldset;
	sigset_t newset;
	worker *w;
	int res;

	w = malloc(sizeof(worker));
	if (w==NULL) {
		return;
	}
	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
	res = pthread_create(&(w->thread_id),&worker_thattr,read_chunkdata_worker,w);
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
	if (res<0) {
		return;
	}
	workers_avail++;
	workers_total++;
}

/* glock:LOCKED */
static inline void read_chunkdata_close_worker(worker *w) {
	workers_avail--;
	workers_total--;
	if (workers_total==0 && worker_term_waiting) {
		zassert(pthread_cond_signal(&worker_term_cond));
		worker_term_waiting--;
	}
	pthread_detach(w->thread_id);
	free(w);
}

static inline void read_chunkdata_refresh(mreqcache *mrc) {
	uint8_t csdataver;
	uint64_t mfleng;
	uint64_t chunkid;
	uint32_t version;
	uint32_t csdatasize;
	uint8_t status;
	const uint8_t *csdata;

	status = fs_readchunk(mrc->inode,mrc->chindx,mrc->canmodatime,&csdataver,&mfleng,&chunkid,&version,&csdata,&csdatasize);

	zassert(pthread_mutex_lock(&mreq_cache_lock));

	mrc->status = status;
	if (status==STATUS_OK) {
		mrc->csdataver = csdataver;
		mrc->chunkid = chunkid;
		mrc->version = version;
		if (mrc->csdatasize==csdatasize && csdatasize>0) {
			memcpy(mrc->csdata,csdata,mrc->csdatasize);
		} else {
			mrc->csdatasize = csdatasize;
			if (mrc->csdata) {
				free(mrc->csdata);
			}
			if (mrc->csdatasize>0) {
				mrc->csdata = malloc(mrc->csdatasize);
				passert(mrc->csdata);
				memcpy(mrc->csdata,csdata,mrc->csdatasize);
			} else {
				mrc->csdata = NULL;
			}
		}
		mrc->mfleng = mfleng;
	} else {
		mrc->csdataver = 0;
		mrc->chunkid = 0;
		mrc->version = 0;
		mrc->csdata = NULL;
		mrc->csdatasize = 0;
		mrc->mfleng = 0;
	}

	mrc->time = monotonic_seconds();
	mrc->state = MR_READY;
	if (mrc->reqwaiting) {
		zassert(pthread_cond_broadcast(&(mrc->reqcond)));
	}
	mrc->reqwaiting = 0;

	zassert(pthread_mutex_unlock(&mreq_cache_lock));
}

void* read_chunkdata_worker(void *arg) {
	uint32_t z1,z2,z3;
	uint8_t *data;
	uint8_t firsttime = 1;
	worker *w = (worker*)arg;

	for (;;) {
		if (firsttime==0) {
			zassert(pthread_mutex_lock(&glock));
			workers_avail++;
			if (workers_avail > SUSTAIN_WORKERS) {
//				fprintf(stderr,"close worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
				read_chunkdata_close_worker(w);
				zassert(pthread_mutex_unlock(&glock));
				return NULL;
			}
			zassert(pthread_mutex_unlock(&glock));
		}
		firsttime = 0;

		// get next job
		queue_get(jqueue,&z1,&z2,&data,&z3);

		zassert(pthread_mutex_lock(&glock));

		if (data==NULL) {
//			fprintf(stderr,"close worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
			read_chunkdata_close_worker(w);
			zassert(pthread_mutex_unlock(&glock));
			return NULL;
		}

		workers_avail--;
		if (workers_avail==0 && workers_total<MAX_WORKERS) {
			read_chunkdata_spawn_worker();
//			fprintf(stderr,"spawn worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
		}
		zassert(pthread_mutex_unlock(&glock));

		read_chunkdata_refresh((mreqcache*)data);
	}
	return NULL;
}

/* API | glock: INITIALIZED,UNLOCKED */
void read_chunkdata_init (void) {
        uint32_t i;

	zassert(pthread_mutex_init(&mreq_cache_lock,NULL));
	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_cond_init(&worker_term_cond,NULL));
	worker_term_waiting = 0;

	mreq_cache = malloc(sizeof(mreqcache*)*MREQ_HASH_SIZE);
	passert(mreq_cache);
	for (i=0 ; i<MREQ_HASH_SIZE ; i++) {
		mreq_cache[i]=NULL;
	}

	jqueue = queue_new(0);

        zassert(pthread_attr_init(&worker_thattr));
        zassert(pthread_attr_setstacksize(&worker_thattr,0x100000));

	zassert(pthread_mutex_lock(&glock));
	workers_avail = 0;
	workers_total = 0;
	read_chunkdata_spawn_worker();
	zassert(pthread_mutex_unlock(&glock));
}

void read_chunkdata_term(void) {
	uint32_t i;
	mreqcache *mrc,*mrcn;

	queue_close(jqueue);
	zassert(pthread_mutex_lock(&glock));
	while (workers_total>0) {
		worker_term_waiting++;
		zassert(pthread_cond_wait(&worker_term_cond,&glock));
	}
	zassert(pthread_mutex_unlock(&glock));
	queue_delete(jqueue);
	zassert(pthread_mutex_lock(&mreq_cache_lock));
	for (i=0 ; i<MREQ_HASH_SIZE ; i++) {
		for (mrc = mreq_cache[i] ; mrc ; mrc = mrcn) {
			mrcn = mrc->next;
			zassert(pthread_cond_destroy(&(mrc->reqcond)));
			free(mrc->csdata);
			free(mrc);
		}
	}
	free(mreq_cache);
	zassert(pthread_mutex_unlock(&mreq_cache_lock));

	zassert(pthread_attr_destroy(&worker_thattr));
	zassert(pthread_cond_destroy(&worker_term_cond));
        zassert(pthread_mutex_destroy(&glock));
        zassert(pthread_mutex_destroy(&mreq_cache_lock));
}

/* master request cache */

void read_chunkdata_invalidate (uint32_t inode,uint32_t chindx) {
	uint32_t hash;
	mreqcache *mrc;

	zassert(pthread_mutex_lock(&mreq_cache_lock));
	hash = ((inode * 0x4A599F6D + chindx) * 0xB15831CB) % MREQ_HASH_SIZE;
	for (mrc = mreq_cache[hash] ; mrc ; mrc=mrc->next) {
		if (mrc->inode==inode && mrc->chindx==chindx) {
			if (mrc->state==MR_READY) {
				mrc->state = MR_INVALID;
			}
			zassert(pthread_mutex_unlock(&mreq_cache_lock));
			return;
		}
	}
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
}

void read_chunkdata_inject (uint32_t inode,uint32_t chindx,uint64_t mfleng,uint64_t chunkid,uint32_t version,uint8_t csdataver,uint8_t *csdata,uint32_t csdatasize) {
	uint32_t hash;
	mreqcache *mrc;
	double now;

	now = monotonic_seconds();
	zassert(pthread_mutex_lock(&mreq_cache_lock));
	hash = ((inode * 0x4A599F6D + chindx) * 0xB15831CB) % MREQ_HASH_SIZE;
	for (mrc = mreq_cache[hash] ; mrc != NULL ; mrc = mrc->next) {
		if (mrc->inode==inode && mrc->chindx==chindx) { // as for now - just ignore it
			zassert(pthread_mutex_unlock(&mreq_cache_lock));
			return;
		} 
	}
	mrc = malloc(sizeof(mreqcache));
	memset(mrc,0,sizeof(mreqcache));
	mrc->time = now;
	mrc->inode = inode;
	mrc->mfleng = mfleng;
	mrc->chindx = chindx;
	mrc->chunkid = chunkid;
	mrc->version = version;
	mrc->csdataver = csdataver;
	mrc->csdatasize = csdatasize;
	mrc->csdata = malloc(csdatasize);
	memcpy(mrc->csdata,csdata,csdatasize);
	mrc->reqwaiting = 0;
	mrc->status = STATUS_OK;
	mrc->state = MR_READY;
	zassert(pthread_cond_init(&(mrc->reqcond),NULL));
	mrc->next = mreq_cache[hash];
	mreq_cache[hash] = mrc;
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
}

uint8_t read_chunkdata_check(uint32_t inode,uint32_t chindx,uint64_t mfleng,uint64_t chunkid,uint32_t version) { // after read check if chunk doeasn't change, and if so then repeat reading
	uint32_t hash;
	mreqcache *mrc;
	zassert(pthread_mutex_lock(&mreq_cache_lock));
	hash = ((inode * 0x4A599F6D + chindx) * 0xB15831CB) % MREQ_HASH_SIZE;
	for (mrc = mreq_cache[hash] ; mrc != NULL ; mrc = mrc->next) {
		if (mrc->inode==inode && mrc->chindx==chindx) {
			while (mrc->state!=MR_READY && mrc->state!=MR_INVALID) {
				mrc->reqwaiting = 1;
				zassert(pthread_cond_wait(&(mrc->reqcond),&mreq_cache_lock));
			}
			if (mrc->state==MR_INVALID || mrc->status!=STATUS_OK || mrc->mfleng>mfleng || mrc->chunkid!=chunkid || mrc->version!=version) {
				zassert(pthread_mutex_unlock(&mreq_cache_lock));
				return 0;
			} else {
				zassert(pthread_mutex_unlock(&mreq_cache_lock));
				return 1;
			}
		}
	}
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
	// not found !!!
	return 0;
}

uint8_t read_chunkdata_get(uint32_t inode,uint8_t *canmodatime,cspri chain[100],uint16_t *chainelements,uint32_t chindx,uint64_t *mfleng,uint64_t *chunkid,uint32_t *version) {
	uint32_t hash;
	mreqcache *mrc,**mrcp;
	mreqcache **oldestreq;
	double oldestreqtime;
	uint32_t hchainleng;
	double now;

	now = monotonic_seconds();
	zassert(pthread_mutex_lock(&mreq_cache_lock));
	hash = ((inode * 0x4A599F6D + chindx) * 0xB15831CB) % MREQ_HASH_SIZE;
	mrcp = mreq_cache+hash;
	oldestreq = NULL;
	oldestreqtime = now;
	hchainleng = 0;
	while ((mrc = *mrcp)) {
		if (mrc->state==MR_READY && (mrc->time + MREQ_TIMEOUT < now || mrc->status!=STATUS_OK)) {
			*mrcp = mrc->next;
			zassert(pthread_cond_destroy(&(mrc->reqcond)));
			if (mrc->csdata) {
				free(mrc->csdata);
			}
			free(mrc);
		} else if (mrc->inode==inode && mrc->chindx==chindx) {
			if (mrc->state==MR_INIT) { // request in progress - ignore other fields
				while (mrc->state==MR_INIT) {
					mrc->reqwaiting = 1;
					zassert(pthread_cond_wait(&(mrc->reqcond),&mreq_cache_lock));
				}
				if (mrc->status!=STATUS_OK) {
					zassert(pthread_mutex_unlock(&mreq_cache_lock));
					*chunkid = 0;
					*version = 0;
					*chainelements = 0;
					return mrc->status;
				}
				*chunkid = mrc->chunkid;
				*version = mrc->version;
				if (mrc->csdata && mrc->csdatasize>0) {
					*chainelements = csorder_sort(chain,mrc->csdataver,mrc->csdata,mrc->csdatasize,0);
				} else {
					*chainelements = 0;
				}
				zassert(pthread_mutex_unlock(&mreq_cache_lock));
				return STATUS_OK;
			} else if ((mrc->state==MR_READY || mrc->state==MR_REFRESH) && mrc->status==STATUS_OK && mrc->time + MREQ_TIMEOUT >= now) {
				*chunkid = mrc->chunkid;
				*version = mrc->version;
				if (mrc->csdata && mrc->csdatasize>0) {
					*chainelements = csorder_sort(chain,mrc->csdataver,mrc->csdata,mrc->csdatasize,0);
				} else {
					*chainelements = 0;
				}
				if (mrc->state==MR_READY) {
					mrc->state = MR_REFRESH;
					mrc->canmodatime = *canmodatime;
					if (mrc->canmodatime==2) {
						*canmodatime = 1;
					}
					queue_put(jqueue,0,0,(uint8_t*)mrc,0);
				}
				zassert(pthread_mutex_unlock(&mreq_cache_lock));
				return STATUS_OK;
			} else { //refresh data
				break;
			}
		} else {
			if (mrc->state==MR_READY) {
				if (oldestreq==NULL || mrc->time < oldestreqtime) {
					oldestreq = mrcp;
					oldestreqtime = mrc->time;
				}
				hchainleng++;
			}
			mrcp = &(mrc->next);
		}
	}
	if (mrc==NULL) {
		if (hchainleng > MREQ_MAX_CHAIN_LENG && oldestreq!=NULL) { // chain too long - reuse oldest entry
			mrc = *oldestreq;
			if (mrc->csdata) {
				free(mrc->csdata);
			}
		} else {
			mrc = malloc(sizeof(mreqcache));
			memset(mrc,0,sizeof(mreqcache));
			zassert(pthread_cond_init(&(mrc->reqcond),NULL));
			mrc->next = mreq_cache[hash];
			mreq_cache[hash] = mrc;
		}
		mrc->inode = inode;
		mrc->chindx = chindx;
	} else {
		if (mrc->csdata) {
			free(mrc->csdata);
		}
	}
	mrc->csdata = NULL;
	mrc->csdatasize = 0;
	mrc->state = MR_INIT;
	mrc->reqwaiting = 0;

	mrc->canmodatime = *canmodatime;
	if (mrc->canmodatime==2) {
		*canmodatime = 1;
	}
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
	read_chunkdata_refresh(mrc);
	zassert(pthread_mutex_lock(&mreq_cache_lock));
//	while (mrc->state==MR_INIT) {
//		mrc->reqwaiting = 1;
//		zassert(pthread_cond_wait(&(mrc->reqcond),&mreq_cache_lock));
//	}
	if (mrc->status!=STATUS_OK) {
		zassert(pthread_mutex_unlock(&mreq_cache_lock));
		*chunkid = 0;
		*version = 0;
		*chainelements = 0;
		return mrc->status;
	}
	*chunkid = mrc->chunkid;
	*version = mrc->version;
	if (mrc->mfleng > *mfleng) {
		*mfleng = mrc->mfleng;
	}
	if (mrc->csdata && mrc->csdatasize>0) {
		*chainelements = csorder_sort(chain,mrc->csdataver,mrc->csdata,mrc->csdatasize,0);
	} else {
		*chainelements = 0;
	}
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
	return STATUS_OK;
}
