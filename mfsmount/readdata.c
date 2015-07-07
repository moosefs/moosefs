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

#include <sys/types.h>
#include <stdio.h>
#ifdef HAVE_READV
#include <sys/uio.h>
#endif
#include <sys/time.h>
#include <unistd.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <pthread.h>
#include <inttypes.h>

#include "massert.h"
#include "datapack.h"
#include "crc.h"
#include "strerr.h"
#include "mfsstrerr.h"
#include "pcqueue.h"
#include "sockets.h"
#include "conncache.h"
#include "csorder.h"
#include "csdb.h"
#include "delayrun.h"
#include "mastercomm.h"
#include "clocks.h"
#include "portable.h"
#include "readdata.h"
#include "MFSCommunication.h"

#define CHUNKSERVER_ACTIVITY_TIMEOUT 2.0

#define WORKER_IDLE_TIMEOUT 1.0

#define WORKER_BUSY_LAST_REQUEST_TIMEOUT 5.0
#define WORKER_BUSY_WAIT_FOR_FINISH 5.0
#define WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT 20.0

#define BUFFER_VALIDITY_TIMEOUT 60.0

#define SUSTAIN_WORKERS 50
#define HEAVYLOAD_WORKERS 150
#define MAX_WORKERS 250

#define IDHASHSIZE 256
#define IDHASH(inode) (((inode)*0xB239FB71)%IDHASHSIZE)

#define READAHEAD_MAX 4

/*
typedef struct cblock_s {
	uint8_t data[MFSBLOCKSIZE];
	uint32_t chindx;	// chunk number
	uint16_t pos;		// block in chunk (0..1023)
	uint8_t filled;		// data present
	uint8_t wakeup;		// somebody wants to be woken up when data are present
	struct readbuffer *next;
} cblock;
*/

// #define RDEBUG 1

#define MAXREQINQUEUE 16

#define MREQ_CACHE_SIZE 256

#define MREQ_TIMEOUT 1.0

enum {NEW,INQUEUE,BUSY,REFRESH,BREAK,FILLED,READY,FREE};

enum {MR_INIT,MR_READY,MR_INVALID};

#ifdef RDEBUG
char* read_data_modename(uint8_t mode) {
	switch (mode) {
	case NEW:
		return "NEW";
	case INQUEUE:
		return "INQUEUE";
	case BUSY:
		return "BUSY";
	case REFRESH:
		return "REFRESH";
	case BREAK:
		return "BREAK";
	case FILLED:
		return "FILLED";
	case READY:
		return "READY";
	case FREE:
		return "FREE";
	}
	return "<unknown>";
}

void read_data_hexdump(uint8_t *buff,uint32_t leng) {
	uint32_t i;
	for (i=0 ; i<leng ; i++) {
		if ((i%32)==0) {
			fprintf(stderr,"%p %08X:",(void*)(buff+i),i);
		}
		fprintf(stderr," %02X",buff[i]);
		if ((i%32)==31) {
			fprintf(stderr,"\n");
		}
	}
	if ((leng%32)!=0) {
		fprintf(stderr,"\n");
	}
}
#endif

struct inodedata_s;

typedef struct rrequest_s {
	struct inodedata_s *ind;
	int pipe[2];
	uint8_t waitingworker;
	uint8_t *data;
	uint64_t offset;
	uint32_t leng;
	uint32_t rleng;
	uint32_t currentpos;
	uint32_t chindx;
	double modified;
	uint8_t refresh;
	uint8_t mode;
	uint16_t lcnt;
	uint16_t waiting;
	pthread_cond_t cond;
	struct rrequest_s *next,**prev;
} rrequest;

typedef struct inodedata_s {
	uint32_t inode;
	uint32_t seqdata;
	uint64_t fleng;
	int status;
	uint16_t closewaiting;
	uint32_t trycnt;
	uint8_t closing;
	uint8_t flengisvalid;
	uint8_t inqueue;
	uint8_t canmodatime;
	uint8_t readahead;
	uint64_t lastoffset;
//	double mreq_time;
//	uint32_t mreq_chindx;
//	uint64_t mreq_chunkid;
//	uint32_t mreq_version;
//	uint8_t mreq_csdataver;
//	uint32_t mreq_csdatasize;
//	uint8_t *mreq_csdata;
//	uint32_t mreq_csdatabuffsize;
//	uint8_t laststatus;
	rrequest *reqhead,**reqtail;
	pthread_cond_t closecond;
	pthread_mutex_t lock;
	struct inodedata_s *next;
} inodedata;

typedef struct mreqcache_s {
	double time;
	uint32_t inode;
	uint32_t chindx;
	uint64_t chunkid;
	uint32_t version;
	uint8_t csdataver;
	uint32_t csdatasize;
	uint8_t *csdata;
//	uint32_t csdatabuffsize;
	uint8_t reqwaiting;
	uint8_t status;
	uint8_t state;
	struct mreqcache_s *next;
	pthread_cond_t reqcond;
} mreqcache;

typedef struct worker_s {
	pthread_t thread_id;
} worker;

//static pthread_cond_t fcbcond;
//static uint8_t fcbwaiting;
//static cblock *cacheblocks,*freecblockshead;
//static uint32_t freecacheblocks;

static uint32_t readahead;
static uint32_t readahead_trigger;

static uint32_t maxretries;
static uint64_t maxreadaheadsize;

static uint64_t reqbufftotalsize;
#ifndef HAVE___SYNC_OP_AND_FETCH
static pthread_mutex_t buffsizelock;
#endif

static mreqcache **mreq_cache;
static pthread_mutex_t mreq_cache_lock;

static inodedata **indhash;

static pthread_mutex_t glock;

//#ifdef BUFFER_DEBUG
//static pthread_t info_worker_th;
//static uint32_t usedblocks;
//#endif

//static pthread_t dqueue_worker_th;

static uint32_t workers_avail;
static uint32_t workers_total;
static uint32_t worker_term_waiting;
static pthread_cond_t worker_term_cond;
static pthread_attr_t worker_thattr;

// static pthread_t read_worker_th[WORKERS];
//static inodedata *read_worker_id[WORKERS];

static void *jqueue; //,*dqueue;

/* master request cache */

static inline void read_invalidate_masterdata(uint32_t inode,uint32_t chindx) {
	uint32_t hash;
	mreqcache *mrc;

	zassert(pthread_mutex_lock(&mreq_cache_lock));
	hash = ((inode * 0x4A599F6D + chindx) * 0xB15831CB) % MREQ_CACHE_SIZE;
	for (mrc = mreq_cache[hash] ; mrc ; mrc=mrc->next) {
		if (mrc->inode==inode && mrc->chindx==chindx) {
			if (mrc->state==MR_READY) {
				mrc->state=MR_INVALID;
			}
			zassert(pthread_mutex_unlock(&mreq_cache_lock));
			return;
		}
	}
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
}

static inline uint8_t read_get_masterdata(inodedata *ind,cspri chain[100],uint16_t *chainelements,uint32_t chindx,uint64_t *mfleng,uint64_t *chunkid,uint32_t *version) {
	uint32_t hash;
	mreqcache *mrc,**mrcp;
	const uint8_t *csdata;
	uint8_t canmodatime;
	uint8_t flengisvalid;
	uint32_t inode;
	double now;

//	zassert(pthread_mutex_lock(&(ind->lock)));
	*mfleng = ind->fleng;
	inode = ind->inode;
	flengisvalid = ind->flengisvalid;
	zassert(pthread_mutex_unlock(&(ind->lock)));

	now = monotonic_seconds();
	zassert(pthread_mutex_lock(&mreq_cache_lock));
	hash = ((inode * 0x4A599F6D + chindx) * 0xB15831CB) % MREQ_CACHE_SIZE;
	mrcp = mreq_cache+hash;
	while ((mrc = *mrcp)) {
		if (mrc->inode==inode && mrc->chindx==chindx) {
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
			} else if (mrc->state==MR_READY && mrc->status==STATUS_OK && flengisvalid) {
				*chunkid = mrc->chunkid;
				*version = mrc->version;
				if (mrc->csdata && mrc->csdatasize>0) {
					*chainelements = csorder_sort(chain,mrc->csdataver,mrc->csdata,mrc->csdatasize,0);
				} else {
					*chainelements = 0;
				}
				zassert(pthread_mutex_unlock(&mreq_cache_lock));
				return STATUS_OK;
			} else { //refresh data
				break;
			}
		} else if (mrc->state==MR_READY && (mrc->time + MREQ_TIMEOUT < now || mrc->status!=STATUS_OK)) {
			*mrcp = mrc->next;
			if (mrc->csdata) {
				free(mrc->csdata);
			}
			free(mrc);
		} else {
			mrcp = &(mrc->next);
		}
	}
	if (mrc==NULL) {
		mrc = malloc(sizeof(mreqcache));
		memset(mrc,0,sizeof(mreqcache));
		mrc->inode = inode;
		mrc->chindx = chindx;
		mrc->state = MR_INIT;
		mrc->reqwaiting = 0;
		zassert(pthread_cond_init(&(mrc->reqcond),NULL));
		mrc->next = mreq_cache[hash];
		mreq_cache[hash] = mrc;
	} else {
		if (mrc->csdata) {
			free(mrc->csdata);
		}
		mrc->csdata = NULL;
		mrc->csdatasize = 0;
		mrc->state = MR_INIT;
		mrc->reqwaiting = 0;
	}
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
	zassert(pthread_mutex_lock(&(ind->lock)));
	canmodatime = ind->canmodatime;
	if (canmodatime==2) {
		ind->canmodatime = 1;
	}
	zassert(pthread_mutex_unlock(&(ind->lock)));
	mrc->status = fs_readchunk(inode,chindx,canmodatime,&(mrc->csdataver),mfleng,&(mrc->chunkid),&(mrc->version),&csdata,&(mrc->csdatasize));
	if (mrc->status==STATUS_OK) {
		if (mrc->csdatasize>0) {
			mrc->csdata = malloc(mrc->csdatasize);
			passert(mrc->csdata);
			memcpy(mrc->csdata,csdata,mrc->csdatasize);
		} else {
			mrc->csdata = NULL;
		}
		zassert(pthread_mutex_lock(&(ind->lock)));
		ind->fleng = *mfleng;
		ind->flengisvalid = 1;
//		ind->laststatus = 1;
		zassert(pthread_mutex_unlock(&(ind->lock)));
	} else {
		mrc->csdata = NULL;
		mrc->csdatasize = 0;
	}
	zassert(pthread_mutex_lock(&mreq_cache_lock));
	mrc->time = monotonic_seconds();
	mrc->state = MR_READY;
	if (mrc->reqwaiting) {
		zassert(pthread_cond_broadcast(&(mrc->reqcond)));
	}
	mrc->reqwaiting = 0;
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
}

/* queues */

void read_enqueue(rrequest *rreq) {
	queue_put(jqueue,0,0,(uint8_t*)rreq,0);
}

void read_delayrun_enqueue(void *udata) {
	queue_put(jqueue,0,0,(uint8_t*)udata,0);
}

void read_delayed_enqueue(rrequest *rreq,uint32_t usecs) {
	if (usecs>0) {
		delay_run(read_delayrun_enqueue,rreq,usecs);
	} else {
		queue_put(jqueue,0,0,(uint8_t*)rreq,0);
	}
}

/* worker thread | glock: UNUSED
void* read_dqueue_worker(void *arg) {
	uint64_t t,usec;
	uint32_t husec,lusec,cnt;
	uint8_t *ind;
	(void)arg;
	for (;;) {
		queue_get(dqueue,&husec,&lusec,&ind,&cnt);
		if (ind==NULL) {
			return NULL;
		}
		t = monotonic_useconds();
		usec = husec;
		usec <<= 32;
		usec |= lusec;
		if (t>usec) {
			t -= usec;
			while (t>=1000000 && cnt>0) {
				t-=1000000;
				cnt--;
			}
			if (cnt>0) {
				if (t<1000000) {
					portable_usleep(1000000-t);
				}
				cnt--;
			}
		}
		if (cnt>0) {
			t = monotonic_useconds();
			queue_put(dqueue,t>>32,t&0xFFFFFFFFU,(uint8_t*)ind,cnt);
		} else {
			queue_put(jqueue,0,0,ind,0);
		}
	}
	return NULL;
}
*/

// void read_job_end(inodedata *ind,int status,uint32_t delay) {
void read_job_end(rrequest *rreq,int status,uint32_t delay) {
	inodedata *ind;
	uint8_t breakmode;
#ifdef RDEBUG
	uint64_t rbuffsize;
#endif

	ind = rreq->ind;
	zassert(pthread_mutex_lock(&(ind->lock)));
	breakmode = 0;
	if (rreq->mode==FILLED) {
		rreq->mode = READY;
		ind->trycnt = 0;
	} else {
		if (rreq->mode==BREAK) {
			breakmode = 1;
			rreq->mode = FREE;
		} else { // REFRESH
			rreq->mode = NEW;
		}
	}
	ind->inqueue--;
	if (status) {
		if (ind->closing==0) {
			errno = status;
			syslog(LOG_WARNING,"error reading file number %"PRIu32": %s",ind->inode,strerr(errno));
		}
		ind->status = status;
	}
	status = ind->status;

	if (ind->closing || status!=STATUS_OK || breakmode) {
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: readworker end (rreq: %"PRIu64":%"PRIu32") inode: %"PRIu32" - closing: %u ; status: %u ; breakmode: %u\n",monotonic_seconds(),rreq->offset,rreq->leng,ind->inode,ind->closing,status,breakmode);
#endif
		if (rreq->lcnt==0) {
			*(rreq->prev) = rreq->next;
			if (rreq->next) {
				rreq->next->prev = rreq->prev;
			} else {
				ind->reqtail = rreq->prev;
			}
#ifdef HAVE___SYNC_OP_AND_FETCH
#ifdef RDEBUG
			rbuffsize = __sync_sub_and_fetch(&reqbufftotalsize,rreq->leng);
#else
			__sync_sub_and_fetch(&reqbufftotalsize,rreq->leng);
#endif
#else
			zassert(pthread_mutex_lock(&buffsizelock));
#ifdef RDEBUG
			rbuffsize = (reqbufftotalsize -= rreq->leng);
#else
			reqbufftotalsize -= rreq->leng;
#endif
			zassert(pthread_mutex_unlock(&buffsizelock));
#endif
			close(rreq->pipe[0]);
			close(rreq->pipe[1]);
			free(rreq->data);
			free(rreq);
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: inode: %"PRIu32" - reqhead: %s (reqbufftotalsize: %"PRIu64")\n",monotonic_seconds(),ind->inode,ind->reqhead?"NOT NULL":"NULL",rbuffsize);
#endif

			if (ind->closewaiting && ind->reqhead==NULL) {
				zassert(pthread_cond_broadcast(&(ind->closecond)));
			}
		} else {
			if (breakmode==0 && rreq->mode!=READY) {
				rreq->rleng = 0;
				rreq->mode = READY;
				if (rreq->waiting) {
					zassert(pthread_cond_broadcast(&(rreq->cond)));
				}
			}
		}
	} else {
		for (rreq = ind->reqhead ; rreq && ind->inqueue < MAXREQINQUEUE ; rreq=rreq->next) {
			if (rreq->mode==NEW) {
				rreq->mode = INQUEUE;
		                read_delayed_enqueue(rreq,delay);
				ind->inqueue++;
			}
		}
        }
        zassert(pthread_mutex_unlock(&(ind->lock)));
}

void* read_worker(void *arg);

#ifndef RDEBUG
static uint32_t lastnotify = 0;
#endif

/* glock:LOCKED */
static inline void read_data_spawn_worker(void) {
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
	res = pthread_create(&(w->thread_id),&worker_thattr,read_worker,w);
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
	if (res<0) {
		return;
	}
	workers_avail++;
	workers_total++;
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: spawn read worker (total: %"PRIu32")\n",monotonic_seconds(),workers_total);
#else
	if (workers_total%10==0 && workers_total!=lastnotify) {
		syslog(LOG_INFO,"read workers: %"PRIu32"+",workers_total);
		lastnotify = workers_total;
	}
#endif
}

/* glock:LOCKED */
static inline void read_data_close_worker(worker *w) {
	workers_avail--;
	workers_total--;
	if (workers_total==0 && worker_term_waiting) {
		zassert(pthread_cond_signal(&worker_term_cond));
		worker_term_waiting--;
	}
	pthread_detach(w->thread_id);
	free(w);
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: close read worker (total: %"PRIu32")\n",monotonic_seconds(),workers_total);
#else
	if (workers_total%10==0 && workers_total!=lastnotify) {
		syslog(LOG_INFO,"read workers: %"PRIu32"-",workers_total);
		lastnotify = workers_total;
	}
#endif
}

static inline void read_prepare_ip (char ipstr[16],uint32_t ip) {
	if (ipstr[0]==0) {
		snprintf(ipstr,16,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,(uint8_t)(ip>>24),(uint8_t)(ip>>16),(uint8_t)(ip>>8),(uint8_t)ip);
		ipstr[15]=0;
	}
}

void* read_worker(void *arg) {
	uint32_t z1,z2,z3;
	uint8_t *data;
	int fd;
	int i;
	struct pollfd pfd[3];
	uint32_t sent,tosend,received,currentpos;
	uint8_t recvbuff[20];
	uint8_t sendbuff[29];
#ifdef HAVE_READV
	struct iovec siov[2];
#endif
	uint8_t pipebuff[1024];
	uint8_t *wptr;
	const uint8_t *rptr;

	uint32_t inode;
	uint32_t trycnt;
	uint32_t rleng;

	uint32_t reccmd;
	uint32_t recleng;
	uint64_t recchunkid;
	uint16_t recblocknum;
	uint16_t recoffset;
	uint32_t recsize;
	uint32_t reccrc;
	uint8_t recstatus;
	uint8_t gotstatus;

	cspri chain[100];
	uint16_t chainelements;

	uint32_t chindx;
	uint32_t ip;
	uint16_t port;
	uint32_t srcip;
	uint64_t mfleng;
	uint64_t chunkid;
	uint32_t version;
//	const uint8_t *csdata;
	uint32_t csver;
	uint32_t cnt;
//	uint32_t csdatasize;
//	uint8_t csdataver;
	uint8_t rdstatus;
//	uint8_t canmodatime;
	int status;
	char csstrip[16];
	uint8_t reqsend;
	uint8_t closing;
	uint8_t mode;
	double start,now,lastrcvd,lastsend;
	double workingtime,lrdiff;
	double timeoutadd;
	uint8_t firsttime = 1;
	worker *w = (worker*)arg;

	inodedata *ind;
	rrequest *rreq;

	ip = 0;
	port = 0;
	csstrip[0] = 0;

	for (;;) {
		if (ip || port) {
			csdb_readdec(ip,port);
		}
		ip = 0;
		port = 0;
		csstrip[0] = 0;

		if (firsttime==0) {
			zassert(pthread_mutex_lock(&glock));
			workers_avail++;
			if (workers_avail > SUSTAIN_WORKERS) {
//				fprintf(stderr,"close worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
				read_data_close_worker(w);
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
			read_data_close_worker(w);
			zassert(pthread_mutex_unlock(&glock));
			return NULL;
		}

		workers_avail--;
		if (workers_avail==0 && workers_total<MAX_WORKERS) {
			read_data_spawn_worker();
//			fprintf(stderr,"spawn worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
		}
		timeoutadd = (workers_total>HEAVYLOAD_WORKERS)?0.0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT;
		zassert(pthread_mutex_unlock(&glock));

		rreq = (rrequest*)data;
		ind = rreq->ind;

		zassert(pthread_mutex_lock(&(ind->lock)));
		rreq->mode = BUSY;

		chindx = rreq->chindx;
		status = ind->status;
		inode = ind->inode;
		rleng = rreq->leng;
		trycnt = ind->trycnt;

		if (status!=STATUS_OK) {
			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,status,0);
			continue;
		}
		if (ind->closing) {
			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,0,0);
			continue;
		}

		rdstatus = read_get_masterdata(ind,chain,&chainelements,chindx,&mfleng,&chunkid,&version); // unlocks (ind->lock)

#if 0
		now = monotonic_seconds();
		if (ind->mreq_time + MREQ_TIMEOUT > now && chindx == ind->mreq_chindx && ind->laststatus!=0 && ind->flengisvalid) {
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu32") inode: %"PRIu32" ; indx: %"PRIu32" (use chunk data cache)\n",monotonic_seconds(),rreq->offset,rreq->leng,inode,chindx);
#endif
			csdataver = ind->mreq_csdataver;
			chunkid = ind->mreq_chunkid;
			version = ind->mreq_version;
			csdata = ind->mreq_csdata;
			csdatasize = ind->mreq_csdatasize;
			mfleng = ind->fleng;
			rdstatus = STATUS_OK;
			canmodatime = 1;
		} else {
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu32") inode: %"PRIu32" ; indx: %"PRIu32" (get chunk data from master) mreq_time: %.6lf ; mreq_chindx: %"PRIu32" ; laststatus: %u ; flengisvalid: %u\n",monotonic_seconds(),rreq->offset,rreq->leng,inode,chindx,ind->mreq_time,ind->mreq_chindx,ind->laststatus,ind->flengisvalid);
#endif
			canmodatime = ind->canmodatime;
			if (canmodatime==2) {
				ind->canmodatime = 1;
			}
			rdstatus = ERROR_ENOENT; // any error - means do master request
			ind->laststatus = 0;
		}

		zassert(pthread_mutex_unlock(&(ind->lock)));
//		start = monotonic_seconds();

		// get chunk data from master
		if (rdstatus!=STATUS_OK) {
			rdstatus = fs_readchunk(inode,chindx,canmodatime,&csdataver,&mfleng,&chunkid,&version,&csdata,&csdatasize);
			if (rdstatus==STATUS_OK) {
				zassert(pthread_mutex_lock(&(ind->lock)));
				ind->mreq_time = now;
				ind->mreq_chindx = chindx;
				ind->mreq_chunkid = chunkid;
				ind->mreq_version = version;
				ind->mreq_csdataver = csdataver;
				ind->mreq_csdatasize = csdatasize;
				if (csdatasize > ind->mreq_csdatabuffsize) {
					free(ind->mreq_csdata);
					ind->mreq_csdata = malloc(csdatasize+100);
					passert(ind->mreq_csdata);
					ind->mreq_csdatabuffsize = csdatasize+100;
				}
				memcpy(ind->mreq_csdata,csdata,csdatasize);
				ind->laststatus = 1;
				zassert(pthread_mutex_unlock(&(ind->lock)));
			}
		}
#endif
		if (rdstatus!=STATUS_OK) {
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
			if (rdstatus==ERROR_ENOENT) {
				read_job_end(rreq,EBADF,0);
			} else if (rdstatus==ERROR_QUOTA) {
				read_job_end(rreq,EDQUOT,0);
			} else if (rdstatus==ERROR_NOSPACE) {
				read_job_end(rreq,ENOSPC,0);
			} else if (rdstatus==ERROR_CHUNKLOST) {
				read_job_end(rreq,ENXIO,0);
			} else {
				zassert(pthread_mutex_lock(&(ind->lock)));
				ind->trycnt++;
				trycnt = ind->trycnt;
				if (trycnt>=maxretries) {
					zassert(pthread_mutex_unlock(&(ind->lock)));
					if (rdstatus==ERROR_NOCHUNKSERVERS) {
						read_job_end(rreq,ENOSPC,0);
					} else if (rdstatus==ERROR_CSNOTPRESENT) {
						read_job_end(rreq,ENXIO,0);
					} else {
						read_job_end(rreq,EIO,0);
					}
				} else {
					rreq->mode = INQUEUE;
					zassert(pthread_mutex_unlock(&(ind->lock)));
					read_delayed_enqueue(rreq,10000+((trycnt<30)?((trycnt-1)*300000):10000000));
				}
			}
			continue;	// get next job
		}

//		now = monotonic_seconds();
//		fprintf(stderr,"fs_readchunk time: %.3lf\n",now-start);
		if (chunkid==0 && version==0) { // empty chunk
			zassert(pthread_mutex_lock(&(ind->lock)));
//			ind->fleng = mfleng;
//			ind->flengisvalid = 1;
			rreq->mode = FILLED;
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu32") inode: %"PRIu32" ; mfleng: %"PRIu64" (empty chunk)\n",monotonic_seconds(),rreq->offset,rreq->leng,inode,ind->fleng);
#endif
			if (rreq->offset > mfleng) {
				rreq->rleng = 0;
			} else if ((rreq->offset + rreq->leng) > mfleng) {
				rreq->rleng = mfleng - rreq->offset;
			} else {
				rreq->rleng = rreq->leng;
			}

			if (rreq->rleng>0) {
				memset(rreq->data,0,rreq->rleng);
			}
			rreq->modified = monotonic_seconds();
			if (rreq->waiting>0) {
				zassert(pthread_cond_broadcast(&(rreq->cond)));
			}

			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,0,0);

			continue;
		}

#if 0
		if (csdata!=NULL && csdatasize>0) {
			zassert(pthread_mutex_lock(&(ind->lock))); // csdata may point to ind->mreq_csdata
			chainelements = csorder_sort(chain,csdataver,csdata,csdatasize,0);
			zassert(pthread_mutex_unlock(&(ind->lock)));
		} else {
			chainelements = 0;
		}
#endif
		if (/*csdata==NULL || csdatasize==0 || */chainelements==0) {
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies",inode,chindx,chunkid,version);
			zassert(pthread_mutex_lock(&(ind->lock)));
			ind->trycnt+=6;
			if (ind->trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,ENXIO,0);
			} else {
				rreq->mode = INQUEUE;
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_delayed_enqueue(rreq,60000000);
			}
			continue;
		}

		ip = chain[0].ip;
		port = chain[0].port;
		csver = chain[0].version;
/*
		if (ind->lastchunkid==chunkid) {
			if (ind->laststatus==0) { // error occured
				for (i=0 ; i<chainelements ; i++) {
					if (chain[i].ip != ind->lastip || chain[i].port != ind->lastport) {
						ip = chain[i].ip;
						port = chain[i].port;
						csver = chain[i].version;
						break;
					}
				}
			} else { // ok
				for (i=1 ; i<chainelements ; i++) {
					if (chain[i].ip == ind->lastip && chain[i].port == ind->lastport) {
						ip = chain[i].ip;
						port = chain[i].port;
						csver = chain[i].version;
						break;
					}
				}
			}
		}
*/
		if (ip || port) {
			csdb_readinc(ip,port);
//			ind->lastchunkid = chunkid;
//			ind->lastip = ip;
//			ind->lastport = port;
		} else {
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies (bad ip and/or port)",inode,chindx,chunkid,version);
			zassert(pthread_mutex_lock(&(ind->lock)));
			ind->trycnt+=6;
			if (ind->trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,ENXIO,0);
			} else {
				rreq->mode = INQUEUE;
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_invalidate_masterdata(inode,chindx);
				read_delayed_enqueue(rreq,60000000);
			}
			continue;
		}

		start = monotonic_seconds();


		// make connection to cs
		srcip = fs_getsrcip();
		fd = conncache_get(ip,port);
		if (fd<0) {
			cnt=0;
			while (cnt<10) {
				fd = tcpsocket();
				if (fd<0) {
					syslog(LOG_WARNING,"readworker: can't create tcp socket: %s",strerr(errno));
					break;
				}
				if (srcip) {
					if (tcpnumbind(fd,srcip,0)<0) {
						syslog(LOG_WARNING,"readworker: can't bind socket to given ip: %s",strerr(errno));
						tcpclose(fd);
						fd=-1;
						break;
					}
				}
				if (tcpnumtoconnect(fd,ip,port,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
					cnt++;
					if (cnt>=10) {
						int err = errno;
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"readworker: can't connect to (%s:%"PRIu16"): %s",csstrip,port,strerr(err));
					}
					close(fd);
					fd=-1;
				} else {
					cnt=10;
				}
			}
		}
		if (fd<0) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			ind->trycnt++;
			trycnt = ind->trycnt;
			if (trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,EIO,0);
			} else {
				rreq->mode = INQUEUE;
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_invalidate_masterdata(inode,chindx);
				read_delayed_enqueue(rreq,10000+((trycnt<30)?((trycnt-1)*300000):10000000));
			}
			continue;
		}
		if (tcpnodelay(fd)<0) {
			syslog(LOG_WARNING,"readworker: can't set TCP_NODELAY: %s",strerr(errno));
		}

		pfd[0].fd = fd;
		pfd[1].fd = rreq->pipe[0];
		gotstatus = 0;
		received = 0;
		reqsend = 0;
		sent = 0;
		tosend = 0;
		lastrcvd = 0.0;
		lastsend = 0.0;

		zassert(pthread_mutex_lock(&(ind->lock)));

		currentpos = rreq->currentpos;
#ifdef RDEBUG
		if (currentpos!=0) {
			fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu32" ; start position: %"PRIu32"\n",monotonic_seconds(),inode,rreq->offset,rreq->leng,currentpos);
		}
#endif

		ind->fleng = mfleng;
		ind->flengisvalid = 1;
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; mfleng: %"PRIu64"\n",monotonic_seconds(),inode,ind->fleng);
#endif
		zassert(pthread_mutex_unlock(&(ind->lock)));

		reccmd = 0; // makes gcc happy
		recleng = 0; // makes gcc happy

		do {
			now = monotonic_seconds();

			zassert(pthread_mutex_lock(&(ind->lock)));

#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu32" ; currentpos: %"PRIu32"\n",monotonic_seconds(),inode,rreq->offset,rreq->leng,currentpos);
#endif


			if (ind->flengisvalid) {
				mfleng = ind->fleng;
			}

			if (reqsend && gotstatus) {
				rreq->mode = FILLED;
				rreq->modified = monotonic_seconds();
				if (rreq->waiting>0) {
					zassert(pthread_cond_broadcast(&(rreq->cond)));
				}
				zassert(pthread_mutex_unlock(&(ind->lock)));
				break;
			}

			if (lastrcvd==0.0) {
				lastrcvd = now;
			} else {
				lrdiff = now - lastrcvd;
				if (lrdiff>=CHUNKSERVER_ACTIVITY_TIMEOUT) {
					read_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was timed out (lastrcvd:%.6lf,now:%.6lf,lrdiff:%.6lf received: %"PRIu32"/%"PRIu32", try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,lastrcvd,now,lrdiff,currentpos,(rreq?rreq->rleng:0),trycnt+1);
					if (rreq) {
						status = EIO;
					}
					zassert(pthread_mutex_unlock(&(ind->lock)));
					break;
				}
			}

			workingtime = now - start;

			if (workingtime>(WORKER_BUSY_LAST_REQUEST_TIMEOUT+WORKER_BUSY_WAIT_FOR_FINISH+timeoutadd)) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker: current request not finished but busy timeout passed\n",monotonic_seconds());
#endif
				zassert(pthread_mutex_unlock(&(ind->lock)));
				status = EINTR;
				break;
			}

			if (reqsend==0) {
				if (rreq->offset > mfleng) {
					rreq->rleng = 0;
				} else if ((rreq->offset + rreq->leng) > mfleng) {
					rreq->rleng = mfleng - rreq->offset;
				} else {
					rreq->rleng = rreq->leng;
				}
				rleng = rreq->rleng;
				if (rreq->rleng>0) {
					wptr = sendbuff;
					put32bit(&wptr,CLTOCS_READ);
					if (csver>=VERSION2INT(1,7,32)) {
						put32bit(&wptr,21);
						put8bit(&wptr,1);
						tosend = 29;
					} else {
						put32bit(&wptr,20);
						tosend = 28;
					}
					put64bit(&wptr,chunkid);
					put32bit(&wptr,version);
					put32bit(&wptr,(rreq->offset+currentpos) & MFSCHUNKMASK);
					put32bit(&wptr,rreq->rleng-currentpos);
					sent = 0;
					reqsend = 1;
				} else {
					tosend = 0;
					sent = 0;
					reqsend = 1;
					gotstatus = 1;
					zassert(pthread_mutex_unlock(&(ind->lock)));
					continue;
				}
			}

			rreq->waitingworker=1;
			zassert(pthread_mutex_unlock(&(ind->lock)));

			if (tosend==0 && (now - lastsend > (CHUNKSERVER_ACTIVITY_TIMEOUT/2.0))) {
				wptr = sendbuff;
				put32bit(&wptr,ANTOAN_NOP);
				put32bit(&wptr,0);
				tosend = 8;
				sent = 0;
			}

			if (tosend>0) {
				i = write(fd,sendbuff+sent,tosend-sent);
				if (i<0) { // error
					if (ERRNO_ERROR && errno!=EINTR) {
						int err = errno;
						read_prepare_ip(csstrip,ip);
						status = EIO;
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: write to (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(err),currentpos,rleng,trycnt+1);
						zassert(pthread_mutex_lock(&(ind->lock)));
						rreq->waitingworker=0;
						zassert(pthread_mutex_unlock(&(ind->lock)));
						break;
					} else {
						i=0;
					}
				}
				if (i>0) {
					sent += i;
					if (tosend<=sent) {
						sent = 0;
						tosend = 0;
					}
					lastsend = now;
				}
			}

			pfd[0].events = POLLIN | ((tosend>0)?POLLOUT:0);
			pfd[0].revents = 0;
			pfd[1].events = POLLIN;
			pfd[1].revents = 0;
			if (poll(pfd,2,100)<0) {
				if (errno!=EINTR) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: poll error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,strerr(errno),currentpos,rleng,trycnt+1);
					status = EIO;
					break;
				}
			}
			zassert(pthread_mutex_lock(&(ind->lock)));
			rreq->waitingworker=0;
			closing = (ind->closing>0)?1:0;
			mode = rreq->mode;
			zassert(pthread_mutex_unlock(&(ind->lock)));
			if (pfd[1].revents&POLLIN) {    // used just to break poll - so just read all data from pipe to empty it
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker: %"PRIu32" woken up by pipe\n",monotonic_seconds(),inode);
#endif
				i = read(rreq->pipe[0],pipebuff,1024);
				if (i<0) { // mainly to make happy static code analyzers
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: read pipe error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,strerr(errno),currentpos,rleng,trycnt+1);
				}
			}
			if (mode!=BUSY) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker: mode=%s\n",monotonic_seconds(),read_data_modename(mode));
#endif
				status = EINTR;
				currentpos = 0;
				break;
			}
			if (closing) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker: closing\n",monotonic_seconds());
#endif
				status = EINTR;
				currentpos = 0;
				break;
			}
			if (pfd[0].revents&POLLHUP) {
				read_prepare_ip(csstrip,ip);
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / POLLHUP (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
				status = EIO;
				break;
			}
			if (pfd[0].revents&POLLERR) {
				read_prepare_ip(csstrip,ip);
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") got error status / POLLERR (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
				status = EIO;
				break;
			}
			if (pfd[0].revents&POLLIN) {
				lastrcvd = monotonic_seconds();
				if (received < 8) {
					i = read(fd,recvbuff+received,8-received);
					if (i==0) {
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / ZEROREAD (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
						status = EIO;
						break;
					}
					if (i<0) {
						int err = errno;
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: read from (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(err),currentpos,rleng,trycnt+1);
						status = EIO;
						break;
					}
					received += i;
					if (received == 8) { // full header
						rptr = recvbuff;

					        reccmd = get32bit(&rptr);
						recleng = get32bit(&rptr);
						if (reccmd==CSTOCL_READ_STATUS) {
							if (recleng!=9) {
								syslog(LOG_WARNING,"readworker: got wrong sized status packet from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
						} else if (reccmd==CSTOCL_READ_DATA) {
							if (rreq==NULL) {
								syslog(LOG_WARNING,"readworker: got unexpected data from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							} else if (recleng<20) {
								syslog(LOG_WARNING,"readworker: got too short data packet from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							} else if ((recleng-20) + currentpos > rleng) {
								syslog(LOG_WARNING,"readworker: got too long data packet from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
						} else if (reccmd==ANTOAN_NOP) {
							if (recleng!=0) {
								syslog(LOG_WARNING,"readworker: got wrong sized nop packet from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							received = 0;
						} else {
							uint32_t myip,peerip;
							uint16_t myport,peerport;
							tcpgetpeer(fd,&peerip,&peerport);
							tcpgetmyaddr(fd,&myip,&myport);
							syslog(LOG_WARNING,"readworker: got unrecognized packet from chunkserver (cmd:%"PRIu32",leng:%"PRIu32",%u.%u.%u.%u:%u<->%u.%u.%u.%u:%u)",reccmd,recleng,(myip>>24)&0xFF,(myip>>16)&0xFF,(myip>>8)&0xFF,myip&0xFF,myport,(peerip>>24)&0xFF,(peerip>>16)&0xFF,(peerip>>8)&0xFF,peerip&0xFF,peerport);
							status = EIO;
							currentpos = 0; // start again from beginning
							break;
						}
					}
				}
				if (received >= 8) {
					if (recleng<=20) {
						i = read(fd,recvbuff + (received-8),recleng - (received-8));
					} else {
						if (received < 8 + 20) {
#ifdef HAVE_READV
							siov[0].iov_base = recvbuff + (received-8);
							siov[0].iov_len = 20 - (received-8);
							siov[1].iov_base = rreq->data + currentpos;
							siov[1].iov_len = recleng - 20;
							i = readv(fd,siov,2);
#else
							i = read(fd,recvbuff + (received-8),20 - (received-8));
#endif
						} else {
							i = read(fd,rreq->data + currentpos,recleng - (received-8));
						}
					}
					if (i==0) {
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
						status = EIO;
						break;
					}
					if (i<0) {
						int err = errno;
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(err),currentpos,rleng,trycnt+1);
						status = EIO;
						break;
					}
					if (received < 8+20) {
						if (received+i >= 8+20) {
							currentpos += i - ((8+20) - received);
						}
					} else {
						currentpos += i;
					}
					received += i;
					if (received > 8+recleng) {
						syslog(LOG_WARNING,"readworker: internal error - received more bytes than expected");
						status = EIO;
						currentpos = 0; // start again from beginning
						break;
					} else if (received == 8+recleng) {

						if (reccmd==CSTOCL_READ_STATUS) {
							rptr = recvbuff;
							recchunkid = get64bit(&rptr);
							recstatus = get8bit(&rptr);
							if (recchunkid != chunkid) {
								syslog(LOG_WARNING,"readworker: got unexpected status packet (expected chunkdid:%"PRIu64",packet chunkid:%"PRIu64")",chunkid,recchunkid);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							if (recstatus!=STATUS_OK) {
								syslog(LOG_WARNING,"readworker: read error: %s",mfsstrerr(recstatus));
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							if (currentpos != rleng) {
								syslog(LOG_WARNING,"readworker: unexpected data block size (requested: %"PRIu32" / received: %"PRIu32")",rleng,currentpos);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							gotstatus = 1;
						} else if (reccmd==CSTOCL_READ_DATA) {
							rptr = recvbuff;
							recchunkid = get64bit(&rptr);
							recblocknum = get16bit(&rptr);
							recoffset = get16bit(&rptr);
							recsize = get32bit(&rptr);
							reccrc = get32bit(&rptr);
							(void)recoffset;
							(void)recblocknum;
							if (recchunkid != chunkid) {
								syslog(LOG_WARNING,"readworker: got unexpected data packet (expected chunkdid:%"PRIu64",packet chunkid:%"PRIu64")",chunkid,recchunkid);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							if (recsize+20 != recleng) {
								syslog(LOG_WARNING,"readworker: got malformed data packet (datasize: %"PRIu32",packetsize: %"PRIu32")",recsize,recleng);
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							if (reccrc != mycrc32(0,rreq->data + (currentpos - recsize),recsize)) {
								syslog(LOG_WARNING,"readworker: data checksum error");
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
						}
						received = 0;
					}
				}
			}
		} while (1);

		if (status==0 && csver>=VERSION2INT(1,7,32)) {
			conncache_insert(ip,port,fd);
		} else {
			tcpclose(fd);
		}

		if (status==EINTR) {
			status=0;
		}

#ifdef WORKER_DEBUG
		now = monotonic_seconds();
		workingtime = now - start;

		syslog(LOG_NOTICE,"worker %lu received data from chunk %016"PRIX64"_%08"PRIX32", bw: %.6lfMB/s ( %"PRIu32" B / %.6lf s )",(unsigned long)arg,chunkid,version,(double)bytesreceived/workingtime,bytesreceived,workingtime);
#endif

		zassert(pthread_mutex_lock(&(ind->lock)));
		rreq->currentpos = currentpos;
		if (status!=0) {
			ind->trycnt++;
			if (ind->trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_invalidate_masterdata(inode,chindx);
				read_job_end(rreq,status,0);
			} else {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_invalidate_masterdata(inode,chindx);
				read_job_end(rreq,0,10000+((ind->trycnt<30)?((ind->trycnt-1)*300000):10000000));
			}
		} else {
			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,0,0);
		}
	}
	return NULL;
}

/* API | glock: INITIALIZED,UNLOCKED */
void read_data_init (uint64_t readaheadsize,uint32_t readaheadleng,uint32_t readaheadtrigger,uint32_t retries) {
        uint32_t i;
//	sigset_t oldset;
//	sigset_t newset;

	maxretries = retries;
	readahead = readaheadleng;
	readahead_trigger = readaheadtrigger;
	maxreadaheadsize = readaheadsize;
	reqbufftotalsize = 0;

#ifndef HAVE___SYNC_OP_AND_FETCH
	zassert(pthread_mutex_init(&buffsizelock,NULL));
#endif
	zassert(pthread_mutex_init(&mreq_cache_lock,NULL));
	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_cond_init(&worker_term_cond,NULL));
	worker_term_waiting = 0;

	mreq_cache = malloc(sizeof(mreqcache*)*MREQ_CACHE_SIZE);
	passert(mreq_cache);
	for (i=0 ; i<MREQ_CACHE_SIZE ; i++) {
		mreq_cache[i]=NULL;
	}

	indhash = malloc(sizeof(inodedata*)*IDHASHSIZE);
	passert(indhash);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		indhash[i]=NULL;
	}

//	dqueue = queue_new(0);
	jqueue = queue_new(0);

        zassert(pthread_attr_init(&worker_thattr));
        zassert(pthread_attr_setstacksize(&worker_thattr,0x100000));

//	sigemptyset(&newset);
//	sigaddset(&newset, SIGTERM);
//	sigaddset(&newset, SIGINT);
//	sigaddset(&newset, SIGHUP);
//	sigaddset(&newset, SIGQUIT);
//	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
//	zassert(pthread_create(&dqueue_worker_th,&worker_thattr,read_dqueue_worker,NULL));
//	pthread_sigmask(SIG_SETMASK, &oldset, NULL);

	zassert(pthread_mutex_lock(&glock));
	workers_avail = 0;
	workers_total = 0;
	read_data_spawn_worker();
	zassert(pthread_mutex_unlock(&glock));
//	fprintf(stderr,"spawn worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);

//#ifdef BUFFER_DEBUG
//        pthread_create(&info_worker_th,&thattr,read_info_worker,NULL);
//#endif
//	for (i=0 ; i<WORKERS ; i++) {
//		zassert(pthread_create(read_worker_th+i,&thattr,read_worker,(void*)(unsigned long)(i)));
//	}
}

void read_data_term(void) {
	uint32_t i;
	inodedata *ind,*indn;
	mreqcache *mrc,*mrcn;

//	queue_close(dqueue);
	queue_close(jqueue);
	zassert(pthread_mutex_lock(&glock));
	while (workers_total>0) {
		worker_term_waiting++;
		zassert(pthread_cond_wait(&worker_term_cond,&glock));
	}
	zassert(pthread_mutex_unlock(&glock));
//	zassert(pthread_join(dqueue_worker_th,NULL));
//	queue_delete(dqueue);
	queue_delete(jqueue);
	zassert(pthread_mutex_lock(&glock));
	for (i=0 ; i<IDHASHSIZE ; i++) {
		for (ind = indhash[i] ; ind ; ind = indn) {
			indn = ind->next;
			zassert(pthread_mutex_lock(&(ind->lock)));
			zassert(pthread_mutex_unlock(&(ind->lock)));
			zassert(pthread_cond_destroy(&(ind->closecond)));
			zassert(pthread_mutex_destroy(&(ind->lock)));
//			free(ind->mreq_csdata);
			free(ind);
		}
	}
	free(indhash);
	zassert(pthread_mutex_unlock(&glock));
	zassert(pthread_mutex_lock(&mreq_cache_lock));
	for (i=0 ; i<MREQ_CACHE_SIZE ; i++) {
		for (mrc = mreq_cache[i] ; mrc ; mrc = mrcn) {
			mrcn = mrc->next;
			zassert(pthread_cond_destroy(&(mrc->reqcond)));
			free(mrc->csdata);
			free(mrc);
		}
	}
	free(mreq_cache);
	zassert(pthread_mutex_unlock(&mreq_cache_lock));
	//        free(cacheblocks);
	//        pthread_cond_destroy(&fcbcond);
	zassert(pthread_attr_destroy(&worker_thattr));
	zassert(pthread_cond_destroy(&worker_term_cond));
        zassert(pthread_mutex_destroy(&glock));
        zassert(pthread_mutex_destroy(&mreq_cache_lock));
#ifndef HAVE___SYNC_OP_AND_FETCH
	zassert(pthread_mutex_destroy(&buffsizelock));
#endif
}



rrequest* read_new_request(inodedata *ind,uint64_t *offset,uint64_t blockend) {
	uint64_t chunkoffset;
	uint64_t chunkend;
	uint32_t chunkleng;
	uint32_t chindx;
	int pfd[2];

	chunkoffset = *offset;
	chindx = chunkoffset>>MFSCHUNKBITS;
	chunkend = chindx;
	chunkend <<= MFSCHUNKBITS;
	chunkend += MFSCHUNKSIZE;
	if (blockend > chunkend) {
		chunkleng = chunkend - chunkoffset;
		*offset = chunkend;
	} else {
		chunkleng = blockend - (*offset);
		*offset = blockend;
	}

	if (pipe(pfd)<0) {
		syslog(LOG_WARNING,"pipe error: %s",strerr(errno));
		return NULL;
	}

	rrequest *rreq;
	rreq = malloc(sizeof(rrequest));
	passert(rreq);
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: inode: %"PRIu32" - new request: chindx: %"PRIu32" chunkoffset: %"PRIu64" chunkleng: %"PRIu32"\n",monotonic_seconds(),ind->inode,chindx,chunkoffset,chunkleng);
#endif
	rreq->ind = ind;
	rreq->pipe[0] = pfd[0];
	rreq->pipe[1] = pfd[1];
	rreq->modified = monotonic_seconds();
	rreq->waitingworker = 0;
	rreq->offset = chunkoffset;
	rreq->leng = chunkleng;
	rreq->chindx = chindx;
	rreq->rleng = 0;
	rreq->currentpos = 0;
	rreq->mode = NEW;
//	rreq->filled = 0;
	rreq->refresh = 0;
//	rreq->busy = 0;
//	rreq->free = 0;
	rreq->lcnt = 0;
	rreq->data = malloc(chunkleng);
	passert(rreq->data);
	rreq->waiting = 0;
	zassert(pthread_cond_init(&(rreq->cond),NULL));
	if (ind->inqueue<MAXREQINQUEUE) {
		rreq->mode = INQUEUE;
		read_enqueue(rreq);
		ind->inqueue++;
	}
	rreq->next = NULL;
	rreq->prev = ind->reqtail;
	*(ind->reqtail) = rreq;
	ind->reqtail = &(rreq->next);
#ifdef HAVE___SYNC_OP_AND_FETCH
	__sync_add_and_fetch(&reqbufftotalsize,chunkleng);
#else
	zassert(pthread_mutex_lock(&buffsizelock));
	reqbufftotalsize += chunkleng;
	zassert(pthread_mutex_unlock(&buffsizelock));
#endif
	return rreq;
}

typedef struct rlist_s {
	rrequest *rreq;
	uint64_t offsetadd;
	uint32_t reqleng;
	struct rlist_s *next;
} rlist;

static inline void read_rreq_not_needed(rrequest *rreq) {
	if (rreq->mode!=BUSY && rreq->mode!=INQUEUE && rreq->mode!=REFRESH && rreq->mode!=BREAK && rreq->mode!=FILLED) { // nobody wants it anymore, so delete it
		if (rreq->lcnt==0) { // nobody wants it anymore, so delete it
			*(rreq->prev) = rreq->next;
			if (rreq->next) {
				rreq->next->prev = rreq->prev;
			} else {
				rreq->ind->reqtail = rreq->prev;
			}
#ifdef HAVE___SYNC_OP_AND_FETCH
			__sync_sub_and_fetch(&reqbufftotalsize,rreq->leng);
#else
			zassert(pthread_mutex_lock(&buffsizelock));
			reqbufftotalsize -= rreq->leng;
			zassert(pthread_mutex_unlock(&buffsizelock));
#endif
			close(rreq->pipe[0]);
			close(rreq->pipe[1]);
			free(rreq->data);
			free(rreq);
		} else if (rreq->mode==NEW || rreq->mode==READY) {
			rreq->mode = FREE; // somenody still using it, so mark it for removal
		}
	} else {
		if (rreq->lcnt==0) {
			rreq->mode = BREAK;
			if (rreq->waitingworker) {
				if (write(rreq->pipe[1]," ",1)!=1) {
					syslog(LOG_ERR,"can't write to pipe !!!");
				}
				rreq->waitingworker=0;
			}
		}
	}
}

// return list of rreq
int read_data(void *vid, uint64_t offset, uint32_t *size, void **vrhead,struct iovec **iov,uint32_t *iovcnt) {
	inodedata *ind = (inodedata*)vid;
	rrequest *rreq,*rreqn;
	rlist *rl,*rhead,**rtail;
	uint64_t rbuffsize;
	uint64_t firstbyte;
	uint64_t lastbyte;
	uint32_t cnt;
	int status;
	double now;

	zassert(pthread_mutex_lock(&(ind->lock)));

	ind->canmodatime = 2;

	*vrhead = NULL;
	*iov = NULL;
	*iovcnt = 0;
	cnt = 0;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" ind->status: %d ind->closewaiting: %"PRIu16"\n",monotonic_seconds(),ind->inode,ind->status,ind->closewaiting);
#endif

#ifdef HAVE___SYNC_OP_AND_FETCH
	rbuffsize = __sync_or_and_fetch(&reqbufftotalsize,0);
#else
	zassert(pthread_mutex_lock(&buffsizelock));
	rbuffsize = reqbufftotalsize;
	zassert(pthread_mutex_unlock(&buffsizelock));
#endif

	if (ind->status==0 && ind->closing==0) {
		if (offset==ind->lastoffset) {
			if (offset==0) { // begin with read-ahead turned on
				ind->readahead = 1;
				ind->seqdata = 0;
			} else if (ind->readahead<READAHEAD_MAX) {
				if (ind->seqdata>=readahead_trigger) {
					ind->readahead++;
					ind->seqdata = 0;
				}
			}
		} else {
			if (offset+(readahead/2) < ind->lastoffset || ind->lastoffset+(readahead/2) < offset) {
				if (ind->readahead>0) {
					ind->readahead--;
				}
				ind->seqdata = 0;
			}
		}
		if (ind->readahead > 1 && reqbufftotalsize >= (maxreadaheadsize / 2) + ((maxreadaheadsize * 1) / (ind->readahead * 2))) {
			ind->readahead--;
			ind->seqdata = 0;
		}
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" seqdata: %"PRIu32" offset: %"PRIu64" ind->lastoffset: %"PRIu64" ind->readahead: %u reqbufftotalsize:%"PRIu64"\n",monotonic_seconds(),ind->inode,ind->seqdata,offset,ind->lastoffset,ind->readahead,rbuffsize);
#endif

		// prepare requests

		firstbyte = offset;
		lastbyte = offset + (*size);
		rhead = NULL;
		rtail = &rhead;
		rreq = ind->reqhead;
		now = monotonic_seconds();
		while (rreq && lastbyte>firstbyte) {
			rreqn = rreq->next;
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" , rreq->modified:%.6lf , rreq->offset: %"PRIu64" , rreq->leng: %"PRIu32" , firstbyte: %"PRIu64" , lastbyte: %"PRIu64"\n",monotonic_seconds(),ind->inode,rreq->modified,rreq->offset,rreq->leng,firstbyte,lastbyte);
#endif
			if (rreq->mode==BREAK || rreq->mode==FREE) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" bad request mode (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
			} else if (rreq->modified+BUFFER_VALIDITY_TIMEOUT<now) { // buffer too old
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" data too old: free rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				read_rreq_not_needed(rreq);
			} else if (firstbyte < rreq->offset || firstbyte >= rreq->offset+rreq->leng) { // all not sequential read cases
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" case 0: free rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				// rreq:      |---------|
				// read: |--|
				// read: |-------|
				// read: |-------------------|
				// read:                  |--|
				read_rreq_not_needed(rreq);
			} else if (lastbyte <= rreq->offset+rreq->leng) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" case 1: use rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				// rreq: |---------|
				// read:    |---|
				rl = malloc(sizeof(rlist));
				passert(rl);
				rl->rreq = rreq;
				rl->offsetadd = firstbyte - rreq->offset;
				rl->reqleng = (lastbyte - firstbyte) + rl->offsetadd;
				rl->next = NULL;
				*rtail = rl;
				rtail = &(rl->next);
				rreq->lcnt++;
				if (ind->readahead && ind->flengisvalid) {
					if (lastbyte > rreq->offset) {
						// request next block of data
						if (rreq->next==NULL && rbuffsize<maxreadaheadsize) {
							uint64_t blockstart,blockend;
							blockstart = rreq->offset+rreq->leng;
							blockend = blockstart + (readahead * (1<<((ind->readahead-1)*2)));
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (middle of existing block) add new read-ahead rreq (%"PRIu64":%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,blockend-blockstart);
#endif
							if (blockend<=ind->fleng) {
								read_new_request(ind,&blockstart,blockend);
							} else if (blockstart<ind->fleng) {
								read_new_request(ind,&blockstart,ind->fleng);
							}
							// and another one if necessary
							if ((blockstart % MFSCHUNKSIZE) == 0 && rreq->next!=NULL && rreq->next->next==NULL && rbuffsize<maxreadaheadsize) {
								blockend = blockstart + (readahead * (1<<((ind->readahead-1)*2)));
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (middle of existing block) add new extra read-ahead rreq (%"PRIu64":%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,blockend-blockstart);
#endif
								if (blockend<=ind->fleng) {
									read_new_request(ind,&blockstart,blockend);
								} else if (blockstart<ind->fleng) {
									read_new_request(ind,&blockstart,ind->fleng);
								}
							}
						}
					}
				}
				lastbyte = 0;
				firstbyte = 0;
			} else {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" case 2: use tail of rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				// rreq: |---------|
				// read:         |---|
				rl = malloc(sizeof(rlist));
				passert(rl);
				rl->rreq = rreq;
				rl->offsetadd = firstbyte - rreq->offset;
				rl->reqleng = rreq->leng;
				rl->next = NULL;
				*rtail = rl;
				rtail = &(rl->next);
				rreq->lcnt++;
				firstbyte = rreq->offset+rreq->leng;
			}
			rreq = rreqn;
		}
		while (lastbyte>firstbyte) {
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" add new rreq (%"PRIu64":%"PRId64")\n",monotonic_seconds(),ind->inode,firstbyte,(lastbyte-firstbyte));
#endif
			rreq = read_new_request(ind,&firstbyte,lastbyte);
			rl = malloc(sizeof(rlist));
			passert(rl);
			rl->rreq = rreq;
			rl->offsetadd = 0;
			rl->reqleng = rreq->leng;
			rl->next = NULL;
			*rtail = rl;
			rtail = &(rl->next);
			rreq->lcnt++;
			if (lastbyte==firstbyte && ind->readahead && ind->flengisvalid && rbuffsize<maxreadaheadsize) {
				uint64_t blockend;
				blockend = lastbyte + (readahead * (1<<((ind->readahead-1)*2)))/2;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (after new block) add new read-ahead rreq (%"PRIu64":%"PRId64")\n",monotonic_seconds(),ind->inode,lastbyte,blockend-lastbyte);
#endif
				if (blockend<=ind->fleng) {
					(void)read_new_request(ind,&firstbyte,blockend);
				} else if (lastbyte<ind->fleng) {
					(void)read_new_request(ind,&firstbyte,ind->fleng);
				}
			}
		}

		*vrhead = rhead;

		cnt = 0;
		*size = 0;
		for (rl = rhead ; rl ; rl=rl->next) {
			while (rl->rreq->mode!=READY && rl->rreq->mode!=FILLED && ind->status==0 && ind->closing==0) {
				rl->rreq->waiting++;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" wait for data: %"PRIu64":%"PRIu32"\n",monotonic_seconds(),ind->inode,rl->rreq->offset,rl->rreq->leng);
#endif
				zassert(pthread_cond_wait(&(rl->rreq->cond),&(ind->lock)));
				rl->rreq->waiting--;
			}
			if (ind->status==0) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" block %"PRIu64":%"PRIu32"(%"PRIu32") has been read\n",monotonic_seconds(),ind->inode,rl->rreq->offset,rl->rreq->rleng,rl->rreq->leng);
#endif
				if (rl->rreq->rleng < rl->rreq->leng) {
					if (rl->rreq->rleng > rl->offsetadd) {
						cnt++;
						if (rl->reqleng > rl->rreq->rleng) {
							rl->reqleng = rl->rreq->rleng;
						}
						*size += rl->reqleng - rl->offsetadd;
					}
					break; // end of file
				} else {
					cnt++;
					*size += rl->reqleng - rl->offsetadd;
				}
			} else {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" error reading block: %"PRIu64":%"PRIu32"\n",monotonic_seconds(),ind->inode,rl->rreq->offset,rl->rreq->leng);
#endif
				break;
			}
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" size: %"PRIu32" ; cnt: %u\n",monotonic_seconds(),ind->inode,*size,cnt);
#endif
		}
	}

	if (ind->status==0 && ind->closing==0 && cnt>0) {
		ind->lastoffset = offset + (*size);
		if (ind->readahead<READAHEAD_MAX) {
			ind->seqdata += (*size);
		}
		*iov = malloc(sizeof(struct iovec)*cnt);
		passert(*iov);
		cnt = 0;
		for (rl = rhead ; rl ; rl=rl->next) {
			if (rl->rreq->rleng < rl->rreq->leng) {
				if (rl->rreq->rleng > rl->offsetadd) {
					(*iov)[cnt].iov_base = rl->rreq->data + rl->offsetadd;
					(*iov)[cnt].iov_len = rl->reqleng - rl->offsetadd;
					cnt++;
				}
				break;
			} else {
				(*iov)[cnt].iov_base = rl->rreq->data + rl->offsetadd;
				(*iov)[cnt].iov_len = rl->reqleng - rl->offsetadd;
				cnt++;
			}
		}
		*iovcnt = cnt;
	} else {
		*iovcnt = 0;
		*iov = NULL;
	}

	status = ind->status;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" ind->status: %d iovcnt: %"PRIu32" iovec: %p\n",monotonic_seconds(),ind->inode,ind->status,*iovcnt,(void*)(*iov));
#endif

	zassert(pthread_mutex_unlock(&(ind->lock)));
	return status;
}

void read_data_free_buff(void *vid,void *vrhead,struct iovec *iov) {
	inodedata *ind = (inodedata*)vid;
	rlist *rl,*rln;
	rrequest *rreq;
	rl = (rlist*)vrhead;
	zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" inode_structure: %p vrhead: %p iovec: %p\n",monotonic_seconds(),ind->inode,(void*)ind,(void*)vrhead,(void*)iov);
#endif
	while (rl) {
		rln = rl->next;
		rreq = rl->rreq;
		rreq->lcnt--;
		if (rreq->lcnt==0 && rreq->mode==FREE) {
			*(rreq->prev) = rreq->next;
			if (rreq->next) {
				rreq->next->prev = rreq->prev;
			} else {
				ind->reqtail = rreq->prev;
			}
#ifdef HAVE___SYNC_OP_AND_FETCH
			__sync_sub_and_fetch(&reqbufftotalsize,rreq->leng);
#else
			zassert(pthread_mutex_lock(&buffsizelock));
			reqbufftotalsize -= rreq->leng;
			zassert(pthread_mutex_unlock(&buffsizelock));
#endif
			close(rreq->pipe[0]);
			close(rreq->pipe[1]);
			free(rreq->data);
			free(rreq);
		}
		free(rl);
		rl = rln;
	}
	if (ind->reqhead==NULL && ind->closewaiting>0) {
		zassert(pthread_cond_broadcast(&(ind->closecond)));
	}
	if (iov) {
		free(iov);
	}
	zassert(pthread_mutex_unlock(&(ind->lock)));
}

void read_inode_dirty_region(uint32_t inode,uint64_t offset,uint32_t size,const char *buff) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;
	rrequest *rreq,*rreqn;
//	int clearedbuff = 0;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_inode_dirty_region: inode: %"PRIu32" set dirty region: %"PRIu64":%"PRIu32"\n",monotonic_seconds(),inode,offset,size);
#endif
	zassert(pthread_mutex_lock(&glock));
	for (ind = indhash[indh] ; ind ; ind=ind->next) {
		if (ind->inode == inode) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			for (rreq = ind->reqhead ; rreq ; rreq=rreqn) {
				rreqn = rreq->next;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (before): (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				if (rreq->mode!=FREE && rreq->mode!=BREAK && ((rreq->offset < offset + size) && (rreq->offset + rreq->leng > offset))) {
					if (rreq->mode==READY || rreq->mode==FILLED) { // already filled, exchange data
						if (rreq->offset > offset) {
							if (rreq->offset + rreq->leng > offset + size) {
								// rreq:   |-------|
								// buff: |----|
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_dirty_region: case 1: rreq (%"PRIu64":%"PRIu32") / buff (%"PRIu64":%"PRIu32")\n",monotonic_seconds(),rreq->offset,rreq->leng,offset,size);
#endif
								memcpy(rreq->data,buff + (rreq->offset - offset),size - (rreq->offset - offset));
								if (size - (rreq->offset - offset) > rreq->rleng) {
									rreq->rleng = size - (rreq->offset - offset);
								}
							} else {
								// rreq:   |-------|
								// buff: |-----------|
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_dirty_region: case 2: rreq (%"PRIu64":%"PRIu32") / buff (%"PRIu64":%"PRIu32")\n",monotonic_seconds(),rreq->offset,rreq->leng,offset,size);
#endif
								memcpy(rreq->data,buff + (rreq->offset - offset),rreq->leng);
								if (rreq->leng > rreq->rleng) {
									rreq->rleng = rreq->leng;
								}
							}
						} else {
							if (rreq->offset + rreq->leng > offset + size) {
								// rreq: |-------|
								// buff:   |----|
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_dirty_region: case 3: rreq (%"PRIu64":%"PRIu32") / buff (%"PRIu64":%"PRIu32")\n",monotonic_seconds(),rreq->offset,rreq->leng,offset,size);
#endif
								memcpy(rreq->data + (offset - rreq->offset),buff,size);
								if ((offset - rreq->offset) > rreq->rleng) {
									memset(rreq->data + rreq->rleng,0,(offset - rreq->offset) - rreq->rleng);
								}
								if (size + (offset - rreq->offset) > rreq->rleng) {
									rreq->rleng = size + (offset - rreq->offset);
								}
							} else {
								// rreq: |-------|
								// buff:   |--------|
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_dirty_region: case 4: rreq (%"PRIu64":%"PRIu32") / buff (%"PRIu64":%"PRIu32")\n",monotonic_seconds(),rreq->offset,rreq->leng,offset,size);
#endif
								memcpy(rreq->data+(offset-rreq->offset),buff,rreq->leng-(offset-rreq->offset));
								if ((offset - rreq->offset) > rreq->rleng) {
									memset(rreq->data + rreq->rleng,0,(offset - rreq->offset) - rreq->rleng);
								}
								if (rreq->leng > rreq->rleng) {
									rreq->rleng = rreq->leng;
								}
							}
						}
					} else if (rreq->mode==BUSY) { // in progress, so refresh it
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (%"PRIu64":%"PRIu32") : refresh\n",monotonic_seconds(),rreq->offset,rreq->leng);
#endif
						rreq->mode = REFRESH;
						if (rreq->waitingworker) {
							if (write(rreq->pipe[1]," ",1)!=1) {
								syslog(LOG_ERR,"can't write to pipe !!!");
							}
							rreq->waitingworker=0;
						}
					}
				}
#ifdef RDEBUG
				if (rreq) {
					fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (after): (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
				} else {
					fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (after): NULL\n",monotonic_seconds());
				}
#endif
			}
			if (ind->flengisvalid && offset+size>ind->fleng) {
				ind->fleng = offset+size;
			}
			zassert(pthread_mutex_unlock(&(ind->lock)));
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}

void read_inode_dont_modify_atime(uint32_t inode) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;

	zassert(pthread_mutex_lock(&glock));
	for (ind = indhash[indh] ; ind ; ind=ind->next) {
		if (ind->inode == inode) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			ind->canmodatime = 0;
			zassert(pthread_mutex_unlock(&(ind->lock)));
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}

// void read_inode_ops(uint32_t inode) {
void read_inode_set_length(uint32_t inode,uint64_t newlength,uint8_t active) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;
	rrequest *rreq,*rreqn;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_inode_set_length: inode: %"PRIu32" set length: %"PRIu64"\n",monotonic_seconds(),inode,newlength);
#endif
	zassert(pthread_mutex_lock(&glock));
	for (ind = indhash[indh] ; ind ; ind=ind->next) {
		if (ind->inode == inode) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			for (rreq = ind->reqhead ; rreq ; rreq=rreqn) {
				rreqn = rreq->next;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_inode_set_length: rreq (before): (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				if (rreq->mode==READY) {
					if (active) {
						if (newlength < rreq->offset + rreq->rleng) {
							if (newlength < rreq->offset) {
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_set_length: block is filled (%"PRIu64":%"PRIu32") / newlength: %"PRIu64", case 1: - set rleng to 0\n",monotonic_seconds(),rreq->offset,rreq->rleng,newlength);
#endif
								rreq->rleng = 0;
							} else {
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_set_length: block is filled (%"PRIu64":%"PRIu32") / newlength: %"PRIu64", case 2: - set rleng to %"PRIu32"\n",monotonic_seconds(),rreq->offset,rreq->rleng,newlength,(uint32_t)(newlength - rreq->offset));
#endif
								rreq->rleng = newlength - rreq->offset;
							}
						} else if (newlength > rreq->offset + rreq->rleng) {
							if (newlength > rreq->offset + rreq->leng) {
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_set_length: block is filled (%"PRIu64":%"PRIu32") / newlength: %"PRIu64", case 3: - clear data from rleng, set rleng to %"PRIu32"\n",monotonic_seconds(),rreq->offset,rreq->rleng,newlength,rreq->leng);
#endif
								memset(rreq->data + rreq->rleng,0,rreq->leng - rreq->rleng);
								rreq->rleng = rreq->leng;
							} else {
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_inode_set_length: block is filled (%"PRIu64":%"PRIu32") / newlength: %"PRIu64", case 4: - clear data from rleng, set rleng to %"PRIu32"\n",monotonic_seconds(),rreq->offset,rreq->rleng,newlength,(uint32_t)(newlength - rreq->offset));
#endif
								memset(rreq->data + rreq->rleng,0,newlength - (rreq->offset + rreq->rleng));
								rreq->rleng = newlength - rreq->offset;
							}
						}
					} else {
						if (rreq->lcnt==0) { // nobody wants it anymore, so delete it
							*(rreq->prev) = rreq->next;
							if (rreq->next) {
								rreq->next->prev = rreq->prev;
							} else {
								ind->reqtail = rreq->prev;
							}
#ifdef HAVE___SYNC_OP_AND_FETCH
							__sync_sub_and_fetch(&reqbufftotalsize,rreq->leng);
#else
							zassert(pthread_mutex_lock(&buffsizelock));
							reqbufftotalsize -= rreq->leng;
							zassert(pthread_mutex_unlock(&buffsizelock));
#endif
							close(rreq->pipe[0]);
							close(rreq->pipe[1]);
							free(rreq->data);
							free(rreq);
							rreq = NULL;
						} else { // somebody wants it, so clear it
							rreq->mode = NEW;
							if (ind->inqueue<MAXREQINQUEUE) {
								rreq->mode = INQUEUE;
								read_enqueue(rreq);
								ind->inqueue++;
							}
						}
					}
				} else if (rreq->mode==BUSY || rreq->mode==FILLED) {
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: read_inode_set_length: block is busy - refresh\n",monotonic_seconds());
#endif
					rreq->mode = REFRESH;
					if (rreq->waitingworker) {
						if (write(rreq->pipe[1]," ",1)!=1) {
							syslog(LOG_ERR,"can't write to pipe !!!");
						}
						rreq->waitingworker=0;
					}
				}
#ifdef RDEBUG
				if (rreq) {
					fprintf(stderr,"%.6lf: read_inode_set_length: rreq (after): (%"PRIu64":%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
				} else {
					fprintf(stderr,"%.6lf: read_inode_set_length: rreq (after): NULL\n",monotonic_seconds());
				}
#endif
			}
			ind->fleng = newlength;
			ind->flengisvalid = 1;
			zassert(pthread_mutex_unlock(&(ind->lock)));
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}

void* read_data_new(uint32_t inode) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;

	zassert(pthread_mutex_lock(&glock));

	ind = malloc(sizeof(inodedata));
	passert(ind);
	ind->inode = inode;
	ind->flengisvalid = 0;
	ind->seqdata = 0;
	ind->fleng = 0;
	ind->status = 0;
	ind->trycnt = 0;
	ind->inqueue = 0;
	ind->canmodatime = 1;
	ind->readahead = 0;
	ind->lastoffset = 0;
	ind->closewaiting = 0;
	ind->closing = 0;
//	ind->mreq_time = 0.0;
//	ind->mreq_chindx = 0xFFFFFFFF;
//	ind->mreq_chunkid = 0;
//	ind->mreq_version = 0;
//	ind->mreq_csdataver = 0;
//	ind->mreq_csdatasize = 0;
//	ind->mreq_csdata = malloc(256);
//	passert(ind->mreq_csdata);
//	ind->mreq_csdatabuffsize = 256;
//	ind->laststatus = 0;
	zassert(pthread_cond_init(&(ind->closecond),NULL));
	zassert(pthread_mutex_init(&(ind->lock),NULL));
	ind->reqhead = NULL;
	ind->reqtail = &(ind->reqhead);
	ind->next = indhash[indh];
	indhash[indh] = ind;
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: opening: %"PRIu32" ; inode_structure: %p\n",monotonic_seconds(),inode,(void*)ind);
//	read_data_hexdump((uint8_t*)ind,sizeof(inodedata));
#endif
	zassert(pthread_mutex_unlock(&glock));
	return ind;
}

void read_data_end(void *vid) {
	inodedata *ind,**indp;
	rrequest *rreq,*rreqn;
	uint32_t indh;

	ind = (inodedata*)vid;
	indh = IDHASH(ind->inode);

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: %"PRIu32" ; inode_structure: %p\n",monotonic_seconds(),ind->inode,(void*)ind);
//	read_data_hexdump((uint8_t*)ind,sizeof(inodedata));
#endif
	zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: %"PRIu32" ; cleaning req list\n",monotonic_seconds(),ind->inode);
#endif
	ind->closing = 1;
	for (rreq = ind->reqhead ; rreq ; rreq=rreqn) {
		rreqn = rreq->next;
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: closing: %"PRIu32" ; rreq: lcnt: %u ; mode: %s\n",monotonic_seconds(),ind->inode,rreq->lcnt,read_data_modename(rreq->mode));
#endif
		if (rreq->lcnt==0 && rreq->mode!=INQUEUE && rreq->mode!=BUSY && rreq->mode!=REFRESH && rreq->mode!=BREAK && rreq->mode!=FILLED) {
			*(rreq->prev) = rreq->next;
			if (rreq->next) {
				rreq->next->prev = rreq->prev;
			} else {
				ind->reqtail = rreq->prev;
			}
#ifdef HAVE___SYNC_OP_AND_FETCH
			__sync_sub_and_fetch(&reqbufftotalsize,rreq->leng);
#else
			zassert(pthread_mutex_lock(&buffsizelock));
			reqbufftotalsize -= rreq->leng;
			zassert(pthread_mutex_unlock(&buffsizelock));
#endif
			close(rreq->pipe[0]);
			close(rreq->pipe[1]);
			free(rreq->data);
			free(rreq);
		}
	}
	while (ind->reqhead!=NULL) {
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: closing: %"PRIu32" ; reqhead: %s ; inqueue: %u\n",monotonic_seconds(),ind->inode,ind->reqhead?"NOT NULL":"NULL",ind->inqueue);
#endif
		ind->closewaiting++;
		if (ind->reqhead->waitingworker) {
			if (write(ind->reqhead->pipe[1]," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			ind->reqhead->waitingworker=0;
		}
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: inode: %"PRIu32" ; waiting for close\n",monotonic_seconds(),ind->inode);
#endif
		zassert(pthread_cond_wait(&(ind->closecond),&(ind->lock)));
		ind->closewaiting--;
	}
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: %"PRIu32" ; reqhead: %s ; inqueue: %u - delete structure\n",monotonic_seconds(),ind->inode,ind->reqhead?"NOT NULL":"NULL",ind->inqueue);
#endif
	zassert(pthread_mutex_unlock(&(ind->lock)));
	zassert(pthread_mutex_lock(&glock));
	indp = &(indhash[indh]);
	while ((ind=*indp)) {
		if (ind==(inodedata*)vid) {
			*indp = ind->next;
			zassert(pthread_mutex_unlock(&glock));
			zassert(pthread_mutex_lock(&(ind->lock)));
			zassert(pthread_mutex_unlock(&(ind->lock)));
			zassert(pthread_cond_destroy(&(ind->closecond)));
			zassert(pthread_mutex_destroy(&(ind->lock)));
//			free(ind->mreq_csdata[i]);
			free(ind);
			return;
		} else {
			indp = &(ind->next);
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}
