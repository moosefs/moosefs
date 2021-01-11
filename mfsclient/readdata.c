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

#include <sys/types.h>
#include <stdio.h>
#ifdef HAVE_READV
#include <sys/uio.h>
#endif
#include <sys/time.h>
#include <unistd.h>
#ifndef WIN32
#include <poll.h>
#include <syslog.h>
#endif
#include <stdlib.h>
#include <string.h>
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
#include "chunkrwlock.h"
#include "chunksdatacache.h"
#include "mfsalloc.h"
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

enum {NEW,INQUEUE,BUSY,REFRESH,BREAK,FILLED,READY,NOTNEEDED};

#define STATE_NOT_NEEDED(mode) (((mode)==BREAK) || ((mode)==NOTNEEDED))
#define STATE_BG_JOBS(mode) (((mode)==BUSY) || ((mode)==INQUEUE) || ((mode)==REFRESH) || ((mode)==BREAK) || ((mode)==FILLED))
#define STATE_HAVE_VALID_DATA(mode) (((mode)==READY) || ((mode)==NOTNEEDED))

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
	case NOTNEEDED:
		return "NOTNEEDED";
	default:
		return "<unknown>";
	}
	return "<?>";
}

#ifdef RDEBUG
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
	int wakeup_fd;
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
	pthread_cond_t cond;
	struct rrequest_s *next,**prev;
} rrequest;

typedef struct inodedata_s {
	uint32_t inode;
	uint32_t seqdata;
	uint64_t fleng;
	int status;
	uint32_t trycnt;
	uint8_t closing;
	uint8_t inqueue;
	uint8_t readahead;
	uint64_t lastoffset;
	uint16_t waiting_writers;
	uint16_t readers_cnt;
	uint16_t lcnt;
	rrequest *reqhead,**reqtail;
	pthread_cond_t closecond;
	pthread_cond_t readerscond;
	pthread_cond_t writerscond;
	pthread_mutex_t lock;
	struct inodedata_s *next;
} inodedata;

typedef struct worker_s {
	pthread_t thread_id;
} worker;

static pthread_key_t rangesstorage;

static uint32_t readahead_leng;
static uint32_t readahead_trigger;

static uint64_t usectimeout;
static uint32_t maxretries;
static uint32_t minlogretry;
static uint64_t maxreadaheadsize;
static uint8_t erroronlostchunk;
static uint8_t erroronnospace;

static uint64_t reqbufftotalsize;
#ifndef HAVE___SYNC_OP_AND_FETCH
static pthread_mutex_t buffsizelock;
#endif

static inodedata **indhash;

static pthread_mutex_t inode_lock;

static pthread_mutex_t workers_lock;

//#ifdef BUFFER_DEBUG
//static pthread_t info_worker_th;
//static uint32_t usedblocks;
//#endif

//static pthread_t dqueue_worker_th;

static uint32_t workers_avail;
static uint32_t workers_total;
//static uint32_t worker_term_waiting;
static pthread_cond_t worker_term_cond;
static pthread_attr_t worker_thattr;

// static pthread_t read_worker_th[WORKERS];
//static inodedata *read_worker_id[WORKERS];

static void *jqueue; //,*dqueue;

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

/* requests */

static inline rrequest* read_new_request(inodedata *ind,uint64_t *offset,uint64_t blockend) {
	uint64_t chunkoffset;
	uint64_t chunkend;
	uint32_t chunkleng;
	uint32_t chindx;

	sassert(blockend>*offset);

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

	rrequest *rreq;
	rreq = malloc(sizeof(rrequest));
	passert(rreq);
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: inode: %"PRIu32" - new request: chindx: %"PRIu32" chunkoffset: %"PRIu64" chunkleng: %"PRIu32"\n",monotonic_seconds(),ind->inode,chindx,chunkoffset,chunkleng);
#endif
	rreq->ind = ind;
	rreq->modified = monotonic_seconds();
	rreq->wakeup_fd = -1;
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
//	rreq->waiting = 0;
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

#ifdef RDEBUG
static inline uint64_t read_delete_request(rrequest *rreq) {
	uint64_t rbuffsize;
#else
static inline void read_delete_request(rrequest *rreq) {
#endif
	*(rreq->prev) = rreq->next;
	if (rreq->next) {
		rreq->next->prev = rreq->prev;
	} else {
		rreq->ind->reqtail = rreq->prev;
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
	free(rreq->data);
	zassert(pthread_cond_destroy(&(rreq->cond)));
	free(rreq);
#ifdef RDEBUG
	return rbuffsize;
#endif
}



// void read_job_end(inodedata *ind,int status,uint32_t delay) {
void read_job_end(rrequest *rreq,int status,uint32_t delay) {
	inodedata *ind;
	uint8_t breakmode;
#ifdef RDEBUG
	uint64_t rbuffsize;
	uint8_t pmode;
#endif

	ind = rreq->ind;
	zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: readworker end (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" - status:%d ; delay:%"PRIu32" ; rreq->mode: %s\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,ind->inode,status,delay,read_data_modename(rreq->mode));
	pmode = rreq->mode;
#endif
	breakmode = 0;
	if (rreq->mode==FILLED) {
		rreq->mode = READY;
		ind->trycnt = 0;
		zassert(pthread_cond_broadcast(&(rreq->cond)));
	} else {
		if (rreq->mode==BREAK) {
			breakmode = 1;
			rreq->mode = NOTNEEDED;
		} else if (rreq->mode==REFRESH) {
			delay = 0;
			rreq->mode = NEW;
		} else { // BUSY - this means I/O error
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
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: readworker end (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" - rreq->mode: %s->%s\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,ind->inode,read_data_modename(pmode),read_data_modename(rreq->mode));
#endif

	if (ind->closing || status!=0 || breakmode) {
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: readworker end (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" - closing: %u ; status: %d ; breakmode: %u\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,ind->inode,ind->closing,status,breakmode);
#endif
		if (rreq->lcnt==0) {
#ifdef RDEBUG
			rbuffsize = read_delete_request(rreq);
#else
			read_delete_request(rreq);
#endif
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: inode: %"PRIu32" - reqhead: %s (reqbufftotalsize: %"PRIu64")\n",monotonic_seconds(),ind->inode,ind->reqhead?"NOT NULL":"NULL",rbuffsize);
#endif

			if (ind->closing && ind->reqhead==NULL) {
				zassert(pthread_cond_broadcast(&(ind->closecond)));
			}
		} else {
			if (breakmode==0 && rreq->mode!=READY) {
				rreq->rleng = 0;
				rreq->mode = READY;
				zassert(pthread_cond_broadcast(&(rreq->cond)));
			}
		}
	} else {
		if (rreq->mode==NEW) {
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker end (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" - rreq->mode: NEW->INQUEUE\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,ind->inode);
#endif
			rreq->mode=INQUEUE;
			read_delayed_enqueue(rreq,delay);
			ind->inqueue++;
		}
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: readworker end - enqueue waiting requests ; inqueue: %"PRIu8" ; closing: %"PRIu8" ; status: %d ; breakmode: %u\n",monotonic_seconds(),ind->inqueue,ind->closing,status,breakmode);
#endif
		for (rreq = ind->reqhead ; rreq && ind->inqueue < MAXREQINQUEUE ; rreq=rreq->next) {
			if (rreq->mode==NEW) {
				rreq->mode = INQUEUE;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker end (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" - rreq->mode: NEW->INQUEUE\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,ind->inode);
#endif
				// read_delayed_enqueue(rreq,delay);
				read_enqueue(rreq);
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

/* workers_lock:LOCKED */
static inline void read_data_spawn_worker(void) {
#ifndef WIN32
	sigset_t oldset;
	sigset_t newset;
#endif
	worker *w;
	int res;

	w = malloc(sizeof(worker));
	if (w==NULL) {
		return;
	}
#ifndef WIN32
	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
#endif
	res = pthread_create(&(w->thread_id),&worker_thattr,read_worker,w);
#ifndef WIN32
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
#endif
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

/* workers_lock:LOCKED */
static inline void read_data_close_worker(worker *w) {
	workers_avail--;
	workers_total--;
	if (workers_total==0/* && worker_term_waiting*/) {
		zassert(pthread_cond_signal(&worker_term_cond));
//		worker_term_waiting--;
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
	uint8_t notdone;
	uint8_t recvbuff[20];
	uint8_t sendbuff[29];
#ifdef HAVE_READV
	struct iovec siov[2];
#endif
	uint8_t pipebuff[1024];
	int pipefd[2];
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
	const uint8_t *csdata;
	uint32_t csver;
	uint32_t cnt;
	uint32_t csdatasize;
	uint8_t csdataver;
	uint8_t rdstatus;
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

	if (pipe(pipefd)<0) {
		syslog(LOG_WARNING,"pipe error: %s",strerr(errno));
		return NULL;
	}

	for (;;) {
		if (ip || port) {
			csdb_readdec(ip,port);
		}
		ip = 0;
		port = 0;
		csstrip[0] = 0;

		if (firsttime==0) {
			zassert(pthread_mutex_lock(&workers_lock));
			workers_avail++;
			if (workers_avail > SUSTAIN_WORKERS) {
//				fprintf(stderr,"close worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
				read_data_close_worker(w);
				zassert(pthread_mutex_unlock(&workers_lock));
				close_pipe(pipefd);
				return NULL;
			}
			zassert(pthread_mutex_unlock(&workers_lock));
		}
		firsttime = 0;

		// get next job
		queue_get(jqueue,&z1,&z2,&data,&z3);

		zassert(pthread_mutex_lock(&workers_lock));

		if (data==NULL) {
//			fprintf(stderr,"close worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
			read_data_close_worker(w);
			zassert(pthread_mutex_unlock(&workers_lock));
			close_pipe(pipefd);
			return NULL;
		}

		workers_avail--;
		if (workers_avail==0 && workers_total<MAX_WORKERS) {
			read_data_spawn_worker();
//			fprintf(stderr,"spawn worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
		}
		timeoutadd = (workers_total>HEAVYLOAD_WORKERS)?0.0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT;
		zassert(pthread_mutex_unlock(&workers_lock));

		rreq = (rrequest*)data;
		ind = rreq->ind;

#ifdef RDEBUG
		fprintf(stderr,"%.6lf: readworker got request (rreq: %"PRIu64":%"PRIu64"/%"PRIu32")\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng);
#endif
		zassert(pthread_mutex_lock(&(ind->lock)));
		if (rreq->mode == INQUEUE) {
			rreq->mode = BUSY;
		} else if (rreq->mode == BREAK) {
			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,0,0);
			continue;
		} else {
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - bad request state: %s (expected INQUEUE or BREAK)",ind->inode,rreq->chindx,read_data_modename(rreq->mode));
			rreq->mode = BUSY;
		}

		chindx = rreq->chindx;
		status = ind->status;
		inode = ind->inode;
		rleng = rreq->leng;
		trycnt = ind->trycnt;

		if (status!=MFS_STATUS_OK) {
			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,status,0);
			continue;
		}
		if (ind->closing) {
			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,0,0);
			continue;
		}
		zassert(pthread_mutex_unlock(&(ind->lock)));

		chunkrwlock_rlock(inode,chindx);

		csdatasize = 1024; // pipebuff here is used as a temporary data buffer
		if (master_version()>=VERSION2INT(3,0,74) && chunksdatacache_find(inode,chindx,&chunkid,&version,&csdataver,pipebuff,&csdatasize)) {
			rdstatus = MFS_STATUS_OK;
			csdata = pipebuff;
			zassert(pthread_mutex_lock(&(ind->lock)));
			if (rreq->mode == BREAK) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,0,0);
				chunkrwlock_runlock(inode,chindx);
				continue;
			}
			mfleng = ind->fleng;
			zassert(pthread_mutex_unlock(&(ind->lock)));
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; chunksdatacache_find: mfleng: %"PRIu64" ; chunkid: %016"PRIX64" ; version: %"PRIu32" ; status:%"PRIu8"\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode,mfleng,chunkid,version,rdstatus);
#endif
		} else {
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; fs_readchunk (before)\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode);
#endif
			rdstatus = fs_readchunk(inode,chindx,0,&csdataver,&mfleng,&chunkid,&version,&csdata,&csdatasize);
			if (rdstatus==MFS_STATUS_OK) {
				chunksdatacache_insert(inode,chindx,chunkid,version,csdataver,csdata,csdatasize);
				zassert(pthread_mutex_lock(&(ind->lock)));
				if (rreq->mode == BREAK) {
					zassert(pthread_mutex_unlock(&(ind->lock)));
					read_job_end(rreq,0,0);
					chunkrwlock_runlock(inode,chindx);
					continue;
				}
				ind->fleng = mfleng;
				zassert(pthread_mutex_unlock(&(ind->lock)));
			}
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; fs_readchunk (after): mfleng: %"PRIu64" ; chunkid: %016"PRIX64" ; version: %"PRIu32" ; status:%"PRIu8"\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode,mfleng,chunkid,version,rdstatus);
#endif
		}

// rdstatus potential results:
// MFS_ERROR_NOCHUNK - internal error (can't be repaired)
// MFS_ERROR_ENOENT - internal error (wrong inode - can't be repaired)
// MFS_ERROR_EPERM - internal error (wrong inode - can't be repaired)
// MFS_ERROR_INDEXTOOBIG - requested file position is too big
// MFS_ERROR_CHUNKLOST - according to master chunk is definitelly lost (all chunkservers are connected and chunk is not there)
// statuses that are here just in case:
// MFS_ERROR_QUOTA (used in write only)
// MFS_ERROR_NOSPACE (used in write only)
// MFS_ERROR_IO (for future use)
// MFS_ERROR_NOCHUNKSERVERS (used in write only)
// MFS_ERROR_CSNOTPRESENT (used in write only)

		if (rdstatus!=MFS_STATUS_OK) {
			if (rdstatus!=MFS_ERROR_LOCKED && rdstatus!=MFS_ERROR_EAGAIN) {
				if (rdstatus==MFS_ERROR_ENOENT || rdstatus==MFS_ERROR_EPERM || rdstatus==MFS_ERROR_NOCHUNK) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,EBADF,0);
				} else if (rdstatus==MFS_ERROR_INDEXTOOBIG) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,EINVAL,0);
				} else if (rdstatus==MFS_ERROR_QUOTA) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
#ifdef EDQUOT
					read_job_end(rreq,EDQUOT,0);
#else
					read_job_end(rreq,ENOSPC,0);
#endif
				} else if (rdstatus==MFS_ERROR_NOSPACE && erroronnospace) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,ENOSPC,0);
				} else if (rdstatus==MFS_ERROR_CHUNKLOST && erroronlostchunk) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,ENXIO,0);
				} else if (rdstatus==MFS_ERROR_IO) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,EIO,0);
				} else {
					zassert(pthread_mutex_lock(&(ind->lock)));
					if (trycnt >= minlogretry) {
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					}
					ind->trycnt++;
					trycnt = ind->trycnt;
					if (trycnt >= maxretries) {
						zassert(pthread_mutex_unlock(&(ind->lock)));
						if (rdstatus==MFS_ERROR_NOCHUNKSERVERS || rdstatus==MFS_ERROR_NOSPACE) {
							read_job_end(rreq,ENOSPC,0);
						} else if (rdstatus==MFS_ERROR_CSNOTPRESENT || rdstatus==MFS_ERROR_CHUNKLOST) {
							read_job_end(rreq,ENXIO,0);
						} else {
							read_job_end(rreq,EIO,0);
						}
					} else {
						if (rreq->mode == BREAK) {
							zassert(pthread_mutex_unlock(&(ind->lock)));
							read_job_end(rreq,0,0);
						} else {
							rreq->mode = INQUEUE;
							zassert(pthread_mutex_unlock(&(ind->lock)));
							read_delayed_enqueue(rreq,1000+((trycnt<30)?((trycnt-1)*300000):10000000));
						}
					}
				}
			} else {
				zassert(pthread_mutex_lock(&(ind->lock)));
				if (rreq->mode == BREAK) {
					zassert(pthread_mutex_unlock(&(ind->lock)));
					read_job_end(rreq,0,0);
				} else {
					rreq->mode = INQUEUE;
					if (ind->trycnt<=6) {
						ind->trycnt++;
					}
					trycnt = ind->trycnt;
					zassert(pthread_mutex_unlock(&(ind->lock)));
					read_delayed_enqueue(rreq,(trycnt<=2)?1000:(trycnt<=6)?100000:500000);
				}
			}
			chunkrwlock_runlock(inode,chindx);
			continue;	// get next job
		}

//		now = monotonic_seconds();
//		fprintf(stderr,"fs_readchunk time: %.3lf\n",now-start);
		if (chunkid==0 && version==0) { // empty chunk
			zassert(pthread_mutex_lock(&(ind->lock)));
			if (rreq->mode == BREAK) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
			} else {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; mfleng: %"PRIu64" (empty chunk)\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode,ind->fleng);
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
				zassert(pthread_mutex_unlock(&(ind->lock)));
				if (chunksdatacache_check(inode,chindx,chunkid,version)==0) {
					zassert(pthread_mutex_lock(&(ind->lock)));

#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk changed\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode,chunkid,version);
#endif
					rreq->currentpos = 0;
					rreq->mode = REFRESH;
					zassert(pthread_mutex_unlock(&(ind->lock)));
				} else {
					zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk ok\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode,chunkid,version);
#endif
					rreq->mode = FILLED;
					rreq->modified = monotonic_seconds();
					zassert(pthread_mutex_unlock(&(ind->lock)));
				}
			}
			read_job_end(rreq,0,0);
			chunkrwlock_runlock(inode,chindx);

			continue;
		}

		if (csdata!=NULL && csdatasize>0) {
			chainelements = csorder_sort(chain,csdataver,csdata,csdatasize,0);
		} else {
			chainelements = 0;
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
			zassert(pthread_mutex_lock(&(ind->lock)));
			if (ind->trycnt >= minlogretry) {
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - there are no valid copies",inode,chindx,chunkid,version);
			}
			ind->trycnt++;
			if (ind->trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,ENXIO,0);
			} else if (rreq->mode == BREAK) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,0,0);
			} else {
				rreq->mode = INQUEUE;
				zassert(pthread_mutex_unlock(&(ind->lock)));
				chunksdatacache_invalidate(inode,chindx);
				read_delayed_enqueue(rreq,10000000);
			}
			chunkrwlock_runlock(inode,chindx);
			continue;
		}

		ip = chain[0].ip;
		port = chain[0].port;
		csver = chain[0].version;
/*
		if (ind->lastchunkid==chunkid) {
			if (ind->laststatus==0) { // error occurred
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
			zassert(pthread_mutex_lock(&(ind->lock)));
			if (ind->trycnt >= minlogretry) {
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - there are no valid copies (bad ip and/or port)",inode,chindx,chunkid,version);
			}
			ind->trycnt++;
			if (ind->trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,ENXIO,0);
			} else if (rreq->mode == BREAK) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,0,0);
			} else {
				rreq->mode = INQUEUE;
				zassert(pthread_mutex_unlock(&(ind->lock)));
				chunksdatacache_invalidate(inode,chindx);
				read_delayed_enqueue(rreq,10000000);
			}
			chunkrwlock_runlock(inode,chindx);
			continue;
		}

		start = monotonic_seconds();


		// make connection to cs
		srcip = fs_getsrcip();
		fd = conncache_get(ip,port);
		if (fd<0) {
			uint32_t connmaxtry;
			zassert(pthread_mutex_lock(&(ind->lock)));
			connmaxtry = (ind->trycnt*2)+2;
			if (connmaxtry>10) {
				connmaxtry = 10;
			}
			zassert(pthread_mutex_unlock(&(ind->lock)));
			cnt=0;
			while (cnt<connmaxtry) {
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
					if (cnt>=connmaxtry) {
						int err = errno;
						zassert(pthread_mutex_lock(&(ind->lock)));
						if (ind->trycnt >= minlogretry) {
							read_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"readworker: can't connect to (%s:%"PRIu16"): %s",csstrip,port,strerr(err));
						}
						zassert(pthread_mutex_unlock(&(ind->lock)));
					}
					close(fd);
					fd=-1;
				} else {
					cnt=connmaxtry;
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
			} else if (rreq->mode == BREAK) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				read_job_end(rreq,0,0);
			} else {
				rreq->mode = INQUEUE;
				zassert(pthread_mutex_unlock(&(ind->lock)));
				chunksdatacache_invalidate(inode,chindx);
				read_delayed_enqueue(rreq,1000+((trycnt<30)?((trycnt-1)*300000):10000000));
			}
			chunkrwlock_runlock(inode,chindx);
			continue;
		}
		if (tcpnodelay(fd)<0) {
			syslog(LOG_WARNING,"readworker: can't set TCP_NODELAY: %s",strerr(errno));
		}

		pfd[0].fd = fd;
		pfd[1].fd = pipefd[0];
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
			fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; start position: %"PRIu32"\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,currentpos);
		}
#endif

#ifdef RDEBUG
		fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; mfleng: %"PRIu64"\n",monotonic_seconds(),inode,ind->fleng);
#endif
		zassert(pthread_mutex_unlock(&(ind->lock)));

		reccmd = 0; // makes gcc happy
		recleng = 0; // makes gcc happy
		notdone = 0;

		do {
			now = monotonic_seconds();

			zassert(pthread_mutex_lock(&(ind->lock)));

#ifdef RDEBUG
			fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; loop entry point ; currentpos: %"PRIu32"\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,currentpos);
#endif


			mfleng = ind->fleng;

			if (rreq->mode == BREAK) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				status = EINTR;
				currentpos = 0;
				break;
			}

			if (reqsend && gotstatus) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				if (chunksdatacache_check(inode,chindx,chunkid,version)==0) {
					zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk changed\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode,chunkid,version);
#endif
					currentpos = 0;
					rreq->mode = REFRESH;
					zassert(pthread_mutex_unlock(&(ind->lock)));
				} else {
					zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker (rreq: %"PRIu64":%"PRIu64"/%"PRIu32") inode: %"PRIu32" ; chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk ok\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,inode,chunkid,version);
#endif
					rreq->mode = FILLED;
					rreq->modified = monotonic_seconds();
//					if (rreq->waiting>0) {
//					}
					zassert(pthread_mutex_unlock(&(ind->lock)));
				}
				break;
			}

			if (lastrcvd==0.0) {
				lastrcvd = now;
			} else {
				lrdiff = now - lastrcvd;
				if (lrdiff>=CHUNKSERVER_ACTIVITY_TIMEOUT) {
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; currentpos: %"PRIu32" - time out (lrdiff:%.6lf)\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,currentpos,lrdiff);
#endif
					if (trycnt >= minlogretry) {
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was timed out (lastrcvd:%.6lf,now:%.6lf,lrdiff:%.6lf received: %"PRIu32"/%"PRIu32", try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,lastrcvd,now,lrdiff,currentpos,rreq->rleng,trycnt+1);
					}
					status = EIO;
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

			rreq->waitingworker = 1;
			rreq->wakeup_fd = pipefd[1];
			zassert(pthread_mutex_unlock(&(ind->lock)));

			if (tosend==0 && (now - lastsend > (CHUNKSERVER_ACTIVITY_TIMEOUT/2.0))) {
				wptr = sendbuff;
				put32bit(&wptr,ANTOAN_NOP);
				put32bit(&wptr,0);
				tosend = 8;
				sent = 0;
			}

			if (tosend>0) {
				i = universal_write(fd,sendbuff+sent,tosend-sent);
				if (i<0) { // error
					if (ERRNO_ERROR && errno!=EINTR) {
						if (trycnt >= minlogretry) {
							int err = errno;
							read_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: write to (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(err),currentpos,rleng,trycnt+1);
						}
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; write error: %s\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,strerr(errno));
#endif
						status = EIO;
						zassert(pthread_mutex_lock(&(ind->lock)));
						rreq->waitingworker = 0;
						rreq->wakeup_fd = -1;
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
					if (trycnt >= minlogretry) {
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: poll error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,strerr(errno),currentpos,rleng,trycnt+1);
					}
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; poll error: %s\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,strerr(errno));
#endif
					status = EIO;
					break;
				}
			}
			zassert(pthread_mutex_lock(&(ind->lock)));
			rreq->waitingworker = 0;
			rreq->wakeup_fd = -1;
			closing = (ind->closing>0)?1:0;
			mode = rreq->mode;
			zassert(pthread_mutex_unlock(&(ind->lock)));
			if (pfd[1].revents&POLLIN) { // used just to break poll - so just read all data from pipe to empty it
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker: %"PRIu32" woken up by pipe\n",monotonic_seconds(),inode);
#endif
				i = universal_read(pipefd[0],pipebuff,1024);
				if (i<0) { // mainly to make happy static code analyzers
					if (trycnt >= minlogretry) {
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: read pipe error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,strerr(errno),currentpos,rleng,trycnt+1);
					}
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
				if (trycnt >= minlogretry) {
					read_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / POLLHUP (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
				}
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; connection got error status / POLLHUP\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng);
#endif
				status = EIO;
				break;
			}
			if (pfd[0].revents&POLLERR) {
				if (trycnt >= minlogretry) {
					read_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") got error status / POLLERR (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
				}
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; connection got error status / POLLERR\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng);
#endif
				status = EIO;
				break;
			}
			if (pfd[0].revents&POLLIN) {
				lastrcvd = monotonic_seconds();
				if (received < 8) {
					i = universal_read(fd,recvbuff+received,8-received);
					if (i==0) {
						if (trycnt >= minlogretry) {
							read_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / ZEROREAD (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
						}
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; connection reset by peer\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng);
#endif
						status = EIO;
						break;
					}
					if (i<0 && ERRNO_ERROR) {
						if (trycnt >= minlogretry) {
							int err = errno;
							read_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: read from (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(err),currentpos,rleng,trycnt+1);
						}
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; connection error: %s\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,strerr(errno));
#endif
						status = EIO;
						break;
					}
					if (i<0) {
						i = 0;
					}
					received += i;
					if (received == 8) { // full header
						rptr = recvbuff;

						reccmd = get32bit(&rptr);
						recleng = get32bit(&rptr);
						if (reccmd==CSTOCL_READ_STATUS) {
							if (recleng!=9) {
								syslog(LOG_WARNING,"readworker: got wrong sized status packet from chunkserver (leng:%"PRIu32")",recleng);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got wrong sized status packet from chunkserver (leng:%"PRIu32")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,recleng);
#endif
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
						} else if (reccmd==CSTOCL_READ_DATA) {
							if (recleng<20) {
								syslog(LOG_WARNING,"readworker: got too short data packet from chunkserver (leng:%"PRIu32")",recleng);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got too short data packet from chunkserver (leng:%"PRIu32")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,recleng);
#endif
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							} else if ((recleng-20) + currentpos > rleng) {
								syslog(LOG_WARNING,"readworker: got too long data packet from chunkserver (leng:%"PRIu32")",recleng);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got too long data packet from chunkserver (leng:%"PRIu32")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,recleng);
#endif
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
						} else if (reccmd==ANTOAN_NOP) {
							if (recleng!=0) {
								syslog(LOG_WARNING,"readworker: got wrong sized nop packet from chunkserver (leng:%"PRIu32")",recleng);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got wrong sized nop packet from chunkserver (leng:%"PRIu32")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,recleng);
#endif
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
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got unrecognized packet from chunkserver (cmd:%"PRIu32",leng:%"PRIu32")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,reccmd,recleng);
#endif
							status = EIO;
							currentpos = 0; // start again from beginning
							break;
						}
					}
				}
				if (received >= 8) {
					if (recleng<=20) {
						i = universal_read(fd,recvbuff + (received-8),recleng - (received-8));
					} else {
						if (received < 8 + 20) {
#ifdef HAVE_READV
							siov[0].iov_base = recvbuff + (received-8);
							siov[0].iov_len = 20 - (received-8);
							siov[1].iov_base = rreq->data + currentpos;
							siov[1].iov_len = recleng - 20;
							i = readv(fd,siov,2);
#else
							i = universal_read(fd,recvbuff + (received-8),20 - (received-8));
#endif
						} else {
							i = universal_read(fd,rreq->data + currentpos,recleng - (received-8));
						}
					}
					if (i==0) {
						if (trycnt >= minlogretry) {
							read_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,currentpos,rleng,trycnt+1);
						}
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; connection reset by peer\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng);
#endif
						status = EIO;
						break;
					}
					if (i<0 && ERRNO_ERROR) {
						if (trycnt >= minlogretry) {
							int err = errno;
							read_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(err),currentpos,rleng,trycnt+1);
						}
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; connection error: %s\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,strerr(errno));
#endif
						status = EIO;
						break;
					}
					if (i<0) {
						i = 0;
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
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; internal error - received more bytes than expected\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng);
#endif
						status = EIO;
						currentpos = 0; // start again from beginning
						break;
					} else if (received == 8+recleng) {

						if (reccmd==CSTOCL_READ_STATUS) {
							rptr = recvbuff;
							recchunkid = get64bit(&rptr);
							recstatus = get8bit(&rptr);
							if (recchunkid != chunkid) {
								syslog(LOG_WARNING,"readworker: got unexpected status packet (expected chunkdid:%016"PRIX64",packet chunkid:%016"PRIX64")",chunkid,recchunkid);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got unexpected status packet (expected chunkdid:%016"PRIX64",packet chunkid:%016"PRIX64")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,chunkid,recchunkid);
#endif
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							if (recstatus!=MFS_STATUS_OK) {
								if (trycnt >= minlogretry) {
									syslog(LOG_WARNING,"readworker: read error: %s",mfsstrerr(recstatus));
								}
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; read error: %s\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,mfsstrerr(recstatus));
#endif
								status = EIO;
								if (recstatus==MFS_ERROR_NOTDONE) {
									notdone = 1;
									// in such case do not reset pos
								} else {
									currentpos = 0; // start again from beginning
								}
								break;
							}
							if (currentpos != rleng) {
								syslog(LOG_WARNING,"readworker: unexpected data block size (requested: %"PRIu32" / received: %"PRIu32")",rleng,currentpos);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; unexpected data block size (requested: %"PRIu32" / received: %"PRIu32")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,rleng,currentpos);
#endif
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
								syslog(LOG_WARNING,"readworker: got unexpected data packet (expected chunkdid:%016"PRIX64",packet chunkid:%016"PRIX64")",chunkid,recchunkid);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got unexpected data packet (expected chunkdid:%016"PRIX64",packet chunkid:%016"PRIX64")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,chunkid,recchunkid);
#endif
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							if (recsize+20 != recleng) {
								syslog(LOG_WARNING,"readworker: got malformed data packet (datasize: %"PRIu32",packetsize: %"PRIu32")",recsize,recleng);
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; got malformed data packet (datasize: %"PRIu32",packetsize: %"PRIu32")\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,recsize,recleng);
#endif
								status = EIO;
								currentpos = 0; // start again from beginning
								break;
							}
							if (reccrc != mycrc32(0,rreq->data + (currentpos - recsize),recsize)) {
								syslog(LOG_WARNING,"readworker: data checksum error");
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; data checksum error\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng);
#endif
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

#ifdef RDEBUG
		fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; loop has ended ; currentpos: %"PRIu32" ; status: %s\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,currentpos,strerr(status));
#endif

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
			if (notdone==0) {
				ind->trycnt++;
			}
			trycnt = ind->trycnt;
			if (trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				chunksdatacache_invalidate(inode,chindx);
				read_job_end(rreq,status,0);
			} else {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				chunksdatacache_invalidate(inode,chindx);
				if (notdone) {
					read_job_end(rreq,0,300000);
				} else {
					read_job_end(rreq,0,(trycnt<3)?0:(1000+((trycnt<30)?((trycnt-3)*300000):10000000)));
				}
			}
		} else {
			zassert(pthread_mutex_unlock(&(ind->lock)));
			read_job_end(rreq,0,0);
		}
		chunkrwlock_runlock(inode,chindx);
	}
	return NULL;
}

void read_data_ranges_free(void* ptr) {
	if (ptr!=NULL) {
		free(ptr);
	}
}

void read_data_init (uint64_t readaheadsize,uint32_t readaheadleng,uint32_t readaheadtrigger,uint32_t retries,uint32_t timeout,uint32_t logretry,uint8_t erronlostchunk,uint8_t erronnospace) {
	uint32_t i;
	size_t mystacksize;
//	sigset_t oldset;
//	sigset_t newset;

	maxretries = retries;
	usectimeout = timeout;
	usectimeout *= 1000000;
	minlogretry = logretry;
	readahead_leng = readaheadleng;
	readahead_trigger = readaheadtrigger;
	maxreadaheadsize = readaheadsize;
	erroronlostchunk = erronlostchunk;
	erroronnospace = erronnospace;
	reqbufftotalsize = 0;

	zassert(pthread_key_create(&rangesstorage,read_data_ranges_free));
	zassert(pthread_setspecific(rangesstorage,NULL));

#ifndef HAVE___SYNC_OP_AND_FETCH
	zassert(pthread_mutex_init(&buffsizelock,NULL));
#endif
	zassert(pthread_mutex_init(&workers_lock,NULL));
	zassert(pthread_mutex_init(&inode_lock,NULL));
	zassert(pthread_cond_init(&worker_term_cond,NULL));
//	worker_term_waiting = 0;

	indhash = malloc(sizeof(inodedata*)*IDHASHSIZE);
	passert(indhash);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		indhash[i]=NULL;
	}

//	dqueue = queue_new(0);
	jqueue = queue_new(0);

	zassert(pthread_attr_init(&worker_thattr));
#ifdef PTHREAD_STACK_MIN
	mystacksize = PTHREAD_STACK_MIN;
	if (mystacksize < 0x20000) {
		mystacksize = 0x20000;
	}
#else
	mystacksize = 0x20000;
#endif
	zassert(pthread_attr_setstacksize(&worker_thattr,mystacksize));

//	sigemptyset(&newset);
//	sigaddset(&newset, SIGTERM);
//	sigaddset(&newset, SIGINT);
//	sigaddset(&newset, SIGHUP);
//	sigaddset(&newset, SIGQUIT);
//	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
//	zassert(pthread_create(&dqueue_worker_th,&worker_thattr,read_dqueue_worker,NULL));
//	pthread_sigmask(SIG_SETMASK, &oldset, NULL);

	zassert(pthread_mutex_lock(&workers_lock));
	workers_avail = 0;
	workers_total = 0;
	read_data_spawn_worker();
	zassert(pthread_mutex_unlock(&workers_lock));

//	read_chunkdata_init();
//	fprintf(stderr,"spawn worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);

//#ifdef BUFFER_DEBUG
//	pthread_create(&info_worker_th,&thattr,read_info_worker,NULL);
//#endif
//	for (i=0 ; i<WORKERS ; i++) {
//		zassert(pthread_create(read_worker_th+i,&thattr,read_worker,(void*)(unsigned long)(i)));
//	}
}

void read_data_term(void) {
	uint32_t i;
	inodedata *ind,*indn;

//	queue_close(dqueue);
	queue_close(jqueue);
	zassert(pthread_mutex_lock(&workers_lock));
	while (workers_total>0) {
//		worker_term_waiting++;
		zassert(pthread_cond_wait(&worker_term_cond,&workers_lock));
	}
	zassert(pthread_mutex_unlock(&workers_lock));
//	zassert(pthread_join(dqueue_worker_th,NULL));
//	queue_delete(dqueue);
	queue_delete(jqueue);
	zassert(pthread_mutex_lock(&inode_lock));
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
	zassert(pthread_mutex_unlock(&inode_lock));
	zassert(pthread_attr_destroy(&worker_thattr));
	zassert(pthread_cond_destroy(&worker_term_cond));
	zassert(pthread_mutex_destroy(&workers_lock));
	zassert(pthread_mutex_destroy(&inode_lock));
#ifndef HAVE___SYNC_OP_AND_FETCH
	zassert(pthread_mutex_destroy(&buffsizelock));
#endif
	zassert(pthread_key_delete(rangesstorage));

//	read_chunkdata_term();
}




typedef struct rlist_s {
	rrequest *rreq;
	uint32_t offsetadd;
	uint32_t reqleng;
	struct rlist_s *next;
} rlist;

static inline rrequest* read_rreq_invalidate(rrequest *rreq) {
	if (!STATE_BG_JOBS(rreq->mode)) {
		if (rreq->lcnt==0) {
			read_delete_request(rreq); // nobody wants it anymore, so delete it
			return NULL;
		} else if (rreq->mode==READY) {
			rreq->mode = NOTNEEDED; // somebody still using it, so mark it for removal
		}
	} else {
		if (rreq->lcnt==0) {
			rreq->mode = BREAK;
		} else if (rreq->mode!=INQUEUE) {
			rreq->mode = REFRESH;
		} else {
			return rreq; // INQUEUE && lcnt>0
		}
		if (rreq->waitingworker) {
			if (universal_write(rreq->wakeup_fd," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			rreq->waitingworker = 0;
			rreq->wakeup_fd = -1;
		}
	}
	return rreq;
}

static inline void read_rreq_not_needed(rrequest *rreq) {
	if (!STATE_BG_JOBS(rreq->mode)) {
		if (rreq->lcnt==0) {
			read_delete_request(rreq); // nobody wants it anymore, so delete it
		} else if (rreq->mode==READY) {
			rreq->mode = NOTNEEDED; // somebody still using it, so mark it for removal
		}
	} else {
		if (rreq->lcnt==0) {
			rreq->mode = BREAK;
			if (rreq->waitingworker) {
				if (universal_write(rreq->wakeup_fd," ",1)!=1) {
					syslog(LOG_ERR,"can't write to pipe !!!");
				}
				rreq->waitingworker = 0;
				rreq->wakeup_fd = -1;
			}
		}
	}
}

int read_data_offset_cmp(const void *aa,const void *bb) {
	uint64_t a = *((const uint64_t*)aa);
	uint64_t b = *((const uint64_t*)bb);
	return (a<b)?-1:(a>b)?1:0;
}

static inline void read_inode_free(uint32_t indh,inodedata *indf) {
	inodedata *ind,**indp;
	
	indp = &(indhash[indh]);
	while ((ind=*indp)) {
		if (ind==indf) {
			*indp = ind->next;
			zassert(pthread_mutex_lock(&(ind->lock)));
			zassert(pthread_mutex_unlock(&(ind->lock)));
			zassert(pthread_cond_destroy(&(ind->readerscond)));
			zassert(pthread_cond_destroy(&(ind->writerscond)));
			zassert(pthread_cond_destroy(&(ind->closecond)));
			zassert(pthread_mutex_destroy(&(ind->lock)));
			free(ind);
			return;
		}
		indp = &(ind->next);
	}
}

// return list of rreq
int read_data(void *vid, uint64_t offset, uint32_t *size, void **vrhead,struct iovec **iov,uint32_t *iovcnt) {
	inodedata *ind = (inodedata*)vid;
	rrequest *rreq,*rreqn;
	rlist *rl,*rhead,**rtail;
	uint64_t rbuffsize;
	uint64_t blockstart,blockend;
	uint64_t firstbyte;
	uint64_t lastbyte;
	uint32_t cnt;
	uint32_t edges,i,reqno;
	uint8_t added,raok;
	uint64_t *etab,*ranges;
	int status;
	double now;

	zassert(pthread_mutex_lock(&inode_lock));
	ind->lcnt++;
	zassert(pthread_mutex_unlock(&inode_lock));

	zassert(pthread_mutex_lock(&(ind->lock)));

	while (ind->waiting_writers>0) {
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: read_data: wait (waiting_writers:%"PRIu16")\n",monotonic_seconds(),ind->waiting_writers);
#endif
		zassert(pthread_cond_wait(&(ind->readerscond),&(ind->lock)));
	}

	ind->readers_cnt++;

	*vrhead = NULL;
	*iov = NULL;
	*iovcnt = 0;
	cnt = 0;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" ind->status: %d ind->closing: %"PRIu8"\n",monotonic_seconds(),ind->inode,ind->status,ind->closing);
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
			if (offset+(readahead_leng/2) < ind->lastoffset || ind->lastoffset+(readahead_leng/2) < offset) {
				if (ind->readahead>0) {
					ind->readahead--;
				}
				ind->seqdata = 0;
			}
		}
		if (ind->readahead > 1 && rbuffsize >= (maxreadaheadsize / 2) + ((maxreadaheadsize * 1) / (ind->readahead * 2))) {
			ind->readahead--;
			ind->seqdata = 0;
		}
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" seqdata: %"PRIu32" offset: %"PRIu64" ind->lastoffset: %"PRIu64" ind->readahead: %u reqbufftotalsize:%"PRIu64"\n",monotonic_seconds(),ind->inode,ind->seqdata,offset,ind->lastoffset,ind->readahead,rbuffsize);
#endif

		firstbyte = offset;
		lastbyte = offset + (*size);
		if (master_version()>=VERSION2INT(3,0,74)) {
			if (firstbyte > ind->fleng) {
				firstbyte = ind->fleng;
			}
			if (lastbyte > ind->fleng) {
				lastbyte = ind->fleng;
			}
		}
		now = monotonic_seconds();

		// cleanup unused requests
		reqno = 0;
		for (rreq = ind->reqhead ; rreq ; rreq=rreq->next) {
			if (!STATE_NOT_NEEDED(rreq->mode)) {
				reqno++;
			}
		}
		rreq = ind->reqhead;
		while (rreq) {
			rreqn = rreq->next;
			if (STATE_NOT_NEEDED(rreq->mode)) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" ignored request mode (%"PRIu64":%"PRIu64"/%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				read_rreq_not_needed(rreq);
			} else if (rreq->modified+BUFFER_VALIDITY_TIMEOUT<now) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" - data too old: free rreq (%"PRIu64":%"PRIu64"/%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				read_rreq_not_needed(rreq);
				reqno--;
			} else if ((lastbyte <= rreq->offset || firstbyte >= rreq->offset+rreq->leng) && reqno>3) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" - too many requests: free rreq (%"PRIu64":%"PRIu64"/%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
				read_rreq_not_needed(rreq);
				reqno--;
			}
			rreq = rreqn;
		}

		// split read block by request edges
		ranges = pthread_getspecific(rangesstorage);
		if (ranges==NULL) {
			ranges = malloc(sizeof(uint64_t)*11);
			passert(ranges);
			zassert(pthread_setspecific(rangesstorage,ranges));
			ranges[0] = 10;
		}
		etab = ranges+1;
		edges = 0;
		etab[edges++] = firstbyte;
		etab[edges++] = lastbyte;
		for (rreq = ind->reqhead ; rreq ; rreq=rreq->next) {
			if (!STATE_NOT_NEEDED(rreq->mode)) {
				if (rreq->offset > firstbyte && rreq->offset < lastbyte) {
					for (i=0 ; i<edges && etab[i]!=rreq->offset ; i++) {}
					if (i>=edges) {
						if (i>=ranges[0]) {
							ranges[0] += 10;
							ranges = mfsrealloc(ranges,sizeof(uint64_t)*(ranges[0]+1));
							passert(ranges);
							etab = ranges+1;
							zassert(pthread_setspecific(rangesstorage,ranges));
						}
						etab[edges++] = rreq->offset;
					}
				}
				if (rreq->offset+rreq->leng > firstbyte && rreq->offset+rreq->leng < lastbyte) {
					for (i=0 ; i<edges && etab[i]!=rreq->offset+rreq->leng ; i++) {}
					if (i>=edges) {
						if (i>=ranges[0]) {
							ranges[0] += 10;
							ranges = mfsrealloc(ranges,sizeof(uint64_t)*(ranges[0]+1));
							passert(ranges);
							etab = ranges+1;
							zassert(pthread_setspecific(rangesstorage,ranges));
						}
						etab[edges++] = rreq->offset+rreq->leng;
					}
				}
			}
		}
		if (edges>2) {
			qsort(etab,edges,sizeof(uint64_t),read_data_offset_cmp);
		}
		sassert(etab[0]==firstbyte);
		sassert(etab[edges-1]==lastbyte);
#ifdef RDEBUG
		for (i=0 ; i<edges-1 ; i++) {
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" ; read(%"PRIu64":%"PRIu64") ; range %u : (%"PRIu64":%"PRIu64")\n",monotonic_seconds(),ind->inode,firstbyte,lastbyte,i,etab[i],etab[i+1]);
		}
#endif

		// prepare requests

		rhead = NULL;
		rtail = &rhead;
		for (i=0 ; i<edges-1 ; i++) {
			added = 0;
			for (rreq = ind->reqhead ; rreq && added==0 ; rreq=rreq->next) {
				if (!STATE_NOT_NEEDED(rreq->mode)) {
					if (rreq->offset<=etab[i] && rreq->offset+rreq->leng>=etab[i+1]) {
						rl = malloc(sizeof(rlist));
						passert(rl);
						rl->rreq = rreq;
						rl->offsetadd = etab[i] - rreq->offset;
						rl->reqleng = etab[i+1] - etab[i];
						rl->next = NULL;
						*rtail = rl;
						rtail = &(rl->next);
						rreq->lcnt++;
						added = 1;
						if (ind->readahead && i==edges-2) {
							// request next block of data
							if (rreq->next==NULL && rbuffsize<maxreadaheadsize) {
								blockstart = rreq->offset+rreq->leng;
								blockend = blockstart + (readahead_leng * (1<<((ind->readahead-1)*2)));
								sassert(blockend>blockstart);
								raok = 1;
								for (rreqn = ind->reqhead ; rreqn && raok ; rreqn=rreqn->next) {
									if (!STATE_NOT_NEEDED(rreq->mode)) {
										if (!(blockend <= rreq->offset || blockstart >= rreq->offset+rreq->leng)) {
											raok = 0;
										}
									}
								}
								if (raok) {
									if (blockend<=ind->fleng) {
#ifdef RDEBUG
										fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (middle of existing block) add new read-ahead rreq (%"PRIu64":%"PRIu64"/%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,blockend,blockend-blockstart);
#endif
										read_new_request(ind,&blockstart,blockend);
									} else if (blockstart<ind->fleng) {
#ifdef RDEBUG
										fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (middle of existing block) add new read-ahead rreq (%"PRIu64":%"PRIu64"/%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,ind->fleng,ind->fleng-blockstart);
#endif
										read_new_request(ind,&blockstart,ind->fleng);
									}
								// and another one if necessary
									if ((blockstart % MFSCHUNKSIZE) == 0 && rreq->next!=NULL && rreq->next->next==NULL && rbuffsize<maxreadaheadsize) {
										blockend = blockstart + (readahead_leng * (1<<((ind->readahead-1)*2)));
										sassert(blockend>blockstart);
										raok = 1;
										for (rreqn = ind->reqhead ; rreqn && raok ; rreqn=rreqn->next) {
											if (!STATE_NOT_NEEDED(rreq->mode)) {
												if (!(blockend <= rreq->offset || blockstart >= rreq->offset+rreq->leng)) {
													raok = 0;
												}
											}
										}
										if (raok) {
											if (blockend<=ind->fleng) {
#ifdef RDEBUG
												fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (middle of existing block) add new extra read-ahead rreq (%"PRIu64":%"PRIu64"/%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,blockend,blockend-blockstart);
#endif
												read_new_request(ind,&blockstart,blockend);
											} else if (blockstart<ind->fleng) {
#ifdef RDEBUG
												fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (middle of existing block) add new extra read-ahead rreq (%"PRIu64":%"PRIu64"/%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,ind->fleng,ind->fleng-blockstart);
#endif
												read_new_request(ind,&blockstart,ind->fleng);
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (added==0) { // new request
				blockstart = etab[i];
				blockend = etab[i+1];
				while (blockstart < blockend) {
					rreq = read_new_request(ind,&blockstart,blockend);
					rl = malloc(sizeof(rlist));
					passert(rl);
					rl->rreq = rreq;
					rl->offsetadd = 0;
					rl->reqleng = rreq->leng;
					rl->next = NULL;
					*rtail = rl;
					rtail = &(rl->next);
					rreq->lcnt++;
					if (blockstart==blockend && ind->readahead && rbuffsize<maxreadaheadsize && i==edges-2) {
						blockend = blockstart + (readahead_leng * (1<<((ind->readahead-1)*2)))/2;
						sassert(blockend>blockstart);
						raok = 1;
						for (rreqn = ind->reqhead ; rreqn && raok ; rreqn=rreqn->next) {
							if (!STATE_NOT_NEEDED(rreq->mode)) {
								if (!(blockend <= rreq->offset || blockstart >= rreq->offset+rreq->leng)) {
									raok = 0;
								}
							}
						}
						if (raok) {
							if (blockend<=ind->fleng) {
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (after new block) add new read-ahead rreq (%"PRIu64":%"PRIu64"/%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,blockend,blockend-blockstart);
#endif
								(void)read_new_request(ind,&blockstart,blockend);
							} else if (blockstart<ind->fleng) {
#ifdef RDEBUG
								fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" (after new block) add new read-ahead rreq (%"PRIu64":%"PRIu64"/%"PRId64")\n",monotonic_seconds(),ind->inode,blockstart,ind->fleng,ind->fleng-blockstart);
#endif
								(void)read_new_request(ind,&blockstart,ind->fleng);
							}
						}
						break;
					}
				}
			}
		}

		*vrhead = rhead;

		cnt = 0;
		*size = 0;
		for (rl=rhead ; rl ; rl=rl->next) {
			while (!STATE_HAVE_VALID_DATA(rl->rreq->mode) && ind->status==0 && ind->closing==0) {
//				rl->rreq->waiting++;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" wait for data: %"PRIu64":%"PRIu64"/%"PRIu32"\n",monotonic_seconds(),ind->inode,rl->rreq->offset,rl->rreq->offset+rl->rreq->leng,rl->rreq->leng);
#endif
				if (usectimeout>0) {
					struct timespec ts;
					struct timeval tv;
					uint64_t usecto;
					gettimeofday(&tv, NULL);
					usecto = tv.tv_sec;
					usecto *= 1000000;
					usecto += tv.tv_usec;
					usecto += usectimeout;
					ts.tv_sec = usecto / 1000000;
					ts.tv_nsec = (usecto % 1000000) * 1000;
					if (pthread_cond_timedwait(&(rl->rreq->cond),&(ind->lock),&ts)==ETIMEDOUT) {
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" ; block: %"PRIu64":%"PRIu64"/%"PRIu32" - timed out\n",monotonic_seconds(),ind->inode,rl->rreq->offset,rl->rreq->offset+rl->rreq->leng,rl->rreq->leng);
#endif
						ind->status=EIO;
					}
				} else {
					zassert(pthread_cond_wait(&(rl->rreq->cond),&(ind->lock)));
				}
//				rl->rreq->waiting--;
			}
			if (ind->status==0) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" block %"PRIu64":%"PRIu64"/%"PRIu32" (data in block: %"PRIu32") has been read\n",monotonic_seconds(),ind->inode,rl->rreq->offset,rl->rreq->offset+rl->rreq->leng,rl->rreq->leng,rl->rreq->rleng);
#endif
				if (rl->rreq->rleng < rl->offsetadd + rl->reqleng) {
					if (rl->rreq->rleng > rl->offsetadd) {
						cnt++;
						rl->reqleng = rl->rreq->rleng - rl->offsetadd;
						*size += rl->reqleng;
					}
					break; // end of file
				} else {
					cnt++;
					*size += rl->reqleng;
				}
			} else {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" error reading block: %"PRIu64":%"PRIu64"/%"PRIu32"\n",monotonic_seconds(),ind->inode,rl->rreq->offset,rl->rreq->offset+rl->rreq->leng,rl->rreq->leng);
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
		for (rl=rhead, i=0 ; i<cnt ; rl=rl->next, i++) {
			passert(rl);
			(*iov)[i].iov_base = rl->rreq->data + rl->offsetadd;
			(*iov)[i].iov_len = rl->reqleng;
		}
		*iovcnt = i;
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
		if (rreq->lcnt==0 && rreq->mode==NOTNEEDED) {
			read_delete_request(rreq);
		}
		free(rl);
		rl = rln;
	}
	if (iov) {
		free(iov);
	}
	if (ind->closing && ind->reqhead==NULL) {
		zassert(pthread_cond_broadcast(&(ind->closecond)));
	}
	ind->readers_cnt--;
	if (ind->waiting_writers>0 && ind->readers_cnt==0) {
		zassert(pthread_cond_signal(&(ind->writerscond)));
	}
//	if (ind->waiting_addfn_cnt>0 && ind->readcnt==0) {
//		zassert(pthread_cond_signal(&(ind->readcond)));
//	}
	
	zassert(pthread_mutex_unlock(&(ind->lock)));
	zassert(pthread_mutex_lock(&inode_lock));
	ind->lcnt--;
	if (ind->lcnt==0) {
		read_inode_free(IDHASH(ind->inode),ind);
	}
	zassert(pthread_mutex_unlock(&inode_lock));
}

void read_inode_clear_cache(uint32_t inode,uint64_t offset,uint64_t leng) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;
	rrequest *rreq,*rreqn;
	zassert(pthread_mutex_lock(&inode_lock));
	for (ind = indhash[indh] ; ind ; ind=ind->next) {
		if (ind->inode == inode) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			for (rreq = ind->reqhead ; rreq ; rreq=rreqn) {
				rreqn = rreq->next;
				if ((leng==0 && offset < rreq->offset + rreq->leng) || (offset+leng > rreq->offset && rreq->offset+rreq->leng > offset)) {
					read_rreq_invalidate(rreq);
				}
			}
			zassert(pthread_mutex_unlock(&(ind->lock)));
		}
	}
	zassert(pthread_mutex_unlock(&inode_lock));
}

void read_data_set_length_active(inodedata *ind,uint64_t newlength) {
	rrequest *rreq,*rreqn;

	zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data_set_length_active: inode: %"PRIu32" set length: %"PRIu64"\n",monotonic_seconds(),ind->inode,newlength);
#endif
	ind->waiting_writers++;
	while (ind->readers_cnt!=0) {
		if (ind->fleng==newlength) {
			ind->waiting_writers--;
			zassert(pthread_mutex_unlock(&(ind->lock)));
			return;
		}
		pthread_cond_wait(&(ind->writerscond),&(ind->lock));
	}
	ind->waiting_writers--;
	ind->fleng = newlength;
	for (rreq = ind->reqhead ; rreq ; rreq=rreqn) {
		rreqn = rreq->next;
		sassert(rreq->lcnt==0);
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: read_data_set_length_active: rreq (before): (%"PRIu64":%"PRIu64"/%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
		rreq = read_rreq_invalidate(rreq);
		if (rreq) {
			fprintf(stderr,"%.6lf: read_data_set_length_active: rreq (after): (%"PRIu64":%"PRIu64"/%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),rreq->offset,rreq->offset+rreq->leng,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
		} else {
			fprintf(stderr,"%.6lf: read_data_set_length_active: rreq (after): NULL\n",monotonic_seconds());
		}
#else
		read_rreq_invalidate(rreq);
#endif
	}
	if (ind->closing && ind->reqhead==NULL) {
		zassert(pthread_cond_broadcast(&(ind->closecond)));
	}
	if (ind->waiting_writers>0) {
		zassert(pthread_cond_signal(&(ind->writerscond)));
	} else {
		zassert(pthread_cond_broadcast(&(ind->readerscond)));
	}
	zassert(pthread_mutex_unlock(&(ind->lock)));
}

void read_inode_set_length_active(uint32_t inode,uint64_t newlength) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind,*indn;
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_inode_set_length_active: inode: %"PRIu32" set length: %"PRIu64"\n",monotonic_seconds(),inode,newlength);
#endif
	zassert(pthread_mutex_lock(&inode_lock));
	for (ind = indhash[indh] ; ind ; ind=indn) {
		if (ind->inode == inode) {
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: read_inode_set_length_active: inode: %"PRIu32" set length: %"PRIu64" - found structure\n",monotonic_seconds(),inode,newlength);
#endif
			ind->lcnt++;
			zassert(pthread_mutex_unlock(&inode_lock));
			read_data_set_length_active(ind,newlength);
			zassert(pthread_mutex_lock(&inode_lock));
			indn = ind->next;
			ind->lcnt--;
			if (ind->lcnt==0) {
				read_inode_free(indh,ind);
			}
		} else {
			indn = ind->next;
		}
	}
	zassert(pthread_mutex_unlock(&inode_lock));
}

void read_inode_set_length_passive(uint32_t inode,uint64_t newlength) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;
	rrequest *rreq,*rreqn;
	uint64_t minfleng,maxfleng;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_inode_set_length_passive: inode: %"PRIu32" set length: %"PRIu64"\n",monotonic_seconds(),inode,newlength);
#endif
	zassert(pthread_mutex_lock(&inode_lock));
	for (ind = indhash[indh] ; ind ; ind=ind->next) {
		if (ind->inode == inode) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			if (ind->fleng != newlength) {
				if (ind->fleng < newlength) {
					minfleng = ind->fleng;
					maxfleng = newlength;
				} else {
					minfleng = newlength;
					maxfleng = ind->fleng;
				}
					
				for (rreq = ind->reqhead ; rreq ; rreq=rreqn) {
					rreqn = rreq->next;
					if ((rreq->offset < maxfleng) && (rreq->offset + rreq->leng > minfleng)) {
						read_rreq_invalidate(rreq);
					}
				}
				ind->fleng = newlength;
			}
			zassert(pthread_mutex_unlock(&(ind->lock)));
		}
	}
	zassert(pthread_mutex_unlock(&inode_lock));
}

void* read_data_new(uint32_t inode,uint64_t fleng) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;


	ind = malloc(sizeof(inodedata));
	passert(ind);
	ind->inode = inode;
	ind->seqdata = 0;
	ind->fleng = fleng;
	ind->status = 0;
	ind->trycnt = 0;
	ind->inqueue = 0;
	ind->readahead = 0;
	ind->lastoffset = 0;
//	ind->closewaiting = 0;
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
	ind->waiting_writers = 0;
	ind->readers_cnt = 0;
	zassert(pthread_cond_init(&(ind->readerscond),NULL));
	zassert(pthread_cond_init(&(ind->writerscond),NULL));
	zassert(pthread_cond_init(&(ind->closecond),NULL));
	zassert(pthread_mutex_init(&(ind->lock),NULL));
	ind->reqhead = NULL;
	ind->reqtail = &(ind->reqhead);
	zassert(pthread_mutex_lock(&inode_lock));
	ind->lcnt = 1;
	ind->next = indhash[indh];
	indhash[indh] = ind;
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: opening: %"PRIu32" ; inode_structure: %p\n",monotonic_seconds(),inode,(void*)ind);
//	read_data_hexdump((uint8_t*)ind,sizeof(inodedata));
#endif
	zassert(pthread_mutex_unlock(&inode_lock));
	return ind;
}

void read_data_end(void *vid) {
	uint32_t indh;
	inodedata *ind;
	rrequest *rreq,*rreqn;

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
	for (rreq = ind->reqhead ; rreq ; rreq = rreqn) {
		rreqn = rreq->next;
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: closing: %"PRIu32" ; free rreq (%"PRIu64":%"PRIu64"/%"PRIu32" ; lcnt:%u ; mode:%s)\n",monotonic_seconds(),ind->inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,rreq->lcnt,read_data_modename(rreq->mode));
#endif
		if (rreq->lcnt==0 && !STATE_BG_JOBS(rreq->mode)) {
#ifdef __clang_analyzer__
			uint8_t ca_ch = (rreq==ind->reqhead)?1:0;
#endif
			read_delete_request(rreq);
#ifdef __clang_analyzer__
			if (ca_ch) {
				ind->reqhead = rreqn;
			}
			// double linked list with pointer to pointer to previous 'next' is too much for clang analyzer
#endif
		}
	}
//	ind->closewaiting++;
	while (ind->reqhead!=NULL) {
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: closing: %"PRIu32" ; reqhead: %s ; inqueue: %u\n",monotonic_seconds(),ind->inode,ind->reqhead?"NOT NULL":"NULL",ind->inqueue);
#endif
		if (ind->reqhead->waitingworker) {
			if (universal_write(ind->reqhead->wakeup_fd," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			ind->reqhead->waitingworker = 0;
			ind->reqhead->wakeup_fd = -1;
		}
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: inode: %"PRIu32" ; waiting for close\n",monotonic_seconds(),ind->inode);
#endif
		zassert(pthread_cond_wait(&(ind->closecond),&(ind->lock)));
	}
//	ind->closewaiting--;
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: %"PRIu32" ; inode_structure: %p ; reqhead: %s ; inqueue: %u - delete structure\n",monotonic_seconds(),ind->inode,(void*)ind,ind->reqhead?"NOT NULL":"NULL",ind->inqueue);
#endif
	zassert(pthread_mutex_unlock(&(ind->lock)));
	zassert(pthread_mutex_lock(&inode_lock));
	ind->lcnt--;
	if (ind->lcnt==0) {
		read_inode_free(indh,ind);
	}
	zassert(pthread_mutex_unlock(&inode_lock));
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: inode_structure: %p - exit\n",monotonic_seconds(),(void*)ind);
#endif
}
