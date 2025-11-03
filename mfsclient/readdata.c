/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
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
#else
#define VERSION2INT(maj,med,min) ((maj)*0x10000+(med)*0x100+(((maj)>1)?((min)*2):(min)))
#endif

#ifdef HAVE_ATOMICS
#include <stdatomic.h>
#undef HAVE_ATOMICS
#define HAVE_ATOMICS 1
#else
#define HAVE_ATOMICS 0
#endif

#if defined(HAVE___SYNC_OP_AND_FETCH)
#define HAVE_SYNCS 1
#else
#define HAVE_SYNCS 0
#endif

#include <sys/types.h>
#include <stdio.h>
#ifdef HAVE_READV
#include <sys/uio.h>
#endif
#ifndef WIN32
#include <sys/time.h>
#include <unistd.h>
#include <poll.h>
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

#define CHUNKSERVER_ACTIVITY_TIMEOUT 5.0

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

#define COMBINE_CHUNKID_AND_ECID(chunkid,ecid) (((chunkid)&UINT64_C(0x00FFFFFFFFFFFFFF))|(((uint64_t)(ecid))<<56))

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
	uint32_t splitcurrpos[8];
	uint32_t chindx;
	uint32_t trycnt;
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

#if HAVE_ATOMICS
static _Atomic uint64_t total_bytes_rcvd;
#elif HAVE_SYNCS
static volatile uint64_t total_bytes_rcvd;
#else
static volatile uint64_t total_bytes_rcvd;
#define TOTAL_COUNT_USE_LOCK 1
static pthread_mutex_t total_bytes_lock;
#endif

static inline void read_increase_total_bytes(uint32_t v) {
#if HAVE_ATOMICS
	atomic_fetch_add(&total_bytes_rcvd,v);
#elif HAVE_SYNCS
	__sync_add_and_fetch(&total_bytes_rcvd,v);
#else
	zassert(pthread_mutex_lock(&total_bytes_lock));
	total_bytes_rcvd += v;
	zassert(pthread_mutex_unlock(&total_bytes_lock));
#endif
}

uint64_t read_get_total_bytes(void) {
	uint64_t v;
#if HAVE_ATOMICS
	v = atomic_fetch_and(&total_bytes_rcvd,0);
#elif HAVE_SYNCS
	v = __sync_fetch_and_and(&total_bytes_rcvd,0);
#else
	zassert(pthread_mutex_lock(&total_bytes_lock));
	v = total_bytes_rcvd;
	total_bytes_rcvd = 0;
	zassert(pthread_mutex_unlock(&total_bytes_lock));
#endif
	return v;
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
	rreq->currentpos = chunkoffset & MFSCHUNKMASK;
	memset(rreq->splitcurrpos,0,sizeof(rreq->splitcurrpos));
	rreq->mode = NEW;
	rreq->trycnt = 0;
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
		rreq->trycnt = 0;
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
	if (status!=0) {
		if (ind->closing==0) {
			errno = status;
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error reading file number %"PRIu32": %s",ind->inode,strerr(errno));
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
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"read workers: %"PRIu32"+",workers_total);
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
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"read workers: %"PRIu32"-",workers_total);
		lastnotify = workers_total;
	}
#endif
}

#ifdef RDEBUG
#define RDEBUG_READWORKER(msg,...) if (parts>1) { \
	fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; part: %u ; " msg "\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,part,__VA_ARGS__); \
} else { \
	fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; " msg "\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,__VA_ARGS__); \
}
#define RDEBUG_READWORKER_SIMPLE(msg) if (parts>1) { \
	fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; part: %u ; " msg "\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,part); \
} else { \
	fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; " msg "\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng); \
}
#define RDEBUG_READWORKER_COMMON(msg,...) fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu64"/%"PRIu32" ; " msg "\n",monotonic_seconds(),inode,rreq->offset,rreq->offset+rreq->leng,rreq->leng,__VA_ARGS__);
#endif

typedef enum _data_source_state {STATE_IDLE,STATE_CONNECTING,STATE_CONNECTED,STATE_ERROR} data_source_state;

typedef struct _data_source {
	int fd;
	uint32_t startpos;
	uint32_t currpos;
	uint32_t endpos;
	uint16_t port;
	uint32_t ip;
	uint32_t csver;
	uint8_t gotstatus;
//	uint8_t reqsend;
	uint32_t sent,tosend,received;
	double lastrcvd,lastsend;
	uint8_t recvbuff[20];
	uint8_t sendbuff[29];
	uint32_t reccmd;
	uint32_t recleng;
	data_source_state state;
} data_source;

void* read_worker(void *arg) {
	uint32_t z1,z2,z3;
	uint8_t *data;
	data_source datasrc[8];
	int i;
	struct pollfd pfd[1+8];
	uint8_t resetpos;
	uint32_t datacurrpos;
//	uint32_t sent,tosend,received,currentpos;
//	uint8_t recvbuff[20];
//	uint8_t sendbuff[29];
#ifdef HAVE_READV
	struct iovec siov[2];
#endif
	uint8_t pipebuff[1024];
	int pipefd[2];
	uint8_t *wptr;
	const uint8_t *rptr;

	uint32_t inode;
	uint32_t trycnt;
	uint32_t connmaxtry;
	uint32_t rleng;

	uint64_t recchunkid;
	uint16_t recblocknum;
	uint16_t recoffset;
	uint32_t recsize;
	uint32_t reccrc;

	uint8_t finished;
	uint8_t desc;
	uint8_t recstatus;
	uint8_t notdone;
	uint8_t readanything;

	cspri chain[100];
	uint16_t chainelements;

	uint8_t cnt;
	uint8_t cpart,part,parts;
	data_source_state connect_status;

	uint32_t chindx;
	uint32_t srcip;
	uint64_t mfleng;
	uint64_t chunkid;
	uint32_t version;
	const uint8_t *csdata;
	uint32_t csdatasize;
	uint8_t csdataver;
	uint8_t rdstatus;
	int status;
	char csstrip[STRIPSIZE];
	uint8_t reqsend;
	uint8_t closing;
	uint8_t mode;
	double start,now;
	double workingtime,lrdiff;
	double timeoutadd;
	uint8_t firsttime = 1;
	worker *w = (worker*)arg;

	inodedata *ind;
	rrequest *rreq;

	parts = 0;
	csstrip[0] = 0;

	if (pipe(pipefd)<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"pipe error: %s",strerr(errno));
		return NULL;
	}

	for (;;) {
		for (part=0 ; part<parts ; part++) {
			if (datasrc[part].ip || datasrc[part].port) {
				csdb_readdec(datasrc[part].ip,datasrc[part].port);
			}
		}
		parts = 0;

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
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - bad request state: %s (expected INQUEUE or BREAK)",ind->inode,rreq->chindx,read_data_modename(rreq->mode));
			rreq->mode = BUSY;
		}

		chindx = rreq->chindx;
		status = ind->status;
		inode = ind->inode;
		// rleng = rreq->leng;
		trycnt = rreq->trycnt;

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

		mfleng = 0;
		chunkid = 0;
		version = 0;
		csdataver = 0;
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
			RDEBUG_READWORKER_COMMON("chunksdatacache_find: mfleng: %"PRIu64" ; chunkid: %016"PRIX64" ; version: %"PRIu32" ; status:%"PRIu8,mfleng,chunkid,version,rdstatus)
#endif
		} else {
#ifdef RDEBUG
			RDEBUG_READWORKER_COMMON("%s","fs_readchunk (before)")
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
			RDEBUG_READWORKER_COMMON("fs_readchunk (after): mfleng: %"PRIu64" ; chunkid: %016"PRIX64" ; version: %"PRIu32" ; status:%"PRIu8,mfleng,chunkid,version,rdstatus)
#endif
		}

// rdstatus potential results:
// MFS_ERROR_NOCHUNK - internal error (can't be repaired)
// MFS_ERROR_ENOENT - internal error (wrong inode - can't be repaired)
// MFS_ERROR_EPERM - internal error (wrong inode - can't be repaired)
// MFS_ERROR_INDEXTOOBIG - requested file position is too big
// MFS_ERROR_CHUNKLOST - according to master chunk is definitely lost (all chunkservers are connected and chunk is not there)
// statuses that are here just in case:
// MFS_ERROR_QUOTA (used in write only)
// MFS_ERROR_NOSPACE (used in write only)
// MFS_ERROR_IO (for future use)
// MFS_ERROR_NOCHUNKSERVERS (used in write only)
// MFS_ERROR_CSNOTPRESENT (used in write only)

		if (rdstatus!=MFS_STATUS_OK) {
			if (rdstatus!=MFS_ERROR_LOCKED && rdstatus!=MFS_ERROR_EAGAIN) {
				if (rdstatus==MFS_ERROR_ENOENT || rdstatus==MFS_ERROR_EPERM || rdstatus==MFS_ERROR_NOCHUNK) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,EBADF,0);
				} else if (rdstatus==MFS_ERROR_INDEXTOOBIG) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,EINVAL,0);
				} else if (rdstatus==MFS_ERROR_QUOTA) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
#ifdef EDQUOT
					read_job_end(rreq,EDQUOT,0);
#else
					read_job_end(rreq,ENOSPC,0);
#endif
				} else if (rdstatus==MFS_ERROR_NOSPACE && erroronnospace) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,ENOSPC,0);
				} else if (rdstatus==MFS_ERROR_CHUNKLOST && erroronlostchunk) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,ENXIO,0);
				} else if (rdstatus==MFS_ERROR_IO) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					read_job_end(rreq,EIO,0);
				} else {
					zassert(pthread_mutex_lock(&(ind->lock)));
					if (trycnt >= minlogretry) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",inode,chindx,mfsstrerr(rdstatus));
					}
					rreq->trycnt++;
					trycnt = rreq->trycnt;
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
					if (rreq->trycnt<=6) {
						rreq->trycnt++;
					}
					trycnt = rreq->trycnt;
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
				RDEBUG_READWORKER_COMMON("mfleng: %"PRIu64" (empty chunk)",ind->fleng)
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
					RDEBUG_READWORKER_COMMON("chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk changed",chunkid,version)
#endif
					rreq->currentpos = rreq->offset & MFSCHUNKMASK;
					rreq->mode = REFRESH;
					zassert(pthread_mutex_unlock(&(ind->lock)));
				} else {
					zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
					RDEBUG_READWORKER_COMMON("chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk ok",chunkid,version)
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
			if (trycnt >= minlogretry) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - there are no valid copies",inode,chindx,chunkid,version);
			}
			rreq->trycnt++;
			trycnt = rreq->trycnt;
			if (trycnt>=maxretries) {
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

		if (csdataver==3) { // PARTS MODE
			if (chainelements!=8 && chainelements!=4) {
				zassert(pthread_mutex_lock(&(ind->lock)));
				if (trycnt >= minlogretry) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - split mode with wrong parts counter (%u)",inode,chindx,chunkid,version,chainelements);
				}
				rreq->trycnt++;
				trycnt = rreq->trycnt;
				if (trycnt>=maxretries) {
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
			parts = chainelements; // 4 or 8
		} else {
			parts = 1;
		}


		for (part=0 ; part<parts ; part++) {
			datasrc[part].ip = chain[part].ip;
			datasrc[part].port = chain[part].port;
			datasrc[part].csver = chain[part].version;
			if (datasrc[part].ip==0 && datasrc[part].port==0) {
				parts = 0;
			}
		}

		if (parts==0) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			if (trycnt >= minlogretry) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - there are no valid copies (bad ip and/or port)",inode,chindx,chunkid,version);
			}
			rreq->trycnt++;
			trycnt = rreq->trycnt;
			if (trycnt>=maxretries) {
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

		for (part=0 ; part<parts ; part++) {
			csdb_readinc(datasrc[part].ip,datasrc[part].port);
		}


		start = monotonic_seconds();

		srcip = fs_getsrcip();
		connect_status = STATE_CONNECTED;
		for (part=0 ; part<parts ; part++) {
			datasrc[part].fd = conncache_get(datasrc[part].ip,datasrc[part].port);
			if (datasrc[part].fd<0) {
				connect_status = STATE_CONNECTING;
				datasrc[part].state = STATE_IDLE;
			} else {
				datasrc[part].state = STATE_CONNECTED;
			}
		}

		connmaxtry = (trycnt*2)+2;
		if (connmaxtry>10) {
			connmaxtry = 10;
		}

		cnt = 0;
		while (cnt<connmaxtry && connect_status==STATE_CONNECTING) {
			uint8_t newconnection;
			newconnection = 0;
			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].state==STATE_IDLE) {
					int cres;
					datasrc[part].fd = tcpsocket();
					if (datasrc[part].fd<0) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: can't create tcp socket: %s",strerr(errno));
						datasrc[part].state = STATE_ERROR;
						connect_status = STATE_ERROR;
						break;
					}
					if (tcpnonblock(datasrc[part].fd)<0) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: can't set socket to non blocking mode: %s",strerr(errno));
						tcpclose(datasrc[part].fd);
						datasrc[part].state = STATE_ERROR;
						connect_status = STATE_ERROR;
						break;
					}
					if (srcip) {
						if (tcpnumbind(datasrc[part].fd,srcip,0)<0) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: can't bind socket to given ip: %s",strerr(errno));
							tcpclose(datasrc[part].fd);
							datasrc[part].state = STATE_ERROR;
							connect_status = STATE_ERROR;
							break;
						}
					}
					cres = tcpnumconnect(datasrc[part].fd,datasrc[part].ip,datasrc[part].port);
					if (cres<0) {
						int err = errno;
						zassert(pthread_mutex_lock(&(ind->lock)));
						if (trycnt >= minlogretry) {
							univmakestrip(csstrip,datasrc[part].ip);
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: can't connect to (%s:%"PRIu16"): %s",csstrip,datasrc[part].port,strerr(err));
						}
						zassert(pthread_mutex_unlock(&(ind->lock)));
						tcpclose(datasrc[part].fd);
						datasrc[part].state = STATE_IDLE;
						newconnection = 1;
					} else if (cres==0) {
						datasrc[part].state = STATE_CONNECTED;
					} else {
						datasrc[part].state = STATE_CONNECTING;
					}
				}
			}
			if (connect_status==STATE_ERROR) {
				break;
			}
			desc = 0;
			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].state==STATE_CONNECTING) {
					pfd[desc].fd = datasrc[part].fd;
					pfd[desc].events = POLLOUT;
					pfd[desc].revents = 0;
					desc++;
				}
			}
			if (desc>0) {
				if (poll(pfd,desc,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: poll error: %s",strerr(errno));
					connect_status = STATE_ERROR;
					break;
				}
				finished = 0;
				desc = 0;
				for (part=0 ; part<parts ; part++) {
					if (datasrc[part].state==STATE_CONNECTING) {
						if (pfd[desc].revents & (POLLOUT|POLLERR|POLLHUP)) {
							if (tcpgetstatus(datasrc[part].fd)) {
								int err = errno;
								if (trycnt >= minlogretry) {
									univmakestrip(csstrip,datasrc[part].ip);
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: can't connect to (%s:%"PRIu16"): %s",csstrip,datasrc[part].port,strerr(err));
								}
								tcpclose(datasrc[part].fd);
								datasrc[part].state=STATE_IDLE;
								newconnection = 1;
							} else {
								datasrc[part].state=STATE_CONNECTED;
							}
							finished = 1;
						}
						desc++;
					}
				}
				if (finished==0) { // timeout on poll
					desc = 0;
					for (part=0 ; part<parts ; part++) {
						if (datasrc[part].state==STATE_CONNECTING) {
							tcpclose(datasrc[part].fd);
							datasrc[part].state=STATE_IDLE;
							newconnection = 1;
							desc++;
						}
					}
				}
			}
			connect_status = STATE_CONNECTED;
			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].state==STATE_CONNECTING || datasrc[part].state==STATE_IDLE) {
					connect_status = STATE_CONNECTING;
					break;
				}
			}
			if (newconnection) {
				cnt++;
			}
		}

		if (connect_status!=STATE_CONNECTED) {
			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].state==STATE_CONNECTING || datasrc[part].state==STATE_CONNECTED) {
					tcpclose(datasrc[part].fd);
				}
			}
			zassert(pthread_mutex_lock(&(ind->lock)));
			rreq->trycnt++;
			trycnt = rreq->trycnt;
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

		for (part=0 ; part<parts ; part++) {
			if (tcpnodelay(datasrc[part].fd)<0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"readworker: can't set TCP_NODELAY: %s",strerr(errno));
			}
		}

		for (part=0 ; part<parts ; part++) {
			datasrc[part].gotstatus = 0;
			datasrc[part].received = 0;
			datasrc[part].sent = 0;
			datasrc[part].tosend = 0;
			datasrc[part].reccmd = 0;
			datasrc[part].recleng = 0;
			datasrc[part].lastrcvd = 0.0;
			datasrc[part].lastsend = 0.0;
			datasrc[part].startpos = 0xFFFFFFFF;
			datasrc[part].currpos = 0xFFFFFFFF;
			datasrc[part].endpos = 0xFFFFFFFF;
		}
		reqsend = 0;

		zassert(pthread_mutex_lock(&(ind->lock)));

		resetpos = 0;
		readanything = 0;
		notdone = 0;

#ifdef RDEBUG
		if (rreq->currentpos!=(rreq->offset & MFSCHUNKMASK)) {
			RDEBUG_READWORKER_COMMON("common start position: %"PRIu32,rreq->currentpos)
		}
		RDEBUG_READWORKER_COMMON("mfleng: %"PRIu64,ind->fleng)
#endif
		zassert(pthread_mutex_unlock(&(ind->lock)));

		do {
			now = monotonic_seconds();

			zassert(pthread_mutex_lock(&(ind->lock)));

#ifdef RDEBUG
			if (parts==1) {
				RDEBUG_READWORKER_COMMON("loop entry point ; currentpos: %"PRIu32,datasrc[0].currpos-datasrc[0].startpos)
			} else if (parts==4) {
				RDEBUG_READWORKER_COMMON("loop entry point ; currentpos[0..3]: %"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32,
					datasrc[0].currpos-datasrc[0].startpos,
					datasrc[1].currpos-datasrc[1].startpos,
					datasrc[2].currpos-datasrc[2].startpos,
					datasrc[3].currpos-datasrc[3].startpos)
			} else if (parts==8) {
				RDEBUG_READWORKER_COMMON("loop entry point ; currentpos[0..7]: %"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32,
					datasrc[0].currpos-datasrc[0].startpos,
					datasrc[1].currpos-datasrc[1].startpos,
					datasrc[2].currpos-datasrc[2].startpos,
					datasrc[3].currpos-datasrc[3].startpos,
					datasrc[4].currpos-datasrc[4].startpos,
					datasrc[5].currpos-datasrc[5].startpos,
					datasrc[6].currpos-datasrc[6].startpos,
					datasrc[7].currpos-datasrc[7].startpos)
			} else {
				RDEBUG_READWORKER_COMMON("loop entry point ; # of parts (%u) not supported",parts)
			}
#endif


			mfleng = ind->fleng;

			if (reqsend==0) { // calculate positions at first (we need correct positions for case of break before sending requests)
				uint32_t startpos,currpos,endpos;
				if (rreq->offset > mfleng) {
					rreq->rleng = 0;
				} else if ((rreq->offset + rreq->leng) > mfleng) {
					rreq->rleng = mfleng - rreq->offset;
				} else {
					rreq->rleng = rreq->leng;
				}
				rleng = rreq->rleng;
				startpos = rreq->offset & MFSCHUNKMASK;
				currpos = rreq->currentpos;
				endpos = startpos + rleng;
				massert(endpos<=MFSCHUNKSIZE,"endpos exceeded chunk size");
				if (currpos>endpos) {
					currpos = endpos;
				}
//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"rreq->offset: %"PRIu64" ; startpos: %"PRIu32" ; rreq->currentpos: %"PRIu32" ; rleng: %"PRIu32" ; currpos: %"PRIu32" ; endpos: %"PRIu32,rreq->offset,startpos,rreq->currentpos,rleng,currpos,endpos);

				if (parts==8 || parts==4) {
					uint32_t firstcluster,currentcluster,lastcluster;
					uint32_t firstpartoffset,currentpartoffset,lastpartoffset;
					int firstpart,currentpart,lastpart;

					uint8_t clusterbits,partmask;

					if (parts==8) {
						clusterbits = 21;
						partmask = 0x7;
					} else {
						clusterbits = 20;
						partmask = 0x3;
					}

					firstcluster = startpos >> clusterbits;
					firstpart = (startpos>>18) & partmask;
					firstpartoffset = startpos & 0x3FFFF;
					currentcluster = currpos >> clusterbits;
					currentpart = (currpos>>18) & partmask;
					currentpartoffset = currpos & 0x3FFFF;
					lastcluster = endpos >> clusterbits;
					lastpart = (endpos>>18) & partmask;
					lastpartoffset = endpos & 0x3FFFF;

					for (part=0 ; part<parts ; part++) {
						datasrc[part].startpos = firstcluster << 18;
						if (part==firstpart) {
							datasrc[part].startpos += firstpartoffset;
						} else if (part<firstpart) {
							datasrc[part].startpos += 0x40000;
						}
						datasrc[part].currpos = currentcluster << 18;
						if (part==currentpart) {
							datasrc[part].currpos += currentpartoffset;
						} else if (part<currentpart) {
							datasrc[part].currpos += 0x40000;
						}
						datasrc[part].endpos = lastcluster << 18;
						if (part==lastpart) {
							datasrc[part].endpos += lastpartoffset;
						} else if (part<lastpart) {
							datasrc[part].endpos += 0x40000;
						}
					}
					for (part=0 ; part<parts ; part++) {
						if (rreq->splitcurrpos[part]>datasrc[part].currpos) {
							datasrc[part].currpos = rreq->splitcurrpos[part];
						}
					}
				} else if (parts==1) {
					datasrc[0].startpos = startpos;
					datasrc[0].currpos = currpos;
					datasrc[0].endpos = endpos;
				} else { // not supported
					for (part=0 ; part<parts ; part++) {
						datasrc[part].startpos = 0;
						datasrc[part].currpos = 0;
						datasrc[part].endpos = 0;
					}
				}
			}

			if (rreq->mode == BREAK) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				status = EINTR;
				rreq->currentpos = rreq->offset & MFSCHUNKMASK;
				break;
			}

			finished = 1;
			for (part=0 ; part<parts && finished ; part++) {
				if (datasrc[part].gotstatus==0) {
					finished = 0;
				}
			}

			if (finished) {
				zassert(pthread_mutex_unlock(&(ind->lock)));
				if (chunksdatacache_check(inode,chindx,chunkid,version)==0) {
					zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
					RDEBUG_READWORKER_COMMON("chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk changed",chunkid,version)
#endif
					resetpos = 1;
					rreq->mode = REFRESH;
					zassert(pthread_mutex_unlock(&(ind->lock)));
				} else {
					zassert(pthread_mutex_lock(&(ind->lock)));
#ifdef RDEBUG
					RDEBUG_READWORKER_COMMON("chunksdatacache_check: chunkid: %016"PRIX64" ; version: %"PRIu32" - chunk ok",chunkid,version)
#endif
					rreq->mode = FILLED;
					rreq->modified = monotonic_seconds();
//					if (rreq->waiting>0) {
//					}
					zassert(pthread_mutex_unlock(&(ind->lock)));
				}
				break;
			}

			lrdiff = 0.0;
			cpart = 0;
			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].lastrcvd==0.0) {
					datasrc[part].lastrcvd = now;
				} else {
					double diff;
					diff = now - datasrc[part].lastrcvd;
					if (diff > lrdiff) {
						lrdiff = diff;
						cpart = part;
					}
				}
			}

			if (lrdiff>=CHUNKSERVER_ACTIVITY_TIMEOUT) {
#ifdef RDEBUG
				RDEBUG_READWORKER_COMMON("time out (lrdiff:%.6lf)",lrdiff)
#endif
				if (trycnt >= minlogretry) {
					univmakestrip(csstrip,datasrc[cpart].ip);
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32", part: %"PRIu8" - readworker: connection with (%s:%"PRIu16") was timed out (lastrcvd:%.6lf,now:%.6lf,lrdiff:%.6lf received: %"PRIu32"/%"PRIu32", try counter: %"PRIu32")",inode,chindx,chunkid,version,cpart,csstrip,datasrc[cpart].port,datasrc[cpart].lastrcvd,now,lrdiff,(uint32_t)(rreq->currentpos-(rreq->offset&MFSCHUNKMASK)),rreq->rleng,trycnt+1);
				}
				status = EIO;
				zassert(pthread_mutex_unlock(&(ind->lock)));
				break;
			}

			workingtime = now - start;

			if (workingtime>(WORKER_BUSY_LAST_REQUEST_TIMEOUT+WORKER_BUSY_WAIT_FOR_FINISH+timeoutadd)) {
#ifdef RDEBUG
				RDEBUG_READWORKER_COMMON("%s","current request not finished but busy timeout passed")
#endif
				zassert(pthread_mutex_unlock(&(ind->lock)));
				status = EINTR;
				break;
			}


			if (reqsend==0) {
				for (part=0 ; part<parts ; part++) {
//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"part: %u ; startpos: %"PRIu32" ; currpos: %"PRIu32" ; endpos: %"PRIu32,part,datasrc[part].startpos,datasrc[part].currpos,datasrc[part].endpos);
					if (datasrc[part].endpos>datasrc[part].currpos) {
						wptr = datasrc[part].sendbuff;
						put32bit(&wptr,CLTOCS_READ);
						if (datasrc[part].csver>=VERSION2INT(1,7,32)) {
							put32bit(&wptr,21);
							put8bit(&wptr,1);
							datasrc[part].tosend = 29;
						} else {
							put32bit(&wptr,20);
							datasrc[part].tosend = 28;
						}
						if (parts==1) {
							put64bit(&wptr,chunkid);
						} else if (parts==8) {
							put64bit(&wptr,COMBINE_CHUNKID_AND_ECID(chunkid,0x20|part));
						} else { // parts==4
							put64bit(&wptr,COMBINE_CHUNKID_AND_ECID(chunkid,0x10|part));
						}
						put32bit(&wptr,version);
						put32bit(&wptr,datasrc[part].currpos);
						put32bit(&wptr,datasrc[part].endpos-datasrc[part].currpos);
						datasrc[part].sent = 0;
					} else {
						datasrc[part].gotstatus = 1;
					}
				}
				reqsend = 1;
			}

			rreq->waitingworker = 1;
			rreq->wakeup_fd = pipefd[1];
			zassert(pthread_mutex_unlock(&(ind->lock)));

			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].tosend==0 && (now - datasrc[part].lastsend > 1.0)) {
					wptr = datasrc[part].sendbuff;
					put32bit(&wptr,ANTOAN_NOP);
					put32bit(&wptr,0);
					datasrc[part].tosend = 8;
					datasrc[part].sent = 0;
				}

				if (datasrc[part].tosend>0) {
					i = universal_write(datasrc[part].fd,datasrc[part].sendbuff+datasrc[part].sent,datasrc[part].tosend-datasrc[part].sent);
					if (i<0) { // error
						if (ERRNO_ERROR && errno!=EINTR) {
							if (trycnt >= minlogretry) {
								int err = errno;
								univmakestrip(csstrip,datasrc[part].ip);
								mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32", part: %"PRIu8" - readworker: write to (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,part,csstrip,datasrc[part].port,strerr(err),(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
							}
#ifdef RDEBUG
							RDEBUG_READWORKER("write error: %s",strerr(errno))
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
#ifdef RDEBUG
						RDEBUG_READWORKER("sent: %"PRIu32" bytes",i)
#endif
						datasrc[part].sent += i;
						if (datasrc[part].tosend<=datasrc[part].sent) {
							datasrc[part].sent = 0;
							datasrc[part].tosend = 0;
						}
						datasrc[part].lastsend = now;
					}
				}
			}

			if (status==EIO) {
				break;
			}

			pfd[0].fd = pipefd[0];
			pfd[0].events = POLLIN;
			pfd[0].revents = 0;
			desc = 1;
			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].tosend>0 || datasrc[part].gotstatus==0) {
					pfd[desc].fd = datasrc[part].fd;
					pfd[desc].events = POLLIN;
					pfd[desc].revents = 0;
					if (datasrc[part].tosend>0) {
						pfd[desc].events |= POLLOUT;
					}
					desc++;
				}
			}

			if (poll(pfd,desc,100)<0) {
				if (errno!=EINTR) {
					if (trycnt >= minlogretry) {
						for (part=0 ; part<parts ; part++) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32", part: %"PRIu8" - readworker: poll error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,part,strerr(errno),(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
						}
					}
#ifdef RDEBUG
					RDEBUG_READWORKER_COMMON("poll error: %s",strerr(errno))
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
			if (pfd[0].revents&POLLIN) { // used just to break poll - so just read all data from pipe to empty it
#ifdef RDEBUG
				RDEBUG_READWORKER_COMMON("%s","woken up by pipe")
#endif
				i = universal_read(pipefd[0],pipebuff,1024);
				if (i<0) { // mainly to make happy static code analyzers
					if (trycnt >= minlogretry) {
						for (part=0 ; part<parts ; part++) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32", part: %"PRIu8" - readworker: read pipe error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,part,strerr(errno),(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
						}
					}
				}
			}
			if (mode!=BUSY) {
#ifdef RDEBUG
				RDEBUG_READWORKER_COMMON("mode=%s",read_data_modename(mode))
#endif
				status = EINTR;
				resetpos = 1;
				break;
			}
			if (closing) {
#ifdef RDEBUG
				RDEBUG_READWORKER_COMMON("%s","closing")
#endif
				status = EINTR;
				resetpos = 1;
				break;
			}
			desc = 1;
			for (part=0 ; part<parts ; part++) {
				if (datasrc[part].tosend>0 || datasrc[part].gotstatus==0) {
					if (pfd[desc].revents&POLLHUP) {
						if (trycnt >= minlogretry) {
							univmakestrip(csstrip,datasrc[part].ip);
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / POLLHUP (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,datasrc[part].port,(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
						}
#ifdef RDEBUG
						RDEBUG_READWORKER_SIMPLE("connection got error status / POLLHUP")
#endif
						status = EIO;
						break;
					}
					if (pfd[desc].revents&POLLERR) {
						if (trycnt >= minlogretry) {
							univmakestrip(csstrip,datasrc[part].ip);
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") got error status / POLLERR (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,datasrc[part].port,(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
						}
#ifdef RDEBUG
						RDEBUG_READWORKER_SIMPLE("connection got error status / POLLERR")
#endif
						status = EIO;
						break;
					}
					if (pfd[desc].revents&POLLIN) {
						datasrc[part].lastrcvd = monotonic_seconds();
						if (datasrc[part].received < 8) {
							i = universal_read(datasrc[part].fd,datasrc[part].recvbuff+datasrc[part].received,8-datasrc[part].received);
							if (i==0) {
								if (trycnt >= minlogretry) {
									univmakestrip(csstrip,datasrc[part].ip);
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / ZEROREAD (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,datasrc[part].port,(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
								}
#ifdef RDEBUG
								RDEBUG_READWORKER_SIMPLE("connection reset by peer")
#endif
								status = EIO;
								break;
							}
							if (i<0 && ERRNO_ERROR) {
								if (trycnt >= minlogretry) {
									int err = errno;
									univmakestrip(csstrip,datasrc[part].ip);
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32", part:%"PRIu8" - readworker: read from (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,part,csstrip,datasrc[part].port,strerr(err),(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
								}
#ifdef RDEBUG
								RDEBUG_READWORKER("connection error: %s",strerr(errno))
#endif
								status = EIO;
								break;
							}
#ifdef RDEBUG
							if (i<0) {
								RDEBUG_READWORKER("connection status: %s",strerr(errno))
							} else {
								RDEBUG_READWORKER("received: %"PRIu32" bytes",i)
							}
#endif
							if (i<0) {
								i = 0;
							}
							datasrc[part].received += i;
							if (datasrc[part].received == 8) { // full header
								rptr = datasrc[part].recvbuff;

								datasrc[part].reccmd = get32bit(&rptr);
								datasrc[part].recleng = get32bit(&rptr);
								if (datasrc[part].reccmd==CSTOCL_READ_STATUS) {
									if (datasrc[part].recleng!=9) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got wrong sized status packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng);
#ifdef RDEBUG
										RDEBUG_READWORKER("got wrong sized status packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
								} else if (datasrc[part].reccmd==CSTOCL_READ_DATA) {
									if (datasrc[part].recleng<20) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got too short data packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng);
#ifdef RDEBUG
										RDEBUG_READWORKER("got too short data packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									} else if ((datasrc[part].recleng-20) + datasrc[part].currpos > datasrc[part].endpos) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got too long data packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng);
#ifdef RDEBUG
										RDEBUG_READWORKER("got too long data packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									} else if ((datasrc[part].currpos & (~0x3FFFF)) != (((datasrc[part].recleng-20) + datasrc[part].currpos - 1) & (~0x3FFFF))) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got data packet from chunkserver (leng:%"PRIu32") which overlaps two logical blocks",datasrc[part].recleng);
#ifdef RDEBUG
										RDEBUG_READWORKER("got data packet from chunkserver (leng:%"PRIu32") which overlaps two logical blocks",datasrc[part].recleng);
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
								} else if (datasrc[part].reccmd==ANTOAN_NOP) {
									if (datasrc[part].recleng!=0) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got wrong sized nop packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng);
#ifdef RDEBUG
										RDEBUG_READWORKER("got wrong sized nop packet from chunkserver (leng:%"PRIu32")",datasrc[part].recleng)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
									datasrc[part].received = 0;
								} else {
									uint32_t myip,peerip;
									uint16_t myport,peerport;
									tcpgetpeer(datasrc[part].fd,&peerip,&peerport);
									tcpgetmyaddr(datasrc[part].fd,&myip,&myport);
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got unrecognized packet from chunkserver (cmd:%"PRIu32",leng:%"PRIu32",%u.%u.%u.%u:%u<->%u.%u.%u.%u:%u)",datasrc[part].reccmd,datasrc[part].recleng,(myip>>24)&0xFF,(myip>>16)&0xFF,(myip>>8)&0xFF,myip&0xFF,myport,(peerip>>24)&0xFF,(peerip>>16)&0xFF,(peerip>>8)&0xFF,peerip&0xFF,peerport);
#ifdef RDEBUG
									RDEBUG_READWORKER("got unrecognized packet from chunkserver (cmd:%"PRIu32",leng:%"PRIu32")",datasrc[part].reccmd,datasrc[part].recleng)
#endif
									status = EIO;
									resetpos = 1; // start again from beginning
									break;
								}
							}
						}
						if (datasrc[part].received >= 8) {
							if (datasrc[part].recleng<=20) {
								i = universal_read(datasrc[part].fd,datasrc[part].recvbuff + (datasrc[part].received-8),datasrc[part].recleng - (datasrc[part].received-8));
							} else {
								datacurrpos = datasrc[part].currpos;
								if (parts==8 || parts==4) {
									datacurrpos &= (~0x3FFFF);
									datacurrpos *= parts;
									datacurrpos += ((uint32_t)part) << 18;
									datacurrpos += datasrc[part].currpos & 0x3FFFF;
								}
								datacurrpos -= (rreq->offset & MFSCHUNKMASK);
//								mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"part: %u ; read data currpos: %"PRIu32,part,datacurrpos);
								if (datasrc[part].received < 8 + 20) {
#ifdef HAVE_READV
									siov[0].iov_base = datasrc[part].recvbuff + (datasrc[part].received-8);
									siov[0].iov_len = 20 - (datasrc[part].received-8);
									siov[1].iov_base = rreq->data + datacurrpos;
									siov[1].iov_len = datasrc[part].recleng - 20;
									i = readv(datasrc[part].fd,siov,2);
#else
									i = universal_read(datasrc[part].fd,datasrc[part].recvbuff + (datasrc[part].received-8),20 - (datasrc[part].received-8));
#endif
								} else {
									i = universal_read(datasrc[part].fd,rreq->data + datacurrpos,datasrc[part].recleng - (datasrc[part].received-8));
								}
							}
							if (i==0) {
								if (trycnt >= minlogretry) {
									univmakestrip(csstrip,datasrc[part].ip);
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32", part: %"PRIu8" - readworker: connection with (%s:%"PRIu16") was reset by peer (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,part,csstrip,datasrc[part].port,(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
								}
#ifdef RDEBUG
								RDEBUG_READWORKER_SIMPLE("connection reset by peer")
#endif
								status = EIO;
								break;
							}
							if (i<0 && ERRNO_ERROR) {
								if (trycnt >= minlogretry) {
									int err = errno;
									univmakestrip(csstrip,datasrc[part].ip);
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %016"PRIX64", version: %"PRIu32", part: %"PRIu8" - readworker: connection with (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",inode,chindx,chunkid,version,part,csstrip,datasrc[part].port,strerr(err),(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),trycnt+1);
								}
#ifdef RDEBUG
								RDEBUG_READWORKER("connection error: %s",strerr(errno))
#endif
								status = EIO;
								break;
							}
#ifdef RDEBUG
							if (i<0) {
								RDEBUG_READWORKER("connection status: %s",strerr(errno))
							} else {
								RDEBUG_READWORKER("received: %"PRIu32" bytes",i)
							}
#endif
							if (i<0) {
								i = 0;
							}
							if (datasrc[part].received < 8+20) {
								if (datasrc[part].received+i >= 8+20) {
									datasrc[part].currpos += i - ((8+20) - datasrc[part].received);
								}
							} else {
								datasrc[part].currpos += i;
							}
							datasrc[part].received += i;
							if (datasrc[part].received > 8+datasrc[part].recleng) {
								mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: internal error - received more bytes than expected");
#ifdef RDEBUG
								RDEBUG_READWORKER("internal error - received more bytes than expected (received: %"PRIu32", expected: %"PRIu32")",datasrc[part].received,8+datasrc[part].recleng)
#endif
								status = EIO;
								resetpos = 1; // start again from beginning
								break;
							} else if (datasrc[part].received == 8+datasrc[part].recleng) {
//								mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"received packet: %"PRIu32" ; leng: %"PRIu32,datasrc[part].reccmd,datasrc[part].recleng);
								if (datasrc[part].reccmd==CSTOCL_READ_STATUS) {
									uint64_t expectedchunkid;
									rptr = datasrc[part].recvbuff;
									recchunkid = get64bit(&rptr);
									recstatus = get8bit(&rptr);
									if (parts==1) {
										expectedchunkid = chunkid;
									} else if (parts==8) {
										expectedchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,0x20|part);
									} else { // parts==4
										expectedchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,0x10|part);
									}
									if (recchunkid != expectedchunkid) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got unexpected status packet (expected chunkdid:%"PRIX64",packet chunkid:%"PRIX64")",expectedchunkid,recchunkid);
#ifdef RDEBUG
										RDEBUG_READWORKER("got unexpected status packet (expected chunkdid:%016"PRIX64",packet chunkid:%016"PRIX64")",expectedchunkid,recchunkid)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
									if (recstatus!=MFS_STATUS_OK) {
										if (trycnt >= minlogretry) {
											mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: read error: %s",mfsstrerr(recstatus));
										}
#ifdef RDEBUG
										RDEBUG_READWORKER("read error: %s",mfsstrerr(recstatus))
#endif
										status = EIO;
										if (recstatus==MFS_ERROR_NOTDONE) {
											notdone = 1;
										} else {
											resetpos = 1; // start again from beginning
										}
										break;
									}
									if (datasrc[part].currpos != datasrc[part].endpos) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: unexpected data block size (requested: %"PRIu32" / received: %"PRIu32")",(uint32_t)(datasrc[part].endpos-datasrc[part].startpos),(uint32_t)(datasrc[part].currpos-datasrc[part].startpos));
#ifdef RDEBUG
										RDEBUG_READWORKER("unexpected data block size (requested: %"PRIu32" / received: %"PRIu32,datasrc[part].endpos-datasrc[part].startpos,datasrc[part].currpos-datasrc[part].startpos)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
									datasrc[part].gotstatus = 1;
								} else if (datasrc[part].reccmd==CSTOCL_READ_DATA) {
									uint64_t expectedchunkid;
									rptr = datasrc[part].recvbuff;
									recchunkid = get64bit(&rptr);
									recblocknum = get16bit(&rptr);
									recoffset = get16bit(&rptr);
									recsize = get32bit(&rptr);
									reccrc = get32bit(&rptr);
									(void)recoffset;
									(void)recblocknum;
//									mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"part: %u ; currpos: %"PRIu32" ; recsize: %"PRIu32,part,datasrc[part].currpos,recsize);
									datacurrpos = datasrc[part].currpos - recsize;
//									mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"part: %u ; start currpos: %"PRIu32,part,datacurrpos);
									if (parts==8 || parts==4) {
										datacurrpos &= (~0x3FFFF);
										datacurrpos *= parts;
										datacurrpos += ((uint32_t)part) << 18;
										datacurrpos += (datasrc[part].currpos - recsize) & 0x3FFFF;
										if (parts==8) {
											expectedchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,0x20|part);
										} else { // parts==4
											expectedchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,0x10|part);
										}
									} else {
										expectedchunkid = chunkid;
									}
									datacurrpos -= (rreq->offset & MFSCHUNKMASK);
//									mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"part: %u ; data currpos: %"PRIu32,part,datacurrpos);
									if (recchunkid != expectedchunkid) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got unexpected data packet (expected chunkdid:%016"PRIX64",packet chunkid:%016"PRIX64")",expectedchunkid,recchunkid);
#ifdef RDEBUG
										RDEBUG_READWORKER("got unexpected data packet (expected chunkdid:%016"PRIX64",packet chunkid:%016"PRIX64")",expectedchunkid,recchunkid)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
									if (recsize+20 != datasrc[part].recleng) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: got malformed data packet (datasize: %"PRIu32",packetsize: %"PRIu32")",recsize,datasrc[part].recleng);
#ifdef RDEBUG
										RDEBUG_READWORKER("got malformed data packet (datasize: %"PRIu32",packetsize: %"PRIu32")",recsize,datasrc[part].recleng)
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
									if (reccrc != mycrc32(0,rreq->data + datacurrpos,recsize)) {
										mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"readworker: data checksum error");
#ifdef RDEBUG
										RDEBUG_READWORKER_SIMPLE("data checksum error")
#endif
										status = EIO;
										resetpos = 1; // start again from beginning
										break;
									}
									read_increase_total_bytes(recsize);
									readanything = 1;
								}
								datasrc[part].received = 0;
							}
						}
					}
					desc++;
				}
			}
			if (status==EIO) {
				break;
			}
		} while (1);

#ifdef RDEBUG
		for (part=0 ; part<parts ; part++) {
			RDEBUG_READWORKER("loop has ended ; startpos/currpos/endpos: %"PRIu32"/%"PRIu32"/%"PRIu32" ; currpos-startpos: %"PRIu32" ; status: %s",datasrc[part].startpos,datasrc[part].currpos,datasrc[part].endpos,(uint32_t)(datasrc[part].currpos-datasrc[part].startpos),strerr(status))
		}
#endif

		for (part=0 ; part<parts ; part++){
			if (status==0 && datasrc[part].csver>=VERSION2INT(1,7,32)) {
				conncache_insert(datasrc[part].ip,datasrc[part].port,datasrc[part].fd);
			} else {
				tcpclose(datasrc[part].fd);
			}
		}

		if (status==EINTR) {
			status=0;
		}

#ifdef WORKER_DEBUG
		now = monotonic_seconds();
		workingtime = now - start;

		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"worker %lu received data from chunk %016"PRIX64"_%08"PRIX32", bw: %.6lfMB/s ( %"PRIu32" B / %.6lf s )",(unsigned long)arg,chunkid,version,(double)bytesreceived/workingtime,bytesreceived,workingtime);
#endif

		zassert(pthread_mutex_lock(&(ind->lock)));

		if (readanything) {
			if (resetpos) {
				readanything = 0;
				rreq->currentpos = rreq->offset & MFSCHUNKMASK;
				memset(rreq->splitcurrpos,0,sizeof(rreq->splitcurrpos));
			} else {
				if (parts==1) {
					rreq->currentpos = datasrc[0].currpos;
					memset(rreq->splitcurrpos,0,sizeof(rreq->splitcurrpos));
				} else if (parts==8 || parts==4) {
					uint32_t minblockpos;

					minblockpos = UINT32_C(0xFFFFFFFF);
					for (part=0 ; part<parts ; part++) {
						datacurrpos = datasrc[part].currpos;
						datacurrpos &= (~0x3FFFF);
						if (part==0 || datacurrpos<minblockpos) {
							minblockpos = datacurrpos;
						}
					}

					for (part=0 ; part<parts ; part++) {
						datacurrpos = datasrc[part].currpos;
						datacurrpos &= (~0x3FFFF);
						if (datacurrpos < minblockpos + 0x40000) {
							datacurrpos *= parts;
							datacurrpos += ((uint32_t)part) << 18;
							datacurrpos += datasrc[part].currpos & 0x3FFFF;
							break;
						}
					}

/*
					if ((datacurrpos < (rreq->offset & MFSCHUNKMASK)) || (datacurrpos > (rreq->offset & MFSCHUNKMASK) + rreq->leng) || part>=parts) {
						uint8_t p;
						mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"assertion extra info: parts:%u ; rreq->offset:0x%"PRIX64" ; datacurrpos:0x%"PRIX32" ; offset_in_chunk:0x%"PRIX32" ; request_leng:0x%"PRIX32" ; end_in_chunk:0x%"PRIX32" ; minblockpos:0x%"PRIX32,parts,rreq->offset,datacurrpos,(uint32_t)(rreq->offset & MFSCHUNKMASK),rreq->leng,(uint32_t)((rreq->offset & MFSCHUNKMASK) + rreq->leng),minblockpos);
						for (p=0 ; p<parts ; p++) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"assertion extra info for part %u: startpos:0x%"PRIX32" ; currpos:0x%"PRIX32" ; endpos:0x%"PRIX32" ; gotstatus:%u ; sent:0x%"PRIX32" ; tosend:0x%"PRIX32" ; received:0x%"PRIX32" ; reccmd:%"PRIu32" ; recleng:0x%"PRIX32,p,datasrc[p].startpos,datasrc[p].currpos,datasrc[p].endpos,datasrc[p].gotstatus,datasrc[p].sent,datasrc[p].tosend,datasrc[p].received,datasrc[p].reccmd,datasrc[p].recleng);
						}
					}
*/
					massert(part<parts,"data mismatch");
					massert((datacurrpos >= (rreq->offset & MFSCHUNKMASK)) && (datacurrpos <= (rreq->offset & MFSCHUNKMASK) + rreq->leng),"current position mismatch");
					rreq->currentpos = datacurrpos;
					for (part=0 ; part<parts ; part++) {
						rreq->splitcurrpos[part] = datasrc[part].currpos;
					}
				}
			}
		}

		if (status!=0) {
			if (readanything==0) {
				if (notdone==0) {
					rreq->trycnt++;
					trycnt = rreq->trycnt;
				}
			}
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
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't write to pipe !!!");
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
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't write to pipe !!!");
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
									if (!STATE_NOT_NEEDED(rreqn->mode)) {
										if (!(blockend <= rreqn->offset || blockstart >= rreqn->offset+rreqn->leng)) {
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
											if (!STATE_NOT_NEEDED(rreqn->mode)) {
												if (!(blockend <= rreqn->offset || blockstart >= rreqn->offset+rreqn->leng)) {
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
							if (!STATE_NOT_NEEDED(rreqn->mode)) {
								if (!(blockend <= rreqn->offset || blockstart >= rreqn->offset+rreqn->leng)) {
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
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't write to pipe !!!");
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

void read_init(void) {
#ifdef TOTAL_COUNT_USE_LOCK
	zassert(pthread_mutex_init(&total_bytes_lock,NULL));
#endif
	read_get_total_bytes(); // zero counters
}

void read_term(void) {
#ifdef TOTAL_COUNT_USE_LOCK
	zassert(pthread_mutex_destroy(&total_bytes_lock));
#endif
}
