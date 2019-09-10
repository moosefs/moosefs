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

#include <sys/types.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif
#include <sys/time.h>
#include <unistd.h>
#ifndef WIN32
#include <poll.h>
#include <syslog.h>
#endif
#include <stdio.h>
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
#include "MFSCommunication.h"
#ifdef MFSMOUNT
#include "fdcache.h"
#endif

// #define WORKER_DEBUG 1
// #define BUFFER_DEBUG 1
// #define WDEBUG 1

#ifndef EDQUOT
#define EDQUOT ENOSPC
#endif

// for Nagle's-like algorithm
#define NEXT_BLOCK_DELAY 0.05

#define CHUNKSERVER_ACTIVITY_TIMEOUT 2.0

#define WORKER_IDLE_TIMEOUT 1.0

#define WORKER_BUSY_LAST_SEND_TIMEOUT 5.0
#define WORKER_BUSY_WAIT_FOR_STATUS 5.0
#define WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT 20.0

#define WORKER_NOP_INTERVAL 1.0

#define MAX_SIM_CHUNKS 16

#define SUSTAIN_WORKERS 50
#define HEAVYLOAD_WORKERS 150
#define MAX_WORKERS 250

#define WCHASHSIZE 256
#define WCHASH(inode,indx) (((inode)*0xB239FB71+(indx)*193)%WCHASHSIZE)

#define IDHASHSIZE 256
#define IDHASH(inode) (((inode)*0xB239FB71)%IDHASHSIZE)

typedef struct cblock_s {
	uint8_t data[MFSBLOCKSIZE];	// modified only when writeid==0
	uint16_t pos;		// block in chunk (0...1023) - never modified
	uint32_t writeid;	// 0 = not sent, >0 = block was sent (modified and accessed only when wchunk is locked)
	uint32_t from;		// first filled byte in data (modified only when writeid==0)
	uint32_t to;		// first not used byte in data (modified only when writeid==0)
	struct cblock_s *next,*prev;
} cblock;

struct inodedata_s;

typedef struct chunkdata_s {
	uint32_t chindx;
	uint16_t trycnt;
	uint8_t waitingworker;
	uint8_t chunkready;
	uint8_t unbreakable;
	uint8_t continueop;
	uint8_t superuser;
	int wakeup_fd;
	cblock *datachainhead,*datachaintail;
	struct inodedata_s *parent;
	struct chunkdata_s *next,**prev;
} chunkdata;

typedef struct inodedata_s {
	uint32_t inode;
	uint64_t maxfleng;
	uint32_t cacheblockcount;
	int status;
	uint16_t flushwaiting;
	uint16_t writewaiting;
	uint16_t chunkwaiting;
	uint16_t lcnt;
//	uint16_t trycnt;
	uint16_t chunkscnt;
	chunkdata *chunks,**chunkstail;
	chunkdata *chunksnext;
	pthread_cond_t flushcond;	// wait for chunks==NULL (flush)
	pthread_cond_t writecond;	// wait for flushwaiting==0 (write)
	pthread_cond_t chunkcond;	// wait for status!=0 or all chunks 'chunkready==1'
	pthread_mutex_t lock;
	struct inodedata_s *next;
} inodedata;

typedef struct worker_s {
	pthread_t thread_id;
} worker;

static pthread_mutex_t fcblock;
static pthread_cond_t fcbcond;
static uint16_t fcbwaiting;
static cblock *cacheblocks,*freecblockshead;
static uint32_t freecacheblocks;
static uint32_t cacheblockcount;

static double optimeout;
static uint32_t maxretries;
static uint32_t minlogretry;

static inodedata **idhash;

static pthread_mutex_t hashlock;

#ifdef BUFFER_DEBUG
static pthread_t info_worker_th;
static uint32_t usedblocks;
#endif

// static pthread_t dqueue_worker_th;

static pthread_mutex_t workerslock;
static uint32_t workers_avail;
static uint32_t workers_total;
static uint32_t worker_term_waiting;
static pthread_cond_t worker_term_cond;
static pthread_attr_t worker_thattr;

static void *jqueue; //,*dqueue;

#ifdef BUFFER_DEBUG
void* write_info_worker(void *arg) {
	(void)arg;
	uint32_t cbcnt,fcbcnt,ucbcnt;
	uint32_t i;
	inodedata *ind;
	chunkdata *chd;
	cblock *cb;
	for (;;) {
		zassert(pthread_mutex_lock(&hashlock));
		cbcnt = 0;
		for (i = 0 ; i<IDHASHSIZE ; i++) {
			for (ind = idhash[i] ; ind ; ind = ind->next) {
				zassert(pthread_mutex_lock(&(ind->lock)));
				ucbcnt = 0;
				for (chd = ind->chunks ; chd!=NULL ; chd = chd->next) {
					for (cb = chd->datachainhead ; cb ; cb = cb->next) {
						ucbcnt++;
					}
				}
				if (ucbcnt != ind->cacheblockcount) {
					syslog(LOG_NOTICE,"inode: %"PRIu32" ; wrong cache block count (%"PRIu32"/%"PRIu32")",ind->inode,ucbcnt,ind->cacheblockcount);
				}
				cbcnt += ucbcnt;
				zassert(pthread_mutex_unlock(&(ind->lock)));
			}
		}
		zassert(pthread_mutex_unlock(&hashlock));
		zassert(pthread_mutex_lock(&fcblock));
		fcbcnt = 0;
		for (cb = freecblockshead ; cb ; cb = cb->next) {
			fcbcnt++;
		}
		syslog(LOG_NOTICE,"used cache blocks: %"PRIu32" ; sum of inode used blocks: %"PRIu32" ; free cache blocks: %"PRIu32" ; free cache chain blocks: %"PRIu32,usedblocks,cbcnt,freecacheblocks,fcbcnt);
		zassert(pthread_mutex_unlock(&fcblock));
		portable_usleep(500000);
	}

}
#endif

void write_cb_release (inodedata *ind,cblock *cb) {
	zassert(pthread_mutex_lock(&fcblock));
	cb->next = freecblockshead;
	freecblockshead = cb;
	freecacheblocks++;
	ind->cacheblockcount--;
	if (fcbwaiting) {
		zassert(pthread_cond_signal(&fcbcond));
	}
#ifdef BUFFER_DEBUG
	usedblocks--;
#endif
	zassert(pthread_mutex_unlock(&fcblock));
}

cblock* write_cb_acquire(inodedata *ind/*,uint8_t *waited*/) {
	cblock *ret;
	zassert(pthread_mutex_lock(&fcblock));
	fcbwaiting++;
//	*waited=0;
	while (freecblockshead==NULL/* || ind->cacheblockcount>(freecacheblocks/3)*/) {
		zassert(pthread_cond_wait(&fcbcond,&fcblock));
//		*waited=1;
	}
	fcbwaiting--;
	ret = freecblockshead;
	freecblockshead = ret->next;
	ret->pos = 0;
	ret->writeid = 0;
	ret->from = 0;
	ret->to = 0;
	ret->next = NULL;
	ret->prev = NULL;
	freecacheblocks--;
	ind->cacheblockcount++;
#ifdef BUFFER_DEBUG
	usedblocks++;
#endif
	zassert(pthread_mutex_unlock(&fcblock));
	return ret;
}

uint8_t write_cache_almost_full(void) {
	uint8_t r;
	zassert(pthread_mutex_lock(&fcblock));
	r = (freecacheblocks < (cacheblockcount / 3))?1:0;
	zassert(pthread_mutex_unlock(&fcblock));
	return r;
}

/* inode */

inodedata* write_find_inodedata(uint32_t inode) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;
	zassert(pthread_mutex_lock(&hashlock));
	for (ind=idhash[indh] ; ind ; ind=ind->next) {
		if (ind->inode == inode) {
			ind->lcnt++;
			zassert(pthread_mutex_unlock(&hashlock));
			return ind;
		}
	}
	zassert(pthread_mutex_unlock(&hashlock));
	return NULL;
}

inodedata* write_get_inodedata(uint32_t inode,uint64_t fleng) {
	uint32_t indh = IDHASH(inode);
	inodedata *ind;
//	int pfd[2];

	zassert(pthread_mutex_lock(&hashlock));
	for (ind=idhash[indh] ; ind ; ind=ind->next) {
		if (ind->inode == inode) {
			ind->lcnt++;
			zassert(pthread_mutex_unlock(&hashlock));
			return ind;
		}
	}

	ind = malloc(sizeof(inodedata));
	passert(ind);
	ind->inode = inode;
	ind->cacheblockcount = 0;
	ind->maxfleng = fleng;
	ind->status = 0;
//	ind->trycnt = 0;
	ind->chunkscnt = 0;
	ind->chunks = NULL;
	ind->chunksnext = NULL;
	ind->chunkstail = &(ind->chunks);
	ind->flushwaiting = 0;
	ind->chunkwaiting = 0;
	ind->writewaiting = 0;
	ind->lcnt = 1;
	zassert(pthread_cond_init(&(ind->flushcond),NULL));
	zassert(pthread_cond_init(&(ind->writecond),NULL));
	zassert(pthread_cond_init(&(ind->chunkcond),NULL));
	zassert(pthread_mutex_init(&(ind->lock),NULL));
	ind->next = idhash[indh];
	idhash[indh] = ind;
	zassert(pthread_mutex_unlock(&hashlock));
	return ind;
}

void write_free_inodedata(inodedata *fid) {
	uint32_t indh = IDHASH(fid->inode);
	inodedata *ind,**indp;
	zassert(pthread_mutex_lock(&hashlock));
	indp = &(idhash[indh]);
	while ((ind=*indp)) {
		if (ind==fid) {
			ind->lcnt--;
			if (ind->lcnt==0) {
				*indp = ind->next;
				zassert(pthread_mutex_lock(&(ind->lock)));
				massert(ind->chunkscnt==0 && ind->flushwaiting==0 && ind->writewaiting==0,"inode structure not clean");
				zassert(pthread_mutex_unlock(&(ind->lock)));
				zassert(pthread_cond_destroy(&(ind->flushcond)));
				zassert(pthread_cond_destroy(&(ind->writecond)));
				zassert(pthread_cond_destroy(&(ind->chunkcond)));
				zassert(pthread_mutex_destroy(&(ind->lock)));
				free(ind);
			}
			zassert(pthread_mutex_unlock(&hashlock));
			return;
		}
		indp = &(ind->next);
	}
	zassert(pthread_mutex_unlock(&hashlock));
}

void write_enqueue(chunkdata *chd);

void write_test_chunkdata(inodedata *ind) {
	chunkdata *chd;

	if (ind->chunkscnt<MAX_SIM_CHUNKS) {
		if (ind->chunksnext!=NULL) {
			chd = ind->chunksnext;
			ind->chunksnext = chd->next;
			ind->chunkscnt++;
			write_enqueue(chd);
		}
	} else {
		for (chd=ind->chunks ; chd!=NULL ; chd=chd->next) {
			if (chd->waitingworker) {
				if (universal_write(chd->wakeup_fd," ",1)!=1) {
					syslog(LOG_ERR,"can't write to pipe !!!");
				}
				chd->waitingworker = 0;
				chd->wakeup_fd = -1;
			}
		}
	}
}

chunkdata* write_new_chunkdata(inodedata *ind,uint32_t chindx) {
	chunkdata *chd;

	chd = malloc(sizeof(chunkdata));
	passert(chd);
	chd->chindx = chindx;
	chd->wakeup_fd = -1;
	chd->datachainhead = NULL;
	chd->datachaintail = NULL;
	chd->waitingworker = 0;
	chd->chunkready = 0;
	chd->unbreakable = 0;
	chd->continueop = 0;
	chd->superuser = 0;
	chd->trycnt = 0;
	chd->parent = ind;
	chd->next = NULL;
	chd->prev = ind->chunkstail;
	*(ind->chunkstail) = chd;
	ind->chunkstail = &(chd->next);
	if (ind->chunksnext==NULL) {
		ind->chunksnext = chd;
	}
	return chd;
}

void write_free_chunkdata(chunkdata *chd) {
	*(chd->prev) = chd->next;
	if (chd->next) {
		chd->next->prev = chd->prev;
	} else {
		chd->parent->chunkstail = chd->prev;
	}
	chd->parent->chunkscnt--;
	write_test_chunkdata(chd->parent);
	free(chd);
}

/* queues */

void write_enqueue(chunkdata *chd) {
	queue_put(jqueue,0,0,(uint8_t*)chd,0);
}

void write_delayrun_enqueue(void *udata) {
	queue_put(jqueue,0,0,(uint8_t*)udata,0);
}

void write_delayed_enqueue(chunkdata *chd,uint32_t usecs) {
	if (usecs>0) {
		delay_run(write_delayrun_enqueue,chd,usecs);
	} else {
		queue_put(jqueue,0,0,(uint8_t*)chd,0);
	}
}

/*
void* write_dqueue_worker(void *arg) {
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

void write_job_end(chunkdata *chd,int status,uint32_t delay) {
	cblock *cb,*fcb;
	inodedata *ind = chd->parent;

	zassert(pthread_mutex_lock(&(ind->lock)));
	if (status!=0) {
		errno = status;
		syslog(LOG_WARNING,"error writing file number %"PRIu32": %s",ind->inode,strerr(errno));
		ind->status = status;
//		if (ind->chunkwaiting>0) {
			zassert(pthread_cond_broadcast(&(ind->chunkcond)));
//		}
	}
	if (status==0 && delay==0) {
		chd->trycnt=0;	// on good write reset try counter
	}
	status = ind->status;

	if (chd->datachainhead && status==0) {	// still have some work to do
		// reset write ind
		for (cb=chd->datachainhead ; cb ; cb=cb->next) {
			cb->writeid = 0;
		}
		write_delayed_enqueue(chd,delay);
	} else {	// no more work or error occurred
		// if this is an error then release all data blocks
		cb = chd->datachainhead;
		while (cb) {
			fcb = cb;
			cb = cb->next;
			write_cb_release(ind,fcb);
		}
		if (ind->flushwaiting>0) {
			zassert(pthread_cond_broadcast(&(ind->flushcond)));
		}
		write_free_chunkdata(chd);
	}
	zassert(pthread_mutex_unlock(&(ind->lock)));
}

void* write_worker(void *arg);

#ifndef WDEBUG
static uint32_t lastnotify = 0;
#endif

static inline void write_data_spawn_worker(void) {
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
	zassert(pthread_sigmask(SIG_BLOCK, &newset, &oldset));
#endif
	res = pthread_create(&(w->thread_id),&worker_thattr,write_worker,w);
#ifndef WIN32
	zassert(pthread_sigmask(SIG_SETMASK, &oldset, NULL));
#endif
	if (res<0) {
		return;
	}
	workers_avail++;
	workers_total++;
#ifdef WDEBUG
	fprintf(stderr,"spawn write worker (total: %"PRIu32")\n",workers_total);
#else
	if (workers_total%10==0 && workers_total!=lastnotify) {
		syslog(LOG_INFO,"write workers: %"PRIu32"+\n",workers_total);
		lastnotify = workers_total;
	}
#endif
}

static inline void write_data_close_worker(worker *w) {
	workers_avail--;
	workers_total--;
	if (workers_total==0 && worker_term_waiting) {
		zassert(pthread_cond_signal(&worker_term_cond));
		worker_term_waiting--;
	}
	pthread_detach(w->thread_id);
	free(w);
#ifdef WDEBUG
	fprintf(stderr,"close write worker (total: %"PRIu32")\n",workers_total);
#else
	if (workers_total%10==0 && workers_total!=lastnotify) {
		syslog(LOG_INFO,"write workers: %"PRIu32"-\n",workers_total);
		lastnotify = workers_total;
	}
#endif
}

static inline void write_prepare_ip (char ipstr[16],uint32_t ip) {
	if (ipstr[0]==0) {
		snprintf(ipstr,16,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,(uint8_t)(ip>>24),(uint8_t)(ip>>16),(uint8_t)(ip>>8),(uint8_t)ip);
		ipstr[15]=0;
	}
}

/* main working thread */
void* write_worker(void *arg) {
	uint32_t z1,z2,z3;
	uint8_t *data;
	int fd;
	int i;
	struct pollfd pfd[2];
	uint32_t sent,rcvd;
	uint32_t hdrtosend;
	uint8_t sending_mode;
	uint8_t recvbuff[21];
	uint8_t sendbuff[32];
#ifdef HAVE_WRITEV
	struct iovec siov[2];
#endif
	uint8_t pipebuff[1024];
	int pipefd[2];
	uint8_t *wptr;
	const uint8_t *rptr;

	uint32_t reccmd;
	uint32_t recleng;
	uint64_t recchunkid;
	uint32_t recwriteid;
	uint8_t recstatus;

#ifdef WORKER_DEBUG
	uint32_t partialblocks;
	uint32_t bytessent;
	char debugchain[200];
	uint32_t cl;
#endif

	uint8_t *cpw;
	cspri chain[100];
	uint32_t chainminver;
	uint16_t chainelements;
	uint8_t cschain[6*99];
	uint32_t cschainsize;

	uint32_t inode;
	uint32_t chindx;
	uint32_t ip;
	uint16_t port;
	uint32_t srcip;
	uint64_t mfleng;
	uint64_t maxwroffset;
	uint64_t chunkid;
	uint32_t version;
	uint32_t nextwriteid;
	const uint8_t *csdata;
	uint32_t csdatasize;
	uint8_t csdataver;
	uint8_t westatus;
	uint8_t wrstatus;
	uint8_t chunkready;
	uint8_t unbreakable;
	uint8_t chunkopflags;
	int status;
	char csstrip[16];
	uint8_t waitforstatus;
	uint8_t donotstayidle;
	double opbegin;
	double start,now,lastrcvd,lastblock,lastsent;
	double workingtime,lrdiff,lbdiff;
	uint32_t wtotal;
	uint8_t cnt;
	uint8_t firsttime = 1;
	worker *w = (worker*)arg;

	uint8_t valid_offsets;
	uint64_t min_offset;
	uint64_t max_offset;

	inodedata *ind;
	chunkdata *chd;
	cblock *cb,*ncb,*rcb;
//	inodedata *ind;

	chainelements = 0;
	chindx = 0;

	if (pipe(pipefd)<0) {
		syslog(LOG_WARNING,"pipe error: %s",strerr(errno));
		return NULL;
	}

	for (;;) {
		for (i=0 ; i<chainelements ; i++) {
			csdb_writedec(chain[i].ip,chain[i].port);
		}
		chainelements=0;

		if (firsttime==0) {
			zassert(pthread_mutex_lock(&workerslock));
			workers_avail++;
			if (workers_avail > SUSTAIN_WORKERS) {
//				fprintf(stderr,"close worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
				write_data_close_worker(w);
				zassert(pthread_mutex_unlock(&workerslock));
				close_pipe(pipefd);
				return NULL;
			}
			zassert(pthread_mutex_unlock(&workerslock));
		}
		firsttime = 0;

		// get next job
		queue_get(jqueue,&z1,&z2,&data,&z3);

		zassert(pthread_mutex_lock(&workerslock));

		if (data==NULL) {
			write_data_close_worker(w);
			zassert(pthread_mutex_unlock(&workerslock));
			close_pipe(pipefd);
			return NULL;
		}

		workers_avail--;
		if (workers_avail==0 && workers_total<MAX_WORKERS) {
			write_data_spawn_worker();
		}
		zassert(pthread_mutex_unlock(&workerslock));

		chd = (chunkdata*)data;
		ind = chd->parent;

		zassert(pthread_mutex_lock(&(ind->lock)));

		if (chd->datachainhead) {
			chindx = chd->chindx;
			status = ind->status;
		} else {
			syslog(LOG_WARNING,"writeworker got inode with no data to write !!!");
			status = EINVAL;	// this should never happen, so status is not important - just anything
		}
		chunkready = chd->chunkready;
		chunkopflags = (chd->continueop?CHUNKOPFLAG_CONTINUEOP:0) | (chd->superuser?CHUNKOPFLAG_CANUSERESERVESPACE:0);

		zassert(pthread_mutex_unlock(&(ind->lock)));

		if (status) {
			write_job_end(chd,status,0);
			continue;
		}

		inode = ind->inode;
		chunkrwlock_wlock(inode,chindx);

		opbegin = 0; // make static code analysers happy
		if (optimeout>0.0) {
			opbegin = monotonic_seconds();
		}

		valid_offsets = 0;
		min_offset = 0;
		max_offset = 0;

		// syslog(LOG_NOTICE,"file: %"PRIu32", index: %"PRIu16" - debug1",inode,chindx);
		// get chunk data from master
//		start = monotonic_seconds();
		wrstatus = fs_writechunk(inode,chindx,chunkopflags,&csdataver,&mfleng,&chunkid,&version,&csdata,&csdatasize);
		if (wrstatus!=MFS_STATUS_OK) {
			if (wrstatus!=MFS_ERROR_LOCKED && wrstatus!=MFS_ERROR_EAGAIN) {
				if (wrstatus==MFS_ERROR_ENOENT) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",inode,chindx,mfsstrerr(wrstatus));
					write_job_end(chd,EBADF,0);
				} else if (wrstatus==MFS_ERROR_QUOTA) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",inode,chindx,mfsstrerr(wrstatus));
#ifdef EDQUOT
					write_job_end(chd,EDQUOT,0);
#else
					write_job_end(chd,ENOSPC,0);
#endif
				} else if (wrstatus==MFS_ERROR_NOSPACE) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",inode,chindx,mfsstrerr(wrstatus));
					write_job_end(chd,ENOSPC,0);
				} else if (wrstatus==MFS_ERROR_CHUNKLOST) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",inode,chindx,mfsstrerr(wrstatus));
					write_job_end(chd,ENXIO,0);
				} else if (wrstatus==MFS_ERROR_IO) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",inode,chindx,mfsstrerr(wrstatus));
					write_job_end(chd,EIO,0);
				} else if (wrstatus==MFS_ERROR_EROFS) {
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",inode,chindx,mfsstrerr(wrstatus));
					write_job_end(chd,EROFS,0);
				} else {
					if (chd->trycnt >= minlogretry) {
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",inode,chindx,mfsstrerr(wrstatus));
					}
					chd->trycnt++;
					if (chd->trycnt>=maxretries) {
						if (wrstatus==MFS_ERROR_NOCHUNKSERVERS) {
							write_job_end(chd,ENOSPC,0);
						} else if (wrstatus==MFS_ERROR_CSNOTPRESENT) {
							write_job_end(chd,ENXIO,0);
						} else {
							write_job_end(chd,EIO,0);
						}
					} else {
						write_delayed_enqueue(chd,1000+((chd->trycnt<30)?((chd->trycnt-1)*300000):10000000));
					}
				}
			} else {
				if (chd->trycnt<=2) {
					chd->trycnt++;
					write_delayed_enqueue(chd,1000);
				} else if (chd->trycnt<=6) {
					chd->trycnt++;
					write_delayed_enqueue(chd,100000);
				} else {
					write_delayed_enqueue(chd,500000);
				}
			}
			chunkrwlock_wunlock(inode,chindx);
			continue;	// get next job
		}

		chunksdatacache_insert(inode,chindx,chunkid,version,csdataver,csdata,csdatasize);
#ifdef MFSMOUNT
		fdcache_invalidate(inode);
#endif

//		now = monotonic_seconds();
//		fprintf(stderr,"fs_writechunk time: %.3lf\n",(now-start));

		if (csdata!=NULL && csdatasize>0) {
			chainelements = csorder_sort(chain,csdataver,csdata,csdatasize,1);
		} else {
			chainelements = 0;
		}

		if (csdata==NULL || csdatasize==0 || chainelements==0) {
			chainelements = 0;
			zassert(pthread_mutex_lock(&(ind->lock)));
			chd->trycnt+=6;
			unbreakable = chd->unbreakable;
			if (chd->trycnt>=maxretries) {
				unbreakable = 0; // unlock chunk on error
			}
			chd->continueop = unbreakable;
			zassert(pthread_mutex_unlock(&(ind->lock)));
			if (unbreakable==0) {
				fs_writeend(chunkid,inode,chindx,0,0);
			}
			if (chd->trycnt >= minlogretry) {
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies",inode,chindx,chunkid,version);
			}
			if (chd->trycnt>=maxretries) {
				write_job_end(chd,ENXIO,0);
			} else {
				write_delayed_enqueue(chd,60000000);
			}
			chunkrwlock_wunlock(inode,chindx);
			continue;
		}
		ip = chain[0].ip;
		port = chain[0].port;
		chainminver = chain[0].version;
		csdb_writeinc(ip,port);
		csstrip[0] = 0;
		cpw = cschain;
		cschainsize = 0;
		for (i=1 ; i<chainelements ; i++) {
			csdb_writeinc(chain[i].ip,chain[i].port);
			if (chain[i].version < chainminver) {
				chainminver = chain[i].version;
			}
			put32bit(&cpw,chain[i].ip);
			put16bit(&cpw,chain[i].port);
			cschainsize += 6;
		}
#if 0
		cp = csdata;
		cpe = csdata+csdatasize;
		chainminver = 0xFFFFFFFF;
		cpw = cschain;
		cschainsize = 0;
		while (cp<cpe && chainelements<100) {
			tmpip = get32bit(&cp);
			tmpport = get16bit(&cp);
			if (csdataver>0) {
				tmpver = get32bit(&cp);
			} else {
				tmpver = 0;
			}
			if (csdataver>1) {
				tmplabelmask = get32bit(&cp);
			} else {
				tmplabelmask = 0;
			}
			chainip[chainelements] = tmpip;
			chainport[chainelements] = tmpport;
			csdb_writeinc(tmpip,tmpport);
			if (tmpver<chainminver) {
				chainminver = tmpver;
			}
			if (chainelements==0) {
				ip = tmpip;
				port = tmpport;
			} else {
				put32bit(&cpw,tmpip);
				put16bit(&cpw,tmpport);
				cschainsize += 6;
			}
			chainelements++;
		}

		if (cp<cpe) {
			fs_writeend(chunkid,inode,0,0);
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are too many copies",inode,chindx,chunkid,version);
			chd->trycnt+=6;
			if (chd->trycnt>=maxretries) {
				write_job_end(chd,ENXIO,0);
			} else {
				write_delayed_enqueue(chd,60000000);
			}
			continue;
		}
#endif
//		chain = csdata;
//		ip = get32bit(&chain);
//		port = get16bit(&chain);
//		chainsize = csdatasize-6;

		start = monotonic_seconds();

/*
		if (csdatasize>CSDATARESERVE) {
			csdatasize = CSDATARESERVE;
		}
		memcpy(wrec->csdata,csdata,csdatasize);
		wrec->csdatasize=csdatasize;
		while (csdatasize>=6) {
			tmpip = get32bit(&csdata);
			tmpport = get16bit(&csdata);
			csdatasize-=6;
			csdb_writeinc(tmpip,tmpport);
		}
*/

		// make connection to cs
		srcip = fs_getsrcip();
		fd = conncache_get(ip,port);
		if (fd<0) {
			uint32_t connmaxtry;
			zassert(pthread_mutex_lock(&(ind->lock)));
			connmaxtry = (chd->trycnt*2)+2;
			if (connmaxtry>10) {
				connmaxtry = 10;
			}
			zassert(pthread_mutex_unlock(&(ind->lock)));
			cnt=0;
			while (cnt<connmaxtry) {
				fd = tcpsocket();
				if (fd<0) {
					syslog(LOG_WARNING,"writeworker: can't create tcp socket: %s",strerr(errno));
					break;
				}
				if (srcip) {
					if (tcpnumbind(fd,srcip,0)<0) {
						syslog(LOG_WARNING,"writeworker: can't bind socket to given ip: %s",strerr(errno));
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
						if (chd->trycnt >= minlogretry) {
							write_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"writeworker: can't connect to (%s:%"PRIu16"): %s",csstrip,port,strerr(err));
						}
						zassert(pthread_mutex_unlock(&(ind->lock)));
					}
					close(fd);
					fd=-1;
				} else {
					uint32_t mip,pip;
					uint16_t mport,pport;
					tcpgetpeer(fd,&pip,&pport);
					tcpgetmyaddr(fd,&mip,&mport);
#ifdef WDEBUG
					fprintf(stderr,"connection ok (%"PRIX32":%"PRIu16"->%"PRIX32":%"PRIu16")\n",mip,mport,pip,pport);
#endif
					cnt=connmaxtry;
				}
			}
		}
		if (fd<0) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			chd->trycnt++;
			unbreakable = chd->unbreakable;
			if (chd->trycnt>=maxretries) {
				unbreakable = 0; // unlock chunk on error
			}
			chd->continueop = unbreakable;
			zassert(pthread_mutex_unlock(&(ind->lock)));
			if (unbreakable==0) {
				fs_writeend(chunkid,inode,chindx,0,0);
			}
			if (chd->trycnt>=maxretries) {
				write_job_end(chd,EIO,0);
			} else {
				write_delayed_enqueue(chd,1000+((chd->trycnt<30)?((chd->trycnt-1)*300000):10000000));
			}
			chunkrwlock_wunlock(inode,chindx);
			continue;
		}
		if (tcpnodelay(fd)<0) {
			syslog(LOG_WARNING,"writeworker: can't set TCP_NODELAY: %s",strerr(errno));
		}

		if (chunkready==0) {
			zassert(pthread_mutex_lock(&(ind->lock)));
			if (chd->chunkready==0) {
				chd->chunkready = 1;
//				if (ind->chunkwaiting>0) {
					zassert(pthread_cond_broadcast(&(ind->chunkcond)));
//				}
			}
			zassert(pthread_mutex_unlock(&(ind->lock)));
		}

#ifdef WORKER_DEBUG
		partialblocks=0;
		bytessent=0;
#endif
		nextwriteid=1;

		pfd[0].fd = fd;
		pfd[1].fd = pipefd[0];
		rcvd = 0;
		sent = 0;
		waitforstatus=1;
		wptr = sendbuff;

		put32bit(&wptr,CLTOCS_WRITE);
		if (chainminver>=VERSION2INT(1,7,32)) {
			put32bit(&wptr,13+cschainsize);
			put8bit(&wptr,1);
			hdrtosend = 21;
		} else {
			put32bit(&wptr,12+cschainsize);
			hdrtosend = 20;
		}

		put64bit(&wptr,chunkid);
		put32bit(&wptr,version);
		sending_mode = 1;
// debug:	syslog(LOG_NOTICE,"writeworker: init packet prepared");
		cb = NULL;

		status = 0;
		wrstatus = MFS_STATUS_OK;

		lastrcvd = 0.0;
		lastsent = 0.0;
		lastblock = 0.0;

		donotstayidle = 0;
//		firstloop = 1;

		do {
			now = monotonic_seconds();
			zassert(pthread_mutex_lock(&workerslock));
			wtotal = workers_total;
			zassert(pthread_mutex_unlock(&workerslock));

			if (optimeout>0.0 && now - opbegin > optimeout) {
				status = EIO;
				break;
			}
			zassert(pthread_mutex_lock(&(ind->lock)));

//			if (ind->status!=0) {
//				zassert(pthread_mutex_unlock(&glock));
//				break;
//			}

			if (lastrcvd==0.0) {
				lastrcvd = now;
			} else {
				lrdiff = now - lastrcvd;
				if (lrdiff>=CHUNKSERVER_ACTIVITY_TIMEOUT) {
					if (chd->trycnt >= minlogretry) {
						write_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") was timed out (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,waitforstatus,chd->trycnt+1);
					}
					zassert(pthread_mutex_unlock(&(ind->lock)));
					break;
				}
			}
			if (lastblock==0.0) {
				lbdiff = NEXT_BLOCK_DELAY; // first block should be send immediately
			} else {
				lbdiff = now - lastblock;
			}
			workingtime = now - start;

			chd->waitingworker=1;
			chd->wakeup_fd = pipefd[1];

			if (sending_mode==0 && workingtime<WORKER_BUSY_LAST_SEND_TIMEOUT+((wtotal>HEAVYLOAD_WORKERS)?0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT) && waitforstatus<64) {
				if (cb==NULL) {
					ncb = chd->datachainhead;
				} else {
					ncb = cb->next;
				}
				if (ncb) {
					if (ncb->to-ncb->from==MFSBLOCKSIZE || lbdiff>=NEXT_BLOCK_DELAY || ncb->next!=NULL || ind->flushwaiting) {
						cb = ncb;
						sending_mode = 2;
					} else {
						chd->waitingworker = 2; // wait for block expand
						chd->wakeup_fd = pipefd[1];
					}
				}
				if (sending_mode==2) {
					uint64_t offset_from;
					uint64_t offset_to;

					cb->writeid = nextwriteid++;
// debug:				syslog(LOG_NOTICE,"writeworker: data packet prepared (writeid:%"PRIu32",pos:%"PRIu16")",cb->writeid,cb->pos);
					waitforstatus++;
					wptr = sendbuff;
					put32bit(&wptr,CLTOCS_WRITE_DATA);
					put32bit(&wptr,24+(cb->to-cb->from));
					put64bit(&wptr,chunkid);
					put32bit(&wptr,cb->writeid);
					put16bit(&wptr,cb->pos);
					put16bit(&wptr,cb->from);
					put32bit(&wptr,cb->to-cb->from);
					put32bit(&wptr,mycrc32(0,cb->data+cb->from,cb->to-cb->from));
#ifdef WORKER_DEBUG
					if (cb->to-cb->from<MFSBLOCKSIZE) {
						partialblocks++;
					}
					bytessent+=(cb->to-cb->from);
#endif
					sent = 0;
					lastblock = now;
					lastsent = now;
					offset_from = chindx;
					offset_from <<= MFSCHUNKBITS;
					offset_from += cb->pos*MFSBLOCKSIZE;
					offset_to = offset_from;
					offset_from += cb->from;
					offset_to += cb->to;
					if (valid_offsets) {
						if (offset_from < min_offset) {
							min_offset = offset_from;
						}
						if (offset_to > max_offset) {
							max_offset = offset_to;
						}
					} else {
						min_offset = offset_from;
						max_offset = offset_to;
						valid_offsets = 1;
					}
				} else if (lastsent+WORKER_NOP_INTERVAL<now && chainminver>=VERSION2INT(1,7,32)) {
					wptr = sendbuff;
					put32bit(&wptr,ANTOAN_NOP);
					put32bit(&wptr,0);
					sent = 0;
					sending_mode = 3;
					lastsent = now;
				}
			}

#ifdef WORKER_DEBUG
			fprintf(stderr,"workerloop: waitforstatus:%u workingtime:%.6lf workers_total:%u lbdiff:%.6lf donotstayidle:%u\n",waitforstatus,workingtime,wtotal,lbdiff,donotstayidle);
#endif
			if (waitforstatus>0) {
				if (workingtime>WORKER_BUSY_LAST_SEND_TIMEOUT+WORKER_BUSY_WAIT_FOR_STATUS+((wtotal>HEAVYLOAD_WORKERS)?0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT)) { // timeout
					chd->waitingworker = 0;
					chd->wakeup_fd = -1;
					zassert(pthread_mutex_unlock(&(ind->lock)));
					break;
				}
			} else {
				if (lbdiff>=WORKER_IDLE_TIMEOUT || donotstayidle || wtotal>HEAVYLOAD_WORKERS) {
					chd->waitingworker = 0;
					chd->wakeup_fd = -1;
					zassert(pthread_mutex_unlock(&(ind->lock)));
					break;
				}
			}


			zassert(pthread_mutex_unlock(&(ind->lock)));

			switch (sending_mode) {
				case 1:
					if (sent<hdrtosend) {
#ifdef HAVE_WRITEV
						if (cschainsize>0) {
							siov[0].iov_base = (void*)(sendbuff+sent);
							siov[0].iov_len = hdrtosend-sent;
							siov[1].iov_base = (void*)cschain;	// discard const (safe - because it's used in writev)
							siov[1].iov_len = cschainsize;
							i = writev(fd,siov,2);
						} else {
#endif
							i = universal_write(fd,sendbuff+sent,hdrtosend-sent);
#ifdef HAVE_WRITEV
						}
#endif
					} else {
						i = universal_write(fd,cschain+(sent-hdrtosend),cschainsize-(sent-hdrtosend));
					}
					if (i>=0) {
						sent+=i;
						if (sent==hdrtosend+cschainsize) {
							sending_mode = 0;
						}
					}
					break;
				case 2:
					if (sent<32) {
#ifdef HAVE_WRITEV
						siov[0].iov_base = (void*)(sendbuff+sent);
						siov[0].iov_len = 32-sent;
						siov[1].iov_base = (void*)(cb->data+cb->from);
						siov[1].iov_len = cb->to-cb->from;
						i = writev(fd,siov,2);
#else
						i = universal_write(fd,sendbuff+sent,32-sent);
#endif
					} else {
						i = universal_write(fd,cb->data+cb->from+(sent-32),cb->to-cb->from-(sent-32));
					}
					if (i>=0) {
						sent+=i;
						if (sent==32+cb->to-cb->from) {
							sending_mode = 0;
						}
					}
					break;
				case 3:
					i = universal_write(fd,sendbuff+sent,8-sent);
					if (i>=0) {
						sent+=i;
						if (sent==8) {
							sending_mode = 0;
						}
					}
					break;
				default:
					i=0;
			}

			if (i<0) {
				if (ERRNO_ERROR && errno!=EINTR) {
					if (chd->trycnt >= minlogretry) {
						write_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: write to (%s:%"PRIu16") error: %s / NEGWRITE (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(errno),waitforstatus,chd->trycnt+1);
					}
					status=EIO;
					break;
				}
			}

			pfd[0].events = POLLIN | (sending_mode?POLLOUT:0);
			pfd[0].revents = 0;
			pfd[1].events = POLLIN;
			pfd[1].revents = 0;
			if (poll(pfd,2,100)<0) { /* correct timeout - in msec */
				if (errno!=EINTR) {
					if (chd->trycnt >= minlogretry) {
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: poll error: %s (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,strerr(errno),waitforstatus,chd->trycnt+1);
					}
					status=EIO;
					break;
				}
			}
			zassert(pthread_mutex_lock(&(ind->lock)));	// make helgrind happy
			chd->waitingworker = 0;
			chd->wakeup_fd = -1;
			donotstayidle = (ind->flushwaiting>0 || ind->status!=0 || ind->chunkscnt>=MAX_SIM_CHUNKS)?1:0;
			zassert(pthread_mutex_unlock(&(ind->lock)));	// make helgrind happy
			if (pfd[1].revents&POLLIN) {	// used just to break poll - so just read all data from pipe to empty it
				i = universal_read(pipefd[0],pipebuff,1024);
				if (i<0) { // mainly to make happy static code analyzers
					if (chd->trycnt >= minlogretry) {
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: read pipe error: %s (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,strerr(errno),waitforstatus,chd->trycnt+1);
					}
				}
			}
			if (pfd[0].revents&POLLHUP) {
				if (chd->trycnt >= minlogretry) {
					write_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") was reset by peer / POLLHUP (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,waitforstatus,chd->trycnt+1);
				}
				status=EIO;
				break;
			}
			if (pfd[0].revents&POLLERR) {
				if (chd->trycnt >= minlogretry) {
					write_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") got error status / POLLERR (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,waitforstatus,chd->trycnt+1);
				}
				status=EIO;
				break;
			}
			if (pfd[0].revents&POLLIN) {
				i = universal_read(fd,recvbuff+rcvd,21-rcvd);
				if (i==0) { 	// connection reset by peer or read error
					if (chd->trycnt >= minlogretry) {
						write_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") was reset by peer / ZEROREAD (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,waitforstatus,chd->trycnt+1);
					}
					status=EIO;
					break;
				}
				if (i<0) {
					if (errno!=EINTR) {
						if (chd->trycnt >= minlogretry) {
							write_prepare_ip(csstrip,ip);
							syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: read from (%s:%"PRIu16") error: %s (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",inode,chindx,chunkid,version,csstrip,port,strerr(errno),waitforstatus,chd->trycnt+1);
						}
						status=EIO;
						break;
					} else {
						i=0;
					}
				}
				lastrcvd = monotonic_seconds();
				rcvd+=i;
				// do not accept ANTOAN_UNKNOWN_COMMAND and ANTOAN_BAD_COMMAND_SIZE here - only ANTOAN_NOP
				if (rcvd>=8 && recvbuff[7]==0 && recvbuff[6]==0 && recvbuff[5]==0 && recvbuff[4]==0 && recvbuff[3]==0 && recvbuff[2]==0 && recvbuff[1]==0 && recvbuff[0]==0) {	// ANTOAN_NOP packet received - skip it
					if (rcvd>8) {
						memmove(recvbuff,recvbuff+8,rcvd-8);
						rcvd-=8;
					}
				}
				if (rcvd==21) {
					rptr = recvbuff;
					reccmd = get32bit(&rptr);
					recleng = get32bit(&rptr);
					recchunkid = get64bit(&rptr);
					recwriteid = get32bit(&rptr);
					recstatus = get8bit(&rptr);
					if (reccmd!=CSTOCL_WRITE_STATUS || recleng!=13) {
						syslog(LOG_WARNING,"writeworker: got unrecognized packet from chunkserver (cmd:%"PRIu32",leng:%"PRIu32")",reccmd,recleng);
						status=EIO;
						break;
					}
					if (recchunkid!=chunkid) {
						syslog(LOG_WARNING,"writeworker: got unexpected packet (expected chunkdid:%"PRIu64",packet chunkid:%"PRIu64")",chunkid,recchunkid);
						status=EIO;
						break;
					}
					if (recstatus!=MFS_STATUS_OK) {
						if (chd->trycnt >= minlogretry) {
							syslog(LOG_WARNING,"writeworker: write error: %s",mfsstrerr(recstatus));
						}
						wrstatus=recstatus;
						break;
					}
// debug:				syslog(LOG_NOTICE,"writeworker: received status ok for writeid:%"PRIu32,recwriteid);
					if (recwriteid>0) {
						zassert(pthread_mutex_lock(&(ind->lock)));
						for (rcb = chd->datachainhead ; rcb && rcb->writeid!=recwriteid ; rcb=rcb->next) {}
						if (rcb==NULL) {
							syslog(LOG_WARNING,"writeworker: got unexpected status (writeid:%"PRIu32")",recwriteid);
							zassert(pthread_mutex_unlock(&(ind->lock)));
							status=EIO;
							break;
						}
						if (rcb==cb) {	// current block
// debug:						syslog(LOG_NOTICE,"writeworker: received status for current block");
							if (sending_mode==2) {	// got status ok before all data had been sent - error
								syslog(LOG_WARNING,"writeworker: got status OK before all data have been sent");
								zassert(pthread_mutex_unlock(&(ind->lock)));
								status=EIO;
								break;
							} else {
								cb = NULL;
							}
						}
						if (rcb->prev) {
							rcb->prev->next = rcb->next;
						} else {
							chd->datachainhead = rcb->next;
						}
						if (rcb->next) {
							rcb->next->prev = rcb->prev;
						} else {
							chd->datachaintail = rcb->prev;
						}
						maxwroffset = (((uint64_t)(chindx))<<MFSCHUNKBITS)+(((uint32_t)(rcb->pos))<<MFSBLOCKBITS)+rcb->to;
						if (maxwroffset>mfleng) {
							mfleng=maxwroffset;
						}
						write_cb_release(ind,rcb);
						zassert(pthread_mutex_unlock(&(ind->lock)));
					}
					waitforstatus--;
					rcvd=0;
				}
			}
		} while (1);

		if (waitforstatus==0 && chainminver>=VERSION2INT(1,7,32)) {
			wptr = sendbuff;
			put32bit(&wptr,CLTOCS_WRITE_FINISH);
			put32bit(&wptr,12);
			put64bit(&wptr,chunkid);
			put32bit(&wptr,version);
			if (universal_write(fd,sendbuff,20)==20) {
				conncache_insert(ip,port,fd);
			} else {
				tcpclose(fd);
			}
		} else {
			tcpclose(fd);
		}

#ifdef WORKER_DEBUG
		now = monotonic_seconds();
		workingtime = now - start;

		cl=0;
		for (cnt=0 ; cnt<chainelements ; cnt++) {
			cl+=snprintf(debugchain+cl,200-cl,"%u.%u.%u.%u:%u->",(chain[cnt].ip>>24)&255,(chain[cnt].ip>>16)&255,(chain[cnt].ip>>8)&255,chain[cnt].ip&255,chain[cnt].port);
		}
		if (cl>=2) {
			debugchain[cl-2]='\0';
		}
		syslog(LOG_NOTICE,"worker %lu sent %"PRIu32" blocks (%"PRIu32" partial) of chunk %016"PRIX64"_%08"PRIX32", received status for %"PRIu32" blocks (%"PRIu32" lost), bw: %.6lfMB/s ( %"PRIu32" B / %.6lf s ), chain: %s",(unsigned long)arg,nextwriteid-1,partialblocks,chunkid,version,nextwriteid-1-waitforstatus,waitforstatus,(double)bytessent/workingtime,bytessent,workingtime,debugchain);
#endif

		if (status!=0 || wrstatus!=MFS_STATUS_OK) {
			if (wrstatus!=MFS_STATUS_OK) {	// convert MFS status to OS errno
				if (wrstatus==MFS_ERROR_NOSPACE) {
					status=ENOSPC;
				} else {
					status=EIO;
				}
			}
		} else if (nextwriteid-1 == waitforstatus) { // nothing has been written - treat it as EIO
			status=EIO;
		}

		zassert(pthread_mutex_lock(&(ind->lock)));	// make helgrind happy
		unbreakable = chd->unbreakable;

		if (optimeout>0.0 && monotonic_seconds() - opbegin > optimeout) {
			unbreakable = 0;
		} else if (status!=0) {
			if (wrstatus!=MFS_ERROR_NOTDONE) {
				chd->trycnt++;
			}
			if (chd->trycnt>=maxretries) {
				unbreakable = 0;
			}
		} else {
			unbreakable = 0; // operation finished
		}
		chd->continueop = unbreakable;
		zassert(pthread_mutex_unlock(&(ind->lock)));
		if (unbreakable==0) {
//			for (cnt=0 ; cnt<10 ; cnt++) {
			westatus = fs_writeend(chunkid,inode,chindx,mfleng,0);
//				if (westatus==MFS_ERROR_ENOENT || westatus==MFS_ERROR_QUOTA) { // can't change -> do not repeat
//					break;
//				} else if (westatus!=MFS_STATUS_OK) {
//					if (optimeout>0.0 && monotonic_seconds() - opbegin > optimeout) {
//						westatus = MFS_ERROR_EIO;
//						break;
//					}
//					portable_usleep(100000+(10000<<cnt));
//					zassert(pthread_mutex_lock(&(ind->lock)));	// make helgrind happy
//					zassert(pthread_mutex_unlock(&(ind->lock)));
//				} else {
//					break;
//				}
//			}
		} else {
			westatus = MFS_STATUS_OK;
		}

		if (optimeout>0.0 && monotonic_seconds() - opbegin > optimeout) {
			write_job_end(chd,EIO,0);
		} else if (westatus==MFS_ERROR_ENOENT) {
			write_job_end(chd,EBADF,0);
		} else if (westatus==MFS_ERROR_QUOTA) {
			write_job_end(chd,EDQUOT,0);
		} else if (westatus==MFS_ERROR_IO) {
			write_job_end(chd,EIO,0);
		} else if (westatus!=MFS_STATUS_OK) {
			write_job_end(chd,ENXIO,0);
		} else {
			if (status!=0) {
				if (chd->trycnt>=maxretries) {
					write_job_end(chd,status,0);
				} else {
					if (wrstatus==MFS_ERROR_NOTDONE) {
						write_job_end(chd,0,300000);
					} else {
						write_job_end(chd,0,1000+((chd->trycnt<30)?((chd->trycnt-1)*300000):10000000));
					}
				}
			} else {
				if (valid_offsets) {
					read_inode_clear_cache(inode,min_offset,max_offset-min_offset);
				}
				// read_inode_set_length_async(inode,mfleng,0);
				write_job_end(chd,0,0);
			}
		}
		chunkrwlock_wunlock(inode,chindx);
	}
	return NULL;
}

void write_data_init (uint32_t cachesize,uint32_t retries,uint32_t timeout,uint32_t logretry) {
	uint32_t i;
	size_t mystacksize;
//	sigset_t oldset;
//	sigset_t newset;

	cacheblockcount = (cachesize/MFSBLOCKSIZE);
	maxretries = retries;
	if (optimeout>0) {
		optimeout = timeout;
	} else {
		optimeout = 0.0;
	}
	minlogretry = logretry;
	if (cacheblockcount<10) {
		cacheblockcount=10;
	}
	zassert(pthread_mutex_init(&hashlock,NULL));
	zassert(pthread_mutex_init(&workerslock,NULL));
	zassert(pthread_cond_init(&worker_term_cond,NULL));
	worker_term_waiting = 0;

	zassert(pthread_mutex_init(&fcblock,NULL));
	zassert(pthread_cond_init(&fcbcond,NULL));
	fcbwaiting=0;
	cacheblocks = malloc(sizeof(cblock)*cacheblockcount);
	passert(cacheblocks);
	for (i=0 ; i<cacheblockcount-1 ; i++) {
		cacheblocks[i].next = cacheblocks+(i+1);
	}
	cacheblocks[cacheblockcount-1].next = NULL;
	freecblockshead = cacheblocks;
	freecacheblocks = cacheblockcount;

	idhash = malloc(sizeof(inodedata*)*IDHASHSIZE);
	passert(idhash);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		idhash[i]=NULL;
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
//	zassert(pthread_sigmask(SIG_BLOCK, &newset, &oldset));
//	zassert(pthread_create(&dqueue_worker_th,&worker_thattr,write_dqueue_worker,NULL));
//	zassert(pthread_sigmask(SIG_SETMASK, &oldset, NULL));

	zassert(pthread_mutex_lock(&workerslock));
	workers_avail = 0;
	workers_total = 0;
	write_data_spawn_worker();
	zassert(pthread_mutex_unlock(&workerslock));
#ifdef BUFFER_DEBUG
	zassert(pthread_create(&info_worker_th,&worker_thattr,write_info_worker,NULL));
#endif
}

void write_data_term(void) {
	uint32_t i;
	inodedata *ind,*indn;
	chunkdata *chd,*chdn;

//	queue_close(dqueue);
	queue_close(jqueue);
	zassert(pthread_mutex_lock(&workerslock));
	while (workers_total>0) {
		worker_term_waiting++;
		zassert(pthread_cond_wait(&worker_term_cond,&workerslock));
	}
	zassert(pthread_mutex_unlock(&workerslock));
//	zassert(pthread_join(dqueue_worker_th,NULL));
//	queue_delete(dqueue);
	queue_delete(jqueue);
	zassert(pthread_mutex_lock(&hashlock));
	for (i=0 ; i<IDHASHSIZE ; i++) {
		for (ind = idhash[i] ; ind ; ind = indn) {
			indn = ind->next;
			zassert(pthread_mutex_lock(&(ind->lock)));
			chd = ind->chunks;
			while (chd) {
				chdn = chd->next;
				write_free_chunkdata(chd);
				chd = chdn;
			}
			zassert(pthread_mutex_unlock(&(ind->lock)));
			zassert(pthread_cond_destroy(&(ind->flushcond)));
			zassert(pthread_cond_destroy(&(ind->writecond)));
			zassert(pthread_mutex_destroy(&(ind->lock)));
			free(ind);
		}
	}
	free(idhash);
	zassert(pthread_mutex_unlock(&hashlock));
	free(cacheblocks);
	zassert(pthread_attr_destroy(&worker_thattr));
	zassert(pthread_cond_destroy(&worker_term_cond));
	zassert(pthread_cond_destroy(&fcbcond));
	zassert(pthread_mutex_destroy(&fcblock));
	zassert(pthread_mutex_destroy(&workerslock));
	zassert(pthread_mutex_destroy(&hashlock));
}

int write_cb_expand(chunkdata *chd,cblock *cb,uint32_t from,uint32_t to,const uint8_t *data) {
	if (cb->writeid>0 || from>cb->to || to<cb->from) {	// can't expand
		return -1;
	}
	memcpy(cb->data+from,data,to-from);
	if (from<cb->from) {
		cb->from = from;
	}
	if (to>cb->to) {
		cb->to = to;
	}
	if (cb->to-cb->from==MFSBLOCKSIZE && cb->next==NULL && chd->waitingworker==2) {
		if (universal_write(chd->wakeup_fd," ",1)!=1) {
			syslog(LOG_ERR,"can't write to pipe !!!");
		}
		chd->waitingworker = 0;
		chd->wakeup_fd = -1;
	}
	return 0;
}

int write_block(inodedata *ind,uint32_t chindx,uint16_t pos,uint32_t from,uint32_t to,const uint8_t *data,uint8_t superuser) {
	cblock *cb,*ncb;
	chunkdata *chd;
	uint8_t newchunk;

	ncb = write_cb_acquire(ind);
	zassert(pthread_mutex_lock(&(ind->lock)));
	for (chd=ind->chunks ; chd ; chd=chd->next) {
		if (chd->chindx == chindx) {
			if (superuser) {
				chd->superuser = 1;
			}
			for (cb=chd->datachaintail ; cb ; cb=cb->prev) {
				if (cb->pos==pos) {
					if (write_cb_expand(chd,cb,from,to,data)==0) {
						write_cb_release(ind,ncb);
						zassert(pthread_mutex_unlock(&(ind->lock)));
						return 0;
					}
					break;
				}
			}
			break;
		}
	}
	ncb->pos = pos;
	ncb->from = from;
	ncb->to = to;
	memcpy(ncb->data+from,data,to-from);
	if (chd==NULL) {
		chd = write_new_chunkdata(ind,chindx);
		if (superuser) {
			chd->superuser = 1;
		}
		newchunk = 1;
	} else {
		newchunk = 0;
	}
	ncb->prev = chd->datachaintail;
	ncb->next = NULL;
	if (chd->datachaintail!=NULL) {
		chd->datachaintail->next = ncb;
	} else {
		chd->datachainhead = ncb;
	}
	chd->datachaintail = ncb;
	if (newchunk) {
		write_test_chunkdata(ind);
	} else {
		if (chd->waitingworker) {
			if (universal_write(chd->wakeup_fd," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			chd->waitingworker = 0;
			chd->wakeup_fd = -1;
		}
	}
	zassert(pthread_mutex_unlock(&(ind->lock)));
	return 0;
}

int write_data(void *vid,uint64_t offset,uint32_t size,const uint8_t *data,uint8_t superuser) {
	uint32_t chindx;
	uint16_t pos;
	uint32_t from;
	int status;
	inodedata *ind = (inodedata*)vid;
	if (ind==NULL) {
		return EIO;
	}
//	int64_t s,e;

//	s = monotonic_useconds();
	zassert(pthread_mutex_lock(&(ind->lock)));

//	syslog(LOG_NOTICE,"write_data: inode:%"PRIu32" offset:%"PRIu64" size:%"PRIu32,ind->inode,offset,size);
	status = ind->status;
	if (status==0) {
		if (offset+size>ind->maxfleng) {	// move fleng
			ind->maxfleng = offset+size;
		}
		ind->writewaiting++;
		while (ind->flushwaiting>0) {
			zassert(pthread_cond_wait(&(ind->writecond),&(ind->lock)));
		}
		ind->writewaiting--;
	}
	zassert(pthread_mutex_unlock(&(ind->lock)));
	if (status!=0) {
		return status;
	}

	chindx = offset>>MFSCHUNKBITS;
	pos = (offset&MFSCHUNKMASK)>>MFSBLOCKBITS;
	from = offset&MFSBLOCKMASK;
	while (size>0) {
		if (size>MFSBLOCKSIZE-from) {
			if (write_block(ind,chindx,pos,from,MFSBLOCKSIZE,data,superuser)<0) {
				return EIO;
			}
			size -= (MFSBLOCKSIZE-from);
			data += (MFSBLOCKSIZE-from);
			from = 0;
			pos++;
			if (pos==1024) {
				pos = 0;
				chindx++;
			}
		} else {
			if (write_block(ind,chindx,pos,from,from+size,data,superuser)<0) {
				return EIO;
			}
			size = 0;
		}
	}
//	e = monotonic_useconds();
//	syslog(LOG_NOTICE,"write_data time: %"PRId64,e-s);
	return 0;
}

void* write_data_new(uint32_t inode,uint64_t fleng) {
	inodedata* ind;
	ind = write_get_inodedata(inode,fleng);
	if (ind==NULL) {
		return NULL;
	}
	return ind;
}

static int write_data_do_chunk_wait(inodedata *ind) {
	int ret;
	chunkdata *chd;
#ifdef WDEBUG
	int64_t s,e;

	s = monotonic_useconds();
#endif
	zassert(pthread_mutex_lock(&(ind->lock)));
//	ind->chunkwaiting++;
	do {
		chd=NULL;
		if (ind->status==0) {
			for (chd = ind->chunks ; chd!=NULL && chd->chunkready ; chd=chd->next) {}
			if (chd!=NULL) {
#ifdef WDEBUG
				syslog(LOG_NOTICE,"(inode:%"PRIu32") chunk_ready: wait ...",ind->inode);
#endif
				zassert(pthread_cond_wait(&(ind->chunkcond),&(ind->lock)));
#ifdef WDEBUG
				syslog(LOG_NOTICE,"(inode:%"PRIu32") chunk_ready: woken up",ind->inode);
#endif
			}
		}
	} while (ind->status==0 && chd!=NULL);
//	ind->chunkwaiting--;
	for (chd = ind->chunks ; chd!=NULL ; chd=chd->next) {
		chd->unbreakable = 1;
	}
	ret = ind->status;
	zassert(pthread_mutex_unlock(&(ind->lock)));
#ifdef WDEBUG
	e = monotonic_useconds();
	syslog(LOG_NOTICE,"flush time: %"PRId64,e-s);
#endif
	return ret;
}

static int write_data_will_flush_wait(inodedata *ind) {
	int ret;
	zassert(pthread_mutex_lock(&(ind->lock)));
	ret = ind->chunkscnt;
	zassert(pthread_mutex_unlock(&(ind->lock)));
	return ret;
}

static int write_data_do_flush(inodedata *ind,uint8_t releaseflag) {
	int ret;
	chunkdata *chd;
#ifdef WDEBUG
	int64_t s,e;

	s = monotonic_useconds();
#endif
	zassert(pthread_mutex_lock(&(ind->lock)));
	ind->flushwaiting++;
	while (ind->chunkscnt>0) {
		for (chd = ind->chunks ; chd!=NULL ; chd=chd->next) {
			if (chd->waitingworker) {
				if (universal_write(chd->wakeup_fd," ",1)!=1) {
					syslog(LOG_ERR,"can't write to pipe !!!");
				}
				chd->waitingworker = 0;
				chd->wakeup_fd = -1;
			}
		}
#ifdef WDEBUG
		syslog(LOG_NOTICE,"(inode:%"PRIu32") flush: wait ...",ind->inode);
#endif
		zassert(pthread_cond_wait(&(ind->flushcond),&(ind->lock)));
#ifdef WDEBUG
		syslog(LOG_NOTICE,"(inode:%"PRIu32") flush: woken up",ind->inode);
#endif
	}
	ind->flushwaiting--;
	if (ind->flushwaiting==0 && ind->writewaiting>0) {
		zassert(pthread_cond_broadcast(&(ind->writecond)));
	}
	ret = ind->status;
	zassert(pthread_mutex_unlock(&(ind->lock)));
	if (releaseflag) {
		write_free_inodedata(ind);
	}
#ifdef WDEBUG
	e = monotonic_useconds();
	syslog(LOG_NOTICE,"flush time: %"PRId64,e-s);
#endif
	return ret;
}

int write_data_flush(void *vid) {
	if (vid==NULL) {
		return EIO;
	}
	return write_data_do_flush((inodedata*)vid,0);
}

int write_data_chunk_wait(void *vid) {
	if (vid==NULL) {
		return EIO;
	}
	return write_data_do_chunk_wait((inodedata*)vid);
}

void write_data_inode_setmaxfleng(uint32_t inode,uint64_t maxfleng) {
	inodedata* ind;
	ind = write_find_inodedata(inode);
	if (ind) {
		zassert(pthread_mutex_lock(&(ind->lock)));
		ind->maxfleng = maxfleng;
		zassert(pthread_mutex_unlock(&(ind->lock)));
		write_free_inodedata(ind);
	}
}

uint64_t write_data_inode_getmaxfleng(uint32_t inode) {
	uint64_t maxfleng;
	inodedata* ind;
	ind = write_find_inodedata(inode);
	if (ind) {
		zassert(pthread_mutex_lock(&(ind->lock)));
		maxfleng = ind->maxfleng;
		zassert(pthread_mutex_unlock(&(ind->lock)));
		write_free_inodedata(ind);
	} else {
		maxfleng = 0;
	}
	return maxfleng;
}

uint64_t write_data_getmaxfleng(void *vid) {
	uint64_t maxfleng;
	inodedata* ind;
	if (vid==NULL) {
		return 0;
	}
	ind = (inodedata*)vid;
	zassert(pthread_mutex_lock(&(ind->lock)));
	maxfleng = ind->maxfleng;
	zassert(pthread_mutex_unlock(&(ind->lock)));
	return maxfleng;
}

int write_data_flush_inode(uint32_t inode) {
	inodedata* ind;
	int ret;
	ind = write_find_inodedata(inode);
	if (ind==NULL) {
		return 0;
	}
	ret = write_data_do_flush(ind,1);
	return ret;
}

int write_data_will_end_wait(void *vid) {
	if (vid!=NULL) {
		return write_data_will_flush_wait((inodedata*)vid);
	} else {
		return 0;
	}
}

int write_data_end(void *vid) {
	int ret;
	if (vid==NULL) {
		return EIO;
	}
	ret = write_data_do_flush((inodedata*)vid,1);
	return ret;
}
