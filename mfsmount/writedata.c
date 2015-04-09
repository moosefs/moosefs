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
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif
#include <sys/time.h>
#include <unistd.h>
#include <poll.h>
#include <stdio.h>
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
#include "csdb.h"
#include "mastercomm.h"
#include "clocks.h"
#include "portable.h"
#include "readdata.h"
#include "MFSCommunication.h"

//#define WORKER_DEBUG 1
//#define BUFFER_DEBUG 1
//#define WDEBUG 1

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

#define SUSTAIN_WORKERS 50
#define HEAVYLOAD_WORKERS 150
#define MAX_WORKERS 250

#define WCHASHSIZE 256
#define WCHASH(inode,indx) (((inode)*0xB239FB71+(indx)*193)%WCHASHSIZE)

#define IDHASHSIZE 256
#define IDHASH(inode) (((inode)*0xB239FB71)%IDHASHSIZE)

typedef struct cblock_s {
	uint8_t data[MFSBLOCKSIZE];	// modified only when writeid==0
	uint32_t chindx;	// chunk number
	uint16_t pos;		// block in chunk (0...1023) - never modified
	uint32_t writeid;	// 0 = not sent, >0 = block was sent (modified and accessed only when wchunk is locked)
	uint32_t from;		// first filled byte in data (modified only when writeid==0)
	uint32_t to;		// first not used byte in data (modified only when writeid==0)
	struct cblock_s *next,*prev;
} cblock;

typedef struct inodedata_s {
	uint32_t inode;
	uint64_t maxfleng;
	uint32_t cacheblockcount;
	int status;
	uint16_t flushwaiting;
	uint16_t writewaiting;
	uint16_t lcnt;
	uint32_t trycnt;
	uint8_t waitingworker;
	uint8_t inqueue;
	int pipe[2];
	cblock *datachainhead,*datachaintail;
	pthread_cond_t flushcond;	// wait for inqueue==0 (flush)
	pthread_cond_t writecond;	// wait for flushwaiting==0 (write)
	struct inodedata_s *next;
} inodedata;

typedef struct worker_s {
	pthread_t thread_id;
} worker;

// static pthread_mutex_t fcblock;

static pthread_cond_t fcbcond;
static uint8_t fcbwaiting;
static cblock *cacheblocks,*freecblockshead;
static uint32_t freecacheblocks;

static uint32_t maxretries;

static inodedata **idhash;

static pthread_mutex_t glock;

#ifdef BUFFER_DEBUG
static pthread_t info_worker_th;
static uint32_t usedblocks;
#endif

static pthread_t dqueue_worker_th;

static uint32_t workers_avail;
static uint32_t workers_total;
static uint32_t worker_term_waiting;
static pthread_cond_t worker_term_cond;
static pthread_attr_t worker_thattr;

static void *jqueue,*dqueue;

#ifdef BUFFER_DEBUG
void* write_info_worker(void *arg) {
	(void)arg;
	uint32_t cbcnt,fcbcnt,ucbcnt;
	uint32_t i;
	inodedata *id;
	cblock *cb;
	for (;;) {
		zassert(pthread_mutex_lock(&glock));
		cbcnt = 0;
		for (i = 0 ; i<IDHASHSIZE ; i++) {
			for (id = idhash[i] ; id ; id = id->next) {
				ucbcnt = 0;
				for (cb = id->datachainhead ; cb ; cb = cb->next) {
					ucbcnt++;
				}
				if (ucbcnt != id->cacheblockcount) {
					syslog(LOG_NOTICE,"inode: %"PRIu32" ; wrong cache block count (%"PRIu32"/%"PRIu32")",id->inode,ucbcnt,id->cacheblockcount);
				}
				cbcnt += ucbcnt;
			}
		}
		fcbcnt = 0;
		for (cb = freecblockshead ; cb ; cb = cb->next) {
			fcbcnt++;
		}
		syslog(LOG_NOTICE,"used cache blocks: %"PRIu32" ; sum of inode used blocks: %"PRIu32" ; free cache blocks: %"PRIu32" ; free cache chain blocks: %"PRIu32,usedblocks,cbcnt,freecacheblocks,fcbcnt);
		zassert(pthread_mutex_unlock(&glock));
		portable_usleep(500000);
	}

}
#endif

/* glock: LOCKED */
void write_cb_release (inodedata *id,cblock *cb) {
//	zassert(pthread_mutex_lock(&fcblock));
	cb->next = freecblockshead;
	freecblockshead = cb;
	freecacheblocks++;
	id->cacheblockcount--;
	if (fcbwaiting) {
		zassert(pthread_cond_signal(&fcbcond));
	}
#ifdef BUFFER_DEBUG
	usedblocks--;
#endif
//	zassert(pthread_mutex_unlock(&fcblock));
}

/* glock: LOCKED */
cblock* write_cb_acquire(inodedata *id) {
	cblock *ret;
//	zassert(pthread_mutex_lock(&fcblock));
	fcbwaiting++;
	while (freecblockshead==NULL || id->cacheblockcount>(freecacheblocks/3)) {
		zassert(pthread_cond_wait(&fcbcond,&glock));
	}
	fcbwaiting--;
	ret = freecblockshead;
	freecblockshead = ret->next;
	ret->chindx = 0;
	ret->pos = 0;
	ret->writeid = 0;
	ret->from = 0;
	ret->to = 0;
	ret->next = NULL;
	ret->prev = NULL;
	freecacheblocks--;
	id->cacheblockcount++;
#ifdef BUFFER_DEBUG
	usedblocks++;
#endif
//	zassert(pthread_mutex_unlock(&fcblock));
	return ret;
}


/* inode */

/* glock: LOCKED */
inodedata* write_find_inodedata(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	for (id=idhash[idh] ; id ; id=id->next) {
		if (id->inode == inode) {
			return id;
		}
	}
	return NULL;
}

/* glock: LOCKED */
inodedata* write_get_inodedata(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	int pfd[2];

	for (id=idhash[idh] ; id ; id=id->next) {
		if (id->inode == inode) {
			return id;
		}
	}

	if (pipe(pfd)<0) {
		syslog(LOG_WARNING,"pipe error: %s",strerr(errno));
		return NULL;
	}
	id = malloc(sizeof(inodedata));
	id->inode = inode;
	id->cacheblockcount = 0;
	id->maxfleng = 0;
	id->status = 0;
	id->trycnt = 0;
	id->pipe[0] = pfd[0];
	id->pipe[1] = pfd[1];
	id->datachainhead = NULL;
	id->datachaintail = NULL;
	id->waitingworker = 0;
	id->inqueue = 0;
	id->flushwaiting = 0;
	id->writewaiting = 0;
	id->lcnt = 0;
	zassert(pthread_cond_init(&(id->flushcond),NULL));
	zassert(pthread_cond_init(&(id->writecond),NULL));
	id->next = idhash[idh];
	idhash[idh] = id;
	return id;
}

/* glock: LOCKED */
void write_free_inodedata(inodedata *fid) {
	uint32_t idh = IDHASH(fid->inode);
	inodedata *id,**idp;
	idp = &(idhash[idh]);
	while ((id=*idp)) {
		if (id==fid) {
			*idp = id->next;
			zassert(pthread_cond_destroy(&(id->flushcond)));
			zassert(pthread_cond_destroy(&(id->writecond)));
			close(id->pipe[0]);
			close(id->pipe[1]);
			free(id);
			return;
		}
		idp = &(id->next);
	}
}


/* queues */

/* glock: UNUSED */
void write_delayed_enqueue(inodedata *id,uint32_t cnt) {
	uint64_t t;
	if (cnt>0) {
		t = monotonic_useconds();
		queue_put(dqueue,t>>32,t&0xFFFFFFFFU,(uint8_t*)id,cnt);
	} else {
		queue_put(jqueue,0,0,(uint8_t*)id,0);
	}
}

/* glock: UNUSED */
void write_enqueue(inodedata *id) {
	queue_put(jqueue,0,0,(uint8_t*)id,0);
}

/* worker thread | glock: UNUSED */
void* write_dqueue_worker(void *arg) {
	uint64_t t,usec;
	uint32_t husec,lusec,cnt;
	uint8_t *id;
	(void)arg;
	for (;;) {
		queue_get(dqueue,&husec,&lusec,&id,&cnt);
		if (id==NULL) {
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
			queue_put(dqueue,t>>32,t&0xFFFFFFFFU,(uint8_t*)id,cnt);
		} else {
			queue_put(jqueue,0,0,id,0);
		}
	}
	return NULL;
}

/* glock: UNLOCKED */
void write_job_end(inodedata *id,int status,uint32_t delay) {
	cblock *cb,*fcb;

	zassert(pthread_mutex_lock(&glock));
	if (status) {
		errno = status;
		syslog(LOG_WARNING,"error writing file number %"PRIu32": %s",id->inode,strerr(errno));
		id->status = status;
	}
	if (status==0 && delay==0) {
		id->trycnt=0;	// on good write reset try counter
	}
	status = id->status;

	if (id->datachainhead && status==0) {	// still have some work to do
		// reset write id
		for (cb=id->datachainhead ; cb ; cb=cb->next) {
			cb->writeid = 0;
		}
		write_delayed_enqueue(id,delay);
	} else {	// no more work or error occured
		// if this is an error then release all data blocks
		cb = id->datachainhead;
		while (cb) {
			fcb = cb;
			cb = cb->next;
			write_cb_release(id,fcb);
		}
		id->datachainhead=NULL;
		id->inqueue=0;

		if (id->flushwaiting>0) {
			zassert(pthread_cond_broadcast(&(id->flushcond)));
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}

void* write_worker(void *arg);

static uint32_t lastnotify = 0;

/* glock:LOCKED */
static inline void write_data_spawn_worker(void) {
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
	zassert(pthread_sigmask(SIG_BLOCK, &newset, &oldset));
	res = pthread_create(&(w->thread_id),&worker_thattr,write_worker,w);
	zassert(pthread_sigmask(SIG_SETMASK, &oldset, NULL));
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

/* glock:LOCKED */
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

/* main working thread | glock:UNLOCKED */
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

	const uint8_t *cp,*cpe;
	uint8_t *cpw;
	uint32_t chainip[100];
	uint16_t chainport[100];
//	uint32_t chainver[100];
	uint32_t chainminver;
	uint16_t chainelements;
	uint32_t tmpip;
	uint16_t tmpport;
	uint32_t tmpver;
	uint8_t cschain[6*99];
	uint32_t cschainsize;

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
	int status;
	char csstrip[16];
	uint8_t waitforstatus;
	uint8_t flushwaiting;
	uint8_t endofchunk;
	double start,now,lastrcvd,lastblock,lastsent;
	double workingtime,lrdiff,lbdiff;
	uint8_t cnt;
	uint8_t firsttime = 1;
	worker *w = (worker*)arg;

	inodedata *id;
	cblock *cb,*ncb,*rcb;
//	inodedata *id;

	chainelements = 0;
	chindx = 0;

	for (;;) {
		for (cnt=0 ; cnt<chainelements ; cnt++) {
			csdb_writedec(chainip[cnt],chainport[cnt]);
		}
		chainelements=0;

		if (firsttime==0) {
			zassert(pthread_mutex_lock(&glock));
			workers_avail++;
			if (workers_avail > SUSTAIN_WORKERS) {
//				fprintf(stderr,"close worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
				write_data_close_worker(w);
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
			write_data_close_worker(w);
			zassert(pthread_mutex_unlock(&glock));
			return NULL;
		}

		workers_avail--;
		if (workers_avail==0 && workers_total<MAX_WORKERS) {
			write_data_spawn_worker();
//			fprintf(stderr,"spawn worker (avail:%"PRIu32" ; total:%"PRIu32")\n",workers_avail,workers_total);
		}

		id = (inodedata*)data;

		if (id->status==0) {
			if (id->datachainhead) {
				chindx = id->datachainhead->chindx;
				status = id->status;
			} else {
				syslog(LOG_WARNING,"writeworker got inode with no data to write !!!");
				status = EINVAL;	// this should never happen, so status is not important - just anything
			}
		} else {
			status = id->status;
		}

		zassert(pthread_mutex_unlock(&glock));

		if (status) {
			write_job_end(id,status,0);
			continue;
		}

		// syslog(LOG_NOTICE,"file: %"PRIu32", index: %"PRIu16" - debug1",id->inode,chindx);
		// get chunk data from master
//		start = monotonic_seconds();
		wrstatus = fs_writechunk(id->inode,chindx,&csdataver,&mfleng,&chunkid,&version,&csdata,&csdatasize);
		if (wrstatus!=STATUS_OK) {
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_writechunk returned status: %s",id->inode,chindx,mfsstrerr(wrstatus));
			if (wrstatus!=ERROR_LOCKED) {
				if (wrstatus==ERROR_ENOENT) {
					write_job_end(id,EBADF,0);
				} else if (wrstatus==ERROR_QUOTA) {
					write_job_end(id,EDQUOT,0);
				} else if (wrstatus==ERROR_NOSPACE) {
					write_job_end(id,ENOSPC,0);
				} else if (wrstatus==ERROR_CHUNKLOST) {
					write_job_end(id,ENXIO,0);
				} else {
					id->trycnt++;
					if (id->trycnt>=maxretries) {
						if (wrstatus==ERROR_NOCHUNKSERVERS) {
							write_job_end(id,ENOSPC,0);
						} else if (wrstatus==ERROR_CSNOTPRESENT) {
							write_job_end(id,ENXIO,0);
						} else {
							write_job_end(id,EIO,0);
						}
					} else {
						write_delayed_enqueue(id,1+((id->trycnt<30)?(id->trycnt/3):10));
					}
				}
			} else {
				write_delayed_enqueue(id,1+((id->trycnt<30)?(id->trycnt/3):10));
			}
			continue;	// get next job
		}
//		now = monotonic_seconds();
//		fprintf(stderr,"fs_writechunk time: %.3lf\n",(now-start));

		if (csdata==NULL || csdatasize==0) {
			fs_writeend(chunkid,id->inode,0);
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies",id->inode,chindx,chunkid,version);
			id->trycnt+=6;
			if (id->trycnt>=maxretries) {
				write_job_end(id,ENXIO,0);
			} else {
				write_delayed_enqueue(id,60);
			}
			continue;
		}
		ip = 0; // make old compilers happy
		port = 0; // make old compilers happy
		csstrip[0] = 0;
		cp = csdata;
		cpe = csdata+csdatasize;
		chainminver = 0xFFFFFFFF;
		cpw = cschain;
		cschainsize = 0;
		while (cp<cpe && chainelements<100) {
			tmpip = get32bit(&cp);
			tmpport = get16bit(&cp);
			if (csdataver==0) {
				tmpver = 0;
			} else {
				tmpver = get32bit(&cp);
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
			fs_writeend(chunkid,id->inode,0);
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are too many copies",id->inode,chindx,chunkid,version);
			id->trycnt+=6;
			if (id->trycnt>=maxretries) {
				write_job_end(id,ENXIO,0);
			} else {
				write_delayed_enqueue(id,60);
			}
			continue;
		}

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
			cnt=0;
			while (cnt<10) {
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
					if (cnt>=10) {
						write_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"writeworker: can't connect to (%s:%"PRIu16"): %s",csstrip,port,strerr(errno));
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
					cnt=10;
				}
			}
		}
		if (fd<0) {
			fs_writeend(chunkid,id->inode,0);
			id->trycnt++;
			if (id->trycnt>=maxretries) {
				write_job_end(id,EIO,0);
			} else {
				write_delayed_enqueue(id,1+((id->trycnt<30)?(id->trycnt/3):10));
			}
			continue;
		}
		if (tcpnodelay(fd)<0) {
			syslog(LOG_WARNING,"writeworker: can't set TCP_NODELAY: %s",strerr(errno));
		}

#ifdef WORKER_DEBUG
		partialblocks=0;
		bytessent=0;
#endif
		nextwriteid=1;

		pfd[0].fd = fd;
		pfd[1].fd = id->pipe[0];
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
		endofchunk = 0;

		status = 0;
		wrstatus = STATUS_OK;

		lastrcvd = 0.0;
		lastsent = 0.0;
		lastblock = 0.0;

		flushwaiting = 0;
//		firstloop = 1;

		do {
			now = monotonic_seconds();
			zassert(pthread_mutex_lock(&glock));

			if (lastrcvd==0.0) {
				lastrcvd = now;
			} else {
				lrdiff = now - lastrcvd;
				if (lrdiff>=CHUNKSERVER_ACTIVITY_TIMEOUT) {
					write_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") was timed out (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,waitforstatus,id->trycnt+1);
					zassert(pthread_mutex_unlock(&glock));
					break;
				}
			}
			if (lastblock==0.0) {
				lbdiff = NEXT_BLOCK_DELAY; // first block should be send immediately
			} else {
				lbdiff = now - lastblock;
			}
			workingtime = now - start;

//			if (!((waitforstatus>0 && workingtime<WORKER_BUSY_LAST_SEND_TIMEOUT+WORKER_BUSY_WAIT_FOR_STATUS+((workers_total>HEAVYLOAD_WORKERS)?0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT)) || (waitforstatus==0 && lbdiff<WORKER_IDLE_TIMEOUT && flushwaiting==0 && (workers_total<=HEAVYLOAD_WORKERS) && endofchunk==0))) {
//					zassert(pthread_mutex_unlock(&glock));
//					break;
//				}
//			}

			id->waitingworker=1;

			if (sending_mode==0 && workingtime<WORKER_BUSY_LAST_SEND_TIMEOUT+((workers_total>HEAVYLOAD_WORKERS)?0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT) && waitforstatus<64) {
				if (cb==NULL) {
					ncb = id->datachainhead;
				} else {
					ncb = cb->next;
				}
				if (ncb) {
					if (ncb->chindx==chindx) {
						if (ncb->to-ncb->from==MFSBLOCKSIZE || lbdiff>=NEXT_BLOCK_DELAY || ncb->next!=NULL || id->flushwaiting) {
							cb = ncb;
							sending_mode = 2;
						} else {
							id->waitingworker=2; // wait for block expand
						}
					} else {
						endofchunk=1;
					}
				}
/*
				if (cb==NULL) {
					if (id->datachainhead) {
						if (id->datachainhead->chindx==chindx) {
							if (id->datachainhead->to-id->datachainhead->from==MFSBLOCKSIZE || id->datachainhead->next!=NULL || id->flushwaiting) {
								cb = id->datachainhead;
								havedata=1;
							}
						} else {
							endofchunk=1;
						}
					} else {
						id->waitingworker=1;
					}
				} else {
					if (cb->next) {
						if (cb->next->chindx==chindx) {
							if (cb->next->to-cb->next->from==MFSBLOCKSIZE || lbdiff>NEXT_BLOCK_DELAY || cb->next->next!=NULL || id->flushwaiting) {
								cb = cb->next;
								havedata=1;
							} else {
								id->waitingworker=2;
							}
						} else {
							endofchunk=1;
						}
					} else {
						id->waitingworker=1;
					}
				}
*/
				if (sending_mode==2) {
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
				} else if (lastsent+WORKER_NOP_INTERVAL<now && chainminver>=VERSION2INT(1,7,32)) {
					wptr = sendbuff;
					put32bit(&wptr,ANTOAN_NOP);
					put32bit(&wptr,0);
					sent = 0;
					sending_mode = 3;
				}
			}

#ifdef WORKER_DEBUG
			fprintf(stderr,"workerloop: waitforstatus:%u workingtime:%.6lf workers_total:%u lbdiff:%.6lf flushwaiting:%u endofchunk:%u\n",waitforstatus,workingtime,workers_total,lbdiff,flushwaiting,endofchunk);
#endif
			if (waitforstatus>0) {
				if (workingtime>WORKER_BUSY_LAST_SEND_TIMEOUT+WORKER_BUSY_WAIT_FOR_STATUS+((workers_total>HEAVYLOAD_WORKERS)?0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT)) { // timeout
					id->waitingworker=0;
					zassert(pthread_mutex_unlock(&glock));
					break;
				}
			} else {
				if (lbdiff>=WORKER_IDLE_TIMEOUT || flushwaiting || workers_total>HEAVYLOAD_WORKERS || endofchunk) {
					id->waitingworker=0;
					zassert(pthread_mutex_unlock(&glock));
					break;
				}
			}


			zassert(pthread_mutex_unlock(&glock));

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
							i = write(fd,sendbuff+sent,hdrtosend-sent);
#ifdef HAVE_WRITEV
						}
#endif
					} else {
						i = write(fd,cschain+(sent-hdrtosend),cschainsize-(sent-hdrtosend));
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
						i = write(fd,sendbuff+sent,32-sent);
#endif
					} else {
						i = write(fd,cb->data+cb->from+(sent-32),cb->to-cb->from-(sent-32));
					}
					if (i>=0) {
						sent+=i;
						if (sent==32+cb->to-cb->from) {
							sending_mode = 0;
						}
					}
					break;
				case 3:
					i = write(fd,sendbuff+sent,8-sent);
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
					write_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: write to (%s:%"PRIu16") error: %s / NEGWRITE (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,strerr(errno),waitforstatus,id->trycnt+1);
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
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: poll error: %s (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,strerr(errno),waitforstatus,id->trycnt+1);
					status=EIO;
					break;
				}
			}
			zassert(pthread_mutex_lock(&glock));	// make helgrind happy
			id->waitingworker=0;
			flushwaiting = (id->flushwaiting>0)?1:0;
			zassert(pthread_mutex_unlock(&glock));	// make helgrind happy
			if (pfd[1].revents&POLLIN) {	// used just to break poll - so just read all data from pipe to empty it
				i = read(id->pipe[0],pipebuff,1024);
				if (i<0) { // mainly to make happy static code analyzers
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: read pipe error: %s (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,strerr(errno),waitforstatus,id->trycnt+1);
				}
			}
			if (pfd[0].revents&POLLHUP) {
				write_prepare_ip(csstrip,ip);
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") was reset by peer / POLLHUP (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,waitforstatus,id->trycnt+1);
				status=EIO;
				break;
			}
			if (pfd[0].revents&POLLERR) {
				write_prepare_ip(csstrip,ip);
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") got error status / POLLERR (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,waitforstatus,id->trycnt+1);
				status=EIO;
				break;
			}
			if (pfd[0].revents&POLLIN) {
				i = read(fd,recvbuff+rcvd,21-rcvd);
				if (i==0) { 	// connection reset by peer or read error
					write_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: connection with (%s:%"PRIu16") was reset by peer / ZEROREAD (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,waitforstatus,id->trycnt+1);
					status=EIO;
					break;
				}
				if (i<0) {
					if (errno!=EINTR) {
						write_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - writeworker: read from (%s:%"PRIu16") error: %s (unfinished writes: %"PRIu8"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,strerr(errno),waitforstatus,id->trycnt+1);
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
					if (reccmd!=CSTOCL_WRITE_STATUS ||  recleng!=13) {
						syslog(LOG_WARNING,"writeworker: got unrecognized packet from chunkserver (cmd:%"PRIu32",leng:%"PRIu32")",reccmd,recleng);
						status=EIO;
						break;
					}
					if (recchunkid!=chunkid) {
						syslog(LOG_WARNING,"writeworker: got unexpected packet (expected chunkdid:%"PRIu64",packet chunkid:%"PRIu64")",chunkid,recchunkid);
						status=EIO;
						break;
					}
					if (recstatus!=STATUS_OK) {
						syslog(LOG_WARNING,"writeworker: write error: %s",mfsstrerr(recstatus));
						wrstatus=recstatus;
						break;
					}
// debug:				syslog(LOG_NOTICE,"writeworker: received status ok for writeid:%"PRIu32,recwriteid);
					if (recwriteid>0) {
						zassert(pthread_mutex_lock(&glock));
						for (rcb = id->datachainhead ; rcb && rcb->writeid!=recwriteid ; rcb=rcb->next) {}
						if (rcb==NULL) {
							syslog(LOG_WARNING,"writeworker: got unexpected status (writeid:%"PRIu32")",recwriteid);
							zassert(pthread_mutex_unlock(&glock));
							status=EIO;
							break;
						}
						if (rcb==cb) {	// current block
// debug:						syslog(LOG_NOTICE,"writeworker: received status for current block");
							if (sending_mode==2) {	// got status ok before all data had been sent - error
								syslog(LOG_WARNING,"writeworker: got status OK before all data have been sent");
								zassert(pthread_mutex_unlock(&glock));
								status=EIO;
								break;
							} else {
								cb = NULL;
							}
						}
						if (rcb->prev) {
							rcb->prev->next = rcb->next;
						} else {
							id->datachainhead = rcb->next;
						}
						if (rcb->next) {
							rcb->next->prev = rcb->prev;
						} else {
							id->datachaintail = rcb->prev;
						}
						maxwroffset = (((uint64_t)(chindx))<<MFSCHUNKBITS)+(((uint32_t)(rcb->pos))<<MFSBLOCKBITS)+rcb->to;
						if (maxwroffset>mfleng) {
							mfleng=maxwroffset;
						}
						write_cb_release(id,rcb);
						zassert(pthread_mutex_unlock(&glock));
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
			if (write(fd,sendbuff,20)==20) {
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
			cl+=snprintf(debugchain+cl,200-cl,"%u.%u.%u.%u:%u->",(chainip[cnt]>>24)&255,(chainip[cnt]>>16)&255,(chainip[cnt]>>8)&255,chainip[cnt]&255,chainport[cnt]);
		}
		if (cl>=2) {
			debugchain[cl-2]='\0';
		}
		syslog(LOG_NOTICE,"worker %lu sent %"PRIu32" blocks (%"PRIu32" partial) of chunk %016"PRIX64"_%08"PRIX32", received status for %"PRIu32" blocks (%"PRIu32" lost), bw: %.6lfMB/s ( %"PRIu32" B / %.6lf s ), chain: %s",(unsigned long)arg,nextwriteid-1,partialblocks,chunkid,version,nextwriteid-1-waitforstatus,waitforstatus,(double)bytessent/workingtime,bytessent,workingtime,debugchain);
#endif

		for (cnt=0 ; cnt<10 ; cnt++) {
			westatus = fs_writeend(chunkid,id->inode,mfleng);
			if (westatus==ERROR_ENOENT || westatus==ERROR_QUOTA) {
				break;
			} else if (westatus!=STATUS_OK) {
				portable_usleep(100000+(10000<<cnt));
			} else {
				break;
			}
		}

		if (westatus==ERROR_ENOENT) {
			write_job_end(id,EBADF,0);
		} else if (westatus==ERROR_QUOTA) {
			write_job_end(id,EDQUOT,0);
		} else if (westatus!=STATUS_OK) {
			write_job_end(id,ENXIO,0);
		} else if (status!=0 || wrstatus!=STATUS_OK) {
			if (wrstatus!=STATUS_OK) {	// convert MFS status to OS errno
				if (wrstatus==ERROR_NOSPACE) {
					status=ENOSPC;
				} else {
					status=EIO;
				}
			}
			id->trycnt++;
			if (id->trycnt>=maxretries) {
				write_job_end(id,status,0);
			} else {
				write_job_end(id,0,1+((id->trycnt<30)?(id->trycnt/3):10));
			}
		} else {
//			read_inode_ops(id->inode);
			read_inode_set_length(id->inode,mfleng,0);
			write_job_end(id,0,0);
		}
	}
}

/* API | glock: INITIALIZED,UNLOCKED */
void write_data_init (uint32_t cachesize,uint32_t retries) {
	uint32_t cacheblockcount = (cachesize/MFSBLOCKSIZE);
	uint32_t i;
	sigset_t oldset;
	sigset_t newset;

	maxretries = retries;
	if (cacheblockcount<10) {
		cacheblockcount=10;
	}
	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_cond_init(&worker_term_cond,NULL));
	worker_term_waiting = 0;

	zassert(pthread_cond_init(&fcbcond,NULL));
	fcbwaiting=0;
	cacheblocks = malloc(sizeof(cblock)*cacheblockcount);
	for (i=0 ; i<cacheblockcount-1 ; i++) {
		cacheblocks[i].next = cacheblocks+(i+1);
	}
	cacheblocks[cacheblockcount-1].next = NULL;
	freecblockshead = cacheblocks;
	freecacheblocks = cacheblockcount;

	idhash = malloc(sizeof(inodedata*)*IDHASHSIZE);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		idhash[i]=NULL;
	}

	dqueue = queue_new(0);
	jqueue = queue_new(0);

        zassert(pthread_attr_init(&worker_thattr));
        zassert(pthread_attr_setstacksize(&worker_thattr,0x100000));
	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	zassert(pthread_sigmask(SIG_BLOCK, &newset, &oldset));
        zassert(pthread_create(&dqueue_worker_th,&worker_thattr,write_dqueue_worker,NULL));
	zassert(pthread_sigmask(SIG_SETMASK, &oldset, NULL));

	zassert(pthread_mutex_lock(&glock));
	workers_avail = 0;
	workers_total = 0;
	write_data_spawn_worker();
	zassert(pthread_mutex_unlock(&glock));
#ifdef BUFFER_DEBUG
	zassert(pthread_create(&info_worker_th,&worker_thattr,write_info_worker,NULL));
#endif
}

void write_data_term(void) {
	uint32_t i;
	inodedata *id,*idn;

	queue_close(dqueue);
	queue_close(jqueue);
	zassert(pthread_mutex_lock(&glock));
	while (workers_total>0) {
		worker_term_waiting++;
		zassert(pthread_cond_wait(&worker_term_cond,&glock));
	}
	zassert(pthread_mutex_unlock(&glock));
	zassert(pthread_join(dqueue_worker_th,NULL));
	queue_delete(dqueue);
	queue_delete(jqueue);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		for (id = idhash[i] ; id ; id = idn) {
			idn = id->next;
			zassert(pthread_cond_destroy(&(id->flushcond)));
			zassert(pthread_cond_destroy(&(id->writecond)));
			close(id->pipe[0]);
			close(id->pipe[1]);
			free(id);
		}
	}
	free(idhash);
	free(cacheblocks);
	zassert(pthread_attr_destroy(&worker_thattr));
	zassert(pthread_cond_destroy(&worker_term_cond));
	zassert(pthread_cond_destroy(&fcbcond));
	zassert(pthread_mutex_destroy(&glock));
}

/* glock: LOCKED */
int write_cb_expand(inodedata *id,cblock *cb,uint32_t from,uint32_t to,const uint8_t *data) {
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
	if (cb->to-cb->from==MFSBLOCKSIZE && cb->next==NULL && id->waitingworker==2) {
		if (write(id->pipe[1]," ",1)!=1) {
			syslog(LOG_ERR,"can't write to pipe !!!");
		}
		id->waitingworker=0;
	}
	return 0;
}

/* glock: UNLOCKED */
int write_block(inodedata *id,uint32_t chindx,uint16_t pos,uint32_t from,uint32_t to,const uint8_t *data) {
	cblock *cb;

	zassert(pthread_mutex_lock(&glock));
	for (cb=id->datachaintail ; cb ; cb=cb->prev) {
		if (cb->pos==pos && cb->chindx==chindx) {
			if (write_cb_expand(id,cb,from,to,data)==0) {
				zassert(pthread_mutex_unlock(&glock));
				return 0;
			} else {
				break;
			}
		}
	}

	cb = write_cb_acquire(id);
//	syslog(LOG_NOTICE,"write_block: acquired new cache block");
	cb->chindx = chindx;
	cb->pos = pos;
	cb->from = from;
	cb->to = to;
	memcpy(cb->data+from,data,to-from);
	cb->prev = id->datachaintail;
	cb->next = NULL;
	if (id->datachaintail!=NULL) {
		id->datachaintail->next = cb;
	} else {
		id->datachainhead = cb;
	}
	id->datachaintail = cb;
	if (id->inqueue) {
		if (id->waitingworker) {
			if (write(id->pipe[1]," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			id->waitingworker=0;
		}
	} else {
		id->inqueue=1;
		write_enqueue(id);
	}
	zassert(pthread_mutex_unlock(&glock));
//	zassert(pthread_mutex_unlock(&(wc->lock)));
	return 0;
}

/* API | glock: UNLOCKED */
int write_data(void *vid,uint64_t offset,uint32_t size,const uint8_t *data) {
	uint32_t chindx;
	uint16_t pos;
	uint32_t from;
	int status;
	inodedata *id = (inodedata*)vid;
	if (id==NULL) {
		return EIO;
	}
//	int64_t s,e;

//	s = monotonic_useconds();
	zassert(pthread_mutex_lock(&glock));
//	syslog(LOG_NOTICE,"write_data: inode:%"PRIu32" offset:%"PRIu64" size:%"PRIu32,id->inode,offset,size);
//	id = write_get_inodedata(inode);
	status = id->status;
	if (status==0) {
		if (offset+size>id->maxfleng) {	// move fleng
			id->maxfleng = offset+size;
		}
		id->writewaiting++;
		while (id->flushwaiting>0) {
			zassert(pthread_cond_wait(&(id->writecond),&glock));
		}
		id->writewaiting--;
	}
	zassert(pthread_mutex_unlock(&glock));
	if (status!=0) {
		return status;
	}

	chindx = offset>>MFSCHUNKBITS;
	pos = (offset&MFSCHUNKMASK)>>MFSBLOCKBITS;
	from = offset&MFSBLOCKMASK;
	while (size>0) {
		if (size>MFSBLOCKSIZE-from) {
			if (write_block(id,chindx,pos,from,MFSBLOCKSIZE,data)<0) {
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
			if (write_block(id,chindx,pos,from,from+size,data)<0) {
				return EIO;
			}
			size = 0;
		}
	}
//	e = monotonic_useconds();
//	syslog(LOG_NOTICE,"write_data time: %"PRId64,e-s);
	return 0;
}

/* API | glock: UNLOCKED */
void* write_data_new(uint32_t inode) {
	inodedata* id;
	zassert(pthread_mutex_lock(&glock));
	id = write_get_inodedata(inode);
	if (id==NULL) {
		zassert(pthread_mutex_unlock(&glock));
		return NULL;
	}
	id->lcnt++;
//	zassert(pthread_mutex_unlock(&(id->lock)));
	zassert(pthread_mutex_unlock(&glock));
	return id;
}

/* common flush routine | glock: LOCKED */
static int write_data_do_flush(inodedata *id,uint8_t releaseflag) {
	int ret;
//	int64_t s,e;

//	s = monotonic_useconds();
	id->flushwaiting++;
	while (id->inqueue) {
		if (id->waitingworker) {
			if (write(id->pipe[1]," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			id->waitingworker=0;
		}
//		syslog(LOG_NOTICE,"flush: wait ...");
		zassert(pthread_cond_wait(&(id->flushcond),&glock));
//		syslog(LOG_NOTICE,"flush: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting==0 && id->writewaiting>0) {
		zassert(pthread_cond_broadcast(&(id->writecond)));
	}
	ret = id->status;
	if (releaseflag) {
		id->lcnt--;
	}
	if (id->lcnt==0 && id->inqueue==0 && id->flushwaiting==0 && id->writewaiting==0) {
		write_free_inodedata(id);
	}
//	e = monotonic_useconds();
//	syslog(LOG_NOTICE,"flush time: %"PRId64,e-s);
	return ret;
}

/* API | glock: UNLOCKED */
int write_data_flush(void *vid) {
	int ret;
	if (vid==NULL) {
		return EIO;
	}
	zassert(pthread_mutex_lock(&glock));
	ret = write_data_do_flush((inodedata*)vid,0);
	zassert(pthread_mutex_unlock(&glock));
	return ret;
}

/* API | glock: UNLOCKED */
uint64_t write_data_getmaxfleng(uint32_t inode) {
	uint64_t maxfleng;
	inodedata* id;
	zassert(pthread_mutex_lock(&glock));
	id = write_find_inodedata(inode);
	if (id) {
		maxfleng = id->maxfleng;
	} else {
		maxfleng = 0;
	}
	zassert(pthread_mutex_unlock(&glock));
	return maxfleng;
}

/* API | glock: UNLOCKED */
int write_data_flush_inode(uint32_t inode) {
	inodedata* id;
	int ret;
	zassert(pthread_mutex_lock(&glock));
	id = write_find_inodedata(inode);
	if (id==NULL) {
		zassert(pthread_mutex_unlock(&glock));
		return 0;
	}
	ret = write_data_do_flush(id,0);
	zassert(pthread_mutex_unlock(&glock));
	return ret;
}

/* API | glock: UNLOCKED */
int write_data_end(void *vid) {
	int ret;
	if (vid==NULL) {
		return EIO;
	}
	zassert(pthread_mutex_lock(&glock));
	ret = write_data_do_flush(vid,1);
	zassert(pthread_mutex_unlock(&glock));
	return ret;
}
