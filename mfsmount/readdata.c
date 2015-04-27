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
#include "csdb.h"
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

typedef struct rrequest_s {
	uint8_t *data;
	uint64_t offset;
	uint32_t leng;
	uint32_t rleng;
	uint32_t chindx;
	double modified;
	uint8_t filled;
	uint8_t refresh;
	uint8_t busy;
	uint8_t free;
	uint16_t lcnt;
	uint16_t waiting;
	pthread_cond_t cond;
	struct rrequest_s *next,**prev;
} rrequest;

typedef struct inodedata_s {
	uint32_t inode;
	uint32_t seqdata;
	uint64_t fleng;
//	uint32_t cacheblockcount;
	int status;
	uint16_t closewaiting;
	uint32_t trycnt;
	uint8_t flengisvalid;
	uint8_t waitingworker;
	uint8_t inqueue;
	int pipe[2];
	uint8_t readahead;
	uint64_t lastoffset;
	uint64_t lastchunkid;
	uint32_t lastip;
	uint16_t lastport;
	uint8_t laststatus;
	rrequest *reqhead,**reqtail;
//	cblock *datachainhead,*datachaintail;
	pthread_cond_t closecond;
	struct inodedata_s *next;
} inodedata;

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

static inodedata **idhash;

static pthread_mutex_t glock;

//#ifdef BUFFER_DEBUG
//static pthread_t info_worker_th;
//static uint32_t usedblocks;
//#endif

static pthread_t dqueue_worker_th;

static uint32_t workers_avail;
static uint32_t workers_total;
static uint32_t worker_term_waiting;
static pthread_cond_t worker_term_cond;
static pthread_attr_t worker_thattr;

// static pthread_t read_worker_th[WORKERS];
//static inodedata *read_worker_id[WORKERS];

static void *jqueue,*dqueue;

/* queues */

/* glock: UNUSED */
void read_delayed_enqueue(inodedata *id,uint32_t cnt) {
	uint64_t t;
	if (cnt>0) {
		t = monotonic_useconds();
		queue_put(dqueue,t>>32,t&0xFFFFFFFFU,(uint8_t*)id,cnt);
	} else {
		queue_put(jqueue,0,0,(uint8_t*)id,0);
	}
}

/* glock: UNUSED */
void read_enqueue(inodedata *id) {
	queue_put(jqueue,0,0,(uint8_t*)id,0);
}

/* worker thread | glock: UNUSED */
void* read_dqueue_worker(void *arg) {
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
void read_job_end(inodedata *id,int status,uint32_t delay) {
	rrequest *rreq,**rreqp;
	uint8_t todo;

	zassert(pthread_mutex_lock(&glock));
	if (status) {
		if (id->closewaiting==0) {
			errno = status;
			syslog(LOG_WARNING,"error reading file number %"PRIu32": %s",id->inode,strerr(errno));
		}
		id->status = status;
	}
	status = id->status;
	todo = 0;
	if (status==0) {
		for (rreq = id->reqhead ; rreq && todo==0 ; rreq=rreq->next) {
			if (rreq->filled==0 && rreq->free==0) {
				todo=1;
			}
		}
                if (delay==0) {
                        id->trycnt=0;   // on good read reset try counter
                }
	}

	if (id->closewaiting) {
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: inode: %"PRIu32" - closewaiting\n",monotonic_seconds(),id->inode);
#endif
		rreqp = &(id->reqhead);
		while ((rreq = *rreqp)) {
			if (rreq->lcnt==0 && rreq->busy==0) {
				reqbufftotalsize -= rreq->leng;
				free(rreq->data);
				*rreqp = rreq->next;
				free(rreq);
			} else {
				if (rreq->filled==0) {
					rreq->rleng = 0;
					rreq->filled = 1;
				}
				if (rreq->waiting) {
					zassert(pthread_cond_broadcast(&(rreq->cond)));
				}
				rreq->free = 1;
				rreqp = &(rreq->next);
			}
		}

                id->inqueue=0;

#ifdef RDEBUG
		fprintf(stderr,"%.6lf: inode: %"PRIu32" - reqhead: %s (reqbufftotalsize: %"PRIu64")\n",monotonic_seconds(),id->inode,id->reqhead?"NOT NULL":"NULL",reqbufftotalsize);
#endif
		if (id->reqhead==NULL) {
			zassert(pthread_cond_broadcast(&(id->closecond)));
		}
	} else if (todo && status==0) {   // still have some work to do
                read_delayed_enqueue(id,delay);
        } else {        // no more work, descriptor wait for being closed or error occured 
		for (rreq = id->reqhead ; rreq ; rreq=rreq->next) {
			if (rreq->filled==0) { // error occured
				rreq->rleng = 0;
				rreq->filled = 1;
				if (rreq->waiting) {
					zassert(pthread_cond_broadcast(&(rreq->cond)));
				}
			}
		}

                id->inqueue=0;
        }
        zassert(pthread_mutex_unlock(&glock));
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

/* main working thread | glock:UNLOCKED */
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

	uint32_t reccmd;
	uint32_t recleng;
	uint64_t recchunkid;
	uint16_t recblocknum;
	uint16_t recoffset;
	uint32_t recsize;
	uint32_t reccrc;
	uint8_t recstatus;
	uint8_t gotstatus;

	uint32_t chindx;
	uint32_t ip;
	uint16_t port;
	uint32_t srcip;
	uint64_t mfleng;
	uint64_t chunkid;
	uint32_t version;
	const uint8_t *csdata;
	uint32_t tmpip;
	uint16_t tmpport;
	uint32_t tmpver;
	uint32_t csver;
	uint32_t cnt,bestcnt;
	uint32_t csdatasize;
	uint8_t csdataver;
	uint8_t csrecsize;
	uint8_t rdstatus;
	int status;
	char csstrip[16];
	uint8_t reqsend;
	uint8_t closewaiting;
	double start,now,lastrcvd,lastsend;
	double workingtime,lrdiff;
	uint8_t firsttime = 1;
	worker *w = (worker*)arg;

	inodedata *id;
	rrequest *rreq,*nrreq;

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

		id = (inodedata*)data;

		for (rreq = id->reqhead ; rreq && rreq->filled==1 && rreq->busy==0 ; rreq=rreq->next) {}
		if (rreq) {
			chindx = rreq->chindx;
			status = id->status;
			if (status==STATUS_OK) {
				rreq->busy = 1;
			}
		} else {
			// no data to read - just ignore it
			zassert(pthread_mutex_unlock(&glock));
			read_job_end(id,0,0);
			continue;
		}

		zassert(pthread_mutex_unlock(&glock));

		if (status!=STATUS_OK) {
			read_job_end(id,status,0);
			continue;
		}

		// get chunk data from master
//		start = monotonic_seconds();
		rdstatus = fs_readchunk(id->inode,chindx,&csdataver,&mfleng,&chunkid,&version,&csdata,&csdatasize);

		if (rdstatus!=STATUS_OK) {
			zassert(pthread_mutex_lock(&glock));
			rreq->busy = 0;
			zassert(pthread_mutex_unlock(&glock));
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32" - fs_readchunk returned status: %s",id->inode,chindx,mfsstrerr(rdstatus));
			if (rdstatus==ERROR_ENOENT) {
				read_job_end(id,EBADF,0);
			} else if (rdstatus==ERROR_QUOTA) {
				read_job_end(id,EDQUOT,0);
			} else if (rdstatus==ERROR_NOSPACE) {
				read_job_end(id,ENOSPC,0);
			} else if (rdstatus==ERROR_CHUNKLOST) {
				read_job_end(id,ENXIO,0);
			} else {
				id->trycnt++;
				if (id->trycnt>=maxretries) {
					if (rdstatus==ERROR_NOCHUNKSERVERS) {
						read_job_end(id,ENOSPC,0);
					} else if (rdstatus==ERROR_CSNOTPRESENT) {
						read_job_end(id,ENXIO,0);
					} else {
						read_job_end(id,EIO,0);
					}
				} else {
					read_delayed_enqueue(id,1+((id->trycnt<30)?(id->trycnt/3):10));
				}
			}
			continue;	// get next job
		}
//		now = monotonic_seconds();
//		fprintf(stderr,"fs_readchunk time: %.3lf\n",now-start);
		if (chunkid==0 && version==0) { // empty chunk
			zassert(pthread_mutex_lock(&glock));
			id->fleng = mfleng;
			id->flengisvalid = 1;
			rreq->busy = 0;
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: inode: %"PRIu32" ; mfleng: %"PRIu64" (empty chunk)\n",monotonic_seconds(),id->inode,id->fleng);
#endif
			while (rreq) {
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
				rreq->filled=1;
				rreq->modified = monotonic_seconds();
				if (rreq->waiting>0) {
					zassert(pthread_cond_broadcast(&(rreq->cond)));
				}
				rreq = NULL;

				for (nrreq = id->reqhead ; nrreq && nrreq->filled==1 && nrreq->busy==0 ; nrreq=nrreq->next) {}
				if (nrreq && nrreq->chindx==chindx) {
					rreq = nrreq;
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker: get next request (empty chunk)\n",monotonic_seconds());
#endif
				}
			}

			zassert(pthread_mutex_unlock(&glock));
			read_job_end(id,0,0);

			continue;
		}

		if (csdata==NULL || csdatasize==0) {
			zassert(pthread_mutex_lock(&glock));
			rreq->busy = 0;
			zassert(pthread_mutex_unlock(&glock));
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies",id->inode,chindx,chunkid,version);
			id->trycnt+=6;
			if (id->trycnt>=maxretries) {
				read_job_end(id,ENXIO,0);
			} else {
				read_delayed_enqueue(id,60);
			}
			continue;
		}

		ip = 0; // make old compilers happy
		port = 0; // make old compilers happy
		csver = 0; // make old compilers happy
		if (csdataver==0) {
			csrecsize = 6;
		} else {
			csrecsize = 10;
		}
		// choose cs
		bestcnt = 0xFFFFFFFF;
		while (csdatasize>=csrecsize) {
			tmpip = get32bit(&csdata);
			tmpport = get16bit(&csdata);
			if (csdataver>0) {
				tmpver = get32bit(&csdata);
			} else {
				tmpver = 0;
			}
			csdatasize-=csrecsize;
			if (id->lastchunkid==chunkid && tmpip==id->lastip && tmpport==id->lastport) {
				if (id->laststatus==1) {
					ip = tmpip;
					port = tmpport;
					csver = tmpver;
					break;
				} else {
					cnt = 0xFFFFFFFE;
				}
			} else {
				cnt = csdb_getopcnt(tmpip,tmpport);
			}
			if (cnt<bestcnt) {
				ip = tmpip;
				port = tmpport;
				csver = tmpver;
				bestcnt = cnt;
			}
		}

		if (ip || port) {
			csdb_readinc(ip,port);
			id->lastchunkid = chunkid;
			id->lastip = ip;
			id->lastport = port;
			id->laststatus = 0;
		} else {
			zassert(pthread_mutex_lock(&glock));
			rreq->busy = 0;
			zassert(pthread_mutex_unlock(&glock));
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies (bad ip and/or port)",id->inode,chindx,chunkid,version);
			id->trycnt+=6;
			if (id->trycnt>=maxretries) {
				read_job_end(id,ENXIO,0);
			} else {
				read_delayed_enqueue(id,60);
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
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"readworker: can't connect to (%s:%"PRIu16"): %s",csstrip,port,strerr(errno));
					}
					close(fd);
					fd=-1;
				} else {
					cnt=10;
				}
			}
		}
		if (fd<0) {
			zassert(pthread_mutex_lock(&glock));
			rreq->busy = 0;
			zassert(pthread_mutex_unlock(&glock));
			id->trycnt++;
			if (id->trycnt>=maxretries) {
				read_job_end(id,EIO,0);
			} else {
				read_delayed_enqueue(id,1+((id->trycnt<30)?(id->trycnt/3):10));
			}
			continue;
		}
		if (tcpnodelay(fd)<0) {
			syslog(LOG_WARNING,"readworker: can't set TCP_NODELAY: %s",strerr(errno));
		}

		pfd[0].fd = fd;
		pfd[1].fd = id->pipe[0];
		currentpos = 0;
		gotstatus = 0;
		received = 0;
		reqsend = 0;
		sent = 0;
		tosend = 0;
		lastrcvd = 0.0;
		lastsend = 0.0;

		zassert(pthread_mutex_lock(&glock));
		id->fleng = mfleng;
		id->flengisvalid = 1;
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: inode: %"PRIu32" ; mfleng: %"PRIu64"\n",monotonic_seconds(),id->inode,id->fleng);
#endif
		zassert(pthread_mutex_unlock(&glock));

		reccmd = 0; // makes gcc happy
		recleng = 0; // makes gcc happy

		do {
			now = monotonic_seconds();
#ifdef RDEBUG
			if (rreq) {
				fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: %"PRIu64":%"PRIu32"\n",monotonic_seconds(),id->inode,rreq->offset,rreq->leng);
			} else {
				fprintf(stderr,"%.6lf: readworker inode: %"PRIu32" ; rreq: NULL\n",monotonic_seconds(),id->inode);
			}
#endif

			zassert(pthread_mutex_lock(&glock));

			if (id->flengisvalid) {
				mfleng = id->fleng;
			}

			if (rreq!=NULL && ((reqsend && gotstatus) || rreq->refresh==1)) { // rreq has been read or needs to be reread
				rreq->busy = 0;
				if (rreq->refresh==1) {
					rreq->refresh = 0;
					rreq->filled = 0;
					zassert(pthread_mutex_unlock(&glock));
					status = EINTR;
					break;
				} else {
					rreq->filled = 1;
					rreq->modified = monotonic_seconds();
				}
				if (rreq->waiting>0) {
					zassert(pthread_cond_broadcast(&(rreq->cond)));
				}
				rreq = NULL;
			}

			if (lastrcvd==0.0) {
				lastrcvd = now;
			} else {
				lrdiff = now - lastrcvd;
				if (lrdiff>=CHUNKSERVER_ACTIVITY_TIMEOUT) {
					read_prepare_ip(csstrip,ip);
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was timed out (lastrcvd:%.6lf,now:%.6lf,lrdiff:%.6lf received: %"PRIu32"/%"PRIu32", try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,lastrcvd,now,lrdiff,currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
					if (rreq) {
						status = EIO;
					}
					zassert(pthread_mutex_unlock(&glock));
					break;
				}
			}

			workingtime = now - start;

			if (rreq==NULL) { // finished current block
				for (nrreq = id->reqhead ; nrreq && nrreq->filled==1 && nrreq->busy==0 ; nrreq=nrreq->next) {}
				if (nrreq) { // have next block
					if (nrreq->chindx!=chindx || nrreq->filled || nrreq->busy || workingtime>WORKER_BUSY_LAST_REQUEST_TIMEOUT+((workers_total>HEAVYLOAD_WORKERS)?0.0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT)) {
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker: ignore next request\n",monotonic_seconds());
#endif
						zassert(pthread_mutex_unlock(&glock));
						break;
					}
					if (nrreq->lcnt==0 && workers_total>HEAVYLOAD_WORKERS) { // currently nobody wants this block and there are a lot of busy workers, so skip this one
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker: lcnt is zero and there are a lot of workers\n",monotonic_seconds());
#endif
						zassert(pthread_mutex_unlock(&glock));
						break;
					}
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker: get next request\n",monotonic_seconds());
#endif
					rreq = nrreq;
					rreq->busy = 1;
					currentpos = 0;
					received = 0;
					reqsend = 0;
					gotstatus = 0;
				} else { // do not have next block
					if (workingtime>WORKER_IDLE_TIMEOUT || workers_total>HEAVYLOAD_WORKERS) {
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: readworker: next request doesn't exist and there are a lot of workers or idle timeout passed\n",monotonic_seconds());
#endif
						zassert(pthread_mutex_unlock(&glock));
						break;
					}
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker: next request doesn't exist, so wait on pipe\n",monotonic_seconds());
#endif
				}
			} else { // have current block
				if (workingtime>(WORKER_BUSY_LAST_REQUEST_TIMEOUT+WORKER_BUSY_WAIT_FOR_FINISH+((workers_total>HEAVYLOAD_WORKERS)?0.0:WORKER_BUSY_NOJOBS_INCREASE_TIMEOUT))) {
#ifdef RDEBUG
					fprintf(stderr,"%.6lf: readworker: current request not finished but busy timeout passed\n",monotonic_seconds());
#endif
					zassert(pthread_mutex_unlock(&glock));
					status = EINTR;
					break;
				}
			}

			if (reqsend==0) {
				if (rreq->offset > mfleng) {
					rreq->rleng = 0;
				} else if ((rreq->offset + rreq->leng) > mfleng) {
					rreq->rleng = mfleng - rreq->offset;
				} else {
					rreq->rleng = rreq->leng;
				}
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
					put32bit(&wptr,(rreq->offset & MFSCHUNKMASK));
					put32bit(&wptr,rreq->rleng);
					sent = 0;
					reqsend = 1;
				} else {
					tosend = 0;
					sent = 0;
					reqsend = 1;
					gotstatus = 1;
					zassert(pthread_mutex_unlock(&glock));
					continue;
				}
			}

			id->waitingworker=1;
			zassert(pthread_mutex_unlock(&glock));

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
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: write to (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,strerr(errno),currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
						status = EIO;
						zassert(pthread_mutex_lock(&glock));
						id->waitingworker=0;
						zassert(pthread_mutex_unlock(&glock));
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
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: poll error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,strerr(errno),currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
					status = EIO;
					break;
				}
			}
			zassert(pthread_mutex_lock(&glock));
			id->waitingworker=0;
			closewaiting = (id->closewaiting>0)?1:0;
			zassert(pthread_mutex_unlock(&glock));
			if (pfd[1].revents&POLLIN) {    // used just to break poll - so just read all data from pipe to empty it
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker: %"PRIu32" woken up by pipe\n",monotonic_seconds(),id->inode);
#endif
				i = read(id->pipe[0],pipebuff,1024);
				if (i<0) { // mainly to make happy static code analyzers
					syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: read pipe error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,strerr(errno),currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
				}
			}
			if (closewaiting) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: readworker: closewaiting\n",monotonic_seconds());
#endif
				status = EINTR;
				break;
			}
			if (pfd[0].revents&POLLHUP) {
				read_prepare_ip(csstrip,ip);
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / POLLHUP (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
				status = EIO;
				break;
			}
			if (pfd[0].revents&POLLERR) {
				read_prepare_ip(csstrip,ip);
				syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") got error status / POLLERR (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
				status = EIO;
				break;
			}
			if (pfd[0].revents&POLLIN) {
				lastrcvd = monotonic_seconds();
				if (received < 8) {
					i = read(fd,recvbuff+received,8-received);
					if (i==0) {
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer / ZEROREAD (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
						status = EIO;
						break;
					}
					if (i<0) {
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: read from (%s:%"PRIu16") error: %s (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,strerr(errno),currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
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
								break;
							}
						} else if (reccmd==CSTOCL_READ_DATA) {
							if (rreq==NULL) {
								syslog(LOG_WARNING,"readworker: got unexpected data from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								break;
							} else if (recleng<20) {
								syslog(LOG_WARNING,"readworker: got too short data packet from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								break;
							} else if ((recleng-20) + currentpos > rreq->rleng) {
								syslog(LOG_WARNING,"readworker: got too long data packet from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
								break;
							}
						} else if (reccmd==ANTOAN_NOP) {
							if (recleng!=0) {
								syslog(LOG_WARNING,"readworker: got wrong sized nop packet from chunkserver (leng:%"PRIu32")",recleng);
								status = EIO;
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
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") was reset by peer (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
						status = EIO;
						break;
					}
					if (i<0) {
						read_prepare_ip(csstrip,ip);
						syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - readworker: connection with (%s:%"PRIu16") got error status (received: %"PRIu32"/%"PRIu32"; try counter: %"PRIu32")",id->inode,chindx,chunkid,version,csstrip,port,currentpos,(rreq?rreq->rleng:0),id->trycnt+1);
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
						break;
					} else if (received == 8+recleng) {

						if (reccmd==CSTOCL_READ_STATUS) {
							rptr = recvbuff;
							recchunkid = get64bit(&rptr);
							recstatus = get8bit(&rptr);
							if (recchunkid != chunkid) {
								syslog(LOG_WARNING,"readworker: got unexpected status packet (expected chunkdid:%"PRIu64",packet chunkid:%"PRIu64")",chunkid,recchunkid);
								status = EIO;
								break;
							}
							if (recstatus!=STATUS_OK) {
								syslog(LOG_WARNING,"readworker: read error: %s",mfsstrerr(recstatus));
								status = EIO;
								break;
							}
							if (currentpos != rreq->rleng) {
								syslog(LOG_WARNING,"readworker: unexpected data block size (requested: %"PRIu32" / received: %"PRIu32")",rreq->rleng,currentpos);
								status = EIO;
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
								break;
							}
							if (recsize+20 != recleng) {
								syslog(LOG_WARNING,"readworker: got malformed data packet (datasize: %"PRIu32",packetsize: %"PRIu32")",recsize,recleng);
								status = EIO;
								break;
							}
							if (reccrc != mycrc32(0,rreq->data + (currentpos - recsize),recsize)) {
								syslog(LOG_WARNING,"readworker: data checksum error");
								status = EIO;
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

		zassert(pthread_mutex_lock(&glock));
		if (rreq) { // block hasn't been read
			rreq->busy = 0;
		}
		if (status!=0) {
			id->trycnt++;
			if (id->trycnt>=maxretries) {
				zassert(pthread_mutex_unlock(&glock));
				read_job_end(id,status,0);
			} else {
				zassert(pthread_mutex_unlock(&glock));
				read_job_end(id,0,1+((id->trycnt<30)?(id->trycnt/3):10));
			}
		} else {
			id->laststatus = 1;
			zassert(pthread_mutex_unlock(&glock));
			read_job_end(id,0,0);
		}
	}
	return NULL;
}

/* API | glock: INITIALIZED,UNLOCKED */
void read_data_init (uint64_t readaheadsize,uint32_t readaheadleng,uint32_t readaheadtrigger,uint32_t retries) {
        uint32_t i;
	sigset_t oldset;
	sigset_t newset;

	maxretries = retries;
	readahead = readaheadleng;
	readahead_trigger = readaheadtrigger;
	maxreadaheadsize = readaheadsize;
	reqbufftotalsize = 0;

	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_cond_init(&worker_term_cond,NULL));
	worker_term_waiting = 0;

	idhash = malloc(sizeof(inodedata*)*IDHASHSIZE);
	passert(idhash);
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
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
        zassert(pthread_create(&dqueue_worker_th,&worker_thattr,read_dqueue_worker,NULL));
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);

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
			zassert(pthread_cond_destroy(&(id->closecond)));
			close(id->pipe[0]);
			close(id->pipe[1]);
			free(id);
		}
	}
	free(idhash);
	//        free(cacheblocks);
	//        pthread_cond_destroy(&fcbcond);
	zassert(pthread_attr_destroy(&worker_thattr));
	zassert(pthread_cond_destroy(&worker_term_cond));
        zassert(pthread_mutex_destroy(&glock));
}



rrequest* read_new_request(inodedata *id,uint64_t *offset,uint64_t blockend) {
	uint64_t chunkoffset;
	uint64_t chunkend;
	uint32_t chunkleng;
	uint32_t chindx;

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
	fprintf(stderr,"%.6lf: inode: %"PRIu32" - new request: chindx: %"PRIu32" chunkoffset: %"PRIu64" chunkleng: %"PRIu32"\n",monotonic_seconds(),id->inode,chindx,chunkoffset,chunkleng);
#endif
	rreq->modified = monotonic_seconds();
	rreq->offset = chunkoffset;
	rreq->leng = chunkleng;
	rreq->chindx = chindx;
	rreq->rleng = 0;
	rreq->filled = 0;
	rreq->refresh = 0;
	rreq->busy = 0;
	rreq->free = 0;
	rreq->lcnt = 0;
	rreq->data = malloc(chunkleng);
	passert(rreq->data);
	rreq->waiting = 0;
	zassert(pthread_cond_init(&(rreq->cond),NULL));
	if (id->inqueue==0) {
		read_enqueue(id);
		id->inqueue=1;
	}
	rreq->next = NULL;
	rreq->prev = id->reqtail;
	*(id->reqtail) = rreq;
	id->reqtail = &(rreq->next);
	reqbufftotalsize+=chunkleng;
	return rreq;
}

typedef struct rlist_s {
	rrequest *rreq;
	uint64_t offsetadd;
	uint32_t reqleng;
	struct rlist_s *next;
} rlist;

// return list of rreq
int read_data(void *vid, uint64_t offset, uint32_t *size, void **vrhead,struct iovec **iov,uint32_t *iovcnt) {
	inodedata *id = (inodedata*)vid;
	rrequest *rreq,*rreqn;
	rlist *rl,*rhead,**rtail;
	uint64_t firstbyte;
	uint64_t lastbyte;
	uint32_t cnt;
	uint8_t newrequests;
	int status;
	double now;
	zassert(pthread_mutex_lock(&glock));

	*vrhead = NULL;
	*iov = NULL;
	*iovcnt = 0;
	cnt = 0;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" id->status: %d id->closewaiting: %"PRIu16"\n",monotonic_seconds(),id->inode,id->status,id->closewaiting);
#endif

	if (id->status==0 && id->closewaiting==0) {
		if (offset==id->lastoffset) {
			if (id->readahead==0) {
				if (id->seqdata>=readahead_trigger) {
					id->readahead = 1;
				}
			}
		} else {
			if (offset+(readahead/2) < id->lastoffset || id->lastoffset+(readahead/2) < offset) {
				id->readahead = 0;
				id->seqdata = 0;
			}
		}
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" seqdata: %"PRIu32" offset: %"PRIu64" id->lastoffset: %"PRIu64" id->readahead: %u reqbufftotalsize:%"PRIu64"\n",monotonic_seconds(),id->inode,id->seqdata,offset,id->lastoffset,id->readahead,reqbufftotalsize);
#endif
		newrequests = 0;

		// prepare requests

		firstbyte = offset;
		lastbyte = offset + (*size);
		rhead = NULL;
		rtail = &rhead;
		rreq = id->reqhead;
		now = monotonic_seconds();
		while (rreq && lastbyte>firstbyte) {
			rreqn = rreq->next;
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" , rreq->modified:%.6lf , rreq->offset: %"PRIu64" , rreq->leng: %"PRIu32" , firstbyte: %"PRIu64" , lastbyte: %"PRIu64"\n",monotonic_seconds(),id->inode,rreq->modified,rreq->offset,rreq->leng,firstbyte,lastbyte);
#endif
			if (rreq->modified+BUFFER_VALIDITY_TIMEOUT<now) { // buffer too old
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" data too old: free rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; free:%u)\n",monotonic_seconds(),id->inode,rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->free);
#endif
				if (rreq->lcnt==0 && rreq->busy==0) { // nobody wants it anymore, so delete it
					*(rreq->prev) = rreq->next;
					if (rreq->next) {
						rreq->next->prev = rreq->prev;
					} else {
						id->reqtail = rreq->prev;
					}
					reqbufftotalsize -= rreq->leng;
					free(rreq->data);
					free(rreq);
				} else {
					rreq->free = 1; // somenody still using it, so mark it for removal
				}
			} else if (firstbyte < rreq->offset || firstbyte >= rreq->offset+rreq->leng) { // all not sequential read cases
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" case 0: free rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; free:%u)\n",monotonic_seconds(),id->inode,rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->free);
#endif
				// rreq:      |---------|
				// read: |--|
				// read: |-------|
				// read: |-------------------|
				// read:                  |--|
				if (rreq->lcnt==0 && rreq->busy==0) { // nobody wants it anymore, so delete it
					*(rreq->prev) = rreq->next;
					if (rreq->next) {
						rreq->next->prev = rreq->prev;
					} else {
						id->reqtail = rreq->prev;
					}
					reqbufftotalsize -= rreq->leng;
					free(rreq->data);
					free(rreq);
				} else {
					rreq->free = 1; // somenody still using it, so mark it for removal
				}
			} else if (lastbyte <= rreq->offset+rreq->leng) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" case 1: use rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; free:%u)\n",monotonic_seconds(),id->inode,rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->free);
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
				if (id->readahead && id->flengisvalid && reqbufftotalsize<maxreadaheadsize) {
					if (lastbyte > rreq->offset + (rreq->leng/5)) {
						// request next block of data
						if (rreq->next==NULL) {
							uint64_t blockstart,blockend;
							blockstart = rreq->offset+rreq->leng;
							blockend = blockstart+readahead;
							if (blockend<=id->fleng) {
								rreq->next = read_new_request(id,&blockstart,blockend);
								newrequests = 1;
							} else if (blockstart<id->fleng) {
								rreq->next = read_new_request(id,&blockstart,id->fleng);
								newrequests = 1;
							}
						}
					}	
				}
				lastbyte = 0;
				firstbyte = 0;
			} else {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" case 2: use tail of rreq (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; free:%u)\n",monotonic_seconds(),id->inode,rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->free);
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
			rreq = read_new_request(id,&firstbyte,lastbyte);
			rl = malloc(sizeof(rlist));
			passert(rl);
			rl->rreq = rreq;
			rl->offsetadd = 0;
			rl->reqleng = rreq->leng;
			rl->next = NULL;
			*rtail = rl;
			rtail = &(rl->next);
			rreq->lcnt++;
			if (lastbyte==firstbyte && id->readahead && id->flengisvalid && reqbufftotalsize<maxreadaheadsize) {
				if (lastbyte+readahead<=id->fleng) {
					(void)read_new_request(id,&firstbyte,lastbyte+readahead);
				} else if (lastbyte<id->fleng) {
					(void)read_new_request(id,&firstbyte,id->fleng);
				}
			}
			newrequests = 1;
		}

		*vrhead = rhead;

#ifdef RDEBUG
		if (newrequests) {
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" newrequest\n",monotonic_seconds(),id->inode);
		}
#endif
		if (newrequests && id->waitingworker) {
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" wakeup readworker\n",monotonic_seconds(),id->inode);
#endif
			if (write(id->pipe[1]," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			id->waitingworker=0;
		}

		cnt = 0;
		*size = 0;
		for (rl = rhead ; rl ; rl=rl->next) {
			while (rl->rreq->filled==0 && id->status==0 && id->closewaiting==0) {
				rl->rreq->waiting++;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" wait for data: %"PRIu64":%"PRIu32"\n",monotonic_seconds(),id->inode,rl->rreq->offset,rl->rreq->leng);
#endif
				zassert(pthread_cond_wait(&(rl->rreq->cond),&glock));
				rl->rreq->waiting--;
			}
			if (id->status==0) {
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" block %"PRIu64":%"PRIu32"(%"PRIu32") has been read\n",monotonic_seconds(),id->inode,rl->rreq->offset,rl->rreq->rleng,rl->rreq->leng);
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
				fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" error reading block: %"PRIu64":%"PRIu32"\n",monotonic_seconds(),id->inode,rl->rreq->offset,rl->rreq->leng);
#endif
				break;
			}
#ifdef RDEBUG
			fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" size: %"PRIu32" ; cnt: %u\n",monotonic_seconds(),id->inode,*size,cnt);
#endif
		}
	}

	if (id->status==0 && id->closewaiting==0 && cnt>0) {
		id->lastoffset = offset + (*size);
		if (id->readahead==0) {
			id->seqdata += (*size);
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

	status = id->status;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" id->status: %d iovcnt: %"PRIu32" iovec: %p\n",monotonic_seconds(),id->inode,id->status,*iovcnt,(void*)(*iov));
#endif

	zassert(pthread_mutex_unlock(&glock));
	return status;
}

void read_data_free_buff(void *vid,void *vrhead,struct iovec *iov) {
	inodedata *id = (inodedata*)vid;
	rlist *rl,*rln;
	rrequest *rreq;
	rl = (rlist*)vrhead;
	zassert(pthread_mutex_lock(&glock));
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_data: inode: %"PRIu32" inode_structure: %p vrhead: %p iovec: %p\n",monotonic_seconds(),id->inode,(void*)id,(void*)vrhead,(void*)iov);
#endif
	while (rl) {
		rln = rl->next;
		rreq = rl->rreq;
		rreq->lcnt--;
		if (rreq->lcnt==0 && rreq->busy==0 && rreq->free) {
			*(rreq->prev) = rreq->next;
			if (rreq->next) {
				rreq->next->prev = rreq->prev;
			} else {
				id->reqtail = rreq->prev;
			}
			reqbufftotalsize -= rreq->leng;
			free(rreq->data);
			free(rreq);
		}
		free(rl);
		rl = rln;
	}
	if (id->reqhead==NULL && id->inqueue==0 && id->closewaiting>0) {
		zassert(pthread_cond_broadcast(&(id->closecond)));
	}
	if (iov) {
		free(iov);
	}
	zassert(pthread_mutex_unlock(&glock));
}

void read_inode_dirty_region(uint32_t inode,uint64_t offset,uint32_t size,const char *buff) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	rrequest *rreq,*rreqn;
//	int clearedbuff = 0;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_inode_dirty_region: inode: %"PRIu32" set dirty region: %"PRIu64":%"PRIu32"\n",monotonic_seconds(),inode,offset,size);
#endif
	zassert(pthread_mutex_lock(&glock));
	for (id = idhash[idh] ; id ; id=id->next) {
		if (id->inode == inode) {
			for (rreq = id->reqhead ; rreq ; rreq=rreqn) {
				rreqn = rreq->next;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (before): (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; filled:%u ; free:%u)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->filled,rreq->free);
#endif
				if (rreq->free==0 && ((rreq->offset < offset + size) && (rreq->offset + rreq->leng > offset))) {
					if (rreq->filled) { // already filled, exchange data
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
					} else if (rreq->busy) { // in progress, so refresh it
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (%"PRIu64":%"PRIu32") : refresh\n",monotonic_seconds(),rreq->offset,rreq->leng);
#endif
						rreq->refresh = 1;
					}
				}
#ifdef RDEBUG
				if (rreq) {
					fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (after): (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; filled:%u ; free:%u)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->filled,rreq->free);
				} else {
					fprintf(stderr,"%.6lf: read_inode_dirty_region: rreq (after): NULL\n",monotonic_seconds());
				}
#endif
			}
			if (id->flengisvalid && offset+size>id->fleng) {
				id->fleng = offset+size;
			}
			if (id->waitingworker) {
				if (write(id->pipe[1]," ",1)!=1) {
					syslog(LOG_ERR,"can't write to pipe !!!");
				}
				id->waitingworker=0;
			}
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}

// void read_inode_ops(uint32_t inode) {
void read_inode_set_length(uint32_t inode,uint64_t newlength,uint8_t active) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	rrequest *rreq,*rreqn;
	int inqueue = 0;

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: read_inode_set_length: inode: %"PRIu32" set length: %"PRIu64"\n",monotonic_seconds(),inode,newlength);
#endif
	zassert(pthread_mutex_lock(&glock));
	for (id = idhash[idh] ; id ; id=id->next) {
		if (id->inode == inode) {
			for (rreq = id->reqhead ; rreq ; rreq=rreqn) {
				rreqn = rreq->next;
#ifdef RDEBUG
				fprintf(stderr,"%.6lf: read_inode_set_length: rreq (before): (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; filled:%u ; free:%u)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->filled,rreq->free);
#endif
				if (rreq->free==0) {
					if (rreq->filled) {
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
							if (rreq->lcnt==0 && rreq->busy==0) { // nobody wants it anymore, so delete it
								*(rreq->prev) = rreq->next;
								if (rreq->next) {
									rreq->next->prev = rreq->prev;
								} else {
									id->reqtail = rreq->prev;
								}
								reqbufftotalsize -= rreq->leng;
								free(rreq->data);
								free(rreq);
								rreq = NULL;
							} else { // somebody wants it, so clear it
								rreq->filled = 0;
								if (rreq->busy==0) { // not busy ?
									inqueue = 1; // add inode to queue
								}
							}
						}
					} else if (rreq->busy) {
#ifdef RDEBUG
						fprintf(stderr,"%.6lf: read_inode_set_length: block is busy - refresh\n",monotonic_seconds());
#endif
						rreq->refresh = 1;
					}
				}
#if 0
				if (rreq->free==0) {
					if (id->flengisvalid==0) { // refresh everything
						if (rreq->filled) {
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: read_inode_set_length: old length unknown, block filled - clear it\n",monotonic_seconds());
#endif
							if (rreq->lcnt==0 && rreq->busy==0) { // nobody wants it anymore, so delete it
								*(rreq->prev) = rreq->next;
								if (rreq->next) {
									rreq->next->prev = rreq->prev;
								} else {
									id->reqtail = rreq->prev;
								}
								reqbufftotalsize -= rreq->leng;
								free(rreq->data);
								free(rreq);
								rreq = NULL;
							} else { // somebody wants it, so clear it
								rreq->filled = 0;
								if (rreq->busy==0) { // not busy ?
									clearedbuff = 1; // add inode to queue
								}
							}
						} else if (rreq->busy) {
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: read_inode_set_length: old length unknown, block is busy - refresh block\n",monotonic_seconds());
#endif
							rreq->refresh = 1;
						}
					} else if (id->fleng > newlength) { // file is shorter
						if (rreq->filled) {
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: read_inode_set_length: new length is smaller than previous, block filled - change rleng\n",monotonic_seconds());
#endif
							if (newlength<=rreq->offset) {
								rreq->rleng = 0;
							} else if (newlength<rreq->offset+rreq->leng) {
								rreq->rleng = newlength - rreq->offset;
							}
						} else if (rreq->busy) {
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: read_inode_set_length: new length is smaller than previous, block is busy - refresh block\n",monotonic_seconds());
#endif
							rreq->refresh = 1;
						}
					} else if (id->fleng < newlength) { // file is longer
						if (rreq->filled) {
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: read_inode_set_length: new length is larger than previous, block filled - clear buffer and change rleng\n",monotonic_seconds());
#endif
							if (newlength >= rreq->offset + rreq->leng) {
								memset(rreq->data + rreq->rleng,0,rreq->leng - rreq->rleng);
								rreq->rleng = rreq->leng;
							} else if (newlength > rreq->offset + rreq->rleng) {
								memset(rreq->data + rreq->rleng,0,newlength - (rreq->offset + rreq->rleng));
								rreq->rleng = (newlength - rreq->offset);
							}
						} else if (rreq->busy) {
#ifdef RDEBUG
							fprintf(stderr,"%.6lf: read_inode_set_length: new length is larger than previous, block is busy - refresh block\n",monotonic_seconds());
#endif
							rreq->refresh = 1;
						}
					}
				}
#endif
/*
				if (rreq->free==0 && ((newlength < rreq->offset + rreq->leng) || (id->fleng < rreq->offset + rreq->leng))) {
					if (rreq->filled) {
						if (rreq->lcnt==0 && rreq->busy==0) { // nobody wants it anymore, so delete it
							*(rreq->prev) = rreq->next;
							if (rreq->next) {
								rreq->next->prev = rreq->prev;
							} else {
								id->reqtail = rreq->prev;
							}
							reqbufftotalsize -= rreq->leng;
							free(rreq->data);
							free(rreq);
							rreq = NULL;
						} else { // somebody wants it, so clear it
							rreq->filled = 0;
							if (rreq->busy==0) { // not busy ?
								clearedbuff = 1; // add inode to queue
							}
						}
					} else if (rreq->busy) {
						rreq->refresh = 1;
					}
				}
*/
#ifdef RDEBUG
				if (rreq) {
					fprintf(stderr,"%.6lf: read_inode_set_length: rreq (after): (%"PRIu64":%"PRIu32" ; lcnt:%u ; busy:%u ; filled:%u ; free:%u)\n",monotonic_seconds(),rreq->offset,rreq->leng,rreq->lcnt,rreq->busy,rreq->filled,rreq->free);
				} else {
					fprintf(stderr,"%.6lf: read_inode_set_length: rreq (after): NULL\n",monotonic_seconds());
				}
#endif
			}
			if (inqueue && id->inqueue==0) {
				read_enqueue(id);
				id->inqueue=1;
			}
			id->fleng = newlength;
			id->flengisvalid = 1;
			if (id->waitingworker) {
				if (write(id->pipe[1]," ",1)!=1) {
					syslog(LOG_ERR,"can't write to pipe !!!");
				}
				id->waitingworker=0;
			}
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}

void* read_data_new(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	int pfd[2];

	zassert(pthread_mutex_lock(&glock));

	if (pipe(pfd)<0) {
		syslog(LOG_WARNING,"pipe error: %s",strerr(errno));
		zassert(pthread_mutex_unlock(&glock));
		return NULL;
	}

	id = malloc(sizeof(inodedata));
	passert(id);
	id->inode = inode;
	id->flengisvalid = 0;
	id->seqdata = 0;
	id->fleng = 0;
	id->status = 0;
	id->trycnt = 0;
	id->pipe[0] = pfd[0];
	id->pipe[1] = pfd[1];
	id->inqueue = 0;
	id->readahead = 0;
	id->lastoffset = 0;
	id->closewaiting = 0;
	id->waitingworker = 0;
	id->lastchunkid = 0;
	id->lastip = 0;
	id->lastport = 0;
	id->laststatus = 0;
	zassert(pthread_cond_init(&(id->closecond),NULL));
	id->reqhead = NULL;
	id->reqtail = &(id->reqhead);
	id->next = idhash[idh];
	idhash[idh] = id;
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: opening: %"PRIu32" ; inode_structure: %p\n",monotonic_seconds(),inode,(void*)id);
//	read_data_hexdump((uint8_t*)id,sizeof(inodedata));
#endif
	zassert(pthread_mutex_unlock(&glock));
	return id;
}

void read_data_end(void *vid) {
	inodedata *id,**idp;
	rrequest *rreq,*rreqn;
	inodedata *rid = (inodedata*)vid;
	uint32_t idh = IDHASH(rid->inode);

#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: %"PRIu32" ; inode_structure: %p\n",monotonic_seconds(),rid->inode,(void*)rid);
//	read_data_hexdump((uint8_t*)rid,sizeof(inodedata));
#endif
	zassert(pthread_mutex_lock(&glock));
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: %"PRIu32" ; cleaning req list\n",monotonic_seconds(),rid->inode);
#endif
	for (rreq = rid->reqhead ; rreq ; rreq=rreqn) {
		rreqn = rreq->next;
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: closing: %"PRIu32" ; rreq: lcnt: %u ; busy: %u ; free: %u ; filled: %u\n",monotonic_seconds(),rid->inode,rreq->lcnt,rreq->busy,rreq->free,rreq->filled);
#endif
		if (rreq->lcnt==0 && rreq->busy==0) {
			*(rreq->prev) = rreq->next;
			if (rreq->next) {
				rreq->next->prev = rreq->prev;
			} else {
				rid->reqtail = rreq->prev;
			}
			reqbufftotalsize -= rreq->leng;
			free(rreq->data);
			free(rreq);
		} else {
			rreq->free = 1;
		}
	}
	while (rid->reqhead!=NULL || rid->inqueue==1) {
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: closing: %"PRIu32" ; reqhead: %s ; inqueue: %u\n",monotonic_seconds(),rid->inode,rid->reqhead?"NOT NULL":"NULL",rid->inqueue);
#endif
		rid->closewaiting++;
		if (rid->waitingworker) {
			if (write(rid->pipe[1]," ",1)!=1) {
				syslog(LOG_ERR,"can't write to pipe !!!");
			}
			rid->waitingworker=0;
		}
#ifdef RDEBUG
		fprintf(stderr,"%.6lf: inode: %"PRIu32" ; waiting for close\n",monotonic_seconds(),rid->inode);
#endif
		zassert(pthread_cond_wait(&(rid->closecond),&glock));
		rid->closewaiting--;
	}
#ifdef RDEBUG
	fprintf(stderr,"%.6lf: closing: %"PRIu32" ; reqhead: %s ; inqueue: %u - delete structure\n",monotonic_seconds(),rid->inode,rid->reqhead?"NOT NULL":"NULL",rid->inqueue);
#endif
	idp = &(idhash[idh]);
	while ((id=*idp)) {
		if (id==rid) {
			*idp = id->next;
			zassert(pthread_cond_destroy(&(id->closecond)));
			close(id->pipe[0]);
			close(id->pipe[1]);
			free(id);
		} else {
			idp = &(id->next);
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}
