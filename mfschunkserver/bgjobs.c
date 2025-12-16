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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <limits.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>

#include "main.h"
#include "cfg.h"
#include "pcqueue.h"
#include "ionice.h"
#include "lwthread.h"
#include "datapack.h"
#include "massert.h"
#include "clocks.h"

#include "mainserv.h"
#include "hddspacemgr.h"
#include "replicator.h"
#include "masterconn.h"

#define JHASHSIZE 0x400
#define JHASHPOS(id) ((id)&0x3FF)

enum {
	JSTATE_DISABLED,
	JSTATE_ENABLED,
	JSTATE_INPROGRESS
};

enum {
	OP_EXIT=0,
	OP_INVAL,
//	OP_MAINSERV,
	OP_CHUNKOP,
//	OP_OPEN,
//	OP_CLOSE,
//	OP_READ,
//	OP_WRITE,
	OP_SERV_READ,
	OP_SERV_WRITE,
	OP_REPLICATE_SIMPLE,
	OP_REPLICATE_SPLIT,
	OP_REPLICATE_RECOVER,
	OP_REPLICATE_JOIN,
	OP_GETINFO,
	OP_CHUNKMOVE,
};

#define TASK_READ 0
#define TASK_WRITE 1
#define TASK_REPLICATE 2
#define TASK_CHUNKOP 3
#define TASK_INFO 4
#define TASK_MOVE 5
#define TASK_COUNT 6

static char* jobtype_str[TASK_COUNT] = {
	"read",
	"write",
	"replicate",
	"chunk operation",
	"chunk info",
	"chunk move"
};

// for OP_CHUNKOP
typedef struct _chunk_op_args {
	uint64_t chunkid,copychunkid;
	uint32_t version,newversion,copyversion;
	uint32_t length;
} chunk_op_args;
/*
// for OP_OPEN and OP_CLOSE
typedef struct _chunk_oc_args {
	uint64_t chunkid;
	uint32_t version;
} chunk_oc_args;

// for OP_READ
typedef struct _chunk_rd_args {
	uint64_t chunkid;
	uint32_t version;
	uint32_t offset,size;
	uint16_t blocknum;
	uint8_t *buffer;
	uint8_t *crcbuff;
} chunk_rd_args;

// for OP_WRITE
typedef struct _chunk_wr_args {
	uint64_t chunkid;
	uint32_t version;
	uint32_t offset,size;
	uint16_t blocknum;
	const uint8_t *buffer;
	const uint8_t *crcbuff;
} chunk_wr_args;
*/

// for OP_SERV_READ and OP_SERV_WRITE
typedef struct _chunk_rw_args {
	int sock;
	const uint8_t *packet;
	uint32_t length;
} chunk_rw_args;

// for OP_REPLICATE_SIMPLE, OP_REPLICATE_SPLIT, OP_REPLICATE_RECOVER and OP_REPLICATE_JOIN
typedef struct _chunk_rp_args {
	uint64_t chunkid;
	uint32_t version;
	uint8_t partno; // SPLIT
	uint8_t parts; // SPLIT,RECOVER,JOIN
	uint32_t srcip[MAX_EC_PARTS];
	uint16_t srcport[MAX_EC_PARTS];
	uint64_t srcchunkid[MAX_EC_PARTS];
} chunk_rp_args;

// for OP_GETINFO
typedef struct _chunk_ij_args {
	uint64_t chunkid;
	uint32_t version;
	uint8_t requested_info;
	void *pointer;
} chunk_ij_args;

// for OP_CHUNKMOVE
typedef struct _chunk_mv_args {
	void *fsrc;
	void *fdst;
} chunk_mv_args;

typedef struct _job {
	uint32_t jobid;
	void (*callback)(uint8_t status,void *extra);
	void *extra;
	void *args;
	uint8_t jstate;
	uint8_t tasktype;
	uint64_t starttime;
	uint64_t chunkid; // debug only
	struct _job *next;
} job;

typedef struct _jobpool {
	int rpipe,wpipe;
	int32_t fdpdescpos;
	uint32_t workers_max;
	uint32_t workers_himark;
	uint32_t workers_lomark;
	uint32_t workers_max_idle;
	uint32_t workers_avail;
	uint32_t workers_total;
	uint32_t workers_term_waiting;
	pthread_cond_t worker_term_cond;
	pthread_mutex_t pipelock;
	pthread_mutex_t jobslock;
	void* (*worker_fn)(void*);
	void *jobqueue;
	void *statusqueue;
	job* jobhash[JHASHSIZE];
	uint64_t jobs_time_max_glob[TASK_COUNT];
	uint64_t jobs_time_max_prev[TASK_COUNT];
	uint64_t jobs_time_prev[TASK_COUNT];
	uint32_t jobs_count_prev[TASK_COUNT];
	uint64_t jobs_time_max[TASK_COUNT];
	uint64_t jobs_time[TASK_COUNT];
	uint32_t jobs_count[TASK_COUNT];
	uint32_t nextjobid;
} jobpool;

typedef struct _worker {
	pthread_t thread_id;
	jobpool *jp;
} worker;

static jobpool* hp_pool = NULL;
static jobpool* lp_pool = NULL;

static uint32_t stats_maxjobscnt = 0;

// static uint8_t exiting;

void job_stats(uint32_t *maxjobscnt) {
	*maxjobscnt = stats_maxjobscnt;
	stats_maxjobscnt = 0;
}

static inline void job_send_status(jobpool *jp,uint32_t jobid,uint8_t status) {
	zassert(pthread_mutex_lock(&(jp->pipelock)));
	if (queue_isempty(jp->statusqueue)) {	// first status
		eassert(write(jp->wpipe,&status,1)==1);	// write anything to wake up select
	}
	queue_put(jp->statusqueue,jobid,status,NULL,1);
	zassert(pthread_mutex_unlock(&(jp->pipelock)));
	return;
}

static inline int job_receive_status(jobpool *jp,uint32_t *jobid,uint8_t *status) {
	uint32_t qstatus;
	zassert(pthread_mutex_lock(&(jp->pipelock)));
	queue_get(jp->statusqueue,jobid,&qstatus,NULL,NULL);
	*status = qstatus;
	if (queue_isempty(jp->statusqueue)) {
		eassert(read(jp->rpipe,&qstatus,1)==1);	// make pipe empty
		zassert(pthread_mutex_unlock(&(jp->pipelock)));
		return 0;	// last element
	}
	zassert(pthread_mutex_unlock(&(jp->pipelock)));
	return 1;	// not last
}

void* job_worker(void *arg);

static uint32_t lastnotify = 0;

static inline void job_spawn_worker(jobpool *jp) {
	worker *w;

	w = malloc(sizeof(worker));
	passert(w);
	w->jp = jp;
	if (lwt_minthread_create(&(w->thread_id),0,jp->worker_fn,w)<0) {
		return;
	}
	jp->workers_avail++;
	jp->workers_total++;
	if (jp->workers_total%10==0 && lastnotify!=jp->workers_total) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"workers: %"PRIu32"+",jp->workers_total);
		lastnotify = jp->workers_total;
	}
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"jobs: spawn worker (total: %"PRIu32")",jp->workers_total);
}

static inline void job_close_worker(worker *w) {
	jobpool *jp = w->jp;
	jp->workers_avail--;
	jp->workers_total--;
	if (jp->workers_total==0 && jp->workers_term_waiting) {
		zassert(pthread_cond_signal(&(jp->worker_term_cond)));
		jp->workers_term_waiting--;
	}
	pthread_detach(w->thread_id);
	free(w);
	if (jp->workers_total%10==0 && lastnotify!=jp->workers_total) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"workers: %"PRIu32"-",jp->workers_total);
		lastnotify = jp->workers_total;
	}
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"jobs: close worker (total: %"PRIu32")",jp->workers_total);
}

#define opargs ((chunk_op_args*)(jptr->args))
// #define ocargs ((chunk_oc_args*)(jptr->args))
// #define rdargs ((chunk_rd_args*)(jptr->args))
// #define wrargs ((chunk_wr_args*)(jptr->args))
#define rwargs ((chunk_rw_args*)(jptr->args))
#define rpargs ((chunk_rp_args*)(jptr->args))
#define ijargs ((chunk_ij_args*)(jptr->args))
#define mvargs ((chunk_mv_args*)(jptr->args))
void* job_worker(void *arg) {
	worker *w = (worker*)arg;
	jobpool *jp = w->jp;
	job *jptr;
	uint8_t *jptrarg;
	uint8_t status,jstate;
	uint32_t jobid;
	uint32_t op;

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"worker %p started (jobqueue: %p ; jptr:%p ; jptrarg:%p ; status:%p )",(void*)pthread_self(),jp->jobqueue,(void*)&jptr,(void*)&jptrarg,(void*)&status);
	for (;;) {
		queue_get(jp->jobqueue,&jobid,&op,&jptrarg,NULL);
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"job worker got job: %"PRIu32",%"PRIu32,jobid,op);
		jptr = (job*)jptrarg;
		zassert(pthread_mutex_lock(&(jp->jobslock)));
		if (jobid==0 && op==0 && jptrarg==NULL) { // queue has been closed
			job_close_worker(w);
			zassert(pthread_mutex_unlock(&(jp->jobslock)));
			return NULL;
		}
		jp->workers_avail--;
		if (jp->workers_avail==0 && jp->workers_total<jp->workers_max) {
			job_spawn_worker(jp);
		}
		if (jptr!=NULL) {
			jstate=jptr->jstate;
			if (jptr->jstate==JSTATE_ENABLED) {
				jptr->jstate=JSTATE_INPROGRESS;
				jptr->starttime = monotonic_useconds();
			}
		} else {
			jstate=JSTATE_DISABLED;
		}
		zassert(pthread_mutex_unlock(&(jp->jobslock)));
		switch (op) {
			case OP_INVAL:
				status = MFS_ERROR_EINVAL;
				break;
/*
			case OP_MAINSERV:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					mainserv_serve(*((int*)(jptr->args)));
					status = MFS_STATUS_OK;
				}
				break;
*/
			case OP_CHUNKOP:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = hdd_chunkop(opargs->chunkid,opargs->version,opargs->newversion,opargs->copychunkid,opargs->copyversion,opargs->length);
				}
				break;
/*
			case OP_OPEN:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = hdd_open(ocargs->chunkid,ocargs->version);
				}
				break;
			case OP_CLOSE:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = hdd_close(ocargs->chunkid);
				}
				break;
			case OP_READ:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = hdd_read(rdargs->chunkid,rdargs->version,rdargs->blocknum,rdargs->buffer,rdargs->offset,rdargs->size,rdargs->crcbuff);
				}
				break;
			case OP_WRITE:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = hdd_write(wrargs->chunkid,wrargs->version,wrargs->blocknum,wrargs->buffer,wrargs->offset,wrargs->size,wrargs->crcbuff);
				}
				break;
*/
			case OP_SERV_READ:
				if (jstate==JSTATE_DISABLED) {
					status = 0;
				} else {
					status = mainserv_read(rwargs->sock,rwargs->packet,rwargs->length);
				}
				break;
			case OP_SERV_WRITE:
				if (jstate==JSTATE_DISABLED) {
					status = 0;
				} else {
					status = mainserv_write(rwargs->sock,rwargs->packet,rwargs->length);
				}
				break;
			case OP_REPLICATE_SIMPLE:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = replicate(SIMPLE,rpargs->chunkid,rpargs->version,rpargs->partno,rpargs->parts,rpargs->srcip,rpargs->srcport,rpargs->srcchunkid);
				}
				break;
			case OP_REPLICATE_SPLIT:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = replicate(SPLIT,rpargs->chunkid,rpargs->version,rpargs->partno,rpargs->parts,rpargs->srcip,rpargs->srcport,rpargs->srcchunkid);
				}
				break;
			case OP_REPLICATE_RECOVER:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = replicate(RECOVER,rpargs->chunkid,rpargs->version,rpargs->partno,rpargs->parts,rpargs->srcip,rpargs->srcport,rpargs->srcchunkid);
				}
				break;
			case OP_REPLICATE_JOIN:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = replicate(JOIN,rpargs->chunkid,rpargs->version,rpargs->partno,rpargs->parts,rpargs->srcip,rpargs->srcport,rpargs->srcchunkid);
				}
				break;
			case OP_GETINFO:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = hdd_get_chunk_info(ijargs->chunkid,ijargs->version,ijargs->requested_info,ijargs->pointer);
				}
				break;
			case OP_CHUNKMOVE:
				if (jstate==JSTATE_DISABLED) {
					status = MFS_ERROR_NOTDONE;
				} else {
					status = hdd_move(mvargs->fsrc,mvargs->fdst);
				}
				break;
			default: // OP_EXIT
//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"worker %p exiting (jobqueue: %p)",(void*)pthread_self(),jp->jobqueue);
				zassert(pthread_mutex_lock(&(jp->jobslock)));
				job_close_worker(w);
				zassert(pthread_mutex_unlock(&(jp->jobslock)));
				return NULL;
		}
		zassert(pthread_mutex_lock(&(jp->jobslock)));
		if (jptr!=NULL) {
			uint64_t tasktime;
			tasktime = (monotonic_useconds()-jptr->starttime);
			jp->jobs_count[jptr->tasktype & 0x7]++;
			jp->jobs_time[jptr->tasktype & 0x7] += tasktime;
			if (tasktime > jp->jobs_time_max[jptr->tasktype & 0x7]) {
				jp->jobs_time_max[jptr->tasktype & 0x7] = tasktime;
			}
			if (tasktime > jp->jobs_time_max_glob[jptr->tasktype & 0x7]) {
				jp->jobs_time_max_glob[jptr->tasktype & 0x7] = tasktime;
			}
			jptr->tasktype |= 0x80; // worker -> finished
			jptr->starttime = 0;
		}
		zassert(pthread_mutex_unlock(&(jp->jobslock)));
		job_send_status(jp,jobid,status);
		zassert(pthread_mutex_lock(&(jp->jobslock)));
		jp->workers_avail++;
		if (jp->workers_avail > jp->workers_max_idle) {
			job_close_worker(w);
			zassert(pthread_mutex_unlock(&(jp->jobslock)));
			return NULL;
		}
		zassert(pthread_mutex_unlock(&(jp->jobslock)));
	}
}

void* job_hp_worker(void *arg) {
	return job_worker(arg);
}

void* job_lp_worker(void *arg) {
	ionice_medium();
	return job_worker(arg);
}

static inline uint32_t job_op_to_tasktype(uint32_t op) {
	switch (op) {
		case OP_CHUNKOP:
			return TASK_CHUNKOP;
		case OP_SERV_READ:
			return TASK_READ;
		case OP_SERV_WRITE:
			return TASK_WRITE;
		case OP_REPLICATE_SIMPLE:
		case OP_REPLICATE_SPLIT:
		case OP_REPLICATE_RECOVER:
		case OP_REPLICATE_JOIN:
			return TASK_REPLICATE;
		case OP_CHUNKMOVE:
			return TASK_MOVE;
		default:
			return TASK_INFO;
	}
	return TASK_INFO;
}

#define JOB_MODE_ALWAYS_DO 0
#define JOB_MODE_LIMITED_RETURN 1
#define JOB_MODE_LIMITED_QUEUE 2

static inline uint32_t job_new(jobpool *jp,uint32_t op,uint64_t chunkid,void *args,void (*callback)(uint8_t status,void *extra),void *extra,uint8_t errstatus,uint8_t jobmode) {
//	jobpool* jp = (jobpool*)jpool;
/*
	if (exiting) {
		if (callback) {
			callback(errstatus,extra);
		}
		if (args) {
			free(args);
		}
		return 0;
	} else {
*/
		uint32_t jobid;
		uint32_t jhpos;
		uint32_t workers_busy;
		uint32_t limit;
		job **jhandle,*jptr;

		jptr = malloc(sizeof(job));
		passert(jptr);

		zassert(pthread_mutex_lock(&(jp->jobslock)));
		jobid = jp->nextjobid;
		jp->nextjobid++;
		if (jp->nextjobid==0) {
			jp->nextjobid=1;
		}
		jhpos = JHASHPOS(jobid);
		jptr->jobid = jobid;
		jptr->callback = callback;
		jptr->extra = extra;
		jptr->args = args;
		jptr->jstate = JSTATE_ENABLED;
		jptr->starttime = 0;
		jptr->chunkid = chunkid;
		jptr->tasktype = job_op_to_tasktype(op);
		jptr->next = jp->jobhash[jhpos];
		jp->jobhash[jhpos] = jptr;
		workers_busy = jp->workers_total-jp->workers_avail;
		limit = jp->workers_max;
		zassert(pthread_mutex_unlock(&(jp->jobslock)));
		if (queue_elements(jp->jobqueue)+workers_busy>limit && jobmode!=JOB_MODE_ALWAYS_DO) {
			if (jobmode==JOB_MODE_LIMITED_RETURN) {
				// remove this job from data structures
				zassert(pthread_mutex_lock(&(jp->jobslock)));
				jhandle = jp->jobhash+jhpos;
				while ((jptr = *jhandle)) {
					if (jptr->jobid==jobid) {
						*jhandle = jptr->next;
						if (jptr->args) {
							free(jptr->args);
						}
						free(jptr);
						break;
					} else {
						jhandle = &(jptr->next);
					}
				}
				zassert(pthread_mutex_unlock(&(jp->jobslock)));
				// end return jobid==0
				return 0;
			} else {
				job_send_status(jp,jobid,errstatus);
			}
		} else {
			queue_put(jp->jobqueue,jobid,op,(uint8_t*)jptr,1);
		}
		return jobid;
//	}
}

/* interface */

void* job_pool_new(void* (*worker_fn)(void*)) {
	int fd[2];
	uint32_t i;
	jobpool* jp;

	if (pipe(fd)<0) {
		return NULL;
	}
	jp=malloc(sizeof(jobpool));
	passert(jp);
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"new pool of workers (%p:%"PRIu8")",(void*)jp,workers);
	jp->worker_fn = worker_fn;
	jp->rpipe = fd[0];
	jp->wpipe = fd[1];
	jp->workers_avail = 0;
	jp->workers_total = 0;
	jp->workers_term_waiting = 0;
	zassert(pthread_cond_init(&(jp->worker_term_cond),NULL));
	zassert(pthread_mutex_init(&(jp->pipelock),NULL));
	zassert(pthread_mutex_init(&(jp->jobslock),NULL));
	jp->jobqueue = queue_new(0);
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"new jobqueue: %p",jp->jobqueue);
	jp->statusqueue = queue_new(0);
	zassert(pthread_mutex_lock(&(jp->jobslock)));
	for (i=0 ; i<TASK_COUNT ; i++) {
		jp->jobs_time_max_glob[i] = 0;
		jp->jobs_time_max_prev[i] = 0;
		jp->jobs_count_prev[i] = 0;
		jp->jobs_time_max[i] = 0;
		jp->jobs_time[i] = 0;
		jp->jobs_count[i] = 0;
	}
	for (i=0 ; i<JHASHSIZE ; i++) {
		jp->jobhash[i]=NULL;
	}
	jp->nextjobid = 1;
	job_spawn_worker(jp);
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
	return jp;
}

uint32_t job_pool_jobs_count(jobpool *jp) {
	uint32_t res;
	zassert(pthread_mutex_lock(&(jp->jobslock)));
	res = (jp->workers_total - jp->workers_avail) + queue_elements(jp->jobqueue);
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
	return res;
}

/*
void job_pool_disable_and_change_callback_all(void (*callback)(uint8_t status,void *extra)) {
	jobpool* jp = globalpool;
	uint32_t jhpos;
	job *jptr;

	zassert(pthread_mutex_lock(&(jp->jobslock)));
	for (jhpos = 0 ; jhpos<JHASHSIZE ; jhpos++) {
		for (jptr = jp->jobhash[jhpos] ; jptr ; jptr=jptr->next) {
			if (jptr->jstate==JSTATE_ENABLED) {
				jptr->jstate=JSTATE_DISABLED;
			}
			jptr->callback=callback;
		}
	}
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
}
*/

static inline void job_pool_disable_job_in_pool(jobpool* jp,uint32_t jobid) {
	uint32_t jhpos = JHASHPOS(jobid);
	job *jptr;

	zassert(pthread_mutex_lock(&(jp->jobslock)));
	for (jptr = jp->jobhash[jhpos] ; jptr ; jptr=jptr->next) {
		if (jptr->jobid==jobid) {
			if (jptr->jstate==JSTATE_ENABLED) {
				jptr->jstate=JSTATE_DISABLED;
			}
		}
	}
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
}

void job_pool_disable_job(uint32_t jobid) {
	job_pool_disable_job_in_pool(hp_pool,jobid);
	job_pool_disable_job_in_pool(lp_pool,jobid);
}

static inline void job_pool_change_callback_in_pool(jobpool* jp,uint32_t jobid,void (*callback)(uint8_t status,void *extra),void *extra) {
	uint32_t jhpos = JHASHPOS(jobid);
	job *jptr;

	zassert(pthread_mutex_lock(&(jp->jobslock)));
	for (jptr = jp->jobhash[jhpos] ; jptr ; jptr=jptr->next) {
		if (jptr->jobid==jobid) {
			jptr->callback=callback;
			jptr->extra=extra;
		}
	}
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
}

void job_pool_change_callback(uint32_t jobid,void (*callback)(uint8_t status,void *extra),void *extra) {
	job_pool_change_callback_in_pool(hp_pool,jobid,callback,extra);
	job_pool_change_callback_in_pool(lp_pool,jobid,callback,extra);
}

static inline void job_pool_check_jobs_in_pool(jobpool* jp,uint8_t cb) {
	uint32_t jobid,jhpos;
	uint8_t status;
	int notlast;
	job **jhandle,*jptr;

	zassert(pthread_mutex_lock(&(jp->jobslock)));
	do {
		notlast = job_receive_status(jp,&jobid,&status);
		jhpos = JHASHPOS(jobid);
		jhandle = jp->jobhash+jhpos;
		while ((jptr = *jhandle)) {
			if (jptr->jobid==jobid) {
				if (jptr->callback && cb) {
					jptr->callback(status,jptr->extra);
				}
				*jhandle = jptr->next;
				if (jptr->args) {
					free(jptr->args);
				}
				free(jptr);
				break;
			} else {
				jhandle = &(jptr->next);
			}
		}
	} while (notlast);
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
}

/*
void job_pool_check_jobs(uint8_t cb) {
	job_pool_check_jobs_in_pool(hp_pool,cb);
	job_pool_check_jobs_in_pool(lp_pool,cb);
}
*/
void job_pool_delete(jobpool* jp) {
	queue_close(jp->jobqueue);
	zassert(pthread_mutex_lock(&(jp->jobslock)));
	while (jp->workers_total>0) {
		jp->workers_term_waiting++;
		zassert(pthread_cond_wait(&(jp->worker_term_cond),&(jp->jobslock)));
	}
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
	if (!queue_isempty(jp->statusqueue)) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"not empty job queue !!!");
		job_pool_check_jobs_in_pool(jp,0);
	}
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"deleting jobqueue: %p",jp->jobqueue);
	queue_delete(jp->jobqueue);
	queue_delete(jp->statusqueue);
	zassert(pthread_cond_destroy(&(jp->worker_term_cond)));
	zassert(pthread_mutex_destroy(&(jp->pipelock)));
	zassert(pthread_mutex_destroy(&(jp->jobslock)));
	close(jp->rpipe);
	close(jp->wpipe);
	free(jp);
}

uint32_t job_inval(void (*callback)(uint8_t status,void *extra),void *extra) {
	jobpool* jp = lp_pool;
	return job_new(jp,OP_INVAL,0,NULL,callback,extra,MFS_ERROR_EINVAL,JOB_MODE_LIMITED_QUEUE);
}

/*
uint32_t job_mainserv(int sock) {
	jobpool* jp = globalpool;
	int *args;
	args = malloc(sizeof(int));
	passert(args);
	*args = sock;
	return job_new(jp,OP_MAINSERV,args,NULL,NULL);
}
*/

uint32_t job_chunkop(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
	jobpool* jp = hp_pool;
	chunk_op_args *args;
	args = malloc(sizeof(chunk_op_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	args->newversion = newversion;
	args->copychunkid = copychunkid;
	args->copyversion = copyversion;
	args->length = length;
	return job_new(jp,OP_CHUNKOP,chunkid,args,callback,extra,MFS_ERROR_NOTDONE,JOB_MODE_LIMITED_QUEUE);
}
/*
uint32_t job_open(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version) {
	jobpool* jp = globalpool;
	chunk_oc_args *args;
	args = malloc(sizeof(chunk_oc_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	return job_new(jp,OP_OPEN,args,callback,extra);
}

uint32_t job_close(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid) {
	jobpool* jp = globalpool;
	chunk_oc_args *args;
	args = malloc(sizeof(chunk_oc_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = 0;
	return job_new(jp,OP_CLOSE,args,callback,extra);
}

uint32_t job_read(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff) {
	jobpool* jp = globalpool;
	chunk_rd_args *args;
	args = malloc(sizeof(chunk_rd_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	args->blocknum = blocknum;
	args->buffer = buffer;
	args->offset = offset;
	args->size = size;
	args->crcbuff = crcbuff;
	return job_new(jp,OP_READ,args,callback,extra);
}

uint32_t job_write(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff) {
	jobpool* jp = globalpool;
	chunk_wr_args *args;
	args = malloc(sizeof(chunk_wr_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	args->blocknum = blocknum;
	args->buffer = buffer;
	args->offset = offset;
	args->size = size;
	args->crcbuff = crcbuff;
	return job_new(jp,OP_WRITE,args,callback,extra);
}
*/

uint32_t job_serv_read(void (*callback)(uint8_t status,void *extra),void *extra,int sock,const uint8_t *packet,uint32_t length) {
	jobpool* jp = hp_pool;
	chunk_rw_args *args;
	args = malloc(sizeof(chunk_rw_args));
	passert(args);
	args->sock = sock;
	args->packet = packet;
	args->length = length;
	return job_new(jp,OP_SERV_READ,0,args,callback,extra,0,JOB_MODE_LIMITED_RETURN);
}

uint32_t job_serv_write(void (*callback)(uint8_t status,void *extra),void *extra,int sock,const uint8_t *packet,uint32_t length) {
	jobpool* jp = hp_pool;
	chunk_rw_args *args;
	args = malloc(sizeof(chunk_rw_args));
	passert(args);
	args->sock = sock;
	args->packet = packet;
	args->length = length;
	return job_new(jp,OP_SERV_WRITE,0,args,callback,extra,0,JOB_MODE_LIMITED_RETURN);
}

uint32_t job_replicate_simple(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t srcip,uint16_t srcport) {
	jobpool* jp = lp_pool;
	chunk_rp_args *args;
	args = malloc(sizeof(chunk_rp_args));
	passert(args);
	memset(args,0,sizeof(chunk_rp_args));
	args->chunkid = chunkid;
	args->version = version;
	args->srcip[0] = srcip;
	args->srcport[0] = srcport;
	args->srcchunkid[0] = chunkid;
	return job_new(jp,OP_REPLICATE_SIMPLE,chunkid,args,callback,extra,MFS_ERROR_NOTDONE,JOB_MODE_LIMITED_QUEUE);
}

uint32_t job_replicate_split(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t srcip,uint16_t srcport,uint64_t srcchunkid,uint8_t partno,uint8_t parts) {
	jobpool* jp = lp_pool;
	chunk_rp_args *args;
	args = malloc(sizeof(chunk_rp_args));
	passert(args);
	memset(args,0,sizeof(chunk_rp_args));
	args->chunkid = chunkid;
	args->version = version;
	args->srcip[0] = srcip;
	args->srcport[0] = srcport;
	args->srcchunkid[0] = srcchunkid;
	args->partno = partno;
	args->parts = parts;
	return job_new(jp,OP_REPLICATE_SPLIT,chunkid,args,callback,extra,MFS_ERROR_NOTDONE,JOB_MODE_LIMITED_QUEUE);
}

uint32_t job_replicate_recover(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t parts,uint32_t srcip[MAX_EC_PARTS],uint16_t srcport[MAX_EC_PARTS],uint64_t srcchunkid[MAX_EC_PARTS]) {
	jobpool* jp = lp_pool;
	chunk_rp_args *args;
	uint8_t i;
	args = malloc(sizeof(chunk_rp_args));
	passert(args);
	memset(args,0,sizeof(chunk_rp_args));
	args->chunkid = chunkid;
	args->version = version;
	args->parts = parts;
	for (i=0 ; i<parts ; i++) {
		args->srcip[i] = srcip[i];
		args->srcport[i] = srcport[i];
		args->srcchunkid[i] = srcchunkid[i];
	}
	return job_new(jp,OP_REPLICATE_RECOVER,chunkid,args,callback,extra,MFS_ERROR_NOTDONE,JOB_MODE_LIMITED_QUEUE);
}

uint32_t job_replicate_join(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t parts,uint32_t srcip[MAX_EC_PARTS],uint16_t srcport[MAX_EC_PARTS],uint64_t srcchunkid[MAX_EC_PARTS]) {
	jobpool* jp = lp_pool;
	chunk_rp_args *args;
	uint8_t i;
	args = malloc(sizeof(chunk_rp_args));
	passert(args);
	memset(args,0,sizeof(chunk_rp_args));
	args->chunkid = chunkid;
	args->version = version;
	args->parts = parts;
	for (i=0 ; i<parts ; i++) {
		args->srcip[i] = srcip[i];
		args->srcport[i] = srcport[i];
		args->srcchunkid[i] = srcchunkid[i];
	}
	return job_new(jp,OP_REPLICATE_JOIN,chunkid,args,callback,extra,MFS_ERROR_NOTDONE,JOB_MODE_LIMITED_QUEUE);
}

uint32_t job_get_chunk_info(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t requested_info,uint8_t *info_buff) {
	jobpool* jp = lp_pool;
	chunk_ij_args *args;
	args = malloc(sizeof(chunk_ij_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	args->requested_info = requested_info;
	args->pointer = info_buff;
	return job_new(jp,OP_GETINFO,chunkid,args,callback,extra,MFS_ERROR_NOTDONE,JOB_MODE_LIMITED_QUEUE);
}

uint32_t job_chunk_move(void (*callback)(uint8_t status,void *extra),void *extra,void *fsrc,void *fdst) {
	jobpool* jp = lp_pool;
	chunk_mv_args *args;
	args = malloc(sizeof(chunk_mv_args));
	passert(args);
	args->fsrc = fsrc;
	args->fdst = fdst;
	return job_new(jp,OP_CHUNKMOVE,0,args,callback,extra,MFS_ERROR_NOTDONE,JOB_MODE_LIMITED_QUEUE);
}

void job_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;

	pdesc[pos].fd = hp_pool->rpipe;
	pdesc[pos].events = POLLIN;
	hp_pool->fdpdescpos = pos;
	pos++;

	pdesc[pos].fd = lp_pool->rpipe;
	pdesc[pos].events = POLLIN;
	lp_pool->fdpdescpos = pos;
	pos++;

	*ndesc = pos;
}

void job_serve(struct pollfd *pdesc) {
	jobpool* jp;
	uint32_t jobscnt;

	jp = hp_pool;
	if (jp->fdpdescpos>=0 && (pdesc[jp->fdpdescpos].revents & POLLIN)) {
		job_pool_check_jobs_in_pool(jp,1);
	}
	jp = lp_pool;
	if (jp->fdpdescpos>=0 && (pdesc[jp->fdpdescpos].revents & POLLIN)) {
		job_pool_check_jobs_in_pool(jp,1);
	}

	jobscnt = job_pool_jobs_count(hp_pool)+job_pool_jobs_count(lp_pool);
	if (jobscnt>=stats_maxjobscnt) {
		stats_maxjobscnt=jobscnt;
	}
}

// can be only HLSTATUS_OK or HLSTATUS_OVERLOADED
static uint8_t current_hlstatus = HLSTATUS_OK;

void job_get_load_and_hlstatus(uint32_t *load,uint8_t *hlstatus) {
	*load = job_pool_jobs_count(hp_pool)+job_pool_jobs_count(lp_pool);
	*hlstatus = current_hlstatus;
}

void job_heavyload_test(void) {
	uint8_t hlstatus;

	zassert(pthread_mutex_lock(&(hp_pool->jobslock)));
	zassert(pthread_mutex_lock(&(lp_pool->jobslock)));
	hlstatus = HLSTATUS_DEFAULT;
	if ((hp_pool->workers_total - hp_pool->workers_avail > hp_pool->workers_himark) || (lp_pool->workers_total - lp_pool->workers_avail > lp_pool->workers_himark)) {
		hlstatus = HLSTATUS_OVERLOADED;
	}
	if ((hp_pool->workers_total - hp_pool->workers_avail < hp_pool->workers_lomark) && (lp_pool->workers_total - lp_pool->workers_avail < lp_pool->workers_lomark)) {
		hlstatus = HLSTATUS_OK;
	}
	zassert(pthread_mutex_unlock(&(lp_pool->jobslock)));
	zassert(pthread_mutex_unlock(&(hp_pool->jobslock)));
	if (hlstatus!=HLSTATUS_DEFAULT && hlstatus!=current_hlstatus) {
		current_hlstatus = hlstatus;
		masterconn_reportload();
	}
}

//void job_wantexit(void) {
//	exiting = 1;
//}

void job_info(FILE *fd) {
	jobpool* jp;
	uint32_t wm,whm,wlm,wmi;
	uint32_t wa,wt;
	uint32_t qe,qa;
	uint32_t tasks_in_queue_disabled[TASK_COUNT];
	uint32_t tasks_in_queue_enabled[TASK_COUNT];
	uint32_t tasks_in_progress[TASK_COUNT];
	uint64_t task_time_max_glob[TASK_COUNT];
	uint64_t task_time_max[TASK_COUNT];
	uint64_t task_time_sum[TASK_COUNT];
	uint32_t task_count_sum[TASK_COUNT];
	uint32_t i,h;
	job *j;

	fprintf(fd,"[background jobs]\n");

	for (h=0 ; h<TASK_COUNT ; h++) {
		tasks_in_queue_disabled[h] = 0;
		tasks_in_queue_enabled[h] = 0;
		tasks_in_progress[h] = 0;
		task_time_max_glob[h] = 0;
		task_time_max[h] = 0;
		task_time_sum[h] = 0;
		task_count_sum[h] = 0;
	}

	for (i=0 ; i<2 ; i++) {
		jp = (i==0)?hp_pool:lp_pool;

		zassert(pthread_mutex_lock(&(jp->jobslock)));
		wm = jp->workers_max;
		whm = jp->workers_himark;
		wlm = jp->workers_lomark;
		wmi = jp->workers_max_idle;
		wa = jp->workers_avail;
		wt = jp->workers_total;
		qe = queue_elements(jp->jobqueue);
		qa = queue_sizeleft(jp->jobqueue);
		for (h=0 ; h<TASK_COUNT ; h++) {
			task_time_sum[h] += jp->jobs_time_prev[h];
			task_count_sum[h] += jp->jobs_count_prev[h];
			if (jp->jobs_time_max_prev[h]>task_time_max[h]) {
				task_time_max[h] = jp->jobs_time_max_prev[h];
			}
			if (jp->jobs_time_max_glob[h]>task_time_max_glob[h]) {
				task_time_max_glob[h] = jp->jobs_time_max_glob[h];
			}
		}
		for (h=0 ; h<JHASHSIZE ; h++) {
			for (j=jp->jobhash[h] ; j!=NULL ; j=j->next) {
				if ((j->tasktype&0x80)==0) {
					switch (j->jstate) {
						case JSTATE_INPROGRESS:
							tasks_in_progress[j->tasktype & 0x7]++;
							break;
						case JSTATE_ENABLED:
							tasks_in_queue_enabled[j->tasktype & 0x7]++;
							break;
						case JSTATE_DISABLED:
							tasks_in_queue_disabled[j->tasktype & 0x7]++;
							break;
					}
				}
			}
		}
		zassert(pthread_mutex_unlock(&(jp->jobslock)));

		fprintf(fd,"%s priority jobs params: workers_max: %"PRIu32" ; workers_himark: %"PRIu32" ; workers_lomark: %"PRIu32" ; workers_max_idle: %"PRIu32"\n",(i==0)?"hi":"lo",wm,whm,wlm,wmi);
		fprintf(fd,"%s priority jobs info: workers_total: %"PRIu32" ; workers_avail: %"PRIu32" ; queue_elements: %"PRIu32" ; queue_available: %"PRIu32"\n",(i==0)?"hi":"lo",wt,wa,qe,qa);
	}

	for (h=0 ; h<TASK_COUNT ; h++) {
		if (task_count_sum[h]>0) {
			task_time_sum[h] /= task_count_sum[h];
		} else {
			task_time_sum[h] = 0;
		}
	}

	fprintf(fd,"tasks in queue (enabled): reads: %"PRIu32" ; writes: %"PRIu32" ; replications: %"PRIu32" ; chunkops: %"PRIu32" ; chunkinfos: %"PRIu32" ; chunkmoves: %"PRIu32"\n",tasks_in_queue_enabled[0],tasks_in_queue_enabled[1],tasks_in_queue_enabled[2],tasks_in_queue_enabled[3],tasks_in_queue_enabled[4],tasks_in_queue_enabled[5]);
	fprintf(fd,"tasks in queue (disabled): reads: %"PRIu32" ; writes: %"PRIu32" ; replications: %"PRIu32" ; chunkops: %"PRIu32" ; chunkinfos: %"PRIu32" ; chunkmoves: %"PRIu32"\n",tasks_in_queue_disabled[0],tasks_in_queue_disabled[1],tasks_in_queue_disabled[2],tasks_in_queue_disabled[3],tasks_in_queue_disabled[4],tasks_in_queue_disabled[5]);
	fprintf(fd,"tasks in progress: reads: %"PRIu32" ; writes: %"PRIu32" ; replications: %"PRIu32" ; chunkops: %"PRIu32" ; chunkinfos: %"PRIu32" ; chunkmoves: %"PRIu32"\n",tasks_in_progress[0],tasks_in_progress[1],tasks_in_progress[2],tasks_in_progress[3],tasks_in_progress[4],tasks_in_progress[5]);
	fprintf(fd,"max task times (microseconds, since start): reads: %"PRIu64" ; writes: %"PRIu64" ; replications: %"PRIu64" ; chunkops: %"PRIu64" ; chunkinfos: %"PRIu64" ; chunkmoves: %"PRIu64"\n",task_time_max_glob[0],task_time_max_glob[1],task_time_max_glob[2],task_time_max_glob[3],task_time_max_glob[4],task_time_max_glob[5]);
	fprintf(fd,"max task times (microseconds, last minute): reads: %"PRIu64" ; writes: %"PRIu64" ; replications: %"PRIu64" ; chunkops: %"PRIu64" ; chunkinfos: %"PRIu64" ; chunkmoves: %"PRIu64"\n",task_time_max[0],task_time_max[1],task_time_max[2],task_time_max[3],task_time_max[4],task_time_max[5]);
	fprintf(fd,"avg task times (microseconds, last minute): reads: %"PRIu64" ; writes: %"PRIu64" ; replications: %"PRIu64" ; chunkops: %"PRIu64" ; chunkinfos: %"PRIu64" ; chunkmoves: %"PRIu64"\n",task_time_sum[0],task_time_sum[1],task_time_sum[2],task_time_sum[3],task_time_sum[4],task_time_sum[5]);
	fprintf(fd,"task counts (last minute): reads: %"PRIu32" ; writes: %"PRIu32" ; replications: %"PRIu32" ; chunkops: %"PRIu32" ; chunkinfos: %"PRIu32" ; chunkmoves: %"PRIu32"\n",task_count_sum[0],task_count_sum[1],task_count_sum[2],task_count_sum[3],task_count_sum[4],task_count_sum[5]);
	fprintf(fd,"\n");
}

void job_counters_shift(void) {
	jobpool* jp;
	uint32_t i,h;

	for (i=0 ; i<2 ; i++) {
		jp = (i==0)?hp_pool:lp_pool;
	
		zassert(pthread_mutex_lock(&(jp->jobslock)));
		for (h=0 ; h<TASK_COUNT ; h++) {
			jp->jobs_time_max_prev[h] = jp->jobs_time_max[h];
			jp->jobs_time_prev[h] = jp->jobs_time[h];
			jp->jobs_count_prev[h] = jp->jobs_count[h];
			jp->jobs_time_max[h] = 0;
			jp->jobs_time[h] = 0;
			jp->jobs_count[h] = 0;
		}
		zassert(pthread_mutex_unlock(&(jp->jobslock)));
	}
}

void job_stalled_check(void) {
	jobpool* jp;
	job *j;
	uint32_t i,h;
	uint64_t t;

	for (i=0 ; i<2 ; i++) {
		jp = (i==0)?hp_pool:lp_pool;

		zassert(pthread_mutex_lock(&(jp->jobslock)));
		t = monotonic_useconds();
		for (h=0 ; h<JHASHSIZE ; h++) {
			for (j=jp->jobhash[h] ; j!=NULL ; j=j->next) {
				if ((j->tasktype&0xC0)==0 && j->starttime>0) {
					if ((t>j->starttime) && (t-j->starttime > 600000000)) {
						if (j->chunkid) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"stalled job '%s' on chunk %016"PRIX64" detected",jobtype_str[j->tasktype & 0x7],j->chunkid);
						} else {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"stalled job '%s' detected",jobtype_str[j->tasktype & 0x7]);
						}
						j->tasktype |= 0x40;
					}
				}
			}
		}
		zassert(pthread_mutex_unlock(&(jp->jobslock)));
	}
}

int job_canexit(void) {
	return (job_pool_jobs_count(hp_pool)+job_pool_jobs_count(lp_pool)>0)?0:1;
}

void job_term(void) {
	job_pool_delete(hp_pool);
	job_pool_delete(lp_pool);
}

void job_reload(void) {
	jobpool* jp;
	uint32_t wm,whm,wlm,wmi;

	wm = cfg_getuint32("WORKERS_MAX",250);
	if (cfg_isdefined("WORKERS_HLOAD_HIMARK")) {
		whm = cfg_getuint32("WORKERS_HLOAD_HIMARK",187); // debug option
	} else {
		whm = (wm * 3) / 4;
	}
	if (cfg_isdefined("WORKERS_HLOAD_LOMARK")) {
		wlm = cfg_getuint32("WORKERS_HLOAD_LOMARK",125); // debug option
	} else {
		wlm = (wm * 2) / 4;
	}
	wmi = cfg_getuint32("WORKERS_MAX_IDLE",40);

	if (whm >= wm) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"WORKERS_HLOAD_HIMARK >= WORKERS_MAX - it doesn't make sense - setting WORKERS_HLOAD_HIMARK to WORKERS_MAX * 3/4");
		whm = (wm * 3) / 4;
	}
	if (wlm >= wm) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"WORKERS_HLOAD_LOMARK >= WORKERS_MAX - it doesn't make sense - setting WORKERS_HLOAD_LOMARK to WORKERS_MAX * 1/2");
		wlm = (wm * 2) / 4;
	} else if (wlm >= whm) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"WORKERS_HLOAD_LOMARK >= WORKERS_HLOAD_HIMARK - it doesn't make sense - setting WORKERS_HLOAD_LOMARK to WORKERS_HLOAD_HIMARK * 2/3");
		wlm = (whm * 2) / 3;
	}

	jp = hp_pool;

	zassert(pthread_mutex_lock(&(jp->jobslock)));

	jp->workers_max = wm;
	jp->workers_himark = whm;
	jp->workers_lomark = wlm;
	jp->workers_max_idle = wmi;

	zassert(pthread_mutex_unlock(&(jp->jobslock)));

	jp = lp_pool;

	zassert(pthread_mutex_lock(&(jp->jobslock)));

	jp->workers_max = wm;
	jp->workers_himark = whm;
	jp->workers_lomark = wlm;
	jp->workers_max_idle = wmi;

	zassert(pthread_mutex_unlock(&(jp->jobslock)));
}

int job_init(void) {
	hp_pool = job_pool_new(job_hp_worker);
	lp_pool = job_pool_new(job_lp_worker);

	if (hp_pool==NULL || lp_pool==NULL) {
		return -1;
	}
	job_reload();

	main_destruct_register(job_term);
//	main_wantexit_register(job_wantexit);
	main_canexit_register(job_canexit);
	main_reload_register(job_reload);
	main_eachloop_register(job_heavyload_test);
	main_poll_register(job_desc,job_serve);
	main_info_register(job_info);
	main_time_register(60,0,job_counters_shift);
	main_time_register(10,0,job_stalled_check);
	return 0;
}
