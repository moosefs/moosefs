/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#ifndef WIN32
#include <syslog.h>
#endif
#include <inttypes.h>

#include "squeue.h"
#include "massert.h"

typedef struct _workers {
	uint32_t sustainworkers;
	uint32_t maxworkers;
	void *jqueue;
	char *name;
	void (*workerfn)(void *data,uint32_t current_workers_count);
	pthread_mutex_t lock;
	pthread_cond_t term_cond;
	pthread_attr_t thattr;
	uint32_t avail;
	uint32_t total;
	uint32_t lastnotify;
} workers;

typedef struct _worker {
	workers *ws;
	pthread_t thread_id;
} worker;

static void* workers_worker_thread(void *arg);

/* ws->lock:LOCKED */
static inline void workers_spawn_worker(workers *ws) {
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
	w->ws = ws;
#ifndef WIN32
	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
#endif
	res = pthread_create(&(w->thread_id),&(ws->thattr),workers_worker_thread,w);
#ifndef WIN32
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
#endif
	if (res<0) {
		return;
	}
	ws->avail++;
	ws->total++;
	if (ws->total%10==0 && ws->total!=ws->lastnotify) {
		syslog(LOG_INFO,"%s workers: %"PRIu32"+",ws->name,ws->total);
		ws->lastnotify = ws->total;
	}
}

/* ws->lock:LOCKED */
static inline void workers_close_worker(worker *w) {
	workers *ws = w->ws;

	ws->avail--;
	ws->total--;
	if (ws->total%10==0 && ws->total!=ws->lastnotify) {
		syslog(LOG_INFO,"%s workers: %"PRIu32"-",ws->name,ws->total);
		ws->lastnotify = ws->total;
	}
	if (ws->total==0) {
		zassert(pthread_cond_signal(&(ws->term_cond)));
	}
	pthread_detach(w->thread_id);
	free(w);
}

static void* workers_worker_thread(void *arg) {
	worker *w = (worker*)arg;
	workers *ws = w->ws;
	uint32_t current_workers;
	void *data;
	uint8_t firstrun = 1;

	for (;;) {
//		syslog(LOG_NOTICE,"workers: worker loop");
		if (firstrun==0) {
			zassert(pthread_mutex_lock(&(ws->lock)));
			ws->avail++;
			if (ws->avail > ws->sustainworkers) {
				workers_close_worker(w);
				zassert(pthread_mutex_unlock(&(ws->lock)));
				return NULL;
			}
			zassert(pthread_mutex_unlock(&(ws->lock)));
		}
		firstrun = 0;

//		syslog(LOG_NOTICE,"waiting for jobs (queue:%p) ...",ws->jqueue);
		squeue_get(ws->jqueue,&data);
//		syslog(LOG_NOTICE,"got job (data:%p)",data);

		zassert(pthread_mutex_lock(&(ws->lock)));

		if (data==NULL) {
			workers_close_worker(w);
			zassert(pthread_mutex_unlock(&(ws->lock)));
			return NULL;
		}

		ws->avail--;
		if (ws->avail==0 && ws->total < ws->maxworkers) {
			workers_spawn_worker(ws);
		}
		current_workers = ws->total;
		zassert(pthread_mutex_unlock(&(ws->lock)));
		ws->workerfn(data,current_workers);
	}
	return NULL;
}

void* workers_init(uint32_t maxworkers,uint32_t sustainworkers,uint32_t qleng,char *name,void (*workerfn)(void *data,uint32_t current_workers_count)) {
	workers *ws;
	size_t mystacksize;

	ws = malloc(sizeof(workers));
	passert(ws);
	ws->sustainworkers = sustainworkers;
	ws->maxworkers = maxworkers;
	ws->jqueue = squeue_new(qleng);
	ws->name = strdup(name);
	ws->workerfn = workerfn;
	zassert(pthread_mutex_init(&(ws->lock),NULL));
	zassert(pthread_cond_init(&(ws->term_cond),NULL));
	zassert(pthread_attr_init(&(ws->thattr)));
#ifdef PTHREAD_STACK_MIN
	mystacksize = PTHREAD_STACK_MIN;
	if (mystacksize < 0x20000) {
		mystacksize = 0x20000;
	}
#else
	mystacksize = 0x20000;
#endif
	zassert(pthread_attr_setstacksize(&(ws->thattr),mystacksize));
	zassert(pthread_mutex_lock(&(ws->lock)));
	ws->avail = 0;
	ws->total = 0;
	ws->lastnotify = 0;
	workers_spawn_worker(ws);
	zassert(pthread_mutex_unlock(&(ws->lock)));

	return (void*)ws;
}

void workers_term(void *wsv) {
	workers *ws = (workers*)wsv;

	squeue_close(ws->jqueue);
	zassert(pthread_mutex_lock(&(ws->lock)));
	while (ws->total>0) {
		zassert(pthread_cond_wait(&(ws->term_cond),&(ws->lock)));
	}
	zassert(pthread_mutex_unlock(&(ws->lock)));
	squeue_delete(ws->jqueue);
	zassert(pthread_attr_destroy(&(ws->thattr)));
	zassert(pthread_cond_destroy(&(ws->term_cond)));
	zassert(pthread_mutex_destroy(&(ws->lock)));
	free(ws->name);
	free(ws);
}

void workers_newjob(void *wsv,void *data) {
	workers *ws = (workers*)wsv;
//	syslog(LOG_NOTICE,"enqueue job %p to queue %p",data,ws->jqueue);
	squeue_put(ws->jqueue,data);
//	syslog(LOG_NOTICE,"job enqueued");
}
