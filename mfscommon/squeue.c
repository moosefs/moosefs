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

#include <stdlib.h>
#include <pthread.h>
#include <inttypes.h>
#include <errno.h>

#include "massert.h"

typedef struct _qentry {
	void *data;
	struct _qentry *next;
} qentry;

typedef struct _queue {
	qentry *head,**tail;
	uint32_t elements;
	uint32_t maxelements;
	uint32_t closed;
	pthread_cond_t waitfree,waitfull;
	pthread_mutex_t lock;
} queue;

void* squeue_new(uint32_t length) {
	queue *q;
	q = (queue*)malloc(sizeof(queue));
	passert(q);
	q->head = NULL;
	q->tail = &(q->head);
	q->elements = 0;
	q->maxelements = length;
	q->closed = 0;
	if (length) {
		zassert(pthread_cond_init(&(q->waitfull),NULL));
	}
	zassert(pthread_cond_init(&(q->waitfree),NULL));
	zassert(pthread_mutex_init(&(q->lock),NULL));
	return q;
}

void squeue_delete(void *que) {
	queue *q = (queue*)que;
	qentry *qe,*qen;
	zassert(pthread_mutex_lock(&(q->lock)));
	for (qe = q->head ; qe ; qe = qen) {
		qen = qe->next;
		free(qe->data);
		free(qe);
	}
	zassert(pthread_mutex_unlock(&(q->lock)));
	zassert(pthread_mutex_destroy(&(q->lock)));
	zassert(pthread_cond_destroy(&(q->waitfree)));
	if (q->maxelements) {
		zassert(pthread_cond_destroy(&(q->waitfull)));
	}
	free(q);
}

void squeue_close(void *que) {
	queue *q = (queue*)que;
	zassert(pthread_mutex_lock(&(q->lock)));
	q->closed = 1;
	zassert(pthread_cond_broadcast(&(q->waitfree)));
	if (q->maxelements) {
		zassert(pthread_cond_broadcast(&(q->waitfull)));
	}
	zassert(pthread_mutex_unlock(&(q->lock)));
}

int squeue_isempty(void *que) {
	queue *q = (queue*)que;
	int r;
	zassert(pthread_mutex_lock(&(q->lock)));
	r=(q->elements==0)?1:0;
	zassert(pthread_mutex_unlock(&(q->lock)));
	return r;
}

uint32_t squeue_elements(void *que) {
	queue *q = (queue*)que;
	uint32_t r;
	zassert(pthread_mutex_lock(&(q->lock)));
	r=q->elements;
	zassert(pthread_mutex_unlock(&(q->lock)));
	return r;
}

int squeue_isfull(void *que) {
	queue *q = (queue*)que;
	int r;
	zassert(pthread_mutex_lock(&(q->lock)));
	r = (q->maxelements>0 && q->maxelements<=q->elements)?1:0;
	zassert(pthread_mutex_unlock(&(q->lock)));
	return r;
}

uint32_t squeue_sizeleft(void *que) {
	queue *q = (queue*)que;
	uint32_t r;
	zassert(pthread_mutex_lock(&(q->lock)));
	if (q->maxelements>0) {
		r = q->maxelements-q->elements;
	} else {
		r = 0xFFFFFFFF;
	}
	zassert(pthread_mutex_unlock(&(q->lock)));
	return r;
}

int squeue_put(void *que,void *data) {
	queue *q = (queue*)que;
	qentry *qe;
	qe = malloc(sizeof(qentry));
	passert(qe);
	qe->data = data;
	qe->next = NULL;
	zassert(pthread_mutex_lock(&(q->lock)));
	if (q->maxelements) {
		while (q->elements>=q->maxelements && q->closed==0) {
			zassert(pthread_cond_wait(&(q->waitfull),&(q->lock)));
		}
		if (q->closed) {
			zassert(pthread_mutex_unlock(&(q->lock)));
			free(qe);
			errno = EIO;
			return -1;
		}
	}
	q->elements++;
	*(q->tail) = qe;
	q->tail = &(qe->next);
	zassert(pthread_cond_signal(&(q->waitfree)));
	zassert(pthread_mutex_unlock(&(q->lock)));
	return 0;
}

int squeue_tryput(void *que,void *data) {
	queue *q = (queue*)que;
	qentry *qe;
	zassert(pthread_mutex_lock(&(q->lock)));
	if (q->maxelements) {
		if (q->elements>=q->maxelements) {
			zassert(pthread_mutex_unlock(&(q->lock)));
			errno = EBUSY;
			return -1;
		}
	}
	qe = malloc(sizeof(qentry));
	passert(qe);
	qe->data = data;
	qe->next = NULL;
	q->elements++;
	*(q->tail) = qe;
	q->tail = &(qe->next);
	zassert(pthread_cond_signal(&(q->waitfree)));
	zassert(pthread_mutex_unlock(&(q->lock)));
	return 0;
}

int squeue_get(void *que,void **data) {
	queue *q = (queue*)que;
	qentry *qe;
	zassert(pthread_mutex_lock(&(q->lock)));
	while (q->elements==0 && q->closed==0) {
		zassert(pthread_cond_wait(&(q->waitfree),&(q->lock)));
	}
	if (q->closed) {
		zassert(pthread_mutex_unlock(&(q->lock)));
		if (data) {
			*data=NULL;
		}
		errno = EIO;
		return -1;
	}
	qe = q->head;
	q->head = qe->next;
	if (q->head==NULL) {
		q->tail = &(q->head);
	}
	q->elements--;
	if (q->maxelements) {
		zassert(pthread_cond_signal(&(q->waitfull)));
	}
	zassert(pthread_mutex_unlock(&(q->lock)));
	if (data) {
		*data = qe->data;
	}
	free(qe);
	return 0;
}

int squeue_tryget(void *que,void **data) {
	queue *q = (queue*)que;
	qentry *qe;
	zassert(pthread_mutex_lock(&(q->lock)));
	if (q->elements==0) {
		zassert(pthread_mutex_unlock(&(q->lock)));
		if (data) {
			*data=NULL;
		}
		errno = EBUSY;
		return -1;
	}
	qe = q->head;
	q->head = qe->next;
	if (q->head==NULL) {
		q->tail = &(q->head);
	}
	q->elements--;
	if (q->maxelements) {
		zassert(pthread_cond_signal(&(q->waitfull)));
	}
	zassert(pthread_mutex_unlock(&(q->lock)));
	if (data) {
		*data = qe->data;
	}
	free(qe);
	return 0;
}
