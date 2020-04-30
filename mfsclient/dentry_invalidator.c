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

#if defined(HAVE_CONFIG_H)
#  include "config.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <inttypes.h>

#include "massert.h"
#include "mastercomm.h"
#include "mfs_fuse.h"
#include "clocks.h"
#include "portable.h"
#include "lwthread.h"

#define HASH_SIZE 0x8000
#define HASH_MASK 0x7FFF
#define MAX_ELEMENTS 10000
#define MIN_TIMEOUT 30.0

typedef struct _dinval_element {
	uint32_t parent;
	uint8_t nleng;
	uint8_t *name;
	uint32_t inode;
	double timestamp;
	struct _dinval_element *queue_next,**queue_prev;
	struct _dinval_element *hash_next,**hash_prev;
} dinval_element;

static dinval_element **hashtab;
static uint32_t elementcnt;
static dinval_element *queue_head,**queue_prev;
static double main_timeout;

static pthread_mutex_t glock;

static inline uint32_t dinval_calc_hash(uint32_t parent,uint8_t nleng,const uint8_t *name) {
	uint32_t hash=5381;
	while (nleng>0) {
		hash = ((hash<<5)+hash)^(*name);
		name++;
		nleng--;
	}
	hash ^= parent;
	return hash & HASH_MASK;
}

static inline uint32_t dinval_calc_elem_hash(dinval_element *dielem) {
	return dinval_calc_hash(dielem->parent,dielem->nleng,dielem->name);
}

static inline void dinval_queue_detach(dinval_element *dielem) {
	if (dielem->queue_next!=NULL) {
		dielem->queue_next->queue_prev = dielem->queue_prev;
	} else {
		queue_prev = dielem->queue_prev;
	}
	*(dielem->queue_prev) = dielem->queue_next;
}

static inline void dinval_element_detach(dinval_element *dielem) {
	if (dielem->queue_next!=NULL) {
		dielem->queue_next->queue_prev = dielem->queue_prev;
	} else {
		queue_prev = dielem->queue_prev;
	}
	*(dielem->queue_prev) = dielem->queue_next;
	if (dielem->hash_next!=NULL) {
		dielem->hash_next->hash_prev = dielem->hash_prev;
	}
	*(dielem->hash_prev) = dielem->hash_next;
}

static inline void dinval_queue_attach(dinval_element *dielem) {
	// queue
	dielem->queue_next = NULL;
	dielem->queue_prev = queue_prev;
	*(queue_prev) = dielem;
	queue_prev = &(dielem->queue_next);
	// timestamp
	dielem->timestamp = monotonic_seconds();
}

static inline void dinval_element_attach(uint32_t hashhint,dinval_element *dielem) {
	uint32_t hash;

	if (hashhint<HASH_SIZE) {
		hash = hashhint;
	} else {
		hash = dinval_calc_elem_hash(dielem);
	}
	// queue
	dielem->queue_next = NULL;
	dielem->queue_prev = queue_prev;
	*(queue_prev) = dielem;
	queue_prev = &(dielem->queue_next);
	// hash
	dielem->hash_next = hashtab[hash];
	if (dielem->hash_next!=NULL) {
		dielem->hash_next->hash_prev = &(dielem->hash_next);
	}
	dielem->hash_prev = hashtab+hash;
	hashtab[hash] = dielem;
	// timestamp
	dielem->timestamp = monotonic_seconds();
}

static inline dinval_element* dinval_element_find(uint32_t *hashhint,uint32_t parent,uint8_t nleng,const uint8_t *name) {
	uint32_t hash;
	dinval_element *dielem;

	hash = dinval_calc_hash(parent,nleng,name);
	for (dielem = hashtab[hash] ; dielem!=NULL ; dielem=dielem->hash_next) {
		if (dielem->parent==parent && dielem->nleng==nleng && memcmp(dielem->name,name,nleng)==0) {
			return dielem;
		}
	}
	if (hashhint!=NULL) {
		*hashhint = hash;
	}
	return NULL;
}

// add or refresh time if exists (lookup)
void dinval_add(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t inode) {
	dinval_element *dielem;
	uint32_t hashhint;

	pthread_mutex_lock(&glock);
	dielem = dinval_element_find(&hashhint,parent,nleng,name);
	if (dielem) {
		dinval_queue_detach(dielem);
		dielem->inode = inode;
		dinval_queue_attach(dielem);
	} else {
		dielem = malloc(sizeof(dinval_element));
		passert(dielem);
		dielem->parent = parent;
		dielem->nleng = nleng;
		dielem->name = malloc(nleng+1);
		passert(dielem->name);
		memcpy(dielem->name,name,nleng);
		dielem->name[nleng]=0; // we use it to print
		dielem->inode = inode;
		dinval_element_attach(hashhint,dielem);
		elementcnt++;
	}
	pthread_mutex_unlock(&glock);
}

// (rmdir)
void dinval_remove(uint32_t parent,uint8_t nleng,const uint8_t *name) {
	dinval_element *dielem;
	pthread_mutex_lock(&glock);
	dielem = dinval_element_find(NULL,parent,nleng,name);
	if (dielem) {
		dinval_element_detach(dielem);
		free(dielem->name);
		free(dielem);
		elementcnt--;
	}
	pthread_mutex_unlock(&glock);
}

void* dinval_invalthread(void* arg) {
	double timeout;
	double now;
	uint32_t i;
	dinval_element *dielem;

	timeout = main_timeout;
	while (1) {
		now = monotonic_seconds();
		pthread_mutex_lock(&glock);
		i = 100;
		while (i>0 && ((elementcnt>MAX_ELEMENTS && queue_head->timestamp+MIN_TIMEOUT<now) || (queue_head!=NULL && queue_head->timestamp+timeout<now))) {
			dielem = queue_head;
			if (fs_isopen(dielem->inode)) { // can't invalidate inodes that are still open
				dinval_queue_detach(dielem);
				dinval_queue_attach(dielem);
			} else {
				dinval_element_detach(dielem);
				pthread_mutex_unlock(&glock);
				mfs_dentry_invalidate(dielem->parent,dielem->nleng,(const char*)(dielem->name));
				pthread_mutex_lock(&glock);
				free(dielem->name);
				free(dielem);
				elementcnt--;
			}
			i--;
		}
		pthread_mutex_unlock(&glock);
		portable_usleep(10000);
	}
	return arg;
}

void dinval_init(double timeout) {
	uint32_t i;
	pthread_t th;

	hashtab = malloc(sizeof(dinval_element*)*HASH_SIZE);
	for (i=0 ; i<HASH_SIZE ; i++) {
		hashtab[i] = NULL;
	}
	elementcnt = 0;
	queue_head = NULL;
	queue_prev = &queue_head;
	if (timeout>MIN_TIMEOUT) { // we need time for CWD check (delay in fs_isopen)
		main_timeout = timeout;
	} else {
		main_timeout = MIN_TIMEOUT;
	}
	zassert(pthread_mutex_init(&glock,NULL));
	lwt_minthread_create(&th,1,dinval_invalthread,NULL);
}
