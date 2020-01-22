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

#include <sys/stat.h>
#include <inttypes.h>
#include <syslog.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "lwthread.h"
#include "massert.h"
#include "portable.h"
#include "clocks.h"

// #define SSTATS_DEBUG 1

#ifdef SSTATS_DEBUG
#include <stdio.h>
#endif

#ifndef HAVE___SYNC_FETCH_AND_OP
static pthread_mutex_t glock;
#endif
static uint8_t term;
static pthread_t clthread;

#define HASHSIZE 65536
#define SSTATS_TIMEOUT 60.0

static uint8_t default_attr[35]={0, 0x01 | (TYPE_DIRECTORY<<4) ,0xFF, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,0,0,0,0};

typedef struct _inode_stats {
	uint32_t inode;
	uint8_t attr[35];
//	uint8_t sustained;
	double lastrefresh;
	struct _inode_stats *next;
} inode_stats;

static inode_stats **ishash;
static pthread_mutex_t *locktab;

/*
void sstats_activate(uint32_t inode) {
	uint32_t hash;
	inode_stats *isc;

#ifdef SSTATS_DEBUG
	printf(" : sstats_activate(%"PRIu32")\n",inode);
#endif
	hash = inode % HASHSIZE;
	zassert(pthread_mutex_lock(locktab+hash));
	for (isc = ishash[hash] ; isc ; isc = isc->next) {
		if (isc->inode == inode) {
			isc->sustained = 1;
			isc->lastrefresh = monotonic_seconds();
			break;
		}
	}
	if (isc==NULL) {
		syslog(LOG_WARNING,"no sustained stats for node: %"PRIu32" - using defaults",inode);
		isc = malloc(sizeof(inode_stats));
		passert(isc);
		isc->inode = inode;
		memcpy(isc->attr,default_attr,35);
		isc->sustained = 1;
		isc->lastrefresh = monotonic_seconds();
		isc->next = ishash[hash];
		ishash[hash] = isc;
	}
	zassert(pthread_mutex_unlock(locktab+hash));
	return;
}

void sstats_deactivate(uint32_t inode) {
	uint32_t hash;
	inode_stats *isc;

#ifdef SSTATS_DEBUG
	printf(" : sstats_deactivate(%"PRIu32")\n",inode);
#endif
	hash = inode % HASHSIZE;
	zassert(pthread_mutex_lock(locktab+hash));
	for (isc = ishash[hash] ; isc ; isc = isc->next) {
		if (isc->inode == inode) {
			isc->sustained = 0;
			isc->lastrefresh = monotonic_seconds();
			break;
		}
	}
	if (isc==NULL) {
		syslog(LOG_WARNING,"no sustained stats for node: %"PRIu32" - ignored",inode);
	}
	zassert(pthread_mutex_unlock(locktab+hash));
	return;
}
*/

int sstats_get(uint32_t inode,uint8_t attr[35],uint8_t forceok) {
	uint32_t hash;
	inode_stats *isc;
//	uint8_t sustained;

#ifdef SSTATS_DEBUG
	printf(" : sstats_get(%"PRIu32",%"PRIu8")\n",inode,forceok);
#endif
	hash = inode % HASHSIZE;
	zassert(pthread_mutex_lock(locktab+hash));
	for (isc = ishash[hash] ; isc ; isc = isc->next) {
		if (isc->inode == inode) {
			memcpy(attr,isc->attr,35);
//			sustained = isc->sustained;
			isc->lastrefresh = monotonic_seconds();
			zassert(pthread_mutex_unlock(locktab+hash));
//			return (sustained || forceok)?MFS_STATUS_OK:MFS_ERROR_ENOENT;
			return MFS_STATUS_OK;
		}
	}
	if (forceok) {
		syslog(LOG_WARNING,"no sustained stats for node: %"PRIu32" - using defaults",inode);
		isc = malloc(sizeof(inode_stats));
		passert(isc);
		isc->inode = inode;
		memcpy(isc->attr,default_attr,35);
		memcpy(attr,default_attr,35);
//		isc->sustained = 0;
		isc->lastrefresh = monotonic_seconds();
		isc->next = ishash[hash];
		ishash[hash] = isc;
	}
	zassert(pthread_mutex_unlock(locktab+hash));
	return MFS_ERROR_ENOENT;
}

void sstats_set(uint32_t inode,const uint8_t attr[35],uint8_t createflag) {
	uint32_t hash;
	inode_stats *isc;

#ifdef SSTATS_DEBUG
	printf(" : sstats_set(%"PRIu32",%"PRIu8")\n",inode,createflag);
#endif
	hash = inode % HASHSIZE;
	zassert(pthread_mutex_lock(locktab+hash));
	for (isc = ishash[hash] ; isc ; isc = isc->next) {
		if (isc->inode == inode) {
			memcpy(isc->attr,attr,35);
			isc->lastrefresh = monotonic_seconds();
			zassert(pthread_mutex_unlock(locktab+hash));
			return;
		}
	}
	if (createflag) {
		isc = malloc(sizeof(inode_stats));
		passert(isc);
		isc->inode = inode;
		memcpy(isc->attr,attr,35);
//		isc->sustained = 0;
		isc->lastrefresh = monotonic_seconds();
		isc->next = ishash[hash];
		ishash[hash] = isc;
	}
	zassert(pthread_mutex_unlock(locktab+hash));
}

void* sstats_thread(void *arg) {
	uint32_t hash = 0;
	inode_stats *isc,**isp;
	double now;
	(void)arg;

	while(1) {
		now = monotonic_seconds();
		zassert(pthread_mutex_lock(locktab+hash));
		isp = ishash+hash;
		while ((isc = *isp)) {
			if (isc->lastrefresh + SSTATS_TIMEOUT < now) {
				*isp = isc->next;
				free(isc);
			} else {
				isp = &(isc->next);
			}
		}
		zassert(pthread_mutex_unlock(locktab+hash));
		portable_usleep(100000);
#ifdef HAVE___SYNC_FETCH_AND_OP
		if (__sync_fetch_and_or(&term,0)==1) {
			return NULL;
		}
#else
		zassert(pthread_mutex_lock(&glock));
		if (term==1) {
			zassert(pthread_mutex_unlock(&glock));
			return NULL;
		}
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
}

void sstats_term(void) {
	uint32_t i;
	inode_stats *isc,*isn;

#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_or(&term,1);
#else
	zassert(pthread_mutex_lock(&glock));
	term = 1;
	zassert(pthread_mutex_unlock(&glock));
#endif
	pthread_join(clthread,NULL);
#ifndef HAVE___SYNC_FETCH_AND_OP
	zassert(pthread_mutex_destroy(&glock));
#endif

	for (i=0 ; i<HASHSIZE ; i++) {
		zassert(pthread_mutex_destroy(locktab+i));
		for (isc = ishash[i] ; isc ; isc = isn) {
			isn = isc->next;
			free(isc);
		}
	}
	free(locktab);
	free(ishash);
}

void sstats_init(void) {
	uint32_t i;
	ishash = (inode_stats**)malloc(sizeof(inode_stats*)*HASHSIZE);
	passert(ishash);
	locktab = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t)*HASHSIZE);
	passert(locktab);
	for (i=0 ; i<HASHSIZE ; i++) {
		ishash[i] = NULL;
		zassert(pthread_mutex_init(locktab+i,NULL));
	}

#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_and(&term,0);
#else
	zassert(pthread_mutex_init(&glock,NULL));
	zassert(pthread_mutex_lock(&glock));
	term = 0;
	zassert(pthread_mutex_unlock(&glock));
#endif
	lwt_minthread_create(&clthread,0,sstats_thread,NULL);
}
