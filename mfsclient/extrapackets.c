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

#include <inttypes.h>
#include <stdlib.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "massert.h"
#include "lwthread.h"
#ifdef MFSMOUNT
#include "mfs_fuse.h"
#include "fdcache.h"
#endif
#include "chunksdatacache.h"
#include "readdata.h"

#define MAX_UNUSED_CNT 100

enum {CHUNK_CHANGED,FLENG_CHANGED,EXIT};

typedef struct _extra_packets {
	uint32_t cmd;
	uint32_t inode;
	uint32_t chindx;
	uint64_t chunkid;
	uint32_t version;
	uint64_t fleng;
	uint8_t truncflag;
	struct _extra_packets *next;
} extra_packets;

static extra_packets *ep_head,**ep_tail;
static extra_packets *ep_unused;
static uint32_t ep_unused_cnt;
static pthread_mutex_t ep_lock;
static pthread_cond_t ep_cond;
static pthread_t ep_worker;

static inline extra_packets* ep_get_packet(void) {
	extra_packets *ep;
	if (ep_unused!=NULL) {
		ep = ep_unused;
		ep_unused = ep_unused->next;
		ep_unused_cnt--;
	} else {
		ep = malloc(sizeof(extra_packets));
		passert(ep);
	}
	return ep;
}

static inline void ep_append_packet(extra_packets *ep) {
	uint8_t wakeup;
	wakeup = (ep_head==NULL)?1:0;
	ep->next = NULL;
	*ep_tail = ep;
	ep_tail = &(ep->next);
	if (wakeup) {
		pthread_cond_signal(&ep_cond);
	}
}

static inline void ep_free_packet(extra_packets *ep) {
	if (ep_unused_cnt>=MAX_UNUSED_CNT) {
		free(ep);
	} else {
		ep->next = ep_unused;
		ep_unused = ep;
		ep_unused_cnt++;
	}
}

void* ep_thread(void *arg) {
	extra_packets *ep = NULL;
	zassert(pthread_mutex_lock(&ep_lock));
	while (1) {
		while (ep_head==NULL) {
			zassert(pthread_cond_wait(&ep_cond,&ep_lock));
		}
		ep = ep_head;
		ep_head = ep->next;
		if (ep_head==NULL) {
			ep_tail = &ep_head;
		}
		zassert(pthread_mutex_unlock(&ep_lock));
		switch (ep->cmd) {
			case CHUNK_CHANGED:
				chunksdatacache_change(ep->inode,ep->chindx,ep->chunkid,ep->version);
				if (ep->truncflag) {
					chunksdatacache_clear_inode(ep->inode,ep->chindx+1);
					read_inode_clear_cache(ep->inode,(uint64_t)(ep->chindx)*MFSCHUNKSIZE,0);
					read_inode_set_length_passive(ep->inode,ep->fleng);
#ifdef MFSMOUNT
					fdcache_invalidate(ep->inode);
					mfs_inode_change_fleng(ep->inode,ep->fleng);
					mfs_inode_clear_cache(ep->inode,(uint64_t)(ep->chindx)*MFSCHUNKSIZE,0);
#endif
				} else {
					read_inode_clear_cache(ep->inode,(uint64_t)(ep->chindx)*MFSCHUNKSIZE,MFSCHUNKSIZE);
#ifdef MFSMOUNT
					fdcache_invalidate(ep->inode);
					mfs_inode_clear_cache(ep->inode,(uint64_t)(ep->chindx)*MFSCHUNKSIZE,MFSCHUNKSIZE);
#endif
				}
				break;
			case FLENG_CHANGED:
				read_inode_set_length_passive(ep->inode,ep->fleng);
#ifdef MFSMOUNT
				fdcache_invalidate(ep->inode);
				mfs_inode_clear_cache(ep->inode,ep->fleng,0);
				mfs_inode_change_fleng(ep->inode,ep->fleng);
#endif
				break;
			default:
				free(ep);
				return arg;
		}
		zassert(pthread_mutex_lock(&ep_lock));
		ep_free_packet(ep);
	}
	zassert(pthread_mutex_unlock(&ep_lock)); // pro forma - unreachable
	return arg; // pro forma - unreachable
}

void ep_chunk_has_changed(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version,uint64_t fleng,uint8_t truncflag) {
	extra_packets *ep;
	zassert(pthread_mutex_lock(&ep_lock));
	ep = ep_get_packet();
	ep->cmd = CHUNK_CHANGED;
	ep->inode = inode;
	ep->chindx = chindx;
	ep->chunkid = chunkid;
	ep->version = version;
	ep->fleng = fleng;
	ep->truncflag = truncflag;
	ep_append_packet(ep);
	zassert(pthread_mutex_unlock(&ep_lock));
}

void ep_fleng_has_changed(uint32_t inode,uint64_t fleng) {
	extra_packets *ep;
	zassert(pthread_mutex_lock(&ep_lock));
	ep = ep_get_packet();
	ep->cmd = FLENG_CHANGED;
	ep->inode = inode;
	ep->fleng = fleng;
	ep_append_packet(ep);
	zassert(pthread_mutex_unlock(&ep_lock));
}

void ep_term(void) {
	extra_packets *ep,*epn;
	zassert(pthread_mutex_lock(&ep_lock));
	ep = ep_get_packet();
	ep->cmd = EXIT;
	ep_append_packet(ep);
	zassert(pthread_mutex_unlock(&ep_lock));
	pthread_join(ep_worker,NULL);
	for (ep = ep_head ; ep ; ep = epn) {
		epn = ep->next;
		free(ep);
	}
	for (ep = ep_unused ; ep ; ep = epn) {
		epn = ep->next;
		free(ep);
	}
	zassert(pthread_cond_destroy(&ep_cond));
	zassert(pthread_mutex_destroy(&ep_lock));
}

void ep_init(void) {
	ep_head = NULL;
	ep_tail = &(ep_head);
	ep_unused = NULL;
	ep_unused_cnt = 0;
	zassert(pthread_mutex_init(&ep_lock,NULL));
	zassert(pthread_cond_init(&ep_cond,NULL));
	lwt_minthread_create(&ep_worker,0,ep_thread,NULL);
}
