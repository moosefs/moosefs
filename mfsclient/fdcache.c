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

#define BUCKETS_MT_MMAP_ALLOC 1

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>

#include "fusecommon.h"

#include "MFSCommunication.h"
#include "chunksdatacache.h"
#include "massert.h"
#include "clocks.h"
#include "buckets_mt.h"

#include "fdcache.h"

#define FDCACHE_HASHSIZE 1024
#define FDCACHE_HASH(inode) ((inode)%FDCACHE_HASHSIZE)
#define FDCACHE_TIMEOUT 1.0

typedef struct _fdcacheentry {
	double createtime;
	uid_t uid;
	gid_t gid;
	pid_t pid;
	uint32_t inode;
	uint8_t attr[35];
	uint16_t lflags;
	uint8_t csdataver;
	uint64_t chunkid;
	uint32_t version;
	uint32_t csdatasize;
	uint8_t csdata[10*14];
	struct _fdcacheentry *next;
} fdcacheentry;

CREATE_BUCKET_MT_ALLOCATOR(fdcachee,fdcacheentry,500)

static fdcacheentry *fdhashtab[FDCACHE_HASHSIZE];
static pthread_mutex_t hashlock[FDCACHE_HASHSIZE];

void fdcache_insert(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t lflags,uint8_t csdataver,uint64_t chunkid,uint32_t version,const uint8_t *csdata,uint32_t csdatasize) {
	uint32_t h;
	double now;
	fdcacheentry *f;
	fdcacheentry *fdce,**fdcep;

	now = monotonic_seconds();
	h = FDCACHE_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	fdcep = fdhashtab + h;
	f = NULL;
	while ((fdce = *fdcep)) {
		if (fdce->createtime + FDCACHE_TIMEOUT < now) {
			*fdcep = fdce->next;
			fdcachee_free(fdce);
		} else {
			if (fdce->inode==inode && fdce->uid==ctx->uid && fdce->gid==ctx->gid && fdce->pid==ctx->pid) {
				if (f==NULL) {
					f = fdce;
					fdcep = &(fdce->next);
				} else {
					*fdcep = fdce->next;
					fdcachee_free(fdce);
				}
			} else {
				fdcep = &(fdce->next);
			}
		}
	}
	if (f==NULL) {
		fdce = fdcachee_malloc();
		fdce->uid = ctx->uid;
		fdce->gid = ctx->gid;
		fdce->pid = ctx->pid;
		fdce->inode = inode;
		fdce->next = fdhashtab[h];
		fdhashtab[h] = fdce;
	} else {
		fdce = f;
	}
	fdce->createtime = now;
	memcpy(fdce->attr,attr,35);
	fdce->lflags = lflags;
	if ((lflags & LOOKUP_CHUNK_ZERO_DATA) && csdatasize<=(10*14)) {
		fdce->csdataver = csdataver;
		fdce->chunkid = chunkid;
		fdce->version = version;
		fdce->csdatasize = csdatasize;
		memcpy(fdce->csdata,csdata,csdatasize);
	} else {
		fdce->lflags &= ~LOOKUP_CHUNK_ZERO_DATA;
		fdce->csdataver = 0;
		fdce->chunkid = 0;
		fdce->version = 0;
		fdce->csdatasize = 0;
	}
	zassert(pthread_mutex_unlock(hashlock+h));
}

void fdcache_invalidate(uint32_t inode) {
	uint32_t h;
	fdcacheentry *fdce,**fdcep;

	h = FDCACHE_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	fdcep = fdhashtab + h;
	while ((fdce = *fdcep)) {
		if (fdce->inode==inode) {
			*fdcep = fdce->next;
			fdcachee_free(fdce);
		} else {
			fdcep = &(fdce->next);
		}
	}
	zassert(pthread_mutex_unlock(hashlock+h));
}

uint8_t fdcache_find(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t *lflags) {
	uint32_t h;
	double now;
	fdcacheentry *fdce;
	now = monotonic_seconds();
	h = FDCACHE_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	for (fdce = fdhashtab[h] ; fdce!=NULL ; fdce = fdce->next) {
		if (fdce->inode==inode && fdce->uid==ctx->uid && fdce->gid==ctx->gid && fdce->pid==ctx->pid && fdce->createtime + FDCACHE_TIMEOUT >= now) {
			if (attr!=NULL) {
				memcpy(attr,fdce->attr,35);
			}
			if (lflags!=NULL) {
				*lflags = fdce->lflags;
			}
			zassert(pthread_mutex_unlock(hashlock+h));
			return 1;
		}
	}
	zassert(pthread_mutex_unlock(hashlock+h));
	return 0;
}

void* fdcache_acquire(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t *lflags) {
	uint32_t h;
	double now;
	fdcacheentry *fdce,**fdcep;

	now = monotonic_seconds();
	h = FDCACHE_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	fdcep = fdhashtab + h;
	while ((fdce = *fdcep)) {
		if (fdce->inode==inode && fdce->uid==ctx->uid && fdce->gid==ctx->gid && fdce->pid==ctx->pid && fdce->createtime + FDCACHE_TIMEOUT >= now) {
			if (attr!=NULL) {
				memcpy(attr,fdce->attr,35);
			}
			if (lflags!=NULL) {
				*lflags = fdce->lflags;
			}
			*fdcep = fdce->next;
			zassert(pthread_mutex_unlock(hashlock+h));
			return fdce;
		} else {
			fdcep = &(fdce->next);
		}
	}
	zassert(pthread_mutex_unlock(hashlock+h));
	return NULL;
}

void fdcache_release(void *vfdce) {
	fdcacheentry *fdce = (fdcacheentry*)vfdce;

	if (fdce!=NULL) {
		fdcachee_free(fdce);
	}
}

void fdcache_inject_chunkdata(void *vfdce) {
	fdcacheentry *fdce = (fdcacheentry*)vfdce;

	if ((fdce->lflags) & LOOKUP_CHUNK_ZERO_DATA) {
		chunksdatacache_insert(fdce->inode,0,fdce->chunkid,fdce->version,fdce->csdataver,fdce->csdata,fdce->csdatasize);
	}
}

void fdcache_init(void) {
	uint32_t i;

	(void)fdcachee_free_all; // just calm down the compiler about unused functions
	(void)fdcachee_getusage;

	for (i=0 ; i<FDCACHE_HASHSIZE ; i++) {
		fdhashtab[i] = NULL;
		zassert(pthread_mutex_init(hashlock+i,NULL));
	}
}
