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

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <fuse_lowlevel.h>

#include "readdata.h"
#include "massert.h"
#include "clocks.h"
#include "buckets.h"

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
	uint8_t locked;
	uint16_t lflags;
	uint8_t csdataver;
	uint64_t chunkid;
	uint32_t version;
	uint8_t csdata[10*14];
	uint32_t csdatasize;
	struct _fdcacheentry *next;
} fdcacheentry;

CREATE_BUCKET_ALLOCATOR(fdcache,fdcacheentry,500)

static fdcacheentry *fdhashtab[FDCACHE_HASHSIZE];
static pthread_mutex_t hashlock[FDCACHE_HASHSIZE];

void fdcache_insert(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t lflags,uint8_t csdataver,uint64_t chunkid,uint32_t version,const uint8_t *csdata,uint32_t csdatasize) {
	uint32_t h;
	double now;
	fdcacheentry *fdce,**fdcep;
	fdcacheentry *ffdce;

	if (csdatasize>10*14) { // more than 10 copies of chunk ??? - just ignore such data
		return;
	}
	now = monotonic_seconds();
	h = FDCACHE_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	ffdce = NULL;
	fdcep = fdhashtab + h;
	while ((fdce = *fdcep)) {
		if (fdce->inode==inode && fdce->uid==ctx->uid && fdce->gid==ctx->gid && fdce->pid==ctx->pid) {
			ffdce = fdce;
			fdcep = &(fdce->next);
#ifdef HAVE___SYNC_FETCH_AND_OP
		} else if (fdce->createtime + FDCACHE_TIMEOUT < now && __sync_fetch_and_or(&(fdce->locked),0)==0) {
#else
		} else if (fdce->createtime + FDCACHE_TIMEOUT < now && fdce->locked==0) {
#endif
			*fdcep = fdce->next;
			fdcache_free(fdce);
		} else {
			fdcep = &(fdce->next);
		}
	}
	if (ffdce==NULL) {
		fdce = fdcache_malloc();
		fdce->uid = ctx->uid;
		fdce->gid = ctx->gid;
		fdce->pid = ctx->pid;
		fdce->inode = inode;
		fdce->next = fdhashtab[h];
		fdhashtab[h] = fdce;
	} else {
		fdce = ffdce;
	}
	fdce->createtime = now;
	memcpy(fdce->attr,attr,35);
	fdce->locked = 0;
	fdce->lflags = lflags;
	fdce->csdataver = csdataver;
	fdce->chunkid = chunkid;
	fdce->version = version;
	if (csdatasize>0) {
		memcpy(fdce->csdata,csdata,csdatasize);
	}
	fdce->csdatasize = csdatasize;
	zassert(pthread_mutex_unlock(hashlock+h));
}

void* fdcache_acquire(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t *lflags) {
	uint32_t h;
	double now;
	fdcacheentry *fdce;

	now = monotonic_seconds();
	h = FDCACHE_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	for (fdce = fdhashtab[h] ; fdce!=NULL ; fdce = fdce->next) {
		if (fdce->inode==inode && fdce->uid==ctx->uid && fdce->gid==ctx->gid && fdce->pid==ctx->pid && fdce->createtime + FDCACHE_TIMEOUT >= now) {
#ifdef HAVE___SYNC_FETCH_AND_OP
			__sync_fetch_and_or(&(fdce->locked),1);
			zassert(pthread_mutex_unlock(hashlock+h));
#else
			fdce->locked = 1;
#endif
			if (attr!=NULL) {
				memcpy(attr,fdce->attr,35);
			}
			if (lflags!=NULL) {
				*lflags = fdce->lflags;
			}
			return fdce;
		}
	}
	zassert(pthread_mutex_unlock(hashlock+h));
	return NULL;
}

void fdcache_release(void *vfdce) {
	fdcacheentry *fdce = (fdcacheentry*)vfdce;
#ifdef HAVE___SYNC_FETCH_AND_OP
	__sync_fetch_and_and(&(fdce->locked),0);
#else
	uint32_t h;
	fdce->locked = 0;
	h = FDCACHE_HASH(fdce->inode);
	zassert(pthread_mutex_unlock(hashlock+h));
#endif
}

void fdcache_inject_chunkdata(void *vfdce) {
	fdcacheentry *fdce = (fdcacheentry*)vfdce;
	read_inject_chunkdata(fdce->inode,0,fdce->chunkid,fdce->version,fdce->csdataver,fdce->csdata,fdce->csdatasize);
}

void fdcache_init(void) {
	uint32_t i;
	(void)fdcache_free_all; // just calm down the compiler about unused functions
	(void)fdcache_getusage;
	for (i=0 ; i<FDCACHE_HASHSIZE ; i++) {
		fdhashtab[i] = NULL;
		zassert(pthread_mutex_init(hashlock+i,NULL));
	}
}
