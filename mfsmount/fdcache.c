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

#define BUCKETS_MT_MMAP_ALLOC 1

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <fuse_lowlevel.h>

#include "MFSCommunication.h"
#include "readchunkdata.h"
#include "massert.h"
#include "clocks.h"
#include "buckets_mt.h"

#include "fdcache.h"

#define FDCACHE_HASHSIZE 1024
#define FDCACHE_HASH(inode) ((inode)%FDCACHE_HASHSIZE)
#define FDCACHE_TIMEOUT 1.0

typedef struct _fdcachechunkdata {
	uint16_t lcnt;
	uint8_t csdataver;
	uint32_t hash;
	uint64_t mfleng;
	uint64_t chunkid;
	uint32_t version;
	uint32_t csdatasize;
	uint8_t csdata[10*14];
} fdcachechunkdata;

typedef struct _fdcacheentry {
	double createtime;
	uid_t uid;
	gid_t gid;
	pid_t pid;
	uint32_t inode;
	uint8_t attr[35];
	uint16_t lflags;
	fdcachechunkdata *chunkdata;
	struct _fdcacheentry *next;
} fdcacheentry;

CREATE_BUCKET_MT_ALLOCATOR(fdcachecd,fdcachechunkdata,500)
CREATE_BUCKET_MT_ALLOCATOR(fdcachee,fdcacheentry,500)

static fdcacheentry *fdhashtab[FDCACHE_HASHSIZE];
static pthread_mutex_t hashlock[FDCACHE_HASHSIZE];

static inline fdcachechunkdata* fdcache_chunkdata_new(uint32_t hash,uint8_t csdataver,uint64_t mfleng,uint64_t chunkid,uint32_t version,const uint8_t *csdata,uint32_t csdatasize) {
	fdcachechunkdata *fdccd;

	if (csdatasize>10*14) { // more than 10 copies of chunk ??? - just ignore such data
		return NULL;
	}

	fdccd = fdcachecd_malloc();
	fdccd->lcnt = 1;
	fdccd->csdataver = csdataver;
	fdccd->hash = hash;
	fdccd->mfleng = mfleng;
	fdccd->chunkid = chunkid;
	fdccd->version = version;
	fdccd->csdatasize = csdatasize;
	memcpy(fdccd->csdata,csdata,csdatasize);
	return fdccd;
}

static inline void fdcache_chunkdata_unref(fdcachechunkdata *fdccd) {
	sassert(fdccd->lcnt>0);
	fdccd->lcnt--;
	if (fdccd->lcnt==0) {
		fdcachecd_free(fdccd);
	}
}

void fdcache_insert(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t lflags,uint8_t csdataver,uint64_t mfleng,uint64_t chunkid,uint32_t version,const uint8_t *csdata,uint32_t csdatasize) {
	uint32_t h;
	double now;
	uint8_t f;
	fdcacheentry *fdce,**fdcep;

	now = monotonic_seconds();
	h = FDCACHE_HASH(inode);
	zassert(pthread_mutex_lock(hashlock+h));
	fdcep = fdhashtab + h;
	f = 0;
	while ((fdce = *fdcep)) {
		if (fdce->createtime + FDCACHE_TIMEOUT < now) {
			*fdcep = fdce->next;
			if (fdce->chunkdata) {
				fdcache_chunkdata_unref(fdce->chunkdata);
			}
			fdcachee_free(fdce);
		} else {
			if (fdce->inode==inode && fdce->uid==ctx->uid && fdce->gid==ctx->gid && fdce->pid==ctx->pid) {
				if (fdce->chunkdata) {
					fdcache_chunkdata_unref(fdce->chunkdata);
				}
				if (lflags & LOOKUP_CHUNK_ZERO_DATA) {
					fdce->chunkdata = fdcache_chunkdata_new(h,csdataver,mfleng,chunkid,version,csdata,csdatasize);
				} else {
					fdce->chunkdata = NULL;
				}
				fdce->createtime = now;
				memcpy(fdce->attr,attr,35);
				fdce->lflags = lflags;
				f = 1;
			}
			fdcep = &(fdce->next);
		}
	}
	if (f==0) {
		fdce = fdcachee_malloc();
		fdce->uid = ctx->uid;
		fdce->gid = ctx->gid;
		fdce->pid = ctx->pid;
		fdce->inode = inode;
		fdce->createtime = now;
		memcpy(fdce->attr,attr,35);
		fdce->lflags = lflags;
		if (lflags & LOOKUP_CHUNK_ZERO_DATA) {
			fdce->chunkdata = fdcache_chunkdata_new(h,csdataver,mfleng,chunkid,version,csdata,csdatasize);
		} else {
			fdce->chunkdata = NULL;
		}
		fdce->next = fdhashtab[h];
		fdhashtab[h] = fdce;
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

void* fdcache_acquire(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t *lflags,uint8_t *found) {
	uint32_t h;
	double now;
	fdcacheentry *fdce;
	fdcachechunkdata *fdccd;

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
			fdccd = fdce->chunkdata;
			if (fdccd!=NULL) {
				fdccd->lcnt++;
			}
			zassert(pthread_mutex_unlock(hashlock+h));
			*found = 1;
			return fdccd;
		}
	}
	zassert(pthread_mutex_unlock(hashlock+h));
	*found = 0;
	return NULL;
}

void fdcache_release(void *vfdccd) {
	fdcachechunkdata *fdccd = (fdcachechunkdata*)vfdccd;
	uint32_t h;

	if (fdccd!=NULL) {
		h = fdccd->hash;
		zassert(pthread_mutex_lock(hashlock+h));
		fdcache_chunkdata_unref(fdccd);
		zassert(pthread_mutex_unlock(hashlock+h));
	}
}

void fdcache_inject_chunkdata(uint32_t inode,void *vfdccd) {
	fdcachechunkdata *fdccd = (fdcachechunkdata*)vfdccd;
	if (fdccd!=NULL) {
		read_chunkdata_inject(inode,0,fdccd->mfleng,fdccd->chunkid,fdccd->version,fdccd->csdataver,fdccd->csdata,fdccd->csdatasize);
	}
}

void fdcache_init(void) {
	uint32_t i;

	(void)fdcachee_free_all; // just calm down the compiler about unused functions
	(void)fdcachee_getusage;
	(void)fdcachecd_free_all;
	(void)fdcachecd_getusage;

	for (i=0 ; i<FDCACHE_HASHSIZE ; i++) {
		fdhashtab[i] = NULL;
		zassert(pthread_mutex_init(hashlock+i,NULL));
	}
}
