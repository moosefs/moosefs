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
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "fusecommon.h"

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "dirblob_name_index.h"
#include "dirblob_node_index.h"

#include "dirattrcache.h"
#include "massert.h"
#include "datapack.h"

#define NAME_INDEX_FLAG 1
#define NODE_INDEX_FLAG 2

typedef struct _dirbuff {
	uint8_t *dbuff;
	uint32_t dsize;
	struct _dirbuff *next;
} dirbuff;

typedef struct _dircache {
	struct fuse_ctx ctx;
	uint32_t parent;
	dirbuff *dbhead;
	uint8_t attrsize;
	void *name_index;
	void *node_index;
	pthread_mutex_t lock;
	struct _dircache *next,**prev;
} dircache;

static dircache *head;
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

static inline uint32_t dcache_elemcount(const uint8_t *dbuff,uint32_t dsize,uint8_t attrsize) {
	const uint8_t *ptr,*eptr;
	uint16_t enleng;
	uint32_t ret;
	ptr = dbuff;
	eptr = dbuff+dsize;
	ret=0;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+5U+attrsize<=eptr) {
			ret++;
		}
		ptr+=enleng+5U+attrsize;
	}
	return ret;
}

void* dcache_new(const struct fuse_ctx *ctx,uint32_t parent,uint8_t attrsize) {
	dircache *d;
	d = malloc(sizeof(dircache));
	d->ctx.pid = ctx->pid;
	d->ctx.uid = ctx->uid;
	d->ctx.gid = ctx->gid;
	d->parent = parent;
	d->dbhead = NULL;
	d->attrsize = attrsize;
	d->name_index = NULL;
	d->node_index = NULL;
	zassert(pthread_mutex_init(&(d->lock),NULL));
	zassert(pthread_mutex_lock(&glock));
	if (head) {
		head->prev = &(d->next);
	}
	d->next = head;
	d->prev = &head;
	head = d;
	zassert(pthread_mutex_unlock(&glock));
	return d;
}

void dcache_release(void *r) {
	dircache *d = (dircache*)r;
	dirbuff *db;

	zassert(pthread_mutex_lock(&glock));
	if (d->next) {
		d->next->prev = d->prev;
	}
	*(d->prev) = d->next;
	zassert(pthread_mutex_unlock(&glock));
	zassert(pthread_mutex_lock(&(d->lock)));
	if (d->name_index) {
		name_index_destroy(d->name_index);
	}
	if (d->node_index) {
		node_index_destroy(d->node_index);
	}
	while (d->dbhead) {
		db = d->dbhead;
		d->dbhead = db->next;
		free(db);	// do not free dbuff here - this module is not an owner of dbuff
	}
	zassert(pthread_mutex_unlock(&(d->lock)));
	zassert(pthread_mutex_destroy(&(d->lock)));
	free(d);
}

// d->lock: LOCKED->LOCKED
static inline void dcache_add_blob_to_indexes(dircache *d,dirbuff *db,uint8_t index_mask) {
	uint8_t *ptr;
	for (ptr = db->dbuff ; ptr < db->dbuff+db->dsize ; ptr = ptr + *ptr + 5 + d->attrsize) {
		if (index_mask & NAME_INDEX_FLAG) {
			name_index_add(d->name_index,ptr);
		}
		if (index_mask & NODE_INDEX_FLAG) {
			node_index_add(d->node_index,ptr);
		}
	}
}

// d->lock: LOCKED->LOCKED
static inline void dcache_make_name_index(dircache *d) {
	dirbuff *db;
	uint32_t elemcount;

	elemcount = 0;
	for (db = d->dbhead ; db!=NULL ; db=db->next) {
		elemcount += dcache_elemcount(db->dbuff,db->dsize,d->attrsize);
	}
	d->name_index = name_index_create(elemcount);
	for (db = d->dbhead ; db!=NULL ; db=db->next) {
		dcache_add_blob_to_indexes(d,db,NAME_INDEX_FLAG);
	}
}

// d->lock: LOCKED->LOCKED
static inline void dcache_make_node_index(dircache *d) {
	dirbuff *db;
	uint32_t elemcount;

	elemcount = 0;
	for (db = d->dbhead ; db!=NULL ; db=db->next) {
		elemcount += dcache_elemcount(db->dbuff,db->dsize,d->attrsize);
	}
	d->node_index = node_index_create(elemcount);
	for (db = d->dbhead ; db!=NULL ; db=db->next) {
		dcache_add_blob_to_indexes(d,db,NODE_INDEX_FLAG);
	}
}

void dcache_append(void *r,uint8_t *dbuff,uint32_t dsize) {
	dircache *d = (dircache*)r;
	dirbuff *db;

	zassert(pthread_mutex_lock(&(d->lock)));
	db = malloc(sizeof(dirbuff));
	db->dbuff = dbuff;
	db->dsize = dsize;
	db->next = d->dbhead;
	d->dbhead = db;
	dcache_add_blob_to_indexes(d,db,((d->name_index)?NAME_INDEX_FLAG:0)|((d->node_index)?NODE_INDEX_FLAG:0));
	zassert(pthread_mutex_unlock(&(d->lock)));
}

static inline void dcache_namehash_invalidate(dircache *d,uint8_t nleng,const uint8_t *name) {
	uint8_t *ptr;

	zassert(pthread_mutex_lock(&(d->lock)));
	if (d->name_index==NULL) {
		dcache_make_name_index(d);
	}
	ptr = name_index_find(d->name_index,name,nleng);
	if (ptr) {
		ptr += *ptr + 1; // skip name
		memset(ptr,0,sizeof(uint32_t)+d->attrsize);
	}
	zassert(pthread_mutex_unlock(&(d->lock)));
}

static inline uint8_t dcache_namehash_get(dircache *d,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *ptr;
	const uint8_t *rptr;
	uint8_t res;

	res = 0;
	zassert(pthread_mutex_lock(&(d->lock)));
	if (d->name_index==NULL) {
		dcache_make_name_index(d);
	}
	ptr = name_index_find(d->name_index,name,nleng);
	if (ptr) {
		rptr = ptr + *ptr + 1;
		*inode = get32bit(&rptr);
		if (*rptr) { // are attributes valid ?
			if (d->attrsize>=ATTR_RECORD_SIZE) {
				memcpy(attr,rptr,ATTR_RECORD_SIZE);
			} else {
				memcpy(attr,rptr,d->attrsize);
				memset(attr+d->attrsize,0,ATTR_RECORD_SIZE-d->attrsize);
			}
			res = 1;
		}
	}
	zassert(pthread_mutex_unlock(&(d->lock)));
	return res;
}

static inline uint8_t dcache_inodehash_get(dircache *d,uint32_t inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *ptr;
	const uint8_t *rptr;
	uint8_t res;

	res = 0;
	zassert(pthread_mutex_lock(&(d->lock)));
	if (d->node_index==NULL) {
		dcache_make_node_index(d);
	}
	ptr = node_index_find(d->node_index,inode);
	if (ptr) {
		rptr = ptr + *ptr + 5;
		if (*rptr) { // are attributes valid ?
			if (d->attrsize>=ATTR_RECORD_SIZE) {
				memcpy(attr,rptr,ATTR_RECORD_SIZE);
			} else {
				memcpy(attr,rptr,d->attrsize);
				memset(attr+d->attrsize,0,ATTR_RECORD_SIZE-d->attrsize);
			}
			res = 1;
		}
	}
	zassert(pthread_mutex_unlock(&(d->lock)));
	return res;
}

static inline uint8_t dcache_inodehash_set(dircache *d,uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *ptr;
	uint8_t *wptr;
	uint8_t res;

	res = 0;
	zassert(pthread_mutex_lock(&(d->lock)));
	if (d->node_index==NULL) {
		dcache_make_node_index(d);
	}
	ptr = node_index_find(d->node_index,inode);
	if (ptr) {
		wptr = ptr + *ptr + 5;
		if (d->attrsize<ATTR_RECORD_SIZE) {
			memcpy(wptr,attr,d->attrsize);
		} else {
			memcpy(wptr,attr,ATTR_RECORD_SIZE);
		}
		res = 1;
	}
	zassert(pthread_mutex_unlock(&(d->lock)));
	return res;
}

static inline uint8_t dcache_inodehash_invalidate_attr(dircache *d,uint32_t inode) {
	uint8_t *ptr;
	uint8_t *wptr;
	uint8_t res;

	res = 0;
	zassert(pthread_mutex_lock(&(d->lock)));
	if (d->node_index==NULL) {
		dcache_make_node_index(d);
	}
	ptr = node_index_find(d->node_index,inode);
	if (ptr) {
		wptr = ptr + *ptr + 5;
		memset(wptr,0,d->attrsize);
		res = 1;
	}
	zassert(pthread_mutex_unlock(&(d->lock)));
	return res;
}


uint8_t dcache_lookup(const struct fuse_ctx *ctx,uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	dircache *d;
	zassert(pthread_mutex_lock(&glock));
	for (d=head ; d ; d=d->next) {
		if (parent==d->parent && ctx->pid==d->ctx.pid && ctx->uid==d->ctx.uid && ctx->gid==d->ctx.gid) {
			if (dcache_namehash_get(d,nleng,name,inode,attr)) {
				zassert(pthread_mutex_unlock(&glock));
				return 1;
			}
		}
	}
	zassert(pthread_mutex_unlock(&glock));
	return 0;
}

uint8_t dcache_getattr(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	dircache *d;
	zassert(pthread_mutex_lock(&glock));
	for (d=head ; d ; d=d->next) {
		if (ctx->pid==d->ctx.pid && ctx->uid==d->ctx.uid && ctx->gid==d->ctx.gid) {
			if (dcache_inodehash_get(d,inode,attr)) {
				zassert(pthread_mutex_unlock(&glock));
				return 1;
			}
		}
	}
	zassert(pthread_mutex_unlock(&glock));
	return 0;
}

void dcache_setattr(uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE]) {
	dircache *d;
	zassert(pthread_mutex_lock(&glock));
	for (d=head ; d ; d=d->next) {
		dcache_inodehash_set(d,inode,attr);
	}
	zassert(pthread_mutex_unlock(&glock));
}

void dcache_invalidate_attr(uint32_t inode) {
	dircache *d;
	zassert(pthread_mutex_lock(&glock));
	for (d=head ; d ; d=d->next) {
		dcache_inodehash_invalidate_attr(d,inode);
	}
	zassert(pthread_mutex_unlock(&glock));
}

void dcache_invalidate_name(uint32_t parent,uint8_t nleng,const uint8_t *name) {
	dircache *d;
	zassert(pthread_mutex_lock(&glock));
	for (d=head ; d ; d=d->next) {
		if (parent==d->parent) {
			dcache_namehash_invalidate(d,nleng,name);
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}
