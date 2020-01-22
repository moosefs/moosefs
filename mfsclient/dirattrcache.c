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

#include "fusecommon.h"

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "dirattrcache.h"
#include "massert.h"
#include "datapack.h"

typedef struct _dircache {
	struct fuse_ctx ctx;
	uint32_t parent;
	const uint8_t *dbuff;
	uint32_t dsize;
	uint32_t hashsize;
	uint8_t attrsize;
	const uint8_t **namehashtab;
	const uint8_t **inodehashtab;
	struct _dircache *next,**prev;
} dircache;

static dircache *head;
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

static inline uint32_t dcache_hash(const uint8_t *name,uint8_t nleng) {
	uint32_t hash=5381;
	while (nleng>0) {
		hash = ((hash<<5)+hash)^(*name);
		name++;
		nleng--;
	}
	return hash;
}

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

static inline void dcache_calchashsize(dircache *d) {
	uint32_t cnt = dcache_elemcount(d->dbuff,d->dsize,d->attrsize);
	d->hashsize = 1;
	cnt = (cnt*3)/2;
	while (cnt) {
		d->hashsize<<=1;
		cnt>>=1;
	}
}

void dcache_makenamehash(dircache *d) {
	const uint8_t *ptr,*eptr;
	uint16_t enleng;
	uint32_t hash,disp;
	uint32_t hashmask;

	if (d->hashsize==0) {
		dcache_calchashsize(d);
	}
	hashmask = d->hashsize-1;
	d->namehashtab = malloc(sizeof(uint8_t*)*d->hashsize);
	memset(d->namehashtab,0,sizeof(uint8_t*)*d->hashsize);

	ptr = d->dbuff;
	eptr = d->dbuff+d->dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+5U+d->attrsize<=eptr) {
			hash = dcache_hash(ptr+1,enleng);
			disp = ((hash*0x53B23891)&hashmask)|1;
			while (d->namehashtab[hash&hashmask]) {
				hash+=disp;
			}
			d->namehashtab[hash&hashmask]=ptr;
		}
		ptr+=enleng+5U+d->attrsize;
	}
}

void dcache_makeinodehash(dircache *d) {
	const uint8_t *iptr,*ptr,*eptr;
	uint16_t enleng;
	uint32_t hash,disp;
	uint32_t hashmask;

	if (d->hashsize==0) {
		dcache_calchashsize(d);
	}
	hashmask = d->hashsize-1;
	d->inodehashtab = malloc(sizeof(uint8_t*)*d->hashsize);
	memset(d->inodehashtab,0,sizeof(uint8_t*)*d->hashsize);

	ptr = d->dbuff;
	eptr = d->dbuff+d->dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+5U+d->attrsize<=eptr) {
			iptr = ptr+1U+enleng;
			hash = get32bit(&iptr);
			disp = ((hash*0x53B23891)&hashmask)|1;
			hash *= 0xB28E457D;
			while (d->inodehashtab[hash&hashmask]) {
				hash+=disp;
			}
			d->inodehashtab[hash&hashmask]=ptr+1U+enleng;
		}
		ptr+=enleng+5U+d->attrsize;
	}
}

void* dcache_new(const struct fuse_ctx *ctx,uint32_t parent,const uint8_t *dbuff,uint32_t dsize,uint8_t attrsize) {
	dircache *d;
	d = malloc(sizeof(dircache));
	d->ctx.pid = ctx->pid;
	d->ctx.uid = ctx->uid;
	d->ctx.gid = ctx->gid;
	d->parent = parent;
	d->dbuff = dbuff;
	d->dsize = dsize;
	d->attrsize = attrsize;
	d->hashsize = 0;
	d->namehashtab = NULL;
	d->inodehashtab = NULL;
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
	zassert(pthread_mutex_lock(&glock));
	if (d->next) {
		d->next->prev = d->prev;
	}
	*(d->prev) = d->next;
	zassert(pthread_mutex_unlock(&glock));
	if (d->namehashtab) {
		free(d->namehashtab);
	}
	if (d->inodehashtab) {
		free(d->inodehashtab);
	}
	free(d);
}

static inline void dcache_namehash_invalidate(dircache *d,uint8_t nleng,const uint8_t *name) {
	uint32_t hash,disp,hashmask;
	const uint8_t *ptr;

	if (d->namehashtab==NULL) {
		dcache_makenamehash(d);
	}
	hashmask = d->hashsize-1;
	hash = dcache_hash(name,nleng);
	disp = ((hash*0x53B23891)&hashmask)|1;
	while ((ptr=d->namehashtab[hash&hashmask])) {
		if (*ptr==nleng && memcmp(ptr+1,name,nleng)==0) {
			ptr+=1U+(uint16_t)nleng;
			memset((uint8_t*)ptr,0,sizeof(uint32_t)+d->attrsize);
			return;
		}
		hash+=disp;
	}
}

static inline uint8_t dcache_namehash_get(dircache *d,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint32_t hash,disp,hashmask;
	const uint8_t *ptr;

	if (d->namehashtab==NULL) {
		dcache_makenamehash(d);
	}
	hashmask = d->hashsize-1;
	hash = dcache_hash(name,nleng);
	disp = ((hash*0x53B23891)&hashmask)|1;
	while ((ptr=d->namehashtab[hash&hashmask])) {
		if (*ptr==nleng && memcmp(ptr+1,name,nleng)==0) {
			ptr+=1U+(uint16_t)nleng;
			*inode = get32bit(&ptr);
			if (*ptr) { // are attributes valid ?
				if (d->attrsize>=ATTR_RECORD_SIZE) {
					memcpy(attr,ptr,ATTR_RECORD_SIZE);
				} else {
					memcpy(attr,ptr,d->attrsize);
					memset(attr+d->attrsize,0,ATTR_RECORD_SIZE-d->attrsize);
				}
				return 1;
			} else {
				return 0;
			}
		}
		hash+=disp;
	}
	return 0;
}

static inline uint8_t dcache_inodehash_get(dircache *d,uint32_t inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint32_t hash,disp,hashmask;
	const uint8_t *ptr;

	if (d->inodehashtab==NULL) {
		dcache_makeinodehash(d);
	}
	hashmask = d->hashsize-1;
	hash = inode*0xB28E457D;
	disp = ((inode*0x53B23891)&hashmask)|1;
	while ((ptr=d->inodehashtab[hash&hashmask])) {
		if (inode==get32bit(&ptr)) {
			if (*ptr) { // are attributes valid ?
				if (d->attrsize>=ATTR_RECORD_SIZE) {
					memcpy(attr,ptr,ATTR_RECORD_SIZE);
				} else {
					memcpy(attr,ptr,d->attrsize);
					memset(attr+d->attrsize,0,ATTR_RECORD_SIZE-d->attrsize);
				}
				return 1;
			} else {
				return 0;
			}
		}
		hash+=disp;
	}
	return 0;
}

static inline uint8_t dcache_inodehash_set(dircache *d,uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE]) {
	uint32_t hash,disp,hashmask;
	const uint8_t *ptr;

	if (d->inodehashtab==NULL) {
		dcache_makeinodehash(d);
	}
	hashmask = d->hashsize-1;
	hash = inode*0xB28E457D;
	disp = ((inode*0x53B23891)&hashmask)|1;
	while ((ptr=d->inodehashtab[hash&hashmask])) {
		if (inode==get32bit(&ptr)) {
			if (d->attrsize<ATTR_RECORD_SIZE) {
				memcpy((uint8_t*)ptr,attr,d->attrsize);
			} else {
				memcpy((uint8_t*)ptr,attr,ATTR_RECORD_SIZE);
			}
			return 1;
		}
		hash+=disp;
	}
	return 0;
}

static inline uint8_t dcache_inodehash_invalidate_attr(dircache *d,uint32_t inode) {
	uint32_t hash,disp,hashmask;
	const uint8_t *ptr;

	if (d->inodehashtab==NULL) {
		dcache_makeinodehash(d);
	}
	hashmask = d->hashsize-1;
	hash = inode*0xB28E457D;
	disp = ((inode*0x53B23891)&hashmask)|1;
	while ((ptr=d->inodehashtab[hash&hashmask])) {
		if (inode==get32bit(&ptr)) {
			memset((uint8_t*)ptr,0,d->attrsize);
			return 1;
		}
		hash+=disp;
	}
	return 0;
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

void dcache_invalidate_name(const struct fuse_ctx *ctx,uint32_t parent,uint8_t nleng,const uint8_t *name) {
	dircache *d;
	zassert(pthread_mutex_lock(&glock));
	for (d=head ; d ; d=d->next) {
		if (parent==d->parent && ctx->pid==d->ctx.pid && ctx->uid==d->ctx.uid && ctx->gid==d->ctx.gid) {
			dcache_namehash_invalidate(d,nleng,name);
		}
	}
	zassert(pthread_mutex_unlock(&glock));
}
