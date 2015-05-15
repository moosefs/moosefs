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

#include <fuse_lowlevel.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "massert.h"
#include "datapack.h"

typedef struct _dircache {
	struct fuse_ctx ctx;
	uint32_t parent;
	const uint8_t *dbuff;
	uint32_t dsize;
	uint32_t hashsize;
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

uint32_t dcache_elemcount(const uint8_t *dbuff,uint32_t dsize) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
	uint32_t ret;
	ptr = dbuff;
	eptr = dbuff+dsize;
	ret=0;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr) {
			ret++;
		}
		ptr+=enleng+40;
	}
	return ret;
}

static inline void dcache_calchashsize(dircache *d) {
	uint32_t cnt = dcache_elemcount(d->dbuff,d->dsize);
	d->hashsize = 1;
	cnt = (cnt*3)/2;
	while (cnt) {
		d->hashsize<<=1;
		cnt>>=1;
	}
}

void dcache_makenamehash(dircache *d) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
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
		if (ptr+enleng+40<=eptr) {
			hash = dcache_hash(ptr+1,enleng);
			disp = ((hash*0x53B23891)&hashmask)|1;
			while (d->namehashtab[hash&hashmask]) {
				hash+=disp;
			}
			d->namehashtab[hash&hashmask]=ptr;
		}
		ptr+=enleng+40;
	}
}

void dcache_makeinodehash(dircache *d) {
	const uint8_t *iptr,*ptr,*eptr;
	uint8_t enleng;
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
		if (ptr+enleng+40<=eptr) {
			iptr = ptr+1+enleng;
			hash = get32bit(&iptr);
			disp = ((hash*0x53B23891)&hashmask)|1;
			hash *= 0xB28E457D;
			while (d->inodehashtab[hash&hashmask]) {
				hash+=disp;
			}
			d->inodehashtab[hash&hashmask]=ptr+1+enleng;
		}
		ptr+=enleng+40;
	}
}

void* dcache_new(const struct fuse_ctx *ctx,uint32_t parent,const uint8_t *dbuff,uint32_t dsize) {
	dircache *d;
	d = malloc(sizeof(dircache));
	d->ctx.pid = ctx->pid;
	d->ctx.uid = ctx->uid;
	d->ctx.gid = ctx->gid;
	d->parent = parent;
	d->dbuff = dbuff;
	d->dsize = dsize;
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

/*
static inline uint8_t dcache_namesearch(const uint8_t *dbuff,uint32_t dsize,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
	ptr = dbuff;
	eptr = dbuff+dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr && enleng==nleng && memcmp(ptr+1,name,enleng)==0) {
			ptr+=1+enleng;
			*inode = get32bit(&ptr);
			memcpy(attr,ptr,35);
			return 1;
		}
		ptr+=enleng+40;
	}
	return 0;
}

static inline uint8_t dcache_inodesearch(const uint8_t *dbuff,uint32_t dsize,uint32_t inode,uint8_t attr[35]) {
	const uint8_t *ptr,*eptr;
	uint8_t enleng;
	ptr = dbuff;
	eptr = dbuff+dsize;
	while (ptr<eptr) {
		enleng = *ptr;
		if (ptr+enleng+40<=eptr) {
			ptr+=1+enleng;
			if (inode==get32bit(&ptr)) {
				memcpy(attr,ptr,35);
				return 1;
			} else {
				ptr+=35;
			}
		} else {
			return 0;
		}
	}
	return 0;
}
*/

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
			ptr+=1+nleng;
			memset((uint8_t*)ptr,0,sizeof(uint32_t)+35);
			return;
		}
		hash+=disp;
	}
}

static inline uint8_t dcache_namehash_get(dircache *d,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]) {
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
			ptr+=1+nleng;
			*inode = get32bit(&ptr);
			if (*ptr) { // are attributes valid ?
				memcpy(attr,ptr,35);
				return 1;
			} else {
				return 0;
			}
		}
		hash+=disp;
	}
	return 0;
}

static inline uint8_t dcache_inodehash_get(dircache *d,uint32_t inode,uint8_t attr[35]) {
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
				memcpy(attr,ptr,35);
				return 1;
			} else {
				return 0;
			}
		}
		hash+=disp;
	}
	return 0;
}

static inline uint8_t dcache_inodehash_set(dircache *d,uint32_t inode,const uint8_t attr[35]) {
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
			memcpy((uint8_t*)ptr,attr,35);
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
			memset((uint8_t*)ptr,0,35);
			return 1;
		}
		hash+=disp;
	}
	return 0;
}

uint8_t dcache_lookup(const struct fuse_ctx *ctx,uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]) {
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

uint8_t dcache_getattr(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35]) {
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

void dcache_setattr(uint32_t inode,const uint8_t attr[35]) {
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
