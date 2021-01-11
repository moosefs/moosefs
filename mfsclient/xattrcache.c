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
#include <string.h>
#include <sys/time.h>
#include <inttypes.h>
#include <pthread.h>

#include "massert.h"
#include "clocks.h"

#define HASHSIZE 65536

typedef struct _xattr_cache_value {
	uint32_t lcnt;
	const uint8_t *value;
} xattr_cache_value;

typedef struct _xattr_cache_entry {
	uint32_t hash;
	uint32_t node;
	uint32_t uid,gid;
	uint32_t nleng;
	uint32_t vleng;
	int status;
	const uint8_t *name;
	xattr_cache_value *value;
	int64_t utimestamp;
	struct _xattr_cache_entry *hashnext,**hashprev;
	struct _xattr_cache_entry *lrunext,**lruprev;
} xattr_cache_entry;

static xattr_cache_entry *lruhead,**lrutail;
static xattr_cache_entry **hashtab;
static int64_t xattr_cache_timeout;
static pthread_mutex_t glock;

static inline uint32_t xattr_cache_hash(uint32_t node,uint32_t nleng,const uint8_t *name) {
	uint32_t hash;
	uint32_t i;
	hash = node * 0x5F2318BD + nleng;
	for (i=0 ; i<nleng ; i++) {
		hash = hash*33+name[i];
	}
	return hash;
}

static inline xattr_cache_value* xattr_cache_value_alloc(void) {
	xattr_cache_value *v;
	v = malloc(sizeof(xattr_cache_value));
	passert(v);
	v->lcnt = 1;
	v->value = NULL;
	return v;
}

static inline void xattr_cache_value_inc(xattr_cache_value *v) {
	v->lcnt++;
}

static inline void xattr_cache_value_dec(xattr_cache_value *v) {
	v->lcnt--;
	if (v->lcnt==0) {
		if (v->value) {
			free((uint8_t*)(v->value));
		}
		free(v);
	}
}

static inline void xattr_cache_remove_entry(xattr_cache_entry *xce) {
	if (xce->hashnext) {
		xce->hashnext->hashprev = xce->hashprev;
	}
	*(xce->hashprev) = xce->hashnext;
	if (xce->lrunext) {
		xce->lrunext->lruprev = xce->lruprev;
	} else {
		lrutail = xce->lruprev;
	}
	*(xce->lruprev) = xce->lrunext;
	if (xce->name) {
		free((uint8_t*)(xce->name));
	}
	xattr_cache_value_dec(xce->value);
	free(xce);
}

static inline void xattr_cache_new(uint32_t node,uint32_t uid,uint32_t gid,uint32_t nleng,const uint8_t *name,const uint8_t *value,uint32_t vleng,int status,int64_t utimestamp) {
	xattr_cache_entry *xce;
	uint32_t hash;
	hash = xattr_cache_hash(node,nleng,name);
	xce = malloc(sizeof(xattr_cache_entry));
	passert(xce);
	xce->hash = hash;
	xce->node = node;
	xce->uid = uid;
	xce->gid = gid;
	xce->nleng = nleng;
	if (nleng>0) {
		xce->name = malloc(nleng);
		passert(xce->name);
		memcpy((uint8_t*)(xce->name),name,nleng);
	} else {
		xce->name = NULL;
	}
	xce->vleng = vleng;
	xce->value = xattr_cache_value_alloc();
	if (vleng>0) {
		xce->value->value = malloc(vleng);
		passert(xce->value->value);
		memcpy((uint8_t*)(xce->value->value),value,vleng);
	}
	xce->status = status;
	xce->utimestamp = utimestamp;
	xce->hashnext = hashtab[hash%HASHSIZE];
	xce->hashprev = hashtab+(hash%HASHSIZE);
	if (xce->hashnext) {
		xce->hashnext->hashprev = &(xce->hashnext);
	}
	hashtab[hash%HASHSIZE] = xce;
	xce->lrunext = NULL;
	xce->lruprev = lrutail;
	*lrutail = xce;
	lrutail = &(xce->lrunext);
}

static inline xattr_cache_entry* xattr_cache_find(uint32_t node,uint32_t uid,uint32_t gid,uint32_t nleng,const uint8_t *name) {
	uint32_t hash;
	xattr_cache_entry *xce;

	hash = xattr_cache_hash(node,nleng,name);
	for (xce = hashtab[hash%HASHSIZE] ; xce!=NULL ; xce=xce->hashnext) {
		if (xce->hash == hash && xce->node == node && xce->uid == uid && xce->gid == gid && xce->nleng == nleng && memcmp(xce->name,name,nleng)==0) {
			return xce;
		}
	}
	return NULL;
}

static inline void xattr_cache_delete(uint32_t node,uint32_t nleng,const uint8_t *name) {
	uint32_t hash;
	xattr_cache_entry *xce,*nxce;
	hash = xattr_cache_hash(node,nleng,name);
	xce = hashtab[hash%HASHSIZE];
	while (xce) {
		nxce = xce->hashnext;
		if (xce->hash == hash && xce->node == node && xce->nleng == nleng && memcmp(xce->name,name,nleng)==0) {
			xattr_cache_remove_entry(xce);
		}
		xce = nxce;
	}
}

static inline void xattr_cache_invalidate(int64_t utimestamp) {
	xattr_cache_entry *xce,*nxce;
	xce = lruhead;
	while (xce!=NULL && xce->utimestamp < utimestamp) {
		nxce = xce->lrunext;
		xattr_cache_remove_entry(xce);
		xce = nxce;
	}
	if (lruhead==NULL) {
		lrutail = &lruhead;
	}
}

void* xattr_cache_get(uint32_t node,uint32_t uid,uint32_t gid,uint32_t nleng,const uint8_t *name,const uint8_t **value,uint32_t *vleng,int *status) {
	xattr_cache_entry *xce;
	xattr_cache_value *v;
	int64_t utimestamp = monotonic_useconds();
	zassert(pthread_mutex_lock(&glock));
	xattr_cache_invalidate(utimestamp);
	xce = xattr_cache_find(node,uid,gid,nleng,name);
	if (xce==NULL) {
		v = NULL;
	} else {
		if (value) {
			*value = xce->value->value;
		}
		if (vleng) {
			*vleng = xce->vleng;
		}
		if (status) {
			*status = xce->status;
		}
		v = xce->value;
		xattr_cache_value_inc(v);
	}
	zassert(pthread_mutex_unlock(&glock));
	return (void*)v;
}

void xattr_cache_set(uint32_t node,uint32_t uid,uint32_t gid,uint32_t nleng,const uint8_t *name,const uint8_t *value,uint32_t vleng,int status) {
	int64_t utimestamp = monotonic_useconds();
	xattr_cache_entry *xce;
	zassert(pthread_mutex_lock(&glock));
	xce = xattr_cache_find(node,uid,gid,nleng,name);
	if (xce!=NULL) {
		xattr_cache_remove_entry(xce);
	}
	xattr_cache_new(node,uid,gid,nleng,name,value,vleng,status,utimestamp+xattr_cache_timeout);
	zassert(pthread_mutex_unlock(&glock));
}

void xattr_cache_del(uint32_t node,uint32_t nleng,const uint8_t *name) {
	zassert(pthread_mutex_lock(&glock));
	xattr_cache_delete(node,nleng,name);
	zassert(pthread_mutex_unlock(&glock));
}

void xattr_cache_rel(void *vv) {
	zassert(pthread_mutex_lock(&glock));
	xattr_cache_value_dec((xattr_cache_value*)vv);
	zassert(pthread_mutex_unlock(&glock));
}

void xattr_cache_term(void) {
#ifdef __clang_analyzer__
	xattr_cache_entry *lhn;
#endif
	zassert(pthread_mutex_lock(&glock));
	while (lruhead!=NULL) {
#ifdef __clang_analyzer__
		lhn = lruhead->lrunext;
#endif
		xattr_cache_remove_entry(lruhead);
#ifdef __clang_analyzer__
		lruhead = lhn;
		// lru list is double linked list, so xattr_cache_remove_entry changes lruhead using 'prev' pointer !!!
		// static analyzers (namely clang) are too stupid to understand such construction - ignore warnings !!!
#endif
	}
	free(hashtab);
	zassert(pthread_mutex_unlock(&glock));
	zassert(pthread_mutex_destroy(&glock));
}

void xattr_cache_init(double timeout) {
	uint32_t i;
	lruhead = NULL;
	lrutail = &lruhead;
	hashtab = malloc(sizeof(xattr_cache_entry*)*HASHSIZE);
	for (i=0; i<HASHSIZE ; i++) {
		hashtab[i] = NULL;
	}
	xattr_cache_timeout = (int64_t)(1000000.0 * timeout);
	zassert(pthread_mutex_init(&glock,NULL));
}
