/*
 * Copyright (C) 2017 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
#include <stddef.h>
#include <inttypes.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#include "massert.h"

#include "glue.h"

typedef struct _dictentry {
	struct _dictentry *next;
	uint32_t hashval;
	uint32_t refcnt;
	uint32_t leng;
	const uint8_t data[1];
} dictentry;

#define LOHASH_BITS 20
#define ENTRY_TYPE dictentry
#define GLUE_FN_NAME_PREFIX(Y) GLUE(dict,Y)
#define HASH_ARGS_TYPE_LIST const uint8_t *data,uint32_t leng
#define HASH_ARGS_LIST data,leng
#define GLUE_HASH_TAB_PREFIX(Y) GLUE(dict,Y)
#define HASH_VALUE_FIELD hashval

static inline int GLUE_FN_NAME_PREFIX(_cmp)(ENTRY_TYPE *e,HASH_ARGS_TYPE_LIST) {
	return (e->leng==leng && memcmp((char*)(e->data),(char*)data,leng)==0);
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_hash)(HASH_ARGS_TYPE_LIST) {
	uint32_t hash,i;
	hash = leng;
	for (i=0 ; i<leng ; i++) {
		hash = hash*33+data[i];
	}
	return hash;
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_ehash)(ENTRY_TYPE *e) {
	return GLUE_FN_NAME_PREFIX(_hash)(e->data,e->leng);
}

#include "hash_begin.h"

#if 0

#define HASHTAB_LOBITS 20
#define HASHTAB_HISIZE (0x80000000>>(HASHTAB_LOBITS))
#define HASHTAB_LOSIZE (1<<HASHTAB_LOBITS)
#define HASHTAB_MASK (HASHTAB_LOSIZE-1)
#define HASHTAB_MOVEFACTOR 5
#define HASHTAB_SIZEHINT 0


typedef struct _dictentry {
	struct _dictentry *next;
	uint32_t hashval;
	uint32_t refcnt;
	uint32_t leng;
	const uint8_t data[1];
} dictentry;

static dictentry **dicthashtab[HASHTAB_HISIZE];
static uint32_t dictrehashpos;
static uint32_t dicthashsize;
static uint32_t dicthashelem;


/* internals */

static inline uint32_t dict_hash(const uint8_t *data,uint32_t leng) {
	uint32_t hash,i;
	hash = leng;
	for (i=0 ; i<leng ; i++) {
		hash = hash*33+data[i];
	}
	return hash;
}

static inline uint32_t dict_calc_hash_size(uint32_t elements) {
	uint32_t res=1;
	while (elements) {
		elements>>=1;
		res<<=1;
	}
	if (res==0) {
		res = UINT32_C(0x80000000);
	}
	if (res<HASHTAB_LOSIZE) {
		return HASHTAB_LOSIZE;
	}
	return res;
}

static inline void dict_hash_init(void) {
	uint16_t i;
	dicthashsize = 0;
	dicthashelem = 0;
	dictrehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		dicthashtab[i] = NULL;
	}
}

static inline void dict_hash_cleanup(void) {
	uint16_t i;
	uint32_t j;
	dicthashelem = 0;
	dicthashsize = 0;
	dictrehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (dicthashtab[i]!=NULL) {
			for (j=0 ; j<HASHTAB_LOSIZE ; j++) {
				massert(dicthashtab[i][j]==NULL,"dictionary has elements during clean up");
			}
#ifdef HAVE_MMAP
			munmap(dicthashtab[i],sizeof(dictentry*)*HASHTAB_LOSIZE);
#else
			free(dicthashtab[i]);
#endif
		}
		dicthashtab[i] = NULL;
	}
}

static inline void dict_hash_move(void) {
	uint32_t hash;
	uint32_t mask;
	uint32_t moved=0;
	dictentry **ehptr,**ehptralt,*e;
	mask = dicthashsize-1;
	do {
		if (dictrehashpos>=dicthashsize) { // rehash complete
			dictrehashpos = dicthashsize;
			return;
		}
		if (dicthashtab[dictrehashpos >> HASHTAB_LOBITS]==NULL) {
#ifdef HAVE_MMAP
			dicthashtab[dictrehashpos >> HASHTAB_LOBITS] = mmap(NULL,sizeof(dictentry*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
			dicthashtab[dictrehashpos >> HASHTAB_LOBITS] = malloc(sizeof(dictentry*)*HASHTAB_LOSIZE);
#endif
			passert(dicthashtab[dictrehashpos >> HASHTAB_LOBITS]);
		}
		ehptr = dicthashtab[(dictrehashpos - (dicthashsize/2)) >> HASHTAB_LOBITS] + (dictrehashpos & HASHTAB_MASK);
		ehptralt = dicthashtab[dictrehashpos >> HASHTAB_LOBITS] + (dictrehashpos & HASHTAB_MASK);
		*ehptralt = NULL;
		while ((e=*ehptr)!=NULL) {
			hash = e->hashval & mask;
			if (hash==dictrehashpos) {
				*ehptralt = e;
				*ehptr = e->next;
				ehptralt = &(e->next);
				e->next = NULL;
			} else {
				ehptr = &(e->next);
			}
			moved++;
		}
		dictrehashpos++;
	} while (moved<HASHTAB_MOVEFACTOR);
}

static inline dictentry* dict_find(const uint8_t *data,uint16_t leng) {
	dictentry *e;
	uint32_t hash;
	uint32_t hashval;

	if (dicthashsize==0) {
		return NULL;
	}
	hashval = dict_hash(data,leng);
	hash = hashval & (dicthashsize-1);
	if (dictrehashpos<dicthashsize) {
		dict_hash_move();
		if (hash >= dictrehashpos) {
			hash -= dicthashsize/2;
		}
	}
	for (e=dicthashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] ; e ; e=e->next) {
		if (e->hashval==hashval && e->leng==leng && memcmp((char*)(e->data),(char*)data,leng)==0) {
			return e;
		}
	}
	return NULL;
}

static inline void dict_delete(dictentry *e) {
	dictentry **ehptr,*eit;
	uint32_t hash;

	if (dicthashsize==0) {
		return;
	}
	hash = (e->hashval) & (dicthashsize-1);
	if (dictrehashpos<dicthashsize) {
		dict_hash_move();
		if (hash >= dictrehashpos) {
			hash -= dicthashsize/2;
		}
	}
	ehptr = dicthashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	while ((eit=*ehptr)!=NULL) {
		if (eit==e) {
			*ehptr = e->next;
			dicthashelem--;
			return;
		}
		ehptr = &(eit->next);
	}
	uassert("dictionary element not found in data structure");
}

static inline void dict_add(dictentry *e) {
	uint16_t i;
	uint32_t hash;

	if (dicthashsize==0) {
		dicthashsize = dict_calc_hash_size(HASHTAB_SIZEHINT);
		dictrehashpos = dicthashsize;
		dicthashelem = 0;
		for (i=0 ; i<dicthashsize>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
			dicthashtab[i] = mmap(NULL,sizeof(dictentry*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
			dicthashtab[i] = malloc(sizeof(dictentry*)*HASHTAB_LOSIZE);
#endif
			passert(dicthashtab[i]);
			memset(dicthashtab[i],0,sizeof(dictentry*));
			if (dicthashtab[i][0]==NULL) {
				memset(dicthashtab[i],0,sizeof(dictentry*)*HASHTAB_LOSIZE);
			} else {
				for (hash=0 ; hash<HASHTAB_LOSIZE ; hash++) {
					dicthashtab[i][hash] = NULL;
				}
			}
		}
	}
	e->hashval = dict_hash(e->data,e->leng);
	hash = (e->hashval) & (dicthashsize-1);
	if (dictrehashpos<dicthashsize) {
		dict_hash_move();
		if (hash >= dictrehashpos) {
			hash -= dicthashsize/2;
		}
		e->next = dicthashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		dicthashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = e;
		dicthashelem++;
	} else {
		e->next = dicthashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		dicthashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = e;
		dicthashelem++;
		if (dicthashelem>dicthashsize && (dicthashsize>>HASHTAB_LOBITS)<HASHTAB_HISIZE) {
			dictrehashpos = dicthashsize;
			dicthashsize *= 2;
		}
	}
}

#endif

/* externals */

int dict_init(void) {
	dict_hash_init();
	return 0;
}

void dict_cleanup(void) {
	dict_hash_cleanup();
}

void* dict_search(const uint8_t *data,uint32_t leng) {
	return dict_find(data,leng);
}

void* dict_insert(const uint8_t *data,uint32_t leng) {
	dictentry *de;
	de = dict_find(data,leng);
	if (de) {
		de->refcnt++;
		return de;
	}
	de = malloc(offsetof(dictentry,data)+leng);
	passert(de);
	de->refcnt = 1;
	de->leng = leng;
	memcpy((uint8_t*)(de->data),data,leng);
	dict_add(de);
	return de;
}

const uint8_t* dict_get_ptr(void *dptr) {
	dictentry *de = (dictentry*)dptr;

	return de->data;
}

uint32_t dict_get_leng(void *dptr) {
	dictentry *de = (dictentry*)dptr;

	return de->leng;
}

uint32_t dict_get_hash(void *dptr) {
	dictentry *de = (dictentry*)dptr;

	return de->hashval;
}

void dict_dec_ref(void *dptr) {
	dictentry *de = (dictentry*)dptr;
	massert(de->refcnt>0,"dictionary reference counter is zero");
	de->refcnt--;
	if (de->refcnt==0) {
		dict_delete(de);
		free(de);
	}
}

void dict_inc_ref(void *dptr) {
	dictentry *de = (dictentry*)dptr;
	massert(de->refcnt>0,"dictionary reference counter is zero");
	de->refcnt++;
}

#include "hash_end.h"

#undef LOHASH_BITS
#undef ENTRY_TYPE
#undef GLUE_FN_NAME_PREFIX
#undef HASH_ARGS_TYPE_LIST
#undef HASH_ARGS_LIST
#undef GLUE_HASH_TAB_PREFIX
#undef HASH_VALUE_FIELD
