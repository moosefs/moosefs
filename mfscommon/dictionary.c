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

// GLUE_FN_NAME_PREFIX(_ehash) is needed only if HASH_VALUE_FIELD is not defined !!!

#include "hash_begin.h"

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
