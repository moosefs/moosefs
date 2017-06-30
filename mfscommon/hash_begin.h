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

#include "glue.h"
#include "massert.h"


// LOHASH_BITS - number of bits in low order hash tabble
// ENTRY_TYPE - type of record stored in hash map
// GLUE_FN_NAME_PREFIX - prefix of function names
// HASH_ARGS_TYPE_LIST - list of find function arguments with types
// HASH_ARGS_LIST - list of find function arguments (also hash function)
// GLUE_HASH_TAB_PREFIX - prefix of hash tab name


// #define LOHASH_BITS 20
// #define ENTRY_TYPE void
// #define FN_NAME_PREFIX dict
// #define HASH_ARGS_TYPE_LIST const uint8_t *data,uint32_t leng
// #define HASH_ARGS_LIST data,leng
// #define HASH_TAB_PREFIX dict
// #define HASH_VALUE_FIELD hashval

#if defined(HASH_TAB_PREFIX) && !defined(GLUE_HASH_TAB_PREFIX)
#define GLUE_HASH_TAB_PREFIX(NAME) GLUE(HASH_TAB_PREFIX,NAME)
#endif

#if defined(FN_NAME_PREFIX) && !defined(GLUE_FN_NAME_PREFIX)
#define GLUE_FN_NAME_PREFIX(NAME) GLUE(FN_NAME_PREFIX,NAME)
#endif

#ifndef LOHASH_BITS
#define LOHASH_BITS 20
#endif

#ifndef ENTRY_TYPE
typedef struct _inthash {
	uint32_t value;
	struct _inthash *next;
} inthash;
#define ENTRY_TYPE inthash

#define GLUE_HELPER(X,Y) X##Y
#define GLUE(X,Y) GLUE_HELPER(X,Y)

#define GLUE_FN_NAME_PREFIX(Y) GLUE(inthash,Y)
#define HASH_ARGS_TYPE_LIST uint32_t value
#define HASH_ARGS_LIST value
#define GLUE_HASH_TAB_PREFIX(Y) GLUE(inthash,Y)


static inline int GLUE_FN_NAME_PREFIX(_cmp)(ENTRY_TYPE *e,HASH_ARGS_TYPE_LIST) {
	return (e->value==value);
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_hash)(HASH_ARGS_TYPE_LIST) {
	return value;
}

static inline uint32_t GLUE_FN_NAME_PREFIX(_ehash)(ENTRY_TYPE *e) {
	return e->value;
}
#endif


#define HASHTAB_LOBITS LOHASH_BITS
#define HASHTAB_HISIZE (0x80000000>>(HASHTAB_LOBITS))
#define HASHTAB_LOSIZE (1<<HASHTAB_LOBITS)
#define HASHTAB_MASK (HASHTAB_LOSIZE-1)

#ifndef HASHTAB_MOVEFACTOR
#define HASHTAB_MOVEFACTOR 5
#endif

#define HASHTAB_SIZEHINT 0

static ENTRY_TYPE **GLUE_HASH_TAB_PREFIX(hashtab) [HASHTAB_HISIZE];
static uint32_t GLUE_HASH_TAB_PREFIX(rehashpos);
static uint32_t GLUE_HASH_TAB_PREFIX(hashsize);
static uint32_t GLUE_HASH_TAB_PREFIX(hashelem);


/* internals */

static inline uint32_t GLUE_FN_NAME_PREFIX(_calc_hash_size)(uint32_t elements) {
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

static inline void GLUE_FN_NAME_PREFIX(_hash_init)(void) {
	uint16_t i;
	GLUE_HASH_TAB_PREFIX(hashsize) = 0;
	GLUE_HASH_TAB_PREFIX(hashelem) = 0;
	GLUE_HASH_TAB_PREFIX(rehashpos) = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		GLUE_HASH_TAB_PREFIX(hashtab)[i] = NULL;
	}
}

static inline void GLUE_FN_NAME_PREFIX(_hash_cleanup)(void) {
	uint16_t i;
	uint32_t j;
	GLUE_HASH_TAB_PREFIX(hashelem) = 0;
	GLUE_HASH_TAB_PREFIX(hashsize) = 0;
	GLUE_HASH_TAB_PREFIX(rehashpos) = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (GLUE_HASH_TAB_PREFIX(hashtab)[i]!=NULL) {
			for (j=0 ; j<HASHTAB_LOSIZE ; j++) {
				massert(GLUE_HASH_TAB_PREFIX(hashtab)[i][j]==NULL,"hash map has elements during clean up");
			}
#ifdef HAVE_MMAP
			munmap(GLUE_HASH_TAB_PREFIX(hashtab)[i],sizeof(ENTRY_TYPE*)*HASHTAB_LOSIZE);
#else
			free(GLUE_HASH_TAB_PREFIX(hashtab)[i]);
#endif
		}
		GLUE_HASH_TAB_PREFIX(hashtab)[i] = NULL;
	}
}

static inline void GLUE_FN_NAME_PREFIX(_hash_move)(void) {
	uint32_t hash;
	uint32_t mask;
	uint32_t moved=0;
	ENTRY_TYPE **ehptr,**ehptralt,*e;
	mask = GLUE_HASH_TAB_PREFIX(hashsize)-1;
	do {
		if (GLUE_HASH_TAB_PREFIX(rehashpos)>=GLUE_HASH_TAB_PREFIX(hashsize)) { // rehash complete
			GLUE_HASH_TAB_PREFIX(rehashpos) = GLUE_HASH_TAB_PREFIX(hashsize);
			return;
		}
		if (GLUE_HASH_TAB_PREFIX(hashtab)[GLUE_HASH_TAB_PREFIX(rehashpos) >> HASHTAB_LOBITS]==NULL) {
#ifdef HAVE_MMAP
			GLUE_HASH_TAB_PREFIX(hashtab)[GLUE_HASH_TAB_PREFIX(rehashpos) >> HASHTAB_LOBITS] = mmap(NULL,sizeof(ENTRY_TYPE*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
			GLUE_HASH_TAB_PREFIX(hashtab)[GLUE_HASH_TAB_PREFIX(rehashpos) >> HASHTAB_LOBITS] = malloc(sizeof(ENTRY_TYPE*)*HASHTAB_LOSIZE);
#endif
			passert(GLUE_HASH_TAB_PREFIX(hashtab)[GLUE_HASH_TAB_PREFIX(rehashpos) >> HASHTAB_LOBITS]);
		}
		ehptr = GLUE_HASH_TAB_PREFIX(hashtab)[(GLUE_HASH_TAB_PREFIX(rehashpos) - (GLUE_HASH_TAB_PREFIX(hashsize)/2)) >> HASHTAB_LOBITS] + (GLUE_HASH_TAB_PREFIX(rehashpos) & HASHTAB_MASK);
		ehptralt = GLUE_HASH_TAB_PREFIX(hashtab)[GLUE_HASH_TAB_PREFIX(rehashpos) >> HASHTAB_LOBITS] + (GLUE_HASH_TAB_PREFIX(rehashpos) & HASHTAB_MASK);
		*ehptralt = NULL;
		while ((e=*ehptr)!=NULL) {
#ifdef HASH_VALUE_FIELD
			hash = e->HASH_VALUE_FIELD & mask;
#else
			hash = GLUE_FN_NAME_PREFIX(_ehash)(e) & mask;
#endif
			if (hash==GLUE_HASH_TAB_PREFIX(rehashpos)) {
				*ehptralt = e;
				*ehptr = e->next;
				ehptralt = &(e->next);
				e->next = NULL;
			} else {
				ehptr = &(e->next);
			}
			moved++;
		}
		GLUE_HASH_TAB_PREFIX(rehashpos)++;
	} while (moved<HASHTAB_MOVEFACTOR);
}

static inline ENTRY_TYPE* GLUE_FN_NAME_PREFIX(_find)(HASH_ARGS_TYPE_LIST) {
	ENTRY_TYPE *e;
	uint32_t hash;
	uint32_t hashval;

	if (GLUE_HASH_TAB_PREFIX(hashsize)==0) {
		return NULL;
	}
	hashval = GLUE_FN_NAME_PREFIX(_hash)(HASH_ARGS_LIST);
	hash = hashval & (GLUE_HASH_TAB_PREFIX(hashsize)-1);
	if (GLUE_HASH_TAB_PREFIX(rehashpos)<GLUE_HASH_TAB_PREFIX(hashsize)) {
		GLUE_FN_NAME_PREFIX(_hash_move)();
		if (hash >= GLUE_HASH_TAB_PREFIX(rehashpos)) {
			hash -= GLUE_HASH_TAB_PREFIX(hashsize)/2;
		}
	}
	for (e=GLUE_HASH_TAB_PREFIX(hashtab)[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] ; e ; e=e->next) {
#ifdef HASH_VALUE_FIELD
		if (e->HASH_VALUE_FIELD==hashval && GLUE_FN_NAME_PREFIX(_cmp)(e,HASH_ARGS_LIST)) {
#else
		if (GLUE_FN_NAME_PREFIX(_cmp)(e,HASH_ARGS_LIST)) {
#endif
			return e;
		}
	}
	return NULL;
}

static inline uint8_t GLUE_FN_NAME_PREFIX(_delete)(ENTRY_TYPE *e) {
	ENTRY_TYPE **ehptr,*eit;
	uint32_t hash;

	if (GLUE_HASH_TAB_PREFIX(hashsize)==0) {
		return 0;
	}
#ifdef HASH_VALUE_FIELD
	hash = (e->HASH_VALUE_FIELD) & (GLUE_HASH_TAB_PREFIX(hashsize)-1);
#else
	hash = GLUE_FN_NAME_PREFIX(_ehash)(e) & (GLUE_HASH_TAB_PREFIX(hashsize)-1);
#endif
	if (GLUE_HASH_TAB_PREFIX(rehashpos)<GLUE_HASH_TAB_PREFIX(hashsize)) {
		GLUE_FN_NAME_PREFIX(_hash_move)();
		if (hash >= GLUE_HASH_TAB_PREFIX(rehashpos)) {
			hash -= GLUE_HASH_TAB_PREFIX(hashsize)/2;
		}
	}
	ehptr = GLUE_HASH_TAB_PREFIX(hashtab)[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	while ((eit=*ehptr)!=NULL) {
		if (eit==e) {
			*ehptr = e->next;
			GLUE_HASH_TAB_PREFIX(hashelem)--;
			return 1;
		}
		ehptr = &(eit->next);
	}
	return 0;
}

static inline void GLUE_FN_NAME_PREFIX(_add)(ENTRY_TYPE *e) {
	uint16_t i;
	uint32_t hash;

	if (GLUE_HASH_TAB_PREFIX(hashsize)==0) {
		GLUE_HASH_TAB_PREFIX(hashsize) = GLUE_FN_NAME_PREFIX(_calc_hash_size)(HASHTAB_SIZEHINT);
		GLUE_HASH_TAB_PREFIX(rehashpos) = GLUE_HASH_TAB_PREFIX(hashsize);
		GLUE_HASH_TAB_PREFIX(hashelem) = 0;
		for (i=0 ; i<GLUE_HASH_TAB_PREFIX(hashsize)>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
			GLUE_HASH_TAB_PREFIX(hashtab)[i] = mmap(NULL,sizeof(ENTRY_TYPE*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
			GLUE_HASH_TAB_PREFIX(hashtab)[i] = malloc(sizeof(ENTRY_TYPE*)*HASHTAB_LOSIZE);
#endif
			passert(GLUE_HASH_TAB_PREFIX(hashtab)[i]);
			memset(GLUE_HASH_TAB_PREFIX(hashtab)[i],0,sizeof(ENTRY_TYPE*));
			if (GLUE_HASH_TAB_PREFIX(hashtab)[i][0]==NULL) {
				memset(GLUE_HASH_TAB_PREFIX(hashtab)[i],0,sizeof(ENTRY_TYPE*)*HASHTAB_LOSIZE);
			} else {
				for (hash=0 ; hash<HASHTAB_LOSIZE ; hash++) {
					GLUE_HASH_TAB_PREFIX(hashtab)[i][hash] = NULL;
				}
			}
		}
	}
#ifdef HASH_VALUE_FIELD
	e->HASH_VALUE_FIELD = GLUE_FN_NAME_PREFIX(_hash)(e->data,e->leng);
	hash = (e->HASH_VALUE_FIELD) & (GLUE_HASH_TAB_PREFIX(hashsize)-1);
#else
	hash = GLUE_FN_NAME_PREFIX(_ehash)(e) & (GLUE_HASH_TAB_PREFIX(hashsize)-1);
#endif
	if (GLUE_HASH_TAB_PREFIX(rehashpos)<GLUE_HASH_TAB_PREFIX(hashsize)) {
		GLUE_FN_NAME_PREFIX(_hash_move)();
		if (hash >= GLUE_HASH_TAB_PREFIX(rehashpos)) {
			hash -= GLUE_HASH_TAB_PREFIX(hashsize)/2;
		}
		e->next = GLUE_HASH_TAB_PREFIX(hashtab)[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		GLUE_HASH_TAB_PREFIX(hashtab)[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = e;
		GLUE_HASH_TAB_PREFIX(hashelem)++;
	} else {
		e->next = GLUE_HASH_TAB_PREFIX(hashtab)[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		GLUE_HASH_TAB_PREFIX(hashtab)[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = e;
		GLUE_HASH_TAB_PREFIX(hashelem)++;
		if (GLUE_HASH_TAB_PREFIX(hashelem)>GLUE_HASH_TAB_PREFIX(hashsize) && (GLUE_HASH_TAB_PREFIX(hashsize)>>HASHTAB_LOBITS)<HASHTAB_HISIZE) {
			GLUE_HASH_TAB_PREFIX(rehashpos) = GLUE_HASH_TAB_PREFIX(hashsize);
			GLUE_HASH_TAB_PREFIX(hashsize) *= 2;
		}
	}
}
