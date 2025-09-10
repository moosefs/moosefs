#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

#include "datapack.h"

#define REHASH_STEP 5
#define CLUSTER_SIZE 9

typedef struct {
	uint8_t **old_hashtab;
	uint8_t **new_hashtab;
	size_t old_size;
	size_t new_size;
	size_t count;
	size_t rehash_pos;
	int is_rehashing;
} node_index;

static uint32_t hash_function(uint32_t node) {
	return node*33U;
}

static void hash_insert(uint8_t **hashtab,uint32_t hashsize,uint8_t *ptr,uint32_t hash) {
	uint32_t i,disp,hashmask;

	hashmask = hashsize - 1;

	disp = ((hash*0x53B23891)&hashmask)|1;
	for (;;) {
		for (i=0 ; i<CLUSTER_SIZE ; i++) {
			if (hashtab[(hash+i)&hashmask]==NULL) {
				hashtab[(hash+i)&hashmask]=ptr;
				return;
			}
		}
		hash += disp;
	}
}

static uint8_t* hash_find(uint8_t **hashtab,uint32_t hashsize,uint32_t node,uint32_t hash) {
	uint32_t i,disp,hashmask;
	uint8_t *ptr;
	const uint8_t *rptr;

	hashmask = hashsize - 1;

	disp = ((hash*0x53B23891)&hashmask)|1;
	for (;;) {
		for (i=0 ; i<CLUSTER_SIZE ; i++) {
			ptr = hashtab[(hash+i)&hashmask];
			if (ptr==NULL) {
				return NULL;
			}
			rptr = ptr + 1 + (*ptr);
			if (node==get32bit(&rptr)) {
				return ptr;
			}
		}
		hash += disp;
	}
}

static void do_incremental_rehash(node_index *idx) {
	int steps;
	uint8_t *ptr;
	const uint8_t *rptr;
	uint32_t node,hash;

	if (!idx->is_rehashing) return;

	steps = REHASH_STEP;
	while (steps > 0 && idx->rehash_pos < idx->old_size) {
		ptr = idx->old_hashtab[idx->rehash_pos];
		if (ptr) {
			rptr = ptr + 1 + (*ptr);
			node = get32bit(&rptr);
			if (node>0) {
				hash = hash_function(node);
				hash_insert(idx->new_hashtab,idx->new_size,ptr,hash);
			} else {
				idx->count--;
			}
		}
		idx->rehash_pos++;
		steps--;
	}

	// Rehash complete?
	if (idx->rehash_pos >= idx->old_size) {
		free(idx->old_hashtab);
		idx->old_hashtab = idx->new_hashtab;
		idx->old_size = idx->new_size;
		idx->new_hashtab = NULL;
		idx->new_size = 0;
		idx->is_rehashing = 0;
		idx->rehash_pos = 0;
	}
}

void *node_index_create(uint32_t minelements) {
	node_index *idx;
	uint32_t initial_size;

	initial_size = (minelements*5)/3;
	initial_size |= initial_size>>1;
	initial_size |= initial_size>>2;
	initial_size |= initial_size>>4;
	initial_size |= initial_size>>8;
	initial_size |= initial_size>>16;
	initial_size++;
	if (initial_size<256) {
		initial_size=256;
	}

	idx = malloc(sizeof(node_index));
	idx->old_hashtab = calloc(initial_size, sizeof(uint8_t*));
	idx->new_hashtab = NULL;
	idx->old_size = initial_size;
	idx->new_size = 0;
	idx->count = 0;
	idx->rehash_pos = 0;
	idx->is_rehashing = 0;
	return idx;
}
/*
static void print_counts(node_index *idx) {
	printf("count: %u\n",idx->count);
	uint32_t rcount=0;
	for (uint32_t i=0 ; i<idx->old_size ; i++) {
		if (idx->old_hashtab[i]) {
			rcount++;
		}
	}
	printf("real count: %u\n",rcount);
}
*/
void node_index_destroy(void *vidx) {
	node_index *idx = (node_index *)vidx;

//	print_counts(idx);
	free(idx->old_hashtab);

	if (idx->new_hashtab) {
		free(idx->new_hashtab);
	}
	free(idx);
}

void node_index_add(void *vidx, uint8_t *ptr) {
	const uint8_t *rptr;
	uint32_t node,hash;
	node_index *idx = (node_index *)vidx;

	do_incremental_rehash(idx);

	// Start rehash if needed
	if (!idx->is_rehashing && idx->count * 5 > idx->old_size * 3) {
//		print_counts(idx);
		idx->new_size = idx->old_size * 2;
		idx->new_hashtab = calloc(idx->new_size, sizeof(uint8_t*));
		idx->is_rehashing = 1;
		idx->rehash_pos = 0;
	}

	rptr = ptr + 1 + (*ptr);
	node = get32bit(&rptr);
	if (node==0) {
		return;
	}
	hash = hash_function(node);

	if (idx->is_rehashing) {
		hash_insert(idx->new_hashtab,idx->new_size,ptr,hash);
	} else {
		hash_insert(idx->old_hashtab,idx->old_size,ptr,hash);
	}
	idx->count++;
}

uint8_t* node_index_find(void *vidx, uint32_t node) {
	uint8_t *ptr;
	uint32_t hash;
	node_index *idx = (node_index *)vidx;

	do_incremental_rehash(idx);

	hash = hash_function(node);

	if (idx->is_rehashing) {
		ptr = hash_find(idx->new_hashtab,idx->new_size,node,hash);
		if (ptr!=NULL) {
			return ptr;
		}
	}

	return hash_find(idx->old_hashtab,idx->old_size,node,hash);
}
