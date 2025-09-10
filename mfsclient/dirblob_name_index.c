#include <stdlib.h>
#include <string.h>
#include <stdint.h>

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
} name_index;

static uint32_t hash_function(const uint8_t *str, uint8_t len) {
	uint32_t h = 5381;
	for (uint8_t i = 0; i < len; ++i) {
		h = ((h << 5) + h) + str[i];
	}
	return h;
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
//	while (hashtab[hash&hashmask]) {
//		hash++; // += disp;
//	}
//	hashtab[hash&hashmask]=ptr;
}

static uint8_t* hash_find(uint8_t **hashtab,uint32_t hashsize,const uint8_t *name,uint8_t len,uint32_t hash) {
	uint32_t i,disp,hashmask;
	uint8_t *ptr;

	hashmask = hashsize - 1;

	disp = ((hash*0x53B23891)&hashmask)|1;
	for (;;) {
		for (i=0 ; i<CLUSTER_SIZE ; i++) {
			ptr = hashtab[(hash+i)&hashmask];
			if (ptr==NULL) {
				return NULL;
			}
			if (*ptr==len && memcmp(ptr+1,name,len)==0) {
				return ptr;
			}
		}
		hash += disp;
	}
//	while ((ptr=hashtab[hash&hashmask])) {
//		if (*ptr==len && memcmp(ptr+1,name,len)==0) {
//			return ptr;
//		}
//		hash++; // += disp;
//	}
//	return NULL;
}

static void do_incremental_rehash(name_index *idx) {
	int steps;
	uint8_t *ptr,len,*str;
	uint32_t hash;

	if (!idx->is_rehashing) return;

	steps = REHASH_STEP;
	while (steps > 0 && idx->rehash_pos < idx->old_size) {
		ptr = idx->old_hashtab[idx->rehash_pos];
		if (ptr) {
			len = ptr[0];
			str = ptr + 1;
			hash = hash_function(str, len);
			hash_insert(idx->new_hashtab,idx->new_size,ptr,hash);
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

void *name_index_create(uint32_t minelements) {
	name_index *idx;
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

	idx = malloc(sizeof(name_index));
	idx->old_hashtab = calloc(initial_size, sizeof(uint8_t*));
	idx->new_hashtab = NULL;
	idx->old_size = initial_size;
	idx->new_size = 0;
	idx->count = 0;
	idx->rehash_pos = 0;
	idx->is_rehashing = 0;
	return idx;
}

void name_index_destroy(void *vidx) {
	name_index *idx = (name_index *)vidx;

	free(idx->old_hashtab);

	if (idx->new_hashtab) {
		free(idx->new_hashtab);
	}
	free(idx);
}

void name_index_add(void *vidx, uint8_t *ptr) {
	uint8_t len,*str;
	uint32_t hash;
	name_index *idx = (name_index *)vidx;

	do_incremental_rehash(idx);

	// Start rehash if needed
	if (!idx->is_rehashing && idx->count * 5 > idx->old_size * 3) {
		idx->new_size = idx->old_size * 2;
		idx->new_hashtab = calloc(idx->new_size, sizeof(uint8_t*));
		idx->is_rehashing = 1;
		idx->rehash_pos = 0;
	}

	// Extract length and string from ptr
	len = ptr[0];
	str = ptr + 1;
	hash = hash_function(str, len);

	if (idx->is_rehashing) {
		hash_insert(idx->new_hashtab,idx->new_size,ptr,hash);
	} else {
		hash_insert(idx->old_hashtab,idx->old_size,ptr,hash);
	}
	idx->count++;
}

uint8_t *name_index_find(void *vidx, const uint8_t *str, uint8_t len) {
	uint8_t *ptr;
	uint32_t hash;
	name_index *idx = (name_index *)vidx;

	do_incremental_rehash(idx);

	hash = hash_function(str, len);

	if (idx->is_rehashing) {
		ptr = hash_find(idx->new_hashtab,idx->new_size,str,len,hash);
		if (ptr!=NULL) {
			return ptr;
		}
	}

	return hash_find(idx->old_hashtab,idx->old_size,str,len,hash);
}
