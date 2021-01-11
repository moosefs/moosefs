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

#ifndef _BUCKETS_H_
#define _BUCKETS_H_


#ifdef BUCKETS_MMAP_ALLOC
#include <sys/mman.h>
#define BUCKETS_ALLOC(size) mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0)
#define BUCKETS_FREE(p,size) munmap(p,size)
#else
#define BUCKETS_ALLOC(size) malloc(size)
#define BUCKETS_FREE(p,size) free(p)
#endif

#define CREATE_BUCKET_ALLOCATOR(allocator_name,element_type,bucket_size) \
typedef struct _##allocator_name##_bucket { \
	element_type bucket[bucket_size]; \
	uint32_t firstfree; \
	struct _##allocator_name##_bucket *next; \
} allocator_name##_bucket; \
static allocator_name##_bucket *allocator_name##_buckets_head = NULL; \
static void *allocator_name##_free_head = NULL; \
static uint64_t allocator_name##_allocated = 0; \
static uint64_t allocator_name##_used = 0; \
static inline void allocator_name##_free_all(void) { \
	allocator_name##_bucket *srb,*nsrb; \
	for (srb = allocator_name##_buckets_head ; srb ; srb = nsrb) { \
		nsrb = srb->next; \
		BUCKETS_FREE(srb,sizeof(allocator_name##_bucket)); \
	} \
	allocator_name##_buckets_head = NULL; \
	allocator_name##_free_head = NULL; \
	allocator_name##_allocated = 0; \
	allocator_name##_used = 0; \
} \
static inline element_type* allocator_name##_malloc() { \
	allocator_name##_bucket *srb; \
	element_type *ret; \
	if (allocator_name##_free_head) { \
		ret = (element_type*)allocator_name##_free_head; \
		allocator_name##_free_head = *((void**)(ret)); \
		allocator_name##_used += sizeof(element_type); \
		return ret; \
	} \
	if (allocator_name##_buckets_head==NULL || allocator_name##_buckets_head->firstfree==(bucket_size)) { \
		srb = (allocator_name##_bucket*)BUCKETS_ALLOC(sizeof(allocator_name##_bucket)); \
		passert(srb); \
		srb->next = allocator_name##_buckets_head; \
		srb->firstfree = 0; \
		allocator_name##_buckets_head = srb; \
		allocator_name##_allocated += sizeof(allocator_name##_bucket); \
	} \
	ret = (allocator_name##_buckets_head->bucket)+(allocator_name##_buckets_head->firstfree); \
	allocator_name##_buckets_head->firstfree++; \
	allocator_name##_used += sizeof(element_type); \
	return ret; \
} \
static inline void allocator_name##_free(element_type *p) { \
	*((void**)p) = allocator_name##_free_head; \
	allocator_name##_free_head = (void*)p; \
	allocator_name##_used -= sizeof(element_type); \
} \
static inline void allocator_name##_getusage(uint64_t *allocated,uint64_t *used) { \
	*allocated = allocator_name##_allocated ; \
	*used = allocator_name##_used ; \
}

#endif
