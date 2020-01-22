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

#include <stdlib.h>
#include <inttypes.h>

#include "massert.h"
#include "mfsalloc.h"

static uint32_t *heap = NULL;
static uint32_t heapsize = 0;
static uint32_t heapelements = 0;

#define PARENT(x) (((x)-1)/2)
#define CHILD(x) (((x)*2)+1)

static inline void heap_sort_down(void) {
	uint32_t l,r,m;
	uint32_t pos=0;
	uint32_t x;
	while (pos<heapelements) {
		l = CHILD(pos);
		r = l+1;
		if (l>=heapelements) {
			return;
		}
		m = l;
		if (r<heapelements && heap[r] < heap[l]) {
			m = r;
		}
		if (heap[pos] <= heap[m]) {
			return;
		}
		x = heap[pos];
		heap[pos] = heap[m];
		heap[m] = x;
		pos = m;
	}
}

static inline void heap_sort_up(void) {
	uint32_t pos=heapelements-1;
	uint32_t p;
	uint32_t x;
	while (pos>0) {
		p = PARENT(pos);
		if (heap[pos] >= heap[p]) {
			return;
		}
		x = heap[pos];
		heap[pos] = heap[p];
		heap[p] = x;
		pos = p;
	}
}

void heap_cleanup(void) {
	heapelements = 0;
}

void heap_push(uint32_t element) {
	if (heapelements>=heapsize) {
		if (heap==NULL) {
			heapsize = 1024;
			heap = malloc(sizeof(uint32_t)*heapsize);
		} else {
			heapsize <<= 1;
			heap = mfsrealloc(heap,sizeof(uint32_t)*heapsize);
		}
		passert(heap);
	}
	heap[heapelements] = element;
	heapelements++;
	heap_sort_up();
}

uint32_t heap_pop(void) {
	uint32_t element;
	if (heapelements==0) {
		return 0;
	}
	element = heap[0];
	heapelements--;
	if (heapelements>0) {
		heap[0] = heap[heapelements];
		heap_sort_down();
	}
	return element;
}

uint32_t heap_elements(void) {
	return heapelements;
}

void heap_term(void) {
	if (heap!=NULL) {
		free(heap);
	}
	heap = NULL;
	heapsize = 0;
	heapelements = 0;
}
