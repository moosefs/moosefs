/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#include <stdlib.h>
#include <inttypes.h>

typedef struct _shp {
	void *pointer;
	void (*freefn)(void*);
	uint32_t refcnt;
} shp;

void* shp_new(void *pointer,void (*freefn)(void*)) {
	shp *s;
	s = malloc(sizeof(shp));
	s->pointer = pointer;
	s->freefn = freefn;
	s->refcnt = 1;
	return s;
}

void* shp_get(void *vs) {
	shp *s = (shp*)vs;
	return s->pointer;
}

void shp_inc(void *vs) {
	shp *s = (shp*)vs;
	s->refcnt++;
}

void shp_dec(void *vs) {
	shp *s = (shp*)vs;
	if (s->refcnt>0) {
		s->refcnt--;
	}
	if (s->refcnt==0) {
		s->freefn(s->pointer);
		free(s);
	}
}
