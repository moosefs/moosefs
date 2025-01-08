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
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <inttypes.h>

#define HASHSIZE 1024

typedef struct _busy_chunk {
	void *packet;
	uint64_t chunkid;
	struct _busy_chunk *next,**prev;
} busy_chunk;

static busy_chunk *bchashmap[HASHSIZE];

static inline uint32_t busychunk_hashfn(uint64_t chunkid) {
	return chunkid%HASHSIZE;
}

void* busychunk_start(void *packet,uint64_t chunkid) {
	busy_chunk *bc;
	uint32_t hash;

	chunkid &= 0x00FFFFFFFFFFFFFF;
	hash = busychunk_hashfn(chunkid);

	bc = malloc(sizeof(busy_chunk));
	bc->packet = packet;
	bc->chunkid = chunkid;
	bc->next = bchashmap[hash];
	bc->prev = bchashmap + hash;
	bchashmap[hash] = bc;
	if (bc->next!=NULL) {
		bc->next->prev = &(bc->next);
	}
	return bc;
}

void* busychunk_end(void *vbc) {
	busy_chunk *bc = (busy_chunk*)vbc;
	void *packet;

	packet = bc->packet;

	*(bc->prev) = bc->next;
	if (bc->next != NULL) {
		bc->next->prev = bc->prev;
	}
	free(bc);
	return packet;
}

uint8_t busychunk_isbusy(uint64_t chunkid) {
	busy_chunk *bc;
	uint32_t hash;

	chunkid &= 0x00FFFFFFFFFFFFFF;
	hash = busychunk_hashfn(chunkid);

	for (bc = bchashmap[hash] ; bc!=NULL ; bc=bc->next) {
		if (bc->chunkid == chunkid) {
			return 1;
		}
	}
	return 0;
}

void busychunk_init(void) {
	uint32_t i;

	for (i=0 ; i<HASHSIZE ; i++) {
		bchashmap[i] = NULL;
	}
}
