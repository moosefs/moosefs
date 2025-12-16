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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <inttypes.h>

#include "clocks.h"
#include "chunks.h"
#include "main.h"
#include "cfg.h"

#define HASH_SIZE 4096

typedef struct chunk_prot {
	uint64_t chunkid;
	double ts;
	struct chunk_prot *next;
} chunk_prot;

static chunk_prot* chunk_prot_hashtab[HASH_SIZE];

static uint32_t ProtectionDelay;

// after replication COPY->EC or EC->COPY
void chunk_delay_protect(uint64_t chunkid) {
	uint32_t hash = (chunkid ^ (chunkid>>16)) % HASH_SIZE;
	chunk_prot *cp;

	for (cp=chunk_prot_hashtab[hash] ; cp!=NULL ; cp=cp->next) {
		if (cp->chunkid == chunkid) {
			cp->ts = monotonic_seconds();
			return;
		}
	}
	cp = malloc(sizeof(chunk_prot));
	cp->chunkid = chunkid;
	cp->ts = monotonic_seconds();
	cp->next = chunk_prot_hashtab[hash];
	chunk_prot_hashtab[hash] = cp;
}

// can delete COPIES or EC PARTS?
uint8_t chunk_delay_is_protected(uint64_t chunkid) {
	uint32_t hash = (chunkid ^ (chunkid>>16)) % HASH_SIZE;
	chunk_prot *cp,**cpp;
	double ts;

	ts = monotonic_seconds();
	cpp = chunk_prot_hashtab + hash;
	while ((cp=*cpp)!=NULL) {
		if (cp->chunkid==chunkid) {
			if (cp->ts + ProtectionDelay < ts) {
				*cpp = cp->next;
				free(cp);
				return 0;
			} else {
				return 1;
			}
		}
		cpp = &(cp->next);
	}
	return 0;
}

typedef struct _chunk_group {
	uint64_t chunkidtab[128];
	uint32_t count;
	struct _chunk_group *next;
} chunk_group;

void chunk_delay_remove_old(void) {
	static uint32_t current_index = 0;
	uint32_t i,j;
	double ts;
	chunk_group cghead,*cgcurr,*cgnext;
	chunk_prot *cp,**cpp;

	ts = monotonic_seconds();
	for (i=0 ; i<10 ; i++) {
		cgcurr = &cghead;
		cgcurr->count = 0;
		cgcurr->next = NULL;
		cpp = chunk_prot_hashtab + current_index;
		while ((cp=*cpp)!=NULL) {
			if (cp->ts + ProtectionDelay < ts) {
				if (cgcurr->count>=128) {
					cgcurr->next = malloc(sizeof(chunk_group));
					cgcurr = cgcurr->next;
					cgcurr->count = 0;
					cgcurr->next = NULL;
				}
				cgcurr->chunkidtab[cgcurr->count]=cp->chunkid;
				cgcurr->count++;
				*cpp = cp->next;
				free(cp);
			} else {
				cpp = &(cp->next);
			}
		}
		for (cgcurr=&cghead ; cgcurr!=NULL ; cgcurr=cgcurr->next) {
			for (j=0 ; j<cgcurr->count ; j++) {
				chunk_do_extra_job(cgcurr->chunkidtab[j]);
			}
		}
		for (cgcurr=cghead.next ; cgcurr ; cgcurr=cgnext) {
			cgnext = cgcurr->next;
			free(cgcurr);
		}
		cghead.next = NULL;
		current_index++;
		if (current_index>=HASH_SIZE) {
			current_index = 0;
		}
	}
}

void chunk_delay_reload(void) {
	ProtectionDelay = cfg_getuint32("CHUNK_PROTECTION_SECONDS",15); // debug option
}

void chunk_delay_init(void) {
	uint32_t i;
	for (i=0 ; i<HASH_SIZE ; i++) {
		chunk_prot_hashtab[i]=NULL;
	}
	chunk_delay_reload();
	main_msectime_register(10,0,chunk_delay_remove_old);
	main_reload_register(chunk_delay_reload);
}
