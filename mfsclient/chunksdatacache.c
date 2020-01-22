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

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "massert.h"

// #define CHUNKS_MAX_ENTRIES 1000000
// #define CHUNKS_MAX_TIME 60

#define CHUNKS_INODE_HASH_SIZE 65536
#define CHUNKS_DATA_HASH_SIZE 524288

struct _chunks_inode_entry;

typedef struct _chunks_data_entry {
	uint32_t inode;
	uint32_t chindx;
	uint64_t chunkid;
	uint32_t version;
	uint8_t csdataver;
	uint8_t *csdata;
	uint32_t csdatasize;
	struct _chunks_inode_entry *parent;
	struct _chunks_data_entry **previnode,*nextinode;
	struct _chunks_data_entry **prevdata,*nextdata;
//	struct _chunks_data_entry **prevlru,*nextlru;
} chunks_data_entry;

typedef struct _chunks_inode_entry {
	uint32_t inode;
	struct _chunks_data_entry *data_head;
	struct _chunks_inode_entry **prev,*next;
} chunks_inode_entry;

static chunks_inode_entry **chunks_inode_hash;
static chunks_data_entry **chunks_data_hash;
//static chunks_data_entry *lruhead,**lrutail;
static pthread_mutex_t lock;

//static uint32_t entries;

static inline uint32_t chunks_inode_hash_fn(uint32_t inode) {
	return ((inode*0x72B5F387U)&(CHUNKS_INODE_HASH_SIZE-1));
}

static inline uint32_t chunks_data_hash_fn(uint32_t inode,uint32_t chindx) {
	return ((((inode*0x72B5F387U)+chindx)*0x56BF7623U)&(CHUNKS_DATA_HASH_SIZE-1));
}

static inline void chunks_try_remove_inode(chunks_inode_entry *ih) {
	if (ih->data_head==NULL) {
		*(ih->prev) = ih->next;
		if (ih->next) {
			ih->next->prev = ih->prev;
		}
		free(ih);
	}
}

static inline void chunks_remove_entry(chunks_data_entry *ca) {
	*(ca->previnode) = ca->nextinode;
	if (ca->nextinode) {
		ca->nextinode->previnode = ca->previnode;
	}
	*(ca->prevdata) = ca->nextdata;
	if (ca->nextdata) {
		ca->nextdata->prevdata = ca->prevdata;
	}
//	*(ca->prevlru) = ca->nextlru;
//	if (ca->nextlru) {
//		ca->nextlru->prevlru = ca->prevlru;
//	} else {
//		lrutail = ca->prevlru;
//	}
	if (ca->csdata) {
		free(ca->csdata);
	}
	chunks_try_remove_inode(ca->parent);
	free(ca);
//	entries--;
}

static inline chunks_data_entry* chunks_new_entry(uint32_t inode,uint32_t chindx) {
	chunks_inode_entry *ih;
	chunks_data_entry *ca;
	uint32_t hash,ihash;

	ihash = chunks_inode_hash_fn(inode);
	for (ih = chunks_inode_hash[ihash] ; ih && ih->inode!=inode ; ih = ih->next) {}

	if (ih==NULL) {
		ih = malloc(sizeof(chunks_inode_entry));
		ih->inode = inode;
		ih->data_head = NULL;
		ih->next = chunks_inode_hash[ihash];
		if (ih->next) {
			ih->next->prev = &(ih->next);
		}
		ih->prev = chunks_inode_hash + ihash;
		chunks_inode_hash[ihash] = ih;
	}
	hash = chunks_data_hash_fn(inode,chindx);
	ca = malloc(sizeof(chunks_data_entry));
	ca->inode = inode;
	ca->chindx = chindx;
	ca->chunkid = 0;
	ca->version = 0;
	ca->csdata = NULL;
	ca->csdatasize = 0;
	ca->csdataver = 0;
	ca->parent = ih;
	ca->nextinode = ih->data_head;
	if (ca->nextinode) {
		ca->nextinode->previnode = &(ca->nextinode);
	}
	ca->previnode = &(ih->data_head);
	ih->data_head = ca;
	ca->nextdata = chunks_data_hash[hash];
	if (ca->nextdata) {
		ca->nextdata->prevdata = &(ca->nextdata);
	}
	ca->prevdata = chunks_data_hash + hash;
	chunks_data_hash[hash] = ca;
//	*lrutail = ca;
//	ca->prevlru = lrutail;
//	ca->nextlru = NULL;
//	lrutail = &(ca->nextlru);
	return ca;
}
/*
static inline void chunks_lru_move(chunks_data_entry *ca) {
	if (ca->nextlru==NULL) {
		return;
	}
	*(ca->prevlru) = ca->nextlru;
	ca->nextlru->prevlru = ca->prevlru;
	*lrutail = ca;
	ca->prevlru = lrutail;
	ca->nextlru = NULL;
	lrutail = &(ca->nextlru);
}
*/

// clears all entries with chindx higher or equal to given
void chunksdatacache_clear_inode(uint32_t inode,uint32_t chindx) {
	chunks_inode_entry *ih,*ihn;
	chunks_data_entry *ca,*can;

	pthread_mutex_lock(&lock);
	ih = chunks_inode_hash[chunks_inode_hash_fn(inode)];
	while (ih) {
		ihn = ih->next;
		if (ih->inode==inode) {
			ca = ih->data_head;
			while (ca) {
				can = ca->nextinode;
				if (ca->chindx>=chindx) {
					chunks_remove_entry(ca);
				}
				ca = can;
			}
		}
		ih = ihn;
	}
	pthread_mutex_unlock(&lock);
}

void chunksdatacache_invalidate(uint32_t inode,uint32_t chindx) {
	chunks_data_entry *ca;
	uint32_t hash;

	pthread_mutex_lock(&lock);
	hash = chunks_data_hash_fn(inode,chindx);
	for (ca = chunks_data_hash[hash] ; ca ; ca=ca->nextdata) {
		if (ca->inode==inode && ca->chindx==chindx) {
			chunks_remove_entry(ca);
			pthread_mutex_unlock(&lock);
			return;
		}
	}
	pthread_mutex_unlock(&lock);
}

uint8_t chunksdatacache_check(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version) {
	chunks_data_entry *ca;
	uint32_t hash;

	pthread_mutex_lock(&lock);
	hash = chunks_data_hash_fn(inode,chindx);
	for (ca = chunks_data_hash[hash] ; ca ; ca=ca->nextdata) {
		if (ca->inode==inode && ca->chindx==chindx) {
			if (ca->chunkid==chunkid && ca->version==version) {
				pthread_mutex_unlock(&lock);
				return 1;
			} else {
				pthread_mutex_unlock(&lock);
				return 0;
			}
		}
	}
	pthread_mutex_unlock(&lock);
	return 0;
}

void chunksdatacache_change(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version) {
	chunks_data_entry *ca;
	uint32_t hash;

	pthread_mutex_lock(&lock);
	hash = chunks_data_hash_fn(inode,chindx);
	for (ca = chunks_data_hash[hash] ; ca ; ca=ca->nextdata) {
		if (ca->inode==inode && ca->chindx==chindx) {
			ca->chunkid = chunkid;
			ca->version = version;
			pthread_mutex_unlock(&lock);
			return;
		}
	}
	pthread_mutex_unlock(&lock);
}

void chunksdatacache_insert(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version,uint8_t csdataver,const uint8_t *csdata,uint32_t csdatasize) {
	chunks_data_entry *ca;
	uint32_t hash;

	pthread_mutex_lock(&lock);
	hash = chunks_data_hash_fn(inode,chindx);
	for (ca = chunks_data_hash[hash] ; ca ; ca=ca->nextdata) {
		if (ca->inode==inode && ca->chindx==chindx) {
//			chunks_lru_move(ca);
			break;
		}
	}

	if (ca==NULL) {
//		while (entries>=CHUNKS_MAX_ENTRIES) {
//			chunks_remove_entry(lruhead);
//		}
		ca = chunks_new_entry(inode,chindx);
	}

	ca->chunkid = chunkid;
	ca->version = version;
	ca->csdataver = csdataver;
	if (ca->csdatasize==csdatasize) {
		if (csdatasize>0) {
			memcpy(ca->csdata,csdata,csdatasize);
		}
	} else {
		if (ca->csdata) {
			free(ca->csdata);
		}
		if (csdatasize>0) {
			ca->csdata = malloc(csdatasize);
			memcpy(ca->csdata,csdata,csdatasize);
		} else {
			ca->csdata = NULL;
		}
		ca->csdatasize = csdatasize;
	}
	pthread_mutex_unlock(&lock);
}

uint8_t chunksdatacache_find(uint32_t inode,uint32_t chindx,uint64_t *chunkid,uint32_t *version,uint8_t *csdataver,uint8_t *csdata,uint32_t *csdatasize) {
	chunks_data_entry *ca;
	uint32_t hash;

	pthread_mutex_lock(&lock);
	hash = chunks_data_hash_fn(inode,chindx);
	for (ca = chunks_data_hash[hash] ; ca ; ca=ca->nextdata) {
		if (ca->inode==inode && ca->chindx==chindx) {
//			chunks_lru_move(ca);
			if (*csdatasize < ca->csdatasize) { // not enough space in external buffer
				pthread_mutex_unlock(&lock);
				return 0;
			}
			*chunkid = ca->chunkid;
			*version = ca->version;
			*csdataver = ca->csdataver;
			memcpy(csdata,ca->csdata,ca->csdatasize);
			*csdatasize = ca->csdatasize;
			pthread_mutex_unlock(&lock);
			return 1;
		}
	}
	pthread_mutex_unlock(&lock);
	return 0;
}

void chunksdatacache_cleanup(void) {
	chunks_inode_entry *ih,*ihn;
	chunks_data_entry *ca,*can;
	uint32_t hash;

	pthread_mutex_lock(&lock);
	for (hash = 0 ; hash < CHUNKS_INODE_HASH_SIZE ; hash++) {
		ih = chunks_inode_hash[hash];
		while (ih) {
			ihn = ih->next;
			free(ih);
			ih = ihn;
		}
		chunks_inode_hash[hash] = NULL;
	}
	for (hash = 0 ; hash < CHUNKS_DATA_HASH_SIZE ; hash++) {
		ca = chunks_data_hash[hash];
		while (ca) {
			can = ca->nextdata;
			if (ca->csdata) {
				free(ca->csdata);
			}
			free(ca);
			ca = can;
		}
		chunks_data_hash[hash] = NULL;
	}
//	lruhead = NULL;
//	lrutail = &lruhead;
//	entries = 0;
	pthread_mutex_unlock(&lock);
}

void chunksdatacache_term(void) {
	chunksdatacache_cleanup();
	free(chunks_inode_hash);
	free(chunks_data_hash);
	pthread_mutex_destroy(&lock);
}

void chunksdatacache_init(void) {
	uint32_t hash;

	chunks_inode_hash = malloc(sizeof(chunks_inode_entry*)*CHUNKS_INODE_HASH_SIZE);
	passert(chunks_inode_hash);

	chunks_data_hash = malloc(sizeof(chunks_data_entry*)*CHUNKS_DATA_HASH_SIZE);
	passert(chunks_data_hash);

	for (hash = 0 ; hash < CHUNKS_INODE_HASH_SIZE ; hash++) {
		chunks_inode_hash[hash] = NULL;
	}
	for (hash = 0 ; hash < CHUNKS_DATA_HASH_SIZE ; hash++) {
		chunks_data_hash[hash] = NULL;
	}
//	lruhead = NULL;
//	lrutail = &lruhead;
//	entries = 0;
	pthread_mutex_init(&lock,NULL);
}
