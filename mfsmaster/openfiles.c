/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "openfiles.h"
#include "metadata.h"
#include "sessions.h"
#include "changelog.h"
#include "main.h"
#include "datapack.h"
#include "bio.h"
#include "massert.h"

#define OF_INODE_HASHSIZE 65536
#define OF_SESSION_HASHSIZE 4096

#define OF_INODE_HASH(inode) ((inode)%(OF_INODE_HASHSIZE))
#define OF_SESSION_HASH(sessionid) ((sessionid)%(OF_SESSION_HASHSIZE))

typedef struct _ofrelation {
	uint32_t sessionid;
	uint32_t inode;
	struct _ofrelation *snext,**sprev;
	struct _ofrelation *inext,**iprev;
} ofrelation;

static ofrelation *sessionhash[OF_SESSION_HASHSIZE];
static ofrelation *inodehash[OF_INODE_HASHSIZE];


static inline void of_newnode(uint32_t sessionid,uint32_t inode) {
	ofrelation *ofr;
	uint32_t shashpos = OF_SESSION_HASH(sessionid);
	uint32_t ihashpos = OF_INODE_HASH(inode);

//	syslog(LOG_NOTICE,"new node: sessionid: %"PRIu32", inode: %"PRIu32" , shashpos: %"PRIu32" , ihashpos: %"PRIu32,sessionid,inode,shashpos,ihashpos);

	ofr = (ofrelation*)malloc(sizeof(ofrelation));
	ofr->sessionid = sessionid;
	ofr->inode = inode;

	ofr->inext = inodehash[ihashpos];
	ofr->iprev = inodehash+ihashpos;
	if (ofr->inext) {
		ofr->inext->iprev = &(ofr->inext);
	}
	inodehash[ihashpos] = ofr;

	ofr->snext = sessionhash[shashpos];
	ofr->sprev = sessionhash+shashpos;
	if (ofr->snext) {
		ofr->snext->sprev = &(ofr->snext);
	}
	sessionhash[shashpos] = ofr;
}

static inline void of_delnode(ofrelation *ofr) {
//	syslog(LOG_NOTICE,"del node: sessionid: %"PRIu32", inode: %"PRIu32,ofr->sessionid,ofr->inode);
	*(ofr->iprev) = ofr->inext;
	if (ofr->inext) {
		ofr->inext->iprev = ofr->iprev;
	}
	*(ofr->sprev) = ofr->snext;
	if (ofr->snext) {
		ofr->snext->sprev = ofr->sprev;
	}
	free(ofr);
}

static inline uint8_t of_checknode(uint32_t sessionid,uint32_t inode) {
	ofrelation *ofr;
	uint32_t ihashpos = OF_INODE_HASH(inode);

//	syslog(LOG_NOTICE,"check node: sessionid: %"PRIu32", inode: %"PRIu32" , ihashpos: %"PRIu32,sessionid,inode,ihashpos);
	for (ofr = inodehash[ihashpos] ; ofr ; ofr = ofr->inext) {
//		syslog(LOG_NOTICE,"check node: pointer: %p (sessionid: %"PRIu32", inode: %"PRIu32")",ofr,ofr->sessionid,ofr->inode);
		if (ofr->sessionid==sessionid && ofr->inode==inode) {
//			syslog(LOG_NOTICE,"check node: found");
			return 1;
		}
	}
//	syslog(LOG_NOTICE,"check node: not found");
	return 0;
}


static inline int32_t of_bisearch(uint32_t search,const uint32_t *array,uint32_t n) {
	int32_t first,last,middle;

	first = 0;
	last = n - 1;
	middle = (first + last) / 2;

	while (first<=last) {
		if (array[middle] < search) {
			first = middle + 1;
		} else if (array[middle] > search) {
			last = middle - 1;
		} else {
			return middle;
		}
		middle = (first + last) / 2;
	}
	return -1;
}

int of_inodecmp(const void *a,const void *b) {
	uint32_t aa = *((const uint32_t*)a);
	uint32_t bb = *((const uint32_t*)b);
	if (aa<bb) {
		return -1;
	} else if (aa>bb) {
		return 1;
	} else {
		return 0;
	}
}


void of_sync(uint32_t sessionid,uint32_t *inodes,uint32_t inodecnt) {
	static uint32_t *bitmask=NULL;
	static uint32_t bitmasksize=0;
	ofrelation *ofr,*nofr;
	int32_t ipos;
	uint32_t i;
	uint32_t shashpos = OF_SESSION_HASH(sessionid);

	if (inodecnt > bitmasksize*32) {
		if (bitmask) {
			free(bitmask);
		}
		bitmasksize = ((inodecnt+31)/32)+10;
		bitmask = malloc(sizeof(uint32_t)*bitmasksize);
		passert(bitmask);
	}

	memset(bitmask,0,sizeof(uint32_t)*bitmasksize);

	qsort(inodes,inodecnt,sizeof(uint32_t),of_inodecmp);

//	for (i=0 ; i<inodecnt ; i++) {
//		syslog(LOG_NOTICE,"sync: inodes[%"PRIu32"]=%"PRIu32,i,inodes[i]);
//	}

	for (ofr = sessionhash[shashpos] ; ofr ; ofr = nofr) {
		nofr = ofr->snext;
		if (ofr->sessionid==sessionid) {
			ipos = of_bisearch(ofr->inode,inodes,inodecnt);
//			syslog(LOG_NOTICE,"sync: search for %"PRIu32" -> pos: %"PRId32,ofr->inode,ipos);
			if (ipos<0) { // close
				changelog("%"PRIu32"|RELEASE(%"PRIu32",%"PRIu32")",main_time(),ofr->sessionid,ofr->inode);
				of_delnode(ofr);
			} else {
				bitmask[ipos>>5] |= (1U<<(ipos&0x1F));
			}
		}
	}

//	for (i=0 ; i<bitmasksize ; i++) {
//		syslog(LOG_NOTICE,"sync: bitmask[%"PRIu32"]=%"PRIX32,i,bitmask[i]);
//	}

	for (i=0 ; i<inodecnt ; i++) {
		if ((bitmask[i>>5] & (1U<<(i&0x1F)))==0) {
			changelog("%"PRIu32"|ACQUIRE(%"PRIu32",%"PRIu32")",main_time(),sessionid,inodes[i]);
			of_newnode(sessionid,inodes[i]);
		}
	}
}


void of_openfile(uint32_t sessionid,uint32_t inode) {
	if (of_checknode(sessionid,inode)==0) {
		changelog("%"PRIu32"|ACQUIRE(%"PRIu32",%"PRIu32")",main_time(),sessionid,inode);
		of_newnode(sessionid,inode);
	}
}

void of_sessionremoved(uint32_t sessionid) {
	ofrelation *ofr,*nofr;
	uint32_t shashpos = OF_SESSION_HASH(sessionid);

	for (ofr = sessionhash[shashpos] ; ofr ; ofr = nofr) {
		nofr = ofr->snext;
		if (ofr->sessionid==sessionid) {
			of_delnode(ofr);
		}
	}
}

uint8_t of_isfileopened(uint32_t inode) {
	ofrelation *ofr;
	uint32_t ihashpos = OF_INODE_HASH(inode);

	for (ofr = inodehash[ihashpos] ; ofr ; ofr = ofr->inext) {
		if (ofr->inode==inode) {
			return 1;
		}
	}
	return 0;
}

uint32_t of_noofopenedfiles(uint32_t sessionid) {
	ofrelation *ofr;
	uint32_t shashpos = OF_SESSION_HASH(sessionid);
	uint32_t cnt = 0;

	for (ofr = sessionhash[shashpos] ; ofr ; ofr = ofr->snext) {
		if (ofr->sessionid==sessionid) {
			cnt++;
		}
	}
	return cnt;
}

int of_mr_acquire(uint32_t sessionid,uint32_t inode) {
	if (of_checknode(sessionid,inode)) {
		return ERROR_MISMATCH;
	}
	of_newnode(sessionid,inode);
	meta_version_inc();
	return STATUS_OK;
}

int of_mr_release(uint32_t sessionid,uint32_t inode) {
	ofrelation *ofr,*nofr;
	uint32_t ihashpos = OF_INODE_HASH(inode);

	for (ofr = inodehash[ihashpos] ; ofr ; ofr = nofr) {
		nofr = ofr->inext;
		if (ofr->sessionid==sessionid && ofr->inode==inode) {
			of_delnode(ofr);
			meta_version_inc();
			return STATUS_OK;
		}
	}
	return ERROR_MISMATCH;
}

#define OF_STORE_BLOCK_CNT 256
#define OF_REC_SIZE 8

uint8_t of_store(bio *fd) {
	uint8_t storebuff[OF_REC_SIZE*OF_STORE_BLOCK_CNT];
	uint8_t *ptr;
	ofrelation *ofr;
	uint32_t i,j;
	uint32_t sessionid,inode;

	if (fd==NULL) {
		return 0x10;
	}
	j=0;
	ptr = storebuff;
	for (i=0 ; i<OF_SESSION_HASHSIZE ; i++) {
		for (ofr = sessionhash[i] ; ofr ; ofr=ofr->snext) {
			sessionid = ofr->sessionid;
			inode = ofr->inode;
			put32bit(&ptr,sessionid);
			put32bit(&ptr,inode);
			j++;
			if (j==OF_STORE_BLOCK_CNT) {
				if (bio_write(fd,storebuff,OF_REC_SIZE*OF_STORE_BLOCK_CNT)!=(OF_REC_SIZE*OF_STORE_BLOCK_CNT)) {
					return 0xFF;
				}
				j=0;
				ptr = storebuff;
			}
		}
	}
	memset(ptr,0,OF_REC_SIZE);
	j++;
	if (bio_write(fd,storebuff,OF_REC_SIZE*j)!=(OF_REC_SIZE*j)) {
		return 0xFF;
	}
	return 0;
}

int of_load(bio *fd,uint8_t mver) {
	uint8_t loadbuff[OF_REC_SIZE];
	const uint8_t *ptr;
	int32_t r;
	uint32_t sessionid,inode;

	(void)mver;

	for (;;) {
		r = bio_read(fd,loadbuff,OF_REC_SIZE);
		if (r!=OF_REC_SIZE) {
			return -1;
		}
		ptr = loadbuff;
		sessionid = get32bit(&ptr);
		inode = get32bit(&ptr);
		if (sessionid>0 && inode>0) {
			if (sessions_find_session(sessionid)!=NULL) {
				of_newnode(sessionid,inode);
			}
		} else if (sessionid==0 && inode==0) {
			return 0;
		} else {
			return -1;
		}
	}
	return 0;	// unreachable
}

void of_cleanup(void) {
	ofrelation *ofr,*nofr;
	uint32_t i;

	for (i=0 ; i<OF_SESSION_HASHSIZE ; i++) {
		for (ofr = sessionhash[i] ; ofr ; ofr=nofr) {
			nofr = ofr->snext;
			free(ofr);
		}
		sessionhash[i] = NULL;
	}
	for (i=0 ; i<OF_INODE_HASHSIZE ; i++) {
		inodehash[i] = NULL;
	}
}

int of_init(void) {
	uint32_t i;

	for (i=0 ; i<OF_SESSION_HASHSIZE ; i++) {
		sessionhash[i] = NULL;
	}
	for (i=0 ; i<OF_INODE_HASHSIZE ; i++) {
		inodehash[i] = NULL;
	}
	return 0;
}
