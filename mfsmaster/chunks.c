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

#define BUCKETS_MMAP_ALLOC 1
#define HASHTAB_PREALLOC 1
#define CHUNKHASH_MOVEFACTOR 5

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "MFSCommunication.h"

#include "main.h"
#include "cfg.h"
#include "bio.h"
#include "metadata.h"
#include "matocsserv.h"
#include "matoclserv.h"
#include "changelog.h"
#include "csdb.h"
#include "random.h"
#include "topology.h"
#include "chunks.h"
#include "filesystem.h"
#include "datapack.h"
#include "massert.h"
#include "hashfn.h"
#include "buckets.h"
#include "clocks.h"

#define MINLOOPTIME 60
#define MAXLOOPTIME 7200
#define MAXCPS 10000000
#define MINCPS 10000
#define TICKSPERSECOND 50

#define NEWCHUNKDELAY 150
#define LOSTCHUNKDELAY 50

#define HASHTAB_LOBITS 24
#define HASHTAB_HISIZE (0x80000000>>(HASHTAB_LOBITS))
#define HASHTAB_LOSIZE (1<<HASHTAB_LOBITS)
#define HASHTAB_MASK (HASHTAB_LOSIZE-1)
#define HASHTAB_MOVEFACTOR 5

// #define DISCLOOPRATIO 0x400

//#define HASHSIZE 0x100000
//#define HASHPOS(chunkid) (((uint32_t)chunkid)&0xFFFFF)

//#define DISCLOOPELEMENTS (HASHSIZE/0x400)


enum {JOBS_INIT,JOBS_EVERYLOOP,JOBS_EVERYTICK};

/* chunk.operation */
enum {NONE=0,CREATE,SET_VERSION,DUPLICATE,TRUNCATE,DUPTRUNC,REPLICATE};

static const char* opstr[] = {
	"NONE",
	"CREATE",
	"SET_VERSION",
	"DUPLICATE",
	"TRUNCATE",
	"DUPLICATE+TRUNCATE",
	"REPLICATE"
};

/* slist.valid */
/* INVALID - wrong version / or got info from chunkserver (IO error etc.)  ->  to delete */
/* DEL - deletion in progress */
/* BUSY - operation in progress */
/* VALID - ok */
/* TDBUSY - to be deleted + operation in progress */
/* TDVALID - ok, to be deleted */
enum {INVALID,DEL,BUSY,VALID,TDBUSY,TDVALID};

typedef struct _discserv {
	uint16_t csid;
	struct _discserv *next;
} discserv;

static discserv *discservers = NULL;
static discserv *discservers_next = NULL;
static uint32_t discserverspos = 0;

/*
typedef struct _bcdata {
	void *ptr;
	uint32_t version;
} bcdata;
*/
/*
typedef struct _hintlist {
	uint32_t ip;
	uint16_t port;
	struct _hintlist *next;
} hintlist;
*/
typedef struct _slist {
	uint16_t csid;
	uint8_t valid;
	uint32_t version;
	struct _slist *next;
} slist;

/*
#define SLIST_BUCKET_SIZE 5000

typedef struct _slist_bucket {
	slist bucket[SLIST_BUCKET_SIZE];
	uint32_t firstfree;
	struct _slist_bucket *next;
} slist_bucket;

static slist_bucket *sbhead = NULL;
static slist *slfreehead = NULL;
*/

typedef struct chunk {
	uint64_t chunkid;
	uint32_t version;
	uint8_t goal;
	uint8_t allvalidcopies;
	uint8_t regularvalidcopies;
	unsigned ondangerlist:1;
	unsigned needverincrease:1;
	unsigned interrupted:1;
	unsigned operation:4;
	uint32_t lockedto;
	uint32_t fcount;
	slist *slisthead;
	uint32_t *ftab;
	struct chunk *next;
} chunk;

/*
#define CHUNK_BUCKET_SIZE 20000
typedef struct _chunk_bucket {
	chunk bucket[CHUNK_BUCKET_SIZE];
	uint32_t firstfree;
	struct _chunk_bucket *next;
} chunk_bucket;

static chunk_bucket *cbhead = NULL;
static chunk *chfreehead = NULL;
*/
static chunk **chunkhashtab[HASHTAB_HISIZE];
static uint32_t chunkrehashpos;
static uint32_t chunkhashsize;
static uint32_t chunkhashelem;

static uint64_t nextchunkid=1;
#define LOCKTIMEOUT 120

#define UNUSED_DELETE_TIMEOUT (86400*7)

typedef struct _csopchunk {
	uint64_t chunkid;
	struct _csopchunk *next;
} csopchunk;

typedef struct _csdata {
	void *ptr;
	csopchunk *opchunks;
	uint8_t valid;
	uint8_t registered;
	uint8_t newchunkdelay;
	uint8_t lostchunkdelay;
	uint32_t next;
	uint32_t prev;
} csdata;

static csdata *cstab = NULL;
static uint32_t csfreehead = 0;
static uint32_t csusedhead = MAXCSCOUNT;
static uint32_t opsinprogress = 0;
static uint16_t csregisterinprogress = 0;
static uint8_t csreceivingchunks = 0;

#define DANGER_PRIORITIES 5

static chunk** chunks_priority_queue[DANGER_PRIORITIES];
static uint32_t chunks_priority_leng[DANGER_PRIORITIES];
static uint32_t chunks_priority_head[DANGER_PRIORITIES];
static uint32_t chunks_priority_tail[DANGER_PRIORITIES];

// static uint32_t ReplicationsDelayDisconnect=3600;
static uint32_t ReplicationsDelayInit=60;
static uint32_t AcceptUnknownChunkDelay=300;
static uint32_t RemoveDelayDisconnect=3600;
static uint32_t DangerMaxLeng=1000000;

static double MaxWriteRepl[4];
static double MaxReadRepl[4];
static uint32_t MaxDelSoftLimit;
static uint32_t MaxDelHardLimit;
static double TmpMaxDelFrac;
static uint32_t TmpMaxDel;
static uint32_t LoopTimeMin;
//static uint32_t HashSteps;
static uint32_t HashCPTMax;
static double AcceptableDifference;

static uint32_t jobshpos;

static uint32_t starttime;

typedef struct _job_info {
	uint32_t del_invalid;
	uint32_t del_unused;
	uint32_t del_diskclean;
	uint32_t del_overgoal;
	uint32_t copy_undergoal;
} job_info;

typedef struct _loop_info {
	job_info done,notdone;
	uint32_t locked_unused;
	uint32_t locked_used;
	uint32_t copy_rebalance;
} loop_info;

static loop_info chunksinfo = {{0,0,0,0,0},{0,0,0,0,0},0,0,0};
static uint32_t chunksinfo_loopstart=0,chunksinfo_loopend=0;

static uint64_t lastchunkid=0;
static chunk* lastchunkptr=NULL;

static uint32_t chunks;

static uint32_t last_rebalance=0;

uint32_t allchunkcounts[11][11];
uint32_t regularchunkcounts[11][11];

static uint32_t stats_deletions=0;
static uint32_t stats_replications=0;

void chunk_stats(uint32_t *del,uint32_t *repl) {
	*del = stats_deletions;
	*repl = stats_replications;
	stats_deletions = 0;
	stats_replications = 0;
}

CREATE_BUCKET_ALLOCATOR(slist,slist,5000)

CREATE_BUCKET_ALLOCATOR(chunk,chunk,20000)

void chunk_get_memusage(uint64_t allocated[3],uint64_t used[3]) {
	allocated[0] = sizeof(chunk*)*chunkrehashpos;
	used[0] = sizeof(chunk*)*chunkhashelem;
	chunk_getusage(allocated+1,used+1);
	slist_getusage(allocated+2,used+2);
}

/*
static inline uint32_t chunk_calc_hash_size(uint32_t elements) {
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
*/

static inline void chunk_hash_init(void) {
	uint16_t i;
	chunkhashsize = 0;
	chunkhashelem = 0;
	chunkrehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		chunkhashtab[i] = NULL;
	}
}

static inline void chunk_hash_cleanup(void) {
	uint16_t i;
	chunkhashelem = 0;
	chunkhashsize = 0;
	chunkrehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (chunkhashtab[i]!=NULL) {
#ifdef HAVE_MMAP
			munmap(chunkhashtab[i],sizeof(chunk*)*HASHTAB_LOSIZE);
#else
			free(chunkhashtab[i]);
#endif
		}
		chunkhashtab[i] = NULL;
	}
}

static inline void chunk_hash_rehash(void) {
	uint16_t i;
	chunkrehashpos = chunkhashsize;
	chunkhashsize *= 2;
	for (i=(chunkhashsize>>HASHTAB_LOBITS)/2 ; i<chunkhashsize>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
		chunkhashtab[i] = mmap(NULL,sizeof(chunk*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		chunkhashtab[i] = malloc(sizeof(chunk*)*HASHTAB_LOSIZE);
#endif
		passert(chunkhashtab[i]);
	}
}

static inline void chunk_hash_move(void) {
	uint32_t hash;
	uint32_t mask;
	uint32_t moved=0;
	chunk **chptr,**chptralt,*c;
	mask = chunkhashsize-1;
	do {
		if (chunkrehashpos>=chunkhashsize) { // rehash complete
			chunkrehashpos = chunkhashsize;
			return;
		}
		chptr = chunkhashtab[(chunkrehashpos - (chunkhashsize/2)) >> HASHTAB_LOBITS] + (chunkrehashpos & HASHTAB_MASK);
		chptralt = chunkhashtab[chunkrehashpos >> HASHTAB_LOBITS] + (chunkrehashpos & HASHTAB_MASK);
		*chptralt = NULL;
		while ((c=*chptr)!=NULL) {
			hash = hash32(c->chunkid) & mask;
			if (hash==chunkrehashpos) {
				*chptralt = c;
				*chptr = c->next;
				chptralt = &(c->next);
				c->next = NULL;
			} else {
				chptr = &(c->next);
			}
			moved++;
		}
		chunkrehashpos++;
	} while (moved<CHUNKHASH_MOVEFACTOR);
}

static inline chunk* chunk_hash_find(uint64_t chunkid) {
	chunk *c;
	uint32_t hash;

	if (chunkhashsize==0) {
		return NULL;
	}
	hash = hash32(chunkid) & (chunkhashsize-1);
	if (chunkrehashpos<chunkhashsize) {
		chunk_hash_move();
		if (hash >= chunkrehashpos) {
			hash -= chunkhashsize/2;
		}
	}
	for (c=chunkhashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] ; c ; c=c->next) {
		if (c->chunkid==chunkid) {
			return c;
		}
	}
	return NULL;
}

static inline void chunk_hash_delete(chunk *c) {
	chunk **chptr,*cit;
	uint32_t hash;

	if (chunkhashsize==0) {
		return;
	}
	hash = hash32(c->chunkid) & (chunkhashsize-1);
	if (chunkrehashpos<chunkhashsize) {
		chunk_hash_move();
		if (hash >= chunkrehashpos) {
			hash -= chunkhashsize/2;
		}
	}
	chptr = chunkhashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	while ((cit=*chptr)!=NULL) {
		if (cit==c) {
			*chptr = c->next;
			chunkhashelem--;
			return;
		}
		chptr = &(cit->next);
	}
}

static inline void chunk_hash_add(chunk *c) {
	uint16_t i;
	uint32_t hash;

	if (chunkhashsize==0) {
		chunkhashsize = HASHTAB_LOSIZE; //chunk_calc_hash_size(maxnodeid);
		chunkrehashpos = chunkhashsize;
		chunkhashelem = 0;
		for (i=0 ; i<chunkhashsize>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
			chunkhashtab[i] = mmap(NULL,sizeof(chunk*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
			chunkhashtab[i] = malloc(sizeof(chunk*)*HASHTAB_LOSIZE);
#endif
			passert(chunkhashtab[i]);
			memset(chunkhashtab[i],0,sizeof(chunk*));
			if (chunkhashtab[i][0]==NULL) {
				memset(chunkhashtab[i],0,sizeof(chunk*)*HASHTAB_LOSIZE);
			} else {
				for (hash=0 ; hash<HASHTAB_LOSIZE ; hash++) {
					chunkhashtab[i][hash] = NULL;
				}
			}
		}
	}
	hash = hash32(c->chunkid) & (chunkhashsize-1);
	if (chunkrehashpos<chunkhashsize) {
		chunk_hash_move();
		if (hash >= chunkrehashpos) {
			hash -= chunkhashsize/2;
		}
		c->next = chunkhashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		chunkhashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = c;
		chunkhashelem++;
	} else {
		c->next = chunkhashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		chunkhashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = c;
		chunkhashelem++;
		if (chunkhashelem>chunkhashsize) {
			chunk_hash_rehash();
		}
	}
}

chunk* chunk_new(uint64_t chunkid) {
	chunk *newchunk;
	newchunk = chunk_malloc();
//#ifdef METARESTORE
//	printf("N%"PRIu64"\n",chunkid);
//#endif
	chunks++;
	allchunkcounts[0][0]++;
	regularchunkcounts[0][0]++;
	newchunk->chunkid = chunkid;
	newchunk->version = 0;
	newchunk->goal = 0;
	newchunk->lockedto = 0;
	newchunk->allvalidcopies = 0;
	newchunk->regularvalidcopies = 0;
	newchunk->needverincrease = 1;
	newchunk->ondangerlist = 0;
	newchunk->interrupted = 0;
	newchunk->operation = NONE;
	newchunk->slisthead = NULL;
	newchunk->fcount = 0;
//	newchunk->flisthead = NULL;
	newchunk->ftab = NULL;
	lastchunkid = chunkid;
	lastchunkptr = newchunk;
	chunk_hash_add(newchunk);
	return newchunk;
}

chunk* chunk_find(uint64_t chunkid) {
	chunk *c;
//#ifdef METARESTORE
//	printf("F%"PRIu64"\n",chunkid);
//#endif
	if (lastchunkid==chunkid) {
		return lastchunkptr;
	}
	c = chunk_hash_find(chunkid);
	if (c) {
		lastchunkid = chunkid;
		lastchunkptr = c;
	}
	return c;
}

void chunk_delete(chunk* c) {
	if (lastchunkptr==c) {
		lastchunkid=0;
		lastchunkptr=NULL;
	}
	chunks--;
	allchunkcounts[c->goal][0]--;
	regularchunkcounts[c->goal][0]--;
	chunk_hash_delete(c);
	chunk_free(c);
}

static inline void chunk_state_change(uint8_t oldgoal,uint8_t newgoal,uint8_t oldavc,uint8_t newavc,uint8_t oldrvc,uint8_t newrvc) {
	if (oldgoal>9) {
		oldgoal=10;
	}
	if (newgoal>9) {
		newgoal=10;
	}
	if (oldavc>9) {
		oldavc=10;
	}
	if (newavc>9) {
		newavc=10;
	}
	if (oldrvc>9) {
		oldrvc=10;
	}
	if (newrvc>9) {
		newrvc=10;
	}
	allchunkcounts[oldgoal][oldavc]--;
	allchunkcounts[newgoal][newavc]++;
	regularchunkcounts[oldgoal][oldrvc]--;
	regularchunkcounts[newgoal][newrvc]++;
}

uint32_t chunk_count(void) {
	return chunks;
}

void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regularvalidcopies) {
	uint32_t i,j,ag,rg;
	*allchunks = chunks;
	*allcopies = 0;
	*regularvalidcopies = 0;
	for (i=1 ; i<=10 ; i++) {
		ag=0;
		rg=0;
		for (j=0 ; j<=10 ; j++) {
			ag += allchunkcounts[j][i];
			rg += regularchunkcounts[j][i];
		}
		*allcopies += ag*i;
		*regularvalidcopies += rg*i;
	}
}

uint32_t chunk_get_missing_count(void) {
	uint32_t res=0;
	uint8_t i;

	for (i=1 ; i<=10 ; i++) {
		res+=allchunkcounts[i][0];
	}
	return res;
}

uint8_t chunk_counters_in_progress(void) {
//	syslog(LOG_NOTICE,"discservers: %p , discservers_next: %p , csregisterinprogress: %"PRIu16,discservers,discservers_next,csregisterinprogress);
	return ((discservers!=NULL || discservers_next!=NULL)?1:0)|((csregisterinprogress>0)?2:0)|csreceivingchunks;
}

void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid) {
	uint8_t i,j;
	if (matrixid==0) {
		for (i=0 ; i<=10 ; i++) {
			for (j=0 ; j<=10 ; j++) {
				put32bit(&buff,allchunkcounts[i][j]);
			}
		}
	} else if (matrixid==1) {
		for (i=0 ; i<=10 ; i++) {
			for (j=0 ; j<=10 ; j++) {
				put32bit(&buff,regularchunkcounts[i][j]);
			}
		}
	} else {
		memset(buff,0,11*11*4);
	}
}

/* --- */

static inline void chunk_priority_enqueue(uint8_t j,chunk *c) {
	uint32_t h,l;
	if (c->ondangerlist) {
		return;
	}
	l = chunks_priority_leng[j];
	h = chunks_priority_head[j];
	if (l>=DangerMaxLeng) {
		if (chunks_priority_queue[j][h]!=NULL) {
			chunks_priority_queue[j][h]->ondangerlist=0;
		}
	}
	chunks_priority_queue[j][h] = c;
	c->ondangerlist = 1;
	h = (h+1)%DangerMaxLeng;
	chunks_priority_head[j] = h;
	if (l<DangerMaxLeng) {
		chunks_priority_leng[j] = l+1;
	} else {
		chunks_priority_tail[j] = h;
	}
}

static inline void chunk_priority_queue_check(chunk *c) {
	slist *s;
	uint32_t vc,tdc;
	uint8_t j;

	vc = 0;
	tdc = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		switch (s->valid) {
		case TDVALID:
			tdc++;
			break;
		case VALID:
			vc++;
			break;
		}
	}
	if (vc+tdc > 0 && vc < c->goal) { // undergoal chunk
		if (vc==0 && tdc==1) { // highest priority - chunks only on one disk "marked for removal"
			j = 0;
		} else if (vc==1 && tdc==0) { // next priority - chunks only on one regular disk
			j = 1;
		} else if (vc==1 && tdc>0) { // next priority - chunks on one regular disk and some "marked for removal" disks
			j = 2;
		} else if (tdc>0) { // next priority - chunks on "marked for removal" disks
			j = 3;
		} else { // lowest priority - standard undergoal chunks
			j = 4;
		}
		chunk_priority_enqueue(j,c);
	}
}

/* --- */

void chunk_addopchunk(uint16_t csid,uint64_t chunkid) {
	csopchunk *csop;
	csop = malloc(sizeof(csopchunk));
	csop->chunkid = chunkid;
	csop->next = cstab[csid].opchunks;
	cstab[csid].opchunks = csop;
	opsinprogress++;
	return;
}

void chunk_delopchunk(uint16_t csid,uint64_t chunkid) {
	csopchunk **csopp,*csop;

	csopp = &(cstab[csid].opchunks);
	while ((csop = (*csopp))) {
		if (csop->chunkid == chunkid) {
			*csopp = csop->next;
			free(csop);
			if (opsinprogress>0) {
				opsinprogress--;
			}
		} else {
			csopp = &(csop->next);
		}
	}
}

void chunk_emergency_increase_version(chunk *c) {
	slist *s;
	uint32_t i;
	i=0;
//	chunk_remove_diconnected_chunks(c);
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL) {
			if (s->valid==TDVALID || s->valid==TDBUSY) {
				s->valid = TDBUSY;
			} else {
				s->valid = BUSY;
			}
			s->version = c->version+1;
			matocsserv_send_setchunkversion(cstab[s->csid].ptr,c->chunkid,c->version+1,c->version);
			chunk_addopchunk(s->csid,c->chunkid);
			i++;
		}
	}
	if (i>0) {	// should always be true !!!
		c->interrupted = 0;
		c->operation = SET_VERSION;
		c->version++;
		fs_incversion(c->chunkid);
	} else {
		matoclserv_chunk_status(c->chunkid,ERROR_CHUNKLOST);
	}
}

static inline int chunk_remove_diconnected_chunks(chunk *c) {
	uint8_t opfinished,validcopies,disc;
	slist *s,**st;

	if (discservers==NULL && discservers_next==NULL) {
		return 0;
	}
	disc = 0;
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (!cstab[s->csid].valid) {
			if (s->valid==TDBUSY || s->valid==TDVALID) {
				chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
				c->allvalidcopies--;
			}
			if (s->valid==BUSY || s->valid==VALID) {
				chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
				c->allvalidcopies--;
				c->regularvalidcopies--;
			}
			c->needverincrease = 1;
			*st = s->next;
			slist_free(s);
			disc = 1;
		} else {
			st = &(s->next);
		}
	}
	if (disc==0) {
		return 0;
	}
	if (c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && ((csdb_getdisconnecttime()+RemoveDelayDisconnect)<main_time())) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		chunk_delete(c);
		return 1;
	}
	if (c->operation!=NONE) {
		validcopies=0;
		opfinished=1;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==BUSY || s->valid==TDBUSY) {
				opfinished=0;
			}
			if (s->valid==VALID || s->valid==TDVALID) {
				validcopies=1;
			}
		}
		if (opfinished) {
			if (c->operation!=REPLICATE) {
				if (validcopies) {
					chunk_emergency_increase_version(c);
				} else {
					matoclserv_chunk_status(c->chunkid,ERROR_NOTDONE);
					c->operation = NONE;
				}
			} else {
				c->operation = NONE;
				c->lockedto = 0;
			}
		} else {
			if (c->operation!=REPLICATE) {
				c->interrupted = 1;
			}
		}
	}
	chunk_priority_queue_check(c);
	return 0;
}

int chunk_mr_increase_version(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->version++;
	return STATUS_OK;
}

/* --- */

int chunk_change_file(uint64_t chunkid,uint8_t prevgoal,uint8_t newgoal) {
	chunk *c;
	uint8_t oldgoal;

	if (prevgoal==newgoal) {
		return STATUS_OK;
	}
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->fcount==0) {
		syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",c->chunkid);
		return ERROR_CHUNKLOST;	// ERROR_STRUCTURE
	}
	oldgoal = c->goal;
	if (c->fcount==1) {
		c->goal = newgoal;
	} else {
		if (c->ftab==NULL) {
			c->ftab = malloc(sizeof(uint32_t)*10);
			passert(c->ftab);
			memset(c->ftab,0,sizeof(uint32_t)*10);
			c->ftab[c->goal]=c->fcount-1;
			c->ftab[newgoal]=1;
			if (newgoal > c->goal) {
				c->goal = newgoal;
			}
		} else {
			c->ftab[prevgoal]--;
			c->ftab[newgoal]++;
			c->goal = 9;
			while (c->ftab[c->goal]==0) {
				c->goal--;
			}
		}
	}
	if (oldgoal!=c->goal) {
		chunk_state_change(oldgoal,c->goal,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
	}
	chunk_priority_queue_check(c);
	return STATUS_OK;
}

static inline int chunk_delete_file_int(chunk *c,uint8_t goal) {
	uint8_t oldgoal;

	if (c->fcount==0) {
		syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",c->chunkid);
		return ERROR_CHUNKLOST;	// ERROR_STRUCTURE
	}
	oldgoal = c->goal;
	if (c->fcount==1) {
		c->goal = 0;
		c->fcount = 0;
//#ifdef METARESTORE
//		printf("D%"PRIu64"\n",c->chunkid);
//#endif
	} else {
		if (c->ftab) {
			c->ftab[goal]--;
			c->goal = 9;
			while (c->ftab[c->goal]==0) {
				c->goal--;
			}
		}
		c->fcount--;
		if (c->fcount==1 && c->ftab) {
			free(c->ftab);
			c->ftab = NULL;
		}
	}
	if (oldgoal!=c->goal) {
		chunk_state_change(oldgoal,c->goal,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
	}
	return STATUS_OK;
}

static inline int chunk_add_file_int(chunk *c,uint8_t goal) {
	uint8_t oldgoal;

	oldgoal = c->goal;
	if (c->fcount==0) {
		c->goal = goal;
		c->fcount = 1;
	} else if (goal==c->goal) {
		c->fcount++;
		if (c->ftab) {
			c->ftab[goal]++;
		}
	} else {
		if (c->ftab==NULL) {
			c->ftab = malloc(sizeof(uint32_t)*10);
			passert(c->ftab);
			memset(c->ftab,0,sizeof(uint32_t)*10);
			c->ftab[c->goal]=c->fcount;
			c->ftab[goal]=1;
			c->fcount++;
			if (goal > c->goal) {
				c->goal = goal;
			}
		} else {
			c->ftab[goal]++;
			c->fcount++;
			c->goal = 9;
			while (c->ftab[c->goal]==0) {
				c->goal--;
			}
		}
	}
	if (oldgoal!=c->goal) {
		chunk_state_change(oldgoal,c->goal,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
	}
	return STATUS_OK;
}

int chunk_delete_file(uint64_t chunkid,uint8_t goal) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	return chunk_delete_file_int(c,goal);
}

int chunk_add_file(uint64_t chunkid,uint8_t goal) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	return chunk_add_file_int(c,goal);
}

int chunk_unlock(uint64_t chunkid) {
	chunk *c;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->lockedto=0;
	chunk_priority_queue_check(c);
	return STATUS_OK;
}

int chunk_mr_unlock(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->lockedto=0;
	return STATUS_OK;
}

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies) {
	chunk *c;
	*vcopies = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	*vcopies = c->allvalidcopies;
	return STATUS_OK;
}

int chunk_univ_multi_modify(uint32_t ts,uint8_t mr,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t *opflag) {
	uint16_t csids[MAXCSCOUNT];
	uint16_t servcount=0;
	uint32_t vc;
	slist *os,*s;
	uint32_t i;
	chunk *oc,*c;
	uint8_t csstable;

	if (ts>(starttime+60) && csregisterinprogress==0) {
		csstable = 1;
	} else {
		csstable = 0;
	}

	if (ochunkid==0) {	// new chunk
		if (mr==0) {
			servcount = matocsserv_getservers_wrandom(csids,AcceptableDifference*1.5,goal);
			if (servcount==0) {
				uint16_t uscount,tscount;
				double minusage,maxusage;
				matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);
				if (uscount>0 && csstable) {
					return ERROR_NOSPACE;
				} else {
					return ERROR_NOCHUNKSERVERS;
				}
			}
			c = chunk_new(nextchunkid++);
			c->version = 1;
			c->interrupted = 0;
			c->operation = CREATE;
			chunk_add_file_int(c,goal);
			if (servcount<goal) {
				c->allvalidcopies = servcount;
				c->regularvalidcopies = servcount;
			} else {
				c->allvalidcopies = goal;
				c->regularvalidcopies = goal;
			}
			for (i=0 ; i<c->allvalidcopies ; i++) {
				s = slist_malloc();
				s->csid = csids[i];
				s->valid = BUSY;
				s->version = c->version;
				s->next = c->slisthead;
				c->slisthead = s;
				matocsserv_send_createchunk(cstab[s->csid].ptr,c->chunkid,c->version);
				chunk_addopchunk(s->csid,c->chunkid);
			}
			chunk_state_change(c->goal,c->goal,0,c->allvalidcopies,0,c->regularvalidcopies);
			*opflag=1;
			*nchunkid = c->chunkid;
		} else {
			if (*nchunkid != nextchunkid) {
				return ERROR_MISMATCH;
			}
			c = chunk_new(nextchunkid++);
			c->version = 1;
			chunk_add_file_int(c,goal);
		}
	} else {
		c = NULL;
		oc = chunk_find(ochunkid);
		if (oc && mr==0) {
			if (chunk_remove_diconnected_chunks(oc)) {
				oc = NULL;
			}
		}
		if (oc==NULL) {
			return ERROR_NOCHUNK;
		}
		if (mr==0 && oc->lockedto>=ts) {
			return ERROR_LOCKED;
		}
		if (oc->fcount==1) {
			c = oc;
			if (mr==0) {
				*nchunkid = ochunkid;
				if (c->operation!=NONE) {
					return ERROR_CHUNKBUSY;
				}
				if (csstable==0) {
					vc = 0;
					for (s=c->slisthead ; s ; s=s->next) {
						if (s->valid==VALID) {
							vc++;
						}
					}
					if (vc < c->goal) {
						return ERROR_LOCKED; // just try again later
					}
				}
				if (c->needverincrease) {
					i=0;
					for (s=c->slisthead ;s ; s=s->next) {
						if (s->valid!=INVALID && s->valid!=DEL) {
							if (s->valid==TDVALID || s->valid==TDBUSY) {
								s->valid = TDBUSY;
							} else {
								s->valid = BUSY;
							}
							s->version = c->version+1;
							matocsserv_send_setchunkversion(cstab[s->csid].ptr,ochunkid,c->version+1,c->version);
							chunk_addopchunk(s->csid,c->chunkid);
							i++;
						}
					}
					if (i>0) {
						c->interrupted = 0;
						c->operation = SET_VERSION;
						c->version++;
						*opflag=1;
					} else {
						if (csstable) {
							return ERROR_CHUNKLOST;
						} else {
							return ERROR_CSNOTPRESENT;
						}
					}
				} else {
					*opflag=0;
				}
			} else {
				if (*nchunkid != ochunkid) {
					return ERROR_MISMATCH;
				}
				if (*opflag) {
					c->version++;
				}
			}
		} else {
			if (oc->fcount==0) {	// it's serious structure error
				if (mr==0) {
					syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",ochunkid);
				} else {
					printf("serious structure inconsistency: (chunkid:%016"PRIX64")\n",ochunkid);
				}
				return ERROR_CHUNKLOST;	// ERROR_STRUCTURE
			}
			if (mr==0) {
				if (oc->operation!=NONE) {
					return ERROR_CHUNKBUSY;
				}
				if (csstable==0) {
					vc = 0;
					for (os=oc->slisthead ; os ; os=os->next) {
						if (os->valid==VALID) {
							vc++;
						}
					}
					if (vc < oc->goal) {
						return ERROR_LOCKED; // just try again later
					}
				}
				i=0;
				for (os=oc->slisthead ;os ; os=os->next) {
					if (os->valid!=INVALID && os->valid!=DEL) {
						if (c==NULL) {
							c = chunk_new(nextchunkid++);
							c->version = 1;
							c->interrupted = 0;
							c->operation = DUPLICATE;
							chunk_delete_file_int(oc,goal);
							chunk_add_file_int(c,goal);
						}
						s = slist_malloc();
						s->csid = os->csid;
						s->valid = BUSY;
						s->version = c->version;
						s->next = c->slisthead;
						c->slisthead = s;
						c->allvalidcopies++;
						c->regularvalidcopies++;
						matocsserv_send_duplicatechunk(cstab[s->csid].ptr,c->chunkid,c->version,oc->chunkid,oc->version);
						chunk_addopchunk(s->csid,c->chunkid);
						i++;
					}
				}
				if (c!=NULL) {
					chunk_state_change(c->goal,c->goal,0,c->allvalidcopies,0,c->regularvalidcopies);
				}
				if (i>0) {
					*nchunkid = c->chunkid;
					*opflag=1;
				} else {
					if (csstable) {
						return ERROR_CHUNKLOST;
					} else {
						return ERROR_CSNOTPRESENT;
					}
				}
			} else {
				if (*nchunkid != nextchunkid) {
					return ERROR_MISMATCH;
				}
				c = chunk_new(nextchunkid++);
				c->version = 1;
				chunk_delete_file_int(oc,goal);
				chunk_add_file_int(c,goal);
				*nchunkid = c->chunkid;
			}
		}
	}

	c->lockedto=ts+LOCKTIMEOUT;
	return STATUS_OK;
}

int chunk_multi_modify(uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t *opflag) {
	return chunk_univ_multi_modify(main_time(),0,nchunkid,ochunkid,goal,opflag);
}

int chunk_mr_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t opflag) {
	return chunk_univ_multi_modify(ts,1,nchunkid,ochunkid,goal,&opflag);
}

int chunk_univ_multi_truncate(uint32_t ts,uint8_t mr,uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t goal) {
	slist *os,*s;
	uint32_t i;
	chunk *oc,*c;
	uint8_t csstable;
	uint32_t vc;

	if (ts>(starttime+60) && csregisterinprogress==0) {
		csstable = 1;
	} else {
		csstable = 0;
	}

	c=NULL;
	oc = chunk_find(ochunkid);
	if (oc && mr==0) {
		if (chunk_remove_diconnected_chunks(oc)) {
			oc = NULL;
		}
	}

	if (oc==NULL) {
		return ERROR_NOCHUNK;
	}
	if (mr==0 && oc->lockedto>=ts) {
		return ERROR_LOCKED;
	}
	if (oc->fcount==1) {
		c = oc;
		if (mr==0) {
			*nchunkid = ochunkid;
			if (c->operation!=NONE) {
				return ERROR_CHUNKBUSY;
			}
			if (csstable==0) {
				vc = 0;
				for (os=oc->slisthead ; os ; os=os->next) {
					if (os->valid==VALID) {
						vc++;
					}
				}
				if (vc < oc->goal) {
					return ERROR_LOCKED; // just try again later
				}
			}
			i=0;
			for (s=c->slisthead ;s ; s=s->next) {
				if (s->valid!=INVALID && s->valid!=DEL) {
					if (s->valid==TDVALID || s->valid==TDBUSY) {
						s->valid = TDBUSY;
					} else {
						s->valid = BUSY;
					}
					s->version = c->version+1;
					matocsserv_send_truncatechunk(cstab[s->csid].ptr,ochunkid,length,c->version+1,c->version);
					chunk_addopchunk(s->csid,c->chunkid);
					i++;
				}
			}
			if (i>0) {
				c->interrupted = 0;
				c->operation = TRUNCATE;
				c->version++;
			} else {
				if (csstable) {
					return ERROR_CHUNKLOST;
				} else {
					return ERROR_CSNOTPRESENT;
				}
			}
		} else {
			if (*nchunkid != ochunkid) {
				return ERROR_MISMATCH;
			}
			c->version++;
		}
	} else {
		if (oc->fcount==0) {	// it's serious structure error
			if (mr==0) {
				syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",ochunkid);
			} else {
				printf("serious structure inconsistency: (chunkid:%016"PRIX64")\n",ochunkid);
			}
			return ERROR_CHUNKLOST;	// ERROR_STRUCTURE
		}
		if (mr==0) {
			if (oc->operation!=NONE) {
				return ERROR_CHUNKBUSY;
			}
			if (csstable==0) {
				vc = 0;
				for (os=oc->slisthead ; os ; os=os->next) {
					if (os->valid==VALID) {
						vc++;
					}
				}
				if (vc < oc->goal) {
					return ERROR_LOCKED; // just try again later
				}
			}
			i=0;
			for (os=oc->slisthead ;os ; os=os->next) {
				if (os->valid!=INVALID && os->valid!=DEL) {
					if (c==NULL) {
						c = chunk_new(nextchunkid++);
						c->version = 1;
						c->interrupted = 0;
						c->operation = DUPTRUNC;
						chunk_delete_file_int(oc,goal);
						chunk_add_file_int(c,goal);
					}
					s = slist_malloc();
					s->csid = os->csid;
					s->valid = BUSY;
					s->version = c->version;
					s->next = c->slisthead;
					c->slisthead = s;
					c->allvalidcopies++;
					c->regularvalidcopies++;
					matocsserv_send_duptruncchunk(cstab[s->csid].ptr,c->chunkid,c->version,oc->chunkid,oc->version,length);
					chunk_addopchunk(s->csid,c->chunkid);
					i++;
				}
			}
			if (c!=NULL) {
				chunk_state_change(c->goal,c->goal,0,c->allvalidcopies,0,c->regularvalidcopies);
			}
			if (i>0) {
				*nchunkid = c->chunkid;
			} else {
				if (csstable) {
					return ERROR_CHUNKLOST;
				} else {
					return ERROR_CSNOTPRESENT;
				}
			}
		} else {
			if (*nchunkid != nextchunkid) {
				return ERROR_MISMATCH;
			}
			c = chunk_new(nextchunkid++);
			c->version = 1;
			chunk_delete_file_int(oc,goal);
			chunk_add_file_int(c,goal);
			*nchunkid = c->chunkid;
		}
	}

	c->lockedto=ts+LOCKTIMEOUT;
	return STATUS_OK;
}

int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t goal) {
	return chunk_univ_multi_truncate(main_time(),0,nchunkid,ochunkid,length,goal);
}

int chunk_mr_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal) {
	return chunk_univ_multi_truncate(ts,1,nchunkid,ochunkid,0,goal);
}

int chunk_repair(uint8_t goal,uint64_t ochunkid,uint32_t *nversion) {
	uint32_t bestversion;
	chunk *c;
	slist *s;

	*nversion=0;
	if (ochunkid==0) {
		return 0;	// not changed
	}

	c = chunk_find(ochunkid);
	if (c==NULL) {	// no such chunk - erase (nchunkid already is 0 - so just return with "changed" status)
		return 1;
	}
	if (c->lockedto>=(uint32_t)main_time()) { // can't repair locked chunks - but if it's locked, then likely it doesn't need to be repaired
		return 0;
	}
	bestversion = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (cstab[s->csid].valid) {
			if (s->valid == VALID || s->valid == TDVALID || s->valid == BUSY || s->valid == TDBUSY) {	// found chunk that is ok - so return
				return 0;
			}
			if (s->valid == INVALID) {
				if (s->version>=bestversion) {
					bestversion = s->version;
				}
			}
		}
	}
	if (bestversion==0) {	// didn't find sensible chunk - so erase it
		chunk_delete_file_int(c,goal);
		return 1;
	}
	if (c->allvalidcopies>0 || c->regularvalidcopies>0) {
		if (c->allvalidcopies>0) {
			syslog(LOG_WARNING,"wrong all valid copies counter - (counter value: %u, should be: 0) - fixed",c->allvalidcopies);
		}
		if (c->regularvalidcopies>0) {
			syslog(LOG_WARNING,"wrong regular valid copies counter - (counter value: %u, should be: 0) - fixed",c->regularvalidcopies);
		}
		chunk_state_change(c->goal,c->goal,c->allvalidcopies,0,c->regularvalidcopies,0);
		c->allvalidcopies = 0;
		c->regularvalidcopies = 0;
	}
	c->version = bestversion;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid == INVALID && s->version==bestversion && cstab[s->csid].valid) {
			s->valid = VALID;
			c->allvalidcopies++;
			c->regularvalidcopies++;
		}
	}
	*nversion = bestversion;
	chunk_state_change(c->goal,c->goal,0,c->allvalidcopies,0,c->regularvalidcopies);
	c->needverincrease = 1;
	return 1;
}

int chunk_mr_set_version(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	c->version = version;
	return STATUS_OK;
}

/* ---- */

typedef struct locsort {
	uint32_t ip;
	uint16_t port;
	uint32_t csver;
	uint32_t dist;
	uint32_t rnd;
} locsort;

int chunk_locsort_cmp(const void *aa,const void *bb) {
	const locsort *a = (const locsort*)aa;
	const locsort *b = (const locsort*)bb;
	if (a->dist<b->dist) {
		return -1;
	} else if (a->dist>b->dist) {
		return 1;
	} else if (a->rnd<b->rnd) {
		return -1;
	} else if (a->rnd>b->rnd) {
		return 1;
	}
	return 0;
}

uint8_t chunk_get_version_and_csdata(uint8_t mode,uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t cs_data[100*10]) {
	chunk *c;
	slist *s;
	uint8_t i;
	uint8_t cnt;
	uint8_t *wptr;
	locsort lstab[100];

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	*version = c->version;
	cnt=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && cstab[s->csid].valid) {
			if (cnt<100 && matocsserv_get_csdata(cstab[s->csid].ptr,&(lstab[cnt].ip),&(lstab[cnt].port),&(lstab[cnt].csver))==0) {
				lstab[cnt].dist = topology_distance(lstab[cnt].ip,cuip);	// in the future prepare more sofisticated distance function
				lstab[cnt].rnd = rndu32();
				cnt++;
			}
		}
	}
	if (cnt==0) {
		*count = 0;
		if (chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
			return ERROR_CHUNKLOST; // this is permanent state - chunk is definitely lost
		} else {
			return STATUS_OK;
		}
	}
	qsort(lstab,cnt,sizeof(locsort),chunk_locsort_cmp);
	wptr = cs_data;
	for (i=0 ; i<cnt ; i++) {
		put32bit(&wptr,lstab[i].ip);
		put16bit(&wptr,lstab[i].port);
		if (mode) {
			put32bit(&wptr,lstab[i].csver);
		}
	}
	*count = cnt;
	return STATUS_OK;
}

/* ---- */

int chunk_mr_nextchunkid(uint64_t nchunkid) {
	if (nchunkid>nextchunkid) {
		nextchunkid=nchunkid;
		meta_version_inc();
		return STATUS_OK;
	} else {
		return ERROR_MISMATCH;
	}
}

int chunk_mr_chunkadd(uint64_t chunkid,uint32_t version,uint32_t lockedto) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c) {
		return ERROR_CHUNKEXIST;
	}
	if (chunkid>nextchunkid+UINT64_C(1000000000)) {
		return ERROR_MISMATCH;
	}
	if (chunkid>=nextchunkid) {
		nextchunkid=chunkid+1;
	}
	c = chunk_new(chunkid);
	c->version = version;
	c->lockedto = lockedto;
	meta_version_inc();
	return STATUS_OK;
}

int chunk_mr_chunkdel(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ERROR_NOCHUNK;
	}
	if (c->version != version) {
		return ERROR_WRONGVERSION;
	}
	if (c->fcount!=0) {
		return ERROR_ACTIVE;
	}
	if (c->slisthead!=NULL) {
		return ERROR_CHUNKBUSY;
	}
	chunk_delete(c);
	meta_version_inc();
	return STATUS_OK;
}

void chunk_server_has_chunk(uint16_t csid,uint64_t chunkid,uint32_t version) {
	chunk *c;
	slist *s;

	cstab[csid].newchunkdelay = NEWCHUNKDELAY;
	csreceivingchunks |= 2;

	c = chunk_find(chunkid);
	if (c) {
		if (chunk_remove_diconnected_chunks(c)) {
			c = NULL;
		}
	}

	if (c==NULL) {
		if (chunkid>nextchunkid+UINT64_C(1000000000)) {
			syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"_%08"PRIX32"), id looks wrong - just ignore it",chunkid,version);
			return;
		}
		syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"_%08"PRIX32"), so create it for future deletion",chunkid,version);
		if (chunkid>=nextchunkid) {
			nextchunkid=chunkid+1;
//			changelog("%"PRIu32"|NEXTCHUNKID(%"PRIu64")",main_time(),nextchunkid);
		}
		c = chunk_new(chunkid);
		c->version = version;
		if (starttime+AcceptUnknownChunkDelay>main_time()) {
			c->lockedto = (uint32_t)main_time()+UNUSED_DELETE_TIMEOUT;
		} else {
			c->lockedto = 0;
		}
		changelog("%"PRIu32"|CHUNKADD(%"PRIu64",%"PRIu32",%"PRIu32")",main_time(),c->chunkid,c->version,c->lockedto);
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->csid==csid) {
			return;
		}
	}
	s = slist_malloc();
	s->csid = csid;
	if (c->version!=(version&0x7FFFFFFF)) {
		s->valid = INVALID;
		s->version = version&0x7FFFFFFF;
	} else {
		if (version&0x80000000) {
			s->valid=TDVALID;
			s->version = c->version;
			chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies);
			c->allvalidcopies++;
		} else {
			s->valid=VALID;
			s->version = c->version;
			chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
			c->allvalidcopies++;
			c->regularvalidcopies++;
		}
	}
	s->next = c->slisthead;
	c->slisthead = s;
	c->needverincrease = 1;
}

void chunk_damaged(uint16_t csid,uint64_t chunkid) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		if (chunkid>nextchunkid+UINT64_C(1000000000)) {
			syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"), id looks wrong - just ignore it",chunkid);
			return;
		}
		syslog(LOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"), so create it for future deletion",chunkid);
		if (chunkid>=nextchunkid) {
			nextchunkid=chunkid+1;
//			changelog("%"PRIu32"|NEXTCHUNKID(%"PRIu64")",main_time(),nextchunkid);
		}
		c = chunk_new(chunkid);
		c->version = 0;
		changelog("%"PRIu32"|CHUNKADD(%"PRIu64",%"PRIu32",%"PRIu32")",main_time(),c->chunkid,c->version,c->lockedto);
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->csid==csid) {
			if (s->valid==TDBUSY || s->valid==TDVALID) {
				chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
				c->allvalidcopies--;
			}
			if (s->valid==BUSY || s->valid==VALID) {
				chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
				c->allvalidcopies--;
				c->regularvalidcopies--;
			}
			s->valid = INVALID;
			s->version = 0;
			c->needverincrease = 1;
			chunk_priority_queue_check(c);
			return;
		}
	}
	s = slist_malloc();
	s->csid = csid;
	s->valid = INVALID;
	s->version = 0;
	s->next = c->slisthead;
	c->needverincrease = 1;
	c->slisthead = s;
}

void chunk_lost(uint16_t csid,uint64_t chunkid) {
	chunk *c;
	slist **sptr,*s;

	cstab[csid].lostchunkdelay = LOSTCHUNKDELAY;
	csreceivingchunks |= 1;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return;
	}
	sptr=&(c->slisthead);
	while ((s=*sptr)) {
		if (s->csid==csid) {
			if (s->valid==TDBUSY || s->valid==TDVALID) {
				chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
				c->allvalidcopies--;
			}
			if (s->valid==BUSY || s->valid==VALID) {
				chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
				c->allvalidcopies--;
				c->regularvalidcopies--;
			}
			c->needverincrease = 1;
			*sptr = s->next;
			slist_free(s);
		} else {
			sptr = &(s->next);
		}
	}
	if (c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && ((csdb_getdisconnecttime()+RemoveDelayDisconnect)<main_time())) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		chunk_delete(c);
	} else {
		chunk_priority_queue_check(c);
	}
}

static inline void chunk_server_remove_csid(uint16_t csid) {
	if (cstab[csid].prev<MAXCSCOUNT) {
		cstab[cstab[csid].prev].next = cstab[csid].next;
	} else {
		csusedhead = cstab[csid].next;
	}
	if (cstab[csid].next<MAXCSCOUNT) {
		cstab[cstab[csid].next].prev = cstab[csid].prev;
	}
	cstab[csid].next = csfreehead;
	cstab[csid].prev = MAXCSCOUNT;
	csfreehead = csid;
}

static inline uint16_t chunk_server_new_csid(void) {
	uint16_t csid;

	csid = csfreehead;
	csfreehead = cstab[csid].next;
	cstab[csfreehead].prev = MAXCSCOUNT;
	if (csusedhead<MAXCSCOUNT) {
		cstab[csusedhead].prev = csid;
	}
	cstab[csid].next = csusedhead;
	cstab[csid].prev = MAXCSCOUNT;
	csusedhead = csid;
	return csid;
}

static inline void chunk_server_check_delays(void) {
	uint16_t csid;

	csreceivingchunks = 0;
	for (csid = csusedhead ; csid < MAXCSCOUNT ; csid = cstab[csid].next) {
		if (cstab[csid].newchunkdelay>0) {
			cstab[csid].newchunkdelay--;
			csreceivingchunks |= 2;
		}
		if (cstab[csid].lostchunkdelay>0) {
			cstab[csid].lostchunkdelay--;
			csreceivingchunks |= 1;
		}
	}
}

uint16_t chunk_server_connected(void *ptr) {
	uint16_t csid;

	csid = chunk_server_new_csid();
	cstab[csid].ptr = ptr;
	cstab[csid].opchunks = NULL;
	cstab[csid].valid = 1;
	cstab[csid].registered = 0;
	csregisterinprogress += 1;
	return csid;
}

void chunk_server_register_end(uint16_t csid) {
	if (cstab[csid].registered==0 && cstab[csid].valid==1) {
		cstab[csid].registered = 1;
		csregisterinprogress -= 1;
	}
}

void chunk_server_disconnected(uint16_t csid) {
	discserv *ds;
	csopchunk *csop;
	chunk *c;

	ds = malloc(sizeof(discserv));
	ds->csid = csid;
	ds->next = discservers_next;
	discservers_next = ds;
	fs_cs_disconnected();
	cstab[csid].valid = 0;
	if (cstab[csid].registered==0) {
		csregisterinprogress -= 1;
	}
	csop = cstab[csid].opchunks;
	while (csop) {
		c = chunk_find(csop->chunkid);
		if (c) {
			chunk_remove_diconnected_chunks(c);
		}
		csop = csop->next;
		if (opsinprogress>0) {
			opsinprogress--;
		}
	}
	cstab[csid].opchunks = NULL;
}

void chunk_server_disconnection_loop(void) {
	uint32_t i;
	chunk *c,*cn;
	discserv *ds;
	uint64_t startutime,currutime;

	if (discservers) {
		startutime = monotonic_useconds();
		currutime = startutime;
		while (startutime+10000>currutime) {
			for (i=0 ; i<1000 ; i++) {
				if (discserverspos<chunkrehashpos) {
					for (c=chunkhashtab[discserverspos>>HASHTAB_LOBITS][discserverspos&HASHTAB_MASK] ; c ; c=cn ) {
						cn = c->next;
						chunk_remove_diconnected_chunks(c);
					}
					discserverspos++;
				} else {
					while (discservers) {
						ds = discservers;
						discservers = ds->next;
						chunk_server_remove_csid(ds->csid);
						matocsserv_disconnection_finished(cstab[csfreehead].ptr);
						free(ds);
					}
					return;
				}
			}
			currutime = monotonic_useconds();
		}
	} else if (discservers_next) {
		discservers = discservers_next;
		discservers_next = NULL;
		discserverspos = 0;
	}
}

void chunk_got_delete_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk *c;
	slist *s,**st;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	if (status!=STATUS_OK && status!=ERROR_NOCHUNK) { // treat here ERROR_NOCHUNK as ok
		return ;
	}
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (s->csid == csid) {
			if (s->valid!=DEL) {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
					c->allvalidcopies--;
				}
				if (s->valid==BUSY || s->valid==VALID) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
				}
				syslog(LOG_WARNING,"got unexpected delete status");
			}
			*st = s->next;
			slist_free(s);
		} else {
			st = &(s->next);
		}
	}
	if (c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && ((csdb_getdisconnecttime()+RemoveDelayDisconnect)<main_time())) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		chunk_delete(c);
	}
}

void chunk_got_replicate_status(uint16_t csid,uint64_t chunkid,uint32_t version,uint8_t status) {
	chunk *c;
	slist *s;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	if (c->operation==REPLICATE) { // high priority replication
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->csid == csid) {
				if (s->valid!=BUSY) {
					syslog(LOG_WARNING,"got replication status from server not signed as busy !!!");
				}
				if (status!=0 || version!=c->version) {
					if (s->valid==TDBUSY || s->valid==TDVALID) {
						chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies--;
					}
					if (s->valid==BUSY || s->valid==VALID) {
						chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
						c->allvalidcopies--;
						c->regularvalidcopies--;
					}
					s->valid=INVALID;
					s->version = 0;	// after unfinished operation can't be shure what version chunk has
				} else {
					if (s->valid == BUSY || s->valid == VALID) {
						s->valid = VALID;
					}
				}
				chunk_delopchunk(s->csid,c->chunkid);
			} else if (s->valid==BUSY) {
				syslog(LOG_WARNING,"got replication status from one server, but another is signed as busy !!!");
			}
		}
		c->operation = NONE;
		c->lockedto = 0;
	} else { // low priority replication
		if (status!=0) {
			return ;
		}
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->csid == csid) {
				syslog(LOG_WARNING,"got replication status from server which had had that chunk before (chunk:%016"PRIX64"_%08"PRIX32")",chunkid,version);
				if (s->valid==VALID && version!=c->version) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
					s->valid = INVALID;
					s->version = version;
				}
				return;
			}
		}
		s = slist_malloc();
		s->csid = csid;
		if (c->lockedto>=(uint32_t)main_time() || version!=c->version) {
			s->valid = INVALID;
		} else {
			chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
			c->allvalidcopies++;
			c->regularvalidcopies++;
			s->valid = VALID;
		}
		s->version = version;
		s->next = c->slisthead;
		c->slisthead = s;
	}
}


void chunk_operation_status(chunk *c,uint8_t status,uint16_t csid) {
	uint8_t opfinished,validcopies;
	slist *s;

	if (chunk_remove_diconnected_chunks(c)) {
		return;
	}
//	for (s=c->slisthead ; s ; s=s->next) {
//		if (!cstab[s->csid].valid) {
//			c->interrupted = 1;
//		}
//	}

	validcopies=0;
	opfinished=1;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->csid == csid) {
			if (status!=0) {
				c->interrupted = 1;	// increase version after finish, just in case
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
					c->allvalidcopies--;
				}
				if (s->valid==BUSY || s->valid==VALID) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
				}
				s->valid=INVALID;
				s->version = 0;	// after unfinished operation can't be shure what version chunk has
			} else {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					s->valid=TDVALID;
				} else {
					s->valid=VALID;
				}
			}
			chunk_delopchunk(s->csid,c->chunkid);
		}
		if (s->valid==BUSY || s->valid==TDBUSY) {
			opfinished=0;
		}
		if (s->valid==VALID || s->valid==TDVALID) {
			validcopies=1;
		}
	}
	if (opfinished) {
		if (validcopies) {
			if (c->interrupted) {
				chunk_emergency_increase_version(c);
			} else {
				matoclserv_chunk_status(c->chunkid,STATUS_OK);
				c->operation = NONE;
				c->needverincrease = 0;
			}
		} else {
			matoclserv_chunk_status(c->chunkid,ERROR_NOTDONE);
			c->operation = NONE;
		}
	}
}

void chunk_got_chunkop_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,csid);
}

void chunk_got_create_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,csid);
}

void chunk_got_duplicate_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,csid);
}

void chunk_got_setversion_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,csid);
}

void chunk_got_truncate_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,csid);
}

void chunk_got_duptrunc_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	chunk_operation_status(c,status,csid);
}

uint8_t chunk_no_more_pending_jobs(void) {
	return (opsinprogress==0)?1:0;
}

/* ----------------------- */
/* JOBS (DELETE/REPLICATE) */
/* ----------------------- */

void chunk_store_info(uint8_t *buff) {
	put32bit(&buff,chunksinfo_loopstart);
	put32bit(&buff,chunksinfo_loopend);
	put32bit(&buff,chunksinfo.done.del_invalid);
	put32bit(&buff,chunksinfo.notdone.del_invalid);
	put32bit(&buff,chunksinfo.done.del_unused);
	put32bit(&buff,chunksinfo.notdone.del_unused);
	put32bit(&buff,chunksinfo.done.del_diskclean);
	put32bit(&buff,chunksinfo.notdone.del_diskclean);
	put32bit(&buff,chunksinfo.done.del_overgoal);
	put32bit(&buff,chunksinfo.notdone.del_overgoal);
	put32bit(&buff,chunksinfo.done.copy_undergoal);
	put32bit(&buff,chunksinfo.notdone.copy_undergoal);
	put32bit(&buff,chunksinfo.copy_rebalance);
	put32bit(&buff,chunksinfo.locked_unused);
	put32bit(&buff,chunksinfo.locked_used);
}

//jobs state: jobshpos

void chunk_do_jobs(chunk *c,uint16_t scount,uint16_t fullservers,double minusage,double maxusage,uint32_t now,uint8_t extrajob) {
	slist *s;
	static uint16_t *dcsids;
	static uint16_t dservcount;
	static uint16_t *bcsids;
	static uint16_t bservcount;
	static uint32_t min,max;
	static uint16_t *rcsids;
	uint16_t rservcount;
	uint16_t srccsid,dstcsid;
	uint16_t i,j;
	uint32_t vc,tdc,ivc,bc,tdb,dc;
	static loop_info inforec;
	static uint32_t delnotdone;
	static uint32_t deldone;
	static uint32_t prevtodeletecount;
	static uint32_t delloopcnt;

	if (c==NULL) {
		if (scount==JOBS_INIT) { // init tasks
			delnotdone = 0;
			deldone = 0;
			prevtodeletecount = 0;
			delloopcnt = 0;
			memset(&inforec,0,sizeof(loop_info));
			dcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
			passert(dcsids);
			bcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
			passert(bcsids);
			rcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
			passert(rcsids);
			dservcount=0;
			bservcount=0;
			min=0;
			max=0;
		} else if (scount==JOBS_EVERYLOOP) { // every loop tasks
			delloopcnt++;
			if (delloopcnt>=16) {
				uint32_t todeletecount = deldone+delnotdone;
				delloopcnt=0;
				if ((delnotdone > deldone) && (todeletecount > prevtodeletecount)) {
					TmpMaxDelFrac *= 1.5;
					if (TmpMaxDelFrac>MaxDelHardLimit) {
						syslog(LOG_NOTICE,"DEL_LIMIT hard limit (%"PRIu32" per server) reached",MaxDelHardLimit);
						TmpMaxDelFrac=MaxDelHardLimit;
					}
					TmpMaxDel = TmpMaxDelFrac;
					syslog(LOG_NOTICE,"DEL_LIMIT temporary increased to: %"PRIu32" per server",TmpMaxDel);
				}
				if ((todeletecount < prevtodeletecount) && (TmpMaxDelFrac > MaxDelSoftLimit)) {
					TmpMaxDelFrac /= 1.5;
					if (TmpMaxDelFrac<MaxDelSoftLimit) {
						syslog(LOG_NOTICE,"DEL_LIMIT back to soft limit (%"PRIu32" per server)",MaxDelSoftLimit);
						TmpMaxDelFrac = MaxDelSoftLimit;
					}
					TmpMaxDel = TmpMaxDelFrac;
					syslog(LOG_NOTICE,"DEL_LIMIT decreased back to: %"PRIu32" per server",TmpMaxDel);
				}
				prevtodeletecount = todeletecount;
				delnotdone = 0;
				deldone = 0;
			}
			chunksinfo = inforec;
			memset(&inforec,0,sizeof(inforec));
			chunksinfo_loopstart = chunksinfo_loopend;
			chunksinfo_loopend = now;
		} else if (scount==JOBS_EVERYTICK) { // every second tasks
			dservcount=0;
			bservcount=0;
		}
		return;
	}

// step 0. remove all disconnected copies from structures
	if (chunk_remove_diconnected_chunks(c)) {
		return;
	}

// step 1. calculate number of valid and invalid copies
	vc = 0;
	tdc = 0;
	ivc = 0;
	bc = 0;
	tdb = 0;
	dc = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		switch (s->valid) {
		case INVALID:
			ivc++;
			break;
		case TDVALID:
			tdc++;
			break;
		case VALID:
			vc++;
			break;
		case TDBUSY:
			tdb++;
			break;
		case BUSY:
			bc++;
			break;
		case DEL:
			dc++;
			break;
		}
	}
	if (c->allvalidcopies!=vc+tdc+bc+tdb) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": wrong all valid copies counter - (counter value: %u, should be: %u) - fixed",c->chunkid,c->version,c->allvalidcopies,vc+tdc+bc+tdb);
		chunk_state_change(c->goal,c->goal,c->allvalidcopies,vc+tdc+bc+tdb,c->regularvalidcopies,c->regularvalidcopies);
		c->allvalidcopies = vc+tdc+bc+tdb;
	}
	if (c->regularvalidcopies!=vc+bc) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": wrong regular valid copies counter - (counter value: %u, should be: %u) - fixed",c->chunkid,c->version,c->regularvalidcopies,vc+bc);
		chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,vc+bc);
		c->regularvalidcopies = vc+bc;
	}
	if (tdb+bc==0 && c->operation!=NONE) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk in middle of operation %s, but no chunk server is busy - finish operation",c->chunkid,c->version,opstr[c->operation]);
		c->operation = NONE;
	}
	if (tdb+bc>0 && c->operation==NONE) {
		if (tdc+vc>0) {
			syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": unexpected BUSY copies - fixing",c->chunkid,c->version);
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid == BUSY) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
					s->valid = INVALID;
					s->version = 0;
				} else if (s->valid == TDBUSY) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
					c->allvalidcopies--;
					s->valid = INVALID;
					s->version = 0;
				}
			}
		} else {
			syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": unexpected BUSY copies - can't fix",c->chunkid,c->version);
		}
	}

//	syslog(LOG_WARNING,"chunk %016"PRIX64": ivc=%"PRIu32" , tdc=%"PRIu32" , vc=%"PRIu32" , bc=%"PRIu32" , tdb=%"PRIu32" , dc=%"PRIu32" , goal=%"PRIu8" , scount=%"PRIu16,c->chunkid,ivc,tdc,vc,bc,tdb,dc,c->goal,scount);

// step 2. check number of copies
	if (tdc+vc+tdb+bc==0 && ivc>0 && c->fcount>0/* c->flisthead */) {
		if (ivc>=c->goal) {
			uint32_t bestversion;
			bestversion = 0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid == INVALID) {
					if (s->version>=bestversion) {
						bestversion = s->version;
					}
				}
			}
			if (bestversion>0 && (bestversion+1)==c->version) {
				syslog(LOG_WARNING,"chunk %016"PRIX64" has only invalid copies (%"PRIu32") - fixing it",c->chunkid,ivc);
				c->version = bestversion;
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->valid == INVALID && s->version==bestversion && cstab[s->csid].valid) {
						s->valid = VALID;
						c->allvalidcopies++;
						c->regularvalidcopies++;
					}
					if (s->valid == INVALID) {
						syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - invalid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
					} else {
						syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - valid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
					}
				}
				chunk_state_change(c->goal,c->goal,0,c->allvalidcopies,0,c->regularvalidcopies);
				c->needverincrease = 1;
				return;
			}
		}
		syslog(LOG_WARNING,"chunk %016"PRIX64" has only invalid copies (%"PRIu32") - please repair it manually",c->chunkid,ivc);
		for (s=c->slisthead ; s ; s=s->next) {
			syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - invalid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
		}
		return;
	}

	if (tdc+vc+tdb+bc+ivc+dc==0) {
		syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": there are no copies",c->chunkid,c->version);
		return;
	}

// step 3a. delete invalid copies

	if (extrajob == 0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
				if (s->valid==INVALID || s->valid==DEL) {
					if (s->valid==DEL) {
						syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": cnunk hasn't been deleted since previous loop - retry",c->chunkid,c->version);
					}
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,0);
					inforec.done.del_invalid++;
					deldone++;
					dc++;
					ivc--;
				}
			} else {
				if (s->valid==INVALID) {
					inforec.notdone.del_invalid++;
					delnotdone++;
				}
			}
		}
	}

// step 3b. check for unfinished replications
	if (extrajob == 0) {
		if (c->operation==REPLICATE && c->lockedto<now) {
			syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk hasn't been replicated since previous loop - retry",c->chunkid,c->version);
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==TDBUSY || s->valid==BUSY) {
					if (s->valid==TDBUSY) {
						chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies--;
						tdb--;
					}
					if (s->valid==BUSY) {
						chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
						c->allvalidcopies--;
						c->regularvalidcopies--;
						bc--;
					}
					s->valid=INVALID;
					s->version = 0;	// after unfinished operation can't be shure what version chunk has
					chunk_delopchunk(s->csid,c->chunkid);
					ivc++;
				}
			}
			c->operation = NONE;
			c->lockedto = 0;
		}
	}

// step 4. return if chunk is during some operation
	if (c->operation!=NONE || (c->lockedto>=now)) {
		if (extrajob == 0) {
			if (c->fcount==0) {
				inforec.locked_unused++;
			} else {
				inforec.locked_used++;
				if (c->goal > vc+bc && vc+tdc+bc+tdb > 0) {
					if (c->operation!=NONE) {
						syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": can't replicate chunk - operation %s in progress",c->chunkid,c->version,opstr[c->operation]);
					} else {
						syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": can't replicate chunk - locked to: %"PRIu32,c->chunkid,c->version,c->lockedto);
					}
				}
			}
		}
		return ;
	}

// step 5. check busy count
	if ((bc+tdb)>0) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" has unexpected BUSY copies",c->chunkid,c->version);
		return ;
	}

// step 6. delete unused chunk

	if (extrajob==0 && c->fcount==0/* c->flisthead==NULL */) {
//		syslog(LOG_WARNING,"unused - delete");
		for (s=c->slisthead ; s ; s=s->next) {
			if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
				if (s->valid==VALID || s->valid==TDVALID) {
					if (s->valid==TDVALID) {
						chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies--;
					} else {
						chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
						c->allvalidcopies--;
						c->regularvalidcopies--;
					}
					c->needverincrease = 1;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,c->version);
					inforec.done.del_unused++;
					deldone++;
				}
			} else {
				if (s->valid==VALID || s->valid==TDVALID) {
					inforec.notdone.del_unused++;
					delnotdone++;
				}
			}
		}
		return ;
	}

// step 7a. if chunk has too many copies then delete some of them
	if (extrajob==0 && vc > c->goal) {
		uint8_t prevdone;
//		syslog(LOG_WARNING,"vc (%"PRIu32") > goal (%"PRIu32") - delete",vc,c->goal);
		if (dservcount==0) {
			// dservcount = matocsserv_getservers_ordered(dcsids,AcceptableDifference/2.0,NULL,NULL);
			dservcount = matocsserv_getservers_ordered(dcsids,0.0,NULL,NULL);
		}
		inforec.notdone.del_overgoal+=(vc-(c->goal));
		delnotdone+=(vc-(c->goal));
		prevdone = 1;
		for (i=0 ; i<dservcount && vc>c->goal && prevdone; i++) {
			for (s=c->slisthead ; s && s->csid!=dcsids[dservcount-1-i] ; s=s->next) {}
			if (s && s->valid==VALID) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
					c->needverincrease = 1;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,0);
					inforec.done.del_overgoal++;
					inforec.notdone.del_overgoal--;
					deldone++;
					delnotdone--;
					vc--;
					dc++;
				} else {
					prevdone=0;
				}
			}
		}
		return;
	}

// step 7b. if chunk has one copy on each server and some of them have status TODEL then delete one of it
	if (extrajob==0 && vc+tdc>=scount && vc<c->goal && tdc>0 && vc+tdc>1) {
		uint8_t prevdone;
//		syslog(LOG_WARNING,"vc+tdc (%"PRIu32") >= scount (%"PRIu32") and vc (%"PRIu32") < goal (%"PRIu32") and tdc (%"PRIu32") > 0 and vc+tdc > 1 - delete",vc+tdc,scount,vc,c->goal,tdc);
		prevdone = 0;
		for (s=c->slisthead ; s && prevdone==0 ; s=s->next) {
			if (s->valid==TDVALID) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
					c->allvalidcopies--;
					c->needverincrease = 1;
					s->valid = DEL;
					stats_deletions++;
					matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,0);
					inforec.done.del_diskclean++;
					tdc--;
					dc++;
					prevdone = 1;
				} else {
					inforec.notdone.del_diskclean++;
				}
			}
		}
		return;
	}

//step 8. if chunk has number of copies less than goal then make another copy of this chunk
	if (c->goal > vc && vc+tdc > 0) {
		if (csdb_replicate_undergoals()) {
//		if ((csdb_getdisconnecttime()+ReplicationsDelayDisconnect)<now) {
			uint32_t rgvc,rgtdc;
			uint32_t lclass;
			if (vc==0 && tdc==1) { // highest priority - chunks only on one disk "marked for removal"
				j = 0;
				lclass = 0;
			} else if (vc==1 && tdc==0) { // next priority - chunks only on one regular disk
				j = 1;
				lclass = 0;
			} else if (vc==1 && tdc>0) { // next priority - chunks on one regular disk and some "marked for removal" disks
				j = 2;
				lclass = 1;
			} else if (tdc>0) { // next priority - chunks on "marked for removal" disks
				j = 3;
				lclass = 1;
			} else { // lowest priority - standard undergoal chunks
				j = 4;
				lclass = 1;
			}
			if (extrajob==0) {
				for (i=0 ; i<j ; i++) {
					if (chunks_priority_leng[i]>0) { // we have chunks with higher priority than current chunk
						chunk_priority_enqueue(j,c); // in such case only enqueue this chunk for future processing
						return;
					}
				}
			}
			rservcount = matocsserv_getservers_lessrepl(rcsids,MaxWriteRepl[lclass]);
			rgvc=0;
			rgtdc=0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass]) {
					if (s->valid==VALID) {
						rgvc++;
					} else if (s->valid==TDVALID) {
						rgtdc++;
					}
				}
			}
			if (rgvc+rgtdc>0 && rservcount>0) { // have at least one server to read from and at least one to write to
				for (i=0 ; i<rservcount ; i++) {
					for (s=c->slisthead ; s && s->csid!=rcsids[i] ; s=s->next) {}
					if (!s) {
						uint32_t r;
						if (rgvc>0) {	// if there are VALID copies then make copy of one VALID chunk
							r = 1+rndu32_ranged(rgvc);
							srccsid = MAXCSCOUNT;
							for (s=c->slisthead ; s && r>0 ; s=s->next) {
								if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass] && s->valid==VALID) {
									r--;
									srccsid = s->csid;
								}
							}
						} else {	// if not then use TDVALID chunks.
							r = 1+rndu32_ranged(rgtdc);
							srccsid = MAXCSCOUNT;
							for (s=c->slisthead ; s && r>0 ; s=s->next) {
								if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass] && s->valid==TDVALID) {
									r--;
									srccsid = s->csid;
								}
							}
						}
						if (srccsid!=MAXCSCOUNT) {
							stats_replications++;
							// high priority replication
							if (matocsserv_send_replicatechunk(cstab[rcsids[i]].ptr,c->chunkid,c->version,cstab[srccsid].ptr)<0) {
								syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": error sending replicate command",c->chunkid,c->version);
								return;
							}
							chunk_addopchunk(rcsids[i],c->chunkid);
							c->operation = REPLICATE;
							c->lockedto = now+LOCKTIMEOUT;
							s = slist_malloc();
							s->csid = rcsids[i];
							s->valid = BUSY;
							s->version = c->version;
							s->next = c->slisthead;
							c->slisthead = s;
							chunk_state_change(c->goal,c->goal,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
							c->allvalidcopies++;
							c->regularvalidcopies++;
							if (extrajob==0) {
								inforec.done.copy_undergoal++;
							}
							return;
						}
					}
				}
			}
			chunk_priority_enqueue(j,c);
		}
		if (extrajob==0) {
			inforec.notdone.copy_undergoal++;
		}
	}

	if (extrajob) { // do not rebalane doing "extra" jobs.
		return;
	}

	if (fullservers==0) { // force rebalance when some servers are almost full
		for (i=0 ; i<DANGER_PRIORITIES ; i++) {
			if (chunks_priority_leng[i]>0) { // we have pending undergaal chunks, so ignore chunkserver rebalance
				return;
			}
		}

		if (c->ondangerlist) {
			syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": fixing 'ondangerlist' flag",c->chunkid,c->version);
			c->ondangerlist = 0;
		}
	}

// step 9. if there is too big difference between chunkservers then make copy of chunk from server with biggest disk usage on server with lowest disk usage
	if (c->goal >= vc && vc+tdc>0 && ((maxusage-minusage)>AcceptableDifference || (maxusage>minusage && last_rebalance+(0.01/(maxusage-minusage))<now))) {
		if (bservcount==0) {
			if ((maxusage-minusage)>AcceptableDifference) { // fast rebalance
				bservcount = matocsserv_getservers_ordered(bcsids,AcceptableDifference/2.0,&min,&max);
			} else { // slow rebalance
				bservcount = matocsserv_getservers_ordered(bcsids,0.0,&min,&max);
			}
		}
		if (min>0 || max>0) {
			srccsid = MAXCSCOUNT;
			dstcsid = MAXCSCOUNT;
			if (max>0) {
				for (i=0 ; i<max && srccsid==MAXCSCOUNT ; i++) {
					uint8_t lclass;
					lclass = (matocsserv_can_create_chunks(cstab[bcsids[bservcount-1-i]].ptr,AcceptableDifference*1.5)<2)?2:3;
					if (matocsserv_replication_read_counter(cstab[bcsids[bservcount-1-i]].ptr,now)<MaxReadRepl[lclass]) {
						for (s=c->slisthead ; s && s->csid!=bcsids[bservcount-1-i] ; s=s->next ) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srccsid=s->csid;
						}
					}
				}
			} else {
				for (i=0 ; i<(bservcount-min) && srccsid==MAXCSCOUNT ; i++) {
					uint8_t lclass;
					lclass = (matocsserv_can_create_chunks(cstab[bcsids[bservcount-1-i]].ptr,AcceptableDifference*1.5)<2)?2:3;
					if (matocsserv_replication_read_counter(cstab[bcsids[bservcount-1-i]].ptr,now)<MaxReadRepl[lclass]) {
						for (s=c->slisthead ; s && s->csid!=bcsids[bservcount-1-i] ; s=s->next ) {}
						if (s && (s->valid==VALID || s->valid==TDVALID)) {
							srccsid=s->csid;
						}
					}
				}
			}
			if (srccsid!=MAXCSCOUNT) {
				if (min>0) {
					for (i=0 ; i<min && dstcsid==MAXCSCOUNT ; i++) {
						uint8_t lclass;
						lclass = matocsserv_can_create_chunks(cstab[bcsids[i]].ptr,AcceptableDifference*1.5)?2:3;
						if (matocsserv_replication_write_counter(cstab[bcsids[i]].ptr,now)<MaxWriteRepl[lclass]) {
							for (s=c->slisthead ; s && s->csid!=bcsids[i] ; s=s->next ) {}
							if (s==NULL) {
								dstcsid=bcsids[i];
							}
						}
					}
				} else {
					for (i=0 ; i<bservcount-max && dstcsid==MAXCSCOUNT ; i++) {
						uint8_t lclass;
						lclass = matocsserv_can_create_chunks(cstab[bcsids[i]].ptr,AcceptableDifference*1.5)?2:3;
						if (matocsserv_replication_write_counter(cstab[bcsids[i]].ptr,now)<MaxWriteRepl[lclass]) {
							for (s=c->slisthead ; s && s->csid!=bcsids[i] ; s=s->next ) {}
							if (s==NULL) {
								dstcsid=bcsids[i];
							}
						}
					}
				}
				if (dstcsid!=MAXCSCOUNT) {
					stats_replications++;
//					matocsserv_getlocation(srcserv,&ip,&port);
					// low priority replication
					matocsserv_send_replicatechunk(cstab[dstcsid].ptr,c->chunkid,c->version,cstab[srccsid].ptr);
					c->needverincrease = 1;
					inforec.copy_rebalance++;
					last_rebalance = now;
				}
			}
		}
	}
}

static inline void chunk_clean_priority_queues(void) {
	uint32_t j,l;
	for (j=0 ; j<DANGER_PRIORITIES ; j++) {
		for (l=0 ; l<DangerMaxLeng ; l++) {
			if (chunks_priority_queue[j][l]!=NULL) {
				chunks_priority_queue[j][l]->ondangerlist = 0;
			}
			chunks_priority_queue[j][l] = NULL;
		}
		chunks_priority_head[j] = 0;
		chunks_priority_tail[j] = 0;
		chunks_priority_leng[j] = 0;
	}
}


void chunk_jobs_main(void) {
	uint32_t i,j,l,h,t,lc,hashsteps;
	uint16_t uscount,tscount,fullservers;
	double minusage,maxusage;
	chunk *c,*cn;
	uint32_t now;
#ifdef MFSDEBUG
	static uint32_t lastsecond=0;
#endif

	chunk_server_disconnection_loop();
	chunk_server_check_delays();

	if (chunk_counters_in_progress()) {
		return;
	}
	if (starttime+ReplicationsDelayInit>main_time()) {
		return;
	}

	matocsserv_usagedifference(&minusage,&maxusage,&uscount,&tscount);

	if (minusage>maxusage || uscount==0 || chunkrehashpos==0) {
		return;
	}

	fullservers = matocsserv_almostfull_servers();

	now = main_time();
	chunk_do_jobs(NULL,JOBS_EVERYTICK,0,0.0,0.0,now,0);

	// first serve some endangered and undergoal chunks
	lc = 0;
	for (j=0 ; j<DANGER_PRIORITIES ; j++) {
		if (((chunks_priority_tail[j]+chunks_priority_leng[j])%DangerMaxLeng) != chunks_priority_head[j]) {
			syslog(LOG_NOTICE,"danger_priority_group %"PRIu32": serious structure error, head: %"PRIu32"; tail: %"PRIu32"; leng: %"PRIu32,j,chunks_priority_head[j],chunks_priority_tail[j],+chunks_priority_leng[j]);
			for (l=0 ; l<DangerMaxLeng ; l++) {
				if (chunks_priority_queue[j][l]!=NULL) {
					chunks_priority_queue[j][l]->ondangerlist = 0;
				}
				chunks_priority_queue[j][l] = NULL;
			}
			chunks_priority_head[j] = 0;
			chunks_priority_tail[j] = 0;
			chunks_priority_leng[j] = 0;
		}
#ifdef MFSDEBUG
		l = chunks_priority_leng[j];
#endif
		if (chunks_priority_leng[j]>0 && lc<HashCPTMax) {
			h = chunks_priority_head[j];
			t = chunks_priority_tail[j];
			do {
				c = chunks_priority_queue[j][t];
				chunks_priority_queue[j][t] = NULL;
				t = (t+1)%DangerMaxLeng;
				chunks_priority_tail[j] = t;
				chunks_priority_leng[j]--;
				if (c!=NULL) {
					c->ondangerlist = 0;
					chunk_do_jobs(c,uscount,fullservers,minusage,maxusage,now,1);
					lc++;
				}
			} while (t!=h && lc<HashCPTMax);
		}
#ifdef MFSDEBUG
		if (now!=lastsecond) {
			syslog(LOG_NOTICE,"danger_priority_group %"PRIu32": %"PRIu32"->%"PRIu32,j,l,chunks_priority_leng[j]);
		}
#endif
	}
#ifdef MFSDEBUG
	lastsecond=now;
#endif

	// then serve standard chunks
	lc = 0;
	hashsteps = 1+((chunkrehashpos)/(LoopTimeMin*TICKSPERSECOND));
	for (i=0 ; i<hashsteps && lc<HashCPTMax ; i++) {
		if (jobshpos>=chunkrehashpos) {
			chunk_do_jobs(NULL,JOBS_EVERYLOOP,0,0.0,0.0,now,0);	// every loop tasks
			jobshpos=0;
		} else {
			c = chunkhashtab[jobshpos>>HASHTAB_LOBITS][jobshpos&HASHTAB_MASK];
			while (c) {
				cn = c->next;
				if (c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && ((csdb_getdisconnecttime()+RemoveDelayDisconnect)<main_time())) {
					changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
					chunk_delete(c);
				} else {
					chunk_do_jobs(c,uscount,fullservers,minusage,maxusage,now,0);
					lc++;
				}
				c = cn;
			}
			jobshpos++;
		}
	}
		// delete unused chunks from structures
//		l=0;
//		cp = &(chunkhash[jobshpos]);
//		while ((c=*cp)!=NULL) {
//			if (c->fcount==0 && /*c->flisthead==NULL && */c->slisthead==NULL) {
//				*cp = (c->next);
//				chunk_delete(c);
//			} else {
//				cp = &(c->next);
//				l++;
//				lc++;
//			}
//		}
//		if (l>0) {
//			r = rndu32_ranged(l);
//			l=0;
		// do jobs on rest of them
//			for (c=chunkhash[jobshpos] ; c ; c=c->next) {
//				if (l>=r) {
//					chunk_do_jobs(c,uscount,0,minusage,maxusage);
//				}
//				l++;
//			}
//			l=0;
//			for (c=chunkhash[jobshpos] ; l<r && c ; c=c->next) {
//				chunk_do_jobs(c,uscount,0,minusage,maxusage);
//				l++;
//			}
//		}
//		jobshpos+=123;	// if HASHSIZE is any power of 2 then any odd number is good here
//		jobshpos%=HASHSIZE;
//	}
}

/* ---- */

#define CHUNKFSIZE 16
#define CHUNKCNT 1000
/*
void chunk_text_dump(FILE *fd) {
	chunk *c;
	uint32_t i,lockedto,now;
	now = main_time();

	for (i=0 ; i<chunkrehashpos ; i++) {
		for (c=chunkhashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; c ; c=c->next) {
			lockedto = c->lockedto;
			if (lockedto<now) {
				lockedto = 0;
			}
			fprintf(fd,"*|i:%016"PRIX64"|v:%08"PRIX32"|g:%"PRIu8"|t:%10"PRIu32"\n",c->chunkid,c->version,c->goal,lockedto);
		}
	}
}
*/
int chunk_load(bio *fd,uint8_t mver) {
	uint8_t hdr[8];
	uint8_t loadbuff[CHUNKFSIZE];
	const uint8_t *ptr;
	int32_t r;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version,lockedto;

	(void)mver;

	chunks=0;
	if (bio_read(fd,hdr,8)!=8) {
		syslog(LOG_WARNING,"chunks: can't read header");
		return -1;
	}
	ptr = hdr;
	nextchunkid = get64bit(&ptr);
	for (;;) {
		r = bio_read(fd,loadbuff,CHUNKFSIZE);
		if (r!=CHUNKFSIZE) {
			syslog(LOG_WARNING,"chunks: read error");
			return -1;
		}
		ptr = loadbuff;
		chunkid = get64bit(&ptr);
		version = get32bit(&ptr);
		lockedto = get32bit(&ptr);
		if (chunkid>0) {
			c = chunk_new(chunkid);
			c->version = version;
			c->lockedto = lockedto;
		} else {
			if (version==0 && lockedto==0) {
				return 0;
			} else {
				syslog(LOG_WARNING,"chunks: wrong ending - chunk zero with version: %"PRIu32" and locked to: %"PRIu32,version,lockedto);
				return -1;
			}
		}
	}
	return 0;	// unreachable
}

uint8_t chunk_store(bio *fd) {
	uint8_t hdr[8];
	uint8_t storebuff[CHUNKFSIZE*CHUNKCNT];
	uint8_t *ptr;
	uint32_t i,j;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version;
	uint32_t lockedto,now;

	if (fd==NULL) {
		return 0x10;
	}
	now = main_time();
	ptr = hdr;
	put64bit(&ptr,nextchunkid);
	if (bio_write(fd,hdr,8)!=8) {
		return 0xFF;
	}
	j=0;
	ptr = storebuff;
	for (i=0 ; i<chunkrehashpos ; i++) {
		for (c=chunkhashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; c ; c=c->next) {
			chunkid = c->chunkid;
			put64bit(&ptr,chunkid);
			version = c->version;
			put32bit(&ptr,version);
			lockedto = c->lockedto;
			if (lockedto<now) {
				lockedto = 0;
			}
			put32bit(&ptr,lockedto);
			j++;
			if (j==CHUNKCNT) {
				if (bio_write(fd,storebuff,CHUNKFSIZE*CHUNKCNT)!=(CHUNKFSIZE*CHUNKCNT)) {
					return 0xFF;
				}
				j=0;
				ptr = storebuff;
			}
		}
	}
	memset(ptr,0,CHUNKFSIZE);
	j++;
	if (bio_write(fd,storebuff,CHUNKFSIZE*j)!=(CHUNKFSIZE*j)) {
		return 0xFF;
	}
	return 0;
}

void chunk_cleanup(void) {
	uint32_t i,j;
	discserv *ds;
//	slist_bucket *sb,*sbn;
//	chunk_bucket *cb,*cbn;

	chunk_clean_priority_queues();
	while (discservers) {
		ds = discservers;
		discservers = discservers->next;
		matocsserv_disconnection_finished(cstab[ds->csid].ptr);
		free(ds);
	}
	while (discservers_next) {
		ds = discservers_next;
		discservers_next = discservers_next->next;
		matocsserv_disconnection_finished(cstab[ds->csid].ptr);
		free(ds);
	}
	slist_free_all();
//	for (sb = sbhead ; sb ; sb = sbn) {
//		sbn = sb->next;
//		free(sb);
//	}
//	sbhead = NULL;
//	slfreehead = NULL;
	chunk_free_all();
//	for (cb = cbhead ; cb ; cb = cbn) {
//		cbn = cb->next;
//		free(cb);
//	}
//	cbhead = NULL;
//	chfreehead = NULL;
	chunk_hash_cleanup();
//	for (i=0 ; i<HASHSIZE ; i++) {
//		chunkhash[i] = NULL;
//	}
	for (i=0 ; i<MAXCSCOUNT ; i++) {
		cstab[i].next = i+1;
		cstab[i].prev = i-1;
		cstab[i].valid = 0;
		cstab[i].registered = 0;
		cstab[i].newchunkdelay = 0;
		cstab[i].lostchunkdelay = 0;
	}
	cstab[0].prev = MAXCSCOUNT;
	csfreehead = 0;
	csusedhead = MAXCSCOUNT;
	for (i=0 ; i<11 ; i++) {
		for (j=0 ; j<11 ; j++) {
			allchunkcounts[i][j]=0;
			regularchunkcounts[i][j]=0;
		}
	}
//	jobshpos = 0;
//	chunk_do_jobs(NULL,JOBS_INIT,0,0.0,0.0);	// clear chunk loop internal data
}

void chunk_newfs(void) {
	chunks = 0;
	nextchunkid = 1;
}

int chunk_parse_rep_list(char *strlist,double *replist) {
	// N
	// A,B,C,D
	char *p;
	uint32_t i;
	double reptmp[4];

	p = strlist;
	while (*p==' ' || *p=='\t' || *p=='\r' || *p=='\n') {
		p++;
	}
	reptmp[0] = strtod(p,&p);
	while (*p==' ' || *p=='\t' || *p=='\r' || *p=='\n') {
		p++;
	}
	if (*p==0) {
		for (i=0 ; i<4 ; i++) {
			replist[i] = reptmp[0];
		}
		return 0;
	}
	for (i=1 ; i<4 ; i++) {
		if (*p!=',') {
			return -1;
		}
		p++;
		while (*p==' ' || *p=='\t' || *p=='\r' || *p=='\n') {
			p++;
		}
		reptmp[i] = strtod(p,&p);
		while (*p==' ' || *p=='\t' || *p=='\r' || *p=='\n') {
			p++;
		}
	}
	if (*p==0) {
		for (i=0 ; i<4 ; i++) {
			replist[i] = reptmp[i];
		}
		return 1;
	}
	return -1;
}

void chunk_reload(void) {
	uint32_t oldMaxDelSoftLimit,oldMaxDelHardLimit,oldDangerMaxLeng;
	uint32_t cps,i,j;
	char *repstr;

	ReplicationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT",60);
//	ReplicationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600);


	oldMaxDelSoftLimit = MaxDelSoftLimit;
	oldMaxDelHardLimit = MaxDelHardLimit;

	MaxDelSoftLimit = cfg_getuint32("CHUNKS_SOFT_DEL_LIMIT",10);
	if (cfg_isdefined("CHUNKS_HARD_DEL_LIMIT")) {
		MaxDelHardLimit = cfg_getuint32("CHUNKS_HARD_DEL_LIMIT",25);
		if (MaxDelHardLimit<MaxDelSoftLimit) {
			MaxDelSoftLimit = MaxDelHardLimit;
			syslog(LOG_WARNING,"CHUNKS_SOFT_DEL_LIMIT is greater than CHUNKS_HARD_DEL_LIMIT - using CHUNKS_HARD_DEL_LIMIT for both");
		}
	} else {
		MaxDelHardLimit = 3 * MaxDelSoftLimit;
	}
	if (MaxDelSoftLimit==0) {
		MaxDelSoftLimit = oldMaxDelSoftLimit;
		MaxDelHardLimit = oldMaxDelHardLimit;
	}
	if (TmpMaxDelFrac<MaxDelSoftLimit) {
		TmpMaxDelFrac = MaxDelSoftLimit;
	}
	if (TmpMaxDelFrac>MaxDelHardLimit) {
		TmpMaxDelFrac = MaxDelHardLimit;
	}
	if (TmpMaxDel<MaxDelSoftLimit) {
		TmpMaxDel = MaxDelSoftLimit;
	}
	if (TmpMaxDel>MaxDelHardLimit) {
		TmpMaxDel = MaxDelHardLimit;
	}


	repstr = cfg_getstr("CHUNKS_WRITE_REP_LIMIT","2,1,1,4");
	switch (chunk_parse_rep_list(repstr,MaxWriteRepl)) {
		case -1:
			syslog(LOG_WARNING,"write replication limit parse error !!!");
			break;
		case 0:
			syslog(LOG_NOTICE,"write replication limit in old format - change limits to new format");
	}
	free(repstr);
	repstr = cfg_getstr("CHUNKS_READ_REP_LIMIT","10,5,2,5");
	switch (chunk_parse_rep_list(repstr,MaxReadRepl)) {
		case -1:
			syslog(LOG_WARNING,"read replication limit parse error !!!");
			break;
		case 0:
			syslog(LOG_NOTICE,"read replication limit in old format - change limits to new format");
	}
	free(repstr);
/*
	repl = cfg_getuint32("CHUNKS_WRITE_REP_LIMIT",2);
	if (repl>0) {
		MaxWriteRepl = repl;
	}


	repl = cfg_getuint32("CHUNKS_READ_REP_LIMIT",10);
	if (repl>0) {
		MaxReadRepl = repl;
	}
*/
	if (cfg_isdefined("CHUNKS_LOOP_TIME")) {
		LoopTimeMin = cfg_getuint32("CHUNKS_LOOP_TIME",300); // deprecated option
		if (LoopTimeMin < MINLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_TIME value too low (%"PRIu32") increased to %u",LoopTimeMin,MINLOOPTIME);
			LoopTimeMin = MINLOOPTIME;
		}
		if (LoopTimeMin > MAXLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_TIME value too high (%"PRIu32") decreased to %u",LoopTimeMin,MAXLOOPTIME);
			LoopTimeMin = MAXLOOPTIME;
		}
//		HashSteps = 1+((HASHSIZE)/(LoopTimeMin*TICKSPERSECOND));
		HashCPTMax = 0xFFFFFFFF;
	} else {
		LoopTimeMin = cfg_getuint32("CHUNKS_LOOP_MIN_TIME",300);
		if (LoopTimeMin < MINLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MIN_TIME value too low (%"PRIu32") increased to %u",LoopTimeMin,MINLOOPTIME);
			LoopTimeMin = MINLOOPTIME;
		}
		if (LoopTimeMin > MAXLOOPTIME) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MIN_TIME value too high (%"PRIu32") decreased to %u",LoopTimeMin,MAXLOOPTIME);
			LoopTimeMin = MAXLOOPTIME;
		}
//		HashSteps = 1+((HASHSIZE)/(LoopTimeMin*TICKSPERSECOND));
		cps = cfg_getuint32("CHUNKS_LOOP_MAX_CPS",100000);
		if (cps < MINCPS) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MAX_CPS value too low (%"PRIu32") increased to %u",cps,MINCPS);
			cps = MINCPS;
		}
		if (cps > MAXCPS) {
			syslog(LOG_NOTICE,"CHUNKS_LOOP_MAX_CPS value too high (%"PRIu32") decreased to %u",cps,MAXCPS);
			cps = MAXCPS;
		}
		HashCPTMax = ((cps+(TICKSPERSECOND-1))/TICKSPERSECOND);
	}

	if (cfg_isdefined("ACCEPTABLE_PERCENTAGE_DIFFERENCE")) {
		AcceptableDifference = cfg_getdouble("ACCEPTABLE_PERCENTAGE_DIFFERENCE",1.0)/100.0; // 1%
	} else {
		AcceptableDifference = cfg_getdouble("ACCEPTABLE_DIFFERENCE",0.01); // 1% - this is deprecated option
	}
	if (AcceptableDifference<0.001) { // 1%
		AcceptableDifference = 0.001;
	}
	if (AcceptableDifference>0.1) { // 10%
		AcceptableDifference = 0.1;
	}

	oldDangerMaxLeng = DangerMaxLeng;
	DangerMaxLeng = cfg_getuint32("PRIORITY_QUEUES_LENGTH",1000000);
	if (DangerMaxLeng<10000) {
		DangerMaxLeng = 10000;
	}
	if (DangerMaxLeng != oldDangerMaxLeng) {
		for (j=0 ; j<DANGER_PRIORITIES ; j++) {
			if (chunks_priority_leng[j]>0) {
				for (i=chunks_priority_tail[j] ; i!=chunks_priority_head[j] ; i = (i+1)%oldDangerMaxLeng) {
					if (chunks_priority_queue[j][i]!=NULL) {
						chunks_priority_queue[j][i]->ondangerlist=0;
					}
				}
			}
			free(chunks_priority_queue[j]);
			chunks_priority_queue[j] = (chunk**)malloc(sizeof(chunk*)*DangerMaxLeng);
			passert(chunks_priority_queue[j]);
			for (i=0 ; i<DangerMaxLeng ; i++) {
				chunks_priority_queue[j][i] = NULL;
			}
			chunks_priority_leng[j] = 0;
			chunks_priority_head[j] = 0;
			chunks_priority_tail[j] = 0;
		}
	}
}

int chunk_strinit(void) {
	uint32_t i;
	uint32_t j;
	uint32_t cps;
	char *repstr;

	starttime = main_time();
	ReplicationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT",60);
//	ReplicationsDelayDisconnect = cfg_getuint32("REPLICATIONS_DELAY_DISCONNECT",3600);
	MaxDelSoftLimit = cfg_getuint32("CHUNKS_SOFT_DEL_LIMIT",10);
	if (cfg_isdefined("CHUNKS_HARD_DEL_LIMIT")) {
		MaxDelHardLimit = cfg_getuint32("CHUNKS_HARD_DEL_LIMIT",25);
		if (MaxDelHardLimit<MaxDelSoftLimit) {
			MaxDelSoftLimit = MaxDelHardLimit;
			fprintf(stderr,"CHUNKS_SOFT_DEL_LIMIT is greater than CHUNKS_HARD_DEL_LIMIT - using CHUNKS_HARD_DEL_LIMIT for both\n");
		}
	} else {
		MaxDelHardLimit = 3 * MaxDelSoftLimit;
	}
	if (MaxDelSoftLimit==0) {
		fprintf(stderr,"delete limit is zero !!!\n");
		return -1;
	}
	TmpMaxDelFrac = MaxDelSoftLimit;
	TmpMaxDel = MaxDelSoftLimit;

	repstr = cfg_getstr("CHUNKS_WRITE_REP_LIMIT","2,1,1,4");
	switch (chunk_parse_rep_list(repstr,MaxWriteRepl)) {
		case -1:
			fprintf(stderr,"write replication limit parse error !!!\n");
			return -1;
		case 0:
			fprintf(stderr,"write replication limit in old format - change limits to new format\n");
	}
	free(repstr);
	repstr = cfg_getstr("CHUNKS_READ_REP_LIMIT","10,5,2,5");
	switch (chunk_parse_rep_list(repstr,MaxReadRepl)) {
		case -1:
			fprintf(stderr,"read replication limit parse error !!!\n");
			return -1;
		case 0:
			fprintf(stderr,"read replication limit in old format - change limits to new format\n");
	}
	free(repstr);
/*
	MaxWriteRepl = cfg_getuint32("CHUNKS_WRITE_REP_LIMIT",2);
	MaxReadRepl = cfg_getuint32("CHUNKS_READ_REP_LIMIT",10);
	if (MaxReadRepl==0) {
		fprintf(stderr,"read replication limit is zero !!!\n");
		return -1;
	}
	if (MaxWriteRepl==0) {
		fprintf(stderr,"write replication limit is zero !!!\n");
		return -1;
	}
*/
	if (cfg_isdefined("CHUNKS_LOOP_TIME")) {
		fprintf(stderr,"Defining loop time by CHUNKS_LOOP_TIME option is deprecated - use CHUNKS_LOOP_MAX_CPS and CHUNKS_LOOP_MIN_TIME\n");
		LoopTimeMin = cfg_getuint32("CHUNKS_LOOP_TIME",300); // deprecated option
		if (LoopTimeMin < MINLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_TIME value too low (%"PRIu32") increased to %u\n",LoopTimeMin,MINLOOPTIME);
			LoopTimeMin = MINLOOPTIME;
		}
		if (LoopTimeMin > MAXLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_TIME value too high (%"PRIu32") decreased to %u\n",LoopTimeMin,MAXLOOPTIME);
			LoopTimeMin = MAXLOOPTIME;
		}
//		HashSteps = 1+((HASHSIZE)/(LoopTimeMin*TICKSPERSECOND));
		HashCPTMax = 0xFFFFFFFF;
	} else {
		LoopTimeMin = cfg_getuint32("CHUNKS_LOOP_MIN_TIME",300);
		if (LoopTimeMin < MINLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_MIN_TIME value too low (%"PRIu32") increased to %u\n",LoopTimeMin,MINLOOPTIME);
			LoopTimeMin = MINLOOPTIME;
		}
		if (LoopTimeMin > MAXLOOPTIME) {
			fprintf(stderr,"CHUNKS_LOOP_MIN_TIME value too high (%"PRIu32") decreased to %u\n",LoopTimeMin,MAXLOOPTIME);
			LoopTimeMin = MAXLOOPTIME;
		}
//		HashSteps = 1+((HASHSIZE)/(LoopTimeMin*TICKSPERSECOND));
		cps = cfg_getuint32("CHUNKS_LOOP_MAX_CPS",100000);
		if (cps < MINCPS) {
			fprintf(stderr,"CHUNKS_LOOP_MAX_CPS value too low (%"PRIu32") increased to %u\n",cps,MINCPS);
			cps = MINCPS;
		}
		if (cps > MAXCPS) {
			fprintf(stderr,"CHUNKS_LOOP_MAX_CPS value too high (%"PRIu32") decreased to %u\n",cps,MAXCPS);
			cps = MAXCPS;
		}
		HashCPTMax = ((cps+(TICKSPERSECOND-1))/TICKSPERSECOND);
	}
	if (cfg_isdefined("ACCEPTABLE_PERCENTAGE_DIFFERENCE")) {
		AcceptableDifference = cfg_getdouble("ACCEPTABLE_PERCENTAGE_DIFFERENCE",1.0)/100.0; // 1%
	} else {
		AcceptableDifference = cfg_getdouble("ACCEPTABLE_DIFFERENCE",0.01); // 1% - deprecated option
	}
	if (AcceptableDifference<0.001) { // 0.1%
		AcceptableDifference = 0.001;
	}
	if (AcceptableDifference>0.1) { // 10%
		AcceptableDifference = 0.1;
	}
	DangerMaxLeng = cfg_getuint32("PRIORITY_QUEUES_LENGTH",1000000);
	if (DangerMaxLeng<10000) {
		DangerMaxLeng = 10000;
	}
	chunk_hash_init();
//	for (i=0 ; i<HASHSIZE ; i++) {
//		chunkhash[i]=NULL;
//	}
	cstab = malloc(sizeof(csdata)*MAXCSCOUNT);
	passert(cstab);
	for (i=0 ; i<MAXCSCOUNT ; i++) {
		cstab[i].next = i+1;
		cstab[i].prev = i-1;
		cstab[i].valid = 0;
		cstab[i].registered = 0;
		cstab[i].newchunkdelay = 0;
		cstab[i].lostchunkdelay = 0;
	}
	cstab[0].prev = MAXCSCOUNT;
	csfreehead = 0;
	csusedhead = MAXCSCOUNT;
	for (i=0 ; i<11 ; i++) {
		for (j=0 ; j<11 ; j++) {
			allchunkcounts[i][j]=0;
			regularchunkcounts[i][j]=0;
		}
	}
	jobshpos = 0;
	for (j=0 ; j<DANGER_PRIORITIES ; j++) {
		chunks_priority_queue[j] = (chunk**)malloc(sizeof(chunk*)*DangerMaxLeng);
		passert(chunks_priority_queue[j]);
		for (i=0 ; i<DangerMaxLeng ; i++) {
			chunks_priority_queue[j][i] = NULL;
		}
		chunks_priority_leng[j] = 0;
		chunks_priority_head[j] = 0;
		chunks_priority_tail[j] = 0;
	}
	chunk_do_jobs(NULL,JOBS_INIT,0,0.0,0.0,main_time(),0);	// clear chunk loop internal data

	main_reload_register(chunk_reload);
	// main_time_register(1,0,chunk_jobs_main);
	main_msectime_register(1000/TICKSPERSECOND,0,chunk_jobs_main);
	return 1;
}
