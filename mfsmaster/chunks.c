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

#define BUCKETS_MMAP_ALLOC 1
#define HASHTAB_PREALLOC 1
#define CHUNKHASH_MOVEFACTOR 5

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <stdlib.h>
#include <stdarg.h>
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
#include "storageclass.h"

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


enum {JOBS_INIT,JOBS_EVERYLOOP,JOBS_EVERYTICK,JOBS_TERM};

/* chunk.operation */
enum {NONE=0,CREATE,SET_VERSION,DUPLICATE,TRUNCATE,DUPTRUNC,REPLICATE};

static const char* opstr[] = {
	"NONE",
	"CREATE",
	"SET_VERSION",
	"DUPLICATE",
	"TRUNCATE",
	"DUPLICATE+TRUNCATE",
	"REPLICATE",
	"???"
};

/* slist.valid */
/* INVALID - got info from chunkserver (IO error etc.)  ->  to delete */
/* DEL - deletion in progress */
/* BUSY - operation in progress */
/* VALID - ok */
/* WVER - wrong version - repair or delete */
/* TDBUSY - to be deleted + operation in progress */
/* TDVALID - ok, to be deleted */
/* TDWVER - wrong version, to be deleted */
enum {INVALID,DEL,BUSY,VALID,WVER,TDBUSY,TDVALID,TDWVER};

#ifdef MFSDEBUG
static const char* validstr[] = {
	"INVALID",
	"DELETE",
	"BUSY",
	"VALID",
	"WVER",
	"TDBUSY",
	"TDVALID",
	"TDWVER"
};
#endif

typedef struct _discserv {
	uint16_t csid;
	struct _discserv *next;
} discserv;

static discserv *discservers = NULL;
static discserv *discservers_next = NULL;

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
	uint8_t sclassid;
	uint8_t allvalidcopies;
	uint8_t regularvalidcopies;
	unsigned ondangerlist:1;
	unsigned needverincrease:1;
	unsigned interrupted:1;
	unsigned writeinprogress:1;
	unsigned archflag:1;
	unsigned operation:3;
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
	uint8_t status;
	struct _csopchunk *next;
} csopchunk;

/* csdata.mfr_state */
/* UNKNOWN_HARD - unknown after disconnect or creation */
/* UNKNOWN_SOFT - unknown, loop in progress */
/* CAN_BE_REMOVED - can be removed, whole loop has passed */
/* REPL_IN_PROGRESS - chunks still needs to be replicated, can't be removed */
/* WAS_IN_PROGRESS - was in REPL_IN_PROGRESS during previous loop */
enum {UNKNOWN_HARD,UNKNOWN_SOFT,CAN_BE_REMOVED,REPL_IN_PROGRESS,WAS_IN_PROGRESS};

// state automaton:
//
//  1 - Chunk with number of valid copies less than goal (servers with chunks marked for removal only)
//  2 - Chunkserver disconnection (all servers)
//  3 - Loop end (all servers)
//
//                            1                 2              3
//  UNKNOWN_HARD     | REPL_IN_PROGRESS | UNKNOWN_HARD | UNKNOWN_SOFT
//  UNKNOWN_SOFT     | REPL_IN_PROGRESS | UNKNOWN_HARD | CAN_BE_REMOVED
//  CAN_BE_REMOVED   | REPL_IN_PROGRESS | UNKNOWN_HARD | CAN_BE_REMOVED
//  REPL_IN_PROGRESS | REPL_IN_PROGRESS | UNKNOWN_HARD | WAS_IN_PROGRESS
//  WAS_IN_PROGRESS  | REPL_IN_PROGRESS | UNKNOWN_HARD | CAN_BE_REMOVED
//
//  UNKNOWN_HARD,UNKNOWN_SOFT        - Unknown state
//  CAN_BE_REMOVED                   - Can be removed
//  REPL_IN_PROGRESS,WAS_IN_PROGRESS - In progress

typedef struct _csdata {
	void *ptr;
	csopchunk *opchunks;
	uint8_t valid;
	unsigned registered:1;
	unsigned mfr_state:3;
	uint8_t newchunkdelay;
	uint8_t lostchunkdelay;
	uint32_t next;
	uint32_t prev;
} csdata;

static csdata *cstab = NULL;
static uint32_t csfreehead = MAXCSCOUNT;
static uint32_t csfreetail = MAXCSCOUNT;
static uint32_t csusedhead = MAXCSCOUNT;
static uint32_t opsinprogress = 0;
static uint16_t csregisterinprogress = 0;
static uint8_t csreceivingchunks = 0;

#define DANGER_PRIORITIES 7
#define REPLICATION_DANGER_PRIORITIES 6

#define DPRIORITY_ENDANGERED_HIGHGOAL 0
#define DPRIORITY_ENDANGERED 1
#define DPRIORITY_UNDERGOAL_MFR 2
#define DPRIORITY_MFR 3
#define DPRIORITY_UNDERGOAL 4
#define DPRIORITY_WRONGLABEL 5
#define DPRIORITY_OVERGOAL 6

static chunk** chunks_priority_queue[DANGER_PRIORITIES];
static uint32_t chunks_priority_leng[DANGER_PRIORITIES];
static uint32_t chunks_priority_head[DANGER_PRIORITIES];
static uint32_t chunks_priority_tail[DANGER_PRIORITIES];

static double chunks_priority_mincpsperc[DANGER_PRIORITIES] = {1.0,0.1,0.1,0.01,0.05,0.01,0.3};

static uint32_t ReplicationsDelayInit=60;
static uint32_t DangerMaxLeng=1000000;

static double MaxWriteRepl[4];
static double MaxReadRepl[4];
static uint32_t MaxDelSoftLimit;
static uint32_t MaxDelHardLimit;
static double TmpMaxDelFrac;
static uint32_t TmpMaxDel;
static uint8_t ReplicationsRespectTopology;
static uint32_t CreationsRespectTopology;
static uint32_t LoopTimeMin;
//static uint32_t HashSteps;
static uint32_t HashCPTMax;
static double AcceptableDifference;

static uint8_t DoNotUseSameIP;
static uint8_t DoNotUseSameRack;
static uint32_t LabelUniqueMask;

static uint32_t jobshpos;
static uint32_t jobshstep;
static uint32_t jobshcnt;
static uint32_t jobshmax;

static uint32_t starttime;

typedef struct _job_info {
	uint32_t del_invalid;
	uint32_t del_unused;
	uint32_t del_diskclean;
	uint32_t del_overgoal;
	uint32_t copy_undergoal;
	uint32_t copy_wronglabels;
} job_info;

typedef struct _loop_info {
	job_info done,notdone;
	uint32_t locked_unused;
	uint32_t locked_used;
	uint32_t copy_rebalance;
	uint32_t labels_dont_match;
} loop_info;

static loop_info chunksinfo = {{0,0,0,0,0,0},{0,0,0,0,0,0},0,0,0,0};
static uint32_t chunksinfo_loopstart=0,chunksinfo_loopend=0;

static uint64_t lastchunkid=0;
static chunk* lastchunkptr=NULL;

static uint32_t chunks;

static uint32_t last_rebalance=0;

static uint32_t **allchunkcounts;
static uint32_t **regularchunkcounts;

static uint32_t stats_chunkops[12];


#ifdef MFSDEBUG
static void mfsdebug(const char *format,...) {
	va_list ap;
	static FILE *fd = NULL;

	if (format==NULL) {
		if (fd==NULL) {
			fd = fopen("mfsdebug.txt","a");
		} else {
			fflush(fd);
		}
	} else if (fd!=NULL) {
		fprintf(fd,"%"PRIu32" : ",(uint32_t)(main_time()));
		va_start(ap,format);
		vfprintf(fd,format,ap);
		va_end(ap);
		fputc('\n',fd);
	}
}

void mfsdebug_flush(void) {
	mfsdebug(NULL);
}
#endif

// input:
//  - number of desired copies -> labelcnt
//  - definition of desired labels -> labelmasks
//  - number of available servers -> servcnt
//  - pointers to servers -> servers
// output:
//  [0 .. labelscnt-1] -> matching server index (+labelcnt)
//  [labelcnt .. labelcnt+servcnt-1] -> matching label definition
// example:
//   3,6,[A,A,A],[B,A,A,C,A,A] -> [4,5,7,-1,0,1,-1,2,-1,-1]

int32_t* do_perfect_match(uint32_t labelcnt,uint32_t servcnt,uint32_t **labelmasks,uint16_t *servers) {
	uint32_t s,i,l,x,v,vi,sp,rsp,sid,sids,gr;
	int32_t t;
	static int32_t *imatching = NULL;
	static int32_t *matching = NULL;
	static int32_t *augment = NULL;
	static uint8_t *visited = NULL;
	static int32_t *stack = NULL;
	static int32_t *group = NULL;
	static int32_t *grnode = NULL;
	static uint32_t *sidval = NULL;
	static int32_t *sidpos = NULL;
	static uint32_t tablength = 0;
	static uint32_t stablength = 0;

	if (labelcnt + servcnt > tablength || imatching==NULL || matching==NULL || augment==NULL || visited==NULL || stack==NULL) {
		tablength = 100 + 2 * (labelcnt + servcnt);
		if (imatching) {
			free(imatching);
		}
		if (matching) {
			free(matching);
		}
		if (augment) {
			free(augment);
		}
		if (visited) {
			free(visited);
		}
		if (stack) {
			free(stack);
		}
		imatching = malloc(sizeof(int32_t)*tablength);
		passert(imatching);
		matching = malloc(sizeof(int32_t)*tablength);
		passert(matching);
		augment = malloc(sizeof(int32_t)*tablength);
		passert(augment);
		visited = malloc(sizeof(uint8_t)*tablength);
		passert(visited);
		stack = malloc(sizeof(int32_t)*tablength);
		passert(stack);
	}
	if (servcnt > stablength || sidval==NULL || sidpos==NULL || grnode==NULL || group==NULL) {
		stablength = 100 + 2 * servcnt;
		if (sidval) {
			free(sidval);
		}
		if (sidpos) {
			free(sidpos);
		}
		if (grnode) {
			free(grnode);
		}
		if (group) {
			free(group);
		}
		sidval = malloc(sizeof(uint32_t)*stablength);
		passert(sidval);
		sidpos = malloc(sizeof(int32_t)*stablength);
		passert(sidpos);
		grnode = malloc(sizeof(int32_t)*stablength);
		passert(grnode);
		group = malloc(sizeof(int32_t)*stablength);
		passert(group);
	}

	s = servcnt + labelcnt;
	if (s==0) {
		return matching;
	}

	for (i=0 ; i<s ; i++) {
		imatching[i] = -1;
		matching[i] = -1;
		augment[i] = -1;
	}

	if (servcnt==0 || labelcnt==0) {
		return matching;
	}

	sp = 0;

	// calc groups
	sids = 0;
	for (i=0 ; i<servcnt ; i++) {
		if (DoNotUseSameIP) {
			sid = matocsserv_server_get_ip(cstab[servers[i]].ptr);
		} else if (DoNotUseSameRack) {
			sid = topology_get_rackid(matocsserv_server_get_ip(cstab[servers[i]].ptr));
		} else {
			sid = matocsserv_server_get_labelmask(cstab[servers[i]].ptr) & LabelUniqueMask;
		}
		if (sid==0) {
			group[i] = i;
		} else {
			for (l=0 ; l<sids && sid!=sidval[l] ; l++) { }
			if (l==sids) {
				sidval[l] = sid;
				sidpos[l] = i;
				sids++;
			}
			group[i] = sidpos[l];
		}
	}

	for (l=0 ; l<labelcnt ; l++) {
		if (imatching[l]==-1) {
			for (i=0 ; i<servcnt+labelcnt ; i++) {
				visited[i] = 0;
			}
			visited[l] = 1;
			augment[l] = -1;
			stack[sp++] = l; // push
			while (sp!=0) { // stack not empty
				x = stack[--sp]; // pop
				if (x<labelcnt) {
					rsp = sp;
					for (v=0 ; v<servcnt ; v++) {
#ifdef MATCH_LEFT
						vi = v;
#else
						vi = servcnt-1-v;
#endif
						gr = group[vi];
						if (visited[labelcnt+gr]==0) {
							if (matocsserv_server_has_labels(cstab[servers[vi]].ptr,labelmasks[x])) {
								visited[labelcnt+gr] = 1;
								augment[labelcnt+gr] = x;
								grnode[gr] = vi;
								stack[sp++] = labelcnt + gr; // push
							}
						}
					}
					// reverse stack from rsp to sp
					for (v=0 ; v<(sp-rsp)/2 ; v++) {
						// swap stack[rsp+v] , stack[sp-1-v]
						t = stack[sp-1-v];
						stack[sp-1-v] = stack[rsp+v];
						stack[rsp+v] = t;
					}
				} else if (imatching[x] >= 0) {
					augment[imatching[x]] = x;
					visited[imatching[x]] = 1;
					stack[sp++] = imatching[x];
				} else {
					while (augment[x]>=0) {
						if (x>=labelcnt) {
							imatching[x] = augment[x];
							imatching[augment[x]] = x;
							matching[augment[x]] = grnode[x-labelcnt] + labelcnt;
						}
						x = augment[x];
					}
					sp = 0; // clear stack;
				}
			}
		}
	}

	// add reverse matching
	for (i=0 ; i<labelcnt ; i++) {
		t = matching[i];
		if (t>=0) {
			matching[t] = i;
		}
	}
	return matching;
}

void chunk_stats(uint32_t chunkops[12]) {
	uint32_t i;
	for (i=0 ; i<12 ; i++) {
		chunkops[i] = stats_chunkops[i];
		stats_chunkops[i] = 0;
	}
}

CREATE_BUCKET_ALLOCATOR(slist,slist,10000000/sizeof(slist))

CREATE_BUCKET_ALLOCATOR(chunk,chunk,10000000/sizeof(chunk))

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
		if (chunkhashelem>chunkhashsize && (chunkhashsize>>HASHTAB_LOBITS)<HASHTAB_HISIZE) {
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
	newchunk->sclassid = 0;
	newchunk->lockedto = 0;
	newchunk->allvalidcopies = 0;
	newchunk->regularvalidcopies = 0;
	newchunk->needverincrease = 1;
	newchunk->ondangerlist = 0;
	newchunk->interrupted = 0;
	newchunk->writeinprogress = 0;
	newchunk->archflag = 0;
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
	uint32_t indx;
	if (lastchunkptr==c) {
		lastchunkid=0;
		lastchunkptr=NULL;
	}
	chunks--;
	indx = c->sclassid + (c->archflag?MAXSCLASS:0);
	allchunkcounts[indx][0]--;
	regularchunkcounts[indx][0]--;
	chunk_hash_delete(c);
	chunk_free(c);
}

static inline void chunk_state_change(uint8_t oldsclassid,uint8_t newsclassid,uint8_t oldarchflag,uint8_t newarchflag,uint8_t oldavc,uint8_t newavc,uint8_t oldrvc,uint8_t newrvc) {
	uint32_t oldindx = oldsclassid + (oldarchflag?MAXSCLASS:0);
	uint32_t newindx = newsclassid + (newarchflag?MAXSCLASS:0);
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
	allchunkcounts[oldindx][oldavc]--;
	allchunkcounts[newindx][newavc]++;
	regularchunkcounts[oldindx][oldrvc]--;
	regularchunkcounts[newindx][newrvc]++;
}

uint32_t chunk_count(void) {
	return chunks;
}

void chunk_sclass_counters(uint8_t sclassid,uint8_t archflag,uint8_t goal,uint64_t *undergoal,uint64_t *exactgoal,uint64_t *overgoal) {
	uint32_t indx = sclassid + (archflag?MAXSCLASS:0);
	uint32_t i;

	*undergoal = 0;
	*exactgoal = 0;
	*overgoal = 0;
	for (i=0 ; i<11 ; i++) {
		if (i<goal) {
			*undergoal += regularchunkcounts[indx][i];
		} else if (i>goal) {
			*overgoal += regularchunkcounts[indx][i];
		} else {
			*exactgoal = regularchunkcounts[indx][i];
		}
	}
}

void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regularvalidcopies) {
	uint32_t i,j,ag,rg;
	*allchunks = chunks;
	*allcopies = 0;
	*regularvalidcopies = 0;
	for (i=1 ; i<=10 ; i++) {
		ag=0;
		rg=0;
		for (j=0 ; j<MAXSCLASS*2 ; j++) {
			ag += allchunkcounts[j][i];
			rg += regularchunkcounts[j][i];
		}
		*allcopies += ag*i;
		*regularvalidcopies += rg*i;
	}
}

uint32_t chunk_get_missing_count(void) {
	uint32_t res=0;
	uint32_t i;

	for (i=1 ; i<MAXSCLASS*2 ; i++) {
		res+=allchunkcounts[i][0];
	}
	return res;
}

uint8_t chunk_counters_in_progress(void) {
//	syslog(LOG_NOTICE,"discservers: %p , discservers_next: %p , csregisterinprogress: %"PRIu16,discservers,discservers_next,csregisterinprogress);
	return ((discservers!=NULL || discservers_next!=NULL)?1:0)|((csregisterinprogress>0)?2:0)|csreceivingchunks;
}

void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid) {
	uint32_t i,j,indx;
	uint32_t counts[11][11];

	for (i=0 ; i<=10 ; i++) {
		for (j=0 ; j<=10 ; j++) {
			counts[i][j]=0;
		}
	}

	if (matrixid==0) {
		for (i=0 ; i<MAXSCLASS*2 ; i++) {
			indx = sclass_get_keeparch_goal(i%MAXSCLASS,i/MAXSCLASS);
			if (indx>10) {
				indx=10;
			}
			for (j=0 ; j<=10 ; j++) {
				counts[indx][j] += allchunkcounts[i][j];
			}
		}
	} else if (matrixid==1) {
		for (i=0 ; i<MAXSCLASS*2 ; i++) {
			indx = sclass_get_keeparch_goal(i%MAXSCLASS,i/MAXSCLASS);
			if (indx>10) {
				indx=10;
			}
			for (j=0 ; j<=10 ; j++) {
				counts[indx][j] += regularchunkcounts[i][j];
			}
		}
	}

	for (i=0 ; i<=10 ; i++) {
		for (j=0 ; j<=10 ; j++) {
			put32bit(&buff,counts[i][j]);
		}
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

static inline void chunk_priority_queue_check(chunk *c,uint8_t checklabels) {
	slist *s;
	uint32_t vc,tdc;
	uint32_t goal;
	uint8_t j;
	static uint16_t *servers = NULL;
	uint32_t servcnt;
	uint32_t **labelmasks;
	uint32_t labelcnt;
	int32_t *matching;
	uint8_t wronglabels;
#ifdef MFSDEBUG
	uint8_t debug;
#endif

	if (c==NULL) {
		if (servers==NULL && checklabels==0) {
			servers = malloc(sizeof(uint16_t)*MAXCSCOUNT);
			passert(servers);
		}
		if (servers!=NULL && checklabels==1) {
			free(servers);
		}
		return;
	}

#ifdef MFSDEBUG
	debug = ((c->chunkid&0xFFF)==0)?1:0;
#endif
	if (c->ondangerlist || servers==NULL || c->sclassid==0 || c->fcount==0 || c->lockedto+3600>=(uint32_t)main_time()) {
#ifdef MFSDEBUG
		if (debug) {
			mfsdebug("chunk_priority_queue_check ; chunkid=%016"PRIX64" ; ignore: ondangerlist=%u ; servers%c=NULL ; sclassid=%u ; fcount=%u ; lockedto=%"PRIu32,c->chunkid,c->ondangerlist,(servers==NULL)?'=':'!',c->sclassid,c->fcount,c->lockedto);
		}
#endif
		return;
	}
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
	wronglabels = 0;
	goal = sclass_get_keeparch_goal(c->sclassid,c->archflag);
	if (((DoNotUseSameIP | DoNotUseSameRack | LabelUniqueMask) || sclass_has_keeparch_labels(c->sclassid,c->archflag)) && vc >= goal && checklabels) {
		servcnt = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==VALID) {
				servers[servcnt++] = s->csid;
			}
		}
		labelcnt = sclass_get_keeparch_labelmasks(c->sclassid,c->archflag,&labelmasks);
		matching = do_perfect_match(labelcnt,servcnt,labelmasks,servers);
		for (j=0 ; j<labelcnt ; j++) {
			if (matching[j]<0) { // there are unmatched labels
				wronglabels = 1;
				break;
			}
		}
	}
	if (vc+tdc > 0 && (vc != goal || wronglabels)) { // wrong-goal chunk
		if (vc+tdc==1 && goal>2) { // highest priority - chunks with one copy and high goal
			j = DPRIORITY_ENDANGERED_HIGHGOAL;
		} else if (vc+tdc==1 && goal==2) { // next priority - chunks with one copy
			j = DPRIORITY_ENDANGERED;
		} else if (vc==1 && tdc>0) { // next priority - chunks on one regular disk and some "marked for removal" disks
			j = DPRIORITY_UNDERGOAL_MFR;
		} else if (tdc>0) { // next priority - chunks on "marked for removal" disks
			j = DPRIORITY_MFR;
		} else if (vc < goal) { // next priority - standard undergoal chunks
			j = DPRIORITY_UNDERGOAL;
		} else if (wronglabels) { // next priority - changed labels
			j = DPRIORITY_WRONGLABEL;
		} else { // lowest priority - overgoal
			j = DPRIORITY_OVERGOAL;
		}
#ifdef MFSDEBUG
		if (debug) {
			mfsdebug("chunk_priority_queue_check ; chunkid=%016"PRIX64" ; enqueue: priority=%u ; vc=%u ; tdc=%u ; goal=%u ; wronglabels=%u",c->chunkid,j,vc,tdc,goal,wronglabels);
		}
#endif
		chunk_priority_enqueue(j,c);
#ifdef MFSDEBUG
	} else if (debug) {
		mfsdebug("chunk_priority_queue_check ; chunkid=%016"PRIX64" ; exit: vc=%u ; tdc=%u ; goal=%u ; wronglabels=%u",c->chunkid,vc,tdc,goal,wronglabels);
#endif
	}
}

/* --- */

void chunk_addopchunk(uint16_t csid,uint64_t chunkid) {
	csopchunk *csop;
	csop = malloc(sizeof(csopchunk));
	csop->chunkid = chunkid;
	csop->status = MFS_ERROR_MISMATCH;
	csop->next = cstab[csid].opchunks;
	cstab[csid].opchunks = csop;
	opsinprogress++;
	return;
}

void chunk_statusopchunk(uint16_t csid,uint64_t chunkid,uint8_t status) {
	csopchunk *csop;
	for (csop=cstab[csid].opchunks ; csop!=NULL; csop=csop->next) {
		if (csop->chunkid==chunkid) {
			csop->status = status;
			return;
		}
	}
}

uint8_t chunk_delopchunk(uint16_t csid,uint64_t chunkid) {
	csopchunk **csopp,*csop;
	uint8_t status;

	status = MFS_ERROR_MISMATCH;
	csopp = &(cstab[csid].opchunks);
	while ((csop = (*csopp))) {
		if (csop->chunkid == chunkid) {
			status = csop->status;
			*csopp = csop->next;
			free(csop);
			if (opsinprogress>0) {
				opsinprogress--;
			}
		} else {
			csopp = &(csop->next);
		}
	}
	return status;
}

static inline uint16_t chunk_creation_servers(uint16_t csids[MAXCSCOUNT],uint8_t sclassid,uint8_t *olflag,uint32_t clientip) {
	uint16_t tmpcsids[MAXCSCOUNT];
	int32_t *matching;
	uint32_t **labelmasks;
	uint8_t labelcnt;
	uint8_t sclass_mode;
	uint16_t servcount;
	uint16_t goodlabelscount;
	uint16_t overloaded;
	uint32_t i,j;
	uint32_t dist;
	uint16_t cpos,fpos;
	int32_t x;

	servcount = matocsserv_getservers_wrandom(csids,&overloaded);
	if (servcount==0) {
		*olflag = (overloaded>0)?1:0;
		return 0;
	}
	sclass_mode = sclass_get_mode(sclassid);
	labelcnt = sclass_get_create_goal(sclassid);
	if (servcount < labelcnt && servcount + overloaded >= labelcnt) {
		*olflag = 1;
		return 0;
	} else {
		*olflag = 0;
	}

	if (CreationsRespectTopology>0) {
		cpos = 0;
		fpos = MAXCSCOUNT;
		for (i=0 ; i<servcount ; i++) {
			dist = topology_distance(matocsserv_server_get_ip(cstab[csids[i]].ptr),clientip);
			if (dist < CreationsRespectTopology) { // close
				tmpcsids[cpos++] = csids[i];
			} else { // far
				tmpcsids[--fpos] = csids[i];
			}
		}
		if (cpos!=0 && fpos!=MAXCSCOUNT) {
			for (i=0 ; i<cpos ; i++) {
				csids[i] = tmpcsids[i];
			}
			for (i=fpos,j=servcount-1 ; i<MAXCSCOUNT ; i++,j--) {
				csids[j] = tmpcsids[i];
			}
		}
	}

	if (sclass_has_create_labels(sclassid)) {
		labelcnt = sclass_get_create_labelmasks(sclassid,&labelmasks);

		// reverse server list
		for (i=0 ; i<servcount/2 ; i++) {
			x = csids[i];
			csids[i] = csids[servcount-1-i];
			csids[servcount-1-i] = x;
		}

		// match servers to labels
		matching = do_perfect_match(labelcnt,servcount,labelmasks,csids);

		if (sclass_mode != SCLASS_MODE_STRICT) {
			goodlabelscount = 0;
			// extend matching to fulfill goal
			for (i=0 ; i<labelcnt ; i++) {
				if (matching[i]<0) {
					for (j=0 ; j<servcount ; j++) {
						if (matching[labelcnt+servcount-j-1]<0) {
							matching[i] = labelcnt+servcount-j-1;
							matching[labelcnt+servcount-j-1] = i;
							break;
						}
					}
				} else {
					goodlabelscount++;
				}
				if (matching[i]<0) { // no more servers
					break;
				}
			}

			if (sclass_mode == SCLASS_MODE_STD) {
				if (goodlabelscount < labelcnt && goodlabelscount + overloaded >= labelcnt) {
					*olflag = 1;
					return 0;
				}
			}

		}

		// setting servers in proper order
		i = 0;
		j = servcount-1;
		while (i<j) {
			while (i<j && matching[labelcnt+i]>=0) {
				i++;
			}
			while (i<j && matching[labelcnt+j]<0) {
				j--;
			}
			if (i<j) {
				x = matching[labelcnt+i];
				matching[labelcnt+i] = matching[labelcnt+j];
				matching[labelcnt+j] = x;
				x = csids[i];
				csids[i] = csids[j];
				csids[j] = x;
			}
		}
		if (sclass_mode == SCLASS_MODE_STRICT) {
			if (i < labelcnt && i + overloaded >= labelcnt) {
				*olflag = 1;
				return 0;
			}
			return i;
		}
	}
	return servcount;
}

void chunk_emergency_increase_version(chunk *c) {
	slist *s;
	uint32_t i;
	i=0;
//	chunk_remove_disconnected_chunks(c);
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
			if (s->valid==TDVALID || s->valid==TDBUSY) {
				s->valid = TDBUSY;
			} else {
				s->valid = BUSY;
			}
			s->version = c->version+1;
			stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
			matocsserv_send_setchunkversion(cstab[s->csid].ptr,c->chunkid,c->version+1,c->version);
			chunk_addopchunk(s->csid,c->chunkid);
			i++;
		}
	}
	if (i>0) {	// should always be true !!!
		c->interrupted = 0;
		c->operation = SET_VERSION;
		c->version++;
		changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
	} else {
		matoclserv_chunk_status(c->chunkid,MFS_ERROR_CHUNKLOST);
	}
}

static inline int chunk_remove_disconnected_chunks(chunk *c) {
	uint8_t opfinished,validcopies,disc;
	uint8_t verfixed;
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
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
				c->allvalidcopies--;
			}
			if (s->valid==BUSY || s->valid==VALID) {
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
				c->allvalidcopies--;
				c->regularvalidcopies--;
			}
			if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) { // pro forma
				matocsserv_write_counters(cstab[s->csid].ptr,0);
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
	if (c->lockedto<(uint32_t)main_time() && c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
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

		if (opfinished && validcopies==0 && (c->operation==SET_VERSION || c->operation==TRUNCATE)) { // we know that version increase was just not completed, so all WVER chunks with version exactly one lower than chunk version are actually VALID copies
			verfixed = 0;
			for (s=c->slisthead ; s!=NULL ; s=s->next) {
				if (s->version+1==c->version) {
					if (s->valid==TDWVER) {
						verfixed = 1;
						s->valid = TDVALID;
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies++;
					} else if (s->valid==WVER) {
						verfixed = 1;
						s->valid = VALID;
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
						c->allvalidcopies++;
						c->regularvalidcopies++;
					}
				}
				if (verfixed) {
					c->version--;
					changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
				}
			}
			// we continue because we still want to return status not done to matoclserv module
		}

		if (opfinished) {
			uint8_t nospace,status;
			nospace = 1;
			for (s=c->slisthead ; s ; s=s->next) {
				status = chunk_delopchunk(s->csid,c->chunkid);
				if (status!=MFS_ERROR_MISMATCH && status!=MFS_ERROR_NOSPACE) {
					nospace = 0;
				}
			}
			if (c->operation==REPLICATE) {
				c->operation = NONE;
				c->lockedto = 0;
				matoclserv_chunk_unlocked(c->chunkid,c);
			} else {
				if (validcopies) {
					chunk_emergency_increase_version(c);
				} else {
					if (nospace) {
						matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOSPACE);
					} else {
						matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOTDONE);
					}
					c->operation = NONE;
				}
			}
		} else {
			if (c->operation!=REPLICATE) {
				c->interrupted = 1;
			}
		}
	}
	chunk_priority_queue_check(c,1);
	return 0;
}

int chunk_mr_increase_version(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	c->version++;
	meta_version_inc();
	return MFS_STATUS_OK;
}

/* --- */

static inline void chunk_find_sclassid(chunk *c) {
	uint32_t i;
	uint8_t g;
	uint8_t mg;
	uint8_t sclassid,v;
	mg = 0;
	sclassid = 0;
	v = 0;
	for (i=1 ; i<c->ftab[0] ; i++) {
		if (c->ftab[i]>0) {
			g = sclass_get_keepmax_goal(i);
			if (g>mg) {
				mg = g;
				sclassid = i;
				v = 0;
			} else if (g==mg) {
				if (sclassid<=9 && v==0) {
					sclassid = i;
				} else if (sclassid>9 && i>9) {
					sclassid = g;
					v = 1;
				}
			}
		}
	}
	massert(sclassid>0,"wrong labels set");
	c->sclassid = sclassid;
}

int chunk_change_file(uint64_t chunkid,uint8_t prevsclassid,uint8_t newsclassid) {
	chunk *c;
	uint8_t oldsclassid;

	if (prevsclassid==newsclassid) {
		return MFS_STATUS_OK;
	}
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->fcount==0) {
		syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",c->chunkid);
		return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
	}
	oldsclassid = c->sclassid;
	if (c->fcount==1) {
		c->sclassid = newsclassid;
	} else {
		if (c->ftab==NULL) {
			uint32_t ftableng = prevsclassid;
			if (newsclassid > ftableng) {
				ftableng = newsclassid;
			}
			ftableng++;
			c->ftab = malloc(sizeof(uint32_t)*(ftableng+1));
			passert(c->ftab);
			memset(c->ftab,0,sizeof(uint32_t)*(ftableng+1));
			c->ftab[0] = ftableng+1;
			massert(c->sclassid==prevsclassid,"wrong labels set");
			c->ftab[prevsclassid] = c->fcount-1;
			c->ftab[newsclassid] = 1;
			chunk_find_sclassid(c);
		} else {
			if (newsclassid >= c->ftab[0]) {
				c->ftab = realloc(c->ftab,sizeof(uint32_t)*(newsclassid+1));
				passert(c->ftab);
				memset(c->ftab+c->ftab[0],0,sizeof(uint32_t)*(newsclassid+1-c->ftab[0]));
				c->ftab[0] = newsclassid+1;
			}
			massert(c->ftab[prevsclassid]>0,"wrong ftab entry");
			c->ftab[prevsclassid]--;
			c->ftab[newsclassid]++;
			chunk_find_sclassid(c);
		}
	}
	if (oldsclassid!=c->sclassid) {
		chunk_state_change(oldsclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
		chunk_priority_queue_check(c,1);
	} else {
		chunk_priority_queue_check(c,0);
	}
	return MFS_STATUS_OK;
}

static inline int chunk_delete_file_int(chunk *c,uint8_t sclassid,uint32_t delete_timeout) {
	uint8_t oldsclassid;

	if (c->fcount==0) {
		syslog(LOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",c->chunkid);
		return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
	}
	massert(sclassid>0,"wrong labels set");
	oldsclassid = c->sclassid;
	if (c->fcount==1) {
		c->sclassid = 0;
		c->fcount = 0;
//#ifdef METARESTORE
//		printf("D%"PRIu64"\n",c->chunkid);
//#endif
	} else {
		if (c->ftab) {
			c->ftab[sclassid]--;
			chunk_find_sclassid(c);
		}
		c->fcount--;
		if (c->fcount==1 && c->ftab) {
			free(c->ftab);
			c->ftab = NULL;
		}
	}
	if (oldsclassid!=c->sclassid) {
		chunk_state_change(oldsclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
	}
	if (c->fcount==0 && delete_timeout>0) {
		c->lockedto = (uint32_t)main_time()+delete_timeout;
	}
	return MFS_STATUS_OK;
}

static inline int chunk_add_file_int(chunk *c,uint8_t sclassid) {
	uint8_t oldsclassid;

	massert(sclassid>0,"wrong labels set");
	oldsclassid = c->sclassid;
	if (c->fcount==0) {
		c->sclassid = sclassid;
		c->fcount = 1;
	} else if (sclassid==c->sclassid) {
		c->fcount++;
		if (c->ftab) {
			c->ftab[sclassid]++;
		}
	} else {
		if (c->ftab==NULL) {
			uint32_t ftableng = c->sclassid;
			if (sclassid > ftableng) {
				ftableng = sclassid;
			}
			ftableng++;
			c->ftab = malloc(sizeof(uint32_t)*(ftableng+1));
			passert(c->ftab);
			memset(c->ftab,0,sizeof(uint32_t)*(ftableng+1));
			c->ftab[0] = ftableng+1;
			c->ftab[c->sclassid] = c->fcount;
			c->ftab[sclassid] = 1;
			c->fcount++;
			chunk_find_sclassid(c);
		} else {
			if (sclassid >= c->ftab[0]) {
				c->ftab = realloc(c->ftab,sizeof(uint32_t)*(sclassid+1));
				passert(c->ftab);
				memset(c->ftab+c->ftab[0],0,sizeof(uint32_t)*(sclassid+1-c->ftab[0]));
				c->ftab[0] = sclassid+1;
			}
			c->ftab[sclassid]++;
			c->fcount++;
			chunk_find_sclassid(c);
		}
	}
	if (oldsclassid!=c->sclassid) {
		chunk_state_change(oldsclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
	}
	return MFS_STATUS_OK;
}

int chunk_delete_file(uint64_t chunkid,uint8_t sclassid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	return chunk_delete_file_int(c,sclassid,0);
}

int chunk_add_file(uint64_t chunkid,uint8_t sclassid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	return chunk_add_file_int(c,sclassid);
}

static inline void chunk_write_counters(chunk *c,uint8_t x) {
	slist *s;
	if (x) {
		if (c->writeinprogress==0) {
			c->writeinprogress = 1;
		} else {
			return;
		}
	} else {
		if (c->writeinprogress) {
			c->writeinprogress = 0;
		} else {
			return;
		}
	}
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
			matocsserv_write_counters(cstab[s->csid].ptr,x);
		}
	}
}

int chunk_locked_or_busy(void *cptr) {
	chunk *c = (chunk*)cptr;
	return (c->lockedto<(uint32_t)(main_time()) && c->operation==NONE)?0:1;
}

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies) {
	chunk *c;
	*vcopies = 0;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	*vcopies = c->allvalidcopies;
	return MFS_STATUS_OK;
}

int chunk_get_archflag(uint64_t chunkid,uint8_t *archflag) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	*archflag = c->archflag;
	return MFS_STATUS_OK;
}

int chunk_univ_archflag(uint64_t chunkid,uint8_t archflag,uint32_t *archflagchanged) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (archflag != c->archflag) {
		chunk_state_change(c->sclassid,c->sclassid,c->archflag,archflag,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
		c->archflag = archflag;
		chunk_priority_queue_check(c,1);
		(*archflagchanged)++;
	}
	return MFS_STATUS_OK;
}

// CHUNK_FLOOP_NOTFOUND
// CHUNK_FLOOP_DELETED
// CHUNK_FLOOP_MISSING_NOCOPY
// CHUNK_FLOOP_MISSING_INVALID
// CHUNK_FLOOP_MISSING_WRONGVERSION
// CHUNK_FLOOP_UNDERGOAL_AFLAG_NOT_CHANGED
// CHUNK_FLOOP_UNDERGOAL_AFLAG_CHANGED
// CHUNK_FLOOP_OK_AFLAG_NOT_CHANGED
// CHUNK_FLOOP_OK_AFLAG_CHANGED
chunkfloop chunk_fileloop_task(uint64_t chunkid,uint8_t sclassid,uint8_t aftereof,uint8_t archflag) {
	chunk *c;
	slist *s;
	uint8_t aflagchg;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return CHUNK_FLOOP_NOTFOUND;
	}
	if (c->allvalidcopies==0 && aftereof && c->lockedto<(uint32_t)(main_time()) && c->operation==NONE) {
		chunk_delete_file_int(c,sclassid,UNUSED_DELETE_TIMEOUT);
		return CHUNK_FLOOP_DELETED;
	}
	if (c->allvalidcopies==0) {
		if (c->slisthead==NULL) {
			return CHUNK_FLOOP_MISSING_NOCOPY;
		}
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==WVER || s->valid==TDWVER) {
				return CHUNK_FLOOP_MISSING_WRONGVERSION;
			}
		}
		return CHUNK_FLOOP_MISSING_INVALID;
	}
	if (archflag==1 && c->archflag==0) {
		chunk_state_change(c->sclassid,c->sclassid,c->archflag,archflag,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,c->regularvalidcopies);
		c->archflag = archflag;
		chunk_priority_queue_check(c,1);
		aflagchg = 1;
	} else {
		aflagchg = 0;
	}
	if (c->allvalidcopies < sclass_get_keeparch_goal(c->sclassid,c->archflag)) {
		return aflagchg?CHUNK_FLOOP_UNDERGOAL_AFLAG_CHANGED:CHUNK_FLOOP_UNDERGOAL_AFLAG_NOT_CHANGED;
	}
	return aflagchg?CHUNK_FLOOP_OK_AFLAG_CHANGED:CHUNK_FLOOP_OK_AFLAG_NOT_CHANGED;
}

int chunk_read_check(uint32_t ts,uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->lockedto>=ts) {
		return MFS_ERROR_LOCKED;
	}
	if (c->operation != NONE) {
		return MFS_ERROR_CHUNKBUSY;
	}
	return MFS_STATUS_OK;
}

int chunk_univ_multi_modify(uint32_t ts,uint8_t mr,uint8_t continueop,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t *opflag,uint32_t clientip) {
	uint16_t csids[MAXCSCOUNT];
	static void **chosen = NULL;
	static uint32_t chosenleng = 0;
	uint16_t servcount=0;
	uint32_t vc;
	uint8_t overloaded;
	slist *os,*s;
	uint32_t i;
	chunk *oc,*c;
	uint8_t csstable,csalldata;
	uint8_t cschanges;

	if (ts>(starttime+10) && csregisterinprogress==0) {
		csstable = 1;
	} else {
		csstable = 0;
	}

	cschanges = (csstable==0 || (csreceivingchunks&2))?1:0;

	if (chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		csalldata = 1;
	} else {
		csalldata = 0;
	}

	if (ochunkid==0) {	// new chunk
		if (mr==0) {
			servcount = chunk_creation_servers(csids,sclassid,&overloaded,clientip);
			if (servcount==0) {
				if (overloaded || csalldata==0) {
					return MFS_ERROR_EAGAIN; // try again for ever
				} else {
					uint16_t scount;
					scount = matocsserv_servers_count();
					if (scount>0 && csstable) {
						return MFS_ERROR_NOSPACE; // return error
					} else {
						return MFS_ERROR_NOCHUNKSERVERS; // try again
					}
				}
			}
			c = chunk_new(nextchunkid++);
			c->version = 1;
			c->interrupted = 0;
			c->operation = CREATE;
			chunk_add_file_int(c,sclassid);
			if (servcount<sclass_get_create_goal(sclassid)) {
				c->allvalidcopies = servcount;
				c->regularvalidcopies = servcount;
			} else {
				c->allvalidcopies = sclass_get_create_goal(sclassid);
				c->regularvalidcopies = sclass_get_create_goal(sclassid);
			}
			if (c->allvalidcopies>chosenleng) {
				chosenleng = c->allvalidcopies+10;
				chosen = malloc(sizeof(void*)*chosenleng);
				passert(chosen);
			}
			for (i=0 ; i<c->allvalidcopies ; i++) {
				s = slist_malloc();
				s->csid = csids[i];
				s->valid = BUSY;
				s->version = c->version;
				s->next = c->slisthead;
				c->slisthead = s;
				chosen[i] = cstab[s->csid].ptr;
				stats_chunkops[CHUNK_OP_CREATE_TRY]++;
				matocsserv_send_createchunk(cstab[s->csid].ptr,c->chunkid,c->version);
				chunk_addopchunk(s->csid,c->chunkid);
			}
			matocsserv_useservers_wrandom(chosen,c->allvalidcopies);
			chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,0,c->allvalidcopies,0,c->regularvalidcopies);
			*opflag=1;
			*nchunkid = c->chunkid;
		} else {
			if (*nchunkid != nextchunkid) {
				return MFS_ERROR_MISMATCH;
			}
			c = chunk_new(nextchunkid++);
			c->version = 1;
			chunk_add_file_int(c,sclassid);
		}
	} else {
		c = NULL;
		oc = chunk_find(ochunkid);
		if (oc && mr==0) {
			if (chunk_remove_disconnected_chunks(oc)) {
				oc = NULL;
			}
		}
		if (oc==NULL) {
			return MFS_ERROR_NOCHUNK;
		}
		if (mr==0 && oc->lockedto>=ts && continueop==0) {
			return MFS_ERROR_LOCKED;
		}
		if (oc->fcount==1) {
			c = oc;
			if (mr==0) {
				*nchunkid = ochunkid;
				if (c->operation!=NONE) {
					return MFS_ERROR_CHUNKBUSY;
				}
				if (cschanges) {
					vc = 0;
					for (s=c->slisthead ; s ; s=s->next) {
						if (s->valid==VALID) {
							vc++;
						}
					}
					if (vc < sclass_get_keeparch_goal(c->sclassid,c->archflag)) {
						return MFS_ERROR_EAGAIN; // just try again later
					}
				}
				if (c->needverincrease) {
					i=0;
					for (s=c->slisthead ;s ; s=s->next) {
						if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
							if (s->valid==TDVALID || s->valid==TDBUSY) {
								s->valid = TDBUSY;
							} else {
								s->valid = BUSY;
							}
							s->version = c->version+1;
							stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
							matocsserv_send_setchunkversion(cstab[s->csid].ptr,ochunkid,c->version+1,c->version);
							chunk_addopchunk(s->csid,c->chunkid);
							i++;
						}
					}
					if (i>0) {
						c->interrupted = 0;
						c->operation = SET_VERSION;
						c->version++;
						*opflag = 1;
					} else {
						if (csalldata) {
							return MFS_ERROR_CHUNKLOST; // return error
						} else {
							return MFS_ERROR_CSNOTPRESENT; // try again
						}
					}
				} else {
					*opflag = 0;
				}
			} else {
				if (*nchunkid != ochunkid) {
					return MFS_ERROR_MISMATCH;
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
				return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
			}
			if (mr==0) {
				if (oc->operation!=NONE) {
					return MFS_ERROR_CHUNKBUSY;
				}
				if (cschanges) {
					vc = 0;
					for (os=oc->slisthead ; os ; os=os->next) {
						if (os->valid==VALID) {
							vc++;
						}
					}
					if (vc < sclass_get_keeparch_goal(oc->sclassid,oc->archflag)) {
						return MFS_ERROR_EAGAIN; // just try again later
					}
				}
				i=0;
				for (os=oc->slisthead ;os ; os=os->next) {
					if (os->valid!=INVALID && os->valid!=DEL && os->valid!=WVER && os->valid!=TDWVER) {
						if (c==NULL) {
							c = chunk_new(nextchunkid++);
							c->version = 1;
							c->interrupted = 0;
							c->operation = DUPLICATE;
							chunk_delete_file_int(oc,sclassid,0);
							chunk_add_file_int(c,sclassid);
						}
						s = slist_malloc();
						s->csid = os->csid;
						s->valid = BUSY;
						s->version = c->version;
						s->next = c->slisthead;
						c->slisthead = s;
						c->allvalidcopies++;
						c->regularvalidcopies++;
						stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
						matocsserv_send_duplicatechunk(cstab[s->csid].ptr,c->chunkid,c->version,oc->chunkid,oc->version);
						chunk_addopchunk(s->csid,c->chunkid);
						i++;
					}
				}
				if (c!=NULL) {
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,0,c->allvalidcopies,0,c->regularvalidcopies);
				}
				if (i>0) {
					*nchunkid = c->chunkid;
					*opflag=1;
				} else {
					if (csalldata) {
						return MFS_ERROR_CHUNKLOST; // return error
					} else {
						return MFS_ERROR_CSNOTPRESENT; // try again
					}
				}
			} else {
				if (*nchunkid != nextchunkid) {
					return MFS_ERROR_MISMATCH;
				}
				c = chunk_new(nextchunkid++);
				c->version = 1;
				chunk_delete_file_int(oc,sclassid,0);
				chunk_add_file_int(c,sclassid);
				*nchunkid = c->chunkid;
			}
		}
	}

	c->lockedto = ts+LOCKTIMEOUT;
	chunk_write_counters(c,1);
	return MFS_STATUS_OK;
}

int chunk_multi_modify(uint8_t continueop,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t *opflag,uint32_t clientip) {
	return chunk_univ_multi_modify(main_time(),0,continueop,nchunkid,ochunkid,sclassid,opflag,clientip);
}

int chunk_mr_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t opflag) {
	return chunk_univ_multi_modify(ts,1,0,nchunkid,ochunkid,sclassid,&opflag,0);
}

int chunk_univ_multi_truncate(uint32_t ts,uint8_t mr,uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t sclassid) {
	slist *os,*s;
	uint32_t i;
	chunk *oc,*c;
	uint8_t csstable,csalldata;
	uint8_t cschanges;
	uint32_t vc;

	if (ts>(starttime+10) && csregisterinprogress==0) {
		csstable = 1;
	} else {
		csstable = 0;
	}

	cschanges = (csstable==0 || (csreceivingchunks&2))?1:0;

	if (chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		csalldata = 1;
	} else {
		csalldata = 0;
	}

	c=NULL;
	oc = chunk_find(ochunkid);
	if (oc && mr==0) {
		if (chunk_remove_disconnected_chunks(oc)) {
			oc = NULL;
		}
	}

	if (oc==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (mr==0 && oc->lockedto>=ts) {
		return MFS_ERROR_LOCKED;
	}
	if (oc->fcount==1) {
		c = oc;
		if (mr==0) {
			*nchunkid = ochunkid;
			if (c->operation!=NONE) {
				return MFS_ERROR_CHUNKBUSY;
			}
			if (cschanges) {
				vc = 0;
				for (os=oc->slisthead ; os ; os=os->next) {
					if (os->valid==VALID) {
						vc++;
					}
				}
				if (vc < sclass_get_keeparch_goal(oc->sclassid,oc->archflag)) {
					return MFS_ERROR_EAGAIN; // just try again later
				}
			}
			i=0;
			for (s=c->slisthead ;s ; s=s->next) {
				if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
					if (s->valid==TDVALID || s->valid==TDBUSY) {
						s->valid = TDBUSY;
					} else {
						s->valid = BUSY;
					}
					s->version = c->version+1;
					stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
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
				if (csalldata) {
					return MFS_ERROR_CHUNKLOST; // return error
				} else {
					return MFS_ERROR_CSNOTPRESENT; // try again
				}
			}
		} else {
			if (*nchunkid != ochunkid) {
				return MFS_ERROR_MISMATCH;
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
			return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
		}
		if (mr==0) {
			if (oc->operation!=NONE) {
				return MFS_ERROR_CHUNKBUSY;
			}
			if (cschanges) {
				vc = 0;
				for (os=oc->slisthead ; os ; os=os->next) {
					if (os->valid==VALID) {
						vc++;
					}
				}
				if (vc < sclass_get_keeparch_goal(oc->sclassid,oc->archflag)) {
					return MFS_ERROR_EAGAIN; // just try again later
				}
			}
			i=0;
			for (os=oc->slisthead ;os ; os=os->next) {
				if (os->valid!=INVALID && os->valid!=DEL && os->valid!=WVER && os->valid!=TDWVER) {
					if (c==NULL) {
						c = chunk_new(nextchunkid++);
						c->version = 1;
						c->interrupted = 0;
						c->operation = DUPTRUNC;
						chunk_delete_file_int(oc,sclassid,0);
						chunk_add_file_int(c,sclassid);
					}
					s = slist_malloc();
					s->csid = os->csid;
					s->valid = BUSY;
					s->version = c->version;
					s->next = c->slisthead;
					c->slisthead = s;
					c->allvalidcopies++;
					c->regularvalidcopies++;
					stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
					matocsserv_send_duptruncchunk(cstab[s->csid].ptr,c->chunkid,c->version,oc->chunkid,oc->version,length);
					chunk_addopchunk(s->csid,c->chunkid);
					i++;
				}
			}
			if (c!=NULL) {
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,0,c->allvalidcopies,0,c->regularvalidcopies);
			}
			if (i>0) {
				*nchunkid = c->chunkid;
			} else {
				if (csalldata) {
					return MFS_ERROR_CHUNKLOST; // return error
				} else {
					return MFS_ERROR_CSNOTPRESENT; // try again
				}
			}
		} else {
			if (*nchunkid != nextchunkid) {
				return MFS_ERROR_MISMATCH;
			}
			c = chunk_new(nextchunkid++);
			c->version = 1;
			chunk_delete_file_int(oc,sclassid,0);
			chunk_add_file_int(c,sclassid);
			*nchunkid = c->chunkid;
		}
	}

	c->lockedto=ts+LOCKTIMEOUT;
	return MFS_STATUS_OK;
}

int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t sclassid) {
	return chunk_univ_multi_truncate(main_time(),0,nchunkid,ochunkid,length,sclassid);
}

int chunk_mr_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid) {
	return chunk_univ_multi_truncate(ts,1,nchunkid,ochunkid,0,sclassid);
}

int chunk_repair(uint8_t sclassid,uint64_t ochunkid,uint32_t *nversion) {
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
	chunk_write_counters(c,0);
	bestversion = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (cstab[s->csid].valid) {
			if (s->valid == VALID || s->valid == TDVALID || s->valid == BUSY || s->valid == TDBUSY) {	// found chunk that is ok - so return
				return 0;
			}
			if (s->valid == WVER || s->valid == TDWVER) {
				if (s->version>=bestversion) {
					bestversion = s->version;
				}
			}
		}
	}
	if (bestversion==0) {	// didn't find sensible chunk - so erase it
		chunk_delete_file_int(c,sclassid,0);
		return 1;
	}
	if (c->allvalidcopies>0 || c->regularvalidcopies>0) {
		if (c->allvalidcopies>0) {
			syslog(LOG_WARNING,"wrong all valid copies counter - (counter value: %u, should be: 0) - fixed",c->allvalidcopies);
		}
		if (c->regularvalidcopies>0) {
			syslog(LOG_WARNING,"wrong regular valid copies counter - (counter value: %u, should be: 0) - fixed",c->regularvalidcopies);
		}
		chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,0,c->regularvalidcopies,0);
		c->allvalidcopies = 0;
		c->regularvalidcopies = 0;
	}
	c->version = bestversion;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->version==bestversion && cstab[s->csid].valid) {
			if (s->valid==WVER) {
				s->valid = VALID;
				c->allvalidcopies++;
				c->regularvalidcopies++;
			} else if (s->valid==TDWVER) {
				s->valid = TDVALID;
				c->allvalidcopies++;
			}
		}
	}
	*nversion = bestversion;
	chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,0,c->allvalidcopies,0,c->regularvalidcopies);
	c->needverincrease = 1;
	return 1;
}

int chunk_mr_set_version(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	c->version = version;
	meta_version_inc();
	return MFS_STATUS_OK;
}

/* ---- */

typedef struct locsort {
	uint32_t ip;
	uint16_t port;
	uint32_t csver;
	uint32_t labelmask;
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

uint8_t chunk_get_version_and_csdata(uint8_t mode,uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t cs_data[100*14]) {
	chunk *c;
	slist *s;
	uint8_t i;
	uint8_t cnt;
	uint8_t *wptr;
	locsort lstab[100];

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	*version = c->version;
	cnt=0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && cstab[s->csid].valid) {
			if (cnt<100 && matocsserv_get_csdata(cstab[s->csid].ptr,&(lstab[cnt].ip),&(lstab[cnt].port),&(lstab[cnt].csver),&(lstab[cnt].labelmask))==0) {
				lstab[cnt].dist = topology_distance(lstab[cnt].ip,cuip);	// in the future prepare more sofisticated distance function
				lstab[cnt].rnd = rndu32();
				cnt++;
			}
		}
	}
	if (cnt==0) {
		*count = 0;
		if (chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
			return MFS_ERROR_CHUNKLOST; // this is permanent state - chunk is definitely lost
		} else {
			return MFS_STATUS_OK;
		}
	}
	qsort(lstab,cnt,sizeof(locsort),chunk_locsort_cmp);
	wptr = cs_data;
	for (i=0 ; i<cnt ; i++) {
		put32bit(&wptr,lstab[i].ip);
		put16bit(&wptr,lstab[i].port);
		if (mode>0) {
			put32bit(&wptr,lstab[i].csver);
		}
		if (mode>1) {
			put32bit(&wptr,lstab[i].labelmask);
		}
	}
	*count = cnt;
	return MFS_STATUS_OK;
}

uint8_t chunk_get_copies(uint64_t chunkid,uint8_t *count) {
	chunk *c;
	slist *s;
	uint8_t cnt;
	uint32_t ip;
	uint16_t port;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	cnt = 0;
	for (s=c->slisthead ; s && cnt<100 ; s=s->next) {
		if (cstab[s->csid].valid && s->valid!=DEL) {
			if (matocsserv_get_csdata(cstab[s->csid].ptr,&ip,&port,NULL,NULL)==0) {
				cnt++;
			}
		}
	}
	*count = cnt;
	return MFS_STATUS_OK;
}

uint8_t chunk_get_version(uint64_t chunkid,uint32_t *version) {
	chunk *c;
	c = chunk_find(chunkid);

	if (c==NULL) {
		*version = 0;
		return MFS_ERROR_NOCHUNK;
	}
	*version = c->version;
	return MFS_STATUS_OK;
}

uint8_t chunk_get_version_and_copies(uint64_t chunkid,uint32_t *version,uint8_t *count,uint8_t cs_data[100*7]) {
	chunk *c;
	slist *s;
	uint8_t cnt;
	uint32_t ip;
	uint16_t port;
	uint8_t *wptr;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	*version = c->version;
	cnt=0;
	wptr = cs_data;

	for (s=c->slisthead ; s && cnt<100 ; s=s->next) {
		if (cstab[s->csid].valid && s->valid!=DEL) {
			if (matocsserv_get_csdata(cstab[s->csid].ptr,&ip,&port,NULL,NULL)==0) {
				put32bit(&wptr,ip);
				put16bit(&wptr,port);
				if (s->valid==VALID || s->valid==BUSY) {
					put8bit(&wptr,CHECK_VALID);
				} else if (s->valid==TDVALID || s->valid==TDBUSY) {
					put8bit(&wptr,CHECK_MARKEDFORREMOVAL);
				} else if (s->valid==WVER) {
					put8bit(&wptr,CHECK_WRONGVERSION);
				} else if (s->valid==TDWVER) {
					put8bit(&wptr,CHECK_WV_AND_MFR);
				} else {
					put8bit(&wptr,CHECK_INVALID);
				}
				cnt++;
			}
		}
	}
	*count = cnt;
	return MFS_STATUS_OK;
}

/* ---- */

int chunk_mr_nextchunkid(uint64_t nchunkid) {
	if (nchunkid>nextchunkid) {
		nextchunkid=nchunkid;
		meta_version_inc();
		return MFS_STATUS_OK;
	} else {
		return MFS_ERROR_MISMATCH;
	}
}

int chunk_mr_chunkadd(uint32_t ts,uint64_t chunkid,uint32_t version,uint32_t lockedto) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c) {
		return MFS_ERROR_CHUNKEXIST;
	}
	if (chunkid>nextchunkid+UINT64_C(1000000000)) {
		return MFS_ERROR_MISMATCH;
	}
	if (chunkid>=nextchunkid) {
		nextchunkid=chunkid+1;
	}
	if (lockedto>0 && lockedto<ts) {
		return MFS_ERROR_MISMATCH;
	}
	c = chunk_new(chunkid);
	c->version = version;
	c->lockedto = lockedto;
	meta_version_inc();
	return MFS_STATUS_OK;
}

int chunk_mr_chunkdel(uint32_t ts,uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version != version) {
		return MFS_ERROR_WRONGVERSION;
	}
	if (c->fcount!=0) {
		return MFS_ERROR_ACTIVE;
	}
	if (c->slisthead!=NULL) {
		return MFS_ERROR_CHUNKBUSY;
	}
	if (c->lockedto>=ts) {
		return MFS_ERROR_LOCKED;
	}
	chunk_delete(c);
	meta_version_inc();
	return MFS_STATUS_OK;
}

static inline void chunk_mfr_state_check(chunk *c) {
	slist *s;
	uint8_t goal,vc,tdc;
	goal = sclass_get_keeparch_goal(c->sclassid,c->archflag);
	vc = 0;
	tdc = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==VALID || s->valid==BUSY) {
			vc++;
		} else if (s->valid==TDVALID || s->valid==TDBUSY) {
			tdc++;
		}
	}
	if (vc < goal && tdc > 0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==TDVALID || s->valid==TDBUSY) {
				cstab[s->csid].mfr_state = REPL_IN_PROGRESS;
			}
		}
	}
}

void chunk_server_has_chunk(uint16_t csid,uint64_t chunkid,uint32_t version) {
	chunk *c;
	slist *s;
#ifndef MFSDEBUG
	static uint32_t loglastts = 0;
	static uint32_t ilogcount = 0;
	static uint32_t clogcount = 0;
#else
	uint8_t debug;
#endif

	cstab[csid].newchunkdelay = NEWCHUNKDELAY;
	csreceivingchunks |= 2;

	c = chunk_find(chunkid);
	if (c) {
		if (chunk_remove_disconnected_chunks(c)) {
			c = NULL;
		}
	}

	if (c==NULL) {
#ifndef MFSDEBUG
		if (loglastts+60<main_time()) {
			ilogcount=0;
			clogcount=0;
			loglastts = main_time();
		}
#endif
		if (chunkid==0 || chunkid>nextchunkid+UINT64_C(1000000000)) {
#ifndef MFSDEBUG
			if (ilogcount<10) {
#endif
				syslog(LOG_WARNING,"chunkserver (%s) has nonexistent chunk (%016"PRIX64"_%08"PRIX32"), id looks wrong - just ignore it",matocsserv_getstrip(cstab[csid].ptr),chunkid,(version&0x7FFFFFFF));
#ifndef MFSDEBUG
			} else if (ilogcount==10) {
				syslog(LOG_WARNING,"there are more nonexistent chunks to ignore - stop logging");
			}
			ilogcount++;
#endif
			return;
		}
#ifndef MFSDEBUG
		if (clogcount<10) {
#endif
			syslog(LOG_WARNING,"chunkserver (%s) has nonexistent chunk (%016"PRIX64"_%08"PRIX32"), so create it for future deletion",matocsserv_getstrip(cstab[csid].ptr),chunkid,(version&0x7FFFFFFF));
#ifndef MFSDEBUG
		} else if (clogcount==10) {
			syslog(LOG_WARNING,"there are more nonexistent chunks to create - stop logging");
		}
		clogcount++;
#endif
		if (chunkid>=nextchunkid) {
			nextchunkid=chunkid+1;
//			changelog("%"PRIu32"|NEXTCHUNKID(%"PRIu64")",main_time(),nextchunkid);
		}
		c = chunk_new(chunkid);
		c->version = (version&0x7FFFFFFF);
		c->lockedto = (uint32_t)main_time()+UNUSED_DELETE_TIMEOUT;
		changelog("%"PRIu32"|CHUNKADD(%"PRIu64",%"PRIu32",%"PRIu32")",main_time(),c->chunkid,c->version,c->lockedto);
	}
#ifdef MFSDEBUG
	debug = ((chunkid&0xFFF)==0)?1:0;
	if (debug) {
		mfsdebug("chunk_server_has_chunk ; chunkid=%016"PRIX64" new copy: mfrstatus=%u ; csid=%"PRIu16" ; ip=%s",chunkid,(version&0x80000000)?1:0,csid,matocsserv_getstrip(cstab[csid].ptr));
		for (s=c->slisthead ; s ; s=s->next) {
			mfsdebug("chunk_server_has_chunk ; chunkid=%016"PRIX64" ; existing copy: mfrstatus=%u ; valid=%s ; csid=%"PRIu16" ; ip=%s",chunkid,(s->valid==TDVALID || s->valid==TDBUSY || s->valid==TDWVER)?1:0,validstr[s->valid],s->csid,matocsserv_getstrip(cstab[s->csid].ptr));
		}
	}
#endif
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->csid==csid) {
			uint8_t nextallvalidcopies = c->allvalidcopies;
			uint8_t nextregularvalidcopies = c->regularvalidcopies;
			uint8_t nextvalid;
			if (s->valid==INVALID || s->valid==DEL || s->valid==WVER || s->valid==TDWVER) { // ignore such copy
				return;
			}
			// in case of other copies just check 'mark for removal' status
			if (s->valid==BUSY || s->valid==TDBUSY) {
				if (version&0x80000000) {
					nextvalid = TDBUSY;
				} else {
					nextvalid = BUSY;
				}
			} else {
				if (version&0x80000000) {
					nextvalid = TDVALID;
				} else {
					nextvalid = VALID;
				}
			}
			if (s->valid==nextvalid) {
				return;
			}
			if (s->valid==VALID || s->valid==BUSY) {
				nextallvalidcopies--;
				nextregularvalidcopies--;
			} else if (s->valid==TDVALID || s->valid==TDBUSY) {
				nextallvalidcopies--;
			}
			if (nextvalid==VALID || nextvalid==BUSY) {
				nextallvalidcopies++;
				nextregularvalidcopies++;
			} else if (nextvalid==TDVALID || nextvalid==TDBUSY) {
				nextallvalidcopies++;
			}
			s->valid = nextvalid;
			if (nextallvalidcopies!=c->allvalidcopies || nextregularvalidcopies!=c->regularvalidcopies) {
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,nextallvalidcopies,c->regularvalidcopies,nextregularvalidcopies);
				c->allvalidcopies = nextallvalidcopies;
				c->regularvalidcopies = nextregularvalidcopies;
				if (version&0x80000000) {
					chunk_mfr_state_check(c);
				}
			}
			return;
		}
	}
	s = slist_malloc();
	s->csid = csid;
	if (c->version!=(version&0x7FFFFFFF)) {
		if (version&0x80000000) {
			s->valid = TDWVER;
		} else {
			s->valid = WVER;
		}
		s->version = version&0x7FFFFFFF;
	} else {
		if (c->writeinprogress) {
			matocsserv_write_counters(cstab[csid].ptr,1);
		}
		if (version&0x80000000) {
			s->valid = TDVALID;
			s->version = c->version;
			chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies);
			c->allvalidcopies++;
		} else {
			s->valid = VALID;
			s->version = c->version;
			chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
			c->allvalidcopies++;
			c->regularvalidcopies++;
		}
	}
	s->next = c->slisthead;
	c->slisthead = s;
	c->needverincrease = 1;
	if (version&0x80000000) {
		chunk_mfr_state_check(c);
	}
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
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
				c->allvalidcopies--;
			}
			if (s->valid==BUSY || s->valid==VALID) {
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
				c->allvalidcopies--;
				c->regularvalidcopies--;
			}
			if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
				matocsserv_write_counters(cstab[csid].ptr,0);
			}
			s->valid = INVALID;
			s->version = 0;
			c->needverincrease = 1;
			chunk_priority_queue_check(c,1);
			chunk_mfr_state_check(c);
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

	c = chunk_find(chunkid);
	if (c==NULL) {
		return;
	}
	sptr=&(c->slisthead);
	while ((s=*sptr)) {
		if (s->csid==csid) {
			if (s->valid==TDBUSY || s->valid==TDVALID) {
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
				c->allvalidcopies--;
			}
			if (s->valid==BUSY || s->valid==VALID) {
				chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
				c->allvalidcopies--;
				c->regularvalidcopies--;
			}
			if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
				matocsserv_write_counters(cstab[csid].ptr,0);
			}
			c->needverincrease = 1;
			*sptr = s->next;
			slist_free(s);

			cstab[csid].lostchunkdelay = LOSTCHUNKDELAY;
			csreceivingchunks |= 1;

		} else {
			sptr = &(s->next);
		}
	}
	if (c->lockedto<(uint32_t)main_time() && c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		chunk_delete(c);
	} else {
		chunk_priority_queue_check(c,1);
		chunk_mfr_state_check(c);
	}
}

uint8_t chunk_get_mfrstatus(uint16_t csid) {
	if (csid<MAXCSCOUNT) {
		switch (cstab[csid].mfr_state) {
			case UNKNOWN_HARD:
			case UNKNOWN_SOFT:
				return 0;
			case CAN_BE_REMOVED:
				return 2;
			case REPL_IN_PROGRESS:
			case WAS_IN_PROGRESS:
				return 1;

		}
	}
	return 0;
}

static inline void chunk_server_remove_csid(uint16_t csid) {
	// remove from used list
	if (cstab[csid].prev<MAXCSCOUNT) {
		cstab[cstab[csid].prev].next = cstab[csid].next;
	} else {
		csusedhead = cstab[csid].next;
	}
	if (cstab[csid].next<MAXCSCOUNT) {
		cstab[cstab[csid].next].prev = cstab[csid].prev;
	}
	// append to free list
	cstab[csid].next = MAXCSCOUNT;
	cstab[csid].prev = csfreetail;
	cstab[csfreetail].next = csid;
	csfreetail = csid;

//	cstab[csid].next = csfreehead;
//	cstab[csid].prev = MAXCSCOUNT;
//	cstab[csfreehead].prev = csid;
//	csfreehead = csid;
}

static inline uint16_t chunk_server_new_csid(void) {
	uint16_t csid;

	// take first element from free list
	csid = csfreehead;
	csfreehead = cstab[csid].next;
	cstab[csfreehead].prev = MAXCSCOUNT;
	// add to used list
	if (csusedhead<MAXCSCOUNT) {
		cstab[csusedhead].prev = csid;
	}
	cstab[csid].next = csusedhead;
//	cstab[csid].prev = MAXCSCOUNT; // not necessary - it was first element in free list
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
	cstab[csid].mfr_state = UNKNOWN_HARD;
	csregisterinprogress += 1;
	return csid;
}

void chunk_server_register_end(uint16_t csid) {
	if (cstab[csid].registered==0 && cstab[csid].valid==1) {
		cstab[csid].registered = 1;
		csregisterinprogress -= 1;
	}
	if (csregisterinprogress==0) {
		matoclserv_fuse_invalidate_chunk_cache();
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
			chunk_remove_disconnected_chunks(c);
		}
		csop = csop->next;
		if (opsinprogress>0) {
			opsinprogress--;
		}
	}
	cstab[csid].opchunks = NULL;

	for (csid = csusedhead ; csid < MAXCSCOUNT ; csid = cstab[csid].next) {
		cstab[csid].mfr_state = UNKNOWN_HARD;
	}
	matoclserv_fuse_invalidate_chunk_cache();
}

void chunk_server_disconnection_loop(void) {
	uint32_t i;
	chunk *c,*cn;
	discserv *ds;
	uint64_t startutime,currutime;
	static uint32_t discserverspos = 0;

	if (discservers) {
		startutime = monotonic_useconds();
		currutime = startutime;
		while (startutime+10000>currutime) {
			for (i=0 ; i<100 ; i++) {
				if (discserverspos<chunkrehashpos) {
					for (c=chunkhashtab[discserverspos>>HASHTAB_LOBITS][discserverspos&HASHTAB_MASK] ; c ; c=cn ) {
						cn = c->next;
						chunk_remove_disconnected_chunks(c);
					}
					discserverspos++;
				} else {
					while (discservers) {
						ds = discservers;
						discservers = ds->next;
						chunk_server_remove_csid(ds->csid);
						matocsserv_disconnection_finished(cstab[ds->csid].ptr);
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

	if (status==MFS_STATUS_OK || status==MFS_ERROR_NOCHUNK) {
		stats_chunkops[CHUNK_OP_DELETE_OK]++;
	} else {
		stats_chunkops[CHUNK_OP_DELETE_ERR]++;
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	if (status!=MFS_STATUS_OK && status!=MFS_ERROR_NOCHUNK) { // treat here MFS_ERROR_NOCHUNK as ok
		return ;
	}
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (s->csid == csid) {
			if (s->valid!=DEL) {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
					c->allvalidcopies--;
				}
				if (s->valid==BUSY || s->valid==VALID) {
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
				}
				if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
					matocsserv_write_counters(cstab[s->csid].ptr,0);
				}
				syslog(LOG_WARNING,"got unexpected delete status");
			}
			*st = s->next;
			slist_free(s);
		} else {
			st = &(s->next);
		}
	}
	if (c->lockedto<(uint32_t)main_time() && c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		chunk_delete(c);
	}
}

void chunk_got_replicate_status(uint16_t csid,uint64_t chunkid,uint32_t version,uint8_t status) {
	chunk *c;
	slist *s,**st;
	uint8_t fix;

	if (status==MFS_STATUS_OK) {
		stats_chunkops[CHUNK_OP_REPLICATE_OK]++;
	} else {
		stats_chunkops[CHUNK_OP_REPLICATE_ERR]++;
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
	if (c->operation==REPLICATE) { // high priority replication
		fix = 0;
		if (status!=0) { // chunk hasn't been replicated (error occured) - simply remove it from copies
			st = &(c->slisthead);
			while (*st) {
				s = *st;
				if (s->csid==csid && s->valid==BUSY) {
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
					if (c->writeinprogress) {
						matocsserv_write_counters(cstab[s->csid].ptr,0);
					}
					fix = 1;
					*st = s->next;
					chunk_delopchunk(s->csid,c->chunkid);
					slist_free(s);
				} else {
					st = &(s->next);
				}
			}
		}
		if (fix==0) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->csid == csid) {
					if (s->valid!=BUSY) {
						syslog(LOG_WARNING,"got replication status from server not set as busy !!!");
					}
					if (status!=0 || version!=c->version) {
						if (s->valid==TDBUSY || s->valid==TDVALID) {
							chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
							c->allvalidcopies--;
						}
						if (s->valid==BUSY || s->valid==VALID) {
							chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
							c->allvalidcopies--;
							c->regularvalidcopies--;
						}
						if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
							matocsserv_write_counters(cstab[s->csid].ptr,0);
						}
						s->valid = INVALID;
						s->version = 0;	// after unfinished operation can't be shure what version chunk has
					} else {
						if (s->valid == BUSY || s->valid == VALID) {
							s->valid = VALID;
						}
					}
					chunk_delopchunk(s->csid,c->chunkid);
				} else if (s->valid==BUSY) {
					syslog(LOG_WARNING,"got replication status from one server, but another is set as busy !!!");
				}
			}
		}
		c->operation = NONE;
		c->lockedto = 0;
		matoclserv_chunk_unlocked(c->chunkid,c);
	} else { // low priority replication
		if (status!=0) {
			chunk_priority_queue_check(c,1);
			return ;
		}
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->csid == csid) {
				syslog(LOG_WARNING,"got replication status from server which had had that chunk before (chunk:%016"PRIX64"_%08"PRIX32")",chunkid,version);
				if (s->valid==VALID && version!=c->version) {
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
					s->valid = INVALID;
					s->version = version;
					if (c->writeinprogress) {
						matocsserv_write_counters(cstab[s->csid].ptr,0);
					}
				}
				chunk_priority_queue_check(c,1);
				return;
			}
		}
		s = slist_malloc();
		s->csid = csid;
		if (c->lockedto>=(uint32_t)main_time() || version!=c->version) {
			s->valid = INVALID;
		} else {
			chunk_write_counters(c,0);
			chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
			c->allvalidcopies++;
			c->regularvalidcopies++;
			s->valid = VALID;
		}
		s->version = version;
		s->next = c->slisthead;
		c->slisthead = s;
	}
	chunk_priority_queue_check(c,1);
}


void chunk_operation_status(uint64_t chunkid,uint8_t status,uint16_t csid,uint8_t operation) {
	chunk *c;
	uint8_t opfinished,validcopies;
	uint8_t verfixed;
	slist *s,**st;

	if (status==MFS_STATUS_OK) {
		if (operation==CREATE) {
			stats_chunkops[CHUNK_OP_CREATE_OK]++;
		} else {
			stats_chunkops[CHUNK_OP_CHANGE_OK]++;
		}
	} else {
		if (operation==CREATE) {
			stats_chunkops[CHUNK_OP_CREATE_ERR]++;
		} else {
			stats_chunkops[CHUNK_OP_CHANGE_ERR]++;
		}
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}

	if (chunk_remove_disconnected_chunks(c)) {
		return;
	}
//	for (s=c->slisthead ; s ; s=s->next) {
//		if (!cstab[s->csid].valid) {
//			c->interrupted = 1;
//		}
//	}

	if (c->operation!=operation && operation!=NONE) {
		uint8_t eop,sop;
		eop = c->operation;
		sop = operation;
		if (eop>7) {
			eop=7;
		}
		if (sop>7) {
			sop=7;
		}
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" - got unexpected status (expected: %s ; got: %s)",chunkid,c->version,opstr[eop],opstr[sop]);
	}

	validcopies = 0;
	opfinished = 1;

	st = &(c->slisthead);
	while ((s=*st)!=NULL) {
		if (s->csid == csid) {
			if (status!=0) {
				c->interrupted = 1;	// increase version after finish, just in case
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
					c->allvalidcopies--;
				}
				if (s->valid==BUSY || s->valid==VALID) {
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
					c->allvalidcopies--;
					c->regularvalidcopies--;
				}
				if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
					 matocsserv_write_counters(cstab[s->csid].ptr,0);
				}
				if (status==MFS_ERROR_NOTDONE) { // special case - this operation was not even started, so we know exact result
					if (c->operation==SET_VERSION || c->operation==TRUNCATE) { // chunk left not changed, but now it has wrong version
						if (s->valid==TDBUSY || s->valid==TDVALID) {
							s->valid = TDWVER;
						} else {
							s->valid = WVER;
						}
						s->version--;
					} else if (c->operation==CREATE || c->operation==DUPLICATE || c->operation==DUPTRUNC) { // copy not created
						*st = s->next;
						slist_free(s);
						continue;
					}
				} else {
					s->valid = INVALID;
					s->version = 0;	// after unfinished operation can't be shure what version chunk has
				}
			} else {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					s->valid=TDVALID;
				} else {
					s->valid=VALID;
				}
			}
			chunk_statusopchunk(s->csid,c->chunkid,status);
//			chunk_delopchunk(s->csid,c->chunkid);
		}
		if (s->valid==BUSY || s->valid==TDBUSY) {
			opfinished=0;
		}
		if (s->valid==VALID || s->valid==TDVALID) {
			validcopies=1;
		}
		st = &(s->next);
	}
	if (opfinished && validcopies==0 && (c->operation==SET_VERSION || c->operation==TRUNCATE)) { // we know that version increase was just not completed, so all WVER chunks with version exactly one lower than chunk version are actually VALID copies
		verfixed = 0;
		for (s=c->slisthead ; s!=NULL ; s=s->next) {
			if (s->version+1==c->version) {
				if (s->valid==TDWVER) {
					verfixed = 1;
					s->valid = TDVALID;
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies);
					c->allvalidcopies++;
				} else if (s->valid==WVER) {
					verfixed = 1;
					s->valid = VALID;
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
					c->allvalidcopies++;
					c->regularvalidcopies++;
				}
			}
			if (verfixed) {
				c->version--;
				changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
			}
		}
		// we continue because we still want to return status not done to matoclserv module
	}
	if (opfinished) {
		uint8_t nospace;
		nospace = 1;
		for (s=c->slisthead ; s ; s=s->next) {
			status = chunk_delopchunk(s->csid,chunkid);
			if (status!=MFS_ERROR_MISMATCH && status!=MFS_ERROR_NOSPACE) {
				nospace = 0;
			}
		}
		if (validcopies) {
//			syslog(LOG_NOTICE,"operation finished, chunk: %016"PRIX64" ; op: %s ; interrupted: %u",c->chunkid,opstr[c->operation],c->interrupted);
			if (c->interrupted) {
				chunk_emergency_increase_version(c);
			} else {
				c->operation = NONE;
				c->needverincrease = 0;
				matoclserv_chunk_status(c->chunkid,MFS_STATUS_OK);
				if (c->lockedto==0) {
					matoclserv_chunk_unlocked(c->chunkid,c);
				}
			}
		} else {
			if (nospace) {
				matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOSPACE);
			} else {
				matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOTDONE);
			}
			c->operation = NONE;
		}
	}
}

void chunk_got_chunkop_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk_operation_status(chunkid,status,csid,NONE);
}

void chunk_got_create_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk_operation_status(chunkid,status,csid,CREATE);
}

void chunk_got_duplicate_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk_operation_status(chunkid,status,csid,DUPLICATE);
}

void chunk_got_setversion_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk_operation_status(chunkid,status,csid,SET_VERSION);
}

void chunk_got_truncate_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk_operation_status(chunkid,status,csid,TRUNCATE);
}

void chunk_got_duptrunc_status(uint16_t csid,uint64_t chunkid,uint8_t status) {
	chunk_operation_status(chunkid,status,csid,DUPTRUNC);
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
	put32bit(&buff,chunksinfo.done.copy_wronglabels);
	put32bit(&buff,chunksinfo.notdone.copy_wronglabels);
	put32bit(&buff,chunksinfo.copy_rebalance);
	put32bit(&buff,chunksinfo.labels_dont_match);
	put32bit(&buff,chunksinfo.locked_unused);
	put32bit(&buff,chunksinfo.locked_used);
}

static inline uint8_t chunk_mindist(uint16_t csid,uint32_t ip[255],uint8_t ipcnt) {
	uint8_t mindist,dist,k;
	uint32_t sip;
	mindist = TOPOLOGY_DIST_MAX;
	if (matocsserv_get_csdata(cstab[csid].ptr,&sip,NULL,NULL,NULL)==0) {
		for (k=0 ; k<ipcnt && mindist>0 ; k++) {
			dist=topology_distance(sip,ip[k]);
			if (dist<mindist) {
				mindist = dist;
			}
		}
	}
	return mindist;
}

// first servers in the same rack (server id), then other (yes, same rack is better than same physical server, so order is 1 and then 0 or 2)
static inline void chunk_rack_sort(uint16_t servers[MAXCSCOUNT],uint16_t servcount,uint32_t ip[255],uint8_t ipcnt) {
	uint16_t i,j;
	uint16_t csid;
	uint8_t mindist;

	if (servcount==0 || ipcnt==0) {
		return;
	}
	i = 0;
	j = servcount-1;
	while (i<j) {
		while (i<j) {
			mindist = chunk_mindist(servers[i],ip,ipcnt);
			if (mindist!=TOPOLOGY_DIST_SAME_RACKID) {
				break;
			}
			i++;
		}
		while (i<j) {
			mindist = chunk_mindist(servers[j],ip,ipcnt);
			if (mindist==TOPOLOGY_DIST_SAME_RACKID) {
				break;
			}
			j--;
		}
		if (i<j) {
			csid = servers[i];
			servers[i] = servers[j];
			servers[j] = csid;
		}
	}
}

static inline uint16_t chunk_get_undergoal_replicate_srccsid(chunk *c,uint16_t dstcsid,uint32_t now,uint32_t lclass,uint32_t rgvc,uint32_t rgtdc) {
	slist *s;
	uint32_t r = 0;
	uint16_t srccsid = MAXCSCOUNT;

	if (ReplicationsRespectTopology) {
		uint32_t min_dist = 0xFFFFFFFF;
		uint32_t dist;
		uint32_t ip;
		uint32_t cuip;
		uint32_t mdrgvc = 0;
		uint32_t mdrgtdc = 0;

		if (matocsserv_get_csdata(cstab[dstcsid].ptr,&cuip,NULL,NULL,NULL)==0) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass] && (s->valid==VALID || s->valid==TDVALID)) {
					if (matocsserv_get_csdata(cstab[s->csid].ptr,&ip,NULL,NULL,NULL)==0) {
						dist=topology_distance(ip,cuip);
						if (min_dist>=dist) {
							if (min_dist>dist) {
								min_dist=dist;
								srccsid=s->csid;
								mdrgvc = 0;
								mdrgtdc = 0;
							} else if (s->valid==VALID) {
								srccsid=s->csid;
							}
							if (s->valid==VALID) {
								mdrgvc++;
							} else {
								mdrgtdc++;
							}
						}
					}
				}
			}
			if (mdrgvc > 1) {
				r = 1+rndu32_ranged(mdrgvc);
			} else if (mdrgvc == 0 && mdrgtdc > 1) {  // we have to choose TDVALID chunks
				r = 1+rndu32_ranged(mdrgtdc);
			}
			for (s=c->slisthead ; s && r>0 ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass] && (s->valid==VALID || s->valid==TDVALID)) {
					if (matocsserv_get_csdata(cstab[s->csid].ptr,&ip,NULL,NULL,NULL)==0) {
						dist=topology_distance(ip,cuip);
						if (min_dist==dist) {
							if (mdrgvc > 1 && s->valid==VALID) {
								r--;
								srccsid=s->csid;
							} else if (mdrgtdc > 1 && s->valid==TDVALID) {
								r--;
								srccsid=s->csid;
							}
						}
					}
				}
			}
		}
	} else {
		if (rgvc>0) {	// if there are VALID copies then make copy of one VALID chunk
			r = 1+rndu32_ranged(rgvc);
			for (s=c->slisthead ; s && r>0 ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass] && s->valid==VALID) {
					r--;
					srccsid = s->csid;
				}
			}
		} else {	// if not then use TDVALID chunks.
			r = 1+rndu32_ranged(rgtdc);
			for (s=c->slisthead ; s && r>0 ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass] && s->valid==TDVALID) {
					r--;
					srccsid = s->csid;
				}
			}
		}
	}

	return srccsid;
}

static inline int chunk_undergoal_replicate(chunk *c,uint16_t dstcsid,uint32_t now,uint32_t lclass,uint16_t priority,loop_info *inforec,uint32_t rgvc,uint32_t rgtdc) {
	slist *s;
	uint16_t srccsid;

	srccsid = chunk_get_undergoal_replicate_srccsid(c,dstcsid,now,lclass,rgvc,rgtdc);
	if (srccsid==MAXCSCOUNT) {
		return -1;
	}
	stats_chunkops[CHUNK_OP_REPLICATE_TRY]++;
	// high priority replication
	if (matocsserv_send_replicatechunk(cstab[dstcsid].ptr,c->chunkid,c->version,cstab[srccsid].ptr)<0) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": error sending replicate command",c->chunkid,c->version);
		return 1;
	}
	chunk_addopchunk(dstcsid,c->chunkid);
	c->operation = REPLICATE;
	c->lockedto = now+LOCKTIMEOUT;
	s = slist_malloc();
	s->csid = dstcsid;
	s->valid = BUSY;
	s->version = c->version;
	s->next = c->slisthead;
	c->slisthead = s;
	chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies+1,c->regularvalidcopies,c->regularvalidcopies+1);
	c->allvalidcopies++;
	c->regularvalidcopies++;
	if (priority<5) {
		inforec->done.copy_undergoal++;
	} else {
		inforec->done.copy_wronglabels++;
	}
	return 0;
}

void chunk_do_jobs(chunk *c,uint16_t scount,uint16_t fullservers,uint32_t now,uint8_t extrajob) {
	slist *s;
	static uint16_t *dcsids = NULL;
	static uint16_t dservcount;
//	static uint16_t *bcsids;
//	static uint16_t bservcount;
	static uint16_t *rcsids = NULL;
	uint16_t servmaxpos[4];
	uint16_t rservcount;
	uint16_t srccsid,dstcsid;
	double repl_read_counter;
	uint16_t i,j,k;
	uint16_t prilevel;
	uint32_t vc,tdc,ivc,bc,tdb,dc,wvc,tdw;
	uint32_t goal;
	static loop_info inforec;
	static uint32_t delnotdone;
	static uint32_t deldone;
	static uint32_t prevtodeletecount;
	static uint32_t delloopcnt;
	uint32_t **labelmasks;
	uint32_t labelcnt;
	static uint16_t *servers = NULL;
	uint32_t servcnt,extraservcnt;
	int32_t *matching;
	uint32_t forcereplication;
	uint32_t vrip[255];
	uint8_t vripcnt;
#ifdef MFSDEBUG
	uint8_t debug;
#endif

	if (servers==NULL) {
		servers = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(servers);
	}
	if (rcsids==NULL) {
		rcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(rcsids);
	}
	if (dcsids==NULL) {
		dcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(dcsids);
	}
//	if (bcsids==NULL) {
//		bcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
//		passert(bcsids);
//	}

	if (c==NULL) {
		if (scount==JOBS_INIT) { // init tasks
			delnotdone = 0;
			deldone = 0;
			prevtodeletecount = 0;
			delloopcnt = 0;
			memset(&inforec,0,sizeof(loop_info));
			dservcount = 0;
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
			dservcount = 0;
//			bservcount=0;
		} else if (scount==JOBS_TERM) {
			if (servers!=NULL) {
				free(servers);
			}
			if (rcsids!=NULL) {
				free(rcsids);
			}
			if (dcsids!=NULL) {
				free(dcsids);
			}
		}
		return;
	}

// step 0. remove all disconnected copies from structures
	if (chunk_remove_disconnected_chunks(c)) {
		return;
	}

	if (c->lockedto < now) {
		chunk_write_counters(c,0);
	}

// step 1. calculate number of valid and invalid copies
	vc = 0;
	tdc = 0;
	ivc = 0;
	bc = 0;
	tdb = 0;
	dc = 0;
	wvc = 0;
	tdw = 0;
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
		case WVER:
			wvc++;
			break;
		case TDWVER:
			tdw++;
			break;
		}
	}
#ifdef MFSDEBUG
	debug = ((c->chunkid&0xFFF)==0)?1:0;
	if (debug) {
		mfsdebug("chunk_do_jobs ; chunkid=%016"PRIX64" ; vc=%u ; tdc=%u ; bc=%u ; tdb=%u ; wvc=%u ; tdw=%u ; ivc=%u ; dc=%u",c->chunkid,vc,tdc,bc,tdb,wvc,tdw,ivc,dc);
		for (s=c->slisthead ; s ; s=s->next) {
			mfsdebug("chunk_do_jobs ; chunkid=%016"PRIX64" ; existing copy: valid=%s ; csid=%"PRIu16" ; ip=%s",c->chunkid,validstr[s->valid],s->csid,matocsserv_getstrip(cstab[s->csid].ptr));
		}
	}
#endif
	if (c->allvalidcopies!=vc+tdc+bc+tdb) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": wrong all valid copies counter - (counter value: %u, should be: %u) - fixed",c->chunkid,c->version,c->allvalidcopies,vc+tdc+bc+tdb);
		chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,vc+tdc+bc+tdb,c->regularvalidcopies,c->regularvalidcopies);
		c->allvalidcopies = vc+tdc+bc+tdb;
	}
	if (c->regularvalidcopies!=vc+bc) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": wrong regular valid copies counter - (counter value: %u, should be: %u) - fixed",c->chunkid,c->version,c->regularvalidcopies,vc+bc);
		chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies,c->regularvalidcopies,vc+bc);
		c->regularvalidcopies = vc+bc;
	}
	if (tdb+bc==0 && c->operation!=NONE) {
		syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk in middle of operation %s, but no chunk server is busy - finish operation",c->chunkid,c->version,opstr[c->operation]);
		c->operation = NONE;
	}
	goal = sclass_get_keeparch_goal(c->sclassid,c->archflag);
	if (vc + bc < goal && tdc + tdb > 0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid == TDVALID || s->valid == TDBUSY) {
				cstab[s->csid].mfr_state = REPL_IN_PROGRESS;
			}
		}
	}
	if (c->lockedto < now) {
		if (tdb+bc>0 && c->operation==NONE) {
			if (tdc+vc>0) {
				syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": unexpected BUSY copies - fixing",c->chunkid,c->version);
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->valid == BUSY) {
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
						c->allvalidcopies--;
						c->regularvalidcopies--;
						s->valid = INVALID;
						s->version = 0;
						ivc++;
						bc--;
					} else if (s->valid == TDBUSY) {
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies--;
						s->valid = INVALID;
						s->version = 0;
						ivc++;
						tdb--;
					}
				}
			} else {
				syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": unexpected BUSY copies - can't fix",c->chunkid,c->version);
			}
		}

/*
#warning debug
	{
		static FILE *fd = NULL;
		static uint32_t cnt = 0;
		if (fd==NULL) {
			fd = fopen("chunklog.txt","a");
		}
		if (fd!=NULL) {
			fprintf(fd,"chunk %016"PRIX64": ivc=%"PRIu32" , dc=%"PRIu32" , vc=%"PRIu32" , bc=%"PRIu32" , wvc=%"PRIu32" , tdv=%"PRIu32" , tdb=%"PRIu32" , tdw=%"PRIu32" , goal=%"PRIu8" , scount=%"PRIu16"\n",c->chunkid,ivc,dc,vc,bc,wvc,tdc,tdb,tdw,c->goal,scount);
			cnt++;
		}
		if (cnt>100000) {
			fclose(fd);
			fd = NULL;
			cnt = 0;
		}
	}
*/
//	syslog(LOG_WARNING,"chunk %016"PRIX64": ivc=%"PRIu32" , dc=%"PRIu32" , vc=%"PRIu32" , bc=%"PRIu32" , wvc=%"PRIu32" , tdv=%"PRIu32" , tdb=%"PRIu32" , tdw=%"PRIu32" , goal=%"PRIu8" , scount=%"PRIu16,c->chunkid,ivc,dc,vc,bc,wvc,tdc,tdb,tdw,c->goal,scount);

// step 2. check number of copies
		if (tdc+vc+tdb+bc==0 && wvc+tdw>0 && c->fcount>0/* c->flisthead */) {
			if ((tdw+wvc)>=goal) {
				uint32_t bestversion;
				bestversion = 0;
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->valid==WVER || s->valid==TDWVER) {
						if (s->version>=bestversion) {
							bestversion = s->version;
						}
					}
				}
				if (bestversion>0 && ((bestversion+1)==c->version || c->version+1==bestversion)) {
					syslog(LOG_WARNING,"chunk %016"PRIX64" has only invalid copies (%"PRIu32") - fixing it",c->chunkid,wvc+tdw);
					c->version = bestversion;
					for (s=c->slisthead ; s ; s=s->next) {
						if (s->version==bestversion && cstab[s->csid].valid) {
							if (s->valid == WVER) {
								s->valid = VALID;
								c->allvalidcopies++;
								c->regularvalidcopies++;
							} else if (s->valid == TDWVER) {
								s->valid = TDVALID;
								c->allvalidcopies++;
							}
						}
						if (s->valid == WVER || s->valid == TDWVER) {
							syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - wrong versioned copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
						} else {
							syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - valid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
						}
					}
					chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,0,c->allvalidcopies,0,c->regularvalidcopies);
					c->needverincrease = 1;
					return;
				}
			}
		}
		if (tdc+vc+tdb+bc==0 && wvc+tdw+ivc>0 && c->fcount>0) {
			if (wvc+tdw==0) {
				syslog(LOG_WARNING,"chunk %016"PRIX64" has only invalid copies (%"PRIu32") - please repair it manually",c->chunkid,ivc);
			} else {
				syslog(LOG_WARNING,"chunk %016"PRIX64" has only copies with wrong versions (%"PRIu32") - please repair it manually",c->chunkid,wvc+tdw);
			}
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==INVALID) {
					syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - invalid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
				} else {
					syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32" - copy with wrong version on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
				}
			}
			return;
		}

		if (tdc+vc+tdb+bc+ivc+dc+wvc+tdw==0 && c->fcount>0) {
			syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": there are no copies",c->chunkid,c->version);
			return;
		}
	}

// step 3.0. delete invalid copies
	if (extrajob==0 && (tdc+vc+tdb+bc>0 || (c->fcount==0 && c->lockedto<now))) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
				if (s->valid==WVER || s->valid==TDWVER || s->valid==INVALID || s->valid==DEL) {
					if (s->valid==DEL) {
						syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk hasn't been deleted since previous loop - retry",c->chunkid,c->version);
					}
					switch (s->valid) {
						case WVER:
							wvc--;
							break;
						case TDWVER:
							tdw--;
							break;
						case INVALID:
							ivc--;
							break;
						case DEL:
							dc--;
							break;
					}
					s->valid = DEL;
					dc++;
					stats_chunkops[CHUNK_OP_DELETE_TRY]++;
					matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,0);
					inforec.done.del_invalid++;
					deldone++;
				}
			} else {
				if (s->valid==WVER || s->valid==TDWVER || s->valid==INVALID) {
					inforec.notdone.del_invalid++;
					delnotdone++;
				}
			}
		}
	}

// step 3.1. check for unfinished replications
	if (extrajob == 0) {
		if (c->operation==REPLICATE && c->lockedto<now) {
			syslog(LOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk hasn't been replicated since previous loop - retry",c->chunkid,c->version);
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==TDBUSY || s->valid==BUSY) {
					if (s->valid==TDBUSY) {
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies--;
						tdb--;
					}
					if (s->valid==BUSY) {
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
						c->allvalidcopies--;
						c->regularvalidcopies--;
						bc--;
					}
					s->valid = INVALID;
					s->version = 0;	// after unfinished operation can't be shure what version chunk has
					chunk_delopchunk(s->csid,c->chunkid);
					ivc++;
				}
			}
			c->operation = NONE;
			c->lockedto = 0;
			matoclserv_chunk_unlocked(c->chunkid,c);
		}
	}

// step 4. return if chunk is during some operation
	if (c->operation!=NONE || (c->lockedto>=now)) {
		if (extrajob == 0) {
			if (c->fcount==0) {
				inforec.locked_unused++;
			} else {
				inforec.locked_used++;
				if (goal > vc+bc && vc+tdc+bc+tdb > 0) {
					if (c->operation!=NONE) {
						syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": can't replicate chunk - operation %s in progress",c->chunkid,c->version,opstr[c->operation]);
					} else {
						syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": can't replicate chunk - chunk is being modified (locked for next %"PRIu32" second%s)",c->chunkid,c->version,(uint32_t)(1+c->lockedto-now),(c->lockedto==now)?"":"s");
					}
				}
			}
		}
		return ;
	}

//	assert(c->lockedto < now && c->writeinprogress == 0)

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
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies--;
					} else {
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
						c->allvalidcopies--;
						c->regularvalidcopies--;
					}
					c->needverincrease = 1;
					s->valid = DEL;
					stats_chunkops[CHUNK_OP_DELETE_TRY]++;
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

// step 7.0. if chunk has enough valid copies and more than one copy with wrong version then delete all copies with wrong version
	if (extrajob==0 && vc >= goal && wvc>0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid == WVER) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					s->valid = DEL;
					stats_chunkops[CHUNK_OP_DELETE_TRY]++;
					matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,0);
					wvc--;
					dc++;
				}
			}
		}
	}

// step 7.1. if chunk has too many copies then delete some of them
	if (vc > goal) {
		uint8_t prevdone;
		if (dservcount==0) {
			// dservcount = matocsserv_getservers_ordered(dcsids,AcceptableDifference/2.0,NULL,NULL);
			dservcount = matocsserv_getservers_ordered(dcsids);
		}
	//	syslog(LOG_WARNING,"vc (%"PRIu32") > goal (%"PRIu32") - delete",vc,sclass_getgoal(c->sclassid));
		inforec.notdone.del_overgoal+=(vc-goal);
		delnotdone+=(vc-goal);
		prevdone = 1;

		if ((DoNotUseSameIP | DoNotUseSameRack | LabelUniqueMask) || sclass_has_keeparch_labels(c->sclassid,c->archflag)) { // labels version
			servcnt = 0;
			for (i=0 ; i<dservcount ; i++) {
				for (s=c->slisthead ; s && s->csid!=dcsids[dservcount-1-i] ; s=s->next) {}
				if (s && s->valid==VALID) {
					servers[servcnt++] = s->csid;
				}
			}
			labelcnt = sclass_get_keeparch_labelmasks(c->sclassid,c->archflag,&labelmasks);
			matching = do_perfect_match(labelcnt,servcnt,labelmasks,servers);
			for (i=0 ; i<servcnt && vc>goal && prevdone ; i++) {
				if (matching[i+labelcnt]<0) {
					for (s=c->slisthead ; s && s->csid!=servers[i] ; s=s->next) {}
					if (s && s->valid==VALID) {
						if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
							chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
							c->allvalidcopies--;
							c->regularvalidcopies--;
							c->needverincrease = 1;
							s->valid = DEL;
							stats_chunkops[CHUNK_OP_DELETE_TRY]++;
							matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,0);
							inforec.done.del_overgoal++;
							inforec.notdone.del_overgoal--;
							deldone++;
							delnotdone--;
							vc--;
							dc++;
						} else {
							prevdone=0;
							chunk_priority_enqueue(DPRIORITY_OVERGOAL,c); // in such case only enqueue this chunk for future processing
						}
					}
				}
			}
		} else { // classic goal version
			for (i=0 ; i<dservcount && vc>goal && prevdone; i++) {
				for (s=c->slisthead ; s && s->csid!=dcsids[dservcount-1-i] ; s=s->next) {}
				if (s && s->valid==VALID) {
					if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies-1);
						c->allvalidcopies--;
						c->regularvalidcopies--;
						c->needverincrease = 1;
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
						matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,0);
						inforec.done.del_overgoal++;
						inforec.notdone.del_overgoal--;
						deldone++;
						delnotdone--;
						vc--;
						dc++;
					} else {
						prevdone=0;
						chunk_priority_enqueue(DPRIORITY_OVERGOAL,c); // in such case only enqueue this chunk for future processing
					}
				}
			}
		}
		return;
	}

// step 7.2. if chunk has one copy on each server and some of them have status TDVALID then delete them
	if (extrajob==0 && vc+tdc>=scount && vc<goal && tdc>0 && vc+tdc>1 && chunks_priority_leng[DPRIORITY_ENDANGERED_HIGHGOAL]==0 && chunks_priority_leng[DPRIORITY_ENDANGERED]==0 && chunks_priority_leng[DPRIORITY_UNDERGOAL_MFR]==0) {
		uint8_t tdcr = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==TDVALID) {
				if (matocsserv_has_avail_space(cstab[s->csid].ptr)) {
					tdcr++;
				}
			}
		}
		if (vc+tdcr>=scount) {
			uint8_t prevdone;
	//		syslog(LOG_WARNING,"vc+tdc (%"PRIu32") >= scount (%"PRIu32") and vc (%"PRIu32") < goal (%"PRIu32") and tdc (%"PRIu32") > 0 and vc+tdc > 1 - delete",vc+tdc,scount,vc,goal,tdc);
			prevdone = 0;
			for (s=c->slisthead ; s && prevdone==0 ; s=s->next) {
				if (s->valid==TDVALID) {
					if (matocsserv_has_avail_space(cstab[s->csid].ptr) && matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
						chunk_state_change(c->sclassid,c->sclassid,c->archflag,c->archflag,c->allvalidcopies,c->allvalidcopies-1,c->regularvalidcopies,c->regularvalidcopies);
						c->allvalidcopies--;
						c->needverincrease = 1;
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
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
			if (prevdone) {
				return;
			}
		}
	}

// step 8. check matching for labeled chunks
	forcereplication = 0;
	if ((DoNotUseSameIP | DoNotUseSameRack | LabelUniqueMask) || sclass_has_keeparch_labels(c->sclassid,c->archflag)) {
		servcnt = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==VALID) {
				servers[servcnt++] = s->csid;
			}
		}
		labelcnt = sclass_get_keeparch_labelmasks(c->sclassid,c->archflag,&labelmasks);
		matching = do_perfect_match(labelcnt,servcnt,labelmasks,servers);
		for (i=0 ; i<labelcnt ; i++) {
			if (matching[i]<0) { // there are unmatched labels
				forcereplication++;
			}
		}
	}

// step 9. if chunk has number of copies less than goal then make another copy of this chunk
	if ((forcereplication || goal > vc) && vc+tdc > 0) {
		uint8_t canbefixed;
		canbefixed = 1;
		if (csdb_replicate_undergoals()) {
//		if ((csdb_getdisconnecttime()+ReplicationsDelayDisconnect)<now) {
			uint32_t rgvc,rgtdc;
			uint32_t lclass;

			if (vc+tdc==1 && goal>2) { // highest priority - chunks with only one copy and high goal
				prilevel = DPRIORITY_ENDANGERED_HIGHGOAL;
				lclass = 0;
			} else if (vc+tdc==1 && goal==2) { // next priority - chunks with only one copy
				prilevel = DPRIORITY_ENDANGERED;
				lclass = 0;
			} else if (vc==1 && tdc>0) { // next priority - chunks on one regular disk and some "marked for removal" disks
				prilevel = DPRIORITY_UNDERGOAL_MFR;
				lclass = 1;
			} else if (tdc>0) { // next priority - chunks on "marked for removal" disks
				prilevel = DPRIORITY_MFR;
				lclass = 1;
			} else if (vc < goal) { // next priority - standard undergoal chunks
				prilevel = DPRIORITY_UNDERGOAL;
				lclass = 1;
			} else { // lowest priority - wrong labeled chunks
				prilevel = DPRIORITY_WRONGLABEL;
				lclass = 1;
			}
			if (extrajob==0) {
				for (i=0 ; i<prilevel ; i++) {
					if (chunks_priority_leng[i]>0) { // we have chunks with higher priority than current chunk
						chunk_priority_enqueue(prilevel,c); // in such case only enqueue this chunk for future processing
						if (prilevel<DPRIORITY_WRONGLABEL) {
							inforec.notdone.copy_undergoal++;
						} else {
							inforec.notdone.copy_wronglabels++;
						}
						return;
					}
				}
			}

			matocsserv_get_server_groups(rcsids,MaxWriteRepl[lclass],servmaxpos);
			// rservcount = number of servers that are allowed to start new replication
			if (prilevel<DPRIORITY_UNDERGOAL_MFR) {
				rservcount = servmaxpos[CSSTATE_OVERLOADED];
			} else {
				rservcount = servmaxpos[CSSTATE_OK];
			}
//			rservcount = matocsserv_getservers_lessrepl(rcsids,MaxWriteRepl[lclass],(j<DPRIORITY_UNDERGOAL_MFR)?1:0,&allservflag);
			rgvc=0;
			rgtdc=0;
			vripcnt=0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<MaxReadRepl[lclass]) {
					if (s->valid==VALID) {
						rgvc++;
						if (vripcnt<255 && ReplicationsRespectTopology>1) {
							if (matocsserv_get_csdata(cstab[s->csid].ptr,vrip+vripcnt,NULL,NULL,NULL)==0) {
								vripcnt++;
							}
						}
					} else if (s->valid==TDVALID) {
						rgtdc++;
						if (vripcnt<255 && ReplicationsRespectTopology>1) {
							if (matocsserv_get_csdata(cstab[s->csid].ptr,vrip+vripcnt,NULL,NULL,NULL)==0) {
								vripcnt++;
							}
						}
					}
				}
			}
			if (ReplicationsRespectTopology>1) {
				chunk_rack_sort(rcsids,rservcount,vrip,vripcnt);
			}
			if (rgvc+rgtdc>0 && rservcount>0) { // have at least one server to read from and at least one to write to
				if ((DoNotUseSameIP | DoNotUseSameRack | LabelUniqueMask) || sclass_has_keeparch_labels(c->sclassid,c->archflag)) { // labels version
					uint8_t sclass_mode;
					uint16_t maxlimited;
					uint16_t dstservcnt;
					// reverse order - do_perfect_match matches servers 'from right' - this is important when ReplicationsRespectTopology>1
					servcnt = 0;
					// first set all servers with limited write replication counter
					for (i=servmaxpos[CSSTATE_OVERLOADED] ; i<servmaxpos[CSSTATE_LIMIT_REACHED] ; i++) {
						for (s=c->slisthead ; s && s->csid!=rcsids[i] ; s=s->next) {}
						if (s==NULL) {
							servers[servcnt++] = rcsids[i];
						}
					}
					// then add overloaded servers (but only when they are not already included in rservcount)
					for (i=rservcount ; i<servmaxpos[CSSTATE_OVERLOADED] ; i++) {
						for (s=c->slisthead ; s && s->csid!=rcsids[i] ; s=s->next) {}
						if (s==NULL) {
							servers[servcnt++] = rcsids[i];
						}
					}
					maxlimited = servcnt;
					// then all valid servers in reverse order (matching from the 'right')
					for (i=0 ; i<rservcount ; i++) {
						for (s=c->slisthead ; s && s->csid!=rcsids[rservcount-1-i] ; s=s->next) {}
						if (s==NULL) {
							servers[servcnt++] = rcsids[rservcount-1-i];
						}
					}
					dstservcnt = servcnt;
					for (s=c->slisthead ; s ; s=s->next) {
						if (s->valid==VALID) {
							servers[servcnt++] = s->csid;
						}
					}
					labelcnt = sclass_get_keeparch_labelmasks(c->sclassid,c->archflag,&labelmasks);
					sclass_mode = sclass_get_mode(c->sclassid);
					matching = do_perfect_match(labelcnt,servcnt,labelmasks,servers);
					for (i=0 ; i<labelcnt ; i++) {
						int32_t servpos;
						if (matching[i]<(int32_t)labelcnt) {
							servpos=-1;
						} else {
							servpos=matching[i]-labelcnt;
						}
//						syslog(LOG_NOTICE,"labelpos: %u ; serverpos: %d ; serverip: %s ; serverlabels: %s",i,servpos,matocsserv_getstrip(cstab[servers[servpos]].ptr),matocsserv_server_get_labelstr(cstab[servers[servpos]].ptr));
						if (servpos<0) { // matched but only on 'no space' server (or totally unmatched)
							if (sclass_mode==SCLASS_MODE_STRICT) {
								canbefixed = 0;
							} else { // all other modes - labels matched to 'no space' servers only can be replicated anywhere
								for (j=maxlimited ; j<dstservcnt ; j++) { // check all possibe destination servers
									if (matching[j+labelcnt]<0) { // matched to label? - if not then we can use it
//										syslog(LOG_NOTICE,"replicate to first available server (STD MODE and LOOSE MODE)");
										if (chunk_undergoal_replicate(c, servers[j], now, lclass, prilevel, &inforec, rgvc, rgtdc)>=0) {
											return;
										}
									}
								}
							}
						} else if (servpos<maxlimited) { // matched but only on 'busy' server
							if (sclass_mode==SCLASS_MODE_STRICT) {
								canbefixed = 0;
							} else if (sclass_mode==SCLASS_MODE_LOOSE) { // in loose mode labels matched only to overloaded servers
								for (j=maxlimited ; j<dstservcnt ; j++) { // check all possibe destination servers
									if (matching[j+labelcnt]<0) { // matched to label? - if not then we can use it
//										syslog(LOG_NOTICE,"replicate to first available server (LOOSE MODE)");
										if (chunk_undergoal_replicate(c, servers[j], now, lclass, prilevel, &inforec, rgvc, rgtdc)>=0) {
											return;
										}
									}
								}
							} // in standard mode - we leave matching and then do not perform replication
						} else if (servpos<dstservcnt) { // can be replicated to correct label
//							syslog(LOG_NOTICE,"replicate to correct label (all modes)");
							if (chunk_undergoal_replicate(c, servers[servpos], now, lclass, prilevel, &inforec, rgvc, rgtdc)>=0) {
								return;
							}
						}
					}
/*
					allowallservers = 0;
					if (scount<=rservcount) { // all servers can accept replication
						uint32_t unmatchedlabels;
						unmatchedlabels = 0;
						for (i=0 ; i<labelcnt ; i++) {
							if (matching[i]<0) {
								unmatchedlabels++;
							}
						}
						if (vc >= goal) { // not undergoal chunk
							if (unmatchedlabels >= forcereplication) { // can't fix wrong labels
								canbefixed = 0;
							}
						} else { // this is undergoal chunk
							if (goal>scount && vc==scount) {
								canbefixed = 0;
							} else if (unmatchedlabels>0) { // can't match all labels, so use all chunkservers
								allowallservers = 1;
							}
						}
					}
*/
/*
					if (vc < sclass_getgoal(c->sclassid) && csdb_servers_count()<=rservcount) { // all servers can accept replications
						for (i=0 ; i<labelcnt ; i++) {
							if (matching[i]<0) {
								unmatchedlabels++;
							}
						}
					}
*/
/*					for (i=0 ; i<dstservcnt && canbefixed ; i++) {
						if (matching[i+labelcnt]>=0 || allowallservers) {
							if (chunk_undergoal_replicate(c, servers[i], now, lclass, prilevel, &inforec, rgvc, rgtdc)>=0) {
								return;
							}
						}
					}
*/
				} else { // classic goal version
					if (goal>servmaxpos[CSSTATE_LIMIT_REACHED] && vc==servmaxpos[CSSTATE_LIMIT_REACHED]) {
						canbefixed = 0;
					} else {
						for (i=0 ; i<rservcount ; i++) {
							for (s=c->slisthead ; s && s->csid!=rcsids[i] ; s=s->next) {}
							if (!s) {
								if (chunk_undergoal_replicate(c, rcsids[i], now, lclass, prilevel, &inforec, rgvc, rgtdc)>=0) {
									return;
								}
							}
						}
					}
				}
			}
			if (canbefixed) { // enqueue only chunks which can be fixed and only if there are servers which reached replication limits
				chunk_priority_enqueue(prilevel,c);
			}
		}
		if (vc < goal) {
			inforec.notdone.copy_undergoal++;
		} else {
			if (canbefixed==0) {
				inforec.labels_dont_match++;
			} else {
				inforec.notdone.copy_wronglabels++;
			}
		}
	}

	if (extrajob) { // do not rebalane doing "extra" jobs.
		return;
	}

	if (fullservers==0) {
		uint8_t queues_empty;
		queues_empty = 1;
		for (i=0 ; i<DANGER_PRIORITIES ; i++) {
			if (chunks_priority_leng[i]>0) {
				if (i<REPLICATION_DANGER_PRIORITIES) {
					return; // we have pending undergaal/wronglabeled chunks, so ignore chunkserver rebalance
				} else {
					queues_empty = 0;
				}
			}
		}
		if (queues_empty) {
			if (c->ondangerlist) {
				syslog(LOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": fixing 'ondangerlist' flag",c->chunkid,c->version);
				c->ondangerlist = 0;
			}
		}
	}

	if (goal == vc && vc+tdc>0) {
		double maxdiff;
		uint8_t sclass_mode;
		uint8_t need_label_version;

//		need_label_version = ((DoNotUseSameIP | DoNotUseSameRack | LabelUniqueMask) || sclass_has_keeparch_labels(c->sclassid,c->archflag))?1:0;
		need_label_version = sclass_has_keeparch_labels(c->sclassid,c->archflag)?1:0;

		servcnt = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==VALID) {
				servers[servcnt++] = s->csid;
			}
		}
		extraservcnt = servcnt;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid!=VALID) {
				servers[extraservcnt++] = s->csid;
			}
		}

		if (need_label_version) {
			labelcnt = sclass_get_keeparch_labelmasks(c->sclassid,c->archflag,&labelmasks);
			sclass_mode = sclass_get_mode(c->sclassid);
			matching = do_perfect_match(labelcnt,servcnt,labelmasks,servers);
		} else {
			// set something to silence potential compiler warnings
			labelcnt = goal;
			sclass_mode = SCLASS_MODE_LOOSE;
			matching = NULL;
		}

		if (dservcount==0) {
			dservcount = matocsserv_getservers_ordered(dcsids);
		}

		srccsid = MAXCSCOUNT;
		dstcsid = MAXCSCOUNT;
		maxdiff = 0.0;

		for (i=0 ; i<servcnt ; i++) {
			uint32_t *labelmask;
			uint8_t anyunmatched;
			double srcusage,dstusage;
			uint8_t lclass;

			labelmask = NULL;
			anyunmatched = 0;
			if (need_label_version) {
				if (matching[labelcnt+i]>=0) {
					labelmask = labelmasks[matching[labelcnt+i]];
				} else if (sclass_mode!=SCLASS_MODE_LOOSE) {
					anyunmatched = 1;
					// in normal and strict mode you can use any definition that do not match any copy (it will even fix wrong labels)
				}
			}
			srcusage = matocsserv_get_usage(cstab[servers[i]].ptr);
			repl_read_counter = matocsserv_replication_read_counter(cstab[servers[i]].ptr,now);
			if (repl_read_counter < MaxReadRepl[2] || repl_read_counter < MaxReadRepl[3]) { // here accept any rebalance limit
				for (j=0 ; j<dservcount ; j++) {
					dstusage = matocsserv_get_usage(cstab[dcsids[j]].ptr);
					if (srcusage - dstusage < maxdiff) { // already found better src,dst pair
						break;
					}
					if (((srcusage - dstusage) <= AcceptableDifference) && last_rebalance+(0.01/(srcusage-dstusage))>=now) { // when difference is small then do not hurry
						break;
					}
					k = 0;
					while (k<extraservcnt) {
						if (servers[k]==dcsids[j]) {
							break;
						}
						if (DoNotUseSameIP) {
							if (matocsserv_server_get_ip(cstab[servers[k]].ptr)==matocsserv_server_get_ip(cstab[dcsids[j]].ptr)) {
								break;
							}
						} else if (DoNotUseSameRack) {
							if (topology_get_rackid(matocsserv_server_get_ip(cstab[servers[k]].ptr))==topology_get_rackid(matocsserv_server_get_ip(cstab[dcsids[j]].ptr))) {
								break;
							}
						} else if (LabelUniqueMask) {
							if ((matocsserv_server_get_labelmask(cstab[servers[k]].ptr) & LabelUniqueMask) == (matocsserv_server_get_labelmask(cstab[dcsids[j]].ptr) & LabelUniqueMask)) {
								break;
							}
						}
						k++;
					}
					if (k<extraservcnt) { // one of existing copies is on this server or this server has same IP/RACKID/UNIQLABELS
						continue;
					}
					if (anyunmatched) { // check if this server match any of unmatched labels
						for (k=0 ; k<labelcnt ; k++) {
							if (matching[k]<0) {
								if (matocsserv_server_has_labels(cstab[dcsids[j]].ptr,labelmasks[k])) {
									break;
								}
							}
						}
						if (k==labelcnt) { // nope
							break;
						}
					}
					if (labelmask==NULL || matocsserv_server_has_labels(cstab[dcsids[j]].ptr,labelmask)) { // if we have specific label then check if this server has them
						if ((srcusage - dstusage) > AcceptableDifference*1.5) { // now we know usage difference, so we can set proper limit class
							lclass = 3;
						} else {
							lclass = 2;
						}
						if (repl_read_counter < MaxReadRepl[lclass] && matocsserv_replication_write_counter(cstab[dcsids[j]].ptr,now)<MaxWriteRepl[lclass]) {
							maxdiff = srcusage - dstusage;
							dstcsid = dcsids[j];
							srccsid = servers[i];
						}
					}
				}
			}
		}

		if (dstcsid!=MAXCSCOUNT && srccsid!=MAXCSCOUNT) {
			stats_chunkops[CHUNK_OP_REPLICATE_TRY]++;
			matocsserv_send_replicatechunk(cstab[dstcsid].ptr,c->chunkid,c->version,cstab[srccsid].ptr);
			c->needverincrease = 1;
			inforec.copy_rebalance++;
			last_rebalance = now;
		}

		return;
	}
}

uint8_t chunk_labelset_can_be_fulfilled(uint8_t labelcnt,uint32_t **labelmasks) {
	static uint16_t *stdcsids = NULL;
	static uint16_t *olcsids = NULL;
	static uint16_t *allcsids = NULL;
	static uint16_t stdcscnt;
	static uint16_t olcscnt;
	static uint16_t allcscnt;
	uint8_t r;
	uint32_t i;
	int32_t *matching;

	if (stdcsids==NULL) {
		stdcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(stdcsids);
	}
	if (olcsids==NULL) {
		olcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(olcsids);
	}
	if (allcsids==NULL) {
		allcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(allcsids);
	}

	if (labelcnt==0 || labelmasks==NULL) {
		matocsserv_getservers_test(&stdcscnt,stdcsids,&olcscnt,olcsids,&allcscnt,allcsids);
	}

	matching = do_perfect_match(labelcnt,stdcscnt,labelmasks,stdcsids);

	r = 1;
	for (i=0 ; i<labelcnt ; i++) {
		if (matching[i]<0) {
			r = 0;
			break;
		}
	}
	if (r==1) {
		return 3; // can be fulfilled
	}

	if (olcsids > stdcsids) {
		matching = do_perfect_match(labelcnt,olcscnt,labelmasks,olcsids);

		r = 1;
		for (i=0 ; i<labelcnt ; i++) {
			if (matching[i]<0) {
				r = 0;
				break;
			}
		}
		if (r==1) {
			return 2; // can be fulfilled using overloaded servers
		}
	}

	if (allcsids > olcsids) {
		matching = do_perfect_match(labelcnt,allcscnt,labelmasks,allcsids);

		r = 1;
		for (i=0 ; i<labelcnt ; i++) {
			if (matching[i]<0) {
				r = 0;
				break;
			}
		}
		if (r==1) {
			return 1; // can be fulfilled using servers with no space available
		}
	}

	return 0;
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

int chunk_unlock(uint64_t chunkid) {
	chunk *c;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	c->lockedto = 0;
	chunk_write_counters(c,0);
	chunk_priority_queue_check(c,1);
	if (c->ondangerlist) {
		chunk_do_jobs(c,matocsserv_servers_count(),matocsserv_almostfull_servers(),main_time(),1);
	} else {
		matoclserv_chunk_unlocked(c->chunkid,c);
	}
	return MFS_STATUS_OK;
}

int chunk_mr_unlock(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	c->lockedto = 0;
	chunk_write_counters(c,0);
	return MFS_STATUS_OK;
}


void chunk_jobs_main(void) {
	uint32_t i,j,l,h,t,lc,hashsteps;
	uint16_t scount,csid;
	uint16_t fullservers;
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

	// full servers are not included here
	scount = matocsserv_servers_count();

	if (scount==0 || chunkrehashpos==0) {
		return;
	}

	fullservers = matocsserv_almostfull_servers();

	now = main_time();
	chunk_do_jobs(NULL,JOBS_EVERYTICK,0,now,0);

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
		if (lc > (1.0 - chunks_priority_mincpsperc[j])*HashCPTMax) { // prevent starvation of lowest priority chunks by highest priority chunks
			lc = HashCPTMax * (1.0 - chunks_priority_mincpsperc[j]);
		}
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
					chunk_do_jobs(c,scount,fullservers,now,1);
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
		if (jobshcnt>=chunkrehashpos) {
			chunk_do_jobs(NULL,JOBS_EVERYLOOP,0,now,0);	// every loop tasks
			jobshpos = 0;
			jobshcnt = 0;
			jobshmax = chunkrehashpos;
			jobshstep *= 16;
			if (jobshstep==0 || jobshstep>=jobshmax) {
				jobshstep = 1;
			} else {
				if ((jobshmax&1)==0) {
					jobshmax--;
				}
			}
			for (csid = csusedhead ; csid < MAXCSCOUNT ; csid = cstab[csid].next) {
				switch (cstab[csid].mfr_state) {
					case CAN_BE_REMOVED:
					case UNKNOWN_SOFT:
					case WAS_IN_PROGRESS:
						cstab[csid].mfr_state = CAN_BE_REMOVED;
						break;
					case REPL_IN_PROGRESS:
						cstab[csid].mfr_state = WAS_IN_PROGRESS;
						break;
					/* case UNKNOWN_HARD: */
					default:
						cstab[csid].mfr_state = UNKNOWN_SOFT;
						break;
				}
			}
		} else {
			c = chunkhashtab[jobshpos>>HASHTAB_LOBITS][jobshpos&HASHTAB_MASK];
			while (c) {
				cn = c->next;
				if (c->lockedto<(uint32_t)main_time() && c->slisthead==NULL && c->fcount==0 && c->ondangerlist==0 && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
					changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
					chunk_delete(c);
				} else {
					chunk_do_jobs(c,scount,fullservers,now,0);
					lc++;
				}
				c = cn;
			}
			jobshcnt++;
			if (jobshcnt<jobshmax) {
				jobshpos += jobshstep;
				jobshpos %= jobshmax;
			} else {
				jobshpos = jobshcnt;
			}
		}
	}
}

/* ---- */

#define CHUNKFSIZE 17
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
			fprintf(fd,"*|i:%016"PRIX64"|v:%08"PRIX32"|g:%"PRIu8"|t:%10"PRIu32"\n",c->chunkid,c->version,c->sclassid,lockedto);
		}
	}
}
*/
int chunk_load(bio *fd,uint8_t mver) {
	uint8_t hdr[8];
	uint8_t loadbuff[CHUNKFSIZE];
	const uint8_t *ptr;
	int32_t r,recsize;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version,lockedto;
	uint8_t archflag;

	chunks=0;
	if (bio_read(fd,hdr,8)!=8) {
		syslog(LOG_WARNING,"chunks: can't read header");
		return -1;
	}
	ptr = hdr;
	nextchunkid = get64bit(&ptr);
	recsize = (mver==0x10)?16:CHUNKFSIZE;
	for (;;) {
		r = bio_read(fd,loadbuff,recsize);
		if (r!=recsize) {
			syslog(LOG_WARNING,"chunks: read error");
			return -1;
		}
		ptr = loadbuff;
		chunkid = get64bit(&ptr);
		version = get32bit(&ptr);
		lockedto = get32bit(&ptr);
		if (mver==0x10) {
			archflag = 0;
		} else {
			archflag = get8bit(&ptr);
		}
		if (chunkid>0) {
			c = chunk_new(chunkid);
			c->version = version;
			c->lockedto = lockedto;
			c->archflag = archflag;
		} else {
			if (version==0 && lockedto==0 && archflag==0) {
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
	uint8_t storebuff[CHUNKFSIZE];
	uint8_t *ptr;
	uint8_t archflag;
	uint32_t i;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version;
	uint32_t lockedto,now;

	if (fd==NULL) {
		return 0x11;
	}
	now = main_time();
	ptr = hdr;
	put64bit(&ptr,nextchunkid);
	if (bio_write(fd,hdr,8)!=8) {
		return 0xFF;
	}
	for (i=0 ; i<chunkrehashpos ; i++) {
		for (c=chunkhashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; c ; c=c->next) {
			ptr = storebuff;
			chunkid = c->chunkid;
			put64bit(&ptr,chunkid);
			version = c->version;
			put32bit(&ptr,version);
			lockedto = c->lockedto;
			if (lockedto<now) {
				lockedto = 0;
			}
			put32bit(&ptr,lockedto);
			archflag = c->archflag;
			put8bit(&ptr,archflag);
			if (bio_write(fd,storebuff,CHUNKFSIZE)!=CHUNKFSIZE) {
				return 0xFF;
			}
		}
	}
	memset(storebuff,0,CHUNKFSIZE);
	if (bio_write(fd,storebuff,CHUNKFSIZE)!=CHUNKFSIZE) {
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
	csfreetail = MAXCSCOUNT-1;
	csusedhead = MAXCSCOUNT;
	for (i=0 ; i<MAXSCLASS*2 ; i++) {
		for (j=0 ; j<11 ; j++) {
			allchunkcounts[i][j]=0;
			regularchunkcounts[i][j]=0;
		}
	}
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

void chunk_term(void) {
	uint32_t i;
	chunk_priority_queue_check(NULL,1); // free tabs
	chunk_do_jobs(NULL,JOBS_TERM,0,main_time(),0); // free tabs
	for (i=0 ; i<MAXSCLASS*2 ; i++) {
		free(allchunkcounts[i]);
		free(regularchunkcounts[i]);
	}
	free(allchunkcounts);
	free(regularchunkcounts);
}

void chunk_load_cfg_common(void) {
	uint32_t uniqmode;

	uniqmode = cfg_getuint32("CHUNKS_UNIQUE_MODE",0);

	DoNotUseSameIP=0;
	DoNotUseSameRack=0;
	LabelUniqueMask=0;

	if (uniqmode==1) {
		DoNotUseSameIP=1;
	} else if (uniqmode==2) {
		DoNotUseSameRack=1;
	}

	ReplicationsDelayInit = cfg_getuint32("REPLICATIONS_DELAY_INIT",60);
	ReplicationsRespectTopology = cfg_getuint8("REPLICATIONS_RESPECT_TOPOLOGY",0);
	CreationsRespectTopology = cfg_getuint32("CREATIONS_RESPECT_TOPOLOGY",0);

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
}

void chunk_reload(void) {
	uint32_t oldMaxDelSoftLimit,oldMaxDelHardLimit,oldDangerMaxLeng;
	uint32_t cps,i,j;
	char *repstr;

	oldDangerMaxLeng = DangerMaxLeng;

	oldMaxDelSoftLimit = MaxDelSoftLimit;
	oldMaxDelHardLimit = MaxDelHardLimit;

	chunk_load_cfg_common();

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

	chunk_load_cfg_common();

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

	chunk_hash_init();
//	for (i=0 ; i<HASHSIZE ; i++) {
//		chunkhash[i]=NULL;
//	}
	cstab = malloc(sizeof(csdata)*MAXCSCOUNT);
	passert(cstab);
	for (i=0 ; i<MAXCSCOUNT ; i++) {
		cstab[i].next = i+1;
		cstab[i].prev = i-1;
		cstab[i].opchunks = NULL;
		cstab[i].valid = 0;
		cstab[i].registered = 0;
		cstab[i].mfr_state = UNKNOWN_HARD;
		cstab[i].newchunkdelay = 0;
		cstab[i].lostchunkdelay = 0;
	}
	cstab[0].prev = MAXCSCOUNT;
	csfreehead = 0;
	csfreetail = MAXCSCOUNT-1;
	csusedhead = MAXCSCOUNT;
	allchunkcounts = malloc(sizeof(uint32_t*)*MAXSCLASS*2);
	passert(allchunkcounts);
	regularchunkcounts = malloc(sizeof(uint32_t*)*MAXSCLASS*2);
	passert(regularchunkcounts);
	for (i=0 ; i<MAXSCLASS*2 ; i++) {
		allchunkcounts[i] = malloc(sizeof(uint32_t)*11);
		passert(allchunkcounts[i]);
		regularchunkcounts[i] = malloc(sizeof(uint32_t)*11);
		passert(regularchunkcounts[i]);
		for (j=0 ; j<11 ; j++) {
			allchunkcounts[i][j]=0;
			regularchunkcounts[i][j]=0;
		}
	}
	jobshpos = 0;
	jobshstep = 1;
	jobshcnt = 0;
	jobshmax = 0;
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
	chunk_do_jobs(NULL,JOBS_INIT,0,main_time(),0);	// clear chunk loop internal data, and allocate tabs
	chunk_priority_queue_check(NULL,0); // allocate 'servers' tab

#ifdef MFSDEBUG
	mfsdebug(NULL);
	main_time_register(1,0,mfsdebug_flush);
#endif

	main_reload_register(chunk_reload);
	// main_time_register(1,0,chunk_jobs_main);
	main_msectime_register(1000/TICKSPERSECOND,0,chunk_jobs_main);
	main_destruct_register(chunk_term);
	return 1;
}
