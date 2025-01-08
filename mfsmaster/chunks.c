/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
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
#include <fcntl.h>
#include <unistd.h>
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
#include "chunkdelay.h"
#include "chunks.h"
#include "filesystem.h"
#include "datapack.h"
#include "massert.h"
#include "hashfn.h"
#include "bitops.h"
#include "buckets.h"
#include "clocks.h"
#include "storageclass.h"

#define MINLOOPTIME 60
#define MAXLOOPTIME 7200
#define MAXCPS 10000000
#define MINCPS 10000

#define HASHTAB_LOBITS 24
#define HASHTAB_HISIZE (0x80000000>>(HASHTAB_LOBITS))
#define HASHTAB_LOSIZE (1<<HASHTAB_LOBITS)
#define HASHTAB_MASK (HASHTAB_LOSIZE-1)
#define HASHTAB_MOVEFACTOR 5

#define ec_multiplier 2

// #define DISCLOOPRATIO 0x400

//#define HASHSIZE 0x100000
//#define HASHPOS(chunkid) (((uint32_t)chunkid)&0xFFFFF)

//#define DISCLOOPELEMENTS (HASHSIZE/0x400)

/* replication_mode */
enum {SIMPLE,SPLIT,RECOVER,JOIN};

enum {JOBS_INIT,JOBS_EVERYLOOP,JOBS_EVERYTICK,JOBS_TERM,JOBS_CHUNK};

/* chunk.operation */
enum {NONE=0,CREATE,SET_VERSION,DUPLICATE,TRUNCATE,DUPTRUNC,REPLICATE,LOCALSPLIT};

static const char* opstr[] = {
	"NONE",
	"CREATE",
	"SET_VERSION",
	"DUPLICATE",
	"TRUNCATE",
	"DUPLICATE+TRUNCATE",
	"REPLICATE",
	"LOCALSPLIT",
	"???"
};

static const char* op_to_str(uint8_t op) {
	if (op<8) {
		return opstr[op];
	} else {
		return opstr[8];
	}
}

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

static const char* validstr[] = {
	"INVALID",
	"DEL",
	"BUSY",
	"VALID",
	"WVER",
	"TDBUSY",
	"TDVALID",
	"TDWVER",
	"???"
};

static const char* valid_to_str(uint8_t valid) {
	if (valid<8) {
		return validstr[valid];
	} else {
		return validstr[8];
	}
}

enum {
	INTERNAL_ERROR,
	DISCONNECTED_CHUNKSERVER,
	DELETED_UNEXPECTED_BUSY_COPY_OR_PART,
	FIXED_COPY_VERSION,
	FIXED_EC8_VERSION,
	FIXED_EC4_VERSION,
	NO_VALID_COPIES_AND_PARTS,
	NO_COPIES_AND_PARTS,
	DELETED_INVALID_COPY_OR_PART_TO_MAKE_SPACE,
	DELETED_SOME_INVALID_COPIES_OR_PARTS,
	FOUND_NOT_FINISHED_REPLICATION,
	CHUNK_IS_BEING_MODIFIED,
	UNEXPECTED_BUSY_COPY_OR_PART,
	DELETED_UNUSED_CHUNK,
	BLOCKED_BY_HIGHER_PRIORITY_QUEUE,
	BLOCKED_BY_CHUNKSERVER_IN_MAINTENANCE_MODE,
	DELETED_PART_ON_THE_SAME_SERVER,
	DELETED_DUPLICATED_PART,
	REPLICATED_DUPLICATED_PART,
	ERROR_REPLICATING_DUPLICATED_PART,
	CANT_FIX_PARTS_ON_THE_SAME_SERVER,
	FOUND_DUPLICATED_EC_PARTS,
	DELETED_INVALID_PART_TO_MAKE_SPACE,
	DELETED_MFR_PART_TO_MAKE_SPACE,
	CANT_DELETE_MFR_PART_TO_MAKE_SPACE,
	CANT_FIND_VALID_REPLICATION_SOURCE,
	REPLICATED_MFR_DATA_PART,
	REPLICATED_MFR_CHKSUM_PART,
	ERROR_REPLICATING_MFR_DATA_PART,
	CANT_REPLICATE_MFR_DATA_PART,
	ERROR_REPLICATING_MFR_CHKSUM_PART,
	CANT_REPLICATE_MFR_CHKSUM_PART,
	ERROR_REPLICATING_MISSING_EC8_DATA_PART_FROM_MFR,
	CANT_FIND_EC8_DECODE_MATRIX,
	RECOVERED_MISSING_EC8_DATA_PART,
	ERROR_RECOVERING_MISSING_EC8_DATA_PART,
	CANT_RECOVER_MISSING_EC8_DATA_PART,
	ERROR_REPLICATING_MISSING_EC4_DATA_PART_FROM_MFR,
	CANT_FIND_EC4_DECODE_MATRIX,
	RECOVERED_MISSING_EC4_DATA_PART,
	ERROR_RECOVERING_MISSING_EC4_DATA_PART,
	CANT_RECOVER_MISSING_EC4_DATA_PART,
	ERROR_CREATING_MISSING_PARTS_FROM_COPY,
	CREATED_MISSING_PARTS_FROM_COPY,
	ERROR_REPLICATING_MISSING_EC8_CHKSUM_PART_FROM_MFR,
	CANT_FIND_EC8_ENCODE_MATRIX,
	CREATED_MISSING_EC8_CHKSUM_PART,
	ERROR_CREATING_MISSING_EC8_CHKSUM_PART,
	CANT_CREATE_MISSING_EC8_CHKSUM_PART,
	ERROR_REPLICATING_MISSING_EC4_CHKSUM_PART_FROM_MFR,
	CANT_FIND_EC4_ENCODE_MATRIX,
	CREATED_MISSING_EC4_CHKSUM_PART,
	ERROR_CREATING_MISSING_EC4_CHKSUM_PART,
	CANT_CREATE_MISSING_EC4_CHKSUM_PART,
	CREATED_MISSING_EC8_PART_FROM_COPY,
	CREATED_MISSING_EC4_PART_FROM_COPY,
	ERROR_CREATING_MISSING_EC8_PART_FROM_COPY,
	ERROR_CREATING_MISSING_EC4_PART_FROM_COPY,
	CANT_CREATE_PART_FROM_COPY,
	CREATED_LABELED_COPY_FROM_EC8_PARTS,
	CREATED_LABELED_COPY_FROM_EC4_PARTS,
	CREATED_COPY_FROM_EC8_PARTS,
	CREATED_COPY_FROM_EC4_PARTS,
	ERROR_CREATING_LABELED_COPY_FROM_EC8_PARTS,
	ERROR_CREATING_LABELED_COPY_FROM_EC4_PARTS,
	ERROR_CREATING_COPY_FROM_EC8_PARTS,
	ERROR_CREATING_COPY_FROM_EC4_PARTS,
	CANT_CREATE_COPY_FROM_PARTS,
	DELETED_COPIES_IN_EC_MODE,
	CANT_DELETE_COPIES_IN_EC_MODE,
	DELETED_PARTS_IN_COPY_MODE,
	CANT_DELETE_PARTS_IN_COPY_MODE,
	DELETED_EXTRA_EC_PARTS,
	CANT_DELETE_EXTRA_EC_PARTS,
	ERROR_REPLICATING_WRONG_LABELED_DATA_PART,
	ERROR_REPLICATING_WRONG_LABELED_CHKSUM_PART,
	REPLICATED_WRONG_LABELED_DATA_PART,
	REPLICATED_WRONG_LABELED_CHKSUM_PART,
	CANT_REPLICATE_WRONG_LABELED_PART,
	REPLICATED_WRONG_LABELED_PARTS,
	DELETED_EXTRA_COPIES,
	CANT_DELETE_EXTRA_COPIES,
	DELETED_MFR_COPY_TO_MAKE_SPACE,
	REPLICATED_WRONG_LABELED_COPY_UNMATCHED,
	REPLICATED_WRONG_LABELED_COPY_BUSY,
	REPLICATED_WRONG_LABELED_COPY_GOOD,
	REPLICATED_UNDERGOAL_COPY,
	CANT_REPLICATE_WRONG_LABELED_COPY,
	CANT_REPLICATE_UNDERGOAL_COPY,
	CANT_REBALANCE_ON_EXTRA_CALL,
	REBALANCE_BLOCKED_BY_PRIORITY_QUEUES,
	REBALANCE_BLOCKED_BY_TOO_MANY_FAILS,
	REBALANCE_DONE,
	REBALANCE_NOT_DONE_OR_NOT_NEEDED,
	JOB_EXIT_REASONS,
};

static const char* job_exit_reason_str[JOB_EXIT_REASONS+1] = {
	"INTERNAL_ERROR",
	"DISCONNECTED_CHUNKSERVER",
	"DELETED_UNEXPECTED_BUSY_COPY_OR_PART",
	"FIXED_COPY_VERSION",
	"FIXED_EC8_VERSION",
	"FIXED_EC4_VERSION",
	"NO_VALID_COPIES_AND_PARTS",
	"NO_COPIES_AND_PARTS",
	"DELETED_INVALID_COPY_OR_PART_TO_MAKE_SPACE",
	"DELETED_SOME_INVALID_COPIES_OR_PARTS",
	"FOUND_NOT_FINISHED_REPLICATION",
	"CHUNK_IS_BEING_MODIFIED",
	"UNEXPECTED_BUSY_COPY_OR_PART",
	"DELETED_UNUSED_CHUNK",
	"BLOCKED_BY_HIGHER_PRIORITY_QUEUE",
	"BLOCKED_BY_CHUNKSERVER_IN_MAINTENANCE_MODE",
	"DELETED_PART_ON_THE_SAME_SERVER",
	"DELETED_DUPLICATED_PART",
	"REPLICATED_DUPLICATED_PART",
	"ERROR_REPLICATING_DUPLICATED_PART",
	"CANT_FIX_PARTS_ON_THE_SAME_SERVER",
	"FOUND_DUPLICATED_EC_PARTS",
	"DELETED_INVALID_PART_TO_MAKE_SPACE",
	"DELETED_MFR_PART_TO_MAKE_SPACE",
	"CANT_DELETE_MFR_PART_TO_MAKE_SPACE",
	"CANT_FIND_VALID_REPLICATION_SOURCE",
	"REPLICATED_MFR_DATA_PART",
	"REPLICATED_MFR_CHKSUM_PART",
	"ERROR_REPLICATING_MFR_DATA_PART",
	"CANT_REPLICATE_MFR_DATA_PART",
	"ERROR_REPLICATE_MFR_CHKSUM_PART",
	"CANT_REPLICATE_MFR_CHKSUM_PART",
	"ERROR_REPLICATING_MISSING_EC8_DATA_PART_FROM_MFR",
	"CANT_FIND_EC8_DECODE_MATRIX",
	"RECOVERED_MISSING_EC8_DATA_PART",
	"ERROR_RECOVERING_MISSING_EC8_DATA_PART",
	"CANT_RECOVER_MISSING_EC8_DATA_PART",
	"ERROR_REPLICATING_MISSING_EC4_DATA_PART_FROM_MFR",
	"CANT_FIND_EC4_DECODE_MATRIX",
	"RECOVERED_MISSING_EC4_DATA_PART",
	"ERROR_RECOVERING_MISSING_EC4_DATA_PART",
	"CANT_RECOVER_MISSING_EC4_DATA_PART",
	"ERROR_CREATING_MISSING_PARTS_FROM_COPY",
	"CREATED_MISSING_PARTS_FROM_COPY",
	"ERROR_REPLICATING_MISSING_EC8_CHKSUM_PART_FROM_MFR",
	"CANT_FIND_EC8_ENCODE_MATRIX",
	"CREATED_MISSING_EC8_CHKSUM_PART",
	"ERROR_CREATING_MISSING_EC8_CHKSUM_PART",
	"CANT_CREATE_MISSING_EC8_CHKSUM_PART",
	"ERROR_REPLICATING_MISSING_EC4_CHKSUM_PART_FROM_MFR",
	"CANT_FIND_EC4_ENCODE_MATRIX",
	"CREATED_MISSING_EC4_CHKSUM_PART",
	"ERROR_CREATING_MISSING_EC4_CHKSUM_PART",
	"CANT_CREATE_MISSING_EC4_CHKSUM_PART",
	"CREATED_MISSING_EC8_PART_FROM_COPY",
	"CREATED_MISSING_EC4_PART_FROM_COPY",
	"ERROR_CREATING_MISSING_EC8_PART_FROM_COPY",
	"ERROR_CREATING_MISSING_EC4_PART_FROM_COPY",
	"CANT_CREATE_PART_FROM_COPY",
	"CREATED_LABELED_COPY_FROM_EC8_PARTS",
	"CREATED_LABELED_COPY_FROM_EC4_PARTS",
	"CREATED_COPY_FROM_EC8_PARTS",
	"CREATED_COPY_FROM_EC4_PARTS",
	"ERROR_CREATING_LABELED_COPY_FROM_EC8_PARTS",
	"ERROR_CREATING_LABELED_COPY_FROM_EC4_PARTS",
	"ERROR_CREATING_COPY_FROM_EC8_PARTS",
	"ERROR_CREATING_COPY_FROM_EC4_PARTS",
	"CANT_CREATE_COPY_FROM_PARTS",
	"DELETED_COPIES_IN_EC_MODE",
	"CANT_DELETE_COPIES_IN_EC_MODE",
	"DELETED_PARTS_IN_COPY_MODE",
	"CANT_DELETE_PARTS_IN_COPY_MODE",
	"DELETED_EXTRA_EC_PARTS",
	"CANT_DELETE_EXTRA_EC_PARTS",
	"ERROR_REPLICATING_WRONG_LABELED_DATA_PART",
	"ERROR_REPLICATING_WRONG_LABELED_CHKSUM_PART",
	"REPLICATED_WRONG_LABELED_DATA_PART",
	"REPLICATED_WRONG_LABELED_CHKSUM_PART",
	"CANT_REPLICATE_WRONG_LABELED_PART",
	"REPLICATED_WRONG_LABELED_PARTS",
	"DELETED_EXTRA_COPIES",
	"CANT_DELETE_EXTRA_COPIES",
	"DELETED_MFR_COPY_TO_MAKE_SPACE",
	"REPLICATED_WRONG_LABELED_COPY_UNMATCHED",
	"REPLICATED_WRONG_LABELED_COPY_BUSY",
	"REPLICATED_WRONG_LABELED_COPY_GOOD",
	"REPLICATED_UNDERGOAL_COPY",
	"CANT_REPLICATE_WRONG_LABELED_COPY",
	"CANT_REPLICATE_UNDERGOAL_COPY",
	"CANT_REBALANCE_ON_EXTRA_CALL",
	"REBALANCE_BLOCKED_BY_PRIORITY_QUEUES",
	"REBALANCE_BLOCKED_BY_TOO_MANY_FAILS",
	"REBALANCE_DONE",
	"REBALANCE_NOT_DONE_OR_NOT_NEEDED",
	"???"
};

static const char* job_exit_reason_to_str(uint8_t reason) {
	if (reason<JOB_EXIT_REASONS) {
		return job_exit_reason_str[reason];
	} else {
		return job_exit_reason_str[JOB_EXIT_REASONS];
	}
}

static uint32_t job_exit_reasons_last[MAXSCLASS][JOB_EXIT_REASONS];
static uint32_t job_exit_reasons[MAXSCLASS][JOB_EXIT_REASONS];

static void chunk_job_exit_counters_shift(void) {
	uint32_t i,sc;

	for (sc=0 ; sc<MAXSCLASS ; sc++) {
		for (i=0 ; i<JOB_EXIT_REASONS ; i++) {
			job_exit_reasons_last[sc][i] = job_exit_reasons[sc][i];
			job_exit_reasons[sc][i] = 0;
		}
	}
}

#define DANGER_PRIORITIES 9

#define CHUNK_PRIORITY_HASHSIZE 0x1000000
#define CHUNK_PRIORITY_HASHMASK 0x0FFFFFF

#define CHUNK_PRIORITY_IOREADY 0
#define CHUNK_PRIORITY_ONECOPY_HIGHGOAL 1
#define CHUNK_PRIORITY_ONECOPY_ANY 2
#define CHUNK_PRIORITY_ONEREGCOPY_PLUSMFR 3
#define CHUNK_PRIORITY_MARKEDFORREMOVAL 4
#define CHUNK_PRIORITY_UNFINISHEDEC 5
#define CHUNK_PRIORITY_UNDERGOAL 6
#define CHUNK_PRIORITY_OVERGOAL 7
#define CHUNK_PRIORITY_WRONGLABELS 8

#define CHUNK_PRIORITY_NEEDSREPLICATION(p) ((p!=CHUNK_PRIORITY_OVERGOAL)?1:0)
#define CHUNK_PRIORITY_HIGH(p) ((p<=CHUNK_PRIORITY_ONECOPY_ANY)?1:0)

static uint32_t job_call_last[MAXSCLASS][DANGER_PRIORITIES+1];
static uint32_t job_call[MAXSCLASS][DANGER_PRIORITIES+1];
static uint32_t job_nocall_last[MAXSCLASS][DANGER_PRIORITIES+1];
static uint32_t job_nocall[MAXSCLASS][DANGER_PRIORITIES+1];


static void chunk_job_call_counters_shift(void) {
	uint32_t i,sc;
	for (sc=0 ; sc<MAXSCLASS ; sc++) {
		for (i=0 ; i<=DANGER_PRIORITIES ; i++) {
			job_call_last[sc][i] = job_call[sc][i];
			job_call[sc][i] = 0;
			job_nocall_last[sc][i] = job_nocall[sc][i];
			job_nocall[sc][i] = 0;
		}
	}
}


// WRITE/TRUNCATE:
//
// write end --------------------------+
// chunks finished op ----+            |
// start_op ---------+    |            |
//                   v    v            v
// operation:   -----XXXXXX------------------
// lockedto:    -----LLLLLLLLLLLLLLLLLLL-----
//
// REPLICATE:
//
// rep end ---------------+
// rep start --------+    |
//                   v    v
// operation:   -----XXXXXX------------------
// lockedto:    -----------------------------
// replock:     -----LLLLLL------------------

#define REPLOCKHASHSIZE 0x10000
#define REPLOCKHASHMASK 0xFFFF
#define REPLOCKTIMEOUT 120

typedef struct _replock {
	uint64_t chunkid;
	uint32_t lockedto;
	struct _replock *next;
} replock;

static replock* replock_hash[REPLOCKHASHSIZE];

CREATE_BUCKET_ALLOCATOR(replock,replock,10000000/sizeof(replock))

static inline void chunk_replock_repstart(uint64_t chunkid,uint32_t now) {
	replock *rl;

	for (rl = replock_hash[chunkid & REPLOCKHASHMASK] ; rl!=NULL ; rl=rl->next) {
		if (rl->chunkid==chunkid) {
//			mfs_dbg_bt("repeated repstart for chunk:%016"PRIX64,chunkid);
			rl->lockedto = now + REPLOCKTIMEOUT;
			return;
		}
	}

	rl = replock_malloc();
	rl->chunkid = chunkid;
	rl->lockedto = now + REPLOCKTIMEOUT;
	rl->next = replock_hash[chunkid & REPLOCKHASHMASK];
	replock_hash[chunkid & REPLOCKHASHMASK] = rl;
//	mfs_dbg("repstart for chunk:%016"PRIX64,chunkid);
}

static inline void chunk_replock_repend(uint64_t chunkid) {
	replock *rl,**rlp;

	rlp = replock_hash + (chunkid & REPLOCKHASHMASK);

	while ((rl=*rlp)!=NULL) {
		if (rl->chunkid==chunkid) {
			*rlp = rl->next;
			replock_free(rl);
//			mfs_dbg("repend for chunk:%016"PRIX64,chunkid);
			return;
		}
		rlp = &(rl->next);
	}
//	mfs_dbg("unsuccessful repend for chunk:%016"PRIX64,chunkid);
}

static inline uint8_t chunk_replock_test(uint64_t chunkid,uint32_t now) {
	replock *rl;

	for (rl = replock_hash[chunkid & REPLOCKHASHMASK] ; rl!=NULL ; rl=rl->next) {
		if (rl->chunkid==chunkid && rl->lockedto >= now) {
			return 1;
		}
	}
	return 0;
}

static inline void chunk_replock_init(void) {
	uint32_t hash;
	for (hash=0 ; hash<REPLOCKHASHSIZE ; hash++) {
		replock_hash[hash]=NULL;
	}
}

static inline void chunk_replock_cleanall(void) {
	replock_free_all();
	chunk_replock_init();
}


typedef struct _discserv {
	uint16_t csid;
	struct _discserv *next;
} discserv;

static discserv *discservers = NULL;
static discserv *discservers_next = NULL;

/* flists */

#define FLISTNULLINDX 0
#define FLISTONEFILEINDX 1
#define FLISTFIRSTINDX 5

#define FLISTMAXFCOUNT 0xFFFFFF

typedef struct _flist {
	unsigned fcount:24;
	uint8_t sclassid;
	uint32_t nexti;
} flist;

static flist **flistmem = NULL;
static uint32_t flistfirstfree = FLISTFIRSTINDX;
static uint32_t flistfreehead = FLISTNULLINDX;

static inline void flist_init(void) {
	uint32_t i;
	if (flistmem==NULL) {
		flistmem = malloc(65536 * sizeof(flist*));
		for (i=0 ; i<65536 ; i++) {
			flistmem[i]=NULL;
		}
	}
	flistfirstfree = FLISTFIRSTINDX;
	flistfreehead = FLISTNULLINDX;
}

static inline void flist_cleanup(uint8_t full) {
	uint32_t i;
	for (i=0 ; i<65536 ; i++) {
		if (flistmem[i]!=NULL) {
			free(flistmem[i]);
			flistmem[i]=NULL;
		}
	}
	flistfirstfree = FLISTFIRSTINDX;
	flistfreehead = FLISTNULLINDX;
	if (full) {
		free(flistmem);
		flistmem = NULL;
	}
}

static inline flist* flist_get(uint32_t indx) {
	return flistmem[indx>>16] + (indx&0xFFFF);
}

static inline uint32_t flist_alloc(void) {
	uint32_t indx;
	flist *fl;
	if (flistfreehead==FLISTNULLINDX) {
		indx = flistfirstfree;
		flistfirstfree++;
		if (flistmem[indx>>16]==NULL) {
			flistmem[indx>>16] = malloc(65536 * sizeof(flist));
		}
	} else {
		indx = flistfreehead;
		fl = flist_get(indx);
		flistfreehead = fl->nexti;
	}
	return indx;
}

static inline void flist_free(uint32_t indx) {
	flist *fl;
	fl = flist_get(indx);
	fl->nexti = flistfreehead;
	flistfreehead = indx;
}

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
	uint8_t ecid;
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

#define FLAG_ARCH 1
#define FLAG_TRASH 2
#define FLAG_MASK 3

#define STORAGE_MODE_COPIES 0
#define STORAGE_MODE_EC8 1
#define STORAGE_MODE_EC4 2

typedef struct chunk {
	uint64_t chunkid;
	unsigned needverincrease:1;
	unsigned allowreadzeros:1;
	unsigned version:30;
	uint8_t sclassid;
	unsigned storage_mode:4;
	unsigned all_gequiv:4;
	unsigned reg_gequiv:4;
	unsigned unused:4;
//	unsigned allvalidcopies:4;
//	unsigned regularvalidcopies:4;
//	unsigned allecgequiv:4;
//	unsigned regularecgequiv:4;
	unsigned ondangerlist:1;
	unsigned interrupted:1;
	unsigned writeinprogress:1;
	unsigned flags:2;
	unsigned operation:3;
	uint32_t fhead; // this is "fcount" when the number is lower than FLISTFIRSTINDX !!!
	uint32_t lockedto;
	slist *slisthead;
	struct chunk *next;
} chunk;

// 8 * 6 (48)

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
	uint32_t next;
	uint32_t prev;
} csdata;

static csdata *cstab = NULL;
static uint32_t csfreehead = MAXCSCOUNT;
static uint32_t csfreetail = MAXCSCOUNT;
static uint32_t csusedhead = MAXCSCOUNT;
static uint32_t opsinprogress = 0;
static uint16_t csregisterinprogress = 0;




typedef struct _chq_element {
	chunk *c;
	uint8_t priority;
	struct _chq_element *hnext,**hprev,*qnext,**qprev;
} chq_element;

static chq_element** chq_hash;
static chq_element* chq_queue_head[DANGER_PRIORITIES];
static chq_element** chq_queue_tail[DANGER_PRIORITIES];
static uint32_t chq_queue_elements[DANGER_PRIORITIES];
static uint32_t chq_elements;

static uint32_t chq_queue_last_append_count[DANGER_PRIORITIES];
static uint32_t chq_queue_current_append_count[DANGER_PRIORITIES];

static uint32_t chq_queue_last_pop_count[DANGER_PRIORITIES];
static uint32_t chq_queue_current_pop_count[DANGER_PRIORITIES];

static uint32_t chq_queue_last_remove_count[DANGER_PRIORITIES];
static uint32_t chq_queue_current_remove_count[DANGER_PRIORITIES];

static const char* pristr[] = {
	"IOREADY",
	"ONECOPY_HIGHGOAL",
	"ONECOPY_ANY",
	"ONEREGCOPY_PLUSMFR",
	"MARKEDFORREMOVAL",
	"UNFINISHEDEC",
	"UNDERGOAL",
	"OVERGOAL",
	"WRONGLABELS"
};

typedef struct _io_ready_chunk {
	chunk *c;
	struct _io_ready_chunk *next;
} io_ready_chunk;

static io_ready_chunk* io_ready_chunk_hash[256];

static uint32_t JobsTimerMiliSeconds;
static uint32_t TicksPerSecond;
static uint32_t MaxFailsPerClass;
static uint32_t FailClassCounterResetCalls;
static uint32_t MaxRebalanceFails;
static uint32_t FailRebalanceCounterResetCalls;

static uint16_t rebalance_fails[256];

static void* jobs_timer;

static uint32_t ReplicationsDelayInit=60;
static uint32_t DangerMinLeng=10000;
static uint32_t DangerMaxLeng=1000000;

static double MaxWriteRepl[5];
static double MaxReadRepl[5];
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

typedef struct _loop_info {
	uint32_t fixed;
	uint32_t forcekeep;
	uint32_t delete_invalid;
	uint32_t delete_no_longer_needed;
	uint32_t delete_wrong_version;
	uint32_t delete_duplicated_ecpart;
	uint32_t delete_excess_ecpart;
	uint32_t delete_excess_copy;
	uint32_t delete_diskclean_ecpart;
	uint32_t delete_diskclean_copy;
	uint32_t replicate_dupserver_ecpart;
	uint32_t replicate_needed_ecpart;
	uint32_t replicate_needed_copy;
	uint32_t replicate_wronglabels_ecpart;
	uint32_t replicate_wronglabels_copy;
	uint32_t split_copy_into_ecparts;
	uint32_t join_ecparts_into_copy;
	uint32_t recover_ecpart;
	uint32_t calculate_ecchksum;
	uint32_t locked_unused;
	uint32_t locked_used;
	uint32_t replicate_rebalance;
} loop_info;

static loop_info chunksinfo = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
static uint32_t chunksinfo_loopstart=0,chunksinfo_loopend=0;

static uint64_t lastchunkid=0;
static chunk* lastchunkptr=NULL;

static uint32_t chunks;

static uint32_t last_rebalance=0;

//static uint64_t **allchunkcounts;
//static uint64_t **regularchunkcounts;

static uint64_t **allchunkcopycounts;
static uint64_t **regchunkcopycounts;
static uint64_t **allchunkec8counts;
static uint64_t **regchunkec8counts;
static uint64_t **allchunkec4counts;
static uint64_t **regchunkec4counts;


static uint32_t stats_chunkops[CHUNK_STATS_CNT];

/* modified Hopcroft-Karp max-matching algorithm */

int32_t* do_advanced_match(const storagemode *sm,uint32_t servcnt,const uint16_t *servers) {
	uint32_t i,l,x,v,sid,sids,gr;
	int32_t t;
	static int32_t *imatching = NULL;
	static int32_t *matching = NULL;
	static int32_t *augment = NULL;
	static uint8_t *visited = NULL;
	static int32_t *queue = NULL;
	static int32_t *group = NULL;
	static int32_t *grnode = NULL;
	static uint32_t *sidval = NULL;
	static int32_t *sidpos = NULL;
	static uint32_t tablength = 0;
	static uint32_t stablength = 0;

#ifdef MATCH_RIGHT
	uint32_t sp;
// use stack
#define QUEUE_INSERT(x) queue[sp++] = (x)
#define QUEUE_NEXT(x) (x) = queue[--sp]
#define QUEUE_INIT sp = 0
#define QUEUE_NOTEMPTY sp!=0

#else
	uint32_t qfu,qff;
// use fifo
#define QUEUE_INSERT(x) queue[qff++] = (x); qff = qff % tablength;
#define QUEUE_NEXT(x) (x) = queue[qfu++]; qfu = qfu % tablength
#define QUEUE_INIT qfu = qff = 0
#define QUEUE_NOTEMPTY (qfu!=qff)

#endif

#ifdef __clang_analyzer__
	if (sm->labelscnt + servcnt > tablength || imatching==NULL || matching==NULL || augment==NULL || visited==NULL || queue==NULL) {
#else
	if (sm->labelscnt + servcnt > tablength) {
#endif
		tablength = 100 + 2 * (sm->labelscnt + servcnt);
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
		if (queue) {
			free(queue);
		}
		imatching = malloc(sizeof(int32_t)*tablength);
		passert(imatching);
		matching = malloc(sizeof(int32_t)*tablength);
		passert(matching);
		augment = malloc(sizeof(int32_t)*tablength);
		passert(augment);
		visited = malloc(sizeof(uint8_t)*tablength);
		passert(visited);
		queue = malloc(sizeof(int32_t)*tablength);
		passert(queue);
	}
#ifdef __clang_analyzer__
	if (servcnt > stablength || sidval==NULL || sidpos==NULL || grnode==NULL || group==NULL) {
#else
	if (servcnt > stablength) {
#endif
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

	for (i=0 ; i<servcnt+sm->labelscnt ; i++) {
		imatching[i] = -1;
		matching[i] = -1;
		augment[i] = -1;
	}

	if (servcnt==0 || sm->labelscnt==0) {
		return matching;
	}

	// this algorithm is way too coplicated for clang analyzer - so many stupid false scenarios
#ifndef __clang_analyzer__

	// calc groups
	sids = 0;
	for (i=0 ; i<servcnt ; i++) {
		if (DoNotUseSameIP) {
			sid = matocsserv_server_get_ip(cstab[servers[i]].ptr);
		} else if (DoNotUseSameRack) {
			sid = topology_get_rackid(matocsserv_server_get_ip(cstab[servers[i]].ptr));
		} else if (sm->uniqmask & UNIQ_MASK_IP) {
			sid = matocsserv_server_get_ip(cstab[servers[i]].ptr);
		} else if (sm->uniqmask & UNIQ_MASK_RACK) {
			sid = topology_get_rackid(matocsserv_server_get_ip(cstab[servers[i]].ptr));
		} else {
			sid = matocsserv_server_get_labelmask(cstab[servers[i]].ptr) & sm->uniqmask;
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

	for (l=0 ; l<sm->labelscnt ; l++) {
		if (imatching[l]==-1) {
			for (i=0 ; i<servcnt+sm->labelscnt ; i++) {
				visited[i] = 0;
			}
			visited[l] = 1;
			augment[l] = -1;
			QUEUE_INIT;
			QUEUE_INSERT(l); // q.push(l)
			while (QUEUE_NOTEMPTY) { // queue not empty
				QUEUE_NEXT(x); // x = q.pop()
				if (x<sm->labelscnt) { // L to R - find alternative path (push all not visited and matching elements)
					for (v=0 ; v<servcnt ; v++) {
						gr = group[v];
						if (visited[sm->labelscnt+gr]==0) {
							if (matocsserv_server_matches_labelexpr(cstab[servers[v]].ptr,sm->labelexpr[x])) {
								visited[sm->labelscnt+gr] = 1;
								augment[sm->labelscnt+gr] = x;
								grnode[gr] = v;
								QUEUE_INSERT(sm->labelscnt+gr); // q.push
							}
						}
					}
				} else if (imatching[x] >= 0) { // R to L - use exisitng connection
					augment[imatching[x]] = x;
					visited[imatching[x]] = 1;
					QUEUE_INSERT(imatching[x]); // q.push
				} else {
					while (augment[x]>=0) { // R to L - found not connected element - create connections from augmented path
						if (x>=sm->labelscnt) {
							imatching[x] = augment[x];
//							imatching[augment[x]] = x;
							matching[augment[x]] = grnode[x-sm->labelscnt]+sm->labelscnt;
						}
						x = augment[x];
					}
					break;
				}
			}
		}
	}

	// add reverse matching
	for (i=0 ; i<sm->labelscnt ; i++) {
		t = matching[i];
		if (t>=0) {
			matching[t] = i;
		}
	}
#endif
	return matching;
#undef QUEUE_INSERT
#undef QUEUE_NEXT
#undef QUEUE_INIT
#undef QUEUE_NOTEMPTY
}

uint16_t do_extend_match(const storagemode *sm,uint32_t servcnt,int32_t *matching) {
	uint32_t i,j;
	uint16_t goodlabels;

	goodlabels = 0;
	for (i=0 ; i<sm->labelscnt ; i++) {
		if (matching[i]<0) {
			for (j=0 ; j<servcnt ; j++) {
#ifdef MATCH_RIGHT
				if (matching[sm->labelscnt+servcnt-j-1]<0) {
					matching[i] = sm->labelscnt+servcnt-j-1;
					matching[sm->labelscnt+servcnt-j-1] = i;
					break;
				}
#else
				if (matching[sm->labelscnt+j]<0) {
					matching[i] = sm->labelscnt+j;
					matching[sm->labelscnt+j] = i;
					break;
				}
#endif
			}
		} else {
			goodlabels++;
		}
		if (matching[i]<0) { // no more servers
			break;
		}
	}
	return goodlabels;
}

void chunk_stats(uint32_t chunkops[CHUNK_STATS_CNT]) {
	uint32_t i;
	for (i=0 ; i<CHUNK_STATS_CNT ; i++) {
		chunkops[i] = stats_chunkops[i];
		stats_chunkops[i] = 0;
	}
}

CREATE_BUCKET_ALLOCATOR(slist,slist,10000000/sizeof(slist))

CREATE_BUCKET_ALLOCATOR(chunk,chunk,10000000/sizeof(chunk))

CREATE_BUCKET_ALLOCATOR(chunk_queue,chq_element,10000000/sizeof(chq_element))

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
	allchunkcopycounts[0][0]++;
	regchunkcopycounts[0][0]++;
	newchunk->chunkid = chunkid;
	newchunk->version = 0;
	newchunk->sclassid = 0;
	newchunk->lockedto = 0;
	newchunk->storage_mode = STORAGE_MODE_COPIES;
	newchunk->all_gequiv = 0;
	newchunk->reg_gequiv = 0;
	newchunk->needverincrease = 1;
	newchunk->allowreadzeros = 0;
	newchunk->ondangerlist = 0;
	newchunk->interrupted = 0;
	newchunk->writeinprogress = 0;
	newchunk->flags = 0;
	newchunk->operation = NONE;
	newchunk->slisthead = NULL;
	newchunk->fhead = FLISTNULLINDX;
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
	indx = (uint32_t)(c->sclassid) + MAXSCLASS * (c->flags & FLAG_MASK);
	if (lastchunkptr==c) {
		lastchunkid=0;
		lastchunkptr=NULL;
	}
	chunks--;
	if (c->storage_mode==STORAGE_MODE_COPIES) {
		allchunkcopycounts[indx][0]--;
		regchunkcopycounts[indx][0]--;
	} else if (c->storage_mode==STORAGE_MODE_EC8) {
		allchunkec8counts[indx][0]--;
		regchunkec8counts[indx][0]--;
	} else if (c->storage_mode==STORAGE_MODE_EC4) {
		allchunkec4counts[indx][0]--;
		regchunkec4counts[indx][0]--;
	}
	chunk_hash_delete(c);
	chunk_free(c);
}

static const char* chunk_ecid_to_str(uint8_t ecid) {
	const char* ecid8names[17] = {
		"DE0","DE1","DE2","DE3","DE4","DE5","DE6","DE7",
		"CE0","CE1","CE2","CE3","CE4","CE5","CE6","CE7","CE8"
	};
	const char* ecid4names[13] = {
		"DF0","DF1","DF2","DF3",
		"CF0","CF1","CF2","CF3","CF4","CF5","CF6","CF7","CF8"
	};

	if (ecid&0x20) {
		if ((ecid&0x1F)<17) {
			return ecid8names[ecid&0x1F];
		}
	} else if (ecid&0x10) {
		if ((ecid&0xF)<13) {
			return ecid4names[ecid&0xF];
		}
	} else if (ecid==0) {
		return "COPY";
	}
	return "\?\?\?";
}

static inline int chunk_check_ecid(uint8_t ecid) {
	if (ecid==0 || (ecid>=0x20 && ecid<=0x30) || (ecid>=0x10 && ecid<=0x1C)) { // accepts all checksums (also from pro version) to be deleted in case of downgrade from pro to ce
		return 0;
	}
	return -1;
}

static inline void chunk_state_change(uint8_t oldsclassid,uint8_t newsclassid,uint8_t oldflags,uint8_t newflags,uint8_t old_storage_mode,uint8_t new_storage_mode,uint8_t old_all_ge,uint8_t new_all_ge,uint8_t old_reg_ge,uint8_t new_reg_ge) {
	uint32_t oldindx,newindx;
	oldindx = (uint32_t)oldsclassid + MAXSCLASS * (oldflags & FLAG_MASK);
	newindx = (uint32_t)newsclassid + MAXSCLASS * (newflags & FLAG_MASK);
	if (old_all_ge>9) {
		old_all_ge=10;
	}
	if (new_all_ge>9) {
		new_all_ge=10;
	}
	if (old_reg_ge>9) {
		old_reg_ge=10;
	}
	if (new_reg_ge>9) {
		new_reg_ge=10;
	}
	if (old_storage_mode==STORAGE_MODE_COPIES) {
		allchunkcopycounts[oldindx][old_all_ge]--;
		regchunkcopycounts[oldindx][old_reg_ge]--;
	} else if (old_storage_mode==STORAGE_MODE_EC8) {
		allchunkec8counts[oldindx][old_all_ge]--;
		regchunkec8counts[oldindx][old_reg_ge]--;
	} else if (old_storage_mode==STORAGE_MODE_EC4) {
		allchunkec4counts[oldindx][old_all_ge]--;
		regchunkec4counts[oldindx][old_reg_ge]--;
	}
	if (new_storage_mode==STORAGE_MODE_COPIES) {
		allchunkcopycounts[newindx][new_all_ge]++;
		regchunkcopycounts[newindx][new_reg_ge]++;
	} else if (new_storage_mode==STORAGE_MODE_EC8) {
		allchunkec8counts[newindx][new_all_ge]++;
		regchunkec8counts[newindx][new_reg_ge]++;
	} else if (new_storage_mode==STORAGE_MODE_EC4) {
		allchunkec4counts[newindx][new_all_ge]++;
		regchunkec4counts[newindx][new_reg_ge]++;
	}
}

static inline void chunk_state_set_counters(chunk *c,uint8_t storage_mode,uint8_t all_ge,uint8_t reg_ge) {
	chunk_state_change(c->sclassid,c->sclassid,c->flags,c->flags,c->storage_mode,storage_mode,c->all_gequiv,all_ge,c->reg_gequiv,reg_ge);
	c->storage_mode = storage_mode;
	c->all_gequiv = all_ge;
	c->reg_gequiv = reg_ge;
}

static inline void chunk_state_set_flags(chunk *c,uint8_t new_flags) {
	chunk_state_change(c->sclassid,c->sclassid,c->flags,new_flags,c->storage_mode,c->storage_mode,c->all_gequiv,c->all_gequiv,c->reg_gequiv,c->reg_gequiv);
	c->flags = new_flags;
}

static inline void chunk_state_set_sclass(chunk *c,uint8_t new_sclassid) {
	chunk_state_change(c->sclassid,new_sclassid,c->flags,c->flags,c->storage_mode,c->storage_mode,c->all_gequiv,c->all_gequiv,c->reg_gequiv,c->reg_gequiv);
	c->sclassid = new_sclassid;
}

static inline uint8_t chunk_calc_ecge(uint32_t mask8,uint32_t mask4,uint8_t ec8uniqserv,uint8_t ec4uniqserv,uint8_t *smode) {
	uint32_t ge,bcnt;
	ge = 0;
	*smode = STORAGE_MODE_COPIES;
	if (mask8) {
		bcnt = bitcount(mask8);
		if (ec8uniqserv<bcnt) {
			bcnt = ec8uniqserv;
		}
		if (bcnt>7) {
			ge = (bcnt-7);
			*smode = STORAGE_MODE_EC8;
		}
	}
	if (mask4) {
		bcnt = bitcount(mask4);
		if (ec4uniqserv<bcnt) {
			bcnt = ec4uniqserv;
		}
		if (bcnt>3 && (bcnt-3)>ge) {
			ge = (bcnt-3);
			*smode = STORAGE_MODE_EC4;
		}
	}
	return ge;
}

static inline void chunk_state_fix(chunk *c) {
	slist *s;
	uint32_t allmask8,allmask4;
	uint32_t regmask8,regmask4;
	uint8_t regc,allc,regecge,allecge;
	int32_t lregec4csid,lregec8csid;
	int32_t lallec4csid,lallec8csid;
	uint8_t regec4uniqserv;
	uint8_t regec8uniqserv;
	uint8_t allec4uniqserv;
	uint8_t allec8uniqserv;
	uint8_t storage_mode;

	allmask8 = 0;
	allmask4 = 0;
	regmask8 = 0;
	regmask4 = 0;
	regc = 0;
	allc = 0;
	lregec4csid = -1;
	lregec8csid = -1;
	lallec4csid = -1;
	lallec8csid = -1;
	regec4uniqserv = 0;
	regec8uniqserv = 0;
	allec4uniqserv = 0;
	allec8uniqserv = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==VALID || s->valid==TDVALID || s->valid==BUSY || s->valid==TDBUSY) {
			if (s->ecid==0) {
				allc++;
			} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
				allmask4 |= (UINT32_C(1) << (s->ecid & 0x0F));
				if (s->csid!=lallec4csid) {
					allec4uniqserv++;
				}
				lallec4csid = s->csid;
			} else if (s->ecid>=0x20 && s->ecid<=0x30) {
				allmask8 |= (UINT32_C(1) << (s->ecid & 0x1F));
				if (s->csid!=lallec8csid) {
					allec8uniqserv++;
				}
				lallec8csid = s->csid;
			}
			if (s->valid==VALID || s->valid==BUSY) {
				if (s->ecid==0) {
					regc++;
				} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
					regmask4 |= (UINT32_C(1) << (s->ecid & 0x0F));
					if (s->csid!=lregec4csid) {
						regec4uniqserv++;
					}
					lregec4csid = s->csid;
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					regmask8 |= (UINT32_C(1) << (s->ecid & 0x1F));
					if (s->csid!=lregec8csid) {
						regec8uniqserv++;
					}
					lregec8csid = s->csid;
				}
			}
		}
	}
	regecge = chunk_calc_ecge(regmask8,regmask4,regec8uniqserv,regec4uniqserv,&storage_mode);
	allecge = chunk_calc_ecge(allmask8,allmask4,allec8uniqserv,allec4uniqserv,&storage_mode);
	if (allc>=allecge) {
		storage_mode = STORAGE_MODE_COPIES;
	}
	if (storage_mode==STORAGE_MODE_COPIES) {
		allecge = allc;
		regecge = regc;
	}
	if (allecge>15) {
		allecge = 15;
	}
	if (regecge>15) {
		regecge = 15;
	}
	if (storage_mode != c->storage_mode || allecge != c->all_gequiv || regecge != c->reg_gequiv) {
		chunk_state_set_counters(c,storage_mode,allecge,regecge);
	}
}

static inline void chunk_set_op(chunk *c,uint8_t op) {
	c->operation = op;
}

uint32_t chunk_count(void) {
	return chunks;
}

// counters order: UNDER_COPY,UNDER_EC,EXACT_COPY,EXACT_EC,OVER_COPY,OVER_EC
void chunk_sclass_inc_counters(uint8_t sclassid,uint8_t flags,uint8_t gequiv,uint64_t counters[6]) {
	uint16_t indx = (uint16_t)sclassid + MAXSCLASS * (uint16_t)(flags & FLAG_MASK);
	uint32_t i;

	for (i=0 ; i<11 ; i++) {
		if (i<gequiv) {
			counters[0] += regchunkcopycounts[indx][i];
			counters[1] += regchunkec8counts[indx][i] + regchunkec4counts[indx][i];
		} else if (i>gequiv) {
			counters[4] += regchunkcopycounts[indx][i];
			counters[5] += regchunkec8counts[indx][i] + regchunkec4counts[indx][i];
		} else {
			counters[2] += regchunkcopycounts[indx][i];
			counters[3] += regchunkec8counts[indx][i] + regchunkec4counts[indx][i];
		}
	}
}

uint8_t chunk_sclass_has_chunks(uint8_t sclassid) {
	uint16_t indx;
	uint32_t i;
	uint8_t gr;

	for (gr=0 ; gr<4 ; gr++) {
		indx = (uint16_t)sclassid + MAXSCLASS * (uint16_t)gr;
		for (i=0 ; i<11 ; i++) {
			if (allchunkcopycounts[indx][i]|allchunkec8counts[indx][i]|allchunkec4counts[indx][i]) {
				return 1;
			}
		}
	}
	return 0;
}

void chunk_info(uint32_t *allchunks,uint32_t *copychunks,uint32_t *ec8chunks,uint32_t *ec4chunks,uint64_t *copies,uint64_t *ec8parts,uint64_t *ec4parts,uint64_t *hypotheticalcopies) {
	uint32_t i,j,avc,aec8,aec4;
	*allchunks = chunks;
	*copychunks = 0;
	*ec8chunks = 0;
	*ec4chunks = 0;
	*copies = 0;
	*ec8parts = 0;
	*ec4parts = 0;
	*hypotheticalcopies = 0;
	for (i=1 ; i<=10 ; i++) {
		avc = 0;
		aec8 = 0;
		aec4 = 0;
		for (j=0 ; j<MAXSCLASS*4 ; j++) {
			avc += allchunkcopycounts[j][i];
			aec8 += allchunkec8counts[j][i];
			aec4 += allchunkec4counts[j][i];
		}
		*copychunks += avc;
		*ec8chunks += aec8;
		*ec4chunks += aec4;
		*copies += (avc * i);
		*ec8parts += (aec8 * (i+7));
		*ec4parts += (aec4 * (i+3));
		*hypotheticalcopies += ((aec8+aec4+avc) * i);
	}
}

void chunk_chart_data(uint64_t *copychunks,uint64_t *ec8chunks,uint64_t *ec4chunks,uint64_t *regendangered,uint64_t *regundergoal,uint64_t *allendangered,uint64_t *allundergoal) {
	uint32_t i,j;
	uint8_t gequiv;
	uint8_t repdisabled;
	*copychunks = 0;
	*ec8chunks = 0;
	*ec4chunks = 0;
	*regendangered = 0;
	*regundergoal = 0;
	*allendangered = 0;
	*allundergoal = 0;
	for (i=0 ; i<MAXSCLASS*4 ; i++) {
		gequiv = sclass_calc_goal_equivalent(sclass_get_keeparch_storagemode(i%MAXSCLASS,i/MAXSCLASS));
		for (j=1 ; j<=10 ; j++) {
			*copychunks += allchunkcopycounts[i][j];
			*ec8chunks += allchunkec8counts[i][j];
			*ec4chunks += allchunkec4counts[i][j];
			if (j<gequiv) {
				if (j==1) {
					*regendangered += regchunkcopycounts[i][j] + regchunkec8counts[i][j] + regchunkec4counts[i][j];
					*allendangered += allchunkcopycounts[i][j] + allchunkec8counts[i][j] + allchunkec4counts[i][j];
				} else {
					*regundergoal += regchunkcopycounts[i][j] + regchunkec8counts[i][j] + regchunkec4counts[i][j];
					*allundergoal += allchunkcopycounts[i][j] + allchunkec8counts[i][j] + allchunkec4counts[i][j];
				}
			}
		}
	}
	repdisabled = 0;
	if (matocsserv_servers_count()==0 || chunkrehashpos==0) {
		repdisabled = 1;
	}
	if (chunk_counters_in_progress()) {
		repdisabled = 1;
	}
	if (main_start_time()+ReplicationsDelayInit>main_time()) {
		repdisabled = 1;
	}
	if (repdisabled) {
		*regendangered = (uint64_t)(0xFFFFFFFFFFFFFFFF);
		*regundergoal = (uint64_t)(0xFFFFFFFFFFFFFFFF);
		*allendangered = (uint64_t)(0xFFFFFFFFFFFFFFFF);
		*allundergoal = (uint64_t)(0xFFFFFFFFFFFFFFFF);
	}
}

uint8_t chunk_counters_in_progress(void) {
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"discservers: %p , discservers_next: %p , csregisterinprogress: %"PRIu16,discservers,discservers_next,csregisterinprogress);
	return ((discservers!=NULL || discservers_next!=NULL)?CHUNKSERVERS_DISCONNECTING:0)|((csregisterinprogress>0)?CHUNKSERVERS_CONNECTING:0)|(matocsserv_receiving_chunks_state()&(TRANSFERING_LOST_CHUNKS|TRANSFERING_NEW_CHUNKS));
}

void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid,int16_t classid) {
	uint32_t i,j;
	uint8_t gequiv;
	uint64_t counts[11][11];
	storagemode *sm;

	for (i=0 ; i<=10 ; i++) {
		for (j=0 ; j<=10 ; j++) {
			counts[i][j]=0;
		}
	}

	if (matrixid==0) {
		for (i=0 ; i<MAXSCLASS*4 ; i++) {
			if (classid<0 || classid>255 || ((uint8_t)classid==(i%MAXSCLASS))) {
				gequiv = sclass_calc_goal_equivalent(sclass_get_keeparch_storagemode(i%MAXSCLASS,i/MAXSCLASS));
				if (gequiv>10) {
					gequiv=10;
				}
				for (j=0 ; j<=10 ; j++) {
					counts[gequiv][j] += allchunkcopycounts[i][j] + allchunkec8counts[i][j] + allchunkec4counts[i][j];
				}
			}
		}
	} else if (matrixid==1) {
		for (i=0 ; i<MAXSCLASS*4 ; i++) {
			if (classid<0 || classid>255 || ((uint8_t)classid==(i%MAXSCLASS))) {
				gequiv = sclass_calc_goal_equivalent(sclass_get_keeparch_storagemode(i%MAXSCLASS,i/MAXSCLASS));
				if (gequiv>10) {
					gequiv=10;
				}
				for (j=0 ; j<=10 ; j++) {
					counts[gequiv][j] += regchunkcopycounts[i][j] + regchunkec8counts[i][j] + regchunkec4counts[i][j];
				}
			}
		}
	} else if (matrixid>=2 && matrixid<=7) {
		uint64_t **srccounters;
		switch (matrixid) {
			case 2:
				srccounters = allchunkcopycounts;
				break;
			case 3:
				srccounters = regchunkcopycounts;
				break;
			case 4:
				srccounters = allchunkec8counts;
				break;
			case 5:
				srccounters = regchunkec8counts;
				break;
			case 6:
				srccounters = allchunkec4counts;
				break;
			default: // 7
				srccounters = regchunkec4counts;
				break;
		}
		for (i=0 ; i<MAXSCLASS*4 ; i++) {
			if (classid<0 || classid>255 || ((uint8_t)classid==(i%MAXSCLASS))) {
				sm = sclass_get_keeparch_storagemode(i%MAXSCLASS,i/MAXSCLASS);
				if (matrixid>=4) { // EC
					if (sm->ec_data_chksum_parts) {
						gequiv = (sm->ec_data_chksum_parts&0xF)+1;
					} else {
						gequiv = 0;
					}
				} else {
					if (sm->ec_data_chksum_parts) {
						gequiv = 0;
					} else{
						gequiv = sm->labelscnt;
					}
				}
				if (gequiv>10) {
					gequiv=10;
				}
				for (j=0 ; j<=10 ; j++) {
					counts[gequiv][j] += srccounters[i][j];
				}
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




static inline chunk* chunk_priority_next(uint8_t priority) {
	chq_element *ce;
	chunk *c;

	ce = chq_queue_head[priority];

	if (ce==NULL) {
		return NULL;
	}
	c = ce->c;
	chq_queue_head[priority] = ce->qnext;
	if (ce->qnext==NULL) {
		chq_queue_tail[priority] = chq_queue_head+priority;
	} else {
		ce->qnext->qprev = chq_queue_head+priority;
	}
	*(ce->hprev) = ce->hnext;
	if (ce->hnext!=NULL) {
		ce->hnext->hprev = ce->hprev;
	}
	chq_queue_elements[priority]--;
	chq_elements--;
	chq_queue_current_pop_count[priority]++;
	chunk_queue_free(ce);
	c->ondangerlist = 0;
	return c;
}

static inline void chunk_priority_enqueue(uint8_t priority,chunk *c) {
	chq_element *ce;
	uint32_t hash = c->chunkid & CHUNK_PRIORITY_HASHMASK;
	uint8_t i;

	if (priority>=DANGER_PRIORITIES) {
		return;
	}

	if (c->ondangerlist) {
		for (ce=chq_hash[hash] ; ce!=NULL ; ce=ce->hnext) {
			if (ce->c==c) {
				if (ce->priority>priority) {
					*(ce->qprev) = ce->qnext;
					if (ce->qnext==NULL) {
						chq_queue_tail[ce->priority] = ce->qprev;
					} else {
						ce->qnext->qprev = ce->qprev;
					}
					*(chq_queue_tail[priority]) = ce;
					ce->qnext = NULL;
					ce->qprev = chq_queue_tail[priority];
					chq_queue_tail[priority] = &(ce->qnext);
					chq_queue_elements[ce->priority]--;
					chq_queue_elements[priority]++;
					ce->priority = priority;
				}
				return;
			}
		}
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"internal structure error: chunk (0x%016"PRIX64") has 'ondanger' bit set but it is not on danger list",c->chunkid);
		c->ondangerlist = 0;
	}

	if (chq_elements >= DangerMaxLeng && chq_queue_elements[priority]>DangerMinLeng) {
		chunk *cx;
		cx = NULL;
		for (i=DANGER_PRIORITIES-1 ; i>priority && cx==NULL ; i--) {
			if (chq_queue_elements[i]>DangerMinLeng) {
				cx = chunk_priority_next(i);
			}
		}
		if (cx==NULL) {
			return;
		}
	}
	chq_elements++;
	chq_queue_elements[priority]++;
	chq_queue_current_append_count[priority]++;
	ce = chunk_queue_malloc();
	ce->c = c;
	ce->priority = priority;
	ce->hnext = chq_hash[hash];
	ce->hprev = chq_hash+hash;
	if (ce->hnext!=NULL) {
		ce->hnext->hprev = &(ce->hnext);
	}
	chq_hash[hash] = ce;
	*(chq_queue_tail[priority]) = ce;
	ce->qnext = NULL;
	ce->qprev = chq_queue_tail[priority];
	chq_queue_tail[priority] = &(ce->qnext);
	c->ondangerlist = 1;
}

static inline void chunk_priority_remove(chunk *c) {
	chq_element *ce;
	uint32_t hash = c->chunkid & CHUNK_PRIORITY_HASHMASK;

	for (ce=chq_hash[hash] ; ce!=NULL ; ce=ce->hnext) {
		if (ce->c==c) {
			*(ce->qprev) = ce->qnext;
			if (ce->qnext==NULL) {
				chq_queue_tail[ce->priority] = ce->qprev;
			} else {
				ce->qnext->qprev = ce->qprev;
			}
			*(ce->hprev) =ce->hnext;
			if (ce->hnext!=NULL) {
				ce->hnext->hprev = ce->hprev;
			}
			c->ondangerlist = 0;
			chq_queue_elements[ce->priority]--;
			chq_elements--;
			chq_queue_current_remove_count[ce->priority]++;
			chunk_queue_free(ce);
			return;
		}
	}
	return;
}

static inline uint8_t chunk_priority_not_empty(uint8_t priority) {
	if (priority<DANGER_PRIORITIES) {
		return (chq_queue_head[priority]!=NULL)?1:0;
	} else {
		return 0;
	}
}

static inline uint32_t chunk_priority_get_elements(uint8_t priority) {
	if (priority<DANGER_PRIORITIES) {
		return chq_queue_elements[priority];
	} else {
		return 0;
	}
}

static inline uint8_t chunk_priority_should_block(uint8_t priority) {
	if (priority==CHUNK_PRIORITY_UNFINISHEDEC || priority==CHUNK_PRIORITY_OVERGOAL) {
		return (chq_queue_elements[priority]>=1000)?1:0;
	} else {
		return chunk_priority_not_empty(priority);
	}
}

static inline uint8_t chunk_priority_is_empty(uint8_t priority) {
	return 1-chunk_priority_not_empty(priority);
}

static inline void chunk_priority_cleanall(void) {
	chq_element *ce;
	uint32_t i;

	for (i=0 ; i<DANGER_PRIORITIES ; i++) {
		chq_queue_head[i] = NULL;
		chq_queue_tail[i] = chq_queue_head+i;
		chq_queue_elements[i] = 0;
		chq_queue_last_append_count[i] = 0;
		chq_queue_current_append_count[i] = 0;
		chq_queue_last_pop_count[i] = 0;
		chq_queue_current_pop_count[i] = 0;
		chq_queue_last_remove_count[i] = 0;
		chq_queue_current_remove_count[i] = 0;
	}
	for (i=0 ; i<CHUNK_PRIORITY_HASHSIZE ; i++) {
		for (ce=chq_hash[i] ; ce!=NULL ; ce=ce->hnext) {
			ce->c->ondangerlist = 0;
		}
		chq_hash[i] = NULL;
	}
	chq_elements = 0;
	chunk_queue_free_all();
}

void chunk_queue_counters_shift(void) {
	uint32_t i;

	for (i=0 ; i<DANGER_PRIORITIES ; i++) {
		chq_queue_last_append_count[i] = chq_queue_current_append_count[i];
		chq_queue_current_append_count[i] = 0;
		chq_queue_last_pop_count[i] = chq_queue_current_pop_count[i];
		chq_queue_current_pop_count[i] = 0;
		chq_queue_last_remove_count[i] = chq_queue_current_remove_count[i];
		chq_queue_current_remove_count[i] = 0;
	}
}

static inline void chunk_new_copy(chunk *c,slist *s) {
	slist *si,**sp;
	sp = &(c->slisthead);
	while ((si=*sp)!=NULL) {
		if (si->csid<s->csid) {
			sp = &(si->next);
		} else if (si->csid==s->csid && si->ecid<s->ecid) {
			sp = &(si->next);
		} else {
			break;
		}
	}
	s->next = *sp;
	*sp = s;
//	s->next = c->slisthead;
//	c->slisthead = s;
}


static inline uint16_t* chunk_replallowed_servers(uint16_t *cnt,uint8_t mode) {
	static uint16_t *aservers = NULL;
	static uint16_t aservcnt;
	if (mode==1) {
		if (aservers==NULL) {
			aservers = malloc(sizeof(uint16_t)*MAXCSCOUNT);
			passert(aservers);
		}
		aservcnt = matocsserv_getservers_replallowed(aservers);
	} else if (mode==2) {
		if (aservers!=NULL) {
			free(aservers);
			aservers = NULL;
		}
	}
	if (cnt!=NULL) {
		*cnt = aservcnt;
	}
	return aservers;
}

static inline uint8_t chunk_get_labels_mode_for_ec(storagemode *sm,uint8_t sclassid) {
	uint8_t labels_mode;
	if (sm->has_labels && sm->labelscnt) {
		labels_mode = sclass_get_labels_mode(sclassid,sm);
		if (labels_mode!=LABELS_MODE_LOOSE) {
			if (sm->valid_ec_counters<sm->labelscnt) {
				matocsserv_recalculate_storagemode_scounts(sm);
			}
		}
	} else {
		labels_mode = LABELS_MODE_LOOSE;
	}
	return labels_mode;
}

// all cases and conditions:

// LOOSE + copy
//	RA < N+2X -> KEEP
// LOOSE + EC
//	VA < N+X -> KEEP
// DEFAULT + copy + 1 label
//	RLA < N+2X -> KEEP
// DEFAULT + copy + 2 labels
//	RLA < N+2X || RLD+RLB < N+X || RLC+RLB < 2X -> KEEP
// DEFAULT + EC + 1 label
//	VA < N+X -> KEEP
//	RLA >= N+X -> ec_strict_mode = 1
// DEFUALT + EC + 2 labels
//	VA < N+X -> KEEP
//	RLA >= N+X && RLD+RLB >= N && RLC+RLB >= X -> ec_strict_mode = 1
// STRICT + copy + 1 label
//	RLA < N+2X -> KEEP
//	ec_strict_mode = 1
// STRICT + copy + 2 labels
//	RLA < N+2X || RLD+RLB < N+X || RLC+RLB < 2X -> KEEP
//	ec_strict_mode = 1
// STRICT + EC + 1 label
//	VLA < N+X -> KEEP
//	ec_strict_mode = 1
// STRICT + EC + 2 labels
//	VLA < N+X || VLD+VLB < N || VLC+VLB < X -> KEEP
//	ec_strict_mode = 1

static inline uint8_t chunk_check_forcekeep_condidiotns_for_ec(storagemode *sm,uint8_t labels_mode,uint8_t has_copies,uint8_t has_ec4parts,uint8_t has_ec8parts,uint16_t replallowed,uint16_t allvalid) {
	uint8_t usekeep;
	uint8_t ec_strict_mode;
	uint8_t ec_data_parts,ec_chksum_parts;

	usekeep = 0;
	ec_strict_mode = 0;
	ec_data_parts = sm->ec_data_chksum_parts >> 4;
	ec_chksum_parts = sm->ec_data_chksum_parts & 0xF;

	if (ec_data_parts==8 && has_ec4parts) {
		usekeep = 1;
	} else if (ec_data_parts==4 && has_ec8parts) {
		usekeep = 1;
	} else if (ec_data_parts==8 || ec_data_parts==4) {
		switch (labels_mode) {
			case LABELS_MODE_LOOSE:
				if (has_copies) { // copy
					if (replallowed < ec_data_parts + 2*ec_chksum_parts) {
						usekeep = 2;
					}
				} else { // EC
					if (allvalid < ec_data_parts + ec_chksum_parts) {
						usekeep = 3;
					}
				}
				break;
			case LABELS_MODE_STD:
				if (has_copies) {
					if (sm->replallowed < ec_data_parts + 2*ec_chksum_parts) {
						usekeep = 2;
					}
					if (sm->labelscnt==2) {
						if (sm->data_replallowed + sm->both_replallowed < ec_data_parts + ec_chksum_parts) {
							usekeep = 2;
						}
						if (sm->chksum_replallowed + sm->both_replallowed < 2*ec_chksum_parts) {
							usekeep = 2;
						}
					}
				} else {
					if (allvalid < ec_data_parts + ec_chksum_parts) {
						usekeep = 3;
					}
					if (sm->labelscnt==1) {
						if (sm->replallowed >= ec_data_parts + ec_chksum_parts) {
							ec_strict_mode = 1;
						}
					} else { // sm->labelscnt==2 
						if (sm->replallowed >= ec_data_parts + ec_chksum_parts
								&& sm->data_replallowed + sm->both_replallowed >= ec_data_parts
								&& sm->chksum_replallowed + sm->both_replallowed >= ec_chksum_parts) {
							ec_strict_mode = 1;
						}
					}
				}
				break;
			case LABELS_MODE_STRICT:
				if (has_copies) {
					if (sm->replallowed < ec_data_parts + 2*ec_chksum_parts) {
						usekeep = 2;
					}
					if (sm->labelscnt==2) {
						if (sm->data_replallowed + sm->both_replallowed < ec_data_parts + ec_chksum_parts) {
							usekeep = 2;
						}
						if (sm->chksum_replallowed + sm->both_replallowed < 2*ec_chksum_parts) {
							usekeep = 2;
						}
					}
				} else {
					if (sm->allvalid < ec_data_parts + ec_chksum_parts) {
						usekeep = 3;
					}
					if (sm->labelscnt==2) {
						if (sm->data_allvalid + sm->both_allvalid < ec_data_parts) {
							usekeep = 3;
						}
						if (sm->chksum_allvalid + sm->both_allvalid < ec_chksum_parts) {
							usekeep = 3;
						}

					}
				}
				ec_strict_mode = 1;
				break;
		}
	} else {
		usekeep = 1;
	}
	return (ec_strict_mode<<4) + usekeep;
}

static inline uint8_t chunk_calculate_endanger_priority(chunk *c,uint8_t checklabels) {
	slist *s;
	uint8_t goal;
	uint8_t ec_data_parts;
	uint8_t ec_chksum_parts;
	uint8_t j;
	uint32_t m;
	static uint16_t *servers = NULL;
	uint16_t *aservers;
	uint16_t aservcnt;
	uint16_t scount;
	storagemode *sm;
	uint8_t labels_mode;
	uint32_t vcmask8,vcmask4;
	uint32_t ecmask8,ecmask4;
	uint32_t partmask;
	int32_t lcsid4,lcsid8;
	uint8_t extra_chunks;
	uint8_t wrong_repairable_labels;
	uint8_t redundant_ec_parts;
	uint8_t parts_on_the_same_server;
	uint8_t server_count_with_parts8,server_count_with_parts4;
	uint8_t hasvc,hasec8parts,hasec4parts;

	if (c==NULL) {
#ifndef __clang_analyzer__
		// construction too complicated for clang - silence false static analyzer warnings
		if (checklabels==0) {
			if (servers==NULL) {
				servers = malloc(sizeof(uint16_t)*MAXCSCOUNT);
				passert(servers);
			}
		} else {
			if (servers!=NULL) {
				free(servers);
				servers = NULL;
			}
		}
#endif
		return 0xFF;
	}

#ifdef __clang_analyzer__
	servers = malloc(sizeof(uint16_t)*MAXCSCOUNT);
#endif

	extra_chunks = 0;
	parts_on_the_same_server = 0;
	wrong_repairable_labels = 0;
	redundant_ec_parts = 0;
	server_count_with_parts8 = 0;
	server_count_with_parts4 = 0;

	scount = matocsserv_servers_count();
	aservers = chunk_replallowed_servers(&aservcnt,0);
	hasvc = 0;
	hasec4parts = 0;
	hasec8parts = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==VALID) {
			if (s->ecid&0x20) {
				hasec8parts = 1;
			} else if (s->ecid&0x10) {
				hasec4parts = 1;
			} else {
				hasvc = 1;
			}
		}
	}

	sm = sclass_get_keeparch_storagemode(c->sclassid,c->flags);
	if (sm->ec_data_chksum_parts) {
		uint8_t usekeep;
		labels_mode = chunk_get_labels_mode_for_ec(sm,c->sclassid);
		usekeep = chunk_check_forcekeep_condidiotns_for_ec(sm,labels_mode,hasvc,hasec4parts,hasec8parts,aservcnt,scount) & 0xF;
		if (usekeep) {
			sm = sclass_get_keeparch_storagemode(c->sclassid,0);
		}
	}
	if (sm->ec_data_chksum_parts) {
		ec_data_parts = sm->ec_data_chksum_parts >> 4;
		ec_chksum_parts = sm->ec_data_chksum_parts & 0xF;
		goal = ec_chksum_parts+1;
	} else {
		ec_data_parts = 0;
		ec_chksum_parts = 0;
		goal = sm->labelscnt;
	}
	// reduce goal to possible max
	if (ec_data_parts) {
		if (aservcnt<ec_data_parts) { // not enough servers for EC - stop
			return DANGER_PRIORITIES;
		}
		if (goal+(ec_data_parts-1)>aservcnt) {
//			printf("danger test - reducing redundancy level due to lack of servers (servcnt:%u ; goal:%u->%u)\n",aservcnt,goal,aservcnt-7);
			goal = aservcnt-(ec_data_parts-1);
		}
	} else {
		if (goal>aservcnt) {
			goal = aservcnt;
		}
	}

	vcmask8 = 0;
	vcmask4 = 0;
	ecmask8 = 0;
	ecmask4 = 0;
	lcsid4 = -1;
	lcsid8 = -1;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==VALID) {
			if ((s->ecid==0 && ec_data_parts) || ((s->ecid>=0x10 && s->ecid<=0x1C) && ec_data_parts!=4) || ((s->ecid>=0x20 && s->ecid<=0x30) && ec_data_parts!=8)) {
				extra_chunks = 1;
			}
			if (s->ecid!=0) {
				if (s->ecid>=0x10 && s->ecid<=0x1C) {
					partmask = UINT32_C(1) << (s->ecid & 0x0F);
					vcmask4 |= partmask;
					if (partmask & ecmask4) {
						redundant_ec_parts = 1;
					} else {
						if (lcsid4!=s->csid) {
							server_count_with_parts4++;
						}
					}
					ecmask4 |= partmask;
					if (lcsid4==s->csid) {
						parts_on_the_same_server = 1;
					}
					lcsid4 = s->csid;
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					partmask = UINT32_C(1) << (s->ecid & 0x1F);
					vcmask8 |= partmask;
					if (partmask & ecmask8) {
						redundant_ec_parts = 1;
					} else {
						if (lcsid8!=s->csid) {
							server_count_with_parts8++;
						}
					}
					ecmask8 |= partmask;
					if (lcsid8==s->csid) {
						parts_on_the_same_server = 1;
					}
					lcsid8 = s->csid;
				}
			}
		}
	}

	if (extra_chunks==0 && parts_on_the_same_server==0 && checklabels && c->reg_gequiv >= goal && (sm->has_labels || DoNotUseSameIP || DoNotUseSameRack)) {
		if (ec_data_parts) {
			uint8_t checkindex;
			uint8_t need_l0,need_l1;
			uint8_t minecid,maxecid,ecidmask,eciddata;

			if (ec_data_parts==8) {
				minecid = 0x20;
				maxecid = 0x30;
				ecidmask = 0x1F;
				eciddata = 8;
			} else { // ec_data_parts==4
				minecid = 0x10;
				maxecid = 0x1C;
				ecidmask = 0xF;
				eciddata = 4;
			}

			if (sm->labelscnt==1) {
				checkindex = 0;
			} else {
				checkindex = 1;
			}
			need_l0 = 0;
			need_l1 = 0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid) {
					if ((s->ecid & ecidmask) < eciddata) {
						if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[0])==0) {
							need_l0++;
						}
					} else {
						if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[checkindex])==0) {
							if (checkindex==0) {
								need_l0++;
							} else {
								need_l1++;
							}
						}
					}
				}
			}
			if (need_l0 | need_l1) { // '|' on purpose - faster than '||'
				for (m=0 ; m<aservcnt && wrong_repairable_labels==0 ; m++) {
					for (s=c->slisthead ; s ; s=s->next) {
						if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid && s->csid==aservers[m]) {
							break;
						}
					}
					if (s==NULL) {
						if (need_l0) {
							if (matocsserv_server_matches_labelexpr(cstab[aservers[m]].ptr,sm->labelexpr[0])) {
								wrong_repairable_labels = 1;
							}
						}
						if (need_l1) {
							if (matocsserv_server_matches_labelexpr(cstab[aservers[m]].ptr,sm->labelexpr[1])) {
								wrong_repairable_labels = 1;
							}
						}
					}
				}
			}
		} else {
			uint32_t servcnt;
			int32_t *matching;
			uint32_t vcms,scms;
			servcnt = 0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid==0) {
					servers[servcnt++] = s->csid;
				}
			}
			matching = do_advanced_match(sm,servcnt,servers);
			vcms = 0;
			for (j=0 ; j<sm->labelscnt ; j++) {
				if (matching[j]>=0) {
					vcms++;
				}
			}
			if (vcms<=sm->labelscnt) { // there are not matched labels
				scms = sm->matching_servers;
				if (vcms<scms) { // better matching is available
					wrong_repairable_labels = 1;
				} else {
					for (m=0 ; m<aservcnt ; m++) {
						for (s=c->slisthead ; s ; s=s->next) {
							if (s->valid==VALID && s->ecid==0 && s->csid==aservers[m]) {
								break;
							}
						}
						if (s==NULL) {
							servers[servcnt++] = aservers[m];
						}
					}
					if (servcnt!=aservcnt) { // some copies are on full or overloaded servers - then check matching
						matching = do_advanced_match(sm,servcnt,servers);
						scms = 0;
						for (j=0 ; j<sm->labelscnt ; j++) {
							if (matching[j]>=0) {
								scms++;
							}
						}
						if (vcms<scms) { // there is better solution than current
							wrong_repairable_labels = 1;
						} // not repairable
					} // else - system is not repairable
				}
			}
		}
	}

#ifdef __clang_analyzer__
	free(servers);
#endif

	if (c->all_gequiv > 0) { // chunk is not missing
		if (c->all_gequiv==1 && goal>2) { // highest priority - chunks with one copy and high goal
			return CHUNK_PRIORITY_ONECOPY_HIGHGOAL;
		} else if (c->all_gequiv==1 && goal==2) { // next priority - chunks with one copy
			return CHUNK_PRIORITY_ONECOPY_ANY;
		} else if (c->reg_gequiv<=1 && c->all_gequiv>c->reg_gequiv) { // next priority - chunks on one regular disk and some "marked for removal" disks
			return CHUNK_PRIORITY_ONEREGCOPY_PLUSMFR;
		} else if (c->all_gequiv>c->reg_gequiv && c->reg_gequiv<goal) { // next priority - chunks on "marked for removal" disks
			return CHUNK_PRIORITY_MARKEDFORREMOVAL;
		} else if ((hasec8parts || hasec4parts) && hasvc) { // next priority - unfinished EC<->COPY conversion
			return CHUNK_PRIORITY_UNFINISHEDEC;
		} else if (hasec8parts && hasec4parts) { // unfinished EC8<->EC4 conversion
			return CHUNK_PRIORITY_UNFINISHEDEC;
		} else if (c->reg_gequiv < goal) { // next priority - standard undergoal chunks and EC undergoal chunks
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if (ec_data_parts==8 && (hasec4parts || hasvc)) { // wrong format EC4 or COPY -> EC8
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if (ec_data_parts==4 && (hasec8parts || hasvc)) { // wrong format EC8 or COPY -> EC4
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if (ec_data_parts==0 && (hasec4parts || hasec8parts)) { // wrong format EC4 or EC8 -> COPY
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if (ec_data_parts==8 && server_count_with_parts8 < goal+7) { // same priority - parts are duplicated on different servers - treat as undergoal
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if (ec_data_parts==4 && server_count_with_parts4 < goal+3) { // same priority - parts are duplicated on different servers - treat as undergoal
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if ((vcmask8&0xFF)!=0xFF && parts_on_the_same_server) { // same priority - missing data parts and parts on the same server
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if ((vcmask4&0x0F)!=0x0F && parts_on_the_same_server) { // same priority - missing data parts and parts on the same server
			return CHUNK_PRIORITY_UNDERGOAL;
		} else if (c->reg_gequiv > goal || redundant_ec_parts || /*extra_chunks ||*/ parts_on_the_same_server) { // next priority - overgoal or some extra chunks
			return CHUNK_PRIORITY_OVERGOAL;
		} else if (wrong_repairable_labels) { // lowest priority - wrong labels (but system is able to fulfill them)
			return CHUNK_PRIORITY_WRONGLABELS;
		}
	}
	return DANGER_PRIORITIES;
}

static inline void chunk_priority_queue_check(chunk *c,uint8_t checklabels) {
	uint8_t j;

	if (c->sclassid==0 || c->fhead==FLISTNULLINDX || c->lockedto>=(uint32_t)main_time()+3600) { // lockedto from far future means that lockedto is used here to keep this chunk in memory
		return;
	}

	j = chunk_calculate_endanger_priority(c,checklabels);
	if (j<DANGER_PRIORITIES) {
		chunk_priority_enqueue(j,c);
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

static inline uint16_t chunk_creation_servers(uint16_t csids[MAXCSCOUNT],storagemode *sm,uint8_t labels_mode,uint8_t *olflag,uint32_t clientip) {
	uint16_t tmpcsids[MAXCSCOUNT];
	int32_t *matching;
	uint16_t servcount;
	uint16_t goodlabelscount;
	uint16_t overloaded;
	int32_t i,j;
	uint16_t cpos,fpos;
	uint32_t dist;
	int32_t x;

	servcount = matocsserv_getservers_wrandom(csids,&overloaded);
	if (servcount==0) {
		*olflag = (overloaded>0)?1:0;
		return 0;
	}
	if (servcount < sm->labelscnt && servcount + overloaded >= sm->labelscnt) {
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

	if (sm->has_labels || DoNotUseSameIP || DoNotUseSameRack) {
#ifdef MATCH_RIGHT
		// reverse server list
		for (i=0 ; i<servcount/2 ; i++) {
			x = csids[i];
			csids[i] = csids[servcount-1-i];
			csids[servcount-1-i] = x;
		}
#endif

		// match servers to labels
		matching = do_advanced_match(sm,servcount,csids);

		if (labels_mode != LABELS_MODE_STRICT) {
			goodlabelscount = do_extend_match(sm,servcount,matching);
			if (labels_mode == LABELS_MODE_STD) {
				if (goodlabelscount < sm->labelscnt && goodlabelscount + overloaded >= sm->labelscnt) {
					*olflag = 1;
					return 0;
				}
			}

		}

		// setting servers in proper order
		i = 0;
		j = servcount-1;
		while (i<=j) {
			while (i<=j && matching[sm->labelscnt+i]>=0) {
				i++;
			}
			while (i<=j && matching[sm->labelscnt+j]<0) {
				j--;
			}
			if (i<j) {
				x = matching[sm->labelscnt+i];
				matching[sm->labelscnt+i] = matching[sm->labelscnt+j];
				matching[sm->labelscnt+j] = x;
				x = csids[i];
				csids[i] = csids[j];
				csids[j] = x;
			}
		}
		if (labels_mode == LABELS_MODE_STRICT) {
			if (i < sm->labelscnt && i + overloaded >= sm->labelscnt) {
				*olflag = 1;
				return 0;
			}
			return i;
		}
	}
	return servcount;
}




void chunk_do_fast_job(chunk *c,uint32_t now,uint8_t extrajob);

CREATE_BUCKET_ALLOCATOR(io_ready_chunk,io_ready_chunk,10000000/sizeof(io_ready_chunk))

static inline void chunk_io_ready_begin(chunk *c) {
	io_ready_chunk *rch;
	uint32_t hash;
	hash = c->chunkid & 0xFF;
	for (rch = io_ready_chunk_hash[hash] ; rch!=NULL ; rch = rch->next) {
		if (rch->c == c) {
			return;
		}
	}
	rch = io_ready_chunk_malloc();
	rch->c = c;
	rch->next = io_ready_chunk_hash[hash];
	io_ready_chunk_hash[hash] = rch;
}

static inline void chunk_io_ready_end(chunk *c) {
	io_ready_chunk *rch,**rchp;
	uint32_t hash;
	hash = c->chunkid & 0xFF;
	rchp = io_ready_chunk_hash + hash;
	while ((rch = *rchp)!=NULL) {
		if (rch->c == c) {
			*rchp = rch->next;
			io_ready_chunk_free(rch);
		} else {
			rchp = &(rch->next);
		}
	}
}

static inline uint8_t chunk_not_ready_for_io(chunk *c) {
	io_ready_chunk *rch;
	uint32_t hash;
	hash = c->chunkid & 0xFF;
	for (rch = io_ready_chunk_hash[hash] ; rch!=NULL ; rch = rch->next) {
		if (rch->c == c) {
			return 1;
		}
	}
	return 0;
}

static inline void chunk_io_ready_check(chunk *c) {
	if (chunk_not_ready_for_io(c)) {
		chunk_do_fast_job(c,main_time(),2);
		if (c->operation==NONE && c->ondangerlist==0) {
			chunk_io_ready_end(c);
		}
	} else if (c->ondangerlist) {
		chunk_do_fast_job(c,main_time(),1);
	}
}

#ifdef MFSDEBUG
static inline void chunk_io_ready_log(void) {
	uint32_t hash;
	io_ready_chunk *rch;
	uint32_t io_ready_cnt;

	io_ready_cnt = 0;
	for (hash=0 ; hash<256 ; hash++) {
		for (rch = io_ready_chunk_hash[hash] ; rch!=NULL ; rch = rch->next) {
			io_ready_cnt++;
		}
	}
	if (io_ready_cnt>0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunk_io_ready_hash: %"PRIu32,io_ready_cnt);
	}
}
#endif

static inline void chunk_io_ready_init(void) {
	uint32_t hash;
	for (hash=0 ; hash<256 ; hash++) {
		io_ready_chunk_hash[hash]=NULL;
	}
}

static inline void chunk_io_ready_cleanall(void) {
	io_ready_chunk_free_all();
	chunk_io_ready_init();
}




void chunk_emergency_increase_version(chunk *c) {
	slist *s;
	uint32_t i;
	uint8_t fix;

	i = 0;
	fix = 0;
//	chunk_remove_disconnected_chunks(c);
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
			if (s->ecid==0) {
				if (s->valid==TDVALID || s->valid==TDBUSY) {
					s->valid = TDBUSY;
				} else {
					s->valid = BUSY;
				}
				s->version = c->version+1;
				stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
				matocsserv_send_setchunkversion(cstab[s->csid].ptr,c->chunkid,0,c->version+1,c->version);
				chunk_addopchunk(s->csid,c->chunkid);
				i++;
			} else {
				if (s->valid==TDVALID || s->valid==TDBUSY) {
					s->valid = TDWVER;
				} else {
					s->valid = WVER;
				}
				fix = 1;
			}
		}
	}
	if (fix) {
		chunk_state_fix(c);
	}
	if (i>0) {	// should always be true !!!
		c->interrupted = 0;
		chunk_set_op(c,SET_VERSION);
		c->version++;
		c->allowreadzeros = 0;
		changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
	} else {
		matoclserv_chunk_status(c->chunkid,MFS_ERROR_CHUNKLOST);
	}
}

static inline int chunk_remove_disconnected_chunks(chunk *c) {
	uint8_t opfinished,validcopies,disc;
	uint8_t verfixed;
	slist *s,**st;
	uint32_t now;

	if (discservers==NULL && discservers_next==NULL) {
		return 0;
	}
	disc = 0;
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (!cstab[s->csid].valid) {
			if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && s->ecid==0) { // pro forma
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
	if (c->lockedto<(uint32_t)main_time() && c->operation==NONE && c->slisthead==NULL && c->fhead==FLISTNULLINDX && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		if (c->ondangerlist) {
			chunk_priority_remove(c);
		}
		chunk_delete(c);
		return 1;
	}
	now = main_time();
	if (c->operation!=NONE) {
		validcopies=0;
		opfinished=1;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==BUSY || s->valid==TDBUSY) {
				opfinished=0;
			}
			if (s->ecid==0 && (s->valid==VALID || s->valid==TDVALID)) {
				validcopies=1;
			}
		}

		if (opfinished && validcopies==0 && (c->operation==SET_VERSION || c->operation==TRUNCATE)) { // we know that version increase was just not completed, so all WVER chunks with version exactly one lower than chunk version are actually VALID copies
			verfixed = 0;
			for (s=c->slisthead ; s!=NULL ; s=s->next) {
				if (s->version+1==c->version && s->ecid==0) {
					if (s->valid==TDWVER) {
						verfixed = 1;
						s->valid = TDVALID;
					} else if (s->valid==WVER) {
						verfixed = 1;
						s->valid = VALID;
					}
				}
			}
			if (verfixed) {
				c->version--;
				c->allowreadzeros = 0;
				changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
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
			if (c->operation==REPLICATE || c->operation==LOCALSPLIT) {
				chunk_set_op(c,NONE);
				chunk_replock_repend(c->chunkid);
//				c->lockedto = 0;
				chunk_io_ready_check(c);
				if (c->operation==NONE) {
					matoclserv_chunk_status(c->chunkid,MFS_STATUS_OK);
				}
				if (c->lockedto<now && chunk_replock_test(c->chunkid,now)==0) {
					matoclserv_chunk_unlocked(c->chunkid,c);
				}
//				matoclserv_chunk_unlocked(c->chunkid,c);
			} else {
				if (validcopies) {
					chunk_emergency_increase_version(c);
				} else {
					chunk_set_op(c,NONE);
					if (nospace) {
						matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOSPACE);
					} else {
						matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOTDONE);
					}
				}
			}
		} else {
			if (c->operation!=REPLICATE && c->operation!=LOCALSPLIT) {
				c->interrupted = 1;
			}
		}
	}
	chunk_state_fix(c);
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

// find new sclassid and compact list if possible
static inline uint16_t chunk_compact_and_find_sclassid(chunk *c) {
	uint8_t g;
	uint8_t mg;
	uint8_t sclassid,v;
	uint32_t findx;
	flist *fl;

	if (c->fhead<FLISTFIRSTINDX) {
		if (c->fhead==FLISTNULLINDX) {
			return 0;
		} else {
			return c->sclassid;
		}
	}
	fl = flist_get(c->fhead);
	if (fl->nexti==FLISTNULLINDX) {
		sclassid = fl->sclassid;
		findx = fl->fcount;
		if (findx<FLISTFIRSTINDX) {
			flist_free(c->fhead);
			c->fhead = findx;
		}
		return sclassid;
	} else {
		mg = 0;
		sclassid = 0;
		v = 0;
		findx = c->fhead;
		while (findx!=FLISTNULLINDX) {
			fl = flist_get(findx);
			findx = fl->nexti;
			g = sclass_get_keeparch_max_goal_equivalent(fl->sclassid);
			if (g>mg) {
				mg = g;
				sclassid = fl->sclassid;
				v = 0;
			} else if (g==mg) {
				if (sclassid<=9 && v==0) {
					sclassid = fl->sclassid;
				} else if (sclassid>9 && fl->sclassid>9) {
					sclassid = g;
					v = 1;
				}
			}
		}
		massert(sclassid>0,"wrong labels set");
		return sclassid;
	}
}

int chunk_change_file(uint64_t chunkid,uint8_t prevsclassid,uint8_t newsclassid) {
	chunk *c;
	uint16_t new_calculated_sclassid;
	uint32_t fcount,findx,*findxptr;
	uint8_t flags;
	flist *fl;

	if (prevsclassid==newsclassid) {
		return MFS_STATUS_OK;
	}
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->fhead==FLISTNULLINDX) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",c->chunkid);
		return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
	}
//	oldsclassid = c->sclassid;
	if (c->fhead==FLISTONEFILEINDX) {
		new_calculated_sclassid = newsclassid;
	} else {
		if (c->fhead<FLISTFIRSTINDX) {
			massert(prevsclassid==c->sclassid,"wrong chunk sclassid");
			fcount = c->fhead;
			fl = flist_get(c->fhead = flist_alloc());
			fl->fcount = fcount-1;
			fl->sclassid = c->sclassid;
			fl = flist_get(fl->nexti = flist_alloc());
			fl->fcount = 1;
			fl->sclassid = newsclassid;
			fl->nexti = FLISTNULLINDX;
		} else {
			findxptr = &(c->fhead);
			flags = 0;
			while (flags!=3 && (findx=*findxptr)!=FLISTNULLINDX) {
				fl = flist_get(findx);
				if (fl->sclassid==prevsclassid && ((flags&2)==0)) {
					if (fl->fcount>1) {
						fl->fcount--;
						findxptr = &(fl->nexti);
					} else {
						*findxptr = fl->nexti;
						flist_free(findx);
					}
					flags |= 2;
				} else {
					findxptr = &(fl->nexti);
					if (fl->sclassid==newsclassid && fl->fcount<FLISTMAXFCOUNT && ((flags&1)==0)) {
						fl->fcount++;
						flags |= 1;
					}
				}
			}
			massert(flags&2,"prevsclassid not found");
			if ((flags&1)==0) {
				fl = flist_get(findx = flist_alloc());
				fl->nexti = c->fhead;
				c->fhead = findx;
/* code that adds new element at the end - not used because in case of fcount==FLISTMAXFCOUNT we want to add new elements before 'full' ones 
				fl = flist_get(*findxptr = flist_alloc());
				fl->nexti = FLISTNULLINDX;
*/
				fl->sclassid = newsclassid;
				fl->fcount = 1;
			}
		}
		new_calculated_sclassid = chunk_compact_and_find_sclassid(c);
	}
	if (new_calculated_sclassid!=c->sclassid) {
		chunk_state_set_sclass(c,new_calculated_sclassid);
		chunk_priority_queue_check(c,1);
	} else {
		chunk_priority_queue_check(c,0);
	}
	return MFS_STATUS_OK;
}

static inline int chunk_delete_file_int(chunk *c,uint8_t sclassid,uint32_t delete_timeout) {
	uint16_t new_calculated_sclassid;
	uint32_t findx,*findxptr;
	uint8_t flags;
	flist *fl;

	if (c->fhead==FLISTNULLINDX) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",c->chunkid);
		return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
	}
	massert(sclassid>0,"wrong storage class id");
	if (c->fhead==FLISTONEFILEINDX) {
		massert(sclassid==c->sclassid,"wrong chunk sclassid");
		new_calculated_sclassid = 0;
		c->fhead = FLISTNULLINDX;
	} else if (c->fhead<FLISTFIRSTINDX) {
		massert(sclassid==c->sclassid,"wrong chunk sclassid");
		c->fhead--;
		new_calculated_sclassid = c->sclassid;
	} else {
		findxptr = &(c->fhead);
		flags = 0;
		while (flags==0 && (findx=*findxptr)!=FLISTNULLINDX) {
			fl = flist_get(findx);
			if (fl->sclassid==sclassid) {
				if (fl->fcount>1) {
					fl->fcount--;
					findxptr = &(fl->nexti);
				} else {
					*findxptr = fl->nexti;
					flist_free(findx);
				}
				flags = 1;
			} else {
				findxptr = &(fl->nexti);
			}
		}
		massert(flags==1,"sclassid not found");
		new_calculated_sclassid = chunk_compact_and_find_sclassid(c);
	}
	if (new_calculated_sclassid!=c->sclassid) {
		chunk_state_set_sclass(c,new_calculated_sclassid);
	}
	if (c->fhead==FLISTNULLINDX && delete_timeout>0) {
		c->lockedto = (uint32_t)main_time()+delete_timeout;
	}
	return MFS_STATUS_OK;
}

static inline int chunk_add_file_int(chunk *c,uint8_t sclassid) {
	uint16_t new_calculated_sclassid;
	uint32_t findx,*findxptr;
	uint8_t flags;
	flist *fl;

	massert(sclassid>0,"wrong labels set");
	if (c->fhead==FLISTNULLINDX) {
		new_calculated_sclassid = sclassid;
		c->fhead = FLISTONEFILEINDX;
	} else if (c->fhead<(FLISTFIRSTINDX-1)) {
		if (sclassid==c->sclassid) {
			c->fhead++;
			new_calculated_sclassid = sclassid;
		} else {
			fl = flist_get(findx = flist_alloc());
			fl->sclassid = c->sclassid;
			fl->fcount = c->fhead;
			c->fhead = findx;
			fl = flist_get(fl->nexti = flist_alloc());
			fl->sclassid = sclassid;
			fl->fcount = 1;
			fl->nexti = FLISTNULLINDX;
			new_calculated_sclassid = chunk_compact_and_find_sclassid(c);
		}
	} else {
		if (c->fhead==(FLISTFIRSTINDX-1)) {
			fl = flist_get(findx = flist_alloc());
			fl->nexti = FLISTNULLINDX;
			fl->sclassid = c->sclassid;
			fl->fcount = c->fhead;
			c->fhead = findx;
		}
		findxptr = &(c->fhead);
		flags = 0;
		while (flags==0 && (findx=*findxptr)!=FLISTNULLINDX) {
			fl = flist_get(findx);
			findxptr = &(fl->nexti);
			if (fl->sclassid==sclassid && fl->fcount<FLISTMAXFCOUNT) {
				fl->fcount++;
				flags = 1;
			}
		}
		if (flags==0) {
			fl = flist_get(findx = flist_alloc());
			fl->nexti = c->fhead;
			c->fhead = findx;
/* code that adds new element at the end - not used - see coments in 'chunk_change_file'
			fl = flist_get(*findxptr = flist_alloc());
			fl->nexti = FLISTNULLINDX;
*/
			fl->sclassid = sclassid;
			fl->fcount = 1;
		}
		new_calculated_sclassid = chunk_compact_and_find_sclassid(c);
	}
	if (new_calculated_sclassid!=c->sclassid) {
		chunk_state_set_sclass(c,new_calculated_sclassid);
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
		if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && s->ecid==0) {
			matocsserv_write_counters(cstab[s->csid].ptr,x);
		}
	}
}

int chunk_locked_or_busy(void *cptr) {
	chunk *c = (chunk*)cptr;
	return (c->lockedto<(uint32_t)(main_time()) && c->operation==NONE)?0:1;
}

uint8_t chunk_get_storage_status(uint64_t chunkid,uint8_t *fcopies,uint8_t *ec8parts,uint8_t *ec4parts) {
	chunk *c;
	slist *s;
	uint8_t cnt,c8,c4;
	uint32_t ecmask8,ecmask4;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	cnt = 0;
	ecmask8 = 0;
	ecmask4 = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (cstab[s->csid].valid && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && s->valid!=INVALID) {
			if (s->ecid==0) {
				if (cnt<255) {
					cnt++;
				}
			} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
				ecmask4 |= UINT32_C(1) << (s->ecid & 0x0F);
			} else if (s->ecid>=0x20 && s->ecid<=0x30) {
				ecmask8 |= UINT32_C(1) << (s->ecid & 0x1F);
			}
		}
	}
	*fcopies = cnt;
	c8 = 0;
	c4 = 0;
	if (ecmask8) {
		c8 = bitcount(ecmask8);
	}
	if (ecmask4) {
		c4 = bitcount(ecmask4);
	}
	*ec8parts = c8;
	*ec4parts = c4;
	return MFS_STATUS_OK;
}

int chunk_get_archflag(uint64_t chunkid,uint8_t *archflag) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	*archflag = c->flags & FLAG_ARCH;
	return MFS_STATUS_OK;
}

int chunk_set_archflag(uint64_t chunkid,uint8_t archflag,uint32_t *archflagchanged) {
	chunk *c;
	uint8_t newflags;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	newflags = (c->flags & FLAG_TRASH) | (archflag ? FLAG_ARCH : 0);
	if (newflags != c->flags) {
		chunk_state_set_flags(c,newflags);
		chunk_priority_queue_check(c,1);
		(*archflagchanged)++;
	}
	return MFS_STATUS_OK;
}

int chunk_set_trashflag(uint64_t chunkid,uint8_t trashflag) {
	chunk *c;
	uint8_t newflags;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (trashflag==1 && c->fhead>FLISTONEFILEINDX) { // chunk belongs to more than one file - skip trash flag
		return MFS_ERROR_NOTDONE;
	}
	newflags = (c->flags & FLAG_ARCH) | (trashflag ? FLAG_TRASH : 0);
	if (newflags != c->flags) {
		chunk_state_set_flags(c,newflags);
		chunk_priority_queue_check(c,1);
	}
	return MFS_STATUS_OK;
}

int chunk_set_autoarch(uint64_t chunkid,uint32_t archreftime,uint32_t *archflagchanged,uint8_t intrash,uint32_t *trashflagchanged) {
	chunk *c;
	uint8_t newflags;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	newflags = c->flags;
	if (archreftime > c->lockedto && (c->flags & FLAG_ARCH)==0) {
		newflags = (c->flags & FLAG_TRASH) | FLAG_ARCH;
		(*archflagchanged)++;
	}
	if ((intrash==0 || c->fhead>FLISTONEFILEINDX) && (c->flags & FLAG_TRASH)) {
		newflags &= FLAG_ARCH; // delete FLAG_TRASH
		(*trashflagchanged)++;
	}
	if (newflags != c->flags) {
		chunk_state_set_flags(c,newflags);
		chunk_priority_queue_check(c,1);
	}
	return MFS_STATUS_OK;
}

// CHUNK_FLOOP_NOTFOUND
// CHUNK_FLOOP_DELETED
// CHUNK_FLOOP_MISSING_NOCOPY
// CHUNK_FLOOP_MISSING_INVALID
// CHUNK_FLOOP_MISSING_WRONGVERSION
// CHUNK_FLOOP_UNDERGOAL
// CHUNK_FLOOP_OK
chunkfloop chunk_fileloop_task(uint64_t chunkid,uint8_t sclassid,uint8_t aftereof,uint32_t archreftime,uint32_t *archflagchanged,uint8_t intrash,uint32_t *trashflagchanged) {
	chunk *c;
	slist *s;
	uint8_t newflags;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return CHUNK_FLOOP_NOTFOUND;
	}
	if (c->all_gequiv==0 && aftereof && c->lockedto<(uint32_t)(main_time()) && c->operation==NONE) {
//		chunk_delete_file_int(c,sclassid,UNUSED_DELETE_TIMEOUT);
		chunk_delete_file_int(c,sclassid,0);
		return CHUNK_FLOOP_DELETED;
	}
	newflags = c->flags;
	if (archreftime > c->lockedto && (c->flags & FLAG_ARCH)==0) {
		newflags = (newflags & FLAG_TRASH) | FLAG_ARCH;
		(*archflagchanged)++;
	}
	if ((intrash==0 || c->fhead>FLISTONEFILEINDX) && (c->flags & FLAG_TRASH)) {
		newflags &= FLAG_ARCH; // delete FLAG_TRASH
		(*trashflagchanged)++;
	}
	if (newflags != c->flags) {
		chunk_state_set_flags(c,newflags);
		chunk_priority_queue_check(c,1);
	}
	if (c->all_gequiv==0) {
		uint8_t wv,in;
		uint32_t wvec4mask,wvec8mask;
		uint32_t inec4mask,inec8mask;
		uint32_t ec4mask,ec8mask;
		wv = 0;
		in = 0;
		wvec4mask = 0;
		wvec8mask = 0;
		inec4mask = 0;
		inec8mask = 0;
		ec4mask = 0;
		ec8mask = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==WVER || s->valid==TDWVER) {
				if (s->ecid==0) {
					wv = 1;
				} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
					wvec4mask |= UINT32_C(1) << (s->ecid & 0x0F);
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					wvec8mask |= UINT32_C(1) << (s->ecid & 0x1F);
				}
			} else if (s->valid==INVALID) {
				if (s->ecid==0) {
					in = 1;
				} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
					inec4mask |= UINT32_C(1) << (s->ecid & 0x0F);
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					inec8mask |= UINT32_C(1) << (s->ecid & 0x1F);
				}
			} else if (s->valid!=DEL) {
				if (s->ecid>=0x10 && s->ecid<=0x1C) {
					ec4mask |= UINT32_C(1) << (s->ecid & 0x0F);
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					ec8mask |= UINT32_C(1) << (s->ecid & 0x1F);
				}
			}
		}
		if (wv || bitcount(wvec4mask|ec4mask)>=4 || bitcount(wvec8mask|ec8mask)>=8) {
			return CHUNK_FLOOP_MISSING_WRONGVERSION;
		}
		if (in || bitcount(wvec4mask|inec4mask|ec4mask)>=4 || bitcount(wvec8mask|inec8mask|ec8mask)>=8) {
			return CHUNK_FLOOP_MISSING_INVALID;
		}
		if (ec4mask|ec8mask) {
			return CHUNK_FLOOP_MISSING_PARTIALEC;
		}
		return CHUNK_FLOOP_MISSING_NOCOPY;
	}
	if (c->all_gequiv < sclass_calc_goal_equivalent(sclass_get_keeparch_storagemode(c->sclassid,c->flags))) {
		return CHUNK_FLOOP_UNDERGOAL;
	}
	return CHUNK_FLOOP_OK;
}

uint8_t chunk_remove_from_missing_log(uint64_t chunkid) {
	chunk *c;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return 1;
	}
	if (c->all_gequiv==0) {
		return 0;
	}
	return 1;
}

int chunk_read_check(uint32_t ts,uint64_t chunkid) {
	chunk *c;
	uint32_t ecmask8,ecmask4;
	slist *s;

	c = chunk_find(chunkid);
	if (chunk_remove_disconnected_chunks(c)) {
		c = NULL;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->lockedto>=ts) {
		return MFS_ERROR_LOCKED;
	}
	if (c->operation != NONE) {
		return MFS_ERROR_CHUNKBUSY;
	}
	ecmask4 = 0;
	ecmask8 = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==BUSY || s->valid==TDBUSY) {
			// something is wrong - we have busy chunks but operation is NONE !!!
			return MFS_ERROR_EAGAIN;
		}
		if (s->valid==VALID || s->valid==TDVALID) {
			if (s->ecid==0) {
				return MFS_STATUS_OK; // we have at least one valid full copy - ok to read data
			} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
				ecmask4 |= UINT32_C(1) << (s->ecid & 0x0F);
			} else if (s->ecid>=0x20 && s->ecid<=0x30) {
				ecmask8 |= UINT32_C(1) << (s->ecid & 0x1F);
			}
		}
	}
	if (((ecmask8 & 0xFF) == 0xFF) || ((ecmask4 & 0x0F) == 0x0F)) { // we have all data parts - we can read data
		return MFS_STATUS_OK;
	}
	if (bitcount(ecmask8)>=8 || bitcount(ecmask4)>=4) { // can be recovered
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"chunk 0x%016"PRIX64": data parts missing for read operation - trying to recover",chunkid);
		chunk_do_fast_job(c,ts,2);
		if (c->operation!=NONE) {
			chunk_io_ready_begin(c);
//			chunk_priority_enqueue(CHUNK_PRIORITY_IOREADY,c); // enqueue it with highest priority
			return MFS_ERROR_CHUNKBUSY; // and wait for end of replication
		}
	}
	if (csregisterinprogress) {
		matocsserv_broadcast_regfirst_chunk(chunkid);
	}
	return MFS_STATUS_OK; // leave OK here - the rest of the code will return error in this case
}

static inline int chunk_can_be_fixed(chunk *c,storagemode *sm) {
	static uint16_t *servers = NULL;
	static uint16_t *rcsids = NULL;
	int32_t *matching;
	slist *s;
	uint16_t servcnt;
	uint16_t vcservcnt;
	uint16_t availservers;
	uint16_t i;

	if (c==NULL && sm==NULL) { // term condition
		if (rcsids!=NULL) {
			free(rcsids);
			rcsids = NULL;
		}
		if (servers!=NULL) {
			free(servers);
			servers = NULL;
		}
		return 0;
	}
	if (servers==NULL) {
		servers = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(servers);
	}
	if (rcsids==NULL) {
		rcsids = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(rcsids);
	}

	availservers = matocsserv_getservers_replpossible(rcsids);

	servcnt = 0;
	// first add copies
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==VALID && s->ecid==0) {
			servers[servcnt++] = s->csid;
		}
	}
	vcservcnt = servcnt;

	if ((sm->has_labels || DoNotUseSameIP || DoNotUseSameRack) && (sclass_get_labels_mode(c->sclassid,sm) == LABELS_MODE_STRICT)) { // strict labeled version
		// then all other valid servers
		for (i=0 ; i<availservers ; i++) {
			for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid!=0) ; s=s->next) {}
			if (s==NULL) {
				servers[servcnt++] = rcsids[i];
			}
		}

		matching = do_advanced_match(sm,servcnt,servers);

		for (i=0 ; i<sm->labelscnt ; i++) {
			int32_t servpos;
			if (matching[i]<(int32_t)(sm->labelscnt)) {
				servpos=-1;
			} else {
				servpos=matching[i]-sm->labelscnt;
			}
			if (servpos>=vcservcnt) { // we have fixable label
				return 1;
			}
		}

		return 0;
	}
	// sm->labelscnt == goal
	// vcservcnt == vc
	return (sm->labelscnt <= availservers || vcservcnt < availservers)?1:0;
}

static inline int chunk_prepare_to_modify(chunk *c,uint32_t ts,uint8_t cschanges,uint8_t csalldata) {
	uint16_t vc;
	storagemode *sm;
	slist *s;
	uint8_t gequiv;

	if (c->operation!=NONE) {
		return MFS_ERROR_CHUNKBUSY;
	}
	vc = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==BUSY || s->valid==TDBUSY) {
			// something is wrong - we have busy chunks but operation is NONE !!!
			return MFS_ERROR_EAGAIN;
		}
		if (s->ecid==0 && (s->valid==VALID || s->valid==TDVALID)) {
			vc++;
		}
	}
	sm = sclass_get_keeparch_storagemode(c->sclassid,c->flags);
	if (sm->ec_data_chksum_parts && c->flags!=0) { // with current flags - this is EC chunk - if so, clear flags
		chunk_state_set_flags(c,0);
		changelog("%"PRIu32"|CHUNKFLAGSCLR(%"PRIu64")",ts,c->chunkid);
		sm = sclass_get_keeparch_storagemode(c->sclassid,c->flags);
	}
	gequiv = sclass_calc_goal_equivalent(sm);
	if (vc==0 && c->all_gequiv>0) { // chunk is in EC mode without copies - make them
		chunk_do_fast_job(c,ts,2);
		if (c->operation!=NONE) {
			chunk_io_ready_begin(c);
//			chunk_priority_enqueue(CHUNK_PRIORITY_IOREADY,c); // enqueue it with highest priority
			return MFS_ERROR_CHUNKBUSY; // and wait for the end of operation
		}
		// refresh vc
		vc = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->ecid==0 && (s->valid==VALID || s->valid==TDVALID)) {
				vc++;
			}
		}
	}
	if (cschanges) {
		if (vc < gequiv) {
			if (csregisterinprogress) {
				matocsserv_broadcast_regfirst_chunk(c->chunkid);
			}
			return MFS_ERROR_EAGAIN; // just try again later
		}
	} else {
		if (vc > 0 && vc < gequiv) {
			chunk_do_fast_job(c,ts,2);
			if (c->operation!=NONE) {
				chunk_io_ready_begin(c);
				return MFS_ERROR_CHUNKBUSY; // wait for the end of operation
			}
			// refresh vc
			vc = 0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->ecid==0 && (s->valid==VALID || s->valid==TDVALID)) {
					vc++;
				}
			}
		}
	}
	if (vc>0 && vc<gequiv) { // not enough copies
		if (chunk_can_be_fixed(c,sm)) {
			return MFS_ERROR_EAGAIN;
		}
	}
	if (vc==0) {
		if (c->all_gequiv>0) { // can be fixed somehow
			// special condition for EC chunk that can't be converted to COPY due to lack of labeled servers in STRICT mode !!!
			// in this case c->all_gequiv>0 but chunk can't be converted to COPIES and therefore can't be modified !!!
			if (sclass_get_labels_mode(c->sclassid,sm)==LABELS_MODE_STRICT && sclass_get_keeparch_storagemode(c->sclassid,0)->matching_servers==0 && csalldata) { // special case - can't be fixed
				return MFS_ERROR_NOSPACE;
			}
			return MFS_ERROR_EAGAIN; // try again later
		} else if (csalldata) { // can't be fixed using all chunkservers
			return MFS_ERROR_CHUNKLOST; // return error
		} else { // can't be fixed using connected chunkservers by possibly can be fixed with all chunkservers
			return MFS_ERROR_CSNOTPRESENT; // try again
		}
	}
	return MFS_STATUS_OK;
}

int chunk_univ_multi_modify(uint32_t ts,uint8_t mr,uint8_t continueop,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t *opflag,uint32_t clientip) {
	uint16_t csids[MAXCSCOUNT];
	static void **chosen = NULL;
	static uint32_t chosenleng = 0;
	uint16_t servcount=0;
//	uint32_t vc,rvc;
	uint8_t rvcexists;
	int status;
	uint8_t overloaded,fix;
	slist *os,*s;
	uint32_t i;
	chunk *oc,*c;
	uint8_t csstable,csalldata;
	uint8_t cschanges;
	uint8_t labels_mode;
	storagemode *sm;
	uint8_t allvc;

	if (ts>(main_start_time()+5) && csregisterinprogress==0) {
		csstable = 1;
	} else {
		csstable = 0;
	}

	cschanges = (csstable==0 || (matocsserv_receiving_chunks_state()&TRANSFERING_NEW_CHUNKS))?1:0;

	if (chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		csalldata = 1;
	} else {
		csalldata = 0;
	}

	sm = sclass_get_create_storagemode(sclassid);

	if (ochunkid==0) {	// new chunk
		if (mr==0) {
			labels_mode = sclass_get_labels_mode(sclassid,sm);
			servcount = chunk_creation_servers(csids,sm,labels_mode,&overloaded,clientip);
			if (servcount==0) {
				if (overloaded || csalldata==0) {
					return MFS_ERROR_EAGAIN; // try again forever
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
			chunk_set_op(c,CREATE);
			chunk_add_file_int(c,sclassid);
			if (servcount<sm->labelscnt) {
				allvc = servcount;
			} else {
				allvc = sm->labelscnt;
			}
			if (allvc>chosenleng) {
				chosenleng = allvc+10;
				chosen = malloc(sizeof(void*)*chosenleng);
				passert(chosen);
			}
			for (i=0 ; i<allvc ; i++) {
				s = slist_malloc();
				s->csid = csids[i];
				s->ecid = 0;
				s->valid = BUSY;
				s->version = c->version;
				chunk_new_copy(c,s);
				chosen[i] = cstab[s->csid].ptr;
				stats_chunkops[CHUNK_OP_CREATE_TRY]++;
				matocsserv_send_createchunk(cstab[s->csid].ptr,c->chunkid,0,c->version);
				chunk_addopchunk(s->csid,c->chunkid);
			}
			matocsserv_useservers_wrandom(chosen,allvc);
			if (allvc>15) {
				allvc=15;
			}
			chunk_state_set_counters(c,STORAGE_MODE_COPIES,allvc,allvc);
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
		if (mr==0 && (oc->lockedto>=ts || chunk_replock_test(ochunkid,ts)) && continueop==0) {
			return MFS_ERROR_LOCKED;
		}
		if (oc->fhead==FLISTONEFILEINDX) {
			c = oc;
			if (mr==0) {
				*nchunkid = ochunkid;
				status = chunk_prepare_to_modify(c,ts,cschanges,csalldata);
				if (status!=MFS_STATUS_OK) {
					return status;
				}
				rvcexists = 0;
				if (c->needverincrease==0) {
					for (s=c->slisthead ; s && rvcexists==0 ; s=s->next) {
						if (s->ecid!=0 && (s->valid==VALID || s->valid==TDVALID)) {
							rvcexists = 1;
						}
					}
				}
				if (c->needverincrease || rvcexists) {
					fix = 0;
					for (s=c->slisthead ;s ; s=s->next) {
						if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
							if (s->ecid==0) {
								if (s->valid==TDVALID) {
									s->valid = TDBUSY;
								} else {
									s->valid = BUSY;
								}
								s->version = c->version+1;
								stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
								matocsserv_send_setchunkversion(cstab[s->csid].ptr,ochunkid,0,c->version+1,c->version);
								chunk_addopchunk(s->csid,c->chunkid);
							} else {
								if (s->valid==TDVALID) {
									s->valid = TDWVER;
								} else {
									s->valid = WVER;
								}
								fix = 1;
							}
						}
					}
					if (fix) {
						chunk_state_fix(c);
					}
					c->interrupted = 0;
					chunk_set_op(c,SET_VERSION);
					c->version++;
					*opflag = 1;
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
			if (oc->fhead==FLISTNULLINDX) {	// it's serious structure error
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",ochunkid);
				return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
			}
			if (mr==0) {
				status = chunk_prepare_to_modify(oc,ts,cschanges,csalldata);
				if (status!=MFS_STATUS_OK) {
					return status;
				}
				c = chunk_new(nextchunkid++);
				c->version = 1;
				c->interrupted = 0;
				chunk_delete_file_int(oc,sclassid,0);
				chunk_add_file_int(c,sclassid);

				allvc = 0;
				for (os=oc->slisthead ; os ; os=os->next) {
					if (os->valid!=INVALID && os->valid!=DEL && os->valid!=WVER && os->valid!=TDWVER) {
						if (os->ecid==0) {
							chunk_set_op(c,DUPLICATE);
							s = slist_malloc();
							s->csid = os->csid;
							s->ecid = os->ecid;
							s->valid = BUSY;
							s->ecid = 0;
							s->version = c->version;
							chunk_new_copy(c,s);
							allvc++;
							stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
							matocsserv_send_duplicatechunk(cstab[s->csid].ptr,c->chunkid,0,c->version,oc->chunkid,0,oc->version);
							chunk_addopchunk(s->csid,c->chunkid);
						}
					}
				}
				if (allvc>15) {
					allvc=15;
				}
				chunk_state_set_counters(c,STORAGE_MODE_COPIES,allvc,allvc);
				*nchunkid = c->chunkid;
				*opflag=1;
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
	chunk *oc,*c;
	uint8_t csstable,csalldata;
	uint8_t cschanges;
	uint8_t allvc;
//	uint32_t vc;
	int status;
	uint8_t fix;

	if (ts>(main_start_time()+5) && csregisterinprogress==0) {
		csstable = 1;
	} else {
		csstable = 0;
	}

	cschanges = (csstable==0 || (matocsserv_receiving_chunks_state()&TRANSFERING_NEW_CHUNKS))?1:0;

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
	if (mr==0 && (oc->lockedto>=ts || chunk_replock_test(ochunkid,ts))) {
		return MFS_ERROR_LOCKED;
	}
	if (oc->fhead==FLISTONEFILEINDX) {
		c = oc;
		if (mr==0) {
			*nchunkid = ochunkid;
			status = chunk_prepare_to_modify(c,ts,cschanges,csalldata);
			if (status!=MFS_STATUS_OK) {
				return status;
			}
			fix = 0;
			for (s=c->slisthead ;s ; s=s->next) {
				if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER) {
					if (s->ecid==0) {
						if (s->valid==TDVALID) {
							s->valid = TDBUSY;
						} else { // s->valid here is always VALID
							s->valid = BUSY;
						}
						s->version = c->version+1;
						stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
						matocsserv_send_truncatechunk(cstab[s->csid].ptr,ochunkid,0,length,c->version+1,c->version);
						chunk_addopchunk(s->csid,c->chunkid);
					} else {
						if (s->valid==TDVALID) {
							s->valid = TDWVER;
						} else {
							s->valid = WVER;
						}
						fix = 1;
					}
				}
			}
			if (fix) {
				chunk_state_fix(c);
			}
			c->interrupted = 0;
			chunk_set_op(c,TRUNCATE);
			c->version++;
		} else {
			if (*nchunkid != ochunkid) {
				return MFS_ERROR_MISMATCH;
			}
			c->version++;
		}
	} else {
		if (oc->fhead==FLISTNULLINDX) {	// it's serious structure error
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"serious structure inconsistency: (chunkid:%016"PRIX64")",ochunkid);
			return MFS_ERROR_CHUNKLOST;	// MFS_ERROR_STRUCTURE
		}
		if (mr==0) {
			status = chunk_prepare_to_modify(oc,ts,cschanges,csalldata);
			if (status!=MFS_STATUS_OK) {
				return status;
			}
			c = chunk_new(nextchunkid++);
			c->version = 1;
			c->interrupted = 0;
			chunk_delete_file_int(oc,sclassid,0);
			chunk_add_file_int(c,sclassid);

			allvc = 0;
			for (os=oc->slisthead ; os ; os=os->next) {
				if (os->valid!=INVALID && os->valid!=DEL && os->valid!=WVER && os->valid!=TDWVER) {
					if (os->ecid==0) {
						chunk_set_op(c,DUPTRUNC);
						s = slist_malloc();
						s->csid = os->csid;
						s->ecid = os->ecid;
						s->valid = BUSY;
						s->version = c->version;
						chunk_new_copy(c,s);
						allvc++;
						stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
						matocsserv_send_duptruncchunk(cstab[s->csid].ptr,c->chunkid,0,c->version,oc->chunkid,0,oc->version,length);
						chunk_addopchunk(s->csid,c->chunkid);
					}
				}
			}
			if (allvc>15) {
				allvc=15;
			}
			chunk_state_set_counters(c,STORAGE_MODE_COPIES,allvc,allvc);
			*nchunkid = c->chunkid;
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

int chunk_repair(uint8_t sclassid,uint64_t ochunkid,uint8_t flags,uint32_t *nversion) {
	uint32_t bestversion,bestecversion8,bestecversion4;
	chunk *c;
	slist *s;
	uint32_t mask8,mask4;
	uint32_t now;

	*nversion=0;
	if (ochunkid==0) {
		return 0;	// not changed
	}

	c = chunk_find(ochunkid);
	if (c==NULL) {	// no such chunk - erase (nchunkid already is 0 - so just return with "changed" status)
		return 1;
	}
	now = main_time();
	if (c->lockedto>=now || chunk_replock_test(ochunkid,now)) { // can't repair locked chunks - but if it's locked, then likely it doesn't need to be repaired
		return 0;
	}
	if (c->all_gequiv>0) { // chunk is ok
		return 0;
	}
	chunk_write_counters(c,0);
	mask8 = 0;
	mask4 = 0;
	bestversion = 0;
	bestecversion8 = 0;
	bestecversion4 = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (cstab[s->csid].valid) {
			if (s->valid == WVER || s->valid == TDWVER) {
				if (s->ecid==0) {
					if (s->version>=bestversion) {
						bestversion = s->version;
					}
				} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
					mask4 |= (UINT32_C(1) << (s->ecid & 0x0F));
					if (s->version>=bestecversion4) {
						bestecversion4 = s->version;
					}
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					mask8 |= (UINT32_C(1) << (s->ecid & 0x1F));
					if (s->version>=bestecversion8) {
						bestecversion8 = s->version;
					}
				}
			} else if (s->valid == VALID || s->valid == TDVALID || s->valid == BUSY || s->valid == TDBUSY) {
				if (s->ecid==0) {
					return 0;
				} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
					mask4 |= (UINT32_C(1) << (s->ecid & 0x0F));
					if (s->version>=bestecversion4) {
						bestecversion4 = s->version;
					}
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					mask8 |= (UINT32_C(1) << (s->ecid & 0x1F));
					if (s->version>=bestecversion8) {
						bestecversion8 = s->version;
					}
				}
			}
		}
	}
	if (bestversion==0) {
		uint8_t useec;
		useec = 0;
		if (bitcount(mask8)>=8 && bitcount(mask4)>=4) {
			if (bestecversion8>bestecversion4) {
				useec = 8;
			} else {
				useec = 4;
			}
		} else if (bitcount(mask8)>=8) {
			useec = 8;
		} else if (bitcount(mask4)>=4) {
			useec = 4;
		} else {
			if (flags&1) { // erase allowed?
				chunk_delete_file_int(c,sclassid,0);
				return 1;
			} else {
				c->allowreadzeros = 1;
				*nversion = (c->version | 0x80000000);
				return 1;
			}
		}
		if (useec==8) {
			c->version = bestecversion8;
			for (s=c->slisthead ; s ; s=s->next) {
				if (cstab[s->csid].valid && s->ecid>=0x20 && s->ecid<=0x30) {
					if (s->valid==WVER) {
						s->valid = BUSY;
					} else if (s->valid==TDWVER) {
						s->valid = TDBUSY;
					} else {
						continue;
					}
					s->version = bestecversion8;
					stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
					matocsserv_send_setchunkversion(cstab[s->csid].ptr,ochunkid,s->ecid,bestecversion8,0);
				}
				if ((s->ecid<0x20 || s->ecid>0x30) && (s->valid==VALID || s->valid==TDVALID)) { // we want to leave only valid EC8 parts
					s->valid = INVALID;
				}
			}
			*nversion = bestecversion8;
		} else { // useec==4
			c->version = bestecversion4;
			for (s=c->slisthead ; s ; s=s->next) {
				if (cstab[s->csid].valid && s->ecid>=0x10 && s->ecid<=0x1C) {
					if (s->valid==WVER) {
						s->valid = BUSY;
					} else if (s->valid==TDWVER) {
						s->valid = TDBUSY;
					} else {
						continue;
					}
					s->version = bestecversion4;
					stats_chunkops[CHUNK_OP_CHANGE_TRY]++;
					matocsserv_send_setchunkversion(cstab[s->csid].ptr,ochunkid,s->ecid,bestecversion4,0);
				}
				if ((s->ecid<0x10 || s->ecid>0x1C) && (s->valid==VALID || s->valid==TDVALID)) { // we want to leave only valid EC4 parts
					s->valid = INVALID;
				}
			}
			*nversion = bestecversion4;
		}
		c->interrupted = 0;
		chunk_set_op(c,SET_VERSION);
		chunk_state_fix(c);
	} else {
		c->version = bestversion;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->version==bestversion && cstab[s->csid].valid && s->ecid==0) {
				if (s->valid==WVER) {
					s->valid = VALID;
				} else if (s->valid==TDWVER) {
					s->valid = TDVALID;
				}
			}
			if (s->ecid!=0 && (s->valid==VALID || s->valid==TDVALID)) { // when we decided to use copies then we should invalidate all ec parts
				s->valid = INVALID;
			}
		}
		*nversion = bestversion;
		c->needverincrease = 1;
		chunk_state_fix(c);
	}
	return 1;
}

int chunk_mr_set_version(uint64_t chunkid,uint32_t version) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	c->allowreadzeros = (version&0x80000000)?1:0;
	c->version = version&0x3FFFFFFF;
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

uint8_t chunk_get_version_and_csdata(uint8_t mode,uint64_t chunkid,uint32_t clientip,uint32_t *version,uint8_t *count,uint8_t cs_data[100*14],uint8_t *split) {
	chunk *c;
	slist *s;
	uint8_t i;
	uint8_t cnt;
	uint8_t dmask;
	uint8_t dmask4,dmask8,minecid,maxecid;
	uint8_t *wptr;
	locsort lstab[100];

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	*version = c->version;
	cnt=0;
	dmask4 = 0;
	dmask8 = 0;
	for (s=c->slisthead ;s ; s=s->next) {
		if (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && cstab[s->csid].valid) {
			if (s->ecid==0) {
				if (cnt<100 && matocsserv_get_csdata(cstab[s->csid].ptr,clientip,&(lstab[cnt].ip),&(lstab[cnt].port),&(lstab[cnt].csver),&(lstab[cnt].labelmask))==0) {
					lstab[cnt].dist = topology_distance(lstab[cnt].ip,clientip);	// in the future prepare more sophisticated distance function
					lstab[cnt].rnd = rndu32();
					cnt++;
				}
			} else if (s->ecid>=0x10 && s->ecid<=0x13) {
				dmask4 |= (1U << (s->ecid&0x03));
			} else if (s->ecid>=0x20 && s->ecid<=0x27) {
				dmask8 |= (1U << (s->ecid&0x07));
			}
		}
	}
	*split = 0;
	if (cnt==0) {
		dmask = 0;
		minecid = 0;
		maxecid = 0;
		if (dmask8==0xFF) {
			minecid = 0x20;
			maxecid = 0x27;
			cnt = 8;
		} else if (dmask4==0x0F) {
			minecid = 0x10;
			maxecid = 0x13;
			cnt = 4;
		} else {
			*count = 0;
			if (c->all_gequiv==0 && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
				if (c->allowreadzeros) {
					*version = 0;
					return MFS_STATUS_OK;
				}
				return MFS_ERROR_CHUNKLOST; // this is permanent state - chunk is definitely lost
			} else {
				return MFS_STATUS_OK; // check later
			}
		}
		for (s=c->slisthead ;s ; s=s->next) {
			if (s->ecid>=minecid && s->ecid<=maxecid && (s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && cstab[s->csid].valid)) {
				i = s->ecid & 0x7;
				if ((dmask & (1 << i))==0) {
					matocsserv_get_csdata(cstab[s->csid].ptr,clientip,&(lstab[i].ip),&(lstab[i].port),&(lstab[i].csver),&(lstab[i].labelmask));
					lstab[i].dist = i;
					lstab[i].rnd = 0;
					dmask |= (1 << i);
				}
			}
		}
		*split = cnt;
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

uint8_t chunk_get_eights_copies(uint64_t chunkid,uint8_t *count) {
	chunk *c;
	slist *s;
	uint8_t cnt;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	cnt = 0;
	for (s=c->slisthead ; s && cnt<100 ; s=s->next) {
		if (cstab[s->csid].valid && s->valid!=DEL) {
			if (matocsserv_isvalid(cstab[s->csid].ptr)) {
				if (s->ecid==0) {
					cnt += 8;	// full copy
				} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
					cnt += 2;	// double ec part (EC4)
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					cnt++;		// single ec part (EC8)
				}
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

uint8_t chunk_get_version_and_copies(uint8_t mode,uint64_t chunkid,uint32_t clientip,uint32_t *version,uint32_t *chunkmtime,uint8_t *count,uint8_t cs_data[100*8]) {
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
	if (c->allowreadzeros) {
		*version |= 0x80000000;
	}
	*chunkmtime = c->lockedto;
	cnt=0;
	wptr = cs_data;

	for (s=c->slisthead ; s && cnt<100 ; s=s->next) {
		if (cstab[s->csid].valid && s->valid!=DEL) {
			if (matocsserv_get_csdata(cstab[s->csid].ptr,clientip,&ip,&port,NULL,NULL)==0) {
				if (mode>=1 || s->ecid==0) {
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
					if (mode>=1) {
						put8bit(&wptr,s->ecid);
					}
					cnt++;
				}
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
	if (lockedto>0 && lockedto<ts) {
		return MFS_ERROR_MISMATCH;
	}
	if (chunkid>=nextchunkid) {
		nextchunkid=chunkid+1;
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
	if (c->fhead!=FLISTNULLINDX) {
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

int chunk_mr_flagsclr(uint32_t ts,uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->lockedto>=ts) {
		return MFS_ERROR_LOCKED;
	}
	chunk_state_set_flags(c,0);
	meta_version_inc();
	return MFS_STATUS_OK;
}

static inline void chunk_mfr_state_check(chunk *c) {
	slist *s;
	uint8_t gequiv;

	gequiv = sclass_calc_goal_equivalent(sclass_get_keeparch_storagemode(c->sclassid,c->flags));
	if (c->all_gequiv>=gequiv && c->reg_gequiv<gequiv) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==TDVALID || s->valid==TDBUSY) {
				cstab[s->csid].mfr_state = REPL_IN_PROGRESS;
			}
		}
	}
}

void chunk_server_has_chunk(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint32_t version) {
	chunk *c;
	slist *s;
	uint8_t fix;
#ifndef MFSDEBUG
	static uint32_t loglastts = 0;
	static uint32_t ilogcount = 0;
	static uint32_t clogcount = 0;
#endif

	if (chunk_check_ecid(ecid)<0) { // just ignore chunks with unrecognized ecid
		return;
	}

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
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunkserver (%s) has nonexistent chunk (%016"PRIX64"_%08"PRIX32"), id looks wrong - just ignore it",matocsserv_getstrip(cstab[csid].ptr),chunkid,(version&0x7FFFFFFF));
#ifndef MFSDEBUG
			} else if (ilogcount==10) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"there are more nonexistent chunks to ignore - stop logging");
			}
			ilogcount++;
#endif
			return;
		}
#ifndef MFSDEBUG
		if (clogcount<10) {
#endif
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunkserver (%s) has nonexistent chunk (%016"PRIX64"_%08"PRIX32"), so create it for future deletion",matocsserv_getstrip(cstab[csid].ptr),chunkid,(version&0x7FFFFFFF));
#ifndef MFSDEBUG
		} else if (clogcount==10) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"there are more nonexistent chunks to create - stop logging");
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
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->csid==csid && s->ecid==ecid) {
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
			s->valid = nextvalid;
			chunk_state_fix(c);
			if (version&0x80000000) {
				chunk_mfr_state_check(c);
			}
			return;
		}
	}
	fix = 0;
	s = slist_malloc();
	s->csid = csid;
	s->ecid = ecid;
	if (c->version!=(version&0x7FFFFFFF)) {
		if (version&0x80000000) {
			s->valid = TDWVER;
		} else {
			s->valid = WVER;
		}
		s->version = version&0x7FFFFFFF;
	} else {
		if (c->writeinprogress && ecid==0) {
			matocsserv_write_counters(cstab[csid].ptr,1);
		}
		if (version&0x80000000) {
			s->valid = TDVALID;
			s->version = c->version;
			if (ecid==0 && c->storage_mode==STORAGE_MODE_COPIES && c->all_gequiv<15) {
				chunk_state_set_counters(c,STORAGE_MODE_COPIES,c->all_gequiv+1,c->reg_gequiv);
			} else {
				fix = 1;
			}
		} else {
			s->valid = VALID;
			s->version = c->version;
			if (ecid==0 && c->storage_mode==STORAGE_MODE_COPIES && c->all_gequiv<15 && c->reg_gequiv<15) {
				chunk_state_set_counters(c,STORAGE_MODE_COPIES,c->all_gequiv+1,c->reg_gequiv+1);
			} else {
				fix = 1;
			}
		}
	}
	chunk_new_copy(c,s);
	c->needverincrease = 1;
	if (fix) {
		chunk_state_fix(c);
	}
	if (version&0x80000000) {
		chunk_mfr_state_check(c);
	}
}

void chunk_damaged(uint16_t csid,uint64_t chunkid,uint8_t ecid) {
	chunk *c;
	slist *s;

	if (chunk_check_ecid(ecid)<0) { // just ignore chunks with unrecognized ecid
		return;
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		if (chunkid>nextchunkid+UINT64_C(1000000000)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"), id looks wrong - just ignore it",chunkid);
			return;
		}
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunkserver has nonexistent chunk (%016"PRIX64"), so create it for future deletion",chunkid);
		if (chunkid>=nextchunkid) {
			nextchunkid=chunkid+1;
//			changelog("%"PRIu32"|NEXTCHUNKID(%"PRIu64")",main_time(),nextchunkid);
		}
		c = chunk_new(chunkid);
		c->version = 0;
		changelog("%"PRIu32"|CHUNKADD(%"PRIu64",%"PRIu32",%"PRIu32")",main_time(),c->chunkid,c->version,c->lockedto);
	}
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->csid==csid && s->ecid==ecid) {
			if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && ecid==0) {
				matocsserv_write_counters(cstab[csid].ptr,0);
			}
			s->valid = INVALID;
			s->version = 0;
			c->needverincrease = 1;
			chunk_state_fix(c);
			chunk_priority_queue_check(c,1);
			chunk_mfr_state_check(c);
			return;
		}
	}
	s = slist_malloc();
	s->csid = csid;
	s->ecid = ecid;
	s->valid = INVALID;
	s->version = 0;
	chunk_new_copy(c,s);
	c->needverincrease = 1;
}

void chunk_lost(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t report) {
	chunk *c;
	slist **sptr,*s;

	if (chunk_check_ecid(ecid)<0) { // just ignore chunks with unrecognized ecid
		return;
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		return;
	}
	sptr=&(c->slisthead);
	while ((s=*sptr)) {
		if (s->csid==csid && s->ecid==ecid) {
			if (report) {
				if (ecid==0) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunkserver (%s) reported nonexistent chunk copy (chunkid: %016"PRIX64")",matocsserv_getstrip(cstab[csid].ptr),chunkid);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunkserver (%s) reported nonexistent chunk part (chunkid: %016"PRIX64" ; ecid: %s)",matocsserv_getstrip(cstab[csid].ptr),chunkid,chunk_ecid_to_str(ecid));
				}
			}
			if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && ecid==0) {
				matocsserv_write_counters(cstab[csid].ptr,0);
			}
			c->needverincrease = 1;
			*sptr = s->next;
			slist_free(s);
			chunk_state_fix(c);
		} else {
			sptr = &(s->next);
		}
	}
	if (c->lockedto<(uint32_t)main_time() && c->operation==NONE && c->slisthead==NULL && c->fhead==FLISTNULLINDX && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		if (c->ondangerlist) {
			chunk_priority_remove(c);
		}
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
				return MFRSTATUS_VALIDATING;
			case CAN_BE_REMOVED:
				return MFRSTATUS_READY;
			case REPL_IN_PROGRESS:
			case WAS_IN_PROGRESS:
				return MFRSTATUS_INPROGRESS;

		}
	}
	return MFRSTATUS_VALIDATING;
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
	if (csfreetail<MAXCSCOUNT) {
		cstab[csfreetail].next = csid;
	} else {
		csfreehead = csid;
	}
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
		while (startutime+(JobsTimerMiliSeconds*200)>currutime) {
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

void chunk_got_delete_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status) {
	chunk *c;
	slist *s,**st;
	uint8_t fix;

	if (chunk_check_ecid(ecid)<0) { // just ignore chunks with unrecognized ecid
		return;
	}

	if (status==MFS_ERROR_NOCHUNK) { // treat here MFS_ERROR_NOCHUNK as ok
		status = MFS_STATUS_OK;
	}

	if (status==MFS_STATUS_OK) {
		stats_chunkops[CHUNK_OP_DELETE_OK]++;
	} else {
		stats_chunkops[CHUNK_OP_DELETE_ERR]++;
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}
//	if (status!=MFS_STATUS_OK) {
//		return ;
//	}
	fix = 0;
	st = &(c->slisthead);
	while (*st) {
		s = *st;
		if (s->csid==csid && s->ecid==ecid) {
			if (status==MFS_STATUS_OK) {
				if (s->valid!=DEL) {
					fix = 1;
					if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && ecid==0) {
						matocsserv_write_counters(cstab[s->csid].ptr,0);
					}
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" - got unexpected delete status from %s",chunkid,c->version,matocsserv_getstrip(cstab[csid].ptr));
				}
				*st = s->next;
				slist_free(s);
			} else {
				if (s->valid==DEL) {
					s->valid = INVALID;
				}
				st = &(s->next);
			}
		} else {
			st = &(s->next);
		}
	}
	if (fix) {
		chunk_state_fix(c);
	}
	if (c->lockedto<(uint32_t)main_time() && c->operation==NONE && c->slisthead==NULL && c->fhead==FLISTNULLINDX && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
		changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
		if (c->ondangerlist) {
			chunk_priority_remove(c);
		}
		chunk_delete(c);
		return;
	}
	if (c->operation==NONE) {
		chunk_priority_queue_check(c,1);
	}
}

void chunk_got_replicate_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t status) {
	chunk *c;
	slist *s,**st;
	uint8_t fix;
	uint8_t finished;
	uint32_t now;

	if (chunk_check_ecid(ecid)<0) { // just ignore chunks with unrecognized ecid
		return;
	}

	if (status==MFS_STATUS_OK) {
		stats_chunkops[CHUNK_OP_REPLICATE_OK]++;
	} else {
		stats_chunkops[CHUNK_OP_REPLICATE_ERR]++;
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		return ;
	}

	now = main_time();

	if (c->operation==REPLICATE) { // high priority replication
		fix = 0;
		finished = 1;
		if (status!=0) { // just try to remove BUSY copy
			st = &(c->slisthead);
			while (*st) {
				s = *st;
				if (s->csid==csid && s->ecid==ecid && s->valid==BUSY) {
					fix = 1;
					*st = s->next;
					chunk_delopchunk(s->csid,c->chunkid);
					slist_free(s);
				} else {
					if (s->valid==BUSY) {
						finished = 0;
					}
					st = &(s->next);
				}
			}
		}
		if (fix==0) { // BUSY copy not removed
			finished = 1;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->csid==csid && s->ecid==ecid) {
					if (s->valid!=BUSY) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" - got replication status from server not set as busy (server: %s)",chunkid,version,matocsserv_getstrip(cstab[csid].ptr));
					}
					if (status!=0 || version!=c->version) {
						fix = 1;
						if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && ecid==0) {
							matocsserv_write_counters(cstab[s->csid].ptr,0);
						}
						s->valid = INVALID;
						s->version = 0;	// after unfinished operation can't be sure what version chunk has
					} else {
						if (s->valid == BUSY || s->valid == VALID) {
							s->valid = VALID;
						}
					}
					chunk_delopchunk(s->csid,c->chunkid);
				} else if (s->valid==BUSY) {
					finished = 0;
				}
			}
		}
		if (fix) {
			chunk_state_fix(c);
		}
		if (finished) {
			chunk_set_op(c,NONE);
			chunk_replock_repend(c->chunkid);
//			c->lockedto = 0;
			if (status!=MFS_ERROR_ETIMEDOUT) { // operation was timed out - in this case we want to wait a moment (chunk will be in danger queue anyway)
				chunk_io_ready_check(c);
			}
			if (c->operation==NONE && c->ondangerlist==0) {
				matoclserv_chunk_status(c->chunkid,MFS_STATUS_OK);
			}
			if (c->lockedto<now && chunk_replock_test(c->chunkid,now)==0) {
				matoclserv_chunk_unlocked(c->chunkid,c);
			}
		}
	} else { // low priority replication
		if (status!=0) {
			chunk_priority_queue_check(c,1);
			return ;
		}
		fix = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->csid==csid && s->ecid==ecid) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" - got replication status from server which had had that chunk before (server: %s)",chunkid,version,matocsserv_getstrip(cstab[csid].ptr));
				if (s->valid==VALID && version!=c->version) {
					fix = 1;
					s->valid = INVALID;
					s->version = version;
					if (c->writeinprogress && ecid==0) {
						matocsserv_write_counters(cstab[s->csid].ptr,0);
					}
				}
				if (fix) {
					chunk_state_fix(c);
				}
				chunk_priority_queue_check(c,1);
				return;
			}
		}
		s = slist_malloc();
		s->csid = csid;
		s->ecid = ecid;
		if (c->lockedto>=(uint32_t)main_time() || version!=c->version) {
			s->valid = INVALID;
		} else {
			chunk_write_counters(c,0);
			fix = 1;
			s->valid = VALID;
		}
		s->version = version;
		chunk_new_copy(c,s);
		if (fix) {
			chunk_state_fix(c);
		}
	}
	if (c->operation==NONE) {
		chunk_priority_queue_check(c,1);
	}
}

void chunk_operation_status(uint64_t chunkid,uint8_t ecid,uint8_t status,uint16_t csid,uint8_t operation) {
	chunk *c;
	uint8_t opfinished,validcopies;
	uint8_t verfixed;
	slist *s,**st;
	uint8_t fix;
	uint32_t mask4,mask8;
	uint32_t now;

	if (chunk_check_ecid(ecid)<0) { // just ignore chunks with unrecognized ecid
		return;
	}

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
		if (eop>8) {
			eop=8;
		}
		if (sop>8) {
			sop=8;
		}
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" - got unexpected status (expected: %s ; got: %s) from %s",chunkid,c->version,op_to_str(eop),op_to_str(sop),matocsserv_getstrip(cstab[csid].ptr));
	}

	now = main_time();

	fix = 0;
	validcopies = 0;
	opfinished = 1;
	mask4 = 0;
	mask8 = 0;

	st = &(c->slisthead);
	while ((s=*st)!=NULL) {
		if (s->csid==csid && s->ecid==ecid) {
			if (status!=0) {
				fix = 1;
				if (status==MFS_ERROR_NOTDONE) { // special case - this operation was not even started, so we know exact result
					if (c->operation==SET_VERSION || c->operation==TRUNCATE) { // chunk left not changed, but now it has wrong version
						if (s->valid==TDBUSY || s->valid==TDVALID) {
							s->valid = TDWVER;
						} else {
							s->valid = WVER;
						}
						s->version--;
					} else if (c->operation==CREATE || c->operation==DUPLICATE || c->operation==DUPTRUNC) { // copy not created
						if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && ecid==0) {
							matocsserv_write_counters(cstab[s->csid].ptr,0);
						}
						*st = s->next;
						slist_free(s);
						continue;
					}
				} else {
					if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && ecid==0) {
						matocsserv_write_counters(cstab[s->csid].ptr,0);
					}
					c->interrupted = 1;	// increase version after finish, just in case
					s->valid = INVALID;
					s->version = 0;	// after unfinished operation can't be sure what version chunk has
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
			if (s->ecid==0) {
				validcopies=1;
			} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
				mask4 |= UINT32_C(1) << (s->ecid & 0x0F);
			} else if (s->ecid>=0x20 && s->ecid<=0x30) {
				mask8 |= UINT32_C(1) << (s->ecid & 0x1F);
			}
		}
		st = &(s->next);
	}
	if (opfinished && validcopies==0 && (c->operation==SET_VERSION || c->operation==TRUNCATE)) { // we know that version increase was just not completed, so all WVER chunks with version exactly one lower than chunk version are actually VALID copies
		verfixed = 0;
		for (s=c->slisthead ; s!=NULL ; s=s->next) {
			if (s->version+1==c->version && s->ecid==0) {
				if (s->valid==TDWVER) {
					verfixed = 1;
					s->valid = TDVALID;
					fix = 1;
				} else if (s->valid==WVER) {
					verfixed = 1;
					s->valid = VALID;
					fix = 1;
				}
			}
		}
		if (verfixed) {
			c->version--;
			c->allowreadzeros = 0;
			changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",now,c->chunkid,c->version);
		}
		// we continue because we still want to return status not done to matoclserv module
	}
	if (fix) {
		chunk_state_fix(c);
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
		if ((validcopies && ecid==0) || (bitcount(mask4)>=4 && ecid>=0x10 && ecid<=0x1C) || (bitcount(mask8)>=8 && ecid>=0x20 && ecid<=0x30)) {
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"operation finished, chunk: %016"PRIX64" ; op: %s ; interrupted: %u",c->chunkid,op_to_str(c->operation),c->interrupted);
			if (c->interrupted) {
				chunk_emergency_increase_version(c);
			} else {
				chunk_set_op(c,NONE);
				c->needverincrease = 0;
				matoclserv_chunk_status(c->chunkid,MFS_STATUS_OK);
				if (c->lockedto<now && chunk_replock_test(c->chunkid,now)==0) {
					matoclserv_chunk_unlocked(c->chunkid,c);
				}
			}
		} else {
			chunk_set_op(c,NONE);
			if (nospace) {
				matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOSPACE);
			} else {
				matoclserv_chunk_status(c->chunkid,MFS_ERROR_NOTDONE);
			}
		}
	}
}

void chunk_got_chunkop_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status) {
	chunk_operation_status(chunkid,ecid,status,csid,NONE);
}

void chunk_got_create_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status) {
	chunk_operation_status(chunkid,ecid,status,csid,CREATE);
}

void chunk_got_duplicate_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status) {
	chunk_operation_status(chunkid,ecid,status,csid,DUPLICATE);
}

void chunk_got_setversion_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status) {
	chunk_operation_status(chunkid,ecid,status,csid,SET_VERSION);
}

void chunk_got_truncate_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status) {
	chunk_operation_status(chunkid,ecid,status,csid,TRUNCATE);
}

void chunk_got_duptrunc_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status) {
	chunk_operation_status(chunkid,ecid,status,csid,DUPTRUNC);
}

void chunk_got_localsplit_status(uint16_t csid,uint64_t chunkid,uint32_t version,uint8_t status) {
	chunk *c;
	uint8_t fix;
	slist *s;
	uint32_t now;

	if (status==MFS_STATUS_OK) {
		stats_chunkops[CHUNK_OP_SPLIT_OK]++;
	} else {
		stats_chunkops[CHUNK_OP_SPLIT_ERR]++;
	}

	c = chunk_find(chunkid);
	if (c==NULL) {
		return;
	}

	if (c->operation!=LOCALSPLIT) {
		uint8_t eop;
		eop = c->operation;
		if (eop>8) {
			eop=8;
		}
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" - got unexpected status (expected: %s ; got: LOCALSPLIT) from %s",chunkid,c->version,op_to_str(eop),matocsserv_getstrip(cstab[csid].ptr));
		return;
	}

	if (chunk_remove_disconnected_chunks(c)) {
		return;
	}

	now = main_time();

	fix = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->csid==csid && s->ecid>=0x10 && (s->valid==BUSY || s->valid==TDBUSY)) {
			if (status!=0 || version!=c->version) {
				fix = 1;
				/* not needed since ecid is always > 0 
				if (c->writeinprogress && s->valid!=INVALID && s->valid!=DEL && s->valid!=WVER && s->valid!=TDWVER && s->ecid==0) {
					matocsserv_write_counters(cstab[s->csid].ptr,0);
				}
				*/
				s->valid = INVALID;
				s->version = 0;
			} else {
				if (s->valid==TDBUSY || s->valid==TDVALID) {
					s->valid=TDVALID;
				} else {
					s->valid=VALID;
				}
			}
		}
	}
	chunk_delopchunk(csid,chunkid);
	if (fix) {
		chunk_state_fix(c);
	}
	// this is single chunk operation, so is always finished
	chunk_set_op(c,NONE);
	chunk_replock_repend(c->chunkid);
//	c->lockedto = 0;
	chunk_io_ready_check(c);
	if (c->operation==NONE && c->ondangerlist==0) {
		matoclserv_chunk_status(c->chunkid,MFS_STATUS_OK);
	}
	if (c->lockedto<now && chunk_replock_test(c->chunkid,now)==0) {
		matoclserv_chunk_unlocked(c->chunkid,c);
	}
	if (c->operation==NONE) {
		chunk_priority_queue_check(c,1);
	}
}

void chunk_got_status_data(uint64_t chunkid,const char *servdesc,uint16_t csid,uint8_t parts,uint8_t *ecid,uint32_t *version,uint8_t *damaged,uint16_t *blocks,uint8_t fixmode) {
	chunk *c;
	slist *s,*si,**sp,*slisthead;
	uint8_t i,error;
	char msgbuff[1024];
	char *buff;
	int32_t leng;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return;
	}
	if (c->operation!=NONE) {
		return;
	}
	(void)blocks; // currently unused parameter
	slisthead = NULL;
	for (i=0 ; i<parts ; i++) {
		s = slist_malloc();
		s->csid = csid;
		s->ecid = ecid[i];
		s->version = version[i]&0x7FFFFFFF;
		if (damaged[i]) {
			s->valid = INVALID;
		} else if (version[i]&0x80000000) {
			if (s->version!=c->version) {
				s->valid = TDWVER;
			} else {
				s->valid = TDVALID;
			}
		} else {
			if (s->version!=c->version) {
				s->valid = WVER;
			} else {
				s->valid = VALID;
			}
		}
		sp = &(slisthead);
		while ((si=*sp)!=NULL) {
			if (si->ecid<s->ecid) {
				sp = &(si->next);
			} else {
				break;
			}
		}
		s->next = *sp;
		*sp = s;
	}
	si = slisthead;
	error = 0;
	for (s=c->slisthead ; s!=NULL && error==0 ; s=s->next) {
		if (s->csid==csid) {
			if (s->valid==DEL) {
				if (si!=NULL && s->ecid==si->ecid) {
					si = si->next;
				}
			} else if (si==NULL) {
				error = 1;
			} else if (s->ecid!=si->ecid) {
				error = 1;
			} else if (s->version!=si->version) {
				error = 2;
			} else if (s->valid == BUSY || s->valid == TDBUSY) {
				error = 3;
			} else if (s->valid != si->valid) {
				error = 4;
			} else {
				si = si->next;
			}
		}
	}
	if (si!=NULL) {
		error = 1;
	}
	if (error) {
		leng = 0;
		buff = msgbuff;
		if (leng<1024) {
			leng += snprintf(buff+leng,1024-leng,"chunk %016"PRIX64"_%08"PRIX32": copies/parts on server %s mismatch ; master data:",chunkid,c->version,servdesc);
		}
		i = 0;
		for (s=c->slisthead ; s!=NULL ; s=s->next) {
			if (s->csid==csid) {
				if (leng<1024) {
					leng += snprintf(buff+leng,1024-leng,"%c%s:%08X:%s",(i==0)?' ':',',chunk_ecid_to_str(s->ecid),s->version,valid_to_str(s->valid));
				}
				i = 1;
			}
		}
		if (leng<1024) {
			leng += snprintf(buff+leng,1024-leng," ; chunkserver data:");
		}
		i = 0;
		for (s=slisthead ; s!=NULL ; s=s->next) {
			if (leng<1024) {
				leng += snprintf(buff+leng,1024-leng,"%c%s:%08X:%s",(i==0)?' ':',',chunk_ecid_to_str(s->ecid),s->version,valid_to_str(s->valid));
			}
			i = 1;
		}
		if (leng<1024) {
			buff[leng] = '\0';
		} else {
			buff[1023] = '\0';
		}
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"%s",msgbuff);
		if (fixmode) {
			sp = &(c->slisthead);
			while ((si=*sp)!=NULL) {
				if (si->csid<csid) {
					sp = &(si->next);
				} else if (si->csid==csid) {
					*sp = si->next;
					slist_free(si);
				} else {
					break;
				}
			}
			if (slisthead) {
				*sp = slisthead;
				sp = &(slisthead);
				while ((s=*sp)!=NULL) {
					sp = &(s->next);
				}
				*sp = si;
				slisthead = NULL;
			}
		}
	}
	for (si = slisthead ; si ; si = s) {
		s = si->next;
		slist_free(si);
	}
}

uint8_t chunk_no_more_pending_jobs(void) {
	return (opsinprogress==0)?1:0;
}

/* ----------------------- */
/* JOBS (DELETE/REPLICATE) */
/* ----------------------- */

uint32_t chunk_store_info(uint8_t *buff) {
	// 24 * 32bit = 24 * 4 = 96B
	if (buff!=NULL) {
		put32bit(&buff,chunksinfo_loopstart);
		put32bit(&buff,chunksinfo_loopend);
		put32bit(&buff,chunksinfo.fixed);
		put32bit(&buff,chunksinfo.forcekeep);
		put32bit(&buff,chunksinfo.delete_invalid);
		put32bit(&buff,chunksinfo.delete_no_longer_needed);
		put32bit(&buff,chunksinfo.delete_wrong_version);
		put32bit(&buff,chunksinfo.delete_duplicated_ecpart);
		put32bit(&buff,chunksinfo.delete_excess_ecpart);
		put32bit(&buff,chunksinfo.delete_excess_copy);
		put32bit(&buff,chunksinfo.delete_diskclean_ecpart);
		put32bit(&buff,chunksinfo.delete_diskclean_copy);
		put32bit(&buff,chunksinfo.replicate_dupserver_ecpart);
		put32bit(&buff,chunksinfo.replicate_needed_ecpart);
		put32bit(&buff,chunksinfo.replicate_needed_copy);
		put32bit(&buff,chunksinfo.replicate_wronglabels_ecpart);
		put32bit(&buff,chunksinfo.replicate_wronglabels_copy);
		put32bit(&buff,chunksinfo.split_copy_into_ecparts);
		put32bit(&buff,chunksinfo.join_ecparts_into_copy);
		put32bit(&buff,chunksinfo.recover_ecpart);
		put32bit(&buff,chunksinfo.calculate_ecchksum);
		put32bit(&buff,chunksinfo.locked_unused);
		put32bit(&buff,chunksinfo.locked_used);
		put32bit(&buff,chunksinfo.replicate_rebalance);
	}
	return 96;
}

static inline uint8_t chunk_mindist(uint16_t csid,uint32_t ip[255],uint8_t ipcnt) {
	uint8_t mindist,dist,k;
	uint32_t sip;
	mindist = TOPOLOGY_DIST_MAX;
	sip = matocsserv_server_get_ip(cstab[csid].ptr);
	for (k=0 ; k<ipcnt && mindist>0 ; k++) {
		dist=topology_distance(sip,ip[k]);
		if (dist<mindist) {
			mindist = dist;
		}
	}
	return mindist;
}

// first servers in the same rack (server id), then other (yes, same rack is better than same physical server, so order is 1 and then 0 or 2)
static inline void chunk_rack_sort(uint16_t servers[MAXCSCOUNT],uint16_t servcount,uint32_t ip[255],uint8_t ipcnt) {
	int16_t i,j;
	uint16_t csid;
	uint8_t mindist;

	if (servcount==0 || ipcnt==0) {
		return;
	}
	i = 0;
	j = servcount-1;
	while (i<=j) {
		while (i<=j) {
			mindist = chunk_mindist(servers[i],ip,ipcnt);
			if (mindist!=TOPOLOGY_DIST_SAME_RACKID) {
				break;
			}
			i++;
		}
		while (i<=j) {
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

static inline uint16_t chunk_get_undergoal_replicate_srccsid(chunk *c,uint16_t dstcsid,uint32_t now,double repl_limit_read,uint32_t rgvc,uint32_t rgtdc) {
	slist *s;
	uint32_t r = 0;
	uint16_t srccsid = MAXCSCOUNT;

	if (ReplicationsRespectTopology) {
		uint32_t min_dist = UINT32_C(0xFFFFFFFF);
		uint8_t tdcflag = 0;
		uint32_t dist;
		uint32_t ip;
		uint32_t cuip;
		uint32_t cnt;
		uint32_t frnd,rnd;

		rnd = frnd = rndu32_ranged(3628800); // 3628800 = 10!
		cuip = matocsserv_server_get_ip(cstab[dstcsid].ptr);
		cnt = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read && (s->valid==VALID || (tdcflag==0 && s->valid==TDVALID))) {
				ip = matocsserv_server_get_ip(cstab[s->csid].ptr);
				dist = topology_distance(ip,cuip);
				if ((tdcflag==0 && s->valid==VALID) || (dist<min_dist)) {
					if (s->valid==VALID) {
						tdcflag = 1;
					}
					min_dist = dist;
					srccsid = s->csid;
					cnt = 1;
					rnd = frnd;
				} else if (dist==min_dist) {
					cnt++;
					if (cnt<=10) {
						if ((rnd%cnt)==0) {
							srccsid = s->csid;
						}
						rnd /= cnt;
					} else {
						if (rndu32_ranged(cnt)==0) {
							srccsid = s->csid;
						}
					}
				}
			}
		}
	} else {
		if (rgvc>0) {	// if there are VALID copies then make copy of one VALID chunk
			r = 1+rndu32_ranged(rgvc);
			for (s=c->slisthead ; s && r>0 ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read && s->valid==VALID && s->ecid==0) {
					r--;
					srccsid = s->csid;
				}
			}
		} else {	// if not then use TDVALID chunks.
			r = 1+rndu32_ranged(rgtdc);
			for (s=c->slisthead ; s && r>0 ; s=s->next) {
				if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read && s->valid==TDVALID && s->ecid==0) {
					r--;
					srccsid = s->csid;
				}
			}
		}
	}
	return srccsid;
}

static inline uint8_t chunk_replicate(uint8_t repication_mode,uint32_t now,chunk *c,uint8_t ec_data_parts,uint8_t ecid,uint16_t src,uint16_t dst,void *user,uint16_t *survivorscsid,uint8_t *survivorsecid,uint8_t reason) {
	slist *s;
	int res;
	uint8_t i;
	uint8_t part;
	void* survivorsptrs[8];

	if (chunk_check_ecid(ecid)<0) { // do not replicate wrong ecid
		return 1;
	}

	if (ecid>=0x20 && ecid<=0x30) {
		part = ecid & 0x1F;
	} else if (ecid>=0x10 && ecid<=0x1C) {
		part = ecid & 0x0F;
	} else {
		part = 0;
	}

	(void)user;

	for (s=c->slisthead ; s!=NULL ; s=s->next) { // check for duplicates
		if (s->csid==dst && s->ecid==ecid) {
			if (ecid!=0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't replicate chunk %016"PRIX64"_%08"PRIX32" part: %s : found duplicate",c->chunkid,c->version,chunk_ecid_to_str(ecid));
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't replicate chunk %016"PRIX64"_%08"PRIX32" : found duplicate",c->chunkid,c->version);
			}
			return 1;
		}
	}
	chunk_delay_protect(c->chunkid);
	stats_chunkops[CHUNK_OP_REPLICATE_TRY]++;
	switch (repication_mode) {
		case SIMPLE:
			res = matocsserv_send_replicatechunk(cstab[dst].ptr,c->chunkid,ecid,c->version,cstab[src].ptr,reason);
			break;
		case SPLIT:
			res = matocsserv_send_replicatechunk_split(cstab[dst].ptr,c->chunkid,ecid,c->version,cstab[src].ptr,0,part,ec_data_parts,reason);
			break;
		case RECOVER:
			for (i=0 ; i<ec_data_parts ; i++) {
				survivorsptrs[i] = cstab[survivorscsid[i]].ptr;
			}
			res = matocsserv_send_replicatechunk_recover(cstab[dst].ptr,c->chunkid,ecid,c->version,ec_data_parts,survivorsptrs,survivorsecid,reason);
			break;
		case JOIN:
			for (i=0 ; i<ec_data_parts ; i++) {
				survivorsptrs[i] = cstab[survivorscsid[i]].ptr;
			}
			res = matocsserv_send_replicatechunk_join(cstab[dst].ptr,c->chunkid,ecid,c->version,ec_data_parts,survivorsptrs,survivorsecid,reason);
			break;
		default:
			res = -1;
	}
	if (res<0) {
		const char* rmodestr = (repication_mode==SIMPLE)?"SIMPLE":(repication_mode==SPLIT)?"SPLIT":(repication_mode==RECOVER)?"RECOVER":(repication_mode==JOIN)?"JOIN":"???";
		if (ecid!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" part: %s : error sending replicate (%s) command",c->chunkid,c->version,chunk_ecid_to_str(ecid),rmodestr);
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" : error sending replicate (%s) command",c->chunkid,c->version,rmodestr);
		}
		return 1;
	}
	chunk_addopchunk(dst,c->chunkid);
	chunk_set_op(c,REPLICATE);
	chunk_replock_repstart(c->chunkid,now);
	s = slist_malloc();
	s->csid = dst;
	s->ecid = ecid;
	s->valid = BUSY;
	s->version = c->version;
	chunk_new_copy(c,s);
	return 0;
}

static inline int chunk_undergoal_replicate(chunk *c,uint16_t dstcsid,uint32_t now,double repl_limit_read,uint8_t extrajob,uint16_t chunk_priority,loop_info *inforec,uint32_t rgvc,uint32_t rgtdc) {
	uint16_t srccsid;

	srccsid = chunk_get_undergoal_replicate_srccsid(c,dstcsid,now,repl_limit_read,rgvc,rgtdc);
	if (srccsid==MAXCSCOUNT) {
		return -1;
	}
	if (chunk_replicate(SIMPLE,now,c,0,0,srccsid,dstcsid,NULL,NULL,NULL,(chunk_priority==CHUNK_PRIORITY_IOREADY)?REPL_COPY_IO:(chunk_priority<=CHUNK_PRIORITY_ONEREGCOPY_PLUSMFR)?REPL_COPY_ENDANGERED:(chunk_priority<=CHUNK_PRIORITY_UNDERGOAL)?REPL_COPY_UNDERGOAL:REPL_COPY_WRONGLABEL)!=0) {
		return -1;
	}
	if (extrajob==0) {
		if (chunk_priority<=CHUNK_PRIORITY_UNDERGOAL) {
			inforec->replicate_needed_copy++;
		} else if (chunk_priority==CHUNK_PRIORITY_WRONGLABELS) {
			inforec->replicate_wronglabels_copy++;
		}
	}
	return 0;
}

static inline uint8_t chunk_check_label_for_ec_part(uint16_t servid,storagemode *sm,uint8_t chksumflag,int32_t *ec_both_labels_limit) {
	if (sm->labelscnt==0) {
		return 1;
	}
	if (sm->ec_data_chksum_parts) {
		if (sm->labelscnt==1) {
			return (matocsserv_server_matches_labelexpr(cstab[servid].ptr,sm->labelexpr[0]))?1:0;
		} else if (sm->labelscnt==2) {
			uint8_t datamatch,chksummatch;
			datamatch = matocsserv_server_matches_labelexpr(cstab[servid].ptr,sm->labelexpr[0]);
			chksummatch = matocsserv_server_matches_labelexpr(cstab[servid].ptr,sm->labelexpr[1]);
			if (datamatch && chksummatch && (*ec_both_labels_limit)>0) {
				(*ec_both_labels_limit)--;
				return 1;
			} else if (datamatch && chksumflag==0) {
				return 1;
			} else if (chksummatch && chksumflag) {
				return 1;
			}
		} else { // wrong definition !!! - allow everything
			return 1;
		}
	} else { // it is possible that we want to go from EC to COPY mode, but need a server for a missing part - in such case we will use labels from COPIES
		uint8_t i;
		for (i=0 ; i<sm->labelscnt ; i++) {
			if (matocsserv_server_matches_labelexpr(cstab[servid].ptr,sm->labelexpr[i])) {
				return 1;
			}
		}
	}
	return 0;
}

static inline uint16_t chunk_get_available_servers_for_parts(uint16_t *servers,chunk *c,storagemode *sm,uint8_t strict_mode,uint8_t data_recovery_mode,int32_t *ec_both_labels_limit,uint8_t minecid,uint8_t maxecid,uint8_t chksumflag,uint16_t srcservcount,uint16_t *srcsids) {
	uint16_t servcnt;
	uint16_t extraservcnt;
	uint16_t i;
	slist *s;

	servcnt = 0;
	extraservcnt = 0;
	for (i=0 ; i<srcservcount ; i++) {
		for (s=c->slisthead ; s && (s->csid!=srcsids[i] || s->ecid<minecid || s->ecid>maxecid) ; s=s->next) {}
		if (s==NULL) {
			if (chunk_check_label_for_ec_part(srcsids[i],sm,chksumflag,ec_both_labels_limit)) {
				servers[servcnt++] = srcsids[i];
			} else {
				extraservcnt++;
				servers[MAXCSCOUNT-extraservcnt] = srcsids[i];
			}
		}
	}
	if (strict_mode==0) {
		for (i=1 ; i<=extraservcnt ; i++) {
			servers[servcnt++] = servers[MAXCSCOUNT-i];
		}
	}
	if (servcnt==0 && data_recovery_mode) { // recover mode - try to return something
		extraservcnt = 0;
		for (i=0 ; i<srcservcount ; i++) {
			if (chunk_check_label_for_ec_part(srcsids[i],sm,chksumflag,ec_both_labels_limit)) {
				servers[servcnt++] = srcsids[i];
			} else {
				extraservcnt++;
				servers[MAXCSCOUNT-extraservcnt] = srcsids[i];
			}
		}
		if (strict_mode==0 && sclass_get_labels_mode(c->sclassid,sm)==LABELS_MODE_STD) { // in data recovery mode use all labels in 'default' labels mode
			for (i=1 ; i<=extraservcnt ; i++) {
				servers[servcnt++] = servers[MAXCSCOUNT-i];
			}
		}
	}
	return servcnt;
}

static inline uint8_t chunk_fix_wrong_version_ec(chunk *c,uint8_t gequiv,uint8_t ecidmask,uint8_t minecid,uint8_t maxecid) {
	slist *s;
	uint32_t bestversionmask;
	uint32_t bestversion;
	bestversion = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ecid>=minecid && s->ecid<=maxecid && (s->valid==WVER || s->valid==TDWVER)) {
			if (s->version>=bestversion) {
				bestversion = s->version;
			}
		}
	}
	if (bestversion>0 && ((bestversion+1)==c->version || (uint32_t)(c->version)+1==bestversion)) {
		bestversionmask = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->ecid>=minecid && s->ecid<=maxecid && (s->valid==WVER || s->valid==TDWVER) && s->version==bestversion) {
				bestversionmask |= (UINT32_C(1) << (s->ecid & ecidmask));
			}
		}
		if (bitcount(bestversionmask)>=gequiv) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64" has only ec parts with wrong version - fixing it",c->chunkid);
			c->version = bestversion;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->ecid>=minecid && s->ecid<=maxecid && s->version==bestversion && cstab[s->csid].valid) {
					if (s->valid == WVER) {
						s->valid = VALID;
					} else if (s->valid == TDWVER) {
						s->valid = TDVALID;
					}
				}
				if (s->valid == WVER || s->valid == TDWVER) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - wrong versioned EC part on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - valid EC part on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
				}
			}
			c->allowreadzeros = 0;
			changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
			return 1;
		}
	}
	return 0;
}

// after return
// eccsid maps ecid -> csid;
// survivorsecid has ecids of survivors ready to use
// survivorscsid has csids of survivors ready to use
static inline uint32_t chunk_find_ec_survivors(chunk *c,uint8_t ec_data_parts,uint8_t ecidmask,uint8_t minecid,uint8_t maxecid,uint32_t now,double repl_limit_read,uint16_t *eccsid,uint8_t *survivorsecid,uint16_t *survivorscsid) {
	slist *s;
	uint8_t survmap[32];
	uint32_t readysurvmask = 0;
	uint32_t i,j,mask;
	uint32_t survivorsmask = 0;

	for (s=c->slisthead ; s ; s=s->next) {
		if ((s->valid == VALID || s->valid == TDVALID) && s->ecid>=minecid && s->ecid<=maxecid && matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read) {
			readysurvmask |= (UINT32_C(1) << (s->ecid & ecidmask));
		}
	}

	if (bitcount(readysurvmask)>=ec_data_parts) {
		for (i=0,mask=1,j=0 ; i<32 ; i++,mask<<=1) {
			if (readysurvmask & mask && j<ec_data_parts) {
				survmap[i]=j;
				j++;
			} else {
				survmap[i]=0xFF;
			}
		}

		// assert j=ec_data_parts;
		for (i=0 ; i<32 ; i++) {
			eccsid[i] = 0;
		}
		for (s=c->slisthead ; s ; s=s->next) {
			if ((s->valid == VALID || s->valid == TDVALID) && s->ecid>=minecid && s->ecid<=maxecid && matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read) {
				uint8_t ecindx = s->ecid & ecidmask;
				if (survmap[ecindx]<ec_data_parts) {
					survivorscsid[survmap[ecindx]] = s->csid;
					survivorsecid[survmap[ecindx]] = s->ecid;
					survivorsmask |= (UINT32_C(1) << ecindx);
				}
				eccsid[ecindx] = s->csid;
			}
		}
	}
	return survivorsmask;
}

//jobs state: jobshpos

void chunk_do_jobs(chunk *c,uint8_t mode,uint32_t now,uint8_t extrajob) {
	slist *s,*sf;
	static uint16_t *dcsids = NULL;
	static uint16_t dservcount;
//	static uint16_t *bcsids;
//	static uint16_t bservcount;
	static uint16_t *rcsids = NULL;
	uint16_t scount,fullservers,replservers;
	uint16_t rservcount;
	uint8_t rservallflag;
	uint16_t srccsid,dstcsid;
	uint16_t preferedsrccsid;
	uint8_t survivorsecid4[4];
	uint16_t survivorscsid4[4];
	uint16_t eccsid4[32];
	uint32_t survivorsmask4;
	uint8_t survivorsecid8[8];
	uint16_t survivorscsid8[8];
	uint16_t eccsid8[32];
	uint32_t survivorsmask8;
	uint8_t chunk_priority;
	uint8_t enqueue;
	uint8_t repl_limit_class;
	double repl_limit_read,repl_limit_write;
	double repl_read_counter;
	uint16_t i,j,k;
	uint8_t done;
	uint16_t vc,tdc,ivc,bc,tdb,dc,wvc,tdw;
	uint32_t vcmask4,tdcmask4,ivcmask4,bcmask4,tdbmask4,dcmask4,wvcmask4,tdwmask4;
	uint32_t vcmask8,tdcmask8,ivcmask8,bcmask8,tdbmask8,dcmask8,wvcmask8,tdwmask8;
	uint32_t overmask4,overmask8;
	uint32_t mfrmask4,mfrmask8;
	uint32_t mask,wlmask;
	int32_t ltotalec4csid,ltotalec8csid;
	int32_t ldataec4csid,ldataec8csid;
	int32_t lchksumec4csid,lchksumec8csid;
	int32_t lregec4csid,lregec8csid;
	int32_t lallec4csid,lallec8csid;
	uint16_t regec4uniqserv;
	uint16_t regec8uniqserv;
	uint16_t allec4uniqserv;
	uint16_t allec8uniqserv;
	uint16_t totalec4uniqserv;
	uint16_t totalec8uniqserv;
	uint16_t dataec4uniqserv;
	uint16_t dataec8uniqserv;
	uint16_t chksumec4uniqserv;
	uint16_t chksumec8uniqserv;
	uint16_t validec4uniqserv;
	uint16_t validec8uniqserv;
	uint16_t validdataec4uniqserv;
	uint16_t validdataec8uniqserv;
	uint16_t validchksumec4uniqserv;
	uint16_t validchksumec8uniqserv;
	uint32_t allecgoalequiv,regularecgoalequiv,validecgoalequiv;
	uint8_t storage_mode;
	uint8_t labels_mode;
	uint8_t ec_strict_mode;
	int32_t dataec_both_labels_limit;
	int32_t chksumec_both_labels_limit;
	uint8_t goal,ec_data_parts,ec_chksum_parts,minecid,maxecid,ecidmask;
	uint8_t parts_on_the_same_server,lecid;
	uint8_t can_delete_invalid_chunks;
	int32_t lcsid;
	int32_t lcsid4,lcsid8;
	uint8_t repecid;
	uint8_t usekeep;
	uint8_t dontdelete;
	uint8_t overloaded;
	double maxdiff;
	static loop_info inforec;
	static uint32_t delnotdone;
	static uint32_t deldone;
	static uint32_t prevtodeletecount;
	static uint32_t delloopcnt;
	storagemode *sm;
//	uint8_t **labelexpr;
//	uint32_t labelcnt;
//	uint32_t uniqmask;
	static uint16_t *servers = NULL;
	static uint8_t *ecids = NULL;
	uint32_t servcnt,extraservcnt;
	int32_t *matching;
//	uint32_t forcereplication;
	uint32_t vrip[255];
	uint8_t vripcnt;

	if (servers==NULL) {
		servers = malloc(sizeof(uint16_t)*MAXCSCOUNT);
		passert(servers);
	}
	if (ecids==NULL) {
		ecids = malloc(sizeof(uint8_t)*MAXCSCOUNT);
		passert(ecids);
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
		if (mode==JOBS_INIT) { // init tasks
			delnotdone = 0;
			deldone = 0;
			prevtodeletecount = 0;
			delloopcnt = 0;
			memset(&inforec,0,sizeof(loop_info));
			dservcount = 0;
		} else if (mode==JOBS_EVERYLOOP) { // every loop tasks
			delloopcnt++;
			if (delloopcnt>=16) {
				uint32_t todeletecount = deldone+delnotdone;
				delloopcnt=0;
				if ((delnotdone > deldone) && (todeletecount > prevtodeletecount)) {
					TmpMaxDelFrac *= 1.5;
					if (TmpMaxDelFrac>MaxDelHardLimit) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"DEL_LIMIT hard limit (%"PRIu32" per server) reached",MaxDelHardLimit);
						TmpMaxDelFrac=MaxDelHardLimit;
					}
					TmpMaxDel = TmpMaxDelFrac;
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"DEL_LIMIT temporary increased to: %"PRIu32" per server",TmpMaxDel);
				}
				if ((todeletecount < prevtodeletecount) && (TmpMaxDelFrac > MaxDelSoftLimit)) {
					TmpMaxDelFrac /= 1.5;
					if (TmpMaxDelFrac<MaxDelSoftLimit) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"DEL_LIMIT back to soft limit (%"PRIu32" per server)",MaxDelSoftLimit);
						TmpMaxDelFrac = MaxDelSoftLimit;
					}
					TmpMaxDel = TmpMaxDelFrac;
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"DEL_LIMIT decreased back to: %"PRIu32" per server",TmpMaxDel);
				}
				prevtodeletecount = todeletecount;
				delnotdone = 0;
				deldone = 0;
			}
			chunksinfo = inforec;
			memset(&inforec,0,sizeof(inforec));
			chunksinfo_loopstart = chunksinfo_loopend;
			chunksinfo_loopend = now;
		} else if (mode==JOBS_EVERYTICK) {
			dservcount = 0;
//			bservcount=0;
		} else if (mode==JOBS_TERM) {
			if (servers!=NULL) {
				free(servers);
			}
			if (ecids!=NULL) {
				free(ecids);
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

	scount = matocsserv_servers_count();
	fullservers = matocsserv_almostfull_servers();
	replservers = matocsserv_replallowed_servers();

// ----------------------------------------------
// remove all disconnected copies from structures
// ----------------------------------------------

	if (chunk_remove_disconnected_chunks(c)) {
		job_exit_reasons[c->sclassid][DISCONNECTED_CHUNKSERVER]++;
		return;
	}

	if (c->lockedto < now) {
		chunk_write_counters(c,0);
	}

// --------------------------------------------
// calculate number of valid and invalid copies
// --------------------------------------------

	vc = 0;
	tdc = 0;
	ivc = 0;
	bc = 0;
	tdb = 0;
	dc = 0;
	wvc = 0;
	tdw = 0;
	vcmask4 = 0;
	tdcmask4 = 0;
	ivcmask4 = 0;
	bcmask4 = 0;
	tdbmask4 = 0;
	dcmask4 = 0;
	wvcmask4 = 0;
	tdwmask4 = 0;
	overmask4 = 0;
	vcmask8 = 0;
	tdcmask8 = 0;
	ivcmask8 = 0;
	bcmask8 = 0;
	tdbmask8 = 0;
	dcmask8 = 0;
	wvcmask8 = 0;
	tdwmask8 = 0;
	overmask8 = 0;
	parts_on_the_same_server = 0;
	lcsid4 = -1;
	lcsid8 = -1;
	lregec4csid = -1;
	lregec8csid = -1;
	lallec4csid = -1;
	lallec8csid = -1;
	ltotalec4csid = -1;
	ltotalec8csid = -1;
	ldataec4csid = -1;
	ldataec8csid = -1;
	lchksumec4csid = -1;
	lchksumec8csid = -1;
	regec4uniqserv = 0;
	regec8uniqserv = 0;
	allec4uniqserv = 0;
	allec8uniqserv = 0;
	totalec4uniqserv = 0;
	totalec8uniqserv = 0;
	dataec4uniqserv = 0;
	dataec8uniqserv = 0;
	chksumec4uniqserv = 0;
	chksumec8uniqserv = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->ecid>=0x20 && s->ecid<=0x30) {
			mask = (UINT32_C(1) << (s->ecid & 0x1F));
		} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
			mask = (UINT32_C(1) << (s->ecid & 0x0F));
		} else {
			mask = 0;
		}
		if (s->ecid>=0x10 && s->ecid<=0x1C) {
			if (s->csid!=ltotalec4csid) {
				totalec4uniqserv++;
			}
			ltotalec4csid = s->csid;
			if (s->ecid<0x14) {
				if (s->csid!=ldataec4csid) {
					dataec4uniqserv++;
				}
				ldataec4csid = s->csid;
			} else {
				if (s->csid!=lchksumec4csid) {
					chksumec4uniqserv++;
				}
				lchksumec4csid = s->csid;
			}
		} else if (s->ecid>=0x20 && s->ecid<=0x30) {
			if (s->csid!=ltotalec8csid) {
				totalec8uniqserv++;
			}
			ltotalec8csid = s->csid;
			if (s->ecid<0x28) {
				if (s->csid!=ldataec8csid) {
					dataec8uniqserv++;
				}
				ldataec8csid = s->csid;
			} else {
				if (s->csid!=lchksumec8csid) {
					chksumec8uniqserv++;
				}
				lchksumec8csid = s->csid;
			}
		}
		if (s->valid==VALID || s->valid==TDVALID || s->valid==BUSY || s->valid==TDBUSY) {
			if (s->ecid>=0x10 && s->ecid<=0x1C) {
				if (s->csid!=lallec4csid) {
					allec4uniqserv++;
				}
				lallec4csid = s->csid;
			} else if (s->ecid>=0x20 && s->ecid<=0x30) {
				if (s->csid!=lallec8csid) {
					allec8uniqserv++;
				}
				lallec8csid = s->csid;
			}
			if (s->valid==VALID || s->valid==BUSY) {
				if (s->ecid>=0x10 && s->ecid<=0x1C) {
					if (s->csid!=lregec4csid) {
						regec4uniqserv++;
					}
					lregec4csid = s->csid;
				} else if (s->ecid>=0x20 && s->ecid<=0x30) {
					if (s->csid!=lregec8csid) {
						regec8uniqserv++;
					}
					lregec8csid = s->csid;
				}
			}
		}
		switch (s->valid) {
		case INVALID:
			if (s->ecid&0x20) {
				ivcmask8 |= mask;
			} else if (s->ecid&0x10) {
				ivcmask4 |= mask;
			} else {
				ivc++;
			}
			break;
		case TDVALID:
			if (s->ecid&0x20) {
				tdcmask8 |= mask;
			} else if (s->ecid&0x10) {
				tdcmask4 |= mask;
			} else {
				tdc++;
			}
			break;
		case VALID:
			if (s->ecid&0x20) {
				if (vcmask8&mask) {
					overmask8 |= mask;
				}
				vcmask8 |= mask;
				if (lcsid8==s->csid) {
					parts_on_the_same_server = 1;
				}
				lcsid8 = s->csid;
			} else if (s->ecid&0x10) {
				if (vcmask4&mask) {
					overmask4 |= mask;
				}
				vcmask4 |= mask;
				if (lcsid4==s->csid) {
					parts_on_the_same_server = 1;
				}
				lcsid4 = s->csid;
			} else {
				vc++;
			}
			break;
		case TDBUSY:
			if (s->ecid&0x20) {
				tdbmask8 |= mask;
			} else if (s->ecid&0x10) {
				tdbmask4 |= mask;
			} else {
				tdb++;
			}
			break;
		case BUSY:
			if (s->ecid&0x20) {
				bcmask8 |= mask;
			} else if (s->ecid&0x10) {
				bcmask4 |= mask;
			} else {
				bc++;
			}
			break;
		case DEL:
			if (s->ecid&0x20) {
				dcmask8 |= mask;
			} else if (s->ecid&0x10) {
				dcmask4 |= mask;
			} else {
				dc++;
			}
			break;
		case WVER:
			if (s->ecid&0x20) {
				wvcmask8 |= mask;
			} else if (s->ecid&0x10) {
				wvcmask4 |= mask;
			} else {
				wvc++;
			}
			break;
		case TDWVER:
			if (s->ecid&0x20) {
				tdwmask8 |= mask;
			} else if (s->ecid&0x10) {
				tdwmask4 |= mask;
			} else {
				tdw++;
			}
			break;
		}
	}
	(void)dcmask4; // currently not used
	(void)dcmask8; // - " -
	mfrmask8 = (vcmask8^tdcmask8)&tdcmask8; // same as (tdcmask8&~vcmask8) bitwise: tdcmask8 - vcmask8
	mfrmask4 = (vcmask4^tdcmask4)&tdcmask4;

	ltotalec4csid = -1;
	ltotalec8csid = -1;
	ldataec4csid = -1;
	ldataec8csid = -1;
	lchksumec4csid = -1;
	lchksumec8csid = -1;
	validec4uniqserv = 0;
	validec8uniqserv = 0;
	validdataec4uniqserv = 0;
	validdataec8uniqserv = 0;
	validchksumec4uniqserv = 0;
	validchksumec8uniqserv = 0;
	for (s=c->slisthead ; s ; s=s->next) {
		if (s->valid==VALID || s->valid==TDVALID) {
			if (s->ecid>=0x10 && s->ecid<=0x1C) {
				if (s->csid!=ltotalec4csid) {
					validec4uniqserv++;
				}
				ltotalec4csid = s->csid;
				if (s->ecid<0x14) {
					if (s->csid!=ldataec4csid) {
						validdataec4uniqserv++;
					}
					ldataec4csid = s->csid;
				} else {
					if (s->csid!=lchksumec4csid) {
						validchksumec4uniqserv++;
					}
					lchksumec4csid = s->csid;
				}
			} else if (s->ecid>=0x20 && s->ecid<=0x30) {
				if (s->csid!=ltotalec8csid) {
					validec8uniqserv++;
				}
				ltotalec8csid = s->csid;
				if (s->ecid<0x28) {
					if (s->csid!=ldataec8csid) {
						validdataec8uniqserv++;
					}
					ldataec8csid = s->csid;
				} else {
					if (s->csid!=lchksumec8csid) {
						validchksumec8uniqserv++;
					}
					lchksumec8csid = s->csid;
				}
			}
		}
	}

// ---------------------------------------------------
// calculate EC redundancy levels and goal equivalents
// ---------------------------------------------------

	regularecgoalequiv = chunk_calc_ecge(vcmask8|bcmask8,vcmask4|bcmask4,regec8uniqserv,regec4uniqserv,&storage_mode);
	allecgoalequiv = chunk_calc_ecge(vcmask8|tdcmask8|bcmask8|tdbmask8,vcmask4|tdcmask4|bcmask4|tdbmask4,allec8uniqserv,allec4uniqserv,&storage_mode);
	if ((uint32_t)(vc+tdc+bc+tdb) >= allecgoalequiv) {
		storage_mode = STORAGE_MODE_COPIES;
	}

// ----------------------
// check operation status
// ----------------------

	if ((tdb|bc|tdbmask8|bcmask8|tdbmask4|bcmask4)==0 && c->operation!=NONE) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk in the middle of the operation %s, but no chunk server is busy - finish operation",c->chunkid,c->version,op_to_str(c->operation));
		chunk_set_op(c,NONE);
		inforec.fixed++;
	}

// ----------------------------------
// get current storage mode for chunk
// ----------------------------------

// EC + LABELS_MODE
//
// 1. definitions with single label expression
// counters:
// - RLA = repl_allowed servers matching labels (sm->replallowed)
// - VLA = all_valid servers matching labels (sm->allvalid)
// - RA = repl_allowed servers (replservers)
// - VA = all_valid servers (scount)
// - N = number of data parts (ec_data_parts)
// - X = number of checksum parts (ec_chksum_parts)
// * EC works in two modes:
//	- only matched servers (ec_strict_mode == 1)
//	- all servers (ec_strict_mode == 0)
// * COPY -> EC condition
//	- STRICT (RLA >= N+2X)
//	- DEFULT (RLA >= N+2X)
//	- LOOSE (RA >= N+2X)
// * EC -> COPY condition
//	- STRICT (VLA < N+X)
//	- DEFAULT (VA < N+X)
//	- LOOSE (VA < N+X)
// * EC mode condition
//	- STRICT (always 1)
//	- DEFAULT
//		1 when RLA >= N+X
//		0 otherwise
//	- LOOSE (always 0)
//
// 2. definitions with two label expressions
// additional counters:
// - RLD = repl_allowed servers matching data labels (sm->data_replallowed)
// - VLD = all_valid servers matching data labels (sm->data_allvalid)
// - RLC = repl_allowed servers matching chksum labels (sm->chksum_replallowed)
// - VLC = all_valid servers matching chksum labels (sm->chksum_allvalid)
// - RLB = repl_allowed servers matching both labels (sm->both_replallowed)
// - VLB = all_valid servers matching both labels (sm->both_allvalid)
// - UN = data parts on servers matching both labels
// - UX = chksum parts on servers matching both labels
// Note: RLA = RLD + RLC + RLB / VLA = VLD + VLC + VLB
// * number of data and checksum parts on servers with both labels are limited to:
// - (VLC + VLB - 2X) - UN for data parts (dataec_both_labels_limit)
// - (VLD + VLB - (N+X)) - UX for checksum parts (chksumec_both_labels_limit)
// * COPY -> EC condition:
//	- STRICT (RLA >= N+2X && RLD+RLB >= N+X && RLC+RLB >= 2X)
//	- DEFULT (RLA >= N+2X && RLD+RLB >= N+X && RLC+RLB >= 2X)
//	- LOOSE (RA >= N+2X)
// * EC -> COPY condition:
//	- STRICT (VLA < N+X || VLD+VLB < N || VLC+VLB < X)
//	- DEFAULT (VA < N+X)
//	- LOOSE (VA < N+X)
// * EC mode condition
//	- STRICT (always 1)
//	- DEFAULT
//		1 when (N+X <= RLA && N <= RLD+RLB && X <= RLC+RLB)
//		0 otherwise
//	- LOOSE (always 0)

	usekeep = 0;
	ec_strict_mode = 0;
	dataec_both_labels_limit = 0;
	chksumec_both_labels_limit = 0;
	sm = sclass_get_keeparch_storagemode(c->sclassid,c->flags);
	if (sm->ec_data_chksum_parts) {
		labels_mode = chunk_get_labels_mode_for_ec(sm,c->sclassid);
		usekeep = chunk_check_forcekeep_condidiotns_for_ec(sm,labels_mode,vc>0,vcmask4!=0,vcmask8!=0,replservers,scount);
		ec_strict_mode = usekeep>>4;
		usekeep &= 0xF;
		ec_data_parts = sm->ec_data_chksum_parts >> 4;
		ec_chksum_parts = sm->ec_data_chksum_parts & 0xF;
		if (usekeep==0 && sm->labelscnt==2) { // calculate "both_labels_limits"
			uint16_t ec_data_parts_on_both;
			uint16_t ec_chksum_parts_on_both;
			ec_data_parts_on_both = 0;
			ec_chksum_parts_on_both = 0;
			for (s=c->slisthead ; s ; s=s->next) {
				if ((s->ecid>=0x10 && s->ecid<=0x13) || (s->ecid>=0x20 && s->ecid<=0x27)) {
					if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[0])
							&& matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[1])) {
						ec_data_parts_on_both++;
					}
				}
				if ((s->ecid>=0x14 && s->ecid<=0x1C) || (s->ecid>=0x28 && s->ecid>=0x30)) {
					if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[0])
							&& matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[1])) {
						ec_chksum_parts_on_both++;
					}
				}
			}
			dataec_both_labels_limit = (sm->chksum_allvalid + sm->both_allvalid) - 2 * ec_chksum_parts - ec_data_parts_on_both;
			chksumec_both_labels_limit = (sm->data_allvalid + sm->both_allvalid) - (ec_data_parts + ec_chksum_parts) - ec_chksum_parts_on_both;
		}
		if (usekeep) {
			if (usekeep==2 && inforec.forcekeep<10) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": not enough servers to safely convert to EC format (%u servers needed) - using KEEP mode",c->chunkid,c->version,ec_chksum_parts*2+ec_data_parts);
			}
			if (usekeep==3 && inforec.forcekeep<10) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": not enough servers to maintain EC format (%u servers needed) - using KEEP mode",c->chunkid,c->version,ec_chksum_parts+ec_data_parts);
			}
			if (usekeep>=2 && inforec.forcekeep==10) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"there are more chunks that cannot be converted to EC or has to be converted back to copy format - no more messages in this loop - change definition of storage classes or add more servers");
			}
			sm = sclass_get_keeparch_storagemode(c->sclassid,0);
			inforec.forcekeep++;
		}
	}
	if (sm->ec_data_chksum_parts) { // when usekeep>0 then sm here is different, so this condition is not the same as the previous one
		ec_data_parts = sm->ec_data_chksum_parts >> 4;
		ec_chksum_parts = sm->ec_data_chksum_parts & 0xF;
		goal = ec_chksum_parts+1;
	} else {
		ec_data_parts = 0;
		ec_chksum_parts = 0;
		goal = sm->labelscnt;
	}
	if (ec_data_parts) {
		if (scount<ec_data_parts) { // just in case
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"storage class %"PRIu8" : internal error - KEEP mode with EC defined",c->sclassid);
			job_exit_reasons[c->sclassid][INTERNAL_ERROR]++;
			return;
		}
		if (goal+(ec_data_parts-1)>scount) { // just in case
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"reducing redundancy level due to lack of servers (servcnt:%u ; goal:%u->%d)\n",scount,goal,scount-(ec_data_parts-1));
			goal = scount-(ec_data_parts-1);
		}
	} else {
		if (goal>scount) {
			goal = scount;
		}
	}
	if (ec_data_parts==8) {
		minecid = 0x20;
		maxecid = 0x30;
		ecidmask = 0x1F;
	} else if (ec_data_parts==4) {
		minecid = 0x10;
		maxecid = 0x1C;
		ecidmask = 0x0F;
	} else if (ec_data_parts==0) {
		minecid = 0;
		maxecid = 0;
		ecidmask = 0;
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"wrong EC mode (%u) - ignoring chunk",ec_data_parts);
		job_exit_reasons[c->sclassid][INTERNAL_ERROR]++;
		return;
	}

// --------------------------
// fix mark_for_removal state
// --------------------------

	if (vc + bc + regularecgoalequiv < goal && (tdc|tdb|tdcmask8|tdbmask8|tdcmask4|tdbmask4) != 0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid == TDVALID || s->valid == TDBUSY) {
				cstab[s->csid].mfr_state = REPL_IN_PROGRESS;
			}
		}
	}

//

	validecgoalequiv = chunk_calc_ecge(vcmask8|tdcmask8,vcmask4|tdcmask4,0xFF,0xFF,&storage_mode);
//	validecgoalequiv = bitcount(vcmask8|tdcmask8);
//	if (validecgoalequiv>7) {
//		validecgoalequiv -= 7;
//	} else {
//		validecgoalequiv = 0;
//	}

// -----------------------------------------
// check if there are unexpected BUSY copies
// -----------------------------------------

	if (c->lockedto<now && chunk_replock_test(c->chunkid,now)==0) {
		if ((tdb|bc|tdbmask8|bcmask8|tdbmask4|bcmask4)!=0 && c->operation==NONE) {
			if (tdc+vc+validecgoalequiv>0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": unexpected BUSY copies - fixing",c->chunkid,c->version);
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->valid == BUSY) {
						s->valid = INVALID;
						s->version = 0;
					} else if (s->valid == TDBUSY) {
						s->valid = INVALID;
						s->version = 0;
					}
				}
				inforec.fixed++;
				job_exit_reasons[c->sclassid][DELETED_UNEXPECTED_BUSY_COPY_OR_PART]++;
				return;
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": unexpected BUSY copies - can't fix",c->chunkid,c->version);
			}
		}

// -------------------------------------------------------------
// chunk after 'repair' that can be read - remove allowread flag
// -------------------------------------------------------------

//		if (c->allowreadzeros && ((tdc+vc+tdb+bc)>0 || allecgoalequiv>0)) {
//			c->allowreadzeros = 0;
//			changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
//		}

// -----------------------------------------
// if possible fix chunk with wrong versions
// -----------------------------------------

		if (tdc+vc+tdb+bc+allecgoalequiv==0 && (wvc|tdw|wvcmask8|tdwmask8|wvcmask4|tdwmask4)!=0 && c->fhead>FLISTNULLINDX/* c->flisthead */) {
			uint32_t bestversion;
			bestversion = 0;
			if ((tdw+wvc)>=goal) {
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->ecid==0 && (s->valid==WVER || s->valid==TDWVER)) {
						if (s->version>=bestversion) {
							bestversion = s->version;
						}
					}
				}

				if (bestversion>0 && ((bestversion+1)==c->version || (uint32_t)(c->version)+1==bestversion)) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64" has only copies (%"PRIu32") with wrong version - fixing it",c->chunkid,wvc+tdw);
					c->version = bestversion;
					for (s=c->slisthead ; s ; s=s->next) {
						if (s->ecid==0 && s->version==bestversion && cstab[s->csid].valid) {
							if (s->valid == WVER) {
								s->valid = VALID;
							} else if (s->valid == TDWVER) {
								s->valid = TDVALID;
							}
						}
						if (s->valid == WVER || s->valid == TDWVER) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - wrong versioned copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
						} else {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - valid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
						}
					}
					c->needverincrease = 1;
					c->allowreadzeros = 0;
					changelog("%"PRIu32"|SETVERSION(%"PRIu64",%"PRIu32")",(uint32_t)main_time(),c->chunkid,c->version);
					job_exit_reasons[c->sclassid][FIXED_COPY_VERSION]++;
					return;
				}
			} else if (bitcount(wvcmask8|tdwmask8)>=7+goal) {
				if (chunk_fix_wrong_version_ec(c,7+goal,0x1F,0x20,0x30)) {
					job_exit_reasons[c->sclassid][FIXED_EC8_VERSION]++;
					return;
				}
			} else if (bitcount(wvcmask4|tdwmask4)>=3+goal) {
				if (chunk_fix_wrong_version_ec(c,7+goal,0x0F,0x10,0x1C)) {
					job_exit_reasons[c->sclassid][FIXED_EC4_VERSION]++;
					return;
				}
			}
		}

		if (tdc+vc+tdb+bc+allecgoalequiv==0 && (wvc|tdw|ivc|wvcmask8|tdwmask8|ivcmask8|wvcmask4|tdwmask4|ivcmask4)!=0 && c->fhead>FLISTNULLINDX) {
			if ((wvc|tdw|wvcmask8|tdwmask8|wvcmask4|tdwmask4)==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64" has only invalid copies (%"PRIu32") - please repair it manually",c->chunkid,ivc);
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64" has only copies with wrong versions (%"PRIu32") - please repair it manually",c->chunkid,wvc+tdw);
			}
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==INVALID) {
					if (s->ecid==0) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - invalid copy on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
					} else {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - invalid part %s on (%s - ver:%08"PRIX32")",c->chunkid,c->version,chunk_ecid_to_str(s->ecid),matocsserv_getstrip(cstab[s->csid].ptr),s->version);
					}
				} else {
					if (s->ecid==0) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - copy with wrong version on (%s - ver:%08"PRIX32")",c->chunkid,c->version,matocsserv_getstrip(cstab[s->csid].ptr),s->version);
					} else {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunk %016"PRIX64"_%08"PRIX32" - part %s with wrong version on (%s - ver:%08"PRIX32")",c->chunkid,c->version,chunk_ecid_to_str(s->ecid),matocsserv_getstrip(cstab[s->csid].ptr),s->version);

					}
				}
			}
			job_exit_reasons[c->sclassid][NO_VALID_COPIES_AND_PARTS]++;
			return;
		}

		if (c->slisthead==NULL && c->fhead>FLISTNULLINDX) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": there are no copies",c->chunkid,c->version);
			job_exit_reasons[c->sclassid][NO_COPIES_AND_PARTS]++;
			return;
		}
	}

	dontdelete = chunk_delay_is_protected(c->chunkid);

// -----------------------------------------------
// delete redundant invalid/wrong version EC parts
// -----------------------------------------------
#if 0 
	if (dontdelete==0) {
		uint32_t redundantmask4;
		uint32_t redundantmask8;
		redundantmask4 = (extrajob==0)?dcmask4:0;
		redundantmask8 = (extrajob==0)?dcmask8:0;
		redundantmask4 = ((vcmask4|tdcmask4|bcmask4|tdbmask4) & (wvcmask4|tdwmask4|ivcmask4|redundantmask4));
		redundantmask8 = ((vcmask8|tdcmask8|bcmask8|tdbmask8) & (wvcmask8|tdwmask8|ivcmask8|redundantmask8));
		if (redundantmask4!=0 || redundantmask8!=0) { // we have invalid/wver and valid parts with the same ID - we can delete them
			uint8_t actions=0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->ecid>=0x20 && s->ecid<=0x30) {
					mask = (UINT32_C(1) << (s->ecid & 0x1F));
					mask &= redundantmask8;
				} else if (s->ecid>=0x10 && s->ecid<=0x1C) {
					mask = (UINT32_C(1) << (s->ecid & 0x0F));
					mask &= redundantmask4;
				} else {
					mask = 0;
				}
				if (mask!=0) { // this part is redundant - we can proceed
					if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
						if (s->valid==WVER || s->valid==TDWVER || s->valid==INVALID || (extrajob==0 && s->valid==DEL)) {
							if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,0,OP_DEL_INVALID)<0) {
								if (s->valid!=DEL) {
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": can't delete chunk (delete command already sent)",c->chunkid,c->version);
								}
							} else {
								if (s->valid==DEL) {
									mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk hasn't been deleted since previous loop - retry",c->chunkid,c->version);
								}
								inforec.delete_invalid++;
								s->valid = DEL;
								stats_chunkops[CHUNK_OP_DELETE_TRY]++;
								if (extrajob==0) {
									deldone++;
								}
								actions=1;
							}
						}
					} else {
						if (s->valid==WVER || s->valid==TDWVER || s->valid==INVALID) {
							if (extrajob==0) {
								delnotdone++;
							}
						}
					}
				}
			}
			if (actions) { // we did something, so no more actions here
				job_exit_reasons[c->sclassid][DELETED_SOME_REDUNDANT_INVALID_PARTS]++;
				return;
			}
		}
	}
#endif

// ----------------------------------------------
// delete invalid/wrong version copies / EC parts
// ----------------------------------------------

	if (dontdelete==0) {
		can_delete_invalid_chunks = 0;
		if (c->fhead==FLISTNULLINDX && c->lockedto<now && chunk_replock_test(c->chunkid,now)==0) { // deleted chunk
			can_delete_invalid_chunks = 1;
		} else if (regularecgoalequiv>=goal || (vc+bc)>=goal) { // we have enough parts / copies
			can_delete_invalid_chunks = 1;
		} else if (regularecgoalequiv>0 || (vc+bc)>0) { // we have something to recover from, but maybe all servers are occupied?
			if (ec_strict_mode==0) { // EC loose mode or copy mode
				if (totalec4uniqserv>=replservers || totalec8uniqserv>=replservers || vc+tdc+bc+tdb+ivc+wvc+tdw>=replservers) {
					can_delete_invalid_chunks = 2; // in such case - delete only one
				}
				if (extrajob==0 && vc+tdc+bc+tdb+ivc+wvc+tdw+dc>=replservers) {
					can_delete_invalid_chunks = 2; // in such case - delete only one
				}
			} else if (sm->labelscnt==1) { // one label EC in strict mode
				if (totalec4uniqserv>=sm->replallowed || totalec8uniqserv>=sm->replallowed) {
					can_delete_invalid_chunks = 2; // in such case - delete only one
				}
			} else if (sm->labelscnt==2) { // two labels EC in strict mode
				if (dataec4uniqserv>=sm->data_replallowed+sm->both_replallowed || chksumec4uniqserv>=sm->chksum_replallowed+sm->both_replallowed) {
					can_delete_invalid_chunks = 2; // in such case - delete only one
				}
				if (dataec8uniqserv>=sm->data_replallowed+sm->both_replallowed || chksumec8uniqserv>=sm->chksum_replallowed+sm->both_replallowed) {
					can_delete_invalid_chunks = 2; // in such case - delete only one
				}
			}
		}
		if (can_delete_invalid_chunks) {
			uint8_t actions=0;
			for (s=c->slisthead ; s ; s=s->next) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					if (s->valid==WVER || s->valid==TDWVER || s->valid==INVALID || (extrajob==0 && s->valid==DEL)) {
						if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,0,OP_DEL_INVALID)<0) {
							if (s->valid!=DEL) {
								mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": can't delete chunk (delete command already sent)",c->chunkid,c->version);
							}
						} else {
							if (s->valid==DEL) {
								mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk hasn't been deleted since previous loop - retry",c->chunkid,c->version);
							}
							inforec.delete_invalid++;
							s->valid = DEL;
							stats_chunkops[CHUNK_OP_DELETE_TRY]++;
							if (extrajob==0) {
								deldone++;
							}
							if (can_delete_invalid_chunks==2) {
								job_exit_reasons[c->sclassid][DELETED_INVALID_COPY_OR_PART_TO_MAKE_SPACE]++;
								return;
							}
							actions=1;
						}
					}
				} else {
					if (s->valid==WVER || s->valid==TDWVER || s->valid==INVALID) {
						if (extrajob==0) {
							delnotdone++;
						}
					}
				}
			}
			if (actions) { // we did something, so no more actions here
				job_exit_reasons[c->sclassid][DELETED_SOME_INVALID_COPIES_OR_PARTS]++;
				return;
			}
		}
	}

// ---------------------------------
// check for unfinished replications
// ---------------------------------

	if (extrajob == 0) {
		if ((c->operation==REPLICATE || c->operation==LOCALSPLIT) && chunk_replock_test(c->chunkid,now)==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": chunk hasn't been replicated since previous loop - cancel",c->chunkid,c->version);
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==TDBUSY || s->valid==BUSY) {
					s->valid = INVALID;
					s->version = 0;	// after unfinished operation can't be sure what version chunk has
					chunk_delopchunk(s->csid,c->chunkid);
				}
			}
			chunk_set_op(c,NONE);
			chunk_replock_repend(c->chunkid);
//			c->lockedto = 0;
			matoclserv_chunk_unlocked(c->chunkid,c);
			inforec.fixed++;
			job_exit_reasons[c->sclassid][FOUND_NOT_FINISHED_REPLICATION]++;
			return;
		}
	}

// ----------------------------------------
// return if chunk is during some operation
// ----------------------------------------

	if (c->operation!=NONE || (c->lockedto>=now) || chunk_replock_test(c->chunkid,now)) {
		if (extrajob == 0) {
			if (c->fhead==FLISTNULLINDX) {
				inforec.locked_unused++;
			} else {
				inforec.locked_used++;
				if (goal > vc+bc+regularecgoalequiv && vc+tdc+bc+tdb+allecgoalequiv > 0) {
					if (c->operation!=NONE) {
						if (c->operation!=REPLICATE && c->operation!=LOCALSPLIT) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": can't replicate chunk - operation %s in progress",c->chunkid,c->version,op_to_str(c->operation));
						}
					} else {
						if (c->lockedto<=now+LOCKTIMEOUT) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"chunk %016"PRIX64"_%08"PRIX32": can't replicate chunk - chunk is being modified (locked for next %"PRIu32" second%s)",c->chunkid,c->version,(uint32_t)(1+c->lockedto-now),(c->lockedto==now)?"":"s");
						}
					}
				}
			}
		}
		job_exit_reasons[c->sclassid][CHUNK_IS_BEING_MODIFIED]++;
		return ;
	}

//	assert(c->lockedto < now && c->writeinprogress == 0)

// ----------------
// check busy count
// ----------------

	if ((bc|tdb|bcmask8|tdbmask8|bcmask4|tdbmask4)!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" has unexpected BUSY copies",c->chunkid,c->version);
		job_exit_reasons[c->sclassid][UNEXPECTED_BUSY_COPY_OR_PART]++;
		return;
	}

// -------------------
// delete unused chunk
// -------------------

	if (extrajob==0 && c->fhead==FLISTNULLINDX/* c->flisthead==NULL */) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"unused - delete");
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==VALID || s->valid==TDVALID) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_NOTUSED)>=0) {
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
						inforec.delete_no_longer_needed++;
						deldone++;
					} else {
						delnotdone++;
					}
				} else {
					delnotdone++;
				}
			}
		}
		job_exit_reasons[c->sclassid][DELETED_UNUSED_CHUNK]++;
		return;
	}

// --------------------------------------------------------------------------------------------------------------------
// if chunk has enough valid copies and more than one copy with wrong version then delete all copies with wrong version
// --------------------------------------------------------------------------------------------------------------------
/*
	if (dontdelete==0 && extrajob==0 && vc+regularecgoalequiv >= goal && (wvc|wvcmask8)!=0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid == WVER) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,0)>=0) {
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
						inforec.delete_wrong_version++;
						deldone++;
					} else {
						delnotdone++;
					}
				} else {
					delnotdone++;
				}
			}
		}
		return;
	}
*/
// --------------------------------------
// calculate current chunk priority level
// --------------------------------------

	if (extrajob<2) {
		chunk_priority = chunk_calculate_endanger_priority(c,1);
	} else {
		chunk_priority = 0;
	}

// ------------------------------------------------
// check if there is something more important to do
// ------------------------------------------------

	if (extrajob==0) {
		for (i=0 ; i<chunk_priority ; i++) {
			if (chunk_priority_should_block(i)) {
				if (chunk_priority<DANGER_PRIORITIES) {
					chunk_priority_enqueue(chunk_priority,c);
				}
				job_exit_reasons[c->sclassid][BLOCKED_BY_HIGHER_PRIORITY_QUEUE]++;
				return;
			}
		}
	}

	if (extrajob<2 && csdb_stop_chunk_jobs()) {
		if (chunk_priority<DANGER_PRIORITIES) {
			chunk_priority_enqueue(chunk_priority,c);
		}
		job_exit_reasons[c->sclassid][BLOCKED_BY_CHUNKSERVER_IN_MAINTENANCE_MODE]++;
		return;
	}

// ----------------------------
// calculate replication limits
// ----------------------------

	if (chunk_priority>0) {
		repl_limit_class = 1 - CHUNK_PRIORITY_HIGH(chunk_priority);
	} else {
		repl_limit_class = 4;
	}
	repl_limit_read = MaxReadRepl[repl_limit_class];
	repl_limit_write = MaxWriteRepl[repl_limit_class];

// -------------------------------------------------------------------------------------
// check if EC parts occupy all servers and there are 'mark for removal' parts to delete
// -------------------------------------------------------------------------------------

	if (dontdelete==0 && regularecgoalequiv<goal && ((ec_data_parts==8 && tdcmask8!=0) || (ec_data_parts==4 && tdcmask4!=0)) &&
		chunk_priority_is_empty(CHUNK_PRIORITY_ONECOPY_HIGHGOAL) && 
		chunk_priority_is_empty(CHUNK_PRIORITY_ONECOPY_ANY) && 
		chunk_priority_is_empty(CHUNK_PRIORITY_ONEREGCOPY_PLUSMFR)) {

		uint8_t can_delete_mark_for_removal;

		can_delete_mark_for_removal = 0;
		if (ec_strict_mode==0) { // EC loose mode or copy mode
			if ((ec_data_parts==4 && validec4uniqserv>=replservers) || (ec_data_parts==8 && validec8uniqserv>=replservers)) {
				can_delete_mark_for_removal = 1;
			}
		} else if (sm->labelscnt==1) { // one label EC in strict mode
			if ((ec_data_parts==4 && validec4uniqserv>=sm->replallowed) || (ec_data_parts==8 && validec8uniqserv>=sm->replallowed)) {
				can_delete_mark_for_removal = 1;
			}
		} else if (sm->labelscnt==2) { // two labels EC in strict mode
			if (ec_data_parts==4 && (validdataec4uniqserv>=sm->data_replallowed+sm->both_replallowed || validchksumec4uniqserv>=sm->chksum_replallowed+sm->both_replallowed)) {
				can_delete_mark_for_removal = 1;
			}
			if (ec_data_parts==8 && (validdataec8uniqserv>=sm->data_replallowed+sm->both_replallowed || validchksumec8uniqserv>=sm->chksum_replallowed+sm->both_replallowed)) {
				can_delete_mark_for_removal = 1;
			}
		}

		if (can_delete_mark_for_removal) {
			for (s=c->slisthead ; s ; s=s->next) {
				if ((s->valid==TDVALID) && s->ecid>=minecid && s->ecid<=maxecid) {
					if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
						if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
							s->valid = DEL;
							stats_chunkops[CHUNK_OP_DELETE_TRY]++;
							inforec.delete_diskclean_ecpart++;
							job_exit_reasons[c->sclassid][DELETED_MFR_PART_TO_MAKE_SPACE]++;
							return;
						}
					}
				}
			}

			chunk_priority_enqueue(chunk_priority,c);
			job_exit_reasons[c->sclassid][CANT_DELETE_MFR_PART_TO_MAKE_SPACE]++;
			return;
		}
	}

// --------------------------------------
// two or more parts on the same server ?
// --------------------------------------

	if (ec_data_parts && parts_on_the_same_server) {
		uint8_t checkindex;
		uint32_t overmask;
		lcsid = -1;
		lecid = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid) {
				if (lcsid==s->csid) {
					break;
				}
				lecid = s->ecid;
				lcsid = s->csid;
			}
		}
		// lcsid = csid with more than one part

		// one copy is extra checksum? - just delete it
		sf = NULL;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==VALID && s->ecid>=minecid+(ec_data_parts-1)+goal) {
				if (s->csid==lcsid) {
					break;
				} else if (sf==NULL) {
					sf = s;
				}
			}
		}
		if (s==NULL && sf!=NULL) {
			s = sf;
		}
		if (s!=NULL) {
			if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
				if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
					s->valid = DEL;
					stats_chunkops[CHUNK_OP_DELETE_TRY]++;
					inforec.delete_excess_ecpart++;
					deldone++;
				} else {
					delnotdone++;
					chunk_priority_enqueue(chunk_priority,c);
				}
			} else {
				delnotdone++;
				chunk_priority_enqueue(chunk_priority,c);
			}
			job_exit_reasons[c->sclassid][DELETED_PART_ON_THE_SAME_SERVER]++;
			return;
		}
		// we have duplicates? - one copy is duplicate then delete it
		overmask = (ec_data_parts==8)?overmask8:overmask4;
		if (overmask>0) {
			sf = NULL;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid && (overmask&(1<<(s->ecid&ecidmask)))) {
					if (s->csid==lcsid) {
						break;
					} else if (sf==NULL) {
						sf = s;
					}
				}
			}
			if (s==NULL && sf!=NULL) {
				s = sf;
			}
			if (s!=NULL) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
						inforec.delete_excess_ecpart++;
						deldone++;
					} else {
						delnotdone++;
						chunk_priority_enqueue(chunk_priority,c);
					}
				} else {
					delnotdone++;
					chunk_priority_enqueue(chunk_priority,c);
				}
				job_exit_reasons[c->sclassid][DELETED_DUPLICATED_PART]++;
				return;
			}
		}
		// not - then replicate it
		if (sm->labelscnt>0) { // find part with wrong label (if one has wrong label then better to move this one)
			if (sm->labelscnt==1) {
				checkindex = 0;
			} else {
				checkindex = 1;
			}
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid && s->csid==lcsid) {
					if ((s->ecid & ecidmask) < ec_data_parts) {
						if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[0])==0) {
							lecid = s->ecid;
							break;
						}
					} else {
						if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[checkindex])==0) {
							lecid = s->ecid;
							break;
						}
					}
				}
			}
		}
		done = 0;
		if (matocsserv_replication_read_counter(cstab[lcsid].ptr,now)<repl_limit_read) {
			uint16_t clcsid,wlcsid;
			uint8_t clfound,wlfound;
			uint8_t chksumflag;
			// replicate copy with csid==lcsid and ecid==lecid to another server
			chksumflag = ((lecid&ecidmask) < ec_data_parts)?0:1;
			rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
			clfound = 0;
			wlfound = 0;
			clcsid = 0; // silence stupid compilers
			wlcsid = 0; // silence stupid dompilers
			for (i=0 ; i<rservcount && clfound==0 ; i++) {
				for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid<minecid || s->ecid>=minecid+(ec_data_parts-1)+goal) ; s=s->next) {}
				if (s==NULL) {
					if (chunk_check_label_for_ec_part(rcsids[i],sm,chksumflag,(chksumflag?(&chksumec_both_labels_limit):(&dataec_both_labels_limit)))) {
						clcsid = rcsids[i];
						clfound = 1;
					} else {
						if (wlfound==0) {
							wlcsid = rcsids[i];
							wlfound = 1;
						}
					}
				}
			}
			if (clfound==0 && wlfound && ec_strict_mode==0) {
				clcsid = wlcsid;
				clfound = 1;
			}
			if (clfound) {
				if (chunk_replicate(SIMPLE,now,c,ec_data_parts,lecid,lcsid,clcsid,NULL,NULL,NULL,(extrajob==2)?RECOVER_IO:(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
					job_exit_reasons[c->sclassid][ERROR_REPLICATING_DUPLICATED_PART]++;
					return;
				}
				done = 1;
				inforec.replicate_dupserver_ecpart++;
			} else if (rservallflag==0) {
				chunk_priority_enqueue(chunk_priority,c);
			}
		} else {
			chunk_priority_enqueue(chunk_priority,c);
		}
		if (done) {
			job_exit_reasons[c->sclassid][REPLICATED_DUPLICATED_PART]++;
		} else {
			job_exit_reasons[c->sclassid][CANT_FIX_PARTS_ON_THE_SAME_SERVER]++;
		}
		return;
	}

// -----------------------------------------------
// if chunk has extra ec copies then delete them
// -----------------------------------------------

	if ((ec_data_parts==8 && overmask8>0) || (ec_data_parts==4 && overmask4>0)) {
		uint32_t overmask;

		overmask = (ec_data_parts==8)?overmask8:overmask4;

		enqueue = 0;
		for (j=0,mask=1 ; j<(ec_data_parts-1)+goal ; j++,mask<<=1) {
			if (overmask & mask) {
				uint8_t overcnt,prevdone;

				overcnt = 0;
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->ecid>=minecid && s->ecid<=maxecid && (s->ecid&ecidmask)==j && s->valid==VALID) {
						overcnt++;
					}
				}
				if (overcnt>1) {
					if (dservcount==0) {
						dservcount = matocsserv_getservers_ordered(dcsids);
					}
					delnotdone+=overcnt-1;
					prevdone = 1;

					if (sm->labelscnt) { // ec - labels version
						servcnt = 0;
						extraservcnt = 0;
						for (i=0 ; i<dservcount ; i++) {
							for (s=c->slisthead ; s ; s=s->next) {
								if (s->csid==dcsids[dservcount-1-i] && s->ecid==(minecid+j) && s->valid==VALID) {
									if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[(sm->labelscnt==1)?0:(j<ec_data_parts)?0:1])) {
										extraservcnt++;
										servers[MAXCSCOUNT-extraservcnt] = s->csid;
									} else {
										servers[servcnt++] = s->csid;
									}
									break;
								}
							}
						}
						for (i=1 ; i<=extraservcnt ; i++) {
							servers[servcnt++] = servers[MAXCSCOUNT-i];
						}
						for (i=0 ; i<servcnt && overcnt>1 && prevdone; i++) {
							for (s=c->slisthead ; s && (s->csid!=servers[i] || s->ecid!=(minecid+j)) ; s=s->next) {}
							if (s && s->valid==VALID) {
								if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
									if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
										s->valid = DEL;
										stats_chunkops[CHUNK_OP_DELETE_TRY]++;
										inforec.delete_duplicated_ecpart++;
										deldone++;
										delnotdone--;
										overcnt--;
									} else {
										prevdone=0;
									}
								} else {
									prevdone=0;
								}
							}
						}
					} else {
						for (i=0 ; i<dservcount && overcnt>1 && prevdone; i++) {
							for (s=c->slisthead ; s && (s->csid!=dcsids[dservcount-1-i] || s->ecid!=(minecid+j)) ; s=s->next) {}
							if (s && s->valid==VALID) {
								if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
									if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
										s->valid = DEL;
										stats_chunkops[CHUNK_OP_DELETE_TRY]++;
										inforec.delete_duplicated_ecpart++;
										deldone++;
										delnotdone--;
										overcnt--;
									} else {
										prevdone = 0;
									}
								} else {
									prevdone = 0;
								}
							}
						}
					}
					if (prevdone==0) {
						enqueue = 1;
					}
				}
			}
		}
		if (enqueue) {
			if (regularecgoalequiv==goal) {
				chunk_priority_enqueue(CHUNK_PRIORITY_OVERGOAL,c);
			} else {
				chunk_priority_enqueue(chunk_priority,c);
			}
		}
		job_exit_reasons[c->sclassid][FOUND_DUPLICATED_EC_PARTS]++;
		return;
	}

// -------------------------------------------------
// calculate survivors tab (using replication limit)
// -------------------------------------------------

	survivorsmask4 = 0;
	survivorsmask8 = 0;
	if (validecgoalequiv>0) {
		if ((vcmask8|tdcmask8)!=0) {
			survivorsmask8 = chunk_find_ec_survivors(c,8,0x1F,0x20,0x30,now,repl_limit_read,eccsid8,survivorsecid8,survivorscsid8);
		}
		if ((vcmask4|tdcmask4)!=0) {
			survivorsmask4 = chunk_find_ec_survivors(c,4,0x0F,0x10,0x1C,now,repl_limit_read,eccsid4,survivorsecid4,survivorscsid4);
		}
	}

// ----------------------------------------
// find best replication source (full copy)
// ----------------------------------------

	preferedsrccsid = MAXCSCOUNT;
	if (vc+tdc>0) {
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->ecid==0 && matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read) {
				if (s->valid==VALID) {
					preferedsrccsid = s->csid;
					break;
				} else if (s->valid==TDVALID) {
					if (preferedsrccsid==MAXCSCOUNT) {
						preferedsrccsid = s->csid;
					}
				}
			}
		}
	}

// ------------------------------------------
// check if there is valid replication source
// ------------------------------------------

	if (preferedsrccsid==MAXCSCOUNT && survivorsmask8==0 && survivorsmask4==0) {
		if (chunk_priority<DANGER_PRIORITIES) {
			chunk_priority_enqueue(chunk_priority,c);
		}
		job_exit_reasons[c->sclassid][CANT_FIND_VALID_REPLICATION_SOURCE]++;
		return;
	}

// ----------------------------------------------------------------------------------------------------------
// in ecmode there are no valid copies and some parts are only on disks 'marked for removal' that can be read
// ----------------------------------------------------------------------------------------------------------

	if (vc+tdc==0 && ((ec_data_parts==8 && mfrmask8 && (survivorsmask8&mfrmask8)) || (ec_data_parts==4 && mfrmask4 && (survivorsmask4&mfrmask4)))) {
		uint32_t mfrmask;
		uint16_t *eccsid;
		uint32_t survivorsmask;

		// reduce goal if necessary - we need to recover missing data parts using extra checksums if there are no other options
//		if (goal+(ec_data_parts-1)>scount) {
//			goal = scount-(ec_data_parts-1);
//		}
		if (ec_data_parts==8) {
			mfrmask = mfrmask8;
			eccsid = eccsid8;
			survivorsmask = survivorsmask8;
		} else {
			mfrmask = mfrmask4;
			eccsid = eccsid4;
			survivorsmask = survivorsmask4;
		}
	
		rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
		if ((ec_data_parts==8 && mfrmask8&0xFF) || (ec_data_parts==4 && mfrmask4&0xF)) { // we have data parts
			done = 0;
			servcnt = chunk_get_available_servers_for_parts(servers,c,sm,ec_strict_mode,0,&dataec_both_labels_limit,minecid,minecid+(ec_data_parts-2)+goal,0,rservcount,rcsids);
			j = 0;
			for (i=0,mask=1 ; i<ec_data_parts && j<servcnt ; i++,mask<<=1) {
				if ((mask & survivorsmask & mfrmask)) {
					if (chunk_replicate(SIMPLE,now,c,ec_data_parts,i+minecid,eccsid[i],servers[j],NULL,NULL,NULL,(extrajob==2)?RECOVER_IO:(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
						job_exit_reasons[c->sclassid][ERROR_REPLICATING_MFR_DATA_PART]++;
						return;
					}
					done = 1;
					inforec.replicate_needed_ecpart++;
					j++;
				}
			}
		} else { // we have checksums
			done = 2;
			servcnt = chunk_get_available_servers_for_parts(servers,c,sm,ec_strict_mode,0,&chksumec_both_labels_limit,minecid,minecid+(ec_data_parts-2)+goal,1,rservcount,rcsids);
			j = 0;
			for (i=ec_data_parts,mask=(UINT32_C(1)<<ec_data_parts) ; i<(ec_data_parts-1)+goal && j<servcnt ; i++,mask<<=1) {
				if ((mask & survivorsmask & mfrmask)) {
					if (chunk_replicate(SIMPLE,now,c,ec_data_parts,i+minecid,eccsid[i],servers[j],NULL,NULL,NULL,(extrajob==2)?RECOVER_IO:(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
						job_exit_reasons[c->sclassid][ERROR_REPLICATING_MFR_CHKSUM_PART]++;
						return;
					}
					done = 3;
					inforec.replicate_needed_ecpart++;
					j++;
				}
			}
		}
		if (j==servcnt && rservallflag==0) {
			chunk_priority_enqueue(chunk_priority,c);
		}
		switch (done) {
			case 0:
				job_exit_reasons[c->sclassid][CANT_REPLICATE_MFR_DATA_PART]++;
				break;
			case 1:
				job_exit_reasons[c->sclassid][REPLICATED_MFR_DATA_PART]++;
				break;
			case 2:
				job_exit_reasons[c->sclassid][CANT_REPLICATE_MFR_CHKSUM_PART]++;
				break;
			case 3:
				job_exit_reasons[c->sclassid][REPLICATED_MFR_CHKSUM_PART]++;
				break;
		}
		return;
	}

// ----------------------------------------------------------------------------------------------------
// there are no valid copies and some missing data parts then recover them (in both EC and normal mode)
// ----------------------------------------------------------------------------------------------------

// EC8 version
	if (vc+tdc==0 && ((vcmask8|tdcmask8)&0xFF)!=0xFF && bitcount(survivorsmask8)>=8) {
		// reduce goal if necessary - we need to recover missing data parts using extra checksums if there are no other options
//		if (goal+7>scount) {
//			goal = scount-7;
//		}
		rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
		servcnt = chunk_get_available_servers_for_parts(servers,c,sm,ec_strict_mode,1,&dataec_both_labels_limit,0x20,0x26+goal,0,rservcount,rcsids);
		j = 0;
		done = 0;
		// generate missing EC parts from existing EC parts
		for (i=0,mask=1 ; i<8 && j<servcnt ; i++,mask<<=1) {
			if ((mask & vcmask8) == 0) {
				if ((mask & survivorsmask8)) { // hipothetical mark for removal copy
					if (chunk_replicate(SIMPLE,now,c,8,i+0x20,eccsid8[i],servers[j],NULL,NULL,NULL,(extrajob==2)?RECOVER_IO:(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
						job_exit_reasons[c->sclassid][ERROR_REPLICATING_MISSING_EC8_DATA_PART_FROM_MFR]++;
						return;
					}
					done = 1;
					inforec.replicate_needed_ecpart++;
				} else {
					if (chunk_replicate(RECOVER,now,c,8,i+0x20,0,servers[j],NULL,survivorscsid8,survivorsecid8,(extrajob==2)?RECOVER_IO:(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
						job_exit_reasons[c->sclassid][ERROR_RECOVERING_MISSING_EC8_DATA_PART]++;
						return;
					}
					done = 1;
					inforec.recover_ecpart++;
				}
				j++;
			}
		}
		if (j==servcnt && rservallflag==0) {
			chunk_priority_enqueue(chunk_priority,c);
		}
		if (done) {
			job_exit_reasons[c->sclassid][RECOVERED_MISSING_EC8_DATA_PART]++;
		} else {
			job_exit_reasons[c->sclassid][CANT_RECOVER_MISSING_EC8_DATA_PART]++;
		}
		return;
	}

// EC4 version
	if (vc+tdc==0 && ((vcmask4|tdcmask4)&0x0F)!=0x0F && bitcount(survivorsmask4)>=4) {
		// reduce goal if necessary - we need to recover missing data parts using extra checksums if there are no other options
//		if (goal+3>scount) {
//			goal = scount-3;
//		}
		rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
		servcnt = chunk_get_available_servers_for_parts(servers,c,sm,ec_strict_mode,1,&dataec_both_labels_limit,0x10,0x12+goal,0,rservcount,rcsids);
		j = 0;
		done = 0;
		// generate missing EC parts from existing EC parts
		for (i=0,mask=1 ; i<4 && j<servcnt ; i++,mask<<=1) {
			if ((mask & vcmask4) == 0) {
				if ((mask & survivorsmask4)) { // hipothetical mark for removal copy
					if (chunk_replicate(SIMPLE,now,c,4,i+0x10,eccsid4[i],servers[j],NULL,NULL,NULL,(extrajob==2)?RECOVER_IO:(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
						job_exit_reasons[c->sclassid][ERROR_REPLICATING_MISSING_EC4_DATA_PART_FROM_MFR]++;
						return;
					}
					done = 1;
					inforec.replicate_needed_ecpart++;
				} else {
					if (chunk_replicate(RECOVER,now,c,4,i+0x10,0,servers[j],NULL,survivorscsid4,survivorsecid4,(extrajob==2)?RECOVER_IO:(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
						job_exit_reasons[c->sclassid][ERROR_RECOVERING_MISSING_EC4_DATA_PART]++;
						return;
					}
					done = 1;
					inforec.recover_ecpart++;
				}
				j++;
			}
		}
		if (j==servcnt && rservallflag==0) {
			chunk_priority_enqueue(chunk_priority,c);
		}
		if (done) {
			job_exit_reasons[c->sclassid][RECOVERED_MISSING_EC4_DATA_PART]++;
		} else {
			job_exit_reasons[c->sclassid][CANT_RECOVER_MISSING_EC4_DATA_PART]++;
		}
		return;
	}

// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
// in EC mode we have missing data and checksum parts, but we have full copy - then create all missing data parts and checksums locally using this copy - version >= 4.14
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
	if (ec_data_parts && regularecgoalequiv < goal && preferedsrccsid!=MAXCSCOUNT && matocsserv_can_split_chunks(cstab[preferedsrccsid].ptr,ec_data_parts)) {
		uint32_t missingmask = 0;

		for (i=0,mask=0x1 ; i<(ec_data_parts-1)+goal ; i++,mask<<=1) {
			if (ec_data_parts==8) {
				if (((mask & vcmask8) == 0) && ((mask & survivorsmask8) == 0)) {
					missingmask |= mask;
				}
			} else if (ec_data_parts==4) {
				if (((mask & vcmask4) == 0) && ((mask & survivorsmask4) == 0)) {
					missingmask |= mask;
				}
			}
		}
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->csid==preferedsrccsid) {
				if (ec_data_parts==8 && s->ecid>=0x20 && s->ecid<=0x30) {
					mask = (UINT32_C(1) << (s->ecid & 0x1F));
					missingmask &= ~mask;
				} else if (ec_data_parts==4 && s->ecid>=0x10 && s->ecid<=0x1C) {
					mask = (UINT32_C(1) << (s->ecid & 0x0F));
					missingmask &= ~mask;
				}
			}
		}
		if (missingmask!=0) {
			if (matocsserv_send_localsplitchunk(cstab[preferedsrccsid].ptr,c->chunkid,c->version,missingmask,ec_data_parts,(ec_data_parts==8)?LOCALSPLIT_TO_EC8:LOCALSPLIT_TO_EC4)<0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32" : error sending localsplit command",c->chunkid,c->version);
				job_exit_reasons[c->sclassid][ERROR_CREATING_MISSING_PARTS_FROM_COPY]++;
			} else {
				stats_chunkops[CHUNK_OP_SPLIT_TRY]++;
				chunk_addopchunk(preferedsrccsid,c->chunkid);
				chunk_set_op(c,LOCALSPLIT);
				chunk_replock_repstart(c->chunkid,now);
//				c->lockedto = now+LOCKTIMEOUT;
				for (i=0,mask=0x1 ; i<(ec_data_parts-1)+goal ; i++,mask<<=1) {
					if (mask&missingmask) {
						s = slist_malloc();
						s->csid = preferedsrccsid;
						s->ecid = minecid+i;
						s->valid = BUSY;
						s->version = c->version;
						chunk_new_copy(c,s);
					}
				}
				job_exit_reasons[c->sclassid][CREATED_MISSING_PARTS_FROM_COPY]++;
			}
			return;
		}
	}

// --------------------------------------------------
// if there are missing EC checksums then create them
// --------------------------------------------------

	if (regularecgoalequiv < goal && ((ec_data_parts==8 && ((vcmask8|tdcmask8)&0xFF)==0xFF && bitcount(survivorsmask8)>=8) || (ec_data_parts==4 && ((vcmask4|tdcmask4)&0xF)==0xF && bitcount(survivorsmask4)>=4))) {
		rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
		servcnt = chunk_get_available_servers_for_parts(servers,c,sm,ec_strict_mode,0,&chksumec_both_labels_limit,minecid,minecid+(ec_data_parts-2)+goal,1,rservcount,rcsids);
		j = 0;
		done = 0;
		for (i=ec_data_parts,mask=(UINT32_C(1)<<ec_data_parts) ; i<(ec_data_parts-1)+goal && j<servcnt ; i++,mask<<=1) {
			if (ec_data_parts==8) {
				if ((mask & vcmask8) == 0) {
					if ((mask & survivorsmask8)) { // hipothetical mark for removal copy
						if (chunk_replicate(SIMPLE,now,c,ec_data_parts,i+0x20,eccsid8[i],servers[j],NULL,NULL,NULL,(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
							job_exit_reasons[c->sclassid][ERROR_REPLICATING_MISSING_EC8_CHKSUM_PART_FROM_MFR]++;
							return;
						}
						done = 1;
						inforec.replicate_needed_ecpart++;
					} else {
						if (chunk_replicate(RECOVER,now,c,ec_data_parts,i+0x20,0,servers[j],NULL,survivorscsid8,survivorsecid8,(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
							job_exit_reasons[c->sclassid][ERROR_CREATING_MISSING_EC8_CHKSUM_PART]++;
							return;
						}
						done = 1;
						inforec.calculate_ecchksum++;
					}
					j++;
				}
			} else { // ec_data_parts==4
				if ((mask & vcmask4) == 0) {
					if ((mask & survivorsmask4)) { // hipothetical mark for removal copy
						if (chunk_replicate(SIMPLE,now,c,ec_data_parts,i+0x10,eccsid4[i],servers[j],NULL,NULL,NULL,(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
							job_exit_reasons[c->sclassid][ERROR_REPLICATING_MISSING_EC4_CHKSUM_PART_FROM_MFR]++;
							return;
						}
						done = 2;
						inforec.replicate_needed_ecpart++;
					} else {
						if (chunk_replicate(RECOVER,now,c,ec_data_parts,i+0x10,0,servers[j],NULL,survivorscsid4,survivorsecid4,(regularecgoalequiv==1)?REPL_EC_ENDANGERED:REPL_EC_UNDERGOAL)!=0) {
							job_exit_reasons[c->sclassid][ERROR_CREATING_MISSING_EC4_CHKSUM_PART]++;
							return;
						}
						done = 2;
						inforec.calculate_ecchksum++;
					}
					j++;
				}
			}
		}
		if (j==servcnt && rservallflag==0) {
			chunk_priority_enqueue(chunk_priority,c);
		}
		switch (done) {
			case 1:
				job_exit_reasons[c->sclassid][CREATED_MISSING_EC8_CHKSUM_PART]++;
				break;
			case 2:
				job_exit_reasons[c->sclassid][CREATED_MISSING_EC4_CHKSUM_PART]++;
				break;
			default:
				if (ec_data_parts==8) {
					job_exit_reasons[c->sclassid][CANT_CREATE_MISSING_EC8_CHKSUM_PART]++;
				} else {
					job_exit_reasons[c->sclassid][CANT_CREATE_MISSING_EC4_CHKSUM_PART]++;
				}
		}
		return;
	}

// --------------------------------------------------------------------------------------------------
// in EC mode there are missing EC data parts, but we have full copy - then make data parts from copy
// --------------------------------------------------------------------------------------------------

	if (((ec_data_parts==8 && (vcmask8&0xFF)!=0xFF) || (ec_data_parts==4 && (vcmask4&0x0F)!=0x0F)) && preferedsrccsid!=MAXCSCOUNT) {
		rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
		servcnt = chunk_get_available_servers_for_parts(servers,c,sm,ec_strict_mode,0,&dataec_both_labels_limit,minecid,minecid+(ec_data_parts-2)+goal,0,rservcount,rcsids);
		j = 0;
		done = 0;
		for (i=0,mask=1 ; i<ec_data_parts && j<servcnt ; i++,mask<<=1) {
			if (ec_data_parts==8) {
				if ((mask & vcmask8) == 0) {
					if (chunk_replicate(SPLIT,now,c,ec_data_parts,i+0x20,preferedsrccsid,servers[j],NULL,NULL,NULL,SPLIT_EC_GENERIC)!=0) {
						job_exit_reasons[c->sclassid][ERROR_CREATING_MISSING_EC8_PART_FROM_COPY]++;
						return;
					}
					done = 1;
					inforec.split_copy_into_ecparts++;
					j++;
				}
			} else { // ec_data_parts==4
				if ((mask & vcmask4) == 0) {
					if (chunk_replicate(SPLIT,now,c,ec_data_parts,i+0x10,preferedsrccsid,servers[j],NULL,NULL,NULL,SPLIT_EC_GENERIC)!=0) {
						job_exit_reasons[c->sclassid][ERROR_CREATING_MISSING_EC4_PART_FROM_COPY]++;
						return;
					}
					done = 2;
					inforec.split_copy_into_ecparts++;
					j++;
				}
			}
		}
		if (j==servcnt && rservallflag==0) {
			chunk_priority_enqueue(chunk_priority,c);
		}
		switch (done) {
			case 1:
				job_exit_reasons[c->sclassid][CREATED_MISSING_EC8_PART_FROM_COPY]++;
				break;
			case 2:
				job_exit_reasons[c->sclassid][CREATED_MISSING_EC4_PART_FROM_COPY]++;
				break;
			default:
				job_exit_reasons[c->sclassid][CANT_CREATE_PART_FROM_COPY]++;
		}
		return;
	}

// -------------------------------------------------------------------------------------------
// in COPY mode there are no full copies, but there are EC data parts - join them to full copy
// -------------------------------------------------------------------------------------------

	if (ec_data_parts==0 && (vc+tdc)==0 && (survivorsmask8==0xFF || survivorsmask4==0xF)) {
		rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
		done = 0;
		if (rservcount>0) {
			if (sm->has_labels || DoNotUseSameIP || DoNotUseSameRack) {
				uint32_t reps = 0;

				servcnt = 0;
				for (i=0 ; i<rservcount ; i++) {
					for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid!=0) ; s=s->next) {}
					if (s==NULL) {
						servers[servcnt++] = rcsids[i];
					}
				}
				matching = do_advanced_match(sm,servcnt,servers);

				for (i=0 ; i<servcnt ; i++) {
					if (matching[i+sm->labelscnt]>=0) {
						if (survivorsmask4==0xF) {
							if (chunk_replicate(JOIN,now,c,4,0,0,servers[i],NULL,survivorscsid4,survivorsecid4,(extrajob==2)?JOIN_EC_IO:(usekeep==1)?JOIN_EC_CHANGE:(usekeep==2)?JOIN_EC_NOSERVERS:JOIN_EC_GENERIC)!=0) {
								job_exit_reasons[c->sclassid][ERROR_CREATING_LABELED_COPY_FROM_EC4_PARTS]++;
								return;
							}
							done = 1;
						} else { // survivorsmask8==0xFF
							if (chunk_replicate(JOIN,now,c,8,0,0,servers[i],NULL,survivorscsid8,survivorsecid8,(extrajob==2)?JOIN_EC_IO:(usekeep==1)?JOIN_EC_CHANGE:(usekeep==2)?JOIN_EC_NOSERVERS:JOIN_EC_GENERIC)!=0) {
								job_exit_reasons[c->sclassid][ERROR_CREATING_LABELED_COPY_FROM_EC8_PARTS]++;
								return;
							}
							done = 2;
						}
						inforec.join_ecparts_into_copy++;
						reps++;
					}
				}
				if (reps<sm->labelscnt) {
					chunk_priority_enqueue(CHUNK_PRIORITY_UNDERGOAL,c);
				}
			} else {
				uint32_t reps = 0;
				for (i=0 ; i<rservcount && reps<goal ; i++) {
					for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid!=0) ; s=s->next) {}
					if (!s) {
						if (survivorsmask4==0xF) {
							if (chunk_replicate(JOIN,now,c,4,0,0,rcsids[i],NULL,survivorscsid4,survivorsecid4,(extrajob==2)?JOIN_EC_IO:(usekeep==1)?JOIN_EC_CHANGE:(usekeep==2)?JOIN_EC_NOSERVERS:JOIN_EC_GENERIC)!=0) {
								job_exit_reasons[c->sclassid][ERROR_CREATING_COPY_FROM_EC4_PARTS]++;
								return;
							}
							done = 3;
						} else { // survivorsmask8==0xFF
							if (chunk_replicate(JOIN,now,c,8,0,0,rcsids[i],NULL,survivorscsid8,survivorsecid8,(extrajob==2)?JOIN_EC_IO:(usekeep==1)?JOIN_EC_CHANGE:(usekeep==2)?JOIN_EC_NOSERVERS:JOIN_EC_GENERIC)!=0) {
								job_exit_reasons[c->sclassid][ERROR_CREATING_COPY_FROM_EC8_PARTS]++;
								return;
							}
							done = 4;
						}
						inforec.join_ecparts_into_copy++;
						reps++;
					}
				}
				if (reps<goal) {
					chunk_priority_enqueue(CHUNK_PRIORITY_UNDERGOAL,c);
				}
			}
		} else {
			chunk_priority_enqueue(CHUNK_PRIORITY_UNDERGOAL,c);
		}
		switch (done) {
			case 1:
				job_exit_reasons[c->sclassid][CREATED_LABELED_COPY_FROM_EC4_PARTS]++;
				break;
			case 2:
				job_exit_reasons[c->sclassid][CREATED_LABELED_COPY_FROM_EC8_PARTS]++;
				break;
			case 3:
				job_exit_reasons[c->sclassid][CREATED_COPY_FROM_EC4_PARTS]++;
				break;
			case 4:
				job_exit_reasons[c->sclassid][CREATED_COPY_FROM_EC8_PARTS]++;
				break;
			default:
				job_exit_reasons[c->sclassid][CANT_CREATE_COPY_FROM_PARTS]++;
		}
		return;
	}

// -----------------------------------------------------------------------------------------
// in EC mode we have enough parts and full copies together - then we can delete full copies
// -----------------------------------------------------------------------------------------

	if (dontdelete==0 && ec_data_parts && vc>0 && regularecgoalequiv >= goal) { // delete redundant normal copies
		enqueue = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid == VALID && s->ecid==0) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
						inforec.delete_excess_copy++;
						deldone++;
					} else {
						enqueue = 1;
						delnotdone++;
					}
				} else {
					enqueue = 1;
					delnotdone++;
				}
			}
		}
		if (enqueue) {
			chunk_priority_enqueue(chunk_priority,c);
			job_exit_reasons[c->sclassid][CANT_DELETE_COPIES_IN_EC_MODE]++;
		} else {
			job_exit_reasons[c->sclassid][DELETED_COPIES_IN_EC_MODE]++;
		}
		return;
	}

// ----------------------------------------------------------------------------------------------
// in COPY mode we have enough valid copies and still some EC parts - then we can delete EC parts
// ----------------------------------------------------------------------------------------------

	if (dontdelete==0 && ec_data_parts==0 && (vcmask8|vcmask4)!=0 && vc >= goal) {
		enqueue = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid == VALID && s->ecid!=0) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
						inforec.delete_excess_ecpart++;
						deldone++;
					} else {
						enqueue = 1;
						delnotdone++;
					}
				} else {
					enqueue = 1;
					delnotdone++;
				}
			}
		}
		if (enqueue) {
			chunk_priority_enqueue(chunk_priority,c);
			job_exit_reasons[c->sclassid][CANT_DELETE_PARTS_IN_COPY_MODE]++;
		} else {
			job_exit_reasons[c->sclassid][DELETED_PARTS_IN_COPY_MODE]++;
		}
		return;
	}

// ----------------------------
// EC mode - overgoal condition
// ----------------------------

	if (ec_data_parts && regularecgoalequiv>goal) {
		enqueue = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid == VALID && s->ecid>=(minecid + (ec_data_parts-1) + goal)) {
				if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
					if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,0,OP_DEL_OVERGOAL)>=0) {
						s->valid = DEL;
						stats_chunkops[CHUNK_OP_DELETE_TRY]++;
						inforec.delete_excess_ecpart++;
						deldone++;
					} else {
						enqueue = 1;
						delnotdone++;
					}
				} else {
					enqueue = 1;
					delnotdone++;
				}
			}
		}
		if (enqueue) {
			chunk_priority_enqueue(CHUNK_PRIORITY_OVERGOAL,c);
			job_exit_reasons[c->sclassid][CANT_DELETE_EXTRA_EC_PARTS]++;
		} else {
			job_exit_reasons[c->sclassid][DELETED_EXTRA_EC_PARTS]++;
		}
		return;
	}

// ----------------------
// EC mode - wrong labels
// ----------------------

	if (ec_data_parts && chunk_priority==CHUNK_PRIORITY_WRONGLABELS) { // wrong labels
		wlmask = 0;
		if (sm->labelscnt>0) {
			uint8_t checkindex;
			if (sm->labelscnt==1) {
				checkindex = 0;
			} else {
				checkindex = 1;
			}
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid) {
					if ((s->ecid & ecidmask) < ec_data_parts) {
						if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[0])==0) {
							wlmask |= UINT32_C(1) << (s->ecid & ecidmask);
						}
					} else {
						if (matocsserv_server_matches_labelexpr(cstab[s->csid].ptr,sm->labelexpr[checkindex])==0) {
							wlmask |= UINT32_C(1) << (s->ecid & ecidmask);
						}
					}
				}
			}
			if (wlmask!=0) {
				done = 0;
				rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
				if ((ec_data_parts==8 && (wlmask&0xFF)) || (ec_data_parts==4 && (wlmask&0xF))) {
					servcnt = 0;
					for (i=0 ; i<rservcount ; i++) {
						for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid<minecid || s->ecid>maxecid) ; s=s->next) {}
						if (s==NULL) {
							if (matocsserv_server_matches_labelexpr(cstab[rcsids[i]].ptr,sm->labelexpr[0])) {
								servers[servcnt++] = rcsids[i];
							}
						}
					}
					j = 0;
					for (i=0,mask=1 ; i<ec_data_parts && j < servcnt ; i++,mask<<=1) {
						if (wlmask & mask) {
							srccsid = MAXCSCOUNT;
							for (s=c->slisthead ; s && srccsid==MAXCSCOUNT ; s=s->next) {
								if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read && s->valid==VALID && s->ecid==minecid+i) {
									srccsid = s->csid;
								}
							}
							if (srccsid!=MAXCSCOUNT) {
								if (chunk_replicate(SIMPLE,now,c,ec_data_parts,i+minecid,srccsid,servers[j],NULL,NULL,NULL,REPL_EC_WRONGLABEL)!=0) {
									job_exit_reasons[c->sclassid][ERROR_REPLICATING_WRONG_LABELED_DATA_PART]++;
									return;
								}
								done |= 1;
								inforec.replicate_wronglabels_ecpart++;
								j++;
							}
						}
					}
				}
				if ((ec_data_parts==8 && (wlmask&0xFFFFFF00)) || (ec_data_parts==4 && (wlmask&0xFFFFFFF0))) {
					servcnt = 0;
					for (i=0 ; i<rservcount ; i++) {
						for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid<minecid || s->ecid>maxecid) ; s=s->next) {}
						if (s==NULL) {
							if (matocsserv_server_matches_labelexpr(cstab[rcsids[i]].ptr,sm->labelexpr[(sm->labelscnt==1)?0:1])) {
								servers[servcnt++] = rcsids[i];
							}
						}
					}
					j = 0;
					for (i=ec_data_parts,mask=(UINT32_C(1)<<ec_data_parts) ; i<(ec_data_parts-1)+goal && j < servcnt ; i++,mask<<=1) {
						if (wlmask & mask) {
							srccsid = MAXCSCOUNT;
							for (s=c->slisthead ; s && srccsid==MAXCSCOUNT ; s=s->next) {
								if (matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read && s->valid==VALID && s->ecid==minecid+i) {
									srccsid = s->csid;
								}
							}
							if (srccsid!=MAXCSCOUNT) {
								if (chunk_replicate(SIMPLE,now,c,ec_data_parts,i+minecid,srccsid,servers[j],NULL,NULL,NULL,REPL_EC_WRONGLABEL)!=0) {
									job_exit_reasons[c->sclassid][ERROR_REPLICATING_WRONG_LABELED_CHKSUM_PART]++;
									return;
								}
								done |= 2;
								inforec.replicate_wronglabels_ecpart++;
								j++;
							}
						}
					}
				}
				chunk_priority_enqueue(chunk_priority,c);
				switch (done) {
					case 0:
						job_exit_reasons[c->sclassid][CANT_REPLICATE_WRONG_LABELED_PART]++;
						break;
					case 1:
						job_exit_reasons[c->sclassid][REPLICATED_WRONG_LABELED_DATA_PART]++;
						break;
					case 2:
						job_exit_reasons[c->sclassid][REPLICATED_WRONG_LABELED_CHKSUM_PART]++;
						break;
					default:
						job_exit_reasons[c->sclassid][REPLICATED_WRONG_LABELED_PARTS]++;
						break;
				}
				return;
			}
		}
	}

// ------------------------------
// COPY mode - overgoal condition
// ------------------------------

	if (ec_data_parts==0 && vc > goal) {
		uint8_t prevdone;
		uint8_t delcnt;
		if (dservcount==0) {
			// dservcount = matocsserv_getservers_ordered(dcsids,AcceptableDifference/2.0,NULL,NULL);
			dservcount = matocsserv_getservers_ordered(dcsids);
		}
		delnotdone += (vc-goal);
		prevdone = 1;

		if (sm->has_labels || DoNotUseSameIP || DoNotUseSameRack) { // labels version
			servcnt = 0;
			for (i=0 ; i<dservcount ; i++) {
				for (s=c->slisthead ; s ; s=s->next) {
//					if (s->csid==dcsids[dservcount-1-i] && s->ecid==0 && s->valid==VALID) {
					if (s->csid==dcsids[i] && s->ecid==0 && s->valid==VALID) {
						servers[servcnt++] = s->csid;
						break;
					}
				}
			}
			matching = do_advanced_match(sm,servcnt,servers);
			do_extend_match(sm,servcnt,matching);
			delcnt = vc - goal;
			for (i=0 ; i<servcnt && delcnt>0 && prevdone ; i++) {
				if (matching[i+sm->labelscnt]<0) {
					for (s=c->slisthead ; s && (s->csid!=servers[i] || s->ecid!=0) ; s=s->next) {}
					if (s && s->valid==VALID) {
						if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
							if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
								s->valid = DEL;
								stats_chunkops[CHUNK_OP_DELETE_TRY]++;
								inforec.delete_excess_copy++;
								deldone++;
								delnotdone--;
								delcnt--;
							} else {
								prevdone=0;
								chunk_priority_enqueue(CHUNK_PRIORITY_OVERGOAL,c); // in such case only enqueue this chunk for future processing
							}
						} else {
							prevdone=0;
							chunk_priority_enqueue(CHUNK_PRIORITY_OVERGOAL,c); // in such case only enqueue this chunk for future processing
						}
					}
				}
			}
		} else { // classic goal version
			delcnt = vc - goal;
			for (i=0 ; i<dservcount && delcnt>0 && prevdone; i++) {
				for (s=c->slisthead ; s && (s->csid!=dcsids[dservcount-1-i] || s->ecid!=0) ; s=s->next) {}
				if (s && s->valid==VALID) {
					if (matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
						if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
							c->needverincrease = 1;
							s->valid = DEL;
							stats_chunkops[CHUNK_OP_DELETE_TRY]++;
							inforec.delete_excess_copy++;
							deldone++;
							delnotdone--;
							delcnt--;
						} else {
							prevdone=0;
							chunk_priority_enqueue(CHUNK_PRIORITY_OVERGOAL,c); // in such case only enqueue this chunk for future processing
						}
					} else {
						prevdone=0;
						chunk_priority_enqueue(CHUNK_PRIORITY_OVERGOAL,c); // in such case only enqueue this chunk for future processing
					}
				}
			}
		}
		if (prevdone) {
			job_exit_reasons[c->sclassid][DELETED_EXTRA_COPIES]++;
		} else {
			job_exit_reasons[c->sclassid][CANT_DELETE_EXTRA_COPIES]++;
		}
		return;
	}

// ------------------------------------------------------------------------------------------------------------
// COPY mode - all servers are occupied and some copies have mark for removal status - in such case delete them
// ------------------------------------------------------------------------------------------------------------

	if (ec_data_parts==0 && vc+tdc>=scount && vc<goal && tdc>0 && vc+tdc>1 && chunk_priority_is_empty(CHUNK_PRIORITY_ONECOPY_HIGHGOAL) && chunk_priority_is_empty(CHUNK_PRIORITY_ONECOPY_ANY) && chunk_priority_is_empty(CHUNK_PRIORITY_ONEREGCOPY_PLUSMFR)) {
		uint8_t tdcr = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->valid==TDVALID && s->ecid==0) {
				if (matocsserv_has_avail_space(cstab[s->csid].ptr)) {
					tdcr++;
				}
			}
		}
		if (vc+tdcr>=scount) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==TDVALID && s->ecid==0) {
					if (matocsserv_has_avail_space(cstab[s->csid].ptr) && matocsserv_deletion_counter(cstab[s->csid].ptr)<TmpMaxDel) {
						if (matocsserv_send_deletechunk(cstab[s->csid].ptr,c->chunkid,s->ecid,c->version,OP_DEL_OVERGOAL)>=0) {
							c->needverincrease = 1;
							s->valid = DEL;
							stats_chunkops[CHUNK_OP_DELETE_TRY]++;
							inforec.delete_diskclean_copy++;
							job_exit_reasons[c->sclassid][DELETED_MFR_COPY_TO_MAKE_SPACE]++;
							return;
						}
					}
				}
			}
		}
	}

// -----------------------------------------------
// COPY mode - undergoal or wrong labels condition
// -----------------------------------------------

	if (ec_data_parts==0 && (chunk_priority==CHUNK_PRIORITY_WRONGLABELS || goal > vc) && vc+tdc > 0) {
		uint32_t rgvc,rgtdc;
		uint16_t servmaxpos[4];
		uint8_t canbefixed;

		canbefixed = 0;

		matocsserv_get_server_groups(rcsids,repl_limit_write,servmaxpos);

		if (CHUNK_PRIORITY_HIGH(chunk_priority)) {
			rservcount = servmaxpos[CSSTATE_OVERLOADED];
		} else {
			rservcount = servmaxpos[CSSTATE_OK];
		}

//		printf("pos[CSSTATE_OK]=%u\n",servmaxpos[CSSTATE_OK]);
//		printf("pos[CSSTATE_OK]=%u\n",servmaxpos[CSSTATE_OVERLOADED]);
//		printf("pos[CSSTATE_OK]=%u\n",servmaxpos[CSSTATE_LIMIT_REACHED]);
//		printf("pos[CSSTATE_OK]=%u\n",servmaxpos[CSSTATE_NO_SPACE]);

//		rservcount = matocsserv_getservers_lessrepl(rcsids,repl_limit_write,CHUNK_PRIORITY_HIGH(chunk_priority),&rservallflag);
		rgvc = 0;
		rgtdc = 0;
		vripcnt = 0;
		for (s=c->slisthead ; s ; s=s->next) {
			if (s->ecid==0 && matocsserv_replication_read_counter(cstab[s->csid].ptr,now)<repl_limit_read) {
				if (s->valid==VALID) {
					if (vripcnt<255 && ReplicationsRespectTopology>1) {
						vrip[vripcnt++] = matocsserv_server_get_ip(cstab[s->csid].ptr);
					}
					rgvc++;
				} else if (s->valid==TDVALID) {
					if (vripcnt<255 && ReplicationsRespectTopology>1) {
						vrip[vripcnt++] = matocsserv_server_get_ip(cstab[s->csid].ptr);
					}
					rgtdc++;
				}
			}
		}
		if (ReplicationsRespectTopology>1) {
			chunk_rack_sort(rcsids,rservcount,vrip,vripcnt);
		}
		if (rgvc+rgtdc>0 && rservcount>0) { // have at least one server to read from and at least one to write to
			if (sm->has_labels || DoNotUseSameIP || DoNotUseSameRack) { // labels version
				uint16_t maxstdservers;
				uint16_t dstservcnt;

				servcnt = 0;
				// first add copies
				for (s=c->slisthead ; s ; s=s->next) {
					if (s->valid==VALID && s->ecid==0) {
						servers[servcnt++] = s->csid;
					}
				}
				dstservcnt = servcnt;
				// then standard valid servers
				for (i=0 ; i<rservcount ; i++) {
					for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid!=0) ; s=s->next) {}
					if (s==NULL) {
						servers[servcnt++] = rcsids[i];
					}
				}
				maxstdservers = servcnt;
				// then overloaded
				for (i=rservcount ; i<servmaxpos[CSSTATE_OVERLOADED] ; i++) {
					for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid!=0) ; s=s->next) {}
					if (s==NULL) {
						servers[servcnt++] = rcsids[i];
					}
				}
				// then servers with replication limit reached
				for (i=servmaxpos[CSSTATE_OVERLOADED] ; i<servmaxpos[CSSTATE_LIMIT_REACHED] ; i++) {
					for (s=c->slisthead ; s && s->csid!=rcsids[i] ; s=s->next) {}
					if (s==NULL) {
						servers[servcnt++] = rcsids[i];
					}
				}

				matching = do_advanced_match(sm,servcnt,servers);

				labels_mode = sclass_get_labels_mode(c->sclassid,sm);

				for (i=0 ; i<sm->labelscnt ; i++) {
					int32_t servpos;
					if (matching[i]<(int32_t)(sm->labelscnt)) {
						servpos=-1;
					} else {
						servpos=matching[i]-sm->labelscnt;
					}
					if (servpos<0) { // matched but only 'no space' server (or unmatched)
						if (labels_mode!=LABELS_MODE_STRICT) { // no strict rules? - use any server in this case
							canbefixed = 1;
							for (j=dstservcnt ; j<maxstdservers ; j++) { // check all possible destination servers
								if (matching[j+sm->labelscnt]<0) { // not matched to other labels?
									if (chunk_undergoal_replicate(c, servers[j], now, repl_limit_read, extrajob, chunk_priority, &inforec, rgvc, rgtdc)>=0) {
										job_exit_reasons[c->sclassid][REPLICATED_WRONG_LABELED_COPY_UNMATCHED]++;
										return;
									}
								}
							}
						}
					} else if (servpos>=maxstdservers) { // matched only 'busy' server
						canbefixed = 1; // in all case this can be fixed in the future
						if (labels_mode==LABELS_MODE_LOOSE) { // in loose rules use any available server
							for (j=dstservcnt ; j<maxstdservers ; j++) { // check all possible destination servers
								if (matching[j+sm->labelscnt]<0) { // not matched to other labels?
									if (chunk_undergoal_replicate(c, servers[j],now, repl_limit_read, extrajob, chunk_priority, &inforec, rgvc, rgtdc)>=0) {
										job_exit_reasons[c->sclassid][REPLICATED_WRONG_LABELED_COPY_BUSY]++;
										return;
									}
								}
							}
						}
					} else if (servpos>=dstservcnt) { // matched 'good' server
						canbefixed = 1;
						if (chunk_undergoal_replicate(c, servers[servpos], now, repl_limit_read, extrajob, chunk_priority, &inforec, rgvc, rgtdc)>=0) {
							job_exit_reasons[c->sclassid][REPLICATED_WRONG_LABELED_COPY_GOOD]++;
							return;
						}
					}
				}
			} else { // classic goal version
				if (goal<=servmaxpos[CSSTATE_LIMIT_REACHED] || vc<servmaxpos[CSSTATE_LIMIT_REACHED]) {
					canbefixed = 1;
					for (i=0 ; i<rservcount ; i++) {
						for (s=c->slisthead ; s && (s->csid!=rcsids[i] || s->ecid!=0) ; s=s->next) {}
						if (!s) {
							if (chunk_undergoal_replicate(c, rcsids[i], now, repl_limit_read, extrajob, chunk_priority, &inforec, rgvc, rgtdc)>=0) {
								job_exit_reasons[c->sclassid][REPLICATED_UNDERGOAL_COPY]++;
								return;
							}
						}
					}
				}
			}
		}
		if (canbefixed) { // enqueue only chunks which can be fixed and only if there are servers which reached replication limits
			chunk_priority_enqueue(chunk_priority,c);
		}
		if (chunk_priority==CHUNK_PRIORITY_WRONGLABELS) {
			job_exit_reasons[c->sclassid][CANT_REPLICATE_WRONG_LABELED_COPY]++;
		} else {
			job_exit_reasons[c->sclassid][CANT_REPLICATE_UNDERGOAL_COPY]++;
		}
		return;
	}

	if (extrajob) { // do not rebalane doing "extra" jobs.
		job_exit_reasons[c->sclassid][CANT_REBALANCE_ON_EXTRA_CALL]++;
		return;
	}

	if (fullservers==0) {
		uint8_t queues_empty;
		queues_empty = 1;
		for (i=0 ; i<DANGER_PRIORITIES ; i++) {
			if (chunk_priority_not_empty(i)) {
				if (CHUNK_PRIORITY_NEEDSREPLICATION(i)) {
					job_exit_reasons[c->sclassid][REBALANCE_BLOCKED_BY_PRIORITY_QUEUES]++;
					return; // we have pending undergaal/wronglabeled chunks, so ignore chunkserver rebalance
				} else {
					queues_empty = 0;
				}
			}
		}
		if (queues_empty) {
			if (c->ondangerlist) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk %016"PRIX64"_%08"PRIX32": fixing 'ondangerlist' flag",c->chunkid,c->version);
				c->ondangerlist = 0;
			}
		}
	}

	if (rebalance_fails[c->sclassid] >= MaxRebalanceFails && MaxRebalanceFails>0 && FailRebalanceCounterResetCalls>0) {
		job_exit_reasons[c->sclassid][REBALANCE_BLOCKED_BY_TOO_MANY_FAILS]++;
		return;
	}

	/* rebalance */
	srccsid = MAXCSCOUNT;
	dstcsid = MAXCSCOUNT;
	repecid = 0;
	maxdiff = 0.0;
	matching = NULL;
	labels_mode = LABELS_MODE_LOOSE;

	/* find src,dst and ecid */

	/* prepare servers */
	servcnt = 0;
	if (ec_data_parts==8) {
		if (goal==regularecgoalequiv && overmask8==0 && (vc+tdc)==0 && (vcmask4|tdcmask4)==0) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid) {
					ecids[servcnt] = s->ecid;
					servers[servcnt] = s->csid;
					servcnt++;
				}
			}
			extraservcnt = servcnt;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid!=VALID && s->ecid>=minecid && s->ecid<=maxecid) {
					servers[extraservcnt++] = s->csid;
				}
			}
		}
	} else if (ec_data_parts==4) {
		if (goal==regularecgoalequiv && overmask4==0 && (vc+tdc)==0 && (vcmask8|tdcmask8)==0) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid>=minecid && s->ecid<=maxecid) {
					ecids[servcnt] = s->ecid;
					servers[servcnt] = s->csid;
					servcnt++;
				}
			}
			extraservcnt = servcnt;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid!=VALID && s->ecid>=minecid && s->ecid<=maxecid) {
					servers[extraservcnt++] = s->csid;
				}
			}
		}
	} else {
		if (goal==vc && vc+tdc>0) {
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid==VALID && s->ecid==0) {
					ecids[servcnt] = 0;
					servers[servcnt] = s->csid;
					servcnt++;
				}
			}
			extraservcnt = servcnt;
			for (s=c->slisthead ; s ; s=s->next) {
				if (s->valid!=VALID && s->ecid==0) {
					servers[extraservcnt++] = s->csid;
				}
			}

			if (sm->has_labels) {
				matching = do_advanced_match(sm,servcnt,servers);
				labels_mode = sclass_get_labels_mode(c->sclassid,sm);
			}
		}
	}

	/* if there are source servers, then try to find best destination server for each source server */
	overloaded = 0;
	if (servcnt>0) {
		if (dservcount==0) {
			dservcount = matocsserv_getservers_ordered(dcsids);
		}

		for (i=0 ; i<servcnt ; i++) {
			uint8_t *lexpr;
			uint8_t anyunmatched;
			double srcusage,dstusage;
			uint8_t lclass;

			anyunmatched = 0;
			lexpr = NULL;
			if (ec_data_parts==0) {
				if (sm->has_labels) {
					if (matching[sm->labelscnt+i]>=0) {
						lexpr = sm->labelexpr[matching[sm->labelscnt+i]];
					} else if (labels_mode!=LABELS_MODE_LOOSE) {
						anyunmatched = 1;
						// in normal and strict mode you can use any definition that do not match any copy (it will even fix wrong labels)
					}
				}
			} else {
				if (sm->labelscnt) {
					lexpr = sm->labelexpr[(sm->labelscnt==1)?0:((ecids[i]&ecidmask)<ec_data_parts)?0:1];
				}
			}

			srcusage = matocsserv_get_usage(cstab[servers[i]].ptr);
			repl_read_counter = matocsserv_replication_read_counter(cstab[servers[i]].ptr,now);
//			if (repl_read_counter < MaxReadRepl[2] || repl_read_counter < MaxReadRepl[3]) { // here accept any rebalance limit
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
						if (servers[k]==dcsids[j]) { // we have already copy on this server
							break;
						}
						if (ec_data_parts==0) { // extra tests for 'COPY' version
							if (DoNotUseSameIP) {
								if (matocsserv_server_get_ip(cstab[servers[k]].ptr)==matocsserv_server_get_ip(cstab[dcsids[j]].ptr)) { // we have copy on server with the same IP - can't use
									break;
								}
							} else if (DoNotUseSameRack) {
								if (topology_get_rackid(matocsserv_server_get_ip(cstab[servers[k]].ptr))==topology_get_rackid(matocsserv_server_get_ip(cstab[dcsids[j]].ptr))) { // we have copy on server with the same rack id - can't use
									break;
								}
							} else if (sm->uniqmask & UNIQ_MASK_IP) {
								if (matocsserv_server_get_ip(cstab[servers[k]].ptr)==matocsserv_server_get_ip(cstab[dcsids[j]].ptr)) { // we have copy on server with the same IP - can't use
									break;
								}
							} else if (sm->uniqmask & UNIQ_MASK_RACK) {
								if (topology_get_rackid(matocsserv_server_get_ip(cstab[servers[k]].ptr))==topology_get_rackid(matocsserv_server_get_ip(cstab[dcsids[j]].ptr))) { // we have copy on server with the same rack id - can't use
									break;
								}
							} else if (sm->uniqmask) {
								if ((matocsserv_server_get_labelmask(cstab[servers[k]].ptr) & (sm->uniqmask)) == (matocsserv_server_get_labelmask(cstab[dcsids[j]].ptr) & (sm->uniqmask))) { // we have copy on server with the same uniq labels - can't use
									break;
								}
							}
						}
						k++;
					}
					if (k<extraservcnt) { // one of existing copies is on this server or this server has same IP/RACKID/UNIQLABELS
						continue;
					}
					if (anyunmatched) { // check if this server match any of unmatched labels
						for (k=0 ; k<sm->labelscnt ; k++) {
							if (matching[k]<0) {
								if (matocsserv_server_matches_labelexpr(cstab[dcsids[j]].ptr,sm->labelexpr[k])) {
 									break;
 								}
 							}
						}
						if (k==sm->labelscnt) { // nope
							continue;
						}
					}
					if (lexpr==NULL || matocsserv_server_matches_labelexpr(cstab[dcsids[j]].ptr,lexpr)) {
						if ((srcusage - dstusage) > AcceptableDifference*1.5) { // now we know usage difference, so we can set proper limit class
							lclass = 3;
						} else {
							lclass = 2;
						}
						if (repl_read_counter < MaxReadRepl[lclass] && matocsserv_replication_write_counter(cstab[dcsids[j]].ptr,now)<MaxWriteRepl[lclass]) {
							maxdiff = srcusage - dstusage;
							repecid = ecids[i];
							dstcsid = dcsids[j];
							srccsid = servers[i];
						} else {
							overloaded = 1;
						}
					}
				}
//			}
		}
	}

	if (dstcsid!=MAXCSCOUNT && srccsid!=MAXCSCOUNT) {
		chunk_delay_protect(c->chunkid);
		stats_chunkops[CHUNK_OP_REPLICATE_TRY]++;
		chunk_replicate(SIMPLE,now,c,ec_data_parts,repecid,srccsid,dstcsid,NULL,NULL,NULL,(repecid==0)?REPL_COPY_REBALANCE:REPL_EC_REBALANCE);
//		matocsserv_send_replicatechunk(cstab[dstcsid].ptr,c->chunkid,repecid,c->version,cstab[srccsid].ptr,(repecid==0)?REPL_COPY_REBALANCE:REPL_EC_REBALANCE);
//		c->needverincrease = 1;
		inforec.replicate_rebalance++;
		last_rebalance = now;
		rebalance_fails[c->sclassid] = 0;
		job_exit_reasons[c->sclassid][REBALANCE_DONE]++;
	} else {
		if (overloaded) {
			rebalance_fails[c->sclassid]++;
		}
		job_exit_reasons[c->sclassid][REBALANCE_NOT_DONE_OR_NOT_NEEDED]++;
	}
}

void chunk_labelset_fix_matching_servers(storagemode *sm) {
	uint16_t *servcsids;
	uint16_t servcscnt;
	int32_t *matching;
	uint32_t i;

	if (sm==NULL) {
		chunk_replallowed_servers(NULL,1); // refresh servers
		return;
	}

	if (sm->ec_data_chksum_parts) { // EC mode
		matocsserv_recalculate_storagemode_scounts(sm);
		sm->matching_servers = 0; // not used in EC mode
	} else {
		servcsids = chunk_replallowed_servers(&servcscnt,0);

		matching = do_advanced_match(sm,servcscnt,servcsids);

		sm->matching_servers = 0;
		for (i=0 ; i<sm->labelscnt ; i++) {
			if (matching[i]>=0) {
				sm->matching_servers++;
			}
		}
		// server counters not used in COPY mode
		sm->replallowed = 0xFFFF;
		sm->allvalid = 0xFFFF;
		sm->data_replallowed = 0xFFFF;
		sm->data_allvalid = 0xFFFF;
		sm->chksum_replallowed = 0xFFFF;
		sm->chksum_allvalid = 0xFFFF;
		sm->both_replallowed = 0xFFFF;
		sm->both_allvalid = 0xFFFF;
	}
}

// returns:
// 0 - can't be fulfilled
// 1 - only using servers that are full
// 2 - only using overloaded servers
// 3 - can be fulfilled
// 4 - can be kept in ec format

enum canbefulfilled {
	CBF_NO = 0,
	CBF_NOSPACE,
	CBF_OVERLOADED,
	CBF_YES,
	CBF_ECKEEP
};

uint8_t chunk_labelset_can_be_fulfilled(storagemode *sm) {
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

	if (sm==NULL) {
		matocsserv_getservers_test(&stdcscnt,stdcsids,&olcscnt,olcsids,&allcscnt,allcsids);
		return CBF_NO;
	}

	// no labels
	//
	// RL - replallowed servers
	// OL - overloaded servers
	// VL - allvalid servers
	//
	// VL >= OL >= RL
	// 
	// if VL < N+X -> NO
	// elif VL < N+2X -> KEEPEC
	// elif OL < N+2X -> NOSPACE
	// elif RL < N+2X -> OVERLOADED
	// else -> OK
	//
	// 1 label
	//
	// RLA replallowed servers matching labels
	// OLA overloaded servers matching labels
	// VLA allvalid servers matching labels
	// 
	// if VLA < N+X -> NO
	// elif VLA < N+2X -> KEEPEC
	// elif OLA < N+2X -> NOSPACE
	// elif RLA < N+2X -> OVERLOADED
	// else -> OK
	//
	// 2 labels
	//
	// RLD,RLC,RLB - replallowed servers matching labels (data,checksum,both)
	// OLD,OLC,OLB - overloaded servers matching labels (data,checksum,both)
	// VLD,VLC,VLB - allvalid servers matching labels (data,checksum,both)
	//
	// if (VLA < N+X || VLD+VLB < N || VLC+VLB < X) -> NO
	// elif (VLA < N+2X || VLD+VLB < N+X || VLC+VLB < 2X) -> KEEPEC
	// elif (OLA < N+2X || OLD+OLB < N+X || OLC+OLB < 2X) -> OVERLOADED
	// elif (RLA < N+2X || RLD+RLB < N+X || RLC+RLB < 2X) -> NOSPACE
	// else -> OK
	//

	if (sm->ec_data_chksum_parts) { // EC mode
		uint8_t data_parts;
		uint8_t chksum_parts;
		data_parts = sm->ec_data_chksum_parts >> 4;
		chksum_parts = sm->ec_data_chksum_parts & 0xF;
		if (sm->has_labels && sm->labelscnt) {
			if (sm->valid_ec_counters<sm->labelscnt) {
				matocsserv_recalculate_storagemode_scounts(sm);
			}
			if (sm->labelscnt==1) {
				if (sm->allvalid<(data_parts+chksum_parts)) {
					return CBF_NO;
				}
				if (sm->allvalid<(data_parts+2*chksum_parts)) {
					return CBF_ECKEEP;
				}
				if (sm->overloaded<(data_parts+2*chksum_parts)) {
					return CBF_NOSPACE;
				}
				if (sm->replallowed<(data_parts+2*chksum_parts)) {
					return CBF_OVERLOADED;
				}
			} else { // sm->labelscnt==2
				if (sm->allvalid < (data_parts+chksum_parts)
				 || sm->data_allvalid+sm->both_allvalid < data_parts
				 || sm->chksum_allvalid+sm->both_allvalid < chksum_parts) {
					return CBF_NO;
				}
				if (sm->allvalid < (data_parts+2*chksum_parts)
				 || sm->data_allvalid+sm->both_allvalid < data_parts + chksum_parts
				 || sm->chksum_allvalid+sm->both_allvalid < 2*chksum_parts) {
					return CBF_ECKEEP;
				}
				if (sm->overloaded < (data_parts+2*chksum_parts)
				 || sm->data_overloaded+sm->both_overloaded < data_parts + chksum_parts
				 || sm->chksum_overloaded+sm->both_overloaded < 2*chksum_parts) {
					return CBF_NOSPACE;
				}
				if (sm->replallowed < (data_parts+2*chksum_parts)
				 || sm->data_replallowed+sm->both_replallowed < data_parts + chksum_parts
				 || sm->chksum_replallowed+sm->both_replallowed < 2*chksum_parts) {
					return CBF_OVERLOADED;
				}
			}
		} else {
			if (allcscnt<(data_parts+chksum_parts)) {
				return CBF_NO;
			}
			if (allcscnt<(data_parts+2*chksum_parts)) {
				return CBF_ECKEEP;
			}
			if (olcscnt<(data_parts+2*chksum_parts)) {
				return CBF_NOSPACE;
			}
			if (stdcscnt<(data_parts+2*chksum_parts)) {
				return CBF_OVERLOADED;
			}
		}
		return CBF_YES;
	}

	matching = do_advanced_match(sm,stdcscnt,stdcsids);

	r = 1;
	for (i=0 ; i<sm->labelscnt ; i++) {
		if (matching[i]<0) {
			r = 0;
			break;
		}
	}
	if (r==1) {
		return CBF_YES; // can be fulfilled
	}

	if (olcscnt > stdcscnt) {
		matching = do_advanced_match(sm,olcscnt,olcsids);

		r = 1;
		for (i=0 ; i<sm->labelscnt ; i++) {
			if (matching[i]<0) {
				r = 0;
				break;
			}
		}
		if (r==1) {
			return CBF_OVERLOADED; // can be fulfilled using overloaded servers
		}
	}

	if (allcscnt > olcscnt) {
		matching = do_advanced_match(sm,allcscnt,allcsids);

		r = 1;
		for (i=0 ; i<sm->labelscnt ; i++) {
			if (matching[i]<0) {
				r = 0;
				break;
			}
		}
		if (r==1) {
			return CBF_NOSPACE; // can be fulfilled using servers with no space available
		}
	}

	return CBF_NO;
}

int chunk_unlock(uint32_t ts,uint64_t chunkid) {
	chunk *c;

	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	c->lockedto = ts-1;
	chunk_write_counters(c,0);
	chunk_priority_queue_check(c,1);
	if (c->ondangerlist) {
		chunk_do_jobs(c,JOBS_CHUNK,ts,1);
		chunk_state_fix(c);
	} else {
		matoclserv_chunk_unlocked(c->chunkid,c);
	}
	return MFS_STATUS_OK;
}

int chunk_mr_unlock(uint32_t ts,uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	c->lockedto = ts-1;
	chunk_write_counters(c,0);
	return MFS_STATUS_OK;
}


void chunk_do_fast_job(chunk *c,uint32_t now,uint8_t extrajob) {

	chunk_do_jobs(c,JOBS_CHUNK,now,extrajob);
	chunk_state_fix(c);
}

void chunk_do_extra_job(uint64_t chunkid) {
	chunk *c;
	c = chunk_find(chunkid);
	if (c!=NULL) {
		chunk_io_ready_check(c);
	}
}

void chunk_jobs_main(void) {
	uint32_t i,j,k,s,lc,hashsteps;
	uint16_t csid;
	chunk *c,*cn;
	uint32_t now;

	static uint32_t jobshpos=0;
	static uint32_t jobshstep=1;
	static uint32_t jobshcnt=0;
	static uint32_t jobshmax=0;

	static uint32_t chunkcheckpos=0;
	static uint32_t chunkcheckwait=0;

	static uint16_t srcreset=0;
	static uint16_t sccreset=0;
	static uint16_t scmaxug[256];
	static uint16_t scmaxog[256];
	static uint16_t scmaxwl[256];
	static uint16_t scmaxml[256];
	static uint16_t *scmax;
#ifdef MFSDEBUG
	static uint32_t l=0,lq[DANGER_PRIORITIES]={0,};
	static uint32_t lastsecond=0;
#endif

	chunk_server_disconnection_loop();


	if (matocsserv_servers_count()==0 || chunkrehashpos==0) {
		return;
	}

	now = main_time();
	chunk_do_jobs(NULL,JOBS_EVERYTICK,now,0);

	// first check if some chunks need to be fixed for io
	lc = 0;
	do {
		c = chunk_priority_next(0);
		if (c!=NULL) {
			job_call[c->sclassid][0]++;
			chunk_do_jobs(c,JOBS_CHUNK,now,2);
			if (c->operation==NONE && c->ondangerlist==0) {
				chunk_io_ready_end(c);
			}
			chunk_state_fix(c);
			lc++;
		}
	} while (c!=NULL && lc<HashCPTMax);

	if (chunk_counters_in_progress()) {
		return;
	}

	if (main_start_time()+ReplicationsDelayInit>main_time()) {
		return;
	}

	sccreset++;
	if (sccreset>=FailClassCounterResetCalls) {
		memset(scmaxug,0,256*sizeof(uint16_t));
		memset(scmaxog,0,256*sizeof(uint16_t));
		memset(scmaxwl,0,256*sizeof(uint16_t));
		memset(scmaxml,0,256*sizeof(uint16_t));
		sccreset = 0;
	}
	srcreset++;
	if (srcreset>=FailRebalanceCounterResetCalls) {
		memset(rebalance_fails,0,256*sizeof(uint16_t));
		srcreset = 0;
	}
	// first serve some endangered and undergoal chunks
	for (j=1 ; j<DANGER_PRIORITIES && lc<HashCPTMax ; j++) {
		scmax = (j<=CHUNK_PRIORITY_UNDERGOAL)?scmaxug:(j==CHUNK_PRIORITY_OVERGOAL)?scmaxog:scmaxwl;
		i = chunk_priority_get_elements(j);
		s = 0;
		for (k=j+1 ; k<DANGER_PRIORITIES ; k++) {
			s += chunk_priority_get_elements(k);
		}
		if (i>((HashCPTMax-lc)*3/4) && s>0) {
			i = ((HashCPTMax-lc)*3)/4;
		}
		do {
			if (i>0) {
				c = chunk_priority_next(j);
				if (c!=NULL) {
					if (scmax[c->sclassid]<MaxFailsPerClass || MaxFailsPerClass==0 || FailClassCounterResetCalls==0) {
						job_call[c->sclassid][j]++;
						chunk_do_jobs(c,JOBS_CHUNK,now,1);
						chunk_state_fix(c);
						if (c->ondangerlist) {
							scmax[c->sclassid]++;
						} else {
							scmax[c->sclassid]=0;
						}
					} else {
						job_nocall[c->sclassid][j]++;
						chunk_priority_enqueue(j,c);
					}
					lc++;
				}
				i--;
			} else {
				c=NULL;
			}
		} while (c!=NULL && lc<HashCPTMax);
	}
#ifdef MFSDEBUG
	if (now!=lastsecond) {
		if (l!=chq_elements) {
			j=0;
		} else {
			for (j=0 ; j<DANGER_PRIORITIES ; j++) {
				if (lq[j]!=chq_queue_elements[j]) {
					break;
				}
			}
		}
		if (j!=DANGER_PRIORITIES) { // something has changed
			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"danger_priority_queues: %"PRIu32"->%"PRIu32,l,chq_elements);
			for (j=0 ; j<DANGER_PRIORITIES ; j++) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"danger_priority_queue[%s]: %"PRIu32"->%"PRIu32,pristr[j],lq[j],chq_queue_elements[j]);
			}
			l = chq_elements;
			for (j=0 ; j<DANGER_PRIORITIES ; j++) {
				lq[j] = chq_queue_elements[j];
			}
		}
		chunk_io_ready_log();
		lastsecond=now;
	}
#endif

	// then serve standard chunks
	lc = 0;
	hashsteps = 1+((chunkrehashpos)/(LoopTimeMin*TicksPerSecond));
	for (i=0 ; i<hashsteps && lc<HashCPTMax ; i++) {
		if (jobshcnt>=chunkrehashpos) {
			chunk_do_jobs(NULL,JOBS_EVERYLOOP,now,0);	// every loop tasks
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
				if (c->lockedto<(uint32_t)main_time() && c->operation==NONE && c->slisthead==NULL && c->fhead==FLISTNULLINDX && chunk_counters_in_progress()==0 && csdb_have_all_servers()) {
					changelog("%"PRIu32"|CHUNKDEL(%"PRIu64",%"PRIu32")",main_time(),c->chunkid,c->version);
					if (c->ondangerlist) {
						chunk_priority_remove(c);
					}
					chunk_delete(c);
				} else {
					if (scmaxml[c->sclassid]<MaxFailsPerClass || MaxFailsPerClass==0 || FailClassCounterResetCalls==0) {
						job_call[c->sclassid][DANGER_PRIORITIES]++;
						chunk_do_jobs(c,JOBS_CHUNK,now,0);
						chunk_state_fix(c);
						if (c->ondangerlist) {
							scmaxml[c->sclassid]++;
						} else {
							scmaxml[c->sclassid]=0;
						}
					} else {
						job_nocall[c->sclassid][DANGER_PRIORITIES]++;
					}
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

	// chunk status
	c = chunkhashtab[chunkcheckpos>>HASHTAB_LOBITS][chunkcheckpos&HASHTAB_MASK];
	if (chunkcheckwait>0) {
		chunkcheckwait--;
		if (c==NULL) {
			chunkcheckpos++;
		}
	} else {
		c = chunkhashtab[chunkcheckpos>>HASHTAB_LOBITS][chunkcheckpos&HASHTAB_MASK];
		while (c) {
			if (c->lockedto<(uint32_t)main_time() && c->fhead>FLISTNULLINDX && c->ondangerlist==0 && c->operation==NONE) {
				matocsserv_broadcast_chunk_status(c->chunkid);
				chunkcheckwait++;
			}
			c = c->next;
		}
		if (chunkcheckwait>0) {
			chunkcheckwait--;
		}
		chunkcheckpos++;
	}
	if (chunkcheckpos>=chunkrehashpos) {
		chunkcheckpos=0;
	}
}

/* ---- */

void chunk_get_memusage(uint64_t allocated[6],uint64_t used[6]) {
	allocated[0] = sizeof(chunk*)*chunkrehashpos;
	used[0] = sizeof(chunk*)*chunkhashelem;
	chunk_getusage(allocated+1,used+1);
	slist_getusage(allocated+2,used+2);
	chunk_queue_getusage(allocated+3,used+3);
	io_ready_chunk_getusage(allocated+4,used+4);
	replock_getusage(allocated+5,used+5);
}

#define CHUNKFSIZE 18
#define CHUNKMAXPAIRS (255+128)
#define CHUNKDSIZE ((CHUNKMAXPAIRS*(1+3))+1)
// mver:0x10 : chunkid:64 ; version:32 ; lockedto:32
// mver:0x11 : chunkid:64 ; version:32 ; lockedto:32 ; flags:8
// mver:0x12 : chunkid:64 ; version:32 ; lockedto:32 ; flags:8 ; pairs:8 ; pairs * [ sclassid:8 ; fcount:24 ] [ calculated_sclassid:8 ]
//    static part: 18 ; dynamic part: 0 - 1533 (4 * 383) + 1
//    383 = all storage classes + max full nodes = 255 + 128 (128 = ( MAXINODE // FLISTMAXFCOUNT ))
//    calculated_sclassid present only if pairs>1
//       pairs == 0  ->  sclassid = 0
//       pairs == 1  ->  sclassid = pairs[0].sclassid
//       pairs >= 2  ->  sclassid = calculated_sclassid

uint8_t chunk_is_afterload_needed(uint8_t mver) {
	return (mver>=0x12)?0:1;
}

int chunk_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t hdr[8];
	uint8_t loadbuff[CHUNKFSIZE];
	uint8_t pairsbuff[CHUNKDSIZE];
	const uint8_t *ptr;
	uint8_t nl;
	int32_t r,recsize;
	int32_t dynsize;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version,lockedto;
	uint8_t flags;
	uint16_t pairs;
	uint8_t sclassid;
	uint32_t fcount;
	uint32_t *findxptr;
	flist *fl;

	nl = 1;
	chunks = 0;
	if (bio_read(fd,hdr,8)!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunks: can't read header");
		return -1;
	}
	ptr = hdr;
	nextchunkid = get64bit(&ptr);
	recsize = (mver==0x10)?16:(mver==0x11)?17:CHUNKFSIZE;
	for (;;) {
		r = bio_read(fd,loadbuff,recsize);
		if (r!=recsize) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunks: read error");
			return -1;
		}
		ptr = loadbuff;
		chunkid = get64bit(&ptr);
		version = get32bit(&ptr);
		lockedto = get32bit(&ptr);
		if (mver==0x10) {
			flags = 0;
		} else {
			flags = get8bit(&ptr);
		}
		if (mver<=0x11) {
			pairs = 0;
		} else {
			pairs = get8bit(&ptr);
		}
		if (flags&0x80) {
			flags &= 0x7F;
			pairs |= 0x100;
		}
		if (pairs>0) {
			dynsize = 4*pairs;
			if (pairs>1) {
				dynsize++;
			}
			r = bio_read(fd,pairsbuff,dynsize);
			if (r!=dynsize) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunks: read error");
				return -1;
			}
		}
		if (chunkid>0) {
			c = chunk_find(chunkid);
			if (c!=NULL) {
				if (nl) {
					fputc('\n',stderr);
					nl = 0;
				}
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"loading chunk %016"PRIX64" error: chunk already exists",chunkid);
				if (ignoreflag==0) {
					fprintf(stderr,"use option '-i' to ignore\n");
					return -1;
				}
			} else {
				c = chunk_new(chunkid);
				c->allowreadzeros = (version&0x80000000)?1:0;
				c->version = version&0x3FFFFFFF;
				c->lockedto = lockedto;
				if (pairs>0) {
					ptr = pairsbuff;
					if (pairs>1) {
						findxptr = &c->fhead;
						while (pairs>0) {
							sclassid = get8bit(&ptr);
							fcount = get24bit(&ptr);
							fl = flist_get(*findxptr = flist_alloc());
							fl->sclassid = sclassid;
							fl->fcount = fcount;
							fl->nexti = FLISTNULLINDX;
							findxptr = &(fl->nexti);
							pairs--;
						}
						sclassid = get8bit(&ptr);
					} else {
						sclassid = get8bit(&ptr);
						fcount = get24bit(&ptr);
						if (fcount<FLISTFIRSTINDX) {
							c->fhead = fcount;
						} else {
							fl = flist_get(c->fhead = flist_alloc());
							fl->sclassid = sclassid;
							fl->fcount = fcount;
							fl->nexti = FLISTNULLINDX;
						}
					}
					chunk_state_set_sclass(c,sclassid);
				}
				chunk_state_set_flags(c,flags);
			}
		} else {
			if (version==0 && lockedto==0 && flags==0) {
				return 0;
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunks: wrong ending - chunk zero with version: %"PRIu32" and locked to: %"PRIu32,version,lockedto);
				return -1;
			}
		}
	}
	return 0;	// unreachable
}

// static uint32_t store_ref_timestamp;

// void chunk_set_ref_timestamp(uint32_t ref_timestamp) {
//	store_ref_timestamp = ref_timestamp;
// }

uint8_t chunk_store(bio *fd) {
	uint8_t hdr[8];
	uint8_t storebuff[CHUNKFSIZE];
	uint8_t pairsbuff[CHUNKDSIZE];
	uint8_t *ptr,*dptr;
	uint32_t i;
	chunk *c;
// chunkdata
	uint64_t chunkid;
	uint32_t version;
	uint32_t lockedto;
	uint8_t flags;
	uint16_t pairs;
	uint32_t dynsize;
	uint32_t findx;
	flist *fl;

	if (fd==NULL) {
		return 0x12;
	}
	ptr = hdr;
	put64bit(&ptr,nextchunkid);
	if (bio_write(fd,hdr,8)!=8) {
		return 0xFF;
	}
	for (i=0 ; i<chunkrehashpos ; i++) {
		for (c=chunkhashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; c ; c=c->next) {
			ptr = storebuff;
			dptr = pairsbuff;
			chunkid = c->chunkid;
			put64bit(&ptr,chunkid);
			version = c->version;
			if (c->allowreadzeros) {
				version |= 0x80000000;
			}
			put32bit(&ptr,version);
			lockedto = c->lockedto;
//			if (lockedto<store_ref_timestamp || c->operation==REPLICATE || c->operation==LOCALSPLIT) {
//				lockedto = 0;
//			}
			put32bit(&ptr,lockedto);
			flags = c->flags;
			dynsize = 0;
			if (c->fhead==FLISTNULLINDX) {
				pairs = 0;
			} else if (c->fhead<FLISTFIRSTINDX) {
				pairs = 1;
				put8bit(&dptr,c->sclassid);
				put24bit(&dptr,c->fhead);
				dynsize = 4;
			} else {
				pairs = 0;
				for (findx = c->fhead ; pairs<CHUNKMAXPAIRS && findx!=FLISTNULLINDX ; findx=fl->nexti) {
					fl = flist_get(findx);
					put8bit(&dptr,fl->sclassid);
					put24bit(&dptr,fl->fcount);
					dynsize+=4;
					pairs++;
				}
				if (findx!=FLISTNULLINDX) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunks: too many classes to store !!! - serious data structure error");
				}
				if (pairs>1) {
					put8bit(&dptr,c->sclassid);
					dynsize++;
				}
			}
			if (pairs>255) {
				flags |= 0x80;
				pairs &= 0xFF;
			}
			put8bit(&ptr,flags);
			put8bit(&ptr,pairs);
			if (bio_write(fd,storebuff,CHUNKFSIZE)!=CHUNKFSIZE) {
				return 0xFF;
			}
			if (dynsize>0) {
				if (bio_write(fd,pairsbuff,dynsize)!=dynsize) {
					return 0xFF;
				}
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

	chunk_priority_cleanall();
	chunk_io_ready_cleanall();
	chunk_replock_cleanall();
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
	}
	cstab[0].prev = MAXCSCOUNT;
	csfreehead = 0;
	csfreetail = MAXCSCOUNT-1;
	csusedhead = MAXCSCOUNT;
	for (i=0 ; i<MAXSCLASS*4 ; i++) {
		for (j=0 ; j<11 ; j++) {
			allchunkcopycounts[i][j]=0;
			regchunkcopycounts[i][j]=0;
			allchunkec8counts[i][j]=0;
			regchunkec8counts[i][j]=0;
			allchunkec4counts[i][j]=0;
			regchunkec4counts[i][j]=0;
		}
	}
	flist_cleanup(0);
}

void chunk_newfs(void) {
	chunks = 0;
	nextchunkid = 1;
}

int chunk_parse_rep_list(char *strlist,double *replist) {
	// N
	// A,B,C,D
	// A,B,C,D,E
	char *p;
	uint32_t i;
	double reptmp[5];

	p = strlist;
	while (*p==' ' || *p=='\t' || *p=='\r' || *p=='\n') {
		p++;
	}
	reptmp[0] = strtod(p,&p);
	while (*p==' ' || *p=='\t' || *p=='\r' || *p=='\n') {
		p++;
	}
	if (*p==0) {
		for (i=0 ; i<5 ; i++) {
			replist[i] = reptmp[0];
		}
		return 1;
	}
	for (i=1 ; i<5 ; i++) {
		if (*p!=',') {
			if (i==4 && *p==0) {
				replist[4] = reptmp[0];
				for (i=0 ; i<4 ; i++) {
					replist[i] = reptmp[i];
					if (reptmp[i]>replist[4]) {
						replist[4] = reptmp[i];
					}
				}
				return 2;
			}
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
		for (i=0 ; i<5 ; i++) {
			replist[i] = reptmp[i];
		}
		return 3;
	}
	return -1;
}

void chunk_term(void) {
	uint32_t i;
	chunk_calculate_endanger_priority(NULL,1); // free tabs
	chunk_do_jobs(NULL,JOBS_TERM,main_time(),0); // free tabs
	for (i=0 ; i<MAXSCLASS*4 ; i++) {
		free(allchunkcopycounts[i]);
		free(regchunkcopycounts[i]);
		free(allchunkec8counts[i]);
		free(regchunkec8counts[i]);
		free(allchunkec4counts[i]);
		free(regchunkec4counts[i]);
	}
	free(allchunkcopycounts);
	free(regchunkcopycounts);
	free(allchunkec8counts);
	free(regchunkec8counts);
	free(allchunkec4counts);
	free(regchunkec4counts);
	flist_cleanup(1);
}

void chunk_load_cfg_common(void) {
	uint32_t uniqmode;

	uniqmode = cfg_getuint32("CHUNKS_UNIQUE_MODE",0);

	DoNotUseSameIP=0;
	DoNotUseSameRack=0;

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
	if (DangerMaxLeng>100000000) {
		DangerMaxLeng = 100000000;
	}

	DangerMinLeng = DangerMaxLeng/100;

	JobsTimerMiliSeconds = cfg_getuint32("JOBS_TIMER_MILISECONDS",5); // debug option
	if (JobsTimerMiliSeconds<1) {
		JobsTimerMiliSeconds=1;
	}
	if (JobsTimerMiliSeconds>50) {
		JobsTimerMiliSeconds=50;
	}
	TicksPerSecond = 1000/JobsTimerMiliSeconds;

	MaxFailsPerClass = cfg_getuint32("MAX_FAILS_PER_CLASS",5); // debug option

	FailClassCounterResetCalls = cfg_getuint32("FAIL_CLASS_COUNTER_RESET_CALLS",1); // debug option
	
	MaxRebalanceFails = cfg_getuint32("MAX_REBALANCE_FAILS",5); // debug option

	FailRebalanceCounterResetCalls = cfg_getuint32("FAIL_REBALANCE_COUNTER_RESET_CALLS",1); // debug option
}

void chunk_loginfo(FILE *fd) {
	uint8_t i;
	uint16_t sc;
	uint32_t l,a,p,r;
	fprintf(fd,"[chunk loop params]\n");
	fprintf(fd,"Ticks Per Second : %u\n",TicksPerSecond);
	fprintf(fd,"Hash Chunks Per Tick: %u\n",HashCPTMax);
	fprintf(fd,"Max Fails Per Class: %u\n",MaxFailsPerClass);
	fprintf(fd,"Max Rebalance Fails Per Class: %u\n",MaxRebalanceFails);
	fprintf(fd,"\n");
	fprintf(fd,"[chunks]\n");
	for (i=0 ; i<DANGER_PRIORITIES ; i++) {
		l = chq_queue_elements[i];
		a = chq_queue_last_append_count[i];
		p = chq_queue_last_pop_count[i];
		r = chq_queue_last_remove_count[i];
		fprintf(fd,"priority queue %u (%s): %"PRIu32" element%s (last minute appends: %"PRIu32" / pops: %"PRIu32" / removes: %"PRIu32")\n",i,pristr[i],l,(l==1)?"":"s",a,p,r);
	}
	fprintf(fd,"\n");
	fprintf(fd,"[job call counters (performed / skipped due to sclass limits)]\n");
	for (i=0 ; i<=DANGER_PRIORITIES ; i++) {
		uint32_t csum,nocsum;
		int sep;
		csum = 0;
		nocsum = 0;
		for (sc=0 ; sc<MAXSCLASS ; sc++) {
			csum += job_call_last[sc][i];
			nocsum += job_nocall_last[sc][i];
		}
		if (csum>0 || nocsum>0) {
			if (i<DANGER_PRIORITIES) {
				fprintf(fd,"priority queue %u (%s)",i,pristr[i]);
			} else {
				fprintf(fd,"standard");
			}
			fprintf(fd,": %u/%u [ ",csum,nocsum);
			sep = 0;
			for (sc=0 ; sc<MAXSCLASS ; sc++) {
				if (job_call_last[sc][i]>0 || job_nocall_last[sc][i]>0) {
					if (sep) {
						fprintf(fd," , ");
					}
					if (sc==0) {
						fprintf(fd,"(deleted)");
					} else {
						fprintf(fd,"'%s'",sclass_get_name(sc));
					}
					fprintf(fd,":%u/%u",job_call_last[sc][i],job_nocall_last[sc][i]);
					sep = 1;
				}
			}
			fprintf(fd," ]\n");
		}
	}
	fprintf(fd,"\n");
	fprintf(fd,"[job exit reasons]\n");
	for (i=0 ; i<JOB_EXIT_REASONS ; i++) {
		uint32_t sum;
		int sep;
		sum=0;
		for (sc=0 ; sc<MAXSCLASS ; sc++) {
			sum += job_exit_reasons_last[sc][i];
		}
		sep = 0;
		if (sum>0) {
			fprintf(fd,"reason: %s : %u [ ",job_exit_reason_to_str(i),sum);
			for (sc=0 ; sc<MAXSCLASS ; sc++) {
				if (job_exit_reasons_last[sc][i]>0) {
					if (sep) {
						fprintf(fd," , ");
					}
					if (sc==0) {
						fprintf(fd,"(deleted)");
					} else {
						fprintf(fd,"'%s'",sclass_get_name(sc));
					}
					fprintf(fd,":%u",job_exit_reasons_last[sc][i]);
					sep = 1;
				}
			}
			fprintf(fd," ]\n");
		}
	}
	fprintf(fd,"\n");
}

void chunk_reload(void) {
	uint32_t oldMaxDelSoftLimit,oldMaxDelHardLimit;
	uint32_t cps;
	uint32_t oldJobsTimerMiliSeconds;
	char *repstr;

	oldMaxDelSoftLimit = MaxDelSoftLimit;
	oldMaxDelHardLimit = MaxDelHardLimit;
	oldJobsTimerMiliSeconds = JobsTimerMiliSeconds;

	chunk_load_cfg_common();

	if (oldJobsTimerMiliSeconds!=JobsTimerMiliSeconds) {
		main_msectime_change(jobs_timer,JobsTimerMiliSeconds,0);
	}

	MaxDelSoftLimit = cfg_getuint32("CHUNKS_SOFT_DEL_LIMIT",10);
	if (cfg_isdefined("CHUNKS_HARD_DEL_LIMIT")) {
		MaxDelHardLimit = cfg_getuint32("CHUNKS_HARD_DEL_LIMIT",25);
		if (MaxDelHardLimit<MaxDelSoftLimit) {
			MaxDelSoftLimit = MaxDelHardLimit;
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CHUNKS_SOFT_DEL_LIMIT is greater than CHUNKS_HARD_DEL_LIMIT - using CHUNKS_HARD_DEL_LIMIT for both");
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

	repstr = cfg_getstr("CHUNKS_WRITE_REP_LIMIT","2,1,1,4,4");
	switch (chunk_parse_rep_list(repstr,MaxWriteRepl)) {
		case -1:
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"write replication limit parse error !!!");
			break;
		case 1:
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"write replication limit in old format (1 element - now should be 5) - change limits to new format");
			break;
		case 2:
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"write replication limit in old format (4 elements - now should be 5) - change limits to new format");
			break;
	}
	free(repstr);
	repstr = cfg_getstr("CHUNKS_READ_REP_LIMIT","10,5,2,5,10");
	switch (chunk_parse_rep_list(repstr,MaxReadRepl)) {
		case -1:
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"read replication limit parse error !!!");
			break;
		case 1:
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"read replication limit in old format (1 element - now should be 5) - change limits to new format");
			break;
		case 2:
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"read replication limit in old format (4 elements - now should be 5) - change limits to new format");
			break;
	}
	free(repstr);
	if (cfg_isdefined("CHUNKS_LOOP_TIME")) {
		LoopTimeMin = cfg_getuint32("CHUNKS_LOOP_TIME",300); // deprecated option
		if (LoopTimeMin < MINLOOPTIME) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CHUNKS_LOOP_TIME value too low (%"PRIu32") increased to %u",LoopTimeMin,MINLOOPTIME);
			LoopTimeMin = MINLOOPTIME;
		}
		if (LoopTimeMin > MAXLOOPTIME) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CHUNKS_LOOP_TIME value too high (%"PRIu32") decreased to %u",LoopTimeMin,MAXLOOPTIME);
			LoopTimeMin = MAXLOOPTIME;
		}
		HashCPTMax = 0xFFFFFFFF;
	} else {
		LoopTimeMin = cfg_getuint32("CHUNKS_LOOP_MIN_TIME",300);
		if (LoopTimeMin < MINLOOPTIME) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CHUNKS_LOOP_MIN_TIME value too low (%"PRIu32") increased to %u",LoopTimeMin,MINLOOPTIME);
			LoopTimeMin = MINLOOPTIME;
		}
		if (LoopTimeMin > MAXLOOPTIME) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CHUNKS_LOOP_MIN_TIME value too high (%"PRIu32") decreased to %u",LoopTimeMin,MAXLOOPTIME);
			LoopTimeMin = MAXLOOPTIME;
		}
		cps = cfg_getuint32("CHUNKS_LOOP_MAX_CPS",100000);
		if (cps < MINCPS) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CHUNKS_LOOP_MAX_CPS value too low (%"PRIu32") increased to %u",cps,MINCPS);
			cps = MINCPS;
		}
		if (cps > MAXCPS) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CHUNKS_LOOP_MAX_CPS value too high (%"PRIu32") decreased to %u",cps,MAXCPS);
			cps = MAXCPS;
		}
		HashCPTMax = ((cps+(TicksPerSecond-1))/TicksPerSecond);
	}
}

int chunk_strinit(void) {
	uint32_t i;
	uint32_t j;
	uint32_t cps;
	char *repstr;

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

	repstr = cfg_getstr("CHUNKS_WRITE_REP_LIMIT","2,1,1,4,4");
	switch (chunk_parse_rep_list(repstr,MaxWriteRepl)) {
		case -1:
			fprintf(stderr,"write replication limit parse error !!!\n");
			return -1;
		case 1:
			fprintf(stderr,"write replication limit in old format (1 element - now should be 5) - change limits to new format\n");
			break;
		case 2:
			fprintf(stderr,"write replication limit in old format (4 elements - now should be 5) - change limits to new format\n");
			break;
	}
	free(repstr);
	repstr = cfg_getstr("CHUNKS_READ_REP_LIMIT","10,5,2,5,10");
	switch (chunk_parse_rep_list(repstr,MaxReadRepl)) {
		case -1:
			fprintf(stderr,"read replication limit parse error !!!\n");
			return -1;
		case 1:
			fprintf(stderr,"read replication limit in old format (1 element - now should be 5) - change limits to new format\n");
			break;
		case 2:
			fprintf(stderr,"read replication limit in old format (4 elements - now should be 5) - change limits to new format\n");
			break;
	}
	free(repstr);
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
		cps = cfg_getuint32("CHUNKS_LOOP_MAX_CPS",100000);
		if (cps < MINCPS) {
			fprintf(stderr,"CHUNKS_LOOP_MAX_CPS value too low (%"PRIu32") increased to %u\n",cps,MINCPS);
			cps = MINCPS;
		}
		if (cps > MAXCPS) {
			fprintf(stderr,"CHUNKS_LOOP_MAX_CPS value too high (%"PRIu32") decreased to %u\n",cps,MAXCPS);
			cps = MAXCPS;
		}
		HashCPTMax = ((cps+(TicksPerSecond-1))/TicksPerSecond);
	}

	flist_init();
	chunk_hash_init();
	chunk_io_ready_init();
	chunk_replock_init();
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
	}
	cstab[0].prev = MAXCSCOUNT;
	csfreehead = 0;
	csfreetail = MAXCSCOUNT-1;
	csusedhead = MAXCSCOUNT;
	allchunkcopycounts = malloc(sizeof(uint64_t*)*MAXSCLASS*4);
	passert(allchunkcopycounts);
	regchunkcopycounts = malloc(sizeof(uint64_t*)*MAXSCLASS*4);
	passert(regchunkcopycounts);
	allchunkec8counts = malloc(sizeof(uint64_t*)*MAXSCLASS*4);
	passert(allchunkec8counts);
	regchunkec8counts = malloc(sizeof(uint64_t*)*MAXSCLASS*4);
	passert(regchunkec8counts);
	allchunkec4counts = malloc(sizeof(uint64_t*)*MAXSCLASS*4);
	passert(allchunkec4counts);
	regchunkec4counts = malloc(sizeof(uint64_t*)*MAXSCLASS*4);
	passert(regchunkec4counts);
	for (i=0 ; i<MAXSCLASS*4 ; i++) {
		allchunkcopycounts[i] = malloc(sizeof(uint64_t)*11);
		passert(allchunkcopycounts[i]);
		regchunkcopycounts[i] = malloc(sizeof(uint64_t)*11);
		passert(regchunkcopycounts[i]);
		allchunkec8counts[i] = malloc(sizeof(uint64_t)*11);
		passert(allchunkec8counts[i]);
		regchunkec8counts[i] = malloc(sizeof(uint64_t)*11);
		passert(regchunkec8counts[i]);
		allchunkec4counts[i] = malloc(sizeof(uint64_t)*11);
		passert(allchunkec4counts[i]);
		regchunkec4counts[i] = malloc(sizeof(uint64_t)*11);
		passert(regchunkec4counts[i]);
		for (j=0 ; j<11 ; j++) {
			allchunkcopycounts[i][j]=0;
			regchunkcopycounts[i][j]=0;
			allchunkec8counts[i][j]=0;
			regchunkec8counts[i][j]=0;
			allchunkec4counts[i][j]=0;
			regchunkec4counts[i][j]=0;
		}
	}
	for (i=0 ; i<DANGER_PRIORITIES ; i++) {
		chq_queue_head[i] = NULL;
		chq_queue_tail[i] = chq_queue_head+i;
		chq_queue_elements[i] = 0;
		chq_queue_last_append_count[i] = 0;
		chq_queue_current_append_count[i] = 0;
		chq_queue_last_pop_count[i] = 0;
		chq_queue_current_pop_count[i] = 0;
		chq_queue_last_remove_count[i] = 0;
		chq_queue_current_remove_count[i] = 0;
	}
	for (i=0 ; i<MAXSCLASS ; i++) {
		for (j=0 ; j<JOB_EXIT_REASONS ; j++) {
			job_exit_reasons_last[i][j] = 0;
			job_exit_reasons[i][j] = 0;
		}
		for (j=0 ; j<=DANGER_PRIORITIES ; j++) {
			job_call_last[i][j] = 0;
			job_call[i][j] = 0;
			job_nocall_last[i][j] = 0;
			job_nocall[i][j] = 0;
		}
	}
	chq_hash = malloc(sizeof(chq_element*)*CHUNK_PRIORITY_HASHSIZE);
	passert(chq_hash);
	for (i=0 ; i<CHUNK_PRIORITY_HASHSIZE ; i++) {
		chq_hash[i] = NULL;
	}
	chq_elements = 0;
	chunk_do_jobs(NULL,JOBS_INIT,main_time(),0);	// clear chunk loop internal data, and allocate tabs
	chunk_calculate_endanger_priority(NULL,0);

	chunk_delay_init();

	main_reload_register(chunk_reload);
	// main_time_register(1,0,chunk_jobs_main);
	main_info_register(chunk_loginfo);
	jobs_timer = main_msectime_register(JobsTimerMiliSeconds,0,chunk_jobs_main);
	main_time_register(60,0,chunk_queue_counters_shift);
	main_time_register(60,0,chunk_job_exit_counters_shift);
	main_time_register(60,0,chunk_job_call_counters_shift);
	main_destruct_register(chunk_term);
	return 1;
}
