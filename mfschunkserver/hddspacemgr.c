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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// #include <execinfo.h> // for backtrace - debugs only
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#include "MFSCommunication.h"
#include "cfg.h"
#include "datapack.h"
#include "crc.h"
#include "main.h"
#include "masterconn.h"
#include "mfslog.h"
#include "massert.h"
#include "random.h"
#include "sizestr.h"
#include "clocks.h"
#include "strerr.h"
#include "portable.h"
#include "lwthread.h"
#include "sockets.h"
#include "bgjobs.h"
#include "ionice.h"

#define PRESERVE_BLOCK 1

// #define HDD_TESTER_DEBUG 1

#if defined(HAVE_PREAD) && defined(HAVE_PWRITE)
#define USE_PIO 1
#endif

#ifdef EWOULDBLOCK
#  define LOCK_ERRNO_ERROR (errno!=EACCES && errno!=EAGAIN && errno!=EWOULDBLOCK)
#else
#  define LOCK_ERRNO_ERROR (errno!=EACCES && errno!=EAGAIN)
#endif

#define DUPLICATES_DELETE_LIMIT 100

/* usec's to wait after last rebalance before choosing disk for new chunk */
#define REBALANCE_GRACE_PERIOD 10000000

#define REBALANCE_TOTAL_MIN 1000000000
#define REBALANCE_DST_MAX_USAGE 0.99
#define REBALANCE_DIFF_MAX 0.01

/* system every DELAYEDSTEP seconds searches opened/crc_loaded chunk list for chunks to be closed/free crc */
#define DELAYEDUSTEP 100000

#define OPEN_DELAY 0.5
#define CRC_DELAY 100

#ifdef PRESERVE_BLOCK
#define BLOCK_DELAY 10
#endif

#define CHUNKDB_REC_SIZE 23

#define LOCKED_CHUNK_WAIT_USECS 10000000

#define INODE_REDUCE_LOG_FREQ 60.0

#define LOSTCHUNKSBLOCKSIZE 1024
#define NEWCHUNKSBLOCKSIZE 4096

#define OLDHDRSIZE 1024
#define NEWHDRSIZE 4096

#define CHUNKCRCSIZE 4096
#define CHUNKMAXHDRSIZE (NEWHDRSIZE + CHUNKCRCSIZE)

#define STATSHISTORY (24*60)

#define LASTERRSIZE 30

#define RANDOM_CHUNK_RETRIES 50

// HASHSIZE / (60 * 1000)
#define KNOWNBLOCKS_HASH_PER_CYCLE 280

// important note - function hdd_regfirst assumes that highest byte of chunkid is not used in HASHPOS calculation
#define HASHSIZE (0x1000000)
#define HASHPOS(chunkid) ((chunkid)&0xFFFFFF)

#define DHASHSIZE 64
#define DHASHPOS(chunkid) ((chunkid)&0x3F)

#define WFRHASHSIZE (0x10000)
#define WFRHASHPOS(chunkid) ((chunkid)&0xFFFF)
#define WFRCURRENTBLOCK 256

#define CHMODE_EXISTING_ONLY 0
#define CHMODE_NEW_OR_EXISTING 1
#define CHMODE_NEW_ONLY 2
#define CHMODE_EXISTING_ONLY_WITH_ERRORS 3

#define CHUNKLOCKED ((void*)1)

#define MODE_EXISTING 0
#define MODE_IGNVERS 1
#define MODE_NEW 2

#ifdef USE_PIO
#define mypread pread
#define mypwrite pwrite
#else /* USE_PIO */
#define mypread(a,b,c,d) (lseek((a),(d),SEEK_SET),read((a),(b),(c)))
#define mypwrite(a,b,c,d) (lseek((a),(d),SEEK_SET),write((a),(b),(c)))
#endif /* USE_PIO */

#ifdef HAVE_MMAP
static uint8_t CanUseMmap = 0;

#define myalloc(ptr,size) if (CanUseMmap) { \
	ptr = mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0); \
} else { \
	ptr = malloc(size); \
}

#define myunalloc(ptr,size) if (CanUseMmap) { \
	munmap(ptr,size); \
} else { \
	free(ptr); \
}
#else /* HAVE_MMAP */
#define myalloc(ptr,size) ptr = malloc(size)
#define myunalloc(ptr,size) free(size)
#endif /* HAVE_MMAP */

/*
#define WFR_ENTRIES_IN_BLOCK ((4096 / (8+4+2)) - 2)

typedef struct waitforremoval {
	uint64_t chunkid[WFR_ENTRIES_IN_BLOCK];
	uint32_t version[WFR_ENTRIES_IN_BLOCK];
	uint16_t pathid[WFR_ENTRIES_IN_BLOCK];
	uint16_t entries;
	struct waitforremoval *next;
} waitforremoval;
*/

struct waitforremoval;
typedef struct waitforremoval waitforremoval;

typedef struct damagedchunk {
	uint64_t chunkid;
	struct damagedchunk *next;
} damagedchunk;

typedef struct lostchunk {
	uint64_t chunkidblock[LOSTCHUNKSBLOCKSIZE];
	uint32_t chunksinblock;
	struct lostchunk *next;
} lostchunk;

typedef struct newchunk {
	uint64_t chunkidblock[NEWCHUNKSBLOCKSIZE];
	uint32_t versionblock[NEWCHUNKSBLOCKSIZE];
	uint32_t chunksinblock;
	struct newchunk *next;
} newchunk;

#define CHGCHUNKSBLOCKSIZE NEWCHUNKSBLOCKSIZE
typedef newchunk chgchunk;
#define NONECHUNKSBLOCKSIZE LOSTCHUNKSBLOCKSIZE
typedef lostchunk nonechunk;

typedef struct dopchunk {
	uint64_t chunkid;
	struct dopchunk *next;
} dopchunk;

struct folder;

typedef struct ioerror {
	uint64_t chunkid;
	uint32_t timestamp;
	double monotonic_time;
	int errornumber;
} ioerror;

typedef struct _cntcond {
	pthread_cond_t cond;
	uint32_t wcnt;
	struct _cntcond *next;
} cntcond;

typedef struct chunk {
	uint64_t chunkid;
	struct folder *owner;
	uint32_t ownerindx;
	uint32_t version;
	uint16_t blocks;
	uint16_t crcrefcount;
	uint16_t pathid;
	uint16_t hdrsize;
	uint32_t diskusage;
	double opento;
	double crcto;
	uint8_t crcchanged;
	uint8_t fsyncneeded;
	uint8_t damaged;
#define CH_AVAIL 0
#define CH_LOCKED 1
#define CH_DELETED 2
	uint8_t state;	// CH_AVAIL,CH_LOCKED,CH_DELETED
	cntcond *ccond;
	uint8_t *crc;
	int fd;

#ifdef PRESERVE_BLOCK
	double blockto;
	uint8_t *block;
	uint16_t blockno;	// 0xFFFF == invalid
#endif
	uint8_t fileversion;
	uint8_t validattr;
	uint8_t testedflag;
	uint32_t testtime;
	struct chunk *testnext,**testprev;
	struct chunk *next;
} chunk;

typedef struct hddstats {
	uint64_t rbytes;
	uint64_t wbytes;
	uint64_t nsecreadsum;
	uint64_t nsecwritesum;
	uint64_t nsecfsyncsum;
	uint32_t rops;
	uint32_t wops;
	uint32_t fsyncops;
	uint64_t nsecreadmax;
	uint64_t nsecwritemax;
	uint64_t nsecfsyncmax;
} hddstats;

typedef struct folder {
	char *path;
#define SCST_WORKING 0
#define SCST_SCANNEEDED 1
#define SCST_ATTRNEEDED 2
#define SCST_SCANJOBINPROGRESS 3
#define SCST_ATTRJOBINPROGRESS 4
#define SCST_BGJOBTERMINATE 5
#define SCST_BGJOBFINISHED 6
	uint8_t scanstate;
	uint8_t sendneeded;
	uint8_t needrefresh;
#define MFR_NO 0
#define MFR_YES 1
#define MFR_READONLY 2
	uint8_t markforremoval;
#define REBALANCE_STD 0
#define REBALANCE_FORCE_SRC 1
#define REBALANCE_FORCE_DST 2
	uint8_t balancemode;
	uint8_t damaged;
#define REMOVING_NO 0
#define REMOVING_INPROGRESS 1
#define REMOVING_START 2
#define REMOVING_END 3
	uint8_t toremove;
#define REBALANCE_NONE 0
#define REBALANCE_SRC 1
#define REBALANCE_DST 2
	uint8_t tmpbalancemode;
	uint8_t ignoresize;
	uint8_t scanprogress;
#define LMODE_NONE 0
#define LMODE_LIMIT_TOTAL_POS_CONST 1
#define LMODE_LIMIT_TOTAL_POS_PERCENT 2
#define LMODE_LIMIT_TOTAL_NEG_CONST 3
#define LMODE_LIMIT_TOTAL_NEG_PERCENT 4
#define LMODE_SHARED 5
#define LMODE_SHARED_POS_CONST 6
#define LMODE_SHARED_POS_PERCENT 7
#define LMODE_SHARED_NEG_CONST 8
#define LMODE_SHARED_NEG_PERCENT 9
#define LMODE_NEEDS_CALCSIZE(x) ((x)>=1 && (x)<=9)
#define LDATA_IS_RATIO(x) ((x)==LMODE_LIMIT_TOTAL_POS_PERCENT || (x)==LMODE_LIMIT_TOTAL_NEG_PERCENT || (x)==LMODE_SHARED_POS_PERCENT || (x)==LMODE_SHARED_NEG_PERCENT)
#define LDATA_IS_VALUE(x) ((x)==LMODE_LIMIT_TOTAL_POS_CONST || (x)==LMODE_LIMIT_TOTAL_NEG_CONST || (x)==LMODE_SHARED_POS_CONST || (x)==LMODE_SHARED_NEG_CONST)
	uint8_t lmode;
	double ldata;
	uint64_t avail;
	uint64_t total;
	fsblkcnt_t lastblocks;
	uint8_t isro;
	hddstats cstat;
	hddstats monotonic;
	hddstats stats[STATSHISTORY];
	uint32_t statspos;
	ioerror lasterrtab[LASTERRSIZE];
	struct chunk **chunktab;
	uint32_t chunkcount; // all chunks
	uint32_t ec4chunkcount; // EC4 only (chunkid: 0x10... - 0x1F...)
	uint32_t ec8chunkcount; // EC8 only (chunkid: 0x20... - 0x3F...)
	uint32_t chunktabsize;
	uint32_t lasterrindx;
	double lastrefresh;
	uint32_t totalerrorcounter;
	double totalerrorstart;
	dev_t devid;
	ino_t lockinode;
	int lfd;
	int dumpfd;
	double read_corr;
	double write_corr;
	uint32_t read_dist;
	uint32_t write_dist;
	uint8_t read_first;
	uint8_t write_first;
	uint8_t rebalance_in_progress;
	uint64_t rebalance_last_usec;
//	double carry;
	pthread_t scanthread;
	struct chunk *testedhead,**testedtail;
	struct chunk *testneededhead,**testneededtail;
	uint32_t testedcnt,testneededcnt;
	uint32_t testfailcnt;
	uint32_t endlooptime;
	uint32_t startlooptime;
	uint32_t startlooptestneededcnt;
	uint64_t nexttest;
	uint32_t min_count;
	uint16_t min_pathid;
	uint16_t current_pathid;
	uint32_t subf_count[256];
	uint32_t knowncount;
	uint32_t knowncount_next;
	uint64_t knowndiskusage;
	uint64_t knowndiskusage_next;
	double iredlastrep;
	double wfrtime;
	double wfrlast;
	uint32_t wfrcount;
	waitforremoval *wfrchunks;
	struct folder *next;
} folder;

typedef struct cfgline {
	char *path;
	folder *f;
	struct cfgline *next;
} cfgline;

typedef struct regfirstchunk {
	chunk *c;
	struct regfirstchunk *next;
} regfirstchunk;

/*
typedef struct damaged {
	char *path;
	uint64_t avail;
	uint64_t total;
	ioerror lasterror;
	uint32_t chunkcount;
	struct damaged_disk *next;
} damaged;
*/

typedef struct _rebalance {
	folder *fsrc,*fdst;
} rebalance;

struct waitforremoval {
	folder *owner;
	uint64_t chunkid;
	uint32_t version;
	uint16_t pathid;
	waitforremoval *hashnext,**hashprev;
	waitforremoval *foldernext,**folderprev;
};

static waitforremoval* wfrcurrent[WFRCURRENTBLOCK];
static uint32_t wfrcurrentleng;
static waitforremoval* wfrhash[WFRHASHSIZE];

#ifndef HAVE___SYNC_OP_AND_FETCH
static pthread_mutex_t cfglock = PTHREAD_MUTEX_INITIALIZER;
#endif
//static uint8_t AllowStartingWithInvalidDisks;
static uint8_t Sparsification;
static double HDDTestMBPS = 1.0;
static uint32_t HDDRebalancePerc = 20;
static uint32_t HSRebalanceLimit = 0;
static uint32_t HDDErrorCount = 2;
static uint32_t HDDErrorTime = 600;
static uint32_t HDDRoundRobinChunkCount = 10000;
static uint32_t HDDKeepDuplicatesHours = 7*24;
static uint64_t LeaveFree;
static uint8_t DoFsyncBeforeClose = 0;
static uint32_t MinTimeBetweenTests = 86400;
static int32_t MinFlushCacheTime = 86400;

/* cfg data - locked by folderlock together with folderhead */
static cfgline *cfglinehead = NULL;

/* folders data */
static folder *folderhead = NULL;

/* chunk hash */
static chunk* hashtab[HASHSIZE];

/* extra chunk info */
static dopchunk *dophashtab[DHASHSIZE];
//static dopchunk *dopchunks = NULL;
static dopchunk *newdopchunks = NULL;

/* chunks that should be send first during registering */
static regfirstchunk *regfirstchunks = NULL;

// master reports
static damagedchunk *damagedchunks = NULL;
static lostchunk *lostchunks = NULL;
static nonechunk *nonexistentchunks = NULL;
static newchunk *newchunks = NULL;
static chgchunk *chgchunks = NULL;
static uint32_t errorcounter = 0;
static uint8_t hddspacechanged = 0;
static uint8_t hddspacerecalc = 0;
static uint8_t global_rebalance_is_on = 0;

static pthread_t hsrebalancethread,rebalancethread,foldersthread,delayedthread,testerthread,knowndiskusagethread;
static uint8_t term = 0;
static uint8_t folderactions = 0;
static pthread_mutex_t termlock = PTHREAD_MUTEX_INITIALIZER;

// stats_X
static pthread_mutex_t statslock = PTHREAD_MUTEX_INITIALIZER;

// newdopchunks + dophashtab
static pthread_mutex_t doplock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t ndoplock = PTHREAD_MUTEX_INITIALIZER;

// master reports = damaged chunks, lost chunks, errorcounter, hddspacechanged, hddspacerecalc, global_rebalance_is_on
static pthread_mutex_t dclock = PTHREAD_MUTEX_INITIALIZER;

// hashtab - only hash tab, chunks have their own separate locks
static pthread_mutex_t hashlock = PTHREAD_MUTEX_INITIALIZER;
static cntcond *cclist = NULL;

// folderhead + all data in structures
static pthread_mutex_t folderlock = PTHREAD_MUTEX_INITIALIZER;

// chunk tester
static pthread_mutex_t testlock = PTHREAD_MUTEX_INITIALIZER;

static pthread_cond_t highspeed_cond = PTHREAD_COND_INITIALIZER;

#ifndef PRESERVE_BLOCK
static pthread_key_t hdrbufferkey;
static pthread_key_t blockbufferkey;
#endif

/*
static uint8_t wait_for_scan = 0;
static uint32_t scanprogress;
static uint8_t scanprogresswaiting;
static pthread_cond_t scanprogresscond = PTHREAD_COND_INITIALIZER;
*/

typedef struct calc_space_str {
	uint64_t usedspace;
	uint64_t totalspace;
	uint32_t chunkcount;
	uint64_t mfrusedspace;
	uint64_t mfrtotalspace;
	uint32_t mfrchunkcount;
	uint32_t ec4chunkcount;
	uint32_t ec8chunkcount;
	uint32_t usagediff;
	uint32_t hddokcount;
	uint32_t hddmfrcount;
	uint32_t hdddmgcount;
} calc_space_data;

static calc_space_data global_csd = {0,0,0,0,0,0,0,0,0,0,0,0};

static uint32_t emptyblockcrc;
static uint8_t *emptychunkcrc;

static uint64_t stats_bytesr = 0;
static uint64_t stats_bytesw = 0;
static uint32_t stats_opr = 0;
static uint32_t stats_opw = 0;
static uint32_t stats_databytesr = 0;
static uint32_t stats_databytesw = 0;
static uint32_t stats_dataopr = 0;
static uint32_t stats_dataopw = 0;
static uint32_t stats_movels = 0;
static uint32_t stats_movehs = 00;
static uint64_t stats_rtime = 0;
static uint64_t stats_wtime = 0;

static uint32_t stats_create = 0;
static uint32_t stats_delete = 0;
static uint32_t stats_test = 0;
static uint32_t stats_version = 0;
static uint32_t stats_duplicate = 0;
static uint32_t stats_truncate = 0;
static uint32_t stats_duptrunc = 0;
static uint32_t stats_split = 0;

static uint32_t oflimit;

static inline void hdd_stats_clear(hddstats *r) {
	memset(r,0,sizeof(hddstats));
}

static inline void hdd_stats_add(hddstats *dst,hddstats *src) {
	dst->rbytes += src->rbytes;
	dst->wbytes += src->wbytes;
	dst->nsecreadsum += src->nsecreadsum;
	dst->nsecwritesum += src->nsecwritesum;
	dst->nsecfsyncsum += src->nsecfsyncsum;
	dst->rops += src->rops;
	dst->wops += src->wops;
	dst->fsyncops += src->fsyncops;
	if (src->nsecreadmax>dst->nsecreadmax) {
		dst->nsecreadmax = src->nsecreadmax;
	}
	if (src->nsecwritemax>dst->nsecwritemax) {
		dst->nsecwritemax = src->nsecwritemax;
	}
	if (src->nsecfsyncmax>dst->nsecfsyncmax) {
		dst->nsecfsyncmax = src->nsecfsyncmax;
	}
}

/* size: 64 */
static inline void hdd_stats_binary_pack(uint8_t **buff,hddstats *r) {
	put64bit(buff,r->rbytes);
	put64bit(buff,r->wbytes);
	put64bit(buff,r->nsecreadsum/1000);
	put64bit(buff,r->nsecwritesum/1000);
	put64bit(buff,r->nsecfsyncsum/1000);
	put32bit(buff,r->rops);
	put32bit(buff,r->wops);
	put32bit(buff,r->fsyncops);
	put32bit(buff,r->nsecreadmax/1000);
	put32bit(buff,r->nsecwritemax/1000);
	put32bit(buff,r->nsecfsyncmax/1000);
}

/*
void printbacktrace(void) {
	void* callstack[128];
	int i, frames = backtrace(callstack, 128);
	char** strs = backtrace_symbols(callstack, frames);
	for (i=0 ; i<frames ; ++i) {
		printf("%s\n", strs[i]);
	}
	free(strs);
}
*/

static inline void hdd_create_filename(char fname[PATH_MAX],const char *fpath,uint16_t pathid,uint64_t chunkid,uint32_t version) {
	if (pathid<256) {
		int pl = strlen(fpath);
		if (pl>PATH_MAX-100) {
			snprintf(fname,PATH_MAX,"/dev/null");
		} else if (pl>0 && fpath[pl-1]=='/') {
			snprintf(fname,PATH_MAX,"%s%02"PRIX16"/chunk_%016"PRIX64"_%08"PRIX32".mfs",fpath,pathid,chunkid,version);
		} else {
			snprintf(fname,PATH_MAX,"%s/%02"PRIX16"/chunk_%016"PRIX64"_%08"PRIX32".mfs",fpath,pathid,chunkid,version);
		}
		fname[PATH_MAX-1]=0;
	} else {
		snprintf(fname,PATH_MAX,"/dev/null");
	}
}

void hdd_generate_filename(char fname[PATH_MAX],chunk *c) {
	int errmem = errno;
	if (c->owner!=NULL) {
		hdd_create_filename(fname,c->owner->path,c->pathid,c->chunkid,c->version);
	} else {
		hdd_create_filename(fname,"(unknown)",c->pathid,c->chunkid,c->version);
	}
	errno = errmem;
}

void hdd_report_damaged_chunk(chunk *c) {
	damagedchunk *dc;
	c->damaged = 1;
	zassert(pthread_mutex_lock(&dclock));
	dc = malloc(sizeof(damagedchunk));
	passert(dc);
	dc->chunkid = c->chunkid;
	dc->next = damagedchunks;
	damagedchunks = dc;
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_get_damaged_chunk_count(void) {
	damagedchunk *dc;
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = 0;
	for (dc=damagedchunks ; dc ; dc=dc->next) {
		result++;
	}
	return result;
}

void hdd_get_damaged_chunk_data(uint8_t *buff) {
	damagedchunk *dc,*ndc;
	uint64_t chunkid;
	if (buff) {
		dc = damagedchunks;
		while (dc) {
			ndc = dc;
			dc = dc->next;
			chunkid = ndc->chunkid;
			put64bit(&buff,chunkid);
			free(ndc);
		}
		damagedchunks = NULL;
	}
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_report_nonexistent_chunk(uint64_t chunkid) {
	nonechunk *nec;
	zassert(pthread_mutex_lock(&dclock));
	if (nonexistentchunks && nonexistentchunks->chunksinblock<NONECHUNKSBLOCKSIZE) {
		nonexistentchunks->chunkidblock[nonexistentchunks->chunksinblock++] = chunkid;
	} else {
		nec = malloc(sizeof(nonechunk));
		passert(nec);
		nec->chunkidblock[0] = chunkid;
		nec->chunksinblock = 1;
		nec->next = nonexistentchunks;
		nonexistentchunks = nec;
	}
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_get_nonexistent_chunk_count(uint32_t limit) {
	nonechunk *nec;
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = 0;
	for (nec=nonexistentchunks ; nec ; nec=nec->next) {
		if (limit>nec->chunksinblock) {
			limit -= nec->chunksinblock;
			result += nec->chunksinblock;
		}
	}
	return result;
}

void hdd_get_nonexistent_chunk_data(uint8_t *buff,uint32_t limit) {
	nonechunk *nec,**necptr;
	uint64_t chunkid;
	uint32_t i;
	if (limit>0) {
		necptr = &nonexistentchunks;
		while ((nec=*necptr)) {
			if (limit>nec->chunksinblock) {
				if (buff) {
					for (i=0 ; i<nec->chunksinblock ; i++) {
						chunkid = nec->chunkidblock[i];
						put64bit(&buff,chunkid);
					}
				}
				limit -= nec->chunksinblock;
				*necptr = nec->next;
				free(nec);
			} else {
				necptr = &(nec->next);
			}
		}
	}
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_report_lost_chunk(uint64_t chunkid) {
	lostchunk *lc;
	zassert(pthread_mutex_lock(&dclock));
	if (lostchunks && lostchunks->chunksinblock<LOSTCHUNKSBLOCKSIZE) {
		lostchunks->chunkidblock[lostchunks->chunksinblock++] = chunkid;
	} else {
		lc = malloc(sizeof(lostchunk));
		passert(lc);
		lc->chunkidblock[0] = chunkid;
		lc->chunksinblock = 1;
		lc->next = lostchunks;
		lostchunks = lc;
	}
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_get_lost_chunk_count(uint32_t limit) {
	lostchunk *lc;
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = 0;
	for (lc=lostchunks ; lc ; lc=lc->next) {
		if (limit>lc->chunksinblock) {
			limit -= lc->chunksinblock;
			result += lc->chunksinblock;
		}
	}
	return result;
}

void hdd_get_lost_chunk_data(uint8_t *buff,uint32_t limit) {
	lostchunk *lc,**lcptr;
	uint64_t chunkid;
	uint32_t i;
	if (buff) {
		lcptr = &lostchunks;
		while ((lc=*lcptr)) {
			if (limit>lc->chunksinblock) {
				for (i=0 ; i<lc->chunksinblock ; i++) {
					chunkid = lc->chunkidblock[i];
					put64bit(&buff,chunkid);
				}
				limit -= lc->chunksinblock;
				*lcptr = lc->next;
				free(lc);
			} else {
				lcptr = &(lc->next);
			}
		}
	}
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_report_new_chunk(uint64_t chunkid,uint32_t version) {
	newchunk *nc;
	zassert(pthread_mutex_lock(&dclock));
	if (newchunks && newchunks->chunksinblock<NEWCHUNKSBLOCKSIZE) {
		newchunks->chunkidblock[newchunks->chunksinblock] = chunkid;
		newchunks->versionblock[newchunks->chunksinblock] = version;
		newchunks->chunksinblock++;
	} else {
		nc = malloc(sizeof(newchunk));
		passert(nc);
		nc->chunkidblock[0] = chunkid;
		nc->versionblock[0] = version;
		nc->chunksinblock = 1;
		nc->next = newchunks;
		newchunks = nc;
	}
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_get_new_chunk_count(uint32_t limit) {
	newchunk *nc;
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = 0;
	for (nc=newchunks ; nc ; nc=nc->next) {
		if (limit>nc->chunksinblock) {
			limit -= nc->chunksinblock;
			result += nc->chunksinblock;
		}
	}
	return result;
}

void hdd_get_new_chunk_data(uint8_t *buff,uint32_t limit) {
	newchunk *nc,**ncptr;
	uint64_t chunkid;
	uint32_t version;
	uint32_t i;
	if (buff) {
		ncptr = &newchunks;
		while ((nc=*ncptr)) {
			if (limit>nc->chunksinblock) {
				for (i=0 ; i<nc->chunksinblock ; i++) {
					chunkid = nc->chunkidblock[i];
					version = nc->versionblock[i];
					put64bit(&buff,chunkid);
					put32bit(&buff,version);
				}
				limit -= nc->chunksinblock;
				*ncptr = nc->next;
				free(nc);
			} else {
				ncptr = &(nc->next);
			}
		}
	}
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_report_changed_chunk(uint64_t chunkid,uint32_t version) {
	chgchunk *xc;
	zassert(pthread_mutex_lock(&dclock));
	if (chgchunks && chgchunks->chunksinblock<CHGCHUNKSBLOCKSIZE) {
		chgchunks->chunkidblock[chgchunks->chunksinblock] = chunkid;
		chgchunks->versionblock[chgchunks->chunksinblock] = version;
		chgchunks->chunksinblock++;
	} else {
		xc = malloc(sizeof(chgchunk));
		passert(xc);
		xc->chunkidblock[0] = chunkid;
		xc->versionblock[0] = version;
		xc->chunksinblock = 1;
		xc->next = chgchunks;
		chgchunks = xc;
	}
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_get_changed_chunk_count(uint32_t limit) {
	chgchunk *xc;
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = 0;
	for (xc=chgchunks ; xc ; xc=xc->next) {
		if (limit>xc->chunksinblock) {
			limit -= xc->chunksinblock;
			result += xc->chunksinblock;
		}
	}
	return result;
}

void hdd_get_changed_chunk_data(uint8_t *buffl,uint8_t *buffn,uint32_t limit) {
	chgchunk *xc,**xcptr;
	uint64_t chunkid;
	uint32_t version;
	uint32_t i;
	if (buffl && buffn) {
		xcptr = &chgchunks;
		while ((xc=*xcptr)) {
			if (limit>xc->chunksinblock) {
				for (i=0 ; i<xc->chunksinblock ; i++) {
					chunkid = xc->chunkidblock[i];
					version = xc->versionblock[i];
					put64bit(&buffl,chunkid);
					put64bit(&buffn,chunkid);
					put32bit(&buffn,version);
				}
				limit -= xc->chunksinblock;
				*xcptr = xc->next;
				free(xc);
			} else {
				xcptr = &(xc->next);
			}
		}
	}
	zassert(pthread_mutex_unlock(&dclock));
}

uint32_t hdd_errorcounter(void) {
	uint32_t result;
	zassert(pthread_mutex_lock(&dclock));
	result = errorcounter;
	errorcounter = 0;
	zassert(pthread_mutex_unlock(&dclock));
	return result;
}

uint8_t hdd_spacechanged(void) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	return __sync_fetch_and_and(&hddspacechanged,0);
#else
	uint8_t res;
	zassert(pthread_mutex_lock(&dclock));
	res = hddspacechanged;
	hddspacechanged = 0;
	zassert(pthread_mutex_unlock(&dclock));
	return res;
#endif
}

void hdd_stats(uint64_t *br,uint64_t *bw,uint32_t *opr,uint32_t *opw,uint32_t *dbr,uint32_t *dbw,uint32_t *dopr,uint32_t *dopw,uint32_t *movl,uint32_t *movh,uint64_t *rtime,uint64_t *wtime) {
	zassert(pthread_mutex_lock(&statslock));
	*br = stats_bytesr;
	*bw = stats_bytesw;
	*opr = stats_opr;
	*opw = stats_opw;
	*dbr = stats_databytesr;
	*dbw = stats_databytesw;
	*dopr = stats_dataopr;
	*dopw = stats_dataopw;
	*movl = stats_movels;
	*movh = stats_movehs;
	*rtime = stats_rtime;
	*wtime = stats_wtime;
	stats_bytesr = 0;
	stats_bytesw = 0;
	stats_opr = 0;
	stats_opw = 0;
	stats_databytesr = 0;
	stats_databytesw = 0;
	stats_dataopr = 0;
	stats_dataopw = 0;
	stats_movels = 0;
	stats_movehs = 0;
	stats_rtime = 0;
	stats_wtime = 0;
	zassert(pthread_mutex_unlock(&statslock));
}

void hdd_op_stats(uint32_t *op_create,uint32_t *op_delete,uint32_t *op_version,uint32_t *op_duplicate,uint32_t *op_truncate,uint32_t *op_duptrunc,uint32_t *op_test,uint32_t *op_split) {
	zassert(pthread_mutex_lock(&statslock));
	*op_create = stats_create;
	*op_delete = stats_delete;
	*op_version = stats_version;
	*op_duplicate = stats_duplicate;
	*op_truncate = stats_truncate;
	*op_duptrunc = stats_duptrunc;
	*op_test = stats_test;
	*op_split = stats_split;
	stats_create = 0;
	stats_delete = 0;
	stats_version = 0;
	stats_duplicate = 0;
	stats_truncate = 0;
	stats_duptrunc = 0;
	stats_split = 0;
	stats_test = 0;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_move(uint8_t hsflag) {
	zassert(pthread_mutex_lock(&statslock));
	if (hsflag) {
		stats_movehs++;
	} else {
		stats_movels++;
	}
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_read(uint32_t size) {
	zassert(pthread_mutex_lock(&statslock));
	stats_opr++;
	stats_bytesr += size;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_write(uint32_t size) {
	zassert(pthread_mutex_lock(&statslock));
	stats_opw++;
	stats_bytesw += size;
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_dataread(folder *f,uint32_t size,int64_t rtime) {
	if (rtime<=0) {
		return;
	}
	zassert(pthread_mutex_lock(&statslock));
	stats_dataopr++;
	stats_databytesr += size;
	stats_rtime += rtime;
	f->cstat.rops++;
	f->cstat.rbytes += size;
	f->cstat.nsecreadsum += rtime;
	if (rtime>(int64_t)(f->cstat.nsecreadmax)) {
		f->cstat.nsecreadmax = rtime;
	}
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_datawrite(folder *f,uint32_t size,int64_t wtime) {
	if (wtime<=0) {
		return;
	}
	zassert(pthread_mutex_lock(&statslock));
	stats_dataopw++;
	stats_databytesw += size;
	stats_wtime += wtime;
	f->cstat.wops++;
	f->cstat.wbytes += size;
	f->cstat.nsecwritesum += wtime;
	if (wtime>(int64_t)(f->cstat.nsecwritemax)) {
		f->cstat.nsecwritemax = wtime;
	}
	zassert(pthread_mutex_unlock(&statslock));
}

static inline void hdd_stats_datafsync(folder *f,int64_t fsynctime) {
	if (fsynctime<=0) {
		return;
	}
	zassert(pthread_mutex_lock(&statslock));
	stats_wtime += fsynctime;
	f->cstat.fsyncops++;
	f->cstat.nsecfsyncsum += fsynctime;
	if (fsynctime>(int64_t)(f->cstat.nsecfsyncmax)) {
		f->cstat.nsecfsyncmax = fsynctime;
	}
	zassert(pthread_mutex_unlock(&statslock));
}

uint8_t hdd_sendingchunks(void) {
	folder *f;
	uint8_t result;

	result = 0;
	zassert(pthread_mutex_lock(&dclock));
	if (lostchunks!=NULL) {
		result |= TRANSFERING_LOST_CHUNKS;
	}
	if (newchunks!=NULL) {
		result |= TRANSFERING_NEW_CHUNKS;
	}
	zassert(pthread_mutex_unlock(&dclock));
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		if (f->toremove!=REMOVING_NO) {
			result |= TRANSFERING_LOST_CHUNKS;
		}
		if (f->scanstate==SCST_SCANJOBINPROGRESS || f->scanstate==SCST_SCANNEEDED) {
			result |= TRANSFERING_NEW_CHUNKS;
		}
// maybe in the future the master server will want to know that, but currently I don't see what the master can do with such infomration
//		if (f->scanstate==SCST_ATTRJOBINPROGRESS || f->scanstate==SCST_ATTRNEEDED) {
//			result |= SCANNING_ATTRIBUTES;
//		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
	return result;
}

// works as a lock/unlock pair always has to be called twice, first with buff set to NULL and then with correct buff
uint32_t hdd_chunk_status(uint64_t chunkid,uint8_t *buff) {
	uint32_t res;
	chunk *c;
	uint32_t v;
	uint32_t hashpos = HASHPOS(chunkid);

	if (buff==NULL) {
		res = 8;
		zassert(pthread_mutex_lock(&folderlock));
		zassert(pthread_mutex_lock(&hashlock));
		for (c=hashtab[hashpos] ; c!=NULL ; c=c->next) {
			if ((c->chunkid&UINT64_C(0x00FFFFFFFFFFFFFF))==(chunkid&UINT64_C(0x00FFFFFFFFFFFFFF))) {
				res += 8;
			}
		}
	} else {
		res = 0;
		put64bit(&buff,chunkid);
		for (c=hashtab[hashpos] ; c!=NULL ; c=c->next) {
			if ((c->chunkid&UINT64_C(0x00FFFFFFFFFFFFFF))==(chunkid&UINT64_C(0x00FFFFFFFFFFFFFF))) {
				put8bit(&buff,(c->chunkid>>56));
				put8bit(&buff,c->damaged);
				put16bit(&buff,c->blocks);
				v = c->version;
				if (c->owner!=NULL && c->owner->markforremoval!=MFR_NO) {
					v |= 0x80000000;
				}
				put32bit(&buff,v);
			}
		}
		zassert(pthread_mutex_unlock(&hashlock));
		zassert(pthread_mutex_unlock(&folderlock));
	}
	return res;
}

uint32_t hdd_diskinfo_size(void) {
	cfgline *cl;
	uint32_t s,sl;

	s = 0;
	zassert(pthread_mutex_lock(&folderlock));
	for (cl=cfglinehead ; cl!=NULL ; cl=cl->next ) {
		sl = strlen(cl->path);
		if (sl>255) {
			sl = 255;
		}
		s += 2+34+3*64+sl;
	}
	return s;
}

void hdd_diskinfo_data(uint8_t *buff) {
	cfgline *cl;
	folder *f;
	hddstats s;
	uint32_t sl;
	uint32_t ei;
	uint32_t pos;
	if (buff) {
		zassert(pthread_mutex_lock(&statslock));
		for (cl=cfglinehead ; cl!=NULL ; cl=cl->next ) {
			f = cl->f;
			sl = strlen(cl->path);
			if (sl>255) {
				put16bit(&buff,34+3*64+255);	// size of this entry
				put8bit(&buff,255);
				memcpy(buff,"(...)",5);
				memcpy(buff+5,cl->path+(sl-250),250);
				buff += 255;
			} else {
				put16bit(&buff,34+3*64+sl);	// size of this entry
				put8bit(&buff,sl);
				if (sl>0) {
					memcpy(buff,cl->path,sl);
					buff += sl;
				}
			}
			if (f!=NULL) {
				put8bit(&buff,((f->markforremoval)?CS_HDD_MFR:0)+((f->damaged)?CS_HDD_DAMAGED:0)+((f->scanstate==SCST_SCANJOBINPROGRESS || f->scanstate==SCST_ATTRJOBINPROGRESS)?CS_HDD_SCANNING:0));
				ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
				put64bit(&buff,f->lasterrtab[ei].chunkid);
				put32bit(&buff,f->lasterrtab[ei].timestamp);
				if (f->scanstate==SCST_SCANJOBINPROGRESS || f->scanstate==SCST_ATTRJOBINPROGRESS) {
					put64bit(&buff,f->scanprogress);
					put64bit(&buff,0);
				} else {
					put64bit(&buff,f->total-f->avail);
					put64bit(&buff,f->total);
				}
				put32bit(&buff,f->chunkcount);
				s = f->stats[f->statspos];
				hdd_stats_binary_pack(&buff,&s);	// 64B
				for (pos=1 ; pos<60 ; pos++) {
					hdd_stats_add(&s,&(f->stats[(f->statspos+pos)%STATSHISTORY]));
				}
				hdd_stats_binary_pack(&buff,&s);	// 64B
				for (pos=60 ; pos<24*60 ; pos++) {
					hdd_stats_add(&s,&(f->stats[(f->statspos+pos)%STATSHISTORY]));
				}
				hdd_stats_binary_pack(&buff,&s);	// 64B
			} else {
				put8bit(&buff,CS_HDD_DAMAGED+CS_HDD_INVALID);
				memset(buff,0,32+3*64);
				buff+=32+3*64;
			}
		}
		zassert(pthread_mutex_unlock(&statslock));
	}
	zassert(pthread_mutex_unlock(&folderlock));
}

uint32_t hdd_diskinfo_monotonic_size(void) {
	folder *f;
	uint32_t s,sl;

	s = 2;
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next ) {
		sl = strlen(f->path);
		if (sl>255) {
			sl = 255;
		}
		s += 2+34+64+sl;
	}
	return s;
}

void hdd_diskinfo_monotonic_data(uint8_t *buff) {
	folder *f;
	uint32_t sl;
	uint32_t ei;
	uint16_t cnt;
	if (buff) {
		cnt = 0;
		for (f=folderhead ; f ; f=f->next ) {
			cnt++;
		}
		put16bit(&buff,cnt);
		zassert(pthread_mutex_lock(&statslock));
		for (f=folderhead ; f ; f=f->next ) {
			sl = strlen(f->path);
			if (sl>255) {
				put16bit(&buff,34+64+255);	// size of this entry
				put8bit(&buff,255);
				memcpy(buff,"(...)",5);
				memcpy(buff+5,f->path+(sl-250),250);
				buff += 255;
			} else {
				put16bit(&buff,34+64+sl);	// size of this entry
				put8bit(&buff,sl);
				if (sl>0) {
					memcpy(buff,f->path,sl);
					buff += sl;
				}
			}
			put8bit(&buff,((f->markforremoval)?CS_HDD_MFR:0)+((f->damaged)?CS_HDD_DAMAGED:0)+((f->scanstate==SCST_SCANJOBINPROGRESS || f->scanstate==SCST_ATTRJOBINPROGRESS)?CS_HDD_SCANNING:0));
			ei = (f->lasterrindx+(LASTERRSIZE-1))%LASTERRSIZE;
			put64bit(&buff,f->lasterrtab[ei].chunkid);
			put32bit(&buff,f->lasterrtab[ei].timestamp);
			if (f->scanstate==SCST_SCANJOBINPROGRESS || f->scanstate==SCST_ATTRJOBINPROGRESS) {
				put64bit(&buff,f->scanprogress);
				put64bit(&buff,0);
			} else {
				put64bit(&buff,f->total-f->avail);
				put64bit(&buff,f->total);
			}
			put32bit(&buff,f->chunkcount);
			hdd_stats_binary_pack(&buff,&(f->monotonic));	// 64B
		}
		zassert(pthread_mutex_unlock(&statslock));
	}
	zassert(pthread_mutex_unlock(&folderlock));
}

#define OF_BEFORE_OPEN 0
#define OF_AFTER_CLOSE 1
#define OF_INIT 2
#define OF_INFO 3

static inline uint32_t hdd_open_files_handle(uint8_t mode) {
	static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
	static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
	static uint32_t count = 0;
	static uint32_t limit = 500;
	static uint32_t waiting = 0;
	uint32_t c;
	if (mode==OF_BEFORE_OPEN) { // before open
		zassert(pthread_mutex_lock(&lock));
		while (count >= limit) {
			waiting++;
			zassert(pthread_cond_wait(&cond,&lock));
		}
		count++;
		c = count;
		zassert(pthread_mutex_unlock(&lock));
		return c;
	} else if (mode==OF_AFTER_CLOSE) { // after close
		zassert(pthread_mutex_lock(&lock));
		count--;
		if (waiting>0) {
			zassert(pthread_cond_signal(&cond));
			waiting--;
		}
		c = count;
		zassert(pthread_mutex_unlock(&lock));
		return c;
	} else if (mode==OF_INIT) {
		struct rlimit rl;
		getrlimit(RLIMIT_NOFILE,&rl);
		limit = (rl.rlim_cur * 2) / 3;
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"hdd space manager: setting open chunks limit to: %"PRIu32,limit);
		return limit;
	} else if (mode==OF_INFO) {
		zassert(pthread_mutex_lock(&lock));
		c = count;
		zassert(pthread_mutex_unlock(&lock));
		return c;
	}
	return 0;
}

/* testing errors
static inline void hdd_fake_error(folder *f,uint64_t chunkid) {
	uint32_t i;
	struct timeval tv;

	if (f!=NULL) {
		gettimeofday(&tv,NULL);
		i = f->lasterrindx;
		f->lasterrtab[i].chunkid = chunkid;
		f->lasterrtab[i].errornumber = EINVAL;
		f->lasterrtab[i].timestamp = tv.tv_sec;
		f->lasterrtab[i].monotonic_time = monotonic_seconds();
		i = (i+1)%LASTERRSIZE;
		f->lasterrindx = i;
		f->totalerrorcounter++;
	}
}
*/
void hdd_diskinfo_movestats(void) {
	folder *f;
	zassert(pthread_mutex_lock(&folderlock));
	zassert(pthread_mutex_lock(&statslock));
	for (f=folderhead ; f ; f=f->next ) {
		if (f->statspos==0) {
			f->statspos = STATSHISTORY-1;
		} else {
			f->statspos--;
		}
		f->stats[f->statspos] = f->cstat;
		hdd_stats_add(&(f->monotonic),&(f->cstat));
		hdd_stats_clear(&(f->cstat));
/* testing errors
		if ((random()&0x0f)==0) {
			hdd_fake_error(f,random());
		}
*/
	}
	zassert(pthread_mutex_unlock(&statslock));
	zassert(pthread_mutex_unlock(&folderlock));
}

// testlock:locked
static inline void hdd_remove_chunk_from_test_chain(chunk *c,folder *f) {
	*(c->testprev) = c->testnext;
	if (c->testnext) {
		c->testnext->testprev = c->testprev;
	} else {
		if (c->testedflag) {
			f->testedtail = c->testprev;
		} else {
			f->testneededtail = c->testprev;
		}
	}
	c->testnext = NULL;
	c->testprev = NULL;
	if (c->testedflag) {
		f->testedcnt--;
	} else {
		f->testneededcnt--;
	}
	if (f->subf_count[c->pathid]>0) {
		f->subf_count[c->pathid]--;
		if (f->subf_count[c->pathid]<f->min_count) {
			f->min_count = f->subf_count[c->pathid];
			f->min_pathid = c->pathid;
		}
	}
}

// testlock:locked
static inline void hdd_add_chunk_to_test_chain(chunk *c,folder *f,uint8_t testedflag) {
	uint8_t recalcmin;
	uint16_t i;

	c->testedflag = testedflag;
	c->testnext = NULL;
	if (testedflag) {
		c->testprev = f->testedtail;
		*(c->testprev) = c;
		f->testedtail = &(c->testnext);
		f->testedcnt++;
	} else {
		c->testprev = f->testneededtail;
		*(c->testprev) = c;
		f->testneededtail = &(c->testnext);
		f->testneededcnt++;
	}
	if (f->subf_count[c->pathid]<=f->min_count) {
		recalcmin=1;
	} else {
		recalcmin=0;
	}
	f->subf_count[c->pathid]++;
	if (recalcmin) {
		f->min_count = f->subf_count[0];
		f->min_pathid = 0;
		for (i=1 ; i<256 ; i++) {
			if (f->subf_count[i] < f->min_count) {
				f->min_count = f->subf_count[i];
				f->min_pathid = i;
			}
		}
	}
	if (c->pathid==f->current_pathid && f->subf_count[c->pathid]>=f->min_count+HDDRoundRobinChunkCount) {
		f->current_pathid = f->min_pathid;
	}
}

// folderlock:locked
static inline void hdd_remove_chunk_from_folder(chunk *c,folder *f) {
	if (c->chunkid>UINT64_C(0x1000000000000000) && c->chunkid<UINT64_C(0x2000000000000000)) {
		f->ec4chunkcount--;
	}
	if (c->chunkid>UINT64_C(0x2000000000000000) && c->chunkid<UINT64_C(0x4000000000000000)) {
		f->ec8chunkcount--;
	}
	f->chunkcount--;
	f->chunktab[c->ownerindx] = f->chunktab[f->chunkcount];
	f->chunktab[c->ownerindx]->ownerindx = c->ownerindx;
	c->owner = NULL;
	c->ownerindx = 0;
}

// folderlock:locked
static inline void hdd_add_chunk_to_folder(chunk *c,folder *f) {
	if (f->chunkcount==f->chunktabsize) {
		if (f->chunktabsize==0) {
			f->chunktabsize=10000;
			f->chunktab = malloc(sizeof(chunk*)*f->chunktabsize);
		} else {
			f->chunktabsize*=3;
			f->chunktabsize/=2;
			f->chunktab = realloc(f->chunktab,sizeof(chunk*)*f->chunktabsize);
		}
		passert(f->chunktab);
	}
	f->chunktab[f->chunkcount] = c;
	c->owner = f;
	c->ownerindx = f->chunkcount;
	f->chunkcount++;
	if (c->chunkid>UINT64_C(0x1000000000000000) && c->chunkid<UINT64_C(0x2000000000000000)) {
		f->ec4chunkcount++;
	}
	if (c->chunkid>UINT64_C(0x2000000000000000) && c->chunkid<UINT64_C(0x4000000000000000)) {
		f->ec8chunkcount++;
	}
}

uint8_t hdd_clear_errors(uint32_t pleng,const uint8_t *path) {
	folder *f;
	uint32_t i;
	uint8_t res;

	res = 0;
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next ) {
		if (pleng==0 || (strlen(f->path)==pleng && memcmp(f->path,path,pleng)==0)) {
			f->lasterrindx = 0;
			for (i=0 ; i<LASTERRSIZE ; i++) {
				f->lasterrtab[i].chunkid = 0ULL;
				f->lasterrtab[i].timestamp = 0;
				f->lasterrtab[i].monotonic_time = 0.0;
				f->lasterrtab[i].errornumber = 0;
			}
			f->totalerrorcounter = 0;
			f->totalerrorstart = monotonic_seconds();
			res++;
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
	return res;
}

static inline void hdd_error_occured(chunk *c,int report_damaged) {
	uint32_t i;
	folder *f;
	struct timeval tv;
	int errmem = errno;

	f = c->owner;
	if (f!=NULL) {
		zassert(pthread_mutex_lock(&folderlock));
		gettimeofday(&tv,NULL);
		i = f->lasterrindx;
		f->lasterrtab[i].chunkid = c->chunkid;
		f->lasterrtab[i].errornumber = errmem;
		f->lasterrtab[i].timestamp = tv.tv_sec;
		f->lasterrtab[i].monotonic_time = monotonic_seconds();
		i = (i+1)%LASTERRSIZE;
		f->lasterrindx = i;
		f->totalerrorcounter++;
		zassert(pthread_mutex_unlock(&folderlock));

		zassert(pthread_mutex_lock(&dclock));
		errorcounter++;
		zassert(pthread_mutex_unlock(&dclock));
	}

	if (report_damaged) {
		hdd_report_damaged_chunk(c);
	}

	errno = errmem;
}

static inline int chunk_writecrc(chunk *c,uint8_t emergency_mode);

static inline void hdd_chunk_flush(chunk *c) {
	if (c->fd>=0) {
		if (c->crcchanged && c->owner!=NULL) { // mainly pro forma
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_chunk_flush: CRC not flushed - writing now");
			chunk_writecrc(c,0);
			c->diskusage = 0;
		}
		close(c->fd);
		hdd_open_files_handle(OF_AFTER_CLOSE);
		c->fd = -1;
	}
	if (c->crc!=NULL) {
		myunalloc((void*)(c->crc),CHUNKCRCSIZE);
		c->crc = NULL;
	}
#ifdef PRESERVE_BLOCK
	if (c->block!=NULL) {
		myunalloc((void*)(c->block),MFSBLOCKSIZE);
		c->block = NULL;
	}
#endif /* PRESERVE_BLOCK */
}

static inline int hdd_chunk_hash_remove(chunk *c) {
	chunk **cptr,*cp;
	uint32_t hashpos = HASHPOS(c->chunkid);
	cptr = &(hashtab[hashpos]);
	while ((cp=*cptr)) {
		if (c==cp) {
			*cptr = cp->next;
			return 1;
		}
		cptr = &(cp->next);
	}
	return 0;
}

static int hdd_timed_wait(pthread_cond_t *cond, pthread_mutex_t *mutex, uint32_t usecs) {
	struct timeval tv;
	struct timespec ts;
	gettimeofday(&tv, NULL);
	tv.tv_usec += (usecs%1000000);
	while (tv.tv_usec > 1000000) {
		tv.tv_sec++;
		tv.tv_usec -= 1000000;
	}
	tv.tv_sec += usecs/1000000;
	ts.tv_nsec = tv.tv_usec * 1000;
	ts.tv_sec = tv.tv_sec;
	return pthread_cond_timedwait(cond,mutex,&ts);
}

static void hdd_chunk_release(chunk *c) {
	zassert(pthread_mutex_lock(&hashlock));
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hdd_chunk_release got chunk: %016"PRIX64" (c->state:%u)",c->chunkid,c->state);
	if (c->state==CH_LOCKED) {
		c->state = CH_AVAIL;
		if (c->ccond) {
//			printf("wake up one thread waiting for AVAIL chunk: %016"PRIX64" on ccond:%p\n",c->chunkid,c->ccond);
//			printbacktrace();
			zassert(pthread_cond_signal(&(c->ccond->cond)));
		}
	}
	zassert(pthread_mutex_unlock(&hashlock));
}

static int hdd_chunk_getattr(chunk *c,uint8_t forceflag) {
	struct stat sb;
	int err;

	err = 0;
	if (c->fd>=0) {
		if (fstat(c->fd,&sb)<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"hdd_chunk_getattr: chunk %016"PRIX64" fstat error",c->chunkid);
			if (forceflag==0) {
				return -1;
			} else {
				// set hdrsize and blocks to anything - it doesn't matter here
				c->hdrsize = NEWHDRSIZE;
				c->blocks = 0;
				c->diskusage = 0;
				return 0;
			}
		}
	} else {
		char fname[PATH_MAX];
		hdd_generate_filename(fname,c);
		if (stat(fname,&sb)<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"hdd_chunk_getattr: chunk %s stat error",fname);
			if (forceflag==0) {
				return -1;
			} else {
				// set hdrsize and blocks to anything - it doesn't matter here
				c->hdrsize = NEWHDRSIZE;
				c->blocks = 0;
				c->diskusage = 0;
				return 0;
			}
		}
	}
	if ((sb.st_mode & S_IFMT) != S_IFREG) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_chunk_getattr: chunk %016"PRIX64" wrong file mode",c->chunkid);
		return -1; // this error can't be ignored
	}
	if (sb.st_size==0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_chunk_getattr: chunk %016"PRIX64" is empty (size=0)",c->chunkid);
		if (forceflag==0) {
			return -1;
		} else {
			// set hdrsize and blocks to anything - it doesn't matter here
			c->hdrsize = NEWHDRSIZE;
			c->blocks = 0;
			c->diskusage = 0;
			return 0;
		}
	}
	c->hdrsize = (sb.st_size - CHUNKCRCSIZE) & MFSBLOCKMASK;
	if (c->hdrsize!=OLDHDRSIZE && c->hdrsize!=NEWHDRSIZE) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_chunk_getattr: chunk %016"PRIX64" wrong hdr size (file size: %llu)",c->chunkid,(unsigned long long)(sb.st_size));
		if (forceflag==0) {
			return -1;
		} else {
			err = 1;
			c->hdrsize=NEWHDRSIZE;
		}
	}
	if (sb.st_size<(c->hdrsize+CHUNKCRCSIZE) || sb.st_size>((uint32_t)(c->hdrsize)+CHUNKCRCSIZE+MFSCHUNKSIZE)) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_chunk_getattr: chunk %016"PRIX64" wrong file size (%llu)",c->chunkid,(unsigned long long)(sb.st_size));
		if (forceflag==0) {
			return -1;
		} else {
			err = 1;
			if (sb.st_size > ((uint32_t)(c->hdrsize)+CHUNKCRCSIZE+MFSCHUNKSIZE)) {
				c->blocks = MFSBLOCKSINCHUNK;
			} else {
				c->blocks = 0;
			}
		}
	} else {
		if (err) { // we assumed new header - calculate maximal possible number of blocks
			c->blocks = (sb.st_size - CHUNKCRCSIZE - c->hdrsize + MFSBLOCKSIZE - 1) / MFSBLOCKSIZE;
		} else {
			c->blocks = (sb.st_size - CHUNKCRCSIZE - c->hdrsize) / MFSBLOCKSIZE;
		}
	}
	c->diskusage = sb.st_blocks * 512U;
	if (err==0) { // we had errors - return ok if this has been forced, but do not set validattr
		c->validattr = 1;
	}
	return 0;
}

static chunk* hdd_chunk_tryfind(uint64_t chunkid) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
	zassert(pthread_mutex_lock(&hashlock));
	for (c=hashtab[hashpos] ; c && c->chunkid!=chunkid ; c=c->next) {}
	if (c!=NULL) {
		if (c->state==CH_LOCKED) {
			c = CHUNKLOCKED;
		} else if (c->state!=CH_AVAIL) {
			c = NULL;
		} else {
			c->state = CH_LOCKED;
		}
	}
//	if (c!=NULL && c!=CHUNKLOCKED) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hdd_chunk_tryfind returns chunk: %016"PRIX64" (c->state:%u)",c->chunkid,c->state);
//	}
	zassert(pthread_mutex_unlock(&hashlock));
	return c;
}

static void hdd_chunk_delete(chunk *c);

static int hdd_chunk_get(uint64_t chunkid,chunk **cptr,uint8_t cflag) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
	cntcond *cc;
	int res;

	*cptr = NULL;
	zassert(pthread_mutex_lock(&hashlock));
	for (c=hashtab[hashpos] ; c && c->chunkid!=chunkid ; c=c->next) {}
	if (c==NULL) {
		if (cflag!=CHMODE_EXISTING_ONLY && cflag!=CHMODE_EXISTING_ONLY_WITH_ERRORS) { // create if not exists
			c = malloc(sizeof(chunk));
			passert(c);
			c->chunkid = chunkid;
			c->version = 0;
			c->owner = NULL;
			c->pathid = 0xFFFF;
			c->blocks = 0;
			c->diskusage = 0;
			c->hdrsize = 0;
			c->crcrefcount = 0;
			c->opento = 0.0;
			c->crcto = 0.0;
			c->crcchanged = 0;
			c->fsyncneeded = 0;
			c->damaged = 0;
			c->fd = -1;
			c->crc = NULL;
			c->state = CH_LOCKED;
			c->ccond = NULL;
#ifdef PRESERVE_BLOCK
			c->blockto = 0.0;
			c->block = NULL;
			c->blockno = 0xFFFF;
#endif
			c->validattr = 0;
			c->fileversion = 0;
			c->testnext = NULL;
			c->testprev = NULL;
			c->next = hashtab[hashpos];
			hashtab[hashpos] = c;
		} else {
			hdd_report_nonexistent_chunk(chunkid);
		}
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hdd_chunk_get returns chunk: %016"PRIX64" (c->state:%u)",c->chunkid,c->state);
		zassert(pthread_mutex_unlock(&hashlock));
		*cptr = c;
		return 1;
	}
	if (cflag==CHMODE_NEW_ONLY) {
		if (c->state==CH_AVAIL || c->state==CH_LOCKED) {
			zassert(pthread_mutex_unlock(&hashlock));
			return 0;
		}
	}
	for (;;) {
		switch (c->state) {
		case CH_AVAIL:
			c->state = CH_LOCKED;
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hdd_chunk_get returns chunk: %016"PRIX64" (c->state:%u)",c->chunkid,c->state);
			zassert(pthread_mutex_unlock(&hashlock));
			if (c->validattr==0 && cflag!=CHMODE_NEW_OR_EXISTING) {
				if (hdd_chunk_getattr(c,(cflag==CHMODE_EXISTING_ONLY_WITH_ERRORS)?1:0)<0) {
					hdd_error_occured(c,1);
					hdd_chunk_release(c);
					return 0;
				}
			}
			*cptr = c;
			return 1;
		case CH_DELETED:
			if (cflag!=CHMODE_EXISTING_ONLY && cflag!=CHMODE_EXISTING_ONLY_WITH_ERRORS) { // create if not exists
				c->version = 0;
				c->owner = NULL;
				c->pathid = 0xFFFF;
				c->blocks = 0;
				c->diskusage = 0;
				c->hdrsize = 0;
				c->crcrefcount = 0;
				c->opento = 0.0;
				c->crcto = 0.0;
				c->fsyncneeded = 0;
				c->damaged = 0;
#ifdef PRESERVE_BLOCK
				c->blockto = 0.0;
				c->blockno = 0xFFFF;
#endif /* PRESERVE_BLOCK */
				c->validattr = 0;
				c->fileversion = 0;
				c->state = CH_LOCKED;
//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"hdd_chunk_get returns chunk: %016"PRIX64" (c->state:%u)",c->chunkid,c->state);
				zassert(pthread_mutex_unlock(&hashlock));
				hdd_chunk_flush(c);
				*cptr = c;
				return 1;
			}
			if (c->ccond==NULL) {	// no more waiting threads - remove
				res = hdd_chunk_hash_remove(c);
				zassert(pthread_mutex_unlock(&hashlock));
				if (res) { // always true
					hdd_chunk_flush(c);
					free(c);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_chunk_get: serious internal data structure inconsistency - can't remove chunk %016"PRIX64,chunkid);
				}
			} else {	// there are waiting threads - wake them up
//				printf("wake up one thread waiting for DELETED chunk: %016"PRIX64" on ccond:%p\n",c->chunkid,c->ccond);
//				printbacktrace();
				zassert(pthread_cond_signal(&(c->ccond->cond)));
				zassert(pthread_mutex_unlock(&hashlock));
			}
			return 0;
		case CH_LOCKED:
			if (c->ccond==NULL) {
				for (cc=cclist ; cc && cc->wcnt ; cc=cc->next) {}
				if (cc==NULL) {
					cc = malloc(sizeof(cntcond));
					passert(cc);
					zassert(pthread_cond_init(&(cc->cond),NULL));
					cc->wcnt = 0;
					cc->next = cclist;
					cclist = cc;
				}
				c->ccond = cc;
			}
			c->ccond->wcnt++;
//			printf("wait for %s chunk: %016"PRIX64" on ccond:%p\n",(c->state==CH_LOCKED)?"LOCKED":"TOBEDELETED",c->chunkid,c->ccond);
//			printbacktrace();
			if (hdd_timed_wait(&(c->ccond->cond),&hashlock,LOCKED_CHUNK_WAIT_USECS)!=0) { // do not wait for chunk too long
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"hdd_chunk_get: chunk %016"PRIX64" locked too long - giving up",chunkid);
				c->ccond->wcnt--;
				if (c->ccond->wcnt==0) {
					c->ccond = NULL;
				}
				zassert(pthread_mutex_unlock(&hashlock));
				return 2;
			}
//			zassert(pthread_cond_wait(&(c->ccond->cond),&hashlock));
//			printf("%s chunk: %016"PRIX64" woke up on ccond:%p\n",(c->state==CH_LOCKED)?"LOCKED":(c->state==CH_DELETED)?"DELETED":(c->state==CH_AVAIL)?"AVAIL":"TOBEDELETED",c->chunkid,c->ccond);
			c->ccond->wcnt--;
			if (c->ccond->wcnt==0) {
				c->ccond = NULL;
			}
		}
	}
}

static void hdd_chunk_delete(chunk *c) {
	folder *f;
	int res;

	zassert(pthread_mutex_lock(&folderlock));
	f = c->owner;
	hdd_remove_chunk_from_folder(c,f);
	zassert(pthread_mutex_unlock(&folderlock));
	zassert(pthread_mutex_lock(&testlock));
	hdd_remove_chunk_from_test_chain(c,f);
	zassert(pthread_mutex_unlock(&testlock));
	zassert(pthread_mutex_lock(&hashlock));
	if (c->ccond) {
		c->state = CH_DELETED;
//		printf("wake up one thread waiting for DELETED chunk: %016"PRIX64" ccond:%p\n",c->chunkid,c->ccond);
//		printbacktrace();
		zassert(pthread_cond_signal(&(c->ccond->cond)));
		zassert(pthread_mutex_unlock(&hashlock));
	} else {
		res = hdd_chunk_hash_remove(c);
		zassert(pthread_mutex_unlock(&hashlock));
		if (res) { // always true
			hdd_chunk_flush(c);
			free(c);
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_chunk_delete: serious internal data structure inconsistency - can't remove chunk %016"PRIX64,c->chunkid);
		}
	}
}

static chunk* hdd_chunk_create(folder *f,uint64_t chunkid,uint32_t version) {
	chunk *c;

	if (hdd_chunk_get(chunkid,&c,CHMODE_NEW_ONLY)==2) {
		return NULL;
	}
	if (c==NULL) {
		return NULL;
	}
	c->version = version;
	c->blocks = 0;
	c->diskusage = 0;
	c->hdrsize = NEWHDRSIZE;
	c->validattr = 1;
	f->needrefresh = 1;
	hdd_add_chunk_to_folder(c,f);
	zassert(pthread_mutex_lock(&testlock));
	c->pathid = f->current_pathid;
	hdd_add_chunk_to_test_chain(c,f,1);
	zassert(pthread_mutex_unlock(&testlock));
	return c;
}

#define hdd_chunk_find(chunkid,chunkptr) hdd_chunk_get(chunkid,chunkptr,CHMODE_EXISTING_ONLY)

#define hdd_chunk_force_find(chunkid,chunkptr) hdd_chunk_get(chunkid,chunkptr,CHMODE_EXISTING_ONLY_WITH_ERRORS)

static inline void hdd_int_chunk_iomove(chunk *c) {
	folder *f;
	f = c->owner;
	if (c->testnext) {
		*(c->testprev) = c->testnext;
		c->testnext->testprev = c->testprev;
		c->testnext = NULL;
		if (c->testedflag) {
			c->testprev = f->testedtail;
			f->testedtail = &(c->testnext);
		} else {
			c->testprev = f->testneededtail;
			f->testneededtail = &(c->testnext);
		}
		*(c->testprev) = c;
	}
	c->testtime = main_time();
}

static void hdd_chunk_iomove(chunk *c) {
	zassert(pthread_mutex_lock(&testlock));
	hdd_int_chunk_iomove(c);
	zassert(pthread_mutex_unlock(&testlock));
}

static void hdd_int_chunk_testmove(chunk *c) {
	massert(c->testedflag==0,"tested flag is set before chunk testing");
	folder *f;
	f = c->owner;
	*(c->testprev) = c->testnext;
	if (c->testnext) {
		c->testnext->testprev = c->testprev;
	}
	c->testnext = NULL;
	c->testedflag = 1;
	f->testedcnt++;
	f->testneededcnt--;
	c->testprev = f->testedtail;
	f->testedtail = &(c->testnext);
	*(c->testprev) = c;
	c->testtime = main_time();
}

static void hdd_int_testloop(folder *f) {
	uint32_t chunkcnt;
	chunk *c;

	massert(f->testneededcnt==0,"test loop has finished with wrong waiting chunk count");
	chunkcnt = 0;
	for (c=f->testedhead ; c!=NULL ; c=c->testnext) {
		c->testedflag = 0;
		chunkcnt++;
	}
	massert(f->testedcnt==chunkcnt,"test loop has finished with wrong tested chunk count");
	f->testneededcnt = chunkcnt;
	f->testedcnt = 0;
	f->testneededhead = f->testedhead;
	f->testneededtail = f->testedtail;
	if (f->testneededhead!=NULL) {
		f->testneededhead->testprev = &(f->testneededhead);
	} else {
		f->testneededtail = &(f->testneededhead);
	}
	f->testedhead = NULL;
	f->testedtail = &(f->testedhead);
	f->endlooptime = main_time();
	f->startlooptime = f->endlooptime;
	f->startlooptestneededcnt = chunkcnt;
}

/* for debug only
static void hdd_testloopcheck(void) {
	folder *f;
	chunk *c;
	uint32_t cnt;
	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"test loop check");
	zassert(pthread_mutex_lock(&folderlock));
	zassert(pthread_mutex_lock(&hashlock));
	zassert(pthread_mutex_lock(&testlock));
	for (f=folderhead ; f!=NULL ; f=f->next) {
		cnt = 0;
		for (c=f->testedhead ; c!=NULL ; c=c->testnext) { 
			if (c->testedflag==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"folder: %s ; chunks: %016"PRIX64" - wrong set tested flag detected\n",f->path,c->chunkid);
			}
			cnt++;
		}
		if (cnt!=f->testedcnt) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"wrong tested count: %u != %u\n",cnt,f->testedcnt);
		}
		cnt = 0;
		for (c=f->testneededhead ; c!=NULL ; c=c->testnext) { 
			if (c->testedflag!=0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"folder: %s ; chunks: %016"PRIX64" - wrong cleared tested flag detected\n",f->path,c->chunkid);
			}
			cnt++;
		}
		if (cnt!=f->testneededcnt) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"wrong waiting count: %u != %u\n",cnt,f->testneededcnt);
		}
	}
	zassert(pthread_mutex_unlock(&testlock));
	zassert(pthread_mutex_unlock(&hashlock));
	zassert(pthread_mutex_unlock(&folderlock));
}
*/

// no locks - locked by caller
static inline void hdd_refresh_usage(folder *f) {
	struct statvfs fsinfo;
	uint64_t calcsize,tmpsize;
	uint64_t hdd_total;
	uint64_t hdd_avail;
	uint64_t ldata_const;
	double knownratio;
	uint8_t isro;

	if (statvfs(f->path,&fsinfo)<0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"disk: %s ; statvfs error (%s) - mark it as damaged",f->path,strerr(errno));
		f->damaged = 1;
		f->toremove = REMOVING_START;
		return;
	}

	isro = (fsinfo.f_flag & ST_RDONLY)?1:0;
	if (f->lastblocks==0) {
		f->lastblocks = fsinfo.f_blocks;
		f->isro = isro;
	} else if (f->ignoresize==0 && (f->lastblocks * 9 > fsinfo.f_blocks * 10 || f->lastblocks * 10 < fsinfo.f_blocks * 9)) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"disk: %s ; number of total blocks has been changed significantly (%llu -> %llu) - mark it as damaged",f->path,(unsigned long long int)(f->lastblocks),(unsigned long long int)(fsinfo.f_blocks));
		f->damaged = 1;
		f->toremove = REMOVING_START;
		return;
	} else if (f->isro != isro) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"disk: %s ; unit read-only flag has been changed (%s->%s) - mark it as damaged",f->path,(f->isro)?"RO":"RW",isro?"RO":"RW");
		f->damaged = 1;
		f->toremove = REMOVING_START;
		return;
	} else if (f->lastblocks != fsinfo.f_blocks) {
		f->lastblocks = fsinfo.f_blocks;
	}

	if (f->knowncount>0) { // use data from full loop
		knownratio = f->chunkcount;
		knownratio /= f->knowncount;
		calcsize = f->knowndiskusage * knownratio;
	} else if (f->knowncount_next>0) { // if not then use current data
		knownratio = f->chunkcount;
		knownratio /= f->knowncount_next;
		calcsize = f->knowndiskusage_next * knownratio;
	} else if (f->chunkcount==0) {
		calcsize = 0;
	} else { // not available?
		// use data from 'statvfs'
		calcsize = fsinfo.f_blocks;
		calcsize -= fsinfo.f_bfree;
		calcsize *= fsinfo.f_frsize;
		tmpsize = f->chunkcount;
		tmpsize *= MFSCHUNKSIZE;
		if (tmpsize < calcsize) { // calcsize is larger than number of chunks * max chunk size ?
			calcsize = tmpsize;
		}
	}

	hdd_avail = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_bavail);
	hdd_total = (uint64_t)(fsinfo.f_frsize)*(uint64_t)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));

	ldata_const = f->ldata;

	switch (f->lmode) {
		case LMODE_LIMIT_TOTAL_POS_PERCENT:
			ldata_const = hdd_total * f->ldata;
			nobreak;
		case LMODE_LIMIT_TOTAL_POS_CONST:
			if (calcsize > ldata_const) {
				f->avail = 0;
			} else {
				f->avail = ldata_const - calcsize;
			}
			break;
		case LMODE_LIMIT_TOTAL_NEG_PERCENT:
			ldata_const = hdd_total * f->ldata;
			nobreak;
		case LMODE_LIMIT_TOTAL_NEG_CONST:
			if (hdd_total < ldata_const || calcsize > hdd_total - ldata_const) {
				f->avail = 0;
			} else {
				f->avail = (hdd_total - ldata_const) - calcsize;
			}
			break;
		case LMODE_SHARED:
			if (hdd_avail > LeaveFree) {
				f->avail = hdd_avail - LeaveFree;
			} else {
				f->avail = 0;
			}
			break;
		case LMODE_SHARED_POS_PERCENT:
			ldata_const = hdd_avail * f->ldata;
			nobreak;
		case LMODE_SHARED_POS_CONST:
			if (ldata_const > hdd_avail) {
				f->avail = hdd_avail;
			} else {
				f->avail = ldata_const;
			}
			break;
		case LMODE_SHARED_NEG_PERCENT:
			ldata_const = hdd_avail * f->ldata;
			nobreak;
		case LMODE_SHARED_NEG_CONST:
			if (ldata_const > hdd_avail) {
				f->avail = 0;
			} else {
				f->avail = (hdd_avail - ldata_const);
			}
			break;
		default: // LMODE_NONE
			if (hdd_avail > LeaveFree) {
				f->avail = hdd_avail - LeaveFree;
			} else {
				f->avail = 0;
			}
			f->total = hdd_total;
	}

	if (f->lmode!=LMODE_NONE) {
		f->total = f->avail + calcsize;
	}

	// inode limits - reduce available space due to inode limits
	if (fsinfo.f_files>0) { // BTRFS always returns zeros in all inode related fields - in such case we assume there is no inode limit
		uint64_t maxavail;
		if (f->knowncount>0) {
			maxavail = f->knowndiskusage;
			maxavail *= 65536;
			maxavail /= f->knowncount;
			maxavail += CHUNKMAXHDRSIZE;
		} else {
			maxavail = 65536+CHUNKMAXHDRSIZE;
		}
		// maxavail = average chunk size
		maxavail *= fsinfo.f_favail;
		if (f->avail > maxavail) {
			double now = monotonic_seconds();
			maxavail = f->avail - maxavail;
			if (f->iredlastrep+INODE_REDUCE_LOG_FREQ<now) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"disk: %s ; decreasing size reported to the master (%.2lfGiB->%.2lfGiB) due to inode limits",f->path,(f->total)/(1024.0*1024.0*1024.0),(f->total-maxavail)/(1024.0*1024.0*1024.0));
				f->iredlastrep = now;
			}
			f->avail -= maxavail;
			f->total -= maxavail;
		}
	}
}

static inline folder* hdd_getfolder(void) {
	folder *f,*bf;
	double minerr,err,expdist;
//	double usage;
	uint64_t totalsum,good_totalsum;
	uint32_t folder_cnt,good_cnt,notfull_cnt;
	uint8_t onlygood;
	uint64_t usectime;

	usectime = monotonic_useconds();

	totalsum = 0;
	good_totalsum = 0;
	folder_cnt = 0;
	good_cnt = 0;
	notfull_cnt = 0;
	onlygood = 0;

	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged==0 && f->toremove==REMOVING_NO && f->markforremoval==MFR_NO && f->scanstate==SCST_WORKING && f->total>0 && f->avail>0 && f->balancemode!=REBALANCE_FORCE_SRC) {
			if (f->avail * UINT64_C(1000) >= f->total) { // space used <= 99.9%
				notfull_cnt++;
			}
		}
	}

	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged==0 && f->toremove==REMOVING_NO && f->markforremoval==MFR_NO && f->scanstate==SCST_WORKING && f->total>0 && f->avail>0 && f->balancemode!=REBALANCE_FORCE_SRC) {
			if (notfull_cnt==0 || f->avail * UINT64_C(1000) >= f->total) { // space used <= 99.9%
				if (f->rebalance_last_usec + REBALANCE_GRACE_PERIOD < usectime) {
					good_cnt++;
					good_totalsum += f->total;
				}
				totalsum += f->total;
				folder_cnt++;
			}
		}
	}
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"good_cnt: %"PRIu32" ; folder_cnt: %"PRIu32" ; good_totalsum:%"PRIu64" ; totalsum:%"PRIu64,good_cnt,folder_cnt,good_totalsum,totalsum);
	if (good_cnt * 3 >= folder_cnt * 2) {
		onlygood = 1;
		totalsum = good_totalsum;
	}
	bf = NULL;
	minerr = 0.0; // make some old compilers happy
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged==0 && f->toremove==REMOVING_NO && f->markforremoval==MFR_NO && f->scanstate==SCST_WORKING && f->total>0 && f->avail>0 && f->balancemode!=REBALANCE_FORCE_SRC) {
			if (notfull_cnt==0 || f->avail * UINT64_C(1000) >= f->total) { // space used <= 99.9%
				if (onlygood==0 || (f->rebalance_last_usec + REBALANCE_GRACE_PERIOD < usectime)) {
					f->write_dist++;
					if (f->write_first) {
						err = 1.0;
					} else {
						expdist = totalsum;
						expdist /= f->total;
						err = (expdist + f->write_corr) / f->write_dist;
					}
					if (bf==NULL || err<minerr) {
						minerr = err;
						bf = f;
					}
				}
			}
		}
	}
	if (bf) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chosen: %s",bf->path);
		if (bf->write_first) {
			bf->write_first = 0;
		} else {
			expdist = totalsum;
			expdist /= bf->total;
			bf->write_corr += expdist - bf->write_dist;
		}
		bf->write_dist = 0;
	}
	return bf;
}
/*
static inline folder* hdd_getfolder(void) {
	folder *f,*bf;
	double maxcarry;
	double minavail,maxavail;
	double s,d;
	double pavail;
	int ok;
//	uint64_t minavail;

	minavail = 0.0;
	maxavail = 0.0;
	maxcarry = 1.0;
	bf = NULL;
	ok = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged || f->markforremoval!=MFR_NO || f->total==0 || f->avail==0 || f->scanstate!=SCST_WORKING) {
			continue;
		}
		if (f->carry >= maxcarry) {
			maxcarry = f->carry;
			bf = f;
		}
		pavail = (double)(f->avail)/(double)(f->total);
		if (ok==0 || minavail>pavail) {
			minavail = pavail;
			ok = 1;
		}
		if (pavail>maxavail) {
			maxavail = pavail;
		}
	}
	if (bf) {
		bf->carry -= 1.0;
		return bf;
	}
	if (maxavail==0.0) {	// no space
		return NULL;
	}
	if (maxavail<0.01) {
		s = 0.0;
	} else {
		s = minavail*0.8;
		if (s<0.01) {
			s = 0.01;
		}
	}
	d = maxavail-s;
	maxcarry = 1.0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged || f->markforremoval!=MFR_NO || f->total==0 || f->avail==0 || f->scanstate!=SCST_WORKING) {
			continue;
		}
		pavail = (double)(f->avail)/(double)(f->total);
		if (pavail>s) {
			f->carry += ((pavail-s)/d);
		}
		if (f->carry >= maxcarry) {
			maxcarry = f->carry;
			bf = f;
		}
	}
	if (bf) {	// should be always true
		bf->carry -= 1.0;
	}
	return bf;
}
*/

static inline void hdd_wfr_chains_remove(waitforremoval *wfr) {
	if (wfr->hashnext!=NULL) {
		wfr->hashnext->hashprev = wfr->hashprev;
	}
	*(wfr->hashprev) = wfr->hashnext;
	if (wfr->foldernext!=NULL) {
		wfr->foldernext->folderprev = wfr->folderprev;
	}
	*(wfr->folderprev) = wfr->foldernext;
	wfr->owner->wfrcount--;
}

static inline void hdd_wfr_add(folder *f,uint64_t chunkid,uint32_t version,uint16_t pathid) {
	waitforremoval *wfr;
	uint32_t hash;

	hash = WFRHASHPOS(chunkid);
	wfr = malloc(sizeof(waitforremoval));
	wfr->owner = f;
	wfr->chunkid = chunkid;
	wfr->version = version;
	wfr->pathid = pathid;
	zassert(pthread_mutex_lock(&folderlock));
	wfr->hashnext = wfrhash[hash];
	if (wfr->hashnext!=NULL) {
		wfr->hashnext->hashprev = &(wfr->hashnext);
	}
	wfr->hashprev = wfrhash+hash;
	wfrhash[hash] = wfr;
	wfr->foldernext = f->wfrchunks;
	if (wfr->foldernext!=NULL) {
		wfr->foldernext->folderprev = &(wfr->foldernext);
	}
	wfr->folderprev = &(f->wfrchunks);
	f->wfrchunks = wfr;
	f->wfrcount++;
	zassert(pthread_mutex_unlock(&folderlock));
}

static inline int hdd_wfr_remove(uint64_t chunkid,char fname[PATH_MAX]) {
	waitforremoval *wfr;
	uint32_t hash;

	hash = WFRHASHPOS(chunkid);
	zassert(pthread_mutex_lock(&folderlock));
	for (wfr = wfrhash[hash] ; wfr!=NULL ; wfr=wfr->hashnext) {
		if (wfr->chunkid==chunkid) {
			hdd_create_filename(fname,wfr->owner->path,wfr->pathid,wfr->chunkid,wfr->version);
			hdd_wfr_chains_remove(wfr);
			zassert(pthread_mutex_unlock(&folderlock));
			free(wfr);
			return 1;
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
	return 0;
}

static inline void hdd_wfr_delete_all_duplicates(uint64_t chunkid) {
	char fname[PATH_MAX];

	while (hdd_wfr_remove(chunkid,fname)) {
		if (unlink(fname)<0) {
			if (errno!=ENOENT) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"delete_duplicate_chunk: file:%s - unlink error",fname);
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"delete_duplicate_chunk: file:%s - chunk already deleted !!!",fname);
			}
		}
	}
}

static inline void hdd_wfr_check(folder *f) {
	static char fpath[PATH_MAX-100];
	char fname[PATH_MAX];
	waitforremoval *wfr;
	uint32_t now;
	uint32_t i;

	if (f==NULL) { // folderlock not locked
		if (wfrcurrentleng>0) {
			for (i=0 ; i<wfrcurrentleng ; i++) {
				wfr = wfrcurrent[i];
				hdd_create_filename(fname,fpath,wfr->pathid,wfr->chunkid,wfr->version);
				unlink(fname);
				free(wfr);
				wfrcurrent[i] = NULL;
			}
			wfrcurrentleng = 0;
		}
	} else { // folderlock is locked
		if (f->wfrchunks!=NULL && wfrcurrentleng==0) {
			now = monotonic_seconds();
			if (f->wfrtime+HDDKeepDuplicatesHours*3600.0<now) {
				i = strlen(f->path);
				if (i>=PATH_MAX-100) { // unlikely
					fpath[0]=0;
					if (f->wfrlast+300.0<now) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"path too long (%s) - can't remove duplicates",f->path);
						f->wfrlast = now;
					}
				} else {
					memcpy(fpath,f->path,i+1);
					while (f->wfrchunks!=NULL && wfrcurrentleng<WFRCURRENTBLOCK) {
						wfr = f->wfrchunks;
						hdd_wfr_chains_remove(wfr);
						wfrcurrent[wfrcurrentleng++] = wfr;
					}
					if (f->wfrchunks==NULL) {
						massert(f->wfrcount==0,"wait for removal count mismatch after unlink");
						mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"all duplicates have beed removed from drive '%s'",f->path);
					} else {
						if (f->wfrlast+300.0<now) {
							mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"on drive '%s' chunk duplicates are being removed - %"PRIu32" left",f->path,f->wfrcount);
							f->wfrlast = now;
						}
					}
				}
			} else {
				// report
				if (f->wfrlast+3600.0<now) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"on drive '%s' %"PRIu32" chunk duplicates detected - will remove them in about %u hours - no rebalance will occur on this drive until duplicate removal",f->path,f->wfrcount,((uint32_t)(f->wfrtime)+HDDKeepDuplicatesHours*3600+3599U-(uint32_t)(now))/3600U);
					f->wfrlast = now;
				}
			}
		}
	}
}

static inline void hdd_folder_dump_chunkdb_begin(folder *f) {
	uint32_t pleng;
	char *fname;
	uint8_t hdr[14];
	uint8_t *wptr;

	if (f->damaged || f->markforremoval==MFR_READONLY || f->wfrcount>0) { // do not store '.chunkdb'
		if (f->damaged) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"disk %s is marked as 'damaged' - '.chunkdb' not written",f->path);
		} else if (f->markforremoval==MFR_READONLY) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"disk %s is marked as 'read-only' - can't write '.chunkdb'",f->path);
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"disk %s has pending duplicates - can't use '.chunkdb' to avoid full scan",f->path);
		}
		f->dumpfd = -1;
		return;
	}
	pleng = strlen(f->path);
	fname = malloc(pleng+13);
	passert(fname);
	memcpy(fname,f->path,pleng);
	memcpy(fname+pleng,".tmp_chunkdb",12);
	fname[pleng+12] = 0;
	f->dumpfd = open(fname,O_WRONLY | O_TRUNC | O_CREAT,0666);
	if (f->dumpfd<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"%s: open error",fname);
	}
	if (f->dumpfd>=0) {
		memcpy(hdr,"MFS CHUNKDB4",12);
		wptr = hdr+12;
		put16bit(&wptr,pleng);
		if (write(f->dumpfd,hdr,14)!=14) {
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"%s: write error",fname);
			close(f->dumpfd);
			f->dumpfd = -1;
			free(fname);
			return;
		}
		if (write(f->dumpfd,f->path,pleng)!=(int32_t)pleng) {
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"%s: write error",fname);
			close(f->dumpfd);
			f->dumpfd = -1;
		}
	}
	free(fname);
}

static inline void hdd_folder_dump_chunkdb_end(folder *f) {
	if (f->dumpfd>=0) {
		uint32_t pleng;
		char *fname_src,*fname_dst;
		uint8_t buff[CHUNKDB_REC_SIZE];

		pleng = strlen(f->path);
		fname_src = malloc(pleng+13);
		fname_dst = malloc(pleng+9);
		passert(fname_src);
		passert(fname_dst);
		memcpy(fname_src,f->path,pleng);
		memcpy(fname_src+pleng,".tmp_chunkdb",12);
		fname_src[pleng+12] = 0;
		memcpy(fname_dst,f->path,pleng);
		memcpy(fname_dst+pleng,".chunkdb",8);
		fname_dst[pleng+8] = 0;

		memset(buff,0,CHUNKDB_REC_SIZE);

		if (write(f->dumpfd,buff,CHUNKDB_REC_SIZE)!=CHUNKDB_REC_SIZE) {
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"%s: write error",fname_src);
			free(fname_src);
			free(fname_dst);
			close(f->dumpfd);
			f->dumpfd = -1;
			return;
		}

		if (close(f->dumpfd)<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"%s: close error",fname_src);
			free(fname_src);
			free(fname_dst);
			f->dumpfd = -1;
			return;
		}

		f->dumpfd = -1;

		if (rename(fname_src,fname_dst)<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"%s->%s: rename error",fname_src,fname_dst);
			free(fname_src);
			free(fname_dst);
			return;
		}
		free(fname_src);
		free(fname_dst);
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"disk %s: '.chunkdb' has been written",f->path);
	}
}

static inline void hdd_folder_dump_chunkdb_chunk(folder *f,chunk *c) {
	if (f->dumpfd>=0) {
		uint8_t buff[CHUNKDB_REC_SIZE];
		uint8_t *wptr;
		wptr = buff;
		put64bit(&wptr,c->chunkid);
		put32bit(&wptr,c->version);
		if (c->validattr) {
			put16bit(&wptr,c->blocks);
			put16bit(&wptr,c->hdrsize);
		} else {
			put16bit(&wptr,0xFFFF);
			put16bit(&wptr,0);
		}
		put16bit(&wptr,c->pathid);
		put8bit(&wptr,c->testedflag);
		put32bit(&wptr,c->diskusage);
		if (write(f->dumpfd,buff,CHUNKDB_REC_SIZE)!=CHUNKDB_REC_SIZE) {
			uint32_t pleng;
			char *fname;
			pleng = strlen(f->path);
			fname = malloc(pleng+13);
			passert(fname);
			memcpy(fname,f->path,pleng);
			memcpy(fname+pleng,".tmp_chunkdb",12);
			fname[pleng+12] = 0;
			mfs_log(MFSLOG_ERRNO_SYSLOG,MFSLOG_WARNING,"%s: write error",fname);
			free(fname);
			close(f->dumpfd);
			f->dumpfd = -1;
			return;
		}
	}
}

uint8_t hdd_senddata(folder *f,int rmflag) {
	uint32_t i;
	uint8_t markforremoval;
	uint8_t canberemoved;
	chunk **cptr,*c;

	markforremoval = f->markforremoval!=MFR_NO;
	canberemoved = 1;
	zassert(pthread_mutex_lock(&hashlock));
	zassert(pthread_mutex_lock(&testlock));
	for (i=0 ; i<HASHSIZE ; i++) {
		cptr = &(hashtab[i]);
		while ((c=*cptr)) {
			if (c->owner==f) {
				if (rmflag) {
					if (c->state==CH_AVAIL) {
						hdd_report_lost_chunk(c->chunkid);
						hdd_folder_dump_chunkdb_chunk(f,c);
						*cptr = c->next;
						if (c->fd>=0) {
							if (c->crcchanged) {
								mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_senddata: CRC not flushed - writing now");
								chunk_writecrc(c,1);
							}
							close(c->fd);
							hdd_open_files_handle(OF_AFTER_CLOSE);
						}
						if (c->crc!=NULL) {
							myunalloc((void*)(c->crc),CHUNKCRCSIZE);
						}
#ifdef PRESERVE_BLOCK
						if (c->block!=NULL) {
							myunalloc((void*)(c->block),MFSBLOCKSIZE);
						}
#endif /* PRESERVE_BLOCK */
						hdd_remove_chunk_from_test_chain(c,c->owner);
						free(c);
					} else {
						canberemoved = 0;
						cptr = &(c->next);
					}
				} else {
					hdd_report_new_chunk(c->chunkid,c->version|(markforremoval?0x80000000:0));
					cptr = &(c->next);
				}
			} else {
				cptr = &(c->next);
			}
		}
	}
	zassert(pthread_mutex_unlock(&testlock));
	zassert(pthread_mutex_unlock(&hashlock));
	return canberemoved;
}

void* hdd_folder_scan(void *arg);
void* hdd_folder_update_attr(void *arg);

// folderlock:locked

void hdd_calc_space(calc_space_data *csd) {
	folder *f;
	uint64_t avail,total;
	uint64_t mfravail,mfrtotal;
	uint32_t chunks,mfrchunks,ec4chunks,ec8chunks;
	uint32_t usagemin,usagemax,pmusage;
	double dusage;
	uint32_t hddok,hddmfr,hdddmg;
	avail = total = mfravail = mfrtotal = 0ULL;
	chunks = mfrchunks = ec4chunks = ec8chunks = 0;
	hddok = hddmfr = hdddmg = 0;
	usagemin = usagemax = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->damaged || f->toremove!=REMOVING_NO) {
			hdddmg++;
			continue;
		}
		if (f->markforremoval==MFR_NO) {
			hddok++;
			if (f->scanstate==SCST_WORKING || f->scanstate==SCST_ATTRNEEDED || f->scanstate==SCST_ATTRJOBINPROGRESS) {
				avail += f->avail;
				total += f->total;
				if (f->total>0) {
					dusage = f->total - f->avail;
					dusage /= f->total;
					if (dusage<0.0) {
						dusage = 0.0;
					}
					if (dusage>1.0) {
						dusage = 1.0;
					}
					pmusage = 100000 * dusage;
					if (usagemax==0) {
						usagemax = pmusage;
						usagemin = pmusage;
					} else {
						if (pmusage > usagemax) {
							usagemax = pmusage;
						}
						if (pmusage < usagemin) {
							usagemin = pmusage;
						}
					}
				}
			}
			chunks += f->chunkcount;
		} else {
			hddmfr++;
			if (f->scanstate==SCST_WORKING || f->scanstate==SCST_ATTRNEEDED || f->scanstate==SCST_ATTRJOBINPROGRESS) {
				mfravail += f->avail;
				mfrtotal += f->total;
			}
			mfrchunks += f->chunkcount;
		}
		ec4chunks += f->ec4chunkcount;
		ec8chunks += f->ec8chunkcount;
	}
	csd->usedspace = total-avail;
	csd->totalspace = total;
	csd->chunkcount = chunks;
	csd->mfrusedspace = mfrtotal-mfravail;
	csd->mfrtotalspace = mfrtotal;
	csd->mfrchunkcount = mfrchunks;
	csd->ec4chunkcount = ec4chunks;
	csd->ec8chunkcount = ec8chunks;
	csd->usagediff = usagemax - usagemin;
	csd->hddokcount = hddok;
	csd->hddmfrcount = hddmfr;
	csd->hdddmgcount = hdddmg;
}

void hdd_check_folders(void) {
	folder *f,**fptr;
	cfgline *cl;
	waitforremoval *wfr;
	uint32_t i;
	double monotonic_time;
	uint32_t err;
	uint8_t enoent;
	int changed;
	calc_space_data csd;

	monotonic_time = monotonic_seconds();

#ifdef HAVE___SYNC_FETCH_AND_OP
	changed = __sync_fetch_and_and(&hddspacerecalc,0);
#else
	zassert(pthread_mutex_lock(&dclock));
	changed = hddspacerecalc;
	hddspacerecalc = 0;
	zassert(pthread_mutex_unlock(&dclock));
#endif
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"check folders ...");

	zassert(pthread_mutex_lock(&folderlock));
	if (folderactions) {
		fptr = &folderhead;
		while ((f=*fptr)) {
			if (f->toremove!=REMOVING_NO && f->rebalance_in_progress==0) {
				switch (f->scanstate) {
				case SCST_SCANJOBINPROGRESS:
					f->scanstate = SCST_BGJOBTERMINATE;
					break;
				case SCST_ATTRJOBINPROGRESS:
					f->scanstate = SCST_BGJOBTERMINATE;
					break;
				case SCST_BGJOBFINISHED:
					zassert(pthread_join(f->scanthread,NULL));
					// no break - it's ok !!!
					nobreak;
				case SCST_SCANNEEDED:
				case SCST_ATTRNEEDED:
					f->scanstate = SCST_WORKING;
					// no break - it's ok !!!
					nobreak;
				case SCST_WORKING:
					if (f->toremove==REMOVING_START) {
						hdd_folder_dump_chunkdb_begin(f);
						f->toremove = REMOVING_INPROGRESS;
					}
					if (hdd_senddata(f,1)) {
						hdd_folder_dump_chunkdb_end(f);
						f->toremove = REMOVING_END;
					}
					changed = 1;
					break;
				}
				if (f->toremove==REMOVING_END) {
					if (f->damaged) {
						f->toremove = REMOVING_NO;
						f->chunkcount = 0;
						f->ec4chunkcount = 0;
						f->ec8chunkcount = 0;
						f->chunktabsize = 0;
						if (f->chunktab) {
							free(f->chunktab);
						}
						f->chunktab = NULL;
					} else {
						*fptr = f->next;
						if (f->lfd>=0) {
							close(f->lfd);
						}
						if (f->chunktab) {
							free(f->chunktab);
						}
						while (f->wfrchunks) {
							wfr = f->wfrchunks;
							hdd_wfr_chains_remove(wfr);
							free(wfr);
						}
						for (cl=cfglinehead ; cl!=NULL ; cl=cl->next) {
							if (cl->f==f) {
								cl->f = NULL;
							}
						}
						mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"folder %s successfully removed",f->path);
						free(f->path);
						free(f);
					}
				} else {
					fptr = &(f->next);
				}
			} else {
				fptr = &(f->next);
			}
		}
		for (f=folderhead ; f ; f=f->next) {
			if (f->damaged || f->toremove!=REMOVING_NO || (f->rebalance_in_progress>0 && f->scanstate!=SCST_WORKING)) {
				if (f->damaged && f->toremove==REMOVING_NO && f->scanstate==SCST_WORKING && f->lastrefresh+60.0<monotonic_time) {
					hdd_refresh_usage(f);
					f->lastrefresh = monotonic_time;
					changed = 1;
				}
				continue;
			}
			switch (f->scanstate) {
			case SCST_SCANNEEDED:
				f->scanstate = SCST_SCANJOBINPROGRESS;
				zassert(lwt_minthread_create(&(f->scanthread),0,hdd_folder_scan,f));
				break;
			case SCST_ATTRNEEDED:
				f->scanstate = SCST_ATTRJOBINPROGRESS;
				zassert(lwt_minthread_create(&(f->scanthread),0,hdd_folder_update_attr,f));
				break;
			case SCST_BGJOBFINISHED:
				zassert(pthread_join(f->scanthread,NULL));
				f->scanstate = SCST_WORKING;
				hdd_refresh_usage(f);
				f->needrefresh = 0;
				f->lastrefresh = monotonic_time;
				changed = 1;
				break;
			case SCST_WORKING:
				if (f->sendneeded) {
					hdd_senddata(f,0);
					f->sendneeded = 0;
					hdd_refresh_usage(f);
					f->needrefresh = 0;
					f->lastrefresh = monotonic_time;
					changed = 1;
				}
				err = 0;
				enoent = 0;
				for (i=0 ; i<LASTERRSIZE; i++) {
					if (f->lasterrtab[i].monotonic_time+HDDErrorTime>=monotonic_time && (f->lasterrtab[i].errornumber==EIO || f->lasterrtab[i].errornumber==EROFS || f->lasterrtab[i].errornumber==ENOENT)) {
						err++;
						if (f->lasterrtab[i].errornumber==ENOENT) {
							enoent = 1;
						}
					}
				}
				if (err>HDDErrorCount && f->markforremoval!=MFR_READONLY) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"%"PRIu32" errors occurred in %"PRIu32" seconds on folder: %s",err,HDDErrorTime,f->path);
					f->toremove = REMOVING_START;
					f->damaged = 1;
					changed = 1;
				} else if (enoent && err>HDDErrorCount && f->markforremoval==MFR_READONLY) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"%"PRIu32" errors occurred in %"PRIu32" seconds on folder: %s",err,HDDErrorTime,f->path);
					f->damaged = 1;
				} else if (f->needrefresh || f->lastrefresh+60.0<monotonic_time) {
					hdd_refresh_usage(f);
					f->needrefresh = 0;
					f->lastrefresh = monotonic_time;
					changed = 1;
				}
				if (f->damaged==0 && f->toremove==REMOVING_NO) {
					hdd_wfr_check(f);
				}
			}
		}
	}
	if (changed) {
		hdd_calc_space(&csd);
	}
	zassert(pthread_mutex_unlock(&folderlock));
	if (changed) {
#ifdef HAVE___SYNC_FETCH_AND_OP
		__sync_fetch_and_or(&hddspacechanged,1);
		zassert(pthread_mutex_lock(&dclock));
		global_csd = csd;
		zassert(pthread_mutex_unlock(&dclock));
#else
		zassert(pthread_mutex_lock(&dclock));
		hddspacechanged = 1;
		global_csd = csd;
		zassert(pthread_mutex_unlock(&dclock));
#endif
	}
	hdd_wfr_check(NULL);
}


/* interface */

static uint32_t hdd_get_chunks_pos = 0xFFFFFFFF;
static pthread_cond_t hdd_get_chunks_cond = PTHREAD_COND_INITIALIZER;
static uint8_t hdd_get_chunks_waiting = 0;
static uint8_t hdd_get_chunks_partialmode = 0;


void hdd_get_chunks_begin(uint8_t partialmode) {
	zassert(pthread_mutex_lock(&hashlock));
	hdd_get_chunks_pos = 0;
	while (hdd_get_chunks_partialmode) {
		hdd_get_chunks_waiting++;
		zassert(pthread_cond_wait(&hdd_get_chunks_cond,&hashlock));
	}
	hdd_get_chunks_partialmode = partialmode;
	if (partialmode) {
		zassert(pthread_mutex_unlock(&hashlock));
	}
}

void hdd_get_chunks_end(void) {
	if (hdd_get_chunks_partialmode) {
		zassert(pthread_mutex_lock(&hashlock));
		hdd_get_chunks_partialmode = 0;
		if (hdd_get_chunks_waiting) {
			zassert(pthread_cond_signal(&hdd_get_chunks_cond));
			hdd_get_chunks_waiting--;
		}
	}
	hdd_get_chunks_pos = 0xFFFFFFFF;
	zassert(pthread_mutex_unlock(&hashlock));
}

uint32_t hdd_get_chunks_next_list_count(uint32_t stopcount) {
	uint32_t res = 0;
	uint32_t i = 0;
	chunk *c;
	regfirstchunk *rfc;

	zassert(pthread_mutex_lock(&folderlock)); // c->owner !!!
	if (hdd_get_chunks_partialmode) {
		zassert(pthread_mutex_lock(&hashlock));
	}
	rfc = regfirstchunks;
	while (res<stopcount && rfc!=NULL) {
		if (rfc->c->owner!=NULL) {
			res++;
		}
		rfc = rfc->next;
	}
	while (res<stopcount && hdd_get_chunks_pos+i<HASHSIZE) {
		for (c=hashtab[hdd_get_chunks_pos+i] ; c ; c=c->next) {
			if (c->owner!=NULL) {
				res++;
			}
		}
		i++;
	}
	if (res==0) {
		if (hdd_get_chunks_partialmode) {
			zassert(pthread_mutex_unlock(&hashlock));
		}
		zassert(pthread_mutex_unlock(&folderlock));
	}
	return res;
}

void hdd_get_chunks_next_list_data(uint32_t stopcount,uint8_t *buff) {
	uint32_t res = 0;
	uint32_t v;
	chunk *c;
	regfirstchunk *rfcn;

	while (res<stopcount && regfirstchunks!=NULL) {
		c = regfirstchunks->c;
		rfcn = regfirstchunks->next;
		if (c->owner!=NULL) {
			put64bit(&buff,c->chunkid);
			v = c->version;
			if (c->owner->markforremoval!=MFR_NO) {
				v |= 0x80000000;
			}
			put32bit(&buff,v);
			res++;
		}
		free(regfirstchunks);
		regfirstchunks = rfcn;
	}
	while (res<stopcount && hdd_get_chunks_pos<HASHSIZE) {
		for (c=hashtab[hdd_get_chunks_pos] ; c ; c=c->next) {
			if (c->owner!=NULL) {
				put64bit(&buff,c->chunkid);
				v = c->version;
				if (c->owner->markforremoval!=MFR_NO) {
					v |= 0x80000000;
				}
				put32bit(&buff,v);
				res++;
			}
		}
		hdd_get_chunks_pos++;
	}
	if (hdd_get_chunks_partialmode) {
		zassert(pthread_mutex_unlock(&hashlock));
	}
	zassert(pthread_mutex_unlock(&folderlock));
}

void hdd_regfirst(uint64_t chunkid) {
	uint32_t hashpos = HASHPOS(chunkid);
	chunk *c;
	regfirstchunk *rfc;

	zassert(pthread_mutex_lock(&hashlock));
	if (hdd_get_chunks_pos!=0xFFFFFFFF) {
		for (c=hashtab[hashpos] ; c ; c=c->next) {
			if ((c->chunkid&UINT64_C(0x00FFFFFFFFFFFFFF))==(chunkid&UINT64_C(0x00FFFFFFFFFFFFFF))) {
				rfc = malloc(sizeof(regfirstchunk));
				rfc->c = c;
				rfc->next = regfirstchunks;
				regfirstchunks = rfc;
			}
		}
	}
	zassert(pthread_mutex_unlock(&hashlock));
}

/*
// for old register packets - deprecated
uint32_t hdd_get_chunks_count(void) {
	uint32_t res = 0;
	uint32_t i;
	chunk *c;
	zassert(pthread_mutex_lock(&hashlock));
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=hashtab[i] ; c ; c=c->next) {
			res++;
		}
	}
	return res;
}

void hdd_get_chunks_data(uint8_t *buff) {
	uint32_t i,v;
	chunk *c;
	if (buff) {
		for (i=0 ; i<HASHSIZE ; i++) {
			for (c=hashtab[i] ; c ; c=c->next) {
				put64bit(&buff,c->chunkid);
				v = c->version;
				if (c->owner->markforremoval!=MFR_NO) {
					v |= 0x80000000;
				}
				put32bit(&buff,v);
			}
		}
	}
}
*/

/*
uint32_t get_changedchunkscount(void) {
	uint32_t res = 0;
	folder *f;
	chunk *c;
	if (somethingchanged==0) {
		return 0;
	}
	for (f=folderhead ; f ; f=f->next) {
		for (c=f->chunkhead ; c ; c=c->next) {
			if (c->lengthchanged) {
				res++;
			}
		}
	}
	return res;
}

void fill_changedchunksinfo(uint8_t *buff) {
	folder *f;
	chunk *c;
	for (f=folderhead ; f ; f=f->next) {
		for (c=f->chunkhead ; c ; c=c->next) {
			if (c->lengthchanged) {
				put64bit(&buff,c->chunkid);
				put32bit(&buff,c->version);
				c->lengthchanged = 0;
			}
		}
	}
	somethingchanged = 0;
}
*/

void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount) {
	zassert(pthread_mutex_lock(&dclock));
	*usedspace = global_csd.usedspace;
	*totalspace = global_csd.totalspace;
	*chunkcount = global_csd.chunkcount;
	*tdusedspace = global_csd.mfrusedspace;
	*tdtotalspace = global_csd.mfrtotalspace;
	*tdchunkcount = global_csd.mfrchunkcount;
	zassert(pthread_mutex_unlock(&dclock));
}

void hdd_get_chart_data(uint32_t *copychunkcount,uint32_t *ec4chunkcount,uint32_t *ec8chunkcount,uint32_t *hddok,uint32_t *hddmfr,uint32_t *hdddmg,uint32_t *usagediff) {
	zassert(pthread_mutex_lock(&dclock));
	*copychunkcount = (global_csd.chunkcount + global_csd.mfrchunkcount) - (global_csd.ec4chunkcount + global_csd.ec8chunkcount);
	*ec4chunkcount = global_csd.ec4chunkcount;
	*ec8chunkcount = global_csd.ec8chunkcount;
	*hddok = global_csd.hddokcount;
	*hddmfr = global_csd.hddmfrcount;
	*hdddmg = global_csd.hdddmgcount;
	*usagediff = global_csd.usagediff;
	zassert(pthread_mutex_unlock(&dclock));
}

static inline void chunk_emptycrc(chunk *c) {
	myalloc(c->crc,CHUNKCRCSIZE);
	passert(c->crc);
	memcpy(c->crc,emptychunkcrc,CHUNKCRCSIZE);
}

static inline int chunk_readcrc(chunk *c,int mode) {
	int ret;
	uint8_t hdr[20];
	const uint8_t *ptr;
	uint64_t chunkid;
	uint32_t version;
	char fname[PATH_MAX];
	uint16_t block;
	uint32_t bcrc;

	if (mypread(c->fd,hdr,20,0)!=20) {
		int errmem = errno;
		hdd_generate_filename(fname,c); // preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"chunk_readcrc: file:%s - read error",fname);
		errno = errmem;
		return MFS_ERROR_IO;
	}
	if (memcmp(hdr,MFSSIGNATURE "C 1.",7)!=0 || (hdr[7]!='0' && hdr[7]!='1')) { // accept chunks 1.1 (correct CRC for non existing blocks)
		hdd_generate_filename(fname,c);
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk_readcrc: file:%s - wrong header",fname);
		errno = 0;
		return MFS_ERROR_IO;
	}
	c->fileversion = ((hdr[5]-'0')<<4) + (hdr[7]-'0');
	ptr = hdr+8;
	chunkid = get64bit(&ptr);
	version = get32bit(&ptr);
	if (mode==MODE_IGNVERS) { // file name has new version, but header still old one - just ignore it
		version = c->version;
	}
	if (c->chunkid!=chunkid || c->version!=version) {
		hdd_generate_filename(fname,c);
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunk_readcrc: file:%s - wrong id/version in header (%016"PRIX64"_%08"PRIX32")",fname,chunkid,version);
		errno = 0;
		return MFS_ERROR_IO;
	}
	myalloc(c->crc,CHUNKCRCSIZE);
	passert(c->crc);
	ret = mypread(c->fd,c->crc,CHUNKCRCSIZE,c->hdrsize);
	if (ret!=CHUNKCRCSIZE) {
		int errmem = errno;
		hdd_generate_filename(fname,c); // preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"chunk_readcrc: file:%s - read error",fname);
		myunalloc((void*)(c->crc),CHUNKCRCSIZE);
		c->crc = NULL;
		errno = errmem;
		return MFS_ERROR_IO;
	}
	if (c->fileversion>=0x11) {
		ptr = (c->crc)+(4*c->blocks);
		for (block=c->blocks ; block<MFSBLOCKSINCHUNK ; block++) {
			bcrc = get32bit(&ptr);
			if (bcrc!=emptyblockcrc && bcrc!=0) { // accept 0 for non existing blocks
				int errmem = errno;
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"test_chunk: file:%s - crc error (empty block %u)",fname,block);
				myunalloc((void*)(c->crc),CHUNKCRCSIZE);
				c->crc = NULL;
				errno = errmem;
				return MFS_ERROR_CRC;
			}
		}
	}
	hdd_stats_read(CHUNKCRCSIZE);
	errno = 0;
	return MFS_STATUS_OK;
}

static inline void chunk_freecrc(chunk *c) {
	myunalloc((void*)(c->crc),CHUNKCRCSIZE);
	c->crc = NULL;
}

static inline int chunk_writecrc(chunk *c,uint8_t emergency_mode) {
	int ret;
	char fname[PATH_MAX];
	if (c->owner!=NULL && emergency_mode==0) {
		zassert(pthread_mutex_lock(&folderlock));
		c->owner->needrefresh = 1;
		zassert(pthread_mutex_unlock(&folderlock));
	}
	ret = mypwrite(c->fd,c->crc,CHUNKCRCSIZE,c->hdrsize);
	if (ret!=CHUNKCRCSIZE) {
		int errmem = errno;
		hdd_generate_filename(fname,c); // preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"chunk_writecrc: file:%s - write error",fname);
		errno = errmem;
		return MFS_ERROR_IO;
	}
	hdd_stats_write(CHUNKCRCSIZE);
	return MFS_STATUS_OK;
}

void hdd_test_show_chunks(void) {
	uint32_t hashpos;
	chunk *c;
	zassert(pthread_mutex_lock(&hashlock));
	for (hashpos=0 ; hashpos<HASHSIZE ; hashpos++) {
		for (c=hashtab[hashpos] ; c ; c=c->next) {
			printf("chunk id:%"PRIu64" version:%"PRIu32" (%016"PRIX64"_%08"PRIX32") state:%u\n",c->chunkid,c->version,c->chunkid,c->version,c->state);
		}
	}
	zassert(pthread_mutex_unlock(&hashlock));
}

#if 0
void hdd_test_show_openedchunks(void) {
	dopchunk *cc,*tcc;
	uint32_t dhashpos;
	chunk *c;
	double now;

	printf("lock doplock\n");
	if (pthread_mutex_lock(&doplock)<0) {
		printf("lock error: %u\n",errno);
	}
	printf("lock ndoplock\n");
	if (pthread_mutex_lock(&ndoplock)<0) {
		printf("lock error: %u\n",errno);
	}
/* append new chunks */
	cc = newdopchunks;
	while (cc) {
		dhashpos = DHASHPOS(cc->chunkid);
		for (tcc=dophashtab[dhashpos] ; tcc && tcc->chunkid!=cc->chunkid ; tcc=tcc->next) {}
		if (tcc) {	// found - ignore
			tcc = cc;
			cc = cc->next;
			free(tcc);
		} else {	// not found - add
			tcc = cc;
			cc = cc->next;
			tcc->next = dophashtab[dhashpos];
			dophashtab[dhashpos] = tcc;
		}
	}
	newdopchunks = NULL;
	printf("unlock ndoplock\n");
	if (pthread_mutex_unlock(&ndoplock)<0) {
		printf("unlock error: %u\n",errno);
	}
/* show all */
	now = monotonic_seconds();
	for (dhashpos=0 ; dhashpos<DHASHSIZE ; dhashpos++) {
		for (cc=dophashtab[dhashpos]; cc ; cc=cc->next) {
			c = hdd_chunk_find(cc->chunkid);
			if (c==NULL) {	// no chunk - delete entry
				printf("id: %"PRIu64" - chunk doesn't exist\n",cc->chunkid);
			} else if (c->crcrefcount>0) {	// io in progress - skip entry
				printf("id: %"PRIu64" - chunk in use (refcount:%u)\n",cc->chunkid,c->crcrefcount);
				hdd_chunk_release(c);
			} else {
#ifdef PRESERVE_BLOCK
				double fdsec,crcsec,blocksec;
				fdsec = c->opento;
				crcsec = c->crcto;
				blocksec = c->blockto;
				if (fdsec>0.0) {
					fdsec -= now;
				}
				if (crcsec>0.0) {
					crcsec -= now;
				}
				if (blocksec>0.0) {
					blocksec -= now;
				}
				printf("id: %"PRIu64" - fd:%d (delay:%.3lfs) crc:%p (delay:%.3lfs) block:%p,blockno:%u (delay:%.3lfs)\n",cc->chunkid,c->fd,fdsec,(void*)(c->crc),crcsec,c->block,c->blockno,blocksec);
#else /* PRESERVE_BLOCK */
				double fdsec,crcsec;
				fdsec = c->opento;
				crcsec = c->crcto;
				if (fdsec>0.0) {
					fdsec -= now;
				}
				if (crcsec>0.0) {
					crcsec -= now;
				}
				printf("id: %"PRIu64" - fd:%d (delay:%.3lfs) crc:%p (delay:%.3lfs)\n",cc->chunkid,c->fd,fdsec,(void*)(c->crc),crcsec);
#endif /* PRESERVE_BLOCK */
				hdd_chunk_release(c);
			}
		}
	}
	printf("unlock doplock\n");
	if (pthread_mutex_unlock(&doplock)<0) {
		printf("unlock error: %u\n",errno);
	}
}
#endif

void hdd_delayed_ops(void) {
	dopchunk **ccp,*cc,*tcc;
	uint32_t dhashpos;
	uint8_t dofsync;
	chunk *c;
	uint64_t ts,te;
	struct stat sb;
	static double lastreport = 0.0;
	char fname[PATH_MAX];
//	int status;

//	printf("delayed ops: before lock\n");
	zassert(pthread_mutex_lock(&doplock));
	dofsync = DoFsyncBeforeClose;
	zassert(pthread_mutex_unlock(&doplock));

	zassert(pthread_mutex_lock(&ndoplock));
//	printf("delayed ops: after lock\n");
/* append new chunks */
	cc = newdopchunks;
	while (cc) {
		dhashpos = DHASHPOS(cc->chunkid);
		for (tcc=dophashtab[dhashpos] ; tcc && tcc->chunkid!=cc->chunkid ; tcc=tcc->next) {}
		if (tcc) {	// found - ignore
			tcc = cc;
			cc = cc->next;
			free(tcc);
		} else {	// not found - add
			tcc = cc;
			cc = cc->next;
			tcc->next = dophashtab[dhashpos];
			dophashtab[dhashpos] = tcc;
		}
	}
	newdopchunks = NULL;
	zassert(pthread_mutex_unlock(&ndoplock));
/* check all */
//	printf("delayed ops: before loop\n");
	for (dhashpos=0 ; dhashpos<DHASHSIZE ; dhashpos++) {
		ccp = dophashtab+dhashpos;
		while ((cc=*ccp)) {
//			printf("find chunk: %llu\n",cc->chunkid);
			c = hdd_chunk_tryfind(cc->chunkid);
//			if (c!=NULL && c!=CHUNKLOCKED) {
//				printf("found chunk: %llu (c->state:%u c->crcrefcount:%u)\n",cc->chunkid,c->state,c->crcrefcount);
//			}
//			c = hdd_chunk_find(cc->chunkid);
			if (c==NULL) {	// no chunk - delete entry
				*ccp = cc->next;
				free(cc);
			} else if (c==CHUNKLOCKED) {	// locked chunk - just ignore
				ccp = &(cc->next);
			} else if (c->crcrefcount>0) {	// io in progress - skip entry
				hdd_chunk_release(c);
				ccp = &(cc->next);
			} else {
				double now;
				if (c->fd>=0 && c->fsyncneeded && dofsync) {
					ts = monotonic_nseconds();
#ifdef F_FULLFSYNC
					if (fcntl(c->fd,F_FULLFSYNC)<0) {
						hdd_error_occured(c,1); // uses and preserves errno !!!
						hdd_generate_filename(fname,c); // preserves errno !!!
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_delayed_ops: file:%s - fsync (via fcntl) error",fname);
					}
#else
					if (fsync(c->fd)<0) {
						hdd_error_occured(c,1); // uses and preserves errno !!!
						hdd_generate_filename(fname,c); // preserves errno !!!
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_delayed_ops: file:%s - fsync (direct call) error",fname);
					}
#endif
					te = monotonic_nseconds();
					hdd_stats_datafsync(c->owner,te-ts);
					c->fsyncneeded = 0;
				}
				now = monotonic_seconds();
#ifdef PRESERVE_BLOCK
//				printf("block\n");
				if (c->block!=NULL && c->blockto<now) {
					myunalloc((void*)(c->block),MFSBLOCKSIZE);
					c->block = NULL;
					c->blockno = 0xFFFF;
					c->blockto = 0.0;
				}
#endif /* PRESERVE_BLOCK */
//				printf("descriptor\n");
				if (c->fd>=0 && c->opento<now) {
					if (c->crcchanged && c->owner!=NULL) { // should never happened !!!
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_delayed_ops: CRC not flushed - writing now");
						if (chunk_writecrc(c,0)!=MFS_STATUS_OK) {
							hdd_error_occured(c,1);	// uses and preserves errno !!!
						} else {
							if (fstat(c->fd,&sb)<0) {
								hdd_error_occured(c,1);	// uses and preserves errno !!!
							} else {
								c->diskusage = sb.st_blocks * 512;
							}
							c->crcchanged = 0;
						}
					}
					if (close(c->fd)<0) {
						hdd_error_occured(c,1);	// uses and preserves errno !!!
						hdd_generate_filename(fname,c); // preserves errno !!!
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_delayed_ops: file:%s - close error",fname);
					}
					c->fd = -1;
					c->opento = 0.0;
					hdd_open_files_handle(OF_AFTER_CLOSE);
				}
//				printf("crc\n");
				if (c->crc!=NULL && c->crcto<now) {
					if (c->crcchanged) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"serious error: crc changes lost (chunk:%016"PRIX64"_%08"PRIX32")",c->chunkid,c->version);
					}
//					printf("chunk %llu - free crc record\n",c->chunkid);
					chunk_freecrc(c);
					c->crcchanged = 0;
					c->crcto = 0.0;
				}
#ifdef PRESERVE_BLOCK
				if (c->fd<0 && c->crc==NULL && c->block==NULL) {
#else /* PRESERVE_BLOCK */
				if (c->fd<0 && c->crc==NULL) {
#endif /* PRESERVE_BLOCK */
					// before removing from data structures - refresh disk usage
					hdd_generate_filename(fname,c);
					if (stat(fname,&sb)<0) {
						hdd_error_occured(c,1); // uses and preserves errno !!!
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_delayed_ops: file:%s - stat error",fname);
					} else {
						c->diskusage = sb.st_blocks * 512U;
						if (c->diskusage==0) { // filesystem is broken ?
							if (monotonic_seconds() > lastreport + 3600) {
								mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"stat returned disk usage equal to zero for non empty chunk file. File length will be used as a disk usage.");
								lastreport = monotonic_seconds();
							}
							c->diskusage = (c->blocks * 65536) + c->hdrsize;
						}
					}
					// and then remove from data structures
					*ccp = cc->next;
					free(cc);
				} else {
					ccp = &(cc->next);
				}
				hdd_chunk_release(c);
			}
		}
	}
//	printf("delayed ops: after loop , before unlock\n");
//	printf("delayed ops: after unlock\n");
}

static int hdd_io_begin(chunk *c,int mode) {
	dopchunk *cc;
	char fname[PATH_MAX];
	int status;
	int add;

//	sassert(c->state==CH_LOCKED);

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunk: %016"PRIX64" - before io",c->chunkid);
	hdd_chunk_iomove(c);
	if (c->crcrefcount==0) {
		hdd_generate_filename(fname,c);
#ifdef PRESERVE_BLOCK
		add = (c->fd<0 && c->crc==NULL && c->block==NULL);
#else /* PRESERVE_BLOCK */
		add = (c->fd<0 && c->crc==NULL);
#endif /* PRESERVE_BLOCK */
		if (c->fd<0) {
			hdd_open_files_handle(OF_BEFORE_OPEN);
			if (mode==MODE_NEW) {
				c->fd = open(fname,O_RDWR | O_CREAT | O_EXCL,0666);
			} else {
				if (c->owner->markforremoval!=MFR_READONLY) {
					c->fd = open(fname,O_RDWR);
				} else {
					c->fd = open(fname,O_RDONLY);
				}
			}
			if (c->fd<0) {
				int errmem = errno;
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_io_begin: file:%s - open error",fname);
				hdd_open_files_handle(OF_AFTER_CLOSE);
				errno = errmem;
				if (errno==ENOSPC) {
					return MFS_ERROR_NOSPACE;
				} else {
					return MFS_ERROR_IO;
				}
			}
			c->fsyncneeded = 0;
		}
		if (c->crc==NULL) {
			if (mode==MODE_NEW) {
				chunk_emptycrc(c);
			} else {
				status = chunk_readcrc(c,mode);
				if (status!=MFS_STATUS_OK) {
					int errmem = errno;
					if (add) {
						close(c->fd);
						c->fd=-1;
						hdd_open_files_handle(OF_AFTER_CLOSE);
					}
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_io_begin: file:%s - read error",fname);
					errno = errmem;
					return status;
				}
			}
			c->crcchanged = 0;
		}
#ifdef PRESERVE_BLOCK
		if (c->block==NULL) {
			myalloc(c->block,MFSBLOCKSIZE);
			passert(c->block);
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunk: %016"PRIX64", block:%p",c->chunkid,c->block);
			c->blockno = 0xFFFF;
		}
#endif /* PRESERVE_BLOCK */
		if (add) {
			cc = malloc(sizeof(dopchunk));
			passert(cc);
			cc->chunkid = c->chunkid;
			zassert(pthread_mutex_lock(&ndoplock));
			cc->next = newdopchunks;
			newdopchunks = cc;
			zassert(pthread_mutex_unlock(&ndoplock));
		}
	}
	c->crcrefcount++;
	errno = 0;
	return MFS_STATUS_OK;
}

static int hdd_io_end(chunk *c) {
	int status;

	if (c->crcchanged) {
		status = chunk_writecrc(c,0);
		c->crcchanged = 0;
		if (status!=MFS_STATUS_OK) {
			return status;
		}
		c->fsyncneeded = 1;
	}
	c->crcrefcount--;
	if (c->crcrefcount==0) {
		double now = monotonic_seconds();
		c->opento = now + OPEN_DELAY;
		c->crcto = now + CRC_DELAY;
#ifdef PRESERVE_BLOCK
		c->blockto = now + BLOCK_DELAY;
#endif
	}
	errno = 0;
	return MFS_STATUS_OK;
}


/* emergency read - not optimal - used only for data recovery */

int hdd_emergency_read(uint64_t chunkid,uint32_t *version,uint16_t blocknum,uint8_t buffer[MFSBLOCKSIZE],uint8_t retries,uint8_t *errorflags) {
	chunk *c;
	int fd;
	int ret,try;
	char fname[PATH_MAX];
	const uint8_t *rptr;
	uint32_t crc;

	if (retries==0) {
		return MFS_ERROR_EINVAL;
	}
	if (hdd_chunk_force_find(chunkid,&c)==2) { // we want to read whatever we can from chunk, so ignore errors here
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (blocknum>=MFSBLOCKSINCHUNK) {
		hdd_chunk_release(c);
		return MFS_ERROR_BNUMTOOBIG;
	}
	*version = c->version;
	hdd_generate_filename(fname,c);
	fd = open(fname,O_RDONLY);
	if (fd<0) {
		hdd_chunk_release(c);
		return MFS_ERROR_IO;
	}
	*errorflags = 0;
	ret = 0;

	for (try=0 ; try<retries ; try++) {
		ret = mypread(fd,buffer,4,c->hdrsize+4U*blocknum);
		if (ret==4) {
			break;
		}
	}
	if (ret!=4) {
		crc = 0;
		*errorflags |= 1; // can't read CRC
	} else {
		rptr = buffer;
		crc = get32bit(&rptr);
	}
	for (try=0 ; try<retries ; try++) {
		ret = mypread(fd,buffer,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
		if (ret==MFSBLOCKSIZE) {
			break;
		}
	}
	if (ret!=MFSBLOCKSIZE) {
		*errorflags |= 2; // can't read block
	}
	if (crc!=mycrc32(0,buffer,MFSBLOCKSIZE)) {
		*errorflags |= 4; // crc mismatch
	}
	close(fd);
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

/* I/O operations */

int hdd_open(uint64_t chunkid,uint32_t version) {
	int status;
	chunk *c;
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,MODE_EXISTING);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
	}
	hdd_chunk_release(c);
//	if (status==MFS_STATUS_OK) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunk %08"PRIX64" opened",chunkid);
//	}
	return status;
}

int hdd_close(uint64_t chunkid,uint8_t forcefsync) {
	int status;
	chunk *c;
	char fname[PATH_MAX];

	while (hdd_chunk_find(chunkid,&c)==2) {} // this is close - ignore time out here - try again
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->crcchanged) {
		hdd_wfr_delete_all_duplicates(chunkid);
	}
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
	}
	if (forcefsync) {
#ifdef F_FULLFSYNC
		if (fcntl(c->fd,F_FULLFSYNC)<0) {
			hdd_error_occured(c,1); // uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_close: file:%s - fsync (via fcntl) error",fname);
		}
#else
		if (fsync(c->fd)<0) {
			hdd_error_occured(c,1); // uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd_close: file:%s - fsync (direct call) error",fname);
		}
#endif
		c->fsyncneeded = 0;
	}
	hdd_chunk_release(c);
//	if (status==MFS_STATUS_OK) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chunk %08"PRIX64" closed",chunkid);
//	}
	return status;
}

static void hdd_sequential_mode_int(chunk *c) {
#if defined(HAVE_POSIX_FADVISE) && defined(POSIX_FADV_SEQUENTIAL)
	posix_fadvise(c->fd,c->hdrsize+CHUNKCRCSIZE,0,POSIX_FADV_SEQUENTIAL);
#else
	(void)c;
#endif
}

static void hdd_drop_caches_int(chunk *c) {
#if defined(HAVE_POSIX_FADVISE) && defined(POSIX_FADV_DONTNEED)
	posix_fadvise(c->fd,0,0,POSIX_FADV_DONTNEED);
#else
	(void)c;
#endif
}

void hdd_precache_data(uint64_t chunkid,uint32_t offset,uint32_t size) {
#if defined(HAVE_POSIX_FADVISE)	&& (defined(POSIX_FADV_WILLNEED) || defined(POSIX_FADV_SEQUENTIAL))
	chunk *c;
	if (hdd_chunk_find(chunkid,&c)==2) {
		return;
	}
	if (c==NULL) {
		return;
	}
#  ifdef POSIX_FADV_SEQUENTIAL
	posix_fadvise(c->fd,c->hdrsize+CHUNKCRCSIZE+offset,size,POSIX_FADV_SEQUENTIAL);
#  endif
#  ifdef POSIX_FADV_WILLNEED
	posix_fadvise(c->fd,c->hdrsize+CHUNKCRCSIZE+offset,size,POSIX_FADV_WILLNEED);
#  endif
	hdd_chunk_release(c);
#else
	(void)chunkid;
	(void)offset;
	(void)size;
#endif
}

int hdd_read(uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff) {
	chunk *c;
	int ret;
	int error;
	const uint8_t *rcrcptr;
	uint32_t crc,bcrc,precrc,postcrc,combinedcrc;
	uint64_t ts,te;
	char fname[PATH_MAX];
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		myalloc(blockbuffer,MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
#endif /* PRESERVE_BLOCK */
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	if (blocknum>=MFSBLOCKSINCHUNK) {
		hdd_chunk_release(c);
		return MFS_ERROR_BNUMTOOBIG;
	}
	if (size>MFSBLOCKSIZE) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGSIZE;
	}
	if ((offset>=MFSBLOCKSIZE) || (offset+size>MFSBLOCKSIZE)) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGOFFSET;
	}
	if (blocknum>=c->blocks) {
		memset(buffer,0,size);
		if (size==MFSBLOCKSIZE) {
			crc = emptyblockcrc;
		} else {
			crc = mycrc32_zeroblock(0,size);
		}
		put32bit(&crcbuff,crc);
		hdd_chunk_release(c);
		return MFS_STATUS_OK;
	}
	if (offset==0 && size==MFSBLOCKSIZE) {
#ifdef PRESERVE_BLOCK
		if (c->blockno==blocknum) {
			memcpy(buffer,c->block,MFSBLOCKSIZE);
			ret = MFSBLOCKSIZE;
			error = 0;
		} else {
#endif /* PRESERVE_BLOCK */
		ts = monotonic_nseconds();
		ret = mypread(c->fd,buffer,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
		error = errno;
		te = monotonic_nseconds();
		hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
#ifdef PRESERVE_BLOCK
			c->blockno = blocknum;
			memcpy(c->block,buffer,MFSBLOCKSIZE);
		}
#endif /* PRESERVE_BLOCK */
		crc = mycrc32(0,buffer,MFSBLOCKSIZE);
		rcrcptr = (c->crc)+(4*blocknum);
		bcrc = get32bit(&rcrcptr);
		if (bcrc!=crc) {
			errno = error;
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c);
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"read_block_from_chunk: file: %s ; block: %"PRIu16" - crc error (data crc: %08"PRIX32" ; check crc: %08"PRIX32")",fname,blocknum,crc,bcrc);
			hdd_chunk_release(c);
			return MFS_ERROR_CRC;
		}
		if (ret!=MFSBLOCKSIZE) {
			errno = error;
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"read_block_from_chunk: file: %s ; block: %"PRIu16" - read error",fname,blocknum);
			hdd_chunk_release(c);
			return MFS_ERROR_IO;
		}
	} else {
#ifdef PRESERVE_BLOCK
		if (c->blockno != blocknum) {
			ts = monotonic_nseconds();
			ret = mypread(c->fd,c->block,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
			error = errno;
			te = monotonic_nseconds();
			hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
			c->blockno = blocknum;
		} else {
			ret = MFSBLOCKSIZE;
			error = 0;
		}
		precrc = mycrc32(0,c->block,offset);
		crc = mycrc32(0,c->block+offset,size);
		postcrc = mycrc32(0,c->block+offset+size,MFSBLOCKSIZE-(offset+size));
#else /* PRESERVE_BLOCK */
		ts = monotonic_nseconds();
		ret = mypread(c->fd,blockbuffer,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
		error = errno;
		te = monotonic_nseconds();
		hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
//		crc = mycrc32(0,blockbuffer+offset,size);	// first calc crc for piece
		precrc = mycrc32(0,blockbuffer,offset);
		crc = mycrc32(0,blockbuffer+offset,size);
		postcrc = mycrc32(0,blockbuffer+offset+size,MFSBLOCKSIZE-(offset+size));
#endif /* PRESERVE_BLOCK */
		if (offset==0) {
			combinedcrc = mycrc32_combine(crc,postcrc,MFSBLOCKSIZE-(offset+size));
		} else {
			combinedcrc = mycrc32_combine(precrc,crc,size);
			if ((offset+size)<MFSBLOCKSIZE) {
				combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			}
		}
		rcrcptr = (c->crc)+(4*blocknum);
		bcrc = get32bit(&rcrcptr);
//		if (bcrc!=mycrc32(0,blockbuffer,MFSBLOCKSIZE)) {
		if (bcrc!=combinedcrc) {
			errno = error;
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c);
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"read_block_from_chunk: file: %s ; block: %"PRIu16" - crc error (data crc: %08"PRIX32" (0:%"PRIu32" - %08"PRIX32" ; %"PRIu32":%"PRIu32" - %08"PRIX32" ; %"PRIu32":%u - %08"PRIX32") ; check crc: %08"PRIX32")",fname,blocknum,combinedcrc,offset,precrc,offset,size,crc,offset+size,MFSBLOCKSIZE-(offset+size),postcrc,bcrc);
			hdd_chunk_release(c);
			return MFS_ERROR_CRC;
		}
		if (ret!=MFSBLOCKSIZE) {
			errno = error;
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"read_block_from_chunk: file: %s ; block: %"PRIu16" - read error",fname,blocknum);
			hdd_chunk_release(c);
			return MFS_ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		memcpy(buffer,c->block+offset,size);
#else /* PRESERVE_BLOCK */
		memcpy(buffer,blockbuffer+offset,size);
#endif /* PRESERVE_BLOCK */
	}
	put32bit(&crcbuff,crc);
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

int hdd_write(uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff) {
	chunk *c;
	int ret;
	int error;
	uint8_t *wcrcptr;
	const uint8_t *rcrcptr;
	uint32_t crc,bcrc,precrc,postcrc,combinedcrc,chcrc;
	uint32_t i;
	uint64_t ts,te;
	uint8_t truncneeded;
	char fname[PATH_MAX];
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		myalloc(blockbuffer,MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
#endif /* PRESERVE_BLOCK */
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	if (blocknum>=MFSBLOCKSINCHUNK) {
		hdd_chunk_release(c);
		return MFS_ERROR_BNUMTOOBIG;
	}
	if (size>MFSBLOCKSIZE) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGSIZE;
	}
	if ((offset>=MFSBLOCKSIZE) || (offset+size>MFSBLOCKSIZE)) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGOFFSET;
	}
	crc = get32bit(&crcbuff);
#ifdef HAVE___SYNC_OP_AND_FETCH
	if (blocknum>=c->blocks && __sync_or_and_fetch(&Sparsification,0)) { // new block - may be sparsified
#else
	pthread_mutex_lock(&cfglock);
	i = Sparsification;
	pthread_mutex_unlock(&cfglock);
	if (blocknum>=c->blocks && i) {
#endif
		uint32_t nzstart,nzend;
		uint32_t dataoffset,datasize;
		const uint8_t *p,*e;
		p = buffer;
		e = p+size;
		while (p<e && (*p)==0) {
			p++;
		}
		if (p==e) { // only zeros !!!
			nzstart = offset;
			nzend = offset;
		} else {
			e--;
			while (p<=e && (*e)==0) {
				e--;
			}
			e++;
			nzstart = (p - buffer) + offset;
			nzend = (e - buffer) + offset;
			nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
			nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
			if (nzstart<offset) {
				nzstart = offset;
			}
			if (nzend>offset+size) {
				nzend = offset+size;
			}
		}
		if (nzstart!=offset || nzend!=offset+size) { // can be sparsified
			dataoffset = nzstart - offset;
			datasize = nzend - nzstart;
			if (datasize==0) {
				combinedcrc = mycrc32_zeroblock(0,size);
				chcrc = 0;
			} else {
				precrc = mycrc32_zeroblock(0,dataoffset);
				postcrc = mycrc32_zeroblock(0,size-(dataoffset+datasize));
				chcrc = mycrc32(0,buffer+dataoffset,datasize);
				if (dataoffset==0) {
					combinedcrc = mycrc32_combine(chcrc,postcrc,size-(dataoffset+datasize));
				} else {
					combinedcrc = mycrc32_combine(precrc,chcrc,datasize);
					if ((dataoffset+datasize)<size) {
						combinedcrc = mycrc32_combine(combinedcrc,postcrc,size-(dataoffset+datasize));
					}
				}
			}
			if (combinedcrc!=crc) {
				hdd_chunk_release(c);
				return MFS_ERROR_CRC;
			}
			buffer += dataoffset;
			offset += dataoffset;
			size = datasize;
			crc = chcrc;
		} else {
			if (crc!=mycrc32(0,buffer,size)) {
				hdd_chunk_release(c);
				return MFS_ERROR_CRC;
			}
		}
	} else {
		if (crc!=mycrc32(0,buffer,size)) {
			hdd_chunk_release(c);
			return MFS_ERROR_CRC;
		}
	}
	if (offset==0 && size==MFSBLOCKSIZE) {
		if (blocknum>=c->blocks) {
			wcrcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				put32bit(&wcrcptr,emptyblockcrc);
			}
			c->blocks = blocknum+1;
		}
		ts = monotonic_nseconds();
		ret = mypwrite(c->fd,buffer,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
		error = errno;
		te = monotonic_nseconds();
		hdd_stats_datawrite(c->owner,MFSBLOCKSIZE,te-ts);
		if (crc!=mycrc32(0,buffer,MFSBLOCKSIZE)) {
			errno = error;
			hdd_error_occured(c,1);
			hdd_generate_filename(fname,c);
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"write_block_to_chunk: file: %s ; block: %"PRIu16" - crc error (data crc: %08"PRIX32" ; check crc: %08"PRIX32")",fname,blocknum,mycrc32(0,buffer,MFSBLOCKSIZE),crc);
			hdd_chunk_release(c);
			return MFS_ERROR_CRC;
		}
		wcrcptr = (c->crc)+(4*blocknum);
		put32bit(&wcrcptr,crc);
		c->crcchanged = 1;
		c->diskusage = 0;
		if (ret!=MFSBLOCKSIZE) {
			if (error==0 || error==EAGAIN) {
				error=ENOSPC;
			}
			errno = error;
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"write_block_to_chunk: file: %s ; block: %"PRIu16" - write error",fname,blocknum);
			hdd_chunk_release(c);
			return MFS_ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		memcpy(c->block,buffer,MFSBLOCKSIZE);
		c->blockno = blocknum;
#endif /* PRESERVE_BLOCK */
	} else {
		truncneeded = 0;
		if (blocknum<c->blocks) {
#ifdef PRESERVE_BLOCK
			if (c->blockno != blocknum) {
				ts = monotonic_nseconds();
				ret = mypread(c->fd,c->block,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
				error = errno;
				te = monotonic_nseconds();
				hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
				c->blockno = blocknum;
			} else {
				ret = MFSBLOCKSIZE;
				error = 0;
			}
#else /* PRESERVE_BLOCK */
			ts = monotonic_nseconds();
			ret = mypread(c->fd,blockbuffer,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS));
			error = errno;
			te = monotonic_nseconds();
			hdd_stats_dataread(c->owner,MFSBLOCKSIZE,te-ts);
#endif /* PRESERVE_BLOCK */
			if (ret!=MFSBLOCKSIZE) {
				errno = error;
				hdd_error_occured(c,1);	// uses and preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"write_block_to_chunk: file: %s ; block: %"PRIu16" - read error",fname,blocknum);
				hdd_chunk_release(c);
				return MFS_ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			precrc = mycrc32(0,c->block,offset);
			chcrc = mycrc32(0,c->block+offset,size);
			postcrc = mycrc32(0,c->block+offset+size,MFSBLOCKSIZE-(offset+size));
#else /* PRESERVE_BLOCK */
			precrc = mycrc32(0,blockbuffer,offset);
			chcrc = mycrc32(0,blockbuffer+offset,size);
			postcrc = mycrc32(0,blockbuffer+offset+size,MFSBLOCKSIZE-(offset+size));
#endif /* PRESERVE_BLOCK */
			if (offset==0) {
				combinedcrc = mycrc32_combine(chcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			} else {
				combinedcrc = mycrc32_combine(precrc,chcrc,size);
				if ((offset+size)<MFSBLOCKSIZE) {
					combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
				}
			}
			rcrcptr = (c->crc)+(4*blocknum);
			bcrc = get32bit(&rcrcptr);
//			if (bcrc!=mycrc32(0,blockbuffer,MFSBLOCKSIZE)) {
			if (bcrc!=combinedcrc) {
				errno = error;
				hdd_error_occured(c,1);	// uses and preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"write_block_to_chunk: file: %s ; block: %"PRIu16" - crc error (data crc: %08"PRIX32" (0:%"PRIu32" - %08"PRIX32" ; %"PRIu32":%"PRIu32" - %08"PRIX32" ; %"PRIu32":%u - %08"PRIX32") ; check crc: %08"PRIX32")",fname,blocknum,combinedcrc,offset,precrc,offset,size,chcrc,offset+size,MFSBLOCKSIZE-(offset+size),postcrc,bcrc);
				hdd_chunk_release(c);
				return MFS_ERROR_CRC;
			}
		} else {
			if (offset+size < MFSBLOCKSIZE) {
				truncneeded = 1;
			}
			wcrcptr = (c->crc)+(4*(c->blocks));
			for (i=c->blocks ; i<blocknum ; i++) {
				put32bit(&wcrcptr,emptyblockcrc);
			}
			c->blocks = blocknum+1;
#ifdef PRESERVE_BLOCK
			memset(c->block,0,MFSBLOCKSIZE);
			c->blockno = blocknum;
#else /* PRESERVE_BLOCK */
//			memset(blockbuffer,0,MFSBLOCKSIZE); // not needed (we do not preserve this buffer) !!!
#endif /* PRESERVE_BLOCK */
			precrc = mycrc32_zeroblock(0,offset);
			postcrc = mycrc32_zeroblock(0,MFSBLOCKSIZE-(offset+size));
		}
		if (size>0) {
#ifdef PRESERVE_BLOCK
			memcpy(c->block+offset,buffer,size);
			ts = monotonic_nseconds();
			ret = mypwrite(c->fd,c->block+offset,size,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS)+offset);
			error = errno;
			te = monotonic_nseconds();
			hdd_stats_datawrite(c->owner,size,te-ts);
			chcrc = mycrc32(0,c->block+offset,size);
#else /* PRESERVE_BLOCK */
			memcpy(blockbuffer+offset,buffer,size);
			ts = monotonic_nseconds();
			ret = mypwrite(c->fd,blockbuffer+offset,size,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocknum)<<MFSBLOCKBITS)+offset);
			error = errno;
			te = monotonic_nseconds();
			hdd_stats_datawrite(c->owner,size,te-ts);
			chcrc = mycrc32(0,blockbuffer+offset,size);
#endif /* PRESERVE_BLOCK */
			if (offset==0) {
				combinedcrc = mycrc32_combine(chcrc,postcrc,MFSBLOCKSIZE-(offset+size));
			} else {
				combinedcrc = mycrc32_combine(precrc,chcrc,size);
				if ((offset+size)<MFSBLOCKSIZE) {
					combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
				}
			}
		} else {
			ret = 0;
			error = 0;
			chcrc = 0; // just initialize variable to silence warnings in some compilers
			if (offset==0) {
				combinedcrc = postcrc;
			} else {
				combinedcrc = precrc;
				if ((offset+size)<MFSBLOCKSIZE) {
					combinedcrc = mycrc32_combine(combinedcrc,postcrc,MFSBLOCKSIZE-(offset+size));
				}
			}
		}
		wcrcptr = (c->crc)+(4*blocknum);
//		bcrc = mycrc32(0,blockbuffer,MFSBLOCKSIZE);
//		put32bit(&wcrcptr,bcrc);
		put32bit(&wcrcptr,combinedcrc);
		c->crcchanged = 1;
		c->diskusage = 0;
//		if (crc!=mycrc32(0,blockbuffer+offset,size)) {
		if (size>0 && crc!=chcrc) {
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"write_block_to_chunk: file: %s ; block: %"PRIu16" - crc error (%"PRIu32":%"PRIu32" ; data crc: %08"PRIX32" ; check crc: %08"PRIX32")",fname,blocknum,offset,size,chcrc,crc);
			hdd_chunk_release(c);
			return MFS_ERROR_CRC;
		}
		if (ret!=(int)size) {
			if (error==0 || error==EAGAIN) {
				error=ENOSPC;
			}
			errno = error;
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"write_block_to_chunk: file: %s ; block: %"PRIu16" - write error",fname,blocknum);
			hdd_chunk_release(c);
			return MFS_ERROR_IO;
		}
		if (truncneeded) {
			if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)(blocknum+1))<<MFSBLOCKBITS))<0) {
				hdd_error_occured(c,1);	// uses and preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"write_block_to_chunk: file: %s ; block: %"PRIu16" - ftruncate error",fname,blocknum);
				hdd_chunk_release(c);
				return MFS_ERROR_IO;
			}
		}
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}



/* chunk info */
/*
int hdd_check_version(uint64_t chunkid,uint32_t version) {
	chunk *c;
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}
*/

int hdd_get_chunk_info(uint64_t chunkid,uint32_t version,uint8_t requested_info,uint8_t *info_buff) {
	int status;
	uint32_t i;
	uint32_t chksum;
	const uint8_t *rptr;
	chunk *c;
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	if (requested_info & REQUEST_BLOCKS) {
		put16bit(&info_buff,c->blocks);
	}
	if (requested_info & (REQUEST_CHECKSUM|REQUEST_CHECKSUM_TAB)) {
		status = hdd_io_begin(c,MODE_EXISTING);
		if (status!=MFS_STATUS_OK) {
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_chunk_release(c);
			return status;
		}
		if (requested_info & REQUEST_CHECKSUM) {
			chksum = 1;
			rptr = c->crc;
			for (i=0 ; i<CHUNKCRCSIZE ; i+=4) {
				chksum *= 426265243;
				chksum ^= get32bit(&rptr);
			}
			put32bit(&info_buff,chksum);
		}
		if (requested_info & REQUEST_CHECKSUM_TAB) {
			memcpy(info_buff,c->crc,CHUNKCRCSIZE);
			info_buff += CHUNKCRCSIZE;
		}
		status = hdd_io_end(c);
		if (status!=MFS_STATUS_OK) {
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_chunk_release(c);
			return status;
		}
	}
	if (requested_info & REQUEST_FILE_PATH) {
		hdd_generate_filename((char*)info_buff,c);
		info_buff += PATH_MAX;
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

/*
int hdd_get_blocks(uint64_t chunkid,uint32_t version,uint8_t *blocks_buff) {
	chunk *c;
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	put16bit(&blocks_buff,c->blocks);
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

int hdd_get_checksum(uint64_t chunkid,uint32_t version,uint8_t *checksum_buff) {
	int status;
	uint32_t i;
	uint32_t chksum;
	const uint8_t *rptr;
	chunk *c;
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,MODE_EXISTING);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	chksum = 1;
	rptr = c->crc;
	for (i=0 ; i<CHUNKCRCSIZE ; i+=4) {
		chksum *= 426265243;
		chksum ^= get32bit(&rptr);
	}
	put32bit(&checksum_buff,chksum);
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

int hdd_get_checksum_tab(uint64_t chunkid,uint32_t version,uint8_t *checksum_tab) {
	int status;
	chunk *c;
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,MODE_EXISTING);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	memcpy(checksum_tab,c->crc,CHUNKCRCSIZE);
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}
*/




/* chunk operations */

static int hdd_int_create(uint64_t chunkid,uint32_t version) {
	folder *f;
	chunk *c;
	int status;
	uint8_t *ptr;
	char fname[PATH_MAX];
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKMAXHDRSIZE];
#else /* PRESERVE_BLOCK */
	uint8_t *hdrbuffer;
#endif /* PRESERVE_BLOCK */

	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		return MFS_ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f,chunkid,version);
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		return MFS_ERROR_CHUNKEXIST;
	}

#ifndef PRESERVE_BLOCK
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = malloc(CHUNKMAXHDRSIZE);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
#endif /* PRESERVE_BLOCK */

	status = hdd_io_begin(c,MODE_NEW);
	if (status!=MFS_STATUS_OK) {
		if (status!=MFS_ERROR_NOSPACE) {
			hdd_error_occured(c,0);	// uses and preserves errno !!!
		}
		hdd_chunk_delete(c);
		return status;
	}
	memset(hdrbuffer,0,CHUNKMAXHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.1",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (write(c->fd,hdrbuffer,c->hdrsize+CHUNKCRCSIZE)!=(ssize_t)(c->hdrsize+CHUNKCRCSIZE)) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_generate_filename(fname,c); // preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"create_newchunk: file:%s - write error",fname);
		hdd_io_end(c);
		unlink(fname);
		hdd_chunk_delete(c);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(c->hdrsize+CHUNKCRCSIZE);
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,0);
		hdd_generate_filename(fname,c);
		unlink(fname);
		hdd_chunk_delete(c);
		return status;
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

static int hdd_int_test(uint64_t chunkid,uint32_t version,uint16_t *blocks) {
	const uint8_t *ptr;
	uint16_t block;
	uint32_t bcrc;
	int32_t retsize;
	uint32_t lasttesttime,now;
	int status;
	chunk *c;
	char fname[PATH_MAX];
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		myalloc(blockbuffer,MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
#endif /* PRESERVE_BLOCK */
	if (blocks!=NULL) {
		*blocks = 0;
	}
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	lasttesttime = c->testtime;
	status = hdd_io_begin(c,MODE_EXISTING);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	hdd_sequential_mode_int(c);
	now = main_time();
	if (lasttesttime+MinTimeBetweenTests<=now) {
		lseek(c->fd,c->hdrsize+CHUNKCRCSIZE,SEEK_SET);
		ptr = c->crc;
		for (block=0 ; block<c->blocks ; block++) {
#ifdef PRESERVE_BLOCK
			retsize = read(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
			retsize = read(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
			if (retsize!=MFSBLOCKSIZE) {
				hdd_error_occured(c,1);	// uses and preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"test_chunk: file:%s - data read error",fname);
				hdd_io_end(c);
				hdd_chunk_release(c);
				return MFS_ERROR_IO;
			}
			hdd_stats_read(MFSBLOCKSIZE);
#ifdef PRESERVE_BLOCK
			c->blockno = block;
#endif
			bcrc = get32bit(&ptr);
#ifdef PRESERVE_BLOCK
			if (bcrc!=mycrc32(0,c->block,MFSBLOCKSIZE)) {
#else /* PRESERVE_BLOCK */
			if (bcrc!=mycrc32(0,blockbuffer,MFSBLOCKSIZE)) {
#endif /* PRESERVE_BLOCK */
				errno = 0;	// set anything to errno
				hdd_error_occured(c,1);	// uses and preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"test_chunk: file:%s - crc error (data block %u)",fname,block);
				hdd_io_end(c);
				hdd_chunk_release(c);
				return MFS_ERROR_CRC;
			}
		}
/* test moved to chunk_readcrc
		if (c->fileversion>=0x11) {
			for (block=c->blocks ; block<MFSBLOCKSINCHUNK ; block++) {
				bcrc = get32bit(&ptr);
				if (bcrc!=emptyblockcrc && bcrc!=0) { // accept 0 for non existing blocks
					errno = 0;	// set anything to errno
					hdd_error_occured(c,1);	// uses and preserves errno !!!
					hdd_generate_filename(fname,c); // preserves errno !!!
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"test_chunk: file:%s - crc error (empty block %u)",fname,block);
					hdd_io_end(c);
					hdd_chunk_release(c);
					return MFS_ERROR_CRC;
				}
			}
		}
*/
		if (MinFlushCacheTime>=0 && lasttesttime+MinFlushCacheTime<=now) {
			hdd_drop_caches_int(c);
		}
	}
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	if (blocks) {
		*blocks = c->blocks;
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

static int hdd_int_duplicate(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion) {
	folder *f;
	uint8_t *ptr,vbuff[4];
	uint16_t block;
	int32_t retsize;
	int status;
	chunk *c,*oc;
	const uint8_t *writeptr;
	const uint8_t *p,*e;
	uint8_t sp;
	uint32_t nzstart,nzend;
	uint8_t truncneeded;
	char ofname[PATH_MAX];
	char fname[PATH_MAX];
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKMAXHDRSIZE];
#else /* PRESERVE_BLOCK */
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		myalloc(blockbuffer,MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = malloc(CHUNKMAXHDRSIZE);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
#endif /* PRESERVE_BLOCK */
#ifdef HAVE___SYNC_OP_AND_FETCH
	sp = __sync_or_and_fetch(&Sparsification,0);
#else
	pthread_mutex_lock(&cfglock);
	sp = Sparsification;
	pthread_mutex_unlock(&cfglock);
#endif

	if (hdd_chunk_find(chunkid,&oc)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (oc==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (oc->version!=version && version>0) {
		hdd_chunk_release(oc);
		return MFS_ERROR_WRONGVERSION;
	}
	if (copyversion==0) {
		copyversion = newversion;
	}
	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		hdd_chunk_release(oc);
		return MFS_ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f,copychunkid,copyversion);
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		hdd_chunk_release(oc);
		return MFS_ERROR_CHUNKEXIST;
	}

	if (newversion!=version) {
		hdd_generate_filename(ofname,oc);
		oc->version = newversion;
		hdd_generate_filename(fname,oc);
		if (rename(ofname,fname)<0) {
			oc->version = version;
			hdd_error_occured(oc,1); // uses and preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duplicate_chunk: file:%s->%s - rename error",ofname,fname);
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;
		}
		status = hdd_io_begin(oc,MODE_IGNVERS);
		if (status!=MFS_STATUS_OK) {
			hdd_error_occured(oc,1);	// uses and preserves errno !!!
			if (rename(fname,ofname)>=0) {
				oc->version = version;
			}
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;	//can't change file version
		}
		ptr = vbuff;
		put32bit(&ptr,newversion);
		if (mypwrite(oc->fd,vbuff,4,16)!=4) {
			hdd_error_occured(oc,1);	// uses and preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duplicate_chunk: file:%s - write error",fname);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			if (rename(fname,ofname)>=0) {
				oc->version = version;
			}
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;
		}
		hdd_stats_write(4);
	} else {
		status = hdd_io_begin(oc,MODE_EXISTING);
		if (status!=MFS_STATUS_OK) {
			hdd_error_occured(oc,1);	// uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;
		}
	}
	hdd_sequential_mode_int(oc);
	status = hdd_io_begin(c,MODE_NEW);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return status;
	}
	memset(hdrbuffer,0,CHUNKMAXHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.1",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	memcpy(c->crc,oc->crc,CHUNKCRCSIZE);
	memcpy(hdrbuffer+c->hdrsize,oc->crc,CHUNKCRCSIZE);
	if (oc->blocks<MFSBLOCKSINCHUNK) {
		memcpy(hdrbuffer+c->hdrsize+4*oc->blocks,emptychunkcrc,4*(MFSBLOCKSINCHUNK-oc->blocks));
	}
	if (write(c->fd,hdrbuffer,c->hdrsize+CHUNKCRCSIZE)!=(ssize_t)(c->hdrsize+CHUNKCRCSIZE)) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_generate_filename(fname,c); // preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duplicate_chunk: file:%s - hdr write error",fname);
		hdd_io_end(c);
		unlink(fname);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(c->hdrsize+CHUNKCRCSIZE);
#ifndef PRESERVE_BLOCK
	lseek(oc->fd,oc->hdrsize+CHUNKCRCSIZE,SEEK_SET);
#endif /* PRESERVE_BLOCK */
	truncneeded = 0;
	for (block=0 ; block<oc->blocks ; block++) {
#ifdef PRESERVE_BLOCK
		if (oc->blockno==block) {
			memcpy(c->block,oc->block,MFSBLOCKSIZE);
			retsize = MFSBLOCKSIZE;
		} else {
			retsize = mypread(oc->fd,c->block,MFSBLOCKSIZE,oc->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
		}
#else /* PRESERVE_BLOCK */
		retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
		if (retsize!=MFSBLOCKSIZE) {
			hdd_error_occured(oc,1);	// uses and preserves errno !!!
			hdd_generate_filename(ofname,oc); // preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duplicate_chunk: file:%s - data read error",ofname);
			hdd_io_end(c);
			unlink(fname);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		if (oc->blockno!=block) {
			hdd_stats_read(MFSBLOCKSIZE);
		}
		writeptr = c->block;
#else /* PRESERVE_BLOCK */
		hdd_stats_read(MFSBLOCKSIZE);
		writeptr = blockbuffer;
#endif /* PRESERVE_BLOCK */
		if (sp) {
			// sparsify
			p = writeptr;
			e = p+MFSBLOCKSIZE;
			while (p<e && (*p)==0) {
				p++;
			}
			if (p==e) {
				nzstart = nzend = 0;
			} else {
				e--;
				while (p<=e && (*e)==0) {
					e--;
				}
				e++;
				nzstart = (p - writeptr);
				nzend = (e - writeptr);
				nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
				nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
			}
		} else {
			nzstart = 0;
			nzend = MFSBLOCKSIZE;
		}
		if (nzend==nzstart) {
			retsize = 0;
			nzstart = nzend = 0;
		} else {
			retsize = mypwrite(c->fd,writeptr+nzstart,nzend-nzstart,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS)+nzstart);
		}
		if (retsize!=(int32_t)(nzend-nzstart)) {
			hdd_error_occured(c,0);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duplicate_chunk: file:%s - data write error",fname);
			hdd_io_end(c);
			unlink(fname);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;	//write error
		}
		hdd_stats_write(nzend-nzstart);
		if (nzend!=MFSBLOCKSIZE) {
			truncneeded = 1;
		} else {
			truncneeded = 0;
		}
#ifdef PRESERVE_BLOCK
		c->blockno = block;
#endif /* PRESERVE_BLOCK */
	}
	if (truncneeded) {
		if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)oc->blocks)<<MFSBLOCKBITS))<0) { // yes it is ok - oc->blocks not c->blocks !!!
			hdd_error_occured(c,0);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duplicate_chunk: file:%s - ftruncate error",fname);
			hdd_io_end(c);
			unlink(fname);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;	//write error
		}
	}
	status = hdd_io_end(oc);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(oc,1);	// uses and preserves errno !!!
		hdd_io_end(c);
		hdd_generate_filename(fname,c); // preserves errno !!!
		unlink(fname);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_generate_filename(fname,c); // preserves errno !!!
		unlink(fname);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = oc->blocks;
	if (c->owner!=NULL) {
		zassert(pthread_mutex_lock(&folderlock));
		c->owner->needrefresh = 1;
		zassert(pthread_mutex_unlock(&folderlock));
	}
	hdd_chunk_release(c);
	hdd_chunk_release(oc);
	return MFS_STATUS_OK;
}

int hdd_rep_setversion(uint64_t chunkid,uint32_t version) {
	uint8_t *ptr,vbuff[4];
	chunk *c;
	char ofname[PATH_MAX];
	char fname[PATH_MAX];

	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	ptr = vbuff;
	put32bit(&ptr,version);
	if (mypwrite(c->fd,vbuff,4,16)!=4) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replication_set_chunk_version: file:%s - write error",fname);
		hdd_chunk_release(c);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(4);
	hdd_generate_filename(ofname,c);
	c->version = version;
	hdd_generate_filename(fname,c);
	if (rename(ofname,fname)<0) {
		c->version = 0;
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replication_set_chunk_version: file:%s->%s - rename error",ofname,fname);
		memset(vbuff,0,4);
		if (mypwrite(c->fd,vbuff,4,16)!=4) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"replication_set_chunk_version: file:%s - write error",ofname);
		}
		hdd_chunk_release(c);
		return MFS_ERROR_IO;
	}
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

static int hdd_int_version(uint64_t chunkid,uint32_t version,uint32_t newversion) {
	int status;
	uint8_t *ptr,vbuff[4];
	chunk *c;
	char ofname[PATH_MAX];
	char fname[PATH_MAX];

	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	status = hdd_io_begin(c,MODE_EXISTING);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		hdd_chunk_release(c);
		return status;
	}
	ptr = vbuff;
	put32bit(&ptr,newversion);
	if (mypwrite(c->fd,vbuff,4,16)!=4) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"set_chunk_version: file:%s - write error",fname);
		hdd_io_end(c);
		hdd_chunk_release(c);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(4);
	hdd_generate_filename(ofname,c);
	c->version = newversion;
	hdd_generate_filename(fname,c);
	if (rename(ofname,fname)<0) {
		c->version = version;
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"set_chunk_version: file:%s->%s - rename error",ofname,fname);
		ptr = vbuff;
		put32bit(&ptr,version);
		if (mypwrite(c->fd,vbuff,4,16)!=4) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"set_chunk_version: file:%s - write error",ofname);
		}
		hdd_chunk_release(c);
		return MFS_ERROR_IO;
	}
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
	}
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_truncate(uint64_t chunkid,uint32_t version,uint32_t newversion,uint32_t length) {
	int status;
	uint8_t *ptr,vbuff[4];
	chunk *c;
	uint32_t blocks;
	uint32_t i;
	char ofname[PATH_MAX];
	char fname[PATH_MAX];
#ifndef PRESERVE_BLOCK
	uint8_t *blockbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		myalloc(blockbuffer,MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
#endif /* !PRESERVE_BLOCK */

	if (length>MFSCHUNKSIZE) {
		return MFS_ERROR_WRONGSIZE;
	}
	if (hdd_chunk_find(chunkid,&c)==2) {
		return MFS_ERROR_NOTDONE;
	}
	// step 1 - change version
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	hdd_generate_filename(ofname,c);
	c->version = newversion;
	hdd_generate_filename(fname,c);
	if (rename(ofname,fname)<0) {
		c->version = version;
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"truncate_chunk: file:%s->%s - rename error",ofname,fname);
		hdd_chunk_release(c);
		return MFS_ERROR_IO;
	}
	status = hdd_io_begin(c,MODE_IGNVERS);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		if (rename(fname,ofname)>=0) {
			c->version = version;
		}
		hdd_chunk_release(c);
		return status;	//can't change file version
	}
	ptr = vbuff;
	put32bit(&ptr,newversion);
	if (mypwrite(c->fd,vbuff,4,16)!=4) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"truncate_chunk: file:%s - write error",fname);
		hdd_io_end(c);
		if (rename(fname,ofname)>=0) {
			c->version = version;
		}
		hdd_chunk_release(c);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(4);
	// step 2. truncate
	blocks = ((length+MFSBLOCKMASK)>>MFSBLOCKBITS);
	if (blocks>c->blocks) {
		if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+(blocks<<MFSBLOCKBITS))<0) {
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"truncate_chunk: file:%s - ftruncate error",fname);
			hdd_io_end(c);
			hdd_chunk_release(c);
			return MFS_ERROR_IO;
		}
		ptr = (c->crc)+(4*(c->blocks));
		for (i=c->blocks ; i<blocks ; i++) {
			put32bit(&ptr,emptyblockcrc);
		}
		c->crcchanged = 1;
		c->diskusage = 0;
	} else {
		uint32_t blocknum = length>>MFSBLOCKBITS;
		uint32_t blockpos = length&MFSCHUNKBLOCKMASK;
		uint32_t blocksize = length&MFSBLOCKMASK;
		if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+length)<0) {
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"truncate_chunk: file:%s - ftruncate error",fname);
			hdd_io_end(c);
			hdd_chunk_release(c);
			return MFS_ERROR_IO;
		}
#ifdef PRESERVE_BLOCK
		if (c->blockno>=blocks) {
			c->blockno = 0xFFFF;	// invalidate truncated block
		}
#endif
		if (blocksize>0) {
			if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+(blocks<<MFSBLOCKBITS))<0) {
				hdd_error_occured(c,1);	// uses and preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"truncate_chunk: file:%s - ftruncate error",fname);
				hdd_io_end(c);
				hdd_chunk_release(c);
				return MFS_ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (c->blockno!=blocknum) {

				if (mypread(c->fd,c->block,blocksize,c->hdrsize+CHUNKCRCSIZE+blockpos)!=(signed)blocksize) {
#else /* PRESERVE_BLOCK */
			if (mypread(c->fd,blockbuffer,blocksize,c->hdrsize+CHUNKCRCSIZE+blockpos)!=(signed)blocksize) {
#endif /* PRESERVE_BLOCK */
				hdd_error_occured(c,1);	// uses and preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"truncate_chunk: file:%s - read error",fname);
				hdd_io_end(c);
				hdd_chunk_release(c);
				return MFS_ERROR_IO;
			}
			hdd_stats_read(blocksize);
#ifdef PRESERVE_BLOCK
			}
			memset(c->block+blocksize,0,MFSBLOCKSIZE-blocksize);
			c->blockno = blocknum;
			i = mycrc32_zeroexpanded(0,c->block,blocksize,MFSBLOCKSIZE-blocksize);
#else /* PRESERVE_BLOCK */
			i = mycrc32_zeroexpanded(0,blockbuffer,blocksize,MFSBLOCKSIZE-blocksize);
#endif /* PRESERVE_BLOCK */
			ptr = (c->crc)+(4*blocknum);
			put32bit(&ptr,i);
			blocknum++;
			c->crcchanged = 1;
			c->diskusage = 0;
		} else {
			ptr = (c->crc)+(4*blocknum);
		}
		if (blocknum < c->blocks) {
			for (i=blocknum ; i<c->blocks ; i++) {
				put32bit(&ptr,emptyblockcrc);
			}
			c->crcchanged = 1;
			c->diskusage = 0;
		}
	}
	if (c->blocks != blocks && c->owner!=NULL) {
		zassert(pthread_mutex_lock(&folderlock));
		c->owner->needrefresh = 1;
		zassert(pthread_mutex_unlock(&folderlock));
	}
	c->blocks = blocks;
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
	}
	hdd_chunk_release(c);
	return status;
}

static int hdd_int_duptrunc(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
	folder *f;
	uint8_t *ptr,vbuff[4];
	uint16_t block;
	uint16_t blocks;
	int32_t retsize;
	uint32_t crc;
	int status;
	chunk *c,*oc;
	const uint8_t *writeptr;
	const uint8_t *p,*e;
	uint8_t sp;
	uint32_t nzstart,nzend;
	uint8_t truncneeded;
	char ofname[PATH_MAX];
	char fname[PATH_MAX];
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKMAXHDRSIZE];
#else /* PRESERVE_BLOCK */
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		myalloc(blockbuffer,MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = malloc(CHUNKMAXHDRSIZE);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
#endif /* PRESERVE_BLOCK */
#ifdef HAVE___SYNC_OP_AND_FETCH
	sp = __sync_or_and_fetch(&Sparsification,0);
#else
	pthread_mutex_lock(&cfglock);
	sp = Sparsification;
	pthread_mutex_unlock(&cfglock);
#endif

	if (length>MFSCHUNKSIZE) {
		return MFS_ERROR_WRONGSIZE;
	}
	if (hdd_chunk_find(chunkid,&oc)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (oc==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (oc->version!=version && version>0) {
		hdd_chunk_release(oc);
		return MFS_ERROR_WRONGVERSION;
	}
	if (copyversion==0) {
		copyversion = newversion;
	}
	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		hdd_chunk_release(oc);
		return MFS_ERROR_NOSPACE;
	}
	c = hdd_chunk_create(f,copychunkid,copyversion);
	zassert(pthread_mutex_unlock(&folderlock));
	if (c==NULL) {
		hdd_chunk_release(oc);
		return MFS_ERROR_CHUNKEXIST;
	}

	if (newversion!=version) {
		hdd_generate_filename(ofname,oc);
		oc->version = newversion;
		hdd_generate_filename(fname,oc);
		if (rename(ofname,fname)<0) {
			oc->version = version;
			hdd_error_occured(oc,1); // uses and preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s->%s - rename error",ofname,fname);
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;
		}
		status = hdd_io_begin(oc,MODE_IGNVERS);
		if (status!=MFS_STATUS_OK) {
			hdd_error_occured(oc,1);	// uses and preserves errno !!!
			if (rename(fname,ofname)>=0) {
				oc->version = version;
			}
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;	//can't change file version
		}
		ptr = vbuff;
		put32bit(&ptr,newversion);
		if (mypwrite(oc->fd,vbuff,4,16)!=4) {
			hdd_error_occured(oc,1);	// uses and preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - write error",fname);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			if (rename(fname,ofname)>=0) {
				oc->version = version;
			}
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;
		}
		hdd_stats_write(4);
	} else {
		status = hdd_io_begin(oc,MODE_EXISTING);
		if (status!=MFS_STATUS_OK) {
			hdd_error_occured(oc,1);	// uses and preserves errno !!!
			hdd_chunk_delete(c);
			hdd_chunk_release(oc);
			return status;
		}
	}
	hdd_sequential_mode_int(oc);
	status = hdd_io_begin(c,MODE_NEW);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return status;
	}
	blocks = ((length+MFSBLOCKMASK)>>MFSBLOCKBITS);
	memset(hdrbuffer,0,CHUNKMAXHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.1",8);
	ptr = hdrbuffer+8;
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	memcpy(hdrbuffer+c->hdrsize,oc->crc,CHUNKCRCSIZE);
	if (write(c->fd,hdrbuffer,c->hdrsize)!=(ssize_t)(c->hdrsize)) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_generate_filename(fname,c); // preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - hdr write error",fname);
		hdd_io_end(c);
		unlink(fname);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(c->hdrsize);
	lseek(c->fd,c->hdrsize+CHUNKCRCSIZE,SEEK_SET);
#ifndef PRESERVE_BLOCK
	lseek(oc->fd,oc->hdrsize+CHUNKCRCSIZE,SEEK_SET);
#endif /* PRESERVE_BLOCK */
	if (blocks>oc->blocks) { // expanding
//		truncneeded = 0; - always expanding here
		for (block=0 ; block<oc->blocks ; block++) {
#ifdef PRESERVE_BLOCK
			if (oc->blockno==block) {
				memcpy(c->block,oc->block,MFSBLOCKSIZE);
				retsize = MFSBLOCKSIZE;
			} else {
				retsize = mypread(oc->fd,c->block,MFSBLOCKSIZE,oc->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
			}
#else /* PRESERVE_BLOCK */
			retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
			if (retsize!=MFSBLOCKSIZE) {
				hdd_error_occured(oc,1);	// uses and preserves errno !!!
				hdd_generate_filename(ofname,oc); // preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data read error",ofname);
				hdd_io_end(c);
				unlink(fname);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return MFS_ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (oc->blockno!=block) {
				hdd_stats_read(MFSBLOCKSIZE);
			}
			writeptr = c->block;
#else /* PRESERVE_BLOCK */
			hdd_stats_read(MFSBLOCKSIZE);
			writeptr = blockbuffer;
#endif /* PRESERVE_BLOCK */
			if (sp) {
				// sparsify
				p = writeptr;
				e = p+MFSBLOCKSIZE;
				while (p<e && (*p)==0) {
					p++;
				}
				if (p==e) {
					nzstart = nzend = 0;
				} else {
					e--;
					while (p<=e && (*e)==0) {
						e--;
					}
					e++;
					nzstart = (p - writeptr);
					nzend = (e - writeptr);
					nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
					nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
				}
			} else {
				nzstart = 0;
				nzend = MFSBLOCKSIZE;
			}
			if (nzend==nzstart) {
				retsize = 0;
				nzstart = nzend = 0;
			} else {
				retsize = mypwrite(c->fd,writeptr+nzstart,nzend-nzstart,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS)+nzstart);
			}
			if (retsize!=(int32_t)(nzend-nzstart)) {
				hdd_error_occured(c,0);	// uses and preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data write error",fname);
				hdd_io_end(c);
				unlink(fname);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return MFS_ERROR_IO;	//write error
			}
			hdd_stats_write(nzend-nzstart);
//			if (nzend!=MFSBLOCKSIZE) { // at the end we always have truncate, so we don't need to calculate this here (code left as comment to be sure that it is commented out on purpose)
//				truncneeded = 1;
//			} else {
//				truncneeded = 0;
//			}
#ifdef PRESERVE_BLOCK
			c->blockno = block;
#endif /* PRESERVE_BLOCK */
		}
		// always truncate because we are expanding chunk here
		if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocks)<<MFSBLOCKBITS))<0) {
			hdd_error_occured(c,0);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - ftruncate error",fname);
			hdd_io_end(c);
			unlink(fname);
			hdd_chunk_delete(c);
			hdd_io_end(oc);
			hdd_chunk_release(oc);
			return MFS_ERROR_IO;	//write error
		}
		if (oc->blocks < MFSBLOCKSINCHUNK) {
			memcpy(hdrbuffer+c->hdrsize+4*(oc->blocks),emptychunkcrc,4*(MFSBLOCKSINCHUNK-oc->blocks));
		}
	} else { // shrinking
		uint32_t blocksize = (length&MFSBLOCKMASK);
		if (blocksize==0) { // aligned shring
			truncneeded = 0;
			for (block=0 ; block<blocks ; block++) {
#ifdef PRESERVE_BLOCK
				if (oc->blockno==block) {
					memcpy(c->block,oc->block,MFSBLOCKSIZE);
					retsize = MFSBLOCKSIZE;
				} else {
					retsize = mypread(oc->fd,c->block,MFSBLOCKSIZE,oc->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
				}
#else /* PRESERVE_BLOCK */
				retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
				if (retsize!=MFSBLOCKSIZE) {
					hdd_error_occured(oc,1);	// uses and preserves errno !!!
					hdd_generate_filename(ofname,oc); // preserves errno !!!
					hdd_generate_filename(fname,c); // preserves errno !!!
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data read error",ofname);
					hdd_io_end(c);
					unlink(fname);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return MFS_ERROR_IO;
				}
#ifdef PRESERVE_BLOCK
				if (oc->blockno!=block) {
					hdd_stats_read(MFSBLOCKSIZE);
				}
				writeptr = c->block;
#else /* PRESERVE_BLOCK */
				hdd_stats_read(MFSBLOCKSIZE);
				writeptr = blockbuffer;
#endif /* PRESERVE_BLOCK */
				if (sp) {
					// sparsify
					p = writeptr;
					e = p+MFSBLOCKSIZE;
					while (p<e && (*p)==0) {
						p++;
					}
					if (p==e) {
						nzstart = nzend = 0;
					} else {
						e--;
						while (p<=e && (*e)==0) {
							e--;
						}
						e++;
						nzstart = (p - writeptr);
						nzend = (e - writeptr);
						nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
						nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
					}
				} else {
					nzstart = 0;
					nzend = MFSBLOCKSIZE;
				}
				if (nzend==nzstart) {
					retsize = 0;
					nzstart = nzend = 0;
				} else {
					retsize = mypwrite(c->fd,writeptr+nzstart,nzend-nzstart,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS)+nzstart);
				}
				if (retsize!=(int32_t)(nzend-nzstart)) {
					hdd_error_occured(c,0);	// uses and preserves errno !!!
					hdd_generate_filename(fname,c); // preserves errno !!!
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data write error",fname);
					hdd_io_end(c);
					unlink(fname);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return MFS_ERROR_IO;	//write error
				}
				hdd_stats_write(nzend-nzstart);
				if (nzend!=MFSBLOCKSIZE) {
					truncneeded = 1;
				} else {
					truncneeded = 0;
				}
#ifdef PRESERVE_BLOCK
				c->blockno = block;
#endif /* PRESERVE_BLOCK */
			}
			if (truncneeded) {
				if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocks)<<MFSBLOCKBITS))<0) {
					hdd_error_occured(c,0);	// uses and preserves errno !!!
					hdd_generate_filename(fname,c); // preserves errno !!!
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - ftruncate error",fname);
					hdd_io_end(c);
					unlink(fname);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return MFS_ERROR_IO;	//write error
				}
			}
			if (blocks < MFSBLOCKSINCHUNK) {
				memcpy(hdrbuffer+c->hdrsize+4*blocks,emptychunkcrc,4*(MFSBLOCKSINCHUNK-blocks));
			}
		} else { // misaligned shrink
//			truncneeded = 0; - we need to check it only in last block
			for (block=0 ; block<blocks-1 ; block++) {
#ifdef PRESERVE_BLOCK
				if (oc->blockno==block) {
					memcpy(c->block,oc->block,MFSBLOCKSIZE);
					retsize = MFSBLOCKSIZE;
				} else {
					retsize = mypread(oc->fd,c->block,MFSBLOCKSIZE,oc->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
				}
#else /* PRESERVE_BLOCK */
				retsize = read(oc->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
				if (retsize!=MFSBLOCKSIZE) {
					hdd_error_occured(oc,1);	// uses and preserves errno !!!
					hdd_generate_filename(ofname,oc); // preserves errno !!!
					hdd_generate_filename(fname,c); // preserves errno !!!
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data read error",ofname);
					hdd_io_end(c);
					unlink(fname);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return MFS_ERROR_IO;
				}
#ifdef PRESERVE_BLOCK
				if (oc->blockno!=block) {
					hdd_stats_read(MFSBLOCKSIZE);
				}
				writeptr = c->block;
#else /* PRESERVE_BLOCK */
				hdd_stats_read(MFSBLOCKSIZE);
				writeptr = blockbuffer;
#endif /* PRESERVE_BLOCK */
				if (sp) {
					// sparsify
					p = writeptr;
					e = p+MFSBLOCKSIZE;
					while (p<e && (*p)==0) {
						p++;
					}
					if (p==e) {
						nzstart = nzend = 0;
					} else {
						e--;
						while (p<=e && (*e)==0) {
							e--;
						}
						e++;
						nzstart = (p - writeptr);
						nzend = (e - writeptr);
						nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
						nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
					}
				} else {
					nzstart = 0;
					nzend = MFSBLOCKSIZE;
				}
				if (nzend==nzstart) {
					retsize = 0;
					nzstart = nzend = 0;
				} else {
					retsize = mypwrite(c->fd,writeptr+nzstart,nzend-nzstart,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS)+nzstart);
				}
				if (retsize!=(int32_t)(nzend-nzstart)) {
					hdd_error_occured(c,0);	// uses and preserves errno !!!
					hdd_generate_filename(fname,c); // preserves errno !!!
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data write error",fname);
					hdd_io_end(c);
					unlink(fname);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return MFS_ERROR_IO;	//write error
				}
				hdd_stats_write(nzend-nzstart);
//				if (nzend!=MFSBLOCKSIZE) { // we need to check it only in last block (below) - left as a comment on purpose
//					truncneeded = 1;
//				} else {
//					truncneeded = 0;
//				}
			}
			block = blocks-1;
#ifdef PRESERVE_BLOCK
			if (oc->blockno==block) {
				memcpy(c->block,oc->block,blocksize);
				retsize = blocksize;
			} else {
				retsize = mypread(oc->fd,c->block,blocksize,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
			}
#else /* PRESERVE_BLOCK */
			retsize = read(oc->fd,blockbuffer,blocksize);
#endif /* PRESERVE_BLOCK */
			if (retsize!=(signed)blocksize) {
				hdd_error_occured(oc,1);	// uses and preserves errno !!!
				hdd_generate_filename(ofname,oc); // preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data read error",ofname);
				hdd_io_end(c);
				unlink(fname);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return MFS_ERROR_IO;
			}
#ifdef PRESERVE_BLOCK
			if (oc->blockno!=block) {
				hdd_stats_read(blocksize);
			}
			memset(c->block+blocksize,0,MFSBLOCKSIZE-blocksize);
			writeptr = c->block;
#else /* PRESERVE_BLOCK */
			hdd_stats_read(blocksize);
			memset(blockbuffer+blocksize,0,MFSBLOCKSIZE-blocksize);
			writeptr = blockbuffer;
#endif /* PRESERVE_BLOCK */
			if (sp) {
				// sparsify
				p = writeptr;
				e = p+blocksize;
				while (p<e && (*p)==0) {
					p++;
				}
				if (p==e) {
					nzstart = nzend = 0;
				} else {
					e--;
					while (p<=e && (*e)==0) {
						e--;
					}
					e++;
					nzstart = (p - writeptr);
					nzend = (e - writeptr);
					nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
					nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
					if (nzend>blocksize) {
						nzend = blocksize;
					}
				}
			} else {
				nzstart = 0;
				nzend = blocksize;
			}
			if (nzend==nzstart) {
				retsize = 0;
				nzstart = nzend = 0;
			} else {
				retsize = mypwrite(c->fd,writeptr+nzstart,nzend-nzstart,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS)+nzstart);
			}
			if (retsize!=(int32_t)(nzend-nzstart)) {
				hdd_error_occured(c,0);	// uses and preserves errno !!!
				hdd_generate_filename(fname,c); // preserves errno !!!
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - data write error",fname);
				hdd_io_end(c);
				unlink(fname);
				hdd_chunk_delete(c);
				hdd_io_end(oc);
				hdd_chunk_release(oc);
				return MFS_ERROR_IO;	//write error
			}
			hdd_stats_write(nzend-nzstart);
			if (nzend!=MFSBLOCKSIZE) {
				truncneeded = 1;
			} else {
				truncneeded = 0;
			}
			ptr = hdrbuffer+c->hdrsize+4*(blocks-1);
#ifdef PRESERVE_BLOCK
			crc = mycrc32_zeroexpanded(0,c->block,blocksize,MFSBLOCKSIZE-blocksize);
#else /* PRESERVE_BLOCK */
			crc = mycrc32_zeroexpanded(0,blockbuffer,blocksize,MFSBLOCKSIZE-blocksize);
#endif /* PRESERVE_BLOCK */
			put32bit(&ptr,crc);
#ifdef PRESERVE_BLOCK
			c->blockno = block;
#endif /* PRESERVE_BLOCK */
			if (truncneeded) {
				if (ftruncate(c->fd,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)blocks)<<MFSBLOCKBITS))<0) {
					hdd_error_occured(c,0);	// uses and preserves errno !!!
					hdd_generate_filename(fname,c); // preserves errno !!!
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - ftruncate error",fname);
					hdd_io_end(c);
					unlink(fname);
					hdd_chunk_delete(c);
					hdd_io_end(oc);
					hdd_chunk_release(oc);
					return MFS_ERROR_IO;	//write error
				}
			}
			if (blocks < MFSBLOCKSINCHUNK) {
				memcpy(hdrbuffer+c->hdrsize+4*blocks,emptychunkcrc,4*(MFSBLOCKSINCHUNK-blocks));
			}
		}
	}
// and now write header
	memcpy(c->crc,hdrbuffer+c->hdrsize,CHUNKCRCSIZE);
	lseek(c->fd,c->hdrsize,SEEK_SET);
	if (write(c->fd,hdrbuffer+c->hdrsize,CHUNKCRCSIZE)!=(ssize_t)(CHUNKCRCSIZE)) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_generate_filename(fname,c); // preserves errno !!!
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"duptrunc_chunk: file:%s - hdr write error",fname);
		hdd_io_end(c);
		unlink(fname);
		hdd_chunk_delete(c);
		hdd_io_end(oc);
		hdd_chunk_release(oc);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(CHUNKCRCSIZE);
	status = hdd_io_end(oc);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(oc,1);	// uses and preserves errno !!!
		hdd_io_end(c);
		hdd_generate_filename(fname,c); // preserves errno !!!
		unlink(fname);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,0);	// uses and preserves errno !!!
		hdd_generate_filename(fname,c); // preserves errno !!!
		unlink(fname);
		hdd_chunk_delete(c);
		hdd_chunk_release(oc);
		return status;
	}
	c->blocks = blocks;
	if (c->owner!=NULL) {
		zassert(pthread_mutex_lock(&folderlock));
		c->owner->needrefresh = 1;
		zassert(pthread_mutex_unlock(&folderlock));
	}
	hdd_chunk_release(c);
	hdd_chunk_release(oc);
	return MFS_STATUS_OK;
}

static int hdd_int_delete(uint64_t chunkid,uint32_t version) {
	chunk *c;
	char fname[PATH_MAX];

	hdd_wfr_delete_all_duplicates(chunkid);

	if (hdd_chunk_force_find(chunkid,&c)==2) { // we do want to delete chunk - it is possible that there are some errors, so force finding chunks even with errors
		return MFS_ERROR_NOTDONE;
	}
	if (c==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (c->version!=version && version>0) {
		hdd_chunk_release(c);
		return MFS_ERROR_WRONGVERSION;
	}
	hdd_generate_filename(fname,c);
	if (unlink(fname)<0) {
		if (errno!=ENOENT) {
			hdd_error_occured(c,0);	// uses and preserves errno !!!
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"delete_chunk: file:%s - unlink error",fname);
			hdd_chunk_release(c);
			return MFS_ERROR_IO;
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"delete_chunk: file:%s - chunk already deleted !!!",fname);
		}
	} else {
		if (c->owner!=NULL) {
			zassert(pthread_mutex_lock(&folderlock));
			c->owner->needrefresh = 1;
			zassert(pthread_mutex_unlock(&folderlock));
		}
	}
	hdd_chunk_delete(c);
	return MFS_STATUS_OK;
}

static void xordata(uint8_t *dst,const uint8_t *src,uint32_t leng) {
	uint32_t *dst4;
	const uint32_t *src4;
#define XOR_ONE_BYTE (*dst++)^=(*src++)
#define XOR_FOUR_BYTES (*dst4++)^=(*src4++)
	if (((unsigned long)dst&3)==((unsigned long)src&3)) {
		while (leng && ((unsigned long)src & 3)) {
			XOR_ONE_BYTE;
			leng--;
		}
		dst4 = (uint32_t*)dst;
		src4 = (const uint32_t*)src;
		while (leng>=32) {
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			XOR_FOUR_BYTES;
			leng-=32;
		}
		while (leng>=4) {
			XOR_FOUR_BYTES;
			leng-=4;
		}
		src = (const uint8_t*)src4;
		dst = (uint8_t*)dst4;
		if (leng) do {
			XOR_ONE_BYTE;
		} while (--leng);
	} else {
		while (leng>=8) {
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			XOR_ONE_BYTE;
			leng-=8;
		}
		if (leng>0) do {
			XOR_ONE_BYTE;
		} while (--leng);
	}
}
#define EXIT_FLAG_SRC_ERROR 0x01
#define EXIT_FLAG_DST_ERROR 0x02
#define EXIT_FLAG_SRC_IO 0x04
#define EXIT_FLAG_DST_IO 0x08
#define EXIT_FLAG_DST_UNLINK 0x10


static int hdd_int_split(uint64_t chunkid,uint32_t version,uint8_t parts,uint32_t missingparts) {
	folder *f;
	const uint8_t *rcrcptr;
	uint8_t *wcrcptr;
	uint32_t bcrc;
	uint8_t *ptr;
	chunk *c,*oc;
	chunk *ctab[9];
	uint8_t *auxblocks;
	uint8_t pos,bg;
	const uint8_t* srcptr[32];
	uint32_t srccrc[32];
	uint8_t mustreadall;
	uint32_t i,mask;
	uint64_t ecidpart;
	uint8_t err,status;
	uint32_t exitflags;
	uint8_t *blockptr;
	const uint8_t *writeptr;
	const uint8_t *p,*e;
	uint8_t sp;
	uint32_t nzstart,nzend;
	uint32_t truncpos[9];
	uint16_t block,partblock;
	int32_t retsize;
	uint8_t part;
	uint8_t allparts;
	uint32_t datamask;
	uint32_t csummask;
	uint64_t ecidstart;
	char ofname[PATH_MAX];
	char fname[PATH_MAX];
	uint8_t hdrbuffer[CHUNKMAXHDRSIZE];

	if (parts==8) {
		datamask = 0x0FF;
		csummask = 0x100;
		ecidstart = UINT64_C(0x2000000000000000);
		allparts = 9;
	} else if (parts==4) {
		datamask = 0x00F;
		csummask = 0x010;
		ecidstart = UINT64_C(0x1000000000000000);
		allparts = 5;
	} else {
		return MFS_ERROR_EINVAL;
	}

	if ((missingparts & (datamask|csummask)) != missingparts) {
		return MFS_ERROR_EINVAL;
	}

#ifdef HAVE___SYNC_OP_AND_FETCH
	sp = __sync_or_and_fetch(&Sparsification,0);
#else
	pthread_mutex_lock(&cfglock);
	sp = Sparsification;
	pthread_mutex_unlock(&cfglock);
#endif

	c = NULL;

	if (chunkid & UINT64_C(0xFF00000000000000)) {
		return MFS_ERROR_EINVAL;
	}
	if (hdd_chunk_find(chunkid,&oc)==2) {
		return MFS_ERROR_NOTDONE;
	}
	if (oc==NULL) {
		return MFS_ERROR_NOCHUNK;
	}
	if (oc->version!=version && version>0) {
		hdd_chunk_release(oc);
		return MFS_ERROR_WRONGVERSION;
	}

	if (((missingparts&datamask) != datamask) && ((missingparts&csummask)!=0)) {
		mustreadall = 1;
	} else {
		mustreadall = 0;
	}

	auxblocks = malloc(((4*parts)+1)*MFSBLOCKSIZE);
	passert(auxblocks);

	exitflags = 0;

	for (i=0 ; i<allparts ; i++) {
		ctab[i] = NULL;
	}

	zassert(pthread_mutex_lock(&folderlock));
	f = hdd_getfolder();
	if (f==NULL) {
		zassert(pthread_mutex_unlock(&folderlock));
		status = MFS_ERROR_NOSPACE;
		goto error;
	}
	err = 0;
	for (i=0,mask=1,ecidpart=ecidstart ; i<allparts ; i++,mask<<=1,ecidpart+=UINT64_C(0x0100000000000000)) {
		if (err==0 && (missingparts&mask)) {
			ctab[i] = hdd_chunk_create(f,chunkid+ecidpart,0);
			if (ctab[i]==NULL) {
				err = 1;
			}
		} else {
			ctab[i] = NULL;
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
	if (err) {
		status = MFS_ERROR_CHUNKEXIST;
		goto error;
	}

	status = hdd_io_begin(oc,MODE_EXISTING);
	if (status!=MFS_STATUS_OK) {
		exitflags |= EXIT_FLAG_SRC_ERROR;
		goto error;
	}

	exitflags |= EXIT_FLAG_SRC_IO | EXIT_FLAG_DST_IO | EXIT_FLAG_DST_UNLINK;

	hdd_sequential_mode_int(oc);
	for (i=0 ; i<allparts ; i++) {
		c = ctab[i];
		if (c!=NULL) {
			status = hdd_io_begin(c,MODE_NEW);
			if (status!=MFS_STATUS_OK) {
				exitflags |= EXIT_FLAG_DST_ERROR;
				goto error;
			} else {
				exitflags |= (0x100 << i);
			}
		}
	}

	memset(hdrbuffer,0,CHUNKMAXHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.1",8);
	for (i=0,ecidpart=ecidstart ; i<allparts ; i++,ecidpart+=UINT64_C(0x0100000000000000)) {
		c = ctab[i];
		if (c!=NULL) {
			ptr = hdrbuffer+8;
			put64bit(&ptr,chunkid+ecidpart);
			put32bit(&ptr,0);
			memcpy(hdrbuffer+c->hdrsize,emptychunkcrc,4*MFSBLOCKSINCHUNK); // init chunks with 'empty crc'
			if (write(c->fd,hdrbuffer,c->hdrsize+CHUNKCRCSIZE)!=(ssize_t)(c->hdrsize+CHUNKCRCSIZE)) {
				exitflags |= EXIT_FLAG_DST_ERROR;
				status = MFS_ERROR_IO;
				goto error;
			}
		}
	}

	for (part=0 ; part<allparts ; part++) {
		truncpos[part] = 0;
	}
	for (pos=0 ; pos<4*parts ; pos++) {
		srcptr[pos] = NULL;
		srccrc[pos] = 0;
	}

	for (block=0 ; block<oc->blocks ; block++) {
		if (parts==8) {
			// xxxxxxxyyyxx - (x = partblock, y = part number)
			partblock = (block&3) | ((block>>3)&~3);
			part = (block>>2)&7;
			pos = block & 0x1F;
		} else { // parts==4
			// xxxxxxxxyyxx - (x = partblock, y = part number)
			partblock = (block&3) | ((block>>2)&~3);
			part = (block>>2)&3;
			pos = block & 0xF;
		}
		c = ctab[part];
		// silence compiler warnings
		writeptr = NULL;
		nzstart = 0;
		nzend = 0;
		bcrc = 0;
		// -----
		if (c!=NULL || mustreadall) {
			blockptr = auxblocks + pos * MFSBLOCKSIZE;
			retsize = mypread(oc->fd,blockptr,MFSBLOCKSIZE,oc->hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS));
			if (retsize!=MFSBLOCKSIZE) {
				exitflags |= EXIT_FLAG_SRC_ERROR;
				status = MFS_ERROR_IO;
				goto error;
			}
			hdd_stats_read(MFSBLOCKSIZE);
			writeptr = blockptr;
			rcrcptr = (oc->crc)+(4*block);
			bcrc = get32bit(&rcrcptr);
			srcptr[pos] = blockptr;
			srccrc[pos] = bcrc;
		}
		if (writeptr!=NULL && c!=NULL) {
			if (sp) {
				// sparsify
				p = writeptr;
				e = p+MFSBLOCKSIZE;
				while (p<e && (*p)==0) {
					p++;
				}
				if (p==e) {
					nzstart = nzend = 0;
				} else {
					e--;
					while (p<=e && (*e)==0) {
						e--;
					}
					e++;
					nzstart = (p - writeptr);
					nzend = (e - writeptr);
					nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
					nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
				}
			} else {
				nzstart = 0;
				nzend = MFSBLOCKSIZE;
			}
			if (nzend==nzstart) {
				retsize = 0;
				nzstart = nzend = 0;
			} else {
				retsize = mypwrite(c->fd,writeptr+nzstart,nzend-nzstart,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)partblock)<<MFSBLOCKBITS)+nzstart);
			}
			if (retsize!=(int32_t)(nzend-nzstart)) {
				exitflags |= EXIT_FLAG_DST_ERROR;
				status = MFS_ERROR_IO;
				goto error;
			}
			hdd_stats_write(nzend-nzstart);
			if (nzend!=MFSBLOCKSIZE) {
				truncpos[part] = c->hdrsize+CHUNKCRCSIZE+(((uint32_t)partblock)<<MFSBLOCKBITS)+MFSBLOCKSIZE;
			} else {
				truncpos[part] = 0;
			}
			if (partblock>=c->blocks) {
				c->blocks = partblock+1;
			}
			wcrcptr = (c->crc)+(4*partblock);
			put32bit(&wcrcptr,bcrc);
			c->crcchanged = 1;
			c->diskusage = 0;
		}

		if ((parts==8 && (block&0x1F)==0x1F) || (parts==4 && (block&0x0F)==0x0F) || block+1==oc->blocks) {
			c = ctab[parts];
			if (c!=NULL) {
				for (bg=0 ; bg<4 ; bg++) {
					if (parts==8) {
						partblock = ((block>>3)&~3) | bg;
					} else {
						partblock = ((block>>2)&~3) | bg;
					}
					wcrcptr = (c->crc)+4*(partblock);
					blockptr = auxblocks+(4*parts)*MFSBLOCKSIZE;
					if (srcptr[bg]!=NULL) {
						memcpy(blockptr,srcptr[bg],MFSBLOCKSIZE);
						bcrc = srccrc[bg]^emptyblockcrc;
					} else {
						memset(blockptr,0,MFSBLOCKSIZE);
						bcrc = 0; // emptyblockcrc^emptyblockcrc
					}
					for (part=1 ; part<parts ; part++) {
						if (srcptr[bg+4*part]!=NULL) {
							xordata(blockptr,srcptr[bg+4*part],MFSBLOCKSIZE);
							bcrc ^= srccrc[bg+4*part];
						} else {
							bcrc ^= emptyblockcrc;
						}
					}
					writeptr = blockptr;
					if (sp) {
						// sparsify
						p = writeptr;
						e = p+MFSBLOCKSIZE;
						while (p<e && (*p)==0) {
							p++;
						}
						if (p==e) {
							nzstart = nzend = 0;
						} else {
							e--;
							while (p<=e && (*e)==0) {
								e--;
							}
							e++;
							nzstart = (p - writeptr);
							nzend = (e - writeptr);
							nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
							nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
						}
					} else {
						nzstart = 0;
						nzend = MFSBLOCKSIZE;
					}
					if (nzend==nzstart) {
						retsize = 0;
						nzstart = nzend = 0;
					} else {
						retsize = mypwrite(c->fd,writeptr+nzstart,nzend-nzstart,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)partblock)<<MFSBLOCKBITS)+nzstart);
					}
					/*
					   retsize = mypwrite(c->fd,auxblocks+(4*parts)*MFSBLOCKSIZE,MFSBLOCKSIZE,c->hdrsize+CHUNKCRCSIZE+(((uint32_t)partblock)<<MFSBLOCKBITS));
					   if (retsize != MFSBLOCKSIZE) {
					   exitflags |= EXIT_FLAG_DST_ERROR;
					   status = MFS_ERROR_IO;
					   goto error;
					   }
					   hdd_stats_write(MFSBLOCKSIZE);
					   */
					if (retsize!=(int32_t)(nzend-nzstart)) {
						exitflags |= EXIT_FLAG_DST_ERROR;
						status = MFS_ERROR_IO;
						goto error;
					}
					hdd_stats_write(nzend-nzstart);
					if (nzend!=MFSBLOCKSIZE) {
						truncpos[parts] = c->hdrsize+CHUNKCRCSIZE+(((uint32_t)partblock)<<MFSBLOCKBITS)+MFSBLOCKSIZE;
					} else {
						truncpos[parts] = 0;
					}
					put32bit(&wcrcptr,bcrc);
					if (partblock>=c->blocks) {
						c->blocks = partblock+1;
					}
				}
				c->crcchanged = 1;
				c->diskusage = 0;
			}
			for (pos=0 ; pos<4*parts ; pos++) {
				srcptr[pos] = NULL;
				srccrc[pos] = 0;
			}
		}
	}

	for (part=0 ; part<allparts ; part++) {
		if (truncpos[part]>0) {
			c = ctab[part];
			if (ftruncate(c->fd,truncpos[part])<0) {
				exitflags |= EXIT_FLAG_DST_ERROR;
				status = MFS_ERROR_IO;
				goto error;
			}
		}
	}

	status = hdd_io_end(oc);
	exitflags &= ~EXIT_FLAG_SRC_IO; // do not repeat hdd_io_end
	if (status!=MFS_STATUS_OK) {
		exitflags |= EXIT_FLAG_SRC_ERROR;
		goto error;
	}

	for (i=0 ; i<allparts ; i++) {
		c = ctab[i];
		if (c!=NULL) {
			ptr = hdrbuffer+16;
			put32bit(&ptr,version);
			if (mypwrite(c->fd,hdrbuffer+16,4,16)!=4) {
				exitflags |= EXIT_FLAG_DST_ERROR;
				status = MFS_ERROR_IO;
				goto error;
			}
			hdd_generate_filename(ofname,c);
			c->version = version;
			hdd_generate_filename(fname,c);
			if (rename(ofname,fname)<0) {
				c->version = 0;
				exitflags |= EXIT_FLAG_DST_ERROR;
				status = MFS_ERROR_IO;
				goto error;
			}
			status = hdd_io_end(c);
			exitflags &= ~(0x100 << i); // do not repeat this hdd_io_end
			if (status!=MFS_STATUS_OK) {
				exitflags |= EXIT_FLAG_DST_ERROR;
				goto error;
			}
		}
	}
	exitflags &= ~EXIT_FLAG_DST_IO;

	zassert(pthread_mutex_lock(&folderlock));
	f->needrefresh = 1;
	zassert(pthread_mutex_unlock(&folderlock));

	for (i=0 ; i<allparts ; i++) {
		c = ctab[i];
		if (c!=NULL) {
			hdd_chunk_release(c);
		}
		ctab[i] = NULL;
	}

	status = MFS_STATUS_OK;

error:
	if ((exitflags & EXIT_FLAG_SRC_ERROR) && oc!=NULL) {
		hdd_error_occured(oc,1); // uses errno - needs to be here
	}

	if ((exitflags & EXIT_FLAG_DST_ERROR) && c!=NULL) { // assume that i points to chunk with error
		hdd_error_occured(c,0); // uses errno
	}

	for (i=0 ; i<allparts ; i++) {
		if (ctab[i]!=NULL) {
			if (exitflags & (0x100 << i)) {
				if (exitflags & EXIT_FLAG_DST_IO) {
					hdd_io_end(ctab[i]);
				}
				if (exitflags & EXIT_FLAG_DST_UNLINK) {
					hdd_generate_filename(fname,ctab[i]);
					unlink(fname);
				}
			}
			hdd_chunk_delete(ctab[i]);
		}
	}
	if (oc!=NULL) {
		if (exitflags & EXIT_FLAG_SRC_IO) {
			hdd_io_end(oc);
		}
		hdd_chunk_release(oc);
	}
	if (auxblocks!=NULL) {
		free(auxblocks);
	}
	return status;

}


/* all chunk operations in one call */
// newversion>0 && length==0xFFFFFFFF && copychunkid==0      -> change version
// newversion>0 && length==0xFFFFFFFF && copychunkid>0       -> duplicate
// newversion>0 && length&0x80000000                         -> split
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid==0    -> truncate
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid>0     -> duplicate and truncate
// newversion==0 && length==0                                -> delete
// newversion==0 && length==1                                -> create
// newversion==0 && length==2                                -> check chunk contents
int hdd_chunkop(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
	zassert(pthread_mutex_lock(&statslock));
	if (newversion>0) {
		if (length==0xFFFFFFFF) {
			if (copychunkid==0) {
				stats_version++;
			} else {
				stats_duplicate++;
			}
		} else if (length&0x80000000 && (copyversion==4 || copyversion==8)) {
			stats_split++;
		} else if (length<=MFSCHUNKSIZE) {
			if (copychunkid==0) {
				stats_truncate++;
			} else {
				stats_duptrunc++;
			}
		}
	} else {
		if (length==0) {
			stats_delete++;
		} else if (length==1) {
			stats_create++;
		} else if (length==2) {
			stats_test++;
		}
		// length==10 and length==11 - internal operations requested by replicator - do not increase stats
	}
	zassert(pthread_mutex_unlock(&statslock));
	if (newversion>0) {
		if (length==0xFFFFFFFF) {
			if (copychunkid==0) {
				return hdd_int_version(chunkid,version,newversion);
			} else {
				return hdd_int_duplicate(chunkid,version,newversion,copychunkid,copyversion);
			}
		} else if (length&0x80000000 && (copyversion==4 || copyversion==8)) {
			return hdd_int_split(chunkid,version,copyversion,length&0x1FFFF);
		} else if (length<=MFSCHUNKSIZE) {
			if (copychunkid==0) {
				return hdd_int_truncate(chunkid,version,newversion,length);
			} else {
				return hdd_int_duptrunc(chunkid,version,newversion,copychunkid,copyversion,length);
			}
		} else {
			return MFS_ERROR_EINVAL;
		}
	} else {
		if (length==0 || length==10) {
			return hdd_int_delete(chunkid,version);
		} else if (length==1 || length==11) {
			return hdd_int_create(chunkid,version);
		} else if (length==2) {
			return hdd_int_test(chunkid,version,NULL);
		} else {
			return MFS_ERROR_EINVAL;
		}
	}
}

chunk* hdd_random_chunk(folder *f) {
	uint32_t try;
	uint32_t pos;
	chunk *c;

	zassert(pthread_mutex_lock(&folderlock));
	zassert(pthread_mutex_lock(&hashlock));
	if (f->chunkcount>0) {
		for (try=0 ; try<RANDOM_CHUNK_RETRIES ; try++) {
			pos = rndu32_ranged(f->chunkcount);
			c = f->chunktab[pos];
			if (c->state==CH_AVAIL && c->damaged==0) {
				c->state = CH_LOCKED;
				zassert(pthread_mutex_unlock(&hashlock));
				zassert(pthread_mutex_unlock(&folderlock));
				if (c->validattr==0) {
					if (hdd_chunk_getattr(c,0)<0) {
						hdd_error_occured(c,1);
						hdd_chunk_release(c);
					} else {
						return c;
					}
				} else {
					return c;
				}
				zassert(pthread_mutex_lock(&folderlock));
				zassert(pthread_mutex_lock(&hashlock));
			}
		}
	}
	zassert(pthread_mutex_unlock(&hashlock));
	zassert(pthread_mutex_unlock(&folderlock));
	return NULL;
}

static int hdd_int_move(folder *fsrc,folder *fdst) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint16_t block;
	uint32_t bcrc;
	int32_t retsize;
	int status;
	int error;
	char *tmp_filename;
	uint32_t leng;
	int new_fd;
	uint16_t new_hdrsize;
	uint16_t oldpathid,newpathid;
	chunk *c;
	uint64_t ts,te;
	const uint8_t *writeptr;
	const uint8_t *p,*e;
	uint8_t sp;
	uint32_t nzstart,nzend;
	uint8_t truncneeded;
	char fname[PATH_MAX];
#ifdef PRESERVE_BLOCK
	uint8_t hdrbuffer[CHUNKMAXHDRSIZE];
#else /* PRESERVE_BLOCK */
	uint8_t *blockbuffer,*hdrbuffer;
	blockbuffer = pthread_getspecific(blockbufferkey);
	if (blockbuffer==NULL) {
		myalloc(blockbuffer,MFSBLOCKSIZE);
		passert(blockbuffer);
		zassert(pthread_setspecific(blockbufferkey,blockbuffer));
	}
	hdrbuffer = pthread_getspecific(hdrbufferkey);
	if (hdrbuffer==NULL) {
		hdrbuffer = malloc(CHUNKMAXHDRSIZE);
		passert(hdrbuffer);
		zassert(pthread_setspecific(hdrbufferkey,hdrbuffer));
	}
#endif /* PRESERVE_BLOCK */
#ifdef HAVE___SYNC_OP_AND_FETCH
	sp = __sync_or_and_fetch(&Sparsification,0);
#else
	pthread_mutex_lock(&cfglock);
	sp = Sparsification;
	pthread_mutex_unlock(&cfglock);
#endif

	zassert(pthread_mutex_lock(&folderlock));
	if (folderactions==0) {
		zassert(pthread_mutex_unlock(&folderlock));
		return MFS_ERROR_NOTDONE;
	}
	if (!(
		(fsrc->damaged==0 && fsrc->toremove==REMOVING_NO && fsrc->markforremoval==MFR_NO && fsrc->scanstate==SCST_WORKING && fsrc->total>0) &&
		(fdst->damaged==0 && fdst->toremove==REMOVING_NO && fdst->markforremoval==MFR_NO && fdst->scanstate==SCST_WORKING && fdst->total>0 && fdst->wfrcount==0)
	)) {
		zassert(pthread_mutex_unlock(&folderlock));
		return MFS_ERROR_NOTDONE;
	}
	zassert(pthread_mutex_unlock(&folderlock));
	c = hdd_random_chunk(fsrc);
	if (c==NULL) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"move chunk %s -> %s (can't find valid chunk to move)",fsrc->path,fdst->path);
		return MFS_ERROR_NOCHUNK;
	}
#ifdef MFSDEBUG
	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"move chunk %s -> %s (chunk: %016"PRIX64"_%08"PRIX32")",fsrc->path,fdst->path,c->chunkid,c->version);
#endif
	status = hdd_io_begin(c,MODE_EXISTING);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);
		hdd_chunk_release(c);
		return status;
	}
	hdd_sequential_mode_int(c);

	/* create tmp file name */
	leng = strlen(fdst->path);
	tmp_filename = malloc(leng+13+16+1);
	passert(tmp_filename);
	memcpy(tmp_filename,fdst->path,leng);
	snprintf(tmp_filename+leng,13+16+1,"reptmp_chunk_%016"PRIX64,c->chunkid);
	tmp_filename[leng+13+16] = 0;

	/* create new file */
	new_fd = open(tmp_filename,O_RDWR | O_CREAT | O_EXCL,0666);
	if (new_fd<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"move_chunk: file:%s - hdr open error",tmp_filename);
		hdd_io_end(c);
		hdd_chunk_release(c);
		free(tmp_filename);
		return MFS_ERROR_IO;
	}
	new_hdrsize = NEWHDRSIZE;

	memset(hdrbuffer,0,CHUNKMAXHDRSIZE);
	memcpy(hdrbuffer,MFSSIGNATURE "C 1.1",8);
	wptr = hdrbuffer+8;
	put64bit(&wptr,c->chunkid);
	put32bit(&wptr,c->version);
	memcpy(hdrbuffer+new_hdrsize,c->crc,CHUNKCRCSIZE);
	if (c->blocks<MFSBLOCKSINCHUNK) {
		memcpy(hdrbuffer+new_hdrsize+4*c->blocks,emptychunkcrc,4*(MFSBLOCKSINCHUNK-c->blocks));
	}
	if (write(new_fd,hdrbuffer,new_hdrsize+CHUNKCRCSIZE)!=(ssize_t)(new_hdrsize+CHUNKCRCSIZE)) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"move_chunk: file:%s - hdr write error",tmp_filename);
		close(new_fd);
		unlink(tmp_filename);
		hdd_io_end(c);
		hdd_chunk_release(c);
		free(tmp_filename);
		return MFS_ERROR_IO;
	}
	hdd_stats_write(new_hdrsize+CHUNKCRCSIZE);
	lseek(c->fd,c->hdrsize+CHUNKCRCSIZE,SEEK_SET);
	rptr = c->crc;
	truncneeded = 0;
	for (block=0 ; block<c->blocks ; block++) {
		ts = monotonic_nseconds();
#ifdef PRESERVE_BLOCK
		retsize = read(c->fd,c->block,MFSBLOCKSIZE);
#else /* PRESERVE_BLOCK */
		retsize = read(c->fd,blockbuffer,MFSBLOCKSIZE);
#endif /* PRESERVE_BLOCK */
		error = errno;
		te = monotonic_nseconds();
		if (retsize!=MFSBLOCKSIZE) {
			errno = error;
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"move_chunk: file:%s - data read error",fname);
			close(new_fd);
			unlink(tmp_filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			free(tmp_filename);
			return MFS_ERROR_IO;
		}
		hdd_stats_dataread(fsrc,MFSBLOCKSIZE,te-ts);
		hdd_stats_read(MFSBLOCKSIZE);
#ifdef PRESERVE_BLOCK
		c->blockno = block;
#endif
		bcrc = get32bit(&rptr);
#ifdef PRESERVE_BLOCK
		if (bcrc!=mycrc32(0,c->block,MFSBLOCKSIZE)) {
#else /* PRESERVE_BLOCK */
		if (bcrc!=mycrc32(0,blockbuffer,MFSBLOCKSIZE)) {
#endif /* PRESERVE_BLOCK */
			errno = 0;	// set anything to errno
			hdd_error_occured(c,1);	// uses and preserves errno !!!
			hdd_generate_filename(fname,c); // preserves errno !!!
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"move_chunk: file:%s - crc error",fname);
			close(new_fd);
			unlink(tmp_filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			free(tmp_filename);
			return MFS_ERROR_CRC;
		}
#ifdef PRESERVE_BLOCK
		writeptr = c->block;
#else /* PRESERVE_BLOCK */
		writeptr = blockbuffer;
#endif /* PRESERVE_BLOCK */
		if (sp) {
			// sparsify
			p = writeptr;
			e = p+MFSBLOCKSIZE;
			while (p<e && (*p)==0) {
				p++;
			}
			if (p==e) {
				nzstart = nzend = 0;
			} else {
				e--;
				while (p<=e && (*e)==0) {
					e--;
				}
				e++;
				nzstart = (p - writeptr);
				nzend = (e - writeptr);
				nzstart &= UINT32_C(0xFFFFFE00); // floor(nzstart/512)*512
				nzend = (nzend+511) & UINT32_C(0xFFFFFE00); // ceil(nzend/512)*512
			}
		} else {
			nzstart = 0;
			nzend = MFSBLOCKSIZE;
		}
		ts = monotonic_nseconds();
		if (nzend==nzstart) {
			retsize = 0;
			nzstart = nzend = 0;
		} else {
			retsize = mypwrite(new_fd,writeptr+nzstart,nzend-nzstart,new_hdrsize+CHUNKCRCSIZE+(((uint32_t)block)<<MFSBLOCKBITS)+nzstart);
		}
		te = monotonic_nseconds();
		if (retsize!=(int32_t)(nzend-nzstart)) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"move_chunk: file:%s - data write error",tmp_filename);
			close(new_fd);
			unlink(tmp_filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			free(tmp_filename);
			return MFS_ERROR_IO;	//write error
		}
		hdd_stats_datawrite(fdst,nzend-nzstart,te-ts);
		hdd_stats_write(nzend-nzstart);
		if (nzend!=MFSBLOCKSIZE) {
			truncneeded = 1;
		} else {
			truncneeded = 0;
		}
	}
	if (truncneeded) {
		if (ftruncate(new_fd,new_hdrsize+CHUNKCRCSIZE+(((uint32_t)c->blocks)<<MFSBLOCKBITS))<0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"move_chunk: file:%s - ftruncate error",tmp_filename);
			close(new_fd);
			unlink(tmp_filename);
			hdd_io_end(c);
			hdd_chunk_release(c);
			free(tmp_filename);
			return MFS_ERROR_IO;	//write error
		}
	}
	hdd_drop_caches_int(c);
	status = hdd_io_end(c);
	if (status!=MFS_STATUS_OK) {
		hdd_error_occured(c,1);	// uses and preserves errno !!!
		close(new_fd);
		unlink(tmp_filename);
		hdd_chunk_release(c);
		free(tmp_filename);
		return status;
	}

	zassert(pthread_mutex_lock(&testlock));
	oldpathid = c->pathid;
	newpathid = fdst->current_pathid;
	zassert(pthread_mutex_unlock(&testlock));

	// generate new file name
	zassert(pthread_mutex_lock(&folderlock));
	c->owner = fdst;
	c->pathid = newpathid;
	hdd_generate_filename(fname,c);
	c->pathid = oldpathid;
	c->owner = fsrc;
	zassert(pthread_mutex_unlock(&folderlock));

	if (rename(tmp_filename,fname)<0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"move_chunk: file:%s->%s - rename error",tmp_filename,fname);
		close(new_fd);
		unlink(tmp_filename);
		hdd_chunk_release(c);
		free(tmp_filename);
		return MFS_ERROR_IO;
	}

	if (c->fd>=0) {
		close(c->fd);
		c->fd = new_fd;
	} else {
		close(new_fd);
	}
	c->hdrsize = new_hdrsize;

	// generate old file name
	hdd_generate_filename(fname,c);
	unlink(fname);
	free(tmp_filename);
	zassert(pthread_mutex_lock(&folderlock));
	fsrc->needrefresh = 1;
	fdst->needrefresh = 1;
	hdd_remove_chunk_from_folder(c,fsrc);
	hdd_add_chunk_to_folder(c,fdst);
	zassert(pthread_mutex_unlock(&folderlock));
	zassert(pthread_mutex_lock(&testlock));
	hdd_remove_chunk_from_test_chain(c,fsrc);
	c->pathid = newpathid;
	hdd_add_chunk_to_test_chain(c,fdst,1);
	zassert(pthread_mutex_unlock(&testlock));
	hdd_chunk_release(c);
	return MFS_STATUS_OK;
}

uint8_t hdd_is_rebalance_on(void) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	return __sync_fetch_and_or(&global_rebalance_is_on,0);
#else
	uint8_t res;
	zassert(pthread_mutex_lock(&dclock));
	res = global_rebalance_is_on;
	zassert(pthread_mutex_unlock(&dclock));
	return res;
#endif
}

int hdd_move(void *fsrcv,void *fdstv) {
	int status;
	folder *fsrc = (folder*)fsrcv;
	folder *fdst = (folder*)fdstv;
	status = hdd_int_move(fsrc,fdst);
	if (status!=MFS_STATUS_OK) {
		// in case of error - wait a little
		portable_usleep(1000);
	}
	return status;
}

static inline int hdd_server_can_be_used_for_replication(folder *f) {
	return (f->damaged==0 && f->toremove==REMOVING_NO && f->markforremoval==MFR_NO && f->scanstate==SCST_WORKING && f->total>0);
}

static inline int hdd_server_can_be_used_for_rebalancing(folder *f) {
	return (hdd_server_can_be_used_for_replication(f) && f->balancemode==REBALANCE_STD && f->total>REBALANCE_TOTAL_MIN);
}

static inline int hdd_server_can_be_used_as_a_source(folder *f) {
	return ((f->lmode==LMODE_NONE && f->chunkcount>100) || (f->lmode!=LMODE_NONE && f->knowncount>100));
}

static inline int hdd_rebalance_find_servers(folder **fsrc,folder **fdst,uint8_t *changed,uint8_t rebalance_is_on,uint8_t hsmode) {
	folder *f;
	double aboveminerr,belowminerr,err,expdist;
	double usage;
	double avgusage;
	double rebalancediff;
	uint32_t avgcount;
	uint32_t belowcnt;
	uint32_t abovecnt;
	uint64_t belowsum;
	uint64_t abovesum;
	uint8_t rebalance_servers;
	uint8_t waitcond;
	double monotonic_time;

	monotonic_time = 0.0;
	// check REBALANCE_FORCE_SRC and REBALANCE_FORCE_DST
	abovecnt = 0;
	belowcnt = 0;
	avgcount = 0;
	*changed = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (hdd_server_can_be_used_for_replication(f)) {
			if (f->needrefresh || rebalance_is_on) {
				hdd_refresh_usage(f);
				f->needrefresh = 0;
				if (monotonic_time==0.0) {
					monotonic_time = monotonic_seconds();
				}
				f->lastrefresh = monotonic_time;
				*changed = 1;
			}
			if (f->balancemode==REBALANCE_FORCE_SRC && f->chunkcount>0) {
				abovecnt++;
			} else if (f->balancemode==REBALANCE_FORCE_DST && f->wfrcount==0) {
				belowcnt++;
			} else {
				avgcount++;
			}
		}
		f->tmpbalancemode = REBALANCE_NONE;
	}
	rebalance_servers = 0;
	if ((abovecnt>0 && (belowcnt+avgcount)>0) || (belowcnt>0 && (abovecnt+avgcount)>0)) { // force data movement
		for (f=folderhead ; f ; f=f->next) {
			if (hdd_server_can_be_used_for_replication(f)) {
				usage = f->total-f->avail;
				usage /= f->total;
				if (abovecnt==0) {
					if (f->balancemode==REBALANCE_FORCE_DST && usage<REBALANCE_DST_MAX_USAGE && f->wfrcount==0) {
						f->tmpbalancemode = REBALANCE_DST;
						rebalance_servers |= 1;
					} else if (f->chunkcount>0) {
						f->tmpbalancemode = REBALANCE_SRC;
						rebalance_servers |= 2;
					}
				} else if (belowcnt==0) {
					if (f->balancemode==REBALANCE_FORCE_SRC && f->chunkcount>0) {
						f->tmpbalancemode = REBALANCE_SRC;
						rebalance_servers |= 2;
					} else if (usage<REBALANCE_DST_MAX_USAGE && f->wfrcount==0) {
						f->tmpbalancemode = REBALANCE_DST;
						rebalance_servers |= 1;
					}
				} else {
					if (f->balancemode==REBALANCE_FORCE_DST && usage<REBALANCE_DST_MAX_USAGE && f->wfrcount==0) {
						f->tmpbalancemode = REBALANCE_DST;
						rebalance_servers |= 1;
					} else if (f->balancemode==REBALANCE_FORCE_SRC && f->chunkcount>0) {
						f->tmpbalancemode = REBALANCE_SRC;
						rebalance_servers |= 2;
					}
				}
			}
		}
	} else { // usage rebalance (only servers without '<' and '>')
		rebalancediff = REBALANCE_DIFF_MAX;
		if (rebalance_is_on) {
			rebalancediff /= 2.0;
		}
		avgusage = 0.0;
		avgcount = 0;
		for (f=folderhead ; f ; f=f->next) {
			if (hdd_server_can_be_used_for_rebalancing(f) && hdd_server_can_be_used_as_a_source(f)) {
				usage = f->total-f->avail;
				usage /= f->total;
				avgusage += usage;
				avgcount++;
			}
		}
		if (avgcount>0) {
			avgusage /= avgcount;
			belowcnt = 0;
			belowsum = 0;
			abovecnt = 0;
			abovesum = 0;
			for (f=folderhead ; f ; f=f->next) {
				if (hdd_server_can_be_used_for_rebalancing(f)) {
					usage = f->total-f->avail;
					usage /= f->total;
					if (usage < avgusage - rebalancediff) {
						belowcnt++;
						belowsum+=f->total;
					} else if (usage > avgusage + rebalancediff && hdd_server_can_be_used_as_a_source(f)) {
						abovecnt++;
						abovesum+=f->total;
					}
				}
			}
			if (abovecnt>0 || belowcnt>0) {
				for (f=folderhead ; f ; f=f->next) {
					if (hdd_server_can_be_used_for_rebalancing(f)) {
						usage = f->total-f->avail;
						usage /= f->total;
						if ((((usage < avgusage - rebalancediff) && belowcnt>0) || ((usage <= avgusage + rebalancediff) && belowcnt==0)) && usage<REBALANCE_DST_MAX_USAGE && f->wfrcount==0) {
							f->tmpbalancemode = REBALANCE_DST;
							rebalance_servers |= 1;
						} else if ((((usage > avgusage + rebalancediff) && abovecnt>0) || ((usage >= avgusage - rebalancediff) && abovecnt==0)) && hdd_server_can_be_used_as_a_source(f)) {
							f->tmpbalancemode = REBALANCE_SRC;
							rebalance_servers |= 2;
						}
					}
				}
			}
		}
	}
	*fdst = NULL;
	*fsrc = NULL;
	if (rebalance_servers==3) {
		belowcnt = 0;
		belowsum = 0;
		abovecnt = 0;
		abovesum = 0;
		waitcond = 0;
		for (f=folderhead ; f ; f=f->next) {
			if (f->tmpbalancemode == REBALANCE_DST) {
				if (hsmode) {
					waitcond = 1;
					if (*fdst==NULL && f->rebalance_in_progress<HSRebalanceLimit) {
						*fdst = f;
					}
				} else {
					belowcnt++;
					belowsum+=f->total;
				}
			} else if (f->tmpbalancemode == REBALANCE_SRC) {
				abovecnt++;
				abovesum+=f->total;
			}
		}
		if (*fdst==NULL && waitcond && HSRebalanceLimit>0) { // limits reached on all servers - wait
			return -1;
		}
		aboveminerr = 0.0;
		belowminerr = 0.0;
		for (f=folderhead ; f ; f=f->next) {
			if (f->tmpbalancemode == REBALANCE_DST) {
				if (!hsmode) {
					f->write_dist++;
					if (f->write_first) {
						err = 1.0;
					} else {
						expdist = belowsum;
						expdist /= f->total;
						err = (expdist + f->write_corr) / f->write_dist;
					}
					if (*fdst==NULL || err<belowminerr) {
						belowminerr = err;
						*fdst = f;
					}
				}
			} else if (f->tmpbalancemode == REBALANCE_SRC) {
				f->read_dist++;
				if (f->read_first) {
					err = 1.0;
				} else {
					expdist = abovesum;
					expdist /= f->total;
					err = (expdist + f->read_corr) / f->read_dist;
				}
				if (*fsrc==NULL || err<aboveminerr) {
					aboveminerr = err;
					*fsrc = f;
				}
			}
		}
	}
	if (*fsrc!=NULL && *fdst!=NULL) {
		f = *fsrc;
		if (f->read_first) {
			f->read_first = 0;
		} else {
			expdist = abovesum;
			expdist /= f->total;
			f->read_corr += expdist - f->read_dist;
		}
		f->read_dist = 0;
		if (!hsmode) {
			f = *fdst;
			if (f->write_first) {
				f->write_first = 0;
			} else {
				expdist = belowsum;
				expdist /= f->total;
				f->write_corr += expdist - f->write_dist;
			}
			f->write_dist = 0;
		}
		return 1;
	}
	return 0;
}


void hdd_move_finished(uint8_t status,void *arg) {
	rebalance *r = (rebalance*)(arg);
	double monotonic_time;

	(void)status; // ignore status
	monotonic_time = monotonic_seconds();
	zassert(pthread_mutex_lock(&folderlock));
	r->fsrc->rebalance_in_progress--;
	r->fdst->rebalance_in_progress--;
	r->fdst->rebalance_last_usec = monotonic_time;
	zassert(pthread_cond_signal(&highspeed_cond));
	zassert(pthread_mutex_unlock(&folderlock));
	free(r);
	hdd_stats_move(1);
}

void* hdd_highspeed_rebalance_thread(void *arg) {
	rebalance *r;
	folder *fsrc,*fdst,*f;
	int sstatus;
	uint8_t changed;
	uint8_t rebalance_is_on;
	double rebalance_finished;
	double monotonic_time;

	rebalance_is_on = 0;
	rebalance_finished = 0;
	for (;;) {
		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));

		monotonic_time = monotonic_seconds();
		zassert(pthread_mutex_lock(&folderlock));
		if (folderactions==0 || (rebalance_finished + 60.0) > monotonic_time || HSRebalanceLimit==0) {
			zassert(pthread_mutex_unlock(&folderlock));
			if (HSRebalanceLimit==0) {
				rebalance_is_on = 0;
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_and(&global_rebalance_is_on,~2);
#else
				zassert(pthread_mutex_lock(&dclock));
				global_rebalance_is_on &= ~2;
				zassert(pthread_mutex_unlock(&dclock));
#endif
			}
			sleep(1);
			continue;
		}

		do {
			sstatus = hdd_rebalance_find_servers(&fsrc,&fdst,&changed,rebalance_is_on,1);
			if (sstatus<0) {
				zassert(pthread_mutex_lock(&termlock));
				if (term) {
					zassert(pthread_mutex_unlock(&termlock));
					zassert(pthread_mutex_unlock(&folderlock));
					return arg;
				}
				zassert(pthread_mutex_unlock(&termlock));
				zassert(pthread_cond_wait(&highspeed_cond,&folderlock));
			}
		} while (sstatus<0);

		if (sstatus==1) { // have servers
			fsrc->rebalance_in_progress++;
			fdst->rebalance_in_progress++;
			zassert(pthread_mutex_unlock(&folderlock));
			if (changed) {
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_or(&hddspacerecalc,1);
#else
				zassert(pthread_mutex_lock(&dclock));
				hddspacerecalc = 1;
				zassert(pthread_mutex_unlock(&dclock));
#endif
			}
			r = malloc(sizeof(rebalance));
			passert(r);
			r->fsrc = fsrc;
			r->fdst = fdst;
			job_chunk_move(hdd_move_finished,r,r->fsrc,r->fdst);
			rebalance_is_on = 1;
#ifdef HAVE___SYNC_FETCH_AND_OP
			__sync_fetch_and_or(&global_rebalance_is_on,2);
#else
			zassert(pthread_mutex_lock(&dclock));
			global_rebalance_is_on |= 2;
			zassert(pthread_mutex_unlock(&dclock));
#endif
		} else { // finish HS replication
			zassert(pthread_mutex_unlock(&folderlock));
			if (changed) {
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_or(&hddspacerecalc,1);
#else
				zassert(pthread_mutex_lock(&dclock));
				hddspacerecalc = 1;
				zassert(pthread_mutex_unlock(&dclock));
#endif
			}
			if (rebalance_is_on) {
				zassert(pthread_mutex_lock(&folderlock));
				for (f=folderhead ; f ; f=f->next) {
					f->read_dist = 0;
					f->read_first = 1;
					f->read_corr = 0.0;
				}
				zassert(pthread_mutex_unlock(&folderlock));
				rebalance_finished = monotonic_time;
			}
			rebalance_is_on = 0;
#ifdef HAVE___SYNC_FETCH_AND_OP
			__sync_fetch_and_and(&global_rebalance_is_on,~2);
#else
			zassert(pthread_mutex_lock(&dclock));
			global_rebalance_is_on &= ~2;
			zassert(pthread_mutex_unlock(&dclock));
#endif
			sleep(1);
		}
	}
}

void* hdd_rebalance_thread(void *arg) {
	folder *f,*fdst,*fsrc;
	uint8_t changed;
	uint8_t rebalance_is_on;
	double rebalance_finished;
	double monotonic_time;
	uint32_t perc;
	uint64_t st,en;

	ionice_low();

	rebalance_is_on = 0;
	rebalance_finished = 0;
	for (;;) {
		zassert(pthread_mutex_lock(&testlock));
		perc = HDDRebalancePerc;
		zassert(pthread_mutex_unlock(&testlock));
		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));

		monotonic_time = monotonic_seconds();
		zassert(pthread_mutex_lock(&folderlock));
		if (folderactions==0 || (rebalance_finished + 60.0) > monotonic_time || perc==0 || HSRebalanceLimit>0) {
			zassert(pthread_mutex_unlock(&folderlock));
			if (HSRebalanceLimit>0) {
				rebalance_is_on = 0;
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_and(&global_rebalance_is_on,~1);
#else
				zassert(pthread_mutex_lock(&dclock));
				global_rebalance_is_on &= ~1;
				zassert(pthread_mutex_unlock(&dclock));
#endif
			}
			sleep(1);
			continue;
		}

		if (hdd_rebalance_find_servers(&fsrc,&fdst,&changed,rebalance_is_on,0)) {
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"debug: move %s -> %s",fsrc->path,fdst->path);
			fsrc->rebalance_in_progress++;
			fdst->rebalance_in_progress++;
			zassert(pthread_mutex_unlock(&folderlock));
			if (changed) {
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_or(&hddspacerecalc,1);
#else
				zassert(pthread_mutex_lock(&dclock));
				hddspacerecalc = 1;
				zassert(pthread_mutex_unlock(&dclock));
#endif
			}
			st = monotonic_useconds();
			if (hdd_int_move(fsrc,fdst)!=MFS_STATUS_OK) {
				// in case of error - wait a little
				portable_usleep(1000);
			}
			en = monotonic_useconds();
			zassert(pthread_mutex_lock(&folderlock));
			fsrc->rebalance_in_progress--;
			fdst->rebalance_in_progress--;
			fdst->rebalance_last_usec = en;
			zassert(pthread_mutex_unlock(&folderlock));
			hdd_stats_move(0);
			rebalance_is_on = 1;
#ifdef HAVE___SYNC_FETCH_AND_OP
			__sync_fetch_and_or(&global_rebalance_is_on,1);
#else
			zassert(pthread_mutex_lock(&dclock));
			global_rebalance_is_on |= 1;
			zassert(pthread_mutex_unlock(&dclock));
#endif
			if (perc<100 && en>st) {
				en -= st;
				st = en;
				en *= 100;
				en /= perc;
				en -= st;
				if (en>0) {
					portable_usleep(en);
				}
			}
		} else {
			zassert(pthread_mutex_unlock(&folderlock));
			if (changed) {
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_or(&hddspacerecalc,1);
#else
				zassert(pthread_mutex_lock(&dclock));
				hddspacerecalc = 1;
				zassert(pthread_mutex_unlock(&dclock));
#endif
			}
			if (rebalance_is_on) {
				zassert(pthread_mutex_lock(&folderlock));
				for (f=folderhead ; f ; f=f->next) {
					f->read_dist = 0;
					f->read_first = 1;
					f->read_corr = 0.0;
				}
				zassert(pthread_mutex_unlock(&folderlock));
				rebalance_finished = monotonic_time;
			}
			rebalance_is_on = 0;
#ifdef HAVE___SYNC_FETCH_AND_OP
			__sync_fetch_and_and(&global_rebalance_is_on,~1);
#else
			zassert(pthread_mutex_lock(&dclock));
			global_rebalance_is_on &= ~1;
			zassert(pthread_mutex_unlock(&dclock));
#endif
			sleep(1);
		}
	}
	return arg;
}

void* hdd_tester_thread(void* arg) {
	folder *f,*tf;
	chunk *c;
	uint64_t chunkid;
	uint32_t version;
	uint64_t testbps;
	uint16_t blocks;
	uint8_t idlemode;
	uint64_t st,en,nextdelay,nextevent;
#ifdef HDD_TESTER_DEBUG
	FILE *fd;
	uint64_t global_st,global_bytes;

	fd = fopen("looplog.txt","a");
	global_st = monotonic_useconds();
	global_bytes = 0;
#endif

	ionice_low();

	for (;;) {
		st = monotonic_useconds();
#ifdef HDD_TESTER_DEBUG
		if (fd) {
			fprintf(fd,"%"PRIu64".%06u: loop begin\n",st/1000000,(unsigned int)(st%1000000));
		}
#endif
		chunkid = 0;
		version = 0;
		idlemode = 1;
		zassert(pthread_mutex_lock(&folderlock));
		zassert(pthread_mutex_lock(&hashlock));
		zassert(pthread_mutex_lock(&testlock));
		testbps = HDDTestMBPS*1024*1024;

#ifdef HDD_TESTER_DEBUG
		if (fd) {
			fprintf(fd,"folderactions:%u ; folderhead:%p ; HDDTestMBPS: %.3lf ; testbps:%"PRIu64"\n",folderactions,(void*)folderhead,HDDTestMBPS,testbps);
		}
#endif
		tf = NULL;
		if (folderactions!=0 && folderhead!=NULL && testbps>0) {
			idlemode = 0;
#ifdef HDD_TESTER_DEBUG
			if (fd) {
				for (f=folderhead ; f!=NULL ; f=f->next) {
					fprintf(fd,"%s: next event: %"PRIu64".%06u\n",f->path,(f->nexttest)/1000000,(unsigned int)(f->nexttest%1000000));
				}
			}
#endif
			for (f=folderhead ; f!=NULL && tf==NULL ; f=f->next) {
				if (f->damaged==0 && f->markforremoval==MFR_NO && f->toremove==REMOVING_NO && f->scanstate==SCST_WORKING) {
					if (f->nexttest<=st) {
						tf = f;
					}
				}
			}
			if (tf!=NULL) {
#ifdef HDD_TESTER_DEBUG
				if (fd) {
					fprintf(fd,"chosen path: %s\n",tf->path);
				}
#endif
				if (tf->testneededhead==NULL) {
					hdd_int_testloop(tf);
				}
				c = tf->testneededhead;
				if (c) {
					if (c->state==CH_AVAIL) {
						hdd_int_chunk_testmove(c);
						if (c->damaged==0) {
							chunkid = c->chunkid;
							version = c->version;
						}
						tf->testfailcnt=0;
					} else {
						tf->testfailcnt++;
						if (tf->testfailcnt==5) {
							hdd_int_chunk_testmove(c);
							tf->testfailcnt=0;
						}
					}
				}
#ifdef HDD_TESTER_DEBUG
			} else if (fd) {
				fprintf(fd,"path not found\n");
#endif
			}
		}

		zassert(pthread_mutex_unlock(&testlock));
		zassert(pthread_mutex_unlock(&hashlock));
		zassert(pthread_mutex_unlock(&folderlock));

		blocks = 0;
		if (chunkid>0) {
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"testing chunk: %s",path);
			(void)hdd_int_test(chunkid,version,&blocks); // ignore status here - hdd_int_test on error does everything itself
//			free(path);
		}

		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
#ifdef HDD_TESTER_DEBUG
			if (fd) {
				fclose(fd);
			}
#endif
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));

#ifdef HDD_TESTER_DEBUG
		global_bytes += blocks*65536;
#endif
		if (idlemode==0) {
			zassert(pthread_mutex_lock(&folderlock));
			if (testbps>0) {
				nextdelay = blocks;
				nextdelay *= UINT64_C(65536000000);
				nextdelay /= testbps;
			} else {
				nextdelay = 10000;
			}
#ifdef HDD_TESTER_DEBUG
			if (fd) {
				fprintf(fd,"blocks: %u ; nextdelay: %"PRIu64".%06u\n",blocks,nextdelay/1000000,(unsigned int)(nextdelay%1000000));
			}
#endif
			nextevent = 0;
			for (f=folderhead ; f!=NULL ; f=f->next) {
				if (f==tf) {
					f->nexttest = st+nextdelay;
#ifdef HDD_TESTER_DEBUG
					if (fd) {
						fprintf(fd,"%s: set next event to: %"PRIu64".%06u\n",f->path,f->nexttest/1000000,(unsigned int)(f->nexttest%1000000));
					}
#endif
				}
				if (nextevent==0 || f->nexttest < nextevent) {
					nextevent = f->nexttest;
				}
			}
#ifdef HDD_TESTER_DEBUG
			if (fd) {
				fprintf(fd,"next event: %"PRIu64".%06u\n",nextevent/1000000,(unsigned int)(nextevent%1000000));
			}
#endif
			zassert(pthread_mutex_unlock(&folderlock));
			en = monotonic_useconds();
#ifdef HDD_TESTER_DEBUG
			if (fd) {
				fprintf(fd,"%"PRIu64".%06u: loop end\n",en/1000000,(unsigned int)(en%1000000));
			}
#endif
			if (en+10000 < nextevent) {
				nextevent -= en;
#ifdef HDD_TESTER_DEBUG
				if (fd) {
					fprintf(fd,"next event after: %"PRIu64".%06u\n",nextevent/1000000,(unsigned int)(nextevent%1000000));
				}
#endif
				if (nextevent>500000) {
					nextevent = 500000;
				}
			} else {
				nextevent = 10000;
			}
#ifdef HDD_TESTER_DEBUG
			if (fd) {
				fprintf(fd,"average speed: %.3lf MB/s\n",global_bytes*1000000.0/((en-global_st)*1024.0*1024.0));
				fprintf(fd,"normal mode, sleep for: %"PRIu64".%06us\n",nextevent/1000000,(unsigned int)(nextevent%1000000));
				fflush(fd);
			}
#endif
			portable_usleep(nextevent);
		} else {
#ifdef HDD_TESTER_DEBUG
			if (fd) {
				fprintf(fd,"average speed: %.3lf MB/s\n",global_bytes*1000000.0/((st-global_st)*1024.0*1024.0));
				fprintf(fd,"idle mode, sleep for: 0.5s\n");
				fflush(fd);
			}
#endif
			portable_usleep(500000);
		}
	}
#ifdef HDD_TESTER_DEBUG
	if (fd) {
		fclose(fd);
	}
#endif
	return arg;
}

void* hdd_knowndiskusage_thread(void *arg) {
	folder *f;
	chunk *c;
	uint32_t i,j;

	for (;;) {
		zassert(pthread_mutex_lock(&folderlock));
		for (f=folderhead ; f ; f=f->next) {
			f->knowncount = f->knowncount_next;
			f->knowncount_next = 0;
			f->knowndiskusage = f->knowndiskusage_next;
			f->knowndiskusage_next = 0;
		}
		zassert(pthread_mutex_unlock(&folderlock));
		for (i=0 ; i<HASHSIZE ; i+=KNOWNBLOCKS_HASH_PER_CYCLE) {
			zassert(pthread_mutex_lock(&folderlock));
			zassert(pthread_mutex_lock(&hashlock));
//			zassert(pthread_mutex_lock(&testlock));
			for (j=i ; j<i+KNOWNBLOCKS_HASH_PER_CYCLE && j<HASHSIZE ; j++) {
				for (c=hashtab[j] ; c ; c=c->next) {
					if (c->state==CH_AVAIL && c->diskusage>0 && c->owner!=NULL) {
						c->owner->knowncount_next++;
						c->owner->knowndiskusage_next += c->diskusage;
					}
				}
			}
//			zassert(pthread_mutex_unlock(&testlock));
			zassert(pthread_mutex_unlock(&hashlock));
			zassert(pthread_mutex_unlock(&folderlock));

			portable_usleep(1000);

			zassert(pthread_mutex_lock(&termlock));
			if (term) {
				zassert(pthread_mutex_unlock(&termlock));
				return arg;
			}
			zassert(pthread_mutex_unlock(&termlock));
		}
	}
}

void hdd_testshuffle(folder *f) {
	uint32_t i,j,pos;
	chunk **csorttab,*c;

	zassert(pthread_mutex_lock(&testlock));

	if (f->testedcnt>0) {
		csorttab = malloc(sizeof(chunk*)*f->testedcnt);
		passert(csorttab);
		pos = 0;
		for (c=f->testedhead ; c ; c=c->testnext) {
			csorttab[pos++] = c;
			massert(pos<=f->testedcnt,"wrong test chunk count detected (tested chunks)");
		}
		massert(pos==f->testedcnt,"wrong test chunk count detected (tested chunks)");
		if (pos>1) {
			for (i=0 ; i<pos-1 ; i++) {
				j = i+rndu32_ranged(pos-i);
				if (j!=i) {
					c = csorttab[i];
					csorttab[i] = csorttab[j];
					csorttab[j] = c;
				}
			}
		}
	} else {
		csorttab = NULL;
	}
	f->testedhead = NULL;
	f->testedtail = &(f->testedhead);
	for (i=0 ; i<f->testedcnt ; i++) {
		c = csorttab[i];
		c->testnext = NULL;
		c->testprev = f->testedtail;
		*(c->testprev) = c;
		f->testedtail = &(c->testnext);
	}
	if (csorttab) {
		free(csorttab);
	}

	if (f->testneededcnt>0) {
		csorttab = malloc(sizeof(chunk*)*f->testneededcnt);
		passert(csorttab);
		pos = 0;
		for (c=f->testneededhead ; c ; c=c->testnext) {
			csorttab[pos++] = c;
			massert(pos<=f->testneededcnt,"wrong test chunk count detected (waiting to be tested chunks)");
		}
		massert(pos==f->testneededcnt,"wrong test chunk count detected (waiting to be tested chunks)");
		if (pos>1) {
			for (i=0 ; i<pos-1 ; i++) {
				j = i+rndu32_ranged(pos-i);
				if (j!=i) {
					c = csorttab[i];
					csorttab[i] = csorttab[j];
					csorttab[j] = c;
				}
			}
		}
	} else {
		csorttab = NULL;
	}
	f->testneededhead = NULL;
	f->testneededtail = &(f->testneededhead);
	for (i=0 ; i<f->testneededcnt ; i++) {
		c = csorttab[i];
		c->testnext = NULL;
		c->testprev = f->testneededtail;
		*(c->testprev) = c;
		f->testneededtail = &(c->testnext);
	}
	if (csorttab) {
		free(csorttab);
	}

	f->nexttest = 0;
	f->startlooptime = main_time();
	f->startlooptestneededcnt = f->testneededcnt;
	f->endlooptime = 0;
	f->testfailcnt = 0;

	zassert(pthread_mutex_unlock(&testlock));
}

/*
int hdd_testcompare(const void *a,const void *b) {
	chunk const* *aa = (chunk const* *)a;
	chunk const* *bb = (chunk const* *)b;
	return (**aa).testtime - (**bb).testtime;
}

void hdd_testsort(folder *f) {
	uint32_t i,chunksno;
	chunk **csorttab,*c;
	zassert(pthread_mutex_lock(&testlock));
	chunksno = 0;
	for (c=f->testhead ; c ; c=c->testnext) {
		chunksno++;
	}
	if (chunksno>0) {
		csorttab = malloc(sizeof(chunk*)*chunksno);
		passert(csorttab);
		chunksno = 0;
		for (c=f->testhead ; c ; c=c->testnext) {
			csorttab[chunksno++] = c;
		}
		qsort(csorttab,chunksno,sizeof(chunk*),hdd_testcompare);
	} else {
		csorttab = NULL;
	}
	f->testhead = NULL;
	f->testtail = &(f->testhead);
	for (i=0 ; i<chunksno ; i++) {
		c = csorttab[i];
		c->testnext = NULL;
		c->testprev = f->testtail;
		*(c->testprev) = c;
		f->testtail = &(c->testnext);
	}
	if (csorttab) {
		free(csorttab);
	}
	zassert(pthread_mutex_unlock(&testlock));
}
*/

/* initialization */

static inline int hdd_check_filename(const char *fname,uint64_t *chunkid,uint32_t *version) {
	uint64_t namechunkid;
	uint32_t nameversion;
	char ch;
	uint32_t i;

	if (strncmp(fname,"chunk_",6)!=0) {
		return -1;
	}
	namechunkid = 0;
	nameversion = 0;
	for (i=6 ; i<22 ; i++) {
		ch = fname[i];
		if (ch>='0' && ch<='9') {
			ch-='0';
		} else if (ch>='A' && ch<='F') {
			ch-='A'-10;
		} else {
			return -1;
		}
		namechunkid *= 16;
		namechunkid += ch;
	}
	if (fname[22]!='_') {
		return -1;
	}
	for (i=23 ; i<31 ; i++) {
		ch = fname[i];
		if (ch>='0' && ch<='9') {
			ch-='0';
		} else if (ch>='A' && ch<='F') {
			ch-='A'-10;
		} else {
			return -1;
		}
		nameversion *= 16;
		nameversion += ch;
	}
	if (strcmp(fname+31,".mfs")!=0) {
		return -1;
	}
	*chunkid = namechunkid;
	*version = nameversion;
	return 0;
}

void hdd_create_filename_for_path(char fname[PATH_MAX],const char *fpath,uint16_t pathid,uint64_t chunkid,uint32_t version) {
	if (pathid<256) {
		snprintf(fname,PATH_MAX,"%s/%02"PRIX16"/chunk_%016"PRIX64"_%08"PRIX32".mfs",fpath,pathid,chunkid,version);
	} else {
		snprintf(fname,PATH_MAX,"/dev/null");
	}
}

static inline void hdd_add_chunk(folder *f,uint16_t pathid,uint64_t chunkid,uint32_t version,uint16_t blocks,uint16_t hdrsize,uint8_t testedflag,uint32_t diskusage) {
	struct stat sb;
	folder *prevf,*currf;
	chunk *c;
	uint8_t validattr;
	char fname[PATH_MAX];

	if (diskusage>0 && blocks<MFSBLOCKSINCHUNK) {
		validattr = 1;
	} else if (f->lmode!=LMODE_NONE) {
		hdd_create_filename(fname,f->path,pathid,chunkid,version);
		if (stat(fname,&sb)<0) {
			if (f->markforremoval!=MFR_READONLY) {
				hdd_wfr_add(f,chunkid,version,pathid); // add file to 'wait for removal' queue
			}
			return;
		}
		if ((sb.st_mode & S_IFMT) != S_IFREG) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"%s: is not regular file",fname);
			return;
		}
		hdrsize = (sb.st_size - CHUNKCRCSIZE) & MFSBLOCKMASK;
		if (hdrsize!=OLDHDRSIZE && hdrsize!=NEWHDRSIZE) {
			if (f->markforremoval!=MFR_READONLY) {
				hdd_wfr_add(f,chunkid,version,pathid); // add file to 'wait for removal' queue
			}
			return;
		}
		if (sb.st_size<(hdrsize+CHUNKCRCSIZE) || sb.st_size>((uint32_t)hdrsize+CHUNKCRCSIZE+MFSCHUNKSIZE)) {
			if (f->markforremoval!=MFR_READONLY) {
				hdd_wfr_add(f,chunkid,version,pathid); // add file to 'wait for removal' queue
			}
			return;
		}
		blocks = (sb.st_size - hdrsize - CHUNKCRCSIZE) / MFSBLOCKSIZE;
		diskusage = (sb.st_blocks * 512U);
		validattr = 1;
	} else {
		hdrsize = 0;
		blocks = 0;
		diskusage = 0;
		validattr = 0;
	}
	prevf = NULL;
	currf = f;
	while (hdd_chunk_get(chunkid,&c,CHMODE_NEW_OR_EXISTING)==2) {} // here wait for locked chunks forever
	if (c==NULL) { // shouldn't happen
		hdd_create_filename(fname,f->path,pathid,chunkid,version);
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"can't create chunk record for file: %s",fname);
		return;
	}
	if (c->pathid!=0xFFFF) { // already have this chunk
		if (version <= c->version || c->crcrefcount) {	// new chunk is older than existing one or existing one is open
			if (f->markforremoval!=MFR_READONLY) { // this is R/W fs?
				hdd_wfr_add(f,chunkid,version,pathid); // add file to 'wait for removal' queue
			}
			currf = NULL;
		} else { // new chunk is better than existing one, so use it, and clear the existing
			prevf = c->owner;
			if (c->owner->markforremoval!=MFR_READONLY) { // current chunk is on R/W fs?
				hdd_wfr_add(c->owner,chunkid,version,pathid); // add file to 'wait for removal' queue
			}
			c->version = version;
			c->blocks = blocks;
			c->diskusage = diskusage;
			c->hdrsize = hdrsize;
			c->validattr = validattr;
			c->testtime = 0;
			zassert(pthread_mutex_lock(&testlock));
			hdd_remove_chunk_from_test_chain(c,prevf);
			c->pathid = pathid;
			hdd_add_chunk_to_test_chain(c,currf,testedflag);
			zassert(pthread_mutex_unlock(&testlock));
			hdd_report_changed_chunk(c->chunkid,c->version|((f->markforremoval!=MFR_NO)?0x80000000:0));
		}
	} else {
		c->pathid = pathid;
		c->version = version;
		c->blocks = blocks;
		c->diskusage = diskusage;
		c->hdrsize = hdrsize;
		c->validattr = validattr;
		c->testtime = 0;
		zassert(pthread_mutex_lock(&testlock));
		hdd_add_chunk_to_test_chain(c,currf,testedflag);
		zassert(pthread_mutex_unlock(&testlock));
		hdd_report_new_chunk(c->chunkid,c->version|((f->markforremoval!=MFR_NO)?0x80000000:0));
	}
	zassert(pthread_mutex_lock(&folderlock));
	if (prevf) {
		hdd_remove_chunk_from_folder(c,prevf);
		if (validattr) {
			if (prevf->knowncount_next>0 && prevf->knowndiskusage_next>=diskusage) {
				prevf->knowncount_next--;
				prevf->knowndiskusage_next-=diskusage;
			}
		}
	}
	if (currf) {
		hdd_add_chunk_to_folder(c,currf);
		if (validattr) {
			currf->knowncount_next++;
			currf->knowndiskusage_next+=diskusage;
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
	hdd_chunk_release(c);
}

static inline int hdd_folder_fastscan(folder *f,char *fullname,uint16_t plen) {
	struct stat sb;
	int fd;
	uint8_t *chunkbuff;
	const uint8_t *rptr,*rptrmem,*endbuff;
	uint16_t pleng;
	uint64_t chunkid;
	uint32_t version;
	uint16_t blocks;
	uint16_t pathid;
	uint16_t hdrsize;
	uint8_t testedflag;
	uint32_t diskusage;
	uint16_t subf;
	uint8_t mode;
	uint8_t rsize;
	int64_t chdbsize;
	uint64_t rcnt,records;
	uint8_t scanterm;
	uint8_t lastperc,currentperc;
	uint64_t lasttime,currenttime,begintime;
	time_t foldersmaxtime;

	begintime = monotonic_useconds();

	fullname[plen] = '\0';
	if (access(fullname,R_OK|W_OK|X_OK)<0) {
		return -1;
	}
	if (lstat(fullname,&sb)<0) {
		return -1;
	}
	foldersmaxtime = sb.st_mtime;
	for (subf=0 ; subf<256 ; subf++) {
		fullname[plen] = "0123456789ABCDEF"[(subf>>4)&0xF];
		fullname[plen+1] = "0123456789ABCDEF"[subf&0xF];
		fullname[plen+2] = '\0';
		if (access(fullname,R_OK|W_OK|X_OK)<0) {
			return -1;
		}
		if (lstat(fullname,&sb)<0) {
			return -1;
		}
		if (sb.st_atime > foldersmaxtime) {
			foldersmaxtime = sb.st_atime;
		}
		if (sb.st_mtime > foldersmaxtime) {
			foldersmaxtime = sb.st_mtime;
		}
	}

	memcpy(fullname+plen,".chunkdb",8);
	fullname[plen+8] = '\0';

	fd = open(fullname,O_RDONLY);
	if (fd<0) {
		return -1;
	}
	if (fstat(fd,&sb)<0) {
		close(fd);
		return -1;
	}
	if (sb.st_mtime < foldersmaxtime) { // somebody touched data subfolders, so '.chunkdb' might be not valid
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"scanning folder %s: at least one of data subfolders has more recent atime/mtime than '.chunkdb' - fallback to standard scan",f->path);
		close(fd);
		return -1;
	}
//	if (sb.st_size<12 || (sb.st_size-12)%6!=0) {
//		close(fd);
//		return -1;
//	}
	chdbsize = sb.st_size;
	chunkbuff = malloc(chdbsize);
	if (chunkbuff==NULL) {
		close(fd);
		return -1;
	}
	if (read(fd,chunkbuff,chdbsize)!=chdbsize) {
		free(chunkbuff);
		close(fd);
		return -1;
	}
	close(fd);
	if (unlink(fullname)<0) {
		free(chunkbuff);
		return -1;
	}

	rptr = chunkbuff;
	endbuff = rptr+chdbsize;

	if (memcmp(rptr,"MFS CHUNKDB",11)!=0 && rptr[11]!='1' && rptr[11]!='2' && rptr[11]!='3' && rptr[11]!='4') {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"scanning folder %s: wrong header in .chunkdb - fallback to standard scan",f->path);
		free(chunkbuff);
		return -1;
	}
	mode = rptr[11]-'0';
	if (mode==1) {
		rsize = 16;
	} else if (mode==2) {
		rsize = 18;
	} else if (mode==3) {
		rsize = 19;
	} else { // mode==4
		rsize = CHUNKDB_REC_SIZE;
	}
	rptr+=12;

	pleng = get16bit(&rptr);
	if (rptr+pleng>endbuff || pleng != plen || memcmp(rptr,fullname,pleng)!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"scanning folder %s: wrong path in .chunkdb - fallback to standard scan",f->path);
		free(chunkbuff);
		return -1;
	}
	rptr += pleng;
	rptrmem = rptr;
	chunkid = 0;
	version = 0;
	blocks = 0;
	diskusage = 0;
	hdrsize = OLDHDRSIZE;
	pathid = 0xFFFF;
	testedflag = 0;
	records = 0;

	while (rptr+rsize<=endbuff) {
		chunkid = get64bit(&rptr);
		version = get32bit(&rptr);
		blocks = get16bit(&rptr);
		if (mode>=2) {
			hdrsize = get16bit(&rptr);
		}
		pathid = get16bit(&rptr);
		if (pathid>255) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"scanning folder %s: data malformed in .chunkdb - fallback to standard scan",f->path);
			free(chunkbuff);
			return -1;
		}
		if (mode>=3) {
			testedflag = get8bit(&rptr);
		}
		if (mode>=4) {
			diskusage = get32bit(&rptr);
		}
		if (chunkid==0) {
			break;
		}
		records++;
	}
	if (rptr!=endbuff || chunkid!=0 || version!=0 || blocks!=0 || pathid!=0 || testedflag!=0 || diskusage!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"scanning folder %s: data malformed in .chunkdb - fallback to standard scan",f->path);
		free(chunkbuff);
		return -1;
	}

	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"scanning folder %s: valid .chunkdb found - full scan not needed",f->path);

	rptr = rptrmem;
	rcnt = 0;
	scanterm = 0;
	lastperc = 0;
	lasttime = monotonic_useconds();

	while (scanterm==0) {
		chunkid = get64bit(&rptr);
		version = get32bit(&rptr);
		blocks = get16bit(&rptr);
		if (mode>=2) {
			hdrsize = get16bit(&rptr);
		}
		pathid = get16bit(&rptr);
		if (mode>=3) {
			testedflag = get8bit(&rptr);
		}
		if (mode>=4) {
			diskusage = get32bit(&rptr);
		}
		if (chunkid==0 && version==0 && blocks==0 && pathid==0 && testedflag==0 && diskusage==0) {
			break;
		}
		hdd_add_chunk(f,pathid,chunkid,version,blocks,hdrsize,testedflag,diskusage);
		rcnt++;
		if ((rcnt%10000)==0) {
			currentperc = (rcnt*100)/records;
			currenttime = monotonic_useconds();
			zassert(pthread_mutex_lock(&folderlock));
			if (f->scanstate==SCST_BGJOBTERMINATE) {
				scanterm = 1;
			}
			f->scanprogress = currentperc;
			zassert(pthread_mutex_unlock(&folderlock));
			if (currentperc>lastperc && currenttime>lasttime+1000000) {
				lastperc=currentperc;
				lasttime=currenttime;
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_or(&hddspacerecalc,1);
#else
				zassert(pthread_mutex_lock(&dclock));
				hddspacerecalc = 1; // report chunk count to master
				zassert(pthread_mutex_unlock(&dclock));
#endif
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"scanning folder %s: %"PRIu8"%% (%"PRIu64"s)",f->path,lastperc,(currenttime-begintime)/1000000);
			}
		}
	}

	free(chunkbuff);
	return 0;
}

#define CTABSIZE 100

typedef struct ctab {
	chunk *chunks[CTABSIZE];
	uint32_t elems;
	struct ctab *next;
} ctab;

void* hdd_folder_update_attr(void *arg) {
	folder *f = (folder*)arg;
	ctab *chead,**ctail,*ccurr;
	chunk *c;
	uint8_t lastperc,currentperc;
	uint64_t lasttime,currenttime;
	uint64_t begintime;
	uint64_t perctmp;
	uint32_t p,i,j;
	uint8_t scanterm;

	begintime = monotonic_useconds();
	scanterm = 0;

	// update all attributes
	lastperc = 0;
	lasttime = monotonic_useconds();

	for (p=0 ; p<256 && scanterm==0 ; p++) {
		for (i=0 ; i<HASHSIZE && scanterm==0 ; i+=50000) {

			chead = NULL;
			ctail = &chead;
			ccurr = NULL;

			perctmp = p;
			perctmp *= HASHSIZE;
			perctmp += i;
			perctmp *= 100;
			perctmp /= HASHSIZE;
			perctmp /= 256;
			currentperc = perctmp;

			zassert(pthread_mutex_lock(&folderlock));
			zassert(pthread_mutex_lock(&hashlock));

			f->scanprogress = currentperc;

			for (j=i ; j<i+50000 && j<HASHSIZE && f->scanstate!=SCST_BGJOBTERMINATE ; j++) {
				for (c=hashtab[j] ; c ; c=c->next) {
					if (c->state==CH_AVAIL && c->diskusage==0 && c->pathid==p && c->owner==f) {
						c->state = CH_LOCKED;
						if (ccurr==NULL || ccurr->elems==CTABSIZE) {
							ccurr = malloc(sizeof(ctab));
							ccurr->chunks[0] = c;
							ccurr->elems = 1;
							ccurr->next = NULL;
							*ctail = ccurr;
							ctail = &(ccurr->next);
						} else {
							ccurr->chunks[ccurr->elems] = c;
							ccurr->elems++;
						}
					}
				}
			}

			zassert(pthread_mutex_unlock(&hashlock));
			zassert(pthread_mutex_unlock(&folderlock));

			for (ccurr = chead ; ccurr != NULL ; ccurr=ccurr->next) {
				for (j=0 ; j<ccurr->elems ; j++) {
					c = ccurr->chunks[j];
					if (hdd_chunk_getattr(c,0)<0) {
						hdd_error_occured(c,1);
					}
				}
			}

			zassert(pthread_mutex_lock(&folderlock));
			zassert(pthread_mutex_lock(&hashlock));

			for (ccurr = chead ; ccurr != NULL ; ccurr=ccurr->next) {
				for (j=0 ; j<ccurr->elems ; j++) {
					c = ccurr->chunks[j];
					c->state = CH_AVAIL;
					if (c->ccond) {
						zassert(pthread_cond_signal(&(c->ccond->cond)));
					}
				}
			}

			if (f->scanstate==SCST_BGJOBTERMINATE) {
				scanterm = 1;
			}

			zassert(pthread_mutex_unlock(&hashlock));
			zassert(pthread_mutex_unlock(&folderlock));

			while (chead!=NULL) {
				ccurr = chead->next;
				free(chead);
				chead = ccurr;
			}

			currenttime = monotonic_useconds();

			if (currentperc>lastperc && currenttime>lasttime+1000000) {
				lastperc=currentperc;
				lasttime=currenttime;
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_or(&hddspacerecalc,1);
#else
				zassert(pthread_mutex_lock(&dclock));
				hddspacerecalc = 1; // report chunk count to master
				zassert(pthread_mutex_unlock(&dclock));
#endif
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"updating folder %s: %"PRIu8"%% (%"PRIu64"s)",f->path,lastperc,(currenttime-begintime)/1000000);
			}
		}
	}


	zassert(pthread_mutex_lock(&folderlock));
	if (f->scanstate==SCST_BGJOBTERMINATE) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"updating folder %s: interrupted",f->path);
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"updating folder %s: complete (%"PRIu64"s)",f->path,(monotonic_useconds()-begintime)/1000000);
	}
	f->scanstate = SCST_BGJOBFINISHED;
	f->scanprogress = 100;
	zassert(pthread_mutex_unlock(&folderlock));
	return NULL;
}

void* hdd_folder_scan(void *arg) {
	folder *f = (folder*)arg;
	DIR *dd;
	struct dirent *de;
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
	struct dirent *destorage;
#endif
	uint16_t subf;
	char *fullname,*oldfullname;
	uint16_t plen,oldplen;
	uint64_t namechunkid;
	uint32_t nameversion;
	uint32_t tcheckcnt;
	uint8_t scanterm,markforremoval;
//	uint8_t progressreportmode;
	uint8_t lastperc,currentperc;
	uint64_t lasttime,currenttime,begintime;

	begintime = monotonic_useconds();

	zassert(pthread_mutex_lock(&folderlock));
	markforremoval = f->markforremoval;
	hdd_refresh_usage(f);
//	progressreportmode = wait_for_scan;
	zassert(pthread_mutex_unlock(&folderlock));

	plen = strlen(f->path);
	oldplen = plen;

	fullname = malloc(plen+39);
	passert(fullname);

	memcpy(fullname,f->path,plen);
	fullname[plen] = '\0';
	if (markforremoval==MFR_NO) {
		mkdir(fullname,0755);
	}

	if (hdd_folder_fastscan(f,fullname,plen)<0) {

		fullname[plen++]='_';
		fullname[plen++]='_';
		fullname[plen++]='/';
		fullname[plen]='\0';

		/* size of name added to size of structure because on some os'es d_name has size of 1 byte */
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
		destorage = (struct dirent*)malloc(sizeof(struct dirent)+pathconf(f->path,_PC_NAME_MAX)+1);
		passert(destorage);
#endif

		scanterm = 0;

#ifdef HAVE___SYNC_FETCH_AND_OP
		__sync_fetch_and_or(&hddspacerecalc,1);
#else
		zassert(pthread_mutex_lock(&dclock));
		hddspacerecalc = 1;
		zassert(pthread_mutex_unlock(&dclock));
#endif

		if (markforremoval==MFR_NO) {
			for (subf=0 ; subf<256 ; subf++) {
				fullname[plen-3]="0123456789ABCDEF"[(subf>>4)&0xF];
				fullname[plen-2]="0123456789ABCDEF"[subf&0xF];
				mkdir(fullname,0755);
				if (access(fullname,R_OK|W_OK|X_OK)<0) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"disk: %s ; %s: access error (%s) - mark it as damaged",f->path,fullname,strerr(errno));
					f->damaged = 1;
					f->toremove = REMOVING_START; // pro forma only - disk shouldn't have any chunks
					scanterm = 1;
				}
			}

			if (scanterm==0) {
				/* move chunks from "X/name" to "XX/name" */

				oldfullname = malloc(oldplen+38);
				passert(oldfullname);
				memcpy(oldfullname,f->path,oldplen);
				oldfullname[oldplen++]='_';
				oldfullname[oldplen++]='/';
				oldfullname[oldplen]='\0';

				for (subf=0 ; subf<16 ; subf++) {
					oldfullname[oldplen-2]="0123456789ABCDEF"[subf];
					oldfullname[oldplen]='\0';
					dd = opendir(oldfullname);
					if (dd==NULL) {
						continue;
					}
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
					while (readdir_r(dd,destorage,&de)==0 && de!=NULL) {
#else
					while ((de = readdir(dd)) != NULL) {
#endif
						if (hdd_check_filename(de->d_name,&namechunkid,&nameversion)<0) {
							continue;
						}
						memcpy(oldfullname+oldplen,de->d_name,36);
						memcpy(fullname+plen,de->d_name,36);
						fullname[plen-3]="0123456789ABCDEF"[(namechunkid>>4)&0xF];
						fullname[plen-2]="0123456789ABCDEF"[namechunkid&0xF];
						rename(oldfullname,fullname);
					}
					oldfullname[oldplen]='\0';
					rmdir(oldfullname);
					closedir(dd);
				}
				free(oldfullname);
			}
		}

		/* scan new file names */

		tcheckcnt = 0;
		lastperc = 0;
		lasttime = monotonic_useconds();

		for (subf=0 ; subf<256 && scanterm==0 ; subf++) {
			fullname[plen-3]="0123456789ABCDEF"[(subf>>4)&0xF];
			fullname[plen-2]="0123456789ABCDEF"[subf&0xF];
			fullname[plen]='\0';
	//		mkdir(fullname,0755);
			dd = opendir(fullname);
			if (dd) {
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
				while (readdir_r(dd,destorage,&de)==0 && de!=NULL && scanterm==0) {
#else
				while ((de = readdir(dd)) != NULL && scanterm==0) {
#endif
	//#warning debug
	//				portable_usleep(100000);
	//
					if (hdd_check_filename(de->d_name,&namechunkid,&nameversion)<0) {
						continue;
					}
//					memcpy(fullname+plen,de->d_name,36);
					hdd_add_chunk(f,subf,namechunkid,nameversion,0xFFFF,0,0,0);
					tcheckcnt++;
					if (tcheckcnt>=1000) {
						zassert(pthread_mutex_lock(&folderlock));
						if (f->scanstate==SCST_BGJOBTERMINATE) {
							scanterm = 1;
						}
						zassert(pthread_mutex_unlock(&folderlock));
						// portable_usleep(100000); - slow down scanning (also change 1000 in 'if' to something much smaller) - for tests
						tcheckcnt = 0;
					}
				}
				closedir(dd);
			}
			currenttime = monotonic_useconds();
			currentperc = (((subf+1)*100.0)/256.0);
			if (currentperc>lastperc && currenttime>lasttime+1000000) {
				lastperc=currentperc;
				lasttime=currenttime;
				zassert(pthread_mutex_lock(&folderlock));
				f->scanprogress = currentperc;
				zassert(pthread_mutex_unlock(&folderlock));
#ifdef HAVE___SYNC_FETCH_AND_OP
				__sync_fetch_and_or(&hddspacerecalc,1);
#else
				zassert(pthread_mutex_lock(&dclock));
				hddspacerecalc = 1; // report chunk count to master
				zassert(pthread_mutex_unlock(&dclock));
#endif
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"scanning folder %s: %"PRIu8"%% (%"PRIu64"s)",f->path,lastperc,(currenttime-begintime)/10000000);
			}
		}
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
		free(destorage);
#endif
	}
	free(fullname);
//	fprintf(stderr,"hdd space manager: %s: %"PRIu32" chunks found\n",f->path,f->chunkcount);

	zassert(pthread_mutex_lock(&folderlock));
	hdd_testshuffle(f);
	if (f->scanstate==SCST_BGJOBTERMINATE) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"scanning folder %s: interrupted",f->path);
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"scanning folder %s: complete (%"PRIu64"s)",f->path,(monotonic_useconds()-begintime)/1000000);
	}
	f->scanstate = SCST_BGJOBFINISHED;
	f->scanprogress = 100;
	zassert(pthread_mutex_unlock(&folderlock));
	return NULL;
}

void hdd_setmetaid(uint64_t metaid) { // metaid has been verified with master - write .metaid files
	folder *f;
	char *metaidfname;
	int mfd;
	uint32_t l;

	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		l = strlen(f->path);
		metaidfname = (char*)malloc(l+8);
		passert(metaidfname);
		memcpy(metaidfname,f->path,l);
		memcpy(metaidfname+l,".metaid",8);
		mfd = open(metaidfname,O_RDWR|O_CREAT|O_TRUNC,0640);
		if (mfd>=0) {
			uint8_t buff[8];
			uint8_t *wptr;
			wptr = buff;
			put64bit(&wptr,metaid);
			if (write(mfd,buff,8)!=8) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error writing meta id file");
			}
			close(mfd);
		} else {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error writing meta id file");
		}
		free(metaidfname);
	}
	zassert(pthread_mutex_unlock(&folderlock));
}

void* hdd_folders_thread(void *arg) {
	for (;;) {
		hdd_check_folders();
		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));
		sleep(1);
	}
	return arg;
}

void* hdd_delayed_thread(void *arg) {
	for (;;) {
		hdd_delayed_ops();
		zassert(pthread_mutex_lock(&termlock));
		if (term) {
			zassert(pthread_mutex_unlock(&termlock));
			return arg;
		}
		zassert(pthread_mutex_unlock(&termlock));
		portable_usleep(DELAYEDUSTEP);
	}
	return arg;
}

#ifndef PRESERVE_BLOCK
void hdd_blockbuffer_free(void *addr) {
	myunalloc(addr,MFSBLOCKSIZE);
}
#endif

void hdd_clear_cfglines(void) {
	cfgline *cl;
	while ((cl=cfglinehead)!=NULL) {
		free(cl->path);
		cfglinehead = cl->next;
		free(cl);
	}
	cfglinehead = NULL;
}

void hdd_term(void) {
	uint32_t i,m,l;
	folder *f,*fn;
	chunk *c,*cn;
	dopchunk *dc,*dcn;
	cntcond *cc,*ccn;
	lostchunk *lc,*lcn;
	newchunk *nc,*ncn;
	chgchunk *xc,*xcn;
	damagedchunk *dmc,*dmcn;
	waitforremoval *wfr;

	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"terminating aux threads");
	zassert(pthread_mutex_lock(&termlock));
	i = term; // if term is non zero here then it means that threads have not been started, so do not join with them
	term = 1;
	zassert(pthread_mutex_unlock(&termlock));
	if (i==0) {
		zassert(pthread_join(knowndiskusagethread,NULL));
		zassert(pthread_join(testerthread,NULL));
		zassert(pthread_join(foldersthread,NULL));
		zassert(pthread_join(hsrebalancethread,NULL));
		zassert(pthread_join(rebalancethread,NULL));
		zassert(pthread_join(delayedthread,NULL));
	}
	zassert(pthread_mutex_lock(&folderlock));
	i = 0;
	m = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->scanstate==SCST_SCANJOBINPROGRESS || f->scanstate==SCST_ATTRJOBINPROGRESS) {
			f->scanstate = SCST_BGJOBTERMINATE;
		}
		if (f->scanstate==SCST_BGJOBTERMINATE || f->scanstate==SCST_BGJOBFINISHED) {
			i++;
		}
		if (f->rebalance_in_progress>0) {
			m++;
		}
		if (f->scanstate==SCST_WORKING && f->toremove==REMOVING_NO) {
			hdd_folder_dump_chunkdb_begin(f);
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"waiting for scanning threads (%"PRIu32")",i);
	l = 0;
	while ((i>0 || m>0) && l<1000) {
		if ((l%100)==0) {
			if (i>0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"waiting for scanning threads (%"PRIu32")",i);
			}
			if (m>0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"waiting for rebalance jobs (%"PRIu32")",m);
			}
		}
		portable_usleep(10000); // not very elegant solution.
		zassert(pthread_mutex_lock(&folderlock));
		m = 0;
		for (f=folderhead ; f ; f=f->next) {
			if (f->scanstate==SCST_BGJOBFINISHED) {
				zassert(pthread_join(f->scanthread,NULL));
				f->scanstate = SCST_BGJOBTERMINATE;	// any state - to prevent calling pthread_join again
				i--;
			}
			if (f->rebalance_in_progress>0) {
				m++;
			}
		}
		zassert(pthread_mutex_unlock(&folderlock));
		l++;
	}
	if (i>0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't wait longer for scanning threads (%"PRIu32")",i);
	}
	if (m>0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't wait longer for rebalance jobs (%"PRIu32")",m);
	}
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"closing chunks");
	for (i=0 ; i<HASHSIZE ; i++) {
		for (c=hashtab[i] ; c ; c=cn) {
			cn = c->next;
			if (c->owner!=NULL && c->state!=CH_DELETED) {
				hdd_folder_dump_chunkdb_chunk(c->owner,c);
			}
			if (c->state==CH_AVAIL && c->owner!=NULL) {
				if (c->fd>=0) {
					if (c->crcchanged) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_term: CRC not flushed - writing now");
						chunk_writecrc(c,0);
					}
					close(c->fd);
					hdd_open_files_handle(OF_AFTER_CLOSE);
				}
				if (c->crc!=NULL) {
					myunalloc((void*)(c->crc),CHUNKCRCSIZE);
				}
#ifdef PRESERVE_BLOCK
				if (c->block!=NULL) {
					myunalloc((void*)(c->block),MFSBLOCKSIZE);
				}
#endif /* PRESERVE_BLOCK */
				free(c);
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hdd_term: locked chunk !!!");
			}
		}
	}
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"freeing data structures");
	hdd_clear_cfglines();
	for (f=folderhead ; f ; f=fn) {
		fn = f->next;
		if (f->scanstate==SCST_WORKING && f->toremove==REMOVING_NO) {
			hdd_folder_dump_chunkdb_end(f);
		}
		if (f->lfd>=0) {
			close(f->lfd);
		}
		if (f->chunktab) {
			free(f->chunktab);
		}
		free(f->path);
		while (f->wfrchunks) {
			wfr = f->wfrchunks;
			f->wfrchunks = wfr->foldernext;
			free((void*)wfr);
		}
		free(f);
	}
	for (i=0 ; i<DHASHSIZE ; i++) {
		for (dc=dophashtab[i] ; dc ; dc=dcn) {
			dcn = dc->next;
			free(dc);
		}
	}
	for (dc=newdopchunks ; dc ; dc=dcn) {
		dcn = dc->next;
		free(dc);
	}
	for (cc=cclist ; cc ; cc=ccn) {
		ccn = cc->next;
		if (cc->wcnt) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"hddspacemgr (atexit): used cond !!!");
		} else {
			zassert(pthread_cond_destroy(&(cc->cond)));
		}
		free(cc);
	}
	for (xc=chgchunks ; xc ; xc=xcn) {
		xcn = xc->next;
		free(xc);
	}
	for (nc=newchunks ; nc ; nc=ncn) {
		ncn = nc->next;
		free(nc);
	}
	for (lc=lostchunks ; lc ; lc=lcn) {
		lcn = lc->next;
		free(lc);
	}
	for (lc=nonexistentchunks ; lc ; lc=lcn) {
		lcn = lc->next;
		free(lc);
	}
	for (dmc=damagedchunks ; dmc ; dmc=dmcn) {
		dmcn = dmc->next;
		free(dmc);
	}
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"hddspacemgr: terminating done");
}

int hdd_size_parse(const char *str,double *ret) {
	const char *endptr;

	*ret = sizestrtod(str,&endptr);

	if (endptr[0]=='\0' || (endptr[0]=='B' && endptr[1]=='\0')) {
		return 1;
	}
	return -1;
}

int hdd_percent_parse(const char *str,double *ret) {
	char *endptr;

	*ret = strtod(str,&endptr)/100.0;

	if (endptr[0]=='%' && endptr[1]=='\0' && *ret>0.0 && *ret<=1.0) {
		return 1;
	}
	return -1;
}

int hdd_size_parse_u64(const char *str,uint64_t *ret) {
	double val;

	if (hdd_size_parse(str,&val)<0) {
		return -1;
	}

	if (val>18446744073709551615.0) {
		return -2;
	}

	*ret = round(val);
	return 1;
}

int hdd_parseline(char *hddcfgline) {
	uint32_t l,p;
	int lfd,mfr,bm,is;
	int mfd;
	char *pptr;
	char *lockfname;
	char *metaidfname;
	struct stat sb;
	cfgline *cl;
	folder *f;
	uint8_t lockneeded;
	uint8_t cannotbeused;
	uint8_t lmode;
	double ldata;
	uint8_t pmode;
	uint64_t mainmetaid,metaid;

	if (hddcfgline[0]=='#') {
		return 0;
	}
	l = strlen(hddcfgline);
	while (l>0 && (hddcfgline[l-1]=='\r' || hddcfgline[l-1]=='\n' || hddcfgline[l-1]==' ' || hddcfgline[l-1]=='\t')) {
		l--;
	}
	if (l==0) {
		return 0;
	}
	hddcfgline[l]='\0';
	p = l;
	while (p>0 && hddcfgline[p-1]!=' ' && hddcfgline[p-1]!='\t') {
		p--;
	}
	lmode = LMODE_NONE;
	pmode = 0;
	ldata = 0.0;
	if (p>0) {
		if (hddcfgline[l-1]=='%') {
			pmode = 1;
		} else {
			pmode = 0;
		}
		if (hddcfgline[p]=='=') {
			if (hddcfgline[p+1]=='-') {
				if (pmode) {
					if (hdd_percent_parse(hddcfgline+p+2,&ldata)>=0) {
						lmode = LMODE_SHARED_NEG_PERCENT;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"percent parse error, data: %s",hddcfgline+p);
					}
				} else {
					if (hdd_size_parse(hddcfgline+p+2,&ldata)>=0) {
						lmode = LMODE_SHARED_NEG_CONST;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"size parse error, data: %s",hddcfgline+p);
					}
				}
			} else {
				if (pmode) {
					if (hdd_percent_parse(hddcfgline+p+1,&ldata)>=0) {
						lmode = LMODE_SHARED_POS_PERCENT;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"percent parse error, data: %s",hddcfgline+p);
					}
				} else {
					if (hddcfgline[p+1]=='\0') {
						lmode = LMODE_SHARED;
					} else {
						if (hdd_size_parse(hddcfgline+p+1,&ldata)>=0) {
							lmode = LMODE_SHARED_POS_CONST;
						} else {
							mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"size parse error, data: %s",hddcfgline+p);
						}
					}
				}
			}
		} else {
			if (hddcfgline[p]=='-') {
				if (pmode) {
					if (hdd_percent_parse(hddcfgline+p+1,&ldata)>=0) {
						lmode = LMODE_LIMIT_TOTAL_NEG_PERCENT;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"percent parse error, data: %s",hddcfgline+p);
					}
				} else {
					if (hdd_size_parse(hddcfgline+p+1,&ldata)>=0) {
						lmode = LMODE_LIMIT_TOTAL_NEG_CONST;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"size parse error, data: %s",hddcfgline+p);
					}
				}
			} else if ((hddcfgline[p]>='0' && hddcfgline[p]<='9') || hddcfgline[p]=='.') {
				if (pmode) {
					if (hdd_percent_parse(hddcfgline+p,&ldata)>=0) {
						lmode = LMODE_LIMIT_TOTAL_POS_PERCENT;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"percent parse error, data: %s",hddcfgline+p);
					}
				} else {
					if (hdd_size_parse(hddcfgline+p,&ldata)>=0) {
						lmode = LMODE_LIMIT_TOTAL_POS_CONST;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"size parse error, data: %s",hddcfgline+p);
					}
				}
			}
		}
		if (lmode!=LMODE_NONE) {
			l = p;
			while (l>0 && (hddcfgline[l-1]==' ' || hddcfgline[l-1]=='\t')) {
				l--;
			}
			if (l==0) {
				return 0;
			}
		}
	}
	if (hddcfgline[l-1]!='/') {
		hddcfgline[l]='/';
		hddcfgline[l+1]='\0';
		l++;
	} else {
		hddcfgline[l]='\0';
	}
	mfr = MFR_NO;
	bm = REBALANCE_STD;
	is = 0;
	pptr = hddcfgline;
	while (1) {
		if (*pptr == '*') {
			mfr = MFR_YES;
		} else if (*pptr == '~') {
			is = 1;
		} else if (*pptr == '>') {
			bm = REBALANCE_FORCE_DST;
		} else if (*pptr == '<') {
			bm = REBALANCE_FORCE_SRC;
		} else {
			break;
		}
		l--;
		pptr++;
	}

	if (l>PATH_MAX-100) {
		if (l>250) { // should be always true, but the code assumes that the path has at least 100 characters and makes sense only when it is longer than 200 characters
			char *ptail;
			ptail = pptr+(l-100);
			pptr[100]='\0';
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: drive '%s...%s' - path too long",pptr,ptail);
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: drive '%s' - path too long",pptr);
		}
		return -1;
	}

	zassert(pthread_mutex_lock(&folderlock));
	lockneeded = 1;
	cannotbeused = 0;
	for (f=folderhead ; f && lockneeded ; f=f->next) {
		if (strcmp(f->path,pptr)==0) {
			if (f->toremove==REMOVING_INPROGRESS) {
				cannotbeused = 1;
			} else {
				lockneeded = 0;
			}
		}
	}
	zassert(pthread_mutex_unlock(&folderlock));

	if (cannotbeused) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: drive '%s' is being removed and can not be added again while removing is in progress - try it again in couple of seconds",pptr);
		return -1;
	}

	if (lmode==LMODE_LIMIT_TOTAL_NEG_CONST || lmode==LMODE_SHARED_NEG_CONST) { // sanity checks
		if (ldata<0x4000000) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: limit on '%s' < chunk size - leaving so small space on hdd is not recommended",pptr);
		} else {
			struct statvfs fsinfo;

			if (statvfs(pptr,&fsinfo)<0) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: statvfs on '%s'",pptr);
			} else {
				double size = (double)(fsinfo.f_frsize)*(double)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));
				if (ldata > size) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: space to be left free on '%s' (%.0lf) is greater than real volume size (%.0lf) !!!",pptr,ldata,size);
				}
			}
		}
	}
	if (lmode==LMODE_LIMIT_TOTAL_POS_CONST) { // sanity checks
		if (ldata<=0.0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: limit on '%s' set to zero - using real volume size",pptr);
			lmode = LMODE_NONE;
		} else {
			struct statvfs fsinfo;

			if (statvfs(pptr,&fsinfo)<0) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: statvfs on '%s'",pptr);
			} else {
				double size = (double)(fsinfo.f_frsize)*(double)(fsinfo.f_blocks-(fsinfo.f_bfree-fsinfo.f_bavail));
				if (ldata > size) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: limit on '%s' (%.0lf) is greater than real volume size (%.0lf)",pptr,ldata,size);
				}
			}
		}
	}

	// add line to cfgline
	cl = malloc(sizeof(cfgline));
	passert(cl);
	cl->path = malloc(l+1);
	passert(cl->path);
	memcpy(cl->path,pptr,l);
	cl->path[l] = 0;
	cl->f = NULL;
	zassert(pthread_mutex_lock(&folderlock));
	cl->next = cfglinehead;
	cfglinehead = cl;
	zassert(pthread_mutex_unlock(&folderlock));

	mainmetaid = masterconn_getmetaid();
	if (mainmetaid>0) {
		metaid = mainmetaid;
	} else {
		metaid = masterconn_gethddmetaid(); // metaid not verified with master? - use metaid from previous hard disks
	}
	metaidfname = (char*)malloc(l+8);
	passert(metaidfname);
	memcpy(metaidfname,pptr,l);
	memcpy(metaidfname+l,".metaid",8);
	mfd = open(metaidfname,O_RDONLY);
	if (mfd>=0) {
		uint64_t filemetaid;
		uint8_t buff[8];
		const uint8_t *rptr;
		if (read(mfd,buff,8)==8) {
			rptr = buff;
			filemetaid = get64bit(&rptr);
			if (filemetaid!=metaid && metaid>0) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"hdd space manager: wrong meta id (mfs instance id) in file '%s' (0x%016"PRIX64",expected:0x%016"PRIX64") - likely this is drive from different mfs instance (drive ignored, remove this file to skip this test)",metaidfname,filemetaid,metaid);
				close(mfd);
				free(metaidfname);
				return -1;
			}
			if (metaid==0 && filemetaid>0) {
				masterconn_sethddmetaid(filemetaid); // metaid not known - set metaid read from hard disks for future verification with master
			}
			mainmetaid = 0; // file exists and is correct (or forced do be ignored), so do not re create it
		}
		close(mfd);
	}
	free(metaidfname);
	lockfname = (char*)malloc(l+6);
	passert(lockfname);
	memcpy(lockfname,pptr,l);
	memcpy(lockfname+l,".lock",6);
	lfd = open(lockfname,O_RDWR|O_CREAT|O_TRUNC,0640);
	if (lfd<0 && errno==EROFS && mfr!=MFR_NO) {
		lfd = open(lockfname,O_RDONLY); // prevents umounting
		free(lockfname);
		mfr = MFR_READONLY;
	} else {
		if (lfd<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"hdd space manager: can't create lock file '%s'",lockfname);
			free(lockfname);
			return -1;
		}
		if (lockneeded && lockf(lfd,F_TLOCK,0)<0) {
			if (LOCK_ERRNO_ERROR) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"hdd space manager: lockf '%s' error",lockfname);
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"hdd space manager: data folder '%s' already locked (used by another process)",pptr);
			}
			free(lockfname);
			close(lfd);
			return -1;
		}
		if (fstat(lfd,&sb)<0) {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"hdd space manager: fstat '%s' error",lockfname);
			free(lockfname);
			close(lfd);
			return -1;
		}
		free(lockfname);
		if (lockneeded) {
			zassert(pthread_mutex_lock(&folderlock));
			for (f=folderhead ; f ; f=f->next) {
				if (f->devid==sb.st_dev) {
					if (f->lockinode==sb.st_ino) {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"hdd space manager: data folders '%s' and '%s have the same lockfile !!!",pptr,f->path);
						zassert(pthread_mutex_unlock(&folderlock));
						close(lfd);
						return -1;
					} else {
						mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: data folders '%s' and '%s' are on the same physical device (could lead to unexpected behaviours)",pptr,f->path);
					}
				}
			}
			zassert(pthread_mutex_unlock(&folderlock));
		}
	}
	if (mainmetaid>0) { // metaid already verified with master? - create file '.metaid'
		metaidfname = (char*)malloc(l+8);
		passert(metaidfname);
		memcpy(metaidfname,pptr,l);
		memcpy(metaidfname+l,".metaid",8);
		mfd = open(metaidfname,O_RDWR|O_CREAT|O_TRUNC,0640);
		if (mfd>=0) {
			uint8_t buff[8];
			uint8_t *wptr;
			wptr = buff;
			put64bit(&wptr,metaid);
			if (write(mfd,buff,8)!=8) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error writing meta id file");
			}
			close(mfd);
		} else {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error writing meta id file");
		}
		free(metaidfname);
	}
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		if (strcmp(f->path,pptr)==0) {
			if (f->toremove==REMOVING_START) {
				f->toremove = REMOVING_NO;
			}
			if (f->damaged) {
				if (f->chunkcount>0) { // disk only flagged as damaged - do not rescan !!!
					f->damaged = 0;
					f->lastblocks = 0;
					f->isro = 0;
				} else {
					f->scanstate = SCST_SCANNEEDED;
					f->scanprogress = 0;
					f->damaged = 0;
					f->avail = 0ULL;
					f->total = 0ULL;
					f->lastblocks = 0;
					f->isro = 0;
					if (f->chunktab) {
						free(f->chunktab);
					}
					f->chunkcount = 0;
					f->ec4chunkcount = 0;
					f->ec8chunkcount = 0;
					f->chunktabsize = 0;
					f->chunktab = NULL;
					hdd_stats_clear(&(f->cstat));
					hdd_stats_clear(&(f->monotonic));
					for (l=0 ; l<STATSHISTORY ; l++) {
						hdd_stats_clear(&(f->stats[l]));
					}
					f->statspos = 0;
				}
				for (l=0 ; l<LASTERRSIZE ; l++) {
					f->lasterrtab[l].chunkid = 0ULL;
					f->lasterrtab[l].timestamp = 0;
					f->lasterrtab[l].monotonic_time = 0.0;
					f->lasterrtab[l].errornumber = 0;
				}
				f->lasterrindx = 0;
				f->lastrefresh = 0.0;
				f->needrefresh = 1;
			}
			if (f->scanstate!=SCST_SCANNEEDED) {
				if (f->lmode==LMODE_NONE && lmode!=LMODE_NONE) {
					f->scanstate = SCST_ATTRNEEDED;
				}
				if ((f->markforremoval==MFR_NO && mfr!=MFR_NO) || (f->markforremoval!=MFR_NO && mfr==MFR_NO)) {
					// the change is important - chunks need to be send to master again
					f->sendneeded = 1;
				}
			}
			f->lmode = lmode;
			f->ldata = ldata;
			f->markforremoval = mfr;
			f->balancemode = bm;
			f->ignoresize = is;
			cl->f = f;
			zassert(pthread_mutex_unlock(&folderlock));
			if (lfd>=0) {
				close(lfd);
			}
			return 1;
		}
	}
	f = (folder*)malloc(sizeof(folder));
	passert(f);
	f->markforremoval = mfr;
	f->balancemode = bm;
	f->ignoresize = is;
	f->damaged = 0;
	f->scanstate = SCST_SCANNEEDED;
	f->sendneeded = 0;
	f->scanprogress = 0;
	f->path = strdup(pptr);
	passert(f->path);
	f->toremove = REMOVING_NO;
	f->lmode = lmode;
	f->ldata = ldata;
	f->avail = 0ULL;
	f->total = 0ULL;
	f->lastblocks = 0;
	f->isro = 0;
	f->chunkcount = 0;
	f->ec4chunkcount = 0;
	f->ec8chunkcount = 0;
	f->chunktabsize = 0;
	f->chunktab = NULL;
	hdd_stats_clear(&(f->cstat));
	hdd_stats_clear(&(f->monotonic));
	for (l=0 ; l<STATSHISTORY ; l++) {
		hdd_stats_clear(&(f->stats[l]));
	}
	f->statspos = 0;
	for (l=0 ; l<LASTERRSIZE ; l++) {
		f->lasterrtab[l].chunkid = 0ULL;
		f->lasterrtab[l].timestamp = 0;
		f->lasterrtab[l].monotonic_time = 0.0;
		f->lasterrtab[l].errornumber = 0;
	}
	f->totalerrorcounter = 0;
	f->totalerrorstart = monotonic_seconds();
	f->lasterrindx = 0;
	f->lastrefresh = 0.0;
	f->needrefresh = 1;
	f->devid = sb.st_dev;
	f->lockinode = sb.st_ino;
	f->lfd = lfd;
	f->dumpfd = -1;
	f->testedhead = NULL;
	f->testedtail = &(f->testedhead);
	f->testneededhead = NULL;
	f->testneededtail = &(f->testneededhead);
	f->testedcnt = 0;
	f->testneededcnt = 0;
	f->testfailcnt = 0;
	f->endlooptime = 0;
	f->startlooptime = 0;
	f->startlooptestneededcnt = 0;
	f->nexttest = 0;
	f->min_count = 0;
	f->min_pathid = 0;
	f->current_pathid = 0;
	memset(f->subf_count,0,sizeof(f->subf_count));
	f->knowncount = 0;
	f->knowndiskusage = 0;
	f->knowncount_next = 0;
	f->knowndiskusage_next = 0;
//	f->carry = (double)(random()&0x7FFFFFFF)/(double)(0x7FFFFFFF);
	f->read_dist = 0;
	f->write_dist = 0;
	f->read_first = 1;
	f->write_first = 1;
	f->read_corr = 0.0;
	f->write_corr = 0.0;
	f->rebalance_in_progress = 0;
	f->rebalance_last_usec = 0;
	f->iredlastrep = 0.0;
	f->wfrtime = monotonic_seconds();
	f->wfrlast = 0.0;
	f->wfrcount = 0;
	f->wfrchunks = NULL;
	f->next = folderhead;
	folderhead = f;
	cl->f = f;
	zassert(pthread_mutex_unlock(&folderlock));
	return 2;
}

int hdd_folders_reinit(void) {
	folder *f;
	FILE *fd;
	char *buff;
	size_t bsize;
	char *hddfname;
	int ret,datadef;

	if (!cfg_isdefined("HDD_CONF_FILENAME")) {
		hddfname = strdup(ETC_PATH "/mfs/mfshdd.cfg");
		passert(hddfname);
		fd = fopen(hddfname,"r");
		if (!fd && errno==ENOENT) {
			free(hddfname);
			hddfname = strdup(ETC_PATH "/mfshdd.cfg");
			fd = fopen(hddfname,"r");
			if (fd) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"default sysconf path has changed - please move mfshdd.cfg from "ETC_PATH"/ to "ETC_PATH"/mfs/");
			}
		}
	} else {
		hddfname = cfg_getstr("HDD_CONF_FILENAME",ETC_PATH "/mfs/mfshdd.cfg");
		fd = fopen(hddfname,"r");
	}

	if (!fd) {
		if (errno==ENOENT) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_ERR,"hdd space configuration file (%s) not found",hddfname);
		} else {
			mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"can't open hdd space configuration file (%s), error",hddfname);
		}
		free(hddfname);
		return -1;
	}

	ret = 0;

	zassert(pthread_mutex_lock(&folderlock));
	folderactions = 0; // stop folder actions
	for (f=folderhead ; f ; f=f->next) {
		if (f->toremove==REMOVING_NO) {
			f->toremove = REMOVING_START;
		}
	}
	hdd_clear_cfglines();
	zassert(pthread_mutex_unlock(&folderlock));

	bsize = 1000;
	buff = malloc(bsize);

	while (getline(&buff,&bsize,fd)!=-1) {
		if (hdd_parseline(buff)<0) {
			ret = -1;
		}
	}

	free(buff);
	fclose(fd);

	zassert(pthread_mutex_lock(&folderlock));
	datadef = 0;
	for (f=folderhead ; f ; f=f->next) {
		if (f->toremove==REMOVING_NO) {
			datadef = 1;
			if (f->scanstate==SCST_SCANNEEDED) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"hdd space manager: folder %s will be scanned",f->path);
			} else if (f->scanstate==SCST_ATTRNEEDED) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"hdd space manager: folder %s will be updated",f->path);
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"hdd space manager: folder %s didn't change",f->path);
			}
			if (f->sendneeded) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"hdd space manager: folder %s will be resent",f->path);
			}
		} else {
			f->damaged = 0;
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"hdd space manager: folder %s will be removed",f->path);
		}
	}
	folderactions = 1; // continue folder actions
	zassert(pthread_mutex_unlock(&folderlock));

	if (datadef==0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"hdd space manager: no hdd space defined in %s file",hddfname);
	}

	free(hddfname);

	return ret;
}

void hdd_wantexit(void) {
	zassert(pthread_mutex_lock(&folderlock));
	folderactions = 0; // stop folder actions
	zassert(pthread_mutex_unlock(&folderlock));
}

static char* hdd_info_scanstate_name(uint8_t scanstate) {
	switch (scanstate) {
		case SCST_WORKING:
			return "WORKING";
		case SCST_SCANNEEDED:
			return "SCAN_NEEDED";
		case SCST_ATTRNEEDED:
			return "ATTR_NEEDED";
		case SCST_SCANJOBINPROGRESS:
			return "SCANJOB_IN_PROGRESS";
		case SCST_ATTRJOBINPROGRESS:
			return "ATTRJOB_IN_PROGRESS";
		case SCST_BGJOBTERMINATE:
			return "BGJOB_TERMINATE";
		case SCST_BGJOBFINISHED:
			return "BGJOB_FINISHED";
		default:
			return "???";
	}
}

static char* hdd_info_removestate_name(uint8_t removestate) {
	switch (removestate) {
		case REMOVING_NO:
			return "DO_NOT_REMOVE";
		case REMOVING_INPROGRESS:
			return "REMOVING_IN_PROGRESS";
		case REMOVING_START:
			return "REMOVING_STARTED";
		case REMOVING_END:
			return "REMOVING_ENDED";
		default:
			return "???";
	}
}

static char* hdd_info_markforremoval_name(uint8_t mfr) {
	switch (mfr) {
		case MFR_NO:
			return "NO";
		case MFR_YES:
			return "YES";
		case MFR_READONLY:
			return "READ-ONLY";
		default:
			return "???";
	}
}

static char* hdd_info_balancemode_name(uint8_t mfr) {
	switch (mfr) {
		case REBALANCE_STD:
			return "STD";
		case REBALANCE_FORCE_SRC:
			return "SRC";
		case REBALANCE_FORCE_DST:
			return "DST";
		default:
			return "???";
	}
}

static char* hdd_info_limit_mode_name(uint8_t lmode) {
	switch (lmode) {
		case LMODE_NONE:
			return "NONE";
		case LMODE_LIMIT_TOTAL_POS_CONST:
			return "LIMIT_TOTAL_POS_CONST";
		case LMODE_LIMIT_TOTAL_POS_PERCENT:
			return "LIMIT_TOTAL_POS_PERCENT";
		case LMODE_LIMIT_TOTAL_NEG_CONST:
			return "LIMIT_TOTAL_NEG_CONST";
		case LMODE_LIMIT_TOTAL_NEG_PERCENT:
			return "LIMIT_TOTAL_NEG_PERCENT";
		case LMODE_SHARED:
			return "SHARED";
		case LMODE_SHARED_POS_CONST:
			return "SHARED_POS_CONST";
		case LMODE_SHARED_POS_PERCENT:
			return "SHARED_POS_PERCENT";
		case LMODE_SHARED_NEG_CONST:
			return "SHARED_NEG_CONST";
		case LMODE_SHARED_NEG_PERCENT:
			return "SHARED_NEG_PERCENT";
		default:
			return "???";
	}
}

void hdd_info(FILE *fd) {
	uint32_t c;
	folder *f;
	uint32_t i;
	double now,wd;
	uint32_t dur,chdone,etas,etam,etah,etad;
	time_t t;
	struct tm tms;
	char asctimebuff[30];

	now = monotonic_seconds();
	c = hdd_open_files_handle(OF_INFO);
	fprintf(fd,"[hdd-general]\n");
	fprintf(fd,"open files: %"PRIu32"/%"PRIu32"\n",c,oflimit);
	zassert(pthread_mutex_lock(&dclock));
	fprintf(fd,"error counter: %"PRIu32"\n",errorcounter);
	zassert(pthread_mutex_unlock(&dclock));
	fprintf(fd,"\n");
	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		fprintf(fd,"[folder %s]\n",f->path);
		fprintf(fd,"scanstate: %s\n",hdd_info_scanstate_name(f->scanstate));
		fprintf(fd,"sendneeded: %s\n",(f->sendneeded)?"YES":"NO");
		fprintf(fd,"removestate: %s\n",hdd_info_removestate_name(f->toremove));
		fprintf(fd,"markforremoval: %s\n",hdd_info_markforremoval_name(f->markforremoval));
		fprintf(fd,"balancemode: %s\n",hdd_info_balancemode_name(f->balancemode));
		fprintf(fd,"damaged: %s\n",(f->damaged)?"YES":"NO");
		wd = (now - f->totalerrorstart) / 86400.0;
		fprintf(fd,"totalerrorcounter: %"PRIu32"\nworking_days: %.1lf\navg_errors_per_day: %.4lf\n",f->totalerrorcounter,wd,f->totalerrorcounter/wd);
		zassert(pthread_mutex_lock(&statslock));
		fprintf(fd,"totalbytesread: %"PRIu64"\ntotalbyteswritten: %"PRIu64"\n",f->monotonic.rbytes,f->monotonic.wbytes);
		if (f->monotonic.nsecreadsum>0) {
			fprintf(fd,"avgreadspeed: %.2lf MB/s\n",f->monotonic.rbytes/(f->monotonic.nsecreadsum/1000.0));
		} else {
			fprintf(fd,"avgreadspeed: no data\n");
		}
		if (f->monotonic.nsecwritesum+f->monotonic.nsecfsyncsum>0) {
			fprintf(fd,"avgwritespeed: %.2lf MB/s\n",f->monotonic.wbytes/((f->monotonic.nsecwritesum+f->monotonic.nsecfsyncsum)/1000.0));
		} else {
			fprintf(fd,"avgwritespeed: no data\n");
		}
		fprintf(fd,"worstread: %.6lfs\nworstwrite: %.6lfs\nworstfsync: %.6lfs\n",f->monotonic.nsecreadmax/1000000000.0,f->monotonic.nsecwritemax/1000000000.0,f->monotonic.nsecfsyncmax/1000000000.0);
		zassert(pthread_mutex_unlock(&statslock));
		fprintf(fd,"ignoresize: %u\nscanprogress: %u\n",f->ignoresize,f->scanprogress);
		fprintf(fd,"limit_mode: %s\n",hdd_info_limit_mode_name(f->lmode));
		if (LDATA_IS_RATIO(f->lmode)) {
			fprintf(fd,"limit_percent: %.4lf\n",f->ldata*100.0);
		} else if (LDATA_IS_VALUE(f->lmode)) {
			fprintf(fd,"limit_value: %.0lf\n",f->ldata);
		}
		fprintf(fd,"avail: %"PRIu64"\ntotal: %"PRIu64"\n",f->avail,f->total);
		fprintf(fd,"knowncount: %"PRIu32"\nknowndiskusage: %"PRIu64"\n",f->knowncount,f->knowndiskusage);
		fprintf(fd,"rw/ro: %s\n",(f->isro)?"RO":"RW");
		fprintf(fd,"allchunkcount: %"PRIu32"\nec4chunkcount: %"PRIu32"\nec8chunkcount: %"PRIu32"\n",f->chunkcount,f->ec4chunkcount,f->ec8chunkcount);
		fprintf(fd,"read_corr: %.4lf\nwrite_corr: %.4lf\nread_dist: %"PRIu32"\nwrite_dist: %"PRIu32"\nread_first: %u\nwrite_first: %u\n",f->read_corr,f->write_corr,f->read_dist,f->write_dist,f->read_first,f->write_first);
		fprintf(fd,"hs_rebalances_in_progress: %u\n",f->rebalance_in_progress);
		fprintf(fd,"duplicates: %"PRIu32"\n",f->wfrcount);
		fprintf(fd,"min_count: %"PRIu32"\nmin_pathid: %"PRIu16"\ncurrent_pathid: %"PRIu16"\n",f->min_count,f->min_pathid,f->current_pathid);
		fprintf(fd,"chunks_tested: %"PRIu32"\nchunks_waitng_for_test: %"PRIu32"\n",f->testedcnt,f->testneededcnt);
		if (f->endlooptime>0) {
			t = f->endlooptime;
			localtime_r(&t,&tms);
			asctime_r(&tms,asctimebuff);
			for (i=0 ; i<30 ; i++) {
				if (asctimebuff[i]=='\r' || asctimebuff[i]=='\n') {
					asctimebuff[i] = 0;
				}
			}
			asctimebuff[29]=0;
			fprintf(fd,"testloop_last_end_timestamp: %"PRIu32" (%s)\n",f->endlooptime,asctimebuff);
		} else {
			fprintf(fd,"testloop_last_end_timestamp: first loop in progress\n");
		}
		if (f->startlooptime>0) {
			dur = main_time() - f->startlooptime;
			if (dur>0) {
				chdone = f->startlooptestneededcnt - f->testneededcnt;
				fprintf(fd,"testloop_avg_tests_per_minute: %.2lf\n",(chdone*60.0/dur));
				if (chdone>0) {
					etas = ((uint64_t)f->testneededcnt * (uint64_t)dur) / chdone;
					etam = etas/60;
					etas %= 60;
					etah = etam/60;
					etam %= 60;
					etad = etah/24;
					etah %= 24;
					fprintf(fd,"testloop_eta: %ud %02u:%02u:%02u\n",etad,etah,etam,etas);
				}
			}
		}
		fprintf(fd,"\n");
	}
	zassert(pthread_mutex_unlock(&folderlock));
}

static inline void hdd_options_common(uint8_t initflag) {
	char *LeaveFreeStr;
	uint8_t sp;
	uint32_t tmp;

	zassert(pthread_mutex_lock(&folderlock));
	HDDErrorCount = cfg_getuint32("HDD_ERROR_TOLERANCE_COUNT",2);
	if (HDDErrorCount<1) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error tolerance count too small - changed to 1");
		HDDErrorCount = 1;
	} else if (HDDErrorCount>10) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error tolerance count too big - changed to 10");
		HDDErrorCount = 10;
	}
	HDDErrorTime = cfg_getuint32("HDD_ERROR_TOLERANCE_PERIOD",600);
	if (HDDErrorTime<10) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error tolerance period too small - changed to 10 seconds");
		HDDErrorTime = 10;
	} else if (HDDErrorTime>86400) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: error tolerance period too big - changed to 86400 seconds (1 day)");
		HDDErrorTime = 86400;
	}
	HDDRoundRobinChunkCount = cfg_getint32("HDD_RR_CHUNK_COUNT",10000);
	if (HDDRoundRobinChunkCount<1) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: round robin chunk count too small - changed to 1");
		HDDRoundRobinChunkCount = 1;
	} else if (HDDRoundRobinChunkCount>100000) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: round robin chunk count too big - changed to 100000");
		HDDRoundRobinChunkCount = 100000;
	}
	tmp = cfg_gethperiod("HDD_KEEP_DUPLICATES_HOURS","1w");
	if (tmp>100*24) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: hours to keep duplicates value too big - changed to 2400");
		tmp = 100*24;
	}
	if (tmp!=HDDKeepDuplicatesHours && initflag==0) {
		folder *f;
		for (f=folderhead ; f ; f=f->next ) {
			if (f->wfrcount>0) {
				f->wfrtime = monotonic_seconds();
			}
		}
	}
	HDDKeepDuplicatesHours = tmp;
	zassert(pthread_mutex_unlock(&folderlock));
	zassert(pthread_mutex_lock(&testlock));
	if (cfg_isdefined("HDD_TEST_SPEED")) {
		HDDTestMBPS = cfg_getdouble("HDD_TEST_SPEED",1.0);
		if (HDDTestMBPS<0.0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: setting HDD_TEST_SPEED to negative value doesn't make sense - changed to 0.0");
			HDDTestMBPS=0.0;
		}
	} else {
		double testfreq;
		testfreq = cfg_getuint32("HDD_TEST_FREQ",10); // deprecated option
		if (testfreq>0) {
			HDDTestMBPS = 10.0 / testfreq;
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"hdd space manager: regular chunk tests are disabled - this is not recommended setting");
			HDDTestMBPS = 0.0;
		}
	}
	if (HDDTestMBPS==0.0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"hdd space manager: regular chunk tests are disabled - this is not recommended setting");
	}
	HDDRebalancePerc = cfg_getuint32("HDD_REBALANCE_UTILIZATION",20);
	if (HDDRebalancePerc>100) {
		HDDRebalancePerc=100;
	}
	HSRebalanceLimit = cfg_getuint32("HDD_HIGH_SPEED_REBALANCE_LIMIT",0);
	if (HSRebalanceLimit>10) {
		HSRebalanceLimit=10;
	}
	MinTimeBetweenTests = cfg_getsperiod("HDD_MIN_TEST_INTERVAL","1d");
	MinFlushCacheTime = cfg_getsperiod("HDD_FADVISE_MIN_TIME","1d");
	zassert(pthread_mutex_unlock(&testlock));
	zassert(pthread_mutex_lock(&doplock));
	DoFsyncBeforeClose = cfg_getuint8("HDD_FSYNC_BEFORE_CLOSE",0);
	zassert(pthread_mutex_unlock(&doplock));

	LeaveFreeStr = cfg_getstr("HDD_LEAVE_SPACE_DEFAULT","256MiB");
	if (hdd_size_parse_u64(LeaveFreeStr,&LeaveFree)<0) {
		if (initflag) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT parse error - using default (256MiB)");
			LeaveFree = 0x10000000;
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT parse error - left unchanged");
		}
	}
	free(LeaveFreeStr);
	if (LeaveFree<0x4000000) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"hdd space manager: HDD_LEAVE_SPACE_DEFAULT < chunk size - leaving so small space on hdd is not recommended");
	}

	sp = cfg_getuint8("HDD_SPARSIFY_ON_WRITE",1);
#ifdef HAVE___SYNC_OP_AND_FETCH
	if (sp) {
		__sync_or_and_fetch(&Sparsification,1);
	} else {
		__sync_and_and_fetch(&Sparsification,0);
	}
#else
	pthread_mutex_lock(&cfglock);
	Sparsification = sp?1:0;
	pthread_mutex_unlock(&cfglock);
#endif
}

void hdd_reload(void) {
	hdd_options_common(0);
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"reloading hdd data ...");
	hdd_folders_reinit();
}

int hdd_restore(void) {
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"not implemented in the chunkserver");
	return 0;
}

int hdd_late_init(void) {
	zassert(pthread_mutex_lock(&termlock));
	term = 0;
	zassert(pthread_mutex_unlock(&termlock));

	zassert(lwt_minthread_create(&knowndiskusagethread,0,hdd_knowndiskusage_thread,NULL));
	zassert(lwt_minthread_create(&testerthread,0,hdd_tester_thread,NULL));
	zassert(lwt_minthread_create(&foldersthread,0,hdd_folders_thread,NULL));
	zassert(lwt_minthread_create(&rebalancethread,0,hdd_rebalance_thread,NULL));
	zassert(lwt_minthread_create(&hsrebalancethread,0,hdd_highspeed_rebalance_thread,NULL));
	zassert(lwt_minthread_create(&delayedthread,0,hdd_delayed_thread,NULL));
	return 0;
}

int hdd_init(void) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t hp;
	folder *f;

#ifdef HAVE_MMAP
	CanUseMmap = cfg_getuint8("CAN_USE_MMAP",0);
#else
	if (cfg_getuint8("CAN_USE_MMAP",0)!=0) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mmap is not supported in your OS - ignoring CAN_USE_MMAP option");
	}
#endif


	// this routine is called at the beginning from the main thread so no locks are necessary here
	for (hp=0 ; hp<HASHSIZE ; hp++) {
		hashtab[hp] = NULL;
	}
	for (hp=0 ; hp<DHASHSIZE ; hp++) {
		dophashtab[hp] = NULL;
	}

#if 0
	fprintf(stderr,"compiled with features: ");
#ifdef PRESERVE_BLOCK
	fprintf(stderr,"PRESERVE_BLOCK,");
#endif
#ifdef MMAP_ALLOC
	fprintf(stderr,"MMAP_ALLOC,");
#endif
#ifdef HAVE___SYNC_OP_AND_FETCH
	fprintf(stderr,"SYNC_OP_AND_FETCH,");
#endif
#ifdef HAVE___SYNC_FETCH_AND_OP
	fprintf(stderr,"SYNC_FETCH_AND_OP,");
#endif
#ifdef USE_PIO
	fprintf(stderr,"PREAD/PWRITE\n");
#else
	fprintf(stderr,"SEEK+READ/WRITE\n");
#endif
#endif

#ifdef HAVE_MMAP
	if (CanUseMmap && sizeof(waitforremoval)>4096) {
		fprintf(stderr,"bad waitforremoval size (%lu - should be <= 4096)",(unsigned long int)sizeof(waitforremoval));
		return -1;
	}
#endif

#ifndef PRESERVE_BLOCK
	zassert(pthread_key_create(&hdrbufferkey,free));
	zassert(pthread_key_create(&blockbufferkey,hdd_blockbuffer_free));
#endif /* PRESERVE_BLOCK */

	emptyblockcrc = mycrc32_zeroblock(0,MFSBLOCKSIZE);
	myalloc(emptychunkcrc,CHUNKCRCSIZE);
	passert(emptychunkcrc);
	ptr = emptychunkcrc;
	for (i=0 ; i<CHUNKCRCSIZE ; i+=sizeof(uint32_t)) {
		put32bit(&ptr,emptyblockcrc);
	}

	hdd_options_common(1);

	if (hdd_folders_reinit()<0) {
		if (cfg_getuint8("ALLOW_STARTING_WITH_INVALID_DISKS",0)==0) {
			return -1;
		}
	}

	oflimit = hdd_open_files_handle(OF_INIT);

	zassert(pthread_mutex_lock(&folderlock));
	for (f=folderhead ; f ; f=f->next) {
		fprintf(stderr,"hdd space manager: path to scan: %s\n",f->path);
	}
	zassert(pthread_mutex_unlock(&folderlock));
	fprintf(stderr,"hdd space manager: start background hdd scanning (searching for available chunks)\n");

	main_wantexit_register(hdd_wantexit);
	main_reload_register(hdd_reload);
	main_time_register(60,0,hdd_diskinfo_movestats);
//	main_time_register(10,0,hdd_testloopcheck); debug only
	main_destruct_register(hdd_term);
	main_info_register(hdd_info);

	zassert(pthread_mutex_lock(&termlock));
	term = 1;
	zassert(pthread_mutex_unlock(&termlock));

	return 0;
}
