/*
 * Copyright (C) 2019 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifdef HAVE_MMAP
#define BUCKETS_MMAP_ALLOC 1
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <syslog.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif
#include <sys/stat.h>
#include <inttypes.h>
#include <errno.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#include "MFSCommunication.h"

#include "matoclserv.h"
#include "matocsserv.h"
#include "sessions.h"
#include "csdb.h"
#include "chunks.h"
#include "filesystem.h"
#include "openfiles.h"
#include "xattr.h"
#include "posixacl.h"
#include "bio.h"
#include "metadata.h"
#include "datapack.h"
#include "slogger.h"
#include "massert.h"
#include "hashfn.h"
#include "datacachemgr.h"
#include "cfg.h"
#include "main.h"
#include "changelog.h"
#include "cuckoohash.h"
#include "buckets.h"
#include "clocks.h"
#include "storageclass.h"
#include "missinglog.h"

#define HASHTAB_LOBITS 24
#define HASHTAB_HISIZE (0x80000000>>(HASHTAB_LOBITS))
#define HASHTAB_LOSIZE (1<<HASHTAB_LOBITS)
#define HASHTAB_MASK (HASHTAB_LOSIZE-1)
#define HASHTAB_MOVEFACTOR 5

#define DEFAULT_SCLASS 2
#define DEFAULT_TRASHTIME 24

#define MAXFNAMELENG 255

#define MAX_INDEX 0x7FFFFFFF

#define EDGEID_MAX UINT64_C(0x7FFFFFFFFFFFFFFF)
#define EDGEID_HASHSIZE 65536

#define CHIDS_NO 0
#define CHIDS_YES 1
#define CHIDS_AUTO 2

typedef struct _bstnode {
	uint32_t val,count;
	struct _bstnode *left,*right;
} bstnode;

struct _fsnode;

typedef struct _fsedge {
	struct _fsnode *child,*parent;
	struct _fsedge *nextchild,*nextparent;
	struct _fsedge **prevchild,**prevparent;
	struct _fsedge *next;
	uint64_t edgeid;
	uint32_t hashval;
	uint16_t nleng;
	const uint8_t name[1];
} fsedge;

typedef struct _statsrecord {
	uint32_t inodes;
	uint32_t dirs;
	uint32_t files;
	uint32_t chunks;
	uint64_t length;
	uint64_t size;
	uint64_t realsize;
} statsrecord;

typedef struct _quotanode {
	uint32_t graceperiod;
	uint8_t exceeded;	// hard quota exceeded or soft quota reached time limit
	uint8_t flags;
	uint32_t stimestamp;	// time when soft quota exceeded
	uint32_t sinodes,hinodes;
	uint64_t slength,hlength;
	uint64_t ssize,hsize;
	uint64_t srealsize,hrealsize;
	struct _fsnode *node;
	struct _quotanode *next,**prev;
} quotanode;

static quotanode *quotahead;
static uint32_t QuotaDefaultGracePeriod;
static uint16_t MaxAllowedHardLinks;
static uint8_t AtimeMode;

typedef struct _fsnode {
	uint32_t inode;
	uint32_t ctime,mtime,atime;
	uint32_t uid;
	uint32_t gid;
	unsigned xattrflag:1;
	unsigned aclpermflag:1;
	unsigned acldefflag:1;
	unsigned type:4;
	unsigned mode:12;
	uint8_t sclassid;
	uint8_t eattr;
	uint8_t winattr;
	uint16_t trashtime;
	fsedge *parents;
	struct _fsnode *next;
	union _data {
		struct _ddata {				// type==TYPE_DIRECTORY
			fsedge *children;
			uint32_t nlink;
			uint32_t elements;
			statsrecord stats;
			quotanode *quota;
			uint8_t end;
		} ddata;
		struct _sdata {				// type==TYPE_SYMLINK
			uint8_t *path;
			uint32_t pleng;
			uint16_t nlink;
			uint8_t end;
		} sdata;
		struct _devdata {
			uint32_t rdev;			// type==TYPE_BLOCKDEV ; type==TYPE_CHARDEV
			uint16_t nlink;
			uint8_t end;
		} devdata;
		struct _fdata {				// type==TYPE_FILE ; type==TYPE_TRASH ; type==TYPE_SUSTAINED
			uint64_t length;
			uint64_t *chunktab;
			uint32_t chunks;
			uint16_t nlink;			// for TRASH and SUSTAINED should be 0
			uint8_t realsize_ratio;		// max goal
			uint8_t end;
		} fdata;
		struct _odata {
			uint16_t nlink;
			uint8_t end;
		} odata;
	} data;
} fsnode;

typedef struct _freenode {
	uint32_t inode;
	uint32_t ftime;
	struct _freenode *next;
} freenode;

static uint32_t *freebitmask;
static uint32_t bitmasksize;
static uint32_t searchpos;
static freenode *freelist,**freetail;
static uint32_t freelastts;

static uint32_t trash_bid;
static uint32_t sustained_bid;
static fsedge *trash[TRASH_BUCKETS];
static fsedge *sustained[SUSTAINED_BUCKETS];
static fsnode *root;

static fsedge **edgehashtab[HASHTAB_HISIZE];
static uint32_t edgerehashpos;
static uint32_t edgehashsize;
static uint32_t edgehashelem;

static fsnode **nodehashtab[HASHTAB_HISIZE];
static uint32_t noderehashpos;
static uint32_t nodehashsize;
static uint32_t nodehashelem;

static uint32_t hashelements;
static uint32_t maxnodeid;
static uint32_t nodes;
static uint64_t nextedgeid;
static uint8_t edgesneedrenumeration;

static uint64_t trashspace;
static uint64_t sustainedspace;
static uint32_t trashnodes;
static uint32_t sustainednodes;
static uint32_t filenodes;
static uint32_t dirnodes;

static uint64_t *edgeid_id_hashtab;
static fsedge **edgeid_ptr_hashtab;

static void *snapshot_inodehash;

#define MSGBUFFSIZE 1000000

static uint32_t fsinfo_files=0;
static uint32_t fsinfo_ugfiles=0;
static uint32_t fsinfo_mfiles=0;
static uint32_t fsinfo_mtfiles=0;
static uint32_t fsinfo_msfiles=0;
static uint32_t fsinfo_chunks=0;
static uint32_t fsinfo_ugchunks=0;
static uint32_t fsinfo_mchunks=0;
static char *fsinfo_msgbuff=NULL;
static uint32_t fsinfo_msgbuffleng=0;
static uint32_t fsinfo_loopstart=0;
static uint32_t fsinfo_loopend=0;

static uint32_t test_start_time;

static uint32_t stats_statfs=0;
static uint32_t stats_getattr=0;
static uint32_t stats_setattr=0;
static uint32_t stats_lookup=0;
static uint32_t stats_mkdir=0;
static uint32_t stats_rmdir=0;
static uint32_t stats_symlink=0;
static uint32_t stats_readlink=0;
static uint32_t stats_mknod=0;
static uint32_t stats_unlink=0;
static uint32_t stats_rename=0;
static uint32_t stats_link=0;
static uint32_t stats_readdir=0;
static uint32_t stats_open=0;
static uint32_t stats_read=0;
static uint32_t stats_write=0;

void fs_stats(uint32_t stats[16]) {
	stats[0] = stats_statfs;
	stats[1] = stats_getattr;
	stats[2] = stats_setattr;
	stats[3] = stats_lookup;
	stats[4] = stats_mkdir;
	stats[5] = stats_rmdir;
	stats[6] = stats_symlink;
	stats[7] = stats_readlink;
	stats[8] = stats_mknod;
	stats[9] = stats_unlink;
	stats[10] = stats_rename;
	stats[11] = stats_link;
	stats[12] = stats_readdir;
	stats[13] = stats_open;
	stats[14] = stats_read;
	stats[15] = stats_write;
	stats_statfs=0;
	stats_getattr=0;
	stats_setattr=0;
	stats_lookup=0;
	stats_mkdir=0;
	stats_rmdir=0;
	stats_symlink=0;
	stats_readlink=0;
	stats_mknod=0;
	stats_unlink=0;
	stats_rename=0;
	stats_link=0;
	stats_readdir=0;
	stats_open=0;
	stats_read=0;
	stats_write=0;
}







CREATE_BUCKET_ALLOCATOR(freenode,freenode,10000000/sizeof(freenode))

CREATE_BUCKET_ALLOCATOR(quotanode,quotanode,10000000/sizeof(quotanode))

#define fsnode_dir_malloc() fsnode_malloc(0)
#define fsnode_file_malloc() fsnode_malloc(1)
#define fsnode_symlink_malloc() fsnode_malloc(2)
#define fsnode_dev_malloc() fsnode_malloc(3)
#define fsnode_other_malloc() fsnode_malloc(4)

#define fsnode_dir_free(n) fsnode_free(n,0)
#define fsnode_file_free(n) fsnode_free(n,1)
#define fsnode_symlink_free(n) fsnode_free(n,2)
#define fsnode_dev_free(n) fsnode_free(n,3)
#define fsnode_other_free(n) fsnode_free(n,4)

#define NODE_BUCKET_SIZE 10000000
#define NODE_MAX_INDX 5

typedef struct _fsnode_bucket {
	uint32_t firstfree;
	struct _fsnode_bucket *next;
	uint8_t bucket[1];
} fsnode_bucket;

static fsnode_bucket *nrbheads[NODE_MAX_INDX];
static fsnode *nrbfreeheads[NODE_MAX_INDX];
static uint32_t nrbucketsize[NODE_MAX_INDX];
static uint32_t nrelemsize[NODE_MAX_INDX];
static uint64_t fsnode_allocated;
static uint64_t fsnode_used;

static inline void fsnode_init(void) {
	uint32_t i;
	nrelemsize[0] = offsetof(fsnode,data)+offsetof(struct _ddata,end);
	nrelemsize[1] = offsetof(fsnode,data)+offsetof(struct _fdata,end);
	nrelemsize[2] = offsetof(fsnode,data)+offsetof(struct _sdata,end);
	nrelemsize[3] = offsetof(fsnode,data)+offsetof(struct _devdata,end);
	nrelemsize[4] = offsetof(fsnode,data)+offsetof(struct _odata,end);
	for (i=0 ; i<NODE_MAX_INDX ; i++) {
		nrbheads[i] = NULL;
		nrbfreeheads[i] = NULL;
		nrbucketsize[i] = (NODE_BUCKET_SIZE / nrelemsize[i]) * nrelemsize[i];
//		fprintf(stderr,"%u %u %u\n",i,nrelemsize[i],nrbucketsize[i]);
	}
	fsnode_allocated=0;
	fsnode_used=0;
}

static inline void fsnode_cleanup(void) {
	fsnode_bucket *nrb,*nnrb;
	uint32_t i;
	for (i=0 ; i<NODE_MAX_INDX ; i++) {
		for (nrb = nrbheads[i] ; nrb ; nrb=nnrb) {
			nnrb = nrb->next;
#ifdef BUCKETS_MMAP_ALLOC
			munmap(nrb,offsetof(fsnode_bucket,bucket)+nrbucketsize[i]);
#else
			free(nrb);
#endif
		}
		nrbheads[i] = NULL;
		nrbfreeheads[i] = NULL;
	}
	fsnode_allocated=0;
	fsnode_used=0;
}

static inline fsnode* fsnode_malloc(uint8_t indx) {
	fsnode_bucket *nrb;
	fsnode *ret;
	sassert(indx<NODE_MAX_INDX);
	if (nrbfreeheads[indx]) {
		ret = nrbfreeheads[indx];
		nrbfreeheads[indx] = ret->next;
		fsnode_used += nrelemsize[indx];
		return ret;
	}
	if (nrbheads[indx]==NULL || nrbheads[indx]->firstfree + nrelemsize[indx] > nrbucketsize[indx]) {
#ifdef BUCKETS_MMAP_ALLOC
		nrb = (fsnode_bucket*)mmap(NULL,offsetof(fsnode_bucket,bucket)+nrbucketsize[indx],PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		nrb = (fsnode_bucket*)malloc(offsetof(fsnode_bucket)+nrbucketsize[indx]);
#endif
		passert(nrb);
		nrb->next = nrbheads[indx];
		nrb->firstfree = 0;
		nrbheads[indx] = nrb;
		fsnode_allocated += (offsetof(fsnode_bucket,bucket)+nrbucketsize[indx]);
	}
	ret = (fsnode*)((nrbheads[indx]->bucket) + (nrbheads[indx]->firstfree));
	nrbheads[indx]->firstfree += nrelemsize[indx];
	fsnode_used += nrelemsize[indx];
	return ret;
}

static inline void fsnode_free(fsnode *n,uint8_t indx) {
	n->next = nrbfreeheads[indx];
	nrbfreeheads[indx] = n;
	fsnode_used -= nrelemsize[indx];
}

static inline void fsnode_getusage(uint64_t *allocated,uint64_t *used) {
	*allocated = fsnode_allocated;
	*used = fsnode_used;
}






#define EDGE_BUCKET_SIZE 10000000
#define EDGE_MAX_INDX (MFS_PATH_MAX/8)
#define EDGE_REC_INDX(nleng) (((nleng)-1)/8)
#define EDGE_REC_SIZE(indx) (((indx)+1)*8 + ((offsetof(fsedge,name)+7)&UINT32_C(0xFFFFFFF8)))
//#define EDGE_REC_SIZE(nleng) (((offsetof(fsedge,name)+(nleng))+7)&UINT32_C(0xFFFFFFF8))

typedef struct _fsedge_bucket {
	uint32_t firstfree;
	struct _fsedge_bucket *next;
	uint8_t bucket[1];
} fsedge_bucket;

static fsedge_bucket *erbheads[EDGE_MAX_INDX];
static fsedge *erbfreeheads[EDGE_MAX_INDX];
static uint32_t erbucketsize[EDGE_MAX_INDX];
static uint64_t fsedge_allocated;
static uint64_t fsedge_used;

static inline void fsedge_init(void) {
	uint32_t i;
	uint32_t recsize;
	for (i=0 ; i<EDGE_MAX_INDX ; i++) {
		erbheads[i] = NULL;
		erbfreeheads[i] = NULL;
		recsize = EDGE_REC_SIZE(i);
		erbucketsize[i] = (EDGE_BUCKET_SIZE / recsize) * recsize;
	}
	fsedge_allocated=0;
	fsedge_used=0;
}

static inline void fsedge_cleanup(void) {
	fsedge_bucket *erb,*nerb;
	uint32_t i;
	for (i=0 ; i<EDGE_MAX_INDX ; i++) {
		for (erb = erbheads[i] ; erb ; erb=nerb) {
			nerb = erb->next;
#ifdef BUCKETS_MMAP_ALLOC
			munmap(erb,offsetof(fsedge_bucket,bucket)+erbucketsize[i]);
#else
			free(erb);
#endif
		}
		erbheads[i] = NULL;
		erbfreeheads[i] = NULL;
	}
	fsedge_allocated=0;
	fsedge_used=0;
}

static inline fsedge* fsedge_malloc(uint16_t nleng) {
	fsedge_bucket *erb;
	fsedge *ret;
	uint16_t indx = EDGE_REC_INDX(nleng);
	sassert(indx<EDGE_MAX_INDX);
	if (erbfreeheads[indx]) {
		ret = erbfreeheads[indx];
		erbfreeheads[indx] = ret->next;
		fsedge_used += EDGE_REC_SIZE(indx);
		return ret;
	}
	if (erbheads[indx]==NULL || erbheads[indx]->firstfree + EDGE_REC_SIZE(nleng) > erbucketsize[indx]) {
#ifdef BUCKETS_MMAP_ALLOC
		erb = (fsedge_bucket*)mmap(NULL,offsetof(fsedge_bucket,bucket)+erbucketsize[indx],PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		erb = (fsedge_bucket*)malloc(offsetof(fsedge_bucket)+erbucketsize[indx]);
#endif
		passert(erb);
		erb->next = erbheads[indx];
		erb->firstfree = 0;
		erbheads[indx] = erb;
		fsedge_allocated += (offsetof(fsedge_bucket,bucket)+erbucketsize[indx]);
	}
	ret = (fsedge*)((erbheads[indx]->bucket) + (erbheads[indx]->firstfree));
	erbheads[indx]->firstfree += EDGE_REC_SIZE(indx);
	fsedge_used += EDGE_REC_SIZE(indx);
	return ret;
}

static inline void fsedge_free(fsedge *e,uint16_t nleng) {
	uint16_t indx = EDGE_REC_INDX(nleng);
	e->next = erbfreeheads[indx];
	erbfreeheads[indx] = e;
	fsedge_used -= EDGE_REC_SIZE(indx);
}

static inline void fsedge_getusage(uint64_t *allocated,uint64_t *used) {
	*allocated = fsedge_allocated;
	*used = fsedge_used;
}







#define SYMLINK_BUCKET_SIZE 10000000
#define SYMLINK_MAX_INDX (MFS_SYMLINK_MAX/8)
#define SYMLINK_REC_INDX(pathleng) (((pathleng)-1)/8)
#define SYMLINK_REC_SIZE(indx) (((indx)+1)*8)

typedef struct _symlink_bucket {
	uint32_t firstfree;
	struct _symlink_bucket *next;
	uint8_t bucket[1];
} symlink_bucket;

static symlink_bucket *stbheads[SYMLINK_MAX_INDX];
static uint8_t *stbfreeheads[SYMLINK_MAX_INDX];
static uint32_t stbucketsize[SYMLINK_MAX_INDX];
static uint64_t symlink_allocated;
static uint64_t symlink_used;

static inline void symlink_init(void) {
	uint32_t i;
	uint32_t recsize;
	for (i=0 ; i<SYMLINK_MAX_INDX ; i++) {
		stbheads[i] = NULL;
		stbfreeheads[i] = NULL;
		recsize = SYMLINK_REC_SIZE(i);
		stbucketsize[i] = (SYMLINK_BUCKET_SIZE / recsize) * recsize;
	}
	symlink_allocated = 0;
	symlink_used = 0;
}

static inline void symlink_cleanup(void) {
	symlink_bucket *stb,*nstb;
	uint32_t i;
	for (i=0 ; i<SYMLINK_MAX_INDX ; i++) {
		for (stb = stbheads[i] ; stb ; stb=nstb) {
			nstb = stb->next;
#ifdef BUCKETS_MMAP_ALLOC
			munmap(stb,offsetof(symlink_bucket,bucket)+stbucketsize[i]);
#else
			free(stb);
#endif
		}
		stbheads[i] = NULL;
		stbfreeheads[i] = NULL;
	}
	symlink_allocated = 0;
	symlink_used = 0;
}

static inline uint8_t* symlink_malloc(uint16_t pathleng) {
	symlink_bucket *stb;
	uint8_t *ret;
	uint16_t indx = SYMLINK_REC_INDX(pathleng);
	sassert(indx<SYMLINK_MAX_INDX);
	if (stbfreeheads[indx]) {
		ret = stbfreeheads[indx];
		stbfreeheads[indx] = *((uint8_t**)ret);
		symlink_used += SYMLINK_REC_SIZE(indx);
		return ret;
	}
	if (stbheads[indx]==NULL || stbheads[indx]->firstfree + SYMLINK_REC_SIZE(indx) > stbucketsize[indx]) {
#ifdef BUCKETS_MMAP_ALLOC
		stb = (symlink_bucket*)mmap(NULL,offsetof(symlink_bucket,bucket)+stbucketsize[indx],PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		stb = (symlink_bucket*)malloc(offsetof(symlink_bucket,bucket)+stbucketsize[indx]);
#endif
		passert(stb);
		stb->next = stbheads[indx];
		stb->firstfree = 0;
		stbheads[indx] = stb;
		symlink_allocated += (offsetof(symlink_bucket,bucket)+stbucketsize[indx]);
	}
	ret = (uint8_t*)((stbheads[indx]->bucket) + (stbheads[indx]->firstfree));
	stbheads[indx]->firstfree += SYMLINK_REC_SIZE(indx);
	symlink_used += SYMLINK_REC_SIZE(indx);
	return ret;
}

static inline void symlink_free(uint8_t *p,uint16_t pathleng) {
	uint16_t indx = SYMLINK_REC_INDX(pathleng);
	*((uint8_t**)p) = stbfreeheads[indx];
	stbfreeheads[indx] = p;
	symlink_used -= SYMLINK_REC_SIZE(indx);
}

static inline void symlink_getusage(uint64_t *allocated,uint64_t *used) {
	*allocated = symlink_allocated;
	*used = symlink_used;
}







#define CHUNKTAB_BUCKET_SIZE 10000000
#define CHUNKTAB_MAX_INDX 121
#define CHUNKTAB_ELEMENT_SIZE(chunks) ((chunks)*sizeof(uint64_t))
#define CHUNKTAB_REC_INDX(chunks) (((chunks)<=0x10)?(chunks)-1:((chunks)<=0x100)?(((chunks)+0xF)/0x10)+0xE:((chunks)<=0x1000)?(((chunks)+0xFF)/0x100)+0x1D:((chunks)<=0x10000)?(((chunks)+0xFFF)/0x1000)+0x2C:((chunks)<=0x100000)?(((chunks)+0xFFFF)/0x10000)+0x3B:((chunks)<=0x1000000)?(((chunks)+0xFFFFF)/0x100000)+0x4A:((chunks)<=0x10000000)?(((chunks)+0xFFFFFF)/0x1000000)+0x59:(((chunks)+UINT64_C(0xFFFFFFF))/0x10000000)+0x68)
#define CHUNKTAB_REC_SIZE(indx) ((((indx)<0x10)?(indx+1):((indx)<0x1F)?((indx)-0xE)*0x10:((indx)<0x2E)?((indx)-0x1D)*0x100:((indx)<0x3D)?((indx)-0x2C)*0x1000:((indx)<0x4C)?((indx)-0x3B)*0x10000:((indx)<0x5B)?((indx)-0x4A)*0x100000:((indx)<0x6A)?((indx)-0x59)*0x1000000:((indx)-0x68)*UINT64_C(0x10000000))*sizeof(uint64_t))

typedef struct _chunktab_bucket {
	uint64_t firstfree;
	struct _chunktab_bucket *next;
	uint8_t bucket[1];
} chunktab_bucket;

static chunktab_bucket *ctbheads[CHUNKTAB_MAX_INDX];
static uint64_t *ctbfreeheads[CHUNKTAB_MAX_INDX];
static uint64_t ctbucketsize[CHUNKTAB_MAX_INDX];
static uint64_t chunktabsize[CHUNKTAB_MAX_INDX];
static uint64_t chunktab_allocated;
static uint64_t chunktab_used;

static inline void chunktab_init(void) {
	uint32_t i;
	for (i=0 ; i<CHUNKTAB_MAX_INDX ; i++) {
		ctbheads[i] = NULL;
		ctbfreeheads[i] = NULL;
		chunktabsize[i] = CHUNKTAB_REC_SIZE(i);
		ctbucketsize[i] = ((CHUNKTAB_BUCKET_SIZE / chunktabsize[i])+1) * chunktabsize[i];
	}
	chunktab_allocated = 0;
	chunktab_used = 0;
}

static inline void chunktab_cleanup(void) {
	chunktab_bucket *ctb,*nctb;
	uint32_t i;
	for (i=0 ; i<CHUNKTAB_MAX_INDX ; i++) {
		for (ctb = ctbheads[i] ; ctb ; ctb=nctb) {
			nctb = ctb->next;
#ifdef BUCKETS_MMAP_ALLOC
			munmap(ctb,(offsetof(chunktab_bucket,bucket)+ctbucketsize[i]));
#else
			free(ctb);
#endif
		}
		ctbheads[i] = NULL;
		ctbfreeheads[i] = NULL;
	}
	chunktab_allocated = 0;
	chunktab_used = 0;
}

static inline uint64_t* chunktab_indx_malloc(uint8_t indx) {
	chunktab_bucket *ctb;
	uint64_t *ret;
	if (ctbfreeheads[indx]) {
		ret = ctbfreeheads[indx];
		ctbfreeheads[indx] = *((uint64_t**)ret);
		return ret;
	}
	if (ctbheads[indx]==NULL || ctbheads[indx]->firstfree + chunktabsize[indx] > ctbucketsize[indx]) {
#ifdef BUCKETS_MMAP_ALLOC
		ctb = (chunktab_bucket*)mmap(NULL,(offsetof(chunktab_bucket,bucket)+ctbucketsize[indx]),PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		ctb = (chunktab_bucket*)malloc(offsetof(chunktab_bucket,bucket)+ctbucketsize[indx]);
#endif
		passert(ctb);
		ctb->next = ctbheads[indx];
		ctb->firstfree = 0;
		ctbheads[indx] = ctb;
		chunktab_allocated += (offsetof(chunktab_bucket,bucket)+ctbucketsize[indx]);
	}
	ret = (uint64_t*)((ctbheads[indx]->bucket) + (ctbheads[indx]->firstfree));
	ctbheads[indx]->firstfree += chunktabsize[indx];
	return ret;
}

static inline void chunktab_indx_free(uint64_t *chunktab,uint8_t indx) {
	*((uint64_t**)chunktab) = ctbfreeheads[indx];
	ctbfreeheads[indx] = chunktab;
}

static inline uint64_t* chunktab_malloc(uint32_t chunks) {
	uint8_t indx = CHUNKTAB_REC_INDX(chunks);
	if (chunks==0 || indx>=CHUNKTAB_MAX_INDX) {
		return NULL;
	}
	chunktab_used+=CHUNKTAB_ELEMENT_SIZE(chunks);
	return chunktab_indx_malloc(indx);
}

static inline void chunktab_free(uint64_t *chunktab,uint32_t chunks) {
	uint8_t indx = CHUNKTAB_REC_INDX(chunks);
	if (chunks==0 || indx>=CHUNKTAB_MAX_INDX) {
		return;
	}
	chunktab_used-=CHUNKTAB_ELEMENT_SIZE(chunks);
	chunktab_indx_free(chunktab,indx);
}

static inline uint64_t* chunktab_realloc(uint64_t *oldchunktab,uint32_t oldchunks,uint32_t newchunks) {
	uint64_t *newchunktab;
	uint8_t oldindx,newindx;
	oldindx = CHUNKTAB_REC_INDX(oldchunks);
	newindx = CHUNKTAB_REC_INDX(newchunks);
	if (oldindx==newindx) {
		chunktab_used+=CHUNKTAB_ELEMENT_SIZE(newchunks);
		chunktab_used-=CHUNKTAB_ELEMENT_SIZE(oldchunks);
		return oldchunktab;
	} else {
		if (newindx>=CHUNKTAB_MAX_INDX) {
			newchunktab = NULL;
		} else {
			newchunktab = chunktab_indx_malloc(newindx);
			if (oldchunktab!=NULL && oldchunks>0) {
				if (newchunks>oldchunks) {
					memcpy(newchunktab,oldchunktab,sizeof(uint64_t)*oldchunks);
				} else {
					memcpy(newchunktab,oldchunktab,sizeof(uint64_t)*newchunks);
				}
			}
			chunktab_used+=CHUNKTAB_ELEMENT_SIZE(newchunks);
		}
		if (oldchunktab!=NULL && oldindx<CHUNKTAB_MAX_INDX) {
			chunktab_indx_free(oldchunktab,oldindx);
			chunktab_used-=CHUNKTAB_ELEMENT_SIZE(oldchunks);
		}
		return newchunktab;
	}
}

static inline void chunktab_getusage(uint64_t *allocated,uint64_t *used) {
	*allocated = chunktab_allocated;
	*used = chunktab_used;
}







void fs_get_memusage(uint64_t allocated[8],uint64_t used[8]) {
	allocated[0] = sizeof(fsedge*)*edgerehashpos;
	used[0] = sizeof(fsedge*)*edgehashelem;
	fsedge_getusage(allocated+1,used+1);
	allocated[2] = sizeof(fsnode*)*noderehashpos;
	used[2] = sizeof(fsnode*)*nodehashelem;
	fsnode_getusage(allocated+3,used+3);
	freenode_getusage(allocated+4,used+4);
	chunktab_getusage(allocated+5,used+5);
	symlink_getusage(allocated+6,used+6);
	quotanode_getusage(allocated+7,used+7);
//	statsrec_getusage(allocated+7,used+7);
}










uint32_t fsnodes_get_next_id() {
	uint32_t i,mask;
	while (searchpos<bitmasksize && freebitmask[searchpos]==0xFFFFFFFF) {
		searchpos++;
	}
	if (searchpos==bitmasksize) {	// no more freeinodes
		uint32_t *tmpfbm;
		bitmasksize+=0x80;
		tmpfbm = freebitmask;
		freebitmask = (uint32_t*)realloc(freebitmask,bitmasksize*sizeof(uint32_t));
		if (freebitmask==NULL) {
			free(tmpfbm); // pro forma - satisfy cppcheck
		}
		passert(freebitmask);
		memset(freebitmask+searchpos,0,0x80*sizeof(uint32_t));
	}
	mask = freebitmask[searchpos];
	i=0;
	while (mask&1) {
		i++;
		mask>>=1;
	}
	mask = 1<<i;
	freebitmask[searchpos] |= mask;
	i+=(searchpos<<5);
	if (i>maxnodeid) {
		maxnodeid=i;
	}
	return i;
}

void fsnodes_free_fixts(uint32_t ts) {
	syslog(LOG_WARNING,"last freed inode has higher timestamp than the current one - fixing timestamps in free inoes list");
	freenode *n;
	for (n=freelist ; n!=NULL ; n=n->next) {
		if (n->ftime > ts) {
			n->ftime = ts;
		}
	}
	freelastts = ts;
}

void fsnodes_free_id(uint32_t inode,uint32_t ts) {
	freenode *n;
	if (ts<freelastts) {
		fsnodes_free_fixts(ts);
	}
	n = freenode_malloc();
	n->inode = inode;
	n->ftime = ts;
	n->next = NULL;
	*freetail = n;
	freetail = &(n->next);
	freelastts = ts;
}

uint8_t fs_univ_freeinodes(uint32_t ts,uint8_t sesflags,uint32_t freeinodes,uint32_t sustainedinodes,uint32_t inode_chksum) {
	uint32_t si,fi,pos,mask;
	uint32_t ics;
	freenode *n,*an;
	freenode *sn,**snt;
	fi = 0;
	si = 0;
	ics = 0;
	n = freelist;
	sn = NULL;
	snt = &sn;
	if (ts<freelastts) {
		fsnodes_free_fixts(ts);
	}
	while (n && n->ftime+MFS_INODE_REUSE_DELAY<ts) {
		ics ^= n->inode;
		if (((sesflags&SESFLAG_METARESTORE)==0 || sustainedinodes>0) && of_isfileopen(n->inode)) {
			si++;
			an = n->next;
			n->ftime = ts;
			n->next = NULL;
			*snt = n;
			snt = &(n->next);
		} else {
			fi++;
			pos = (n->inode >> 5);
			mask = 1<<(n->inode&0x1F);
			freebitmask[pos] &= ~mask;
			if (pos<searchpos) {
				searchpos = pos;
			}
			an = n->next;
			freenode_free(n);
		}
		n = an;
	}
	if (n) {
		freelist = n;
	} else {
		freelist = NULL;
		freetail = &(freelist);
		freelastts = 0;
	}
	if (sn) {
		*freetail = sn;
		freetail = snt;
		freelastts = ts;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (fi>0 || si>0) {
			changelog("%"PRIu32"|FREEINODES():%"PRIu32",%"PRIu32",%"PRIu32,ts,fi,si,ics);
		}
	} else {
		if (freeinodes!=fi || sustainedinodes!=si || (inode_chksum!=0 && inode_chksum!=ics)) {
			syslog(LOG_WARNING,"FREEINODES data mismatch: my:(%"PRIu32",%"PRIu32",%"PRIu32") != expected:(%"PRIu32",%"PRIu32",%"PRIu32")",fi,si,ics,freeinodes,sustainedinodes,inode_chksum);
			return MFS_ERROR_MISMATCH;
		}
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

void fsnodes_freeinodes(void) {
	fs_univ_freeinodes(main_time(),0,0,0,0);
}

uint8_t fs_mr_freeinodes(uint32_t ts,uint32_t freeinodes,uint32_t sustainedinodes,uint32_t inode_chksum) {
	return fs_univ_freeinodes(ts,SESFLAG_METARESTORE,freeinodes,sustainedinodes,inode_chksum);
}

void fsnodes_init_freebitmask (void) {
	bitmasksize = 0x100+(((maxnodeid)>>5)&0xFFFFFF80U);
	freebitmask = (uint32_t*)malloc(bitmasksize*sizeof(uint32_t));
	passert(freebitmask);
	memset(freebitmask,0,bitmasksize*sizeof(uint32_t));
	freebitmask[0]=1;	// reserve inode 0
	searchpos = 0;
}

void fsnodes_used_inode (uint32_t inode) {
	uint32_t pos,mask;
	pos = inode>>5;
	mask = 1<<(inode&0x1F);
	if (freebitmask[pos]&mask) {
		syslog(LOG_WARNING,"freebitmask: repeated inode: %"PRIu32,inode);
	}
	if (pos>=bitmasksize) {
		uint32_t *tmpfbm;
		uint32_t oldsize = bitmasksize;

		syslog(LOG_WARNING,"freebitmask overrun (%"PRIu32">=%"PRIu32")",pos,bitmasksize);
		bitmasksize = 0x100+(pos&0xFFFFFF80U);
		tmpfbm = freebitmask;
		freebitmask = (uint32_t*)realloc(freebitmask,bitmasksize*sizeof(uint32_t));
		if (freebitmask==NULL) {
			free(tmpfbm); // pro forma - satisfy cppcheck
		}
		passert(freebitmask);
		memset(freebitmask+oldsize,0,(bitmasksize-oldsize)*sizeof(uint32_t));
	}
	freebitmask[pos]|=mask;
}










static inline uint32_t fsnodes_hash(uint32_t parentid,uint16_t nleng,const uint8_t *name) {
	uint32_t hash,i;
	hash = ((parentid * 0x5F2318BD) + nleng);
	for (i=0 ; i<nleng ; i++) {
		hash = hash*33+name[i];
	}
	return hash;
}

static inline uint32_t fsnodes_calc_hash_size(uint32_t elements) {
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

static inline void fsnodes_edge_hash_init(void) {
	uint16_t i;
	edgehashsize = 0;
	edgehashelem = 0;
	edgerehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		edgehashtab[i] = NULL;
	}
}

static inline void fsnodes_edge_hash_cleanup(void) {
	uint16_t i;
	edgehashelem = 0;
	edgehashsize = 0;
	edgerehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (edgehashtab[i]!=NULL) {
#ifdef HAVE_MMAP
			munmap(edgehashtab[i],sizeof(fsedge*)*HASHTAB_LOSIZE);
#else
			free(edgehashtab[i]);
#endif
		}
		edgehashtab[i] = NULL;
	}
}

static inline void fsnodes_edge_hash_rehash(void) {
	uint16_t i;
	edgerehashpos = edgehashsize;
	edgehashsize *= 2;
	for (i=(edgehashsize>>HASHTAB_LOBITS)/2 ; i<edgehashsize>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
		edgehashtab[i] = mmap(NULL,sizeof(fsedge*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		edgehashtab[i] = malloc(sizeof(fsedge*)*HASHTAB_LOSIZE);
#endif
		passert(edgehashtab[i]);
	}
}

static inline void fsnodes_edge_hash_move(void) {
	uint32_t hash;
	uint32_t mask;
	uint32_t moved=0;
	fsedge **ehptr,**ehptralt,*e;
	mask = edgehashsize-1;
	do {
		if (edgerehashpos>=edgehashsize) { // rehash complete
			edgerehashpos = edgehashsize;
			return;
		}
		ehptr = edgehashtab[(edgerehashpos - (edgehashsize/2)) >> HASHTAB_LOBITS] + (edgerehashpos & HASHTAB_MASK);
		ehptralt = edgehashtab[edgerehashpos >> HASHTAB_LOBITS] + (edgerehashpos & HASHTAB_MASK);
		*ehptralt = NULL;
		while ((e=*ehptr)!=NULL) {
			hash = e->hashval & mask;
			if (hash==edgerehashpos) {
				*ehptralt = e;
				*ehptr = e->next;
				ehptralt = &(e->next);
				e->next = NULL;
			} else {
				ehptr = &(e->next);
			}
			moved++;
		}
		edgerehashpos++;
	} while (moved<HASHTAB_MOVEFACTOR);
}

static inline fsedge* fsnodes_edge_find(fsnode *node,uint16_t nleng,const uint8_t *name) {
	fsedge *e;
	uint32_t hash;
	uint32_t hashval;

	if (edgehashsize==0) {
		return NULL;
	}
	hashval = fsnodes_hash(node->inode,nleng,name);
	hash = hashval & (edgehashsize-1);
	if (edgerehashpos<edgehashsize) {
		fsnodes_edge_hash_move();
		if (hash >= edgerehashpos) {
			hash -= edgehashsize/2;
		}
	}
	for (e=edgehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] ; e ; e=e->next) {
		if (e->parent==node && e->hashval==hashval && e->nleng==nleng && memcmp((char*)(e->name),(char*)name,nleng)==0) {
			return e;
		}
	}
	return NULL;
}

static inline void fsnodes_edge_delete(fsedge *e) {
	fsedge **ehptr,*eit;
	uint32_t hash;

	if (edgehashsize==0) {
		return;
	}
	hash = (e->hashval) & (edgehashsize-1);
	if (edgerehashpos<edgehashsize) {
		fsnodes_edge_hash_move();
		if (hash >= edgerehashpos) {
			hash -= edgehashsize/2;
		}
	}
	ehptr = edgehashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	while ((eit=*ehptr)!=NULL) {
		if (eit==e) {
			*ehptr = e->next;
			edgehashelem--;
			return;
		}
		ehptr = &(eit->next);
	}
}

static inline void fsnodes_edge_add(fsedge *e) {
	uint16_t i;
	uint32_t hash;

	if (edgehashsize==0) {
		edgehashsize = fsnodes_calc_hash_size(hashelements);
		edgerehashpos = edgehashsize;
		edgehashelem = 0;
		for (i=0 ; i<edgehashsize>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
			edgehashtab[i] = mmap(NULL,sizeof(fsedge*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
			edgehashtab[i] = malloc(sizeof(fsedge*)*HASHTAB_LOSIZE);
#endif
			passert(edgehashtab[i]);
			memset(edgehashtab[i],0,sizeof(fsedge*));
			if (edgehashtab[i][0]==NULL) {
				memset(edgehashtab[i],0,sizeof(fsedge*)*HASHTAB_LOSIZE);
			} else {
				for (hash=0 ; hash<HASHTAB_LOSIZE ; hash++) {
					edgehashtab[i][hash] = NULL;
				}
			}
		}
	}
	e->hashval = fsnodes_hash(e->parent->inode,e->nleng,e->name);
	hash = (e->hashval) & (edgehashsize-1);
	if (edgerehashpos<edgehashsize) {
		fsnodes_edge_hash_move();
		if (hash >= edgerehashpos) {
			hash -= edgehashsize/2;
		}
		e->next = edgehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		edgehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = e;
		edgehashelem++;
	} else {
		e->next = edgehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		edgehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = e;
		edgehashelem++;
		if (edgehashelem>edgehashsize && (edgehashsize>>HASHTAB_LOBITS)<HASHTAB_HISIZE) {
			fsnodes_edge_hash_rehash();
		}
	}
}

static inline int fsnodes_nameisused(fsnode *node,uint16_t nleng,const uint8_t *name) {
	return (fsnodes_edge_find(node,nleng,name))?1:0;
}

static inline fsedge* fsnodes_lookup(fsnode *node,uint16_t nleng,const uint8_t *name) {
	if (node->type!=TYPE_DIRECTORY) {
		return NULL;
	}
	return fsnodes_edge_find(node,nleng,name);
}

static inline void fsnodes_node_hash_init(void) {
	uint16_t i;
	nodehashsize = 0;
	nodehashelem = 0;
	noderehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		nodehashtab[i] = NULL;
	}
}

static inline void fsnodes_node_hash_cleanup(void) {
	uint16_t i;
	nodehashelem = 0;
	nodehashsize = 0;
	noderehashpos = 0;
	for (i=0 ; i<HASHTAB_HISIZE ; i++) {
		if (nodehashtab[i]!=NULL) {
#ifdef HAVE_MMAP
			munmap(nodehashtab[i],sizeof(fsnode*)*HASHTAB_LOSIZE);
#else
			free(nodehashtab[i]);
#endif
		}
		nodehashtab[i] = NULL;
	}
}

static inline void fsnodes_node_hash_rehash(void) {
	uint16_t i;
	noderehashpos = nodehashsize;
	nodehashsize *= 2;
	for (i=(nodehashsize>>HASHTAB_LOBITS)/2 ; i<nodehashsize>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
		nodehashtab[i] = mmap(NULL,sizeof(fsnode*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		nodehashtab[i] = malloc(sizeof(fsnode*)*HASHTAB_LOSIZE);
#endif
		passert(nodehashtab[i]);
	}
}

static inline void fsnodes_node_hash_move(void) {
	uint32_t hash;
	uint32_t mask;
	uint32_t moved=0;
	fsnode **phptr,**phptralt,*p;
	mask = nodehashsize-1;
	do {
		if (noderehashpos>=nodehashsize) { // rehash complete
			noderehashpos = nodehashsize;
			return;
		}
		phptr = nodehashtab[(noderehashpos - (nodehashsize/2)) >> HASHTAB_LOBITS] + (noderehashpos & HASHTAB_MASK);
		phptralt = nodehashtab[noderehashpos >> HASHTAB_LOBITS] + (noderehashpos & HASHTAB_MASK);
		*phptralt = NULL;
		while ((p=*phptr)!=NULL) {
			hash = hash32(p->inode) & mask;
			if (hash==noderehashpos) {
				*phptralt = p;
				*phptr = p->next;
				phptralt = &(p->next);
				p->next = NULL;
			} else {
				phptr = &(p->next);
			}
			moved++;
		}
		noderehashpos++;
	} while (moved<HASHTAB_MOVEFACTOR);
}

static inline fsnode* fsnodes_node_find(uint32_t inode) {
	fsnode *p;
	uint32_t hash;

	if (nodehashsize==0) {
		return NULL;
	}
	hash = hash32(inode) & (nodehashsize-1);
	if (noderehashpos<nodehashsize) {
		fsnodes_node_hash_move();
		if (hash >= noderehashpos) {
			hash -= nodehashsize/2;
		}
	}
	for (p=nodehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] ; p ; p=p->next) {
		if (p->inode==inode) {
			return p;
		}
	}
	return NULL;
}

static inline void fsnodes_node_delete(fsnode *p) {
	fsnode **phptr,*pit;
	uint32_t hash;

	if (nodehashsize==0) {
		return;
	}
	hash = hash32(p->inode) & (nodehashsize-1);
	if (noderehashpos<nodehashsize) {
		fsnodes_node_hash_move();
		if (hash >= noderehashpos) {
			hash -= nodehashsize/2;
		}
	}
	phptr = nodehashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	while ((pit=*phptr)!=NULL) {
		if (pit==p) {
			*phptr = p->next;
			nodehashelem--;
			return;
		}
		phptr = &(pit->next);
	}
}

static inline void fsnodes_node_add(fsnode *p) {
	uint16_t i;
	uint32_t hash;

	if (nodehashsize==0) {
		nodehashsize = fsnodes_calc_hash_size(hashelements);
		noderehashpos = nodehashsize;
		nodehashelem = 0;
		for (i=0 ; i<nodehashsize>>HASHTAB_LOBITS ; i++) {
#ifdef HAVE_MMAP
			nodehashtab[i] = mmap(NULL,sizeof(fsnode*)*HASHTAB_LOSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
			nodehashtab[i] = malloc(sizeof(fsnode*)*HASHTAB_LOSIZE);
#endif
			passert(nodehashtab[i]);
			memset(nodehashtab[i],0,sizeof(fsnode*));
			if (nodehashtab[i][0]==NULL) {
				memset(nodehashtab[i],0,sizeof(fsnode*)*HASHTAB_LOSIZE);
			} else {
				for (hash=0 ; hash<HASHTAB_LOSIZE ; hash++) {
					nodehashtab[i][hash] = NULL;
				}
			}
		}
	}
	hash = hash32(p->inode) & (nodehashsize-1);
	if (noderehashpos<nodehashsize) {
		fsnodes_node_hash_move();
		if (hash >= noderehashpos) {
			hash -= nodehashsize/2;
		}
		p->next = nodehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		nodehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = p;
		nodehashelem++;
	} else {
		p->next = nodehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK];
		nodehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] = p;
		nodehashelem++;
		if (nodehashelem>nodehashsize && (nodehashsize>>HASHTAB_LOBITS)<HASHTAB_HISIZE) {
			fsnodes_node_hash_rehash();
		}
	}
}




// returns 1 only if f is ancestor of p
static inline int fsnodes_isancestor(fsnode *f,fsnode *p) {
	fsedge *e;
	for (e=p->parents ; e ; e=e->nextparent) {	// check all parents of 'p' because 'p' can be any object, so it can be hardlinked
		p=e->parent;	// warning !!! since this point 'p' is used as temporary variable
		while (p) {
			if (f==p) {
				return 1;
			}
			if (p->parents) {
				massert(p->parents->nextparent==NULL,"directory has more than one parent !!!");
				p = p->parents->parent;	// here 'p' is always a directory so it should have only one parent
			} else {
				p = NULL;
			}
		}
	}
	return 0;
}

static inline void fsnodes_edgeid_init(void) {
	uint32_t h;
	edgeid_id_hashtab = malloc(sizeof(uint64_t)*EDGEID_HASHSIZE);
	edgeid_ptr_hashtab = malloc(sizeof(fsedge*)*EDGEID_HASHSIZE);
	passert(edgeid_id_hashtab);
	passert(edgeid_ptr_hashtab);
	for (h=0 ; h<EDGEID_HASHSIZE ; h++) {
		edgeid_id_hashtab[h] = 0;
		edgeid_ptr_hashtab[h] = NULL;
	}
}

static inline void fsnodes_edgeid_insert(fsedge *e) {
	uint32_t hashid;

	hashid = (e->edgeid) % EDGEID_HASHSIZE;
	edgeid_id_hashtab[hashid] = e->edgeid;
	edgeid_ptr_hashtab[hashid] = e;
}

static inline fsedge* fsnodes_edgeid_find(uint64_t edgeid) {
	uint32_t hashid;

	hashid = edgeid % EDGEID_HASHSIZE;
	if (edgeid_id_hashtab[hashid]==edgeid) {
		return edgeid_ptr_hashtab[hashid];
	}
	return NULL;
}

static inline void fsnodes_edgeid_remove(fsedge *e) {
	uint32_t hashid;

	hashid = (e->edgeid) % EDGEID_HASHSIZE;
	if (edgeid_id_hashtab[hashid]==e->edgeid) {
		edgeid_ptr_hashtab[hashid] = NULL;
	}
}







/* keep alive helper */

static uint64_t keep_alive_ts;
static uint32_t keep_alive_cnt;

static inline void fsnodes_keep_alive_begin(void) {
	keep_alive_ts = monotonic_useconds();
	keep_alive_cnt = 0;
}

static inline void fsnodes_keep_alive_check(void) {
	keep_alive_cnt++;
	if (keep_alive_cnt>=10000) {
		if (keep_alive_ts+100000<monotonic_useconds()) {
			main_keep_alive();
			keep_alive_ts = monotonic_useconds();
		}
		keep_alive_cnt = 0;
	}
}








static inline uint8_t fsnodes_type_convert(uint8_t type) {
	switch (type) {
		case DISP_TYPE_FILE:
			return TYPE_FILE;
		case DISP_TYPE_DIRECTORY:
			return TYPE_DIRECTORY;
		case DISP_TYPE_SYMLINK:
			return TYPE_SYMLINK;
		case DISP_TYPE_FIFO:
			return TYPE_FIFO;
		case DISP_TYPE_BLOCKDEV:
			return TYPE_BLOCKDEV;
		case DISP_TYPE_CHARDEV:
			return TYPE_CHARDEV;
		case DISP_TYPE_SOCKET:
			return TYPE_SOCKET;
		case DISP_TYPE_TRASH:
			return TYPE_TRASH;
		case DISP_TYPE_SUSTAINED:
			return TYPE_SUSTAINED;
	}
	return 0;
}







// quotas

static inline quotanode* fsnodes_new_quotanode(fsnode *p) {
	quotanode *qn;
	qn = quotanode_malloc();
	passert(qn);
	memset(qn,0,sizeof(quotanode));
	qn->next = quotahead;
	if (qn->next) {
		qn->next->prev = &(qn->next);
	}
	qn->prev = &(quotahead);
	quotahead = qn;
	qn->node = p;
	p->data.ddata.quota = qn;
	return qn;
}

static inline void fsnodes_delete_quotanode(fsnode *p) {
	quotanode *qn = p->data.ddata.quota;
	if (qn) {
		*(qn->prev) = qn->next;
		if (qn->next) {
			qn->next->prev = qn->prev;
		}
		quotanode_free(qn);
		p->data.ddata.quota = NULL;
	}
}

static inline void fsnodes_check_quotanode(quotanode *qn,uint32_t ts) {
	statsrecord *psr = &(qn->node->data.ddata.stats);
	uint8_t sq,chg,exceeded;
	sq=0;
	if (qn->flags&QUOTA_FLAG_SINODES) {
		if (psr->inodes>qn->sinodes) {
			sq = 1;
		}
	}
	if (qn->flags&QUOTA_FLAG_SLENGTH) {
		if (psr->length>qn->slength) {
			sq = 1;
		}
	}
	if (qn->flags&QUOTA_FLAG_SSIZE) {
		if (psr->size>qn->ssize) {
			sq = 1;
		}
	}
	if (qn->flags&QUOTA_FLAG_SREALSIZE) {
		if (psr->realsize>qn->srealsize) {
			sq = 1;
		}
	}
	chg = 0;
	if (sq==0 && qn->stimestamp>0) {
		qn->stimestamp = 0;
		chg = 1;
	} else if (sq && qn->stimestamp==0) {
		qn->stimestamp = ts;
		chg = 1;
	}
	exceeded = ((qn->stimestamp && qn->stimestamp+qn->graceperiod<ts))?1:0;
	if (qn->exceeded != exceeded) {
		qn->exceeded = exceeded;
		chg = 1;
	}
	if (chg) {
		changelog("%"PRIu32"|QUOTA(%"PRIu32",%"PRIu8",%"PRIu8",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",ts,qn->node->inode,qn->exceeded,qn->flags,qn->stimestamp,qn->sinodes,qn->hinodes,qn->slength,qn->hlength,qn->ssize,qn->hsize,qn->srealsize,qn->hrealsize,qn->graceperiod);
	}
}

void fsnodes_check_all_quotas(void) {
	quotanode *qn;
	uint32_t now;
	now = main_time();
		for (qn = quotahead ; qn ; qn=qn->next) {
			fsnodes_check_quotanode(qn,now);
		}
}

static inline uint8_t fsnodes_test_quota_noparents(fsnode *node,uint32_t inodes,uint64_t length,uint64_t size,uint64_t realsize) {
	statsrecord *psr;
	quotanode *qn;
	if (node && node->type==TYPE_DIRECTORY && (qn=node->data.ddata.quota)) {
		psr = &(node->data.ddata.stats);
		if (inodes>0 && (qn->flags&QUOTA_FLAG_HINODES)) {
			if (psr->inodes+inodes>qn->hinodes) {
				return 1;
			}
		}
		if (length>0 && (qn->flags&QUOTA_FLAG_HLENGTH)) {
			if (psr->length+length>qn->hlength) {
				return 1;
			}
		}
		if (size>0 && (qn->flags&QUOTA_FLAG_HSIZE)) {
			if (psr->size+size>qn->hsize) {
				return 1;
			}
		}
		if (realsize>0 && (qn->flags&QUOTA_FLAG_HREALSIZE)) {
			if (psr->realsize+realsize>qn->hrealsize) {
				return 1;
			}
		}
		if (qn->exceeded) { // soft exceeded
			if (inodes>0 && (qn->flags&QUOTA_FLAG_SINODES)) {
				if (psr->inodes+inodes>qn->sinodes) {
					return 1;
				}
			}
			if (length>0 && (qn->flags&QUOTA_FLAG_SLENGTH)) {
				if (psr->length+length>qn->slength) {
					return 1;
				}
			}
			if (size>0 && (qn->flags&QUOTA_FLAG_SSIZE)) {
				if (psr->size+size>qn->ssize) {
					return 1;
				}
			}
			if (realsize>0 && (qn->flags&QUOTA_FLAG_SREALSIZE)) {
				if (psr->realsize+realsize>qn->srealsize) {
					return 1;
				}
			}
		}
	}
	return 0;
}

static inline uint8_t fsnodes_test_quota(fsnode *node,uint32_t inodes,uint64_t length,uint64_t size,uint64_t realsize) {
	fsedge *e;
	if (fsnodes_test_quota_noparents(node,inodes,length,size,realsize)) {
		return 1;
	}
	if (node && node!=root) {
		for (e=node->parents ; e ; e=e->nextparent) {
			if (fsnodes_test_quota(e->parent,inodes,length,size,realsize)) {
				return 1;
			}
		}
	}
	return 0;
}

static inline uint8_t fsnodes_test_quota_for_uncommon_nodes(fsnode *dstnode,fsnode *srcnode,uint32_t inodes,uint64_t length,uint64_t size,uint64_t realsize) {
	fsedge *e;
	struct _node_list {
		fsnode *node;
		struct _node_list *next;
	} *dhead,*shead,*nlptr;
	uint8_t ret = 0;

	if (dstnode==srcnode) {
		return 0;
	}
	dhead = NULL;
	while (dstnode!=NULL) {
		if (dstnode->data.ddata.quota!=NULL) {
			nlptr = malloc(sizeof(struct _node_list));
			passert(nlptr);
			nlptr->node = dstnode;
			nlptr->next = dhead;
			dhead = nlptr;
		}
		e = dstnode->parents;
		if (e!=NULL) {
			massert(e->nextparent==NULL,"directory has more than one parent !!!");
			dstnode = e->parent;
		} else {
			dstnode = NULL;
		}
	}
	shead = NULL;
	while (srcnode!=NULL) {
		if (srcnode->data.ddata.quota!=NULL) {
			nlptr = malloc(sizeof(struct _node_list));
			passert(nlptr);
			nlptr->node = srcnode;
			nlptr->next = shead;
			shead = nlptr;
		}
		e = srcnode->parents;
		if (e!=NULL) {
			massert(e->nextparent==NULL,"directory has more than one parent !!!");
			srcnode = e->parent;
		} else {
			srcnode = NULL;
		}
	}
	while (shead!=NULL && dhead!=NULL && shead->node==dhead->node) { // skip common nodes
		nlptr = shead;
		shead = shead->next;
		free(nlptr);
		nlptr = dhead;
		dhead = dhead->next;
		free(nlptr);
	}
	while (shead!=NULL) { // ignore other source nodes
		nlptr = shead;
		shead = shead->next;
		free(nlptr);
	}
	while (dhead!=NULL) { // check quota for other destination nodes
		if (fsnodes_test_quota(dhead->node,inodes,length,size,realsize)) {
			ret = 1;
		}
		nlptr = dhead;
		dhead = dhead->next;
		free(nlptr);
	}
	return ret;
}







// stats
static inline void fsnodes_fix_realsize(fsnode *parent,uint64_t realsize_diff) {
	statsrecord *psr;
	fsedge *e;
	if (parent) {
		psr = &(parent->data.ddata.stats);
		psr->realsize += realsize_diff;
		if (parent!=root) {
			for (e=parent->parents ; e ; e=e->nextparent) {
				fsnodes_fix_realsize(e->parent,realsize_diff);
			}
		}
	}
}

static inline void fsnodes_check_realsize(fsnode *node) {
	uint32_t i,lastchunk,lastchunksize;
	uint64_t size;
	fsedge *e;
	uint64_t new_realsize;
	uint64_t old_realsize;
	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		uint8_t realsize_ratio = sclass_get_keepmax_goal(node->sclassid);
		if (realsize_ratio != node->data.fdata.realsize_ratio) {
			size = 0;
			if (node->data.fdata.length>0) {
				lastchunk = (node->data.fdata.length-1)>>MFSCHUNKBITS;
				lastchunksize = ((((node->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
			} else {
				lastchunk = 0;
				lastchunksize = MFSHDRSIZE;
			}
			for (i=0 ; i<node->data.fdata.chunks ; i++) {
				if (node->data.fdata.chunktab[i]>0) {
					if (i<lastchunk) {
						size+=MFSCHUNKSIZE+MFSHDRSIZE;
					} else if (i==lastchunk) {
						size+=lastchunksize;
					}
				}
			}
			new_realsize = size * realsize_ratio;
			old_realsize = size * node->data.fdata.realsize_ratio;
			for (e=node->parents ; e ; e=e->nextparent) {
				fsnodes_fix_realsize(e->parent,new_realsize-old_realsize);
			}
			node->data.fdata.realsize_ratio = realsize_ratio;
		}
	}
}

static inline void fsnodes_get_stats(fsnode *node,statsrecord *sr,uint8_t fix_realsize_ratio) {
	uint32_t i,lastchunk,lastchunksize;
	switch (node->type) {
	case TYPE_DIRECTORY:
		*sr = node->data.ddata.stats;
		sr->inodes++;
		sr->dirs++;
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
		sr->inodes = 1;
		sr->dirs = 0;
		sr->files = 1;
		sr->chunks = 0;
		sr->length = node->data.fdata.length;
		sr->size = 0;
		if (node->data.fdata.length>0) {
			lastchunk = (node->data.fdata.length-1)>>MFSCHUNKBITS;
			lastchunksize = ((((node->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastchunk = 0;
			lastchunksize = MFSHDRSIZE;
		}
		for (i=0 ; i<node->data.fdata.chunks ; i++) {
			if (node->data.fdata.chunktab[i]>0) {
				if (i<lastchunk) {
					sr->size+=MFSCHUNKSIZE+MFSHDRSIZE;
				} else if (i==lastchunk) {
					sr->size+=lastchunksize;
				}
				sr->chunks++;
			}
		}
		if (fix_realsize_ratio==2) {
			uint8_t realsize_ratio = sclass_get_keepmax_goal(node->sclassid);
			if (realsize_ratio != node->data.fdata.realsize_ratio) {
				fsedge *e;
				uint64_t new_realsize = sr->size * realsize_ratio;
				uint64_t old_realsize = sr->size * node->data.fdata.realsize_ratio;
				for (e=node->parents ; e ; e=e->nextparent) {
					fsnodes_fix_realsize(e->parent,new_realsize-old_realsize);
				}
				node->data.fdata.realsize_ratio = realsize_ratio;
			}
		} else if (fix_realsize_ratio) {
			node->data.fdata.realsize_ratio = sclass_get_keepmax_goal(node->sclassid);
		}
		sr->realsize = sr->size * node->data.fdata.realsize_ratio;
		break;
	case TYPE_SYMLINK:
		sr->inodes = 1;
		sr->files = 0;
		sr->dirs = 0;
		sr->chunks = 0;
		sr->length = node->data.sdata.pleng;
		sr->size = 0;
		sr->realsize = 0;
		break;
	default:
		sr->inodes = 1;
		sr->files = 0;
		sr->dirs = 0;
		sr->chunks = 0;
		sr->length = 0;
		sr->size = 0;
		sr->realsize = 0;
	}
}

static inline void fsnodes_sub_stats(fsnode *parent,statsrecord *sr) {
	statsrecord *psr;
	fsedge *e;
	if (parent) {
		psr = &(parent->data.ddata.stats);
		psr->inodes -= sr->inodes;
		psr->dirs -= sr->dirs;
		psr->files -= sr->files;
		psr->chunks -= sr->chunks;
		psr->length -= sr->length;
		psr->size -= sr->size;
		psr->realsize -= sr->realsize;
		if (parent!=root) {
			for (e=parent->parents ; e ; e=e->nextparent) {
				fsnodes_sub_stats(e->parent,sr);
			}
		}
	}
}

static inline void fsnodes_add_stats(fsnode *parent,statsrecord *sr) {
	statsrecord *psr;
	fsedge *e;
	if (parent) {
		psr = &(parent->data.ddata.stats);
		psr->inodes += sr->inodes;
		psr->dirs += sr->dirs;
		psr->files += sr->files;
		psr->chunks += sr->chunks;
		psr->length += sr->length;
		psr->size += sr->size;
		psr->realsize += sr->realsize;
		if (parent!=root) {
			for (e=parent->parents ; e ; e=e->nextparent) {
				fsnodes_add_stats(e->parent,sr);
			}
		}
	}
}

static inline void fsnodes_add_sub_stats(fsnode *parent,statsrecord *newsr,statsrecord *prevsr) {
	statsrecord sr;
	sr.inodes = newsr->inodes - prevsr->inodes;
	sr.dirs = newsr->dirs - prevsr->dirs;
	sr.files = newsr->files - prevsr->files;
	sr.chunks = newsr->chunks - prevsr->chunks;
	sr.length = newsr->length - prevsr->length;
	sr.size = newsr->size - prevsr->size;
	sr.realsize = newsr->realsize - prevsr->realsize;
	fsnodes_add_stats(parent,&sr);
}






static inline void fsnodes_quota_fixspace(fsnode *node,uint64_t *totalspace,uint64_t *availspace) {
	quotanode *qn;
	fsedge *e;
	statsrecord sr;
	uint64_t quotasize;
	if (node && node->type==TYPE_DIRECTORY && (qn=node->data.ddata.quota) && (qn->flags&(QUOTA_FLAG_HREALSIZE|QUOTA_FLAG_SREALSIZE|QUOTA_FLAG_HSIZE|QUOTA_FLAG_SSIZE|QUOTA_FLAG_HLENGTH|QUOTA_FLAG_SLENGTH))) {
		fsnodes_get_stats(node,&sr,2);
		if (qn->flags&(QUOTA_FLAG_HREALSIZE|QUOTA_FLAG_SREALSIZE)) {
			quotasize = UINT64_C(0xFFFFFFFFFFFFFFFF);
			if ((qn->flags&QUOTA_FLAG_HREALSIZE) && quotasize > qn->hrealsize) {
				quotasize = qn->hrealsize;
			}
			if ((qn->flags&QUOTA_FLAG_SREALSIZE) && quotasize > qn->srealsize) {
				quotasize = qn->srealsize;
			}
			if (sr.realsize >= quotasize) {
				*availspace = 0;
			} else if (*availspace > quotasize - sr.realsize) {
				*availspace = quotasize - sr.realsize;
			}
			if (*totalspace > quotasize) {
				*totalspace = quotasize;
			}
			if (sr.realsize + *availspace < *totalspace) {
				*totalspace = sr.realsize + *availspace;
			}
		}
		if (qn->flags&(QUOTA_FLAG_HSIZE|QUOTA_FLAG_SSIZE)) {
			quotasize = UINT64_C(0xFFFFFFFFFFFFFFFF);
			if ((qn->flags&QUOTA_FLAG_HSIZE) && quotasize > qn->hsize) {
				quotasize = qn->hsize;
			}
			if ((qn->flags&QUOTA_FLAG_SSIZE) && quotasize > qn->ssize) {
				quotasize = qn->ssize;
			}
			if (sr.size >= quotasize) {
				*availspace = 0;
			} else if (*availspace > quotasize - sr.size) {
				*availspace = quotasize - sr.size;
			}
			if (*totalspace > quotasize) {
				*totalspace = quotasize;
			}
			if (sr.size + *availspace < *totalspace) {
				*totalspace = sr.size + *availspace;
			}
		}
		if (qn->flags&(QUOTA_FLAG_HLENGTH|QUOTA_FLAG_SLENGTH)) {
			quotasize = UINT64_C(0xFFFFFFFFFFFFFFFF);
			if ((qn->flags&QUOTA_FLAG_HLENGTH) && quotasize > qn->hlength) {
				quotasize = qn->hlength;
			}
			if ((qn->flags&QUOTA_FLAG_SLENGTH) && quotasize > qn->slength) {
				quotasize = qn->slength;
			}
			if (sr.length >= quotasize) {
				*availspace = 0;
			} else if (*availspace > quotasize - sr.length) {
				*availspace = quotasize - sr.length;
			}
			if (*totalspace > quotasize) {
				*totalspace = quotasize;
			}
			if (sr.length + *availspace < *totalspace) {
				*totalspace = sr.length + *availspace;
			}
		}
	}
	if (node && node!=root) {
		for (e=node->parents ; e ; e=e->nextparent) {
			fsnodes_quota_fixspace(e->parent,totalspace,availspace);
		}
	}
}






static inline uint8_t fsnodes_accessmode(fsnode *node,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t sesflags) {
	static uint8_t modetoaccmode[8] = MODE_TO_ACCMODE;
	if (uid==0) {
		return modetoaccmode[0x7];
	}
	if (node->aclpermflag) {
		return posix_acl_accmode(node->inode,uid,gids,gid,node->uid,node->gid);
	} else if (uid==node->uid || (node->eattr&EATTR_NOOWNER)) {
		return modetoaccmode[((node->mode)>>6) & 7];
	} else if (sesflags&SESFLAG_IGNOREGID) {
		return modetoaccmode[(((node->mode)>>3) | (node->mode)) & 7];
	} else {
		while (gids>0) {
			gids--;
			if (gid[gids]==node->gid) {
				return modetoaccmode[((node->mode)>>3) & 7];
			}
		}
		return modetoaccmode[(node->mode & 7)];
	}
}

static inline int fsnodes_access_ext(fsnode *node,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t modemask,uint8_t sesflags) {
	return (fsnodes_accessmode(node,uid,gids,gid,sesflags) & (1 << (modemask&0x7)))?1:0;
}

static inline int fsnodes_sticky_access(fsnode *parent,fsnode *node,uint32_t uid) {
	if (uid==0 || (parent->mode&01000)==0) {	// super user or sticky bit is not set
		return 1;
	}
	if (uid==parent->uid || (parent->eattr&EATTR_NOOWNER) || uid==node->uid || (node->eattr&EATTR_NOOWNER)) {
		return 1;
	}
	return 0;
}

static inline uint32_t fsnodes_nlink(uint32_t rootinode,fsnode *node) {
	fsedge *e;
	fsnode *p;
	uint32_t nlink;
	nlink = 0;
	if (node->inode!=rootinode) {
		if (rootinode==MFS_ROOT_ID) {
			for (e=node->parents ; e ; e=e->nextparent) {
				nlink++;
			}
		} else {
			for (e=node->parents ; e ; e=e->nextparent) {
				p = e->parent;
				while (p) {
					if (rootinode==p->inode) {
						nlink++;
						p = NULL;
					} else if (p->parents) {
						p = p->parents->parent;
					} else {
						p = NULL;
					}
				}
			}
		}
	}
	return nlink;
}

static inline void fsnodes_get_parents(uint32_t rootinode,fsnode *node,uint8_t *buff) {
	fsedge *e;
	fsnode *p;
	if (node->inode!=rootinode) {
		if (rootinode==MFS_ROOT_ID) {
			for (e=node->parents ; e ; e=e->nextparent) {
				put32bit(&buff,e->parent->inode);
			}
		} else {
			for (e=node->parents ; e ; e=e->nextparent) {
				p = e->parent;
				while (p) {
					if (rootinode==p->inode) {
						if (e->parent->inode==rootinode) {
							put32bit(&buff,MFS_ROOT_ID);
						} else {
							put32bit(&buff,e->parent->inode);
						}
						p = NULL;
					} else if (p->parents) {
						p = p->parents->parent;
					} else {
						p = NULL;
					}
				}
			}
		}
	}
}

static inline uint32_t fsnodes_get_paths_size(uint32_t rootinode,fsnode *node) {
	fsedge *e;
	fsnode *p;
	uint32_t totalpsize;
	uint32_t psize;

	totalpsize = 0;
	if (node->inode!=rootinode) {
		for (e=node->parents ; e ; e=e->nextparent) {
			psize = e->nleng;
			p = e->parent;
			while (p) {
				if (rootinode==p->inode) {
					totalpsize += psize + 4;
					p = NULL;
				} else if (p->parents) {
					psize += p->parents->nleng + 1;
					p = p->parents->parent;
				} else {
					p = NULL;
				}
			}
		}
	} else {
		return 5;
	}
	return totalpsize;
}

static inline void fsnodes_get_paths_data(uint32_t rootinode,fsnode *node,uint8_t *buff) {
	fsedge *e;
	fsnode *p;
	uint32_t psize;
	uint8_t *b;

	if (node->inode!=rootinode) {
		for (e=node->parents ; e ; e=e->nextparent) {
			psize = e->nleng;
			p = e->parent;
			while (p) {
				if (rootinode==p->inode) {
					put32bit(&buff,psize);
					b = buff;
					buff += psize;

					psize -= e->nleng;
					memcpy(b+psize,e->name,e->nleng);
					p = e->parent;
					while (p) {
						if (rootinode==p->inode) {
							p = NULL;
						} else if (p->parents) {
							psize--;
							b[psize] = '/';
							psize -= p->parents->nleng;
							memcpy(b+psize,p->parents->name,p->parents->nleng);
							p = p->parents->parent;
						} else {
							p = NULL;
						}
					}
				} else if (p->parents) {
					psize += p->parents->nleng + 1;
					p = p->parents->parent;
				} else {
					p = NULL;
				}
			}
		}
	} else {
		put32bit(&buff,1);
		*buff = '/';
		return;
	}
}

static inline void fsnodes_fill_attr(fsnode *node,fsnode *parent,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t sesflags,uint8_t attr[ATTR_RECORD_SIZE],uint8_t addwinattr) {
	uint8_t *ptr;
	uint8_t type;
	uint8_t flags;
	uint16_t mode;
	uint64_t dleng;

	(void)sesflags;
	ptr = attr;
	type = node->type;
	if (type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		type = TYPE_FILE;
	}
	flags = 0;
	if (parent) {
		if (parent->eattr&EATTR_NOECACHE) {
			flags |= MATTR_NOECACHE;
		}
	}
	if ((node->eattr&(EATTR_NOOWNER|EATTR_NOACACHE)) || (sesflags&SESFLAG_MAPALL)) {
		flags |= MATTR_NOACACHE;
	}
	if ((node->eattr&EATTR_NODATACACHE)==0) {
		flags |= MATTR_ALLOWDATACACHE;
	} else {
		flags |= MATTR_DIRECTMODE;
	}
	if (node->xattrflag==0 && node->aclpermflag==0 && node->acldefflag==0) {
		flags |= MATTR_NOXATTR;
	}
	if (node->aclpermflag) {
		mode = (posix_acl_getmode(node->inode) & 0777) | (node->mode & 07000);
	} else {
		mode = node->mode & 07777;
	}
	if ((node->eattr&EATTR_NOOWNER) && uid!=0) {
		// copy owner rights to group and other
		mode &= 07700;
		mode |= (mode&0700)>>3;
		mode |= (mode&0700)>>6;
		if (sesflags&SESFLAG_MAPALL) {
			uid = auid;
			gid = agid;
		}
	} else {
		if (sesflags&SESFLAG_MAPALL && auid!=0) {
			if (node->uid==uid) {
				uid = auid;
			} else {
				uid = 0;
			}
			if (node->gid==gid) {
				gid = agid;
			} else {
				gid = 0;
			}
		} else {
			uid = node->uid;
			gid = node->gid;
		}
	}
	if (sesflags&SESFLAG_ATTRBIT) {
		put8bit(&ptr,flags);
		mode |= (((uint16_t)type)<<12);
		put16bit(&ptr,mode);
	} else {
		put8bit(&ptr,DISP_TYPE_REMAP_STR[type]);
		mode |= (((uint16_t)flags)<<12);
		put16bit(&ptr,mode);
	}
	put32bit(&ptr,uid);
	put32bit(&ptr,gid);
	put32bit(&ptr,node->atime);
	put32bit(&ptr,node->mtime);
	put32bit(&ptr,node->ctime);
	switch (node->type) {
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
		put32bit(&ptr,node->data.fdata.nlink);
		put64bit(&ptr,node->data.fdata.length);
		break;
	case TYPE_DIRECTORY:
		dleng = node->data.ddata.stats.length;
		/* make 'floating-point' dsize (must be 32-bit because of Linux)
		 * examples:
		 *    1200 =  12.00 B
		 * 1023443 = 234.43 kB
		 * 2052312 = 523.12 MB
		 * 3001298 =  12.98 GB
		 * 4001401 =  14.01 TB
		 */
		if (dleng==0) { // never return size 0 for directories
			dleng = 1;
		} else if (dleng<UINT64_C(0x400)) {
			dleng *= 100;
		} else if (dleng<UINT64_C(0x100000)) {
			dleng *= 100;
			dleng >>= 10;
			dleng += 1000000;
		} else if (dleng<UINT64_C(0x40000000)) {
			dleng *= 100;
			dleng >>= 20;
			dleng += 2000000;
		} else if (dleng<UINT64_C(0x10000000000)) {
			dleng *= 100;
			dleng >>= 30;
			dleng += 3000000;
		} else if (dleng<UINT64_C(0x4000000000000)) {
			dleng *= 100;
			dleng >>= 40;
			dleng += 4000000;
		} else if (dleng<UINT64_C(0x1000000000000000)) {
			dleng >>= 10; // overflow !!!
			dleng *= 100;
			dleng >>= 40;
			dleng += 5000000;
		} else {
			dleng >>= 10;
			dleng *= 100;
			dleng >>= 50;
			dleng += 6000000;
		}

		put32bit(&ptr,node->data.ddata.nlink);
		put64bit(&ptr,dleng);
		break;
	case TYPE_SYMLINK:
		put32bit(&ptr,node->data.sdata.nlink);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		put32bit(&ptr,node->data.sdata.pleng);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr,node->data.devdata.nlink);
		put32bit(&ptr,node->data.devdata.rdev);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		break;
	default:
		put32bit(&ptr,node->data.odata.nlink);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
	}
	if (addwinattr) {
		put8bit(&ptr,node->winattr);
	}
}

static inline void fsnodes_remove_edge(uint32_t ts,fsedge *e) {
	statsrecord sr;
	if (e->parent) {
		fsnodes_edgeid_remove(e);
		fsnodes_get_stats(e->child,&sr,0);
		fsnodes_sub_stats(e->parent,&sr);
		e->parent->mtime = e->parent->ctime = ts;
		e->parent->data.ddata.elements--;
		switch (e->child->type) {
			case TYPE_FILE:
			case TYPE_TRASH:
			case TYPE_SUSTAINED:
				// TRASH and SUSTAINED here are only pro forma and to avoid potential future bugs
				e->child->data.fdata.nlink--;
				break;
			case TYPE_DIRECTORY:
				// directories doesn't have hard links - nlink here is calculated differently
				e->parent->data.ddata.nlink--;
				break;
			case TYPE_SYMLINK:
				e->child->data.sdata.nlink--;
				break;
			case TYPE_BLOCKDEV:
			case TYPE_CHARDEV:
				e->child->data.devdata.nlink--;
				break;
			default:
				e->child->data.odata.nlink--;
				break;
		}
		e->parent->eattr &= ~(EATTR_SNAPSHOT);
	}
	if (ts>0 && e->child) {
		e->child->ctime = ts;
	}
	*(e->prevchild) = e->nextchild;
	if (e->nextchild) {
		e->nextchild->prevchild = e->prevchild;
	}
	*(e->prevparent) = e->nextparent;
	if (e->nextparent) {
		e->nextparent->prevparent = e->prevparent;
	}
	if (e->parent) {
		fsnodes_edge_delete(e);
	}

	fsedge_free(e,e->nleng);
}

static inline void fsnodes_link(uint32_t ts,fsnode *parent,fsnode *child,uint16_t nleng,const uint8_t *name) {
	fsedge *e;
	statsrecord sr;

	e = fsedge_malloc(nleng);
	passert(e);
	if (nextedgeid<EDGEID_MAX) {
		e->edgeid = nextedgeid--;
	} else {
		e->edgeid = 0;
	}
	e->nleng = nleng;
	memcpy((uint8_t*)(e->name),name,nleng);
	e->child = child;
	e->parent = parent;
	e->nextchild = parent->data.ddata.children;
	if (e->nextchild) {
		e->nextchild->prevchild = &(e->nextchild);
	}
	parent->data.ddata.children = e;
	e->prevchild = &(parent->data.ddata.children);
	e->nextparent = child->parents;
	if (e->nextparent) {
		e->nextparent->prevparent = &(e->nextparent);
	}
	child->parents = e;
	e->prevparent = &(child->parents);
	fsnodes_edge_add(e);

	parent->data.ddata.elements++;
	switch (child->type) {
		case TYPE_FILE:
		case TYPE_TRASH:
		case TYPE_SUSTAINED:
			// TRASH and SUSTAINED here are only pro forma and to avoid potential future bugs
			child->data.fdata.nlink++;
			break;
		case TYPE_DIRECTORY:
			// directories doesn't have hard links - nlink here is calculated differently
			parent->data.ddata.nlink++;
			break;
		case TYPE_SYMLINK:
			child->data.sdata.nlink++;
			break;
		case TYPE_BLOCKDEV:
		case TYPE_CHARDEV:
			child->data.devdata.nlink++;
			break;
		default:
			child->data.odata.nlink++;
			break;
	}
	parent->eattr &= ~(EATTR_SNAPSHOT);
	fsnodes_get_stats(child,&sr,1);
	fsnodes_add_stats(parent,&sr);
	if (ts>0) {
		parent->mtime = parent->ctime = ts;
		child->ctime = ts;
	}
}

static inline fsnode* fsnodes_create_node(uint32_t ts,fsnode* node,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gid,uint8_t copysgid) {
	fsnode *p;
	uint8_t aclcopied;
	switch (type) {
		case TYPE_DIRECTORY:
			p = fsnode_dir_malloc();
			break;
		case TYPE_FILE:
		case TYPE_TRASH:
		case TYPE_SUSTAINED:
			p = fsnode_file_malloc();
			break;
		case TYPE_SYMLINK:
			p = fsnode_symlink_malloc();
			break;
		case TYPE_BLOCKDEV:
		case TYPE_CHARDEV:
			p = fsnode_dev_malloc();
			break;
		default:
			p = fsnode_other_malloc();
	}
	passert(p);
	nodes++;
	if (type==TYPE_DIRECTORY) {
		dirnodes++;
	}
	if (type==TYPE_FILE) {
		filenodes++;
	}
	p->inode = fsnodes_get_next_id();
	p->xattrflag = 0;
	p->aclpermflag = 0;
	p->acldefflag = 0;
	p->type = type;
	p->ctime = p->mtime = p->atime = ts;
	if (type==TYPE_DIRECTORY || type==TYPE_FILE) {
		p->sclassid = node->sclassid;
		sclass_incref(p->sclassid,p->type);
		p->trashtime = node->trashtime;
	} else {
		p->sclassid = 0;
		sclass_incref(p->sclassid,p->type);
		p->trashtime = DEFAULT_TRASHTIME;
	}
	if (type==TYPE_DIRECTORY) {
		p->eattr = node->eattr & ~(EATTR_SNAPSHOT);
	} else {
		p->eattr = node->eattr & ~(EATTR_NOECACHE|EATTR_SNAPSHOT);
	}
	p->winattr = 0;
	if (node->acldefflag) {
		aclcopied = posix_acl_copydefaults(node->inode,p->inode,(type==TYPE_DIRECTORY)?1:0,&mode);
		p->mode = mode;
	} else {
		aclcopied = 0;
		p->mode = mode & ~cumask;
	}
	p->uid = uid;
	if ((node->mode&02000)==02000) {	// set gid flag is set in the parent directory ?
		p->gid = node->gid;
		if (copysgid && type==TYPE_DIRECTORY) {
			p->mode |= 02000;
		}
	} else {
		p->gid = gid;
	}
	switch (type) {
	case TYPE_DIRECTORY:
		memset(&(p->data.ddata.stats),0,sizeof(statsrecord));
		p->data.ddata.quota = NULL;
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
	case TYPE_FILE:
		p->data.fdata.length = 0;
		p->data.fdata.chunks = 0;
		p->data.fdata.chunktab = NULL;
		p->data.fdata.nlink = 0;
		break;
	case TYPE_SYMLINK:
		p->data.sdata.pleng = 0;
		p->data.sdata.path = NULL;
		p->data.sdata.nlink = 0;
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = 0;
		p->data.devdata.nlink = 0;
		break;
	default:
		p->data.odata.nlink = 0;
	}
	p->parents = NULL;
	fsnodes_node_add(p);
	fsnodes_link(ts,node,p,nleng,name);
	if (aclcopied&1) {
		p->aclpermflag = 1;
	}
	if (aclcopied&2) {
		p->acldefflag = 1;
	}
	return p;
}

static inline uint32_t fsnodes_getpath_size(fsedge *e) {
	uint32_t size;
	fsnode *p;
	if (e==NULL) {
		return 0;
	}
	p = e->parent;
	size = e->nleng;
	while (p!=root && p->parents) {
		size += p->parents->nleng+1;
		p = p->parents->parent;
	}
	return size;
}

static inline void fsnodes_getpath_data(fsedge *e,uint8_t *path,uint32_t size) {
	fsnode *p;
	if (e==NULL) {
		return;
	}
	if (size>=e->nleng) {
		size-=e->nleng;
		memcpy(path+size,e->name,e->nleng);
	} else if (size>0) {
		memcpy(path,e->name+(e->nleng-size),size);
		size=0;
	}
	if (size>0) {
		path[--size]='/';
	}
	p = e->parent;
	while (p!=root && p->parents) {
		if (size>=p->parents->nleng) {
			size-=p->parents->nleng;
			memcpy(path+size,p->parents->name,p->parents->nleng);
		} else if (size>0) {
			memcpy(path,p->parents->name+(p->parents->nleng-size),size);
			size=0;
		}
		if (size>0) {
			path[--size]='/';
		}
		p = p->parents->parent;
	}
}

static inline void fsnodes_getpath(fsedge *e,uint16_t *pleng,uint8_t **path) {
	uint32_t size;
	uint8_t *ret;
	fsnode *p;

	p = e->parent;
	size = e->nleng;
	while (p!=root && p->parents) {
		size += p->parents->nleng+1;	// get first parent !!!
		p = p->parents->parent;		// when folders can be hardlinked it's the only way to obtain path (one of them)
	}
	if (size>MFS_PATH_MAX) {
		syslog(LOG_WARNING,"path too long !!! - truncate");
		size=MFS_PATH_MAX;
	}
	*pleng = size;
	ret = malloc(size);
	passert(ret);
	size -= e->nleng;
	memcpy(ret+size,e->name,e->nleng);
	if (size>0) {
		ret[--size]='/';
	}
	p = e->parent;
	while (p!=root && p->parents) {
		if (size>=p->parents->nleng) {
			size-=p->parents->nleng;
			memcpy(ret+size,p->parents->name,p->parents->nleng);
		} else {
			if (size>0) {
				memcpy(ret,p->parents->name+(p->parents->nleng-size),size);
				size=0;
			}
		}
		if (size>0) {
			ret[--size]='/';
		}
		p = p->parents->parent;
	}
	*path = ret;
}

static inline uint32_t fsnodes_getdetached(fsedge *start,uint8_t *dbuff) {
	fsedge *e;
	const uint8_t *sptr;
	uint8_t c;
	uint32_t result=0;
	for (e = start ; e ; e=e->nextchild) {
		if (e->nleng>240) {
			if (dbuff!=NULL) {
				*dbuff=240;
				dbuff++;
				memcpy(dbuff,"(...)",5);
				dbuff+=5;
				sptr = e->name+(e->nleng-235);
				for (c=0 ; c<235 ; c++) {
					if (*sptr=='/') {
						*dbuff='|';
					} else {
						*dbuff = *sptr;
					}
					sptr++;
					dbuff++;
				}
			}
			result+=245;
		} else {
			if (dbuff!=NULL) {
				*dbuff=e->nleng;
				dbuff++;
				sptr = e->name;
				for (c=0 ; c<e->nleng ; c++) {
					if (*sptr=='/') {
						*dbuff='|';
					} else {
						*dbuff = *sptr;
					}
					sptr++;
					dbuff++;
				}
			}
			result+=5+e->nleng;
		}
		if (dbuff!=NULL) {
			put32bit(&dbuff,e->child->inode);
		}
	}
	return result;
}

static inline uint32_t fsnodes_readdirsize(fsnode *p,fsedge *e,uint32_t maxentries,uint64_t nedgeid,uint8_t attrmode) {
	uint32_t result = 0;
	uint8_t attrsize = (attrmode==0)?1:(attrmode==1)?35:ATTR_RECORD_SIZE;
	while (maxentries>0 && nedgeid<EDGEID_MAX) {
		if (nedgeid==0) {
			result += (attrsize+5)+1; // self ('.')
			nedgeid=1;
		} else {
			if (nedgeid==1) {
				result += (attrsize+5)+2; // parent ('..')
				e = p->data.ddata.children;
			} else if (e) {
				result += (attrsize+5)+e->nleng;
				e = e->nextchild;
			}
			if (e) {
				nedgeid = e->edgeid;
			} else {
				nedgeid = EDGEID_MAX;
			}
		}
		maxentries--;
	}
	return result;
}

static inline void fsnodes_readdirdata(uint32_t rootinode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t sesflags,fsnode *p,fsedge *e,uint32_t maxentries,uint64_t *nedgeidp,uint8_t *dbuff,uint8_t attrmode) {
	uint64_t nedgeid = *nedgeidp;
	while (maxentries>0 && nedgeid<EDGEID_MAX) {
		if (nedgeid==0) {
			dbuff[0]=1;
			dbuff[1]='.';
			dbuff+=2;
			if (p->inode!=rootinode) {
				put32bit(&dbuff,p->inode);
			} else {
				put32bit(&dbuff,MFS_ROOT_ID);
			}
			if (attrmode==2) {
				fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff,1);
				dbuff+=ATTR_RECORD_SIZE;
			} else if (attrmode==1) {
				fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff,0);
				dbuff+=35;
			} else if (sesflags&SESFLAG_ATTRBIT) {
				put8bit(&dbuff,TYPE_DIRECTORY);
			} else {
				put8bit(&dbuff,'d');
			}
			nedgeid = 1;
		} else {
			if (nedgeid==1) {
				dbuff[0]=2;
				dbuff[1]='.';
				dbuff[2]='.';
				dbuff+=3;
				if (p->inode==rootinode) { // root node should returns self as its parent
					put32bit(&dbuff,MFS_ROOT_ID);
					if (attrmode==2) {
						fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff,1);
						dbuff+=ATTR_RECORD_SIZE;
					} else if (attrmode==1) {
						fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff,0);
						dbuff+=35;
					} else if (sesflags&SESFLAG_ATTRBIT) {
						put8bit(&dbuff,TYPE_DIRECTORY);
					} else {
						put8bit(&dbuff,'d');
					}
				} else {
					if (p->parents && p->parents->parent->inode!=rootinode) {
						put32bit(&dbuff,p->parents->parent->inode);
					} else {
						put32bit(&dbuff,MFS_ROOT_ID);
					}
					if (attrmode) {
						if (p->parents) {
							if (attrmode==2) {
								fsnodes_fill_attr(p->parents->parent,p,uid,gid,auid,agid,sesflags,dbuff,1);
							} else {
								fsnodes_fill_attr(p->parents->parent,p,uid,gid,auid,agid,sesflags,dbuff,0);
							}
						} else {
							if (rootinode==MFS_ROOT_ID) {
								if (attrmode==2) {
									fsnodes_fill_attr(root,p,uid,gid,auid,agid,sesflags,dbuff,1);
								} else {
									fsnodes_fill_attr(root,p,uid,gid,auid,agid,sesflags,dbuff,0);
								}
							} else {
								fsnode *rn = fsnodes_node_find(rootinode);
								if (rn) {	// it should be always true because it's checked before, but better check than sorry
									if (attrmode==2) {
										fsnodes_fill_attr(rn,p,uid,gid,auid,agid,sesflags,dbuff,1);
									} else {
										fsnodes_fill_attr(rn,p,uid,gid,auid,agid,sesflags,dbuff,0);
									}
								} else {
									if (attrmode==2) {
										memset(dbuff,0,ATTR_RECORD_SIZE);
									} else{
										memset(dbuff,0,35);
									}
								}
							}
						}
						dbuff+=(attrmode==2)?ATTR_RECORD_SIZE:35;
					} else if (sesflags&SESFLAG_ATTRBIT) {
						put8bit(&dbuff,TYPE_DIRECTORY);
					} else {
						put8bit(&dbuff,'d');
					}
				}
				e = p->data.ddata.children;
			} else if (e) {
				dbuff[0]=e->nleng;
				dbuff++;
				memcpy(dbuff,e->name,e->nleng);
				dbuff+=e->nleng;
				put32bit(&dbuff,e->child->inode);
				if (attrmode==2) {
					fsnodes_fill_attr(e->child,p,uid,gid,auid,agid,sesflags,dbuff,1);
					dbuff+=ATTR_RECORD_SIZE;
				} else if (attrmode==1) {
					fsnodes_fill_attr(e->child,p,uid,gid,auid,agid,sesflags,dbuff,0);
					dbuff+=35;
				} else if (sesflags&SESFLAG_ATTRBIT) {
					put8bit(&dbuff,e->child->type);
				} else {
					put8bit(&dbuff,DISP_TYPE_REMAP_STR[e->child->type]);
				}
				e = e->nextchild;
			}
			if (e) {
				nedgeid = e->edgeid;
			} else {
				nedgeid = EDGEID_MAX;
			}
		}
		maxentries--;
	}
	*nedgeidp = nedgeid;
	if (e!=NULL) {
		fsnodes_edgeid_insert(e);
	}
}

static inline void fsnodes_checkfile(fsnode *p,uint32_t chunkcount[12]) {
	uint32_t i;
	uint64_t chunkid;
	uint8_t count;
	for (i=0 ; i<12 ; i++) {
		chunkcount[i]=0;
	}
	for (i=0 ; i<p->data.fdata.chunks ; i++) {
		chunkid = p->data.fdata.chunktab[i];
		if (chunkid>0) {
			chunk_get_validcopies(chunkid,&count);
			if (count>10) {
				count=10;
			}
			chunkcount[count]++;
		} else {
			chunkcount[11]++;
		}
	}
}

static inline uint8_t fsnodes_append_slice_of_chunks(uint32_t ts,fsnode *dstobj,fsnode *srcobj,uint32_t slice_from,uint32_t slice_to) {
	uint64_t chunkid,length;
	uint32_t i;
	uint32_t srcchunks,dstchunks,newchunks,lastsrcchunk;
	statsrecord psr,nsr;
	fsedge *e;

	if (srcobj->data.fdata.length>0) {
		lastsrcchunk = (srcobj->data.fdata.length-1)>>MFSCHUNKBITS;
	} else {
		lastsrcchunk = 0;
	}
	if (slice_from==0xFFFFFFFF && slice_to==0) { // special case - compatibility with old append (append whole file)
		slice_from = 0;
		slice_to = lastsrcchunk;
	}
	if (slice_to > lastsrcchunk || slice_from > lastsrcchunk || slice_from > slice_to) {
		return MFS_ERROR_EINVAL;
	}

	srcchunks = (slice_to - slice_from) + 1;

	if (dstobj->data.fdata.length>0) {
		dstchunks = 1+((dstobj->data.fdata.length-1)>>MFSCHUNKBITS);
	} else {
		dstchunks = 0;
	}

	newchunks = srcchunks + dstchunks;

	if (newchunks < dstchunks) { // overflow
		return MFS_ERROR_INDEXTOOBIG;
	}

	if ((newchunks-1)>MAX_INDEX) {	// chain too long
		return MFS_ERROR_INDEXTOOBIG;
	}

	fsnodes_get_stats(dstobj,&psr,0);
	if (newchunks>dstobj->data.fdata.chunks) {
		if (dstobj->data.fdata.chunktab==NULL) {
			dstobj->data.fdata.chunktab = chunktab_malloc(newchunks);
		} else {
			dstobj->data.fdata.chunktab = chunktab_realloc(dstobj->data.fdata.chunktab,dstobj->data.fdata.chunks,newchunks);
		}
		passert(dstobj->data.fdata.chunktab);
		for (i=dstobj->data.fdata.chunks ; i<newchunks ; i++) {
			dstobj->data.fdata.chunktab[i]=0;
		}
		dstobj->data.fdata.chunks = newchunks;
	}

	for (i=dstchunks ; i<dstobj->data.fdata.chunks ; i++) { // pro forma
		chunkid = dstobj->data.fdata.chunktab[i];
		if (chunkid>0) {
			if (chunk_delete_file(chunkid,dstobj->sclassid)!=MFS_STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,dstobj->inode,i);
			}
		}
		dstobj->data.fdata.chunktab[i]=0;
	}

	for (i=0 ; i<srcchunks ; i++) {
		chunkid = srcobj->data.fdata.chunktab[slice_from+i];
		dstobj->data.fdata.chunktab[i+dstchunks] = chunkid;
		if (chunkid>0) {
			if (chunk_add_file(chunkid,dstobj->sclassid)!=MFS_STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,srcobj->inode,i+slice_from);
			}
		}
	}

	if (slice_to>=lastsrcchunk) {
		length = (((uint64_t)dstchunks)<<MFSCHUNKBITS) + srcobj->data.fdata.length - (((uint64_t)slice_from)<<MFSCHUNKBITS);
	} else {
		length = (((uint64_t)newchunks)<<MFSCHUNKBITS);
	}

	if (dstobj->type==TYPE_TRASH) {
		trashspace -= dstobj->data.fdata.length;
		trashspace += length;
	} else if (dstobj->type==TYPE_SUSTAINED) {
		sustainedspace -= dstobj->data.fdata.length;
		sustainedspace += length;
	}
	dstobj->data.fdata.length = length;
	fsnodes_get_stats(dstobj,&nsr,1);
	for (e=dstobj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	dstobj->mtime = ts;
	dstobj->atime = ts;
	if (srcobj->atime!=ts) {
		srcobj->atime = ts;
	}
	return MFS_STATUS_OK;
}

static inline void fsnodes_changefilesclassid(fsnode *obj,uint8_t sclassid) {
	uint32_t i;
	statsrecord psr,nsr;
	fsedge *e;

	fsnodes_get_stats(obj,&psr,0);
	for (i=0 ; i<obj->data.fdata.chunks ; i++) {
		if (obj->data.fdata.chunktab[i]>0) {
			chunk_change_file(obj->data.fdata.chunktab[i],obj->sclassid,sclassid);
		}
	}
	sclass_decref(obj->sclassid,obj->type);
	obj->sclassid = sclassid;
	sclass_incref(sclassid,obj->type);
	fsnodes_get_stats(obj,&nsr,1);
	for (e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
}

static inline void fsnodes_setlength(fsnode *obj,uint64_t length) {
	uint32_t i,chunks;
	uint64_t chunkid;
	fsedge *e;
	statsrecord psr,nsr;
	fsnodes_get_stats(obj,&psr,0);

	if (obj->type==TYPE_TRASH) {
		trashspace -= obj->data.fdata.length;
		trashspace += length;
	} else if (obj->type==TYPE_SUSTAINED) {
		sustainedspace -= obj->data.fdata.length;
		sustainedspace += length;
	}
	obj->data.fdata.length = length;
	if (length>0) {
		chunks = ((length-1)>>MFSCHUNKBITS)+1;
	} else {
		chunks = 0;
	}
	for (i=chunks ; i<obj->data.fdata.chunks ; i++) {
		chunkid = obj->data.fdata.chunktab[i];
		if (chunkid>0) {
			if (chunk_delete_file(chunkid,obj->sclassid)!=MFS_STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,obj->inode,i);
			}
		}
		obj->data.fdata.chunktab[i]=0;
	}
	if (chunks>0) {
		if (chunks<obj->data.fdata.chunks && obj->data.fdata.chunktab) {
			obj->data.fdata.chunktab = chunktab_realloc(obj->data.fdata.chunktab,obj->data.fdata.chunks,chunks);
			passert(obj->data.fdata.chunktab);
			obj->data.fdata.chunks = chunks;
		}
	} else {
		if (obj->data.fdata.chunks>0 && obj->data.fdata.chunktab) {
			chunktab_free(obj->data.fdata.chunktab,obj->data.fdata.chunks);
			obj->data.fdata.chunktab = NULL;
			obj->data.fdata.chunks = 0;
		}
	}
	fsnodes_get_stats(obj,&nsr,1);
	for (e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	obj->eattr &= ~(EATTR_SNAPSHOT);
}


static inline void fsnodes_remove_node(uint32_t ts,fsnode *toremove) {
	if (toremove->parents!=NULL) {
		return;
	}
	fsnodes_node_delete(toremove);
	nodes--;
	if (toremove->type==TYPE_DIRECTORY) {
		dirnodes--;
		fsnodes_delete_quotanode(toremove);
	}
	if (toremove->type==TYPE_FILE || toremove->type==TYPE_TRASH || toremove->type==TYPE_SUSTAINED) {
		uint32_t i;
		uint64_t chunkid;
		filenodes--;
		for (i=0 ; i<toremove->data.fdata.chunks ; i++) {
			chunkid = toremove->data.fdata.chunktab[i];
			if (chunkid>0) {
				if (chunk_delete_file(chunkid,toremove->sclassid)!=MFS_STATUS_OK) {
					syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,toremove->inode,i);
				}
			}
		}
		if (toremove->data.fdata.chunktab!=NULL) {
			chunktab_free(toremove->data.fdata.chunktab,toremove->data.fdata.chunks);
		}
	}
	if (toremove->type==TYPE_SYMLINK) {
		if (toremove->data.sdata.path) {
			symlink_free(toremove->data.sdata.path,toremove->data.sdata.pleng);
		}
	}
	sclass_decref(toremove->sclassid,toremove->type);
	fsnodes_free_id(toremove->inode,ts);
	if (toremove->xattrflag) {
		xattr_removeinode(toremove->inode);
	}
	if (toremove->aclpermflag) {
		posix_acl_remove(toremove->inode,POSIX_ACL_ACCESS);
	}
	if (toremove->acldefflag) {
		posix_acl_remove(toremove->inode,POSIX_ACL_DEFAULT);
	}
	dcm_modify(toremove->inode,0);
	switch (toremove->type) {
		case TYPE_DIRECTORY:
			fsnode_dir_free(toremove);
			break;
		case TYPE_FILE:
		case TYPE_TRASH:
		case TYPE_SUSTAINED:
			fsnode_file_free(toremove);
			break;
		case TYPE_SYMLINK:
			fsnode_symlink_free(toremove);
			break;
		case TYPE_BLOCKDEV:
		case TYPE_CHARDEV:
			fsnode_dev_free(toremove);
			break;
		default:
			fsnode_other_free(toremove);
	}
}


static inline void fsnodes_unlink(uint32_t ts,fsedge *e) {
	fsnode *child;
	uint32_t bid;
	uint16_t pleng=0;
	uint8_t *path=NULL;
	uint8_t isopen;

	child = e->child;
	isopen = of_isfileopen(child->inode);
	if (child->parents->nextparent==NULL) { // last link
		if (child->type==TYPE_FILE && (child->trashtime>0 || isopen)) {	// go to trash or sustained ? - get path
			fsnodes_getpath(e,&pleng,&path);
		}
	}
	fsnodes_remove_edge(ts,e);
	if (child->parents==NULL) {	// last link
		if (child->type == TYPE_FILE) {
			if (child->trashtime>0) {
				bid = child->inode % TRASH_BUCKETS;
				child->type = TYPE_TRASH;
				child->ctime = ts;
				e = fsedge_malloc(pleng);
				passert(e);
				if (nextedgeid<EDGEID_MAX) {
					e->edgeid = nextedgeid--;
				} else {
					e->edgeid = 0;
				}
				e->nleng = pleng;
				memcpy((uint8_t*)(e->name),path,pleng);
				e->child = child;
				e->parent = NULL;
				e->nextchild = trash[bid];
				e->nextparent = NULL;
				e->prevchild = trash + bid;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				trash[bid] = e;
				child->parents = e;
				trashspace += child->data.fdata.length;
				trashnodes++;
			} else if (isopen) {
				bid = child->inode % SUSTAINED_BUCKETS;
				child->type = TYPE_SUSTAINED;
				e = fsedge_malloc(pleng);
				passert(e);
				if (nextedgeid<EDGEID_MAX) {
					e->edgeid = nextedgeid--;
				} else {
					e->edgeid = 0;
				}
				e->nleng = pleng;
				memcpy((uint8_t*)(e->name),path,pleng);
				e->child = child;
				e->parent = NULL;
				e->nextchild = sustained[bid];
				e->nextparent = NULL;
				e->prevchild = sustained + bid;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				sustained[bid] = e;
				child->parents = e;
				sustainedspace += child->data.fdata.length;
				sustainednodes++;
			} else {
				fsnodes_remove_node(ts,child);
			}
		} else {
			fsnodes_remove_node(ts,child);
		}
	}
	if (path) {
		free(path);
	}
}

static inline int fsnodes_purge(uint32_t ts,fsnode *p) {
	uint32_t bid;
	fsedge *e;
	e = p->parents;

	if (p->type==TYPE_TRASH) {
		trashspace -= p->data.fdata.length;
		trashnodes--;
		if (of_isfileopen(p->inode)) {
			bid = p->inode % SUSTAINED_BUCKETS;
			p->type = TYPE_SUSTAINED;
			sustainedspace += p->data.fdata.length;
			sustainednodes++;
			*(e->prevchild) = e->nextchild;
			if (e->nextchild) {
				e->nextchild->prevchild = e->prevchild;
			}
			e->nextchild = sustained[bid];
			e->prevchild = sustained + bid;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			sustained[bid] = e;
			return 0;
		} else {
			fsnodes_remove_edge(ts,e);
			fsnodes_remove_node(ts,p);
			return 1;
		}
	} else if (p->type==TYPE_SUSTAINED) {
		sustainedspace -= p->data.fdata.length;
		sustainednodes--;
		fsnodes_remove_edge(ts,e);
		fsnodes_remove_node(ts,p);
		return 1;
	}
	return -1;
}

static inline uint8_t fsnodes_undel(uint32_t ts,fsnode *node) {
	uint16_t pleng;
	const uint8_t *path;
	uint8_t new;
	uint32_t i,partleng,dots;
	fsedge *e,*pe;
	fsnode *p,*n;

/* check path */
	e = node->parents;
	pleng = e->nleng;
	path = e->name;

	if (path==NULL) {
		return MFS_ERROR_CANTCREATEPATH;
	}
	while (*path=='/' && pleng>0) {
		path++;
		pleng--;
	}
	if (pleng==0) {
		return MFS_ERROR_CANTCREATEPATH;
	}
	partleng=0;
	dots=0;
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {	// incorrect name character
			return MFS_ERROR_CANTCREATEPATH;
		} else if (path[i]=='/') {
			if (partleng==0) {	// "//" in path
				return MFS_ERROR_CANTCREATEPATH;
			}
			if (partleng==dots && partleng<=2) {	// '.' or '..' in path
				return MFS_ERROR_CANTCREATEPATH;
			}
			partleng=0;
			dots=0;
		} else {
			if (path[i]=='.') {
				dots++;
			}
			partleng++;
			if (partleng>MAXFNAMELENG) {
				return MFS_ERROR_CANTCREATEPATH;
			}
		}
	}
	if (partleng==0) {	// last part canot be empty - it's the name of undeleted file
		return MFS_ERROR_CANTCREATEPATH;
	}
	if (partleng==dots && partleng<=2) {	// '.' or '..' in path
		return MFS_ERROR_CANTCREATEPATH;
	}

/* create path */
	n = NULL;
	p = root;
	new = 0;
	for (;;) {
		if (p->data.ddata.quota && p->data.ddata.quota->exceeded) {
			return MFS_ERROR_QUOTA;
		}
		partleng=0;
		while (partleng<pleng && path[partleng]!='/') {
			partleng++;
		}
		if (partleng==pleng) {	// last name
			if (fsnodes_nameisused(p,partleng,path)) {
				return MFS_ERROR_EEXIST;
			}
			// remove from trash and link to new parent
			node->type = TYPE_FILE;
			node->ctime = ts;
			fsnodes_link(ts,p,node,partleng,path);
			fsnodes_remove_edge(ts,e);
			trashspace -= node->data.fdata.length;
			trashnodes--;
			return MFS_STATUS_OK;
		} else {
			if (new==0) {
				pe = fsnodes_lookup(p,partleng,path);
				if (pe==NULL) {
					new=1;
				} else {
					n = pe->child;
					if (n->type!=TYPE_DIRECTORY) {
						return MFS_ERROR_CANTCREATEPATH;
					}
				}
			}
			if (new==1) {
				n = fsnodes_create_node(ts,p,partleng,path,TYPE_DIRECTORY,0755,0,0,0,0);
			}
			p = n;
		}
		path+=partleng+1;
		pleng-=partleng+1;
	}
}

static inline void fsnodes_getsclass_recursive(fsnode *node,uint8_t gmode,uint32_t fgtab[256],uint32_t dgtab[256]) {
	fsedge *e;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		fgtab[node->sclassid]++;
	} else if (node->type==TYPE_DIRECTORY) {
		dgtab[node->sclassid]++;
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_getsclass_recursive(e->child,gmode,fgtab,dgtab);
			}
		}
	}
}

static inline void fsnodes_bst_add(bstnode **n,uint32_t val) {
	while (*n) {
		if (val<(*n)->val) {
			n = &((*n)->left);
		} else if (val>(*n)->val) {
			n = &((*n)->right);
		} else {
			(*n)->count++;
			return;
		}
	}
	(*n)=malloc(sizeof(bstnode));
	passert(*n);
	(*n)->val = val;
	(*n)->count = 1;
	(*n)->left = NULL;
	(*n)->right = NULL;
}

static inline uint32_t fsnodes_bst_nodes(bstnode *n) {
	if (n) {
		return 1+fsnodes_bst_nodes(n->left)+fsnodes_bst_nodes(n->right);
	} else {
		return 0;
	}
}

static inline void fsnodes_bst_storedata(bstnode *n,uint8_t **ptr) {
	if (n) {
		fsnodes_bst_storedata(n->left,ptr);
		put32bit(&*ptr,n->val);
		put32bit(&*ptr,n->count);
		fsnodes_bst_storedata(n->right,ptr);
	}
}

static inline void fsnodes_bst_free(bstnode *n) {
	if (n) {
		fsnodes_bst_free(n->left);
		fsnodes_bst_free(n->right);
		free(n);
	}
}

static inline void fsnodes_gettrashtime_recursive(fsnode *node,uint8_t gmode,bstnode **bstrootfiles,bstnode **bstrootdirs) {
	fsedge *e;
	uint32_t trashseconds;

	fsnodes_keep_alive_check();
	trashseconds = node->trashtime;
	trashseconds *= 3600;
	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		fsnodes_bst_add(bstrootfiles,trashseconds);
	} else if (node->type==TYPE_DIRECTORY) {
		fsnodes_bst_add(bstrootdirs,trashseconds);
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_gettrashtime_recursive(e->child,gmode,bstrootfiles,bstrootdirs);
			}
		}
	}
}

static inline void fsnodes_geteattr_recursive(fsnode *node,uint8_t gmode,uint32_t feattrtab[32],uint32_t deattrtab[32]) {
	fsedge *e;

	fsnodes_keep_alive_check();
	if (node->type!=TYPE_DIRECTORY) {
		feattrtab[(node->eattr)&(EATTR_NOOWNER|EATTR_NOACACHE|EATTR_NODATACACHE|EATTR_SNAPSHOT)]++;
	} else {
		deattrtab[(node->eattr)]++;
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_geteattr_recursive(e->child,gmode,feattrtab,deattrtab);
			}
		}
	}
}

static inline void fsnodes_getarch_recursive(fsnode *node,uint64_t *archchunks,uint64_t *notarchchunks,uint32_t *archinodes,uint32_t *partinodes,uint32_t *notarchinodes) {
	fsedge *e;
	uint8_t archflag;
	uint32_t j,archived,notarchived;
	uint64_t chunkid;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		archived = 0;
		notarchived = 0;
		for (j=0 ; j<node->data.fdata.chunks ; j++) {
			chunkid = node->data.fdata.chunktab[j];
			if (chunkid>0) {
				if (chunk_get_archflag(chunkid,&archflag)==MFS_STATUS_OK) {
					if (archflag) {
						archived++;
					} else {
						notarchived++;
					}
				}
			}
		}
		if (archived>0 && notarchived>0) {
			(*partinodes)++;
		} else if (archived==0 && notarchived>0) {
			(*notarchinodes)++;
		} else if (notarchived==0 && archived>0) {
			(*archinodes)++;
		}
		(*archchunks) += archived;
		(*notarchchunks) += notarchived;
	} else if (node->type==TYPE_DIRECTORY) {
		for (e = node->data.ddata.children ; e ; e=e->nextchild) {
			fsnodes_getarch_recursive(e->child,archchunks,notarchchunks,archinodes,partinodes,notarchinodes);
		}
	}
}

static inline uint64_t fsnodes_setsclass_recursive_test_quota(fsnode *node,uint32_t uid,uint8_t goal,uint8_t recursive,uint64_t *realsize) {
	fsedge *e;
	uint32_t i,lastchunk,lastchunksize;
	uint64_t rs,size;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_DIRECTORY && recursive) {
		rs = 0;
		for (e = node->data.ddata.children ; e ; e=e->nextchild) {
			if (fsnodes_setsclass_recursive_test_quota(e->child,uid,goal,2,&rs)) {
				return 1;
			}
		}
		if (recursive<2) {
			if (fsnodes_test_quota(node,0,0,0,rs)) {
				return 1;
			}
		} else {
			if (fsnodes_test_quota_noparents(node,0,0,0,rs)) {
				return 1;
			}
		}
		(*realsize) += rs;
		return 0;
	} else if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		if (!((node->eattr&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid)) {
			if (goal>sclass_get_keepmax_goal(node->sclassid)) {
				size = 0;
				if (node->data.fdata.length>0) {
					lastchunk = (node->data.fdata.length-1)>>MFSCHUNKBITS;
					lastchunksize = ((((node->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
				} else {
					lastchunk = 0;
					lastchunksize = MFSHDRSIZE;
				}
				for (i=0 ; i<node->data.fdata.chunks ; i++) {
					if (node->data.fdata.chunktab[i]>0) {
						if (i<lastchunk) {
							size += MFSCHUNKSIZE+MFSHDRSIZE;
						} else if (i==lastchunk) {
							size += lastchunksize;
						}
					}
				}
				rs = size * (goal - sclass_get_keepmax_goal(node->sclassid));
				if (recursive<2) {
					if (fsnodes_test_quota(node,0,0,0,rs)) {
						return 1;
					}
				}
				(*realsize) += rs;
			}
		}
	}
	return 0;
}

static inline void fsnodes_setsclass_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint8_t src_sclassid,uint8_t dst_sclassid,uint8_t smode,uint8_t admin,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t set;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_DIRECTORY || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		if (((node->eattr&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid) || ((sclass_is_admin_only(node->sclassid) || sclass_is_admin_only(dst_sclassid)) && admin==0)) {
			(*nsinodes)++;
		} else {
			set=0;
			switch (smode&SMODE_TMASK) {
			case SMODE_SET:
				if (node->sclassid!=dst_sclassid) {
					set = 1;
				}
				break;
			case SMODE_INCREASE:
				if (node->sclassid<dst_sclassid) {
					set = 1;
				}
				break;
			case SMODE_DECREASE:
				if (node->sclassid>dst_sclassid) {
					set = 1;
				}
				break;
			case SMODE_EXCHANGE:
				if (node->sclassid==src_sclassid) {
					set = 1;
				}
				break;
			}
			if (set) {
				if (node->type!=TYPE_DIRECTORY) {
					fsnodes_changefilesclassid(node,dst_sclassid);
					(*sinodes)++;
				} else {
					sclass_decref(node->sclassid,node->type);
					node->sclassid = dst_sclassid;
					sclass_incref(dst_sclassid,node->type);
					(*sinodes)++;
				}
				node->ctime = ts;
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type==TYPE_DIRECTORY && (smode&SMODE_RMASK)) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_setsclass_recursive(e->child,ts,uid,src_sclassid,dst_sclassid,smode,admin,sinodes,ncinodes,nsinodes);
			}
		}
	}
}

static inline void fsnodes_settrashtime_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint16_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t set;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_DIRECTORY || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		if ((node->eattr&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid) {
			(*nsinodes)++;
		} else {
			set=0;
			switch (smode&SMODE_TMASK) {
			case SMODE_SET:
				if (node->trashtime!=trashtime) {
					node->trashtime=trashtime;
					set=1;
				}
				break;
			case SMODE_INCREASE:
				if (node->trashtime<trashtime) {
					node->trashtime=trashtime;
					set=1;
				}
				break;
			case SMODE_DECREASE:
				if (node->trashtime>trashtime) {
					node->trashtime=trashtime;
					set=1;
				}
				break;
			}
			if (set) {
				(*sinodes)++;
				node->ctime = ts;
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type==TYPE_DIRECTORY && (smode&SMODE_RMASK)) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_settrashtime_recursive(e->child,ts,uid,trashtime,smode,sinodes,ncinodes,nsinodes);
			}
		}
	}
}

static inline void fsnodes_seteattr_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t neweattr,seattr;

	fsnodes_keep_alive_check();
	if ((node->eattr&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid) {
		(*nsinodes)++;
	} else {
		seattr = eattr;
		if (node->type!=TYPE_DIRECTORY) {
			node->eattr &= ~(EATTR_NOECACHE);
			seattr &= ~(EATTR_NOECACHE);
		}
		neweattr = node->eattr;
		switch (smode&SMODE_TMASK) {
			case SMODE_SET:
				neweattr = seattr;
				break;
			case SMODE_INCREASE:
				neweattr |= seattr;
				break;
			case SMODE_DECREASE:
				neweattr &= ~seattr;
				break;
		}
		if (neweattr!=node->eattr) {
			node->eattr = neweattr;
//			node->mode = (node->mode&0xFFF) | (((uint16_t)neweattr)<<12);
			(*sinodes)++;
			node->ctime = ts;
		} else {
			(*ncinodes)++;
		}
	}
	if (node->type==TYPE_DIRECTORY && (smode&SMODE_RMASK)) {
		for (e = node->data.ddata.children ; e ; e=e->nextchild) {
			fsnodes_seteattr_recursive(e->child,ts,uid,eattr,smode,sinodes,ncinodes,nsinodes);
		}
	}
}

static inline void fsnodes_chgarch_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint8_t cmd,uint64_t *chgchunks,uint64_t *notchgchunks,uint32_t *nsinodes) {
	fsedge *e;
	uint32_t aflagchanged;
	uint32_t allchunks;
	uint32_t j;
	uint64_t chunkid;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		if ((node->eattr&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid) {
			(*nsinodes)++;
		} else {
			aflagchanged = 0;
			allchunks = 0;
			for (j=0 ; j<node->data.fdata.chunks ; j++) {
				chunkid = node->data.fdata.chunktab[j];
				if (chunkid>0) {
					allchunks++;
					chunk_univ_archflag(chunkid,(cmd==ARCHCTL_SET)?1:0,&aflagchanged);
				}
			}
			(*chgchunks) += aflagchanged;
			(*notchgchunks) += (allchunks - aflagchanged);
			if (cmd==ARCHCTL_CLR) {
				node->ctime = ts;
			}
		}
	}
	if (node->type==TYPE_DIRECTORY) {
		for (e = node->data.ddata.children ; e ; e=e->nextchild) {
			fsnodes_chgarch_recursive(e->child,ts,uid,cmd,chgchunks,notchgchunks,nsinodes);
		}
	}
}

typedef struct _fsnodes_snapshot_params {
	uint32_t ts;
	uint8_t smode;
	uint8_t sesflags;
	uint16_t cumask;
	uint32_t uid;
	uint32_t gids;
	uint32_t *gid;
	uint32_t inode_chksum;
	uint32_t removed_object;
	uint32_t same_file;
	uint32_t existing_object;
	uint32_t new_hardlink;
	uint32_t new_object;
} fsnodes_snapshot_params;

static inline uint8_t fsnodes_remove_snapshot_test(fsedge *e,fsnodes_snapshot_params *args) {
	fsnode *n;
	fsedge *ie;
	uint8_t status;
	n = e->child;
	fsnodes_keep_alive_check();
	if (n->type == TYPE_DIRECTORY) {
		if (fsnodes_access_ext(n,args->uid,args->gids,args->gid,MODE_MASK_W|MODE_MASK_X,args->sesflags)) {
			for (ie = n->data.ddata.children ; ie ; ie=ie->nextchild) {
				status = fsnodes_remove_snapshot_test(ie,args);
				if (status!=MFS_STATUS_OK) {
					return status;
				}
			}
		} else {
			return MFS_ERROR_EACCES;
		}
	}
	if ((n->eattr & EATTR_SNAPSHOT) == 0) {
		return MFS_ERROR_EPERM;
	}
	return MFS_STATUS_OK;
}

static inline void fsnodes_remove_snapshot(fsedge *e,fsnodes_snapshot_params *args) {
	uint8_t eattr_back;
	fsnode *n;
	fsedge *ie,*ien;
	n = e->child;
	fsnodes_keep_alive_check();
	if (n->type == TYPE_DIRECTORY) {
		eattr_back = n->eattr;
		if (fsnodes_access_ext(n,args->uid,args->gids,args->gid,MODE_MASK_W|MODE_MASK_X,args->sesflags)) {
			for (ie = n->data.ddata.children ; ie ; ie=ien) {
				ien = ie->nextchild;
				fsnodes_remove_snapshot(ie,args);
			}
		}
		if (n->data.ddata.children!=NULL) {
			return;
		}
		n->eattr = eattr_back;
	}
	if (n->eattr & EATTR_SNAPSHOT) {
		n->trashtime = 0;
		args->inode_chksum ^= n->inode;
		args->removed_object++;
		fsnodes_unlink(args->ts,e);
	}
}

static inline void fsnodes_snapshot(fsnode *srcnode,fsnode *parentnode,uint32_t nleng,const uint8_t *name,uint8_t newflag,fsnodes_snapshot_params *args) {
	fsedge *e;
	fsnode *dstnode;
	uint32_t i;
	uint64_t chunkid;
	uint8_t rec,accessstatus;

	fsnodes_keep_alive_check();
	if (srcnode->type==TYPE_DIRECTORY) {
		rec = fsnodes_access_ext(srcnode,args->uid,args->gids,args->gid,MODE_MASK_R|MODE_MASK_X,args->sesflags);
		accessstatus = 1;
	} else if (srcnode->type==TYPE_FILE) {
		rec = 0;
		accessstatus = fsnodes_access_ext(srcnode,args->uid,args->gids,args->gid,MODE_MASK_R,args->sesflags);
	} else {
		rec = 0;
		accessstatus = 1;
	}
	if (accessstatus==0) {
		return;
	}
	if (newflag==0 && (e=fsnodes_lookup(parentnode,nleng,name))) { // element already exists
		dstnode = e->child;
		if (srcnode->type==TYPE_DIRECTORY) {
			args->existing_object++;
			if (rec) {
				for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
					fsnodes_snapshot(e->child,dstnode,e->nleng,e->name,0,args);
				}
			}
		} else if (srcnode->type==TYPE_FILE) {
			uint8_t same;
			if (dstnode->data.fdata.length==srcnode->data.fdata.length && dstnode->data.fdata.chunks==srcnode->data.fdata.chunks) {
				same=1;
				for (i=0 ; i<srcnode->data.fdata.chunks && same ; i++) {
					if (srcnode->data.fdata.chunktab[i]!=dstnode->data.fdata.chunktab[i]) {
						same=0;
					}
				}
			} else {
				same=0;
			}
			if (same==0) {
				statsrecord psr,nsr;
				args->inode_chksum ^= dstnode->inode;
				fsnodes_unlink(args->ts,e);
				if (args->smode&SNAPSHOT_MODE_CPLIKE_ATTR) {
					dstnode = fsnodes_create_node(args->ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode,args->cumask,args->uid,args->gid[0],0);
				} else {
					if (args->uid==0 || args->uid==srcnode->uid) {
						dstnode = fsnodes_create_node(args->ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode&0xFFF,0,srcnode->uid,srcnode->gid,0);
					} else {
						dstnode = fsnodes_create_node(args->ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode&0x3FF,0,args->uid,args->gid[0],0);
					}
				}
				args->existing_object++;
				args->inode_chksum ^= dstnode->inode;
				fsnodes_get_stats(dstnode,&psr,0);
				sclass_decref(dstnode->sclassid,dstnode->type);
				dstnode->sclassid = srcnode->sclassid;
				sclass_incref(dstnode->sclassid,dstnode->type);
				dstnode->trashtime = srcnode->trashtime;
//				dstnode->mode = srcnode->mode;
//				dstnode->atime = srcnode->atime;
//				dstnode->mtime = srcnode->mtime;
				if (srcnode->data.fdata.chunks>0) {
					dstnode->data.fdata.chunktab = chunktab_malloc(srcnode->data.fdata.chunks);
					passert(dstnode->data.fdata.chunktab);
					dstnode->data.fdata.chunks = srcnode->data.fdata.chunks;
					for (i=0 ; i<srcnode->data.fdata.chunks ; i++) {
						chunkid = srcnode->data.fdata.chunktab[i];
						dstnode->data.fdata.chunktab[i] = chunkid;
						if (chunkid>0) {
							if (chunk_add_file(chunkid,dstnode->sclassid)!=MFS_STATUS_OK) {
								syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,srcnode->inode,i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
				fsnodes_get_stats(dstnode,&nsr,1);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
			} else {
				args->same_file++;
			}
		} else if (srcnode->type==TYPE_SYMLINK) {
			args->existing_object++;
			if (dstnode->data.sdata.pleng!=srcnode->data.sdata.pleng) {
				statsrecord sr;
				memset(&sr,0,sizeof(statsrecord));
				sr.length = srcnode->data.sdata.pleng;
				sr.length -= dstnode->data.sdata.pleng;
				fsnodes_add_stats(parentnode,&sr);
			}
			if (dstnode->data.sdata.path) {
				symlink_free(dstnode->data.sdata.path,dstnode->data.sdata.pleng);
			}
			if (srcnode->data.sdata.pleng>0) {
				dstnode->data.sdata.path = symlink_malloc(srcnode->data.sdata.pleng);
				passert(dstnode->data.sdata.path);
				memcpy(dstnode->data.sdata.path,srcnode->data.sdata.path,srcnode->data.sdata.pleng);
				dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
			} else {
				dstnode->data.sdata.path=NULL;
				dstnode->data.sdata.pleng=0;
			}
		} else if (srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV) {
			args->existing_object++;
			dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
		} else {
			args->existing_object++;
		}
		if (args->smode&SNAPSHOT_MODE_CPLIKE_ATTR) {
			dstnode->uid = args->uid;
			dstnode->gid = args->gid[0];
			dstnode->mode = srcnode->mode & ~(args->cumask);
			dstnode->ctime = args->ts;
		} else {
			if (args->uid==0 || args->uid==srcnode->uid) {
				dstnode->mode = srcnode->mode;
				dstnode->uid = srcnode->uid;
				dstnode->gid = srcnode->gid;
				dstnode->atime = srcnode->atime;
				dstnode->mtime = srcnode->mtime;
				dstnode->ctime = args->ts;
			} else {
				dstnode->mode = srcnode->mode & 0x3FF; // clear suid/sgid
				dstnode->uid = args->uid;
				dstnode->gid = args->gid[0];
				dstnode->atime = srcnode->atime;
				dstnode->mtime = srcnode->mtime;
				dstnode->ctime = args->ts;
			}
		}
		dstnode->eattr |= EATTR_SNAPSHOT;
	} else { // new element
		if (srcnode->type==TYPE_FILE || srcnode->type==TYPE_DIRECTORY || srcnode->type==TYPE_SYMLINK || srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV || srcnode->type==TYPE_SOCKET || srcnode->type==TYPE_FIFO) {
			statsrecord psr,nsr;
			if (args->smode&SNAPSHOT_MODE_PRESERVE_HARDLINKS && srcnode->type!=TYPE_DIRECTORY && srcnode->parents->nextparent!=NULL) {
				dstnode = chash_find(snapshot_inodehash,srcnode->inode);
				if (dstnode!=NULL) {
					args->new_hardlink++;
					fsnodes_link(args->ts,parentnode,dstnode,nleng,name);
					return;
				}
			}
			if (args->smode&SNAPSHOT_MODE_CPLIKE_ATTR) {
				dstnode = fsnodes_create_node(args->ts,parentnode,nleng,name,srcnode->type,srcnode->mode,args->cumask,args->uid,args->gid[0],0);
			} else {
				if (args->uid==0 || args->uid==srcnode->uid) {
					dstnode = fsnodes_create_node(args->ts,parentnode,nleng,name,srcnode->type,srcnode->mode,0,srcnode->uid,srcnode->gid,0);
				} else {
					dstnode = fsnodes_create_node(args->ts,parentnode,nleng,name,srcnode->type,srcnode->mode&0x3FF,0,args->uid,args->gid[0],0);
				}
			}
			args->inode_chksum ^= dstnode->inode;
			args->new_object++;
			if (args->smode&SNAPSHOT_MODE_PRESERVE_HARDLINKS && srcnode->type!=TYPE_DIRECTORY && srcnode->parents->nextparent!=NULL) {
				chash_add(snapshot_inodehash,srcnode->inode,dstnode);
			}
			fsnodes_get_stats(dstnode,&psr,0);
			if ((args->smode&SNAPSHOT_MODE_CPLIKE_ATTR)==0) {
				sclass_decref(dstnode->sclassid,dstnode->type);
				dstnode->sclassid = srcnode->sclassid;
				sclass_incref(dstnode->sclassid,dstnode->type);
				dstnode->trashtime = srcnode->trashtime;
				dstnode->eattr = srcnode->eattr;
				dstnode->winattr = srcnode->winattr;
				dstnode->mode = srcnode->mode;
				if (args->uid!=0 && args->uid!=srcnode->uid) {
					dstnode->mode &= 0x3FF; // clear suid+sgid
				}
				dstnode->atime = srcnode->atime;
				dstnode->mtime = srcnode->mtime;
				if (srcnode->xattrflag) {
					dstnode->xattrflag = xattr_copy(srcnode->inode,dstnode->inode);
				}
				if (srcnode->aclpermflag) {
					dstnode->aclpermflag = posix_acl_copy(srcnode->inode,dstnode->inode,POSIX_ACL_ACCESS);
				}
				if (srcnode->acldefflag) {
					dstnode->acldefflag = posix_acl_copy(srcnode->inode,dstnode->inode,POSIX_ACL_DEFAULT);
				}
			}
			if (srcnode->type==TYPE_DIRECTORY) {
				if (rec) {
					for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
						fsnodes_snapshot(e->child,dstnode,e->nleng,e->name,1,args);
					}
				}
			} else if (srcnode->type==TYPE_FILE) {
				if (srcnode->data.fdata.chunks>0) {
					dstnode->data.fdata.chunktab = chunktab_malloc(srcnode->data.fdata.chunks);
					passert(dstnode->data.fdata.chunktab);
					dstnode->data.fdata.chunks = srcnode->data.fdata.chunks;
					for (i=0 ; i<srcnode->data.fdata.chunks ; i++) {
						chunkid = srcnode->data.fdata.chunktab[i];
						dstnode->data.fdata.chunktab[i] = chunkid;
						if (chunkid>0) {
							if (chunk_add_file(chunkid,dstnode->sclassid)!=MFS_STATUS_OK) {
								syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,srcnode->inode,i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
				fsnodes_get_stats(dstnode,&nsr,1);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
			} else if (srcnode->type==TYPE_SYMLINK) {
				if (srcnode->data.sdata.pleng>0) {
					dstnode->data.sdata.path = symlink_malloc(srcnode->data.sdata.pleng);
					passert(dstnode->data.sdata.path);
					memcpy(dstnode->data.sdata.path,srcnode->data.sdata.path,srcnode->data.sdata.pleng);
					dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
				}
				fsnodes_get_stats(dstnode,&nsr,1);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
			} else if (srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV) {
				dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
			}
			dstnode->eattr |= EATTR_SNAPSHOT;
		}
	}
}

static inline uint8_t fsnodes_snapshot_test(fsnode *origsrcnode,fsnode *srcnode,fsnode *parentnode,uint32_t nleng,const uint8_t *name,uint8_t canoverwrite) {
	fsedge *e;
	fsnode *dstnode;
	uint8_t status;

	fsnodes_keep_alive_check();
	if ((e=fsnodes_lookup(parentnode,nleng,name))) {
		dstnode = e->child;
		if (dstnode==origsrcnode) {
			return MFS_ERROR_EINVAL;
		}
		if (dstnode->type!=srcnode->type) {
			return MFS_ERROR_EPERM;
		}
		if (srcnode->type==TYPE_TRASH || srcnode->type==TYPE_SUSTAINED) {
			return MFS_ERROR_EPERM;
		}
		if (srcnode->type==TYPE_DIRECTORY) {
			for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
				status = fsnodes_snapshot_test(origsrcnode,e->child,dstnode,e->nleng,e->name,canoverwrite);
				if (status!=MFS_STATUS_OK) {
					return status;
				}
			}
		} else if (canoverwrite==0) {
			return MFS_ERROR_EEXIST;
		}
	}
	return MFS_STATUS_OK;
}

static inline uint8_t fsnodes_snapshot_recursive_test_quota(fsnode *srcnode,fsnode *parentnode,uint32_t nleng,const uint8_t *name,uint32_t *inodes,uint64_t *length,uint64_t *size,uint64_t *realsize) {
	fsedge *e;
	fsnode *dstnode;
	uint32_t lastchunk,lastchunksize,i;
	uint64_t fsize;

	fsnodes_keep_alive_check();
	if ((e=fsnodes_lookup(parentnode,nleng,name))) {
		dstnode = e->child;
		(*inodes)++;
		if (dstnode->type==TYPE_FILE) {
			(*length) += dstnode->data.fdata.length;
			if (dstnode->data.fdata.length>0) {
				lastchunk = (dstnode->data.fdata.length-1)>>MFSCHUNKBITS;
				lastchunksize = ((((dstnode->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
			} else {
				lastchunk = 0;
				lastchunksize = MFSHDRSIZE;
			}
			fsize = 0;
			for (i=0 ; i<dstnode->data.fdata.chunks ; i++) {
				if (dstnode->data.fdata.chunktab[i]>0) {
					if (i<lastchunk) {
						fsize += MFSCHUNKSIZE+MFSHDRSIZE;
					} else if (i==lastchunk) {
						fsize += lastchunksize;
					}
				}
			}
			(*size) += fsize;
			(*realsize) += fsize * sclass_get_keepmax_goal(dstnode->sclassid);
		} else if (dstnode->type==TYPE_SYMLINK) {
			(*length) += dstnode->data.sdata.pleng;
		} else if (dstnode->type==TYPE_DIRECTORY) {
			uint32_t common_inodes;
			uint64_t common_length,common_size,common_realsize;
			statsrecord ssr;

			fsnodes_get_stats(srcnode,&ssr,2);
			common_inodes = 0;
			common_length = 0;
			common_size = 0;
			common_realsize = 0;
			for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
				if (fsnodes_snapshot_recursive_test_quota(e->child,dstnode,e->nleng,e->name,&common_inodes,&common_length,&common_size,&common_realsize)) {
					return 1;
				}
			}
			if (ssr.inodes>common_inodes) {
				ssr.inodes -= common_inodes;
			} else {
				ssr.inodes = 0;
			}
			if (ssr.length>common_length) {
				ssr.length -= common_length;
			} else {
				ssr.length = 0;
			}
			if (ssr.size>common_size) {
				ssr.size -= common_size;
			} else {
				ssr.size = 0;
			}
			if (ssr.realsize>common_realsize) {
				ssr.realsize -= common_realsize;
			} else {
				ssr.realsize = 0;
			}
			if (fsnodes_test_quota_noparents(dstnode,ssr.inodes,ssr.length,ssr.size,ssr.realsize)) {
				return 1;
			}
			(*inodes) += common_inodes;
			(*length) += common_length;
			(*size) += common_size;
			(*realsize) += common_realsize;
		}
	}
	return 0;
}

static inline int fsnodes_namecheck(uint32_t nleng,const uint8_t *name) {
	uint32_t i;
	if (nleng==0 || nleng>MAXFNAMELENG) {
		return -1;
	}
	if (name[0]=='.') {
		if (nleng==1) {
			return -1;
		}
		if (nleng==2 && name[1]=='.') {
			return -1;
		}
	}
	for (i=0 ; i<nleng ; i++) {
		if (name[i]=='\0' || name[i]=='/') {
			return -1;
		}
	}
	return 0;
}

static inline uint8_t fsnodes_node_find_ext(uint32_t rootinode,uint8_t sesflags,uint32_t *inode,fsnode **rootnode,fsnode **node,uint8_t skipancestor) {
	fsnode *p,*rn;
	if ((sesflags&SESFLAG_METARESTORE) || rootinode==MFS_ROOT_ID) {
		rn = root;
		p = fsnodes_node_find(*inode);
		if (p==NULL) {
			*node = NULL;
			return 0;
		}
	} else if (rootinode==0) {
		rn = NULL;
		p = fsnodes_node_find(*inode);
		if (p==NULL || (p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED)) {
			*node = NULL;
			return 0;
		}
	} else {
		rn = fsnodes_node_find(rootinode);
		if (rn==NULL || rn->type!=TYPE_DIRECTORY) {
			*node = NULL;
			return 0;
		}
		if ((*inode)==MFS_ROOT_ID) {
			*inode = rootinode;
			p = rn;
		} else {
			p = fsnodes_node_find(*inode);
			if (p==NULL) {
				*node = NULL;
				return 0;
			}
			if (skipancestor==0 && !fsnodes_isancestor(rn,p)) {
				*node = NULL;
				return 0;
			}
		}
	}
	if (rootnode!=NULL) {
		*rootnode = rn;
	}
	*node = p;
	return 1;
}











/* fs <-> xattr,acl */

uint8_t fs_check_inode(uint32_t inode) {
	return (fsnodes_node_find(inode)!=NULL)?1:0;
}

/* fs <-> xattr */

void fs_set_xattrflag(uint32_t inode) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (p) {
		p->xattrflag = 1;
	}
}

void fs_del_xattrflag(uint32_t inode) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (p) {
		p->xattrflag = 0;
	}
}






/* fs <-> acl */

void fs_set_aclflag(uint32_t inode,uint8_t acltype) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (p) {
		if (acltype==POSIX_ACL_ACCESS) {
			p->aclpermflag = 1;
		} else if (acltype==POSIX_ACL_DEFAULT) {
			p->acldefflag = 1;
		}
	}
}

void fs_del_aclflag(uint32_t inode,uint8_t acltype) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (p) {
		if (acltype==POSIX_ACL_ACCESS) {
			p->aclpermflag = 0;
		} else if (acltype==POSIX_ACL_DEFAULT) {
			p->acldefflag = 0;
		}
	}
}

uint16_t fs_get_mode(uint32_t inode) { // for fixing ACL's produced by mfs before 3.0.98
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (p) {
		return p->mode;
	}
	return 0;
}












/* interface */

uint8_t fs_mr_access(uint32_t ts,uint32_t inode) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	p->atime = ts;
	meta_version_inc();
	return MFS_STATUS_OK;
}

uint8_t fs_readsustained_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize) {
	uint32_t bid;
	if (rootinode!=0) {
		return MFS_ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = 0;
	for (bid=0 ; bid<SUSTAINED_BUCKETS ; bid++) {
		*dbuffsize += fsnodes_getdetached(sustained[bid],NULL);
	}
	return MFS_STATUS_OK;
}

void fs_readsustained_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff) {
	uint32_t pos,bid;
	(void)rootinode;
	(void)sesflags;
	pos = 0;
	for (bid=0 ; bid<SUSTAINED_BUCKETS ; bid++) {
		pos += fsnodes_getdetached(sustained[bid],dbuff+pos);
	}
}

uint8_t fs_readtrash_size(uint32_t rootinode,uint8_t sesflags,uint32_t bid,uint32_t *dbuffsize) {
	if (rootinode!=0) {
		return MFS_ERROR_EPERM;
	}
	(void)sesflags;
	if (bid>=TRASH_BUCKETS) {
		*dbuffsize = 0;
		for (bid=0 ; bid<TRASH_BUCKETS ; bid++) {
			*dbuffsize += fsnodes_getdetached(trash[bid],NULL);
		}
	} else {
		*dbuffsize = fsnodes_getdetached(trash[bid],NULL);
	}
	return MFS_STATUS_OK;
}

void fs_readtrash_data(uint32_t rootinode,uint8_t sesflags,uint32_t bid,uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	if (bid>=TRASH_BUCKETS) {
		uint32_t pos;
		pos = 0;
		for (bid=0 ; bid<TRASH_BUCKETS ; bid++) {
			pos += fsnodes_getdetached(trash[bid],dbuff+pos);
		}
	} else {
		fsnodes_getdetached(trash[bid],dbuff);
	}
}

/* common procedure for trash and sustained files */
uint8_t fs_getdetachedattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t attr[ATTR_RECORD_SIZE],uint8_t dtype) {
	fsnode *p;
	memset(attr,0,ATTR_RECORD_SIZE);
	if (rootinode!=0) {
		return MFS_ERROR_EPERM;
	}
	(void)sesflags;
	if (!DTYPE_ISVALID(dtype)) {
		return MFS_ERROR_EINVAL;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_ENOENT;
	}
	if (dtype==DTYPE_TRASH && p->type==TYPE_SUSTAINED) {
		return MFS_ERROR_ENOENT;
	}
	if (dtype==DTYPE_SUSTAINED && p->type==TYPE_TRASH) {
		return MFS_ERROR_ENOENT;
	}
	fsnodes_fill_attr(p,NULL,p->uid,p->gid,p->uid,p->gid,sesflags,attr,1);
	return MFS_STATUS_OK;
}

uint8_t fs_gettrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,const uint8_t **path) {
	fsnode *p;
	*pleng = 0;
	*path = NULL;
	if (rootinode!=0) {
		return MFS_ERROR_EPERM;
	}
	(void)sesflags;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return MFS_ERROR_ENOENT;
	}
	*pleng = p->parents->nleng;
	*path = p->parents->name;
	return MFS_STATUS_OK;
}

uint8_t fs_univ_setpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t pleng,const uint8_t *path) {
	uint32_t trash_cid;
	fsnode *p;
	fsedge *e;
	uint32_t i;
	if (rootinode!=0) {
		return MFS_ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (pleng==0 || pleng>MFS_PATH_MAX) {
		return MFS_ERROR_EINVAL;
	}
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {
			return MFS_ERROR_EINVAL;
		}
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return MFS_ERROR_ENOENT;
	}
	fsnodes_remove_edge(0,p->parents);
	e = fsedge_malloc(pleng);
	passert(e);
	if (nextedgeid<EDGEID_MAX) {
		e->edgeid = nextedgeid--;
	} else {
		e->edgeid = 0;
	}
	trash_cid = inode % TRASH_BUCKETS;
	e->nleng = pleng;
	memcpy((uint8_t*)(e->name),path,pleng);
	e->child = p;
	e->parent = NULL;
	e->nextchild = trash[trash_cid];
	e->nextparent = NULL;
	e->prevchild = trash + trash_cid;
	e->prevparent = &(p->parents);
	if (e->nextchild) {
		e->nextchild->prevchild = &(e->nextchild);
	}
	trash[trash_cid] = e;
	p->parents = e;

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETPATH(%"PRIu32",%s)",(uint32_t)main_time(),inode,changelog_escape_name(pleng,path));
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_settrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t pleng,const uint8_t *path) {
	return fs_univ_setpath(rootinode,sesflags,inode,pleng,path);
}

uint8_t fs_mr_setpath(uint32_t inode,const uint8_t *path) {
	return fs_univ_setpath(0,SESFLAG_METARESTORE,inode,strlen((char*)path),path);
}

uint8_t fs_univ_undel(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode) {
	fsnode *p;
	uint8_t status;
	if (rootinode!=0) {
		return MFS_ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return MFS_ERROR_ENOENT;
	}
	status = fsnodes_undel(ts,p);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|UNDEL(%"PRIu32")",ts,inode);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_undel(uint32_t rootinode,uint8_t sesflags,uint32_t inode) {
	return fs_univ_undel(main_time(),rootinode,sesflags,inode);
}

uint8_t fs_mr_undel(uint32_t ts,uint32_t inode) {
	return fs_univ_undel(ts,0,SESFLAG_METARESTORE,inode);
}

uint8_t fs_univ_purge(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode) {
	fsnode *p;
	if (rootinode!=0) {
		return MFS_ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return MFS_ERROR_ENOENT;
	}
	fsnodes_purge(ts,p);
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|PURGE(%"PRIu32")",ts,inode);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_purge(uint32_t rootinode,uint8_t sesflags,uint32_t inode) {
	return fs_univ_purge(main_time(),rootinode,sesflags,inode);
}

uint8_t fs_mr_purge(uint32_t ts,uint32_t inode) {
	return fs_univ_purge(ts,0,SESFLAG_METARESTORE,inode);
}

uint8_t fs_get_parents_count(uint32_t rootinode,uint32_t inode,uint32_t *cnt) {
	fsnode *p;

	if (fsnodes_node_find_ext(rootinode,0,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}

	*cnt = fsnodes_nlink(rootinode,p);
	return MFS_STATUS_OK;
}

void fs_get_parents_data(uint32_t rootinode,uint32_t inode,uint8_t *buff) {
	fsnode *p;

	if (fsnodes_node_find_ext(rootinode,0,&inode,NULL,&p,0)) {
		fsnodes_get_parents(rootinode,p,buff);
	}
}

uint8_t fs_get_paths_size(uint32_t rootinode,uint32_t inode,uint32_t *psize) {
	fsnode *p;

	if (fsnodes_node_find_ext(rootinode,0,&inode,NULL,&p,0)==0) {
		*psize = 9+4;
		return MFS_ERROR_ENOENT;
	}

	if (p->type==TYPE_TRASH) {
		*psize = 7+4+3+p->parents->nleng;
	} else if (p->type==TYPE_SUSTAINED) {
		*psize = 11+4+3+p->parents->nleng;
	} else {
		*psize = fsnodes_get_paths_size(rootinode,p);
	}
	return MFS_STATUS_OK;
}

void fs_get_paths_data(uint32_t rootinode,uint32_t inode,uint8_t *buff) {
	fsnode *p;

	if (fsnodes_node_find_ext(rootinode,0,&inode,NULL,&p,0)) {
		if (p->type==TYPE_TRASH) {
			put32bit(&buff,7+3+p->parents->nleng);
			memcpy(buff,"./TRASH (",9);
			memcpy(buff+9,p->parents->name,p->parents->nleng);
			buff[9+p->parents->nleng]=')';
		} else if (p->type==TYPE_SUSTAINED) {
			put32bit(&buff,11+3+p->parents->nleng);
			memcpy(buff,"./SUSTAINED (",13);
			memcpy(buff+13,p->parents->name,p->parents->nleng);
			buff[13+p->parents->nleng]=')';
		} else {
			fsnodes_get_paths_data(rootinode,p,buff);
		}
	} else {
		put32bit(&buff,9);
		memcpy(buff,"(deleted)",9);
	}
}

void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *freespace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes) {
	matocsserv_getspace(totalspace,availspace,freespace);
	*trspace = trashspace;
	*trnodes = trashnodes;
	*respace = sustainedspace;
	*renodes = sustainednodes;
	*inodes = nodes;
	*dnodes = dirnodes;
	*fnodes = filenodes;
}

uint8_t fs_getrootinode(uint32_t *rootinode,const uint8_t *path) {
	uint32_t nleng;
	const uint8_t *name;
	fsnode *p;
	fsedge *e;

	name = path;
	p = root;
	for (;;) {
		while (*name=='/') {
			name++;
		}
		if (*name=='\0') {
			*rootinode = p->inode;
			return MFS_STATUS_OK;
		}
		nleng=0;
		while (name[nleng] && name[nleng]!='/') {
			nleng++;
		}
		if (fsnodes_namecheck(nleng,name)<0) {
			return MFS_ERROR_EINVAL;
		}
		e = fsnodes_lookup(p,nleng,name);
		if (!e) {
			return MFS_ERROR_ENOENT;
		}
		p = e->child;
		if (p->type!=TYPE_DIRECTORY) {
			return MFS_ERROR_ENOTDIR;
		}
		name += nleng;
	}
}

void fs_statfs(uint32_t rootinode,uint8_t sesflags,uint64_t *totalspace,uint64_t *availspace,uint64_t *freespace,uint64_t *trspace,uint64_t *respace,uint32_t *inodes) {
	fsnode *rn;
	statsrecord sr;
	(void)sesflags;
	if (rootinode==MFS_ROOT_ID) {
		*trspace = trashspace;
		*respace = sustainedspace;
		rn = root;
	} else {
		*trspace = 0;
		*respace = 0;
		rn = fsnodes_node_find(rootinode);
	}
	if (!rn || rn->type!=TYPE_DIRECTORY) {
		*totalspace = 0;
		*availspace = 0;
		*freespace = 0;
		*inodes = 0;
	} else {
		matocsserv_getspace(totalspace,availspace,freespace);
		fsnodes_quota_fixspace(rn,totalspace,availspace);
		fsnodes_quota_fixspace(rn,totalspace,freespace);
		fsnodes_get_stats(rn,&sr,2);
		*inodes = sr.inodes;
	}
	stats_statfs++;
}

uint8_t fs_access(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,int modemask) {
	fsnode *p;
	if ((sesflags&SESFLAG_READONLY) && (modemask&MODE_MASK_W)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	return fsnodes_access_ext(p,uid,gids,gid,modemask,sesflags)?MFS_STATUS_OK:MFS_ERROR_EACCES;
}

uint8_t fs_lookup(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE],uint8_t *accmode,uint8_t *filenode,uint8_t *validchunk,uint64_t *chunkid) {
	fsnode *wd,*rn,*p;
	fsedge *e;

	*inode = 0;
	memset(attr,0,ATTR_RECORD_SIZE);

	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,&rn,&wd,0)==0) {
		return MFS_ERROR_ENOENT_NOCACHE;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}
	if (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_X,sesflags)) {
		return MFS_ERROR_EACCES;
	}
	if (name[0]=='.') {
		if (nleng==1) {	// self
			if (parent==rootinode) {
				*inode = MFS_ROOT_ID;
			} else {
				*inode = wd->inode;
			}
			fsnodes_fill_attr(wd,wd,uid,gid[0],auid,agid,sesflags,attr,1);
			stats_lookup++;
			return MFS_STATUS_OK;
		}
		if (nleng==2 && name[1]=='.') {	// parent
			if (parent==rootinode) {
				*inode = MFS_ROOT_ID;
				fsnodes_fill_attr(wd,wd,uid,gid[0],auid,agid,sesflags,attr,1);
			} else {
				if (wd->parents) {
					if (wd->parents->parent->inode==rootinode) {
						*inode = MFS_ROOT_ID;
					} else {
						*inode = wd->parents->parent->inode;
					}
					fsnodes_fill_attr(wd->parents->parent,wd,uid,gid[0],auid,agid,sesflags,attr,1);
				} else {
					*inode=MFS_ROOT_ID; // rn->inode;
					fsnodes_fill_attr(rn,wd,uid,gid[0],auid,agid,sesflags,attr,1);
				}
			}
			stats_lookup++;
			return MFS_STATUS_OK;
		}
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return MFS_ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		if (wd->eattr&EATTR_NOECACHE) {
			return MFS_ERROR_ENOENT_NOCACHE;
		} else {
			return MFS_ERROR_ENOENT;
		}
	}
	p = e->child;
	*inode = p->inode;
	if (filenode) {
		*filenode = (p->type==TYPE_FILE || p->type==TYPE_TRASH || p->type==TYPE_SUSTAINED)?1:0;
	}
	fsnodes_fill_attr(p,wd,uid,gid[0],auid,agid,sesflags,attr,1);
	if (accmode!=NULL) {
		*accmode = fsnodes_accessmode(p,uid,gids,gid,sesflags);
	}
	if (validchunk!=NULL && chunkid!=NULL) {
		*validchunk = 0;
		*chunkid = 0;
		if (p->type==TYPE_FILE || p->type==TYPE_TRASH || p->type==TYPE_SUSTAINED) {
			if (p->data.fdata.chunks==1) {
				*chunkid = p->data.fdata.chunktab[0];
				if (*chunkid == 0) {
					*validchunk = 1;
				} else if (chunk_read_check(main_time(),*chunkid)==MFS_STATUS_OK) {
					*validchunk = 1;
				}
			}
		}
	}
	stats_lookup++;
	return MFS_STATUS_OK;
}

uint8_t fs_getattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t attr[ATTR_RECORD_SIZE]) {
	fsnode *p;

	(void)sesflags;
	memset(attr,0,ATTR_RECORD_SIZE);
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return MFS_ERROR_ENOENT;
	}
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr,1);
	stats_getattr++;
	return MFS_STATUS_OK;
}

uint8_t fs_try_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t flags,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[ATTR_RECORD_SIZE],uint32_t *indx,uint64_t *prevchunkid,uint64_t *chunkid) {
	fsnode *p;
	memset(attr,0,ATTR_RECORD_SIZE);
	if (sesflags&SESFLAG_READONLY) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,flags & TRUNCATE_FLAG_OPENED)==0) {
		return MFS_ERROR_ENOENT;
	}
	if ((flags & TRUNCATE_FLAG_OPENED)==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
			return MFS_ERROR_EACCES;
		}
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (flags & TRUNCATE_FLAG_UPDATE) {
		return MFS_STATUS_OK;
	}
	if (length>p->data.fdata.length) {
		uint32_t lastchunk_pre,lastchunksize_pre,lastchunk_post,lastchunksize_post;
		uint64_t size_diff;
		if (p->data.fdata.length>0) {
			lastchunk_pre = (p->data.fdata.length-1)>>MFSCHUNKBITS;
			lastchunksize_pre = ((((p->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastchunk_pre = 0;
			lastchunksize_pre = MFSHDRSIZE;
		}
		if (length>0) {
			lastchunk_post = (length-1)>>MFSCHUNKBITS;
			lastchunksize_post = ((((length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastchunk_post = 0;
			lastchunksize_post = MFSHDRSIZE;
		}
		if (p->data.fdata.chunktab==NULL || p->data.fdata.chunktab[lastchunk_pre]==0) {
			size_diff = 0;
		} else {
			if (lastchunk_post>lastchunk_pre) {
				size_diff = MFSCHUNKSIZE+MFSHDRSIZE - lastchunksize_pre;
			} else { // lastchunk_post == lastchunk_pre
				size_diff = lastchunksize_post-lastchunksize_pre;
			}
		}
		if (fsnodes_test_quota(p,0,length-p->data.fdata.length,size_diff,sclass_get_keepmax_goal(p->sclassid)*size_diff)) {
			return MFS_ERROR_QUOTA;
		}
	}
	if (length!=p->data.fdata.length) {
		if (length&MFSCHUNKMASK) {
			*indx = (length>>MFSCHUNKBITS);
			if (*indx<p->data.fdata.chunks) {
				uint64_t ochunkid = p->data.fdata.chunktab[*indx];
				if (ochunkid>0) {
					uint8_t status;
					uint64_t nchunkid;
					status = chunk_multi_truncate(&nchunkid,ochunkid,length&MFSCHUNKMASK,p->sclassid);
					*prevchunkid = ochunkid;
					if (status!=MFS_STATUS_OK) {
						return status;
					}
					p->data.fdata.chunktab[*indx] = nchunkid;
					*chunkid = nchunkid;
					changelog("%"PRIu32"|TRUNC(%"PRIu32",%"PRIu32"):%"PRIu64,(uint32_t)main_time(),inode,*indx,nchunkid);
					return MFS_ERROR_DELAYED;
				}
			}
		}
	}
	fsnodes_fill_attr(p,NULL,uid,gid[0],auid,agid,sesflags,attr,1);
	stats_setattr++;
	return MFS_STATUS_OK;
}

uint8_t fs_mr_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t nchunkid) {
	uint64_t ochunkid;
	uint8_t status;
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EINVAL;
	}
	if (indx>MAX_INDEX) {
		return MFS_ERROR_INDEXTOOBIG;
	}
	if (indx>=p->data.fdata.chunks) {
		return MFS_ERROR_EINVAL;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_mr_multi_truncate(ts,&nchunkid,ochunkid,p->sclassid);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	meta_version_inc();
	return MFS_STATUS_OK;
}

uint8_t fs_end_setlength(uint64_t chunkid) {
	changelog("%"PRIu32"|UNLOCK(%"PRIu64")",(uint32_t)main_time(),chunkid);
	return chunk_unlock(chunkid);
}

uint8_t fs_mr_unlock(uint64_t chunkid) {
	uint8_t status;
	status = chunk_mr_unlock(chunkid);
	if (status==MFS_STATUS_OK) {
		meta_version_inc();
	}
	return status;
}

uint8_t fs_do_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t flags,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[ATTR_RECORD_SIZE]) {
	fsnode *p;
	uint32_t ts = main_time();
	uint8_t chtime = 1;

	memset(attr,0,ATTR_RECORD_SIZE);
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (flags & TRUNCATE_FLAG_UPDATE) {
		if (length>p->data.fdata.length) {
			fsnodes_setlength(p,length);
			changelog("%"PRIu32"|LENGTH(%"PRIu32",%"PRIu64",0)",ts,inode,p->data.fdata.length);
		}
	} else {
		if (length==p->data.fdata.length && (flags&TRUNCATE_FLAG_TIMEFIX)) {
			chtime = 0;
		}
		fsnodes_setlength(p,length);
		changelog("%"PRIu32"|LENGTH(%"PRIu32",%"PRIu64",%"PRIu8")",ts,inode,p->data.fdata.length,chtime);
		if (chtime) {
			p->ctime = p->mtime = ts;
		}
		stats_setattr++;
	}
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr,1);
	return MFS_STATUS_OK;
}


uint8_t fs_setattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t winattr,uint8_t sugidclearmode,uint8_t attr[ATTR_RECORD_SIZE]) {
	fsnode *p;
	uint8_t gf;
	uint32_t i;
	uint32_t ts = main_time();

	memset(attr,0,ATTR_RECORD_SIZE);
	if (sesflags&SESFLAG_READONLY) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (!(uid!=0 && setmask==SET_MODE_FLAG && attrmode==(p->mode&01777) && (p->mode&06000)!=0)) { // ignore permission tests for special case - clear suid/sgid during write
		if (uid!=0 && (sesflags&SESFLAG_MAPALL) && (setmask&(SET_UID_FLAG|SET_GID_FLAG))) {
			return MFS_ERROR_EPERM;
		}
		if ((p->eattr&EATTR_NOOWNER)==0) {
			if (uid!=0 && uid!=p->uid && (setmask&(SET_MODE_FLAG|SET_UID_FLAG|SET_GID_FLAG|SET_ATIME_FLAG|SET_MTIME_FLAG))) {
				return MFS_ERROR_EPERM;
			}
			if (uid!=0 && uid!=p->uid && (setmask&(SET_ATIME_NOW_FLAG|SET_MTIME_NOW_FLAG))) {
				if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
					return MFS_ERROR_EACCES;
				}
			}
		}
		if (uid!=0 && uid!=attruid && (setmask&SET_UID_FLAG)) {
			return MFS_ERROR_EPERM;
		}
		if ((sesflags&SESFLAG_IGNOREGID)==0) {
			if (uid!=0 && (setmask&SET_GID_FLAG)) {
				gf = 0;
				for (i=0 ; i<gids && gf==0 ; i++) {
					if (gid[i]==attrgid) {
						gf = 1;
					}
				}
				if (gf==0) {
					return MFS_ERROR_EPERM;
				}
			}
		}
	}
	// first ignore sugid clears done by kernel
	if ((setmask&(SET_UID_FLAG|SET_GID_FLAG)) && (setmask&SET_MODE_FLAG)) {	// chown+chmod = chown with sugid clears
		attrmode |= (p->mode & 06000);
	}
	// then do it yourself
	if ((p->mode & 06000) && (setmask&(SET_UID_FLAG|SET_GID_FLAG))) { // this is "chown" operation and suid or sgid bit is set
		switch (sugidclearmode) {
		case SUGID_CLEAR_MODE_ALWAYS:
			p->mode &= 01777; // safest approach - always delete both suid and sgid
			attrmode &= 01777;
			break;
		case SUGID_CLEAR_MODE_OSX:
			if (uid!=0) { // OSX+Solaris - every change done by unprivileged user should clear suid and sgid
				p->mode &= 01777;
				attrmode &= 01777;
			}
			break;
		case SUGID_CLEAR_MODE_BSD:
			if (uid!=0 && (setmask&SET_GID_FLAG) && p->gid!=attrgid) { // *BSD - like in OSX but only when something is actually changed
				p->mode &= 01777;
				attrmode &= 01777;
			}
			break;
		case SUGID_CLEAR_MODE_EXT:
			if (p->type!=TYPE_DIRECTORY) {
				if (p->mode & 010) { // when group exec is set - clear both bits
					p->mode &= 01777;
					attrmode &= 01777;
				} else { // when group exec is not set - clear suid only
					p->mode &= 03777;
					attrmode &= 03777;
				}
			}
			break;
		case SUGID_CLEAR_MODE_XFS:
			if (p->type!=TYPE_DIRECTORY) { // similar to EXT3, but unprivileged users also clear suid/sgid bits on directories
				if (p->mode & 010) {
					p->mode &= 01777;
					attrmode &= 01777;
				} else {
					p->mode &= 03777;
					attrmode &= 03777;
				}
			} else if (uid!=0) {
				p->mode &= 01777;
				attrmode &= 01777;
			}
			break;
		}
	}
	if (setmask&SET_UID_FLAG) {
		p->uid = attruid;
	}
	if (setmask&SET_GID_FLAG) {
		p->gid = attrgid;
	}
	if (setmask&SET_MODE_FLAG) {
		if (uid!=0 && (attrmode&02000)!=0) {
			gf = 0;
			for (i=0 ; i<gids && gf==0 ; i++) {
				if (gid[i]==p->gid) {
					gf = 1;
				}
			}
			if (gf==0) { // not my group, so can't set sgid flag
				attrmode &= 05777;
			}
		}
		if (p->aclpermflag) {
			posix_acl_setmode(p->inode,attrmode);
			p->mode &= 00070;
			attrmode &= 07707;
			p->mode |= attrmode;
		} else {
			p->mode = attrmode;
		}
	}
// 
	if (setmask&SET_ATIME_FLAG) {
		p->atime = attratime;
	}
	if (setmask&SET_MTIME_FLAG) {
		p->mtime = attrmtime;
	}
	if (setmask&SET_ATIME_NOW_FLAG) {
		p->atime = ts;
	}
	if (setmask&SET_MTIME_NOW_FLAG) {
		p->mtime = ts;
	}
	if (setmask&SET_WINATTR_FLAG) {
		p->winattr = winattr;
	}
	changelog("%"PRIu32"|ATTR(%"PRIu32",%"PRIu16",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu8",%"PRIu16")",ts,inode,(uint16_t)(p->mode),p->uid,p->gid,p->atime,p->mtime,p->winattr,(uint16_t)((p->aclpermflag)?((posix_acl_getmode(p->inode)&07777)+(1U<<12)):0));
	p->ctime = ts;
	fsnodes_fill_attr(p,NULL,uid,gid[0],auid,agid,sesflags,attr,1);
	stats_setattr++;
	return MFS_STATUS_OK;
}

uint8_t fs_mr_attr(uint32_t ts,uint32_t inode,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime,uint8_t winattr,uint16_t aclmode) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (mode>07777) {
		return MFS_ERROR_EINVAL;
	}
	p->mode = mode;
	if (p->aclpermflag) {
		if ((aclmode & (1<<12)) == 0) { // wrong aclmode produced by mfs older than 3.0.98
			aclmode = mode & 0707; // fix user and other bits
			aclmode |= 0070; // use 'full' mask
			mfs_arg_syslog(LOG_WARNING,"set attributes for inode %"PRIu32" with posix acl - emergency set mask to 'rwx' - upgrade all masters to newest version and check ACL's for this inode",inode);
		}
		posix_acl_setmode(p->inode,aclmode);
	}
	p->uid = uid;
	p->gid = gid;
	p->atime = atime;
	p->mtime = mtime;
	p->ctime = ts;
	p->winattr = winattr;
	meta_version_inc();
	return MFS_STATUS_OK;
}

uint8_t fs_mr_length(uint32_t ts,uint32_t inode,uint64_t length,uint8_t canmodmtime) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EINVAL;
	}
	fsnodes_setlength(p,length);
	if (canmodmtime) {
		p->mtime = p->ctime = ts;
	}
	meta_version_inc();
	return MFS_STATUS_OK;
}

uint8_t fs_readlink(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path) {
	fsnode *p;
	uint32_t ts = main_time();

	(void)sesflags;
	*pleng = 0;
	*path = NULL;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_SYMLINK) {
		return MFS_ERROR_EINVAL;
	}
	*pleng = p->data.sdata.pleng;
	*path = p->data.sdata.path;
	if (p->atime!=ts) {
		if ((AtimeMode==ATIME_ALWAYS) || (((p->atime <= p->ctime && ts >= p->ctime) || (p->atime <= p->mtime && ts >= p->mtime) || (p->atime + 86400 < ts)) && AtimeMode==ATIME_RELATIVE_ONLY)) {
			p->atime = ts;
			changelog("%"PRIu32"|ACCESS(%"PRIu32")",ts,inode);
		}
	}
	stats_readlink++;
	return MFS_STATUS_OK;
}

uint8_t fs_univ_symlink(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	fsnode *wd,*p;
	uint8_t *newpath;
	statsrecord sr;
	uint32_t i;
	*inode = 0;
	if (attr) {
		memset(attr,0,ATTR_RECORD_SIZE);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (pleng==0 || pleng>MFS_SYMLINK_MAX) {
		return MFS_ERROR_EINVAL;
	}
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {
			return MFS_ERROR_EINVAL;
		}
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,NULL,&wd,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return MFS_ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return MFS_ERROR_EEXIST;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && fsnodes_test_quota(wd,1,pleng,0,0)) {
		return MFS_ERROR_QUOTA;
	}
	newpath = symlink_malloc(pleng);
	passert(newpath);
	p = fsnodes_create_node(ts,wd,nleng,name,TYPE_SYMLINK,0777,0,uid,gid[0],0);
	memcpy(newpath,path,pleng);
	p->data.sdata.path = newpath;
	p->data.sdata.pleng = pleng;

	memset(&sr,0,sizeof(statsrecord));
	sr.length = pleng;
	fsnodes_add_stats(wd,&sr);

	*inode = p->inode;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (attr) {
			fsnodes_fill_attr(p,wd,uid,gid[0],auid,agid,sesflags,attr,1);
		}
		changelog("%"PRIu32"|SYMLINK(%"PRIu32",%s,%s,%"PRIu32",%"PRIu32"):%"PRIu32,(uint32_t)main_time(),parent,changelog_escape_name(nleng,name),changelog_escape_name(pleng,newpath),uid,gid[0],p->inode);
	} else {
		meta_version_inc();
	}
	stats_symlink++;
	return MFS_STATUS_OK;
}

uint8_t fs_symlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	return fs_univ_symlink(main_time(),rootinode,sesflags,parent,nleng,name,pleng,path,uid,gids,gid,auid,agid,inode,attr);
}

uint8_t fs_mr_symlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_symlink(ts,0,SESFLAG_METARESTORE,parent,nleng,name,strlen((char*)path),path,uid,1,&gid,0,0,&rinode,NULL);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		syslog(LOG_WARNING,"SYMLINK data mismatch: my:%"PRIu32" != expected:%"PRIu32,rinode,inode);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_univ_create(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t rdev,uint8_t copysgid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	fsnode *wd,*p;
	*inode = 0;
	if (attr) {
		memset(attr,0,ATTR_RECORD_SIZE);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (type!=TYPE_FILE && type!=TYPE_SOCKET && type!=TYPE_FIFO && type!=TYPE_BLOCKDEV && type!=TYPE_CHARDEV && type!=TYPE_DIRECTORY) {
		return MFS_ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,NULL,&wd,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return MFS_ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return MFS_ERROR_EEXIST;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && fsnodes_test_quota(wd,1,0,0,0)) {
		return MFS_ERROR_QUOTA;
	}
	p = fsnodes_create_node(ts,wd,nleng,name,type,mode,cumask,uid,gid[0],copysgid);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
	}
	*inode = p->inode;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (attr) {
			fsnodes_fill_attr(p,wd,uid,gid[0],auid,agid,sesflags,attr,1);
		}
		changelog("%"PRIu32"|CREATE(%"PRIu32",%s,%"PRIu8",%"PRIu16",%"PRIu16",%"PRIu32",%"PRIu32",%"PRIu32"):%"PRIu32,ts,parent,changelog_escape_name(nleng,name),type,mode,cumask,uid,gid[0],rdev,p->inode);
	} else {
		meta_version_inc();
	}
	if (type==TYPE_DIRECTORY) {
		stats_mkdir++;
	} else {
		stats_mknod++;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_mknod(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t rdev,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	if (type>15) {
		type = fsnodes_type_convert(type);
	}
	return fs_univ_create(main_time(),rootinode,sesflags,parent,nleng,name,type,mode,cumask,uid,gids,gid,auid,agid,rdev,0,inode,attr);
}

uint8_t fs_mkdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t copysgid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	return fs_univ_create(main_time(),rootinode,sesflags,parent,nleng,name,TYPE_DIRECTORY,mode,cumask,uid,gids,gid,auid,agid,0,copysgid,inode,attr);
}

uint8_t fs_mr_create(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	if (type>15) {
		type = fsnodes_type_convert(type);
	}
	status = fs_univ_create(ts,0,SESFLAG_METARESTORE,parent,nleng,name,type,mode,cumask,uid,1,&gid,0,0,rdev,0,&rinode,NULL);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		syslog(LOG_WARNING,"CREATE data mismatch: my:%"PRIu32" != expected:%"PRIu32,rinode,inode);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_univ_unlink(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t dirmode,uint32_t *inode) {
	fsnode *wd;
	fsedge *e;
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,NULL,&wd,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return MFS_ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return MFS_ERROR_ENOENT;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_sticky_access(wd,e->child,uid))) {
		return MFS_ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (dirmode==0) {
			if (e->child->type==TYPE_DIRECTORY) {
				return MFS_ERROR_EPERM;
			}
		} else {
			if (e->child->type!=TYPE_DIRECTORY) {
				return MFS_ERROR_ENOTDIR;
			}
			if (e->child->data.ddata.children!=NULL) {
				return MFS_ERROR_ENOTEMPTY;
			}
		}
	} else {
		if (e->child->type==TYPE_DIRECTORY) {
			dirmode = 1;
			if (e->child->data.ddata.children!=NULL) {
				return MFS_ERROR_ENOTEMPTY;
			}
		} else {
			dirmode = 0;
		}
	}
	if (inode) {
		*inode = e->child->inode;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|UNLINK(%"PRIu32",%s):%"PRIu32,ts,parent,changelog_escape_name(nleng,name),e->child->inode);
	} else {
		meta_version_inc();
	}
	fsnodes_unlink(ts,e);
	if (dirmode==0) {
		stats_unlink++;
	} else {
		stats_rmdir++;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_unlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode) {
	return fs_univ_unlink(main_time(),rootinode,sesflags,parent,nleng,name,uid,gids,gid,0,inode);
}

uint8_t fs_rmdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode) {
	return fs_univ_unlink(main_time(),rootinode,sesflags,parent,nleng,name,uid,gids,gid,1,inode);
}

uint8_t fs_mr_unlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_unlink(ts,0,SESFLAG_METARESTORE,parent,nleng,name,0,0,0,0,&rinode);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		syslog(LOG_WARNING,"UNLINK data mismatch: my:%"PRIu32" != expected:%"PRIu32,rinode,inode);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_univ_move(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent_src,uint16_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	fsnode *swd;
	fsedge *se;
	fsnode *dwd;
	fsedge *de;
	fsnode *node;
	statsrecord ssr,dsr;
	*inode = 0;
	if (attr) {
		memset(attr,0,ATTR_RECORD_SIZE);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent_src,NULL,&swd,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&parent_dst,NULL,&dwd,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (swd->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(swd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng_src,name_src)<0) {
		return MFS_ERROR_EINVAL;
	}
	se = fsnodes_lookup(swd,nleng_src,name_src);
	if (!se) {
		return MFS_ERROR_ENOENT;
	}
	node = se->child;
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_sticky_access(swd,node,uid))) {
		return MFS_ERROR_EPERM;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(dwd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (node->type==TYPE_DIRECTORY) {
		if (fsnodes_isancestor(node,dwd)) {
			return MFS_ERROR_EINVAL;
		}
		if (parent_src!=parent_dst && (sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(node,uid,gids,gid,MODE_MASK_W,sesflags))) { // '..' link has to be formally changed during such operation - we need 'W' access
			return MFS_ERROR_EACCES;
		}
	}
	if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
		return MFS_ERROR_EINVAL;
	}
	de = fsnodes_lookup(dwd,nleng_dst,name_dst);
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		fsnodes_get_stats(node,&ssr,2);
		if (de) {
			fsnodes_get_stats(de->child,&dsr,2);
			if (ssr.inodes>dsr.inodes) {
				ssr.inodes -= dsr.inodes;
			} else {
				ssr.inodes = 0;
			}
			if (ssr.length>dsr.length) {
				ssr.length -= dsr.length;
			} else {
				ssr.length = 0;
			}
			if (ssr.size>dsr.size) {
				ssr.size -= dsr.size;
			} else {
				ssr.size = 0;
			}
			if (ssr.realsize>dsr.realsize) {
				ssr.realsize -= dsr.realsize;
			} else {
				ssr.realsize = 0;
			}
		}
		if (fsnodes_test_quota_for_uncommon_nodes(dwd,swd,ssr.inodes,ssr.length,ssr.size,ssr.realsize)) {
			return MFS_ERROR_QUOTA;
		}
	}
	if (de) {
		if (de->child->type==TYPE_DIRECTORY && de->child->data.ddata.children!=NULL) {
			return MFS_ERROR_ENOTEMPTY;
		}
		if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_sticky_access(dwd,de->child,uid))) {
			return MFS_ERROR_EPERM;
		}
		fsnodes_unlink(ts,de);
	}
	fsnodes_remove_edge(ts,se);
	fsnodes_link(ts,dwd,node,nleng_dst,name_dst);
	*inode = node->inode;
	if (attr) {
		fsnodes_fill_attr(node,dwd,uid,gid[0],auid,agid,sesflags,attr,1);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|MOVE(%"PRIu32",%s,%"PRIu32",%s):%"PRIu32,ts,parent_src,changelog_escape_name(nleng_src,name_src),parent_dst,changelog_escape_name(nleng_dst,name_dst),node->inode);
	} else {
		meta_version_inc();
	}
	stats_rename++;
	return MFS_STATUS_OK;
}

uint8_t fs_rename(uint32_t rootinode,uint8_t sesflags,uint32_t parent_src,uint16_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	return fs_univ_move(main_time(),rootinode,sesflags,parent_src,nleng_src,name_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,inode,attr);
}

uint8_t fs_mr_move(uint32_t ts,uint32_t parent_src,uint32_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint32_t nleng_dst,const uint8_t *name_dst,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_move(ts,0,SESFLAG_METARESTORE,parent_src,nleng_src,name_src,parent_dst,nleng_dst,name_dst,0,0,0,0,0,&rinode,NULL);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		syslog(LOG_WARNING,"MOVE data mismatch: my:%"PRIu32" != expected:%"PRIu32,rinode,inode);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_univ_link(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	statsrecord sr;
	fsnode *sp;
	fsnode *dwd;
	uint16_t nlink;

	*inode = 0;
	if (attr) {
		memset(attr,0,ATTR_RECORD_SIZE);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode_src,NULL,&sp,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&parent_dst,NULL,&dwd,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	nlink = 0;
	switch (sp->type) {
		case TYPE_TRASH:
		case TYPE_SUSTAINED:
			return MFS_ERROR_ENOENT;
		case TYPE_DIRECTORY:
			return MFS_ERROR_EPERM;
		case TYPE_FILE:
			nlink = sp->data.fdata.nlink;
			break;
		case TYPE_SOCKET:
		case TYPE_FIFO:
			nlink = sp->data.odata.nlink;
			break;
		case TYPE_BLOCKDEV:
		case TYPE_CHARDEV:
			nlink = sp->data.devdata.nlink;
			break;
		case TYPE_SYMLINK:
			nlink = sp->data.sdata.nlink;
			break;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && nlink>=MaxAllowedHardLinks) {
		return MFS_ERROR_NOSPACE;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(dwd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
		return MFS_ERROR_EINVAL;
	}
	if (fsnodes_nameisused(dwd,nleng_dst,name_dst)) {
		return MFS_ERROR_EEXIST;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		fsnodes_get_stats(sp,&sr,2);
		if (fsnodes_test_quota(dwd,sr.inodes,sr.length,sr.size,sr.realsize)) {
			return MFS_ERROR_QUOTA;
		}
	}

	fsnodes_link(ts,dwd,sp,nleng_dst,name_dst);
	*inode = inode_src;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (attr) {
			fsnodes_fill_attr(sp,dwd,uid,gid[0],auid,agid,sesflags,attr,1);
		}
		changelog("%"PRIu32"|LINK(%"PRIu32",%"PRIu32",%s)",ts,inode_src,parent_dst,changelog_escape_name(nleng_dst,name_dst));
	} else {
		meta_version_inc();
	}
	stats_link++;
	return MFS_STATUS_OK;
}

uint8_t fs_link(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	return fs_univ_link(main_time(),rootinode,sesflags,inode_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,inode,attr);
}

uint8_t fs_mr_link(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint32_t nleng_dst,uint8_t *name_dst) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_link(ts,0,SESFLAG_METARESTORE,inode_src,parent_dst,nleng_dst,name_dst,0,0,NULL,0,0,&rinode,NULL);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (rinode!=inode_src) {
		syslog(LOG_WARNING,"LINK data mismatch: my:%"PRIu32" != expected:%"PRIu32,rinode,inode_src);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_univ_snapshot(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t smode,uint16_t cumask,uint32_t inodecheck,uint32_t removed,uint32_t same,uint32_t exisiting,uint32_t hardlinks,uint32_t new) {
	statsrecord ssr;
	uint32_t common_inodes;
	uint64_t common_length;
	uint64_t common_size;
	uint64_t common_realsize;
	fsnodes_snapshot_params args;
	fsnode *sp;
	fsnode *dwd;
	fsedge *e;
	uint8_t status;
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	memset(&args,0,sizeof(fsnodes_snapshot_params));
	args.inode_chksum = 0;
	args.ts = ts;
	args.smode = smode;
	args.sesflags = sesflags;
	args.uid = uid;
	args.gids = gids;
	args.gid = gid;
	args.cumask = cumask;
	if (smode & SNAPSHOT_MODE_DELETE) { // remove mode
		if (inode_src!=0) {
			return MFS_ERROR_EINVAL;
		}
		if (fsnodes_node_find_ext(rootinode,sesflags,&parent_dst,NULL,&dwd,0)==0) {
			return MFS_ERROR_ENOENT;
		}
		if (dwd->type!=TYPE_DIRECTORY) {
			return MFS_ERROR_EPERM;
		}
		if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(dwd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
			return MFS_ERROR_EACCES;
		}
		if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
			return MFS_ERROR_EINVAL;
		}
		e = fsnodes_lookup(dwd,nleng_dst,name_dst);
		if (!e) {
			return MFS_ERROR_ENOENT;
		}
		if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_sticky_access(dwd,e->child,uid))) {
			return MFS_ERROR_EPERM;
		}
		fsnodes_keep_alive_begin();
		if ((smode & SNAPSHOT_MODE_FORCE_REMOVAL)==0 && (sesflags&SESFLAG_METARESTORE)==0) {
			status = fsnodes_remove_snapshot_test(e,&args);
			if (status!=MFS_STATUS_OK) {
				return status;
			}
		}
		fsnodes_remove_snapshot(e,&args);
	} else {
		if (inode_src==0) {
			return MFS_ERROR_EINVAL;
		}
		if (fsnodes_node_find_ext(rootinode,sesflags,&inode_src,NULL,&sp,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&parent_dst,NULL,&dwd,0)==0) {
			return MFS_ERROR_ENOENT;
		}
		if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(sp,uid,gids,gid,MODE_MASK_R,sesflags))) {
			return MFS_ERROR_EACCES;
		}
		if (dwd->type!=TYPE_DIRECTORY) {
			return MFS_ERROR_EPERM;
		}
		if (sp->type==TYPE_DIRECTORY) {
			if (sp==dwd || fsnodes_isancestor(sp,dwd)) {
				return MFS_ERROR_EINVAL;
			}
		}
		if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(dwd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
			return MFS_ERROR_EACCES;
		}
		if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
			return MFS_ERROR_EINVAL;
		}

		fsnodes_keep_alive_begin();
		status = fsnodes_snapshot_test(sp,sp,dwd,nleng_dst,name_dst,smode & SNAPSHOT_MODE_CAN_OVERWRITE);
		if (status!=MFS_STATUS_OK) {
			return status;
		}

		if ((sesflags&SESFLAG_METARESTORE)==0) {
			fsnodes_get_stats(sp,&ssr,2);
			common_inodes = 0;
			common_length = 0;
			common_size = 0;
			common_realsize = 0;
			if (fsnodes_snapshot_recursive_test_quota(sp,dwd,nleng_dst,name_dst,&common_inodes,&common_length,&common_size,&common_realsize)) {
				return MFS_ERROR_QUOTA;
			}
			if (ssr.inodes>common_inodes) {
				ssr.inodes -= common_inodes;
			} else {
				ssr.inodes = 0;
			}
			if (ssr.length>common_length) {
				ssr.length -= common_length;
			} else {
				ssr.length = 0;
			}
			if (ssr.size>common_size) {
				ssr.size -= common_size;
			} else {
				ssr.size = 0;
			}
			if (ssr.realsize>common_realsize) {
				ssr.realsize -= common_realsize;
			} else {
				ssr.realsize = 0;
			}
			if (fsnodes_test_quota(dwd,ssr.inodes,ssr.length,ssr.size,ssr.realsize)) {
				return MFS_ERROR_QUOTA;
			}
		}
//		if (smode & SNAPSHOT_MODE_PRESERVE_HARDLINKS) {
//			chash_erase(snapshot_inodehash);
//		}
		fsnodes_snapshot(sp,dwd,nleng_dst,name_dst,0,&args);
		if (smode & SNAPSHOT_MODE_PRESERVE_HARDLINKS) {
			chash_erase(snapshot_inodehash);
		}
	}
//	syslog(LOG_NOTICE,"snapshot ts=%"PRIu32" ; inode=%"PRIu32" ; inode_chksum=0x%08"PRIX32" ; removed=%"PRIu32" ; same=%"PRIu32" ; existing=%"PRIu32" ; hardlinks=%"PRIu32" ; new=%"PRIu32,ts,inode_src,args.inode_chksum,args.removed_object,args.same_file,args.existing_object,args.new_hardlink,args.new_object);
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SNAPSHOT(%"PRIu32",%"PRIu32",%s,%"PRIu8",%"PRIu8",%"PRIu32",%s,%"PRIu16"):%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32,ts,inode_src,parent_dst,changelog_escape_name(nleng_dst,name_dst),smode,sesflags,uid,changelog_generate_gids(gids,gid),cumask,args.inode_chksum,args.removed_object,args.same_file,args.existing_object,args.new_hardlink,args.new_object);
	} else {
		if ((inodecheck|removed|same|exisiting|hardlinks|new)!=0 && (inodecheck!=args.inode_chksum || removed!=args.removed_object || same!=args.same_file || exisiting!=args.existing_object || hardlinks!=args.new_hardlink || new!=args.new_object)) {
			syslog(LOG_WARNING,"SNAPSHOT data mismatch: my:(%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32") != expected:(%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32")",args.inode_chksum,args.removed_object,args.same_file,args.existing_object,args.new_hardlink,args.new_object,inodecheck,removed,same,exisiting,hardlinks,new);
			return MFS_ERROR_MISMATCH;
		}
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_snapshot(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t smode,uint16_t cumask) {
	return fs_univ_snapshot(main_time(),rootinode,sesflags,inode_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,smode,cumask,0,0,0,0,0,0);
}

uint8_t fs_mr_snapshot(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint8_t smode,uint8_t sesflags,uint32_t uid,uint32_t gids,uint32_t *gid,uint16_t cumask,uint32_t inodecheck,uint32_t removed,uint32_t same,uint32_t exisiting,uint32_t hardlinks,uint32_t new) {
	return fs_univ_snapshot(ts,0,sesflags|SESFLAG_METARESTORE,inode_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,smode,cumask,inodecheck,removed,same,exisiting,hardlinks,new);
}

uint8_t fs_univ_append_slice(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint8_t flags,uint32_t inode,uint32_t inode_src,uint32_t slice_from,uint32_t slice_to,uint32_t uid,uint32_t gids,uint32_t *gid,uint64_t *fleng) {
	uint32_t dstchunks,i;
	uint32_t lastsrcchunk,lastsrcchunksize;
	uint32_t lastdstchunk,lastdstchunksize;
	uint64_t addlength,newlength;
	uint64_t lengdiff,sizediff;
	uint8_t status;
	fsnode *p,*sp;
	if (inode==inode_src) {
		return MFS_ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode_src,NULL,&sp,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (sp->type!=TYPE_FILE && sp->type!=TYPE_TRASH && sp->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(sp,uid,gids,gid,MODE_MASK_R,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags))) {
		return MFS_ERROR_EACCES;
	}
	if (sp->data.fdata.length==0) {
		if ((sesflags&SESFLAG_METARESTORE)!=0) {
			meta_version_inc();
		} else if (fleng!=NULL) {
			*fleng = p->data.fdata.length;
		}
		return MFS_STATUS_OK;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		// fix slices and check quota
		lastsrcchunk = (sp->data.fdata.length-1)>>MFSCHUNKBITS;
		if (flags&APPEND_SLICE_FROM_NEG) {
			if (slice_from>lastsrcchunk+1) {
				slice_from = 0;
			} else {
				slice_from = lastsrcchunk+1-slice_from;
			}
		}
		if (flags&APPEND_SLICE_TO_NEG) {
			if (slice_to>lastsrcchunk+1) {
				if (fleng!=NULL) {
					*fleng = p->data.fdata.length;
				}
				return MFS_STATUS_OK;
			} else {
				slice_to = lastsrcchunk+1-slice_to;
			}
		}
		slice_to--; // change [from,to) -> [from,to] ; 0 can become 0xFFFFFFFF here - it is ok, because 0 means end of file
		if (slice_to>=lastsrcchunk) {
			slice_to = lastsrcchunk;
			if (slice_to<slice_from) {
				if (fleng!=NULL) {
					*fleng = p->data.fdata.length;
				}
				return MFS_STATUS_OK;
			}
			addlength = sp->data.fdata.length - (((uint64_t)slice_from)<<MFSCHUNKBITS);
			lastsrcchunksize = ((((sp->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			if (slice_to<slice_from) {
				if (fleng!=NULL) {
					*fleng = p->data.fdata.length;
				}
				return MFS_STATUS_OK;
			}
			addlength = ((uint64_t)(1+slice_to-slice_from))<<MFSCHUNKBITS;
			lastsrcchunksize = MFSCHUNKSIZE+MFSHDRSIZE;
		}
		if (p->data.fdata.length>0) {
			lastdstchunk = (p->data.fdata.length-1)>>MFSCHUNKBITS;
			dstchunks = lastdstchunk+1;
			lastdstchunksize = ((((p->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastdstchunk = 0xFFFFFFFF;
			dstchunks = 0;
			lastdstchunksize = MFSHDRSIZE;
		}
		newlength = (((uint64_t)dstchunks)<<MFSCHUNKBITS) + addlength;
		if (newlength < p->data.fdata.length) { // length overflow
			return MFS_ERROR_EINVAL;
		}
		lengdiff = newlength - p->data.fdata.length;
		sizediff = 0;
		for (i=slice_from ; i<=slice_to ; i++) {
			if (i<sp->data.fdata.chunks && sp->data.fdata.chunktab[i]>0) {
				if (i<lastsrcchunk) {
					sizediff += MFSCHUNKSIZE+MFSHDRSIZE;
				} else if (i==lastsrcchunk) {
					sizediff += lastsrcchunksize;
				}
			}
		}
		for (i=0 ; i<p->data.fdata.chunks ; i++) {
			if (p->data.fdata.chunktab[i]>0) {
				if (i>lastdstchunk || dstchunks==0) {
					sizediff -= MFSCHUNKSIZE+MFSHDRSIZE;
				} else if (i==lastdstchunk) {
					sizediff += (MFSCHUNKSIZE+MFSHDRSIZE) - lastdstchunksize;
				}
			}
		}
		if (fsnodes_test_quota(p,0,lengdiff,sizediff,sclass_get_keepmax_goal(p->sclassid)*sizediff)) {
			return MFS_ERROR_QUOTA;
		}
	}
	status = fsnodes_append_slice_of_chunks(ts,p,sp,slice_from,slice_to);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|APPEND(%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32")",ts,inode,inode_src,slice_from,slice_to);
		if (fleng!=NULL) {
			*fleng = p->data.fdata.length;
		}
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

// slice - [from,to) and uses 'negative' flags
uint8_t fs_append_slice(uint32_t rootinode,uint8_t sesflags,uint8_t flags,uint32_t inode,uint32_t inode_src,uint32_t slice_from,uint32_t slice_to,uint32_t uid,uint32_t gids,uint32_t *gid,uint64_t *fleng) {
	return fs_univ_append_slice(main_time(),rootinode,sesflags,flags,inode,inode_src,slice_from,slice_to,uid,gids,gid,fleng);
}

// slice - [from,to]
uint8_t fs_mr_append_slice(uint32_t ts,uint32_t inode,uint32_t inode_src,uint32_t slice_from,uint32_t slice_to) {
	return fs_univ_append_slice(ts,0,SESFLAG_METARESTORE,0,inode,inode_src,slice_from,slice_to,0,0,NULL,NULL);
}

uint8_t fs_readdir_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t flags,uint32_t maxentries,uint64_t nedgeid,void **dnode,void **dedge,uint32_t *dbuffsize,uint8_t attrmode) {
	fsnode *p;
	fsedge *e;
	*dnode = NULL;
	*dbuffsize = 0;

	if (nedgeid==0 || nedgeid==1) {
		e = NULL;
	} else {
		e = fsnodes_edgeid_find(nedgeid);
	}
	if (e==NULL) {
		if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
			return MFS_ERROR_ENOENT;
		}
		if (p->type!=TYPE_DIRECTORY) {
			return MFS_ERROR_ENOTDIR;
		}
		if (nedgeid==0) {
			if (flags&GETDIR_FLAG_WITHATTR) {
				if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R|MODE_MASK_X,sesflags)) {
					return MFS_ERROR_EACCES;
				}
			} else {
				if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
					return MFS_ERROR_EACCES;
				}
			}
		} else {
			e=p->data.ddata.children;
			while (e && e->edgeid < nedgeid) {
				e = e->nextchild;
			}
		}
	} else {
		p = e->parent;
	}
	*dnode = p;
	*dedge = e;
	*dbuffsize = fsnodes_readdirsize(p,e,maxentries,nedgeid,(flags&GETDIR_FLAG_WITHATTR)?attrmode:0);
	return MFS_STATUS_OK;
}

void fs_readdir_data(uint32_t rootinode,uint8_t sesflags,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,uint32_t maxentries,uint64_t *nedgeid,void *dnode,void *dedge,uint8_t *dbuff,uint8_t attrmode) {
	fsnode *p = (fsnode*)dnode;
	fsedge *e = (fsedge*)dedge;
	uint32_t ts = main_time();

	if (p->atime!=ts) {
		if ((AtimeMode==ATIME_ALWAYS) || (((p->atime <= p->ctime && ts >= p->ctime) || (p->atime <= p->mtime && ts >= p->mtime) || (p->atime + 86400 < ts)) && AtimeMode==ATIME_RELATIVE_ONLY)) {
			p->atime = ts;
			changelog("%"PRIu32"|ACCESS(%"PRIu32")",ts,p->inode);
		}
	}
	fsnodes_readdirdata(rootinode,uid,gid,auid,agid,sesflags,p,e,maxentries,nedgeid,dbuff,(flags&GETDIR_FLAG_WITHATTR)?attrmode:0);
	stats_readdir++;
}

uint8_t fs_filechunk(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t indx,uint64_t *chunkid) {
	fsnode *p;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return MFS_ERROR_INDEXTOOBIG;
	}
	if (indx<p->data.fdata.chunks) {
		*chunkid = p->data.fdata.chunktab[indx];
	} else {
		*chunkid = 0;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_checkfile(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t chunkcount[12]) {
	fsnode *p;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	fsnodes_checkfile(p,chunkcount);
	return MFS_STATUS_OK;
}

uint8_t fs_opencheck(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t flags,uint8_t attr[ATTR_RECORD_SIZE]) {
	fsnode *p;
	if ((sesflags&SESFLAG_READONLY) && (flags&WANT_WRITE)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if ((flags&AFTER_CREATE)==0) {
		uint8_t modemask=0;
		if (flags&WANT_READ) {
			modemask|=MODE_MASK_R;
		}
		if (flags&WANT_WRITE) {
			modemask|=MODE_MASK_W;
		}
		if (!fsnodes_access_ext(p,uid,gids,gid,modemask,sesflags)) {
			return MFS_ERROR_EACCES;
		}
	}
	fsnodes_fill_attr(p,NULL,uid,gid[0],auid,agid,sesflags,attr,1);
	stats_open++;
	return MFS_STATUS_OK;
}

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint8_t chunkopflags,uint64_t *chunkid,uint64_t *length) {
	int status;
	fsnode *p;
	uint32_t ts = main_time();

	*chunkid = 0;
	*length = 0;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return MFS_ERROR_INDEXTOOBIG;
	}
	if (indx<p->data.fdata.chunks) {
		*chunkid = p->data.fdata.chunktab[indx];
	}
	if (*chunkid>0) {
		status = chunk_read_check(ts,*chunkid);
		if (status!=MFS_STATUS_OK) {
			return status;
		}
	}
	*length = p->data.fdata.length;
	if (p->atime!=ts && (chunkopflags&CHUNKOPFLAG_CANMODTIME)) {
		if ((AtimeMode==ATIME_ALWAYS || AtimeMode==ATIME_FILES_ONLY) || (((p->atime <= p->ctime && ts >= p->ctime) || (p->atime <= p->mtime && ts >= p->mtime) || (p->atime + 86400 < ts)) && (AtimeMode==ATIME_RELATIVE_ONLY || AtimeMode==ATIME_FILES_AND_RELATIVE_ONLY))) {
			p->atime = ts;
			changelog("%"PRIu32"|ACCESS(%"PRIu32")",ts,inode);
		}
	}
	stats_read++;
	return MFS_STATUS_OK;
}

uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint8_t chunkopflags,uint64_t *prevchunkid,uint64_t *chunkid,uint64_t *length,uint8_t *opflag) {
	int status;
	uint32_t i;
	uint64_t ochunkid,nchunkid,nlength,lengdiff;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;
	uint32_t ts = main_time();
	uint32_t lastchunk,lastchunksize,sizediff;

	if (matocsserv_have_availspace()==0 && (chunkopflags&CHUNKOPFLAG_CANUSERESERVESPACE)==0) {
		return MFS_ERROR_NOSPACE;
	}
	*chunkid = 0;
	*length = 0;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return MFS_ERROR_INDEXTOOBIG;
	}
	// quota check
//	maxlengatend = (indx+1);
//	maxlengatend <<= MFSCHUNKBITS;
//	if (maxlengatend>p->data.fdata.length) {
//		lengdiff = maxlengatend-p->data.fdata.length;
//	} else {
//		lengdiff = 0;
//	}
	if (p->data.fdata.length>0) {
		lastchunk = (p->data.fdata.length-1)>>MFSCHUNKBITS;
		lastchunksize = ((((p->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
	} else {
		lastchunk = 0;
		lastchunksize = MFSHDRSIZE;
	}
	if (indx<lastchunk) {
		nlength = p->data.fdata.length;
		if (indx>=p->data.fdata.chunks || p->data.fdata.chunktab[indx]==0) {
			sizediff = MFSCHUNKSIZE+MFSHDRSIZE;
		} else {
			sizediff = 0;
		}
	} else if (indx==lastchunk) {
		nlength = p->data.fdata.length;
		if (indx>=p->data.fdata.chunks || p->data.fdata.chunktab[indx]==0) {
			sizediff = MFSCHUNKSIZE+MFSHDRSIZE;
		} else {
			sizediff = (MFSCHUNKSIZE+MFSHDRSIZE) - lastchunksize;
		}
	} else {
		nlength = indx;
		nlength <<= MFSCHUNKBITS;
		sizediff = MFSCHUNKSIZE+MFSHDRSIZE;
	}
	lengdiff = nlength - p->data.fdata.length;
	if (fsnodes_test_quota(p,0,lengdiff,sizediff,sclass_get_keepmax_goal(p->sclassid)*sizediff)) {
		return MFS_ERROR_QUOTA;
	}
	fsnodes_get_stats(p,&psr,0);
	/* resize chunks structure */
	if (indx>=p->data.fdata.chunks) {
		uint32_t newchunks = indx+1;
		if (p->data.fdata.chunktab==NULL) {
			p->data.fdata.chunktab = chunktab_malloc(newchunks);
		} else {
			p->data.fdata.chunktab = chunktab_realloc(p->data.fdata.chunktab,p->data.fdata.chunks,newchunks);
		}
		passert(p->data.fdata.chunktab);
		for (i=p->data.fdata.chunks ; i<newchunks ; i++) {
			p->data.fdata.chunktab[i]=0;
		}
		p->data.fdata.chunks = newchunks;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	*prevchunkid = ochunkid;
	status = chunk_multi_modify((chunkopflags&CHUNKOPFLAG_CONTINUEOP)?1:0,&nchunkid,ochunkid,p->sclassid,opflag);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	if (nlength>p->data.fdata.length) {
		if (p->type==TYPE_TRASH) {
			trashspace -= p->data.fdata.length;
			trashspace += nlength;
		} else if (p->type==TYPE_SUSTAINED) {
			sustainedspace -= p->data.fdata.length;
			sustainedspace += nlength;
		}
		p->data.fdata.length = nlength;
	}
	fsnodes_get_stats(p,&nsr,1);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	p->eattr &= ~(EATTR_SNAPSHOT);
	*chunkid = nchunkid;
	*length = p->data.fdata.length;
	changelog("%"PRIu32"|WRITE(%"PRIu32",%"PRIu32",%"PRIu8",%u):%"PRIu64,ts,inode,indx,*opflag,(chunkopflags&CHUNKOPFLAG_CANMODTIME)?1:0,nchunkid);
	if ((p->mtime!=ts || p->ctime!=ts) && (chunkopflags&CHUNKOPFLAG_CANMODTIME)) {
		p->mtime = p->ctime = ts;
	}
//	syslog(LOG_NOTICE,"write end: inode: %u ; indx: %u ; chunktab[indx]: %"PRIu64" ; chunks: %u",inode,indx,p->data.fdata.chunktab[indx],p->data.fdata.chunks);
	stats_write++;
	return MFS_STATUS_OK;
}

uint8_t fs_mr_write(uint32_t ts,uint32_t inode,uint32_t indx,uint8_t opflag,uint8_t canmodmtime,uint64_t nchunkid) {
	int status;
	uint32_t i;
	uint64_t ochunkid,nlength;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;

	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return MFS_ERROR_INDEXTOOBIG;
	}
	/* resize chunks structure */
	fsnodes_get_stats(p,&psr,0);
	if (indx>=p->data.fdata.chunks) {
		uint32_t newchunks = indx+1;
		if (p->data.fdata.chunktab==NULL) {
			p->data.fdata.chunktab = chunktab_malloc(newchunks);
		} else {
			p->data.fdata.chunktab = chunktab_realloc(p->data.fdata.chunktab,p->data.fdata.chunks,newchunks);
		}
		passert(p->data.fdata.chunktab);
		for (i=p->data.fdata.chunks ; i<newchunks ; i++) {
			p->data.fdata.chunktab[i]=0;
		}
		p->data.fdata.chunks = newchunks;
	}
	nlength = indx;
	nlength<<=MFSCHUNKBITS;
	if (nlength < p->data.fdata.length) {
		nlength = p->data.fdata.length;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_mr_multi_modify(ts,&nchunkid,ochunkid,p->sclassid,opflag);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	if (nlength>p->data.fdata.length) {
		if (p->type==TYPE_TRASH) {
			trashspace -= p->data.fdata.length;
			trashspace += nlength;
		} else if (p->type==TYPE_SUSTAINED) {
			sustainedspace -= p->data.fdata.length;
			sustainedspace += nlength;
		}
		p->data.fdata.length = nlength;
	}
	fsnodes_get_stats(p,&nsr,1);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	p->eattr &= ~(EATTR_SNAPSHOT);
	if (canmodmtime) {
		p->mtime = p->ctime = ts;
	}
	meta_version_inc();
	return MFS_STATUS_OK;
}

static inline uint8_t fs_univ_rollback(uint8_t mr,uint32_t inode,uint32_t indx,uint64_t prevchunkid,uint64_t chunkid) {
	uint32_t ts = main_time();
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;
//	syslog(LOG_NOTICE,"rollback inode:%u indx:%u !!!",inode,indx);
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return MFS_ERROR_INDEXTOOBIG;
	}
	if (indx>=p->data.fdata.chunks) {
		return MFS_ERROR_EINVAL;
	}
	if (prevchunkid!=chunkid) {
		fsnodes_get_stats(p,&psr,0);
		if (prevchunkid>0) {
			chunk_add_file(prevchunkid,p->sclassid);
		}
		chunk_delete_file(chunkid,p->sclassid);
		p->data.fdata.chunktab[indx] = prevchunkid;
		fsnodes_get_stats(p,&nsr,1);
		for (e=p->parents ; e ; e=e->nextparent) {
			fsnodes_add_sub_stats(e->parent,&nsr,&psr);
		}
	}
	chunk_unlock(chunkid);
	if (mr) {
		meta_version_inc();
	} else {
		changelog("%"PRIu32"|ROLLBACK(%"PRIu32",%"PRIu32",%"PRIu64",%"PRIu64")",ts,inode,indx,prevchunkid,chunkid);
	}
	return MFS_STATUS_OK;
}

uint8_t fs_rollback(uint32_t inode,uint32_t indx,uint64_t prevchunkid,uint64_t chunkid) {
	return fs_univ_rollback(0,inode,indx,prevchunkid,chunkid);
}

uint8_t fs_mr_rollback(uint32_t inode,uint32_t indx,uint64_t prevchunkid,uint64_t chunkid) {
	return fs_univ_rollback(1,inode,indx,prevchunkid,chunkid);
}

uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid,uint8_t chunkopflags,uint8_t *flenghaschanged) {
	uint32_t ts = main_time();
	if (length>0) {
		fsnode *p;
		p = fsnodes_node_find(inode);
		if (!p) {
			return MFS_ERROR_ENOENT;
		}
		if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
			return MFS_ERROR_EPERM;
		}
//		syslog(LOG_NOTICE,"writeend: inode: %u ; length: %"PRIu64":%"PRIu64" ; chunkid: %016"PRIX64,inode,length,p->data.fdata.length,chunkid);
//		{
//			uint32_t i;
//			for (i=0 ; i<p->data.fdata.chunks ; i++) {
//				syslog(LOG_NOTICE,"writeend: inode: %u ; indx: %u ; chunkid: %016"PRIX64,inode,i,p->data.fdata.chunktab[i]);
//			}
//		}
		if (length>p->data.fdata.length) {
			if (fsnodes_test_quota(p,0,length-p->data.fdata.length,0,0)) {
				return MFS_ERROR_QUOTA;
			}
			fsnodes_setlength(p,length);
			if (chunkopflags & CHUNKOPFLAG_CANMODTIME) {
				p->mtime = p->ctime = ts;
			}
			changelog("%"PRIu32"|LENGTH(%"PRIu32",%"PRIu64",%u)",ts,inode,length,(chunkopflags & CHUNKOPFLAG_CANMODTIME)?1:0);
			if (flenghaschanged!=NULL) {
				*flenghaschanged = 1;
			}
		}
//		{
//			uint32_t i;
//			for (i=0 ; i<p->data.fdata.chunks ; i++) {
//				syslog(LOG_NOTICE,"writeend: inode: %u ; indx: %u ; chunkid: %016"PRIX64,inode,i,p->data.fdata.chunktab[i]);
//			}
//		}
//	} else {
//		syslog(LOG_NOTICE,"writeend: inode: %u ; length: 0 ; chunkid: %016"PRIX64,inode,chunkid);
	}
	changelog("%"PRIu32"|UNLOCK(%"PRIu64")",ts,chunkid);
	return chunk_unlock(chunkid);
}

uint8_t fs_mr_amtime(uint32_t inode,uint32_t atime,uint32_t mtime,uint32_t ctime) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	p->atime = atime;
	p->mtime = mtime;
	p->ctime = ctime;
	meta_version_inc();
	return MFS_STATUS_OK;
}

void fs_amtime_update(uint32_t rootinode,uint8_t sesflags,uint32_t *inodetab,uint32_t *atimetab,uint32_t *mtimetab,uint32_t cnt) {
	fsnode *p;
	uint32_t i,atime,mtime,ts;
	uint8_t chg;

	ts = main_time();

	for (i=0 ; i<cnt ; i++) {
		if (fsnodes_node_find_ext(rootinode,sesflags,inodetab+i,NULL,&p,0)) {
			chg = 0;
			atime = atimetab[i];
			mtime = mtimetab[i];
			if (p->atime<atime) {
				if ((AtimeMode==ATIME_ALWAYS || AtimeMode==ATIME_FILES_ONLY) || (((p->atime <= p->ctime && atime >= p->ctime) || (p->atime <= p->mtime && atime >= p->mtime) || (p->atime + 86400 < atime)) && (AtimeMode==ATIME_RELATIVE_ONLY || AtimeMode==ATIME_FILES_AND_RELATIVE_ONLY))) {
					p->atime = atime;
					chg = 1;
				}
			}
			if (p->mtime<mtime) {
				p->ctime = p->mtime = mtime;
				chg = 1;
			}
			if (chg) {
				changelog("%"PRIu32"|AMTIME(%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32")",ts,inodetab[i],p->atime,p->mtime,p->ctime);
			}
		}
	}
}

uint8_t fs_repair(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *notchanged,uint32_t *erased,uint32_t *repaired) {
	uint32_t nversion,indx;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;
	uint32_t ts = main_time();

	*notchanged = 0;
	*erased = 0;
	*repaired = 0;
	if (sesflags&SESFLAG_READONLY) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
		return MFS_ERROR_EACCES;
	}
	fsnodes_get_stats(p,&psr,0);
	for (indx=0 ; indx<p->data.fdata.chunks ; indx++) {
		if (chunk_repair(p->sclassid,p->data.fdata.chunktab[indx],&nversion)) {
			changelog("%"PRIu32"|REPAIR(%"PRIu32",%"PRIu32"):%"PRIu32,ts,inode,indx,nversion);
			p->mtime = p->ctime = ts;
			if (nversion>0) {
				(*repaired)++;
			} else {
				p->data.fdata.chunktab[indx] = 0;
				(*erased)++;
			}
		} else {
			(*notchanged)++;
		}
	}
	fsnodes_get_stats(p,&nsr,1);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	return MFS_STATUS_OK;
}

uint8_t fs_mr_repair(uint32_t ts,uint32_t inode,uint32_t indx,uint32_t nversion) {
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;
	uint8_t status;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return MFS_ERROR_INDEXTOOBIG;
	}
	if (indx>=p->data.fdata.chunks) {
		return MFS_ERROR_NOCHUNK;
	}
	if (p->data.fdata.chunktab[indx]==0) {
		return MFS_ERROR_NOCHUNK;
	}
	fsnodes_get_stats(p,&psr,0);
	if (nversion==0) {
		status = chunk_delete_file(p->data.fdata.chunktab[indx],p->sclassid);
		p->data.fdata.chunktab[indx]=0;
	} else {
		status = chunk_mr_set_version(p->data.fdata.chunktab[indx],nversion);
	}
	fsnodes_get_stats(p,&nsr,1);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	p->mtime = p->ctime = ts;
	if (status==MFS_STATUS_OK) {
		meta_version_inc();
	}
	return status;
}

uint8_t fs_getsclass(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t fgtab[256],uint32_t dgtab[256]) {
	fsnode *p;
	(void)sesflags;
	memset(fgtab,0,256*sizeof(uint32_t));
	memset(dgtab,0,256*sizeof(uint32_t));
	if (!GMODE_ISVALID(gmode)) {
		return MFS_ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	fsnodes_keep_alive_begin();
	fsnodes_getsclass_recursive(p,gmode,fgtab,dgtab);
	return MFS_STATUS_OK;
}

uint8_t fs_gettrashtime_prepare(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,void **fptr,void **dptr,uint32_t *fnodes,uint32_t *dnodes) {
	fsnode *p;
	bstnode *froot,*droot;
	(void)sesflags;
	froot = NULL;
	droot = NULL;
	*fptr = NULL;
	*dptr = NULL;
	*fnodes = 0;
	*dnodes = 0;
	if (!GMODE_ISVALID(gmode)) {
		return MFS_ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	fsnodes_keep_alive_begin();
	fsnodes_gettrashtime_recursive(p,gmode,&froot,&droot);
	*fptr = froot;
	*dptr = droot;
	*fnodes = fsnodes_bst_nodes(froot);
	*dnodes = fsnodes_bst_nodes(droot);
	return MFS_STATUS_OK;
}

void fs_gettrashtime_store(void *fptr,void *dptr,uint8_t *buff) {
	bstnode *froot,*droot;
	froot = (bstnode*)fptr;
	droot = (bstnode*)dptr;
	fsnodes_bst_storedata(froot,&buff);
	fsnodes_bst_storedata(droot,&buff);
	fsnodes_bst_free(froot);
	fsnodes_bst_free(droot);
}

uint8_t fs_geteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t feattrtab[32],uint32_t deattrtab[32]) {
	fsnode *p;
	(void)sesflags;
	memset(feattrtab,0,32*sizeof(uint32_t));
	memset(deattrtab,0,32*sizeof(uint32_t));
	if (!GMODE_ISVALID(gmode)) {
		return MFS_ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	fsnodes_keep_alive_begin();
	fsnodes_geteattr_recursive(p,gmode,feattrtab,deattrtab);
	return MFS_STATUS_OK;
}

uint8_t fs_univ_setsclass(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t src_sclassid,uint8_t dst_sclassid,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	uint64_t realsize;
	uint8_t admin;
	fsnode *p;

	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;

	if (!SMODE_ISVALID(smode) || dst_sclassid==0 || src_sclassid==0) {
		return MFS_ERROR_EINVAL;
	}
	if (((smode&SMODE_TMASK)==SMODE_INCREASE || (smode&SMODE_TMASK)==SMODE_DECREASE) && dst_sclassid>9) {
		return MFS_ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}

	fsnodes_keep_alive_begin();
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		// quota
		if ((smode&SMODE_TMASK)==SMODE_SET || (smode&SMODE_TMASK)==SMODE_INCREASE) {
			realsize = 0;
			if (fsnodes_setsclass_recursive_test_quota(p,uid,sclass_get_keepmax_goal(dst_sclassid),(smode&SMODE_RMASK)?1:0,&realsize)) {
				return MFS_ERROR_QUOTA;
			}
		}
	}

	admin = (sesflags&(SESFLAG_ADMIN|SESFLAG_METARESTORE))?1:0;

	fsnodes_setsclass_recursive(p,ts,uid,src_sclassid,dst_sclassid,smode,admin,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return MFS_ERROR_EPERM;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETSCLASS(%"PRIu32",%"PRIu32",%"PRIu8",%"PRIu8",%"PRIu8"):%"PRIu32",%"PRIu32",%"PRIu32,ts,inode,uid,src_sclassid,dst_sclassid,smode,*sinodes,*ncinodes,*nsinodes);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_setsclass(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t src_sclassid,uint8_t dst_sclassid,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	return fs_univ_setsclass(main_time(),rootinode,sesflags,inode,uid,src_sclassid,dst_sclassid,smode,sinodes,ncinodes,nsinodes);
}

uint8_t fs_mr_setsclass(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t src_sclassid,uint8_t dst_sclassid,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
	uint8_t status;
	status = fs_univ_setsclass(ts,0,SESFLAG_METARESTORE,inode,uid,src_sclassid,dst_sclassid,smode,&si,&nci,&nsi);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		syslog(LOG_WARNING,"SETSCLASS data mismatch: my:(%"PRIu32",%"PRIu32",%"PRIu32") != expected:(%"PRIu32",%"PRIu32",%"PRIu32")",si,nci,nsi,sinodes,ncinodes,nsinodes);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_univ_settrashtime(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsnode *p;

	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
	if (!SMODE_ISVALID(smode)) {
		return MFS_ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}

	fsnodes_keep_alive_begin();
	fsnodes_settrashtime_recursive(p,ts,uid,(trashtime+3599)/3600,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return MFS_ERROR_EPERM;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETTRASHTIME(%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu8"):%"PRIu32",%"PRIu32",%"PRIu32,ts,inode,uid,trashtime,smode,*sinodes,*ncinodes,*nsinodes);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_settrashtime(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	return fs_univ_settrashtime(main_time(),rootinode,sesflags,inode,uid,trashtime,smode,sinodes,ncinodes,nsinodes);
}

uint8_t fs_mr_settrashtime(uint32_t ts,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
	uint8_t status;
	status = fs_univ_settrashtime(ts,0,SESFLAG_METARESTORE,inode,uid,trashtime,smode,&si,&nci,&nsi);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		syslog(LOG_WARNING,"SETTRASHTIME data mismatch: my:(%"PRIu32",%"PRIu32",%"PRIu32") != expected:(%"PRIu32",%"PRIu32",%"PRIu32")",si,nci,nsi,sinodes,ncinodes,nsinodes);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_univ_seteattr(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsnode *p;

	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
	if (!SMODE_ISVALID(smode) || (eattr&(~(EATTR_NOOWNER|EATTR_NOACACHE|EATTR_NOECACHE|EATTR_NODATACACHE|EATTR_SNAPSHOT)))) {
		return MFS_ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}

	fsnodes_keep_alive_begin();
	fsnodes_seteattr_recursive(p,ts,uid,eattr,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return MFS_ERROR_EPERM;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETEATTR(%"PRIu32",%"PRIu32",%"PRIu8",%"PRIu8"):%"PRIu32",%"PRIu32",%"PRIu32,ts,inode,uid,eattr,smode,*sinodes,*ncinodes,*nsinodes);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_seteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	return fs_univ_seteattr(main_time(),rootinode,sesflags,inode,uid,eattr,smode,sinodes,ncinodes,nsinodes);
}

uint8_t fs_mr_seteattr(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
	uint8_t status;
	status = fs_univ_seteattr(ts,0,SESFLAG_METARESTORE,inode,uid,eattr,smode,&si,&nci,&nsi);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		syslog(LOG_WARNING,"SETEATTR data mismatch: my:(%"PRIu32",%"PRIu32",%"PRIu32") != expected:(%"PRIu32",%"PRIu32",%"PRIu32")",si,nci,nsi,sinodes,ncinodes,nsinodes);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint8_t fs_listxattr_leng(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,void **xanode,uint32_t *xasize) {
	fsnode *p;

	*xasize = 0;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
			return MFS_ERROR_EACCES;
		}
	}
	return xattr_listattr_leng(inode,xanode,xasize);
}

void fs_listxattr_data(void *xanode,uint8_t *xabuff) {
	xattr_listattr_data(xanode,xabuff);
}

uint8_t fs_setxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode) {
	uint32_t ts;
	fsnode *p;
	uint8_t status;

	ts = main_time();
	if (sesflags&SESFLAG_READONLY) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (uid!=0) {
		if ((anleng>=8 && memcmp(attrname,"trusted.",8)==0) || (anleng>=9 && memcmp(attrname,"security.",9)==0)) {
			return MFS_ERROR_EPERM;
		}
	}
	if ((p->eattr&EATTR_NOOWNER)==0 && uid!=0 && uid!=p->uid) {
		if (anleng>=7 && memcmp(attrname,"system.",7)==0) {
			return MFS_ERROR_EPERM;
		}
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
			return MFS_ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng,attrname)<0) {
		return MFS_ERROR_EINVAL;
	}
	if (mode>MFS_XATTR_REMOVE) {
		return MFS_ERROR_EINVAL;
	}
	status = xattr_setattr(inode,anleng,attrname,avleng,attrvalue,mode);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	changelog("%"PRIu32"|SETXATTR(%"PRIu32",%s,%s,%"PRIu8")",ts,inode,changelog_escape_name(anleng,attrname),changelog_escape_name(avleng,attrvalue),mode);
	return MFS_STATUS_OK;
}

uint8_t fs_getxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,const uint8_t **attrvalue) {
	fsnode *p;

	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (uid!=0) {
		if (anleng>=8 && memcmp(attrname,"trusted.",8)==0) {
			return MFS_ERROR_EPERM;
		}
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
			return MFS_ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng,attrname)<0) {
		return MFS_ERROR_EINVAL;
	}
	return xattr_getattr(inode,anleng,attrname,avleng,attrvalue);
}

uint8_t fs_mr_setxattr(uint32_t ts,uint32_t inode,uint32_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint32_t mode) {
	fsnode *p;
	uint8_t status;
	if (anleng==0 || anleng>MFS_XATTR_NAME_MAX || avleng>MFS_XATTR_SIZE_MAX || mode>MFS_XATTR_REMOVE) {
		return MFS_ERROR_EINVAL;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	status = xattr_setattr(inode,anleng,attrname,avleng,attrvalue,mode);

	if (status!=MFS_STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	if (status==MFS_STATUS_OK) {
		meta_version_inc();
	}
	return status;
}

uint8_t fs_setfacl(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob) {
	uint32_t ts;
	uint16_t pmode;
	fsnode *p;

	ts = main_time();
	if (sesflags&SESFLAG_READONLY) {
		return MFS_ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if ((p->eattr&EATTR_NOOWNER)==0 && uid!=0 && uid!=p->uid) {
		return MFS_ERROR_EPERM;
	}
//	if (opened==0) {
//		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
//			return MFS_ERROR_EACCES;
//		}
//	}
	if (acltype!=POSIX_ACL_ACCESS && acltype!=POSIX_ACL_DEFAULT) {
		return MFS_ERROR_EINVAL;
	}
	pmode = p->mode;
	if ((userperm&groupperm&otherperm&mask)==0xFFFF && (namedusers|namedgroups)==0) { // special case (remove)
		posix_acl_remove(inode,acltype);
		if (acltype==POSIX_ACL_ACCESS) {
			p->aclpermflag = 0;
		} else if (acltype==POSIX_ACL_DEFAULT) {
			p->acldefflag = 0;
		}
	} else {
		if (userperm==0xFFFF) {
			userperm = (p->mode >> 6) & 7;
		}
		if (groupperm==0xFFFF) {
			groupperm = (p->mode >> 3) & 7;
		}
		if (otherperm==0xFFFF) {
			otherperm = p->mode & 7;
		}
		posix_acl_set(inode,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,aclblob);
		if (acltype==POSIX_ACL_ACCESS) {
			p->mode &= 07000;
			p->mode |= ((userperm&7)<<6) | ((groupperm&7)<<3) | (otherperm&7);
		}
		if (p->mode!=pmode) {
			p->ctime = ts;
		}
	}
	changelog("%"PRIu32"|SETACL(%"PRIu32",%u,%u,%"PRIu8",%"PRIu16",%"PRIu16",%"PRIu16",%"PRIu16",%"PRIu16",%"PRIu16",%s)",ts,inode,p->mode,(p->ctime==ts)?1U:0U,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,changelog_escape_name((namedusers+namedgroups)*6,aclblob));
	return MFS_STATUS_OK;
}

uint8_t fs_getfacl_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,/*uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,*/uint8_t acltype,void **custom,uint32_t *aclblobsize) {
	fsnode *p;
	int32_t bsize;

	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
//	if (opened==0) {
//		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
//			return MFS_ERROR_EACCES;
//		}
//	}
	if ((acltype==POSIX_ACL_ACCESS && p->aclpermflag==0) || (acltype==POSIX_ACL_DEFAULT && p->acldefflag==0)) {
		return MFS_ERROR_ENOATTR;
	}
	bsize = posix_acl_get_blobsize(inode,acltype,custom);
	if (bsize<0) {
		return MFS_ERROR_ENOATTR;
	}
	*aclblobsize = bsize;
	return MFS_STATUS_OK;
}

void fs_getfacl_data(void *custom,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *mask,uint16_t *namedusers,uint16_t *namedgroups,uint8_t *aclblob) {
	posix_acl_get_data(custom,userperm,groupperm,otherperm,mask,namedusers,namedgroups,aclblob);
}

uint8_t fs_mr_setacl(uint32_t ts,uint32_t inode,uint16_t mode,uint8_t changectime,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (acltype!=POSIX_ACL_ACCESS && acltype!=POSIX_ACL_DEFAULT) {
		return MFS_ERROR_EINVAL;
	}
	if ((userperm&groupperm&otherperm&mask)==0xFFFF && (namedusers|namedgroups)==0) { // special case (remove)
		posix_acl_remove(inode,acltype);
		if (acltype==POSIX_ACL_ACCESS) {
			p->aclpermflag = 0;
		} else if (acltype==POSIX_ACL_DEFAULT) {
			p->acldefflag = 0;
		}
	} else {
		posix_acl_set(inode,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,aclblob);
		p->mode = mode;
		if (changectime) {
			p->ctime = ts;
		}
	}
	meta_version_inc();
	return MFS_STATUS_OK;
}

uint8_t fs_quotacontrol(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t delflag,uint8_t *flags,uint32_t *graceperiod,uint32_t *sinodes,uint64_t *slength,uint64_t *ssize,uint64_t *srealsize,uint32_t *hinodes,uint64_t *hlength,uint64_t *hsize,uint64_t *hrealsize,uint32_t *curinodes,uint64_t *curlength,uint64_t *cursize,uint64_t *currealsize) {
	fsnode *p;
	quotanode *qn;
	statsrecord *psr;
	uint8_t chg;

	if (*flags) {
		if (sesflags&SESFLAG_READONLY) {
			return MFS_ERROR_EROFS;
		}
		if ((sesflags&SESFLAG_ADMIN)==0) {
			return MFS_ERROR_EPERM;
		}
	}
	if (rootinode==0) {
		return MFS_ERROR_EPERM;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_EPERM;
	}
	qn = p->data.ddata.quota;
	chg = (*flags)?1:0;
	if (delflag) {
		if (qn) {
			qn->flags &= ~(*flags);
			if (qn->flags==0) {
				chg = 1;
				fsnodes_delete_quotanode(p);
				qn=NULL;
			}
		}
	} else {
		if (qn==NULL && (*flags)!=0) {
			qn = fsnodes_new_quotanode(p);
		}
		if (qn) {
			qn->flags |= *flags;
			if (*graceperiod!=0) {
				qn->graceperiod = *graceperiod;
			}
			if ((*flags)&QUOTA_FLAG_SINODES) {
				qn->sinodes = *sinodes;
			}
			if ((*flags)&QUOTA_FLAG_SLENGTH) {
				qn->slength = *slength;
			}
			if ((*flags)&QUOTA_FLAG_SSIZE) {
				qn->ssize = *ssize;
			}
			if ((*flags)&QUOTA_FLAG_SREALSIZE) {
				qn->srealsize = *srealsize;
			}
			if ((*flags)&QUOTA_FLAG_HINODES) {
				qn->hinodes = *hinodes;
			}
			if ((*flags)&QUOTA_FLAG_HLENGTH) {
				qn->hlength = *hlength;
			}
			if ((*flags)&QUOTA_FLAG_HSIZE) {
				qn->hsize = *hsize;
			}
			if ((*flags)&QUOTA_FLAG_HREALSIZE) {
				qn->hrealsize = *hrealsize;
			}
		}
	}
	if (qn) {
		if (((qn->flags)&QUOTA_FLAG_SINODES)==0) {
			qn->sinodes = 0;
		}
		if (((qn->flags)&QUOTA_FLAG_HINODES)==0) {
			qn->hinodes = 0;
		}
		if (((qn->flags)&QUOTA_FLAG_SLENGTH)==0) {
			qn->slength = 0;
		}
		if (((qn->flags)&QUOTA_FLAG_HLENGTH)==0) {
			qn->hlength = 0;
		}
		if (((qn->flags)&QUOTA_FLAG_SSIZE)==0) {
			qn->ssize = 0;
		}
		if (((qn->flags)&QUOTA_FLAG_HSIZE)==0) {
			qn->hsize = 0;
		}
		if (((qn->flags)&QUOTA_FLAG_SREALSIZE)==0) {
			qn->srealsize = 0;
		}
		if (((qn->flags)&QUOTA_FLAG_HREALSIZE)==0) {
			qn->hrealsize = 0;
		}
		*flags = qn->flags;
		*graceperiod = qn->graceperiod;
		*sinodes = qn->sinodes;
		*slength = qn->slength;
		*ssize = qn->ssize;
		*srealsize = qn->srealsize;
		*hinodes = qn->hinodes;
		*hlength = qn->hlength;
		*hsize = qn->hsize;
		*hrealsize = qn->hrealsize;
	} else {
		*flags = 0;
		*graceperiod = 0;
		*sinodes = 0;
		*slength = 0;
		*ssize = 0;
		*srealsize = 0;
		*hinodes = 0;
		*hlength = 0;
		*hsize = 0;
		*hrealsize = 0;
	}
	psr = &(p->data.ddata.stats);
	*curinodes = psr->inodes;
	*curlength = psr->length;
	*cursize = psr->size;
	*currealsize = psr->realsize;
	if (chg) {
		if (qn) {
			changelog("%"PRIu32"|QUOTA(%"PRIu32",%"PRIu8",%"PRIu8",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",main_time(),inode,qn->exceeded,qn->flags,qn->stimestamp,qn->sinodes,qn->hinodes,qn->slength,qn->hlength,qn->ssize,qn->hsize,qn->srealsize,qn->hrealsize,qn->graceperiod);
		} else {
			changelog("%"PRIu32"|QUOTA(%"PRIu32",0,0,0,0,0,0,0,0,0,0,0,0)",main_time(),inode);
		}
	}
	return MFS_STATUS_OK;
}

uint8_t fs_mr_quota(uint32_t ts,uint32_t inode,uint8_t exceeded,uint8_t flags,uint32_t stimestamp,uint32_t sinodes,uint32_t hinodes,uint64_t slength,uint64_t hlength,uint64_t ssize,uint64_t hsize,uint64_t srealsize,uint64_t hrealsize,uint32_t graceperiod) {
	fsnode *p;
	quotanode *qn;

	(void)ts;
	p = fsnodes_node_find(inode);
	if (!p) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY) {
		return MFS_ERROR_EPERM;
	}
	qn = p->data.ddata.quota;
	if (flags==0) {
		if (qn!=NULL) {
			fsnodes_delete_quotanode(p);
		}
	} else {
		if (qn==NULL) {
			qn = fsnodes_new_quotanode(p);
		}
		qn->flags = flags;
		if (graceperiod!=0) {
			qn->graceperiod = graceperiod;
		}
		qn->exceeded = exceeded;
		qn->stimestamp = stimestamp;
		qn->sinodes = sinodes;
		qn->slength = slength;
		qn->ssize = ssize;
		qn->srealsize = srealsize;
		qn->hinodes = hinodes;
		qn->hlength = hlength;
		qn->hsize = hsize;
		qn->hrealsize = hrealsize;
	}
	meta_version_inc();
	return MFS_STATUS_OK;
}


uint32_t fs_getquotainfo(uint8_t *buff) {
	quotanode *qn;
	statsrecord *psr;
	uint32_t size;
	uint32_t ts;
	if (buff!=NULL) {
		ts = main_time();
	} else {
		ts = 0;
	}
	for (qn=quotahead ; qn ; qn=qn->next) {
		size=fsnodes_getpath_size(qn->node->parents);
		if (buff==NULL) {
			ts += 4+4+4+1+1+4+3*(4+8+8+8)+1+size;
		} else {
			psr = &(qn->node->data.ddata.stats);
			put32bit(&buff,qn->node->inode);
			put32bit(&buff,size+1);
			put8bit(&buff,'/');
			fsnodes_getpath_data(qn->node->parents,buff,size);
			buff+=size;
			put32bit(&buff,qn->graceperiod);
			put8bit(&buff,qn->exceeded);
			put8bit(&buff,qn->flags);
			if (qn->stimestamp==0) {					// soft quota not exceeded
				put32bit(&buff,0xFFFFFFFF); 				// time to block = INF
			} else if (qn->stimestamp+qn->graceperiod<ts) {			// soft quota timed out
				put32bit(&buff,0);					// time to block = 0 (blocked)
			} else {							// soft quota exceeded, but not timed out
				put32bit(&buff,qn->stimestamp+qn->graceperiod-ts);
			}
			if (qn->flags&QUOTA_FLAG_SINODES) {
				put32bit(&buff,qn->sinodes);
			} else {
				put32bit(&buff,0);
			}
			if (qn->flags&QUOTA_FLAG_SLENGTH) {
				put64bit(&buff,qn->slength);
			} else {
				put64bit(&buff,0);
			}
			if (qn->flags&QUOTA_FLAG_SSIZE) {
				put64bit(&buff,qn->ssize);
			} else {
				put64bit(&buff,0);
			}
			if (qn->flags&QUOTA_FLAG_SREALSIZE) {
				put64bit(&buff,qn->srealsize);
			} else {
				put64bit(&buff,0);
			}
			if (qn->flags&QUOTA_FLAG_HINODES) {
				put32bit(&buff,qn->hinodes);
			} else {
				put32bit(&buff,0);
			}
			if (qn->flags&QUOTA_FLAG_HLENGTH) {
				put64bit(&buff,qn->hlength);
			} else {
				put64bit(&buff,0);
			}
			if (qn->flags&QUOTA_FLAG_HSIZE) {
				put64bit(&buff,qn->hsize);
			} else {
				put64bit(&buff,0);
			}
			if (qn->flags&QUOTA_FLAG_HREALSIZE) {
				put64bit(&buff,qn->hrealsize);
			} else {
				put64bit(&buff,0);
			}
			put32bit(&buff,psr->inodes);
			put64bit(&buff,psr->length);
			put64bit(&buff,psr->size);
			put64bit(&buff,psr->realsize);
		}
	}
	return ts;
}

uint8_t fs_archget(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint64_t *archchunks,uint64_t *notarchchunks,uint32_t *archinodes,uint32_t *partinodes,uint32_t *notarchinodes) {
	fsnode *p;

	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	fsnodes_keep_alive_begin();
	*archchunks = 0;
	*notarchchunks = 0;
	*archinodes = 0;
	*partinodes = 0;
	*notarchinodes = 0;
	fsnodes_getarch_recursive(p,archchunks,notarchchunks,archinodes,partinodes,notarchinodes);
	return MFS_STATUS_OK;
}

uint8_t fs_univ_archchg(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t cmd,uint64_t *chgchunks,uint64_t *notchgchunks,uint32_t *nsinodes) {
	fsnode *p;

	*chgchunks = 0;
	*notchgchunks = 0;
	*nsinodes = 0;

	if (sesflags&SESFLAG_READONLY) {
		return MFS_ERROR_EROFS;
	}
//	if ((sesflags&SESFLAG_ADMIN)==0) {
//		return MFS_ERROR_EPERM;
//	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	fsnodes_keep_alive_begin();
	fsnodes_chgarch_recursive(p,ts,uid,cmd,chgchunks,notchgchunks,nsinodes);
	if (p->type!=TYPE_DIRECTORY && *nsinodes>0 && *chgchunks==0 && *notchgchunks==0) {
		return MFS_ERROR_EPERM;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|ARCHCHG(%"PRIu32",%"PRIu32",%"PRIu8"):%"PRIu64",%"PRIu64",%"PRIu32,ts,inode,uid,cmd,*chgchunks,*notchgchunks,*nsinodes);
	} else {
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

uint8_t fs_archchg(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t cmd,uint64_t *chgchunks,uint64_t *notchgchunks,uint32_t *nsinodes) {
	return fs_univ_archchg(main_time(),rootinode,sesflags,inode,uid,cmd,chgchunks,notchgchunks,nsinodes);
}

uint8_t fs_mr_archchg(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t cmd,uint64_t chgchunks,uint64_t notchgchunks,uint32_t nsinodes) {
	uint64_t cc,ncc;
	uint32_t nsi;
	uint8_t status;
	status = fs_univ_archchg(ts,0,SESFLAG_METARESTORE,inode,uid,cmd,&cc,&ncc,&nsi);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (cc!=chgchunks || ncc!=notchgchunks || nsi!=nsinodes) {
		syslog(LOG_WARNING,"ARCHCHG data mismatch: my:(%"PRIu64",%"PRIu64",%"PRIu32") != expected:(%"PRIu64",%"PRIu64",%"PRIu32")",cc,ncc,nsi,chgchunks,notchgchunks,nsinodes);
		return MFS_ERROR_MISMATCH;
	}
	return MFS_STATUS_OK;
}

uint32_t fs_getdirpath_size(uint32_t inode) {
	fsnode *node;
	node = fsnodes_node_find(inode);
	if (node) {
		if (node->type!=TYPE_DIRECTORY) {
			return 15; // "(not directory)"
		} else {
			return 1+fsnodes_getpath_size(node->parents);
		}
	} else {
		return 11; // "(not found)"
	}
	return 0;	// unreachable
}

void fs_getdirpath_data(uint32_t inode,uint8_t *buff,uint32_t size) {
	fsnode *node;
	node = fsnodes_node_find(inode);
	if (node) {
		if (node->type!=TYPE_DIRECTORY) {
			if (size>=15) {
				memcpy(buff,"(not directory)",15);
				return;
			}
		} else {
			if (size>0) {
				buff[0]='/';
				fsnodes_getpath_data(node->parents,buff+1,size-1);
				return;
			}
		}
	} else {
		if (size>=11) {
			memcpy(buff,"(not found)",11);
			return;
		}
	}
}

uint8_t fs_get_dir_stats(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *chunks,uint64_t *length,uint64_t *size,uint64_t *rsize) {
	fsnode *p;
	statsrecord sr;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return MFS_ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return MFS_ERROR_EPERM;
	}
	fsnodes_get_stats(p,&sr,2);
	*inodes = sr.inodes;
	*dirs = sr.dirs;
	*files = sr.files;
	*chunks = sr.chunks;
	*length = sr.length;
	*size = sr.size;
	*rsize = sr.realsize;
//	syslog(LOG_NOTICE,"using fast stats");
	return MFS_STATUS_OK;
}

uint32_t fs_node_info(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t maxentries,uint64_t continueid,uint8_t *ptr) {
	uint32_t i;
	uint64_t chunkid;
	uint32_t lastchunk,lastchunksize;
	uint8_t copies;
	uint8_t *nextcidptr;
	uint64_t ncontid;
	fsnode *p;
	fsedge *e;
	uint32_t ret = 0;

	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		ret = 1;
		if (ptr!=NULL) {
			put8bit(&ptr,MFS_ERROR_ENOENT);
		}
		return ret;
	}
	if (p->type==TYPE_DIRECTORY) {
		ret = 10;
		if (ptr!=NULL) {
			put16bit(&ptr,1);
			nextcidptr = ptr;
			ptr += 8;
		} else {
			nextcidptr = NULL;
		}
		ncontid = continueid;
		if (continueid==0) {
			e = p->data.ddata.children;
		} else {
			e = fsnodes_edgeid_find(continueid);
		}
		if (e==NULL) {
			e = p->data.ddata.children;
			while (e && e->edgeid < continueid) {
				e = e->nextchild;
			}
			if (e) {
				ncontid = e->edgeid;
			} else {
				ncontid = 0;
			}
		}
		while (e && maxentries > 0) {
			if (ptr!=NULL) {
				put32bit(&ptr,e->child->inode);
			}
			ret += 4;
			maxentries--;
			e = e->nextchild;
			if (e) {
				ncontid = e->edgeid;
			} else {
				ncontid = 0;
			}
		}
		if (e!=NULL) {
			fsnodes_edgeid_insert(e);
		}
		if (nextcidptr!=NULL) {
			put64bit(&nextcidptr,ncontid);
		}
	} else if (p->type==TYPE_FILE || p->type==TYPE_TRASH || p->type==TYPE_SUSTAINED) {
		ret = 18;
		if (ptr!=NULL) {
			put16bit(&ptr,2);
			nextcidptr = ptr;
			ptr += 8;
			put64bit(&ptr,p->data.fdata.length);
		} else {
			nextcidptr = NULL;
		}
		if (p->data.fdata.length>0) {
			lastchunk = (p->data.fdata.length-1)>>MFSCHUNKBITS;
			lastchunksize = ((((p->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastchunk = 0;
			lastchunksize = MFSHDRSIZE;
		}
		ncontid = continueid;
		for (i = continueid ; i < p->data.fdata.chunks && maxentries > 0 ; i++) {
			chunkid = p->data.fdata.chunktab[i];
			if (chunkid>0 && chunk_get_copies(chunkid,&copies)==MFS_STATUS_OK) {
				if (ptr!=NULL) {
					put64bit(&ptr,chunkid);
					if (i<lastchunk) {
						put32bit(&ptr,MFSCHUNKSIZE+MFSHDRSIZE);
					} else if (i==lastchunk) {
						put32bit(&ptr,lastchunksize);
					} else {
						put32bit(&ptr,0);
					}
					put8bit(&ptr,copies);
				}
				ret += 13;
				ncontid = i+1;
				maxentries--;
			}
		}
		if (ncontid>=p->data.fdata.chunks) {
			ncontid = 0;
		}
		if (nextcidptr!=NULL) {
			put64bit(&nextcidptr,ncontid);
		}
	} else {
		ret = 2;
		if (ptr!=NULL) {
			put16bit(&ptr,0);
		}
	}
	return ret;
}

static inline void fs_add_file_to_chunks(fsnode *f) {
	uint32_t i;
	uint64_t chunkid;
	if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_SUSTAINED) {
		for (i=0 ; i<f->data.fdata.chunks ; i++) {
			chunkid = f->data.fdata.chunktab[i];
			if (chunkid>0) {
				chunk_add_file(chunkid,f->sclassid);
			}
		}
	}
}

void fs_add_files_to_chunks() {
	uint32_t i;
	fsnode *p;
	for (i=0 ; i<noderehashpos ; i++) {
		for (p=nodehashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; p ; p=p->next) {
			fs_add_file_to_chunks(p);
		}
	}
}

uint8_t fs_mr_set_file_chunk(uint32_t inode,uint32_t indx,uint64_t chunkid) {
	fsnode *node;
	uint64_t oldchunkid;
	node = fsnodes_node_find(inode);
	if (node==NULL) {
		return MFS_ERROR_ENOENT;
	}
	if (indx>=node->data.fdata.chunks) {
		syslog(LOG_WARNING,"set_file_chunk index too big: indx:%"PRIu32" ; chunks:%"PRIu32,indx,node->data.fdata.chunks);
		return MFS_ERROR_MISMATCH;
	}
	oldchunkid = node->data.fdata.chunktab[indx];
	if (oldchunkid>0) {
		chunk_delete_file(oldchunkid,node->sclassid);
	}
	node->data.fdata.chunktab[indx] = chunkid;
	if (chunkid>0) {
		chunk_add_file(chunkid,node->sclassid);
	}
	meta_version_inc();
	return MFS_STATUS_OK;
}

void fs_test_getdata(uint32_t *loopstart,uint32_t *loopend,uint32_t *files,uint32_t *ugfiles,uint32_t *mfiles,uint32_t *mtfiles,uint32_t *msfiles,uint32_t *chunks,uint32_t *ugchunks,uint32_t *mchunks,char **msgbuff,uint32_t *msgbuffleng) {
	*loopstart = fsinfo_loopstart;
	*loopend = fsinfo_loopend;
	*files = fsinfo_files;
	*ugfiles = fsinfo_ugfiles;
	*mfiles = fsinfo_mfiles;
	*mtfiles = fsinfo_mtfiles;
	*msfiles = fsinfo_msfiles;
	*chunks = fsinfo_chunks;
	*ugchunks = fsinfo_ugchunks;
	*mchunks = fsinfo_mchunks;
	*msgbuff = fsinfo_msgbuff;
	*msgbuffleng = fsinfo_msgbuffleng;
}

uint32_t fs_test_log_inconsistency(fsedge *e,const char *iname,char *buff,uint32_t size) {
	uint32_t leng;
	leng=0;
	if (e->parent) {
		syslog(LOG_ERR,"structure error - %s inconsistency (edge: %"PRIu32",%s -> %"PRIu32")",iname,e->parent->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
		if (leng<size) {
			leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: %"PRIu32",%s -> %"PRIu32")\n",iname,e->parent->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
		}
	} else {
		if (e->child->type==TYPE_TRASH) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: TRASH,%s -> %"PRIu32")",iname,changelog_escape_name(e->nleng,e->name),e->child->inode);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: TRASH,%s -> %"PRIu32")\n",iname,changelog_escape_name(e->nleng,e->name),e->child->inode);
			}
		} else if (e->child->type==TYPE_SUSTAINED) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: SUSTAINED,%s -> %"PRIu32")",iname,changelog_escape_name(e->nleng,e->name),e->child->inode);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: SUSTAINED,%s -> %"PRIu32")\n",iname,changelog_escape_name(e->nleng,e->name),e->child->inode);
			}
		} else {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: NULL,%s -> %"PRIu32")",iname,changelog_escape_name(e->nleng,e->name),e->child->inode);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: NULL,%s -> %"PRIu32")\n",iname,changelog_escape_name(e->nleng,e->name),e->child->inode);
			}
		}
	}
	return leng;
}

void fs_test_files() {
	static uint32_t i=0;
	uint32_t j;
	uint32_t k;
	uint32_t lengchunks;
	uint64_t chunkid;
	uint8_t valid,ugflag,aflag;
	uint32_t aflagchanged,allchunks;
	uint16_t arch_delay;
	uint32_t arch_delay_sec;
	static uint32_t files=0;
	static uint32_t ugfiles=0;
	static uint32_t mfiles=0;
	static uint32_t mtfiles=0;
	static uint32_t msfiles=0;
	static uint32_t chunks=0;
	static uint32_t ugchunks=0;
	static uint32_t mchunks=0;
	static uint32_t notfoundchunks=0;
	static char *msgbuff=NULL,*tmp;
	static uint32_t leng=0;
	uint32_t now;
	fsnode *f;
	fsedge *e;

	now = main_time();

	if (now<=test_start_time) {
		return;
	}
	if (i==0) {
		if (notfoundchunks>0) {
			syslog(LOG_ERR,"unknown chunks: %"PRIu32,notfoundchunks);
			if (leng<MSGBUFFSIZE) {
				leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"unknown chunks: %"PRIu32"\n",notfoundchunks);
			}
			notfoundchunks=0;
		}
		fsinfo_files=files;
		fsinfo_ugfiles=ugfiles;
		fsinfo_mfiles=mfiles;
		fsinfo_mtfiles=mtfiles;
		fsinfo_msfiles=msfiles;
		fsinfo_chunks=chunks;
		fsinfo_ugchunks=ugchunks;
		fsinfo_mchunks=mchunks;
		files=0;
		ugfiles=0;
		mfiles=0;
		mtfiles=0;
		msfiles=0;
		chunks=0;
		ugchunks=0;
		mchunks=0;

		missing_log_swap();

		if (fsinfo_msgbuff==NULL) {
			fsinfo_msgbuff=malloc(MSGBUFFSIZE);
			passert(fsinfo_msgbuff);
		}
		tmp = fsinfo_msgbuff;
		fsinfo_msgbuff=msgbuff;
		msgbuff = tmp;
		if (leng>MSGBUFFSIZE) {
			fsinfo_msgbuffleng=MSGBUFFSIZE;
		} else {
			fsinfo_msgbuffleng=leng;
		}
		leng=0;

		fsinfo_loopstart = fsinfo_loopend;
		fsinfo_loopend = now;
	}
	for (k=0 ; k<(nodehashsize/32768) && i<noderehashpos ; k++,i++) {
		for (f=nodehashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; f ; f=f->next) {
			if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_SUSTAINED) {
				valid = 1;
				ugflag = 0;
				aflagchanged = 0;
				allchunks = 0;
				arch_delay = sclass_get_arch_delay(f->sclassid);
				if (arch_delay==0U || arch_delay>49710U) {
					aflag = 0;
				} else {
					arch_delay_sec = ((uint32_t)arch_delay) * 86400U;
					arch_delay_sec += f->ctime;
					aflag = (arch_delay_sec < now && arch_delay_sec >= f->ctime)?1:0; // arch_delay_sec < f->mtime means overflow
				}
				lengchunks = ((f->data.fdata.length+MFSCHUNKMASK)>>MFSCHUNKBITS);
				for (j=0 ; j<f->data.fdata.chunks ; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid>0) {
						allchunks++;
						switch (chunk_fileloop_task(chunkid,f->sclassid,(j>=lengchunks)?1:0,aflag)) {
							case CHUNK_FLOOP_NOTFOUND:
								syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,f->inode,j);
								if (leng<MSGBUFFSIZE) {
									leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")\n",chunkid,f->inode,j);
								}
								notfoundchunks++;
								if ((notfoundchunks%1000)==0) {
									syslog(LOG_ERR,"unknown chunks: %"PRIu32" ...",notfoundchunks);
								}
								valid =0;
								mchunks++;
								break;
							case CHUNK_FLOOP_DELETED:
								f->data.fdata.chunktab[j] = 0;
								changelog("%"PRIu32"|SETFILECHUNK(%"PRIu32",%"PRIu32",0)",main_time(),f->inode,j);
								syslog(LOG_NOTICE,"inode: %"PRIu32" ; index: %"PRIu32" - removed not existing chunk exceeding file size (chunkid: %016"PRIX64")",f->inode,j,chunkid);
								break;
							case CHUNK_FLOOP_MISSING_NOCOPY:
								missing_log_insert(chunkid,f->inode,j,0);
								valid = 0;
								mchunks++;
								break;
							case CHUNK_FLOOP_MISSING_INVALID:
								missing_log_insert(chunkid,f->inode,j,1);
								valid = 0;
								mchunks++;
								break;
							case CHUNK_FLOOP_MISSING_WRONGVERSION:
								missing_log_insert(chunkid,f->inode,j,2);
								valid = 0;
								mchunks++;
								break;
							case CHUNK_FLOOP_UNDERGOAL_AFLAG_CHANGED:
								aflagchanged++;
								// no break - intentionally
								nobreak;
							case CHUNK_FLOOP_UNDERGOAL_AFLAG_NOT_CHANGED:
								ugflag = 1;
								ugchunks++;
								break;
							case CHUNK_FLOOP_OK_AFLAG_CHANGED:
								aflagchanged++;
								// no break - intentionally
								nobreak;
							case CHUNK_FLOOP_OK_AFLAG_NOT_CHANGED:
								break;
						}
					}
				}
				if (aflagchanged) {
					changelog("%"PRIu32"|ARCHCHG(%"PRIu32",0,%u):%"PRIu32",%"PRIu32",0",main_time(),f->inode,ARCHCTL_SET,aflagchanged,allchunks-aflagchanged);
				}
				if (valid==0) {
					if (f->type==TYPE_TRASH) {
						mtfiles++;
					} else if (f->type==TYPE_SUSTAINED) {
						msfiles++;
					} else {
						for (e=f->parents ; e ; e=e->nextparent) {
							mfiles++;
						}
					}
				} else if (ugflag) {
					ugfiles++;
				}
				files++;
				chunks += allchunks;
				fsnodes_check_realsize(f);
			}
			for (e=f->parents ; e ; e=e->nextparent) {
				if (e->child != f) {
					if (e->parent) {
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")",f->inode,e->parent->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")\n",f->inode,e->parent->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
						}
					} else {
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")",f->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")\n",f->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
						}
					}
				} else if (e->nextchild) {
					if (e->nextchild->prevchild != &(e->nextchild)) {
						if (leng<MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(e,"nextchild/prevchild",msgbuff+leng,MSGBUFFSIZE-leng);
						} else {
							fs_test_log_inconsistency(e,"nextchild/prevchild",NULL,0);
						}
					}
				} else if (e->nextparent) {
					if (e->nextparent->prevparent != &(e->nextparent)) {
						if (leng<MSGBUFFSIZE) {
							leng += fs_test_log_inconsistency(e,"nextparent/prevparent",msgbuff+leng,MSGBUFFSIZE-leng);
						} else {
							fs_test_log_inconsistency(e,"nextparent/prevparent",NULL,0);
						}
					}
				}
			}
			if (f->type == TYPE_DIRECTORY) {
				for (e=f->data.ddata.children ; e ; e=e->nextchild) {
					if (e->parent != f) {
						if (e->parent) {
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")",f->inode,e->parent->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")\n",f->inode,e->parent->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
							}
						} else {
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")",f->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")\n",f->inode,changelog_escape_name(e->nleng,e->name),e->child->inode);
							}
						}
					} else if (e->nextchild) {
						if (e->nextchild->prevchild != &(e->nextchild)) {
							if (leng<MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(e,"nextchild/prevchild",msgbuff+leng,MSGBUFFSIZE-leng);
							} else {
								fs_test_log_inconsistency(e,"nextchild/prevchild",NULL,0);
							}
						}
					} else if (e->nextparent) {
						if (e->nextparent->prevparent != &(e->nextparent)) {
							if (leng<MSGBUFFSIZE) {
								leng += fs_test_log_inconsistency(e,"nextparent/prevparent",msgbuff+leng,MSGBUFFSIZE-leng);
							} else {
								fs_test_log_inconsistency(e,"nextparent/prevparent",NULL,0);
							}
						}
					}
				}
			}
		}
	}
	if (i>=noderehashpos) {
		syslog(LOG_NOTICE,"structure check loop");
		i=0;
	}
}

static inline uint32_t fs_univ_empty_trash_part(uint32_t ts,uint32_t bid,uint32_t *fi,uint32_t *si) {
	fsedge *e;
	fsnode *p;
	uint64_t trashseconds;
	uint32_t ics;

	ics = 0;
	e = trash[bid];
	while (e) {
		p = e->child;
		e = e->nextchild;
		trashseconds = p->trashtime;
		trashseconds *= 3600;
		if (((uint64_t)(p->atime) + trashseconds < (uint64_t)ts) && ((uint64_t)(p->mtime) + trashseconds < (uint64_t)ts) && ((uint64_t)(p->ctime) + trashseconds < (uint64_t)ts)) {
			ics ^= p->inode;
			if (fsnodes_purge(ts,p)) {
				(*fi)++;
			} else {
				(*si)++;
			}
		}
	}
	return ics;
}

uint8_t fs_univ_emptytrash(uint32_t ts,uint8_t sesflags,uint32_t bid,uint32_t freeinodes,uint32_t sustainedinodes,uint32_t inode_chksum) {
	uint32_t fi,si,ics;

	fi=0;
	si=0;
	ics = 0;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		bid = trash_bid;
		trash_bid++;
		if (trash_bid >= TRASH_BUCKETS) {
			trash_bid = 0;
		}
	}
	if (bid>=TRASH_BUCKETS) {
		for (bid = 0 ; bid<TRASH_BUCKETS ; bid++) {
			ics ^= fs_univ_empty_trash_part(ts,bid,&fi,&si);
		}
	} else {
		ics ^= fs_univ_empty_trash_part(ts,bid,&fi,&si);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if ((fi|si)>0) {
			changelog("%"PRIu32"|EMPTYTRASH(%"PRIu32"):%"PRIu32",%"PRIu32",%"PRIu32,ts,bid,fi,si,ics);
		}
	} else {
		if (freeinodes!=fi || sustainedinodes!=si || (inode_chksum!=0 && ics!=inode_chksum)) {
			syslog(LOG_WARNING,"EMPTYTRASH data mismatch: my:(%"PRIu32",%"PRIu32",%"PRIu32") != expected:(%"PRIu32",%"PRIu32",%"PRIu32")",fi,si,ics,freeinodes,sustainedinodes,inode_chksum);
			return MFS_ERROR_MISMATCH;
		}
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

void fs_emptytrash(void) {
	(void)fs_univ_emptytrash(main_time(),0,0,0,0,0);
}

uint8_t fs_mr_emptytrash(uint32_t ts,uint32_t bid,uint32_t freeinodes,uint32_t sustainedinodes,uint32_t inode_chksum) {
	return fs_univ_emptytrash(ts,SESFLAG_METARESTORE,bid,freeinodes,sustainedinodes,inode_chksum);
}

static inline uint32_t fs_univ_empty_sustained_part(uint32_t ts,uint32_t bid,uint32_t *fi) {
	fsedge *e;
	fsnode *p;
	uint32_t ics;

	ics = 0;
	e = sustained[bid];
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (of_isfileopen(p->inode)==0) {
			ics ^= p->inode;
			fsnodes_purge(ts,p);
			(*fi)++;
		}
	}
	return ics;
}

uint8_t fs_univ_emptysustained(uint32_t ts,uint8_t sesflags,uint32_t bid,uint32_t freeinodes,uint32_t inode_chksum) {
	uint32_t fi,ics;

	fi=0;
	ics = 0;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		bid = sustained_bid;
		sustained_bid++;
		if (sustained_bid >= SUSTAINED_BUCKETS) {
			sustained_bid = 0;
		}
	}
	if (bid>=SUSTAINED_BUCKETS) {
		for (bid = 0 ; bid<SUSTAINED_BUCKETS ; bid++) {
			ics ^= fs_univ_empty_sustained_part(ts,bid,&fi);
		}
	} else {
		ics ^= fs_univ_empty_sustained_part(ts,bid,&fi);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (fi>0) {
			changelog("%"PRIu32"|EMPTYSUSTAINED(%"PRIu32"):%"PRIu32",%"PRIu32,ts,bid,fi,ics);
		}
	} else {
		if (freeinodes!=fi || (inode_chksum!=0 && inode_chksum!=ics)) {
			syslog(LOG_WARNING,"EMPTYSUSTAINED data mismatch: my:(%"PRIu32",%"PRIu32") != expected:(%"PRIu32",%"PRIu32")",fi,ics,freeinodes,inode_chksum);
			return MFS_ERROR_MISMATCH;
		}
		meta_version_inc();
	}
	return MFS_STATUS_OK;
}

void fs_emptysustained(void) {
	(void)fs_univ_emptysustained(main_time(),0,0,0,0);
}

uint8_t fs_mr_emptysustained(uint32_t ts,uint32_t bid,uint32_t freeinodes,uint32_t inode_chksum) {
	return fs_univ_emptysustained(ts,SESFLAG_METARESTORE,bid,freeinodes,inode_chksum);
}

static inline void fs_renumerate_edges(fsnode *p) {
	fsedge *e;
	uint64_t fedgeid;
	fedgeid = nextedgeid;
	for (e=p->data.ddata.children ; e ; e=e->nextchild) {
		fedgeid--;
	}
	nextedgeid = fedgeid;
	for (e=p->data.ddata.children ; e ; e=e->nextchild) {
		e->edgeid = fedgeid++;
	}
	for (e=p->data.ddata.children ; e ; e=e->nextchild) {
		fsnodes_keep_alive_check();
		if (e->child->type==TYPE_DIRECTORY) {
			fs_renumerate_edges(e->child);
		}
	}
}

uint8_t fs_mr_renumerate_edges(uint64_t expected_nextedgeid) {
	nextedgeid = EDGEID_MAX;
	nextedgeid--;
	fsnodes_keep_alive_begin();
	fs_renumerate_edges(root);
	edgesneedrenumeration = 0;
	if (nextedgeid!=expected_nextedgeid) {
		syslog(LOG_WARNING,"RENUMERATEEDGES data mismatch: my:%"PRIu64" != expected:%"PRIu64,nextedgeid,expected_nextedgeid);
		return MFS_ERROR_MISMATCH;
	}
	meta_version_inc();
	return MFS_STATUS_OK;
}

void fs_renumerate_edge_test(void) {
	if (nextedgeid==EDGEID_MAX || edgesneedrenumeration) {
		nextedgeid = EDGEID_MAX;
		nextedgeid--;
		fsnodes_keep_alive_begin();
		fs_renumerate_edges(root);
		edgesneedrenumeration = 0;
		changelog("%"PRIu32"|RENUMERATEEDGES():%"PRIu64,main_time(),nextedgeid);
	}
}

void fs_cleanupedges(void) {
	uint32_t bid;
	fsedge_cleanup();
	fsnodes_edge_hash_cleanup();
	for (bid=0 ; bid<TRASH_BUCKETS ; bid++) {
		trash[bid] = NULL;
	}
	for (bid=0 ; bid<SUSTAINED_BUCKETS ; bid++) {
		sustained[bid] = NULL;
	}
}

void fs_cleanupnodes(void) {
	fsnode_cleanup();
	chunktab_cleanup();
	symlink_cleanup();
	fsnodes_node_hash_cleanup();
	root = NULL;
}

void fs_cleanupfreenodes(void) {
	free(freebitmask);
	freebitmask = NULL;
	bitmasksize = 0;
	searchpos = 0;
	freenode_free_all();
	freelist = NULL;
	freetail = &freelist;
	freelastts = 0;
}

void fs_cleanup(void) {
	fprintf(stderr,"cleaning objects ...");
	fflush(stderr);
	fs_cleanupnodes();
	fprintf(stderr," done\n");
	fprintf(stderr,"cleaning names ...");
	fflush(stderr);
	fs_cleanupedges();
	fprintf(stderr," done\n");
	fprintf(stderr,"cleaning deletion timestamps ...");
	fflush(stderr);
	fs_cleanupfreenodes();
	fprintf(stderr," done\n");
	fprintf(stderr,"cleaning quota definitions ...");
	fflush(stderr);
	quotanode_free_all();
//	fs_cleanupquota();
	fprintf(stderr," done\n");
	quotahead = NULL;
	trashspace = 0;
	sustainedspace = 0;
	trashnodes = 0;
	sustainednodes = 0;
	nodes=0;
	dirnodes=0;
	filenodes=0;
	maxnodeid=0;
	hashelements=0;
}

static inline void fs_storeedge(fsedge *e,bio *fd) {
	uint8_t uedgebuff[4+4+8+2+65535];
	uint8_t *ptr;
	if (e==NULL) {	// last edge
		memset(uedgebuff,0,4+4+8+2);
		if (bio_write(fd,uedgebuff,4+4+8+2)!=(4+4+8+2)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}
		return;
	}
	ptr = uedgebuff;
	if (e->parent==NULL) {
		put32bit(&ptr,0);
	} else {
		put32bit(&ptr,e->parent->inode);
	}
	put32bit(&ptr,e->child->inode);
	put64bit(&ptr,e->edgeid);
	put16bit(&ptr,e->nleng);
	memcpy(ptr,e->name,e->nleng);
	if (bio_write(fd,uedgebuff,4+4+8+2+e->nleng)!=(4+4+8+2+e->nleng)) {
		syslog(LOG_NOTICE,"write error");
		return;
	}
}

static inline int fs_loadedge(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t uedgebuff[4+4+8+2];
	const uint8_t *ptr;
	uint32_t parent_id;
	uint32_t child_id;
	uint64_t edgeid;
	uint16_t nleng;
	uint32_t bid;
//	uint32_t hpos;
	fsedge *e;
	statsrecord sr;
	static fsedge **root_tail;
	static uint64_t root_edgeid;
	static fsedge **current_tail;
	static uint64_t current_edgeid;
	static fsnode *current_parent;
	static uint32_t current_parent_id;
	static uint8_t nl;
	static ssize_t bsize;

	if (fd==NULL) {
		current_parent_id = 0;
		current_parent = NULL;
		current_tail = NULL;
		root_tail = NULL;
		root_edgeid = 0;
		current_edgeid = 0;
		nl = 1;
		if (mver<=0x10) {
			bsize = 4+4+2;
		} else {
			bsize = 4+4+8+2;
		}
		return 0;
	}

	if (bio_read(fd,uedgebuff,bsize)!=bsize) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading edge: read error");
		return -1;
	}
	ptr = uedgebuff;
	parent_id = get32bit(&ptr);
	child_id = get32bit(&ptr);
	if (parent_id==0 && child_id==0) {	// last edge
		return 1;
	}
	if (mver>0x10) {
		edgeid = get64bit(&ptr);
	} else {
		edgeid = 0;
	}
	nleng = get16bit(&ptr);
	if (nleng==0) {
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading edge: %"PRIu32"->%"PRIu32" error: empty name",parent_id,child_id);
		return -1;
	}
	if (parent_id==0 && nleng>MFS_PATH_MAX) {
		mfs_arg_syslog(LOG_WARNING,"loading edge: %"PRIu32"->%"PRIu32" error: name too long (%"PRIu16") -> truncate",parent_id,child_id,nleng);
		e = fsedge_malloc(MFS_PATH_MAX);
		passert(e);
		bio_skip(fd,nleng-MFS_PATH_MAX);
		e->nleng = MFS_PATH_MAX;
	} else if (parent_id>0 && nleng>MFS_NAME_MAX) {
		mfs_arg_syslog(LOG_WARNING,"loading edge: %"PRIu32"->%"PRIu32" error: name too long (%"PRIu16") -> truncate",parent_id,child_id,nleng);
		e = fsedge_malloc(MFS_NAME_MAX);
		passert(e);
		bio_skip(fd,nleng-MFS_NAME_MAX);
		e->nleng = MFS_NAME_MAX;
	} else {
		e = fsedge_malloc(nleng);
		passert(e);
		e->nleng = nleng;
	}
	if (bio_read(fd,(uint8_t*)(e->name),e->nleng)!=e->nleng) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading edge: read error");
		fsedge_free(e,nleng);
		return -1;
	}
	e->child = fsnodes_node_find(child_id);
	if (e->child==NULL) {
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" error: child not found",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
		fsedge_free(e,nleng);
		if (ignoreflag) {
			return 0;
		}
		return -1;
	}
	if (parent_id==0) {
		if (e->child->type==TYPE_TRASH) {
			bid = child_id % TRASH_BUCKETS;
			e->parent = NULL;
			e->nextchild = trash[bid];
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			trash[bid] = e;
			e->prevchild = trash + bid;
			trashspace += e->child->data.fdata.length;
			trashnodes++;
		} else if (e->child->type==TYPE_SUSTAINED) {
			bid = child_id % SUSTAINED_BUCKETS;
			e->parent = NULL;
			e->nextchild = sustained[bid];
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			sustained[bid] = e;
			e->prevchild = sustained + bid;
			sustainedspace += e->child->data.fdata.length;
			sustainednodes++;
		} else {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" error: bad child type (%u)\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id,e->child->type);
			syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" error: bad child type (%u)",parent_id,changelog_escape_name(e->nleng,e->name),child_id,e->child->type);
			fsedge_free(e,nleng);
			return -1;
		}
	} else {
		if (current_parent_id==parent_id) {
			e->parent = current_parent;
		} else {
			e->parent = fsnodes_node_find(parent_id);
		}
		if (e->parent==NULL) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" error: parent not found\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
			syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" error: parent not found",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
			if (ignoreflag) {
				e->parent = fsnodes_node_find(MFS_ROOT_ID);
				if (e->parent==NULL || e->parent->type!=TYPE_DIRECTORY) {
					fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" root dir not found !!!\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
					syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" root dir not found !!!",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
					fsedge_free(e,nleng);
					return -1;
				}
				fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" attaching node to root dir\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
				syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" attaching node to root dir",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
				parent_id = MFS_ROOT_ID;
			} else {
				fprintf(stderr,"use option '-i' to attach this node to root dir\n");
				fsedge_free(e,nleng);
				return -1;
			}
		}
		if (e->parent->type!=TYPE_DIRECTORY) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" error: bad parent type (%u)\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id,e->parent->type);
			syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" error: bad parent type (%u)",parent_id,changelog_escape_name(e->nleng,e->name),child_id,e->parent->type);
			if (ignoreflag) {
				e->parent = fsnodes_node_find(MFS_ROOT_ID);
				if (e->parent==NULL || e->parent->type!=TYPE_DIRECTORY) {
					fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" root dir not found !!!\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
					syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" root dir not found !!!",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
					fsedge_free(e,nleng);
					return -1;
				}
				fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" attaching node to root dir\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
				syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" attaching node to root dir",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
				parent_id = MFS_ROOT_ID;
			} else {
				fprintf(stderr,"use option '-i' to attach this node to root dir\n");
				fsedge_free(e,nleng);
				return -1;
			}
		}
		if (parent_id==MFS_ROOT_ID) {	// special case - because of 'ignoreflag' and possibility of attaching orphans into root node
			if (root_tail==NULL) {
				root_tail = &(e->parent->data.ddata.children);
				while (*root_tail) {
					root_edgeid = (*root_tail)->edgeid;
					root_tail = &((*root_tail)->nextchild);
				}
			}
		} else if (current_parent_id!=parent_id) {
			if (e->parent->data.ddata.children) {
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				fprintf(stderr,"loading edge: %"PRIu32",%s->%"PRIu32" error: parent node sequence error\n",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
				syslog(LOG_ERR,"loading edge: %"PRIu32",%s->%"PRIu32" error: parent node sequence error",parent_id,changelog_escape_name(e->nleng,e->name),child_id);
				if (ignoreflag) {
					current_tail = &(e->parent->data.ddata.children);
					while (*current_tail) {
						current_edgeid = (*current_tail)->edgeid;
						current_tail = &((*current_tail)->nextchild);
					}
				} else {
					fsedge_free(e,nleng);
					return -1;
				}
			} else {
				current_tail = &(e->parent->data.ddata.children);
				current_edgeid = 0;
			}
			current_parent_id = parent_id;
			current_parent = e->parent;
		}
		e->nextchild = NULL;
		if (parent_id==MFS_ROOT_ID) {
			*(root_tail) = e;
			e->prevchild = root_tail;
			root_tail = &(e->nextchild);
			if (edgeid <= root_edgeid) {
				if (edgesneedrenumeration==0) {
					syslog(LOG_WARNING,"edgeid mismatch detected - force edgeid renumeration");
					edgesneedrenumeration = 1;
				}
			}
			root_edgeid = edgeid;
		} else {
			*(current_tail) = e;
			e->prevchild = current_tail;
			current_tail = &(e->nextchild);
			if (edgeid <= current_edgeid) {
				if (edgesneedrenumeration==0) {
					syslog(LOG_WARNING,"edgeid mismatch detected - force edgeid renumeration");
					edgesneedrenumeration = 1;
				}
			}
			current_edgeid = edgeid;
		}
		e->parent->data.ddata.elements++;
		switch (e->child->type) {
			case TYPE_FILE:
			case TYPE_TRASH:
			case TYPE_SUSTAINED:
				// TRASH and SUSTAINED here are only pro forma and to avoid potential future bugs
				e->child->data.fdata.nlink++;
				break;
			case TYPE_DIRECTORY:
				// directories doesn't have hard links - nlink here is calculated differently
				e->parent->data.ddata.nlink++;
				break;
			case TYPE_SYMLINK:
				e->child->data.sdata.nlink++;
				break;
			case TYPE_BLOCKDEV:
			case TYPE_CHARDEV:
				e->child->data.devdata.nlink++;
				break;
			default:
				e->child->data.odata.nlink++;
				break;
		}
		fsnodes_edge_add(e);
	}
	e->nextparent = e->child->parents;
	if (e->nextparent) {
		e->nextparent->prevparent = &(e->nextparent);
	}
	e->child->parents = e;
	e->prevparent = &(e->child->parents);
	if (e->parent) {
		fsnodes_get_stats(e->child,&sr,1);
		fsnodes_add_stats(e->parent,&sr);
	}
	e->edgeid = edgeid;
	return 0;
}

static inline void fs_storenode(fsnode *f,bio *fd) {
	uint8_t unodebuff[1+4+1+1+1+2+4+4+4+4+4+2+8+4+2+8*65536+4*65536+4];
	uint8_t *ptr,*chptr;
	uint32_t i,indx,ch;

	if (f==NULL) {	// last node
		if (bio_write(fd,"\0",1)!=1) {
			syslog(LOG_NOTICE,"write error");
		}
		return;
	}
	ptr = unodebuff;
	put8bit(&ptr,f->type);
	put32bit(&ptr,f->inode);
	put8bit(&ptr,f->sclassid);
	put8bit(&ptr,f->eattr);
	put8bit(&ptr,f->winattr);
	put16bit(&ptr,f->mode);
	put32bit(&ptr,f->uid);
	put32bit(&ptr,f->gid);
	put32bit(&ptr,f->atime);
	put32bit(&ptr,f->mtime);
	put32bit(&ptr,f->ctime);
	put16bit(&ptr,f->trashtime);
	switch (f->type) {
	case TYPE_DIRECTORY:
	case TYPE_SOCKET:
	case TYPE_FIFO:
		if (bio_write(fd,unodebuff,1+4+1+1+1+2+4+4+4+4+4+2)!=(1+4+1+1+1+2+4+4+4+4+4+2)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr,f->data.devdata.rdev);
		if (bio_write(fd,unodebuff,1+4+1+1+1+2+4+4+4+4+4+2+4)!=(1+4+1+1+1+2+4+4+4+4+4+2+4)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}
		break;
	case TYPE_SYMLINK:
		put32bit(&ptr,f->data.sdata.pleng);
		if (bio_write(fd,unodebuff,1+4+1+1+1+2+4+4+4+4+4+2+4)!=(1+4+1+1+1+2+4+4+4+4+4+2+4)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}
		if (bio_write(fd,f->data.sdata.path,f->data.sdata.pleng)!=(f->data.sdata.pleng)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
		put64bit(&ptr,f->data.fdata.length);
		ch = 0;
		for (indx=0 ; indx<f->data.fdata.chunks ; indx++) {
			if (f->data.fdata.chunktab[indx]!=0) {
				ch=indx+1;
			}
		}
		put32bit(&ptr,ch);

		if (bio_write(fd,unodebuff,1+4+1+1+1+2+4+4+4+4+4+2+8+4)!=(1+4+1+1+1+2+4+4+4+4+4+2+8+4)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}

		indx = 0;
		while (ch>65536) {
			chptr = ptr;
			for (i=0 ; i<65536 ; i++) {
				put64bit(&chptr,f->data.fdata.chunktab[indx]);
				indx++;
			}
			if (bio_write(fd,ptr,8*65536)!=(8*65536)) {
				syslog(LOG_NOTICE,"write error");
				return;
			}
			ch-=65536;
		}

		chptr = ptr;
		for (i=0 ; i<ch ; i++) {
			put64bit(&chptr,f->data.fdata.chunktab[indx]);
			indx++;
		}
		if (ch>0) {
			if (bio_write(fd,ptr,8*ch)!=(8*ch)) {
				syslog(LOG_NOTICE,"write error");
				return;
			}
		}
	}
}

static inline int fs_loadnode(bio *fd,uint8_t mver) {
	uint8_t unodebuff[4+1+1+1+2+4+4+4+4+4+4+8+4+2+8*65536+4*65536+4];
	const uint8_t *ptr,*chptr;
	uint8_t type;
	uint32_t trashseconds;
	uint32_t i,indx,pleng,ch,sessionids,sessionid;
	fsnode *p;
	static uint8_t nl;
	uint32_t hdrsize;

	if (fd==NULL) {
		nl=1;
		return 0;
	}

	if (bio_read(fd,&type,1)!=1) {
		return -1;
	}
	if (type==0) {	// last node
		return 1;
	}
	if (mver<=0x11) {
		hdrsize = 4+1+2+6*4;
	} else if (mver<=0x13) {
		hdrsize = 4+1+1+2+6*4;
	} else { // mver==0x14
		hdrsize = 4+1+1+1+2+5*4+2;
	}
	if (mver<=0x12) {
		type = fsnodes_type_convert(type);
	}
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		if (bio_read(fd,unodebuff,hdrsize)!=(hdrsize)) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading node: read error");
			return -1;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		if (bio_read(fd,unodebuff,hdrsize+4)!=(hdrsize+4)) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading node: read error");
			return -1;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
		if (mver<=0x13) {
			if (bio_read(fd,unodebuff,hdrsize+8+4+2)!=(hdrsize+8+4+2)) {
				int err = errno;
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				errno = err;
				mfs_errlog(LOG_ERR,"loading node: read error");
				return -1;
			}
		} else {
			if (bio_read(fd,unodebuff,hdrsize+8+4)!=(hdrsize+8+4)) {
				int err = errno;
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				errno = err;
				mfs_errlog(LOG_ERR,"loading node: read error");
				return -1;
			}
		}
		break;
	default:
		if (nl) {
			fputc('\n',stderr);
			nl=0;
		}
		mfs_arg_syslog(LOG_ERR,"loading node: unrecognized node type: %"PRIu8,type);
		return -1;
	}
	ptr = unodebuff;
	switch (type) {
		case TYPE_DIRECTORY:
			p = fsnode_dir_malloc();
			break;
		case TYPE_FILE:
		case TYPE_TRASH:
		case TYPE_SUSTAINED:
			p = fsnode_file_malloc();
			break;
		case TYPE_SYMLINK:
			p = fsnode_symlink_malloc();
			break;
		case TYPE_BLOCKDEV:
		case TYPE_CHARDEV:
			p = fsnode_dev_malloc();
			break;
		default:
			p = fsnode_other_malloc();
	}
	passert(p);
	p->xattrflag = 0;
	p->aclpermflag = 0;
	p->acldefflag = 0;
	p->type = type;
	p->inode = get32bit(&ptr);
	p->sclassid = get8bit(&ptr);
	if (type!=TYPE_DIRECTORY && type!=TYPE_FILE && type!=TYPE_TRASH && type!=TYPE_SUSTAINED) {
		p->sclassid=0;
	}
	sclass_incref(p->sclassid,p->type);
	if (mver<=0x11) {
		uint16_t flagsmode = get16bit(&ptr);
		p->eattr = flagsmode>>12;
		p->winattr = 0;
		p->mode = flagsmode&0xFFF;
	} else {
		p->eattr = get8bit(&ptr);
		if (mver>=0x14) {
			p->winattr = get8bit(&ptr);
		} else {
			p->winattr = 0;
		}
		p->mode = get16bit(&ptr);
	}
	p->uid = get32bit(&ptr);
	p->gid = get32bit(&ptr);
	p->atime = get32bit(&ptr);
	p->mtime = get32bit(&ptr);
	p->ctime = get32bit(&ptr);
	if (mver<=0x13) {
		trashseconds = get32bit(&ptr);
		p->trashtime = (trashseconds+3599)/3600;
	} else {
		p->trashtime = get16bit(&ptr);
	}
	switch (type) {
	case TYPE_DIRECTORY:
		memset(&(p->data.ddata.stats),0,sizeof(statsrecord));
		p->data.ddata.quota = NULL;
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
		break;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		p->data.odata.nlink = 0;
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.nlink = 0;
		p->data.devdata.rdev = get32bit(&ptr);
		break;
	case TYPE_SYMLINK:
		p->data.sdata.nlink = 0;
		pleng = get32bit(&ptr);
		p->data.sdata.pleng = pleng;
		if (pleng>0) {
			if (pleng>MFS_SYMLINK_MAX) {
				p->data.sdata.pleng = 22;
				p->data.sdata.path = symlink_malloc(p->data.sdata.pleng);
				passert(p->data.sdata.path);
				memcpy(p->data.sdata.path,"... path too long ...",p->data.sdata.pleng);
				bio_skip(fd,pleng);
			} else {
				p->data.sdata.path = symlink_malloc(pleng);
				passert(p->data.sdata.path);
				if (bio_read(fd,p->data.sdata.path,pleng)!=pleng) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						nl=0;
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading node: read error");
					symlink_free(p->data.sdata.path,pleng);
					fsnode_symlink_free(p);
					return -1;
				}
			}
		} else {
			p->data.sdata.path = NULL;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
		p->data.fdata.nlink = 0;
		p->data.fdata.length = get64bit(&ptr);
		ch = get32bit(&ptr);
		p->data.fdata.chunks = ch;
		if (mver<=0x13) {
			sessionids = get16bit(&ptr);
		} else {
			sessionids = 0;
		}
		if (ch>0) {
			p->data.fdata.chunktab = chunktab_malloc(ch);
			passert(p->data.fdata.chunktab);
		} else {
			p->data.fdata.chunktab = NULL;
		}
		indx = 0;
		while (ch>65536) {
			chptr = ptr;
			if (bio_read(fd,(uint8_t*)ptr,8*65536)!=(8*65536)) {
				int err = errno;
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				errno = err;
				mfs_errlog(LOG_ERR,"loading node: read error");
				if (p->data.fdata.chunktab) {
					chunktab_free(p->data.fdata.chunktab,p->data.fdata.chunks);
				}
				fsnode_file_free(p);
				return -1;
			}
			for (i=0 ; i<65536 ; i++) {
				p->data.fdata.chunktab[indx] = get64bit(&chptr);
				indx++;
			}
			ch-=65536;
		}
		if (bio_read(fd,(uint8_t*)ptr,8*ch+4*sessionids)!=(8*ch+4*sessionids)) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading node: read error");
			if (p->data.fdata.chunktab) {
				chunktab_free(p->data.fdata.chunktab,p->data.fdata.chunks);
			}
			fsnode_file_free(p);
			return -1;
		}
		for (i=0 ; i<ch ; i++) {
			p->data.fdata.chunktab[indx] = get64bit(&ptr);
			indx++;
		}

		while (sessionids) {
			sessionid = get32bit(&ptr);
			of_mr_acquire(sessionid,p->inode);
			sessionids--;
		}
	}
	p->parents = NULL;
	fsnodes_node_add(p);
	fsnodes_used_inode(p->inode);
	nodes++;
	if (type==TYPE_DIRECTORY) {
		dirnodes++;
	}
	if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		filenodes++;
	}
	return 0;
}

uint8_t fs_storenodes(bio *fd) {
	uint32_t i;
	uint8_t hdr[8];
	uint8_t *ptr;
	fsnode *p;

	if (fd==NULL) {
		return 0x14;
	}
	ptr = hdr;
	put32bit(&ptr,maxnodeid);
	put32bit(&ptr,nodes);
	if (bio_write(fd,hdr,8)!=8) {
		return 0xFF;
	}

	for (i=0 ; i<noderehashpos ; i++) {
		for (p=nodehashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; p && bio_error(fd)==0 ; p=p->next) {
			fs_storenode(p,fd);
		}
	}
	fs_storenode(NULL,fd);	// end marker
	return 0;
}

static inline void fs_storeedgelist(fsedge *e,bio *fd) {
	while (e && bio_error(fd)==0) {
		fs_storeedge(e,fd);
		e=e->nextchild;
	}
}

static inline void fs_storeedges_rec(fsnode *f,bio *fd) {
	fsedge *e;
	fs_storeedgelist(f->data.ddata.children,fd);
	for (e=f->data.ddata.children ; e && bio_error(fd)==0 ; e=e->nextchild) {
		if (e->child->type==TYPE_DIRECTORY) {
			fs_storeedges_rec(e->child,fd);
		}
	}
}

uint8_t fs_storeedges(bio *fd) {
	uint32_t bid;
	uint8_t hdr[8];
	uint8_t *ptr;

	if (fd==NULL) {
		return 0x11;
	}

	ptr = hdr;
	put64bit(&ptr,nextedgeid);
	if (bio_write(fd,hdr,8)!=8) {
		return 0xFF;
	}

	fs_storeedges_rec(root,fd);
	for (bid=0 ; bid<TRASH_BUCKETS ; bid++) {
		fs_storeedgelist(trash[bid],fd);
	}
	for (bid=0 ; bid<SUSTAINED_BUCKETS ; bid++) {
		fs_storeedgelist(sustained[bid],fd);
	}
	fs_storeedge(NULL,fd);	// end marker
	return 0;
}

int fs_lostnode(fsnode *p) {
	uint8_t artname[40];
	uint32_t i,l;
	i=0;
	do {
		if (i==0) {
			l = snprintf((char*)artname,40,"lost_node_%"PRIu32,p->inode);
		} else {
			l = snprintf((char*)artname,40,"lost_node_%"PRIu32".%"PRIu32,p->inode,i);
		}
		if (!fsnodes_nameisused(root,l,artname)) {
			fsnodes_link(0,root,p,l,artname);
			return 1;
		}
		i++;
	} while (i);
	return -1;
}

int fs_checknodes(int ignoreflag) {
	uint32_t i;
	uint8_t nl;
	fsnode *p;
	nl=1;
	for (i=0 ; i<noderehashpos ; i++) {
		for (p=nodehashtab[i>>HASHTAB_LOBITS][i&HASHTAB_MASK] ; p ; p=p->next) {
			if (p->parents==NULL && p!=root) {
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				fprintf(stderr,"found orphaned inode: %"PRIu32"\n",p->inode);
				syslog(LOG_ERR,"found orphaned inode: %"PRIu32,p->inode);
				if (ignoreflag) {
					if (fs_lostnode(p)<0) {
						return -1;
					}
				} else {
					fprintf(stderr,"use option '-i' to attach this node to root dir\n");
					return -1;
				}
			}
		}
	}
	return 1;
}

int fs_importnodes(bio *fd,uint32_t mni) {
	int s;
	maxnodeid = mni;
	hashelements = 1;

	fsnodes_init_freebitmask();

	fs_loadnode(NULL,0);
	do {
		s = fs_loadnode(fd,0x10);
	} while (s==0);

	return (s<0)?-1:0;
}

int fs_loadnodes(bio *fd,uint8_t mver) {
	int s;
	uint8_t hdr[8];
	const uint8_t *ptr;

	if (mver>=0x11) {
		if (bio_read(fd,hdr,8)!=8) {
			return -1;
		}
		ptr = hdr;
		maxnodeid = get32bit(&ptr);
		hashelements = get32bit(&ptr);
	} else {
		if (bio_read(fd,hdr,4)!=4) {
			return -1;
		}
		ptr = hdr;
		maxnodeid = get32bit(&ptr);
		hashelements = 1;
	}
	fsnodes_init_freebitmask();

	fs_loadnode(NULL,0);
	do {
		s = fs_loadnode(fd,mver);
	} while (s==0);

	return (s<0)?-1:0;
}

int fs_loadedges(bio *fd,uint8_t mver,int ignoreflag) {
	int s;
	uint8_t hdr[8];
	const uint8_t *ptr;

	if (mver>=0x11) {
		if (bio_read(fd,hdr,8)!=8) {
			return -1;
		}
		ptr = hdr;
		nextedgeid = get64bit(&ptr);
		edgesneedrenumeration = 0;
	} else {
		nextedgeid = EDGEID_MAX;
		edgesneedrenumeration = 1;
	}

	fs_loadedge(NULL,mver,ignoreflag);	// init
	do {
		s = fs_loadedge(fd,mver,ignoreflag);
	} while (s==0);

	return (s<0)?-1:0;
}

uint8_t fs_storefree(bio *fd) {
	uint8_t wbuff[8*1024],*ptr;
	freenode *n;
	uint32_t l;
	if (fd==NULL) {
		return 0x10;
	}
	l=0;
	for (n=freelist ; n ; n=n->next) {
		l++;
	}
	ptr = wbuff;
	put32bit(&ptr,l);
	if (bio_write(fd,wbuff,4)!=4) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}
	l=0;
	ptr=wbuff;
	for (n=freelist ; n ; n=n->next) {
		if (l==1024) {
			if (bio_write(fd,wbuff,8*1024)!=(8*1024)) {
				syslog(LOG_NOTICE,"write error");
				return 0xFF;
			}
			l=0;
			ptr=wbuff;
		}
		put32bit(&ptr,n->inode);
		put32bit(&ptr,n->ftime);
		l++;
	}
	if (l>0) {
		if (bio_write(fd,wbuff,8*l)!=(8*l)) {
			syslog(LOG_NOTICE,"write error");
			return 0xFF;
		}
	}
	return 0;
}

int fs_loadfree(bio *fd,uint8_t mver) {
	uint8_t rbuff[8*1024];
	const uint8_t *ptr;
	freenode *n;
	uint32_t l,t;
	uint32_t nodeid,ftime;
	uint8_t nl=1;

	(void)mver;

	if (bio_read(fd,rbuff,4)!=4) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			// nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading free nodes: read error");
		return -1;
	}
	ptr=rbuff;
	t = get32bit(&ptr);
	freelist = NULL;
	freetail = &(freelist);
	freelastts = 0;
	l=0;
	while (t>0) {
		if (l==0) {
			if (t>1024) {
				if (bio_read(fd,rbuff,8*1024)!=(8*1024)) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						// nl=0;
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading free nodes: read error");
					return -1;
				}
				l=1024;
			} else {
				if (bio_read(fd,rbuff,8*t)!=(8*t)) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						// nl=0;
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading free nodes: read error");
					return -1;
				}
				l=t;
			}
			ptr = rbuff;
		}
		nodeid = get32bit(&ptr);
		ftime = get32bit(&ptr);
		n = freenode_malloc();
		n->inode = nodeid;
		n->ftime = ftime;
		n->next = NULL;
		*freetail = n;
		freetail = &(n->next);
		freelastts = ftime;
		fsnodes_used_inode(nodeid);
		l--;
		t--;
	}
	return 0;
}

// quota entry:
// inode:4 graceperiod:4 exceeded:1 flags:1 ts:4 sinodes:4 hinodes:4 slength:8 hlength:8 ssize:8 hsize:8 srealsize:8 hrealsize:8 = 66B

uint8_t fs_storequota(bio *fd) {
	uint8_t wbuff[70],*ptr;
	uint32_t l;
	quotanode *qn;
	if (fd==NULL) {
		return 0x11;
	}
	l=0;
	for (qn = quotahead ; qn ; qn=qn->next) {
		l++;
	}
	ptr = wbuff;
	put32bit(&ptr,l);
	if (bio_write(fd,wbuff,4)!=4) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}
	for (qn = quotahead ; qn ; qn=qn->next) {
		ptr=wbuff;
			put32bit(&ptr,qn->node->inode);
			put32bit(&ptr,qn->graceperiod);
			put8bit(&ptr,qn->exceeded);
			put8bit(&ptr,qn->flags);
			put32bit(&ptr,qn->stimestamp);
			put32bit(&ptr,qn->sinodes);
			put32bit(&ptr,qn->hinodes);
			put64bit(&ptr,qn->slength);
			put64bit(&ptr,qn->hlength);
			put64bit(&ptr,qn->ssize);
			put64bit(&ptr,qn->hsize);
			put64bit(&ptr,qn->srealsize);
			put64bit(&ptr,qn->hrealsize);
			if (bio_write(fd,wbuff,70)!=70) {
				syslog(LOG_NOTICE,"write error");
				return 0xFF;
			}
	}
	return 0;
}

int fs_loadquota(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t rbuff[70];
	const uint8_t *ptr;
	quotanode *qn;
	fsnode *fn;
	uint32_t l,inode;
	uint8_t nl=1;
	int32_t rsize;

	if (bio_read(fd,rbuff,4)!=4) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			// nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading quota: read error");
		return -1;
	}
	ptr=rbuff;
	l = get32bit(&ptr);
	quotahead = NULL;
	rsize = (mver==0x10)?66:70;
//	freetail = &(freelist);
	while (l>0) {
		if (bio_read(fd,rbuff,rsize)!=rsize) {
			int err = errno;
			if (nl) {
				fputc('\n',stderr);
				// nl=0;
			}
			errno = err;
			mfs_errlog(LOG_ERR,"loading quota: read error");
			return -1;
		}
		ptr = rbuff;
		inode = get32bit(&ptr);
		fn = fsnodes_node_find(inode);
		if (fn==NULL || fn->type!=TYPE_DIRECTORY) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"quota defined for %s inode: %"PRIu32"\n",(fn==NULL)?"non existing":"not directory",inode);
			syslog(LOG_ERR,"quota defined for %s inode: %"PRIu32,(fn==NULL)?"non existing":"not directory",inode);
			if (ignoreflag) {
				ptr+=(rsize-4);
			} else {
				fprintf(stderr,"use option '-i' to remove this quota definition");
				return -1;
			}
		} else {
			qn = fsnodes_new_quotanode(fn);
			if (mver==0x10) {
				qn->graceperiod = QuotaDefaultGracePeriod;
			} else {
				qn->graceperiod = get32bit(&ptr);
			}
			qn->exceeded = get8bit(&ptr);
			qn->flags = get8bit(&ptr);
			qn->stimestamp = get32bit(&ptr);
			qn->sinodes = get32bit(&ptr);
			qn->hinodes = get32bit(&ptr);
			qn->slength = get64bit(&ptr);
			qn->hlength = get64bit(&ptr);
			qn->ssize = get64bit(&ptr);
			qn->hsize = get64bit(&ptr);
			qn->srealsize = get64bit(&ptr);
			qn->hrealsize = get64bit(&ptr);
		}
		l--;
	}
	return 0;
}

void fs_new(void) {
	nextedgeid = (EDGEID_MAX-1);
	edgesneedrenumeration = 0;
	hashelements = 1;
	maxnodeid = MFS_ROOT_ID;
	fsnodes_init_freebitmask();
	root = fsnode_dir_malloc();
	passert(root);
	root->inode = MFS_ROOT_ID;
	root->xattrflag = 0;
	root->aclpermflag = 0;
	root->acldefflag = 0;
	root->type = TYPE_DIRECTORY;
	root->ctime = root->mtime = root->atime = main_time();
	root->sclassid = DEFAULT_SCLASS;
	sclass_incref(root->sclassid,root->type);
	root->trashtime = DEFAULT_TRASHTIME;
	root->eattr = 0;
	root->winattr = 0;
	root->mode = 0777;
	root->uid = 0;
	root->gid = 0;
	memset(&(root->data.ddata.stats),0,sizeof(statsrecord));
	root->data.ddata.quota = NULL;
	root->data.ddata.children = NULL;
	root->data.ddata.elements = 0;
	root->data.ddata.nlink = 2;
	root->parents = NULL;
	fsnodes_node_add(root);
	fsnodes_used_inode(root->inode);
	nodes=1;
	dirnodes=1;
	filenodes=0;
}

void fs_afterload(void) {
	fprintf(stderr,"connecting files and chunks ... ");
	fflush(stderr);
	fs_add_files_to_chunks();
	fprintf(stderr,"ok\n");
	fprintf(stderr,"all inodes: %"PRIu32"\n",nodes);
	fprintf(stderr,"directory inodes: %"PRIu32"\n",dirnodes);
	fprintf(stderr,"file inodes: %"PRIu32"\n",filenodes);
	fprintf(stderr,"chunks: %"PRIu32"\n",chunk_count());
}

int fs_check_consistency(int ignoreflag) {
	fprintf(stderr,"checking filesystem consistency ... ");
	fflush(stderr);
	root = fsnodes_node_find(MFS_ROOT_ID);
	if (root==NULL) {
		fprintf(stderr,"root node not found !!!\n");
		syslog(LOG_ERR,"error reading metadata (no root)");
		return -1;
	}
	if (fs_checknodes(ignoreflag)<0) {
		return -1;
	}
	fprintf(stderr,"ok\n");
	return 0;
}

void fs_cs_disconnected(void) {
	test_start_time = main_time()+600;
}


void fs_reload(void) {
	uint32_t mlink;
	if (cfg_isdefined("QUOTA_TIME_LIMIT") && !cfg_isdefined("QUOTA_DEFAULT_GRACE_PERIOD")) {
		QuotaDefaultGracePeriod = cfg_getuint32("QUOTA_TIME_LIMIT",7*86400); // deprecated option
	} else {
		QuotaDefaultGracePeriod = cfg_getuint32("QUOTA_DEFAULT_GRACE_PERIOD",604800);
	}
	AtimeMode = cfg_getuint8("ATIME_MODE",0);
	if (AtimeMode>ATIME_NEVER) {
		syslog(LOG_NOTICE,"unrecognized value for ATIME_MODE - using defaults");
		AtimeMode = 0;
	}
	mlink = cfg_getuint32("MAX_ALLOWED_HARD_LINKS",32767);
	if (mlink<8) {
		syslog(LOG_NOTICE,"MAX_ALLOWED_HARD_LINKS is less than 8 - less that minimum number of hard links requierd by POSIX - setting to 8");
		mlink = 8;
	}
	if (mlink>65000) {
		syslog(LOG_NOTICE,"MAX_ALLOWED_HARD_LINKS is greater than 65000 - setting to 65000");
		mlink = 65000;
	}
	MaxAllowedHardLinks = mlink;
}

int fs_strinit(void) {
	uint32_t bid;
	trash_bid = 0;
	sustained_bid = 0;
	root = NULL;
	for (bid=0 ; bid<TRASH_BUCKETS ; bid++) {
		trash[bid] = NULL;
	}
	for (bid=0 ; bid<SUSTAINED_BUCKETS ; bid++) {
		sustained[bid] = NULL;
	}
	trashspace = 0;
	sustainedspace = 0;
	trashnodes = 0;
	sustainednodes = 0;
	quotahead = NULL;
	freelist = NULL;
	freetail = &(freelist);
	freelastts = 0;
	fsnodes_edgeid_init();
	fsnodes_node_hash_init();
	fsnodes_edge_hash_init();
	fsnode_init();
	fsedge_init();
	symlink_init();
	chunktab_init();
	test_start_time = main_time()+900;
	fs_reload();
	snapshot_inodehash = chash_new();

	main_reload_register(fs_reload);
	main_msectime_register(100,0,fs_test_files);
	main_time_register(1,0,fsnodes_check_all_quotas);
	main_time_register(1,0,fs_emptytrash);
	main_time_register(1,0,fs_emptysustained);
	main_time_register(60,0,fsnodes_freeinodes);
	return 0;
}

