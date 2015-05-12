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
#include "buckets.h"
#include "clocks.h"
#include "missinglog.h"

#define HASHTAB_LOBITS 24
#define HASHTAB_HISIZE (0x80000000>>(HASHTAB_LOBITS))
#define HASHTAB_LOSIZE (1<<HASHTAB_LOBITS)
#define HASHTAB_MASK (HASHTAB_LOSIZE-1)
#define HASHTAB_MOVEFACTOR 5

#define DEFAULT_GOAL 1
#define DEFAULT_TRASHTIME 86400

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
static uint32_t QuotaTimeLimit;

typedef struct _fsnode {
	uint32_t id;
	uint32_t ctime,mtime,atime;
	unsigned xattrflag:1;
	unsigned aclpermflag:1;
	unsigned acldefflag:1;
	unsigned type:4;
	unsigned goal:4;
	unsigned spare:4;
	unsigned flags:4;
	unsigned mode:12;
//	uint16_t mode;	// only 12 lowest bits are used for mode, in unix standard upper 4 are used for object type, but since there is field "type" this bits can be used as extra flags
	uint32_t uid;
	uint32_t gid;
	uint32_t trashtime;
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
			uint8_t end;
		} sdata;
		struct _devdata {
			uint32_t rdev;			// type==TYPE_BLOCKDEV ; type==TYPE_CHARDEV
			uint8_t end;
		} devdata;
		struct _fdata {				// type==TYPE_FILE ; type==TYPE_TRASH ; type==TYPE_SUSTAINED
			uint64_t length;
			uint64_t *chunktab;
			uint32_t chunks;
			uint8_t end;
		} fdata;
	} data;
} fsnode;

typedef struct _freenode {
	uint32_t id;
	uint32_t ftime;
	struct _freenode *next;
} freenode;

static uint32_t *freebitmask;
static uint32_t bitmasksize;
static uint32_t searchpos;
static freenode *freelist,**freetail;

static fsedge *trash;
static fsedge *sustained;
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

static uint64_t trashspace;
static uint64_t sustainedspace;
static uint32_t trashnodes;
static uint32_t sustainednodes;
static uint32_t filenodes;
static uint32_t dirnodes;

static uint64_t *edgeid_id_hashtab;
static fsedge **edgeid_ptr_hashtab;

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

CREATE_BUCKET_ALLOCATOR(freenode,freenode,5000)

CREATE_BUCKET_ALLOCATOR(quotanode,quotanode,500)

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

#define NODE_BUCKET_SIZE 65500
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
	nrelemsize[4] = offsetof(fsnode,data);
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



#define EDGE_BUCKET_SIZE 65500
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




#define SYMLINK_BUCKET_SIZE 65500
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




#define CHUNKTAB_BUCKET_SIZE 100000
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

void fsnodes_free_id(uint32_t id,uint32_t ts) {
	freenode *n;
	n = freenode_malloc();
	n->id = id;
	n->ftime = ts;
	n->next = NULL;
	*freetail = n;
	freetail = &(n->next);
}

uint8_t fs_univ_freeinodes(uint32_t ts,uint8_t sesflags,uint32_t freeinodes) {
	uint32_t fi,pos,mask;
	freenode *n,*an;
	fi = 0;
	n = freelist;
	while (n && n->ftime+86400<ts) {
		fi++;
		pos = (n->id >> 5);
		mask = 1<<(n->id&0x1F);
		freebitmask[pos] &= ~mask;
		if (pos<searchpos) {
			searchpos = pos;
		}
		an = n->next;
		freenode_free(n);
		n = an;
	}
	if (n) {
		freelist = n;
	} else {
		freelist = NULL;
		freetail = &(freelist);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (fi>0) {
			changelog("%" PRIu32 "|FREEINODES():%" PRIu32,ts,fi);
		}
	} else {
		meta_version_inc();
		if (freeinodes!=fi) {
			return 1;
		}
	}
	return 0;
}

void fsnodes_freeinodes(void) {
	fs_univ_freeinodes(main_time(),0,0);
}

uint8_t fs_mr_freeinodes(uint32_t ts,uint32_t freeinodes) {
	return fs_univ_freeinodes(ts,SESFLAG_METARESTORE,freeinodes);
}

void fsnodes_init_freebitmask (void) {
	bitmasksize = 0x100+(((maxnodeid)>>5)&0xFFFFFF80U);
	freebitmask = (uint32_t*)malloc(bitmasksize*sizeof(uint32_t));
	passert(freebitmask);
	memset(freebitmask,0,bitmasksize*sizeof(uint32_t));
	freebitmask[0]=1;	// reserve inode 0
	searchpos = 0;
}

void fsnodes_used_inode (uint32_t id) {
	uint32_t pos,mask;
	pos = id>>5;
	mask = 1<<(id&0x1F);
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
	hashval = fsnodes_hash(node->id,nleng,name);
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
	e->hashval = fsnodes_hash(e->parent->id,e->nleng,e->name);
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
		if (edgehashelem>edgehashsize) {
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
			hash = hash32(p->id) & mask;
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

static inline fsnode* fsnodes_node_find(uint32_t id) {
	fsnode *p;
	uint32_t hash;

	if (nodehashsize==0) {
		return NULL;
	}
	hash = hash32(id) & (nodehashsize-1);
	if (noderehashpos<nodehashsize) {
		fsnodes_node_hash_move();
		if (hash >= noderehashpos) {
			hash -= nodehashsize/2;
		}
	}
	for (p=nodehashtab[hash>>HASHTAB_LOBITS][hash&HASHTAB_MASK] ; p ; p=p->next) {
		if (p->id==id) {
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
	hash = hash32(p->id) & (nodehashsize-1);
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
	hash = hash32(p->id) & (nodehashsize-1);
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
		if (nodehashelem>nodehashsize) {
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
	exceeded = ((qn->stimestamp && qn->stimestamp+QuotaTimeLimit<ts))?1:0;
	if (qn->exceeded != exceeded) {
		qn->exceeded = exceeded;
		chg = 1;
	}
	if (chg) {
		changelog("%"PRIu32"|QUOTA(%"PRIu32",%"PRIu8",%"PRIu8",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64")",ts,qn->node->id,qn->exceeded,qn->flags,qn->stimestamp,qn->sinodes,qn->hinodes,qn->slength,qn->hlength,qn->ssize,qn->hsize,qn->srealsize,qn->hrealsize);
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

// stats
static inline void fsnodes_get_stats(fsnode *node,statsrecord *sr) {
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
		sr->realsize = sr->size * node->goal;
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
	uint64_t quotarsize;
	if (node && node->type==TYPE_DIRECTORY && (qn=node->data.ddata.quota) && (qn->flags&(QUOTA_FLAG_HREALSIZE|QUOTA_FLAG_SREALSIZE))) {
		fsnodes_get_stats(node,&sr);
		quotarsize = UINT64_C(0xFFFFFFFFFFFFFFFF);
		if ((qn->flags&QUOTA_FLAG_HREALSIZE) && quotarsize > qn->hrealsize) {
			quotarsize = qn->hrealsize;
		}
		if ((qn->flags&QUOTA_FLAG_SREALSIZE) && quotarsize > qn->srealsize) {
			quotarsize = qn->srealsize;
		}
		if (sr.realsize >= quotarsize) {
			*availspace = 0;
		} else if (*availspace > quotarsize - sr.realsize) {
			*availspace = quotarsize - sr.realsize;
		}
		if (*totalspace > quotarsize) {
			*totalspace = quotarsize;
		}
		if (sr.realsize + *availspace < *totalspace) {
			*totalspace = sr.realsize + *availspace;
		}
	}
	if (node && node!=root) {
		for (e=node->parents ; e ; e=e->nextparent) {
			fsnodes_quota_fixspace(e->parent,totalspace,availspace);
		}
	}
}

/*
static inline int fsnodes_access(fsnode *node,uint32_t uid,uint32_t gid,uint8_t modemask,uint8_t sesflags) {
	uint8_t nodemode;
	if (uid==0) {
		return 1;
	}
	if (node->aclpermflag) { // ignore acl's in "simple" mode
		return 1;
	}
	if (uid==node->uid || (node->flags&EATTR_NOOWNER)) {
		nodemode = ((node->mode)>>6) & 7;
	} else if (sesflags&SESFLAG_IGNOREGID) {
		nodemode = (((node->mode)>>3) | (node->mode)) & 7;
	} else if (gid==node->gid) {
		nodemode = ((node->mode)>>3) & 7;
	} else {
		nodemode = (node->mode & 7);
	}
	if ((nodemode & modemask) == modemask) {
		return 1;
	}
	return 0;
}
*/

static inline int fsnodes_access_ext(fsnode *node,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t modemask,uint8_t sesflags) {
	uint8_t nodemode,gf;
	if (uid==0) {
		return 1;
	}
	if (node->aclpermflag) {
		return posix_acl_perm(node->id,uid,gids,gid,node->uid,node->gid,modemask);
	} else if (uid==node->uid || (node->flags&EATTR_NOOWNER)) {
		nodemode = ((node->mode)>>6) & 7;
	} else if (sesflags&SESFLAG_IGNOREGID) {
		nodemode = (((node->mode)>>3) | (node->mode)) & 7;
	} else {
		nodemode = 0;
		gf = 0;
		while (gids>0 && gf==0) {
			gids--;
			if (gid[gids]==node->gid) {
				nodemode = ((node->mode)>>3) & 7;
				gf = 1;
			}
		}
		if (gf==0) {
			nodemode = (node->mode & 7);
		}
	}
	if ((nodemode & modemask) == modemask) {
		return 1;
	}
	return 0;
}

static inline int fsnodes_sticky_access(fsnode *parent,fsnode *node,uint32_t uid) {
	if (uid==0 || (parent->mode&01000)==0) {	// super user or sticky bit is not set
		return 1;
	}
	if (uid==parent->uid || (parent->flags&EATTR_NOOWNER) || uid==node->uid || (node->flags&EATTR_NOOWNER)) {
		return 1;
	}
	return 0;
}

static inline uint32_t fsnodes_nlink(uint32_t rootinode,fsnode *node) {
	fsedge *e;
	fsnode *p;
	uint32_t nlink;
	nlink = 0;
	if (node->id!=rootinode) {
		if (rootinode==MFS_ROOT_ID) {
			for (e=node->parents ; e ; e=e->nextparent) {
				nlink++;
			}
		} else {
			for (e=node->parents ; e ; e=e->nextparent) {
				p = e->parent;
				while (p) {
					if (rootinode==p->id) {
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
	if (node->id!=rootinode) {
		if (rootinode==MFS_ROOT_ID) {
			for (e=node->parents ; e ; e=e->nextparent) {
				put32bit(&buff,e->parent->id);
			}
		} else {
			for (e=node->parents ; e ; e=e->nextparent) {
				p = e->parent;
				while (p) {
					if (rootinode==p->id) {
						if (e->parent->id==rootinode) {
							put32bit(&buff,MFS_ROOT_ID);
						} else {
							put32bit(&buff,e->parent->id);
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
	if (node->id!=rootinode) {
		for (e=node->parents ; e ; e=e->nextparent) {
			psize = e->nleng;
			p = e->parent;
			while (p) {
				if (rootinode==p->id) {
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
	}
	return totalpsize;
}

static inline void fsnodes_get_paths_data(uint32_t rootinode,fsnode *node,uint8_t *buff) {
	fsedge *e;
	fsnode *p;
	uint32_t psize;
	uint8_t *b;

	if (node->id!=rootinode) {
		for (e=node->parents ; e ; e=e->nextparent) {
			psize = e->nleng;
			p = e->parent;
			while (p) {
				if (rootinode==p->id) {
					put32bit(&buff,psize);
					b = buff;
					buff += psize;

					psize -= e->nleng;
					memcpy(b+psize,e->name,e->nleng);
					p = e->parent;
					while (p) {
						if (rootinode==p->id) {
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
	}
}

static inline void fsnodes_fill_attr(fsnode *node,fsnode *parent,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t sesflags,uint8_t attr[35]) {
	uint8_t *ptr;
	uint8_t type;
	uint8_t flags;
	uint16_t mode;
	uint32_t nlink;
	uint64_t dleng;

	(void)sesflags;
	ptr = attr;
	type = node->type;
	if (type==TYPE_TRASH || type==TYPE_SUSTAINED) {
		type = TYPE_FILE;
	}
	flags = 0;
	if (parent) {
		if (parent->flags&EATTR_NOECACHE) {
			flags |= MATTR_NOECACHE;
		}
	}
	if ((node->flags&(EATTR_NOOWNER|EATTR_NOACACHE)) || (sesflags&SESFLAG_MAPALL)) {
		flags |= MATTR_NOACACHE;
	}
	if ((node->flags&EATTR_NODATACACHE)==0) {
		flags |= MATTR_ALLOWDATACACHE;
	}
	if (node->xattrflag==0 && node->aclpermflag==0 && node->acldefflag==0) {
		flags |= MATTR_NOXATTR;
	}
	if (node->aclpermflag) {
		mode = (posix_acl_getmode(node->id) & 0777) | (node->mode & 07000);
	} else {
		mode = node->mode & 07777;
	}
	if ((node->flags&EATTR_NOOWNER) && uid!=0) {
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
	nlink = fsnodes_nlink(MFS_ROOT_ID,node);
	switch (node->type) {
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_SUSTAINED:
		put32bit(&ptr,nlink);
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
		if (dleng<UINT64_C(0x400)) {
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
		put32bit(&ptr,nlink);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		put32bit(&ptr,node->data.sdata.pleng);
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr,nlink);
		put32bit(&ptr,node->data.devdata.rdev);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		break;
	default:
		put32bit(&ptr,nlink);
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
		*ptr++=0;
	}
}

static inline void fsnodes_remove_edge(uint32_t ts,fsedge *e) {
	statsrecord sr;
	if (e->parent) {
		fsnodes_edgeid_remove(e);
		fsnodes_get_stats(e->child,&sr);
		fsnodes_sub_stats(e->parent,&sr);
		e->parent->mtime = e->parent->ctime = ts;
		e->parent->data.ddata.elements--;
		if (e->child->type==TYPE_DIRECTORY) {
			e->parent->data.ddata.nlink--;
		}
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
	if (child->type==TYPE_DIRECTORY) {
		parent->data.ddata.nlink++;
	}
	fsnodes_get_stats(child,&sr);
	fsnodes_add_stats(parent,&sr);
	if (ts>0) {
		parent->mtime = parent->ctime = ts;
		child->ctime = ts;
	}
}

static inline fsnode* fsnodes_create_node(uint32_t ts,fsnode* node,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gid,uint8_t copysgid) {
	fsnode *p;
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
	p->id = fsnodes_get_next_id();
	p->xattrflag = 0;
	p->aclpermflag = 0;
	p->acldefflag = 0;
	p->type = type;
	p->ctime = p->mtime = p->atime = ts;
	if (type==TYPE_DIRECTORY || type==TYPE_FILE) {
		p->goal = node->goal;
		p->trashtime = node->trashtime;
	} else {
		p->goal = DEFAULT_GOAL;
		p->trashtime = DEFAULT_TRASHTIME;
	}
	if (type==TYPE_DIRECTORY) {
		p->flags = node->flags;
	} else {
		p->flags = node->flags & ~(EATTR_NOECACHE);
	}
	if (node->acldefflag) {
		p->mode = mode;
	} else {
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
	case TYPE_FILE:
		p->data.fdata.length = 0;
		p->data.fdata.chunks = 0;
		p->data.fdata.chunktab = NULL;
		break;
	case TYPE_SYMLINK:
		p->data.sdata.pleng = 0;
		p->data.sdata.path = NULL;
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = 0;
	}
	p->parents = NULL;
	fsnodes_node_add(p);
	fsnodes_link(ts,node,p,nleng,name);
	if (node->acldefflag) {
		uint8_t aclcopied;
		aclcopied = posix_acl_copydefaults(node->id,p->id,(type==TYPE_DIRECTORY)?1:0,mode);
		if (aclcopied&1) {
			p->aclpermflag = 1;
		}
		if (aclcopied&2) {
			p->acldefflag = 1;
		}
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

static inline uint32_t fsnodes_getdetachedsize(fsedge *start) {
	fsedge *e;
	uint32_t result=0;
	for (e = start ; e ; e=e->nextchild) {
		if (e->nleng>240) {
			result+=245;
		} else {
			result+=5+e->nleng;
		}
	}
	return result;
}

static inline void fsnodes_getdetacheddata(fsedge *start,uint8_t *dbuff) {
	fsedge *e;
	const uint8_t *sptr;
	uint8_t c;
	for (e = start ; e ; e=e->nextchild) {
		if (e->nleng>240) {
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
		} else {
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
		put32bit(&dbuff,e->child->id);
	}
}

static inline uint32_t fsnodes_readdirsize(fsnode *p,fsedge *e,uint32_t maxentries,uint64_t nedgeid,uint8_t withattr) {
	uint32_t result = 0;
	while (maxentries>0 && nedgeid<EDGEID_MAX) {
		if (nedgeid==0) {
			result += ((withattr)?40:6)+1; // self ('.')
			nedgeid=1;
		} else {
			if (nedgeid==1) {
				result += ((withattr)?40:6)+2; // parent ('..')
				e = p->data.ddata.children;
			} else if (e) {
				result+=((withattr)?40:6)+e->nleng;
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

static inline void fsnodes_readdirdata(uint32_t rootinode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t sesflags,fsnode *p,fsedge *e,uint32_t maxentries,uint64_t *nedgeidp,uint8_t *dbuff,uint8_t withattr) {
	uint64_t nedgeid = *nedgeidp;
	while (maxentries>0 && nedgeid<EDGEID_MAX) {
		if (nedgeid==0) {
			dbuff[0]=1;
			dbuff[1]='.';
			dbuff+=2;
			if (p->id!=rootinode) {
				put32bit(&dbuff,p->id);
			} else {
				put32bit(&dbuff,MFS_ROOT_ID);
			}
			if (withattr) {
				fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff);
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
				if (p->id==rootinode) { // root node should returns self as its parent
					put32bit(&dbuff,MFS_ROOT_ID);
					if (withattr) {
						fsnodes_fill_attr(p,p,uid,gid,auid,agid,sesflags,dbuff);
						dbuff+=35;
					} else if (sesflags&SESFLAG_ATTRBIT) {
						put8bit(&dbuff,TYPE_DIRECTORY);
					} else {
						put8bit(&dbuff,'d');
					}
				} else {
					if (p->parents && p->parents->parent->id!=rootinode) {
						put32bit(&dbuff,p->parents->parent->id);
					} else {
						put32bit(&dbuff,MFS_ROOT_ID);
					}
					if (withattr) {
						if (p->parents) {
							fsnodes_fill_attr(p->parents->parent,p,uid,gid,auid,agid,sesflags,dbuff);
						} else {
							if (rootinode==MFS_ROOT_ID) {
								fsnodes_fill_attr(root,p,uid,gid,auid,agid,sesflags,dbuff);
							} else {
								fsnode *rn = fsnodes_node_find(rootinode);
								if (rn) {	// it should be always true because it's checked before, but better check than sorry
									fsnodes_fill_attr(rn,p,uid,gid,auid,agid,sesflags,dbuff);
								} else {
									memset(dbuff,0,35);
								}
							}
						}
						dbuff+=35;
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
				put32bit(&dbuff,e->child->id);
				if (withattr) {
					fsnodes_fill_attr(e->child,p,uid,gid,auid,agid,sesflags,dbuff);
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

static inline void fsnodes_checkfile(fsnode *p,uint32_t chunkcount[11]) {
	uint32_t i;
	uint64_t chunkid;
	uint8_t count;
	for (i=0 ; i<11 ; i++) {
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
		}
	}
}

static inline uint8_t fsnodes_appendchunks(uint32_t ts,fsnode *dstobj,fsnode *srcobj) {
	uint64_t chunkid,length;
	uint32_t i;
	uint32_t srcchunks,dstchunks;
	statsrecord psr,nsr;
	fsedge *e;

	srcchunks=srcobj->data.fdata.chunks;
	while (srcchunks>0 && srcobj->data.fdata.chunktab[srcchunks-1]==0) {
		srcchunks--;
	}
	if (srcchunks==0) {
		return STATUS_OK;
	}
	dstchunks=dstobj->data.fdata.chunks;
	while (dstchunks>0 && dstobj->data.fdata.chunktab[dstchunks-1]==0) {
		dstchunks--;
	}
	i = srcchunks+dstchunks-1;	// last new chunk pos
	if (i>MAX_INDEX) {	// chain too long
		return ERROR_INDEXTOOBIG;
	}
	fsnodes_get_stats(dstobj,&psr);
	if (i>=dstobj->data.fdata.chunks) {
		uint32_t newchunks = i+1;
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

	for (i=0 ; i<srcchunks ; i++) {
		chunkid = srcobj->data.fdata.chunktab[i];
		dstobj->data.fdata.chunktab[i+dstchunks] = chunkid;
		if (chunkid>0) {
			if (chunk_add_file(chunkid,dstobj->goal)!=STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,srcobj->id,i);
			}
		}
	}

	length = (((uint64_t)dstchunks)<<MFSCHUNKBITS)+srcobj->data.fdata.length;
	if (dstobj->type==TYPE_TRASH) {
		trashspace -= dstobj->data.fdata.length;
		trashspace += length;
	} else if (dstobj->type==TYPE_SUSTAINED) {
		sustainedspace -= dstobj->data.fdata.length;
		sustainedspace += length;
	}
	dstobj->data.fdata.length = length;
	fsnodes_get_stats(dstobj,&nsr);
	for (e=dstobj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	dstobj->mtime = ts;
	dstobj->atime = ts;
	if (srcobj->atime!=ts) {
		srcobj->atime = ts;
	}
	return STATUS_OK;
}

static inline void fsnodes_changefilegoal(fsnode *obj,uint8_t goal) {
	uint32_t i;
	statsrecord psr,nsr;
	fsedge *e;

	fsnodes_get_stats(obj,&psr);
	nsr = psr;
	nsr.realsize = goal * nsr.size;
	for (e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	for (i=0 ; i<obj->data.fdata.chunks ; i++) {
		if (obj->data.fdata.chunktab[i]>0) {
			chunk_change_file(obj->data.fdata.chunktab[i],obj->goal,goal);
		}
	}
	obj->goal = goal;
}

static inline void fsnodes_setlength(fsnode *obj,uint64_t length) {
	uint32_t i,chunks;
	uint64_t chunkid;
	fsedge *e;
	statsrecord psr,nsr;
	fsnodes_get_stats(obj,&psr);

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
			if (chunk_delete_file(chunkid,obj->goal)!=STATUS_OK) {
				syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,obj->id,i);
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
	fsnodes_get_stats(obj,&nsr);
	for (e=obj->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
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
				if (chunk_delete_file(chunkid,toremove->goal)!=STATUS_OK) {
					syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,toremove->id,i);
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
	fsnodes_free_id(toremove->id,ts);
	if (toremove->xattrflag) {
		xattr_removeinode(toremove->id);
	}
	if (toremove->aclpermflag) {
		posix_acl_remove(toremove->id,POSIX_ACL_ACCESS);
	}
	if (toremove->acldefflag) {
		posix_acl_remove(toremove->id,POSIX_ACL_DEFAULT);
	}
	dcm_modify(toremove->id,0);
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
	uint16_t pleng=0;
	uint8_t *path=NULL;

	child = e->child;
	if (child->parents->nextparent==NULL) { // last link
		if (child->type==TYPE_FILE && (child->trashtime>0 || of_isfileopened(child->id))) {	// go to trash or sustained ? - get path
			fsnodes_getpath(e,&pleng,&path);
		}
	}
	fsnodes_remove_edge(ts,e);
	if (child->parents==NULL) {	// last link
		if (child->type == TYPE_FILE) {
			if (child->trashtime>0) {
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
				e->nextchild = trash;
				e->nextparent = NULL;
				e->prevchild = &trash;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				trash = e;
				child->parents = e;
				trashspace += child->data.fdata.length;
				trashnodes++;
			} else if (of_isfileopened(child->id)) {
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
				e->nextchild = sustained;
				e->nextparent = NULL;
				e->prevchild = &sustained;
				e->prevparent = &(child->parents);
				if (e->nextchild) {
					e->nextchild->prevchild = &(e->nextchild);
				}
				sustained = e;
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
	fsedge *e;
	e = p->parents;

	if (p->type==TYPE_TRASH) {
		trashspace -= p->data.fdata.length;
		trashnodes--;
		if (of_isfileopened(p->id)) {
			p->type = TYPE_SUSTAINED;
			sustainedspace += p->data.fdata.length;
			sustainednodes++;
			*(e->prevchild) = e->nextchild;
			if (e->nextchild) {
				e->nextchild->prevchild = e->prevchild;
			}
			e->nextchild = sustained;
			e->prevchild = &(sustained);
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			sustained = e;
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
		return ERROR_CANTCREATEPATH;
	}
	while (*path=='/' && pleng>0) {
		path++;
		pleng--;
	}
	if (pleng==0) {
		return ERROR_CANTCREATEPATH;
	}
	partleng=0;
	dots=0;
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {	// incorrect name character
			return ERROR_CANTCREATEPATH;
		} else if (path[i]=='/') {
			if (partleng==0) {	// "//" in path
				return ERROR_CANTCREATEPATH;
			}
			if (partleng==dots && partleng<=2) {	// '.' or '..' in path
				return ERROR_CANTCREATEPATH;
			}
			partleng=0;
			dots=0;
		} else {
			if (path[i]=='.') {
				dots++;
			}
			partleng++;
			if (partleng>MAXFNAMELENG) {
				return ERROR_CANTCREATEPATH;
			}
		}
	}
	if (partleng==0) {	// last part canot be empty - it's the name of undeleted file
		return ERROR_CANTCREATEPATH;
	}
	if (partleng==dots && partleng<=2) {	// '.' or '..' in path
		return ERROR_CANTCREATEPATH;
	}

/* create path */
	n = NULL;
	p = root;
	new = 0;
	for (;;) {
		if (p->data.ddata.quota && p->data.ddata.quota->exceeded) {
			return ERROR_QUOTA;
		}
		partleng=0;
		while (partleng<pleng && path[partleng]!='/') {
			partleng++;
		}
		if (partleng==pleng) {	// last name
			if (fsnodes_nameisused(p,partleng,path)) {
				return ERROR_EEXIST;
			}
			// remove from trash and link to new parent
			node->type = TYPE_FILE;
			node->ctime = ts;
			fsnodes_link(ts,p,node,partleng,path);
			fsnodes_remove_edge(ts,e);
			trashspace -= node->data.fdata.length;
			trashnodes--;
			return STATUS_OK;
		} else {
			if (new==0) {
				pe = fsnodes_lookup(p,partleng,path);
				if (pe==NULL) {
					new=1;
				} else {
					n = pe->child;
					if (n->type!=TYPE_DIRECTORY) {
						return ERROR_CANTCREATEPATH;
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

static inline void fsnodes_getgoal_recursive(fsnode *node,uint8_t gmode,uint32_t fgtab[10],uint32_t dgtab[10]) {
	fsedge *e;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		if (node->goal>9) {
			syslog(LOG_WARNING,"inode %"PRIu32": goal>9 !!! - fixing",node->id);
			fsnodes_changefilegoal(node,9);
		} else if (node->goal<1) {
			syslog(LOG_WARNING,"inode %"PRIu32": goal<1 !!! - fixing",node->id);
			fsnodes_changefilegoal(node,1);
		}
		fgtab[node->goal]++;
	} else if (node->type==TYPE_DIRECTORY) {
		if (node->goal>9) {
			syslog(LOG_WARNING,"inode %"PRIu32": goal>9 !!! - fixing",node->id);
			node->goal=9;
		} else if (node->goal<1) {
			syslog(LOG_WARNING,"inode %"PRIu32": goal<1 !!! - fixing",node->id);
			node->goal=1;
		}
		dgtab[node->goal]++;
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_getgoal_recursive(e->child,gmode,fgtab,dgtab);
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

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		fsnodes_bst_add(bstrootfiles,node->trashtime);
	} else if (node->type==TYPE_DIRECTORY) {
		fsnodes_bst_add(bstrootdirs,node->trashtime);
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_gettrashtime_recursive(e->child,gmode,bstrootfiles,bstrootdirs);
			}
		}
	}
}

static inline void fsnodes_geteattr_recursive(fsnode *node,uint8_t gmode,uint32_t feattrtab[16],uint32_t deattrtab[16]) {
	fsedge *e;

	fsnodes_keep_alive_check();
	if (node->type!=TYPE_DIRECTORY) {
		feattrtab[(node->flags)&(EATTR_NOOWNER|EATTR_NOACACHE|EATTR_NODATACACHE)]++;
	} else {
		deattrtab[(node->flags)]++;
		if (gmode==GMODE_RECURSIVE) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_geteattr_recursive(e->child,gmode,feattrtab,deattrtab);
			}
		}
	}
}

static inline uint64_t fsnodes_setgoal_recursive_test_quota(fsnode *node,uint32_t uid,uint8_t goal,uint8_t recursive,uint64_t *realsize) {
	fsedge *e;
	uint32_t i,lastchunk,lastchunksize;
	uint64_t rs,size;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_DIRECTORY && recursive) {
		rs = 0;
		for (e = node->data.ddata.children ; e ; e=e->nextchild) {
			if (fsnodes_setgoal_recursive_test_quota(e->child,uid,goal,2,&rs)) {
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
		if (!((node->flags&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid)) {
			if (goal>node->goal) {
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
				rs = size * (goal - node->goal);
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

static inline void fsnodes_setgoal_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t set;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_DIRECTORY || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		if ((node->flags&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid) {
			(*nsinodes)++;
		} else {
			set=0;
			switch (smode&SMODE_TMASK) {
			case SMODE_SET:
				if (node->goal!=goal) {
					set=1;
				}
				break;
			case SMODE_INCREASE:
				if (node->goal<goal) {
					set=1;
				}
				break;
			case SMODE_DECREASE:
				if (node->goal>goal) {
					set=1;
				}
				break;
			}
			if (set) {
				if (node->type!=TYPE_DIRECTORY) {
					fsnodes_changefilegoal(node,goal);
					(*sinodes)++;
				} else {
					node->goal=goal;
					(*sinodes)++;
				}
				node->ctime = ts;
			} else {
				(*ncinodes)++;
			}
		}
		if (node->type==TYPE_DIRECTORY && (smode&SMODE_RMASK)) {
			for (e = node->data.ddata.children ; e ; e=e->nextchild) {
				fsnodes_setgoal_recursive(e->child,ts,uid,goal,smode,sinodes,ncinodes,nsinodes);
			}
		}
	}
}

static inline void fsnodes_settrashtime_recursive(fsnode *node,uint32_t ts,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsedge *e;
	uint8_t set;

	fsnodes_keep_alive_check();
	if (node->type==TYPE_FILE || node->type==TYPE_DIRECTORY || node->type==TYPE_TRASH || node->type==TYPE_SUSTAINED) {
		if ((node->flags&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid) {
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
	if ((node->flags&EATTR_NOOWNER)==0 && uid!=0 && node->uid!=uid) {
		(*nsinodes)++;
	} else {
		seattr = eattr;
		if (node->type!=TYPE_DIRECTORY) {
			node->flags &= ~(EATTR_NOECACHE);
			seattr &= ~(EATTR_NOECACHE);
		}
		neweattr = node->flags;
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
		if (neweattr!=node->flags) {
			node->flags = neweattr;
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

static inline void fsnodes_snapshot(uint32_t ts,fsnode *srcnode,fsnode *parentnode,uint32_t nleng,const uint8_t *name,uint8_t smode,uint8_t sesflags,uint32_t uid,uint32_t gids,uint32_t *gid,uint16_t cumask,uint8_t mr) {
	fsedge *e;
	fsnode *dstnode;
	uint32_t i;
	uint64_t chunkid;
	uint8_t rec,accessstatus;

	fsnodes_keep_alive_check();
	if (srcnode->type==TYPE_DIRECTORY) {
		rec = fsnodes_access_ext(srcnode,uid,gids,gid,MODE_MASK_R|MODE_MASK_X,sesflags);
		accessstatus = 1;
	} else if (srcnode->type==TYPE_FILE) {
		rec = 0;
		accessstatus = fsnodes_access_ext(srcnode,uid,gids,gid,MODE_MASK_R,sesflags);
	} else {
		rec = 0;
		accessstatus = 1;
	}
	if (accessstatus==0) {
		return;
	}
	if ((e=fsnodes_lookup(parentnode,nleng,name))) { // element already exists
		dstnode = e->child;
		if (srcnode->type==TYPE_DIRECTORY) {
			if (rec) {
				for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
					fsnodes_snapshot(ts,e->child,dstnode,e->nleng,e->name,smode,sesflags,uid,gids,gid,cumask,mr);
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
				fsnodes_unlink(ts,e);
				if (smode&SNAPSHOT_MODE_CPLIKE_ATTR) {
					dstnode = fsnodes_create_node(ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode,cumask,uid,gid[0],0);
				} else {
					if (uid==0 || uid==srcnode->uid) {
						dstnode = fsnodes_create_node(ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode&0xFFF,0,srcnode->uid,srcnode->gid,0);
					} else {
						dstnode = fsnodes_create_node(ts,parentnode,nleng,name,TYPE_FILE,srcnode->mode&0x3FF,0,uid,gid[0],0);
					}
				}
				fsnodes_get_stats(dstnode,&psr);
				dstnode->goal = srcnode->goal;
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
							if (chunk_add_file(chunkid,dstnode->goal)!=STATUS_OK) {
								syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,srcnode->id,i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
			}
		} else if (srcnode->type==TYPE_SYMLINK) {
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
			dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
		}
		if (smode&SNAPSHOT_MODE_CPLIKE_ATTR) {
			dstnode->uid = uid;
			dstnode->gid = gid[0];
			dstnode->mode = srcnode->mode & ~cumask;
			dstnode->ctime = ts;
		} else {
			if (uid==0 || uid==srcnode->uid) {
				dstnode->mode = srcnode->mode;
				dstnode->uid = srcnode->uid;
				dstnode->gid = srcnode->gid;
				dstnode->atime = srcnode->atime;
				dstnode->mtime = srcnode->mtime;
				dstnode->ctime = ts;
			} else {
				dstnode->mode = srcnode->mode & 0x3FF; // clear suid/sgid
				dstnode->uid = uid;
				dstnode->gid = gid[0];
				dstnode->atime = srcnode->atime;
				dstnode->mtime = srcnode->mtime;
				dstnode->ctime = ts;
			}
		}
	} else { // new element
		if (srcnode->type==TYPE_FILE || srcnode->type==TYPE_DIRECTORY || srcnode->type==TYPE_SYMLINK || srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV || srcnode->type==TYPE_SOCKET || srcnode->type==TYPE_FIFO) {
			statsrecord psr,nsr;
			if (smode&SNAPSHOT_MODE_CPLIKE_ATTR) {
				dstnode = fsnodes_create_node(ts,parentnode,nleng,name,srcnode->type,srcnode->mode,cumask,uid,gid[0],0);
			} else {
				if (uid==0 || uid==srcnode->uid) {
					dstnode = fsnodes_create_node(ts,parentnode,nleng,name,srcnode->type,srcnode->mode,0,srcnode->uid,srcnode->gid,0);
				} else {
					dstnode = fsnodes_create_node(ts,parentnode,nleng,name,srcnode->type,srcnode->mode&0x3FF,0,uid,gid[0],0);
				}
			}
			fsnodes_get_stats(dstnode,&psr);
			if ((smode&SNAPSHOT_MODE_CPLIKE_ATTR)==0) {
				dstnode->goal = srcnode->goal;
				dstnode->trashtime = srcnode->trashtime;
				dstnode->flags = srcnode->flags;
				dstnode->mode = srcnode->mode;
				if (uid!=0 && uid!=srcnode->uid) {
					dstnode->mode &= 0x3FF; // clear suid+sgid
				}
				dstnode->atime = srcnode->atime;
				dstnode->mtime = srcnode->mtime;
				if (srcnode->xattrflag) {
					dstnode->xattrflag = xattr_copy(srcnode->id,dstnode->id);
				}
			}
			if (srcnode->type==TYPE_DIRECTORY) {
				if (rec) {
					for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
						fsnodes_snapshot(ts,e->child,dstnode,e->nleng,e->name,smode,sesflags,uid,gids,gid,cumask,mr);
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
							if (chunk_add_file(chunkid,dstnode->goal)!=STATUS_OK) {
								syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,srcnode->id,i);
							}
						}
					}
				} else {
					dstnode->data.fdata.chunktab = NULL;
					dstnode->data.fdata.chunks = 0;
				}
				dstnode->data.fdata.length = srcnode->data.fdata.length;
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
			} else if (srcnode->type==TYPE_SYMLINK) {
				if (srcnode->data.sdata.pleng>0) {
					dstnode->data.sdata.path = symlink_malloc(srcnode->data.sdata.pleng);
					passert(dstnode->data.sdata.path);
					memcpy(dstnode->data.sdata.path,srcnode->data.sdata.path,srcnode->data.sdata.pleng);
					dstnode->data.sdata.pleng = srcnode->data.sdata.pleng;
				}
				fsnodes_get_stats(dstnode,&nsr);
				fsnodes_add_sub_stats(parentnode,&nsr,&psr);
			} else if (srcnode->type==TYPE_BLOCKDEV || srcnode->type==TYPE_CHARDEV) {
				dstnode->data.devdata.rdev = srcnode->data.devdata.rdev;
			}
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
			return ERROR_EINVAL;
		}
		if (dstnode->type!=srcnode->type) {
			return ERROR_EPERM;
		}
		if (srcnode->type==TYPE_TRASH || srcnode->type==TYPE_SUSTAINED) {
			return ERROR_EPERM;
		}
		if (srcnode->type==TYPE_DIRECTORY) {
			for (e = srcnode->data.ddata.children ; e ; e=e->nextchild) {
				status = fsnodes_snapshot_test(origsrcnode,e->child,dstnode,e->nleng,e->name,canoverwrite);
				if (status!=STATUS_OK) {
					return status;
				}
			}
		} else if (canoverwrite==0) {
			return ERROR_EEXIST;
		}
	}
	return STATUS_OK;
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
			(*realsize) += fsize * dstnode->goal;
		} else if (dstnode->type==TYPE_SYMLINK) {
			(*length) += dstnode->data.sdata.pleng;
		} else if (dstnode->type==TYPE_DIRECTORY) {
			uint32_t common_inodes;
			uint64_t common_length,common_size,common_realsize;
			statsrecord ssr;

			fsnodes_get_stats(srcnode,&ssr);
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

/* master <-> fuse operations */

uint8_t fs_mr_access(uint32_t ts,uint32_t inode) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	p->atime = ts;
	meta_version_inc();
	return STATUS_OK;
}

uint8_t fs_readsustained_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize) {
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(sustained);
	return STATUS_OK;
}

void fs_readsustained_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(sustained,dbuff);
}


uint8_t fs_readtrash_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize) {
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	*dbuffsize = fsnodes_getdetachedsize(trash);
	return STATUS_OK;
}

void fs_readtrash_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff) {
	(void)rootinode;
	(void)sesflags;
	fsnodes_getdetacheddata(trash,dbuff);
}

/* common procedure for trash and sustained files */
uint8_t fs_getdetachedattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t attr[35],uint8_t dtype) {
	fsnode *p;
	memset(attr,0,35);
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	if (!DTYPE_ISVALID(dtype)) {
		return ERROR_EINVAL;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_ENOENT;
	}
	if (dtype==DTYPE_TRASH && p->type==TYPE_SUSTAINED) {
		return ERROR_ENOENT;
	}
	if (dtype==DTYPE_SUSTAINED && p->type==TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	fsnodes_fill_attr(p,NULL,p->uid,p->gid,p->uid,p->gid,sesflags,attr);
	return STATUS_OK;
}

uint8_t fs_gettrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,const uint8_t **path) {
	fsnode *p;
	*pleng = 0;
	*path = NULL;
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	(void)sesflags;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	*pleng = p->parents->nleng;
	*path = p->parents->name;
	return STATUS_OK;
}

uint8_t fs_univ_setpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t pleng,const uint8_t *path) {
	fsnode *p;
	fsedge *e;
	uint32_t i;
	if (rootinode!=0) {
		return ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (pleng==0 || pleng>MFS_PATH_MAX) {
		return ERROR_EINVAL;
	}
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {
			return ERROR_EINVAL;
		}
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	fsnodes_remove_edge(0,p->parents);
	e = fsedge_malloc(pleng);
	passert(e);
	if (nextedgeid<EDGEID_MAX) {
		e->edgeid = nextedgeid--;
	} else {
		e->edgeid = 0;
	}
	e->nleng = pleng;
	memcpy((uint8_t*)(e->name),path,pleng);
	e->child = p;
	e->parent = NULL;
	e->nextchild = trash;
	e->nextparent = NULL;
	e->prevchild = &trash;
	e->prevparent = &(p->parents);
	if (e->nextchild) {
		e->nextchild->prevchild = &(e->nextchild);
	}
	trash = e;
	p->parents = e;

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETPATH(%"PRIu32",%s)",(uint32_t)main_time(),inode,changelog_escape_name(pleng,path));
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
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
		return ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	status = fsnodes_undel(ts,p);
	if (status!=STATUS_OK) {
		return status;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|UNDEL(%"PRIu32")",ts,inode);
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
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
		return ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_TRASH) {
		return ERROR_ENOENT;
	}
	fsnodes_purge(ts,p);
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|PURGE(%"PRIu32")",ts,inode);
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
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
		return ERROR_ENOENT;
	}

	*cnt = fsnodes_nlink(rootinode,p);
	return STATUS_OK;
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
		*psize = 0;
		return ERROR_ENOENT;
	}

	if (p->type==TYPE_TRASH) {
		*psize = 7+4+3+p->parents->nleng;
	} else if (p->type==TYPE_SUSTAINED) {
		*psize = 11+4+3+p->parents->nleng;
	} else {
		*psize = fsnodes_get_paths_size(rootinode,p);
	}
	return STATUS_OK;
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
	}
}

void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes) {
	matocsserv_getspace(totalspace,availspace);
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
			*rootinode = p->id;
			return STATUS_OK;
		}
		nleng=0;
		while (name[nleng] && name[nleng]!='/') {
			nleng++;
		}
		if (fsnodes_namecheck(nleng,name)<0) {
			return ERROR_EINVAL;
		}
		e = fsnodes_lookup(p,nleng,name);
		if (!e) {
			return ERROR_ENOENT;
		}
		p = e->child;
		if (p->type!=TYPE_DIRECTORY) {
			return ERROR_ENOTDIR;
		}
		name += nleng;
	}
}

void fs_statfs(uint32_t rootinode,uint8_t sesflags,uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint64_t *respace,uint32_t *inodes) {
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
		*inodes = 0;
	} else {
		matocsserv_getspace(totalspace,availspace);
		fsnodes_quota_fixspace(rn,totalspace,availspace);
		fsnodes_get_stats(rn,&sr);
		*inodes = sr.inodes;
	}
	stats_statfs++;
}

uint8_t fs_access(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,int modemask) {
	fsnode *p;
	if ((sesflags&SESFLAG_READONLY) && (modemask&MODE_MASK_W)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	return fsnodes_access_ext(p,uid,gids,gid,modemask,sesflags)?STATUS_OK:ERROR_EACCES;
}

uint8_t fs_lookup(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	fsnode *wd,*rn;
	fsedge *e;

	*inode = 0;
	memset(attr,0,35);

	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,&rn,&wd,0)==0) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_X,sesflags)) {
		return ERROR_EACCES;
	}
	if (name[0]=='.') {
		if (nleng==1) {	// self
			if (parent==rootinode) {
				*inode = MFS_ROOT_ID;
			} else {
				*inode = wd->id;
			}
			fsnodes_fill_attr(wd,wd,uid,gid[0],auid,agid,sesflags,attr);
			stats_lookup++;
			return STATUS_OK;
		}
		if (nleng==2 && name[1]=='.') {	// parent
			if (parent==rootinode) {
				*inode = MFS_ROOT_ID;
				fsnodes_fill_attr(wd,wd,uid,gid[0],auid,agid,sesflags,attr);
			} else {
				if (wd->parents) {
					if (wd->parents->parent->id==rootinode) {
						*inode = MFS_ROOT_ID;
					} else {
						*inode = wd->parents->parent->id;
					}
					fsnodes_fill_attr(wd->parents->parent,wd,uid,gid[0],auid,agid,sesflags,attr);
				} else {
					*inode=MFS_ROOT_ID; // rn->id;
					fsnodes_fill_attr(rn,wd,uid,gid[0],auid,agid,sesflags,attr);
				}
			}
			stats_lookup++;
			return STATUS_OK;
		}
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
	*inode = e->child->id;
	fsnodes_fill_attr(e->child,wd,uid,gid[0],auid,agid,sesflags,attr);
	stats_lookup++;
	return STATUS_OK;
}

uint8_t fs_getattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t attr[35]) {
	fsnode *p;

	(void)sesflags;
	memset(attr,0,35);
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return ERROR_ENOENT;
	}
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	stats_getattr++;
	return STATUS_OK;
}

uint8_t fs_try_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[35],uint64_t *chunkid) {
	fsnode *p;
	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return ERROR_ENOENT;
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
			return ERROR_EACCES;
		}
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
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
		if (fsnodes_test_quota(p,0,length-p->data.fdata.length,size_diff,p->goal*size_diff)) {
			return ERROR_QUOTA;
		}
	}
	if (length&MFSCHUNKMASK) {
		uint32_t indx = (length>>MFSCHUNKBITS);
		if (indx<p->data.fdata.chunks) {
			uint64_t ochunkid = p->data.fdata.chunktab[indx];
			if (ochunkid>0) {
				uint8_t status;
				uint64_t nchunkid;
				status = chunk_multi_truncate(&nchunkid,ochunkid,length&MFSCHUNKMASK,p->goal);
				if (status!=STATUS_OK) {
					return status;
				}
				p->data.fdata.chunktab[indx] = nchunkid;
				*chunkid = nchunkid;
				changelog("%"PRIu32"|TRUNC(%"PRIu32",%"PRIu32"):%"PRIu64,(uint32_t)main_time(),inode,indx,nchunkid);
				return ERROR_DELAYED;
			}
		}
	}
	fsnodes_fill_attr(p,NULL,uid,gid[0],auid,agid,sesflags,attr);
	stats_setattr++;
	return STATUS_OK;
}

uint8_t fs_mr_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t nchunkid) {
	uint64_t ochunkid;
	uint8_t status;
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EINVAL;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	if (indx>=p->data.fdata.chunks) {
		return ERROR_EINVAL;
	}
	ochunkid = p->data.fdata.chunktab[indx];
	status = chunk_mr_multi_truncate(ts,&nchunkid,ochunkid,p->goal);
	if (status!=STATUS_OK) {
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	meta_version_inc();
	return STATUS_OK;
}

uint8_t fs_end_setlength(uint64_t chunkid) {
	changelog("%"PRIu32"|UNLOCK(%"PRIu64")",(uint32_t)main_time(),chunkid);
	return chunk_unlock(chunkid);
}

uint8_t fs_mr_unlock(uint64_t chunkid) {
	meta_version_inc();
	return chunk_mr_unlock(chunkid);
}

uint8_t fs_do_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[35]) {
	fsnode *p;
	uint32_t ts = main_time();

	memset(attr,0,35);
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	fsnodes_setlength(p,length);
	changelog("%"PRIu32"|LENGTH(%"PRIu32",%"PRIu64")",ts,inode,p->data.fdata.length);
	p->ctime = p->mtime = ts;
	fsnodes_fill_attr(p,NULL,uid,gid,auid,agid,sesflags,attr);
	stats_setattr++;
	return STATUS_OK;
}


uint8_t fs_setattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t sugidclearmode,uint8_t attr[35]) {
	fsnode *p;
	uint8_t gf;
	uint32_t i;
	uint32_t ts = main_time();

	memset(attr,0,35);
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return ERROR_ENOENT;
	}
	if (uid!=0 && (sesflags&SESFLAG_MAPALL) && (setmask&(SET_UID_FLAG|SET_GID_FLAG))) {
		return ERROR_EPERM;
	}
	if ((p->flags&EATTR_NOOWNER)==0) {
		if (uid!=0 && uid!=p->uid && (setmask&(SET_MODE_FLAG|SET_UID_FLAG|SET_GID_FLAG|SET_ATIME_FLAG|SET_MTIME_FLAG))) {
			return ERROR_EPERM;
		}
	}
	if (uid!=0 && uid!=attruid && (setmask&SET_UID_FLAG)) {
		return ERROR_EPERM;
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
				return ERROR_EPERM;
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
			posix_acl_setmode(p->id,attrmode);
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
	changelog("%"PRIu32"|ATTR(%"PRIu32",%"PRIu16",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu32")",ts,inode,(uint16_t)(p->mode),p->uid,p->gid,p->atime,p->mtime);
	p->ctime = ts;
	fsnodes_fill_attr(p,NULL,uid,gid[0],auid,agid,sesflags,attr);
	stats_setattr++;
	return STATUS_OK;
}

uint8_t fs_mr_attr(uint32_t ts,uint32_t inode,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (mode>07777) {
		return ERROR_EINVAL;
	}
	p->mode = mode;
	if (p->aclpermflag) {
		posix_acl_setmode(p->id,mode);
	}
	p->uid = uid;
	p->gid = gid;
	p->atime = atime;
	p->mtime = mtime;
	p->ctime = ts;
	meta_version_inc();
	return STATUS_OK;
}

uint8_t fs_mr_length(uint32_t ts,uint32_t inode,uint64_t length) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EINVAL;
	}
	fsnodes_setlength(p,length);
	p->mtime = ts;
	p->ctime = ts;
	meta_version_inc();
	return STATUS_OK;
}

uint8_t fs_readlink(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path) {
	fsnode *p;
	uint32_t ts = main_time();

	(void)sesflags;
	*pleng = 0;
	*path = NULL;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_SYMLINK) {
		return ERROR_EINVAL;
	}
	*pleng = p->data.sdata.pleng;
	*path = p->data.sdata.path;
	if (p->atime!=ts) {
		p->atime = ts;
		changelog("%"PRIu32"|ACCESS(%"PRIu32")",ts,inode);
	}
	stats_readlink++;
	return STATUS_OK;
}

uint8_t fs_univ_symlink(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	fsnode *wd,*p;
	uint8_t *newpath;
	statsrecord sr;
	uint32_t i;
	*inode = 0;
	if (attr) {
		memset(attr,0,35);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (pleng==0 || pleng>MFS_SYMLINK_MAX) {
		return ERROR_EINVAL;
	}
	for (i=0 ; i<pleng ; i++) {
		if (path[i]==0) {
			return ERROR_EINVAL;
		}
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,NULL,&wd,0)==0) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && fsnodes_test_quota(wd,1,pleng,0,0)) {
		return ERROR_QUOTA;
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

	*inode = p->id;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (attr) {
			fsnodes_fill_attr(p,wd,uid,gid[0],auid,agid,sesflags,attr);
		}
		changelog("%"PRIu32"|SYMLINK(%"PRIu32",%s,%s,%"PRIu32",%"PRIu32"):%"PRIu32,(uint32_t)main_time(),parent,changelog_escape_name(nleng,name),changelog_escape_name(pleng,newpath),uid,gid[0],p->id);
	} else {
		meta_version_inc();
	}
	stats_symlink++;
	return STATUS_OK;
}

uint8_t fs_symlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	return fs_univ_symlink(main_time(),rootinode,sesflags,parent,nleng,name,pleng,path,uid,gids,gid,auid,agid,inode,attr);
}

uint8_t fs_mr_symlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_symlink(ts,0,SESFLAG_METARESTORE,parent,nleng,name,strlen((char*)path),path,uid,1,&gid,0,0,&rinode,NULL);
	if (status!=STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_univ_create(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t rdev,uint8_t copysgid,uint32_t *inode,uint8_t attr[35]) {
	fsnode *wd,*p;
	*inode = 0;
	if (attr) {
		memset(attr,0,35);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (type!=TYPE_FILE && type!=TYPE_SOCKET && type!=TYPE_FIFO && type!=TYPE_BLOCKDEV && type!=TYPE_CHARDEV && type!=TYPE_DIRECTORY) {
		return ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,NULL,&wd,0)==0) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(wd,nleng,name)) {
		return ERROR_EEXIST;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && fsnodes_test_quota(wd,1,0,0,0)) {
		return ERROR_QUOTA;
	}
	p = fsnodes_create_node(ts,wd,nleng,name,type,mode,cumask,uid,gid[0],copysgid);
	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		p->data.devdata.rdev = rdev;
	}
	*inode = p->id;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (attr) {
			fsnodes_fill_attr(p,wd,uid,gid[0],auid,agid,sesflags,attr);
		}
		changelog("%"PRIu32"|CREATE(%"PRIu32",%s,%"PRIu8",%"PRIu16",%"PRIu16",%"PRIu32",%"PRIu32",%"PRIu32"):%"PRIu32,(uint32_t)main_time(),parent,changelog_escape_name(nleng,name),type,mode,cumask,uid,gid[0],rdev,p->id);
	} else {
		meta_version_inc();
	}
	if (type==TYPE_DIRECTORY) {
		stats_mkdir++;
	} else {
		stats_mknod++;
	}
	return STATUS_OK;
}

uint8_t fs_mknod(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]) {
	if (type>15) {
		type = fsnodes_type_convert(type);
	}
	return fs_univ_create(main_time(),rootinode,sesflags,parent,nleng,name,type,mode,cumask,uid,gids,gid,auid,agid,rdev,0,inode,attr);
}

uint8_t fs_mkdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t copysgid,uint32_t *inode,uint8_t attr[35]) {
	return fs_univ_create(main_time(),rootinode,sesflags,parent,nleng,name,TYPE_DIRECTORY,mode,cumask,uid,gids,gid,auid,agid,0,copysgid,inode,attr);
}

uint8_t fs_mr_create(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	if (type>15) {
		type = fsnodes_type_convert(type);
	}
	status = fs_univ_create(ts,0,SESFLAG_METARESTORE,parent,nleng,name,type,mode,cumask,uid,1,&gid,0,0,rdev,0,&rinode,NULL);
	if (status!=STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_univ_unlink(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t dirmode,uint32_t *inode) {
	fsnode *wd;
	fsedge *e;
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent,NULL,&wd,0)==0) {
		return ERROR_ENOENT;
	}
	if (wd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(wd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng,name)<0) {
		return ERROR_EINVAL;
	}
	e = fsnodes_lookup(wd,nleng,name);
	if (!e) {
		return ERROR_ENOENT;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_sticky_access(wd,e->child,uid))) {
		return ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (dirmode==0) {
			if (e->child->type==TYPE_DIRECTORY) {
				return ERROR_EPERM;
			}
		} else {
			if (e->child->type!=TYPE_DIRECTORY) {
				return ERROR_ENOTDIR;
			}
			if (e->child->data.ddata.children!=NULL) {
				return ERROR_ENOTEMPTY;
			}
		}
	} else {
		if (e->child->type==TYPE_DIRECTORY) {
			dirmode = 1;
			if (e->child->data.ddata.children!=NULL) {
				return ERROR_ENOTEMPTY;
			}
		} else {
			dirmode = 0;
		}
	}
	if (inode) {
		*inode = e->child->id;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|UNLINK(%"PRIu32",%s):%"PRIu32,ts,parent,changelog_escape_name(nleng,name),e->child->id);
	} else {
		meta_version_inc();
	}
	fsnodes_unlink(ts,e);
	if (dirmode==0) {
		stats_unlink++;
	} else {
		stats_rmdir++;
	}
	return STATUS_OK;
}

uint8_t fs_unlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid) {
	return fs_univ_unlink(main_time(),rootinode,sesflags,parent,nleng,name,uid,gids,gid,0,NULL);
}

uint8_t fs_rmdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid) {
	return fs_univ_unlink(main_time(),rootinode,sesflags,parent,nleng,name,uid,gids,gid,1,NULL);
}

uint8_t fs_mr_unlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_unlink(ts,0,SESFLAG_METARESTORE,parent,nleng,name,0,0,0,0,&rinode);
	if (status!=STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_univ_move(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t parent_src,uint16_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	fsnode *swd;
	fsedge *se;
	fsnode *dwd;
	fsedge *de;
	fsnode *node;
	statsrecord ssr,dsr;
	*inode = 0;
	if (attr) {
		memset(attr,0,35);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&parent_src,NULL,&swd,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&parent_dst,NULL,&dwd,0)==0) {
		return ERROR_ENOENT;
	}
	if (swd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(swd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng_src,name_src)<0) {
		return ERROR_EINVAL;
	}
	se = fsnodes_lookup(swd,nleng_src,name_src);
	if (!se) {
		return ERROR_ENOENT;
	}
	node = se->child;
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_sticky_access(swd,node,uid))) {
		return ERROR_EPERM;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(dwd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return ERROR_EACCES;
	}
	if (se->child->type==TYPE_DIRECTORY) {
		if (fsnodes_isancestor(se->child,dwd)) {
			return ERROR_EINVAL;
		}
	}
	if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
		return ERROR_EINVAL;
	}
	de = fsnodes_lookup(dwd,nleng_dst,name_dst);
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		fsnodes_get_stats(node,&ssr);
		if (de) {
			fsnodes_get_stats(de->child,&dsr);
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
		if (fsnodes_test_quota(dwd,ssr.inodes,ssr.length,ssr.size,ssr.realsize)) {
			return ERROR_QUOTA;
		}
	}
	if (de) {
		if (de->child->type==TYPE_DIRECTORY && de->child->data.ddata.children!=NULL) {
			return ERROR_ENOTEMPTY;
		}
		if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_sticky_access(dwd,de->child,uid))) {
			return ERROR_EPERM;
		}
		fsnodes_unlink(ts,de);
	}
	fsnodes_remove_edge(ts,se);
	fsnodes_link(ts,dwd,node,nleng_dst,name_dst);
	*inode = node->id;
	if (attr) {
		fsnodes_fill_attr(node,dwd,uid,gid[0],auid,agid,sesflags,attr);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|MOVE(%"PRIu32",%s,%"PRIu32",%s):%"PRIu32,ts,parent_src,changelog_escape_name(nleng_src,name_src),parent_dst,changelog_escape_name(nleng_dst,name_dst),node->id);
	} else {
		meta_version_inc();
	}
	stats_rename++;
	return STATUS_OK;
}

uint8_t fs_rename(uint32_t rootinode,uint8_t sesflags,uint32_t parent_src,uint16_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	return fs_univ_move(main_time(),rootinode,sesflags,parent_src,nleng_src,name_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,inode,attr);
}

uint8_t fs_mr_move(uint32_t ts,uint32_t parent_src,uint32_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint32_t nleng_dst,const uint8_t *name_dst,uint32_t inode) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_move(ts,0,SESFLAG_METARESTORE,parent_src,nleng_src,name_src,parent_dst,nleng_dst,name_dst,0,0,0,0,0,&rinode,NULL);
	if (status!=STATUS_OK) {
		return status;
	}
	if (rinode!=inode) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_univ_link(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	statsrecord sr;
	fsnode *sp;
	fsnode *dwd;
	*inode = 0;
	if (attr) {
		memset(attr,0,35);
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode_src,NULL,&sp,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&parent_dst,NULL,&dwd,0)==0) {
		return ERROR_ENOENT;
	}
	if (sp->type==TYPE_TRASH || sp->type==TYPE_SUSTAINED) {
		return ERROR_ENOENT;
	}
	if (sp->type==TYPE_DIRECTORY) {
		return ERROR_EPERM;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_ENOTDIR;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(dwd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return ERROR_EACCES;
	}
	if (fsnodes_namecheck(nleng_dst,name_dst)<0) {
		return ERROR_EINVAL;
	}
	if (fsnodes_nameisused(dwd,nleng_dst,name_dst)) {
		return ERROR_EEXIST;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		fsnodes_get_stats(sp,&sr);
		if (fsnodes_test_quota(dwd,sr.inodes,sr.length,sr.size,sr.realsize)) {
			return ERROR_QUOTA;
		}
	}

	fsnodes_link(ts,dwd,sp,nleng_dst,name_dst);
	*inode = inode_src;
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (attr) {
			fsnodes_fill_attr(sp,dwd,uid,gid[0],auid,agid,sesflags,attr);
		}
		changelog("%"PRIu32"|LINK(%"PRIu32",%"PRIu32",%s)",ts,inode_src,parent_dst,changelog_escape_name(nleng_dst,name_dst));
	} else {
		meta_version_inc();
	}
	stats_link++;
	return STATUS_OK;
}

uint8_t fs_link(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]) {
	return fs_univ_link(main_time(),rootinode,sesflags,inode_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,inode,attr);
}

uint8_t fs_mr_link(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint32_t nleng_dst,uint8_t *name_dst) {
	uint32_t rinode;
	uint8_t status;
	status = fs_univ_link(ts,0,SESFLAG_METARESTORE,inode_src,parent_dst,nleng_dst,name_dst,0,0,NULL,0,0,&rinode,NULL);
	if (status!=STATUS_OK) {
		return status;
	}
	if (rinode!=inode_src) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_univ_snapshot(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t smode,uint16_t cumask) {
	statsrecord ssr;
	uint32_t common_inodes;
	uint64_t common_length;
	uint64_t common_size;
	uint64_t common_realsize;
	fsnode *sp;
	fsnode *dwd;
	uint8_t status;
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode_src,NULL,&sp,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&parent_dst,NULL,&dwd,0)==0) {
		return ERROR_ENOENT;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(sp,uid,gids,gid,MODE_MASK_R,sesflags))) {
		return ERROR_EACCES;
	}
	if (dwd->type!=TYPE_DIRECTORY) {
		return ERROR_EPERM;
	}
	if (sp->type==TYPE_DIRECTORY) {
		if (sp==dwd || fsnodes_isancestor(sp,dwd)) {
			return ERROR_EINVAL;
		}
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(dwd,uid,gids,gid,MODE_MASK_W|MODE_MASK_X,sesflags))) {
		return ERROR_EACCES;
	}

	fsnodes_keep_alive_begin();
	status = fsnodes_snapshot_test(sp,sp,dwd,nleng_dst,name_dst,smode & SNAPSHOT_MODE_CAN_OVERWRITE);
	if (status!=STATUS_OK) {
		return status;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		fsnodes_get_stats(sp,&ssr);
		common_inodes = 0;
		common_length = 0;
		common_size = 0;
		common_realsize = 0;
		if (fsnodes_snapshot_recursive_test_quota(sp,dwd,nleng_dst,name_dst,&common_inodes,&common_length,&common_size,&common_realsize)) {
			return ERROR_QUOTA;
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
			return ERROR_QUOTA;
		}
	}

	fsnodes_snapshot(ts,sp,dwd,nleng_dst,name_dst,smode,sesflags,uid,gids,gid,cumask,((sesflags&SESFLAG_METARESTORE)==0)?0:1);

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SNAPSHOT(%"PRIu32",%"PRIu32",%s,%"PRIu8",%"PRIu8",%"PRIu32",%"PRIu32",%s,%"PRIu16")",ts,inode_src,parent_dst,changelog_escape_name(nleng_dst,name_dst),smode,sesflags,uid,gids,changelog_escape_name(gids*4,(const uint8_t*)gid),cumask);
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
}

uint8_t fs_snapshot(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t smode,uint16_t cumask) {
	return fs_univ_snapshot(main_time(),rootinode,sesflags,inode_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,smode,cumask);
}

uint8_t fs_mr_snapshot(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint8_t smode,uint8_t sesflags,uint32_t uid,uint32_t gids,uint32_t *gid,uint16_t cumask) {
	return fs_univ_snapshot(ts,0,sesflags|SESFLAG_METARESTORE,inode_src,parent_dst,nleng_dst,name_dst,uid,gids,gid,smode,cumask);
}

uint8_t fs_univ_append(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t inode_src,uint32_t uid,uint32_t gids,uint32_t *gid) {
	uint32_t pchunks,lastchunk,lastchunksize,i;
	uint64_t newlength,lengdiff,sizediff;
	uint8_t status;
	fsnode *p,*sp;
	if (inode==inode_src) {
		return ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode_src,NULL,&sp,0)==0 || fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (sp->type!=TYPE_FILE && sp->type!=TYPE_TRASH && sp->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(sp,uid,gids,gid,MODE_MASK_R,sesflags))) {
		return ERROR_EACCES;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags))) {
		return ERROR_EACCES;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		// quota
		pchunks = p->data.fdata.chunks;
		while (pchunks>0 && p->data.fdata.chunktab[pchunks-1]==0) {
			pchunks--;
		}
		newlength = (((uint64_t)pchunks)<<MFSCHUNKBITS)+sp->data.fdata.length;
		if (newlength>p->data.fdata.length) {
			lengdiff = newlength - p->data.fdata.length;
		} else {
			lengdiff = 0;
		}
		sizediff = 0;
		if (p->data.fdata.length>0) {
			lastchunk = (p->data.fdata.length-1)>>MFSCHUNKBITS;
			lastchunksize = ((((p->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastchunk = 0;
			lastchunksize = MFSHDRSIZE;
		}
		for (i=0 ; i<p->data.fdata.chunks ; i++) {
			if (p->data.fdata.chunktab[i]>0) {
				if (i>lastchunk) {
					sizediff += MFSCHUNKSIZE+MFSHDRSIZE;
				} else if (i==lastchunk) {
					sizediff += (MFSCHUNKSIZE+MFSHDRSIZE) - lastchunksize;
				}
			}
		}
		if (sp->data.fdata.length>0) {
			lastchunk = (sp->data.fdata.length-1)>>MFSCHUNKBITS;
			lastchunksize = ((((sp->data.fdata.length-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
		} else {
			lastchunk = 0;
			lastchunksize = MFSHDRSIZE;
		}
		for (i=0 ; i<sp->data.fdata.chunks ; i++) {
			if (sp->data.fdata.chunktab[i]>0) {
				if (i<lastchunk) {
					sizediff += MFSCHUNKSIZE+MFSHDRSIZE;
				} else if (i==lastchunk) {
					sizediff += lastchunksize;
				}
			}
		}
		if (fsnodes_test_quota(p,0,lengdiff,sizediff,p->goal*sizediff)) {
			return ERROR_QUOTA;
		}
	}
	status = fsnodes_appendchunks(ts,p,sp);
	if (status!=STATUS_OK) {
		return status;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|APPEND(%"PRIu32",%"PRIu32")",ts,inode,inode_src);
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
}

uint8_t fs_append(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t inode_src,uint32_t uid,uint32_t gids,uint32_t *gid) {
	return fs_univ_append(main_time(),rootinode,sesflags,inode,inode_src,uid,gids,gid);
}

uint8_t fs_mr_append(uint32_t ts,uint32_t inode,uint32_t inode_src) {
	return fs_univ_append(ts,0,SESFLAG_METARESTORE,inode,inode_src,0,0,NULL);
}

uint8_t fs_readdir_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t flags,uint32_t maxentries,uint64_t nedgeid,void **dnode,void **dedge,uint32_t *dbuffsize) {
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
			return ERROR_ENOENT;
		}
		if (p->type!=TYPE_DIRECTORY) {
			return ERROR_ENOTDIR;
		}
		if (nedgeid==0) {
			if (flags&GETDIR_FLAG_WITHATTR) {
				if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R|MODE_MASK_X,sesflags)) {
					return ERROR_EACCES;
				}
			} else {
				if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
					return ERROR_EACCES;
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
	*dbuffsize = fsnodes_readdirsize(p,e,maxentries,nedgeid,flags&GETDIR_FLAG_WITHATTR);
	return STATUS_OK;
}

void fs_readdir_data(uint32_t rootinode,uint8_t sesflags,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,uint32_t maxentries,uint64_t *nedgeid,void *dnode,void *dedge,uint8_t *dbuff) {
	fsnode *p = (fsnode*)dnode;
	fsedge *e = (fsedge*)dedge;
	uint32_t ts = main_time();

	if (p->atime!=ts) {
		p->atime = ts;
		changelog("%"PRIu32"|ACCESS(%"PRIu32")",ts,p->id);
	}
	fsnodes_readdirdata(rootinode,uid,gid,auid,agid,sesflags,p,e,maxentries,nedgeid,dbuff,flags&GETDIR_FLAG_WITHATTR);
	stats_readdir++;
}

uint8_t fs_checkfile(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t chunkcount[11]) {
	fsnode *p;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	fsnodes_checkfile(p,chunkcount);
	return STATUS_OK;
}

uint8_t fs_opencheck(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t flags,uint8_t attr[35]) {
	fsnode *p;
	if ((sesflags&SESFLAG_READONLY) && (flags&WANT_WRITE)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
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
			return ERROR_EACCES;
		}
	}
	fsnodes_fill_attr(p,NULL,uid,gid[0],auid,agid,sesflags,attr);
	stats_open++;
	return STATUS_OK;
}

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length) {
	fsnode *p;
	uint32_t ts = main_time();

	*chunkid = 0;
	*length = 0;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	if (indx<p->data.fdata.chunks) {
		*chunkid = p->data.fdata.chunktab[indx];
	}
	*length = p->data.fdata.length;
	if (p->atime!=ts) {
		p->atime = ts;
		changelog("%"PRIu32"|ACCESS(%"PRIu32")",ts,inode);
	}
	stats_read++;
	return STATUS_OK;
}

uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length,uint8_t *opflag) {
	int status;
	uint32_t i;
	uint64_t ochunkid,nchunkid;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;
	uint32_t ts = main_time();
	uint32_t lastchunk,lastchunksize,sizediff;

	*chunkid = 0;
	*length = 0;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
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
		if (indx>=p->data.fdata.chunks || p->data.fdata.chunktab[indx]==0) {
			sizediff = MFSCHUNKSIZE+MFSHDRSIZE;
		} else {
			sizediff = 0;
		}
	} else if (indx==lastchunk) {
		if (indx>=p->data.fdata.chunks || p->data.fdata.chunktab[indx]==0) {
			sizediff = MFSCHUNKSIZE+MFSHDRSIZE;
		} else {
			sizediff = (MFSCHUNKSIZE+MFSHDRSIZE) - lastchunksize;
		}
	} else {
		sizediff = MFSCHUNKSIZE+MFSHDRSIZE;
	}
	if (fsnodes_test_quota(p,0,0/*lengdiff*/,sizediff,p->goal*sizediff)) {
		return ERROR_QUOTA;
	}
	fsnodes_get_stats(p,&psr);
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
	status = chunk_multi_modify(&nchunkid,ochunkid,p->goal,opflag);
	if (status!=STATUS_OK) {
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	fsnodes_get_stats(p,&nsr);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	*chunkid = nchunkid;
	*length = p->data.fdata.length;
	changelog("%"PRIu32"|WRITE(%"PRIu32",%"PRIu32",%"PRIu8"):%"PRIu64,ts,inode,indx,*opflag,nchunkid);
	if (p->mtime!=ts || p->ctime!=ts) {
		p->mtime = p->ctime = ts;
	}
	stats_write++;
	return STATUS_OK;
}

uint8_t fs_mr_write(uint32_t ts,uint32_t inode,uint32_t indx,uint8_t opflag,uint64_t nchunkid) {
	int status;
	uint32_t i;
	uint64_t ochunkid;
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;

	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	/* resize chunks structure */
	fsnodes_get_stats(p,&psr);
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
	status = chunk_mr_multi_modify(ts,&nchunkid,ochunkid,p->goal,opflag);
	if (status!=STATUS_OK) {
		return status;
	}
	p->data.fdata.chunktab[indx] = nchunkid;
	fsnodes_get_stats(p,&nsr);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	meta_version_inc();
	p->mtime = p->ctime = ts;
	return STATUS_OK;
}

uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid) {
	uint32_t ts = main_time();
	if (length>0) {
		fsnode *p;
		p = fsnodes_node_find(inode);
		if (!p) {
			return ERROR_ENOENT;
		}
		if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
			return ERROR_EPERM;
		}
		if (length>p->data.fdata.length) {
			if (fsnodes_test_quota(p,0,length-p->data.fdata.length,0,0)) {
				return ERROR_QUOTA;
			}
			fsnodes_setlength(p,length);
			p->mtime = p->ctime = ts;
			changelog("%"PRIu32"|LENGTH(%"PRIu32",%"PRIu64")",ts,inode,length);
		}
	}
	changelog("%"PRIu32"|UNLOCK(%"PRIu64")",ts,chunkid);
	return chunk_unlock(chunkid);
}

void fs_incversion(uint64_t chunkid) {
	changelog("%"PRIu32"|INCVERSION(%"PRIu64")",(uint32_t)main_time(),chunkid);
}

uint8_t fs_mr_incversion(uint64_t chunkid) {
	meta_version_inc();
	return chunk_mr_increase_version(chunkid);
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
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
		return ERROR_EACCES;
	}
	fsnodes_get_stats(p,&psr);
	for (indx=0 ; indx<p->data.fdata.chunks ; indx++) {
		if (chunk_repair(p->goal,p->data.fdata.chunktab[indx],&nversion)) {
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
	fsnodes_get_stats(p,&nsr);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	return STATUS_OK;
}

uint8_t fs_mr_repair(uint32_t ts,uint32_t inode,uint32_t indx,uint32_t nversion) {
	statsrecord psr,nsr;
	fsedge *e;
	fsnode *p;
	uint8_t status;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	if (indx>MAX_INDEX) {
		return ERROR_INDEXTOOBIG;
	}
	if (indx>=p->data.fdata.chunks) {
		return ERROR_NOCHUNK;
	}
	if (p->data.fdata.chunktab[indx]==0) {
		return ERROR_NOCHUNK;
	}
	fsnodes_get_stats(p,&psr);
	if (nversion==0) {
		status = chunk_delete_file(p->data.fdata.chunktab[indx],p->goal);
		p->data.fdata.chunktab[indx]=0;
	} else {
		status = chunk_mr_set_version(p->data.fdata.chunktab[indx],nversion);
	}
	fsnodes_get_stats(p,&nsr);
	for (e=p->parents ; e ; e=e->nextparent) {
		fsnodes_add_sub_stats(e->parent,&nsr,&psr);
	}
	meta_version_inc();
	p->mtime = p->ctime = ts;
	return status;
}

uint8_t fs_getgoal(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t fgtab[10],uint32_t dgtab[10]) {
	fsnode *p;
	(void)sesflags;
	memset(fgtab,0,10*sizeof(uint32_t));
	memset(dgtab,0,10*sizeof(uint32_t));
	if (!GMODE_ISVALID(gmode)) {
		return ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	fsnodes_keep_alive_begin();
	fsnodes_getgoal_recursive(p,gmode,fgtab,dgtab);
	return STATUS_OK;
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
		return ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	fsnodes_keep_alive_begin();
	fsnodes_gettrashtime_recursive(p,gmode,&froot,&droot);
	*fptr = froot;
	*dptr = droot;
	*fnodes = fsnodes_bst_nodes(froot);
	*dnodes = fsnodes_bst_nodes(droot);
	return STATUS_OK;
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

uint8_t fs_geteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t feattrtab[16],uint32_t deattrtab[16]) {
	fsnode *p;
	(void)sesflags;
	memset(feattrtab,0,16*sizeof(uint32_t));
	memset(deattrtab,0,16*sizeof(uint32_t));
	if (!GMODE_ISVALID(gmode)) {
		return ERROR_EINVAL;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	fsnodes_keep_alive_begin();
	fsnodes_geteattr_recursive(p,gmode,feattrtab,deattrtab);
	return STATUS_OK;
}

uint8_t fs_univ_setgoal(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	uint64_t realsize;
	fsnode *p;

	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;

	if (!SMODE_ISVALID(smode) || goal>9 || goal<1) {
		return ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}

	fsnodes_keep_alive_begin();
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		// quota
		if ((smode&SMODE_TMASK)==SMODE_SET || (smode&SMODE_TMASK)==SMODE_INCREASE) {
			realsize = 0;
			if (fsnodes_setgoal_recursive_test_quota(p,uid,goal,(smode&SMODE_RMASK)?1:0,&realsize)) {
				return ERROR_QUOTA;
			}
		}
	}

	fsnodes_setgoal_recursive(p,ts,uid,goal,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return ERROR_EPERM;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETGOAL(%"PRIu32",%"PRIu32",%"PRIu8",%"PRIu8"):%"PRIu32",%"PRIu32",%"PRIu32,ts,inode,uid,goal,smode,*sinodes,*ncinodes,*nsinodes);
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
}

uint8_t fs_setgoal(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	return fs_univ_setgoal(main_time(),rootinode,sesflags,inode,uid,goal,smode,sinodes,ncinodes,nsinodes);
}

uint8_t fs_mr_setgoal(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
	uint8_t status;
	status = fs_univ_setgoal(ts,0,SESFLAG_METARESTORE,inode,uid,goal,smode,&si,&nci,&nsi);
	if (status!=STATUS_OK) {
		return status;
	}
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_univ_settrashtime(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsnode *p;

	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
	if (!SMODE_ISVALID(smode)) {
		return ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}

	fsnodes_keep_alive_begin();
	fsnodes_settrashtime_recursive(p,ts,uid,trashtime,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return ERROR_EPERM;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETTRASHTIME(%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu8"):%"PRIu32",%"PRIu32",%"PRIu32,ts,inode,uid,trashtime,smode,*sinodes,*ncinodes,*nsinodes);
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
}

uint8_t fs_settrashtime(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	return fs_univ_settrashtime(main_time(),rootinode,sesflags,inode,uid,trashtime,smode,sinodes,ncinodes,nsinodes);
}

uint8_t fs_mr_settrashtime(uint32_t ts,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
	uint8_t status;
	status = fs_univ_settrashtime(ts,0,SESFLAG_METARESTORE,inode,uid,trashtime,smode,&si,&nci,&nsi);
	if (status!=STATUS_OK) {
		return status;
	}
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_univ_seteattr(uint32_t ts,uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	fsnode *p;

	*sinodes = 0;
	*ncinodes = 0;
	*nsinodes = 0;
	if (!SMODE_ISVALID(smode) || (eattr&(~(EATTR_NOOWNER|EATTR_NOACACHE|EATTR_NOECACHE|EATTR_NODATACACHE)))) {
		return ERROR_EINVAL;
	}
	if ((sesflags&SESFLAG_METARESTORE)==0 && (sesflags&SESFLAG_READONLY)) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}

	fsnodes_keep_alive_begin();
	fsnodes_seteattr_recursive(p,ts,uid,eattr,smode,sinodes,ncinodes,nsinodes);
	if ((smode&SMODE_RMASK)==0 && *nsinodes>0 && *sinodes==0 && *ncinodes==0) {
		return ERROR_EPERM;
	}

	if ((sesflags&SESFLAG_METARESTORE)==0) {
		changelog("%"PRIu32"|SETEATTR(%"PRIu32",%"PRIu32",%"PRIu8",%"PRIu8"):%"PRIu32",%"PRIu32",%"PRIu32,ts,inode,uid,eattr,smode,*sinodes,*ncinodes,*nsinodes);
	} else {
		meta_version_inc();
	}
	return STATUS_OK;
}

uint8_t fs_seteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes) {
	return fs_univ_seteattr(main_time(),rootinode,sesflags,inode,uid,eattr,smode,sinodes,ncinodes,nsinodes);
}

uint8_t fs_mr_seteattr(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes) {
	uint32_t si,nci,nsi;
	uint8_t status;
	status = fs_univ_seteattr(ts,0,SESFLAG_METARESTORE,inode,uid,eattr,smode,&si,&nci,&nsi);
	if (status!=STATUS_OK) {
		return status;
	}
	if (sinodes!=si || ncinodes!=nci || nsinodes!=nsi) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

uint8_t fs_listxattr_leng(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,void **xanode,uint32_t *xasize) {
	fsnode *p;

	*xasize = 0;
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return ERROR_ENOENT;
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
			return ERROR_EACCES;
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
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return ERROR_ENOENT;
	}
	if (uid!=0) {
		if ((anleng>=8 && memcmp(attrname,"trusted.",8)==0) || (anleng>=9 && memcmp(attrname,"security.",9)==0)) {
			return ERROR_EPERM;
		}
	}
	if ((p->flags&EATTR_NOOWNER)==0 && uid!=0 && uid!=p->uid) {
		if (anleng>=7 && memcmp(attrname,"system.",7)==0) {
			return ERROR_EPERM;
		}
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
			return ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng,attrname)<0) {
		return ERROR_EINVAL;
	}
	if (mode>MFS_XATTR_REMOVE) {
		return ERROR_EINVAL;
	}
	status = xattr_setattr(inode,anleng,attrname,avleng,attrvalue,mode);
	if (status!=STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	changelog("%"PRIu32"|SETXATTR(%"PRIu32",%s,%s,%"PRIu8")",ts,inode,changelog_escape_name(anleng,attrname),changelog_escape_name(avleng,attrvalue),mode);
	return STATUS_OK;
}

uint8_t fs_getxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue) {
	fsnode *p;

	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,opened)==0) {
		return ERROR_ENOENT;
	}
	if (uid!=0) {
		if (anleng>=8 && memcmp(attrname,"trusted.",8)==0) {
			return ERROR_EPERM;
		}
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
			return ERROR_EACCES;
		}
	}
	if (xattr_namecheck(anleng,attrname)<0) {
		return ERROR_EINVAL;
	}
	return xattr_getattr(inode,anleng,attrname,avleng,attrvalue);
}

uint8_t fs_mr_setxattr(uint32_t ts,uint32_t inode,uint32_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint32_t mode) {
	fsnode *p;
	uint8_t status;
	if (anleng==0 || anleng>MFS_XATTR_NAME_MAX || avleng>MFS_XATTR_SIZE_MAX || mode>MFS_XATTR_REMOVE) {
		return ERROR_EINVAL;
	}
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	status = xattr_setattr(inode,anleng,attrname,avleng,attrvalue,mode);

	if (status!=STATUS_OK) {
		return status;
	}
	p->ctime = ts;
	meta_version_inc();
	return status;
}

uint8_t fs_setacl(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob) {
	uint32_t ts;
	uint16_t pmode;
	fsnode *p;

	ts = main_time();
	if (sesflags&SESFLAG_READONLY) {
		return ERROR_EROFS;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if ((p->flags&EATTR_NOOWNER)==0 && uid!=0 && uid!=p->uid) {
		return ERROR_EPERM;
	}
//	if (opened==0) {
//		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_W,sesflags)) {
//			return ERROR_EACCES;
//		}
//	}
	if (acltype!=POSIX_ACL_ACCESS && acltype!=POSIX_ACL_DEFAULT) {
		return ERROR_EINVAL;
	}
	pmode = p->mode;
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
	changelog("%"PRIu32"|SETACL(%"PRIu32",%u,%u,%"PRIu8",%"PRIu16",%"PRIu16",%"PRIu16",%"PRIu16",%"PRIu16",%"PRIu16",%s)",ts,inode,p->mode,(p->ctime==ts)?1U:0U,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,changelog_escape_name((namedusers+namedgroups)*6,aclblob));
	return STATUS_OK;
}

uint8_t fs_getacl_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t acltype,void **custom,uint32_t *aclblobsize) {
	fsnode *p;
	int32_t bsize;

	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (opened==0) {
		if (!fsnodes_access_ext(p,uid,gids,gid,MODE_MASK_R,sesflags)) {
			return ERROR_EACCES;
		}
	}
	if ((acltype==POSIX_ACL_ACCESS && p->aclpermflag==0) || (acltype==POSIX_ACL_DEFAULT && p->acldefflag==0)) {
		return ERROR_ENOATTR;
	}
	bsize = posix_acl_get_blobsize(inode,acltype,custom);
	if (bsize<0) {
		return ERROR_ENOATTR;
	}
	*aclblobsize = bsize;
	return STATUS_OK;
}

void fs_getacl_data(void *custom,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *mask,uint16_t *namedusers,uint16_t *namedgroups,uint8_t *aclblob) {
	posix_acl_get_data(custom,userperm,groupperm,otherperm,mask,namedusers,namedgroups,aclblob);
}

uint8_t fs_mr_setacl(uint32_t ts,uint32_t inode,uint16_t mode,uint8_t changectime,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob) {
	fsnode *p;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (acltype!=POSIX_ACL_ACCESS && acltype!=POSIX_ACL_DEFAULT) {
		return ERROR_EINVAL;
	}
	posix_acl_set(inode,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,aclblob);
	p->mode = mode;
	if (changectime) {
		p->ctime = ts;
	}
	meta_version_inc();
	return STATUS_OK;
}

uint8_t fs_quotacontrol(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t delflag,uint8_t *flags,uint32_t *sinodes,uint64_t *slength,uint64_t *ssize,uint64_t *srealsize,uint32_t *hinodes,uint64_t *hlength,uint64_t *hsize,uint64_t *hrealsize,uint32_t *curinodes,uint64_t *curlength,uint64_t *cursize,uint64_t *currealsize) {
	fsnode *p;
	quotanode *qn;
	statsrecord *psr;
	uint8_t chg;

	if (*flags) {
		if (sesflags&SESFLAG_READONLY) {
			return ERROR_EROFS;
		}
		if ((sesflags&SESFLAG_ADMIN)==0) {
			return ERROR_EPERM;
		}
	}
	if (rootinode==0) {
		return ERROR_EPERM;
	}
	if (fsnodes_node_find_ext(rootinode,sesflags,&inode,NULL,&p,0)==0) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY) {
		return ERROR_EPERM;
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
			changelog("%"PRIu32"|QUOTA(%"PRIu32",%"PRIu8",%"PRIu8",%"PRIu32",%"PRIu32",%"PRIu32",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64")",main_time(),inode,qn->exceeded,qn->flags,qn->stimestamp,qn->sinodes,qn->hinodes,qn->slength,qn->hlength,qn->ssize,qn->hsize,qn->srealsize,qn->hrealsize);
		} else {
			changelog("%"PRIu32"|QUOTA(%"PRIu32",0,0,0,0,0,0,0,0,0,0,0)",main_time(),inode);
		}
	}
	return STATUS_OK;
}

uint8_t fs_mr_quota(uint32_t ts,uint32_t inode,uint8_t exceeded,uint8_t flags,uint32_t stimestamp,uint32_t sinodes,uint32_t hinodes,uint64_t slength,uint64_t hlength,uint64_t ssize,uint64_t hsize,uint64_t srealsize,uint64_t hrealsize) {
	fsnode *p;
	quotanode *qn;

	(void)ts;
	p = fsnodes_node_find(inode);
	if (!p) {
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY) {
		return ERROR_EPERM;
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
	return STATUS_OK;
}


uint32_t fs_getquotainfo_size() {
	quotanode *qn;
	uint32_t s=0,size;
//	s=4;	// QuotaTimeLimit
	for (qn=quotahead ; qn ; qn=qn->next) {
		size=fsnodes_getpath_size(qn->node->parents);
		s+=4+4+1+1+4+3*(4+8+8+8)+1+size;
	}
	return s;
}

void fs_getquotainfo_data(uint8_t * buff) {
	quotanode *qn;
	statsrecord *psr;
	uint32_t size;
	uint32_t ts = main_time();
//	put32bit(&buff,QuotaTimeLimit);
	for (qn=quotahead ; qn ; qn=qn->next) {
		psr = &(qn->node->data.ddata.stats);
		put32bit(&buff,qn->node->id);
		size=fsnodes_getpath_size(qn->node->parents);
		put32bit(&buff,size+1);
		put8bit(&buff,'/');
		fsnodes_getpath_data(qn->node->parents,buff,size);
		buff+=size;
		put8bit(&buff,qn->exceeded);
		put8bit(&buff,qn->flags);
		if (qn->stimestamp==0) {					// soft quota not exceeded
			put32bit(&buff,0xFFFFFFFF); 				// time to block = INF
		} else if (qn->stimestamp+QuotaTimeLimit<ts) {			// soft quota timed out
			put32bit(&buff,0);					// time to block = 0 (blocked)
		} else {							// soft quota exceeded, but not timed out
			put32bit(&buff,qn->stimestamp+QuotaTimeLimit-ts);
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
		return ERROR_ENOENT;
	}
	if (p->type!=TYPE_DIRECTORY && p->type!=TYPE_FILE && p->type!=TYPE_TRASH && p->type!=TYPE_SUSTAINED) {
		return ERROR_EPERM;
	}
	fsnodes_get_stats(p,&sr);
	*inodes = sr.inodes;
	*dirs = sr.dirs;
	*files = sr.files;
	*chunks = sr.chunks;
	*length = sr.length;
	*size = sr.size;
	*rsize = sr.realsize;
//	syslog(LOG_NOTICE,"using fast stats");
	return STATUS_OK;
}

static inline void fs_add_file_to_chunks(fsnode *f) {
	uint32_t i;
	uint64_t chunkid;
	if (f->type==TYPE_FILE || f->type==TYPE_TRASH || f->type==TYPE_SUSTAINED) {
		for (i=0 ; i<f->data.fdata.chunks ; i++) {
			chunkid = f->data.fdata.chunktab[i];
			if (chunkid>0) {
				chunk_add_file(chunkid,f->goal);
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
		syslog(LOG_ERR,"structure error - %s inconsistency (edge: %"PRIu32",%s -> %"PRIu32")",iname,e->parent->id,changelog_escape_name(e->nleng,e->name),e->child->id);
		if (leng<size) {
			leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: %"PRIu32",%s -> %"PRIu32")\n",iname,e->parent->id,changelog_escape_name(e->nleng,e->name),e->child->id);
		}
	} else {
		if (e->child->type==TYPE_TRASH) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: TRASH,%s -> %"PRIu32")",iname,changelog_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: TRASH,%s -> %"PRIu32")\n",iname,changelog_escape_name(e->nleng,e->name),e->child->id);
			}
		} else if (e->child->type==TYPE_SUSTAINED) {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: SUSTAINED,%s -> %"PRIu32")",iname,changelog_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: SUSTAINED,%s -> %"PRIu32")\n",iname,changelog_escape_name(e->nleng,e->name),e->child->id);
			}
		} else {
			syslog(LOG_ERR,"structure error - %s inconsistency (edge: NULL,%s -> %"PRIu32")",iname,changelog_escape_name(e->nleng,e->name),e->child->id);
			if (leng<size) {
				leng += snprintf(buff+leng,size-leng,"structure error - %s inconsistency (edge: NULL,%s -> %"PRIu32")\n",iname,changelog_escape_name(e->nleng,e->name),e->child->id);
			}
		}
	}
	return leng;
}

void fs_test_files() {
	static uint32_t i=0;
	uint32_t j;
	uint32_t k;
	uint64_t chunkid;
	uint8_t vc,valid,ugflag;
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
				for (j=0 ; j<f->data.fdata.chunks ; j++) {
					chunkid = f->data.fdata.chunktab[j];
					if (chunkid>0) {
						if (chunk_get_validcopies(chunkid,&vc)!=STATUS_OK) {
							syslog(LOG_ERR,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")",chunkid,f->id,j);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - chunk %016"PRIX64" not found (inode: %"PRIu32" ; index: %"PRIu32")\n",chunkid,f->id,j);
							}
							notfoundchunks++;
							if ((notfoundchunks%1000)==0) {
								syslog(LOG_ERR,"unknown chunks: %"PRIu32" ...",notfoundchunks);
							}
							valid =0;
							mchunks++;
						} else if (vc==0) {
							missing_log_insert(chunkid,f->id,j);
							valid = 0;
							mchunks++;
						} else if (vc<f->goal) {
							ugflag = 1;
							ugchunks++;
						}
						chunks++;
					}
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
			}
			for (e=f->parents ; e ; e=e->nextparent) {
				if (e->child != f) {
					if (e->parent) {
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")",f->id,e->parent->id,changelog_escape_name(e->nleng,e->name),e->child->id);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")\n",f->id,e->parent->id,changelog_escape_name(e->nleng,e->name),e->child->id);
						}
					} else {
						syslog(LOG_ERR,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")",f->id,changelog_escape_name(e->nleng,e->name),e->child->id);
						if (leng<MSGBUFFSIZE) {
							leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->child/child->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")\n",f->id,changelog_escape_name(e->nleng,e->name),e->child->id);
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
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")",f->id,e->parent->id,changelog_escape_name(e->nleng,e->name),e->child->id);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: %"PRIu32",%s -> %"PRIu32")\n",f->id,e->parent->id,changelog_escape_name(e->nleng,e->name),e->child->id);
							}
						} else {
							syslog(LOG_ERR,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")",f->id,changelog_escape_name(e->nleng,e->name),e->child->id);
							if (leng<MSGBUFFSIZE) {
								leng += snprintf(msgbuff+leng,MSGBUFFSIZE-leng,"structure error - edge->parent/parent->edges (node: %"PRIu32" ; edge: NULL,%s -> %"PRIu32")\n",f->id,changelog_escape_name(e->nleng,e->name),e->child->id);
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

uint8_t fs_univ_emptytrash(uint32_t ts,uint8_t sesflags,uint32_t freeinodes,uint32_t sustainedinodes) {
	uint32_t fi,ri;
	fsedge *e;
	fsnode *p;
	fi=0;
	ri=0;
	e = trash;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (((uint64_t)(p->atime) + (uint64_t)(p->trashtime) < (uint64_t)ts) && ((uint64_t)(p->mtime) + (uint64_t)(p->trashtime) < (uint64_t)ts) && ((uint64_t)(p->ctime) + (uint64_t)(p->trashtime) < (uint64_t)ts)) {
			if (fsnodes_purge(ts,p)) {
				fi++;
			} else {
				ri++;
			}
		}
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if ((fi|ri)>0) {
			changelog("%"PRIu32"|EMPTYTRASH():%"PRIu32",%"PRIu32,ts,fi,ri);
		}
	} else {
		meta_version_inc();
		if (freeinodes!=fi || sustainedinodes!=ri) {
			return ERROR_MISMATCH;
		}
	}
	return STATUS_OK;
}

void fs_emptytrash(void) {
	(void)fs_univ_emptytrash(main_time(),0,0,0);
}

uint8_t fs_mr_emptytrash(uint32_t ts,uint32_t freeinodes,uint32_t sustainedinodes) {
	return fs_univ_emptytrash(ts,SESFLAG_METARESTORE,freeinodes,sustainedinodes);
}

uint8_t fs_univ_emptysustained(uint32_t ts,uint8_t sesflags,uint32_t freeinodes) {
	fsedge *e;
	fsnode *p;
	uint32_t fi;

	fi=0;
	e = sustained;
	while (e) {
		p = e->child;
		e = e->nextchild;
		if (of_isfileopened(p->id)==0) {
//		if (p->data.fdata.sessionids==NULL) {
			fsnodes_purge(ts,p);
			fi++;
		}
	}
	if ((sesflags&SESFLAG_METARESTORE)==0) {
		if (fi>0) {
			changelog("%"PRIu32"|EMPTYSUSTAINED():%"PRIu32,ts,fi);
		}
	} else {
		meta_version_inc();
		if (freeinodes!=fi) {
			return ERROR_MISMATCH;
		}
	}
	return STATUS_OK;
}

void fs_emptysustained(void) {
	(void)fs_univ_emptysustained(main_time(),0,0);
}

uint8_t fs_mr_emptysustained(uint32_t ts,uint32_t freeinodes) {
	return fs_univ_emptysustained(ts,SESFLAG_METARESTORE,freeinodes);
}

static inline void fs_renumerate_edges(fsnode *p) {
	fsedge *e;
	for (e=p->data.ddata.children ; e ; e=e->nextchild) {
		fsnodes_keep_alive_check();
		if (e->edgeid==0) {
			e->edgeid = nextedgeid;
			nextedgeid--;
		}
		if (e->child->type==TYPE_DIRECTORY) {
			fs_renumerate_edges(e->child);
		}
	}
}

uint8_t fs_mr_renumerate_edges(uint64_t expected_nextedgeid) {
	if (nextedgeid!=EDGEID_MAX) {
		return ERROR_MISMATCH;
	}
	nextedgeid--;
	fsnodes_keep_alive_begin();
	fs_renumerate_edges(root);
	meta_version_inc();
	if (nextedgeid!=expected_nextedgeid) {
		return ERROR_MISMATCH;
	}
	return STATUS_OK;
}

void fs_renumerate_edge_test(void) {
	if (nextedgeid==EDGEID_MAX) {
		nextedgeid--;
		fsnodes_keep_alive_begin();
		fs_renumerate_edges(root);
		changelog("%"PRIu32"|RENUMEDGES():%"PRIu64,main_time(),nextedgeid);
	}
}

void fs_cleanupedges(void) {
	fsedge_cleanup();
	fsnodes_edge_hash_cleanup();
	trash = NULL;
	sustained = NULL;
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
		put32bit(&ptr,e->parent->id);
	}
	put32bit(&ptr,e->child->id);
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
//	uint32_t hpos;
	fsedge *e;
	statsrecord sr;
	static fsedge **root_tail;
	static fsedge **current_tail;
	static fsnode *current_parent;
	static uint32_t current_parent_id;
	static uint8_t nl;
	static ssize_t bsize;

	if (fd==NULL) {
		current_parent_id = 0;
		current_parent = NULL;
		current_tail = NULL;
		root_tail = NULL;
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
			e->parent = NULL;
			e->nextchild = trash;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			trash = e;
			e->prevchild = &trash;
			trashspace += e->child->data.fdata.length;
			trashnodes++;
		} else if (e->child->type==TYPE_SUSTAINED) {
			e->parent = NULL;
			e->nextchild = sustained;
			if (e->nextchild) {
				e->nextchild->prevchild = &(e->nextchild);
			}
			sustained = e;
			e->prevchild = &sustained;
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
						current_tail = &((*current_tail)->nextchild);
					}
				} else {
					fsedge_free(e,nleng);
					return -1;
				}
			} else {
				current_tail = &(e->parent->data.ddata.children);
			}
			current_parent_id = parent_id;
			current_parent = e->parent;
		}
		e->nextchild = NULL;
		if (parent_id==MFS_ROOT_ID) {
			*(root_tail) = e;
			e->prevchild = root_tail;
			root_tail = &(e->nextchild);
		} else {
			*(current_tail) = e;
			e->prevchild = current_tail;
			current_tail = &(e->nextchild);
		}
		e->parent->data.ddata.elements++;
		if (e->child->type==TYPE_DIRECTORY) {
			e->parent->data.ddata.nlink++;
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
		fsnodes_get_stats(e->child,&sr);
		fsnodes_add_stats(e->parent,&sr);
	}
	e->edgeid = edgeid;
	return 0;
}

static inline void fs_storenode(fsnode *f,bio *fd) {
	uint8_t unodebuff[1+4+1+1+2+4+4+4+4+4+4+8+4+2+8*65536+4*65536+4];
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
	put32bit(&ptr,f->id);
	put8bit(&ptr,f->goal);
	put8bit(&ptr,f->flags);
	put16bit(&ptr,f->mode);
	put32bit(&ptr,f->uid);
	put32bit(&ptr,f->gid);
	put32bit(&ptr,f->atime);
	put32bit(&ptr,f->mtime);
	put32bit(&ptr,f->ctime);
	put32bit(&ptr,f->trashtime);
	switch (f->type) {
	case TYPE_DIRECTORY:
	case TYPE_SOCKET:
	case TYPE_FIFO:
		if (bio_write(fd,unodebuff,1+4+1+1+2+4+4+4+4+4+4)!=(1+4+1+1+2+4+4+4+4+4+4)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		put32bit(&ptr,f->data.devdata.rdev);
		if (bio_write(fd,unodebuff,1+4+1+1+2+4+4+4+4+4+4+4)!=(1+4+1+1+2+4+4+4+4+4+4+4)) {
			syslog(LOG_NOTICE,"write error");
			return;
		}
		break;
	case TYPE_SYMLINK:
		put32bit(&ptr,f->data.sdata.pleng);
		if (bio_write(fd,unodebuff,1+4+1+1+2+4+4+4+4+4+4+4)!=(1+4+1+1+2+4+4+4+4+4+4+4)) {
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
		put16bit(&ptr,0);

		if (bio_write(fd,unodebuff,1+4+1+1+2+4+4+4+4+4+4+8+4+2)!=(1+4+1+1+2+4+4+4+4+4+4+8+4+2)) {
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
	uint8_t unodebuff[4+1+1+2+4+4+4+4+4+4+8+4+2+8*65536+4*65536+4];
	const uint8_t *ptr,*chptr;
	uint8_t type;
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
		type = fsnodes_type_convert(type);
	} else {
		hdrsize = 4+1+1+2+6*4;
		if (mver<=0x12) {
			type = fsnodes_type_convert(type);
		}
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
	p->id = get32bit(&ptr);
	p->goal = get8bit(&ptr);
	if (mver<=0x11) {
		uint16_t flagsmode = get16bit(&ptr);
		p->flags = flagsmode>>12;
		p->mode = flagsmode&0xFFF;
	} else {
		p->flags = get8bit(&ptr);
		p->mode = get16bit(&ptr);
	}
	p->uid = get32bit(&ptr);
	p->gid = get32bit(&ptr);
	p->atime = get32bit(&ptr);
	p->mtime = get32bit(&ptr);
	p->ctime = get32bit(&ptr);
	p->trashtime = get32bit(&ptr);
	switch (type) {
	case TYPE_DIRECTORY:
		memset(&(p->data.ddata.stats),0,sizeof(statsrecord));
		p->data.ddata.quota = NULL;
		p->data.ddata.children = NULL;
		p->data.ddata.nlink = 2;
		p->data.ddata.elements = 0;
	case TYPE_SOCKET:
	case TYPE_FIFO:
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
		p->data.devdata.rdev = get32bit(&ptr);
		break;
	case TYPE_SYMLINK:
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
		p->data.fdata.length = get64bit(&ptr);
		ch = get32bit(&ptr);
		p->data.fdata.chunks = ch;
		sessionids = get16bit(&ptr);
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
			of_mr_acquire(sessionid,p->id);
			sessionids--;
		}
	}
	p->parents = NULL;
	fsnodes_node_add(p);
	fsnodes_used_inode(p->id);
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
		return 0x13;
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
	fs_storeedgelist(trash,fd);
	fs_storeedgelist(sustained,fd);
	fs_storeedge(NULL,fd);	// end marker
	return 0;
}

int fs_lostnode(fsnode *p) {
	uint8_t artname[40];
	uint32_t i,l;
	i=0;
	do {
		if (i==0) {
			l = snprintf((char*)artname,40,"lost_node_%"PRIu32,p->id);
		} else {
			l = snprintf((char*)artname,40,"lost_node_%"PRIu32".%"PRIu32,p->id,i);
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
				fprintf(stderr,"found orphaned inode: %"PRIu32"\n",p->id);
				syslog(LOG_ERR,"found orphaned inode: %"PRIu32,p->id);
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
	} else {
		nextedgeid = EDGEID_MAX;
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
		put32bit(&ptr,n->id);
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
		n->id = nodeid;
		n->ftime = ftime;
		n->next = NULL;
		*freetail = n;
		freetail = &(n->next);
		fsnodes_used_inode(nodeid);
		l--;
		t--;
	}
	return 0;
}

// quota entry:
// inode:4 exceeded:1 flags:1 ts:4 sinodes:4 hinodes:4 slength:8 hlength:8 ssize:8 hsize:8 srealsize:8 hrealsize:8 = 66B

uint8_t fs_storequota(bio *fd) {
	uint8_t wbuff[66*100],*ptr;
	quotanode *qn;
	uint32_t l;
	if (fd==NULL) {
		return 0x10;
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
	l=0;
	ptr=wbuff;
	for (qn = quotahead ; qn ; qn=qn->next) {
		if (l==100) {
				if (bio_write(fd,wbuff,66*100)!=(66*100)) {
					syslog(LOG_NOTICE,"write error");
					return 0xFF;
				}
				l=0;
				ptr=wbuff;
			}
			put32bit(&ptr,qn->node->id);
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
			l++;
	}
	if (l>0) {
		if (bio_write(fd,wbuff,66*l)!=(66*l)) {
			syslog(LOG_NOTICE,"write error");
			return 0xFF;
		}
	}
	return 0;
}

int fs_loadquota(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t rbuff[66*100];
	const uint8_t *ptr;
	quotanode *qn;
	fsnode *fn;
	uint32_t l,t,id;
	uint8_t nl=1;

	(void)mver;

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
	t = get32bit(&ptr);
	quotahead = NULL;
//	freetail = &(freelist);
	l=0;
	while (t>0) {
		if (l==0) {
			if (t>100) {
				if (bio_read(fd,rbuff,66*100)!=(66*100)) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						// nl=0;
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading quota: read error");
					return -1;
				}
				l=100;
			} else {
				if (bio_read(fd,rbuff,66*t)!=(66*t)) {
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
		id = get32bit(&ptr);
		fn = fsnodes_node_find(id);
		if (fn==NULL || fn->type!=TYPE_DIRECTORY) {
			if (nl) {
				fputc('\n',stderr);
				nl=0;
			}
			fprintf(stderr,"quota defined for %s inode: %"PRIu32"\n",(fn==NULL)?"non existing":"not directory",id);
			syslog(LOG_ERR,"quota defined for %s inode: %"PRIu32,(fn==NULL)?"non existing":"not directory",id);
			if (ignoreflag) {
				ptr+=62;
			} else {
				fprintf(stderr,"use option '-i' to remove this quota definition");
				return -1;
			}
		} else {
			qn = fsnodes_new_quotanode(fn);
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
		t--;
	}
	return 0;
}

void fs_new(void) {
	nextedgeid = (EDGEID_MAX-1);
	hashelements = 1;
	maxnodeid = MFS_ROOT_ID;
	fsnodes_init_freebitmask();
	root = fsnode_dir_malloc();
	passert(root);
	root->id = MFS_ROOT_ID;
	root->xattrflag = 0;
	root->aclpermflag = 0;
	root->acldefflag = 0;
	root->type = TYPE_DIRECTORY;
	root->ctime = root->mtime = root->atime = main_time();
	root->goal = DEFAULT_GOAL;
	root->trashtime = DEFAULT_TRASHTIME;
	root->flags = 0;
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
	fsnodes_used_inode(root->id);
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
	QuotaTimeLimit = cfg_getuint32("QUOTA_TIME_LIMIT",7*86400);
}

int fs_strinit(void) {
	root = NULL;
	trash = NULL;
	sustained = NULL;
	trashspace = 0;
	sustainedspace = 0;
	trashnodes = 0;
	sustainednodes = 0;
	quotahead = NULL;
	fsnodes_edgeid_init();
	fsnodes_node_hash_init();
	fsnodes_edge_hash_init();
	fsnode_init();
	fsedge_init();
	symlink_init();
	chunktab_init();
	test_start_time = main_time()+900;
	QuotaTimeLimit = cfg_getuint32("QUOTA_TIME_LIMIT",7*86400);

	main_reload_register(fs_reload);
	main_msectime_register(100,0,fs_test_files);
	main_time_register(1,0,fsnodes_check_all_quotas);
	main_time_register(300,0,fs_emptytrash);
	main_time_register(60,0,fs_emptysustained);
	main_time_register(60,0,fsnodes_freeinodes);
	return 0;
}

