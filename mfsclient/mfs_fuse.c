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

#if defined(__APPLE__)
# if ! defined(__DARWIN_64_BIT_INO_T) && ! defined(_DARWIN_USE_64_BIT_INODE)
#  define __DARWIN_64_BIT_INO_T 0
# endif
#endif

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "fusecommon.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <syslog.h>
#include <inttypes.h>
#include <pthread.h>
#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif
#ifdef HAVE_ATTR_XATTR_H
#include <attr/xattr.h>
#endif
#ifdef HAVE_SYS_FILE_H
#include <sys/file.h>
#endif

#include "stats.h"
#include "oplog.h"
#include "datapack.h"
#include "clocks.h"
#include "portable.h"
#include "mastercomm.h"
#include "masterproxy.h"
#include "getgroups.h"
#include "readdata.h"
#include "writedata.h"
#include "massert.h"
#include "strerr.h"
#include "mfsalloc.h"
#include "lwthread.h"
#include "MFSCommunication.h"

#include "mfsmount.h"
#include "sustained_stats.h"
#include "chunksdatacache.h"
#include "dirattrcache.h"
#include "symlinkcache.h"
#include "negentrycache.h"
#include "xattrcache.h"
#include "fdcache.h"
#include "inoleng.h"

#if MFS_ROOT_ID != FUSE_ROOT_ID
#error FUSE_ROOT_ID is not equal to MFS_ROOT_ID
#endif

/* check for well known constants and define them if necessary */
#ifndef XATTR_CREATE
#define XATTR_CREATE 1
#endif
#ifndef XATTR_REPLACE
#define XATTR_REPLACE 2
#endif
#ifndef LOCK_SH
#define LOCK_SH 1
#endif
#ifndef LOCK_EX
#define LOCK_EX 2
#endif
#ifndef LOCK_NB
#define LOCK_NB 4
#endif
#ifndef LOCK_UN
#define LOCK_UN 8
#endif

#if defined(__FreeBSD__)
static int freebsd_workarounds = 1;
// workaround for bug in FreeBSD Fuse version (kernel part)
#  define FREEBSD_DELAYED_RELEASE 1
#  define FREEBSD_RELEASE_DELAY 10.0
#endif

#define RANDOM_BUFFSIZE 0x100000

#define READDIR_BUFFSIZE 50000

#define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

#define PKGVERSION ((VERSMAJ)*1000000+(VERSMID)*10000+((VERSMIN)>>1)*100+(RELEASE))

#define MASTERINFO_WITH_VERSION 1

#define MASTERINFO_NAME ".masterinfo"
#define MASTERINFO_INODE 0x7FFFFFFF
// 0x0124 == 0b100100100 == 0444
#ifdef MASTERINFO_WITH_VERSION
static uint8_t masterinfoattr[ATTR_RECORD_SIZE]={0, (TYPE_FILE << 4) | 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,14, 0};
#else
static uint8_t masterinfoattr[ATTR_RECORD_SIZE]={0, (TYPE_FILE << 4) | 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10, 0};
#endif

#define STATS_NAME ".stats"
#define STATS_INODE 0x7FFFFFF0
// 0x01A4 == 0b110100100 == 0644
static uint8_t statsattr[ATTR_RECORD_SIZE]={0, (TYPE_FILE << 4) | 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0, 0};

#define OPLOG_NAME ".oplog"
#define OPLOG_INODE 0x7FFFFFF1
#define OPHISTORY_NAME ".ophistory"
#define OPHISTORY_INODE 0x7FFFFFF2
// 0x0100 == 0b100000000 == 0400
static uint8_t oplogattr[ATTR_RECORD_SIZE]={0, (TYPE_FILE << 4) | 0x01,0x00, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0, 0};

#define MOOSE_NAME ".mooseart"
#define MOOSE_INODE 0x7FFFFFF3
// 0x01A4 == 0b110100100 == 0644
static uint8_t mooseattr[ATTR_RECORD_SIZE]={0, (TYPE_FILE << 4) | 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0, 0};

#define RANDOM_NAME ".random"
#define RANDOM_INODE 0x7FFFFFF4
// 0x0124 == 0b100100100 == 0444
static uint8_t randomattr[ATTR_RECORD_SIZE]={0, (TYPE_FILE << 4) | 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0, 0};

#define PARAMS_NAME ".params"
#define PARAMS_INODE 0x7FFFFFF5
// 0x0124 == 0b100100100 == 0400
static uint8_t paramsattr[ATTR_RECORD_SIZE]={0, (TYPE_FILE << 4) | 0x01,0x00, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0, 0};

#define MIN_SPECIAL_INODE 0x7FFFFFF0
#define IS_SPECIAL_INODE(ino) ((ino)>=MIN_SPECIAL_INODE)
#define IS_SPECIAL_NAME(name) ((name)[0]=='.' && (strcmp(STATS_NAME,(name))==0 || strcmp(MASTERINFO_NAME,(name))==0 || strcmp(OPLOG_NAME,(name))==0 || strcmp(OPHISTORY_NAME,(name))==0 || strcmp(MOOSE_NAME,(name))==0 || strcmp(RANDOM_NAME,(name))==0 || strcmp(PARAMS_NAME,(name))==0))

// generators from: http://school.anhb.uwa.edu.au/personalpages/kwessen/shared/Marsaglia99.html (by George Marsaglia)

/* random state */
static uint32_t rndz=362436069;
static uint32_t rndw=521288629;
static uint32_t rndjsr=123456789;
static uint32_t rndjcong=380116160;

#define znew (rndz=36969*(rndz&65535)+(rndz>>16))
#define wnew (rndw=18000*(rndw&65535)+(rndw>>16))
#define MWC ((znew<<16)+wnew)
#define SHR3 (rndjsr^=(rndjsr<<17), rndjsr^=(rndjsr>>13), rndjsr^=(rndjsr<<5))
#define CONG (rndjcong=69069*rndjcong+1234567)
#define KISS ((MWC^CONG)+SHR3)

static pthread_mutex_t randomlock = PTHREAD_MUTEX_INITIALIZER;

static char *params_buff;
static uint32_t params_leng;

/* STATS INODE BUFFER */

typedef struct _sinfo {
	char *buff;
	uint32_t leng;
	uint8_t reset;
	uint8_t valid;
	pthread_mutex_t lock;
	uint32_t next;
} sinfo;

static uint32_t sinfo_head=0,sinfo_size=0,sinfo_max=1;
static sinfo* *sinfo_tab=NULL;
static pthread_mutex_t sinfo_tab_lock = PTHREAD_MUTEX_INITIALIZER;

static uint32_t sinfo_new(void) {
	uint32_t i;
	zassert(pthread_mutex_lock(&sinfo_tab_lock));
	if (sinfo_head!=0) {
		i = sinfo_head;
		sinfo_head = sinfo_tab[i]->next;
	} else {
		if (sinfo_max>=sinfo_size) {
			if (sinfo_size==0) {
				sinfo_size = 16;
				sinfo_tab = malloc(sizeof(sinfo*)*sinfo_size);
			} else {
				sinfo_size *= 2;
				sinfo_tab = mfsrealloc(sinfo_tab,sizeof(sinfo*)*sinfo_size);
			}
			passert(sinfo_tab);
		}
		i = sinfo_max++;
		sinfo_tab[i] = malloc(sizeof(sinfo));
		passert(sinfo_tab[i]);
		memset(sinfo_tab[i],0,sizeof(sinfo));
		zassert(pthread_mutex_init(&(sinfo_tab[i]->lock),NULL));
	}
	zassert(pthread_mutex_unlock(&sinfo_tab_lock));
	return i;
}

static inline sinfo* sinfo_get(uint32_t sindex) {
	zassert(pthread_mutex_lock(&sinfo_tab_lock));
	if (sindex==0 || sindex>=sinfo_max) {
		zassert(pthread_mutex_unlock(&sinfo_tab_lock));
		return NULL;
	} else {
		zassert(pthread_mutex_unlock(&sinfo_tab_lock));
		return sinfo_tab[sindex];
	}
}

static void sinfo_release(uint32_t sindex) {
	sinfo *statsinfo;
	if (sindex>0) {
		zassert(pthread_mutex_lock(&sinfo_tab_lock));
		statsinfo = sinfo_tab[sindex];
		zassert(pthread_mutex_lock(&(statsinfo->lock)));
		if (statsinfo->buff!=NULL) {
			free(statsinfo->buff);
			statsinfo->buff = NULL;
		}
		zassert(pthread_mutex_unlock(&(statsinfo->lock)));
		statsinfo->next = sinfo_head;
		sinfo_head = sindex;
		zassert(pthread_mutex_unlock(&sinfo_tab_lock));
	}
}

static void sinfo_freeall(void) {
	uint32_t i;
	sinfo *statsinfo;
	zassert(pthread_mutex_lock(&sinfo_tab_lock));
	if (sinfo_tab!=NULL) {
		for (i=1 ; i<sinfo_max ; i++) {
			statsinfo = sinfo_tab[i];
			zassert(pthread_mutex_lock(&(statsinfo->lock)));
			if (statsinfo->buff) {
				free(statsinfo->buff);
			}
			zassert(pthread_mutex_unlock(&(statsinfo->lock)));
			zassert(pthread_mutex_destroy(&(statsinfo->lock)));
			free(statsinfo);
		}
		free(sinfo_tab);
	}
	sinfo_max = 1;
	sinfo_size = 0;
	sinfo_head = 0;
	zassert(pthread_mutex_unlock(&sinfo_tab_lock));
}



/* DIRECTORY INODE BUFFERS */

typedef struct _dirbuf {
	int wasread;
	int dataformat;
	uid_t uid;
	gid_t gid;
	const uint8_t *p;
	size_t size;
	void *dcache;
	pthread_mutex_t lock;
	uint32_t next;
} dirbuf;

static uint32_t dirbuf_head=0,dirbuf_size=0,dirbuf_max=1;
static dirbuf* *dirbuf_tab=NULL;
static pthread_mutex_t dirbuf_tab_lock = PTHREAD_MUTEX_INITIALIZER;

static uint32_t dirbuf_new(void) {
	uint32_t i;
	zassert(pthread_mutex_lock(&dirbuf_tab_lock));
	if (dirbuf_head!=0) {
		i = dirbuf_head;
		dirbuf_head = dirbuf_tab[i]->next;
	} else {
		if (dirbuf_max>=dirbuf_size) {
			if (dirbuf_size==0) {
				dirbuf_size = 32;
				dirbuf_tab = malloc(sizeof(dirbuf*)*dirbuf_size);
			} else {
				dirbuf_size *= 2;
				dirbuf_tab = mfsrealloc(dirbuf_tab,sizeof(dirbuf*)*dirbuf_size);
			}
			passert(dirbuf_tab);
		}
		i = dirbuf_max++;
		dirbuf_tab[i] = malloc(sizeof(dirbuf));
		passert(dirbuf_tab[i]);
		memset(dirbuf_tab[i],0,sizeof(dirbuf));
		zassert(pthread_mutex_init(&(dirbuf_tab[i]->lock),NULL));
	}
	zassert(pthread_mutex_unlock(&dirbuf_tab_lock));
	return i;
}

static inline dirbuf* dirbuf_get(uint32_t dindex) {
	zassert(pthread_mutex_lock(&dirbuf_tab_lock));
	if (dindex==0 || dindex>=dirbuf_max) {
		zassert(pthread_mutex_unlock(&dirbuf_tab_lock));
		return NULL;
	} else {
		zassert(pthread_mutex_unlock(&dirbuf_tab_lock));
		return dirbuf_tab[dindex];
	}
}

static void dirbuf_release(uint32_t dindex) {
	dirbuf *dirinfo;
	if (dindex>0) {
		zassert(pthread_mutex_lock(&dirbuf_tab_lock));
		dirinfo = dirbuf_tab[dindex];
		zassert(pthread_mutex_lock(&(dirinfo->lock)));
		if (dirinfo->p!=NULL) {
			free((uint8_t *)(dirinfo->p));
			dirinfo->p = NULL;
		}
		zassert(pthread_mutex_unlock(&(dirinfo->lock)));
		dirinfo->next = dirbuf_head;
		dirbuf_head = dindex;
		zassert(pthread_mutex_unlock(&dirbuf_tab_lock));
	}
}

static void dirbuf_freeall(void) {
	uint32_t i;
	dirbuf *dirinfo;
	zassert(pthread_mutex_lock(&dirbuf_tab_lock));
	if (dirbuf_tab!=NULL) {
		for (i=1 ; i<dirbuf_max ; i++) {
			dirinfo = dirbuf_tab[i];
			zassert(pthread_mutex_lock(&(dirinfo->lock)));
			if (dirinfo->p) {
				free((uint8_t*)(dirinfo->p));
			}
			zassert(pthread_mutex_unlock(&(dirinfo->lock)));
			zassert(pthread_mutex_destroy(&(dirinfo->lock)));
			free(dirinfo);
		}
		free(dirbuf_tab);
	}
	dirbuf_max = 1;
	dirbuf_size = 0;
	dirbuf_head = 0;
	dirbuf_tab = NULL;
	zassert(pthread_mutex_unlock(&dirbuf_tab_lock));
}



/* FILE INODE DATA */

enum {IO_READWRITE,IO_READONLY,IO_WRITEONLY};

typedef struct _lock_owner {
#ifdef FLUSH_EXTRA_LOCKS
	pid_t pid;
#endif
	uint64_t lock_owner;
	struct _lock_owner *next;
} finfo_lock_owner;

typedef struct _finfo {
	void *flengptr;
	uint32_t inode;
	uint8_t mode;
	uint8_t uselocks;
	uint8_t valid;
	uint8_t writing;
	uint8_t open_waiting;
	uint8_t open_in_master;
	uint32_t readers_cnt;
	uint32_t writers_cnt;
	void *rdata;
	void *wdata;
	double create;
	finfo_lock_owner *posix_lo_head;
	finfo_lock_owner *flock_lo_head;
	pthread_mutex_t lock;
	pthread_cond_t rwcond;
	pthread_cond_t opencond;
	uint32_t findex;
	uint32_t next,*prev;
#ifdef FREEBSD_DELAYED_RELEASE
	uint32_t ops_in_progress;
	double lastuse;
#endif
} finfo;

#ifdef FREEBSD_DELAYED_RELEASE
static uint32_t finfo_released_head=0;
#endif
static uint32_t finfo_head=0,finfo_size=0,finfo_max=1;
static finfo* *finfo_tab=NULL;
static pthread_mutex_t finfo_tab_lock = PTHREAD_MUTEX_INITIALIZER;
static uint32_t finfo_inode_hash[1024] = {0,};

static void finfo_free_resources(finfo *fileinfo) {
	if (fileinfo->rdata) {
		read_data_end(fileinfo->rdata);
	}
	if (fileinfo->wdata) {
		write_data_end(fileinfo->wdata);
	}
	if (fileinfo->flengptr) {
		inoleng_release(fileinfo->flengptr);
	}
	fileinfo->rdata = fileinfo->wdata = fileinfo->flengptr = NULL;
}

#ifdef FREEBSD_DELAYED_RELEASE
static void finfo_delayed_release(double now) {
	finfo *fileinfo;
	uint32_t i,*pi;
	pi = &finfo_released_head;
	while ((i=*pi)!=0) {
		fileinfo = finfo_tab[i];
		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		if (fileinfo->ops_in_progress==0 && fileinfo->lastuse+FREEBSD_RELEASE_DELAY<now) {
			if (write_data_will_end_wait(fileinfo->wdata)) {
				fileinfo->lastuse = now;
				pi = &(fileinfo->next);
			} else {
				finfo_free_resources(fileinfo);
				*pi = fileinfo->next;
				fileinfo->next = finfo_head;
				finfo_head = i;
			}
		} else {
			pi = &(fileinfo->next);
		}
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	}
}

static void* finfo_delayed_release_cleanup_thread(void* arg) {
	double now;

	while (1) {
		now = monotonic_seconds();
		zassert(pthread_mutex_lock(&finfo_tab_lock));
		finfo_delayed_release(now);
		zassert(pthread_mutex_unlock(&finfo_tab_lock));
		sleep(1);
	}
	return arg;
}
#endif

static uint32_t finfo_new(uint32_t inode) {
	uint32_t i,ni,findex;
	finfo *fileinfo;
#ifdef FREEBSD_DELAYED_RELEASE
	double now;
	now = monotonic_seconds();
#endif

	zassert(pthread_mutex_lock(&finfo_tab_lock));
#ifdef FREEBSD_DELAYED_RELEASE
	finfo_delayed_release(now);
#endif
	if (finfo_head!=0) {
		i = finfo_head;
		fileinfo = finfo_tab[i];
		finfo_head = fileinfo->next;
	} else {
		if (finfo_max>=finfo_size) {
			if (finfo_size==0) {
				finfo_size = 1024;
				finfo_tab = malloc(sizeof(finfo*)*finfo_size);
			} else {
				finfo_size *= 2;
				massert(finfo_size<=0x1000000,"file handle tabble too big");
				finfo_tab = mfsrealloc(finfo_tab,sizeof(finfo*)*finfo_size);
			}
			passert(finfo_tab);
		}
		i = finfo_max++;
		finfo_tab[i] = malloc(sizeof(finfo));
		fileinfo = finfo_tab[i];
		passert(fileinfo);
		memset(fileinfo,0,sizeof(finfo));
		zassert(pthread_mutex_init(&(fileinfo->lock),NULL));
		zassert(pthread_cond_init(&(fileinfo->rwcond),NULL));
		zassert(pthread_cond_init(&(fileinfo->opencond),NULL));
		fileinfo->rdata = NULL;
		fileinfo->wdata = NULL;
		fileinfo->flengptr = NULL;
		fileinfo->findex = i;
	}
	findex = fileinfo->findex;
	massert((findex&0xFFFFFF)==i,"file info record index mismatch");
	findex += 0x1000000;
	if ((findex & 0xFF000000)==0) {
		findex += 0x1000000;
	}
	fileinfo->findex = findex;
	fileinfo->inode = inode;
	ni = fileinfo->next = finfo_inode_hash[inode&1023];
	finfo_inode_hash[inode&1023] = i;
	if (ni!=0) {
		finfo_tab[ni]->prev = &(fileinfo->next);
	}
	fileinfo->prev = finfo_inode_hash + (inode&1023);
	zassert(pthread_mutex_unlock(&finfo_tab_lock));
	return findex;
}

static inline finfo* finfo_get(uint32_t findex) {
	uint32_t tindex;
	finfo *fileinfo;
	if (findex>0) {
		tindex = findex & 0xFFFFFF;
		zassert(pthread_mutex_lock(&finfo_tab_lock));
		if (tindex>=finfo_max) {
			fileinfo = NULL;
		} else {
			fileinfo = finfo_tab[tindex];
			if (fileinfo->findex!=findex) {
				fileinfo = NULL;
			}
		}
		zassert(pthread_mutex_unlock(&finfo_tab_lock));
	} else {
		fileinfo = NULL;
	}
	return fileinfo;
}

static void finfo_release(uint32_t findex) {
	uint32_t tindex;
	uint32_t ni;
	finfo *fileinfo;
	if (findex>0) {
		tindex = findex & 0xFFFFFF;
		zassert(pthread_mutex_lock(&finfo_tab_lock));
		if (tindex<finfo_max) {
			fileinfo = finfo_tab[tindex];
			ni = fileinfo->next;
			*(fileinfo->prev) = ni;
			if (ni!=0) {
				finfo_tab[ni]->prev = fileinfo->prev;
			}
#ifdef FREEBSD_DELAYED_RELEASE
			fileinfo->next = finfo_released_head;
			fileinfo->prev = NULL;
			finfo_released_head = tindex;
#else
			fileinfo->next = finfo_head;
			fileinfo->prev = NULL;
			finfo_head = tindex;
#endif
		}
		zassert(pthread_mutex_unlock(&finfo_tab_lock));
	}
}

static void finfo_freeall(void) {
	uint32_t i;
	finfo *fileinfo;
	zassert(pthread_mutex_lock(&finfo_tab_lock));
	if (finfo_tab!=NULL) {
		for (i=1 ; i<finfo_max ; i++) {
			fileinfo = finfo_tab[i];
			zassert(pthread_mutex_lock(&(fileinfo->lock)));
			finfo_free_resources(fileinfo);
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
			zassert(pthread_mutex_destroy(&(fileinfo->lock)));
			zassert(pthread_cond_destroy(&(fileinfo->rwcond)));
			zassert(pthread_cond_destroy(&(fileinfo->opencond)));
			free(fileinfo);
		}
		free(finfo_tab);
	}
	finfo_max = 1;
	finfo_size = 0;
	finfo_head = 0;
#ifdef FREEBSD_DELAYED_RELEASE
	finfo_released_head = 0;
#endif
	finfo_tab = NULL;
	zassert(pthread_mutex_unlock(&finfo_tab_lock));
}

static void finfo_change_fleng(uint32_t inode,uint64_t fleng) {
	inoleng_update_fleng(inode,fleng);
}



#ifdef HAVE_FUSE3
static struct fuse_session *fuse_comm = NULL;
#else /* FUSE2 */
static struct fuse_chan *fuse_comm = NULL;
#endif
static int debug_mode = 0;
static int usedircache = 1;
static int keep_cache = 0;
static double direntry_cache_timeout = 0.1;
static double entry_cache_timeout = 0.0;
static double attr_cache_timeout = 0.1;
static int mkdir_copy_sgid = 0;
static int sugid_clear_mode = 0;
static int xattr_cache_on = 0;
static int xattr_acl_support = 0;
static double fsync_before_close_min_time = 10.0;
static int no_xattrs = 0;
static int no_posix_locks = 0;
static int no_bsd_locks = 0;
static int full_permissions = 0;

//static int local_mode = 0;
//static int no_attr_cache = 0;

enum {
	OP_STATFS = 0,
	OP_ACCESS,
	OP_LOOKUP,
	OP_ERRLOOKUP,
	OP_POSLOOKUP,
	OP_NEGLOOKUP,
	OP_LOOKUP_INTERNAL,
	OP_DIRCACHE_LOOKUP,
	OP_NEGCACHE_LOOKUP,
//	OP_DIRCACHE_LOOKUP_POSITIVE,
//	OP_DIRCACHE_LOOKUP_NEGATIVE,
//	OP_DIRCACHE_LOOKUP_NOATTR,
	OP_GETATTR,
	OP_DIRCACHE_GETATTR,
	OP_SETATTR,
	OP_MKNOD,
	OP_UNLINK,
	OP_MKDIR,
	OP_RMDIR,
	OP_SYMLINK,
	OP_READLINK_MASTER,
	OP_READLINK_CACHED,
	OP_RENAME,
	OP_LINK,
	OP_OPENDIR,
	OP_READDIR,
	OP_RELEASEDIR,
	OP_CREATE,
	OP_OPEN,
	OP_RELEASE,
	OP_READ,
	OP_WRITE,
	OP_FLUSH,
	OP_FSYNC,
#if FUSE_VERSION >= 29
	OP_FLOCK,
#endif
#if FUSE_VERSION >= 26
	OP_GETLK,
	OP_SETLK,
#endif
	OP_SETXATTR,
	OP_GETXATTR,
	OP_LISTXATTR,
	OP_REMOVEXATTR,
//	OP_GETDIR_CACHED,
	OP_GETDIR_FULL,
	OP_GETDIR_SMALL,
#if FUSE_VERSION >= 30
	OP_READDIRPLUS,
	OP_GETDIR_PLUS,
#endif
	STATNODES
};

static void *statsptr[STATNODES];

void mfs_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"fuse_ops",0,1);
	statsptr[OP_SETXATTR] = stats_get_subnode(s,"setxattr",0,1);
	statsptr[OP_GETXATTR] = stats_get_subnode(s,"getxattr",0,1);
	statsptr[OP_LISTXATTR] = stats_get_subnode(s,"listxattr",0,1);
	statsptr[OP_REMOVEXATTR] = stats_get_subnode(s,"removexattr",0,1);
#if FUSE_VERSION >= 29
	statsptr[OP_FLOCK] = stats_get_subnode(s,"flock",0,1);
#endif
#if FUSE_VERSION >= 26
	statsptr[OP_GETLK] = stats_get_subnode(s,"getlk",0,1);
	statsptr[OP_SETLK] = stats_get_subnode(s,"setlk",0,1);
#endif
	statsptr[OP_FSYNC] = stats_get_subnode(s,"fsync",0,1);
	statsptr[OP_FLUSH] = stats_get_subnode(s,"flush",0,1);
	statsptr[OP_WRITE] = stats_get_subnode(s,"write",0,1);
	statsptr[OP_READ] = stats_get_subnode(s,"read",0,1);
	statsptr[OP_RELEASE] = stats_get_subnode(s,"release",0,1);
	statsptr[OP_OPEN] = stats_get_subnode(s,"open",0,1);
	statsptr[OP_CREATE] = stats_get_subnode(s,"create",0,1);
	statsptr[OP_RELEASEDIR] = stats_get_subnode(s,"releasedir",0,1);
	statsptr[OP_READDIR] = stats_get_subnode(s,"readdir",0,1);
#if FUSE_VERSION >= 30
	statsptr[OP_READDIRPLUS] = stats_get_subnode(s,"readdirplus",0,1);
#endif
	statsptr[OP_OPENDIR] = stats_get_subnode(s,"opendir",0,1);
	statsptr[OP_LINK] = stats_get_subnode(s,"link",0,1);
	statsptr[OP_RENAME] = stats_get_subnode(s,"rename",0,1);
	{
		void *rl;
		rl = stats_get_subnode(s,"readlink",0,1);
		statsptr[OP_READLINK_MASTER] = stats_get_subnode(rl,"master",0,1);
		statsptr[OP_READLINK_CACHED] = stats_get_subnode(rl,"cached",0,1);
	}
	statsptr[OP_SYMLINK] = stats_get_subnode(s,"symlink",0,1);
	statsptr[OP_RMDIR] = stats_get_subnode(s,"rmdir",0,1);
	statsptr[OP_MKDIR] = stats_get_subnode(s,"mkdir",0,1);
	statsptr[OP_UNLINK] = stats_get_subnode(s,"unlink",0,1);
	statsptr[OP_MKNOD] = stats_get_subnode(s,"mknod",0,1);
	statsptr[OP_SETATTR] = stats_get_subnode(s,"setattr",0,1);
	statsptr[OP_GETATTR] = stats_get_subnode(s,"getattr",0,1);
	statsptr[OP_DIRCACHE_GETATTR] = stats_get_subnode(s,"getattr-cached",0,1);
	{
		void *l,*cl,*nl;
		l = stats_get_subnode(s,"lookup",0,1);
		cl = stats_get_subnode(l,"cached",0,1);
		nl = stats_get_subnode(l,"master",0,1);
		statsptr[OP_LOOKUP_INTERNAL] = stats_get_subnode(l,"internal",0,1);
		statsptr[OP_POSLOOKUP] = stats_get_subnode(nl,"positive",0,1);
		statsptr[OP_NEGLOOKUP] = stats_get_subnode(nl,"negative",0,1);
		statsptr[OP_ERRLOOKUP] = stats_get_subnode(nl,"error",0,1);
		if (usedircache) {
			statsptr[OP_DIRCACHE_LOOKUP] = stats_get_subnode(cl,"readdir",0,1);
		}
		statsptr[OP_NEGCACHE_LOOKUP] = stats_get_subnode(cl,"negative",0,1);
	}
	statsptr[OP_ACCESS] = stats_get_subnode(s,"access",0,1);
	statsptr[OP_STATFS] = stats_get_subnode(s,"statfs",0,1);
	{
		void *rd;
		rd = stats_get_subnode(s,"readdir",0,1);
		if (usedircache) {
			statsptr[OP_GETDIR_FULL] = stats_get_subnode(rd,"with_attrs",0,1);
		}
		statsptr[OP_GETDIR_SMALL] = stats_get_subnode(rd,"without_attrs",0,1);
#if FUSE_VERSION >= 30
		statsptr[OP_GETDIR_PLUS] = stats_get_subnode(rd,"with_attrs+",0,1);
#endif
	}
}

void mfs_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_counter_inc(statsptr[id]);
	}
}

static pthread_key_t aclstorage;

void mfs_aclstorage_free(void *ptr) {
	if (ptr!=NULL) {
		free(ptr);
	}
}

void mfs_aclstorage_init(void) {
	zassert(pthread_key_create(&aclstorage,mfs_aclstorage_free));
	zassert(pthread_setspecific(aclstorage,NULL));
}

void* mfs_aclstorage_get(uint32_t size) {
	uint8_t *buff,*p;
	const uint8_t *cp;
	uint32_t s;
	buff = pthread_getspecific(aclstorage);
	if (buff!=NULL) {
		cp = p = buff;
		s = get32bit(&cp);
		if (size<=s) {
			return (buff+4);
		}
		free(buff);
	}
	buff = malloc(size+4);
	passert(buff);
	p = buff;
	put32bit(&p,size);
	zassert(pthread_setspecific(aclstorage,buff));
	return p;
}

#ifndef EDQUOT
# define EDQUOT ENOSPC
#endif
#ifndef ENOATTR
# ifdef ENODATA
#  define ENOATTR ENODATA
# else
#  define ENOATTR ENOENT
# endif
#endif

static int mfs_errorconv(int status) {
	int ret;
	switch (status) {
	case MFS_STATUS_OK:
		ret=0;
		break;
	case MFS_ERROR_EPERM:
		ret=EPERM;
		break;
	case MFS_ERROR_ENOTDIR:
		ret=ENOTDIR;
		break;
	case MFS_ERROR_ENOENT:
		ret=ENOENT;
		break;
	case MFS_ERROR_EACCES:
		ret=EACCES;
		break;
	case MFS_ERROR_EEXIST:
		ret=EEXIST;
		break;
	case MFS_ERROR_EINVAL:
		ret=EINVAL;
		break;
	case MFS_ERROR_ENOTEMPTY:
		ret=ENOTEMPTY;
		break;
	case MFS_ERROR_IO:
		ret=EIO;
		break;
	case MFS_ERROR_EROFS:
		ret=EROFS;
		break;
	case MFS_ERROR_EINTR:
		ret=EINTR;
		break;
	case MFS_ERROR_EAGAIN:
		ret=EAGAIN;
		break;
	case MFS_ERROR_ECANCELED:
		ret=ECANCELED;
		break;
	case MFS_ERROR_QUOTA:
		ret=EDQUOT;
		break;
	case MFS_ERROR_ENOATTR:
		ret=ENOATTR;
		break;
	case MFS_ERROR_ENOTSUP:
		ret=ENOTSUP;
		break;
	case MFS_ERROR_ERANGE:
		ret=ERANGE;
		break;
	case MFS_ERROR_NOSPACE:
		ret=ENOSPC;
		break;
	case MFS_ERROR_CHUNKLOST:
		ret=ENXIO;
		break;
	case MFS_ERROR_NOCHUNKSERVERS:
		ret=ENOSPC;
		break;
	case MFS_ERROR_CSNOTPRESENT:
		ret=ENXIO;
		break;
	case MFS_ERROR_NOTOPENED:
		ret=EBADF;
		break;
	default:
		ret=EINVAL;
		break;
	}
	if (debug_mode && ret!=0) {
#ifdef HAVE_STRERROR_R
		char errorbuff[500];
# ifdef STRERROR_R_CHAR_P
		fprintf(stderr,"status: %s\n",strerror_r(ret,errorbuff,500));
# else
		strerror_r(ret,errorbuff,500);
		fprintf(stderr,"status: %s\n",errorbuff);
# endif
#else
# ifdef HAVE_PERROR
		errno=ret;
		perror("status: ");
# else
		fprintf(stderr,"status: %d\n",ret);
# endif
#endif
	}
	return ret;
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

static void mfs_type_to_stat(uint32_t inode,uint8_t type, struct stat *stbuf) {
	memset(stbuf,0,sizeof(struct stat));
	stbuf->st_ino = inode;
	switch (type&0x7F) {
	case DISP_TYPE_DIRECTORY:
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR;
		break;
	case DISP_TYPE_SYMLINK:
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK;
		break;
	case DISP_TYPE_FILE:
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG;
		break;
	case DISP_TYPE_FIFO:
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO;
		break;
	case DISP_TYPE_SOCKET:
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK;
		break;
	case DISP_TYPE_BLOCKDEV:
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK;
		break;
	case DISP_TYPE_CHARDEV:
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR;
		break;
	default:
		stbuf->st_mode = 0;
	}
}

static inline uint8_t mfs_attr_get_type(const uint8_t attr[ATTR_RECORD_SIZE]) {
	if (attr[0]<64) { // 1.7.29 and up
		return (attr[1]>>4);
	} else {
		return fsnodes_type_convert(attr[0]&0x7F);
	}
}

static inline uint8_t mfs_attr_get_mattr(const uint8_t attr[ATTR_RECORD_SIZE]) {
	if (attr[0]<64) { // 1.7.29 and up
		return attr[0];
	} else {
		return (attr[1]>>4);
	}
}

static inline uint64_t mfs_attr_get_fleng(const uint8_t attr[ATTR_RECORD_SIZE]) {
	const uint8_t *ptr;
	ptr = attr+27;
	return get64bit(&ptr);
}

static inline void mfs_attr_set_fleng(uint8_t attr[ATTR_RECORD_SIZE],uint64_t fleng) {
	uint8_t *ptr;
	ptr = attr+27;
	put64bit(&ptr,fleng);
}

static void mfs_attr_modify(uint32_t to_set,uint8_t attr[ATTR_RECORD_SIZE],struct stat *stbuf) {
	uint8_t mattr;
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime;
	const uint8_t *ptr;
	uint8_t *wptr;
	ptr = attr;
	if (attr[0]<64) { // 1.7.29 and up
		mattr = get8bit(&ptr);
		attrmode = get16bit(&ptr);
		attrtype = (attrmode>>12);
	} else {
		attrtype = get8bit(&ptr);
		attrtype = fsnodes_type_convert(attrtype&0x7F);
		attrmode = get16bit(&ptr);
		mattr = attrmode >> 12;
	}
	attrmode &= 0x0FFF;
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);
	attratime = get32bit(&ptr);
	attrmtime = get32bit(&ptr);
	attrctime = get32bit(&ptr);
	if (to_set & FUSE_SET_ATTR_MODE) {
		attrmode = stbuf->st_mode & 07777;
		attrctime = time(NULL);
	}
	if (to_set & FUSE_SET_ATTR_UID) {
		attruid = stbuf->st_uid;
		attrctime = time(NULL);
	}
	if (to_set & FUSE_SET_ATTR_GID) {
		attrgid = stbuf->st_gid;
		attrctime = time(NULL);
	}
	if (to_set & FUSE_SET_ATTR_ATIME) {
		attratime = stbuf->st_atime;
		attrctime = time(NULL);
	}
	if (to_set & FUSE_SET_ATTR_MTIME) {
		attratime = stbuf->st_mtime;
		attrctime = time(NULL);
	}
	wptr = attr;
	put8bit(&wptr,mattr);
	attrmode |= ((uint16_t)attrtype)<<12;
	put16bit(&wptr,attrmode);
	put32bit(&wptr,attruid);
	put32bit(&wptr,attrgid);
	put32bit(&wptr,attratime);
	put32bit(&wptr,attrmtime);
	put32bit(&wptr,attrctime);
}

static void mfs_attr_to_stat(uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE], struct stat *stbuf) {
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime,attrnlink,attrrdev;
	uint64_t attrlength;
	const uint8_t *ptr;
	ptr = attr;
	if (attr[0]<64) { // 1.7.29 and up
		ptr++;
		attrmode = get16bit(&ptr);
		attrtype = (attrmode>>12);
	} else {
		attrtype = get8bit(&ptr);
		attrtype = fsnodes_type_convert(attrtype&0x7F);
		attrmode = get16bit(&ptr);
	}
	attrmode &= 0x0FFF;
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);
	attratime = get32bit(&ptr);
	attrmtime = get32bit(&ptr);
	attrctime = get32bit(&ptr);
	attrnlink = get32bit(&ptr);
	stbuf->st_ino = inode;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	stbuf->st_blksize = MFSBLOCKSIZE;
#endif
	switch (attrtype & 0x7F) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR | attrmode;
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK | attrmode;
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG | attrmode;
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO | attrmode;
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK | attrmode;
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK | attrmode;
		attrrdev = get32bit(&ptr);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR | attrmode;
		attrrdev = get32bit(&ptr);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	default:
		stbuf->st_mode = 0;
	}
	stbuf->st_uid = attruid;
	stbuf->st_gid = attrgid;
	stbuf->st_atime = attratime;
	stbuf->st_mtime = attrmtime;
	stbuf->st_ctime = attrctime;
#ifdef HAVE_STRUCT_STAT_ST_BIRTHTIME
	stbuf->st_birthtime = attrctime;	// for future use
#endif
	stbuf->st_nlink = attrnlink;
}

static inline void mfs_makemodestr(char modestr[11],uint16_t mode) {
	uint32_t i;
	strcpy(modestr,"?rwxrwxrwx");
	switch (mode & S_IFMT) {
	case S_IFSOCK:
		modestr[0] = 's';
		break;
	case S_IFLNK:
		modestr[0] = 'l';
		break;
	case S_IFREG:
		modestr[0] = '-';
		break;
	case S_IFBLK:
		modestr[0] = 'b';
		break;
	case S_IFDIR:
		modestr[0] = 'd';
		break;
	case S_IFCHR:
		modestr[0] = 'c';
		break;
	case S_IFIFO:
		modestr[0] = 'f';
		break;
	}
	if (mode & S_ISUID) {
		modestr[3] = 's';
	}
	if (mode & S_ISGID) {
		modestr[6] = 's';
	}
	if (mode & S_ISVTX) {
		modestr[9] = 't';
	}
	for (i=0 ; i<9 ; i++) {
		if ((mode & (1<<i))==0) {
			if (modestr[9-i]=='s' || modestr[9-i]=='t') {
				modestr[9-i]&=0xDF;
			} else {
				modestr[9-i]='-';
			}
		}
	}
}

static void mfs_makeattrstr(char *buff,uint32_t size,struct stat *stbuf) {
	char modestr[11];
	mfs_makemodestr(modestr,stbuf->st_mode);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
	if (modestr[0]=='b' || modestr[0]=='c') {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%llu,%08lX]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(unsigned long long int)(stbuf->st_size),(unsigned long int)(stbuf->st_rdev));
	} else {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%llu]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(unsigned long long int)(stbuf->st_size));
	}
#else
	snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%llu]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(unsigned long long int)(stbuf->st_size));
#endif
}

#if FUSE_USE_VERSION >= 26
void mfs_statfs(fuse_req_t req,fuse_ino_t ino) {
#else
void mfs_statfs(fuse_req_t req) {
#endif
	uint64_t totalspace,availspace,freespace,trashspace,sustainedspace;
	uint32_t inodes;
	uint32_t bsize;
	struct statvfs stfsbuf;
	memset(&stfsbuf,0,sizeof(stfsbuf));
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_STATFS);
	if (debug_mode) {
#if FUSE_USE_VERSION >= 26
		oplog_printf(&ctx,"statfs (%lu)",(unsigned long int)ino);
#else
		oplog_printf(&ctx,"statfs ()");
#endif
	}
#if FUSE_USE_VERSION >= 26
	(void)ino;
#endif
	fs_statfs(&totalspace,&availspace,&freespace,&trashspace,&sustainedspace,&inodes);

#if defined(__APPLE__)
	if (totalspace>0x0001000000000000ULL) {
		bsize = 0x20000;
	} else {
		bsize = 0x10000;
	}
#else
	bsize = 0x10000;
#endif

	stfsbuf.f_namemax = MFS_NAME_MAX;
	stfsbuf.f_frsize = bsize;
	stfsbuf.f_bsize = bsize;
#if defined(__APPLE__)
	// FUSE on apple (or other parts of kernel) expects 32-bit values, so it's better to saturate this values than let being cut on 32-bit
	// can't change bsize also because 64k seems to be the biggest acceptable value for bsize

	if (totalspace/bsize>0xFFFFFFFFU) {
		stfsbuf.f_blocks = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_blocks = totalspace/bsize;
	}
	if (availspace/bsize>0xFFFFFFFFU) {
		stfsbuf.f_bavail = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_bavail = availspace/bsize;
	}
	if (freespace/bsize>0xFFFFFFFFU) {
		stfsbuf.f_bfree = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_bfree = freespace/bsize;
	}
#else
	stfsbuf.f_blocks = totalspace/bsize;
	stfsbuf.f_bfree = freespace/bsize;
	stfsbuf.f_bavail = availspace/bsize;
#endif
	stfsbuf.f_files = 1000000000+PKGVERSION+inodes;
	stfsbuf.f_ffree = 1000000000+PKGVERSION;
	stfsbuf.f_favail = 1000000000+PKGVERSION;
	//stfsbuf.f_flag = ST_RDONLY;
#if FUSE_USE_VERSION >= 26
	oplog_printf(&ctx,"statfs (%lu): OK (%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",(unsigned long int)ino,totalspace,availspace,freespace,trashspace,sustainedspace,inodes);
#else
	oplog_printf(&ctx,"statfs (): OK (%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",totalspace,availspace,freespace,trashspace,sustainedspace,inodes);
#endif
	fuse_reply_statfs(req,&stfsbuf);
}

/*
static int mfs_node_access(uint8_t attr[32],uint32_t uid,uint32_t gid,int mask) {
	uint32_t emode,mmode;
	uint32_t attruid,attrgid;
	uint16_t attrmode;
	uint8_t *ptr;
	if (uid == 0) {
		return 1;
	}
	ptr = attr+2;
	attrmode = get16bit(&ptr);
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);
	if (uid == attruid) {
		emode = (attrmode & 0700) >> 6;
	} else if (gid == attrgid) {
		emode = (attrmode & 0070) >> 3;
	} else {
		emode = attrmode & 0007;
	}
	mmode = 0;
	if (mask & R_OK) {
		mmode |= 4;
	}
	if (mask & W_OK) {
		mmode |= 2;
	}
	if (mask & X_OK) {
		mmode |= 1;
	}
	if ((emode & mmode) == mmode) {
		return 1;
	}
	return 0;
}
*/

// simple access test for deleted cwd nodes - no ACL's
int mfs_access_test(const uint8_t attr[ATTR_RECORD_SIZE],int mmode,uint32_t uid,uint32_t gidcnt,uint32_t *gidtab) {
	uint8_t modebits,gok;
	uint16_t attrmode;
	uint32_t attruid,attrgid;
	const uint8_t *ptr;

	if (uid==0) {
		return 0;
	}
	ptr = attr+1;
	attrmode = get16bit(&ptr);
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);

	modebits = 0; // makes cppcheck happy
	if (uid == attruid) {
		modebits = (attrmode >> 6) & 7;
	} else {
		gok = 0;
		while (gidcnt>0) {
			gidcnt--;
			if (gidtab[gidcnt] == attrgid) {
				modebits = (attrmode >> 3) & 7;
				gok = 1;
				break;
			}
		}
		if (gok==0) {
			modebits = attrmode & 7;
		}
	}
	if ((mmode & modebits) == mmode) {
		return 0;
	}
	return EACCES;
}

void mfs_access(fuse_req_t req, fuse_ino_t ino, int mask) {
	int status;
	struct fuse_ctx ctx;
	uint8_t attr[ATTR_RECORD_SIZE];
	groups *gids;
	int mmode;
	uint16_t lflags;
	int force_mode;

	ctx = *(fuse_req_ctx(req));
	if (debug_mode) {
		oplog_printf(&ctx,"access (%lu,0x%X) ...",(unsigned long int)ino,mask);
		fprintf(stderr,"access (%lu,0x%X)\n",(unsigned long int)ino,mask);
	}
	mfs_stats_inc(OP_ACCESS);
#if (R_OK==MODE_MASK_R) && (W_OK==MODE_MASK_W) && (X_OK==MODE_MASK_X)
	mmode = mask;
#else
	mmode = 0;
	if (mask & R_OK) {
		mmode |= MODE_MASK_R;
	}
	if (mask & W_OK) {
		mmode |= MODE_MASK_W;
	}
	if (mask & X_OK) {
		mmode |= MODE_MASK_X;
	}
#endif
	if (IS_SPECIAL_INODE(ino)) {
		if (mask & (W_OK | X_OK)) {
			fuse_reply_err(req,EACCES);
		} else {
			fuse_reply_err(req,0);
		}
		return;
	}

	if (fdcache_find(&ctx,ino,NULL,&lflags)) {
		if ((lflags & LOOKUP_RO_FILESYSTEM) && (mmode & MODE_MASK_W)) {
			status = MFS_ERROR_EROFS;
		} else {
			status = (lflags & (1<<(mmode&0x7)))?MFS_STATUS_OK:MFS_ERROR_EACCES;
		}
	} else {
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_access(ino,ctx.uid,gids->gidcnt,gids->gidtab,mmode);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_access(ino,ctx.uid,1,&gidtmp,mmode);
		}
	}
	force_mode = 0;
	if (status==MFS_ERROR_ENOENT) {
		if (ctx.pid == getpid()) {
			force_mode = 1;
		}
		if (sstats_get(ino,attr,force_mode)==MFS_STATUS_OK) {
			if (force_mode==0) {
				force_mode = 2;
			}
		}
	}
	if (force_mode) {
		if (force_mode == 1) {
			if (debug_mode) {
				fprintf(stderr,"special case: internal access (%lu,0x%X) - positive answer\n",(unsigned long int)ino,mask);
			}
			oplog_printf(&ctx,"special case: internal access (%lu,0x%X): OK",(unsigned long int)ino,mask);
			status = 0;
		} else {
			if (debug_mode) {
				fprintf(stderr,"special case: sustained access (%lu,0x%X) - using stored data\n",(unsigned long int)ino,mask);
			}
			if (full_permissions) {
				gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
				status = mfs_access_test(attr,mmode,ctx.uid,gids->gidcnt,gids->gidtab);
				groups_rel(gids);
			} else {
				uint32_t gidtmp = ctx.gid;
				status = mfs_access_test(attr,mmode,ctx.uid,1,&gidtmp);
			}
		}
	} else {
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"access (%lu,0x%X): %s",(unsigned long int)ino,mask,strerr(status));
		} else {
			oplog_printf(&ctx,"access (%lu,0x%X): OK",(unsigned long int)ino,mask);
		}
	}
	fuse_reply_err(req,status);
}

void mfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint64_t maxfleng;
	uint32_t inode;
	uint32_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t csdataver;
	uint16_t lflags;
	uint64_t chunkid;
	uint32_t version;
	const uint8_t *csdata;
	uint32_t csdatasize;
	char attrstr[256];
	uint8_t mattr,type;
	uint8_t icacheflag;
	int status,nocache;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	if (debug_mode) {
		oplog_printf(&ctx,"lookup (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"lookup (%lu,%s)\n",(unsigned long int)parent,name);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		mfs_stats_inc(OP_ERRLOOKUP);
		oplog_printf(&ctx,"lookup (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}
	if (parent==FUSE_ROOT_ID) {
		if (nleng==2 && name[0]=='.' && name[1]=='.') {
			nleng=1;
		}
		if (strcmp(name,MASTERINFO_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = MASTERINFO_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(MASTERINFO_INODE,masterinfoattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: MASTERINFO): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			fuse_reply_entry(req, &e);
			return ;
		}
		if (strcmp(name,STATS_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = STATS_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(STATS_INODE,statsattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: STATS): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			fuse_reply_entry(req, &e);
			return ;
		}
		if (strcmp(name,PARAMS_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = PARAMS_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(PARAMS_INODE,paramsattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: PARAMS): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			fuse_reply_entry(req, &e);
			return ;
		}
		if (strcmp(name,RANDOM_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = RANDOM_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(RANDOM_INODE,randomattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: RANDOM): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			fuse_reply_entry(req, &e);
			return ;
		}
		if (strcmp(name,MOOSE_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = MOOSE_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(MOOSE_INODE,mooseattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: MOOSE): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			fuse_reply_entry(req, &e);
			return ;
		}
		if (strcmp(name,OPLOG_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = OPLOG_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(OPLOG_INODE,oplogattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: OPLOG): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			fuse_reply_entry(req, &e);
			return ;
		}
		if (strcmp(name,OPHISTORY_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = OPHISTORY_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(OPHISTORY_INODE,oplogattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: OPHISTORY): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			fuse_reply_entry(req, &e);
			return ;
		}
/*
		if (strcmp(name,ATTRCACHE_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = ATTRCACHE_INODE;
			e.generation = 1;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(ATTRCACHE_INODE,attrcacheattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			oplog_printf(&ctx,"lookup (%lu,%s) (internal node: ATTRCACHE)",(unsigned long int)parent,name);
			fuse_reply_entry(req, &e);
			return ;
		}
*/
	}
/*
	if (newdircache) {
		const uint8_t *dbuff;
		uint32_t dsize;
		switch (dir_cache_lookup(parent,nleng,(const uint8_t*)name,&inode,attr)) {
			case -1:
				mfs_stats_inc(OP_DIRCACHE_LOOKUP_NEGATIVE);
				oplog_printf(&ctx,"lookup (%lu,%s) (cached answer: %s)",(unsigned long int)parent,name,strerr(ENOENT));
				fuse_reply_err(req,ENOENT);
				return;
			case 1:
				mfs_stats_inc(OP_DIRCACHE_LOOKUP_POSITIVE);
				status = 0;
				oplog_printf(&ctx,"lookup (%lu,%s) (cached answer: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
				break;
			case -2:
				mfs_stats_inc(OP_LOOKUP);
				status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid,&inode,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					oplog_printf(&ctx,"lookup (%lu,%s) (lookup forced by cache: %s)",(unsigned long int)parent,name,strerr(status));
				} else {
					oplog_printf(&ctx,"lookup (%lu,%s) (lookup forced by cache: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
				}
				break;
			case -3:
				mfs_stats_inc(OP_DIRCACHE_LOOKUP_NOATTR);
				status = fs_getattr(inode,ctx.uid,ctx.gid,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					oplog_printf(&ctx,"lookup (%lu,%s) (getattr forced by cache: %s)",(unsigned long int)parent,name,strerr(status));
				} else {
					oplog_printf(&ctx,"lookup (%lu,%s) (getattr forced by cache: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
				}
				break;
			default:
				status = fs_getdir_plus(parent,ctx.uid,ctx.gid,1,&dbuff,&dsize);
				status = mfs_errorconv(status);
				if (status!=0) {
					oplog_printf(&ctx,"lookup (%lu,%s) (readdir: %s)",(unsigned long int)parent,name,strerr(status));
					fuse_reply_err(req, status);
					return;
				}
				mfs_stats_inc(OP_GETDIR_FULL);
				dir_cache_newdirdata(parent,dsize,dbuff);
				switch (dir_cache_lookup(parent,nleng,(const uint8_t*)name,&inode,attr)) {
					case -1:
						mfs_stats_inc(OP_DIRCACHE_LOOKUP_NEGATIVE);
						oplog_printf(&ctx,"lookup (%lu,%s) (after readdir cached answer: %s)",(unsigned long int)parent,name,strerr(ENOENT));
						fuse_reply_err(req,ENOENT);
						return;
					case 1:
						mfs_stats_inc(OP_DIRCACHE_LOOKUP_POSITIVE);
						oplog_printf(&ctx,"lookup (%lu,%s) (after readdir cached answer: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
						break;
					default:
						mfs_stats_inc(OP_LOOKUP);
						status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid,&inode,attr);
						status = mfs_errorconv(status);
						if (status!=0) {
							oplog_printf(&ctx,"lookup (%lu,%s) (after readdir lookup forced by cache: %s)",(unsigned long int)parent,name,strerr(status));
						} else {
							oplog_printf(&ctx,"lookup (%lu,%s) (after readdir lookup forced by cache: %lu)",(unsigned long int)parent,name,(unsigned long int)inode);
						}
				}
		}
	} else 
*/
	if (usedircache && dcache_lookup(&ctx,parent,nleng,(const uint8_t*)name,&inode,attr)) {
		if (debug_mode) {
			fprintf(stderr,"lookup: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_LOOKUP);
		status = 0;
		lflags = 0xFFFF;
		icacheflag = 1;
//		oplog_printf(&ctx,"lookup (%lu,%s) (using open dir cache): OK (%lu)",(unsigned long int)parent,name,(unsigned long int)inode);
	} else {
		if (negentry_cache_search(parent,nleng,(const uint8_t*)name)) {
			if (debug_mode) {
				fprintf(stderr,"lookup: sending data from negcache\n");
			}
			oplog_printf(&ctx,"lookup (%lu,%s) (using negative entry cache): %s",(unsigned long int)parent,name,strerr(ENOENT));
			mfs_stats_inc(OP_NEGCACHE_LOOKUP);
			fuse_reply_err(req,ENOENT);
			return;
		}
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,gids->gidcnt,gids->gidtab,&inode,attr,&lflags,&csdataver,&chunkid,&version,&csdata,&csdatasize);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,1,&gidtmp,&inode,attr,&lflags,&csdataver,&chunkid,&version,&csdata,&csdatasize);
		}
		if (status==MFS_ERROR_ENOENT_NOCACHE) {
			status = MFS_ERROR_ENOENT;
			nocache = 1;
		} else {
			nocache = 0;
		}
		status = mfs_errorconv(status);
		icacheflag = 0;
		if (status==0) {
			mfs_stats_inc(OP_POSLOOKUP);
		} else if (status==ENOENT) {
			if (strcmp(name,MASTERINFO_NAME)==0) {
				memset(&e, 0, sizeof(e));
				e.ino = MASTERINFO_INODE;
				e.generation = 1;
				e.attr_timeout = 3600.0;
				e.entry_timeout = 3600.0;
				mfs_attr_to_stat(MASTERINFO_INODE,masterinfoattr,&e.attr);
				mfs_stats_inc(OP_LOOKUP_INTERNAL);
				mfs_makeattrstr(attrstr,256,&e.attr);
				oplog_printf(&ctx,"lookup (%lu,%s) (internal node: MASTERINFO): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
				fuse_reply_entry(req, &e);
				return ;
			} else {
				mfs_stats_inc(OP_NEGLOOKUP);
				if (nocache==0) {
					negentry_cache_insert(parent,nleng,(const uint8_t*)name);
				}
			}
		} else {
			mfs_stats_inc(OP_ERRLOOKUP);
		}
	}
	if (status!=0) {
		oplog_printf(&ctx,"lookup (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		fuse_reply_err(req, status);
		return;
	}
	type = mfs_attr_get_type(attr);
	if (type==TYPE_FILE) {
		maxfleng = write_data_inode_getmaxfleng(inode);
	} else {
		maxfleng = 0;
	}
	if (type==TYPE_DIRECTORY) {
		sstats_set(inode,attr,1);
	}
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	e.generation = 1;
	mattr = mfs_attr_get_mattr(attr);
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:((type==TYPE_DIRECTORY)?direntry_cache_timeout:entry_cache_timeout);
	mfs_attr_to_stat(inode,attr,&e.attr);
	if (maxfleng>(uint64_t)(e.attr.st_size)) {
		e.attr.st_size=maxfleng;
		mfs_attr_set_fleng(attr,maxfleng);
	}
	if (lflags!=0xFFFF) { // store extra data in cache
		fdcache_insert(&ctx,inode,attr,lflags,csdataver,chunkid,version,csdata,csdatasize);
	}
	if (type==TYPE_FILE) {
		read_inode_set_length_passive(inode,e.attr.st_size);
		finfo_change_fleng(inode,e.attr.st_size);
	}
	fs_fix_amtime(inode,&(e.attr.st_atime),&(e.attr.st_mtime));
//	if (type==TYPE_FILE && debug_mode) {
//		fprintf(stderr,"lookup inode %lu - file size: %llu\n",(unsigned long int)inode,(unsigned long long int)e.attr.st_size);
//	}
	mfs_makeattrstr(attrstr,256,&e.attr);
	oplog_printf(&ctx,"lookup (%lu,%s)%s: OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,icacheflag?" (using open dir cache)":"",e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
	fuse_reply_entry(req, &e);
	if (debug_mode) {
		fprintf(stderr,"lookup: positive answer timeouts (attr:%.3lf,entry:%.3lf)\n",e.attr_timeout,e.entry_timeout);
	}
}

void mfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint64_t maxfleng;
	double attr_timeout;
	struct stat o_stbuf;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t type;
	char attrstr[256];
	int status;
	uint8_t icacheflag;
	struct fuse_ctx ctx;
	int force_mode;
	(void)fi;

	ctx = *(fuse_req_ctx(req));
//	mfs_stats_inc(OP_GETATTR);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"getattr (%lu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"getattr (%lu) ...",(unsigned long int)ino);
		}
		fprintf(stderr,"getattr (%lu)\n",(unsigned long int)ino);
	}
//	if (ino==MASTER_INODE) {
//		memset(&o_stbuf, 0, sizeof(struct stat));
//		mfs_attr_to_stat(ino,masterattr,&o_stbuf);
//		fuse_reply_attr(req, &o_stbuf, 3600.0);
//		mfs_stats_inc(OP_GETATTR);
//		return;
//	}
	if (ino==MASTERINFO_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,masterinfoattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"getattr (%lu) (internal node: MASTERINFO): OK (3600,%s)",(unsigned long int)ino,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"getattr (%lu) (internal node: STATS): OK (3600,%s)",(unsigned long int)ino,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==PARAMS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,paramsattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"getattr (%lu) (internal node: PARAMS): OK (3600,%s)",(unsigned long int)ino,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==RANDOM_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,randomattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"getattr (%lu) (internal node: RANDOM): OK (3600,%s)",(unsigned long int)ino,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==MOOSE_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,mooseattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"getattr (%lu) (internal node: MOOSE): OK (3600,%s)",(unsigned long int)ino,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,oplogattr,&o_stbuf);
//		if (fi && fi->fh) {
//			uint64_t *posptr = (uint64_t*)(unsigned long)(fi->fh);
//			o_stbuf.st_size = (*posptr)+oplog_getpos();
//		}
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"getattr (%lu) (internal node: %s): OK (3600,%s)",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	force_mode = 0;
	if (usedircache && dcache_getattr(&ctx,ino,attr)) {
		if (debug_mode) {
			fprintf(stderr,"getattr: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_GETATTR);
		status = 0;
		icacheflag = 1;
	} else {
		mfs_stats_inc(OP_GETATTR);
		if (fdcache_find(&ctx,ino,attr,NULL)) {
			status = MFS_STATUS_OK;
		} else {
			status = fs_getattr(ino,(fi!=NULL)?1:0,ctx.uid,ctx.gid,attr);
		}
		if (status==MFS_ERROR_ENOENT) {
			if (ctx.pid==getpid()) {
				force_mode = 1;
			}
			status = sstats_get(ino,attr,force_mode);
			if (status==MFS_STATUS_OK && force_mode==0) {
				force_mode = 2;
			}
		}
		status = mfs_errorconv(status);
		icacheflag = 0;
	}
	if (status!=0) {
		oplog_printf(&ctx,"getattr (%lu): %s",(unsigned long int)ino,strerr(status));
		fuse_reply_err(req, status);
		return;
	}
	type = mfs_attr_get_type(attr);
	if (type==TYPE_FILE) {
		maxfleng = write_data_inode_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	if (type==TYPE_DIRECTORY && force_mode==0) {
		sstats_set(ino,attr,1);
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
		mfs_attr_set_fleng(attr,maxfleng);
	}
	if (type==TYPE_FILE) {
		read_inode_set_length_passive(ino,o_stbuf.st_size);
		finfo_change_fleng(ino,o_stbuf.st_size);
		fdcache_invalidate(ino);
	}
	fs_fix_amtime(ino,&(o_stbuf.st_atime),&(o_stbuf.st_mtime));
	attr_timeout = ((mfs_attr_get_mattr(attr)&MATTR_NOACACHE) || force_mode)?0.0:attr_cache_timeout;
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(&ctx,"getattr (%lu)%s: OK (%.1lf,%s)",(unsigned long int)ino,icacheflag?" (using open dir cache)":(force_mode==1)?" (internal getattr)":(force_mode==2)?" (sustained nodes)":"",attr_timeout,attrstr);
	fuse_reply_attr(req, &o_stbuf, attr_timeout);
}

void mfs_make_setattr_str(char *strbuff,uint32_t strsize,struct stat *stbuf,int to_set) {
	uint32_t strleng = 0;
	char modestr[11];
	if (strleng<strsize && (to_set&FUSE_SET_ATTR_MODE)) {
		mfs_makemodestr(modestr,stbuf->st_mode);
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"mode=%s:0%04o;",modestr+1,(unsigned int)(stbuf->st_mode & 07777));
	}
	if (strleng<strsize && (to_set&FUSE_SET_ATTR_UID)) {
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"uid=%ld;",(long int)(stbuf->st_uid));
	}
	if (strleng<strsize && (to_set&FUSE_SET_ATTR_GID)) {
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"gid=%ld;",(long int)(stbuf->st_gid));
	}
#if defined(FUSE_SET_ATTR_ATIME_NOW)
	if (strleng<strsize && ((to_set & FUSE_SET_ATTR_ATIME_NOW) || ((to_set & FUSE_SET_ATTR_ATIME) && stbuf->st_atime<0))) {
#else
	if (strleng<strsize && ((to_set & FUSE_SET_ATTR_ATIME) && stbuf->st_atime<0)) {
#endif
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"atime=NOW;");
	} else if (strleng<strsize && (to_set&FUSE_SET_ATTR_ATIME)) {
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"atime=%lu;",(unsigned long int)(stbuf->st_atime));
	}
#if defined(FUSE_SET_ATTR_MTIME_NOW)
	if (strleng<strsize && ((to_set & FUSE_SET_ATTR_MTIME_NOW) || ((to_set & FUSE_SET_ATTR_MTIME) && stbuf->st_mtime<0))) {
#else
	if (strleng<strsize && ((to_set & FUSE_SET_ATTR_MTIME) && stbuf->st_mtime<0)) {
#endif
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"mtime=NOW;");
	} else if (strleng<strsize && (to_set&FUSE_SET_ATTR_MTIME)) {
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"mtime=%lu;",(unsigned long int)(stbuf->st_mtime));
	}
	if (strleng<strsize && (to_set&FUSE_SET_ATTR_SIZE)) {
		strleng+=snprintf(strbuff+strleng,strsize-strleng,"size=%llu;",(unsigned long long int)(stbuf->st_size));
	}
	if (strleng>0) {
		strleng--;
	}
	strbuff[strleng]='\0';
}

void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	uint64_t maxfleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	char setattr_str[150];
	char attrstr[256];
	double attr_timeout;
	int status;
	struct fuse_ctx ctx;
	groups *gids;
	uint8_t setmask = 0;

	ctx = *(fuse_req_ctx(req));
	mfs_make_setattr_str(setattr_str,150,stbuf,to_set);
	mfs_stats_inc(OP_SETATTR);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]) [handle:%08"PRIX32"] ...",(unsigned long int)ino,to_set,setattr_str,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]) ...",(unsigned long int)ino,to_set,setattr_str);
		}
		fprintf(stderr,"setattr (%lu,0x%X,[%s])\n",(unsigned long int)ino,to_set,setattr_str);
	}
	if (ino==MASTERINFO_INODE) {
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): %s",(unsigned long int)ino,to_set,setattr_str,strerr(EPERM));
		fuse_reply_err(req, EPERM);
		return;
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]) (internal node: STATS): OK (3600,%s)",(unsigned long int)ino,to_set,setattr_str,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==PARAMS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,paramsattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]) (internal node: PARAMS): OK (3600,%s)",(unsigned long int)ino,to_set,setattr_str,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==RANDOM_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,randomattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]) (internal node: RANDOM): OK (3600,%s)",(unsigned long int)ino,to_set,setattr_str,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==MOOSE_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,mooseattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]) (internal node: MOOSE): OK (3600,%s)",(unsigned long int)ino,to_set,setattr_str,attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,oplogattr,&o_stbuf);
//		if (fi && fi->fh) {
//			uint64_t *posptr = (uint64_t*)(unsigned long)(fi->fh);
//			o_stbuf.st_size = (*posptr)+oplog_getpos();
//		}
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]) (internal node: %s): OK (3600,%s)",(unsigned long int)ino,to_set,setattr_str,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,attrcacheattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
*/
	status = EINVAL;
#if defined(FUSE_SET_ATTR_ATIME_NOW) && defined(FUSE_SET_ATTR_MTIME_NOW)
	if ((to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME|FUSE_SET_ATTR_SIZE|FUSE_SET_ATTR_ATIME_NOW|FUSE_SET_ATTR_MTIME_NOW)) == 0) {
#else
	if ((to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME|FUSE_SET_ATTR_SIZE)) == 0) {	// change other flags or change nothing
#endif
//		status = fs_getattr(ino,ctx.uid,ctx.gid,attr);
		// ext3 compatibility - change ctime during this operation (usually chown(-1,-1))
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,gids->gidcnt,gids->gidtab,0,0,0,0,0,0,0,0,attr);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,1,&gidtmp,0,0,0,0,0,0,0,0,attr);
		}
		if (status==MFS_ERROR_ENOENT) {
			status = sstats_get(ino,attr,0);
			if (status==MFS_STATUS_OK) {
				mfs_attr_modify(to_set,attr,stbuf);
			}
		}
		if (status==MFS_STATUS_OK) {
			sstats_set(ino,attr,0);
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): %s",(unsigned long int)ino,to_set,setattr_str,strerr(status));
			fuse_reply_err(req, status);
			return;
		}
	}
	if (to_set & FUSE_SET_ATTR_SIZE) {
		if (stbuf->st_size<0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): %s",(unsigned long int)ino,to_set,setattr_str,strerr(EINVAL));
			fuse_reply_err(req, EINVAL);
			return;
		}
		if (stbuf->st_size>=MAX_FILE_SIZE) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): %s",(unsigned long int)ino,to_set,setattr_str,strerr(EFBIG));
			fuse_reply_err(req, EFBIG);
			return;
		}
		write_data_flush_inode(ino);
		if (full_permissions) {
			uint32_t trycnt;
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			trycnt = 0;
			while (1) {
#if defined(__FreeBSD__)
				if (freebsd_workarounds) {
					status = fs_truncate(ino,(fi!=NULL)?(TRUNCATE_FLAG_OPENED|TRUNCATE_FLAG_TIMEFIX):TRUNCATE_FLAG_TIMEFIX,ctx.uid,gids->gidcnt,gids->gidtab,stbuf->st_size,attr);
				} else {
#endif
				status = fs_truncate(ino,(fi!=NULL)?TRUNCATE_FLAG_OPENED:0,ctx.uid,gids->gidcnt,gids->gidtab,stbuf->st_size,attr);
#if defined(__FreeBSD__)
				}
#endif
				if (status==MFS_STATUS_OK || status==MFS_ERROR_EROFS || status==MFS_ERROR_EACCES || status==MFS_ERROR_EPERM || status==MFS_ERROR_ENOENT || status==MFS_ERROR_QUOTA || status==MFS_ERROR_NOSPACE || status==MFS_ERROR_CHUNKLOST) {
					break;
				} else if (status!=MFS_ERROR_LOCKED) {
					trycnt++;
					if (trycnt>=30) {
						break;
					}
					portable_usleep(1000+((trycnt<30)?((trycnt-1)*300000):10000000));
				} else {
					portable_usleep(10000);
				}
			}
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			uint32_t trycnt;
			trycnt = 0;
			while (1) {
#if defined(__FreeBSD__)
				if (freebsd_workarounds) {
					status = fs_truncate(ino,(fi!=NULL)?(TRUNCATE_FLAG_OPENED|TRUNCATE_FLAG_TIMEFIX):TRUNCATE_FLAG_TIMEFIX,ctx.uid,1,&gidtmp,stbuf->st_size,attr);
				} else {
#endif
				status = fs_truncate(ino,(fi!=NULL)?TRUNCATE_FLAG_OPENED:0,ctx.uid,1,&gidtmp,stbuf->st_size,attr);
#if defined(__FreeBSD__)
				}
#endif
				if (status==MFS_STATUS_OK || status==MFS_ERROR_EROFS || status==MFS_ERROR_EACCES || status==MFS_ERROR_EPERM || status==MFS_ERROR_ENOENT || status==MFS_ERROR_QUOTA || status==MFS_ERROR_NOSPACE || status==MFS_ERROR_CHUNKLOST) {
					break;
				} else if (status!=MFS_ERROR_LOCKED) {
					trycnt++;
					if (trycnt>=30) {
						break;
					}
					portable_usleep(1000+((trycnt<30)?((trycnt-1)*300000):10000000));
				} else {
					portable_usleep(10000);
				}
			}
		}
		status = mfs_errorconv(status);
		// read_inode_ops(ino);
		if (status!=0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): %s",(unsigned long int)ino,to_set,setattr_str,strerr(status));
			fuse_reply_err(req, status);
			return;
		}
		chunksdatacache_clear_inode(ino,stbuf->st_size/MFSCHUNKSIZE);
		finfo_change_fleng(ino,stbuf->st_size);
		write_data_inode_setmaxfleng(ino,stbuf->st_size);
		read_inode_set_length_active(ino,stbuf->st_size);
	}
#if defined(FUSE_SET_ATTR_ATIME_NOW) && defined(FUSE_SET_ATTR_MTIME_NOW)
	if (to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME|FUSE_SET_ATTR_ATIME_NOW|FUSE_SET_ATTR_MTIME_NOW)) {
#else
	if (to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME)) {
#endif
		uint32_t masterversion = master_version();
//#if !(defined(FUSE_SET_ATTR_ATIME_NOW) && defined(FUSE_SET_ATTR_MTIME_NOW))
//		time_t now = time(NULL);
//#endif
		setmask = 0;
		if (to_set & FUSE_SET_ATTR_MODE) {
			setmask |= SET_MODE_FLAG;
			if (no_xattrs==0 && xattr_cache_on) {
				xattr_cache_del(ino,6+1+5+1+3+1+6,(const uint8_t*)"system.posix_acl_access");
			}
		}
		if (to_set & FUSE_SET_ATTR_UID) {
			setmask |= SET_UID_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_GID) {
			setmask |= SET_GID_FLAG;
		}
#if defined(FUSE_SET_ATTR_ATIME_NOW)
		if (((to_set & FUSE_SET_ATTR_ATIME_NOW) || ((to_set & FUSE_SET_ATTR_ATIME) && stbuf->st_atime<0)) && masterversion>=VERSION2INT(2,1,13)) {
#else
		if ((to_set & FUSE_SET_ATTR_ATIME) && stbuf->st_atime<0 && masterversion>=VERSION2INT(2,1,13)) {
#endif
			setmask |= SET_ATIME_NOW_FLAG;
		} else if (to_set & FUSE_SET_ATTR_ATIME) {
			setmask |= SET_ATIME_FLAG;
		}
#if defined(FUSE_SET_ATTR_MTIME_NOW)
		if (((to_set & FUSE_SET_ATTR_MTIME_NOW) || ((to_set & FUSE_SET_ATTR_MTIME) && stbuf->st_mtime<0)) && masterversion>=VERSION2INT(2,1,13)) {
#else
		if ((to_set & FUSE_SET_ATTR_MTIME) && stbuf->st_mtime<0 && masterversion>=VERSION2INT(2,1,13)) {
#endif
			setmask |= SET_MTIME_NOW_FLAG;
		} else if (to_set & FUSE_SET_ATTR_MTIME) {
			setmask |= SET_MTIME_FLAG;
		}
		if (setmask & (SET_ATIME_NOW_FLAG|SET_ATIME_FLAG)) {
			fs_no_atime(ino);
		}
		if (setmask & (SET_MTIME_NOW_FLAG|SET_MTIME_FLAG)) {
			fs_no_mtime(ino);
		}
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,gids->gidcnt,gids->gidtab,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,0,sugid_clear_mode,attr);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,1,&gidtmp,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,0,sugid_clear_mode,attr);
		}
		if (status==MFS_ERROR_ENOENT) {
			status = sstats_get(ino,attr,0);
			if (status==MFS_STATUS_OK) {
				mfs_attr_modify(to_set,attr,stbuf);
			}
		}
		if (status==MFS_STATUS_OK) {
			sstats_set(ino,attr,0);
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): %s",(unsigned long int)ino,to_set,setattr_str,strerr(status));
			fuse_reply_err(req, status);
			return;
		}
	}
	if (status!=0) {	// should never happened but better check than sorry
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): %s",(unsigned long int)ino,to_set,setattr_str,strerr(status));
		fuse_reply_err(req, status);
		return;
	}
	dcache_setattr(ino,attr);
	if (mfs_attr_get_type(attr)==TYPE_FILE) {
		maxfleng = write_data_inode_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
		mfs_attr_set_fleng(attr,maxfleng);
	}
	fdcache_invalidate(ino);
	attr_timeout = (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(&ctx,"setattr (%lu,0x%X,[%s]): OK (%.1lf,%s)",(unsigned long int)ino,to_set,setattr_str,attr_timeout,attrstr);
	fuse_reply_attr(req, &o_stbuf, attr_timeout);
}

void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	uint16_t cumask;
	int status;
	uint8_t type;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_MKNOD);
	if (debug_mode) {
#ifdef FUSE_CAP_DONT_MASK
		char umaskstr[11];
		mfs_makemodestr(umaskstr,ctx.umask);
		oplog_printf(&ctx,"mknod (%lu,%s,%s:0%04o/%s:0%04o,0x%08lX) ...",(unsigned long int)parent,name,modestr,(unsigned int)mode,umaskstr+1,(unsigned int)(ctx.umask),(unsigned long int)rdev);
		fprintf(stderr,"mknod (%lu,%s,%s:0%04o/%s:0%04o,0x%08lX)\n",(unsigned long int)parent,name,modestr,(unsigned int)mode,umaskstr+1,(unsigned int)(ctx.umask),(unsigned long int)rdev);
#else
		oplog_printf(&ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX) ...",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev);
		fprintf(stderr,"mknod (%lu,%s,%s:0%04o,0x%08lX)\n",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev);
#endif
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}
	if (S_ISFIFO(mode)) {
		type = TYPE_FIFO;
	} else if (S_ISCHR(mode)) {
		type = TYPE_CHARDEV;
	} else if (S_ISBLK(mode)) {
		type = TYPE_BLOCKDEV;
	} else if (S_ISSOCK(mode)) {
		type = TYPE_SOCKET;
	} else if (S_ISREG(mode) || (mode&0170000)==0) {
		type = TYPE_FILE;
	} else {
		oplog_printf(&ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(EPERM));
		fuse_reply_err(req, EPERM);
		return;
	}

	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}

#ifdef FUSE_CAP_DONT_MASK
	cumask = ctx.umask;
#else
	cumask = 0;
#endif
	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,cumask,ctx.uid,gids->gidcnt,gids->gidtab,rdev,&inode,attr);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,cumask,ctx.uid,1,&gidtmp,rdev,&inode,attr);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(status));
		fuse_reply_err(req, status);
	} else {
		negentry_cache_remove(parent,nleng,(const uint8_t*)name);
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
		dcache_invalidate_attr(parent);
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		e.generation = 1;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(&ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	uint32_t inode;
	int status;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_UNLINK);
	if (debug_mode) {
		oplog_printf(&ctx,"unlink (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"unlink (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}

	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_unlink(parent,nleng,(const uint8_t*)name,ctx.uid,gids->gidcnt,gids->gidtab,&inode);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_unlink(parent,nleng,(const uint8_t*)name,ctx.uid,1,&gidtmp,&inode);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		fuse_reply_err(req, status);
	} else {
		negentry_cache_insert(parent,nleng,(const uint8_t*)name);
//		if (newdircache) {
//			dir_cache_unlink(parent,nleng,(const uint8_t*)name);
//		}
		fdcache_invalidate(inode);
		dcache_invalidate_attr(parent);
		dcache_invalidate_name(&ctx,parent,nleng,(const uint8_t*)name);
		oplog_printf(&ctx,"unlink (%lu,%s): OK",(unsigned long int)parent,name);
		fuse_reply_err(req, 0);
	}
}

void mfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	uint16_t cumask;
	int status;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_MKDIR);
	if (debug_mode) {
#ifdef FUSE_CAP_DONT_MASK
		char umaskstr[11];
		mfs_makemodestr(umaskstr,ctx.umask);
		oplog_printf(&ctx,"mkdir (%lu,%s,d%s:0%04o/%s:0%04o) ...",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,umaskstr+1,(unsigned int)(ctx.umask));
		fprintf(stderr,"mkdir (%lu,%s,d%s:0%04o/%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,umaskstr+1,(unsigned int)(ctx.umask));
#else
		oplog_printf(&ctx,"mkdir (%lu,%s,d%s:0%04o) ...",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
		fprintf(stderr,"mkdir (%lu,%s,d%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
#endif
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

#ifdef FUSE_CAP_DONT_MASK
	cumask = ctx.umask;
#else
	cumask = 0;
#endif
	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_mkdir(parent,nleng,(const uint8_t*)name,mode,cumask,ctx.uid,gids->gidcnt,gids->gidtab,mkdir_copy_sgid,&inode,attr);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_mkdir(parent,nleng,(const uint8_t*)name,mode,cumask,ctx.uid,1,&gidtmp,mkdir_copy_sgid,&inode,attr);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
		fuse_reply_err(req, status);
	} else {
		sstats_set(inode,attr,1);
		negentry_cache_remove(parent,nleng,(const uint8_t*)name);
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
		dcache_invalidate_attr(parent);
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		e.generation = 1;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:direntry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(&ctx,"mkdir (%lu,%s,d%s:0%04o): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
	uint32_t nleng;
	uint32_t inode;
	int status;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_RMDIR);
	if (debug_mode) {
		oplog_printf(&ctx,"rmdir (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"rmdir (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_rmdir(parent,nleng,(const uint8_t*)name,ctx.uid,gids->gidcnt,gids->gidtab,&inode);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_rmdir(parent,nleng,(const uint8_t*)name,ctx.uid,1,&gidtmp,&inode);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		fuse_reply_err(req, status);
	} else {
		(void)inode; // for future use
		negentry_cache_insert(parent,nleng,(const uint8_t*)name);
//		if (newdircache) {
//			dir_cache_unlink(parent,nleng,(const uint8_t*)name);
//		}
		dcache_invalidate_attr(parent);
		dcache_invalidate_name(&ctx,parent,nleng,(const uint8_t*)name);
		oplog_printf(&ctx,"rmdir (%lu,%s): OK",(unsigned long int)parent,name);
		fuse_reply_err(req, 0);
	}
}

void mfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_SYMLINK);
	if (debug_mode) {
		oplog_printf(&ctx,"symlink (%s,%lu,%s) ...",path,(unsigned long int)parent,name);
		fprintf(stderr,"symlink (%s,%lu,%s)\n",path,(unsigned long int)parent,name);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX || (strlen(path)+1)>MFS_SYMLINK_MAX) {
		oplog_printf(&ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)path,ctx.uid,gids->gidcnt,gids->gidtab,&inode,attr);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)path,ctx.uid,1,&gidtmp,&inode,attr);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(status));
		fuse_reply_err(req, status);
	} else {
		negentry_cache_remove(parent,nleng,(const uint8_t*)name);
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
		dcache_invalidate_attr(parent);
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		e.generation = 1;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(&ctx,"symlink (%s,%lu,%s): OK (%.1lf,%lu,%.1lf,%s)",path,(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_readlink(fuse_req_t req, fuse_ino_t ino) {
	int status;
	uint8_t *path;
	const uint8_t *cpath;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	if (debug_mode) {
		oplog_printf(&ctx,"readlink (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"readlink (%lu)\n",(unsigned long int)ino);
	}
	path = symlink_cache_search(ino);
	if (path!=NULL) {
		mfs_stats_inc(OP_READLINK_CACHED);
		oplog_printf(&ctx,"readlink (%lu) (using cache): OK (%s)",(unsigned long int)ino,(char*)path);
		fuse_reply_readlink(req, (char*)path);
		free(path);
		return;
	}
	mfs_stats_inc(OP_READLINK_MASTER);
	status = fs_readlink(ino,&cpath);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"readlink (%lu): %s",(unsigned long int)ino,strerr(status));
		fuse_reply_err(req, status);
	} else {
		dcache_invalidate_attr(ino);
		symlink_cache_insert(ino,cpath);
		oplog_printf(&ctx,"readlink (%lu): OK (%s)",(unsigned long int)ino,(char*)cpath);
		fuse_reply_readlink(req, (char*)cpath);
	}
}

#if FUSE_VERSION >= 30
void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname,unsigned int flags) {
#else /* FUSE2 */
void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
#endif
	uint32_t nleng,newnleng;
	int status;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	struct fuse_ctx ctx;
	groups *gids;
#if FUSE_VERSION < 30
	unsigned int flags = 0;
#endif

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_RENAME);
	if (debug_mode) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u) ...",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags);
		fprintf(stderr,"rename (%lu,%s,%lu,%s,%u)\n",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags);
	}
	// TODO implement support for new flags available in fuse3
	if (flags!=0) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags,strerr(EINVAL));
		fuse_reply_err(req, EINVAL);
		return;
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	if (newparent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_rename(parent,nleng,(const uint8_t*)name,newparent,newnleng,(const uint8_t*)newname,ctx.uid,gids->gidcnt,gids->gidtab,&inode,attr);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_rename(parent,nleng,(const uint8_t*)name,newparent,newnleng,(const uint8_t*)newname,ctx.uid,1,&gidtmp,&inode,attr);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags,strerr(status));
		fuse_reply_err(req, status);
	} else {
		negentry_cache_insert(parent,nleng,(const uint8_t*)name);
		negentry_cache_remove(newparent,newnleng,(const uint8_t*)newname);
//		if (newdircache) {
//			dir_cache_unlink(parent,nleng,(const uint8_t*)name);
//			dir_cache_link(newparent,newnleng,(const uint8_t*)newname,inode,attr);
//		}
		dcache_invalidate_attr(parent);
		if (newparent!=parent) {
			dcache_invalidate_attr(newparent);
		}
		dcache_invalidate_name(&ctx,parent,nleng,(const uint8_t*)name);
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s,%u): OK",(unsigned long int)parent,name,(unsigned long int)newparent,newname,flags);
		fuse_reply_err(req, 0);
	}
}

void mfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
	uint32_t newnleng;
	int status;
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	char attrstr[256];
	uint8_t mattr;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_LINK);
	if (debug_mode) {
		oplog_printf(&ctx,"link (%lu,%lu,%s) ...",(unsigned long int)ino,(unsigned long int)newparent,newname);
		fprintf(stderr,"link (%lu,%lu,%s)\n",(unsigned long int)ino,(unsigned long int)newparent,newname);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(EACCES));
		fuse_reply_err(req, EACCES);
		return;
	}
	if (newparent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(&ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

//	write_data_flush_inode(ino);
	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx.uid,gids->gidcnt,gids->gidtab,&inode,attr);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx.uid,1,&gidtmp,&inode,attr);
	}
	status = mfs_errorconv(status);
	if (status==ENOSPC) {
		status=EMLINK;
	}
	if (status!=0) {
		oplog_printf(&ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(status));
		fuse_reply_err(req, status);
	} else {
		negentry_cache_remove(newparent,newnleng,(const uint8_t*)newname);
//		if (newdircache) {
//			dir_cache_link(newparent,newnleng,(const uint8_t*)newname,inode,attr);
//		}
		if (ino!=inode) {
			dcache_invalidate_attr(ino);
		}
		dcache_invalidate_attr(newparent);
		dcache_setattr(inode,attr);
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		e.generation = 1;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(&ctx,"link (%lu,%lu,%s): OK (%.1lf,%lu,%.1lf,%s)",(unsigned long int)ino,(unsigned long int)newparent,newname,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		fuse_reply_entry(req, &e);
	}
}

void mfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	dirbuf *dirinfo;
	uint32_t dindex;
	int status;
	uint8_t attr[ATTR_RECORD_SIZE];
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_OPENDIR);
	if (debug_mode) {
		oplog_printf(&ctx,"opendir (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"opendir (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(ENOTDIR));
		fuse_reply_err(req, ENOTDIR);
	}
	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_access(ino,ctx.uid,gids->gidcnt,gids->gidtab,MODE_MASK_R);	// at least test rights
		groups_rel(gids);
	} else { // no acl means - we are using default permissions, so do not check supplementary groups
		uint32_t gidtmp = ctx.gid;
		status = fs_access(ino,ctx.uid,1,&gidtmp,MODE_MASK_R);	// at least test rights
	}
	if (status==MFS_ERROR_ENOENT && sstats_get(ino,attr,0)==MFS_STATUS_OK) {
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = mfs_access_test(attr,MODE_MASK_R,ctx.uid,gids->gidcnt,gids->gidtab);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = mfs_access_test(attr,MODE_MASK_R,ctx.uid,1,&gidtmp);
		}
		if (status!=MFS_STATUS_OK) {
			status = mfs_errorconv(status);
			oplog_printf(&ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(status));
			fuse_reply_err(req, status);
		} else {
			dindex = dirbuf_new();
			dirinfo = dirbuf_get(dindex);
			passert(dirinfo);
			pthread_mutex_lock(&(dirinfo->lock));	// make valgrind happy
			dirinfo->p = NULL;
			dirinfo->size = 0;
			dirinfo->dcache = NULL;
			dirinfo->wasread = 2;
			pthread_mutex_unlock(&(dirinfo->lock));	// make valgrind happy
			fi->fh = dindex;
			oplog_printf(&ctx,"sustained opendir (%lu): forced OK with empty directory",(unsigned long int)ino);
			if (fuse_reply_open(req,fi) == -ENOENT) {
				dirbuf_release(dindex);
				fi->fh = 0;
			}
		}
	} else if (status!=MFS_STATUS_OK) {
		status = mfs_errorconv(status);
		oplog_printf(&ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(status));
		fuse_reply_err(req, status);
	} else {
		dindex = dirbuf_new();
		dirinfo = dirbuf_get(dindex);
		passert(dirinfo);
		pthread_mutex_lock(&(dirinfo->lock));	// make valgrind happy
		dirinfo->p = NULL;
		dirinfo->size = 0;
		dirinfo->dcache = NULL;
		dirinfo->wasread = 0;
		pthread_mutex_unlock(&(dirinfo->lock));	// make valgrind happy
		fi->fh = dindex;
		oplog_printf(&ctx,"opendir (%lu): OK [handle:%08"PRIX32"]",(unsigned long int)ino,dindex);
		if (fuse_reply_open(req,fi) == -ENOENT) {
			dirbuf_release(dindex);
			fi->fh = 0;
		}
	}
}

void mfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	int status;
	dirbuf *dirinfo;
	char buffer[READDIR_BUFFSIZE];
	char name[MFS_NAME_MAX+1];
	const uint8_t *ptr,*eptr;
	uint8_t end;
	size_t opos,oleng;
	uint8_t nleng;
	uint32_t inode;
	uint8_t type;
	uint8_t attrsize;
	struct stat stbuf;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_READDIR);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"readdir (%lu,%llu,%llu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"readdir (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		}
		fprintf(stderr,"readdir (%lu,%llu,%llu)\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (fi==NULL) {
		oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	dirinfo = dirbuf_get(fi->fh);
	if (dirinfo==NULL) {
		oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	if (off<0) {
		oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	attrsize = master_attrsize();

	zassert(pthread_mutex_lock(&(dirinfo->lock)));
	if (dirinfo->wasread==0 || (dirinfo->wasread==1 && off==0)) {
		const uint8_t *dbuff;
		uint32_t dsize;
		uint8_t needscopy;
/*
		if (newdircache) {
			status = dir_cache_getdirdata(ino,&dsize,&dbuff);
			if (status==1) {	// got dir from new cache
				mfs_stats_inc(OP_GETDIR_CACHED);
				needscopy = 0;
				dirinfo->dataformat = 0;
				status = 0;
			} else {
				status = fs_getdir_plus(ino,ctx.uid,ctx.gid,1,&dbuff,&dsize);
				if (status==0) {
					mfs_stats_inc(OP_GETDIR_FULL);
					dir_cache_newdirdata(ino,dsize,dbuff);
				}
				needscopy = 1;
				dirinfo->dataformat = 1;
			}
		} else 
*/
		if (usedircache) {
			uint8_t df;
			df = 1;
			if (full_permissions) {
				gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
				status = fs_readdir(ino,ctx.uid,gids->gidcnt,gids->gidtab,1,0,&dbuff,&dsize);
				if (status==MFS_ERROR_EACCES) {
					df = 0;
					status = fs_readdir(ino,ctx.uid,gids->gidcnt,gids->gidtab,0,0,&dbuff,&dsize);
				}
				groups_rel(gids);
			} else {
				uint32_t gidtmp = ctx.gid;
				status = fs_readdir(ino,ctx.uid,1,&gidtmp,1,0,&dbuff,&dsize);
				if (status==MFS_ERROR_EACCES) {
					df = 0;
					status = fs_readdir(ino,ctx.uid,1,&gidtmp,0,0,&dbuff,&dsize);
				}
			}
			if (status==0) {
				if (df) {
					mfs_stats_inc(OP_GETDIR_FULL);
				} else {
					mfs_stats_inc(OP_GETDIR_SMALL);
				}
			}
			needscopy = 1;
			dirinfo->dataformat = df;
		} else {
			if (full_permissions) {
				gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
				status = fs_readdir(ino,ctx.uid,gids->gidcnt,gids->gidtab,0,0,&dbuff,&dsize);
				groups_rel(gids);
			} else {
				uint32_t gidtmp = ctx.gid;
				status = fs_readdir(ino,ctx.uid,1,&gidtmp,0,0,&dbuff,&dsize);
			}
			if (status==0) {
				mfs_stats_inc(OP_GETDIR_SMALL);
			}
			needscopy = 1;
			dirinfo->dataformat = 0;
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(status));
			fuse_reply_err(req, status);
			zassert(pthread_mutex_unlock(&(dirinfo->lock)));
			return;
		}
		if (dirinfo->dcache) {
			dcache_release(dirinfo->dcache);
			dirinfo->dcache = NULL;
		}
		if (dirinfo->p) {
			free((uint8_t*)(dirinfo->p));
			dirinfo->p = NULL;
		}
		if (needscopy) {
			dirinfo->p = malloc(dsize);
			if (dirinfo->p == NULL) {
				oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EINVAL));
				fuse_reply_err(req,EINVAL);
				zassert(pthread_mutex_unlock(&(dirinfo->lock)));
				return;
			}
			memcpy((uint8_t*)(dirinfo->p),dbuff,dsize);
		} else {
			dirinfo->p = dbuff;
		}
		dirinfo->size = dsize;
		if (usedircache && dirinfo->dataformat==1) {
			dirinfo->dcache = dcache_new(&ctx,ino,dirinfo->p,dirinfo->size,attrsize);
		}
	}
	if (dirinfo->wasread<2) {
		dirinfo->wasread=1;
	}

	if (off>=(off_t)(dirinfo->size)) {
		oplog_printf(&ctx,"readdir (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		fuse_reply_buf(req, NULL, 0);
	} else {
		if (size>READDIR_BUFFSIZE) {
			size=READDIR_BUFFSIZE;
		}
		ptr = dirinfo->p+off;
		eptr = dirinfo->p+dirinfo->size;
		opos = 0;
		end = 0;

		while (ptr<eptr && end==0) {
			nleng = ptr[0];
			ptr++;
			memcpy(name,ptr,nleng);
			name[nleng]=0;
			ptr+=nleng;
			off+=nleng+((dirinfo->dataformat)?(attrsize+5):6);
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				if (dirinfo->dataformat) {
					mfs_attr_to_stat(inode,ptr,&stbuf);
					ptr+=attrsize;
				} else {
					type = get8bit(&ptr);
					mfs_type_to_stat(inode,type,&stbuf);
				}
				oleng = fuse_add_direntry(req, buffer + opos, size - opos, name, &stbuf, off);
				if (opos+oleng>size) {
					end=1;
				} else {
					opos+=oleng;
				}
			}
		}

		oplog_printf(&ctx,"readdir (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)opos);
		fuse_reply_buf(req,buffer,opos);
	}
	zassert(pthread_mutex_unlock(&(dirinfo->lock)));
}

#if FUSE_VERSION >= 30
void mfs_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	int status;
	dirbuf *dirinfo;
	char buffer[READDIR_BUFFSIZE];
	char name[MFS_NAME_MAX+1];
	const uint8_t *ptr,*eptr;
	uint8_t end;
	size_t opos,oleng;
	uint8_t nleng;
	uint32_t inode;
	uint64_t maxfleng;
	uint8_t type;
	uint8_t mattr;
	uint8_t attrsize;
	struct fuse_entry_param e;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_READDIRPLUS);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"readdirplus (%lu,%llu,%llu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"readdirplus (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		}
		fprintf(stderr,"readdirplus (%lu,%llu,%llu)\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (fi==NULL) {
		oplog_printf(&ctx,"readdirplus (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	dirinfo = dirbuf_get(fi->fh);
	if (dirinfo==NULL) {
		oplog_printf(&ctx,"readdirplus (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	if (off<0) {
		oplog_printf(&ctx,"readdirplus (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	attrsize = master_attrsize();

	zassert(pthread_mutex_lock(&(dirinfo->lock)));
	if (dirinfo->wasread==0 || (dirinfo->wasread==1 && (off==0 || dirinfo->dataformat==0))) {
		const uint8_t *dbuff;
		uint32_t dsize;
		uint8_t needscopy;

		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_readdir(ino,ctx.uid,gids->gidcnt,gids->gidtab,1,0,&dbuff,&dsize);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_readdir(ino,ctx.uid,1,&gidtmp,1,0,&dbuff,&dsize);
		}
		if (status==0) {
			mfs_stats_inc(OP_GETDIR_PLUS);
		}
		needscopy = 1;
		dirinfo->dataformat = 1;
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(status));
			fuse_reply_err(req, status);
			zassert(pthread_mutex_unlock(&(dirinfo->lock)));
			return;
		}
		if (dirinfo->dcache) {
			dcache_release(dirinfo->dcache);
			dirinfo->dcache = NULL;
		}
		if (dirinfo->p) {
			free((uint8_t*)(dirinfo->p));
			dirinfo->p = NULL;
		}
		if (needscopy) {
			dirinfo->p = malloc(dsize);
			if (dirinfo->p == NULL) {
				oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EINVAL));
				fuse_reply_err(req,EINVAL);
				zassert(pthread_mutex_unlock(&(dirinfo->lock)));
				return;
			}
			memcpy((uint8_t*)(dirinfo->p),dbuff,dsize);
		} else {
			dirinfo->p = dbuff;
		}
		dirinfo->size = dsize;
		if (usedircache && dirinfo->dataformat==1) {
			dirinfo->dcache = dcache_new(&ctx,ino,dirinfo->p,dirinfo->size,attrsize);
		}
	}

	if (dirinfo->wasread<2) {
		dirinfo->wasread=1;
	}
	// assert(dirinfo->dataformat>0);

	if (off>=(off_t)(dirinfo->size)) {
		oplog_printf(&ctx,"readdirplus (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		fuse_reply_buf(req, NULL, 0);
	} else {
		if (size>READDIR_BUFFSIZE) {
			size=READDIR_BUFFSIZE;
		}
		ptr = dirinfo->p+off;
		eptr = dirinfo->p+dirinfo->size;
		opos = 0;
		end = 0;

		while (ptr<eptr && end==0) {
			nleng = ptr[0];
			ptr++;
			memcpy(name,ptr,nleng);
			name[nleng]=0;
			ptr+=nleng;
			off+=nleng+((dirinfo->dataformat)?(attrsize+5):6);
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				type = mfs_attr_get_type(ptr);
				if (type==TYPE_FILE) {
					maxfleng = write_data_inode_getmaxfleng(inode);
				} else {
					maxfleng = 0;
				}

				memset(&e, 0, sizeof(e));
				e.ino = inode;
				e.generation = 1;
				mattr = mfs_attr_get_mattr(ptr);
				e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
				e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:((type==TYPE_DIRECTORY)?direntry_cache_timeout:entry_cache_timeout);
				mfs_attr_to_stat(inode,ptr,&e.attr);
				if (maxfleng>(uint64_t)(e.attr.st_size)) {
					e.attr.st_size=maxfleng;
					// mfs_attr_set_fleng(ptr,maxfleng);
				}
				if (type==TYPE_FILE) {
					read_inode_set_length_passive(inode,e.attr.st_size);
					finfo_change_fleng(inode,e.attr.st_size);
				}
				fs_fix_amtime(inode,&(e.attr.st_atime),&(e.attr.st_mtime));
				ptr+=attrsize;
				oleng = fuse_add_direntry_plus(req, buffer + opos, size - opos, name, &e, off);
				if (opos+oleng>size) {
					end=1;
				} else {
					opos+=oleng;
				}
			}
		}

		oplog_printf(&ctx,"readdirplus (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)opos);
		fuse_reply_buf(req,buffer,opos);
	}
	zassert(pthread_mutex_unlock(&(dirinfo->lock)));
}
#endif

void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	(void)ino;
	dirbuf *dirinfo;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_RELEASEDIR);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"releasedir (%lu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"releasedir (%lu) ...",(unsigned long int)ino);
		}
		fprintf(stderr,"releasedir (%lu)\n",(unsigned long int)ino);
	}
	if (fi==NULL) {
		oplog_printf(&ctx,"releasedir (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	dirinfo = dirbuf_get(fi->fh);
	if (dirinfo==NULL) {
		oplog_printf(&ctx,"releasedir (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	zassert(pthread_mutex_lock(&(dirinfo->lock)));
	if (dirinfo->dcache) {
		dcache_release(dirinfo->dcache);
	}
	if (dirinfo->p) {
		free((uint8_t*)(dirinfo->p));
		dirinfo->p = NULL;
	}
	zassert(pthread_mutex_unlock(&(dirinfo->lock)));
	dirbuf_release(fi->fh);
	fi->fh = 0;
	oplog_printf(&ctx,"releasedir (%lu): OK",(unsigned long int)ino);
	fuse_reply_err(req,0);
}

static uint32_t mfs_newfileinfo(uint8_t accmode,uint32_t inode,uint64_t fleng,uint8_t open_in_master) {
	finfo *fileinfo;
	uint32_t findex;
	double now;

	now = monotonic_seconds();
	findex = finfo_new(inode);
	fileinfo = finfo_get(findex);
	passert(fileinfo);
	pthread_mutex_lock(&(fileinfo->lock)); // make helgrind happy
	fileinfo->flengptr = inoleng_acquire(inode);
	inoleng_setfleng(fileinfo->flengptr,fleng);
	fileinfo->inode = inode;
#ifdef HAVE___SYNC_OP_AND_FETCH
	__sync_and_and_fetch(&(fileinfo->uselocks),0);
#else
	fileinfo->uselocks = 0;
#endif
	fileinfo->posix_lo_head = NULL;
	fileinfo->flock_lo_head = NULL;
	fileinfo->readers_cnt = 0;
	fileinfo->writers_cnt = 0;
	fileinfo->writing = 0;
#if defined(__FreeBSD__)
	if (freebsd_workarounds) {
	/* old FreeBSD fuse reads whole file when opening with O_WRONLY|O_APPEND,
	 * so can't open it write-only - use read-write instead */
		if (accmode == O_RDONLY) {
			fileinfo->mode = IO_READONLY;
			fileinfo->rdata = NULL; // read_data_new(inode,fleng);
			fileinfo->wdata = NULL;
		} else {
			fileinfo->mode = IO_READWRITE;
			fileinfo->rdata = NULL;
			fileinfo->wdata = NULL;
		}
	} else {
#endif
	if (accmode == O_RDONLY) {
		fileinfo->mode = IO_READONLY;
		fileinfo->rdata = NULL; // read_data_new(inode,fleng);
		fileinfo->wdata = NULL;
	} else if (accmode == O_WRONLY) {
		fileinfo->mode = IO_WRITEONLY;
		fileinfo->rdata = NULL;
		fileinfo->wdata = NULL; // write_data_new(inode,fleng);
	} else {
		fileinfo->mode = IO_READWRITE;
		fileinfo->rdata = NULL;
		fileinfo->wdata = NULL;
	}
#if defined(__FreeBSD__)
	}
#endif
	fileinfo->create = now;
#ifdef FREEBSD_DELAYED_RELEASE
	fileinfo->ops_in_progress = 0;
	fileinfo->lastuse = now;
//	fileinfo->next = NULL;
#endif
	fileinfo->open_waiting = 0;
	fileinfo->open_in_master = open_in_master;
	pthread_mutex_unlock(&(fileinfo->lock)); // make helgrind happy
	return findex;
}

static void mfs_removefileinfo(uint32_t findex) {
	finfo *fileinfo = finfo_get(findex);
	if (fileinfo!=NULL) {
		zassert(pthread_mutex_lock(&(fileinfo->lock)));
#ifdef FREEBSD_DELAYED_RELEASE
		fileinfo->lastuse = monotonic_seconds();
#else
		finfo_free_resources(fileinfo);
#endif
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		finfo_release(findex);
	}
}

void mfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	uint16_t cumask;
	int status;
	struct fuse_ctx ctx;
	groups *gids;
	uint32_t findex;

	ctx = *(fuse_req_ctx(req));
	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_CREATE);
	if (debug_mode) {
#ifdef FUSE_CAP_DONT_MASK
		char umaskstr[11];
		mfs_makemodestr(umaskstr,ctx.umask);
		oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o/%s:0%04o)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,umaskstr+1,(unsigned int)(ctx.umask));
		fprintf(stderr,"create (%lu,%s,-%s:0%04o/%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,umaskstr+1,(unsigned int)(ctx.umask));
#else
		oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
		fprintf(stderr,"create (%lu,%s,-%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
#endif
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EACCES));
			fuse_reply_err(req,EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}

#ifdef FUSE_CAP_DONT_MASK
	cumask = ctx.umask;
#else
	cumask = 0;
#endif
	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_create(parent,nleng,(const uint8_t*)name,mode&07777,cumask,ctx.uid,gids->gidcnt,gids->gidtab,&inode,attr);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_create(parent,nleng,(const uint8_t*)name,mode&07777,cumask,ctx.uid,1,&gidtmp,&inode,attr);
	}
	if (status!=MFS_ERROR_ENOTSUP) {
#if defined(__APPLE__)
		// due to bug in os x - create in deleted directory goes into infinite loop when it gets ENOENT, so we should change it to different error - we use EACCES
		if (status==MFS_ERROR_ENOENT && sstats_get(parent,attr,0)==MFS_STATUS_OK) {
			status=MFS_ERROR_EACCES;
		}
#endif
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
			fuse_reply_err(req, status);
			return;
		}
		negentry_cache_remove(parent,nleng,(const uint8_t*)name);
		if (no_xattrs==0 && xattr_cache_on) { // Linux asks for this xattr before every write, so after create we can safely assume that there is no such attribute, and set it in xattr cache (improve efficiency on small files)
			xattr_cache_set(inode,ctx.uid,ctx.gid,8+1+10,(const uint8_t*)"security.capability",NULL,0,MFS_ERROR_ENOATTR);
			xattr_cache_set(inode,ctx.uid,ctx.gid,8+1+3,(const uint8_t*)"security.ima",NULL,0,MFS_ERROR_ENOATTR);
		}
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
	} else {
		uint8_t oflags;
		uint32_t gidtmp = ctx.gid;
		oflags = AFTER_CREATE;
		if ((fi->flags & O_ACCMODE) == O_RDONLY) {
			oflags |= WANT_READ;
		} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
			oflags |= WANT_WRITE;
		} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
			oflags |= WANT_READ | WANT_WRITE;
		} else {
			oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EINVAL));
			fuse_reply_err(req, EINVAL);
			return;
		}
		status = fs_mknod(parent,nleng,(const uint8_t*)name,TYPE_FILE,mode&07777,cumask,ctx.uid,1,&gidtmp,0,&inode,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o) (mknod): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
			fuse_reply_err(req, status);
			return;
		}
		negentry_cache_remove(parent,nleng,(const uint8_t*)name);
//		if (newdircache) {
//			dir_cache_link(parent,nleng,(const uint8_t*)name,inode,attr);
//		}
		status = fs_opencheck(inode,ctx.uid,1,&gidtmp,oflags,NULL);
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o) (open): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
			fuse_reply_err(req, status);
			return;
		}
	}

	mattr = mfs_attr_get_mattr(attr);
	findex = mfs_newfileinfo(fi->flags & O_ACCMODE,inode,0,1);
	fi->fh = findex;
	if (mattr&MATTR_DIRECTMODE) {
		fi->keep_cache = 0;
		fi->direct_io = 1;
	} else {
		if (keep_cache==1) {
			fi->keep_cache=1;
		} else if (keep_cache==2 || keep_cache>=3) {
			fi->keep_cache=0;
		} else {
			fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
		}
		fi->direct_io = (keep_cache>=3)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"create (%lu) ok -> use %s io ; %s data cache\n",(unsigned long int)inode,(fi->direct_io)?"direct":"cached",(fi->keep_cache)?"keep":"clear");
	}
//	if (fi->keep_cache==0) {
//		chunksdatacache_clear_inode(inode,0);
//	}
	dcache_invalidate_attr(parent);
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	e.generation = 1;
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
	mfs_attr_to_stat(inode,attr,&e.attr);
	mfs_makeattrstr(attrstr,256,&e.attr);
	oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o): OK (%.1lf,%lu,%.1lf,%s) (direct_io:%u,keep_cache:%u) [handle:%08"PRIX32"]",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr,(unsigned int)fi->direct_io,(unsigned int)fi->keep_cache,findex);
	fs_inc_acnt(inode);
	if (fuse_reply_create(req, &e, fi) == -ENOENT) {
		fs_dec_acnt(inode);
		mfs_removefileinfo(findex);
		fi->fh = 0;
	}
}

void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint8_t oflags,mmode;
	uint16_t lflags;
	void *fdrec;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t mattr;
	int status;
	struct fuse_ctx ctx;
	groups *gids;
	uint32_t findex;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_OPEN);
	if (debug_mode) {
		oplog_printf(&ctx,"open (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"open (%lu)\n",(unsigned long int)ino);
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo;
//		status = fs_direct_connect();
//		if (status<0) {
//			fuse_reply_err(req,EIO);
//			return;
//		}
//		masterinfo = malloc(sizeof(minfo));
//		if (masterinfo==NULL) {
//			fuse_reply_err(req,ENOMEM);
//			return;
//		}
//		masterinfo->sd = status;
//		masterinfo->sent = 0;
//		fi->direct_io = 1;
//		fi->fh = (unsigned long)masterinfo;
//		fuse_reply_open(req, fi);
//		return;
//	}
	if (ino==MASTERINFO_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			oplog_printf(&ctx,"open (%lu) (internal node: MASTERINFO): %s",(unsigned long int)ino,strerr(EACCES));
			fuse_reply_err(req,EACCES);
			return;
		}
		fi->fh = 0;
		fi->direct_io = 0;
		fi->keep_cache = 0;
		oplog_printf(&ctx,"open (%lu) (internal node: MASTERINFO): OK (0,1)",(unsigned long int)ino);
		fuse_reply_open(req, fi);
		return;
	}

	if (ino==STATS_INODE) {
		sinfo *statsinfo;
		uint32_t sindex;
		sindex = sinfo_new();
		statsinfo = sinfo_get(sindex);
		passert(statsinfo);
//		statsinfo = malloc(sizeof(sinfo));
//		if (statsinfo==NULL) {
//			oplog_printf(&ctx,"open (%lu) (internal node: STATS): %s",(unsigned long int)ino,strerr(ENOMEM));
//			fuse_reply_err(req,ENOMEM);
//			return;
//		}
		pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
		stats_show_all(&(statsinfo->buff),&(statsinfo->leng));
		statsinfo->reset = 0;
		pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
		fi->fh = sindex;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		oplog_printf(&ctx,"open (%lu) (internal node: STATS): OK (1,0)",(unsigned long int)ino);
		fuse_reply_open(req, fi);
		return;
	}
	if (ino==MOOSE_INODE || ino==RANDOM_INODE) {
		fi->fh = 0;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		oplog_printf(&ctx,"open (%lu) (internal node: %s): OK (1,0)",(unsigned long int)ino,(ino==MOOSE_INODE)?"MOOSE":"RANDOM");
		fuse_reply_open(req, fi);
		return;
	}
	if (ino==PARAMS_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			oplog_printf(&ctx,"open (%lu) (internal node: PARAMS): %s",(unsigned long int)ino,strerr(EACCES));
			fuse_reply_err(req,EACCES);
			return;
		}
		if (ctx.uid != 0) {
			oplog_printf(&ctx,"open (%lu) (internal node: PARAMS): %s",(unsigned long int)ino,strerr(EPERM));
			fuse_reply_err(req,EPERM);
			return;
		}
		fi->fh = 0;
		fi->direct_io = 0;
		fi->keep_cache = 1;
		oplog_printf(&ctx,"open (%lu) (internal node: PARAMS): OK (1,0)",(unsigned long int)ino);
		fuse_reply_open(req, fi);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			oplog_printf(&ctx,"open (%lu) (internal node: %s): %s",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",strerr(EACCES));
			fuse_reply_err(req,EACCES);
			return;
		}
		if (ctx.uid != 0) {
			oplog_printf(&ctx,"open (%lu) (internal node: %s): %s",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",strerr(EPERM));
			fuse_reply_err(req,EPERM);
			return;
		}
		fi->fh = oplog_newhandle((ino==OPHISTORY_INODE)?1:0);
		fi->direct_io = 1;
		fi->keep_cache = 0;
		oplog_printf(&ctx,"open (%lu) (internal node: %s): OK (1,0)",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY");
		fuse_reply_open(req, fi);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		fi->fh = 0;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		fuse_reply_open(req, fi);
		return;
	}
*/
	oflags = 0;
	mmode = 0;
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
		mmode = MODE_MASK_R;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
		mmode = MODE_MASK_W;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
		mmode = MODE_MASK_R | MODE_MASK_W;
	}
	fdrec = fdcache_acquire(&ctx,ino,attr,&lflags);
	if (fdrec) {
		if ((lflags & LOOKUP_RO_FILESYSTEM) && (mmode & MODE_MASK_W)) {
			status = MFS_ERROR_EROFS;
		} else {
			status = (lflags & (1<<(mmode&0x7)))?MFS_STATUS_OK:MFS_ERROR_EACCES;
		}
		if (status==MFS_STATUS_OK) {
			fdcache_inject_chunkdata(fdrec);
		} 
		fdcache_release(fdrec);
	} else {
		write_data_flush_inode(ino); // update file attributes in master !!!
		if (full_permissions) {
			gids = groups_get_x(ctx.pid,ctx.uid,ctx.gid,2); // allow group refresh again (see: getxattr for "com.apple.quarantine")
			status = fs_opencheck(ino,ctx.uid,gids->gidcnt,gids->gidtab,oflags,attr);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_opencheck(ino,ctx.uid,1,&gidtmp,oflags,attr);
		}
	}

	status = mfs_errorconv(status);

	if (status!=0) {
		oplog_printf(&ctx,"open (%lu)%s: %s",(unsigned long int)ino,(fdrec)?" (using cached data from lookup)":"",strerr(status));
		fuse_reply_err(req, status);
		return ;
	}

	mattr = mfs_attr_get_mattr(attr);
	findex = mfs_newfileinfo(fi->flags & O_ACCMODE,ino,mfs_attr_get_fleng(attr),(fdrec)?0:1);
	fi->fh = findex;
	if (mattr&MATTR_DIRECTMODE) {
		fi->keep_cache = 0;
		fi->direct_io = 1;
	} else {
		if (keep_cache==1) {
			fi->keep_cache=1;
		} else if (keep_cache==2 || keep_cache>=3) {
			fi->keep_cache=0;
		} else {
			fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
		}
		fi->direct_io = (keep_cache>=3)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"open (%lu) ok -> use %s io ; %s data cache\n",(unsigned long int)ino,(fi->direct_io)?"direct":"cached",(fi->keep_cache)?"keep":"clear");
	}
//	if (fi->keep_cache==0) {
//		chunksdatacache_clear_inode(ino,0);
//	}
	oplog_printf(&ctx,"open (%lu)%s: OK (direct_io:%u,keep_cache:%u) [handle:%08"PRIX32"]",(unsigned long int)ino,(fdrec)?" (using cached data from lookup)":"",(unsigned int)fi->direct_io,(unsigned int)fi->keep_cache,findex);
	fs_inc_acnt(ino);
	if (fuse_reply_open(req, fi) == -ENOENT) {
		mfs_removefileinfo(findex);
		fs_dec_acnt(ino);
		fi->fh = 0;
	} else if (fdrec) {
		finfo *fileinfo;
		uint32_t gidtmp = 0;
		fs_opencheck(ino,0,1,&gidtmp,oflags,attr); // just send "opencheck" to make sure that master knows that this file is open
		fileinfo = finfo_get(fi->fh);
		if (fileinfo!=NULL) {
			zassert(pthread_mutex_lock(&(fileinfo->lock)));
			fileinfo->open_in_master = 1;
			if (fileinfo->open_waiting) {
				zassert(pthread_cond_broadcast(&(fileinfo->opencond)));
			}
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		}
	}
}

void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	struct fuse_ctx ctx;
	finfo *fileinfo;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_RELEASE);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"release (%lu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"release (%lu) ...",(unsigned long int)ino);
		}
		fprintf(stderr,"release (%lu)\n",(unsigned long int)ino);
	}
	if (fi==NULL) {
		oplog_printf(&ctx,"release (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		if (masterinfo!=NULL) {
//			fs_direct_close(masterinfo->sd);
//			free(masterinfo);
//		}
//		fuse_reply_err(req,0);
//		return;
//	}
	if (ino==MASTERINFO_INODE/* || ino==ATTRCACHE_INODE*/) {
		oplog_printf(&ctx,"release (%lu) (internal node: MASTERINFO): OK",(unsigned long int)ino);
		fuse_reply_err(req,0);
		return;
	}
	if (ino==STATS_INODE || ino==PARAMS_INODE) {
		sinfo *statsinfo = sinfo_get(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
			if (statsinfo->buff!=NULL) {
				free(statsinfo->buff);
				statsinfo->buff = NULL;
			}
			if (statsinfo->reset) {
				stats_reset_all();
			}
			pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
			sinfo_release(fi->fh);
		}
		oplog_printf(&ctx,"release (%lu) (internal node: STATS): OK",(unsigned long int)ino);
		fuse_reply_err(req,0);
		return;
	}
	if (ino==MOOSE_INODE || ino==RANDOM_INODE) {
		oplog_printf(&ctx,"release (%lu) (internal node: %s): OK",(unsigned long int)ino,(ino==MOOSE_INODE)?"MOOSE":"RANDOM");
		fuse_reply_err(req,0);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_releasehandle(fi->fh);
		oplog_printf(&ctx,"release (%lu) (internal node: %s): OK",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY");
		fuse_reply_err(req,0);
		return;
	}
	if (fi->fh>0) {
		fileinfo = finfo_get(fi->fh);
	} else {
		fileinfo = NULL;
	}
	if (fileinfo!=NULL) {
		uint8_t uselocks;
		uint64_t *lock_owner_tab;
		uint32_t lock_owner_posix_cnt;
		uint32_t lock_owner_flock_cnt;
		uint32_t indx;

		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		// rwlock_wait_for_unlock:
		while (fileinfo->writing | fileinfo->writers_cnt | fileinfo->readers_cnt) {
			zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
		}
#ifdef HAVE___SYNC_OP_AND_FETCH
		uselocks = __sync_or_and_fetch(&(fileinfo->uselocks),0);
#else
		uselocks = fileinfo->uselocks;
#endif

		// any locks left?
		lock_owner_tab = NULL;
		lock_owner_posix_cnt = 0;
		lock_owner_flock_cnt = 0;
		if (fileinfo->posix_lo_head!=NULL || fileinfo->flock_lo_head!=NULL) {
			finfo_lock_owner *flo,**flop;

			for (flo=fileinfo->posix_lo_head ; flo!=NULL ; flo=flo->next) {
				lock_owner_posix_cnt++;
			}
			for (flo=fileinfo->flock_lo_head ; flo!=NULL ; flo=flo->next) {
				if (flo->lock_owner!=fi->lock_owner) {
					lock_owner_flock_cnt++;
				}
			}
			if (lock_owner_posix_cnt+lock_owner_flock_cnt>0) {
				lock_owner_tab = malloc(sizeof(uint64_t)*(lock_owner_posix_cnt+lock_owner_flock_cnt));
				passert(lock_owner_tab);
			}

			indx = 0;
			flop = &(fileinfo->posix_lo_head);
			while ((flo=*flop)!=NULL) {
				if (indx<lock_owner_posix_cnt) {
					lock_owner_tab[indx] = flo->lock_owner;
				}
				indx++;
				*flop = flo->next;
				free(flo);
			}
			massert(indx==lock_owner_posix_cnt,"loop mismatch");
			massert(fileinfo->posix_lo_head==NULL,"list not freed");

			indx = 0;
			flop = &(fileinfo->flock_lo_head);
			while ((flo=*flop)!=NULL) {
				if (flo->lock_owner!=fi->lock_owner) {
					if (indx<lock_owner_flock_cnt) {
						lock_owner_tab[lock_owner_posix_cnt+indx] = flo->lock_owner;
					}
					indx++;
				}
				*flop = flo->next;
				free(flo);
			}
			massert(indx==lock_owner_flock_cnt,"loop mismatch");
			massert(fileinfo->flock_lo_head==NULL,"list not freed");
		}

		zassert(pthread_mutex_unlock(&(fileinfo->lock)));

		for (indx=0 ; indx<lock_owner_posix_cnt ; indx++) {
			int status;
			status = fs_posixlock(ino,0,lock_owner_tab[indx],POSIX_LOCK_CMD_SET,POSIX_LOCK_UNLCK,0,UINT64_MAX,0,NULL,NULL,NULL,NULL);
			status = mfs_errorconv(status);
			if (status!=0) {
				oplog_printf(&ctx,"release (%lu) - releasing all POSIX-type locks for %016"PRIX64" (left by kernel): %s",(unsigned long int)ino,lock_owner_tab[indx],strerr(status));
			} else {
				oplog_printf(&ctx,"release (%lu) - releasing all POSIX-type locks for %016"PRIX64" (left by kernel): OK",(unsigned long int)ino,lock_owner_tab[indx]);
			}
		}

		if (uselocks&1) {
			int status;
			status = fs_flock(ino,0,fi->lock_owner,FLOCK_RELEASE);
			status = mfs_errorconv(status);
			if (status!=0) {
				oplog_printf(&ctx,"release (%lu) - releasing all FLOCK-type locks for %016"PRIX64" (received from kernel): %s",(unsigned long int)ino,(uint64_t)(fi->lock_owner),strerr(status));
			} else {
				oplog_printf(&ctx,"release (%lu) - releasing all FLOCK-type locks for %016"PRIX64" (received from kernel): OK",(unsigned long int)ino,(uint64_t)(fi->lock_owner));
			}
		}

		for (indx=0 ; indx<lock_owner_flock_cnt ; indx++) {
			int status;
			status = fs_flock(ino,0,lock_owner_tab[lock_owner_posix_cnt+indx],FLOCK_RELEASE);
			status = mfs_errorconv(status);
			if (status!=0) {
				oplog_printf(&ctx,"release (%lu) - releasing all FLOCK-type locks for %016"PRIX64" (left by kernel): %s",(unsigned long int)ino,lock_owner_tab[indx],strerr(status));
			} else {
				oplog_printf(&ctx,"release (%lu) - releasing all FLOCK-type locks for %016"PRIX64" (left by kernel): OK",(unsigned long int)ino,lock_owner_tab[indx]);
			}
		}

		if (lock_owner_tab!=NULL) {
			free(lock_owner_tab);
		}

	}
	dcache_invalidate_attr(ino);
	if (fileinfo!=NULL) {
		oplog_printf(&ctx,"release (%lu) [handle:%08"PRIX32",uselocks:%u,lock_owner:%016"PRIX64"]: OK",(unsigned long int)ino,(uint32_t)(fi->fh),fileinfo->uselocks,(uint64_t)(fi->lock_owner));
	} else {
		oplog_printf(&ctx,"release (%lu) [handle:%08"PRIX32",lock_owner:%016"PRIX64"]: OK",(unsigned long int)ino,(uint32_t)(fi->fh),(uint64_t)(fi->lock_owner));
	}
	fuse_reply_err(req,0);
	if (fi->fh>0) {
		if (fileinfo!=NULL) {
			zassert(pthread_mutex_lock(&(fileinfo->lock)));
			while (fileinfo->open_in_master==0) {
				fileinfo->open_waiting++;
				zassert(pthread_cond_wait(&(fileinfo->opencond),&(fileinfo->lock)));
				fileinfo->open_waiting--;
			}
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		}
		mfs_removefileinfo(fi->fh); // after writes it waits for data sync, so do it after everything
	}
	fs_dec_acnt(ino);
}

void mfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo;
	uint8_t *buff;
	uint32_t ssize;
	struct iovec *iov;
	uint32_t iovcnt;
	void *buffptr;
	int err;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_READ);
	if (debug_mode) {
		if (ino!=OPLOG_INODE && ino!=OPHISTORY_INODE) {
			if (fi!=NULL) {
				oplog_printf(&ctx,"read (%lu,%llu,%llu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(uint32_t)(fi->fh));
			} else {
				oplog_printf(&ctx,"read (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
			}
		}
		fprintf(stderr,"read from inode %lu up to %llu bytes from position %llu\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (ino==MASTERINFO_INODE) {
		uint8_t masterinfo[14];
		fs_getmasterlocation(masterinfo);
		masterproxy_getlocation(masterinfo);
#ifdef MASTERINFO_WITH_VERSION
		if (off>=14) {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>14) {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)(14-off));
			fuse_reply_buf(req,(char*)(masterinfo+off),14-off);
#else
		if (off>=10) {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>10) {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)(10-off));
			fuse_reply_buf(req,(char*)(masterinfo+off),10-off);
#endif
		} else {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
			fuse_reply_buf(req,(char*)(masterinfo+off),size);
		}
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (fi!=NULL)?sinfo_get(fi->fh):NULL;
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
			if (off>=statsinfo->leng) {
				oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
				fuse_reply_buf(req,NULL,0);
			} else if ((uint64_t)(off+size)>(uint64_t)(statsinfo->leng)) {
				oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)(statsinfo->leng-off));
				fuse_reply_buf(req,statsinfo->buff+off,statsinfo->leng-off);
			} else {
				oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
				fuse_reply_buf(req,statsinfo->buff+off,size);
			}
			pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
		} else {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
			fuse_reply_buf(req,NULL,0);
		}
		return;
	}
	if (ino==PARAMS_INODE) {
		if (off>=params_leng) {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (no data)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
			fuse_reply_buf(req,NULL,0);
		} else if ((uint64_t)(off+size)>(uint64_t)(params_leng)) {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)(params_leng-off));
			fuse_reply_buf(req,params_buff+off,params_leng-off);
		} else {
			oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
			fuse_reply_buf(req,params_buff+off,size);
		}
	}
	if (ino==RANDOM_INODE) {
		uint8_t *rbptr;
		uint32_t nextr;
//		if (size>RANDOM_BUFFSIZE) {
//			size = RANDOM_BUFFSIZE;
//		}
		buff = malloc(size);
		ssize = size;
		rbptr = buff;
		pthread_mutex_lock(&randomlock);
		while (ssize>=4) {
			nextr = KISS;
			*rbptr++ = nextr>>24;
			*rbptr++ = nextr>>16;
			*rbptr++ = nextr>>8;
			*rbptr++ = nextr;
			ssize-=4;
		}
		if (ssize>0) {
			nextr = KISS;
			while (ssize>0) {
				*rbptr++ = nextr>>24;
				nextr <<= 8;
				ssize--;
			}
		}
		pthread_mutex_unlock(&randomlock);
		fuse_reply_buf(req,(char*)buff,size);
		free(buff);
		return;
	}
	if (ino==MOOSE_INODE) {
		static char mooseascii[175] = {
			0x20, 0x5C, 0x5F, 0x5C, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
			0x2F, 0x5F, 0x2F, 0x0A, 0x20, 0x20, 0x20, 0x20, 0x5C, 0x5F, 0x5C, 0x5F, 0x20, 0x20, 0x20, 0x20,
			0x5F, 0x2F, 0x5F, 0x2F, 0x0A, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x5C, 0x2D, 0x2D,
			0x2F, 0x0A, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x2F, 0x40, 0x40, 0x5C, 0x5F, 0x2D,
			0x2D, 0x5F, 0x5F, 0x5F, 0x5F, 0x0A, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x28, 0x5F, 0x5F,
			0x29, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x29, 0x0A, 0x20, 0x20, 0x20, 0x20, 0x20,
			0x20, 0x20, 0x20, 0x60, 0x60, 0x5C, 0x20, 0x20, 0x20, 0x20, 0x5F, 0x5F, 0x20, 0x20, 0x7C, 0x0A,
			0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x7C, 0x7C, 0x2D, 0x27, 0x20,
			0x20, 0x60, 0x7C, 0x7C, 0x0A, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
			0x7C, 0x7C, 0x20, 0x20, 0x20, 0x20, 0x20, 0x7C, 0x7C, 0x0A, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
			0x20, 0x20, 0x20, 0x20, 0x20, 0x22, 0x22, 0x20, 0x20, 0x20, 0x20, 0x20, 0x22, 0x22, 0x0A};
		uint32_t t = monotonic_useconds()%5000000;
		if (t<150000 || (t>=600000 && t<750000)) {
			mooseascii[59]='O';
			mooseascii[60]='O';
		} else if ((t>=150000 && t<300000) || (t>=450000 && t<600000)) {
			mooseascii[59]='O';
			mooseascii[60]='o';
		} else if (t>=300000 && t<450000) {
			mooseascii[59]='O';
			mooseascii[60]='-';
		} else {
			mooseascii[59]='O';
			mooseascii[60]='O';
		}
		if (off>=175) {
			fuse_reply_buf(req,NULL,0);
		} else if ((uint64_t)(off+size)>175) {
			fuse_reply_buf(req,mooseascii+off,175-off);
		} else {
			fuse_reply_buf(req,mooseascii+off,size);
		}
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_getdata(fi->fh,&buff,&ssize,size);
		fuse_reply_buf(req,(char*)buff,ssize);
		oplog_releasedata(fi->fh);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		uint8_t info[2];
		info[0]=dir_cache_ison()+'0';
		if (info[0]!='0' && info[0]!='1') {
			info[0]='X';
		}
		info[1]='\n';
		if (off>2) {
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>2) {
			fuse_reply_buf(req,(char*)(info+off),2-off);
		} else {
			fuse_reply_buf(req,(char*)(info+off),size);
		}
		return;
	}
*/
	if (fi==NULL) {
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	fileinfo = finfo_get(fi->fh);
	if (fi->fh==0 || fileinfo==NULL) {
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	if (fileinfo->inode!=ino) {
		oplog_printf(&ctx,"read (%lu!=%lu,%llu,%llu): %s",(unsigned long int)(fileinfo->inode),(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		if (masterinfo->sent) {
//			int rsize;
//			buff = malloc(size);
//			rsize = fs_direct_read(masterinfo->sd,buff,size);
//			fuse_reply_buf(req,(char*)buff,rsize);
//			//syslog(LOG_WARNING,"master received: %d/%llu",rsize,(unsigned long long int)size);
//			free(buff);
//		} else {
//			syslog(LOG_WARNING,"master: read before write");
//			fuse_reply_buf(req,NULL,0);
//		}
//		return;
//	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EFBIG));
		fuse_reply_err(req,EFBIG);
		return;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==IO_WRITEONLY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		fuse_reply_err(req,EACCES);
		return;
	}
	// rwlock_rdlock begin
	while (fileinfo->writing | fileinfo->writers_cnt) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	fileinfo->readers_cnt++;
	// rwlock_rdlock_end
#ifdef FREEBSD_DELAYED_RELEASE
	fileinfo->ops_in_progress++;
#endif
//	if (fileinfo->mode==IO_WRITE) {
//		err = write_data_flush(fileinfo->wdata);
//		if (err!=0) {
//#ifdef FREEBSD_DELAYED_RELEASE
//			fileinfo->ops_in_progress--;
//			fileinfo->lastuse = monotonic_seconds();
//#endif
//			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
//			if (debug_mode) {
//				fprintf(stderr,"IO error occurred while writing inode %lu\n",(unsigned long int)ino);
//			}
//			oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
//			fuse_reply_err(req,err);
//			return;
//		}
//	}
	if (fileinfo->rdata == NULL) {
		fileinfo->rdata = read_data_new(ino,inoleng_getfleng(fileinfo->flengptr));
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));

	write_data_flush_inode(ino);
	ssize = size;
	fs_atime(ino);
	err = read_data(fileinfo->rdata,off,&ssize,&buffptr,&iov,&iovcnt);
	fs_atime(ino);

	if (err!=0) {
		if (debug_mode) {
			fprintf(stderr,"IO error occurred while reading inode %lu (offset:%llu,size:%llu)\n",(unsigned long int)ino,(unsigned long long int)off,(unsigned long long int)size);
		}
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
		fuse_reply_err(req,err);
	} else {
		if (debug_mode) {
			fprintf(stderr,"%"PRIu32" bytes have been read from inode %lu (offset:%llu)\n",ssize,(unsigned long int)ino,(unsigned long long int)off);
		}
		oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)ssize);
//		fuse_reply_buf(req,(char*)buff,ssize);
		fuse_reply_iov(req,iov,iovcnt);
	}
//	read_data_freebuff(fileinfo->rdata);
	read_data_free_buff(fileinfo->rdata,buffptr,iov);
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	// rwlock_rdunlock begin
	fileinfo->readers_cnt--;
	if (fileinfo->readers_cnt==0) {
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
	}
	// rwlock_rdunlock_end
#ifdef FREEBSD_DELAYED_RELEASE
	fileinfo->ops_in_progress--;
	fileinfo->lastuse = monotonic_seconds();
#endif
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
}

void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo;
	int err;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_WRITE);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"write (%lu,%llu,%llu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"write (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		}
		fprintf(stderr,"write to inode %lu %llu bytes at position %llu\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (ino==MASTERINFO_INODE || ino==OPLOG_INODE || ino==OPHISTORY_INODE || ino==MOOSE_INODE || ino==RANDOM_INODE || ino==PARAMS_INODE) {
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		fuse_reply_err(req,EACCES);
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (fi!=NULL)?sinfo_get(fi->fh):NULL;
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
			statsinfo->reset=1;
			pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
		}
		oplog_printf(&ctx,"write (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
		fuse_reply_write(req,size);
		return;
	}
/*
	if (ino==ATTRCACHE_INODE) {
		if (off==0 && size>0 && buf[0]>='0' && buf[0]<='1') {
			dir_cache_user_switch(buf[0]-'0');
			newdircache = buf[0]-'0';
		}
		fuse_reply_write(req,size);
		return;
	}
*/
	if (fi==NULL) {
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	fileinfo = finfo_get(fi->fh);
	if (fi->fh==0 || fileinfo==NULL) {
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	if (fileinfo->inode!=ino) {
		oplog_printf(&ctx,"write (%lu!=%lu,%llu,%llu): %s",(unsigned long int)(fileinfo->inode),(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		int wsize;
//		masterinfo->sent=1;
//		wsize = fs_direct_write(masterinfo->sd,(const uint8_t*)buf,size);
//		//syslog(LOG_WARNING,"master sent: %d/%llu",wsize,(unsigned long long int)size);
//		fuse_reply_write(req,wsize);
//		return;
//	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EFBIG));
		fuse_reply_err(req, EFBIG);
		return;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==IO_READONLY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		fuse_reply_err(req,EACCES);
		return;
	}
	// rwlock_wrlock begin
	fileinfo->writers_cnt++;
	while (fileinfo->readers_cnt | fileinfo->writing) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	fileinfo->writers_cnt--;
	fileinfo->writing = 1;
	// rwlock_wrlock end
#ifdef FREEBSD_DELAYED_RELEASE
	fileinfo->ops_in_progress++;
#endif
	if (fileinfo->wdata==NULL) {
		fileinfo->wdata = write_data_new(ino,inoleng_getfleng(fileinfo->flengptr));
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));

	fs_mtime(ino);
	err = write_data(fileinfo->wdata,off,size,(const uint8_t*)buf,(ctx.uid==0)?1:0);
	fs_mtime(ino);

	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	// rwlock_wrunlock begin
	fileinfo->writing = 0;
	zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
	// wrlock_wrunlock end
#ifdef FREEBSD_DELAYED_RELEASE
	fileinfo->ops_in_progress--;
	fileinfo->lastuse = monotonic_seconds();
#endif
	if (err!=0) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		if (debug_mode) {
			fprintf(stderr,"IO error occurred while writing inode %lu (offset:%llu,size:%llu)\n",(unsigned long int)ino,(unsigned long long int)off,(unsigned long long int)size);
		}
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
		fuse_reply_err(req,err);
	} else {
		uint64_t newfleng;
		if ((uint64_t)(off+size)>inoleng_getfleng(fileinfo->flengptr)) {
			inoleng_setfleng(fileinfo->flengptr,off+size);
			newfleng = off+size;
		} else {
			newfleng = 0;
		}
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		if (debug_mode) {
			fprintf(stderr,"%llu bytes have been written to inode %lu (offset:%llu)\n",(unsigned long long int)size,(unsigned long int)ino,(unsigned long long int)off);
		}
		oplog_printf(&ctx,"write (%lu,%llu,%llu): OK (%llu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long long int)size);
		if (newfleng>0) {
			read_inode_set_length_passive(ino,newfleng);
			write_data_inode_setmaxfleng(ino,newfleng);
			finfo_change_fleng(ino,newfleng);
		}
		read_inode_clear_cache(ino,off,size);
		fdcache_invalidate(ino);
		fuse_reply_write(req,size);
	}
}

static inline int mfs_do_fsync(finfo *fileinfo) {
	uint32_t inode;
	int err;
	err = 0;
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	inode = fileinfo->inode;
	if (fileinfo->wdata!=NULL && (fileinfo->mode==IO_READWRITE || fileinfo->mode==IO_WRITEONLY)) {
		// rwlock_wrlock begin
		fileinfo->writers_cnt++;
		while (fileinfo->readers_cnt | fileinfo->writing) {
			zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
		}
		fileinfo->writers_cnt--;
		fileinfo->writing = 1;
		// rwlock_wrlock end
#ifdef FREEBSD_DELAYED_RELEASE
		fileinfo->ops_in_progress++;
#endif
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));

		err = write_data_flush(fileinfo->wdata);

		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		// rwlock_wrunlock begin
		fileinfo->writing = 0;
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
		// rwlock_wrunlock end
#ifdef FREEBSD_DELAYED_RELEASE
		fileinfo->ops_in_progress--;
		fileinfo->lastuse = monotonic_seconds();
#endif
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	} else {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	}
	if (err==0) {
		fdcache_invalidate(inode);
		dcache_invalidate_attr(inode);
	}
	return err;
}

void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo;
	int err;
	uint8_t uselocks;
#ifdef FLUSH_EXTRA_LOCKS
	uint64_t *lock_owner_tab;
	uint32_t lock_owner_cnt;
	uint32_t indx;
#endif
	groups *gids;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_FLUSH);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"flush (%lu) [handle:%08"PRIX32"] ...",(unsigned long int)ino,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"flush (%lu) ...",(unsigned long int)ino);
		}
		fprintf(stderr,"flush (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"flush (%lu): OK",(unsigned long int)ino);
		fuse_reply_err(req,0);
		return;
	}
	if (fi==NULL) {
		oplog_printf(&ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	fileinfo = finfo_get(fi->fh);
	if (fi->fh==0 || fileinfo==NULL) {
		oplog_printf(&ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	if (fileinfo->inode!=ino) {
		oplog_printf(&ctx,"flush (%lu!=%lu) [handle:%08"PRIX32"]: %s",(unsigned long int)(fileinfo->inode),(unsigned long int)ino,(uint32_t)(fi->fh),strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
//	syslog(LOG_NOTICE,"remove_locks inode:%lu owner:%llu",(unsigned long int)ino,(unsigned long long int)fi->lock_owner);
	err = 0;
//	fuse_reply_err(req,err);

#ifdef HAVE___SYNC_OP_AND_FETCH
	uselocks = __sync_or_and_fetch(&(fileinfo->uselocks),0);
#else
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	uselocks = fileinfo->uselocks;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
#endif

	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->wdata!=NULL && (fileinfo->mode==IO_READWRITE || fileinfo->mode==IO_WRITEONLY)) {
		// rwlock_wrlock begin
		fileinfo->writers_cnt++;
		while (fileinfo->readers_cnt | fileinfo->writing) {
			zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
		}
		fileinfo->writers_cnt--;
		fileinfo->writing = 1;
		// rwlock_wrlock end
#ifdef FREEBSD_DELAYED_RELEASE
		fileinfo->ops_in_progress++;
#endif
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		if ((uselocks&2) || master_version()<VERSION2INT(3,0,43) || fileinfo->create + fsync_before_close_min_time < monotonic_seconds() || write_cache_almost_full()) {
//			fs_fsync_send(ino);
			err = write_data_flush(fileinfo->wdata);
//			fs_fsync_wait();
		} else {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			fs_truncate(ino,TRUNCATE_FLAG_OPENED|TRUNCATE_FLAG_UPDATE,ctx.uid,gids->gidcnt,gids->gidtab,write_data_getmaxfleng(fileinfo->wdata),NULL);
			groups_rel(gids);
			err = write_data_chunk_wait(fileinfo->wdata);
		}
		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		// rwlock_wrunlock begin
		fileinfo->writing = 0;
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
		// rwlock_wrunlock end
#ifdef FREEBSD_DELAYED_RELEASE
		fileinfo->ops_in_progress--;
		fileinfo->lastuse = monotonic_seconds();
#endif
	}

#ifdef FLUSH_EXTRA_LOCKS
	lock_owner_tab = NULL;
	lock_owner_cnt = 0;
	if (fileinfo->posix_lo_head!=NULL) {
		finfo_lock_owner *flo,**flop;

		for (flo=fileinfo->posix_lo_head ; flo!=NULL ; flo=flo->next) {
			if (flo->pid==ctx.pid && flo->lock_owner!=fi->lock_owner) {
				lock_owner_cnt++;
			}
		}
		if (lock_owner_cnt>0) {
			lock_owner_tab = malloc(sizeof(uint64_t)*lock_owner_cnt);
			passert(lock_owner_tab);
		}
		indx = 0;
		flop = &(fileinfo->posix_lo_head);
		while ((flo=*flop)!=NULL) {
			if (flo->pid==ctx.pid && flo->lock_owner!=fi->lock_owner) {
				if (indx<lock_owner_cnt) {
					lock_owner_tab[indx] = flo->lock_owner;
				}
				indx++;
			}
			if (flo->pid==ctx.pid || flo->lock_owner==fi->lock_owner) {
				*flop = flo->next;
				free(flo);
			} else {
				flop = &(flo->next);
			}
		}
		massert(indx==lock_owner_cnt,"loop mismatch");
	}
#endif
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));

	if (uselocks&2) {
		int status;
		status = fs_posixlock(ino,0,fi->lock_owner,POSIX_LOCK_CMD_SET,POSIX_LOCK_UNLCK,0,UINT64_MAX,0,NULL,NULL,NULL,NULL);
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"flush (%lu) - releasing all POSIX-type locks for %016"PRIX64" (received from kernel): %s",(unsigned long int)ino,(uint64_t)(fi->lock_owner),strerr(status));
		} else {
			oplog_printf(&ctx,"flush (%lu) - releasing all POSIX-type locks for %016"PRIX64" (received from kernel): OK",(unsigned long int)ino,(uint64_t)(fi->lock_owner));
		}
	}
#ifdef FLUSH_EXTRA_LOCKS
	for (indx=0 ; indx<lock_owner_cnt ; indx++) {
		int status;
		status = fs_posixlock(ino,0,lock_owner_tab[indx],POSIX_LOCK_CMD_SET,POSIX_LOCK_UNLCK,0,UINT64_MAX,0,NULL,NULL,NULL,NULL);
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"flush (%lu) - releasing all POSIX-type locks for %016"PRIX64" (data structures): %s",(unsigned long int)ino,lock_owner_tab[indx],strerr(status));
		} else {
			oplog_printf(&ctx,"flush (%lu) - releasing all POSIX-type locks for %016"PRIX64" (data structures): OK",(unsigned long int)ino,lock_owner_tab[indx]);
		}
	}

	if (lock_owner_tab!=NULL) {
		free(lock_owner_tab);
	}
#endif
	if (err!=0) {
		oplog_printf(&ctx,"flush (%lu) [handle:%08"PRIX32",uselocks:%u,lock_owner:%016"PRIX64"]: %s",(unsigned long int)ino,(uint32_t)(fi->fh),uselocks,(uint64_t)(fi->lock_owner),strerr(err));
	} else {
		fdcache_invalidate(ino);
		dcache_invalidate_attr(ino);
		oplog_printf(&ctx,"flush (%lu) [handle:%08"PRIX32",uselocks:%u,lock_owner:%016"PRIX64"]: OK",(unsigned long int)ino,(uint32_t)(fi->fh),uselocks,(uint64_t)(fi->lock_owner));
	}
	fuse_reply_err(req,err);
}

void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	finfo *fileinfo;
	int err;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_FSYNC);
	if (debug_mode) {
		if (fi!=NULL) {
			oplog_printf(&ctx,"fsync (%lu,%d) [handle:%08"PRIX32"] ...",(unsigned long int)ino,datasync,(uint32_t)(fi->fh));
		} else {
			oplog_printf(&ctx,"fsync (%lu,%d) ...",(unsigned long int)ino,datasync);
		}
		fprintf(stderr,"fsync (%lu,%d)\n",(unsigned long int)ino,datasync);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"fsync (%lu,%d): OK",(unsigned long int)ino,datasync);
		fuse_reply_err(req,0);
		return;
	}
	if (fi==NULL) {
		oplog_printf(&ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	fileinfo = finfo_get(fi->fh);
	if (fi->fh==0 || fileinfo==NULL) {
		oplog_printf(&ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	if (fileinfo->inode!=ino) {
		oplog_printf(&ctx,"fsync (%lu!=%lu,%d) [handle:%08"PRIX32"]: %s",(unsigned long int)(fileinfo->inode),(unsigned long int)ino,datasync,(uint32_t)(fi->fh),strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	err = mfs_do_fsync(fileinfo);
	if (err!=0) {
		oplog_printf(&ctx,"fsync (%lu,%d) [handle:%08"PRIX32"]: %s",(unsigned long int)ino,datasync,(uint32_t)(fi->fh),strerr(err));
	} else {
		oplog_printf(&ctx,"fsync (%lu,%d) [handle:%08"PRIX32"]: OK",(unsigned long int)ino,datasync,(uint32_t)(fi->fh));
	}
	fuse_reply_err(req,err);
}

#if FUSE_VERSION >= 29

typedef struct _flock_data {
	uint32_t reqid;
	uint32_t inode;
	uint64_t owner;
	uint32_t refs;
} flock_data;

static uint32_t flock_reqid = 0;
#ifndef HAVE___SYNC_OP_AND_FETCH
static pthread_mutex_t flock_lock = PTHREAD_MUTEX_INITIALIZER;
#endif

void* mfs_flock_interrupt (void *data) {
	flock_data *fld = (flock_data*)data;
	uint32_t refs;

	for (;;) {
#ifdef HAVE___SYNC_OP_AND_FETCH
		refs = __sync_or_and_fetch(&(fld->refs),0);
#else
		zassert(pthread_mutex_lock(&flock_lock));
		refs = fld->refs;
		zassert(pthread_mutex_unlock(&flock_lock));
#endif
		if (refs<=1) {
			break;
		}
		fs_flock(fld->inode,fld->reqid,fld->owner,FLOCK_INTERRUPT);
		portable_usleep(100000);
	}
#ifdef HAVE___SYNC_OP_AND_FETCH
	(void)__sync_sub_and_fetch(&(fld->refs),1);
#else
	zassert(pthread_mutex_lock(&flock_lock));
	fld->refs--;
	refs = fld->refs;
	zassert(pthread_mutex_unlock(&flock_lock));
#endif
	if (refs==0) {
		free(fld);
	}
	return NULL;
}

void mfs_flock_interrupt_spawner(fuse_req_t req, void *data) {
	struct fuse_ctx ctx;
	pthread_t th;
	flock_data *fld = (flock_data*)data;
	ctx = *(fuse_req_ctx(req));

#ifdef HAVE___SYNC_OP_AND_FETCH
	(void)__sync_add_and_fetch(&(fld->refs),1);
#else
	zassert(pthread_mutex_lock(&flock_lock));
	fld->refs++;
	zassert(pthread_mutex_unlock(&flock_lock));
#endif
	if (debug_mode) {
		oplog_printf(&ctx,"flock (%"PRIu32",%"PRIu32",%016"PRIX64",-): interrupted",fld->reqid,fld->inode,fld->owner);
		fprintf(stderr,"flock (%"PRIu32",%"PRIu32",%016"PRIX64",-): interrupted\n",fld->reqid,fld->inode,fld->owner);
	}
	lwt_minthread_create(&th,1,mfs_flock_interrupt,data);
}

void mfs_flock (fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, int op) {
	int status;
	struct fuse_ctx ctx;
	uint32_t reqid;
	uint64_t owner;
	uint8_t lock_mode,lmvalid;
	char *lock_mode_str;
	finfo *fileinfo;
	flock_data *fld;
	uint32_t refs;

	if (no_bsd_locks) {
		fuse_reply_err(req,ENOSYS);
		return;
	}
	if (op&LOCK_UN) {
		lmvalid = 1;
		lock_mode = FLOCK_UNLOCK;
		lock_mode_str = "UNLOCK";
	} else if (op&LOCK_SH) {
		lmvalid = 1;
		if (op&LOCK_NB) {
			lock_mode=FLOCK_TRY_SHARED;
			lock_mode_str = "TRYSH";
		} else {
			lock_mode=FLOCK_LOCK_SHARED;
			lock_mode_str = "LOCKSH";
		}
	} else if (op&LOCK_EX) {
		lmvalid = 1;
		if (op&LOCK_NB) {
			lock_mode=FLOCK_TRY_EXCLUSIVE;
			lock_mode_str = "TRYEX";
		} else {
			lock_mode=FLOCK_LOCK_EXCLUSIVE;
			lock_mode_str = "LOCKEX";
		}
	} else {
		lmvalid = 0;
		lock_mode = 0;
		lock_mode_str = "-";
	}
	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_FLOCK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(&ctx,"flock (-,%lu,-,%s): %s",(unsigned long int)ino,lock_mode_str,strerr(EPERM));
			fprintf(stderr,"flock (-,%lu,-,%s)\n",(unsigned long int)ino,lock_mode_str);
		}
		fuse_reply_err(req,EPERM);
		return;
	}
	if (lmvalid==0) {
		if (debug_mode) {
			oplog_printf(&ctx,"flock (-,%lu,-,%s): %s",(unsigned long int)ino,lock_mode_str,strerr(EINVAL));
			fprintf(stderr,"flock (-,%lu,-,%s)\n",(unsigned long int)ino,lock_mode_str);
		}
		fuse_reply_err(req,EINVAL);
		return;
	}
	if (fi==NULL) {
		if (debug_mode) {
			oplog_printf(&ctx,"flock (-,%lu,-,%s): %s",(unsigned long int)ino,lock_mode_str,strerr(EBADF));
			fprintf(stderr,"flock (-,%lu,-,%s)\n",(unsigned long int)ino,lock_mode_str);
		}
		fuse_reply_err(req,EBADF);
		return;
	}
	fileinfo = finfo_get(fi->fh);
	if (fileinfo==NULL) {
		if (debug_mode) {
			oplog_printf(&ctx,"flock (-,%lu,-,%s): %s",(unsigned long int)ino,lock_mode_str,strerr(EBADF));
			fprintf(stderr,"flock (-,%lu,-,%s)\n",(unsigned long int)ino,lock_mode_str);
		}
		fuse_reply_err(req,EBADF);
		return;
	}
	if (fileinfo->inode!=ino) {
		if (debug_mode) {
			oplog_printf(&ctx,"flock (-,%lu!=%lu,-,%s): %s",(unsigned long int)(fileinfo->inode),(unsigned long int)ino,lock_mode_str,strerr(EBADF));
			fprintf(stderr,"flock (-,%lu,-,%s)\n",(unsigned long int)ino,lock_mode_str);
		}
		fuse_reply_err(req,EBADF);
		return;
	}

	zassert(pthread_mutex_lock(&(fileinfo->lock)));

	// wait for full open
	while (fileinfo->open_in_master==0) {
		fileinfo->open_waiting++;
		zassert(pthread_cond_wait(&(fileinfo->opencond),&(fileinfo->lock)));
		fileinfo->open_waiting--;
	}

	owner = fi->lock_owner;

	// track all locks to unlock them on release
	if (lock_mode!=FLOCK_UNLOCK) {
		finfo_lock_owner *flo;

		// add owner_id to list
		for (flo=fileinfo->flock_lo_head ; flo!=NULL ; flo=flo->next) {
			if (flo->lock_owner==owner) {
				break;
			}
		}
		if (flo==NULL) {
			flo = malloc(sizeof(finfo_lock_owner));
			flo->lock_owner = owner;
			flo->next = fileinfo->flock_lo_head;
			fileinfo->flock_lo_head = flo;
		}
	}

	zassert(pthread_mutex_unlock(&(fileinfo->lock)));

#ifdef HAVE___SYNC_OP_AND_FETCH
	do {
		reqid = __sync_add_and_fetch(&flock_reqid,1);
	} while (reqid==0);
	__sync_or_and_fetch(&(fileinfo->uselocks),1);
#else
	zassert(pthread_mutex_lock(&flock_lock));
	flock_reqid++;
	if (flock_reqid==0) {
		flock_reqid=1;
	}
	reqid = flock_reqid;
	zassert(pthread_mutex_unlock(&flock_lock));
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	fileinfo->uselocks |= 1;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
#endif
	if (debug_mode) {
		oplog_printf(&ctx,"flock (%"PRIu32",%lu,%016"PRIX64",%s) [handle:%08"PRIX32"] ...",reqid,(unsigned long int)ino,owner,lock_mode_str,(uint32_t)(fi->fh));
		fprintf(stderr,"flock (%"PRIu32",%lu,%016"PRIX64",%s)\n",reqid,(unsigned long int)ino,owner,lock_mode_str);
	}
	if (lock_mode==FLOCK_UNLOCK) {
		mfs_do_fsync(fileinfo);
	}
	if (lock_mode==FLOCK_LOCK_SHARED || lock_mode==FLOCK_LOCK_EXCLUSIVE) {
		fld = malloc(sizeof(flock_data));
		passert(fld);
		fld->reqid = reqid;
		fld->inode = ino;
		fld->owner = owner;
		fld->refs = 1;
		fuse_req_interrupt_func(req,mfs_flock_interrupt_spawner,fld);
		if (fuse_req_interrupted(req)==0) {
			status = fs_flock(ino,reqid,owner,lock_mode);
			status = mfs_errorconv(status);
		} else {
			status = EINTR;
		}
		fuse_req_interrupt_func(req,NULL,NULL);
	} else {
		status = fs_flock(ino,reqid,owner,lock_mode);
		status = mfs_errorconv(status);
		fld = NULL;
	}
	if (status==0) {
		oplog_printf(&ctx,"flock (%"PRIu32",%lu,%016"PRIX64",%s) [handle:%08"PRIX32"]: OK",reqid,(unsigned long int)ino,owner,lock_mode_str,(uint32_t)(fi->fh));
	} else {
		oplog_printf(&ctx,"flock (%"PRIu32",%lu,%016"PRIX64",%s) [handle:%08"PRIX32"]: %s",reqid,(unsigned long int)ino,owner,lock_mode_str,(uint32_t)(fi->fh),strerr(status));
	}
	fuse_reply_err(req,status);
	if (fld!=NULL) {
#ifdef HAVE___SYNC_OP_AND_FETCH
		refs = __sync_sub_and_fetch(&(fld->refs),1);
#else
		zassert(pthread_mutex_lock(&flock_lock));
		fld->refs--;
		refs = fld->refs;
		zassert(pthread_mutex_unlock(&flock_lock));
#endif
		if (refs==0) {
			free(fld);
		}
	}
}
#endif

#if FUSE_VERSION >= 26

typedef struct _plock_data {
	uint32_t reqid;
	uint32_t inode;
	uint64_t owner;
	uint64_t start;
	uint64_t end;
	char ctype;
	uint32_t refs;
} plock_data;

static uint32_t plock_reqid = 0;
#ifndef HAVE___SYNC_OP_AND_FETCH
static pthread_mutex_t plock_lock = PTHREAD_MUTEX_INITIALIZER;
#endif

void* mfs_plock_interrupt (void *data) {
	plock_data *pld = (plock_data*)data;
	uint32_t refs;

	for (;;) {
#ifdef HAVE___SYNC_OP_AND_FETCH
		refs = __sync_or_and_fetch(&(pld->refs),0);
#else
		zassert(pthread_mutex_lock(&plock_lock));
		refs = pld->refs;
		zassert(pthread_mutex_unlock(&plock_lock));
#endif
		if (refs<=1) {
			break;
		}
		fs_posixlock(pld->inode,pld->reqid,pld->owner,POSIX_LOCK_CMD_INT,POSIX_LOCK_UNLCK,0,0,0,NULL,NULL,NULL,NULL);
		portable_usleep(100000);
	}
#ifdef HAVE___SYNC_OP_AND_FETCH
	refs = __sync_sub_and_fetch(&(pld->refs),1);
#else
	zassert(pthread_mutex_lock(&plock_lock));
	pld->refs++;
	refs = pld->refs;
	zassert(pthread_mutex_unlock(&plock_lock));
#endif
	if (refs==0) {
		free(pld);
	}
	return NULL;
}

void mfs_plock_interrupt_spawner (fuse_req_t req, void *data) {
	struct fuse_ctx ctx;
	pthread_t th;
	plock_data *pld = (plock_data*)data;
	ctx = *(fuse_req_ctx(req));

#ifdef HAVE___SYNC_OP_AND_FETCH
	(void)__sync_add_and_fetch(&(pld->refs),1);
#else
	zassert(pthread_mutex_lock(&plock_lock));
	pld->refs++;
	zassert(pthread_mutex_unlock(&plock_lock));
#endif
	if (debug_mode) {
		oplog_printf(&ctx,"setlkw (%"PRIu32",%016"PRIX64",%"PRIu64",%"PRIu64",%c): interrupted",pld->inode,pld->owner,pld->start,pld->end,pld->ctype);
		fprintf(stderr,"setlkw (%"PRIu32",%016"PRIX64",%"PRIu64",%"PRIu64",%c): interrupted\n",pld->inode,pld->owner,pld->start,pld->end,pld->ctype);
	}
	lwt_minthread_create(&th,1,mfs_plock_interrupt,data);
}

void mfs_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock) {
	int status;
	struct fuse_ctx ctx;
	struct flock rlock;
	uint64_t owner;
	uint64_t start,end,rstart,rend;
	uint32_t pid,rpid;
	uint8_t type,rtype;
	uint8_t invalid;
	char ctype,rctype;

	if (no_posix_locks) {
		fuse_reply_err(req,ENOSYS);
		return;
	}
	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_GETLK);
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(&ctx,"getlk (inode:%lu owner:- start:- end:- type:-): %s",(unsigned long int)ino,strerr(EPERM));
			fprintf(stderr,"getlk (inode:%lu owner:- start:- end:- type:-)\n",(unsigned long int)ino);
		}
		fuse_reply_err(req,EPERM);
		return;
	}
	invalid = 0;
	type = 0; // make gcc happy
	ctype = '-';
	if (lock->l_whence!=SEEK_SET) { // position has to be converted by the kernel
		invalid = 1;
	} else if (lock->l_type==F_UNLCK) {
		type = POSIX_LOCK_UNLCK;
		ctype = 'U';
	} else if (lock->l_type==F_RDLCK) {
		type = POSIX_LOCK_RDLCK;
		ctype = 'R';
	} else if (lock->l_type==F_WRLCK) {
		type = POSIX_LOCK_WRLCK;
		ctype = 'W';
	} else {
		invalid = 1;
	}
	if (invalid) {
		if (debug_mode) {
			oplog_printf(&ctx,"getlk (inode:%lu owner:- start:- end:- type:-): %s",(unsigned long int)ino,strerr(EINVAL));
			fprintf(stderr,"getlk (inode:%lu owner:- start:- end:- type:-)\n",(unsigned long int)ino);
		}
		fuse_reply_err(req,EINVAL);
		return;
	}
	if (fi==NULL || finfo_get(fi->fh)==NULL) {
		if (debug_mode) {
			oplog_printf(&ctx,"getlk (inode:%lu owner:- start:- end:- type:-): %s",(unsigned long int)ino,strerr(EBADF));
			fprintf(stderr,"getlk (inode:%lu owner:- start:- end:- type:-)\n",(unsigned long int)ino);
		}
		fuse_reply_err(req,EBADF);
		return;
	}
	owner = fi->lock_owner;
	start = lock->l_start;
	if (lock->l_len==0) {
		end = UINT64_MAX;
	} else {
		end = start + lock->l_len;
	}
	pid = ctx.pid;
	if (debug_mode) {
		oplog_printf(&ctx,"getlk (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c) [handle:%08"PRIX32"] ...",(unsigned long int)ino,owner,start,end,ctype,(uint32_t)(fi->fh));
		fprintf(stderr,"getlk (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c)\n",(unsigned long int)ino,owner,start,end,ctype);
	}
	status = fs_posixlock(ino,0,owner,POSIX_LOCK_CMD_GET,type,start,end,pid,&rtype,&rstart,&rend,&rpid);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"getlk (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c): %s",(unsigned long int)ino,owner,start,end,ctype,strerr(status));
		fuse_reply_err(req,status);
		return;
	}
	memset(&rlock,0,sizeof(struct flock));
	if (rtype==POSIX_LOCK_RDLCK) {
		rlock.l_type = F_RDLCK;
		rctype = 'R';
	} else if (rtype==POSIX_LOCK_WRLCK) {
		rlock.l_type = F_WRLCK;
		rctype = 'W';
	} else {
		rlock.l_type = F_UNLCK;
		rctype = 'U';
	}
	rlock.l_whence = SEEK_SET;
	rlock.l_start = rstart;
	if ((rend-rstart)>INT64_MAX) {
		rlock.l_len = 0;
	} else {
		rlock.l_len = (rend - rstart);
	}
	rlock.l_pid = rpid;
	oplog_printf(&ctx,"getlk (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c) [handle:%08"PRIX32"]: (start:%"PRIu64" end:%"PRIu64" type:%c pid:%"PRIu32")",(unsigned long int)ino,owner,start,end,ctype,(uint32_t)(fi->fh),rstart,rend,rctype,rpid);
	fuse_reply_lock(req,&rlock);
}

void mfs_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sl) {
	int status;
	struct fuse_ctx ctx;
	uint32_t reqid;
	uint64_t owner;
	uint64_t start,end;
	uint32_t pid;
	uint8_t type;
	uint8_t invalid;
	char ctype;
	finfo *fileinfo;
	plock_data *pld;
	uint32_t refs;
	char *cmdname;

	if (no_posix_locks) {
		fuse_reply_err(req,ENOSYS);
		return;
	}
	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_SETLK);
	if (sl) {
		cmdname = "setlkw";
	} else {
		cmdname = "setlk";
	}
	if (IS_SPECIAL_INODE(ino)) {
		if (debug_mode) {
			oplog_printf(&ctx,"%s (inode:%lu owner:- start:- end:- type:-): %s",cmdname,(unsigned long int)ino,strerr(EPERM));
			fprintf(stderr,"%s (inode:%lu owner:- start:- end:- type:-)\n",cmdname,(unsigned long int)ino);
		}
		fuse_reply_err(req,EPERM);
		return;
	}
	invalid = 0;
	type = 0; // make gcc happy
	ctype = '-';
	if (lock->l_whence!=SEEK_SET) { // position has to be converted by the kernel
		invalid = 1;
	} else if (lock->l_type==F_UNLCK) {
		type = POSIX_LOCK_UNLCK;
		ctype = 'U';
	} else if (lock->l_type==F_RDLCK) {
		type = POSIX_LOCK_RDLCK;
		ctype = 'R';
	} else if (lock->l_type==F_WRLCK) {
		type = POSIX_LOCK_WRLCK;
		ctype = 'W';
	} else {
		invalid = 1;
	}
	if (invalid) {
		if (debug_mode) {
			oplog_printf(&ctx,"%s (inode:%lu owner:- start:- end:- type:-): %s",cmdname,(unsigned long int)ino,strerr(EINVAL));
			fprintf(stderr,"%s (inode:%lu owner:- start:- end:- type:-)\n",cmdname,(unsigned long int)ino);
		}
		fuse_reply_err(req,EINVAL);
		return;
	}
	if (fi==NULL) {
		if (debug_mode) {
			oplog_printf(&ctx,"%s (inode:%lu owner:- start:- end:- type:-): %s",cmdname,(unsigned long int)ino,strerr(EBADF));
			fprintf(stderr,"%s (inode:%lu owner:- start:- end:- type:-)\n",cmdname,(unsigned long int)ino);
		}
		fuse_reply_err(req,EBADF);
		return;
	}
	fileinfo = finfo_get(fi->fh);
	if (fileinfo==NULL) {
		if (debug_mode) {
			oplog_printf(&ctx,"%s (inode:%lu owner:- start:- end:- type:-): %s",cmdname,(unsigned long int)ino,strerr(EBADF));
			fprintf(stderr,"%s (inode:%lu owner:- start:- end:- type:-)\n",cmdname,(unsigned long int)ino);
		}
		fuse_reply_err(req,EBADF);
		return;
	}
	if (fileinfo->inode!=ino) {
		if (debug_mode) {
			oplog_printf(&ctx,"%s (handle_inode:%lu != inode:%lu owner:- start:- end:- type:-): %s",cmdname,(unsigned long int)(fileinfo->inode),(unsigned long int)ino,strerr(EBADF));
			fprintf(stderr,"%s (inode:%lu owner:- start:- end:- type:-)\n",cmdname,(unsigned long int)ino);
		}
		fuse_reply_err(req,EBADF);
		return;
	}

	owner = fi->lock_owner;

	zassert(pthread_mutex_lock(&(fileinfo->lock)));

	// wait for full open
	while (fileinfo->open_in_master==0) {
		fileinfo->open_waiting++;
		zassert(pthread_cond_wait(&(fileinfo->opencond),&(fileinfo->lock)));
		fileinfo->open_waiting--;
	}

	// track all locks to unlock them on release
	if (type!=POSIX_LOCK_UNLCK) {
		finfo_lock_owner *flo;
		// add pid,owner_id to list
		for (flo=fileinfo->posix_lo_head ; flo!=NULL ; flo=flo->next) {
#ifdef FLUSH_EXTRA_LOCKS
			if (flo->pid==ctx.pid && flo->lock_owner==owner) {
#else
			if (flo->lock_owner==owner) {
#endif
				break;
			}
		}
		if (flo==NULL) {
			flo = malloc(sizeof(finfo_lock_owner));
#ifdef FLUSH_EXTRA_LOCKS
			flo->pid = ctx.pid;
#endif
			flo->lock_owner = owner;
			flo->next = fileinfo->posix_lo_head;
			fileinfo->posix_lo_head = flo;
		}
	}

	zassert(pthread_mutex_unlock(&(fileinfo->lock)));

	start = lock->l_start;
	if (lock->l_len==0) {
		end = UINT64_MAX;
	} else {
		end = start + lock->l_len;
	}
	pid = ctx.pid;
#ifdef HAVE___SYNC_OP_AND_FETCH
	do {
		reqid = __sync_add_and_fetch(&plock_reqid,1);
	} while (reqid==0);
	__sync_or_and_fetch(&(fileinfo->uselocks),2);
#else
	zassert(pthread_mutex_lock(&plock_lock));
	plock_reqid++;
	if (plock_reqid==0) {
		plock_reqid=1;
	}
	reqid = plock_reqid;
	zassert(pthread_mutex_unlock(&plock_lock));
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	fileinfo->uselocks |= 2;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
#endif
	if (debug_mode) {
		oplog_printf(&ctx,"%s (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c) [handle:%08"PRIX32"] ...",cmdname,(unsigned long int)ino,owner,start,end,ctype,(uint32_t)(fi->fh));
		fprintf(stderr,"%s (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c)\n",cmdname,(unsigned long int)ino,owner,start,end,ctype);
	}
	if (type==POSIX_LOCK_UNLCK) {
		mfs_do_fsync(fileinfo);
		status = fs_posixlock(ino,reqid,owner,POSIX_LOCK_CMD_SET,POSIX_LOCK_UNLCK,start,end,pid,NULL,NULL,NULL,NULL);
		status = mfs_errorconv(status);
		pld = NULL;
	} else if (sl==0) {
		status = fs_posixlock(ino,reqid,owner,POSIX_LOCK_CMD_TRY,type,start,end,pid,NULL,NULL,NULL,NULL);
		status = mfs_errorconv(status);
		pld = NULL;
	} else {
		pld = malloc(sizeof(plock_data));
		passert(pld);
		pld->reqid = reqid;
		pld->inode = ino;
		pld->owner = owner;
		pld->start = start;
		pld->end = end;
		pld->ctype = ctype;
		pld->refs = 1;
		fuse_req_interrupt_func(req,mfs_plock_interrupt_spawner,pld);
		if (fuse_req_interrupted(req)==0) {
			status = fs_posixlock(ino,reqid,owner,POSIX_LOCK_CMD_SET,type,start,end,pid,NULL,NULL,NULL,NULL);
			status = mfs_errorconv(status);
		} else {
			status = EINTR;
		}
		fuse_req_interrupt_func(req,NULL,NULL);
	}
	if (status==0) {
		oplog_printf(&ctx,"%s (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c) [handle:%08"PRIX32"]: OK",cmdname,(unsigned long int)ino,owner,start,end,ctype,(uint32_t)(fi->fh));
	} else {
		oplog_printf(&ctx,"%s (inode:%lu owner:%016"PRIX64" start:%"PRIu64" end:%"PRIu64" type:%c) [handle:%08"PRIX32"]: %s",cmdname,(unsigned long int)ino,owner,start,end,ctype,(uint32_t)(fi->fh),strerr(status));
	}
	fuse_reply_err(req,status);
	if (pld!=NULL) {
#ifdef HAVE___SYNC_OP_AND_FETCH
		refs = __sync_sub_and_fetch(&(pld->refs),1);
#else
		zassert(pthread_mutex_lock(&plock_lock));
		pld->refs--;
		refs = pld->refs;
		zassert(pthread_mutex_unlock(&plock_lock));
#endif
		if (refs==0) {
			free(pld);
		}
	}
}
#endif

// Linux ACL format:
//   version:8 (2)
//   flags:8 (0)
//   filler:16
//   N * [ tag:16 perm:16 id:32 ]
//   tag:
//     01 - user
//     02 - named user
//     04 - group
//     08 - named group
//     10 - mask
//     20 - other

int mfs_getfacl(fuse_req_t req,fuse_ino_t ino,/*uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,*/uint8_t aclxattr,const uint8_t **buff,uint32_t *leng) {
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t maskperm;
	uint16_t namedusers;
	uint16_t namedgroups;
	const uint8_t *namedacls;
	uint8_t *b;
	uint32_t namedaclssize;
	const uint8_t *p;
	uint32_t i;
	int status;

	(void)req;
	*buff = NULL;
	*leng = 0;
	status = fs_getfacl(ino,/*opened,uid,gids,gid,*/aclxattr,&userperm,&groupperm,&otherperm,&maskperm,&namedusers,&namedgroups,&namedacls,&namedaclssize);

	if (status!=MFS_STATUS_OK) {
		return status;
	}

	if (((namedusers+namedgroups)*6U) != namedaclssize) {
		return MFS_ERROR_EINVAL;
	}

	*leng = 4+32+(namedusers+namedgroups)*8;
	b = mfs_aclstorage_get(4+32+(namedusers+namedgroups)*8);
//	fprintf(stderr,"getacl buff ptr: %p (size: %u)\n",(void*)b,4+32+(namedusers+namedgroups)*8);
	*buff = b;
	p = namedacls;
	b[0] = 2;
	b[1] = 0;
	b[2] = 0;
	b[3] = 0;
	b+=4;
	*(uint16_t*)(b) = 1;
	*(uint16_t*)(b+2) = userperm;
	*(uint32_t*)(b+4) = UINT32_C(0xFFFFFFFF);
	b+=8;
	for (i=0 ; i<namedusers ; i++) {
		*(uint32_t*)(b+4) = get32bit(&p);
		*(uint16_t*)(b) = 2;
		*(uint16_t*)(b+2) = get16bit(&p);
		b+=8;
	}
	*(uint16_t*)(b) = 4;
	*(uint16_t*)(b+2) = groupperm;
	*(uint32_t*)(b+4) = UINT32_C(0xFFFFFFFF);
	b+=8;
	for (i=0 ; i<namedgroups ; i++) {
		*(uint32_t*)(b+4) = get32bit(&p);
		*(uint16_t*)(b) = 8;
		*(uint16_t*)(b+2) = get16bit(&p);
		b+=8;
	}
	*(uint16_t*)(b) = 16;
	*(uint16_t*)(b+2) = maskperm;
	*(uint32_t*)(b+4) = UINT32_C(0xFFFFFFFF);
	b+=8;
	*(uint16_t*)(b) = 32;
	*(uint16_t*)(b+2) = otherperm;
	*(uint32_t*)(b+4) = UINT32_C(0xFFFFFFFF);
//	b+=8;

//	fprintf(stderr,"getacl buff end ptr: %p\n",(void*)b);
	return MFS_STATUS_OK;
}

int mfs_setfacl(fuse_req_t req,fuse_ino_t ino,uint32_t uid,uint8_t aclxattr,const char *buff,uint32_t leng) {
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t maskperm;
	uint16_t namedusers;
	uint16_t namedgroups;
	uint16_t acls;
	uint8_t *p,*namedacls;
	uint32_t i;
	uint16_t tag;

	(void)req;
	if (leng<4 || ((leng % 8) != 4) ) {
		return MFS_ERROR_EINVAL;
	}

	if (buff[0]!=2) {
		return MFS_ERROR_EINVAL;
	}

	acls = (leng - 4) / 8;
	userperm = 0xFFFF; // means empty
	groupperm = 0xFFFF; // means empty
	otherperm = 0xFFFF; // means empty
	maskperm = 0xFFFF; // means no mask
	namedusers = 0;
	namedgroups = 0;

	for (i=0 ; i<acls ; i++) {
		tag = *(const uint16_t*)(buff+4+i*8);
		if (tag & 1) {
			if (userperm!=0xFFFF) {
				return MFS_ERROR_EINVAL;
			}
			userperm = *(const uint16_t*)(buff+6+i*8);
		}
		if (tag & 2) {
			namedusers++;
		}
		if (tag & 4) {
			if (groupperm!=0xFFFF) {
				return MFS_ERROR_EINVAL;
			}
			groupperm = *(const uint16_t*)(buff+6+i*8);
		}
		if (tag & 8) {
			namedgroups++;
		}
		if (tag & 16) {
			if (maskperm!=0xFFFF) {
				return MFS_ERROR_EINVAL;
			}
			maskperm = *(const uint16_t*)(buff+6+i*8);
		}
		if (tag & 32) {
			if (otherperm!=0xFFFF) {
				return MFS_ERROR_EINVAL;
			}
			otherperm = *(const uint16_t*)(buff+6+i*8);
		}
	}
	if (maskperm==0xFFFF && (namedusers|namedgroups)>0) {
		return MFS_ERROR_EINVAL;
	}

	namedacls = mfs_aclstorage_get((namedusers+namedgroups)*6);
//	fprintf(stderr,"namedacls ptr: %p (size: %u)\n",(void*)namedacls,(namedusers+namedgroups)*6);
	p = namedacls;
	for (i=0 ; i<acls ; i++) {
		tag = *(const uint16_t*)(buff+4+i*8);
		if (tag & 2) {
			put32bit(&p,*(const uint32_t*)(buff+8+i*8));
			put16bit(&p,*(const uint16_t*)(buff+6+i*8));
		}
	}
	for (i=0 ; i<acls ; i++) {
		tag = *(const uint16_t*)(buff+4+i*8);
		if (tag & 8) {
			put32bit(&p,*(const uint32_t*)(buff+8+i*8));
			put16bit(&p,*(const uint16_t*)(buff+6+i*8));
		}
	}
//	fprintf(stderr,"namedacls end ptr: %p\n",(void*)p);
	return fs_setfacl(ino,uid,aclxattr,userperm,groupperm,otherperm,maskperm,namedusers,namedgroups,namedacls,(namedusers+namedgroups)*6);
}

#if defined(__APPLE__)
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position) {
#else
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags) {
	uint32_t position=0;
#endif
	uint32_t nleng;
	int status;
	uint8_t mode;
	struct fuse_ctx ctx;
	groups *gids;
	uint8_t aclxattr;

	if (no_xattrs) {
		fuse_reply_err(req,ENOSYS);
		return;
	}
	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_SETXATTR);
	if (debug_mode) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d) ...",(unsigned long int)ino,name,(unsigned long long int)size,flags);
		fprintf(stderr,"setxattr (%lu,%s,%llu,%d)\n",(unsigned long int)ino,name,(unsigned long long int)size,flags);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EPERM));
		fuse_reply_err(req,EPERM);
		return;
	}
	if (size>MFS_XATTR_SIZE_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns E2BIG here
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(E2BIG));
		fuse_reply_err(req,E2BIG);
#else
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(ERANGE));
		fuse_reply_err(req,ERANGE);
#endif
		return;
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EPERM));
		fuse_reply_err(req,EPERM);
#else
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(ERANGE));
		fuse_reply_err(req,ERANGE);
#endif
		return;
	}
	if (nleng==0) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	if ((flags&XATTR_CREATE) && (flags&XATTR_REPLACE)) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	mode = (flags==XATTR_CREATE)?MFS_XATTR_CREATE_ONLY:(flags==XATTR_REPLACE)?MFS_XATTR_REPLACE_ONLY:MFS_XATTR_CREATE_OR_REPLACE;
	aclxattr = POSIX_ACL_NONE;
	if (strcmp(name,"system.posix_acl_access")==0) {
		aclxattr = POSIX_ACL_ACCESS;
	} else if (strcmp(name,"system.posix_acl_default")==0) {
		aclxattr = POSIX_ACL_DEFAULT;
	}
	(void)position;
	if (xattr_cache_on) {
		xattr_cache_del(ino,nleng,(const uint8_t*)name);
	}
	if (aclxattr!=POSIX_ACL_NONE && xattr_acl_support==0) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(ENOTSUP));
		fuse_reply_err(req,ENOTSUP);
		return;
	}
	if (aclxattr!=POSIX_ACL_NONE) {
		status = mfs_setfacl(req,ino,ctx.uid,aclxattr,value,size);
	} else {
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_setxattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,nleng,(const uint8_t*)name,(uint32_t)size,(const uint8_t*)value,mode);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_setxattr(ino,0,ctx.uid,1,&gidtmp,nleng,(const uint8_t*)name,(uint32_t)size,(const uint8_t*)value,mode);
		}
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(status));
		fuse_reply_err(req,status);
		return;
	}
	oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): OK",(unsigned long int)ino,name,(unsigned long long int)size,flags);
	xattr_cache_set(ino,ctx.uid,ctx.gid,nleng,(const uint8_t*)name,(const uint8_t*)value,(uint32_t)size,MFS_STATUS_OK);
	fuse_reply_err(req,0);
}

#if defined(__APPLE__)
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position) {
#else
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) {
	uint32_t position=0;
#endif /* __APPLE__ */
	uint32_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	int status;
	uint8_t mode;
	const uint8_t *buff;
	uint32_t leng;
	struct fuse_ctx ctx;
	groups *gids;
	void *xattr_value_release;
	uint8_t aclxattr;
	uint8_t use_cache;

	if (no_xattrs) {
		fuse_reply_err(req,ENOSYS);
		return;
	}
	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_GETXATTR);
	if (debug_mode) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu) ...",(unsigned long int)ino,name,(unsigned long long int)size);
		fprintf(stderr,"getxattr (%lu,%s,%llu)\n",(unsigned long int)ino,name,(unsigned long long int)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(EPERM));
		fuse_reply_err(req,EPERM);
		return;
	}
//	if (xattr_acl_support==0 && (strcmp(name,"system.posix_acl_default")==0 || strcmp(name,"system.posix_acl_access")==0)) {
//		oplog_printf(&ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(ENOTSUP));
//		fuse_reply_err(req,ENOTSUP);
//		return;
//	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(EPERM));
		fuse_reply_err(req,EPERM);
#else
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(ERANGE));
		fuse_reply_err(req,ERANGE);
#endif
		return;
	}
	if (nleng==0) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	if (size==0) {
		mode = MFS_XATTR_LENGTH_ONLY;
	} else {
		mode = MFS_XATTR_GETA_DATA;
	}
	aclxattr = POSIX_ACL_NONE;
	if (strcmp(name,"system.posix_acl_access")==0) {
		aclxattr = POSIX_ACL_ACCESS;
	} else if (strcmp(name,"system.posix_acl_default")==0) {
		aclxattr = POSIX_ACL_DEFAULT;
	}
	if (aclxattr!=POSIX_ACL_NONE && xattr_acl_support==0) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(ENOTSUP));
		fuse_reply_err(req,ENOTSUP);
		return;
	}
	(void)position;
	if (xattr_cache_on) { // check chache before getting groups
		xattr_value_release = xattr_cache_get(ino,ctx.uid,ctx.gid,nleng,(const uint8_t*)name,&buff,&leng,&status);
	} else {
		xattr_value_release = NULL;
	}
	if (aclxattr==POSIX_ACL_NONE && full_permissions && xattr_value_release==NULL) { // and get groups only if data were not found in cache
		if (strcmp(name,"com.apple.quarantine")==0) { // special case - obtaining groups from the kernel here leads to freeze, so avoid it
			gids = groups_get_x(ctx.pid,ctx.uid,ctx.gid,1);
		} else {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		}
	} else {
		gids = NULL;
	}
	use_cache = 0;
	if (xattr_cache_on) {
		if (xattr_value_release==NULL) {
			if (usedircache && dcache_getattr(&ctx,ino,attr) && (mfs_attr_get_mattr(attr)&MATTR_NOXATTR)) { // no xattr
				status = MFS_ERROR_ENOATTR;
				buff = NULL;
				leng = 0;
				use_cache = 2;
				if (debug_mode) {
					fprintf(stderr,"getxattr: sending negative answer using open dir cache\n");
				}
			} else {
				if (aclxattr!=POSIX_ACL_NONE) {
					status = mfs_getfacl(req,ino,aclxattr,&buff,&leng);
				} else {
					if (gids!=NULL) { // full_permissions
						status = fs_getxattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,nleng,(const uint8_t*)name,MFS_XATTR_GETA_DATA,&buff,&leng);
					} else {
						uint32_t gidtmp = ctx.gid;
						status = fs_getxattr(ino,0,ctx.uid,1,&gidtmp,nleng,(const uint8_t*)name,MFS_XATTR_GETA_DATA,&buff,&leng);
					}
				}
			}
			xattr_cache_set(ino,ctx.uid,ctx.gid,nleng,(const uint8_t*)name,buff,leng,status);
		} else {
			use_cache = 1;
			if (debug_mode) {
				fprintf(stderr,"getxattr: sending data from cache\n");
			}
		}
	} else {
		if (usedircache && dcache_getattr(&ctx,ino,attr) && (mfs_attr_get_mattr(attr)&MATTR_NOXATTR)) { // no xattr
			status = MFS_ERROR_ENOATTR;
			buff = NULL;
			leng = 0;
			use_cache = 2;
			if (debug_mode) {
				fprintf(stderr,"getxattr: sending negative answer using open dir cache\n");
			}
		} else {
			if (aclxattr!=POSIX_ACL_NONE) {
				status = mfs_getfacl(req,ino,aclxattr,&buff,&leng);
			} else {
				if (gids!=NULL) { // full_permissions
					status = fs_getxattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,nleng,(const uint8_t*)name,mode,&buff,&leng);
				} else {
					uint32_t gidtmp = ctx.gid;
					status = fs_getxattr(ino,0,ctx.uid,1,&gidtmp,nleng,(const uint8_t*)name,mode,&buff,&leng);
				}
			}
		}
	}
	if (gids!=NULL) {
		groups_rel(gids);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: %s",(unsigned long int)ino,name,(unsigned long long int)size,(use_cache==0)?"":(use_cache==1)?" (using cache)":" (using open dir cache)",strerr(status));
		fuse_reply_err(req,status);
		if (xattr_value_release!=NULL) {
			xattr_cache_rel(xattr_value_release);
		}
		return;
	}
	if (size==0) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: OK (%"PRIu32")",(unsigned long int)ino,name,(unsigned long long int)size,(use_cache==0)?"":(use_cache==1)?" (using cache)":" (using open dir cache)",leng);
		fuse_reply_xattr(req,leng);
	} else {
		if (leng>size) {
			oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: %s",(unsigned long int)ino,name,(unsigned long long int)size,(use_cache==0)?"":(use_cache==1)?" (using cache)":" (using open dir cache)",strerr(ERANGE));
			fuse_reply_err(req,ERANGE);
		} else {
			oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: OK (%"PRIu32")",(unsigned long int)ino,name,(unsigned long long int)size,(use_cache==0)?"":(use_cache==1)?" (using cache)":" (using open dir cache)",leng);
			fuse_reply_buf(req,(const char*)buff,leng);
		}
	}
	if (xattr_value_release!=NULL) {
		xattr_cache_rel(xattr_value_release);
	}
}

void mfs_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size) {
	const uint8_t *buff;
	uint32_t leng;
	uint8_t attr[ATTR_RECORD_SIZE];
	int status;
	uint8_t mode;
	struct fuse_ctx ctx;
	groups *gids;

	if (no_xattrs) {
		fuse_reply_err(req,ENOSYS);
		return;
	}
	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_LISTXATTR);
	if (debug_mode) {
		oplog_printf(&ctx,"listxattr (%lu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size);
		fprintf(stderr,"listxattr (%lu,%llu)\n",(unsigned long int)ino,(unsigned long long int)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"listxattr (%lu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,strerr(EPERM));
		fuse_reply_err(req,EPERM);
		return;
	}
	if (size==0) {
		mode = MFS_XATTR_LENGTH_ONLY;
	} else {
		mode = MFS_XATTR_GETA_DATA;
	}
	// posix_acl_XXX are not added here - on purpose (on XFS getfattr doesn't list those ACL-like xattrs)
	if (usedircache && dcache_getattr(&ctx,ino,attr) && (mfs_attr_get_mattr(attr)&MATTR_NOXATTR)) { // no xattr
		status = MFS_STATUS_OK;
		buff = NULL;
		leng = 0;
	} else {
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_listxattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,mode,&buff,&leng);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_listxattr(ino,0,ctx.uid,1,&gidtmp,mode,&buff,&leng);
		}
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"listxattr (%lu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,strerr(status));
		fuse_reply_err(req,status);
		return;
	}
	if (size==0) {
		oplog_printf(&ctx,"listxattr (%lu,%llu): OK (%"PRIu32")",(unsigned long int)ino,(unsigned long long int)size,leng);
		fuse_reply_xattr(req,leng);
	} else {
		if (leng>size) {
			oplog_printf(&ctx,"listxattr (%lu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,strerr(ERANGE));
			fuse_reply_err(req,ERANGE);
		} else {
			oplog_printf(&ctx,"listxattr (%lu,%llu): OK (%"PRIu32")",(unsigned long int)ino,(unsigned long long int)size,leng);
			fuse_reply_buf(req,(const char*)buff,leng);
		}
	}
}

void mfs_removexattr (fuse_req_t req, fuse_ino_t ino, const char *name) {
	uint32_t nleng;
	int status;
	uint8_t usecache;
	struct fuse_ctx ctx;
	groups *gids;
	uint8_t aclxattr;
	void *xattr_value_release;

	if (no_xattrs) {
		fuse_reply_err(req,ENOSYS);
		return;
	}
	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_REMOVEXATTR);
	if (debug_mode) {
		oplog_printf(&ctx,"removexattr (%lu,%s) ...",(unsigned long int)ino,name);
		fprintf(stderr,"removexattr (%lu,%s)\n",(unsigned long int)ino,name);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EPERM));
		fuse_reply_err(req,EPERM);
		return;
	}
	aclxattr = POSIX_ACL_NONE;
	if (strcmp(name,"system.posix_acl_access")==0) {
		aclxattr = POSIX_ACL_ACCESS;
	} else if (strcmp(name,"system.posix_acl_default")==0) {
		aclxattr = POSIX_ACL_DEFAULT;
	}
	if (aclxattr!=POSIX_ACL_NONE && xattr_acl_support==0) {
		oplog_printf(&ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(ENOTSUP));
		fuse_reply_err(req,ENOTSUP);
		return;
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(&ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EPERM));
		fuse_reply_err(req,EPERM);
#else
		oplog_printf(&ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(ERANGE));
		fuse_reply_err(req,ERANGE);
#endif
		return;
	}
	if (nleng==0) {
		oplog_printf(&ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	xattr_value_release = NULL;
	usecache = 0;
	if (xattr_cache_on) {
		xattr_value_release = xattr_cache_get(ino,ctx.uid,ctx.gid,nleng,(const uint8_t*)name,NULL,NULL,&status);
		if (xattr_value_release) {
			if (status==MFS_ERROR_ENOATTR) {
				usecache = 1;
			}
			xattr_cache_rel(xattr_value_release);
		}
	}
	if (usecache == 0) {
		if (aclxattr!=POSIX_ACL_NONE) {
			status = fs_setfacl(ino,ctx.uid,aclxattr,0xFFFF,0xFFFF,0xFFFF,0xFFFF,0,0,NULL,0);
		} else {
			if (full_permissions) {
				gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
				status = fs_removexattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,nleng,(const uint8_t*)name);
				groups_rel(gids);
			} else {
				uint32_t gidtmp = ctx.gid;
				status = fs_removexattr(ino,0,ctx.uid,1,&gidtmp,nleng,(const uint8_t*)name);
			}
		}
	}
	if (xattr_cache_on && (status==MFS_STATUS_OK || status==MFS_ERROR_ENOATTR)) {
		xattr_cache_set(ino,ctx.uid,ctx.gid,nleng,(const uint8_t*)name,NULL,0,MFS_ERROR_ENOATTR);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"removexattr (%lu,%s)%s: %s",(unsigned long int)ino,name,usecache?" (using cache)":"",strerr(status));
		fuse_reply_err(req,status);
	} else {
		oplog_printf(&ctx,"removexattr (%lu,%s): OK",(unsigned long int)ino,name);
		fuse_reply_err(req,0);
	}
	if (usecache) {
		if (aclxattr!=POSIX_ACL_NONE) {
			status = fs_setfacl(ino,ctx.uid,aclxattr,0xFFFF,0xFFFF,0xFFFF,0xFFFF,0,0,NULL,0);
		} else {
			if (full_permissions) {
				gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
				status = fs_removexattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,nleng,(const uint8_t*)name);
				groups_rel(gids);
			} else {
				uint32_t gidtmp = ctx.gid;
				status = fs_removexattr(ino,0,ctx.uid,1,&gidtmp,nleng,(const uint8_t*)name);
			}
		}
	}
}

void mfs_inode_clear_cache(uint32_t inode,uint64_t offset,uint64_t leng) {
	struct fuse_ctx ctx;
	ctx.uid = 0;
	ctx.gid = 0;
	ctx.pid = 0;

	fdcache_invalidate(inode);
#if (FUSE_VERSION >= 28)
#if defined(__FreeBSD__)
	if (freebsd_workarounds) {
		oplog_printf(&ctx,"invalidate cache (%"PRIu32":%"PRIu64":%"PRIu64"): not supported",inode,offset,leng);
	} else 
#endif
	if (fuse_comm!=NULL) {
		fuse_lowlevel_notify_inval_inode(fuse_comm,inode,offset,leng);
		oplog_printf(&ctx,"invalidate cache (%"PRIu32":%"PRIu64":%"PRIu64"): ok",inode,offset,leng);
	} else {
		oplog_printf(&ctx,"invalidate cache (%"PRIu32":%"PRIu64":%"PRIu64"): lost",inode,offset,leng);
	}
#else
	oplog_printf(&ctx,"invalidate cache (%"PRIu32":%"PRIu64":%"PRIu64"): not supported",inode,offset,leng);
#endif
}

void mfs_inode_change_fleng(uint32_t inode,uint64_t fleng) {
	finfo_change_fleng(inode,fleng);
}

void mfs_term(void) {
	sinfo_freeall();
	dirbuf_freeall();
	finfo_freeall();
	xattr_cache_term();
	if (full_permissions) {
		groups_term();
	}
}

#define AUXBUFFSIZE 10000
void mfs_prepare_params(void) {
	params_buff = malloc(AUXBUFFSIZE);
	params_leng = main_snprint_parameters(params_buff,AUXBUFFSIZE);
	if (params_leng<AUXBUFFSIZE) {
		params_buff = mfsrealloc(params_buff,params_leng);
	}
	mfs_attr_set_fleng(paramsattr,params_leng);
}

#if defined(__FreeBSD__)
void mfs_freebsd_workarounds(int on) {
	freebsd_workarounds = on;
	if (keep_cache==4) {
		if (on) {
			if (debug_mode) {
				fprintf(stderr,"cachemode change fbsdauto -> direct\n");
			}
			keep_cache=3;
		} else {
			if (debug_mode) {
				fprintf(stderr,"cachemode change fbsdauto -> auto\n");
			}
			keep_cache=0;
		}
	}
}
#endif

#ifdef HAVE_FUSE3
void mfs_setsession(struct fuse_session *se) {
	fuse_comm = se;
}
#endif

#ifdef HAVE_FUSE3
void mfs_init (int debug_mode_in,int keep_cache_in,double direntry_cache_timeout_in,double entry_cache_timeout_in,double attr_cache_timeout_in,double xattr_cache_timeout_in,double groups_cache_timeout,int mkdir_copy_sgid_in,int sugid_clear_mode_in,int xattr_acl_support_in,double fsync_before_close_min_time_in,int no_xattrs_in,int no_posix_locks_in,int no_bsd_locks_in) {
#else /* FUSE2 */
void mfs_init(struct fuse_chan *ch,int debug_mode_in,int keep_cache_in,double direntry_cache_timeout_in,double entry_cache_timeout_in,double attr_cache_timeout_in,double xattr_cache_timeout_in,double groups_cache_timeout,int mkdir_copy_sgid_in,int sugid_clear_mode_in,int xattr_acl_support_in,double fsync_before_close_min_time_in,int no_xattrs_in,int no_posix_locks_in,int no_bsd_locks_in) {
#endif
#ifdef FREEBSD_DELAYED_RELEASE
	pthread_t th;
#endif
	const char* sugid_clear_mode_strings[] = {SUGID_CLEAR_MODE_STRINGS};
#ifndef HAVE_FUSE3
	fuse_comm = ch;
#endif
	debug_mode = debug_mode_in;
	keep_cache = keep_cache_in;
	direntry_cache_timeout = direntry_cache_timeout_in;
	entry_cache_timeout = entry_cache_timeout_in;
	attr_cache_timeout = attr_cache_timeout_in;
	mkdir_copy_sgid = mkdir_copy_sgid_in;
	sugid_clear_mode = sugid_clear_mode_in;
	xattr_cache_init(xattr_cache_timeout_in);
	xattr_cache_on = (xattr_cache_timeout_in>0.0)?1:0;
	xattr_acl_support = xattr_acl_support_in;
	fsync_before_close_min_time = fsync_before_close_min_time_in;
	no_xattrs = no_xattrs_in;
	no_posix_locks = no_posix_locks_in;
	no_bsd_locks = no_bsd_locks_in;
	if (groups_cache_timeout>0.0) {
		groups_init(groups_cache_timeout,debug_mode);
		full_permissions = 1;
	} else {
		full_permissions = 0;
	}
	fdcache_init();
	mfs_aclstorage_init();
	if (debug_mode) {
		fprintf(stderr,"cache parameters: file_keep_cache=%s direntry_cache_timeout=%.2lf entry_cache_timeout=%.2lf attr_cache_timeout=%.2lf xattr_cache_timeout_in=%.2lf (%s)\n",(keep_cache==1)?"always":(keep_cache==2)?"never":(keep_cache==3)?"direct":(keep_cache==4)?"fbsdauto":"auto",direntry_cache_timeout,entry_cache_timeout,attr_cache_timeout,xattr_cache_timeout_in,xattr_cache_on?"on":"off");
		fprintf(stderr,"mkdir copy sgid=%d\nsugid clear mode=%s\n",mkdir_copy_sgid_in,(sugid_clear_mode_in<SUGID_CLEAR_MODE_OPTIONS)?sugid_clear_mode_strings[sugid_clear_mode_in]:"???");
	}
	mfs_statsptr_init();
	mfs_prepare_params();
#ifdef FREEBSD_DELAYED_RELEASE
	lwt_minthread_create(&th,1,finfo_delayed_release_cleanup_thread,NULL);
#endif
}
