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

#if defined(__APPLE__)
# if ! defined(__DARWIN_64_BIT_INO_T) && ! defined(_DARWIN_USE_64_BIT_INODE)
#  define __DARWIN_64_BIT_INO_T 0
# endif
#endif

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <fuse_lowlevel.h>
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

#include "stats.h"
#include "oplog.h"
#include "datapack.h"
#include "clocks.h"
#include "mastercomm.h"
#include "masterproxy.h"
#include "getgroups.h"
#include "readdata.h"
#include "writedata.h"
#include "massert.h"
#include "strerr.h"
#include "MFSCommunication.h"

#include "dirattrcache.h"
#include "symlinkcache.h"
#include "negentrycache.h"
#include "xattrcache.h"
// #include "dircache.h"

#if MFS_ROOT_ID != FUSE_ROOT_ID
#error FUSE_ROOT_ID is not equal to MFS_ROOT_ID
#endif

#if defined(__FreeBSD__)
	// workaround for bug in FreeBSD Fuse version (kernel part)
#  define FREEBSD_EARLY_RELEASE_BUG_WORKAROUND 1
#  define FREEBSD_EARLY_RELEASE_DELAY 10.0
#endif

#define READDIR_BUFFSIZE 50000

#define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

#define PKGVERSION ((VERSMAJ)*1000000+(VERSMID)*10000+((VERSMIN)>>1)*100+(RELEASE))

#define MASTERINFO_WITH_VERSION 1
// #define MASTER_NAME ".master"
// #define MASTER_INODE 0x7FFFFFFF
// 0x01b6 == 0666
// static uint8_t masterattr[35]={'f', 0x01,0xB6, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define MASTERINFO_NAME ".masterinfo"
#define MASTERINFO_INODE 0x7FFFFFFF
// 0x0124 == 0b100100100 == 0444
#ifdef MASTERINFO_WITH_VERSION
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,14};
#else
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10};
#endif

#define STATS_NAME ".stats"
#define STATS_INODE 0x7FFFFFF0
// 0x01A4 == 0b110100100 == 0644
static uint8_t statsattr[35]={'f', 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define OPLOG_NAME ".oplog"
#define OPLOG_INODE 0x7FFFFFF1
#define OPHISTORY_NAME ".ophistory"
#define OPHISTORY_INODE 0x7FFFFFF2
// 0x0100 == 0b100000000 == 0400
static uint8_t oplogattr[35]={'f', 0x01,0x00, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define MOOSE_NAME ".mooseart"
#define MOOSE_INODE 0x7FFFFFF3
// 0x01A4 == 0b110100100 == 0644
static uint8_t mooseattr[35]={'f', 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

/* DIRCACHE
#define ATTRCACHE_NAME ".attrcache"
#define ATTRCACHE_INODE 0x7FFFFFF3
// 0x0180 == 0b110000000 == 0600
static uint8_t attrcacheattr[35]={'f', 0x01,0x80, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};
*/

#define MIN_SPECIAL_INODE 0x7FFFFFF0
#define IS_SPECIAL_INODE(ino) ((ino)>=MIN_SPECIAL_INODE)
#define IS_SPECIAL_NAME(name) ((name)[0]=='.' && (strcmp(STATS_NAME,(name))==0 || strcmp(MASTERINFO_NAME,(name))==0 || strcmp(OPLOG_NAME,(name))==0 || strcmp(OPHISTORY_NAME,(name))==0 || strcmp(MOOSE_NAME,(name))==0/* || strcmp(ATTRCACHE_NAME,(name))==0*/))

typedef struct _sinfo {
	char *buff;
	uint32_t leng;
	uint8_t reset;
	pthread_mutex_t lock;
} sinfo;

typedef struct _dirbuf {
	int wasread;
	int dataformat;
	uid_t uid;
	gid_t gid;
	const uint8_t *p;
	size_t size;
	void *dcache;
	pthread_mutex_t lock;
} dirbuf;

enum {IO_NONE,IO_READ,IO_WRITE,IO_READONLY,IO_WRITEONLY};

typedef struct _finfo {
	uint8_t mode;
	void *data;
	pthread_mutex_t lock;
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	uint32_t ops_in_progress;
	double lastuse;
	struct _finfo *next;
#endif
} finfo;

#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
static finfo *finfo_head = NULL;
static pthread_mutex_t finfo_list_lock = PTHREAD_MUTEX_INITIALIZER;
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
	OP_SETXATTR,
	OP_GETXATTR,
	OP_LISTXATTR,
	OP_REMOVEXATTR,
//	OP_GETDIR_CACHED,
	OP_GETDIR_FULL,
	OP_GETDIR_SMALL,
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
	statsptr[OP_FSYNC] = stats_get_subnode(s,"fsync",0,1);
	statsptr[OP_FLUSH] = stats_get_subnode(s,"flush",0,1);
	statsptr[OP_WRITE] = stats_get_subnode(s,"write",0,1);
	statsptr[OP_READ] = stats_get_subnode(s,"read",0,1);
	statsptr[OP_RELEASE] = stats_get_subnode(s,"release",0,1);
	statsptr[OP_OPEN] = stats_get_subnode(s,"open",0,1);
	statsptr[OP_CREATE] = stats_get_subnode(s,"create",0,1);
	statsptr[OP_RELEASEDIR] = stats_get_subnode(s,"releasedir",0,1);
	statsptr[OP_READDIR] = stats_get_subnode(s,"readdir",0,1);
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
	case STATUS_OK:
		ret=0;
		break;
	case ERROR_EPERM:
		ret=EPERM;
		break;
	case ERROR_ENOTDIR:
		ret=ENOTDIR;
		break;
	case ERROR_ENOENT:
		ret=ENOENT;
		break;
	case ERROR_EACCES:
		ret=EACCES;
		break;
	case ERROR_EEXIST:
		ret=EEXIST;
		break;
	case ERROR_EINVAL:
		ret=EINVAL;
		break;
	case ERROR_ENOTEMPTY:
		ret=ENOTEMPTY;
		break;
	case ERROR_IO:
		ret=EIO;
		break;
	case ERROR_EROFS:
		ret=EROFS;
		break;
	case ERROR_QUOTA:
		ret=EDQUOT;
		break;
	case ERROR_ENOATTR:
		ret=ENOATTR;
		break;
	case ERROR_ENOTSUP:
		ret=ENOTSUP;
		break;
	case ERROR_ERANGE:
		ret=ERANGE;
		break;
	case ERROR_NOSPACE:
		ret=ENOSPC;
		break;
	case ERROR_CHUNKLOST:
		ret=ENXIO;
		break;
	case ERROR_NOCHUNKSERVERS:
		ret=ENOSPC;
		break;
	case ERROR_CSNOTPRESENT:
		ret=ENXIO;
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

static inline uint8_t mfs_attr_get_type(const uint8_t attr[35]) {
	if (attr[0]<64) { // 1.7.29 and up
		return (attr[1]>>4);
	} else {
		return fsnodes_type_convert(attr[0]&0x7F);
	}
}

static inline uint8_t mfs_attr_get_mattr(const uint8_t attr[35]) {
	if (attr[0]<64) { // 1.7.29 and up
		return attr[0];
	} else {
		return (attr[1]>>4);
	}
}

static void mfs_attr_to_stat(uint32_t inode,const uint8_t attr[35], struct stat *stbuf) {
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
	uint64_t totalspace,availspace,trashspace,sustainedspace;
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
	fs_statfs(&totalspace,&availspace,&trashspace,&sustainedspace,&inodes);

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
		stfsbuf.f_bfree = 0xFFFFFFFFU;
		stfsbuf.f_bavail = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_bfree = availspace/bsize;
		stfsbuf.f_bavail = availspace/bsize;
	}
#else
	stfsbuf.f_blocks = totalspace/bsize;
	stfsbuf.f_bfree = availspace/bsize;
	stfsbuf.f_bavail = availspace/bsize;
#endif
	stfsbuf.f_files = 1000000000+PKGVERSION+inodes;
	stfsbuf.f_ffree = 1000000000+PKGVERSION;
	stfsbuf.f_favail = 1000000000+PKGVERSION;
	//stfsbuf.f_flag = ST_RDONLY;
#if FUSE_USE_VERSION >= 26
	oplog_printf(&ctx,"statfs (%lu): OK (%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",(unsigned long int)ino,totalspace,availspace,trashspace,sustainedspace,inodes);
#else
	oplog_printf(&ctx,"statfs (): OK (%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu32")",totalspace,availspace,trashspace,sustainedspace,inodes);
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

void mfs_access(fuse_req_t req, fuse_ino_t ino, int mask) {
	int status;
	struct fuse_ctx ctx;
	groups *gids;
	int mmode;

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

	if (full_permissions) {
		gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		status = fs_access(ino,ctx.uid,gids->gidcnt,gids->gidtab,mmode);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_access(ino,ctx.uid,1,&gidtmp,mmode);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"access (%lu,0x%X): %s",(unsigned long int)ino,mask,strerr(status));
	} else {
		oplog_printf(&ctx,"access (%lu,0x%X): OK",(unsigned long int)ino,mask);
	}
	fuse_reply_err(req,status);
}

void mfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint64_t maxfleng;
	uint32_t inode;
	uint32_t nleng;
	uint8_t attr[35];
	char attrstr[256];
	uint8_t mattr,type;
	uint8_t icacheflag;
	int status;
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
//		if (strcmp(name,MASTER_NAME)==0) {
//			memset(&e, 0, sizeof(e));
//			e.ino = MASTER_INODE;
//			e.generation = 1;
//			e.attr_timeout = 3600.0;
//			e.entry_timeout = 3600.0;
//			mfs_attr_to_stat(MASTER_INODE,masterattr,&e.attr);
//			fuse_reply_entry(req, &e);
//			mfs_stats_inc(OP_LOOKUP);
//			return ;
//		}
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
			status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,gids->gidcnt,gids->gidtab,&inode,attr);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,1,&gidtmp,&inode,attr);
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
				negentry_cache_insert(parent,nleng,(const uint8_t*)name);
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
		maxfleng = write_data_getmaxfleng(inode);
	} else {
		maxfleng = 0;
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
	}
	if (mfs_attr_get_type(attr)==TYPE_FILE) {
		read_inode_set_length(inode,e.attr.st_size,0);
	}
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
	uint8_t attr[35];
	char attrstr[256];
	int status;
	uint8_t icacheflag;
	struct fuse_ctx ctx;
	(void)fi;

	ctx = *(fuse_req_ctx(req));
//	mfs_stats_inc(OP_GETATTR);
	if (debug_mode) {
		oplog_printf(&ctx,"getattr (%lu) ...",(unsigned long int)ino);
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
/*
	if (ino==ATTRCACHE_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,attrcacheattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		oplog_printf(&ctx,"getattr (%lu) (internal node ATTRCACHE)",(unsigned long int)ino);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
*/
//	if (write_data_flush_inode(ino)) {
//		mfs_stats_inc(OP_GETATTR);
//		status = fs_getattr(ino,ctx.uid,ctx.gid,attr);
//		status = mfs_errorconv(status);
/*
	if (newdircache) {
		if (dir_cache_getattr(ino,attr)) {
			mfs_stats_inc(OP_DIRCACHE_GETATTR);
			status = 0;
			oplog_printf(&ctx,"getattr (%lu) (data found in cache)",(unsigned long int)ino);
		} else {
			mfs_stats_inc(OP_GETATTR);
			status = fs_getattr(ino,ctx.uid,ctx.gid,attr);
			status = mfs_errorconv(status);
			if (status!=0) {
				oplog_printf(&ctx,"getattr (%lu) (data not found in cache: %s)",(unsigned long int)ino,strerr(status));
			} else {
				oplog_printf(&ctx,"getattr (%lu) (data not found in cache)",(unsigned long int)ino);
			}
		}
	} else 
*/
	if (usedircache && dcache_getattr(&ctx,ino,attr)) {
		if (debug_mode) {
			fprintf(stderr,"getattr: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_GETATTR);
		status = 0;
		icacheflag = 1;
	} else {
		mfs_stats_inc(OP_GETATTR);
		status = fs_getattr(ino,(fi!=NULL)?1:0,ctx.uid,ctx.gid,attr);
		status = mfs_errorconv(status);
		icacheflag = 0;
	}
	if (status!=0) {
		oplog_printf(&ctx,"getattr (%lu): %s",(unsigned long int)ino,strerr(status));
		fuse_reply_err(req, status);
		return;
	}
	if (mfs_attr_get_type(attr)==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	if (mfs_attr_get_type(attr)==TYPE_FILE) {
		read_inode_set_length(ino,o_stbuf.st_size,0);
	}
	attr_timeout = (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(&ctx,"getattr (%lu)%s: OK (%.1lf,%s)",(unsigned long int)ino,icacheflag?" (using open dir cache)":"",attr_timeout,attrstr);
	fuse_reply_attr(req, &o_stbuf, attr_timeout);
}

void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	uint64_t maxfleng;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	double attr_timeout;
	int status;
	struct fuse_ctx ctx;
	groups *gids;
	uint8_t setmask = 0;

	ctx = *(fuse_req_ctx(req));
	mfs_makemodestr(modestr,stbuf->st_mode);
	mfs_stats_inc(OP_SETATTR);
	if (debug_mode) {
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]) ...",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size));
		fprintf(stderr,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu])\n",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size));
	}
	if (ino==MASTERINFO_INODE) {
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(EPERM));
		fuse_reply_err(req, EPERM);
		return;
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]) (internal node: STATS): OK (3600,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),attrstr);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
		return;
	}
	if (ino==MOOSE_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,mooseattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]) (internal node: MOOSE): OK (3600,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),attrstr);
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
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]) (internal node: %s): OK (3600,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",attrstr);
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
	if ((to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME|FUSE_SET_ATTR_SIZE)) == 0) {	// change other flags or change nothing
//		status = fs_getattr(ino,ctx.uid,ctx.gid,attr);
		// ext3 compatibility - change ctime during this operation (usually chown(-1,-1))
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,gids->gidcnt,gids->gidtab,0,0,0,0,0,0,0,attr);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,1,&gidtmp,0,0,0,0,0,0,0,attr);	
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
			fuse_reply_err(req, status);
			return;
		}
	}
	if (to_set & (FUSE_SET_ATTR_MODE|FUSE_SET_ATTR_UID|FUSE_SET_ATTR_GID|FUSE_SET_ATTR_ATIME|FUSE_SET_ATTR_MTIME)) {
		setmask = 0;
		if (to_set & FUSE_SET_ATTR_MODE) {
			setmask |= SET_MODE_FLAG;
			if (xattr_cache_on) {
				xattr_cache_del(ino,6+1+5+1+3+1+6,(const uint8_t*)"system.posix_acl_access");
			}
		}
		if (to_set & FUSE_SET_ATTR_UID) {
			setmask |= SET_UID_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_GID) {
			setmask |= SET_GID_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_ATIME) {
			setmask |= SET_ATIME_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_MTIME) {
			setmask |= SET_MTIME_FLAG;
			write_data_flush_inode(ino);	// in this case we want flush all pending writes because they could overwrite mtime
		}
		if (full_permissions) {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,gids->gidcnt,gids->gidtab,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,sugid_clear_mode,attr);
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			status = fs_setattr(ino,(fi!=NULL)?1:0,ctx.uid,1,&gidtmp,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,sugid_clear_mode,attr);
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
			fuse_reply_err(req, status);
			return;
		}
	}
	if (to_set & FUSE_SET_ATTR_SIZE) {
		if (stbuf->st_size<0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(EINVAL));
			fuse_reply_err(req, EINVAL);
			return;
		}
		if (stbuf->st_size>=MAX_FILE_SIZE) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(EFBIG));
			fuse_reply_err(req, EFBIG);
			return;
		}
		write_data_flush_inode(ino);
		if (full_permissions) {
			uint32_t trycnt;
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
			trycnt = 0;
			while (1) {
				status = fs_truncate(ino,(fi!=NULL)?1:0,ctx.uid,gids->gidcnt,gids->gidtab,stbuf->st_size,attr);
				if (status==STATUS_OK || status==ERROR_EROFS || status==ERROR_EACCES || status==ERROR_EPERM || status==ERROR_ENOENT || status==ERROR_QUOTA || status==ERROR_NOSPACE || status==ERROR_CHUNKLOST) {
					break;
				} else if (status!=ERROR_LOCKED) {
					trycnt++;
					if (trycnt>=30) {
						break;
					}
				}
				sleep(1+((trycnt<30)?(trycnt/3):10));
			}
			groups_rel(gids);
		} else {
			uint32_t gidtmp = ctx.gid;
			uint32_t trycnt;
			trycnt = 0;
			while (1) {
				status = fs_truncate(ino,(fi!=NULL)?1:0,ctx.uid,1,&gidtmp,stbuf->st_size,attr);
				if (status==STATUS_OK || status==ERROR_EROFS || status==ERROR_EACCES || status==ERROR_EPERM || status==ERROR_ENOENT || status==ERROR_QUOTA || status==ERROR_NOSPACE || status==ERROR_CHUNKLOST) {
					break;
				} else if (status!=ERROR_LOCKED) {
					trycnt++;
					if (trycnt>=30) {
						break;
					}
				}
				sleep(1+((trycnt<30)?(trycnt/3):10));
			}
		}
		status = mfs_errorconv(status);
		// read_inode_ops(ino);
		read_inode_set_length(ino,stbuf->st_size,1);
		if (status!=0) {
			oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
			fuse_reply_err(req, status);
			return;
		}
	}
	if (status!=0) {	// should never happend but better check than sorry
		oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),strerr(status));
		fuse_reply_err(req, status);
		return;
	}
	dcache_setattr(ino,attr);
	if (mfs_attr_get_type(attr)==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	attr_timeout = (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(&ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%llu]): OK (%.1lf,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long long int)(stbuf->st_size),attr_timeout,attrstr);
	fuse_reply_attr(req, &o_stbuf, attr_timeout);
}

void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
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
		status = fs_unlink(parent,nleng,(const uint8_t*)name,ctx.uid,gids->gidcnt,gids->gidtab);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_unlink(parent,nleng,(const uint8_t*)name,ctx.uid,1,&gidtmp);
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
		dcache_invalidate_attr(parent);
		dcache_invalidate_name(&ctx,parent,nleng,(const uint8_t*)name);
		oplog_printf(&ctx,"unlink (%lu,%s): OK",(unsigned long int)parent,name);
		fuse_reply_err(req, 0);
	}
}

void mfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
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
		status = fs_rmdir(parent,nleng,(const uint8_t*)name,ctx.uid,gids->gidcnt,gids->gidtab);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_rmdir(parent,nleng,(const uint8_t*)name,ctx.uid,1,&gidtmp);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		fuse_reply_err(req, status);
	} else {
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
	uint8_t attr[35];
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
	const uint8_t *path;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	if (debug_mode) {
		oplog_printf(&ctx,"readlink (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"readlink (%lu)\n",(unsigned long int)ino);
	}
	if (symlink_cache_search(ino,&path)) {
		mfs_stats_inc(OP_READLINK_CACHED);
		oplog_printf(&ctx,"readlink (%lu) (using cache): OK (%s)",(unsigned long int)ino,(char*)path);
		fuse_reply_readlink(req, (char*)path);
		return;
	}
	mfs_stats_inc(OP_READLINK_MASTER);
	status = fs_readlink(ino,&path);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"readlink (%lu): %s",(unsigned long int)ino,strerr(status));
		fuse_reply_err(req, status);
	} else {
		dcache_invalidate_attr(ino);
		symlink_cache_insert(ino,path);
		oplog_printf(&ctx,"readlink (%lu): OK (%s)",(unsigned long int)ino,(char*)path);
		fuse_reply_readlink(req, (char*)path);
	}
}

void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
	uint32_t nleng,newnleng;
	int status;
	uint32_t inode;
	uint8_t attr[35];
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_RENAME);
	if (debug_mode) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s) ...",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
		fprintf(stderr,"rename (%lu,%s,%lu,%s)\n",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
	}
	if (parent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(&ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	if (newparent==FUSE_ROOT_ID) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(&ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(EACCES));
			fuse_reply_err(req, EACCES);
			return;
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		fuse_reply_err(req, ENAMETOOLONG);
		return;
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
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
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(status));
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
		oplog_printf(&ctx,"rename (%lu,%s,%lu,%s): OK",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
		fuse_reply_err(req, 0);
	}
}

void mfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
	uint32_t newnleng;
	int status;
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
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
	int status;
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
		status = fs_access(ino,ctx.uid,1,&(ctx.gid),MODE_MASK_R);	// at least test rights
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(status));
		fuse_reply_err(req, status);
	} else {
		dirinfo = malloc(sizeof(dirbuf));
		pthread_mutex_init(&(dirinfo->lock),NULL);
		pthread_mutex_lock(&(dirinfo->lock));	// make valgrind happy
		dirinfo->p = NULL;
		dirinfo->size = 0;
		dirinfo->dcache = NULL;
		dirinfo->wasread = 0;
		pthread_mutex_unlock(&(dirinfo->lock));	// make valgrind happy
		fi->fh = (unsigned long)dirinfo;
		oplog_printf(&ctx,"opendir (%lu): OK",(unsigned long int)ino);
		if (fuse_reply_open(req,fi) == -ENOENT) {
			fi->fh = 0;
			pthread_mutex_destroy(&(dirinfo->lock));
			free(dirinfo);
		}
	}
}

void mfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	int status;
        dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	char buffer[READDIR_BUFFSIZE];
	char name[MFS_NAME_MAX+1];
	const uint8_t *ptr,*eptr;
	uint8_t end;
	size_t opos,oleng;
	uint8_t nleng;
	uint32_t inode;
	uint8_t type;
	struct stat stbuf;
	struct fuse_ctx ctx;
	groups *gids;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_READDIR);
	if (debug_mode) {
		oplog_printf(&ctx,"readdir (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		fprintf(stderr,"readdir (%lu,%llu,%llu)\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (off<0) {
		oplog_printf(&ctx,"readdir (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	pthread_mutex_lock(&(dirinfo->lock));
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
				if (status==ERROR_EACCES) {
					df = 0;
					status = fs_readdir(ino,ctx.uid,gids->gidcnt,gids->gidtab,0,0,&dbuff,&dsize);
				}
				groups_rel(gids);
			} else {
				uint32_t gidtmp = ctx.gid;
				status = fs_readdir(ino,ctx.uid,1,&gidtmp,1,0,&dbuff,&dsize);
				if (status==ERROR_EACCES) {
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
			pthread_mutex_unlock(&(dirinfo->lock));
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
				pthread_mutex_unlock(&(dirinfo->lock));
				return;
			}
			memcpy((uint8_t*)(dirinfo->p),dbuff,dsize);
		} else {
			dirinfo->p = dbuff;
		}
		dirinfo->size = dsize;
		if (usedircache && dirinfo->dataformat==1) {
			dirinfo->dcache = dcache_new(&ctx,ino,dirinfo->p,dirinfo->size);
		}
	}
	dirinfo->wasread=1;

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
			off+=nleng+((dirinfo->dataformat)?40:6);
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				if (dirinfo->dataformat) {
					mfs_attr_to_stat(inode,ptr,&stbuf);
					ptr+=35;
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
	pthread_mutex_unlock(&(dirinfo->lock));
}

void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	(void)ino;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_RELEASEDIR);
	if (debug_mode) {
		oplog_printf(&ctx,"releasedir (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"releasedir (%lu)\n",(unsigned long int)ino);
	}
	pthread_mutex_lock(&(dirinfo->lock));
	pthread_mutex_unlock(&(dirinfo->lock));
	pthread_mutex_destroy(&(dirinfo->lock));
	if (dirinfo->dcache) {
		dcache_release(dirinfo->dcache);
	}
	if (dirinfo->p) {
		free((uint8_t*)(dirinfo->p));
	}
	free(dirinfo);
	fi->fh = 0;
	oplog_printf(&ctx,"releasedir (%lu): OK",(unsigned long int)ino);
	fuse_reply_err(req,0);
}

#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
static void mfs_real_removefileinfo(finfo* fileinfo) {
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode == IO_READONLY || fileinfo->mode == IO_READ) {
		read_data_end(fileinfo->data);
	} else if (fileinfo->mode == IO_WRITEONLY || fileinfo->mode == IO_WRITE) {
//		write_data_flush(fileinfo->data);
		write_data_end(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	pthread_mutex_destroy(&(fileinfo->lock));
	free(fileinfo);
}
#endif

static finfo* mfs_newfileinfo(uint8_t accmode,uint32_t inode) {
	finfo *fileinfo;
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	finfo **fileinfoptr;
	double now;
	now = monotonic_seconds();
	pthread_mutex_lock(&finfo_list_lock);
	fileinfoptr = &finfo_head;
	while ((fileinfo=*fileinfoptr)) {
		if (fileinfo->ops_in_progress==0 && fileinfo->lastuse+FREEBSD_EARLY_RELEASE_DELAY<now) {
			*fileinfoptr = fileinfo->next;
			mfs_real_removefileinfo(fileinfo);
		} else {
			fileinfoptr = &(fileinfo->next);
		}
	}
	pthread_mutex_unlock(&finfo_list_lock);
#endif
	fileinfo = malloc(sizeof(finfo));
	pthread_mutex_init(&(fileinfo->lock),NULL);
	pthread_mutex_lock(&(fileinfo->lock)); // make helgrind happy
#ifdef __FreeBSD__
	/* old FreeBSD fuse reads whole file when opening with O_WRONLY|O_APPEND,
	 * so can't open it write-only */
	(void)accmode;
	(void)inode;
	fileinfo->mode = IO_NONE;
	fileinfo->data = NULL;
#else
	if (accmode == O_RDONLY) {
		fileinfo->mode = IO_READONLY;
		fileinfo->data = read_data_new(inode);
	} else if (accmode == O_WRONLY) {
		fileinfo->mode = IO_WRITEONLY;
		fileinfo->data = write_data_new(inode);
	} else {
		fileinfo->mode = IO_NONE;
		fileinfo->data = NULL;
	}
#endif
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress = 0;
	fileinfo->lastuse = now;
	fileinfo->next = NULL;
#endif
	pthread_mutex_unlock(&(fileinfo->lock)); // make helgrind happy
	return fileinfo;
}

static void mfs_removefileinfo(finfo* fileinfo) {
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	pthread_mutex_lock(&(fileinfo->lock));
	fileinfo->lastuse = monotonic_seconds();
	pthread_mutex_unlock(&(fileinfo->lock));
	pthread_mutex_lock(&finfo_list_lock);
	fileinfo->next = finfo_head;
	finfo_head = fileinfo;
	pthread_mutex_unlock(&finfo_list_lock);
#else
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode == IO_READONLY || fileinfo->mode == IO_READ) {
		read_data_end(fileinfo->data);
	} else if (fileinfo->mode == IO_WRITEONLY || fileinfo->mode == IO_WRITE) {
//		write_data_flush(fileinfo->data);
		write_data_end(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	pthread_mutex_destroy(&(fileinfo->lock));
	free(fileinfo);
#endif
}

void mfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi) {
	struct fuse_entry_param e;
	uint32_t inode;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	uint16_t cumask;
	int status;
	struct fuse_ctx ctx;
	groups *gids;
	finfo *fileinfo;

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
	if (status!=ERROR_ENOTSUP) {
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
			fuse_reply_err(req, status);
			return;
		}
		negentry_cache_remove(parent,nleng,(const uint8_t*)name);
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
	fileinfo = mfs_newfileinfo(fi->flags & O_ACCMODE,inode);
	fi->fh = (unsigned long)fileinfo;
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"create (%lu) ok -> keep cache: %lu\n",(unsigned long int)inode,(unsigned long int)fi->keep_cache);
	}
	dcache_invalidate_attr(parent);
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	e.generation = 1;
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
	mfs_attr_to_stat(inode,attr,&e.attr);
	mfs_makeattrstr(attrstr,256,&e.attr);
	oplog_printf(&ctx,"create (%lu,%s,-%s:0%04o): OK (%.1lf,%lu,%.1lf,%s,%lu)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr,(unsigned long int)fi->keep_cache);
	if (fuse_reply_create(req, &e, fi) == -ENOENT) {
		mfs_removefileinfo(fileinfo);
	}
}

void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	uint8_t oflags;
	uint8_t attr[35];
	uint8_t mattr;
	int status;
	struct fuse_ctx ctx;
	groups *gids;
	finfo *fileinfo;

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
		fi->keep_cache = 1;
		oplog_printf(&ctx,"open (%lu) (internal node: MASTERINFO): OK (0,1)",(unsigned long int)ino);
		fuse_reply_open(req, fi);
		return;
	}

	if (ino==STATS_INODE) {
		sinfo *statsinfo;
//		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
//			stats_reset_all();
//			fuse_reply_err(req,EACCES);
//			return;
//		}
		statsinfo = malloc(sizeof(sinfo));
		if (statsinfo==NULL) {
			oplog_printf(&ctx,"open (%lu) (internal node: STATS): %s",(unsigned long int)ino,strerr(ENOMEM));
			fuse_reply_err(req,ENOMEM);
			return;
		}
		pthread_mutex_init(&(statsinfo->lock),NULL);	// make helgrind happy
		pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
		stats_show_all(&(statsinfo->buff),&(statsinfo->leng));
		statsinfo->reset = 0;
		pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
		fi->fh = (unsigned long)statsinfo;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		oplog_printf(&ctx,"open (%lu) (internal node: STATS): OK (1,0)",(unsigned long int)ino);
		fuse_reply_open(req, fi);
		return;
	}
	if (ino==MOOSE_INODE) {
		fi->fh = 0;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		oplog_printf(&ctx,"open (%lu) (internal node: MOOSE): OK (1,0)",(unsigned long int)ino);
		fuse_reply_open(req, fi);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			oplog_printf(&ctx,"open (%lu) (internal node: %s): %s",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",strerr(EACCES));
			fuse_reply_err(req,EACCES);
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
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	}
	// TODO: idea - only recently used inodes may have their data in cache, so if inode wasn't recently used then return ok status and force cache clear
	//       fs_opencheck should always return status ok (because system should check access rights before using attributes), but even though perform opencheck
	//       and if it returns error then save this error in fileinfo record, and return it on the first following I/O operation
	//
	//	This should significantly speed up opening process.
	//
	// if (usedinodes_cache_check(ino)==0 || keep_cache>0) {
	//	usedinodes_cache_add(ino);
	//	fileinfo = mfs_newfileinfo(fi->flags & O_ACCMODE,ino);
	//	fi->fh = (unsigned long)fileinfo;
	//	if (keep_cache==1) {
	//		fi->keep_cache=1;
	//	} else {
	//		fi->keep_cache=0;
	//	}
	//	if (debug_mode) {
	//		fprintf(stderr,"open (%lu) ok -> keep cache: %lu\n",(unsigned long int)ino,(unsigned long int)fi->keep_cache);
	//	}
	//	fi->direct_io = 0;
	//	oplog_printf(&ctx,"open (%lu): OK (%lu,%lu)",(unsigned long int)ino,(unsigned long int)fi->direct_io,(unsigned long int)fi->keep_cache);
	//	if (fuse_reply_open(req, fi) == -ENOENT) {
	//		mfs_removefileinfo(fileinfo);
	//	}
	//	status = fs_opencheck(ino,ctx.uid,ctx.gid,oflags,attr);
	//	status = mfs_errorconv(status);
	//	if (status!=0) {
	//		pthread_mutes_lock(&(fileinfo->lock));
	//		fileinfo->status = status;
	//		pthread_mutex_unlock(&(fileinfo->lock));
	//	}
	// } else {
	if (full_permissions) {
		gids = groups_get_x(ctx.pid,ctx.uid,ctx.gid,2);
		status = fs_opencheck(ino,ctx.uid,gids->gidcnt,gids->gidtab,oflags,attr);
		groups_rel(gids);
	} else {
		uint32_t gidtmp = ctx.gid;
		status = fs_opencheck(ino,ctx.uid,1,&gidtmp,oflags,attr);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"open (%lu): %s",(unsigned long int)ino,strerr(status));
		fuse_reply_err(req, status);
		return ;
	}

	mattr = mfs_attr_get_mattr(attr);
	fileinfo = mfs_newfileinfo(fi->flags & O_ACCMODE,ino);
	fi->fh = (unsigned long)fileinfo;
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"open (%lu) ok -> keep cache: %lu\n",(unsigned long int)ino,(unsigned long int)fi->keep_cache);
	}
	fi->direct_io = 0;
	oplog_printf(&ctx,"open (%lu): OK (%lu,%lu)",(unsigned long int)ino,(unsigned long int)fi->direct_io,(unsigned long int)fi->keep_cache);
	if (fuse_reply_open(req, fi) == -ENOENT) {
		mfs_removefileinfo(fileinfo);
	}
	// }
}

void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_RELEASE);
	if (debug_mode) {
		oplog_printf(&ctx,"release (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"release (%lu)\n",(unsigned long int)ino);
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
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));		// make helgrind happy
			if (statsinfo->buff!=NULL) {
				free(statsinfo->buff);
			}
			if (statsinfo->reset) {
				stats_reset_all();
			}
			pthread_mutex_unlock(&(statsinfo->lock));	// make helgrind happy
			pthread_mutex_destroy(&(statsinfo->lock));	// make helgrind happy
			free(statsinfo);
		}
		oplog_printf(&ctx,"release (%lu) (internal node: STATS): OK",(unsigned long int)ino);
		fuse_reply_err(req,0);
		return;
	}
	if (ino==MOOSE_INODE) {
		oplog_printf(&ctx,"release (%lu) (internal node: MOOSE): OK",(unsigned long int)ino);
		fuse_reply_err(req,0);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_releasehandle(fi->fh);
		oplog_printf(&ctx,"release (%lu) (internal node: %s): OK",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY");
		fuse_reply_err(req,0);
		return;
	}
//	fuse_reply_err(req,0);
	if (fileinfo!=NULL) {
		mfs_removefileinfo(fileinfo);
	}
	dcache_invalidate_attr(ino);
	fs_release(ino);
	oplog_printf(&ctx,"release (%lu): OK",(unsigned long int)ino);
	fuse_reply_err(req,0);
}

void mfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
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
			oplog_printf(&ctx,"read (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
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
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
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
	if (fileinfo==NULL) {
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
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
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITEONLY) {
		pthread_mutex_unlock(&(fileinfo->lock));
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		fuse_reply_err(req,EACCES);
		return;
	}
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress++;
#endif
	if (fileinfo->mode==IO_WRITE) {
		err = write_data_flush(fileinfo->data);
		if (err!=0) {
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
			fileinfo->ops_in_progress--;
			fileinfo->lastuse = monotonic_seconds();
#endif
			pthread_mutex_unlock(&(fileinfo->lock));
			if (debug_mode) {
				fprintf(stderr,"IO error occured while writing inode %lu\n",(unsigned long int)ino);
			}
			oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
			fuse_reply_err(req,err);
			return;
		}
		write_data_end(fileinfo->data);
	}
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_READ;
		fileinfo->data = read_data_new(ino);
	}
	write_data_flush_inode(ino);
/* stable version
	ssize = size;
	buff = NULL;	// use internal 'readdata' buffer
	err = read_data(fileinfo->data,off,&ssize,&buff);
*/
	ssize = size;
	err = read_data(fileinfo->data,off,&ssize,&buffptr,&iov,&iovcnt);

	if (err!=0) {
		if (debug_mode) {
			fprintf(stderr,"IO error occured while reading inode %lu\n",(unsigned long int)ino);
		}
		oplog_printf(&ctx,"read (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
		fuse_reply_err(req,err);
	} else {
		if (debug_mode) {
			fprintf(stderr,"%"PRIu32" bytes have been read from inode %lu\n",ssize,(unsigned long int)ino);
		}
		oplog_printf(&ctx,"read (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)ssize);
//		fuse_reply_buf(req,(char*)buff,ssize);
		fuse_reply_iov(req,iov,iovcnt);
	}
//	read_data_freebuff(fileinfo->data);
	read_data_free_buff(fileinfo->data,buffptr,iov);
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress--;
	fileinfo->lastuse = monotonic_seconds();
#endif
	pthread_mutex_unlock(&(fileinfo->lock));
}

void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_WRITE);
	if (debug_mode) {
		oplog_printf(&ctx,"write (%lu,%llu,%llu) ...",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
		fprintf(stderr,"write to inode %lu %llu bytes at position %llu\n",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off);
	}
	if (ino==MASTERINFO_INODE || ino==OPLOG_INODE || ino==OPHISTORY_INODE || ino==MOOSE_INODE) {
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		fuse_reply_err(req,EACCES);
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
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
	if (fileinfo==NULL) {
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EBADF));
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
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_READONLY) {
		pthread_mutex_unlock(&(fileinfo->lock));
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(EACCES));
		fuse_reply_err(req,EACCES);
		return;
	}
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress++;
#endif
	if (fileinfo->mode==IO_READ) {
		read_data_end(fileinfo->data);
	}
	if (fileinfo->mode==IO_READ || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_WRITE;
		fileinfo->data = write_data_new(ino);
	}
	err = write_data(fileinfo->data,off,size,(const uint8_t*)buf);
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress--;
	fileinfo->lastuse = monotonic_seconds();
#endif
	if (err!=0) {
		pthread_mutex_unlock(&(fileinfo->lock));
		if (debug_mode) {
			fprintf(stderr,"IO error occured while writing inode %lu\n",(unsigned long int)ino);
		}
		oplog_printf(&ctx,"write (%lu,%llu,%llu): %s",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,strerr(err));
		fuse_reply_err(req,err);
	} else {
		pthread_mutex_unlock(&(fileinfo->lock));
		if (debug_mode) {
			fprintf(stderr,"%llu bytes have been written to inode %lu\n",(unsigned long long int)size,(unsigned long int)ino);
		}
		oplog_printf(&ctx,"write (%lu,%llu,%llu): OK (%lu)",(unsigned long int)ino,(unsigned long long int)size,(unsigned long long int)off,(unsigned long int)size);
		read_inode_dirty_region(ino,off,size,buf);
		fuse_reply_write(req,size);
	}
}

void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_FLUSH);
	if (debug_mode) {
		oplog_printf(&ctx,"flush (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"flush (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"flush (%lu): OK",(unsigned long int)ino);
		fuse_reply_err(req,0);
		return;
	}
	if (fileinfo==NULL) {
		oplog_printf(&ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
//	syslog(LOG_NOTICE,"remove_locks inode:%lu owner:%llu",(unsigned long int)ino,(unsigned long long int)fi->lock_owner);
	err = 0;
//	fuse_reply_err(req,err);
	pthread_mutex_lock(&(fileinfo->lock));
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress++;
#endif
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress--;
	fileinfo->lastuse = monotonic_seconds();
#endif
	pthread_mutex_unlock(&(fileinfo->lock));
	if (err!=0) {
		oplog_printf(&ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(err));
	} else {
		dcache_invalidate_attr(ino);
		oplog_printf(&ctx,"flush (%lu): OK",(unsigned long int)ino);
	}
	fuse_reply_err(req,err);
}

void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;
	struct fuse_ctx ctx;

	ctx = *(fuse_req_ctx(req));
	mfs_stats_inc(OP_FSYNC);
	if (debug_mode) {
		oplog_printf(&ctx,"fsync (%lu,%d) ...",(unsigned long int)ino,datasync);
		fprintf(stderr,"fsync (%lu,%d)\n",(unsigned long int)ino,datasync);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(&ctx,"fsync (%lu,%d): OK",(unsigned long int)ino,datasync);
		fuse_reply_err(req,0);
		return;
	}
	if (fileinfo==NULL) {
		oplog_printf(&ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(EBADF));
		fuse_reply_err(req,EBADF);
		return;
	}
	err = 0;
	pthread_mutex_lock(&(fileinfo->lock));
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress++;
#endif
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
#ifdef FREEBSD_EARLY_RELEASE_BUG_WORKAROUND
	fileinfo->ops_in_progress--;
	fileinfo->lastuse = monotonic_seconds();
#endif
	pthread_mutex_unlock(&(fileinfo->lock));
	if (err!=0) {
		oplog_printf(&ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(err));
	} else {
		dcache_invalidate_attr(ino);
		oplog_printf(&ctx,"fsync (%lu,%d): OK",(unsigned long int)ino,datasync);
	}
	fuse_reply_err(req,err);
}

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

int mfs_getacl(fuse_req_t req, fuse_ino_t ino, uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t aclxattr,const uint8_t **buff,uint32_t *leng) {
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
	status = fs_getacl(ino,opened,uid,gids,gid,aclxattr,&userperm,&groupperm,&otherperm,&maskperm,&namedusers,&namedgroups,&namedacls,&namedaclssize);

	if (status!=STATUS_OK) {
		return status;
	}

	if (((namedusers+namedgroups)*6U) != namedaclssize) {
		return ERROR_EINVAL;
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
	b+=8;
//	fprintf(stderr,"getacl buff end ptr: %p\n",(void*)b);
	return STATUS_OK;
}

int mfs_setacl(fuse_req_t req,fuse_ino_t ino,uint32_t uid,uint8_t aclxattr,const char *buff,uint32_t leng) {
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
		return ERROR_EINVAL;
	}

	if (buff[0]!=2) {
		return ERROR_EINVAL;
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
				return ERROR_EINVAL;
			}
			userperm = *(const uint16_t*)(buff+6+i*8);
		}
		if (tag & 2) {
			namedusers++;
		}
		if (tag & 4) {
			if (groupperm!=0xFFFF) {
				return ERROR_EINVAL;
			}
			groupperm = *(const uint16_t*)(buff+6+i*8);
		}
		if (tag & 8) {
			namedgroups++;
		}
		if (tag & 16) {
			if (maskperm!=0xFFFF) {
				return ERROR_EINVAL;
			}
			maskperm = *(const uint16_t*)(buff+6+i*8);
		}
		if (tag & 32) {
			if (otherperm!=0xFFFF) {
				return ERROR_EINVAL;
			}
			otherperm = *(const uint16_t*)(buff+6+i*8);
		}
	}
	if (maskperm==0xFFFF && (namedusers|namedgroups)>0) {
		return ERROR_EINVAL;
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
	return fs_setacl(ino,uid,aclxattr,userperm,groupperm,otherperm,maskperm,namedusers,namedgroups,namedacls,(namedusers+namedgroups)*6);
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
#if defined(XATTR_CREATE) && defined(XATTR_REPLACE)
	if ((flags&XATTR_CREATE) && (flags&XATTR_REPLACE)) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(EINVAL));
		fuse_reply_err(req,EINVAL);
		return;
	}
	mode = (flags==XATTR_CREATE)?MFS_XATTR_CREATE_ONLY:(flags==XATTR_REPLACE)?MFS_XATTR_REPLACE_ONLY:MFS_XATTR_CREATE_OR_REPLACE;
#else
	mode = 0;
#endif
	aclxattr = 0;
	if (strcmp(name,"system.posix_acl_access")==0) {
		aclxattr=1;
	} else if (strcmp(name,"system.posix_acl_default")==0) {
		aclxattr=2;
	}
	(void)position;
	if (xattr_cache_on) {
		xattr_cache_del(ino,nleng,(const uint8_t*)name);
	}
	if (aclxattr && xattr_acl_support==0) {
		oplog_printf(&ctx,"setxattr (%lu,%s,%llu,%d): %s",(unsigned long int)ino,name,(unsigned long long int)size,flags,strerr(ENOTSUP));
		fuse_reply_err(req,ENOTSUP);
		return;
	}
	if (aclxattr) {
		status = mfs_setacl(req,ino,ctx.uid,aclxattr,value,size);
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
	fuse_reply_err(req,0);
}

#if defined(__APPLE__)
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position) {
#else
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) {
	uint32_t position=0;
#endif /* __APPLE__ */
	uint32_t nleng;
	uint8_t attr[35];
	int status;
	uint8_t mode;
	const uint8_t *buff;
	uint32_t leng;
	struct fuse_ctx ctx;
	groups *gids;
	void *xattr_value_release;
	uint8_t aclxattr;

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
	aclxattr = 0;
	if (strcmp(name,"system.posix_acl_access")==0) {
		aclxattr=1;
	} else if (strcmp(name,"system.posix_acl_default")==0) {
		aclxattr=2;
	}
	if (aclxattr && xattr_acl_support==0) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu): %s",(unsigned long int)ino,name,(unsigned long long int)size,strerr(ENOTSUP));
		fuse_reply_err(req,ENOTSUP);
		return;
	}
	(void)position;
	gids = NULL; // make gcc happy
	if (full_permissions) {
		if (strcmp(name,"com.apple.quarantine")==0) {
			gids = groups_get_x(ctx.pid,ctx.uid,ctx.gid,1);
		} else {
			gids = groups_get(ctx.pid,ctx.uid,ctx.gid);
		}
	}
	xattr_value_release = NULL;
	if (xattr_cache_on) {
		xattr_value_release = xattr_cache_get(ino,ctx.uid,ctx.gid,nleng,(const uint8_t*)name,&buff,&leng,&status);
		if (xattr_value_release==NULL) {
			if (usedircache && dcache_getattr(&ctx,ino,attr) && (mfs_attr_get_mattr(attr)&MATTR_NOXATTR)) { // no xattr
				status = ERROR_ENOATTR;
				buff = NULL;
				leng = 0;
			} else {
				if (aclxattr) {
					if (full_permissions) {
						status = mfs_getacl(req,ino,0,ctx.uid,gids->gidcnt,gids->gidtab,aclxattr,&buff,&leng);
					} else {
						uint32_t gidtmp = ctx.gid;
						status = mfs_getacl(req,ino,0,ctx.uid,1,&gidtmp,aclxattr,&buff,&leng);
					}
				} else {
					if (full_permissions) {
						status = fs_getxattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,nleng,(const uint8_t*)name,MFS_XATTR_GETA_DATA,&buff,&leng);
					} else {
						uint32_t gidtmp = ctx.gid;
						status = fs_getxattr(ino,0,ctx.uid,1,&gidtmp,nleng,(const uint8_t*)name,MFS_XATTR_GETA_DATA,&buff,&leng);
					}
				}
			}
			xattr_cache_set(ino,ctx.uid,ctx.gid,nleng,(const uint8_t*)name,buff,leng,status);
		} else if (debug_mode) {
			fprintf(stderr,"getxattr: sending data from cache\n");
		}
	} else {
		if (usedircache && dcache_getattr(&ctx,ino,attr) && (mfs_attr_get_mattr(attr)&MATTR_NOXATTR)) { // no xattr
			status = ERROR_ENOATTR;
			buff = NULL;
			leng = 0;
		} else {
			if (aclxattr) {
				if (full_permissions) {
					status = mfs_getacl(req,ino,0,ctx.uid,gids->gidcnt,gids->gidtab,aclxattr,&buff,&leng);
				} else {
					uint32_t gidtmp = ctx.gid;
					status = mfs_getacl(req,ino,0,ctx.uid,1,&gidtmp,aclxattr,&buff,&leng);
				}
			} else {
				if (full_permissions) {
					status = fs_getxattr(ino,0,ctx.uid,gids->gidcnt,gids->gidtab,nleng,(const uint8_t*)name,mode,&buff,&leng);
				} else {
					uint32_t gidtmp = ctx.gid;
					status = fs_getxattr(ino,0,ctx.uid,1,&gidtmp,nleng,(const uint8_t*)name,mode,&buff,&leng);
				}
			}
		}
	}
	if (full_permissions) {
		groups_rel(gids);
	}
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: %s",(unsigned long int)ino,name,(unsigned long long int)size,(xattr_value_release==NULL)?"":" (using cache)",strerr(status));
		fuse_reply_err(req,status);
		if (xattr_value_release!=NULL) {
			xattr_cache_rel(xattr_value_release);
		}
		return;
	}
	if (size==0) {
		oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: OK (%"PRIu32")",(unsigned long int)ino,name,(unsigned long long int)size,(xattr_value_release==NULL)?"":" (using cache)",leng);
		fuse_reply_xattr(req,leng);
	} else {
		if (leng>size) {
			oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: %s",(unsigned long int)ino,name,(unsigned long long int)size,(xattr_value_release==NULL)?"":" (using cache)",strerr(ERANGE));
			fuse_reply_err(req,ERANGE);
		} else {
			oplog_printf(&ctx,"getxattr (%lu,%s,%llu)%s: OK (%"PRIu32")",(unsigned long int)ino,name,(unsigned long long int)size,(xattr_value_release==NULL)?"":" (using cache)",leng);
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
	uint8_t attr[35];
	int status;
	uint8_t mode;
	struct fuse_ctx ctx;
	groups *gids;

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
	if (usedircache && dcache_getattr(&ctx,ino,attr) && (mfs_attr_get_mattr(attr)&MATTR_NOXATTR)) { // no xattr
		status = STATUS_OK;
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
	struct fuse_ctx ctx;
	groups *gids;
	uint8_t aclxattr;

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
	aclxattr = 0;
	if (strcmp(name,"system.posix_acl_access")==0) {
		aclxattr=1;
	} else if (strcmp(name,"system.posix_acl_default")==0) {
		aclxattr=2;
	}
	if (aclxattr && xattr_acl_support==0) {
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
	if (xattr_cache_on) {
		xattr_cache_del(ino,nleng,(const uint8_t*)name);
	}
	if (aclxattr) {
		status = fs_setacl(ino,ctx.uid,aclxattr,0xFFFF,0xFFFF,0xFFFF,0xFFFF,0,0,NULL,0);
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
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(&ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(status));
		fuse_reply_err(req,status);
	} else {
		oplog_printf(&ctx,"removexattr (%lu,%s): OK",(unsigned long int)ino,name);
		fuse_reply_err(req,0);
	}
}

void mfs_init(int debug_mode_in,int keep_cache_in,double direntry_cache_timeout_in,double entry_cache_timeout_in,double attr_cache_timeout_in,double xattr_cache_timeout_in,double groups_cache_timeout,int mkdir_copy_sgid_in,int sugid_clear_mode_in,int xattr_acl_support_in) {
	const char* sugid_clear_mode_strings[] = {SUGID_CLEAR_MODE_STRINGS};
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
	if (groups_cache_timeout>0.0) {
		groups_init(groups_cache_timeout,debug_mode);
		full_permissions = 1;
	} else {
		full_permissions = 0;
	}
	mfs_aclstorage_init();
	if (debug_mode) {
		fprintf(stderr,"cache parameters: file_keep_cache=%s direntry_cache_timeout=%.2lf entry_cache_timeout=%.2lf attr_cache_timeout=%.2lf xattr_cache_timeout_in=%.2lf (%s)\n",(keep_cache==1)?"always":(keep_cache==2)?"never":"auto",direntry_cache_timeout,entry_cache_timeout,attr_cache_timeout,xattr_cache_timeout_in,xattr_cache_on?"on":"off");
		fprintf(stderr,"mkdir copy sgid=%d\nsugid clear mode=%s\n",mkdir_copy_sgid_in,(sugid_clear_mode_in<SUGID_CLEAR_MODE_OPTIONS)?sugid_clear_mode_strings[sugid_clear_mode_in]:"???");
	}
	mfs_statsptr_init();
}
