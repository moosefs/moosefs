/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
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

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#ifdef HAVE_SYS_FILE_H
#include <sys/file.h>
#endif

#include "MFSCommunication.h"
#include "datapack.h"

#if 0
#include "mastercomm.h"
#include "inoleng.h"
#include "csorder.h"
#include "readdata.h"
#include "writedata.h"
#include "truncate.h"
#include "csdb.h"
#include "delayrun.h"
#include "conncache.h"
#include "chunkrwlock.h"
#include "chunksdatacache.h"
#include "portable.h"
#include "stats.h"
#include "crc.h"
#include "strerr.h"
#include "cfg.h"
#include "mfsstrerr.h"
#include "massert.h"
#include "md5.h"
#endif

#include "idstr.h"

#include "mfsioint.h"
#include "mfsio.h"

// #define DEBUG

// #define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

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
		case MFS_ERROR_ENAMETOOLONG:
			ret=ENAMETOOLONG;
			break;
		case MFS_ERROR_EMLINK:
			ret=EMLINK;
			break;
		case MFS_ERROR_EBADF:
			ret=EBADF;
			break;
		case MFS_ERROR_EFBIG:
			ret=EFBIG;
			break;
		case MFS_ERROR_EISDIR:
			ret=EISDIR;
			break;
		default:
			ret=EINVAL;
			break;
	}
	return ret;
}

#if 0
static inline uint8_t mfs_type_convert(uint8_t type) {
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

// for future use
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
#endif

#define PKGVERSION ((VERSMAJ)*1000000+(VERSMID)*10000+((VERSMIN)>>1)*100+(RELEASE))

static void mfsstatfs_to_statvfs(mfs_int_statfsrec *mfsstatfs,struct statvfs *stvfsbuf) {
	const uint32_t bsize = 0x10000;
	stvfsbuf->f_bsize = bsize;
	stvfsbuf->f_frsize = bsize;
	stvfsbuf->f_blocks = mfsstatfs->totalspace/bsize;
	stvfsbuf->f_bfree = mfsstatfs->freespace/bsize;
	stvfsbuf->f_bavail = mfsstatfs->availspace/bsize;
	stvfsbuf->f_files = 1100000000+PKGVERSION+mfsstatfs->inodes;
	stvfsbuf->f_ffree = 1100000000+PKGVERSION;
	stvfsbuf->f_favail = 1100000000+PKGVERSION;
	stvfsbuf->f_namemax = MFS_NAME_MAX;
	stvfsbuf->f_fsid = mfsstatfs->sessionid;
}

static void mfsstat_to_stat(mfs_int_statrec *mfsstat,struct stat *stbuf) {
	stbuf->st_ino = mfsstat->inode;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	stbuf->st_blksize = MFSBLOCKSIZE;
#endif
	switch (mfsstat->type & 0x7F) {
		case TYPE_DIRECTORY:
			stbuf->st_mode = S_IFDIR | mfsstat->mode;
			break;
		case TYPE_SYMLINK:
			stbuf->st_mode = S_IFLNK | mfsstat->mode;
			break;
		case TYPE_FILE:
			stbuf->st_mode = S_IFREG | mfsstat->mode;
			break;
		case TYPE_FIFO:
			stbuf->st_mode = S_IFIFO | mfsstat->mode;
			break;
		case TYPE_SOCKET:
			stbuf->st_mode = S_IFSOCK | mfsstat->mode;
			break;
		case TYPE_BLOCKDEV:
			stbuf->st_mode = S_IFBLK | mfsstat->mode;
			break;
		case TYPE_CHARDEV:
			stbuf->st_mode = S_IFCHR | mfsstat->mode;
			break;
		default:
			stbuf->st_mode = 0;
	}
	stbuf->st_uid = mfsstat->uid;
	stbuf->st_gid = mfsstat->gid;
	stbuf->st_atime = mfsstat->atime;
	stbuf->st_mtime = mfsstat->mtime;
	stbuf->st_ctime = mfsstat->ctime;
#ifdef HAVE_STRUCT_STAT_ST_BIRTHTIME
	stbuf->st_birthtime = mfsstat->ctime;        // for future use
#endif
	stbuf->st_nlink = mfsstat->nlink;
	stbuf->st_size = mfsstat->length;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
	stbuf->st_blocks = (mfsstat->length+511)/512;
#endif
#ifdef HAVE_STRUCT_STAT_ST_RDEV
	stbuf->st_rdev = mfsstat->dev;
#endif
}

typedef struct _cred {
	uint32_t uid;
	uint32_t gidcnt;
	uint32_t gidtab[MFS_NGROUPS_MAX+1];
} cred;

#define CRED_BASIC 0
#define CRED_UMASK 1

static void mfs_get_credentials(mfs_int_cred *ctx,uint8_t mode) {
	static mode_t last_umask = 0;
	gid_t gids[MFS_NGROUPS_MAX];
	gid_t gid;
	uint32_t i,j;

	ctx->uid = geteuid();
	ctx->gidcnt = getgroups(MFS_NGROUPS_MAX,gids);
	gid = getegid();
	ctx->gidtab[0] = gid;
	for (i=0,j=1 ; i<ctx->gidcnt ; i++) {
		if (gids[i]!=gid) {
			ctx->gidtab[j++] = gids[i];
		}
	}
	ctx->gidcnt = j;
	if (mode==CRED_UMASK) {
		// mutex lock ???
		last_umask = umask(last_umask); // This is potential race-condition, but there is no portable way to obtain umask atomically. Last umask is remembered to minimize probability of changing umask here.
		umask(last_umask);
		ctx->umask = last_umask;
		// mutex unlock ???
	}
}

#if 0
static int mfs_path_to_inodes(const char *path,uint32_t *parent,uint32_t *inode,uint8_t name[256],uint8_t *nleng,uint8_t existflag,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint32_t cinode = MFS_ROOT_ID;
	uint32_t pinode = MFS_ROOT_ID;
	cred cr;
	const char *pptr = path;
	uint8_t partlen,status;

	mfs_get_credentials(&cr);
	if (inode!=NULL) {
		*inode = 0;
	}
	memset(attr,0,ATTR_RECORD_SIZE);
	if (path[0]==0) {
		errno = EINVAL;
		return -1;
	}
	if (path[0]=='/' && path[1]==0) {
		if (existflag==PATH_TO_INODES_SKIP_LAST) {
			if (parent!=NULL) {
				*parent = pinode;
			}
			if (inode!=NULL) {
				*inode = cinode;
			}
			name[0] = '.';
			name[1] = 0;
			*nleng = 1;
			return 0;
		}
		name[0] = '.';
		status = fs_simple_lookup(pinode,1,name,cr.uid,cr.gidcnt,cr.gidtab,&cinode,attr);
		name[0] = 0;
		if (status!=MFS_STATUS_OK) {
			errno = mfs_errorconv(status);
			return -1;
		}
		if (parent!=NULL) {
			*parent = pinode;
		}
		if (inode!=NULL) {
			*inode = cinode;
		}
		*nleng = 0;
		return 0;
	}
	partlen = 0;
	while (*pptr) {
		if (*pptr=='/') {
			pinode = cinode;
			if (partlen>0) {
				name[partlen] = 0;
#ifdef DEBUG
				printf("perform lookup for (%u,%s) (internal part)\n",pinode,(char*)name);
#endif
				status = fs_simple_lookup(pinode,partlen,name,cr.uid,cr.gidcnt,cr.gidtab,&cinode,attr);
				if (status!=MFS_STATUS_OK) {
					errno = mfs_errorconv(status);
					return -1;
				}
				if (mfs_attr_get_type(attr)!=TYPE_DIRECTORY) {
					errno = ENOTDIR;
					return -1;
				}
#ifdef DEBUG
				printf("result inode: %u\n",cinode);
#endif
			}
			partlen = 0;
		} else {
			if (partlen==255) {
				errno = ENAMETOOLONG;
				return -1; // name too long
			}
			name[partlen++] = *pptr;
		}
		pptr++;
	}
	pinode = cinode;
	if (partlen>0 && existflag!=PATH_TO_INODES_SKIP_LAST) {
#ifdef DEBUG
		printf("perform lookup for (%u,%s) (last part)\n",pinode,(char*)name);
#endif
		status = fs_simple_lookup(pinode,partlen,name,cr.uid,cr.gidcnt,cr.gidtab,&cinode,attr);
#ifdef DEBUG
		if (status==MFS_STATUS_OK) {
			printf("result inode: %u\n",cinode);
		} else {
			printf("lookup error: %s\n",mfsstrerr(status));
		}
#endif
		if (existflag==PATH_TO_INODES_EXPECT_NOENTRY) {
			if (status==MFS_STATUS_OK) {
				if (inode!=NULL) {
					*inode = cinode;
				}
				errno = EEXIST;
				return -1;
			} else if (status!=MFS_ERROR_ENOENT) {
				errno = mfs_errorconv(status);
				return -1;
			}
		} else if (existflag==PATH_TO_INODES_EXPECT_OBJECT) {
			if (status!=MFS_STATUS_OK) {
				errno = mfs_errorconv(status);
				return -1;
			}
		} else {
			if (status!=MFS_STATUS_OK) {
				cinode = 0;
			}
		}
		name[partlen] = 0;
	}
	if (parent!=NULL) {
		*parent = pinode;
	}
	if (inode!=NULL) {
		*inode = cinode;
	}
	*nleng = partlen;
	return 0;
}

static uint8_t mfs_attr_to_type(const uint8_t attr[ATTR_RECORD_SIZE]) {
	const uint8_t *ptr;
	ptr = attr;
	if (ptr[0]<64) {
		return attr[1]>>4;
	} else {
		return mfs_type_convert(attr[0]&0x7f);
	}
	return 0;
}

static uint64_t mfs_attr_to_size(const uint8_t attr[ATTR_RECORD_SIZE]) {
	const uint8_t *ptr;
	ptr = attr+27;
	return get64bit(&ptr);
}

enum {MFS_IO_READWRITE,MFS_IO_READONLY,MFS_IO_WRITEONLY,MFS_IO_READAPPEND,MFS_IO_APPENDONLY,MFS_IO_FORBIDDEN};

typedef struct file_info {
	void *flengptr;
	uint32_t inode;
	uint8_t mode;
	uint8_t writing;
	off_t offset;
	uint32_t readers_cnt;
	uint32_t writers_cnt;
	void *rdata,*wdata;
	pthread_mutex_t lock;
	pthread_cond_t rwcond;
} file_info;

static file_info *fdtab;
static uint32_t fdtabsize;
static uint32_t *fdtabusemask;
static pthread_mutex_t fdtablock;

#define FDTABSIZE_INIT 1024

static void mfs_fi_init(file_info *fileinfo) {
	memset(fileinfo,0,sizeof(file_info));
	fileinfo->mode = MFS_IO_FORBIDDEN;
	zassert(pthread_mutex_init(&(fileinfo->lock),NULL));
	zassert(pthread_cond_init(&(fileinfo->rwcond),NULL));
}

static void mfs_fi_term(file_info *fileinfo) {
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	zassert(pthread_mutex_destroy(&(fileinfo->lock)));
	zassert(pthread_cond_destroy(&(fileinfo->rwcond)));
}

static void mfs_resize_fd(void) {
	file_info *newfdtab;
	uint32_t *newfdtabusemask;
	uint32_t newfdtabsize;
	uint32_t i;

	newfdtabsize = fdtabsize * 2;

	newfdtab = realloc(fdtab,sizeof(file_info)*newfdtabsize);
	passert(newfdtab);
	newfdtabusemask = realloc(fdtabusemask,sizeof(uint32_t)*((newfdtabsize+31)/32));
	passert(newfdtabusemask);
	fdtab = newfdtab;
	fdtabusemask = newfdtabusemask;
	for (i=fdtabsize ; i<newfdtabsize ; i++) {
		mfs_fi_init(fdtab+i);
	}
	i = fdtabsize+31/32;
	memset(fdtabusemask+i,0,sizeof(uint32_t)*(((newfdtabsize+31)/32)-i));
	if ((fdtabsize&0x1F)!=0) {
		fdtabusemask[i-1] &= (0xFFFFFFFF >> (0x20-(fdtabsize&0x1F)));
	}
	fdtabsize = newfdtabsize;
}

static int mfs_next_fd(void) {
	uint32_t i,m;
	int fd;
	zassert(pthread_mutex_lock(&fdtablock));
	for (i=0 ; i<(fdtabsize+31)/32 ; i++) {
		if (fdtabusemask[i]!=0xFFFFFFFF) {
			fd = i*32;
			m = fdtabusemask[i];
			while (m&1) {
				fd++;
				m>>=1;
			}
			while ((uint32_t)fd>=fdtabsize) {
				mfs_resize_fd();
			}
			fdtabusemask[fd>>5] |= (1<<(fd&0x1F));
			zassert(pthread_mutex_unlock(&fdtablock));
			return fd;
		}
	}
	fd = fdtabsize;
	mfs_resize_fd();
	fdtabusemask[fd>>5] |= (1<<(fd&0x1F));
	zassert(pthread_mutex_unlock(&fdtablock));
	return fd;
}

static void mfs_free_fd(int fd) {
	uint32_t i,m;
	zassert(pthread_mutex_lock(&fdtablock));
	if (fd>=0 && (uint32_t)fd<fdtabsize) {
		i = fd>>5;
		m = 1<<(fd&0x1F);
		fdtabusemask[i] &= ~m;
	}
	zassert(pthread_mutex_unlock(&fdtablock));
}

static file_info* mfs_get_fi(int fd) {
	uint32_t i,m;
	zassert(pthread_mutex_lock(&fdtablock));
	if (fd>=0 && (uint32_t)fd<fdtabsize) {
		i = fd>>5;
		m = 1<<(fd&0x1F);
		if (fdtabusemask[i] & m) {
			zassert(pthread_mutex_unlock(&fdtablock));
			return fdtab+fd;
		}
	}
	zassert(pthread_mutex_unlock(&fdtablock));
	return NULL;
}

static void finfo_change_fleng(uint32_t inode,uint64_t fleng) {
	inoleng_update_fleng(inode,fleng);
}

static int sugid_clear_mode = 0;
static int mkdir_copy_sgid = 0;

static mode_t last_umask = 0;

#endif

int mfs_mknod(const char *path, mode_t mode, dev_t dev) {
	uint8_t status;
	uint8_t type;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_UMASK);
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
		errno = EPERM;
		return -1;
	}
	status = mfs_int_mknod(&cr,path,type,mode,dev);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_unlink(const char *path) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_unlink(&cr,path);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_mkdir(const char *path, mode_t mode) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_UMASK);
	status = mfs_int_mkdir(&cr,path,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_rmdir(const char *path) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_rmdir(&cr,path);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_rename(const char *src, const char *dst) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_rename(&cr,src,dst);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_link(const char *src, const char *dst) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_link(&cr,src,dst);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_symlink(const char *path1, const char *path2) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_symlink(&cr,path1,path2);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

ssize_t mfs_readlink(const char *path, char *buf, size_t bufsize) {
	uint8_t status;
	char lnkbuff[MFS_SYMLINK_MAX];
	ssize_t leng;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_readlink(&cr,path,lnkbuff);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	lnkbuff[MFS_SYMLINK_MAX-1]=0;
	leng = strlen(lnkbuff);
	if ((size_t)leng>bufsize) {
		leng = bufsize;
	}
	memcpy(buf,lnkbuff,leng);
	return leng;
}

int mfs_chmod(const char *path, mode_t mode) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_chmod(&cr,path,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_fchmod(int fildes, mode_t mode) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_fchmod(&cr,fildes,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_chown(const char *path, uid_t owner, gid_t group) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_chown(&cr,path,(owner!=(uid_t)-1)?owner:MFS_UGID_NONE,(group!=(gid_t)-1)?group:MFS_UGID_NONE);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_fchown(int fildes, uid_t owner, gid_t group) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_fchown(&cr,fildes,(owner!=(uid_t)-1)?owner:MFS_UGID_NONE,(group!=(gid_t)-1)?group:MFS_UGID_NONE);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_utimes(const char *path, const struct timeval times[2]) {
	uint8_t status;
	uint8_t flags;
	uint32_t atime,mtime;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	if (times==NULL) {
		flags = MFS_TIMES_ATIME_NOW | MFS_TIMES_MTIME_NOW;
		atime = 0;
		mtime = 0;
	} else {
		flags = 0;
		atime = times[0].tv_sec;
		mtime = times[1].tv_sec;
	}
	status = mfs_int_utimes(&cr,path,flags,atime,mtime);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_futimes(int fildes, const struct timeval times[2]) {
	uint8_t status;
	uint8_t flags;
	uint32_t atime,mtime;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	if (times==NULL) {
		flags = MFS_TIMES_ATIME_NOW | MFS_TIMES_MTIME_NOW;
		atime = 0;
		mtime = 0;
	} else {
		flags = 0;
		atime = times[0].tv_sec;
		mtime = times[1].tv_sec;
	}
	status = mfs_int_futimes(&cr,fildes,flags,atime,mtime);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_futimens(int fildes, const struct timespec times[2]) {
	uint8_t status;
	uint8_t flags;
	uint32_t atime,mtime;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	atime = 0;
	mtime = 0;
	flags = 0;
	if (times==NULL) {
		flags = MFS_TIMES_ATIME_NOW | MFS_TIMES_MTIME_NOW;
	} else {
		if (times[0].tv_nsec == UTIME_NOW) {
			flags |= MFS_TIMES_ATIME_NOW;
		} else if (times[0].tv_nsec == UTIME_OMIT) {
			flags |= MFS_TIMES_ATIME_OMIT;
		} else {
			atime = times[0].tv_sec;
		}
		if (times[1].tv_nsec == UTIME_NOW) {
			flags |= MFS_TIMES_MTIME_NOW;
		} else if (times[1].tv_nsec == UTIME_OMIT) {
			flags |= MFS_TIMES_MTIME_OMIT;
		} else {
			mtime = times[1].tv_sec;
		}
	}
	status = mfs_int_futimes(&cr,fildes,flags,atime,mtime);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_truncate(const char *path, off_t size) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_truncate(&cr,path,size);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_ftruncate(int fildes, off_t size) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_ftruncate(&cr,fildes,size);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

off_t mfs_lseek(int fildes, off_t offset, int whence) {
	int64_t ioffset;
	uint8_t iwhence;
	uint8_t status;

	ioffset = offset;
	switch (whence) {
		case SEEK_SET:
			iwhence = MFS_SEEK_SET;
			break;
		case SEEK_CUR:
			iwhence = MFS_SEEK_CUR;
			break;
		case SEEK_END:
			iwhence = MFS_SEEK_END;
			break;
		default:
			errno = EINVAL;
			return -1;
	}
	status = mfs_int_lseek(fildes,&ioffset,iwhence);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return ioffset;
}

int mfs_statvfs(const char *path, struct statvfs *buf) {
	uint8_t status;
	mfs_int_statfsrec stvfs;

	(void)path; // ignore path - there is one filesystem here
	status = mfs_int_statfs(&stvfs);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	memset(buf,0,sizeof(struct statvfs));
	mfsstatfs_to_statvfs(&stvfs,buf);
	return 0;
}

int mfs_fstatvfs(int fildes, struct statvfs *buf) {
	uint8_t status;
	mfs_int_statfsrec stvfs;

	(void)fildes; /// ignore file descriptor
	status = mfs_int_statfs(&stvfs);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	memset(buf,0,sizeof(struct statvfs));
	mfsstatfs_to_statvfs(&stvfs,buf);
	return 0;
}

int mfs_stat(const char *path, struct stat *buf) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_int_statrec st;

	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_stat(&cr,path,&st);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	memset(buf,0,sizeof(struct stat));
	mfsstat_to_stat(&st,buf);
	return 0;
}

int mfs_fstat(int fildes, struct stat *buf) {
	uint8_t status;
	mfs_int_cred cr;
	mfs_int_statrec st;

	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_fstat(&cr,fildes,&st);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	memset(buf,0,sizeof(struct stat));
	mfsstat_to_stat(&st,buf);
	return 0;
}

int mfs_open(const char *path,int oflag,...) {
	uint8_t status;
	mfs_int_cred cr;
	va_list ap;
	int mfsoflag;
	int mode;
	int fildes;

	if (oflag&O_CREAT) {
		va_start(ap,oflag);
		mode = va_arg(ap,int);
		va_end(ap);
		mfs_get_credentials(&cr,CRED_UMASK);
	} else {
		mode = 0;
		mfs_get_credentials(&cr,CRED_BASIC);
	}
	mfsoflag = MFS_O_ACCMODE;
	switch (oflag&O_ACCMODE) {
		case O_RDONLY:
			mfsoflag = MFS_O_RDONLY;
			break;
		case O_WRONLY:
			mfsoflag = MFS_O_WRONLY;
			break;
		case O_RDWR:
			mfsoflag = MFS_O_RDWR;
			break;
	}
	if (oflag&O_CREAT) {
		mfsoflag |= MFS_O_CREAT;
	}
	if (oflag&O_TRUNC) {
		mfsoflag |= MFS_O_TRUNC;
	}
	if (oflag&O_EXCL) {
		mfsoflag |= MFS_O_EXCL;
	}
	if (oflag&O_APPEND) {
		mfsoflag |= MFS_O_APPEND;
	}
	status = mfs_int_open(&cr,&fildes,path,mfsoflag,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return fildes;
}

ssize_t mfs_pread(int fildes,void *buf,size_t nbyte,off_t offset) {
	uint8_t status;
	int64_t rsize;

	status = mfs_int_pread(fildes,&rsize,buf,nbyte,offset);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return rsize;
}

ssize_t mfs_read(int fildes,void *buf,size_t nbyte) {
	uint8_t status;
	int64_t rsize;

	status = mfs_int_read(fildes,&rsize,buf,nbyte);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return rsize;
}

ssize_t mfs_pwrite(int fildes,const void *buf,size_t nbyte,off_t offset) {
	uint8_t status;
	int64_t rsize;

	status = mfs_int_pwrite(fildes,&rsize,buf,nbyte,offset);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return rsize;
}

ssize_t mfs_write(int fildes,const void *buf,size_t nbyte) {
	uint8_t status;
	int64_t rsize;

	status = mfs_int_write(fildes,&rsize,buf,nbyte);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return rsize;
}

int mfs_fsync(int fildes) {
	uint8_t status;

	status = mfs_int_fsync(fildes);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_close(int fildes) {
	uint8_t status;

	status = mfs_int_close(fildes);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_flock(int fildes, int op) {
	uint8_t status;
	uint8_t mfsop;

	mfsop = 0;
	if (op&LOCK_SH) {
		mfsop |= MFS_LOCK_SH;
	}
	if (op&LOCK_EX) {
		mfsop |= MFS_LOCK_EX;
	}
	if (op&LOCK_NB) {
		mfsop |= MFS_LOCK_NB;
	}
	if (op&LOCK_UN) {
		mfsop |= MFS_LOCK_UN;
	}
	status = mfs_int_flock(fildes,mfsop);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_lockf(int fildes, int function, off_t size) {
	uint8_t mfsfunction;
	uint8_t status;

	switch (function) {
		case F_ULOCK:
			mfsfunction = MFS_F_ULOCK;
			break;
		case F_LOCK:
			mfsfunction = MFS_F_LOCK;
			break;
		case F_TLOCK:
			mfsfunction = MFS_F_TLOCK;
			break;
		case F_TEST:
			mfsfunction = MFS_F_TEST;
			break;
		default:
			errno = EINVAL;
			return -1;
	}
	status = mfs_int_lockf(fildes,getpid(),mfsfunction,size);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_fcntl_locks(int fildes, int function, struct flock *fl) {
	uint8_t status;
	mfs_int_flockrec mfsfl;
	uint8_t mfsfunction;

	memset(&mfsfl,0,sizeof(mfs_int_flockrec));
	if (fl->l_whence==SEEK_CUR) {
		mfsfl.whence = MFS_SEEK_CUR;
	} else if (fl->l_whence==SEEK_SET) {
		mfsfl.whence = MFS_SEEK_SET;
	} else if (fl->l_whence==SEEK_END) {
		mfsfl.whence = MFS_SEEK_END;
	} else {
		errno = EINVAL;
		return -1;
	}
	mfsfl.start = fl->l_start;
	mfsfl.len = fl->l_len;
	if (fl->l_type == F_UNLCK) {
		mfsfl.type = MFS_F_UNLCK;
	} else if (fl->l_type == F_RDLCK) {
		mfsfl.type = MFS_F_RDLCK;
	} else if (fl->l_type == F_WRLCK) {
		mfsfl.type = MFS_F_WRLCK;
	} else {
		errno = EINVAL;
		return -1;
	}
	if (function==F_GETLK) {
		mfsfunction = MFS_F_GETLK;
	} else if (function==F_SETLK) {
		mfsfunction = MFS_F_SETLK;
	} else if (function==F_SETLKW) {
		mfsfunction = MFS_F_SETLKW;
	} else {
		errno = EINVAL;
		return -1;
	}

	status = mfs_int_fcntl_locks(fildes,getpid(),mfsfunction,&mfsfl);

	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}

	if (function==F_GETLK) {
		memset(fl,0,sizeof(struct flock));
		if (mfsfl.type==MFS_F_RDLCK) {
			fl->l_type = F_RDLCK;
		} else if (mfsfl.type==MFS_F_WRLCK) {
			fl->l_type = F_WRLCK;
		} else {
			fl->l_type = F_UNLCK;
		}
		fl->l_whence = SEEK_SET;
		fl->l_start = mfsfl.start;
		fl->l_len = mfsfl.len;
		fl->l_pid = mfsfl.pid;
	}

	return 0;
}

ssize_t mfs_getxattr(const char *path, const char *name, void *value, size_t size) {
	uint8_t status;
	uint8_t mode;
	const uint8_t *vbuff;
	uint32_t vleng;
	mfs_int_cred cr;

	mode = (size==0)?MFS_XATTR_LENGTH_ONLY:MFS_XATTR_GETA_DATA;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_getxattr(&cr,path,name,&vbuff,&vleng,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	if (size>0) {
		if (vleng>size) {
			errno = ERANGE;
			return -1;
		}
		if (vleng>0) {
			memcpy(value,vbuff,vleng);
		}
	}
	return vleng;
}

ssize_t mfs_fgetxattr(int fildes, const char *name, void *value, size_t size) {
	uint8_t status;
	uint8_t mode;
	const uint8_t *vbuff;
	uint32_t vleng;
	mfs_int_cred cr;

	mode = (size==0)?MFS_XATTR_LENGTH_ONLY:MFS_XATTR_GETA_DATA;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_fgetxattr(&cr,fildes,name,&vbuff,&vleng,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	if (size>0) {
		if (vleng>size) {
			errno = ERANGE;
			return -1;
		}
		if (vleng>0) {
			memcpy(value,vbuff,vleng);
		}
	}
	return vleng;
}

int mfs_setxattr(const char *path, const char *name, const void *value, size_t size, int flags) {
	uint8_t status;
	uint8_t mode;
	mfs_int_cred cr;

	if (size>MFS_XATTR_SIZE_MAX) {
		errno = ERANGE; // E2BIG
		return -1;
	}
	mode = (flags==XATTR_CREATE)?MFS_XATTR_CREATE_ONLY:(flags==XATTR_REPLACE)?MFS_XATTR_REPLACE_ONLY:MFS_XATTR_CREATE_OR_REPLACE;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_setxattr(&cr,path,name,value,size,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_fsetxattr(int fildes, const char *name, const void *value, size_t size, int flags) {
	uint8_t status;
	uint8_t mode;
	mfs_int_cred cr;

	if (size>MFS_XATTR_SIZE_MAX) {
		errno = ERANGE; // E2BIG
		return -1;
	}
	mode = (flags==XATTR_CREATE)?MFS_XATTR_CREATE_ONLY:(flags==XATTR_REPLACE)?MFS_XATTR_REPLACE_ONLY:MFS_XATTR_CREATE_OR_REPLACE;
	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_fsetxattr(&cr,fildes,name,value,size,mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

ssize_t mfs_listxattr(const char *path, char *list, size_t size) {
	uint8_t status;
	int32_t rsize;
	mfs_int_cred cr;

	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_listxattr(&cr,path,&rsize,list,size);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return rsize;
}

ssize_t mfs_flistxattr(int fildes, char *list, size_t size) {
	uint8_t status;
	int32_t rsize;
	mfs_int_cred cr;

	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_flistxattr(&cr,fildes,&rsize,list,size);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return rsize;
}

int mfs_removexattr(const char *path, const char *name) {
	uint8_t status;
	mfs_int_cred cr;

	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_removexattr(&cr,path,name);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_fremovexattr(int fildes, const char *name) {
	uint8_t status;
	mfs_int_cred cr;

	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_fremovexattr(&cr,fildes,name);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

mfsacl* mfs_acl_alloc(uint32_t namedaclscnt) {
	if (namedaclscnt==0) {
		return malloc(sizeof(mfsacl));
	} else {
		return malloc(sizeof(mfsacl)+sizeof(mfsaclid)*(namedaclscnt-1));
	}
	return NULL;
}

void mfs_acl_free(mfsacl *aclrec) {
	free(aclrec);
}

int mfs_getfacl(const char *path, uint8_t acltype, mfsacl **aclrec) {
	uint8_t status;
	mfs_int_cred cr;
	const uint8_t *namedacls;
	uint32_t namedaclsize;
	uint32_t i,namedaclscnt;
	uint16_t userperm,groupperm,otherperm,maskperm;
	uint16_t nuserscnt,ngroupscnt;

	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_getfacl(&cr,path,acltype,&userperm,&groupperm,&otherperm,&maskperm,&nuserscnt,&ngroupscnt,&namedacls,&namedaclsize);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	namedaclscnt = (nuserscnt+ngroupscnt);
	if (namedaclscnt*6!=namedaclsize) {
		errno = EINVAL;
		return -1;
	}
	*aclrec = mfs_acl_alloc(namedaclscnt);
	if (*aclrec==NULL) {
		return -1;
	}
	(*aclrec)->userperm = userperm;
	(*aclrec)->groupperm = groupperm;
	(*aclrec)->otherperm = otherperm;
	(*aclrec)->maskperm = maskperm;
	(*aclrec)->nuserscnt = nuserscnt;
	(*aclrec)->ngroupscnt = ngroupscnt;
	for (i=0 ; i<namedaclscnt ; i++) {
		(*aclrec)->namedacls[i].id = get32bit(&namedacls);
		(*aclrec)->namedacls[i].perm = get16bit(&namedacls);
	}
	return 0;
}

int mfs_fgetfacl(int filedes, uint8_t acltype, mfsacl **aclrec) {
	uint8_t status;
	mfs_int_cred cr;
	const uint8_t *namedacls;
	uint32_t namedaclsize;
	uint32_t i,namedaclscnt;
	uint16_t userperm,groupperm,otherperm,maskperm;
	uint16_t nuserscnt,ngroupscnt;


	mfs_get_credentials(&cr,CRED_BASIC);
	status = mfs_int_fgetfacl(&cr,filedes,acltype,&userperm,&groupperm,&otherperm,&maskperm,&nuserscnt,&ngroupscnt,&namedacls,&namedaclsize);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	namedaclscnt = (nuserscnt+ngroupscnt);
	if (namedaclscnt*6!=namedaclsize) {
		errno = EINVAL;
		return -1;
	}
	*aclrec = mfs_acl_alloc(namedaclscnt);
	if (*aclrec==NULL) {
		return -1;
	}
	(*aclrec)->userperm = userperm;
	(*aclrec)->groupperm = groupperm;
	(*aclrec)->otherperm = otherperm;
	(*aclrec)->maskperm = maskperm;
	(*aclrec)->nuserscnt = nuserscnt;
	(*aclrec)->ngroupscnt = ngroupscnt;
	for (i=0 ; i<namedaclscnt ; i++) {
		(*aclrec)->namedacls[i].id = get32bit(&namedacls);
		(*aclrec)->namedacls[i].perm = get16bit(&namedacls);
	}
	return 0;
}

int mfs_setfacl(const char *path, uint8_t acltype, mfsacl *aclrec) {
	uint8_t status;
	mfs_int_cred cr;
	uint8_t *namedacls;
	uint8_t *wptr;
	uint32_t namedaclsize;
	uint32_t i,namedaclscnt;

	mfs_get_credentials(&cr,CRED_BASIC);
	namedaclscnt = (aclrec->nuserscnt+aclrec->ngroupscnt);
	namedaclsize = 6*namedaclscnt;
	namedacls = malloc(namedaclsize);
	wptr = namedacls;
	for (i=0 ; i<namedaclscnt ; i++) {
		put32bit(&wptr,aclrec->namedacls[i].id);
		put16bit(&wptr,aclrec->namedacls[i].perm);
	}
	status = mfs_int_setfacl(&cr,path,acltype,aclrec->userperm,aclrec->groupperm,aclrec->otherperm,aclrec->maskperm,aclrec->nuserscnt,aclrec->ngroupscnt,namedacls,namedaclsize);
	free(namedacls);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_fsetfacl(int filedes, uint8_t acltype, mfsacl *aclrec) {
	uint8_t status;
	mfs_int_cred cr;
	uint8_t *namedacls;
	uint8_t *wptr;
	uint32_t namedaclsize;
	uint32_t i,namedaclscnt;

	mfs_get_credentials(&cr,CRED_BASIC);
	namedaclscnt = (aclrec->nuserscnt+aclrec->ngroupscnt);
	namedaclsize = 6*namedaclscnt;
	namedacls = malloc(namedaclsize);
	wptr = namedacls;
	for (i=0 ; i<namedaclscnt ; i++) {
		put32bit(&wptr,aclrec->namedacls[i].id);
		put16bit(&wptr,aclrec->namedacls[i].perm);
	}
	status = mfs_int_fsetfacl(&cr,filedes,acltype,aclrec->userperm,aclrec->groupperm,aclrec->otherperm,aclrec->maskperm,aclrec->nuserscnt,aclrec->ngroupscnt,namedacls,namedaclsize);
	free(namedacls);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

void mfs_set_defaults(mfscfg *mcfg) {
	memset(mcfg,0,sizeof(mfscfg));
	mcfg->masterhost = strdup(DEFAULT_MASTERNAME);
	mcfg->masterport = strdup(DEFAULT_MASTER_CLIENT_PORT);
	mcfg->masterpath = strdup("/");
	mcfg->masterbind = NULL;
	mcfg->masterpassword = NULL;
	mcfg->mastermd5pass = NULL;
	mcfg->mountpoint = strdup("[MFSIO]");
	mcfg->preferedlabels = NULL;
	mcfg->read_cache_mb = 128;
	mcfg->write_cache_mb = 128;
	mcfg->io_try_cnt = 30;
	mcfg->io_timeout = 0;
	mcfg->min_log_entry = 5;
	mcfg->readahead_leng = 0x200000;
	mcfg->readahead_trigger = 10*0x200000;
	mcfg->lcache_retention = 1.0;
	mcfg->logident = strdup("libmfsio");
	mcfg->logdaemon = 0;
	mcfg->logminlevel = MFSLOG_INFO;
	mcfg->logelevateto = MFSLOG_NOTICE;
	mcfg->master_min_version_maj = 0;
	mcfg->master_min_version_mid = 0;
}

int mfs_init(mfscfg *mcfg,uint8_t stage) {
	mfs_int_cfg mcfgi;

	mcfgi.masterhost = mcfg->masterhost;
	mcfgi.masterport = mcfg->masterport;
	mcfgi.masterbind = mcfg->masterbind;
	mcfgi.masterpassword = mcfg->masterpassword;
	mcfgi.mastermd5pass = mcfg->mastermd5pass;
	mcfgi.mountpoint = mcfg->mountpoint;
	mcfgi.masterpath = mcfg->masterpath;
	mcfgi.preferedlabels = mcfg->preferedlabels;
	mcfgi.read_cache_mb = mcfg->read_cache_mb;
	mcfgi.write_cache_mb = mcfg->write_cache_mb;
	mcfgi.io_try_cnt = mcfg->io_try_cnt;
	mcfgi.io_timeout = mcfg->io_timeout;
	mcfgi.min_log_entry = mcfg->min_log_entry;
	mcfgi.readahead_leng = mcfg->readahead_leng;
	mcfgi.readahead_trigger = mcfg->readahead_trigger;
	mcfgi.error_on_lost_chunk = mcfg->error_on_lost_chunk;
	mcfgi.error_on_no_space = mcfg->error_on_no_space;
	mcfgi.sugid_clear_mode = mcfg->sugid_clear_mode;
	mcfgi.mkdir_copy_sgid = mcfg->mkdir_copy_sgid;
	mcfgi.lcache_retention = mcfg->lcache_retention;
	mcfgi.logident = mcfg->logident;
	mcfgi.logdaemon = mcfg->logdaemon;
	mcfgi.logminlevel = mcfg->logminlevel;
	mcfgi.logelevateto = mcfg->logelevateto;
	mcfgi.master_min_version_maj = mcfg->master_min_version_maj;
	mcfgi.master_min_version_mid = mcfg->master_min_version_mid;

	return mfs_int_init(&mcfgi,stage);
}

void mfs_term(void) {
	mfs_int_term();
}
