/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
#ifdef HAVE_SYS_FILE_H
#include <sys/file.h>
#endif

#include "MFSCommunication.h"
#include "mastercomm.h"
#include "inoleng.h"
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
#include "datapack.h"
#include "cfg.h"
#include "mfsstrerr.h"
#include "massert.h"
#include "md5.h"

#include "mfsio.h"

// #define DEBUG

#define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

#define PATH_TO_INODES_EXPECT_NOENTRY 0
#define PATH_TO_INODES_EXPECT_OBJECT 1
#define PATH_TO_INODES_SKIP_LAST 2
#define PATH_TO_INODES_CHECK_LAST 3

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
	return ret;
}

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

#if 0
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

static inline uint8_t mfs_attr_get_type(const uint8_t attr[ATTR_RECORD_SIZE]) {
	if (attr[0]<64) { // 1.7.29 and up
		return (attr[1]>>4);
	} else {
		return fsnodes_type_convert(attr[0]&0x7F);
	}
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
		attrtype = mfs_type_convert(attrtype&0x7F);
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
	stbuf->st_birthtime = attrctime;        // for future use
#endif
	stbuf->st_nlink = attrnlink;
}

typedef struct _cred {
	uint32_t uid;
	uint32_t gidcnt;
	uint32_t gidtab[NGROUPS_MAX+1];
} cred;

static void mfs_get_credentials(cred *ctx) {
	gid_t gids[NGROUPS_MAX];
	gid_t gid;
	uint32_t i,j;

	ctx->uid = geteuid();
	ctx->gidcnt = getgroups(NGROUPS_MAX,gids);
	gid = getegid();
	ctx->gidtab[0] = gid;
	for (i=0,j=1 ; i<ctx->gidcnt ; i++) {
		if (gids[i]!=gid) {
			ctx->gidtab[j++] = gids[i];
		}
	}
	ctx->gidcnt = j;
}

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

//
// TODO sugid_clear_mode
// TODO mkdir_copy_sgid

static int sugid_clear_mode = 0;
static int mkdir_copy_sgid = 0;

static mode_t last_umask = 0;

int mfs_mknod(const char *path, mode_t mode, dev_t dev) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	uint8_t type;
	cred cr;
	if (mfs_path_to_inodes(path,&parent,NULL,name,&nleng,PATH_TO_INODES_SKIP_LAST,attr)<0) {
		return -1;
	}
	mfs_get_credentials(&cr);
	last_umask = umask(last_umask); // This is potentail race-condition, but there is no portable way to obtain umask atomically. Last umask is remembered to minimize probability of changing umask here.
	umask(last_umask);
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
	status = fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,last_umask,cr.uid,cr.gidcnt,cr.gidtab,dev,&inode,attr);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_unlink(const char *path) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	cred cr;
	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr)<0) {
		return -1;
	}
	mfs_get_credentials(&cr);
	status = fs_unlink(parent,nleng,(const uint8_t*)name,cr.uid,cr.gidcnt,cr.gidtab,&inode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_mkdir(const char *path, mode_t mode) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	cred cr;
	if (mfs_path_to_inodes(path,&parent,NULL,name,&nleng,PATH_TO_INODES_SKIP_LAST,attr)<0) {
		return -1;
	}
	mfs_get_credentials(&cr);
	last_umask = umask(last_umask); // This is potentail race-condition, but there is no portable way to obtain umask atomically. Last umask is remembered to minimize probability of changing umask here.
	umask(last_umask);
	status = fs_mkdir(parent,nleng,(const uint8_t*)name,mode,last_umask,cr.uid,cr.gidcnt,cr.gidtab,mkdir_copy_sgid,&inode,attr);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_rmdir(const char *path) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	cred cr;
	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr)<0) {
		return -1;
	}
	mfs_get_credentials(&cr);
	status = fs_rmdir(parent,nleng,(const uint8_t*)name,cr.uid,cr.gidcnt,cr.gidtab,&inode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_rename(const char *src, const char *dst) {
	uint32_t src_parent;
	uint8_t src_name[256];
	uint8_t src_nleng;
	uint32_t dst_parent;
	uint8_t dst_name[256];
	uint8_t dst_nleng;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	cred cr;
	if (mfs_path_to_inodes(src,&src_parent,NULL,src_name,&src_nleng,PATH_TO_INODES_SKIP_LAST,attr)<0) {
		return -1;
	}
	if (mfs_path_to_inodes(dst,&dst_parent,NULL,dst_name,&dst_nleng,PATH_TO_INODES_SKIP_LAST,attr)<0) {
		return -1;
	}
	mfs_get_credentials(&cr);
	status = fs_rename(src_parent,src_nleng,(const uint8_t*)src_name,dst_parent,dst_nleng,(const uint8_t*)dst_name,cr.uid,cr.gidcnt,cr.gidtab,&inode,attr);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

static int mfs_setattr_int(uint32_t inode,uint8_t opened,uint8_t setmask,mode_t mode,uid_t uid,gid_t gid,time_t atime,time_t mtime) {
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	cred cr;

	mfs_get_credentials(&cr);
	status = fs_setattr(inode,opened,cr.uid,cr.gidcnt,cr.gidtab,setmask,mode&07777,uid,gid,atime,mtime,0,sugid_clear_mode,attr);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_chmod(const char *path, mode_t mode) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr)<0) {
		return -1;
	}
	return mfs_setattr_int(inode,0,SET_MODE_FLAG,mode,0,0,0,0);
}

int mfs_fchmod(int fildes, mode_t mode) {
	file_info *fileinfo;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	return mfs_setattr_int(fileinfo->inode,1,SET_MODE_FLAG,mode,0,0,0,0);
}

int mfs_chown(const char *path, uid_t owner, gid_t group) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t setmask;
	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr)<0) {
		return -1;
	}
	setmask = 0;
	if (owner!=(uid_t)-1) {
		setmask |= SET_UID_FLAG;
	}
	if (group!=(gid_t)-1) {
		setmask |= SET_GID_FLAG;
	}
	return mfs_setattr_int(inode,0,setmask,0,owner,group,0,0);
}

int mfs_fchown(int fildes, uid_t owner, gid_t group) {
	file_info *fileinfo;
	uint8_t setmask;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	setmask = 0;
	if (owner!=(uid_t)-1) {
		setmask |= SET_UID_FLAG;
	}
	if (group!=(gid_t)-1) {
		setmask |= SET_GID_FLAG;
	}
	return mfs_setattr_int(fileinfo->inode,1,setmask,0,owner,group,0,0);
}

int mfs_utimes(const char *path, const struct timeval times[2]) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr)<0) {
		return -1;
	}
	if (times==NULL) {
		return mfs_setattr_int(inode,0,SET_ATIME_NOW_FLAG|SET_MTIME_NOW_FLAG,0,0,0,0,0);
	} else {
		return mfs_setattr_int(inode,0,SET_ATIME_FLAG|SET_MTIME_FLAG,0,0,0,times[0].tv_sec,times[1].tv_sec);
	}
}

int mfs_futimes(int fildes, const struct timeval times[2]) {
	file_info *fileinfo;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	if (times==NULL) {
		return mfs_setattr_int(fileinfo->inode,1,SET_ATIME_NOW_FLAG|SET_MTIME_NOW_FLAG,0,0,0,0,0);
	} else {
		return mfs_setattr_int(fileinfo->inode,1,SET_ATIME_FLAG|SET_MTIME_FLAG,0,0,0,times[0].tv_sec,times[1].tv_sec);
	}
}

int mfs_futimens(int fildes, const struct timespec times[2]) {
	file_info *fileinfo;
	uint8_t setmask;
	uint32_t atime,mtime;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	atime = 0;
	mtime = 0;
	if (times==NULL) {
		setmask = SET_ATIME_NOW_FLAG|SET_MTIME_NOW_FLAG;
	} else {
		setmask = 0;
		if (times[0].tv_nsec == UTIME_NOW) {
			setmask |= SET_ATIME_NOW_FLAG;
		} else if (times[0].tv_nsec != UTIME_OMIT) {
			setmask |= SET_ATIME_FLAG;
			atime = times[0].tv_sec;
		}
		if (times[1].tv_nsec == UTIME_NOW) {
			setmask |= SET_MTIME_NOW_FLAG;
		} else if (times[1].tv_nsec != UTIME_OMIT) {
			setmask |= SET_MTIME_FLAG;
			mtime = times[1].tv_sec;
		}
	}
	return mfs_setattr_int(fileinfo->inode,1,setmask,0,0,0,atime,mtime);
}

static int mfs_truncate_int(uint32_t inode,uint8_t opened,off_t size,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t status;
	cred cr;

	if (size<0) {
		errno = EINVAL;
		return -1;
	}
	if (size>=MAX_FILE_SIZE) {
		errno = EFBIG;
		return -1;
	}
	write_data_flush_inode(inode);
	mfs_get_credentials(&cr);
	status = do_truncate(inode,(opened)?TRUNCATE_FLAG_OPENED:0,cr.uid,cr.gidcnt,cr.gidtab,size,attr,NULL);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	chunksdatacache_clear_inode(inode,size/MFSCHUNKSIZE);
	finfo_change_fleng(inode,size);
	write_data_inode_setmaxfleng(inode,size);
	read_inode_set_length_active(inode,size);
	return 0;
}

int mfs_truncate(const char *path, off_t size) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr)<0) {
		return -1;
	}
	return mfs_truncate_int(inode,0,size,attr);
}

int mfs_ftruncate(int fildes, off_t size) {
	file_info *fileinfo;
	uint8_t attr[ATTR_RECORD_SIZE];

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	return mfs_truncate_int(fileinfo->inode,1,size,attr);
}

off_t mfs_lseek(int fildes, off_t offset, int whence) {
	file_info *fileinfo;
	off_t ret;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	switch (whence) {
		case SEEK_SET:
			fileinfo->offset = offset;
			break;
		case SEEK_CUR:
			fileinfo->offset += offset;
			break;
		case SEEK_END:
			fileinfo->offset = inoleng_getfleng(fileinfo->flengptr) + offset;
			break;
		default:
			errno = EINVAL;
			return -1;
	}
	if (fileinfo->offset<0) {
		fileinfo->offset = 0;
	}
	ret = fileinfo->offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return ret;
}

static void mfs_fix_attr(uint8_t type,uint32_t inode,struct stat *buf) {
	if (type==TYPE_FILE) {
		uint64_t maxfleng = write_data_inode_getmaxfleng(inode);
		if (maxfleng>(uint64_t)(buf->st_size)) {
			buf->st_size = maxfleng;
		}
		read_inode_set_length_passive(inode,buf->st_size);
		finfo_change_fleng(inode,buf->st_size);
	}
	fs_fix_amtime(inode,&(buf->st_atime),&(buf->st_mtime));
}

int mfs_stat(const char *path, struct stat *buf) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t type;

	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr)<0) {
		return -1;
	}
	memset(buf,0,sizeof(struct stat));
	mfs_attr_to_stat(inode,attr,buf);
	type = mfs_attr_get_type(attr);
	mfs_fix_attr(type,inode,buf);
	return 0;
}

int mfs_fstat(int fildes, struct stat *buf) {
	uint8_t attr[ATTR_RECORD_SIZE];
	file_info *fileinfo;
	uint8_t status;
	uint8_t type;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	status = fs_getattr(fileinfo->inode,1,geteuid(),getegid(),attr);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	memset(buf,0,sizeof(struct stat));
	mfs_attr_to_stat(fileinfo->inode,attr,buf);
	type = mfs_attr_get_type(attr);
	mfs_fix_attr(type,fileinfo->inode,buf);
	return 0;
}

int mfs_open(const char *path,int oflag,...) {
	uint64_t fsize;
	uint32_t parent;
	uint32_t inode;
	uint8_t noatomictrunc;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	cred cr;
	uint8_t mfsoflag;
	uint8_t oflags;
	int fildes;
	int needopen;
	file_info *fileinfo;

	mfsoflag = 0;
	switch (oflag&O_ACCMODE) {
		case O_RDONLY:
			mfsoflag |= OPEN_READ;
			break;
		case O_WRONLY:
			mfsoflag |= OPEN_WRITE;
			break;
		case O_RDWR:
			mfsoflag |= OPEN_READ | OPEN_WRITE;
			break;
	}
	if (oflag&O_TRUNC) {
		uint32_t mver;
		mver = master_version();
		noatomictrunc = (mver<VERSION2INT(3,0,113))?1:0;
		mfsoflag |= OPEN_TRUNCATE;
	} else {
		noatomictrunc = 0;
	}

	oflags = 0;
	needopen = 1;
	if (mfs_path_to_inodes(path,&parent,&inode,name,&nleng,PATH_TO_INODES_CHECK_LAST,attr)<0) {
		return -1;
	}
	if (oflag&O_CREAT) {
		if (oflag&O_EXCL) {
			if (inode!=0) { // file exists
				errno = EEXIST;
				return -1;
			}
		} else {
			if (inode==0) { // file doesn't exists - create it
				int mode;
				va_list ap;
				// create
				va_start(ap,oflag);
				mode = va_arg(ap,int);
				va_end(ap);
				mfs_get_credentials(&cr);
				last_umask = umask(last_umask); // see - mkdir
				umask(last_umask);
				status = fs_create(parent,nleng,(const uint8_t*)name,mode,last_umask,cr.uid,cr.gidcnt,cr.gidtab,&inode,attr,&oflags);
				if (status!=MFS_STATUS_OK) {
					errno = mfs_errorconv(status);
					return -1;
				}
				needopen = 0;
			}
		}
	} else {
		if (inode==0) {
			errno = ENOENT;
			return -1;
		}
	}
	if (needopen) {
		if (mfs_attr_to_type(attr)!=TYPE_FILE) {
			errno = EISDIR;
			return -1;
		}

		// open
		mfs_get_credentials(&cr);
		status = fs_opencheck(inode,cr.uid,cr.gidcnt,cr.gidtab,mfsoflag,attr,&oflags);
		if (status!=MFS_STATUS_OK) {
			errno = mfs_errorconv(status);
			return -1;
		}
		if (mfsoflag&OPEN_TRUNCATE && noatomictrunc) {
			if (mfs_truncate_int(inode,1,0,attr)<0) {
				return -1;
			}
		}
	}
	if (oflags & OPEN_APPENDONLY) {
		if ((oflag&O_APPEND)==0) {
			errno = EPERM;
			return -1;
		}
	}

	fs_inc_acnt(inode);

	fsize = mfs_attr_to_size(attr);
	fildes = mfs_next_fd();
	fileinfo = mfs_get_fi(fildes);
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	fileinfo->flengptr = inoleng_acquire(inode);
	fileinfo->inode = inode;
	fileinfo->mode = MFS_IO_FORBIDDEN;
	fileinfo->offset = 0;
	fileinfo->rdata = NULL;
	fileinfo->wdata = NULL;
	fileinfo->readers_cnt = 0;
	fileinfo->writers_cnt = 0;
	fileinfo->writing = 0;

	inoleng_setfleng(fileinfo->flengptr,fsize);
	if ((oflag&O_ACCMODE) == O_RDONLY) {
		fileinfo->mode = MFS_IO_READONLY;
		fileinfo->rdata = read_data_new(inode,fsize);
	} else if ((oflag&O_ACCMODE) == O_WRONLY) {
		if (oflag&O_APPEND) {
			fileinfo->mode = MFS_IO_APPENDONLY;
		} else {
			fileinfo->mode = MFS_IO_WRITEONLY;
		}
		fileinfo->wdata = write_data_new(inode,fsize);
	} else if ((oflag&O_ACCMODE) == O_RDWR) {
		if (oflag&O_APPEND) {
			fileinfo->mode = MFS_IO_READAPPEND;
		} else {
			fileinfo->mode = MFS_IO_READWRITE;
		}
		fileinfo->rdata = read_data_new(inode,fsize);
		fileinfo->wdata = write_data_new(inode,fsize);
	}
	if (oflag&O_APPEND) {
		fileinfo->offset = fsize;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return fildes;
}

static ssize_t mfs_pread_int(file_info *fileinfo,void *buf,size_t nbyte,off_t offset) {
	uint32_t ssize;
	struct iovec *iov;
	uint32_t iovcnt,pos,i;
	void *buffptr;
	int err;

	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	if (offset>=MAX_FILE_SIZE || offset+nbyte>=MAX_FILE_SIZE) {
		errno = EFBIG;
		return -1;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_WRITEONLY || fileinfo->mode==MFS_IO_APPENDONLY || fileinfo->mode==MFS_IO_FORBIDDEN) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		errno = EACCES;
		return -1;
	}
	// rwlock_rdlock begin
	while (fileinfo->writing | fileinfo->writers_cnt) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	fileinfo->readers_cnt++;
	// rwlock_rdlock_end
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));

	write_data_flush_inode(fileinfo->inode);

	ssize = nbyte;
	fs_atime(fileinfo->inode);
	err = read_data(fileinfo->rdata,offset,&ssize,&buffptr,&iov,&iovcnt);
	fs_atime(fileinfo->inode);

	if (err==0) {
		pos = 0;
		for (i=0 ; i<iovcnt ; i++) {
			memcpy((uint8_t*)buf+pos,iov[i].iov_base,iov[i].iov_len);
			pos += iov[i].iov_len;
		}
	}
	read_data_free_buff(fileinfo->rdata,buffptr,iov);
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	// rwlock_rdunlock begin
	fileinfo->readers_cnt--;
	if (fileinfo->readers_cnt==0) {
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
	}
	// rwlock_rdunlock_end
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	if (err!=0) {
		errno = err;
		return -1;
	}
	return ssize;
}

ssize_t mfs_pread(int fildes,void *buf,size_t nbyte,off_t offset) {
	return mfs_pread_int(mfs_get_fi(fildes),buf,nbyte,offset);
}

ssize_t mfs_read(int fildes,void *buf,size_t nbyte) {
	ssize_t s;
	file_info *fileinfo;
	off_t offset;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return -1;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	offset = fileinfo->offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	s = mfs_pread_int(fileinfo,buf,nbyte,offset);
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (s>0) {
		fileinfo->offset = offset + s;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return s;
}

static ssize_t mfs_pwrite_int(file_info *fileinfo,const void *buf,size_t nbyte,off_t offset) {
	uint64_t newfleng;
	uint8_t appendonly;
	int err;

	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	if (offset>=MAX_FILE_SIZE || offset+nbyte>=MAX_FILE_SIZE) {
		errno = EFBIG;
		return -1;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	appendonly = (fileinfo->mode==MFS_IO_APPENDONLY || fileinfo->mode==MFS_IO_READAPPEND)?1:0;
	if (fileinfo->mode==MFS_IO_READONLY || fileinfo->mode==MFS_IO_FORBIDDEN) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		errno = EACCES;
		return -1;
	}
	// rwlock_wrlock begin
	fileinfo->writers_cnt++;
	while (fileinfo->readers_cnt | fileinfo->writing) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	fileinfo->writers_cnt--;
	fileinfo->writing = 1;
	// rwlock_wrlock end

	err = 0;
	if (appendonly) {
		if (master_version()>=VERSION2INT(3,0,113)) {
			uint8_t status;
			uint64_t prevleng;
			uint32_t gid = 0;
			uint32_t inode = fileinfo->inode;
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
			status = do_truncate(inode,TRUNCATE_FLAG_OPENED|TRUNCATE_FLAG_UPDATE|TRUNCATE_FLAG_RESERVE,0,1,&gid,nbyte,NULL,&prevleng);
			zassert(pthread_mutex_lock(&(fileinfo->lock)));
			if (status!=MFS_STATUS_OK) {
				err = mfs_errorconv(status);
			} else {
				offset = prevleng;
			}
		} else {
			offset = inoleng_getfleng(fileinfo->flengptr);
			if (offset+nbyte>=MAX_FILE_SIZE) {
				err = EFBIG;
			}
		}
	}
	if (err==0) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		fs_mtime(fileinfo->inode);
		err = write_data(fileinfo->wdata,offset,nbyte,(const uint8_t*)buf,(geteuid()==0)?1:0);
		fs_mtime(fileinfo->inode);
		zassert(pthread_mutex_lock(&(fileinfo->lock)));
	}

	// rwlock_wrunlock begin
	fileinfo->writing = 0;
	zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
	// wrlock_wrunlock end

	if (err!=0) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		errno = err;
		return -1;
	}
	if ((uint64_t)(offset+nbyte)>inoleng_getfleng(fileinfo->flengptr)) {
		inoleng_setfleng(fileinfo->flengptr,offset+nbyte);
		newfleng = offset+nbyte;
	} else {
		newfleng = 0;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	if (newfleng>0) {
		read_inode_set_length_passive(fileinfo->inode,newfleng);
		write_data_inode_setmaxfleng(fileinfo->inode,newfleng);
		finfo_change_fleng(fileinfo->inode,newfleng);
	}
	read_inode_clear_cache(fileinfo->inode,offset,nbyte);
//	fdcache_invalidate(fileinfo->inode);
	return nbyte;
}

ssize_t mfs_pwrite(int fildes,const void *buf,size_t nbyte,off_t offset) {
	return mfs_pwrite_int(mfs_get_fi(fildes),buf,nbyte,offset);
}

ssize_t mfs_write(int fildes,const void *buf,size_t nbyte) {
	ssize_t s;
	file_info *fileinfo;
	off_t offset;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return -1;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	offset = fileinfo->offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	s = mfs_pwrite_int(fileinfo,buf,nbyte,fileinfo->offset);
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_APPENDONLY || fileinfo->mode==MFS_IO_READAPPEND) {
		fileinfo->offset = inoleng_getfleng(fileinfo->flengptr);
	} else {
		if (s>0) {
			fileinfo->offset = offset + s;
		}
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return s;
}

static int mfs_fsync_int(file_info *fileinfo) {
	int err;
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->wdata!=NULL && (fileinfo->mode!=MFS_IO_READONLY && fileinfo->mode!=MFS_IO_FORBIDDEN)) {
		// rwlock_wrlock begin
		fileinfo->writers_cnt++;
		while (fileinfo->readers_cnt | fileinfo->writing) {
			zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
		}
		fileinfo->writers_cnt--;
		fileinfo->writing = 1;
		// rwlock_wrlock end
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));

		err = write_data_flush(fileinfo->wdata);

		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		// rwlock_wrunlock begin
		fileinfo->writing = 0;
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
		// rwlock_wrunlock end
	} else {
		err = 0;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
//	if (err==0) {
//		fdcache_invalidate(inode);
//		dcache_invalidate_attr(inode);
//	}
	if (err!=0) {
		errno = err;
		return -1;
	}
	return 0;
}

int mfs_fsync(int fildes) {
	return mfs_fsync_int(mfs_get_fi(fildes));
}

int mfs_close(int fildes) {
	file_info *fileinfo;
	int err;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	// rwlock_wait_for_unlock:
	while (fileinfo->writing | fileinfo->writers_cnt | fileinfo->readers_cnt) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	err = mfs_fsync_int(fileinfo);
	if (fileinfo->rdata != NULL) {
		read_data_end(fileinfo->rdata);
		fileinfo->rdata = NULL;
	}
	if (fileinfo->wdata != NULL) {
		write_data_end(fileinfo->wdata);
		fileinfo->wdata = NULL;
	}
	if (fileinfo->flengptr != NULL) {
		inoleng_release(fileinfo->flengptr);
		fileinfo->flengptr = NULL;
	}
	if (fileinfo->mode != MFS_IO_FORBIDDEN) {
		fs_dec_acnt(fileinfo->inode);
		fileinfo->mode = MFS_IO_FORBIDDEN;
	}
	mfs_free_fd(fildes);
	if (err!=0) {
		errno = err;
		return -1;
	}
	return 0;
}

int mfs_flock(int fildes, int op) {
	uint8_t lock_mode;
	file_info *fileinfo;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}

	if (op&LOCK_UN) {
		lock_mode = FLOCK_UNLOCK;
	} else if (op&LOCK_SH) {
		if (op&LOCK_NB) {
			lock_mode=FLOCK_TRY_SHARED;
		} else {
			lock_mode=FLOCK_LOCK_SHARED;
		}
	} else if (op&LOCK_EX) {
		if (op&LOCK_NB) {
			lock_mode=FLOCK_TRY_EXCLUSIVE;
		} else {
			lock_mode=FLOCK_LOCK_EXCLUSIVE;
		}
	} else {
		errno = EINVAL;
		return -1;
	}

	if (lock_mode==FLOCK_UNLOCK) {
		mfs_fsync_int(fileinfo);
	}

	status = fs_flock(fileinfo->inode,0,fildes,lock_mode);
	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_lockf(int fildes, int function, off_t size) {
	uint64_t start,end;
	uint32_t pid;
	file_info *fileinfo;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}

	if (size>0) {
		start = fileinfo->offset;
		end = start+size;
		if (end<start) {
			errno = EINVAL;
			return -1;
		}
	} else if (size<0) {
		end = fileinfo->offset;
		start = end+size;
		if (end<start) {
			errno = EINVAL;
			return -1;
		}
	} else { //size = 0;
		start = fileinfo->offset;
		end = UINT64_MAX;
	}

	pid = getpid();
	if (function==F_ULOCK) {
		mfs_fsync_int(fileinfo);
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_SET,POSIX_LOCK_UNLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==F_LOCK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_SET,POSIX_LOCK_WRLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==F_TLOCK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_TRY,POSIX_LOCK_WRLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==F_TEST) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_GET,POSIX_LOCK_WRLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else {
		errno = EINVAL;
		return -1;
	}

	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}
	return 0;
}

int mfs_fcntl_locks(int fildes, int function, struct flock *fl) {
	uint64_t start,end,rstart,rend;
	uint32_t pid,rpid;
	uint8_t type,rtype;
	file_info *fileinfo;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		errno = EBADF;
		return -1;
	}

	if (fl->l_whence==SEEK_CUR) {
		if (fl->l_start > fileinfo->offset) {
			start = 0;
		} else {
			start = fileinfo->offset + fl->l_start;
		}
	} else if (fl->l_whence==SEEK_SET) {
		if (fl->l_start < 0) {
			start = 0;
		} else {
			start = fl->l_start;
		}
	} else if (fl->l_whence==SEEK_END) {
		if (fl->l_start > (off_t)inoleng_getfleng(fileinfo->flengptr)) {
			start = 0;
		} else {
			start = inoleng_getfleng(fileinfo->flengptr) + fl->l_start;
		}
	} else {
		errno = EINVAL;
		return -1;
	}
	if (fl->l_len <= 0) {
		end = UINT64_MAX;
	} else {
		end = start + fl->l_len;
		if (end<start) {
			end = UINT64_MAX;
		}
	}
	if (fl->l_type == F_UNLCK) {
		type = POSIX_LOCK_UNLCK;
	} else if (fl->l_type == F_RDLCK) {
		type = POSIX_LOCK_RDLCK;
	} else if (fl->l_type == F_WRLCK) {
		type = POSIX_LOCK_WRLCK;
	} else {
		errno = EINVAL;
		return -1;
	}
	pid = getpid();

	if (type==POSIX_LOCK_UNLCK) {
		mfs_fsync_int(fileinfo);
	}


	if (function==F_GETLK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_GET,type,start,end,pid,&rtype,&rstart,&rend,&rpid);
	} else if (function==F_SETLKW) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_SET,type,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==F_SETLK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_TRY,type,start,end,pid,NULL,NULL,NULL,NULL);
	} else {
		errno = EINVAL;
		return -1;
	}

	if (status!=MFS_STATUS_OK) {
		errno = mfs_errorconv(status);
		return -1;
	}

	if (function==F_GETLK) {
		memset(fl,0,sizeof(struct flock));
		if (rtype==POSIX_LOCK_RDLCK) {
			fl->l_type = F_RDLCK;
		} else if (rtype==POSIX_LOCK_WRLCK) {
			fl->l_type = F_WRLCK;
		} else {
			fl->l_type = F_UNLCK;
		}
		fl->l_whence = SEEK_SET;
		fl->l_start = rstart;
		if ((rend-rstart)>INT64_MAX) {
			fl->l_len = 0;
		} else {
			fl->l_len = (rend - rstart);
		}
		fl->l_pid = rpid;
	}

	return 0;
}


int mfs_init(mfscfg *mcfg,uint8_t stage) {
	uint32_t i;
	md5ctx ctx;
	uint8_t md5pass[16];

	if (stage==0 || stage==1) {
		if (mcfg->masterpassword!=NULL) {
			md5_init(&ctx);
			md5_update(&ctx,(uint8_t*)(mcfg->masterpassword),strlen(mcfg->masterpassword));
			md5_final(md5pass,&ctx);
			memset(mcfg->masterpassword,0,strlen(mcfg->masterpassword));
		}
		strerr_init();
		mycrc32_init();
		if (fs_init_master_connection(NULL,mcfg->masterhost,mcfg->masterport,0,mcfg->mountpoint,mcfg->masterpath,(mcfg->masterpassword!=NULL)?md5pass:NULL,1,0)<0) {
			return -1;
		}
		memset(md5pass,0,16);
	}

	if (stage==0 || stage==2) {
		inoleng_init();
		conncache_init(200);
		chunkrwlock_init();
		chunksdatacache_init();
		fs_init_threads(mcfg->io_try_cnt,0);

		csdb_init();
		delay_init();
		read_data_init(mcfg->read_cache_mb*1024*1024,0x200000,10*0x200000,mcfg->io_try_cnt,0,5,mcfg->error_on_lost_chunk,mcfg->error_on_no_space);
		write_data_init(mcfg->write_cache_mb*1024*1024,mcfg->io_try_cnt,0,5,mcfg->error_on_lost_chunk,mcfg->error_on_no_space);

		zassert(pthread_mutex_init(&fdtablock,NULL));
		fdtab = malloc(sizeof(file_info)*FDTABSIZE_INIT);
		fdtabsize = FDTABSIZE_INIT;
		fdtabusemask = malloc(sizeof(uint32_t)*((FDTABSIZE_INIT+31)/32));
		passert(fdtab);
		passert(fdtabusemask);
		for (i=0 ; i<fdtabsize ; i++) {
			mfs_fi_init(fdtab+i);
		}
		memset(fdtabusemask,0,sizeof(uint32_t)*((FDTABSIZE_INIT+31)/32));

		last_umask = umask(0);
		umask(last_umask);

		if (mcfg->mkdir_copy_sgid<0) {
#ifdef __linux__
			mkdir_copy_sgid = 1;
#else
			mkdir_copy_sgid = 0;
#endif
		} else {
			mkdir_copy_sgid = mcfg->mkdir_copy_sgid;
		}

		if (mcfg->sugid_clear_mode<0) {
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
			sugid_clear_mode= SUGID_CLEAR_MODE_EXT;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
			sugid_clear_mode = SUGID_CLEAR_MODE_BSD;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
			sugid_clear_mode = SUGID_CLEAR_MODE_OSX;
#else
			sugid_clear_mode = SUGID_CLEAR_MODE_NEVER;
#endif
		} else {
			sugid_clear_mode = mcfg->sugid_clear_mode;
		}
	}

	return 0;
}

void mfs_term(void) {
	uint32_t i;
	for (i=0 ; i<fdtabsize ; i++) {
		mfs_close(i);
		mfs_fi_term(fdtab+i);
	}
	free(fdtabusemask);
	free(fdtab);
	zassert(pthread_mutex_lock(&fdtablock));
	zassert(pthread_mutex_unlock(&fdtablock));
	zassert(pthread_mutex_destroy(&fdtablock));
	write_data_term();
	read_data_term();
	delay_term();
	csdb_term();

	fs_term();
	chunksdatacache_term();
	chunkrwlock_term();
	conncache_term();
	inoleng_term();
	stats_term();
}
