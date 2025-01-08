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

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stddef.h>
#include <string.h>
#ifndef WIN32
#include <unistd.h>
#endif
#include <pthread.h>

#include "MFSCommunication.h"
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
#include "datapack.h"
#include "cfg.h"
#include "mfsstrerr.h"
#include "massert.h"
#include "md5.h"
#include "mfslog.h"

#include "mfsioint_lookupcache.h"
#include "mfsioint.h"

// #define DEBUG

#define USE_PATH_LOOKUP 1

#define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

#define PATH_TO_INODES_EXPECT_NOENTRY 0
#define PATH_TO_INODES_EXPECT_OBJECT 1
#define PATH_TO_INODES_SKIP_LAST 2
#define PATH_TO_INODES_CHECK_LAST 3

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

static void mfs_attr_to_mfsstat(uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE], mfs_int_statrec *stbuf) {
	const uint8_t *ptr;
	ptr = attr;
	stbuf->inode = inode;
	if (attr[0]<64) { // 1.7.29 and up
		ptr++;
		stbuf->mode = get16bit(&ptr);
		stbuf->type = (stbuf->mode>>12);
	} else {
		stbuf->type = get8bit(&ptr);
		stbuf->type = mfs_type_convert((stbuf->type)&0x7F);
		stbuf->mode = get16bit(&ptr);
	}
	stbuf->mode &= 0x0FFF;
	stbuf->uid = get32bit(&ptr);
	stbuf->gid = get32bit(&ptr);
	stbuf->atime = get32bit(&ptr);
	stbuf->mtime = get32bit(&ptr);
	stbuf->ctime = get32bit(&ptr);
	stbuf->nlink = get32bit(&ptr);
	if (stbuf->type==TYPE_DIRECTORY || stbuf->type==TYPE_SYMLINK || stbuf->type==TYPE_FILE) {
		stbuf->length = get64bit(&ptr);
	} else if (stbuf->type==TYPE_BLOCKDEV || stbuf->type==TYPE_CHARDEV) {
		stbuf->dev = get32bit(&ptr);
		ptr+=4;
	} else {
		ptr+=8;
	}
	stbuf->winattr = get8bit(&ptr);
}

static void mfs_attr_to_direntry(uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE], mfs_int_direntryplus *stbuf) {
	const uint8_t *ptr;
	ptr = attr;
	stbuf->inode = inode;
	if (attr[0]<64) { // 1.7.29 and up
		ptr++;
		stbuf->mode = get16bit(&ptr);
		stbuf->type = (stbuf->mode>>12);
	} else {
		stbuf->type = get8bit(&ptr);
		stbuf->type = mfs_type_convert((stbuf->type)&0x7F);
		stbuf->mode = get16bit(&ptr);
	}
	stbuf->mode &= 0x0FFF;
	stbuf->uid = get32bit(&ptr);
	stbuf->gid = get32bit(&ptr);
	stbuf->atime = get32bit(&ptr);
	stbuf->mtime = get32bit(&ptr);
	stbuf->ctime = get32bit(&ptr);
	stbuf->nlink = get32bit(&ptr);
	if (stbuf->type==TYPE_DIRECTORY || stbuf->type==TYPE_SYMLINK || stbuf->type==TYPE_FILE) {
		stbuf->length = get64bit(&ptr);
	} else if (stbuf->type==TYPE_BLOCKDEV || stbuf->type==TYPE_CHARDEV) {
		stbuf->dev = get32bit(&ptr);
		ptr+=4;
	} else {
		ptr+=8;
	}
	stbuf->winattr = get8bit(&ptr);
}

#ifdef USE_PATH_LOOKUP
static uint8_t mfs_path_to_inodes(mfs_int_cred *cr,const char *path,uint32_t *parent,uint32_t *inode,uint8_t name[256],uint8_t *nleng,uint8_t existflag,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint32_t parent_inode,last_inode;
	uint8_t status;

	if (inode!=NULL) {
		*inode = 0;
	}
	memset(attr,0,ATTR_RECORD_SIZE);
	status = lcache_path_lookup(MFS_ROOT_ID,strlen(path),(const uint8_t *)path,cr->uid,cr->gidcnt,cr->gidtab,&parent_inode,&last_inode,nleng,name,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (parent!=NULL) {
		*parent = parent_inode;
	}
	if (inode!=NULL) {
		*inode = last_inode;
	}
	if (existflag==PATH_TO_INODES_EXPECT_NOENTRY && last_inode!=0) {
		return MFS_ERROR_EEXIST;
	}
	if (existflag==PATH_TO_INODES_EXPECT_OBJECT && last_inode==0) {
		return MFS_ERROR_ENOENT;
	}
	return MFS_STATUS_OK;
}

static void mfs_path_removed(const char *path) {
	lcache_path_invalidate(MFS_ROOT_ID,strlen(path),(const uint8_t *)path);
}

static void mfs_path_created(const char *path) {
	lcache_path_invalidate(MFS_ROOT_ID,strlen(path),(const uint8_t *)path);
}

static void mfs_inode_invalidate(uint32_t inode) {
	lcache_inode_invalidate(inode);
}
#else
static uint8_t mfs_path_to_inodes(mfs_int_cred *cr,const char *path,uint32_t *parent,uint32_t *inode,uint8_t name[256],uint8_t *nleng,uint8_t existflag,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint32_t cinode = MFS_ROOT_ID;
	uint32_t pinode = MFS_ROOT_ID;
	const char *pptr = path;
	uint8_t partlen,status;

	if (inode!=NULL) {
		*inode = 0;
	}
	memset(attr,0,ATTR_RECORD_SIZE);
	if (path[0]==0) {
		return MFS_ERROR_EINVAL;
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
			return MFS_STATUS_OK;
		}
		name[0] = '.';
		status = fs_simple_lookup(pinode,1,name,cr->uid,cr->gidcnt,cr->gidtab,&cinode,attr);
		name[0] = 0;
		if (status!=MFS_STATUS_OK) {
			return status;
		}
		if (parent!=NULL) {
			*parent = pinode;
		}
		if (inode!=NULL) {
			*inode = cinode;
		}
		*nleng = 0;
		return MFS_STATUS_OK;
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
				status = fs_simple_lookup(pinode,partlen,name,cr->uid,cr->gidcnt,cr->gidtab,&cinode,attr);
				if (status!=MFS_STATUS_OK) {
					return status;
				}
				if (mfs_attr_get_type(attr)!=TYPE_DIRECTORY) {
					return MFS_ERROR_ENOTDIR;
				}
#ifdef DEBUG
				printf("result inode: %u\n",cinode);
#endif
			}
			partlen = 0;
		} else {
			if (partlen==255) {
				return MFS_ERROR_ENAMETOOLONG;
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
		status = fs_simple_lookup(pinode,partlen,name,cr->uid,cr->gidcnt,cr->gidtab,&cinode,attr);
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
				return MFS_ERROR_EEXIST;
			} else if (status!=MFS_ERROR_ENOENT) {
				return status;
			}
		} else if (existflag==PATH_TO_INODES_EXPECT_OBJECT) {
			if (status!=MFS_STATUS_OK) {
				return status;
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
	return MFS_STATUS_OK;
}

static void mfs_path_removed(const char *path) {
	(void)path;
}

static void mfs_path_created(const char *path) {
	(void)path;
}

static void mfs_inode_invalidate(uint32_t inode) {
	(void)inode;
}
#endif

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

enum {MFS_IO_READWRITE,MFS_IO_READONLY,MFS_IO_WRITEONLY,MFS_IO_ATTRONLY,MFS_IO_READAPPEND,MFS_IO_APPENDONLY,MFS_IO_FORBIDDEN,MFS_IO_DIRECTORY};

typedef struct file_info {
	void *flengptr;              // F
	uint32_t inode;              // F,D
	uint8_t mode;                // F,D
	uint8_t writing;             // F
	uint8_t reading;             // D
	uint8_t privuser;            // F
	uint8_t wasread;             // D
	uint8_t dataformat;          // D
	uint64_t offset;             // F,D
	uint32_t readers_cnt;        // F
	uint32_t writers_cnt;        // F
	void *rdata,*wdata;          // F
	uint8_t *dbuff;              // D
	uint64_t dbuffsize;          // D
	pthread_mutex_t lock;        // F,D
	pthread_cond_t rwcond;       // F
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

uint8_t mfs_int_mknod(mfs_int_cred *cr, const char *path, uint8_t type, uint16_t mode, uint32_t dev) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,NULL,name,&nleng,PATH_TO_INODES_SKIP_LAST,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (parent==0) {
		return MFS_ERROR_EINVAL;
	}
	status = fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,cr->umask,cr->uid,cr->gidcnt,cr->gidtab,dev,&inode,attr);
	if (status==MFS_STATUS_OK) {
		mfs_path_created(path);
	}
	return status;
}

uint8_t mfs_int_unlink(mfs_int_cred *cr, const char *path) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (parent==0) {
		return MFS_ERROR_EINVAL;
	}
	status = fs_unlink(parent,nleng,(const uint8_t*)name,cr->uid,cr->gidcnt,cr->gidtab,&inode);
	if (status==MFS_STATUS_OK) {
		mfs_path_removed(path);
	}
	return status;
}

uint8_t mfs_int_mkdir(mfs_int_cred *cr, const char *path, uint16_t mode) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,NULL,name,&nleng,PATH_TO_INODES_SKIP_LAST,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (parent==0) {
		return MFS_ERROR_EINVAL;
	}
	status = fs_mkdir(parent,nleng,(const uint8_t*)name,mode,cr->umask,cr->uid,cr->gidcnt,cr->gidtab,mkdir_copy_sgid,&inode,attr);
	if (status==MFS_STATUS_OK) {
		mfs_path_created(path);
	}
	return status;
}

uint8_t mfs_int_rmdir(mfs_int_cred *cr, const char *path) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (parent==0) {
		return MFS_ERROR_EINVAL;
	}
	status = fs_rmdir(parent,nleng,(const uint8_t*)name,cr->uid,cr->gidcnt,cr->gidtab,&inode);
	if (status==MFS_STATUS_OK) {
		mfs_path_removed(path);
	}
	return status;
}

uint8_t mfs_int_rename(mfs_int_cred *cr, const char *src, const char *dst) {
	uint32_t src_parent;
	uint8_t src_name[256];
	uint8_t src_nleng;
	uint32_t dst_parent;
	uint8_t dst_name[256];
	uint8_t dst_nleng;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,src,&src_parent,NULL,src_name,&src_nleng,PATH_TO_INODES_SKIP_LAST,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (src_parent==0) {
		return MFS_ERROR_EINVAL;
	}
	status = mfs_path_to_inodes(cr,dst,&dst_parent,NULL,dst_name,&dst_nleng,PATH_TO_INODES_SKIP_LAST,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (dst_parent==0) {
		return MFS_ERROR_EINVAL;
	}

	status = fs_rename(src_parent,src_nleng,(const uint8_t*)src_name,dst_parent,dst_nleng,(const uint8_t*)dst_name,cr->uid,cr->gidcnt,cr->gidtab,0,&inode,attr);
	if (status==MFS_STATUS_OK) {
		mfs_path_removed(src);
		mfs_path_created(dst);
	}
	return status;
}

uint8_t mfs_int_link(mfs_int_cred *cr, const char *src, const char *dst) {
	uint32_t src_inode;
	uint32_t src_parent;
	uint8_t src_name[256];
	uint8_t src_nleng;
	uint32_t dst_parent;
	uint8_t dst_name[256];
	uint8_t dst_nleng;
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,src,&src_parent,&src_inode,src_name,&src_nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	status = mfs_path_to_inodes(cr,dst,&dst_parent,NULL,dst_name,&dst_nleng,PATH_TO_INODES_SKIP_LAST,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (dst_parent==0) {
		return MFS_ERROR_EINVAL;
	}

	status = fs_link(src_inode,dst_parent,dst_nleng,(const uint8_t*)dst_name,cr->uid,cr->gidcnt,cr->gidtab,&inode,attr);
	if (status==MFS_STATUS_OK) {
		mfs_path_created(dst);
	}
	return status;
}

uint8_t mfs_int_symlink(mfs_int_cred *cr, const char *nodepath, const char *linkpath) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,nodepath,&parent,NULL,name,&nleng,PATH_TO_INODES_SKIP_LAST,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (parent==0) {
		return MFS_ERROR_EINVAL;
	}

	status = fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)linkpath,cr->uid,cr->gidcnt,cr->gidtab,&inode,attr);
	if (status==MFS_STATUS_OK) {
		mfs_path_created(nodepath);
	}
	return status;
}

uint8_t mfs_int_readlink(mfs_int_cred *cr, const char *nodepath, char linkpath[MFS_SYMLINK_MAX]) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	const uint8_t *cpath;

	status = mfs_path_to_inodes(cr,nodepath,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	status = fs_readlink(inode,&cpath);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	strncpy(linkpath,(const char *)cpath,MFS_SYMLINK_MAX);
	linkpath[MFS_SYMLINK_MAX-1]='\0';
	return MFS_STATUS_OK;
}

static uint8_t mfs_int_setattr(mfs_int_cred *cr, uint32_t inode, uint8_t opened, uint8_t setmask, uint16_t mode, uint32_t uid, uint32_t gid, uint32_t atime, uint32_t mtime, uint8_t winattr) {
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = fs_setattr(inode,opened,cr->uid,cr->gidcnt,cr->gidtab,setmask,mode&07777,uid,gid,atime,mtime,winattr,sugid_clear_mode,attr);
	if (status==MFS_STATUS_OK) {
		mfs_inode_invalidate(inode);
	}
	return status;
}

uint8_t mfs_int_setwinattr(mfs_int_cred *cr, const char *path, uint8_t winattr) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	return mfs_int_setattr(cr,inode,0,SET_WINATTR_FLAG,0,0,0,0,0,winattr);
}

uint8_t mfs_int_fsetwinattr(mfs_int_cred *cr, int fildes, uint8_t winattr) {
	file_info *fileinfo;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_FORBIDDEN) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return mfs_int_setattr(cr,fileinfo->inode,1,SET_WINATTR_FLAG,0,0,0,0,0,winattr);
}

uint8_t mfs_int_chmod(mfs_int_cred *cr, const char *path, uint16_t mode) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	return mfs_int_setattr(cr,inode,0,SET_MODE_FLAG,mode,0,0,0,0,0);
}

uint8_t mfs_int_fchmod(mfs_int_cred *cr, int fildes, uint16_t mode) {
	file_info *fileinfo;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_FORBIDDEN) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return mfs_int_setattr(cr,fileinfo->inode,1,SET_MODE_FLAG,mode,0,0,0,0,0);
}

uint8_t mfs_int_chown(mfs_int_cred *cr, const char *path, uint32_t owner, uint32_t group) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t setmask;
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	setmask = 0;
	if (owner!=MFS_UGID_NONE) {
		setmask |= SET_UID_FLAG;
	}
	if (group!=MFS_UGID_NONE) {
		setmask |= SET_GID_FLAG;
	}
	return mfs_int_setattr(cr,inode,0,setmask,0,owner,group,0,0,0);
}

uint8_t mfs_int_fchown(mfs_int_cred *cr, int fildes, uint32_t owner, uint32_t group) {
	file_info *fileinfo;
	uint8_t setmask;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_FORBIDDEN) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	setmask = 0;
	if (owner!=MFS_UGID_NONE) {
		setmask |= SET_UID_FLAG;
	}
	if (group!=MFS_UGID_NONE) {
		setmask |= SET_GID_FLAG;
	}
	return mfs_int_setattr(cr,fileinfo->inode,1,setmask,0,owner,group,0,0,0);
}

uint8_t mfs_int_utimes(mfs_int_cred *cr, const char *path, uint8_t flags, uint32_t atime, uint32_t mtime) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t setmask;
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	setmask = 0;
	if (flags & MFS_TIMES_ATIME_NOW) {
		setmask |= SET_ATIME_NOW_FLAG;
	} else if ((flags & MFS_TIMES_ATIME_OMIT)==0) {
		setmask |= SET_ATIME_FLAG;
	}
	if (flags & MFS_TIMES_MTIME_NOW) {
		setmask |= SET_MTIME_NOW_FLAG;
	} else if ((flags & MFS_TIMES_MTIME_OMIT)==0) {
		setmask |= SET_MTIME_FLAG;
	}
	return mfs_int_setattr(cr,inode,0,setmask,0,0,0,atime,mtime,0);
}

uint8_t mfs_int_futimes(mfs_int_cred *cr, int fildes, uint8_t flags, uint32_t atime, uint32_t mtime) {
	file_info *fileinfo;
	uint8_t setmask;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_FORBIDDEN) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	setmask = 0;
	if (flags & MFS_TIMES_ATIME_NOW) {
		setmask |= SET_ATIME_NOW_FLAG;
	} else if ((flags & MFS_TIMES_ATIME_OMIT)==0) {
		setmask |= SET_ATIME_FLAG;
	}
	if (flags & MFS_TIMES_MTIME_NOW) {
		setmask |= SET_MTIME_NOW_FLAG;
	} else if ((flags & MFS_TIMES_MTIME_OMIT)==0) {
		setmask |= SET_MTIME_FLAG;
	}
	return mfs_int_setattr(cr,fileinfo->inode,1,setmask,0,0,0,atime,mtime,0);
}

static uint8_t mfs_int_truncate_common(mfs_int_cred *cr, uint32_t inode, uint8_t opened, int64_t size, uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t status;

	if (size<0) {
		return MFS_ERROR_EINVAL;
	}
	if (size>=MAX_FILE_SIZE) {
		return MFS_ERROR_EFBIG;
	}
	write_data_flush_inode(inode);
	status = do_truncate(inode,(opened)?TRUNCATE_FLAG_OPENED:0,cr->uid,cr->gidcnt,cr->gidtab,size,attr,NULL);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	chunksdatacache_clear_inode(inode,size/MFSCHUNKSIZE);
	finfo_change_fleng(inode,size);
	write_data_inode_setmaxfleng(inode,size);
	read_inode_set_length_active(inode,size);
	mfs_inode_invalidate(inode);
	return MFS_STATUS_OK;
}

uint8_t mfs_int_truncate(mfs_int_cred *cr, const char *path, int64_t size) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return -1;
	}
	return mfs_int_truncate_common(cr,inode,0,size,attr);
}

uint8_t mfs_int_ftruncate(mfs_int_cred *cr, int fildes, int64_t size) {
	file_info *fileinfo;
	uint8_t attr[ATTR_RECORD_SIZE];

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_READONLY || fileinfo->mode==MFS_IO_ATTRONLY || fileinfo->mode==MFS_IO_FORBIDDEN || fileinfo->mode==MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return mfs_int_truncate_common(cr,fileinfo->inode,1,size,attr);
	// should I change fileinfo->offset when it is higher than current file size
}

uint8_t mfs_int_lseek(int fildes, int64_t *offset, uint8_t whence) {
	file_info *fileinfo;
	int64_t noffset;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_FORBIDDEN || fileinfo->mode==MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	switch (whence) {
		case MFS_SEEK_SET:
			noffset = *offset;
			break;
		case MFS_SEEK_CUR:
			noffset = (int64_t)(fileinfo->offset) + *offset;
			break;
		case MFS_SEEK_END:
			noffset = (int64_t)inoleng_getfleng(fileinfo->flengptr) + *offset;
			break;
		default:
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
			return MFS_ERROR_EINVAL;
	}
	if (noffset<0) {
		noffset = 0;
	}
	*offset = fileinfo->offset = noffset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return MFS_STATUS_OK;
}

static void mfs_fix_attr(uint8_t type, uint32_t inode, mfs_int_statrec *buf) {
	if (type==TYPE_FILE) {
		uint64_t maxfleng = write_data_inode_getmaxfleng(inode);
		if (maxfleng>buf->length) {
			buf->length = maxfleng;
		}
		read_inode_set_length_passive(inode,buf->length);
		finfo_change_fleng(inode,buf->length);
	}
	fs_fix_amtime(inode,&(buf->atime),&(buf->mtime));
}

uint8_t mfs_int_stat(mfs_int_cred *cr, const char *path, mfs_int_statrec *buf) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t type;
	uint8_t status;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	memset(buf,0,sizeof(mfs_int_statrec));
	mfs_attr_to_mfsstat(inode,attr,buf);
	type = mfs_attr_get_type(attr);
	mfs_fix_attr(type,inode,buf);
	return MFS_STATUS_OK;
}

uint8_t mfs_int_fstat(mfs_int_cred *cr, int fildes, mfs_int_statrec *buf) {
	uint8_t attr[ATTR_RECORD_SIZE];
	file_info *fileinfo;
	uint8_t status;
	uint8_t type;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_FORBIDDEN) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	status = fs_getattr(fileinfo->inode,1,cr->uid,cr->gidtab[0],attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	memset(buf,0,sizeof(mfs_int_statrec));
	mfs_attr_to_mfsstat(fileinfo->inode,attr,buf);
	type = mfs_attr_get_type(attr);
	mfs_fix_attr(type,fileinfo->inode,buf);
	return MFS_STATUS_OK;
}

uint8_t mfs_int_statfs(mfs_int_statfsrec *buf) {
	memset(buf,0,sizeof(mfs_int_statfsrec));
	fs_statfs(&(buf->totalspace),&(buf->availspace),&(buf->freespace),&(buf->trashspace),&(buf->sustainedspace),&(buf->inodes));
	fs_getmasterparams(&(buf->masterip),&(buf->masterport),&(buf->sessionid),&(buf->masterversion),&(buf->masterprocessid));
	return MFS_STATUS_OK;
}

uint8_t mfs_int_open(mfs_int_cred *cr, int *fildes, const char *path, int oflag, int mode) {
	uint64_t fsize;
	uint32_t parent;
	uint32_t inode;
	uint8_t noatomictrunc;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	uint8_t mfsoflag;
	uint8_t oflags;
	int needopen;
	file_info *fileinfo;

	mfsoflag = 0;
	switch (oflag&MFS_O_ACCMODE) {
		case MFS_O_RDONLY:
			mfsoflag |= OPEN_READ;
			break;
		case MFS_O_WRONLY:
			mfsoflag |= OPEN_WRITE;
			break;
		case MFS_O_RDWR:
			mfsoflag |= OPEN_READ | OPEN_WRITE;
			break;
		case MFS_O_ATTRONLY:
			break;
		default:
			return MFS_ERROR_EINVAL;
	}
	if (oflag&MFS_O_TRUNC) {
		uint32_t mver;
		mver = master_version();
		noatomictrunc = ((mver<VERSION2INT(4,18,0)&&mver>=VERSION2INT(4,0,0))||mver<VERSION2INT(3,0,113))?1:0;
		mfsoflag |= OPEN_TRUNCATE;
	} else {
		noatomictrunc = 0;
	}

	oflags = 0;
	needopen = 1;
	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_CHECK_LAST,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (oflag&MFS_O_CREAT) {
		if (oflag&MFS_O_EXCL) {
			if (inode!=0) { // file exists
				return MFS_ERROR_EEXIST;
			}
		} else {
			if (inode==0) { // file doesn't exists - create it
				status = fs_create(parent,nleng,(const uint8_t*)name,mode,cr->umask,cr->uid,cr->gidcnt,cr->gidtab,&inode,attr,&oflags);
				if (status!=MFS_STATUS_OK) {
					return status;
				}
				mfs_path_created(path);
				needopen = 0;
			}
		}
	} else {
		if (inode==0) {
			return MFS_ERROR_ENOENT;
		}
	}
	if (needopen) {
		if (mfs_attr_to_type(attr)!=TYPE_FILE) {
			return MFS_ERROR_EISDIR;
		}

		// open
		status = fs_opencheck(inode,cr->uid,cr->gidcnt,cr->gidtab,mfsoflag,attr,&oflags);
		if (status!=MFS_STATUS_OK) {
			return status;
		}
		if (mfsoflag&OPEN_TRUNCATE && noatomictrunc) {
			status = mfs_int_truncate_common(cr,inode,1,0,attr);
			if (status!=MFS_STATUS_OK) {
				return status;
			}
		}
	}
	if (oflags & OPEN_APPENDONLY) {
		if ((oflag&MFS_O_APPEND)==0) {
			return MFS_ERROR_EPERM;
		}
	}

	fs_inc_acnt(inode);

	fsize = mfs_attr_to_size(attr);
	*fildes = mfs_next_fd();
	fileinfo = mfs_get_fi(*fildes);
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
	fileinfo->privuser = (cr->uid==0)?1:0;
	fileinfo->reading = 0;
	fileinfo->wasread = 0;
	fileinfo->dataformat = 0;
	fileinfo->dbuff = NULL;
	fileinfo->dbuffsize = 0;

	inoleng_setfleng(fileinfo->flengptr,fsize);
	if ((oflag&MFS_O_ACCMODE) == MFS_O_RDONLY) {
		fileinfo->mode = MFS_IO_READONLY;
		fileinfo->rdata = read_data_new(inode,fsize);
	} else if ((oflag&MFS_O_ACCMODE) == MFS_O_WRONLY) {
		if (oflag&MFS_O_APPEND) {
			fileinfo->mode = MFS_IO_APPENDONLY;
		} else {
			fileinfo->mode = MFS_IO_WRITEONLY;
		}
		fileinfo->wdata = write_data_new(inode,fsize);
	} else if ((oflag&MFS_O_ACCMODE) == MFS_O_RDWR) {
		if (oflag&MFS_O_APPEND) {
			fileinfo->mode = MFS_IO_READAPPEND;
		} else {
			fileinfo->mode = MFS_IO_READWRITE;
		}
		fileinfo->rdata = read_data_new(inode,fsize);
		fileinfo->wdata = write_data_new(inode,fsize);
	} else {
		fileinfo->mode = MFS_O_ATTRONLY;
	}
	if (oflag&MFS_O_APPEND) {
		fileinfo->offset = fsize;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return MFS_STATUS_OK;
}

static uint8_t mfs_int_error_conv(int err) { // TODO: change readdata and writedata to return mfs errors instead of errno-like errors
	switch (err) {
		case 0:
			return MFS_STATUS_OK;
		case EBADF:
			return MFS_ERROR_EBADF;
		case EINVAL:
			return MFS_ERROR_EINVAL;
#ifdef EDQUOT
		case EDQUOT:
			return MFS_ERROR_QUOTA;
#endif
		case ENOSPC:
			return MFS_ERROR_NOSPACE;
		case EFBIG:
			return MFS_ERROR_EFBIG;
		case ENXIO:
			return MFS_ERROR_CHUNKLOST;
		case EIO:
			return MFS_ERROR_IO;
	}
	return MFS_ERROR_IO;
}

static uint8_t mfs_int_pread_common(file_info *fileinfo,int32_t *rsize,uint8_t *buf,uint32_t nbyte,uint64_t offset) {
	uint32_t ssize;
	struct iovec *iov;
	uint32_t iovcnt,pos,i;
	void *buffptr;
	uint8_t status;

	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	if (offset>=MAX_FILE_SIZE || offset+nbyte>=MAX_FILE_SIZE) {
		return MFS_ERROR_EFBIG;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_WRITEONLY || fileinfo->mode==MFS_IO_APPENDONLY || fileinfo->mode==MFS_IO_ATTRONLY || fileinfo->mode==MFS_IO_FORBIDDEN || fileinfo->mode==MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
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
	status = mfs_int_error_conv(read_data(fileinfo->rdata,offset,&ssize,&buffptr,&iov,&iovcnt));
	fs_atime(fileinfo->inode);

	if (status==MFS_STATUS_OK) {
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
	if (status!=MFS_STATUS_OK) {
		fs_read_notify(0);
		return status;
	}
	fs_read_notify(ssize);
	*rsize = ssize;
	return MFS_STATUS_OK;
}

uint8_t mfs_int_pread(int fildes,int64_t *rsize,uint8_t *buf,uint64_t nbyte,uint64_t offset) {
	file_info *fileinfo;
	int32_t rsize_part;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}

	*rsize = 0;
	while (nbyte>0x1000000) {
		status = mfs_int_pread_common(fileinfo,&rsize_part,buf,0x1000000,offset);
		if (status!=MFS_STATUS_OK) {
			return status;
		}
		offset += rsize_part;
		buf += rsize_part;
		nbyte -= rsize_part;
		*rsize += rsize_part;
		if (rsize_part<0x1000000) {
			return MFS_STATUS_OK;
		}
	}
	status = mfs_int_pread_common(fileinfo,&rsize_part,buf,nbyte,offset);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	*rsize += rsize_part;
	return MFS_STATUS_OK;
}

uint8_t mfs_int_read(int fildes,int64_t *rsize,uint8_t *buf,uint64_t nbyte) {
	file_info *fileinfo;
	int32_t rsize_part;
	uint64_t offset;
	uint8_t status;

	*rsize = 0;
	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	offset = fileinfo->offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	while (nbyte>0x1000000) {
		status = mfs_int_pread_common(fileinfo,&rsize_part,buf,0x1000000,offset);
		if (status!=MFS_STATUS_OK) {
			return status;
		}
		offset += rsize_part;
		buf += rsize_part;
		nbyte -= rsize_part;
		*rsize += rsize_part;
		if (rsize_part<0x1000000) {
			zassert(pthread_mutex_lock(&(fileinfo->lock)));
			fileinfo->offset = offset;
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
			return MFS_STATUS_OK;
		}
	}
	status = mfs_int_pread_common(fileinfo,&rsize_part,buf,nbyte,offset);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	offset += rsize_part;
	*rsize += rsize_part;
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	fileinfo->offset = offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return MFS_STATUS_OK;
}

static uint8_t mfs_int_pwrite_common(file_info *fileinfo,int32_t *rsize,const uint8_t *buf,uint32_t nbyte,uint64_t offset) {
	uint64_t newfleng;
	uint8_t appendonly;
	uint8_t status;

	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	if (offset>=MAX_FILE_SIZE || offset+nbyte>=MAX_FILE_SIZE) {
		return MFS_ERROR_EFBIG;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	appendonly = (fileinfo->mode==MFS_IO_APPENDONLY || fileinfo->mode==MFS_IO_READAPPEND)?1:0;
	if (fileinfo->mode==MFS_IO_READONLY || fileinfo->mode==MFS_IO_ATTRONLY || fileinfo->mode==MFS_IO_FORBIDDEN || fileinfo->mode==MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	// rwlock_wrlock begin
	fileinfo->writers_cnt++;
	while (fileinfo->readers_cnt | fileinfo->writing) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	fileinfo->writers_cnt--;
	fileinfo->writing = 1;
	// rwlock_wrlock end

	status = MFS_STATUS_OK;
	if (appendonly) {
		if (master_version()>=VERSION2INT(3,0,113)) {
			uint64_t prevleng;
			uint32_t gid = 0;
			uint32_t inode = fileinfo->inode;
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
			status = do_truncate(inode,TRUNCATE_FLAG_OPENED|TRUNCATE_FLAG_UPDATE|TRUNCATE_FLAG_RESERVE,0,1,&gid,nbyte,NULL,&prevleng);
			zassert(pthread_mutex_lock(&(fileinfo->lock)));
			if (status==MFS_STATUS_OK) {
				offset = prevleng;
			}
		} else {
			offset = inoleng_getfleng(fileinfo->flengptr);
			if (offset+nbyte>=MAX_FILE_SIZE) {
				status = MFS_ERROR_EFBIG;
			}
		}
	}
	if (status==MFS_STATUS_OK) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		fs_mtime(fileinfo->inode);
		status = mfs_int_error_conv(write_data(fileinfo->wdata,offset,nbyte,(const uint8_t*)buf,fileinfo->privuser));
		fs_mtime(fileinfo->inode);
		zassert(pthread_mutex_lock(&(fileinfo->lock)));
	}

	// rwlock_wrunlock begin
	fileinfo->writing = 0;
	zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
	// wrlock_wrunlock end

	if (status!=MFS_STATUS_OK) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		fs_write_notify(0);
		return status;
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
	fs_write_notify(nbyte);
	mfs_inode_invalidate(fileinfo->inode);
	*rsize = nbyte;
//	fdcache_invalidate(fileinfo->inode);
	return MFS_STATUS_OK;
}

uint8_t mfs_int_pwrite(int fildes,int64_t *rsize,const uint8_t *buf,uint64_t nbyte,uint64_t offset) {
	file_info *fileinfo;
	int32_t rsize_part;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}

	*rsize = 0;
	while (nbyte>0x1000000) {
		status = mfs_int_pwrite_common(fileinfo,&rsize_part,buf,0x1000000,offset);
		if (status!=MFS_STATUS_OK) {
			return status;
		}
		offset += rsize_part;
		buf += rsize_part;
		nbyte -= rsize_part;
		*rsize += rsize_part;
		if (rsize_part<0x1000000) {
			return MFS_ERROR_IO;
		}
	}
	status = mfs_int_pwrite_common(fileinfo,&rsize_part,buf,nbyte,offset);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	*rsize += rsize_part;
	return MFS_STATUS_OK;
}

uint8_t mfs_int_write(int fildes,int64_t *rsize,const uint8_t *buf,uint64_t nbyte) {
	file_info *fileinfo;
	int32_t rsize_part;
	uint64_t offset;
	uint8_t status;

	*rsize = 0;
	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	offset = fileinfo->offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	while (nbyte>0x1000000) {
		status = mfs_int_pwrite_common(fileinfo,&rsize_part,buf,0x1000000,offset);
		if (status!=MFS_STATUS_OK) {
			return status;
		}
		offset += rsize_part;
		buf += rsize_part;
		nbyte -= rsize_part;
		*rsize += rsize_part;
		if (rsize_part<0x1000000) {
			return MFS_ERROR_IO;
		}
	}
	status = mfs_int_pwrite_common(fileinfo,&rsize_part,buf,nbyte,offset);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	offset += rsize_part;
	*rsize += rsize_part;
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_APPENDONLY || fileinfo->mode==MFS_IO_READAPPEND) {
		fileinfo->offset = inoleng_getfleng(fileinfo->flengptr);
	} else {
		fileinfo->offset = offset;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return MFS_STATUS_OK;
}

static uint8_t mfs_int_fsync_common(file_info *fileinfo) {
	uint8_t status;

	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->wdata!=NULL && (fileinfo->mode!=MFS_IO_READONLY && fileinfo->mode!=MFS_IO_ATTRONLY && fileinfo->mode!=MFS_IO_FORBIDDEN && fileinfo->mode!=MFS_IO_DIRECTORY)) {
		// rwlock_wrlock begin
		fileinfo->writers_cnt++;
		while (fileinfo->readers_cnt | fileinfo->writing) {
			zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
		}
		fileinfo->writers_cnt--;
		fileinfo->writing = 1;
		// rwlock_wrlock end
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));

		status = mfs_int_error_conv(write_data_flush(fileinfo->wdata));

		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		// rwlock_wrunlock begin
		fileinfo->writing = 0;
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
		// rwlock_wrunlock end
	} else {
		status = MFS_STATUS_OK;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
//	if (status==MFS_STATUS_OK) {
//		fdcache_invalidate(inode);
//		dcache_invalidate_attr(inode);
//	}
	return status;
}

uint8_t mfs_int_fsync(int fildes) {
	fs_fsync_notify();
	return mfs_int_fsync_common(mfs_get_fi(fildes));
}

uint8_t mfs_int_close(int fildes) {
	file_info *fileinfo;
	uint8_t decacnt;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode==MFS_IO_FORBIDDEN || fileinfo->mode==MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	// rwlock_wait_for_unlock:
	while (fileinfo->writing | fileinfo->writers_cnt | fileinfo->readers_cnt) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	if (fileinfo->mode != MFS_IO_FORBIDDEN) {
		decacnt = 1;
		fileinfo->mode = MFS_IO_FORBIDDEN;
	} else {
		decacnt = 0;
	}
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	status = mfs_int_fsync_common(fileinfo);
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
	if (decacnt) {
		fs_dec_acnt(fileinfo->inode);
	}
	mfs_free_fd(fildes);
	return status;
}

uint8_t mfs_int_flock(int fildes, uint8_t op) {
	uint8_t lock_mode;
	file_info *fileinfo;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}

	if (op&MFS_LOCK_UN) {
		lock_mode = FLOCK_UNLOCK;
	} else if (op&MFS_LOCK_SH) {
		if (op&MFS_LOCK_NB) {
			lock_mode=FLOCK_TRY_SHARED;
		} else {
			lock_mode=FLOCK_LOCK_SHARED;
		}
	} else if (op&MFS_LOCK_EX) {
		if (op&MFS_LOCK_NB) {
			lock_mode=FLOCK_TRY_EXCLUSIVE;
		} else {
			lock_mode=FLOCK_LOCK_EXCLUSIVE;
		}
	} else {
		return MFS_ERROR_EINVAL;
	}

	if (lock_mode==FLOCK_UNLOCK) {
		mfs_int_fsync_common(fileinfo);
	}

	return fs_flock(fileinfo->inode,0,fildes,lock_mode);
}

uint8_t mfs_int_lockf(int fildes, uint32_t pid, uint8_t function, int64_t size) {
	uint64_t start,end;
	file_info *fileinfo;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}

	if (size>0) {
		start = fileinfo->offset;
		end = start+size;
		if (end<start) {
			return MFS_ERROR_EINVAL;
		}
	} else if (size<0) {
		end = fileinfo->offset;
		start = end+size;
		if (end<start) {
			return MFS_ERROR_EINVAL;
		}
	} else { //size = 0;
		start = fileinfo->offset;
		end = UINT64_MAX;
	}

	if (function==MFS_F_ULOCK) {
		mfs_int_fsync_common(fileinfo);
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_SET,POSIX_LOCK_UNLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==MFS_F_LOCK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_SET,POSIX_LOCK_WRLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==MFS_F_TLOCK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_TRY,POSIX_LOCK_WRLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==MFS_F_TEST) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_GET,POSIX_LOCK_WRLCK,start,end,pid,NULL,NULL,NULL,NULL);
	} else {
		return MFS_ERROR_EINVAL;
	}

	return status;
}

uint8_t mfs_int_fcntl_locks(int fildes, uint32_t pid, uint8_t function, mfs_int_flockrec *fl) {
	uint64_t start,end,rstart,rend;
	uint32_t rpid;
	uint8_t type,rtype;
	file_info *fileinfo;
	uint8_t status;

	fileinfo = mfs_get_fi(fildes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}

	if (fl->whence==MFS_SEEK_CUR) {
		if (fl->start > (int64_t)(fileinfo->offset)) {
			start = 0;
		} else {
			start = fileinfo->offset + fl->start;
		}
	} else if (fl->whence==MFS_SEEK_SET) {
		if (fl->start < 0) {
			start = 0;
		} else {
			start = fl->start;
		}
	} else if (fl->whence==MFS_SEEK_END) {
		if (fl->start > (int64_t)inoleng_getfleng(fileinfo->flengptr)) {
			start = 0;
		} else {
			start = inoleng_getfleng(fileinfo->flengptr) + fl->start;
		}
	} else {
		return MFS_ERROR_EINVAL;
	}
	if (fl->len <= 0) {
		end = UINT64_MAX;
	} else {
		end = start + fl->len;
		if (end<start) {
			end = UINT64_MAX;
		}
	}
	if (fl->type == MFS_F_UNLCK) {
		type = POSIX_LOCK_UNLCK;
	} else if (fl->type == MFS_F_RDLCK) {
		type = POSIX_LOCK_RDLCK;
	} else if (fl->type == MFS_F_WRLCK) {
		type = POSIX_LOCK_WRLCK;
	} else {
		return MFS_ERROR_EINVAL;
	}

	if (type==POSIX_LOCK_UNLCK) {
		mfs_int_fsync_common(fileinfo);
	}


	if (function==MFS_F_GETLK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_GET,type,start,end,pid,&rtype,&rstart,&rend,&rpid);
	} else if (function==MFS_F_SETLKW) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_SET,type,start,end,pid,NULL,NULL,NULL,NULL);
	} else if (function==MFS_F_SETLK) {
		status = fs_posixlock(fileinfo->inode,0,fildes,POSIX_LOCK_CMD_TRY,type,start,end,pid,NULL,NULL,NULL,NULL);
	} else {
		return MFS_ERROR_EINVAL;
	}

	if (status!=MFS_STATUS_OK) {
		return status;
	}

	if (function==MFS_F_GETLK) {
		memset(fl,0,sizeof(mfs_int_flockrec));
		if (rtype==POSIX_LOCK_RDLCK) {
			fl->type = MFS_F_RDLCK;
		} else if (rtype==POSIX_LOCK_WRLCK) {
			fl->type = MFS_F_WRLCK;
		} else {
			fl->type = MFS_F_UNLCK;
		}
		fl->whence = MFS_SEEK_SET;
		fl->start = rstart;
		if ((rend-rstart)>INT64_MAX) {
			fl->len = 0;
		} else {
			fl->len = (rend - rstart);
		}
		fl->pid = rpid;
	}

	return MFS_STATUS_OK;
}

uint8_t mfs_int_opendir(mfs_int_cred *cr, int *dirdes, const char *path) {
	uint32_t parent;
	uint32_t inode;
	uint8_t name[256];
	uint8_t nleng;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t status;
	file_info *fileinfo;

	status = mfs_path_to_inodes(cr,path,&parent,&inode,name,&nleng,PATH_TO_INODES_EXPECT_OBJECT,attr);
	if (status!=MFS_STATUS_OK) {
		return status;
	}
	if (mfs_attr_to_type(attr)!=TYPE_DIRECTORY) {
		return MFS_ERROR_ENOTDIR;
	}

	*dirdes = mfs_next_fd();
	fileinfo = mfs_get_fi(*dirdes);
	fileinfo->flengptr = NULL;
	fileinfo->inode = inode;
	fileinfo->mode = MFS_IO_DIRECTORY;
	fileinfo->offset = 0;
	fileinfo->rdata = NULL;
	fileinfo->wdata = NULL;
	fileinfo->readers_cnt = 0;
	fileinfo->writers_cnt = 0;
	fileinfo->writing = 0;
	fileinfo->privuser = 0;
	fileinfo->reading = 0;
	fileinfo->wasread = 0;
	fileinfo->dataformat = 0;
	fileinfo->dbuff = NULL;
	fileinfo->dbuffsize = 0;

	return MFS_STATUS_OK;
}

uint8_t mfs_int_readdir(mfs_int_cred *cr, int dirdes, mfs_int_direntry *de) {
	file_info *fileinfo;
	const uint8_t *dbuff;
	const uint8_t *ptr,*eptr;
	uint8_t nleng;
	uint32_t dsize;
	uint32_t rsize;
	uint8_t status;

	memset(de,0,sizeof(mfs_int_direntry));
	fileinfo = mfs_get_fi(dirdes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode!=MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	while (fileinfo->reading) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	if (fileinfo->wasread==0 || fileinfo->offset==0) {
		fileinfo->reading = 1;
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		status = fs_readdir(fileinfo->inode,cr->uid,cr->gidcnt,cr->gidtab,NULL,0,0,&dbuff,&dsize);
		if (fileinfo->dbuff!=NULL) {
			free(fileinfo->dbuff);
		}
		if (status==MFS_STATUS_OK) {
			fileinfo->dbuff = malloc(dsize);
			memcpy(fileinfo->dbuff,dbuff,dsize);
			fileinfo->dbuffsize = dsize;
			fileinfo->dataformat = 0;
		} else {
			fileinfo->dbuff = NULL;
			fileinfo->dbuffsize = 0;
		}
		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		fileinfo->reading = 0;
		fileinfo->wasread = 1;
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
		if (status!=MFS_STATUS_OK) {
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
			return status;
		}
	}
	ptr = fileinfo->dbuff+fileinfo->offset;
	eptr = fileinfo->dbuff+fileinfo->dbuffsize;
	if (ptr<eptr) {
		nleng = ptr[0];
		rsize = nleng + (fileinfo->dataformat ? (master_attrsize() + 5) : 6);
	} else {
		nleng = 0;
		rsize = 0;
	}
	if (rsize>0 && ptr+rsize<=eptr) {
		fileinfo->offset += rsize;
		ptr++;
		memcpy(de->name,ptr,nleng);
		de->name[nleng] = 0;
		ptr+=nleng;
		de->inode = get32bit(&ptr);
		if (fileinfo->dataformat) {
			de->type = mfs_attr_get_type(ptr);
		} else {
			de->type = get8bit(&ptr);
		}
		status = MFS_STATUS_OK;
	} else {
		fileinfo->offset = fileinfo->dbuffsize;
		status = MFS_ERROR_NOTFOUND;
	}

	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return status;
}

uint8_t mfs_int_readdirplus(mfs_int_cred *cr, int dirdes, mfs_int_direntryplus *de) {
	file_info *fileinfo;
	const uint8_t *dbuff;
	const uint8_t *ptr,*eptr;
	uint8_t nleng;
	uint32_t inode;
	uint32_t dsize;
	uint32_t rsize;
	uint8_t status;

	memset(de,0,sizeof(mfs_int_direntry));
	fileinfo = mfs_get_fi(dirdes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode!=MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	while (fileinfo->reading) {
		zassert(pthread_cond_wait(&(fileinfo->rwcond),&(fileinfo->lock)));
	}
	if (fileinfo->wasread==0 || fileinfo->offset==0 || fileinfo->dataformat==0) {
		fileinfo->reading = 1;
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		status = fs_readdir(fileinfo->inode,cr->uid,cr->gidcnt,cr->gidtab,NULL,1,0,&dbuff,&dsize);
		if (fileinfo->dbuff!=NULL) {
			free(fileinfo->dbuff);
		}
		if (status==MFS_STATUS_OK) {
			fileinfo->dbuff = malloc(dsize);
			memcpy(fileinfo->dbuff,dbuff,dsize);
			fileinfo->dbuffsize = dsize;
			fileinfo->dataformat = 1;
		} else {
			fileinfo->dbuff = NULL;
			fileinfo->dbuffsize = 0;
		}
		zassert(pthread_mutex_lock(&(fileinfo->lock)));
		fileinfo->reading = 0;
		fileinfo->wasread = 1;
		zassert(pthread_cond_broadcast(&(fileinfo->rwcond)));
		if (status!=MFS_STATUS_OK) {
			zassert(pthread_mutex_unlock(&(fileinfo->lock)));
			return status;
		}
	}
	ptr = fileinfo->dbuff+fileinfo->offset;
	eptr = fileinfo->dbuff+fileinfo->dbuffsize;
	if (ptr<eptr) {
		nleng = ptr[0];
		rsize = nleng + (fileinfo->dataformat ? (master_attrsize() + 5) : 6);
	} else {
		nleng = 0;
		rsize = 0;
	}
	if (rsize>0 && ptr+rsize<=eptr) {
		fileinfo->offset += rsize;
		ptr++;
		memcpy(de->name,ptr,nleng);
		de->name[nleng] = 0;
		ptr+=nleng;
		inode = get32bit(&ptr);
		mfs_attr_to_direntry(inode,ptr,de);
		status = MFS_STATUS_OK;
	} else {
		fileinfo->offset = fileinfo->dbuffsize;
		status = MFS_ERROR_NOTFOUND;
	}

	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return status;
}

uint8_t mfs_int_telldir(int dirdes, uint64_t *offset) {
	file_info *fileinfo;

	fileinfo = mfs_get_fi(dirdes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode!=MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	*offset = fileinfo->offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return MFS_STATUS_OK;
}

uint8_t mfs_int_seekdir(int dirdes, uint64_t offset) {
	file_info *fileinfo;

	fileinfo = mfs_get_fi(dirdes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode!=MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	if (offset>fileinfo->dbuffsize) {
		offset = 0;
	}
	fileinfo->offset = offset;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	return MFS_STATUS_OK;
}

uint8_t mfs_int_rewinddir(int dirdes) {
	return mfs_int_seekdir(dirdes,0);
}

uint8_t mfs_int_closedir(int dirdes) {
	file_info *fileinfo;

	fileinfo = mfs_get_fi(dirdes);
	if (fileinfo==NULL) {
		return MFS_ERROR_EBADF;
	}
	zassert(pthread_mutex_lock(&(fileinfo->lock)));
	if (fileinfo->mode!=MFS_IO_DIRECTORY) {
		zassert(pthread_mutex_unlock(&(fileinfo->lock)));
		return MFS_ERROR_EACCES;
	}
	fileinfo->mode = MFS_IO_FORBIDDEN;
	zassert(pthread_mutex_unlock(&(fileinfo->lock)));
	if (fileinfo->dbuff!=NULL) {
		free(fileinfo->dbuff);
	}
	mfs_free_fd(dirdes);
	return MFS_STATUS_OK;
}

char* mfs_int_get_config_str(const char *option_name) {
	uint8_t vleng;
	const uint8_t *vdata;
	char *ret;
	uint8_t status;

	status = fs_get_cfg(option_name,&vleng,&vdata);
	if (status!=MFS_STATUS_OK) {
		return NULL;
	}

	ret = malloc(1+vleng);
	memcpy(ret,vdata,vleng);
	ret[vleng]=0;
	return ret;
}

data_buff* mfs_int_get_config_file(const char *option_name) {
	uint16_t vleng;
	const uint8_t *vdata;
	data_buff *ret;
	uint8_t status;

	status = fs_get_cfg_file(option_name,&vleng,&vdata);	
	if (status!=MFS_STATUS_OK) {
		return NULL;
	}

	ret = malloc(offsetof(data_buff,data)+vleng);
	ret->leng = vleng;
	memcpy(ret->data,vdata,vleng);
	return ret;
}

void mfs_int_set_defaults(mfs_int_cfg *mcfg) {
	memset(mcfg,0,sizeof(mfs_int_cfg));
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
	mcfg->logident = strdup("libmfsio_int");
	mcfg->logdaemon = 0;
	mcfg->logminlevel = MFSLOG_INFO;
	mcfg->logelevateto = MFSLOG_NOTICE;
	mcfg->master_min_version_maj = 0;
	mcfg->master_min_version_mid = 0;
}

int mfs_int_init(mfs_int_cfg *mcfg,uint8_t stage) {
	uint32_t i;
	md5ctx ctx;
	uint8_t md5pass[16];

	if (stage==0 || stage==1) {
		mfs_log_init(mcfg->logident,mcfg->logdaemon);
		mfs_log_set_min_level(mcfg->logminlevel);
		mfs_log_set_elevate_to(mcfg->logelevateto);
		if (lcache_init(mcfg->lcache_retention)<0) {
			return -1;
		}
		if (csorder_init(mcfg->preferedlabels)<0) {
			return -1;
		}
		if (mcfg->masterpassword!=NULL) {
			md5_init(&ctx);
			md5_update(&ctx,(uint8_t*)(mcfg->masterpassword),strlen(mcfg->masterpassword));
			md5_final(md5pass,&ctx);
			memset(mcfg->masterpassword,0,strlen(mcfg->masterpassword));
		} else if (mcfg->mastermd5pass!=NULL) {
			uint8_t *p = (uint8_t*)(mcfg->mastermd5pass);
			for (i=0 ; i<16 ; i++) {
				if (*p>='0' && *p<='9') {
					md5pass[i]=(*p-'0')<<4;
				} else if (*p>='a' && *p<='f') {
					md5pass[i]=(*p-'a'+10)<<4;
				} else if (*p>='A' && *p<='F') {
					md5pass[i]=(*p-'A'+10)<<4;
				} else {
					return -1;
				}
				p++;
				if (*p>='0' && *p<='9') {
					md5pass[i]+=(*p-'0');
				} else if (*p>='a' && *p<='f') {
					md5pass[i]+=(*p-'a'+10);
				} else if (*p>='A' && *p<='F') {
					md5pass[i]+=(*p-'A'+10);
				} else {
					return -1;
				}
				p++;
			}
			if (*p) {
				return -1;
			}
			memset(mcfg->mastermd5pass,0,strlen(mcfg->mastermd5pass));
		}
		strerr_init();
		mycrc32_init();
		if (fs_init_master_connection(mcfg->masterbind,mcfg->masterhost,mcfg->masterport,0,mcfg->mountpoint,mcfg->masterpath,(mcfg->masterpassword!=NULL||mcfg->mastermd5pass!=NULL)?md5pass:NULL,1,0,VERSION2INT(mcfg->master_min_version_maj,mcfg->master_min_version_mid,0))<0) {
			return -1;
		}
		memset(md5pass,0,16);
	}

	if (stage==0 || stage==2) {
		inoleng_init();
		conncache_init(200);
		chunkrwlock_init();
		chunksdatacache_init();
		read_init();
		write_init();
		fs_init_threads(mcfg->io_try_cnt,mcfg->io_timeout);

		csdb_init();
		delay_init();
		read_data_init(mcfg->read_cache_mb*1024*1024,mcfg->readahead_leng,mcfg->readahead_trigger,mcfg->io_try_cnt,mcfg->io_timeout,mcfg->min_log_entry,mcfg->error_on_lost_chunk,mcfg->error_on_no_space);
		write_data_init(mcfg->write_cache_mb*1024*1024,mcfg->io_try_cnt,mcfg->io_timeout,mcfg->min_log_entry,mcfg->error_on_lost_chunk,mcfg->error_on_no_space);

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

void mfs_int_term(void) {
	uint32_t i;
	for (i=0 ; i<fdtabsize ; i++) {
		mfs_int_close(i);
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
	write_term();
	read_term();
	chunksdatacache_term();
	chunkrwlock_term();
	conncache_term();
	inoleng_term();
	stats_term();
	lcache_term();
	mfs_log_term();
}
