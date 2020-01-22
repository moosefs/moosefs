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
#include <string.h>
#include <stdlib.h>
#include <syslog.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>

#include "datapack.h"
#include "mastercomm.h"
#include "masterproxy.h"
#include "MFSCommunication.h"

#define READDIR_BUFFSIZE 50000

//typedef struct _minfo {
//	int sd;
//	int sent;
//} minfo;
typedef struct _dirbuf {
	int wasread;
	uint8_t *p;
	size_t size;
	pthread_mutex_t lock;
} dirbuf;

typedef struct _pathbuf {
	int changed;
	char *p;
	size_t size;
	pthread_mutex_t lock;
} pathbuf;

#define NAME_MAX 255
#define PATH_SIZE_LIMIT 1024

#define META_ROOT_INODE FUSE_ROOT_ID
#define META_ROOT_MODE 0555

#define META_SUBTRASH_INODE_MIN 0x7FFF0000
#define META_SUBTRASH_INODE_MAX ((0x7FFF0000+TRASH_BUCKETS)-1)
#define META_SUBTRASH_MODE 0700

#define META_TRASH_INODE 0x7FFFFFF8
#define META_TRASH_MODE 0700
#define META_TRASH_NAME "trash"
#define META_UNDEL_INODE 0x7FFFFFF9
#define META_UNDEL_MODE 0200
#define META_UNDEL_NAME "undel"
#define META_SUSTAINED_INODE 0x7FFFFFFA
#define META_SUSTAINED_MODE 0500
#define META_SUSTAINED_NAME "sustained"

//#define META_INODE_MIN META_ROOT_INODE
//#define META_INODE_MAX META_SUSTAINED_INODE

//#define INODE_VALUE_MASK 0x1FFFFFFF
//#define INODE_TYPE_MASK 0x60000000
//#define INODE_TYPE_TRASH 0x20000000
//#define INODE_TYPE_SUSTAINED 0x40000000
//#define INODE_TYPE_SPECIAL 0x00000000

// standard fs - inode(.master)=0x7FFFFFFF / inode(.masterinfo)=0x7FFFFFFE
// meta fs - inode(.master)=0x7FFFFFFE / inode(.masterinfo)=0x7FFFFFFF
//#define MASTER_NAME ".master"
//#define MASTER_INODE 0x7FFFFFFE
// 0x01b6 = 0666
//static uint8_t masterattr[ATTR_RECORD_SIZE]={'f', 0x01,0xB6, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define MASTERINFO_WITH_VERSION 1

#define MASTERINFO_NAME ".masterinfo"
#define MASTERINFO_INODE 0x7FFFFFFF
// 0x0124 = 0444
#ifdef MASTERINFO_WITH_VERSION
static uint8_t masterinfoattr[ATTR_RECORD_SIZE]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,14};
#else
static uint8_t masterinfoattr[ATTR_RECORD_SIZE]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10};
#endif

#define MIN_SPECIAL_INODE 0x7FFF0000
#define IS_SPECIAL_INODE(ino) ((ino)>=MIN_SPECIAL_INODE || (ino)==META_ROOT_INODE)

// info - todo
//#define META_INFO_INODE 0x7FFFFFFD
//#define META_INFO_NAME "info"

#define PKGVERSION ((VERSMAJ)*1000000+(VERSMID)*1000+(VERSMIN))

static int debug_mode = 0;
static int flat_trash = 0;
static double entry_cache_timeout = 0.0;
static double attr_cache_timeout = 1.0;

uint32_t mfs_meta_name_to_inode(const char *name) {
	uint32_t inode=0;
	char *end;
	inode = strtoul(name,&end,16);
	if (*end=='|' && end[1]!=0) {
		return inode;
	} else {
		return 0;
	}
}

static int mfs_errorconv(int status) {
	switch (status) {
		case MFS_STATUS_OK:
			return 0;
		case MFS_ERROR_EPERM:
			return EPERM;
		case MFS_ERROR_ENOTDIR:
			return ENOTDIR;
		case MFS_ERROR_ENOENT:
			return ENOENT;
		case MFS_ERROR_EACCES:
			return EACCES;
		case MFS_ERROR_EEXIST:
			return EEXIST;
		case MFS_ERROR_EINVAL:
			return EINVAL;
		case MFS_ERROR_ENOTEMPTY:
			return ENOTEMPTY;
		case MFS_ERROR_IO:
			return EIO;
		case MFS_ERROR_EROFS:
			return EROFS;
		case MFS_ERROR_QUOTA:
#ifdef EDQUOT
			return EDQUOT;
#else
			return ENOSPC;
#endif
		default:
			return EINVAL;
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

static void mfs_meta_type_to_stat(uint32_t inode,uint8_t type, struct stat *stbuf) {
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


static void mfs_meta_stat(uint32_t inode, struct stat *stbuf) {
	int now;
	stbuf->st_ino = inode;
	stbuf->st_size = 0;
//#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
//	stbuf->st_blocks = 0;
//#endif
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	stbuf->st_blksize = MFSBLOCKSIZE;
#endif
	switch (inode) {
	case META_ROOT_INODE:
		stbuf->st_nlink = 4;
		stbuf->st_mode = S_IFDIR | META_ROOT_MODE ;
		break;
	case META_TRASH_INODE:
		stbuf->st_nlink = 3+TRASH_BUCKETS;
		stbuf->st_mode = S_IFDIR | META_TRASH_MODE ;
		break;
	case META_UNDEL_INODE:
		stbuf->st_nlink = 2+TRASH_BUCKETS;
		stbuf->st_mode = S_IFDIR | META_UNDEL_MODE ;
		break;
	case META_SUSTAINED_INODE:
		stbuf->st_nlink = 2;
		stbuf->st_mode = S_IFDIR | META_SUSTAINED_MODE ;
		break;
	default:
		if (inode>=META_SUBTRASH_INODE_MIN && inode<=META_SUBTRASH_INODE_MAX) {
			stbuf->st_nlink = 3;
			stbuf->st_mode = S_IFDIR | META_SUBTRASH_MODE ;
		}
	}
	stbuf->st_uid = 0;
	stbuf->st_gid = 0;
	now = time(NULL);
	stbuf->st_atime = now;
	stbuf->st_mtime = now;
	stbuf->st_ctime = now;
#ifdef HAVE_STRUCT_STAT_ST_BIRTHTIME
	stbuf->st_birthtime = now;	// for future use
#endif
}

/*
static void mfs_inode_to_stat(uint32_t inode, struct stat *stbuf) {
	memset(stbuf,0,sizeof(struct stat));
	stbuf->st_ino = inode;
	stbuf->st_mode = S_IFREG;
}
*/

static void mfs_attr_to_stat(uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE], struct stat *stbuf) {
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime,attrnlink;
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
	attrlength = get64bit(&ptr);
	stbuf->st_ino = inode;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	stbuf->st_blksize = MFSBLOCKSIZE;
#endif
	if (attrtype==TYPE_FILE || attrtype==TYPE_TRASH || attrtype==TYPE_SUSTAINED) {
		stbuf->st_mode = S_IFREG | ( attrmode & 07777);
	} else {
		stbuf->st_mode = 0;
	}
	stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
	stbuf->st_blocks = (attrlength+511)/512;
#endif
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

#if FUSE_USE_VERSION >= 26
void mfs_meta_statfs(fuse_req_t req, fuse_ino_t ino) {
#else
void mfs_meta_statfs(fuse_req_t req) {
#endif
	uint64_t totalspace,availspace,freespace,trashspace,sustainedspace;
	uint32_t inodes;
	struct statvfs stfsbuf;
	memset(&stfsbuf,0,sizeof(stfsbuf));

#if FUSE_USE_VERSION >= 26
	(void)ino;
#endif
	fs_statfs(&totalspace,&availspace,&freespace,&trashspace,&sustainedspace,&inodes);

	stfsbuf.f_namemax = NAME_MAX;
	stfsbuf.f_frsize = MFSBLOCKSIZE;
	stfsbuf.f_bsize = MFSBLOCKSIZE;
	stfsbuf.f_blocks = trashspace/MFSBLOCKSIZE+sustainedspace/MFSBLOCKSIZE;
	stfsbuf.f_bfree = sustainedspace/MFSBLOCKSIZE;
	stfsbuf.f_bavail = sustainedspace/MFSBLOCKSIZE;
	stfsbuf.f_files = 1000000000+PKGVERSION;
	stfsbuf.f_ffree = 1000000000+PKGVERSION;
	stfsbuf.f_favail = 1000000000+PKGVERSION;

	fuse_reply_statfs(req,&stfsbuf);
}

/*
void mfs_meta_access(fuse_req_t req, fuse_ino_t ino, int mask) {
	const struct fuse_ctx *ctx;
	ctx = fuse_req_ctx(req);
	switch (ino) {
		case FUSE_ROOT_ID:
			if (mask & W_OK) {
				fuse_reply_err(req,EACCES);
				return;
			}
			break;
		case META_TRASH_INODE:
			if (mask & W_OK && ctx->uid!=0) {
				fuse_reply_err(req,EACCES);
				return;
			}
			break;
		case META_UNDEL_INODE:
			if (mask & (R_OK|X_OK)) {
				fuse_reply_err(req,EACCES);
				return;
			}
			break;
	}
	fuse_reply_err(req,0);
}
*/

void mfs_meta_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	struct fuse_entry_param e;
	uint32_t inode;
//	const struct fuse_ctx *ctx;
//	ctx = fuse_req_ctx(req);
	memset(&e, 0, sizeof(e));
	inode = 0;
	switch (parent) {
	case META_ROOT_INODE:
		if (strcmp(name,".")==0 || strcmp(name,"..")==0) {
			inode = META_ROOT_INODE;
		} else if (strcmp(name,META_TRASH_NAME)==0) {
			inode = META_TRASH_INODE;
		} else if (strcmp(name,META_SUSTAINED_NAME)==0) {
			inode = META_SUSTAINED_INODE;
//		} else if (strcmp(name,MASTER_NAME)==0) {
//			memset(&e, 0, sizeof(e));
//			e.ino = MASTER_INODE;
//			e.attr_timeout = 3600.0;
//			e.entry_timeout = 3600.0;
//			mfs_attr_to_stat(MASTER_INODE,masterattr,&e.attr);
//			fuse_reply_entry(req, &e);
//			return ;
		} else if (strcmp(name,MASTERINFO_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = MASTERINFO_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(MASTERINFO_INODE,masterinfoattr,&e.attr);
			fuse_reply_entry(req, &e);
			return ;
		}
		break;
	case META_TRASH_INODE:
		if (strcmp(name,".")==0) {
			inode = META_TRASH_INODE;
		} else if (strcmp(name,"..")==0) {
			inode = META_ROOT_INODE;
		} else if (strcmp(name,META_UNDEL_NAME)==0) {
			inode = META_UNDEL_INODE;
		} else if (master_version()>=VERSION2INT(3,0,64) && flat_trash==0) { // subtrashes
			inode = strtoul(name,NULL,16);
			if (inode<TRASH_BUCKETS) {
				inode += META_SUBTRASH_INODE_MIN;
			} else {
				inode = 0;
			}
		} else { // flat trash
			inode = mfs_meta_name_to_inode(name);
			if (inode>0) {
				int status;
				uint8_t attr[ATTR_RECORD_SIZE];
				status = fs_getdetachedattr(inode,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					fuse_reply_err(req, status);
				} else {
					e.ino = inode;
					e.attr_timeout = attr_cache_timeout;
					e.entry_timeout = entry_cache_timeout;
					mfs_attr_to_stat(inode,attr,&e.attr);
					fuse_reply_entry(req,&e);
				}
				return;
			}
		}
		break;
	case META_UNDEL_INODE:
		if (strcmp(name,".")==0) {
			inode = META_UNDEL_INODE;
		} else if (strcmp(name,"..")==0) {
			inode = META_TRASH_INODE;
		}
		break;
	case META_SUSTAINED_INODE:
		if (strcmp(name,".")==0) {
			inode = META_SUSTAINED_INODE;
		} else if (strcmp(name,"..")==0) {
			inode = META_ROOT_INODE;
		} else {
			inode = mfs_meta_name_to_inode(name);
			if (inode>0) {
				int status;
				uint8_t attr[ATTR_RECORD_SIZE];
				status = fs_getdetachedattr(inode,attr);
				status = mfs_errorconv(status);
				if (status!=0) {
					fuse_reply_err(req, status);
				} else {
					e.ino = inode;
					e.attr_timeout = attr_cache_timeout;
					e.entry_timeout = entry_cache_timeout;
					mfs_attr_to_stat(inode,attr,&e.attr);
					fuse_reply_entry(req,&e);
				}
				return;
			}
		}
		break;
	default:
		if (parent>=META_SUBTRASH_INODE_MIN && parent<=META_SUBTRASH_INODE_MAX) {
			if (strcmp(name,".")==0) {
				inode = parent;
			} else if (strcmp(name,"..")==0) {
				inode = META_TRASH_INODE;
			} else if (strcmp(name,META_UNDEL_NAME)==0) {
				inode = META_UNDEL_INODE;
			} else {
				inode = mfs_meta_name_to_inode(name);
				if (inode>0) {
					int status;
					uint8_t attr[ATTR_RECORD_SIZE];
					status = fs_getdetachedattr(inode,attr);
					status = mfs_errorconv(status);
					if (status!=0) {
						fuse_reply_err(req, status);
					} else {
						e.ino = inode;
						e.attr_timeout = attr_cache_timeout;
						e.entry_timeout = entry_cache_timeout;
						mfs_attr_to_stat(inode,attr,&e.attr);
						fuse_reply_entry(req,&e);
					}
					return;
				}
			}
		}
	}
	if (inode==0) {
		fuse_reply_err(req,ENOENT);
	} else {
		e.ino = inode;
		e.attr_timeout = attr_cache_timeout;
		e.entry_timeout = entry_cache_timeout;
		mfs_meta_stat(inode,&e.attr);
		fuse_reply_entry(req,&e);
	}
}

void mfs_meta_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	struct stat o_stbuf;
	(void)fi;
//	if (ino==MASTER_INODE) {
//		memset(&o_stbuf, 0, sizeof(struct stat));
//		mfs_attr_to_stat(ino,masterattr,&o_stbuf);
//		fuse_reply_attr(req, &o_stbuf, 3600.0);
//	} else
	if (ino==MASTERINFO_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,masterinfoattr,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, 3600.0);
	} else if (IS_SPECIAL_INODE(ino)) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_meta_stat(ino,&o_stbuf);
		fuse_reply_attr(req, &o_stbuf, attr_cache_timeout);
	} else {
		int status;
		uint8_t attr[ATTR_RECORD_SIZE];
		status = fs_getdetachedattr(ino,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
		} else {
			memset(&o_stbuf, 0, sizeof(struct stat));
			mfs_attr_to_stat(ino,attr,&o_stbuf);
			fuse_reply_attr(req, &o_stbuf, attr_cache_timeout);
		}
	}
}

void mfs_meta_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	(void)to_set;
	(void)stbuf;
	mfs_meta_getattr(req,ino,fi);
}

void mfs_meta_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	int status;
	uint32_t inode;
	if (!(parent==META_TRASH_INODE || (parent>=META_SUBTRASH_INODE_MIN && parent<=META_SUBTRASH_INODE_MAX))) {
		fuse_reply_err(req,EACCES);
		return;
	}
	inode = mfs_meta_name_to_inode(name);
	if (inode==0) {
		fuse_reply_err(req,ENOENT);
		return;
	}
	status = fs_purge(inode);
	status = mfs_errorconv(status);
//	if (status!=0) {
	fuse_reply_err(req, status);
//	} else {
//		fuse_reply_err(req,0);
//	}
}

#if FUSE_VERSION >= 30
void mfs_meta_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname,unsigned int flags) {
#else
void mfs_meta_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
#endif
	int status;
	uint32_t inode;
	(void)newname;
#if FUSE_VERSION >= 30
	(void)flags;
#endif
	if ((!(parent==META_TRASH_INODE || (parent>=META_SUBTRASH_INODE_MIN && parent<=META_SUBTRASH_INODE_MAX))) && newparent!=META_UNDEL_INODE) {
		fuse_reply_err(req,EACCES);
		return;
	}
	inode = mfs_meta_name_to_inode(name);
	if (inode==0) {
		fuse_reply_err(req,ENOENT);
		return;
	}
	status = fs_undel(inode);
	status = mfs_errorconv(status);
//	if (status!=0) {
	fuse_reply_err(req, status);
//	} else {
//		fuse_reply_err(req,0);
//	}
}

/*
static void dirbuf_add(dirbuf *b, const char *name, fuse_ino_t ino, uint8_t attr[32]) {
	struct stat stbuf;
	size_t oldsize = b->size;
	b->size += fuse_dirent_size(strlen(name));
	b->p = (char *) realloc(b->p, b->size);
	mfs_attr_to_stat(ino,attr,&stbuf);
	fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

static void dirbuf_meta_add(dirbuf *b, const char *name, fuse_ino_t ino) {
	struct stat stbuf;
	size_t oldsize = b->size;
	b->size += fuse_dirent_size(strlen(name));
	b->p = (char *) realloc(b->p, b->size);
	memset(&stbuf,0,sizeof(struct stat));
	mfs_meta_stat(ino,&stbuf);
	fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}
*/

static uint32_t dir_metaentries_size(uint32_t ino) {
	switch (ino) {
	case META_ROOT_INODE:
		return 4*6+1+2+strlen(META_TRASH_NAME)+strlen(META_SUSTAINED_NAME);
	case META_TRASH_INODE:
		if (master_version()>=VERSION2INT(3,0,64) && flat_trash==0) {
			return (3+TRASH_BUCKETS)*6+1+2+strlen(META_UNDEL_NAME)+(TRASH_BUCKETS*((TRASH_BUCKETS<=4096)?3:4));
		} else {
			return 3*6+1+2+strlen(META_UNDEL_NAME);
		}
	case META_UNDEL_INODE:
		return 2*6+1+2;
	case META_SUSTAINED_INODE:
		return 2*6+1+2;
	default:
		if (ino>=META_SUBTRASH_INODE_MIN && ino<=META_SUBTRASH_INODE_MAX) {
			return 3*6+1+2+strlen(META_UNDEL_NAME);
		}
	}
	return 0;
}

static void dir_metaentries_fill(uint8_t *buff,uint32_t ino) {
	uint8_t l;
	switch (ino) {
	case META_ROOT_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// trash
		l = strlen(META_TRASH_NAME);
		put8bit(&buff,l);
		memcpy(buff,META_TRASH_NAME,l);
		buff+=l;
		put32bit(&buff,META_TRASH_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// sustained
		l = strlen(META_SUSTAINED_NAME);
		put8bit(&buff,l);
		memcpy(buff,META_SUSTAINED_NAME,l);
		buff+=l;
		put32bit(&buff,META_SUSTAINED_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		return;
	case META_TRASH_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_TRASH_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// undel
		l = strlen(META_UNDEL_NAME);
		put8bit(&buff,l);
		memcpy(buff,META_UNDEL_NAME,l);
		buff+=l;
		put32bit(&buff,META_UNDEL_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		if (master_version()>=VERSION2INT(3,0,64) && flat_trash==0) {
			uint32_t tid;
			for (tid=0 ; tid<TRASH_BUCKETS ; tid++) {
				if (TRASH_BUCKETS>4096) {
					put8bit(&buff,4);
					put8bit(&buff,"0123456789ABCDEF"[(tid>>12)&15]);
				} else {
					put8bit(&buff,3);
				}
				put8bit(&buff,"0123456789ABCDEF"[(tid>>8)&15]);
				put8bit(&buff,"0123456789ABCDEF"[(tid>>4)&15]);
				put8bit(&buff,"0123456789ABCDEF"[tid&15]);
				put32bit(&buff,tid+META_SUBTRASH_INODE_MIN);
				put8bit(&buff,TYPE_DIRECTORY);
			}
		}
		return;
	case META_UNDEL_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_UNDEL_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_TRASH_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		return;
	case META_SUSTAINED_INODE:
		// .
		put8bit(&buff,1);
		put8bit(&buff,'.');
		put32bit(&buff,META_SUSTAINED_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		// ..
		put8bit(&buff,2);
		put8bit(&buff,'.');
		put8bit(&buff,'.');
		put32bit(&buff,META_ROOT_INODE);
		put8bit(&buff,TYPE_DIRECTORY);
		return;
	default:
		if (ino>=META_SUBTRASH_INODE_MIN && ino<=META_SUBTRASH_INODE_MAX) {
			// .
			put8bit(&buff,1);
			put8bit(&buff,'.');
			put32bit(&buff,META_TRASH_INODE);
			put8bit(&buff,TYPE_DIRECTORY);
			// ..
			put8bit(&buff,2);
			put8bit(&buff,'.');
			put8bit(&buff,'.');
			put32bit(&buff,META_ROOT_INODE);
			put8bit(&buff,TYPE_DIRECTORY);
			// undel
			l = strlen(META_UNDEL_NAME);
			put8bit(&buff,l);
			memcpy(buff,META_UNDEL_NAME,l);
			buff+=l;
			put32bit(&buff,META_UNDEL_INODE);
			put8bit(&buff,TYPE_DIRECTORY);
			return;
		}
	}
}

static uint32_t dir_dataentries_size(const uint8_t *dbuff,uint32_t dsize) {
	uint8_t nleng;
	uint32_t eleng;
	const uint8_t *eptr;
	eleng=0;
	if (dbuff==NULL || dsize==0) {
		return 0;
	}
	eptr = dbuff+dsize;
	while (dbuff<eptr) {
		nleng = dbuff[0];
		dbuff+=5+nleng;
		if (nleng>255-9) {
			eleng+=6+255;
		} else {
			eleng+=6+nleng+9;
		}
	}
	return eleng;
}

static void dir_dataentries_convert(uint8_t *buff,const uint8_t *dbuff,uint32_t dsize) {
	const char *name;
	uint32_t inode;
	uint8_t nleng;
	uint8_t inoleng;
	const uint8_t *eptr;
	eptr = dbuff+dsize;
	while (dbuff<eptr) {
		nleng = dbuff[0];
		if (dbuff+nleng+5<=eptr) {
			dbuff++;
			if (nleng>255-9) {
				inoleng = 255;
			} else {
				inoleng = nleng+9;
			}
			put8bit(&buff,inoleng);
			name = (const char*)dbuff;
			dbuff+=nleng;
			inode = get32bit(&dbuff);
			sprintf((char*)buff,"%08"PRIX32"|",inode);
			if (nleng>255-9) {
				memcpy(buff+9,name,255-9);
				buff+=255;
			} else {
				memcpy(buff+9,name,nleng);
				buff+=9+nleng;
			}
			put32bit(&buff,inode);
			put8bit(&buff,TYPE_FILE);
		} else {
			syslog(LOG_WARNING,"dir data malformed (trash)");
			dbuff=eptr;
		}
	}
}


static void dirbuf_meta_fill(dirbuf *b, uint32_t ino) {
	int status;
	uint32_t msize,dcsize;
	const uint8_t *dbuff;
	uint32_t dsize;

	b->p = NULL;
	b->size = 0;
	msize = dir_metaentries_size(ino);
	if (ino==META_TRASH_INODE && (master_version()<VERSION2INT(3,0,64) || flat_trash)) {
		status = fs_gettrash(0xFFFFFFFF,&dbuff,&dsize);
		if (status!=MFS_STATUS_OK) {
			return;
		}
		dcsize = dir_dataentries_size(dbuff,dsize);
	} else if (ino==META_SUSTAINED_INODE) {
		status = fs_getsustained(&dbuff,&dsize);
		if (status!=MFS_STATUS_OK) {
			return;
		}
		dcsize = dir_dataentries_size(dbuff,dsize);
	} else if (ino>=META_SUBTRASH_INODE_MIN && ino<=META_SUBTRASH_INODE_MAX && master_version()>=VERSION2INT(3,0,64) && flat_trash==0) {
		status = fs_gettrash(ino-META_SUBTRASH_INODE_MIN,&dbuff,&dsize);
		if (status!=MFS_STATUS_OK) {
			return;
		}
		dcsize = dir_dataentries_size(dbuff,dsize);
	} else {
		dcsize = 0;
	}
	if (msize+dcsize==0) {
		return;
	}
	b->p = malloc(msize+dcsize);
	if (b->p==NULL) {
		syslog(LOG_WARNING,"out of memory");
		return;
	}
	if (msize>0) {
		dir_metaentries_fill(b->p,ino);
	}
	if (dcsize>0) {
		dir_dataentries_convert(b->p+msize,dbuff,dsize);
	}
	b->size = msize+dcsize;
}

void mfs_meta_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	dirbuf *dirinfo;
	if (ino==META_ROOT_INODE || ino==META_TRASH_INODE || ino==META_UNDEL_INODE || ino==META_SUSTAINED_INODE || (ino>=META_SUBTRASH_INODE_MIN && ino<=META_SUBTRASH_INODE_MAX)) {
		dirinfo = malloc(sizeof(dirbuf));
		pthread_mutex_init(&(dirinfo->lock),NULL);
		dirinfo->p = NULL;
		dirinfo->size = 0;
		dirinfo->wasread = 0;
		fi->fh = (unsigned long)dirinfo;
		if (fuse_reply_open(req,fi) == -ENOENT) {
			fi->fh = 0;
			pthread_mutex_destroy(&(dirinfo->lock));
			free(dirinfo->p);
			free(dirinfo);
		}
	} else {
		fuse_reply_err(req, ENOTDIR);
	}
}

void mfs_meta_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	char buffer[READDIR_BUFFSIZE];
	char *name,c;
	const uint8_t *ptr,*eptr;
	uint8_t end;
	size_t opos,oleng;
	uint8_t nleng;
	uint32_t inode;
	uint8_t type;
	struct stat stbuf;

	if (off<0) {
		fuse_reply_err(req,EINVAL);
		return;
	}
	pthread_mutex_lock(&(dirinfo->lock));
	if (dirinfo->wasread==0 || (dirinfo->wasread==1 && off==0)) {
		if (dirinfo->p!=NULL) {
			free(dirinfo->p);
		}
		dirbuf_meta_fill(dirinfo,ino);
//		syslog(LOG_WARNING,"inode: %lu , dirinfo->p: %p , dirinfo->size: %lu",(unsigned long)ino,dirinfo->p,(unsigned long)dirinfo->size);
	}
	dirinfo->wasread=1;

	if (off>=(off_t)(dirinfo->size)) {
		fuse_reply_buf(req, NULL, 0);
	} else {
		if (size>READDIR_BUFFSIZE) {
			size=READDIR_BUFFSIZE;
		}
		ptr = (const uint8_t*)(dirinfo->p)+off;
		eptr = (const uint8_t*)(dirinfo->p)+dirinfo->size;
		opos = 0;
		end = 0;

		while (ptr<eptr && end==0) {
			nleng = ptr[0];
			ptr++;
			name = (char*)ptr;
			ptr+=nleng;
			off+=nleng+6;
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				type = get8bit(&ptr);
				mfs_meta_type_to_stat(inode,type,&stbuf);
				c = name[nleng];
				name[nleng]=0;
				oleng = fuse_add_direntry(req, buffer + opos, size - opos, name, &stbuf, off);
				name[nleng] = c;
				if (opos+oleng>size) {
					end=1;
				} else {
					opos+=oleng;
				}
			}
		}
		fuse_reply_buf(req,buffer,opos);
	}
	pthread_mutex_unlock(&(dirinfo->lock));
}

void mfs_meta_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	(void)ino;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	pthread_mutex_lock(&(dirinfo->lock));
	pthread_mutex_unlock(&(dirinfo->lock));
	pthread_mutex_destroy(&(dirinfo->lock));
	free(dirinfo->p);
	free(dirinfo);
	fi->fh = 0;
	fuse_reply_err(req,0);
}

void mfs_meta_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	pathbuf *pathinfo;
	const uint8_t *path;
	//size_t pleng;
	int status;
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
		fi->fh = 0;
		fi->direct_io = 0;
		fi->keep_cache = 1;
		fuse_reply_open(req, fi);
		return;
	}

	if (IS_SPECIAL_INODE(ino)) {
		fuse_reply_err(req, EACCES);
	} else {
		status = fs_gettrashpath(ino,&path);
		status = mfs_errorconv(status);
		if (status!=0) {
			fuse_reply_err(req, status);
		} else {
			pathinfo = malloc(sizeof(pathbuf));
			pthread_mutex_init(&(pathinfo->lock),NULL);
			pathinfo->changed = 0;
			pathinfo->size = strlen((char*)path)+1;
			pathinfo->p = malloc(pathinfo->size);
			memcpy(pathinfo->p,path,pathinfo->size-1);
			pathinfo->p[pathinfo->size-1]='\n';
			fi->direct_io = 1;
			fi->fh = (unsigned long)pathinfo;
			if (fuse_reply_open(req,fi) == -ENOENT) {
				fi->fh = 0;
				pthread_mutex_destroy(&(pathinfo->lock));
				free(pathinfo->p);
				free(pathinfo);
			}
		}
	}
}

void mfs_meta_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	if (ino==MASTERINFO_INODE) {
		fuse_reply_err(req,0);
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
	pathbuf *pathinfo = (pathbuf *)((unsigned long)(fi->fh));
	pthread_mutex_lock(&(pathinfo->lock));
	if (pathinfo->changed) {
		if (pathinfo->p[pathinfo->size-1]=='\n') {
			pathinfo->p[pathinfo->size-1]=0;
		} else {
			pathinfo->p = realloc(pathinfo->p,pathinfo->size+1);
			pathinfo->p[pathinfo->size]=0;
		}
		fs_settrashpath(ino,(uint8_t*)pathinfo->p);
	}
	pthread_mutex_unlock(&(pathinfo->lock));
	pthread_mutex_destroy(&(pathinfo->lock));
	free(pathinfo->p);
	free(pathinfo);
	fi->fh = 0;
	fuse_reply_err(req,0);
}

void mfs_meta_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	pathbuf *pathinfo = (pathbuf *)((unsigned long)(fi->fh));
	if (ino==MASTERINFO_INODE) {
		uint8_t masterinfo[14];
		fs_getmasterlocation(masterinfo);
		masterproxy_getlocation(masterinfo);
#ifdef MASTERINFO_WITH_VERSION
		if (off>=14) {
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>14) {
			fuse_reply_buf(req,(char*)(masterinfo+off),14-off);
#else
		if (off>=10) {
			fuse_reply_buf(req,NULL,0);
		} else if (off+size>10) {
			fuse_reply_buf(req,(char*)(masterinfo+off),10-off);
#endif
		} else {
			fuse_reply_buf(req,(char*)(masterinfo+off),size);
		}
		return;
	}
	if (pathinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		if (masterinfo->sent) {
//			int rsize;
//			uint8_t *buff;
//			buff = malloc(size);
//			rsize = fs_direct_read(masterinfo->sd,buff,size);
//			fuse_reply_buf(req,(char*)buff,rsize);
			//syslog(LOG_WARNING,"master received: %d/%u",rsize,size);
//			free(buff);
//		} else {
//			syslog(LOG_WARNING,"master: read before write");
//			fuse_reply_buf(req,NULL,0);
//		}
//		return;
//	}
	pthread_mutex_lock(&(pathinfo->lock));
	if (off<0) {
		pthread_mutex_unlock(&(pathinfo->lock));
		fuse_reply_err(req,EINVAL);
		return;
	}
	if ((size_t)off>pathinfo->size) {
		fuse_reply_buf(req, NULL, 0);
	} else if (off + size > pathinfo->size) {
		fuse_reply_buf(req, (pathinfo->p)+off,(pathinfo->size)-off);
	} else {
		fuse_reply_buf(req, (pathinfo->p)+off,size);
	}
	pthread_mutex_unlock(&(pathinfo->lock));
}

void mfs_meta_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	pathbuf *pathinfo = (pathbuf *)((unsigned long)(fi->fh));
	if (ino==MASTERINFO_INODE) {
		fuse_reply_err(req,EACCES);
		return;
	}
	if (pathinfo==NULL) {
		fuse_reply_err(req,EBADF);
		return;
	}
//	if (ino==MASTER_INODE) {
//		minfo *masterinfo = (minfo*)(unsigned long)(fi->fh);
//		int wsize;
//		masterinfo->sent=1;
//		wsize = fs_direct_write(masterinfo->sd,(const uint8_t*)buf,size);
		//syslog(LOG_WARNING,"master sent: %d/%u",wsize,size);
//		fuse_reply_write(req,wsize);
//		return;
//	}
	if (off + size > PATH_SIZE_LIMIT) {
		fuse_reply_err(req,EINVAL);
		return;
	}
	pthread_mutex_lock(&(pathinfo->lock));
	if (pathinfo->changed==0) {
		pathinfo->size = 0;
	}
	if (off+size > pathinfo->size) {
		size_t s = pathinfo->size;
		pathinfo->p = realloc(pathinfo->p,off+size);
		pathinfo->size = off+size;
		memset(pathinfo->p+s,0,off+size-s);
	}
	memcpy((pathinfo->p)+off,buf,size);
	pathinfo->changed = 1;
	pthread_mutex_unlock(&(pathinfo->lock));
	fuse_reply_write(req,size);
}

void mfs_meta_init(int debug_mode_in,double entry_cache_timeout_in,double attr_cache_timeout_in,int flat_trash_in) {
	debug_mode = debug_mode_in;
	entry_cache_timeout = entry_cache_timeout_in;
	attr_cache_timeout = attr_cache_timeout_in;
	flat_trash = flat_trash_in;
	if (debug_mode) {
		fprintf(stderr,"cache parameters: entry_cache_timeout=%.2lf attr_cache_timeout=%.2lf\n",entry_cache_timeout,attr_cache_timeout);
		if (flat_trash) {
			fprintf(stderr,"force using 'flat' trash\n");
		}
	}
}
