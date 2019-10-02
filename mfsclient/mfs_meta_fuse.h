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

#ifndef _MFS_META_FUSE_H_
#define _MFS_META_FUSE_H_

#include "fusecommon.h"

#if FUSE_USE_VERSION >= 26
void mfs_meta_statfs(fuse_req_t req, fuse_ino_t ino);
#else
void mfs_meta_statfs(fuse_req_t req);
#endif
//void mfs_meta_access(fuse_req_t req, fuse_ino_t ino, int mask);
void mfs_meta_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
void mfs_meta_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi);
void mfs_meta_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
#if FUSE_VERSION >= 30
void mfs_meta_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname,unsigned int flags);
#else
void mfs_meta_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);
#endif
void mfs_meta_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_meta_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
void mfs_meta_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_meta_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
void mfs_meta_init(int debug_mode_in,double entry_cache_timeout_in,double attr_cache_timeout_in,int flat_trash_in);

#endif
