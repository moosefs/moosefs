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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifndef _FDCACHE_H_
#define _FDCACHE_H_

#include <inttypes.h>

#include "fusecommon.h"

void fdcache_insert(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t lflags,uint8_t csdataver,uint64_t chunkid,uint32_t version,const uint8_t *csdata,uint32_t csdatasize);
uint8_t fdcache_find(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t *lflags);
void* fdcache_acquire(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35],uint16_t *lflags);
void fdcache_release(void *vfdce);
void fdcache_invalidate(uint32_t inode);
void fdcache_inject_chunkdata(void *vfdce);
void fdcache_init(void);

#endif
