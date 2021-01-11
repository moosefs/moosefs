/*
 * Copyright (C) 2021 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifndef _DIRATTRCACHE_H_
#define _DIRATTRCACHE_H_

#include "fusecommon.h"

#include <inttypes.h>
#include "MFSCommunication.h"

void* dcache_new(const struct fuse_ctx *ctx,uint32_t parent,const uint8_t *dbuff,uint32_t dsize,uint8_t attrsize);
void dcache_release(void *r);
uint8_t dcache_lookup(const struct fuse_ctx *ctx,uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t dcache_getattr(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[ATTR_RECORD_SIZE]);
void dcache_setattr(uint32_t inode,const uint8_t attr[ATTR_RECORD_SIZE]);
void dcache_invalidate_attr(uint32_t inode);
void dcache_invalidate_name(const struct fuse_ctx *ctx,uint32_t parent,uint8_t nleng,const uint8_t *name);

#endif
