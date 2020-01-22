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

#ifndef _CHUNKSDATACACHE_H_
#define _CHUNKSDATACACHE_H_

#include <inttypes.h>

void chunksdatacache_clear_inode(uint32_t inode,uint32_t chindx);
void chunksdatacache_invalidate(uint32_t inode,uint32_t chindx);
uint8_t chunksdatacache_check(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version);
void chunksdatacache_change(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version);
void chunksdatacache_insert(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version,uint8_t csdataver,const uint8_t *csdata,uint32_t csdatasize);
uint8_t chunksdatacache_find(uint32_t inode,uint32_t chindx,uint64_t *chunkid,uint32_t *version,uint8_t *csdataver,uint8_t *csdata,uint32_t *csdatasize);
void chunksdatacache_cleanup(void);
void chunksdatacache_term(void);
void chunksdatacache_init(void);

#endif
