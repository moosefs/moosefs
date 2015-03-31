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

#ifndef _CHUNKLOC_CACHE_H_
#define _CHUNKLOC_CACHE_H_

#include <inttypes.h>

void chunkloc_cache_insert(uint32_t inode,uint32_t pos,uint64_t chunkid,uint32_t chunkversion,uint8_t csdatasize,const uint8_t *csdata);
int chunkloc_cache_search(uint32_t inode,uint32_t pos,uint64_t *chunkid,uint32_t *chunkversion,uint8_t *csdatasize,const uint8_t **csdata);
void chunkloc_cache_init(void);
void chunkloc_cache_term(void);

#endif
