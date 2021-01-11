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

#ifndef _WRITEDATA_H_
#define _WRITEDATA_H_

#include <inttypes.h>

void write_data_init(uint32_t cachesize,uint32_t retries,uint32_t timeout,uint32_t minlogretry,uint8_t erronlostchunk,uint8_t erronnospace);
void write_data_term(void);
void* write_data_new(uint32_t inode,uint64_t fleng);
int write_data_will_end_wait(void *vid);
int write_data_end(void *vid);
int write_data_chunk_wait(void *vid);
int write_data_flush(void *vid);
void write_data_inode_setmaxfleng(uint32_t inode,uint64_t maxfleng);
uint64_t write_data_inode_getmaxfleng(uint32_t inode);
uint64_t write_data_getmaxfleng(void *vid);
int write_data_flush_inode(uint32_t inode);
int write_data(void *vid,uint64_t offset,uint32_t size,const uint8_t *buff,uint8_t superuser);
uint8_t write_cache_almost_full(void);
// uint64_t write_data_get_maxfleng(uint32_t inode);

#endif
