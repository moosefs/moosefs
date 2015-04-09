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

#ifndef _READDATA_H_
#define _READDATA_H_

#include <sys/uio.h>
#include <inttypes.h>

void read_data_init (uint64_t readaheadsize,uint32_t readaheadleng,uint32_t readaheadtrigger,uint32_t retries);
void read_data_term(void);
int read_data(void *vid, uint64_t offset, uint32_t *size, void **rhead,struct iovec **iov,uint32_t *iovcnt);
void read_data_free_buff(void *vid,void *vrhead,struct iovec *iov);
void read_inode_dirty_region(uint32_t inode,uint64_t offset,uint32_t size,const char *buff);
void read_inode_set_length(uint32_t inode,uint64_t newlength,uint8_t active);
void* read_data_new(uint32_t inode);
void read_data_end(void *vid);

#endif
