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

#ifndef _READDATA_H_
#define _READDATA_H_

#ifdef WIN32
#include "portable.h"
#else
#ifdef HAVE_READV
#include <sys/uio.h>
#else
struct iovec {
	char *iov_base;
	size_t iov_len;
};
#endif
#endif
#include <inttypes.h>

void read_data_init (uint64_t readaheadsize,uint32_t readaheadleng,uint32_t readaheadtrigger,uint32_t retries,uint32_t timeout,uint32_t minlogretry,uint8_t erronlostchunk,uint8_t erronnospace);
void read_data_term(void);
int read_data(void *vid, uint64_t offset, uint32_t *size, void **rhead,struct iovec **iov,uint32_t *iovcnt);
void read_data_free_buff(void *vid,void *vrhead,struct iovec *iov);
void read_inode_clear_cache(uint32_t inode,uint64_t offset,uint64_t leng);
void read_inode_set_length_active(uint32_t inode,uint64_t newlength);
void read_inode_set_length_passive(uint32_t inode,uint64_t newlength);
void* read_data_new(uint32_t inode,uint64_t fleng);
void read_data_end(void *vid);

#endif
