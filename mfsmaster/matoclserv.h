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

#ifndef _MATOCLSERV_H_
#define _MATOCLSERV_H_

#include <inttypes.h>

void matoclserv_stats(uint64_t stats[5]);

void matoclserv_chunk_unlocked(uint64_t chunkid,void *cptr);
void matoclserv_chunk_status(uint64_t chunkid,uint8_t status);
void matoclserv_fuse_flock_wake_up(uint32_t sessionid,uint32_t msgid,uint8_t status);
void matoclserv_fuse_posix_lock_wake_up(uint32_t sessionid,uint32_t msgid,uint8_t status);
void matoclserv_fuse_invalidate_chunk_cache(void);
int matoclserv_no_more_pending_jobs(void);
void matoclserv_disconnect_all(void);
void matoclserv_close_lsock(void);
int matoclserv_init(void);

#endif
