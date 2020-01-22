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

#ifndef _FLOCKLOCKS_H_
#define _FLOCKLOCKS_H_

#include <inttypes.h>
#include "bio.h"

uint8_t flock_locks_cmd(uint32_t sessionid,uint32_t message_id,uint32_t req_id,uint32_t inode,uint64_t lock_owner,uint8_t op);
void flock_file_closed(uint32_t sessionid,uint32_t inode);
uint32_t flock_list(uint32_t inode,uint8_t *buff);

uint8_t flock_mr_change(uint32_t inode,uint32_t sessionid,uint64_t owner,char cmd);

uint8_t flock_store(bio *fd);
int flock_load(bio *fd,uint8_t mver,uint8_t ignoreflag);
void flock_cleanup(void);

int flock_init(void);

#endif
