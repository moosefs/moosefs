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

#ifndef _POSIXLOCKS_H_
#define _POSIXLOCKS_H_

#include <inttypes.h>
#include "bio.h"

uint8_t posix_lock_cmd(uint32_t sessionid,uint32_t msgid,uint32_t reqid,uint32_t inode,uint64_t owner,uint8_t op,uint8_t *ltype,uint64_t *start,uint64_t *end,uint32_t *pid);
void posix_lock_file_closed(uint32_t sessionid,uint32_t inode);
uint32_t posix_lock_list(uint32_t inode,uint8_t *buff);

uint8_t posix_lock_mr_change(uint32_t inode,uint32_t sessionid,uint64_t owner,char cmd,uint64_t start,uint64_t end,uint32_t pid);

uint8_t posix_lock_store(bio *fd);
int posix_lock_load(bio *fd,uint8_t mver,uint8_t ignoreflag);
void posix_lock_cleanup(void);

int posix_lock_init(void);

#endif
