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

#ifndef _OPENEDFILES_H_
#define _OPENEDFILES_H_

#include <inttypes.h>
#include "bio.h"

uint8_t of_checknode(uint32_t sessionid,uint32_t inode);
void of_openfile(uint32_t sessionid,uint32_t inode);
void of_sync(uint32_t sessionid,uint32_t *inode,uint32_t inodecnt);
void of_session_removed(uint32_t sessionid);
uint8_t of_isfileopened_by_session(uint32_t inode,uint32_t sessionid);
uint8_t of_isfileopen(uint32_t inode);
uint32_t of_noofopenedfiles(uint32_t sessionid);
uint32_t of_lsof(uint32_t sessionid,uint8_t *buff);

int of_mr_acquire(uint32_t sessionid,uint32_t inode);
int of_mr_release(uint32_t sessionid,uint32_t inode);

uint8_t of_store(bio *fd);
int of_load(bio *fd,uint8_t mver);
void of_cleanup(void);
int of_init(void);

#endif
