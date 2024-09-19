/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
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

#ifndef _PATTERNS_H_
#define _PATTERNS_H_

#include <inttypes.h>

#include "bio.h"

void patterns_sclass_delete(uint8_t scid);
uint8_t patterns_find_matching(uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t name[256],uint8_t *scid,uint16_t *trashretention,uint8_t *seteattr,uint8_t *clreattr);

uint8_t patterns_add(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid,uint8_t priority,uint8_t omask,uint8_t scnleng,const uint8_t *scname,uint16_t trashretention,uint8_t seteattr,uint8_t clreattr);
uint8_t patterns_delete(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid);
uint32_t patterns_list(uint8_t *buff);

uint8_t patterns_mr_add(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid,uint8_t priority,uint8_t omask,uint8_t scid,uint16_t trashretention,uint8_t seteattr,uint8_t clreattr);
uint8_t patterns_mr_delete(uint8_t gnleng,const uint8_t *gname,uint32_t euid,uint32_t egid);

void patterns_cleanup(void);
uint8_t patterns_store(bio *fd);
int patterns_load(bio *fd,uint8_t mver,int ignoreflag);

int patterns_init(void);

#endif
