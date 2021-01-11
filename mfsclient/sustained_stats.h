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

#ifndef _SUSTAINED_STATS_H_
#define _SUSTAINED_STATS_H_

#include <inttypes.h>

//void sstats_activate(uint32_t inode);
//void sstats_deactivate(uint32_t inode);
int sstats_get(uint32_t inode,uint8_t attr[35],uint8_t forceok);
void sstats_set(uint32_t inode,const uint8_t attr[35],uint8_t createflag);
void sstats_term(void);
int sstats_init(void);

#endif
