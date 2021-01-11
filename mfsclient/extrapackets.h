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

#ifndef _EXTRAPACKETS_H_
#define _EXTRAPACKETS_H_

#include <inttypes.h>

void ep_chunk_has_changed(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version,uint64_t fleng,uint8_t truncflag);
void ep_fleng_has_changed(uint32_t inode,uint64_t fleng);
void ep_term(void);
void ep_init(void);

#endif
