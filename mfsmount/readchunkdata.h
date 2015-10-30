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

#ifndef _READCHUNKDATA_H_
#define _READCHUNKDATA_H_

#include <inttypes.h>

#include "csorder.h"

void read_chunkdata_init (void);
void read_chunkdata_term (void);
void read_chunkdata_invalidate (uint32_t inode,uint32_t chindx);
void read_chunkdata_inject (uint32_t inode,uint32_t chindx,uint64_t mfleng,uint64_t chunkid,uint32_t version,uint8_t csdataver,uint8_t *csdata,uint32_t csdatasize);
uint8_t read_chunkdata_check(uint32_t inode,uint32_t chindx,uint64_t mfleng,uint64_t chunkid,uint32_t version);
uint8_t read_chunkdata_get(uint32_t inode,uint8_t *canmodatime,cspri chain[100],uint16_t *chainelements,uint32_t chindx,uint64_t *mfleng,uint64_t *chunkid,uint32_t *version);

#endif
