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

#ifndef _CRC_H_
#define _CRC_H_
#include <inttypes.h>

uint32_t mycrc32(uint32_t crc,const uint8_t *block,uint32_t leng);
uint32_t mycrc32_combine(uint32_t crc1, uint32_t crc2, uint32_t leng2);
#define mycrc32_zeroblock(crc,zeros) mycrc32_combine((crc)^0xFFFFFFFF,0xFFFFFFFF,(zeros))
#define mycrc32_zeroexpanded(crc,block,leng,zeros) mycrc32_zeroblock(mycrc32((crc),(block),(leng)),(zeros))
#define mycrc32_xorblocks(crc,crcblock1,crcblock2,leng) ((crcblock1)^(crcblock2)^mycrc32_zeroblock(crc,leng))

void mycrc32_init(void);

#endif
