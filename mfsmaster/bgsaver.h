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

#ifndef _BGSAVER_H_
#define _BGSAVER_H_

#include <inttypes.h>

void bgsaver_cancel(void);
void bgsaver_open(uint32_t speedlimit,void *ud,void (*donefn)(void*,int));
void bgsaver_store(const uint8_t *data,uint64_t offset,uint32_t leng,uint32_t crc,void *ud,void (*donefn)(void*,int));
void bgsaver_close(void *ud,void (*donefn)(void*,int));
void bgsaver_changelog(uint64_t version,const char *message);
void bgsaver_rotatelog(void);
int bgsaver_init(void);

#endif
