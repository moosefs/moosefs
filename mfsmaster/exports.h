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

#ifndef _EXPORTS_H_
#define _EXPORTS_H_

#include <inttypes.h>

uint32_t exports_info_size(uint8_t versmode);
void exports_info_data(uint8_t versmode,uint8_t *buff);
uint8_t exports_check(uint32_t ip,uint32_t version,const uint8_t *path,const uint8_t rndcode[32],const uint8_t passcode[16],uint8_t *sesflags,uint16_t *umaskval,uint32_t *rootuid,uint32_t *rootgid,uint32_t *mapalluid,uint32_t *mapallgid,uint8_t *mingoal,uint8_t *maxgoal,uint32_t *mintrashtime,uint32_t *maxtrashtime,uint32_t *disables);
void exports_reload(void);
uint64_t exports_checksum(void);
int exports_init(void);

#endif
