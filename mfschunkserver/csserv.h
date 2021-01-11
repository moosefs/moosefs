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

#ifndef _CSSERV_H_
#define _CSSERV_H_

#include <inttypes.h>

void csserv_stats(uint64_t *bin,uint64_t *bout);
// void csserv_cstocs_connected(void *e,void *cptr);
// void csserv_cstocs_gotstatus(void *e,uint64_t chunkid,uint32_t writeid,uint8_t s);
// void csserv_cstocs_disconnected(void *e);
uint32_t csserv_getlistenip();
uint16_t csserv_getlistenport();
int csserv_init(void);

#endif
