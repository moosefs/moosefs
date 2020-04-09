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

#ifndef _MASTERCONN_H_
#define _MASTERCONN_H_

#include <inttypes.h>

void masterconn_stats(uint64_t *bin,uint64_t *bout);
uint16_t masterconn_getcsid(void);
uint64_t masterconn_getmetaid(void);
uint64_t masterconn_gethddmetaid();
void masterconn_sethddmetaid(uint64_t metaid);
uint32_t masterconn_getmasterip(void);
uint16_t masterconn_getmasterport(void);
// void masterconn_replicate_status(uint64_t chunkid,uint32_t version,uint8_t status);
// void masterconn_send_chunk_damaged(uint64_t chunkid);
// void masterconn_send_chunk_lost(uint64_t chunkid);
// void masterconn_send_error_occurred();
// void masterconn_send_space(uint64_t usedspace,uint64_t totalspace,uint32_t chunkcount,uint64_t tdusedspace,uint64_t tdtotalspace,uint32_t tdchunkcount);
void masterconn_heavyload(uint32_t load,uint8_t hlstatus);
void masterconn_forcereconnect(void);
int masterconn_init(void);

#endif
