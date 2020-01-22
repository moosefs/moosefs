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

#ifndef _CSDB_H_
#define _CSDB_H_

#include <inttypes.h>

void csdb_init(void);
void csdb_term(void);
uint32_t csdb_getreadcnt(uint32_t ip,uint16_t port);
uint32_t csdb_getwritecnt(uint32_t ip,uint16_t port);
uint32_t csdb_getopcnt(uint32_t ip,uint16_t port);
void csdb_readinc(uint32_t ip,uint16_t port);
void csdb_readdec(uint32_t ip,uint16_t port);
void csdb_writeinc(uint32_t ip,uint16_t port);
void csdb_writedec(uint32_t ip,uint16_t port);

#endif
