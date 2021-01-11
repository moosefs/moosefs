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

#ifndef _CSORDER_H_
#define _CSORDER_H_

typedef struct _cspri {
	uint32_t ip;
	uint16_t port;
	uint32_t version;
	uint32_t labelmask;
	uint32_t priority;
} cspri;

int csorder_init(char *labelexpr);
uint8_t csorder_calc(uint32_t labelmask);
uint32_t csorder_sort(cspri chain[100],uint8_t csdataver,const uint8_t *csdata,uint32_t csdatasize,uint8_t writeflag);

#endif
