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

#ifndef _MAINSERV_H_
#define _MAINSERV_H_

void mainserv_stats(uint64_t *bin,uint64_t *bout,uint32_t *hlopr,uint32_t *hlopw);
uint8_t mainserv_read(int sock,const uint8_t *packet,uint32_t length);
uint8_t mainserv_write(int sock,const uint8_t *packet,uint32_t length);
// void mainserv_serve(int sock);
int mainserv_init(void);

#endif
