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

#ifndef _MD5_H_
#define _MD5_H_

#include <inttypes.h>

typedef struct _md5ctx {
	uint32_t state[4];
	uint32_t count[2];
	uint8_t buffer[64];
} md5ctx;

void md5_init(md5ctx *ctx);
void md5_update(md5ctx *ctx,const uint8_t *buff,uint32_t leng);
void md5_final(uint8_t digest[16],md5ctx *ctx);

#endif
