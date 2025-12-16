/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifndef _DENTRY_INVALIDATOR_H_
#define _DENTRY_INVALIDATOR_H_

#include <inttypes.h>

void dinval_add(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t inode);
void dinval_remove(uint32_t parent,uint8_t nleng,const uint8_t *name);
void dinval_init(double timeout);

#endif
