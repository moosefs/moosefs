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

#ifndef _ITREE_H_
#define _ITREE_H_

#include <inttypes.h>

void* itree_rebalance(void *o);
void* itree_add_interval(void *o,uint32_t f,uint32_t t,uint32_t id);
uint32_t itree_find(void *o,uint32_t v);
void itree_freeall(void *o);

// void itree_show(void *o);

#endif
