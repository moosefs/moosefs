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

#ifndef _CUCKOOHASH_H_
#define _CUCKOOHASH_H_

#include <inttypes.h>

typedef uint64_t hash_key_t;

void* chash_new(void);
void* chash_find(void *h,hash_key_t x);
void chash_delete(void *h,hash_key_t x);
void chash_add(void *h,hash_key_t x,void *v);
void chash_erase(void *h);
void chash_free(void *h);
uint32_t chash_get_elemcount(void *h);
uint32_t chash_get_size(void *h);

#endif
