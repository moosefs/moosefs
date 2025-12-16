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

#ifndef _RC4RANDOM_H_
#define _RC4RANDOM_H_

#include <inttypes.h>

int rnd_init(void);

uint8_t rndu8(void);
uint32_t rndu32(void);
uint64_t rndu64(void);

void rndbuff(uint8_t *buff,uint32_t size);

uint64_t rndu64_ranged(uint64_t range);
uint32_t rndu32_ranged(uint32_t range);

#endif
