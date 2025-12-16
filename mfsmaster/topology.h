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

#ifndef _TOPOLOGY_H_
#define _TOPOLOGY_H_

#include <inttypes.h>

#define TOPOLOGY_DIST_SAME_IP 0
#define TOPOLOGY_DIST_SAME_RACKID 1
#define TOPOLOGY_DIST_MAX 2

uint32_t topology_get_rackid(uint32_t ip);
uint8_t topology_distance(uint32_t ip1,uint32_t ip2);
int topology_init(void);

#endif
