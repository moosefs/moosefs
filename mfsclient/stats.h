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

#ifndef _STATS_H_
#define _STATS_H_

#include <inttypes.h>

void stats_counter_add(void *node,uint64_t delta);
void stats_counter_sub(void *node,uint64_t delta);
void stats_counter_inc(void *node);
void stats_counter_dec(void *node);
void stats_counter_set(void *node,uint64_t value);
void* stats_get_subnode(void *node,const char *name,uint8_t absolute,uint8_t printflag);
// uint64_t* stats_get_counterptr(void *node);
void stats_reset_all(void);
void stats_show_all(char **buff,uint32_t *leng);
void stats_term(void);

#endif
