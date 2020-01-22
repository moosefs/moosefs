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

#ifndef _CHANGELOG_H_
#define _CHANGELOG_H_

#include <inttypes.h>

uint32_t changelog_get_old_changes(uint64_t version,void (*sendfn)(void *,uint64_t,uint8_t *,uint32_t),void *userdata,uint32_t limit);
uint64_t changelog_get_minversion(void);

void changelog_rotate(void);
void changelog_mr(uint64_t version,const char *data);

#ifdef __printflike
void changelog(const char *format,...) __printflike(1, 2);
#else
void changelog(const char *format,...);
#endif

char* changelog_generate_gids(uint32_t gids,uint32_t *gid);
char* changelog_escape_name(uint32_t nleng,const uint8_t *name);
int changelog_init(void);

uint64_t changelog_findfirstversion(const char *fname);
uint64_t changelog_findlastversion(const char *fname);
int changelog_checkname(const char *fname);

#endif
