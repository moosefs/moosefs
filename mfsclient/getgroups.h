/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
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

#ifndef _GETGROUPS_H_
#define _GETGROUPS_H_

#include <sys/types.h>
#include <inttypes.h>

typedef struct groups {
	uint32_t lcnt;
	uint32_t gidcnt;
	uint32_t *gidtab;
} groups;

groups* groups_get_common(pid_t pid,uid_t uid,gid_t gid,uint8_t cacheonly);
#define groups_get(p,u,g) groups_get_common(p,u,g,0)
#define groups_get_cacheonly(p,u,g) groups_get_common(p,u,g,1)
void groups_rel(groups* g);
void groups_term(void);
void groups_init(double _to,int dm);

#endif
