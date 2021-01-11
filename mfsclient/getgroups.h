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

#ifndef _GETGROUPS_H_
#define _GETGROUPS_H_

#include <sys/types.h>
#include <inttypes.h>

typedef struct groups {
	double time;
	pid_t pid;
	uid_t uid;
	gid_t gid;
	uint16_t lcnt;
	uint8_t locked;
	uint32_t gidcnt;
	uint32_t *gidtab;
	struct groups *next,**prev;
} groups;

groups* groups_get_x(pid_t pid,uid_t uid,gid_t gid,uint8_t lockmode);
#define groups_get(p,u,g) groups_get_x(p,u,g,0)
void groups_rel(groups* g);
void groups_term(void);
void groups_init(double _to,int dm);

#endif
