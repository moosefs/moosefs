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

#ifndef _XATTRCACHE_H_
#define _XATTRCACHE_H_

void* xattr_cache_get(uint32_t node,uint32_t uid,uint32_t gid,uint32_t nleng,const uint8_t *name,const uint8_t **value,uint32_t *vleng,int *status);
void xattr_cache_set(uint32_t node,uint32_t uid,uint32_t gid,uint32_t nleng,const uint8_t *name,const uint8_t *value,uint32_t vleng,int status);
void xattr_cache_del(uint32_t node,uint32_t nleng,const uint8_t *name);
void xattr_cache_rel(void *vv);
void xattr_cache_term(void);
void xattr_cache_init(double timeout);

#endif
