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

#ifndef _POSIXACL_H_
#define _POSIXACL_H_

#include <inttypes.h>

#include "bio.h"

uint16_t posix_acl_getmode(uint32_t inode);
void posix_acl_setmode(uint32_t inode,uint16_t mode);
uint8_t posix_acl_accmode(uint32_t inode,uint32_t auid,uint32_t agids,uint32_t *agid,uint32_t fuid,uint32_t fgid);
void posix_acl_remove(uint32_t inode,uint8_t acltype);
uint8_t posix_acl_copydefaults(uint32_t parent,uint32_t inode,uint8_t directory,uint16_t *mode);
int posix_acl_set(uint32_t inode,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob);
int32_t posix_acl_get_blobsize(uint32_t inode,uint8_t acltype,void **aclnode);
void posix_acl_get_data(void *aclnode,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *mask,uint16_t *namedusers,uint16_t *namedgroups,uint8_t *aclblob);
void posix_acl_cleanup(void);
uint8_t posix_acl_copy(uint32_t srcinode,uint32_t dstinode,uint8_t acltype);
uint8_t posix_acl_store(bio *fd);
int posix_acl_load(bio *fd,uint8_t mver,int ignoreflag);
int posix_acl_init(void);

#endif
