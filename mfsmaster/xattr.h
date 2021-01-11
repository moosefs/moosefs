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

#ifndef _XATTR_H_
#define _XATTR_H_

#include <inttypes.h>
#include "bio.h"

int xattr_namecheck(uint8_t anleng,const uint8_t *attrname);
void xattr_removeinode(uint32_t inode);
uint8_t xattr_setattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode);
uint8_t xattr_getattr(uint32_t inode,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,const uint8_t **attrvalue);
uint8_t xattr_listattr_leng(uint32_t inode,void **xanode,uint32_t *xasize);
void xattr_listattr_data(void *xanode,uint8_t *xabuff);
uint8_t xattr_copy(uint32_t srcinode,uint32_t dstinode);
void xattr_cleanup(void);
uint8_t xattr_store(bio *fd);
int xattr_load(bio *fd,uint8_t mver,int ignoreflag);
int xattr_init(void);

#endif
