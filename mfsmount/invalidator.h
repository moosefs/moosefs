/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifndef _INVALIDATOR_H_
#define _INVALIDATOR_H_

#include <inttypes.h>
#include <fuse_lowlevel.h>

void invalidator_insert(uint32_t parent,const char *name,uint8_t nleng,uint32_t inode,double timeout);
void invalidator_forget(uint32_t inode,uint64_t nlookup);
void invalidator_term(void);
void invalidator_on(void);
void invalidator_init(struct fuse_chan *ch);

#endif
