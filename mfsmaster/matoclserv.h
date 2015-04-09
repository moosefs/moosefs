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

#ifndef _MATOCLSERV_H_
#define _MATOCLSERV_H_

#include <inttypes.h>

void matoclserv_stats(uint64_t stats[5]);
/*
void matoclserv_notify_attr(uint32_t dirinode,uint32_t inode,const uint8_t attr[35]);
void matoclserv_notify_link(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t inode,const uint8_t attr[35],uint32_t ts);
void matoclserv_notify_unlink(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t ts);
void matoclserv_notify_remove(uint32_t dirinode);
void matoclserv_notify_parent(uint32_t dirinode,uint32_t parent);
*/
void matoclserv_chunk_status(uint64_t chunkid,uint8_t status);
int matoclserv_no_more_pending_jobs(void);
void matoclserv_disconnect_all(void);
int matoclserv_init(void);

#endif
