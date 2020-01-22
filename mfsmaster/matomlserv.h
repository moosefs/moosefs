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

#ifndef _MATOMLSERV_H_
#define _MATOMLSERV_H_

#include <inttypes.h>



uint32_t matomlserv_mloglist_size(void);
void matomlserv_mloglist_data(uint8_t *ptr);

void matomlserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize);
void matomlserv_broadcast_logrotate();
int matomlserv_no_more_pending_jobs(void);
void matomlserv_disconnect_all(void);
uint16_t matomlserv_getport(void);
void matomlserv_close_lsock(void);
int matomlserv_init(void);

#endif
