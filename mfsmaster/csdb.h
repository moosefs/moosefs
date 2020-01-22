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

#ifndef _CSDB_H_
#define _CSDB_H_

#include <inttypes.h>
#include "bio.h"

void* csdb_new_connection(uint32_t ip,uint16_t port,uint16_t csid,void *eptr);
uint16_t csdb_get_csid(void *v_csptr);
void csdb_temporary_maintenance_mode(void *v_csptr);
void csdb_lost_connection(void *v_csptr);
void csdb_server_load(void *v_csptr,uint32_t load);
uint8_t csdb_server_is_overloaded(void *v_csptr,uint32_t now);
uint8_t csdb_server_is_being_maintained(void *v_csptr);
uint32_t csdb_servlist_size(void);
void csdb_servlist_data(uint8_t *ptr);
uint8_t csdb_remove_server(uint32_t ip,uint16_t port);
uint8_t csdb_back_to_work(uint32_t ip,uint16_t port);
uint8_t csdb_maintenance(uint32_t ip,uint16_t port,uint8_t onoff);
uint8_t csdb_have_all_servers(void);
uint8_t csdb_have_more_than_half_servers(void);
uint8_t csdb_replicate_undergoals(void);
uint16_t csdb_servers_count(void);
void csdb_cleanup(void);
uint16_t csdb_sort_servers(void);
uint16_t csdb_getnumber(void *v_csptr);
uint8_t csdb_store(bio *fd);
int csdb_load(bio *fd,uint8_t mver,int ignoreflag);
int csdb_init(void);
uint8_t csdb_mr_op(uint8_t csop,uint32_t ip,uint16_t port,uint32_t arg);
#define csdb_mr_csadd(x,y) csdb_mr_op(0,x,y,0)
#define csdb_mr_csdel(x,y) csdb_mr_op(1,x,y,0)
// uint32_t csdb_getdisconnecttime(void);

#endif
