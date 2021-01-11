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

#ifndef _SESSIONS_H_
#define _SESSIONS_H_

#include <inttypes.h>
#include "bio.h"

void sessions_attach_session(void* vsesdata,uint32_t peerip,uint32_t version);
void sessions_close_session(uint32_t sessionid);
void sessions_disconnection(void *vsesdata);
void* sessions_find_session(uint32_t sessionid);
int sessions_open_file(void* vsesdata,uint32_t inode);
int sessions_connect_session_with_inode(uint32_t sessionid,uint32_t inode);
uint32_t sessions_get_statscnt(void);
uint32_t sessions_datasize(uint8_t vmode);
void sessions_datafill(uint8_t *ptr,uint8_t vmode);
uint8_t sessions_force_remove(uint32_t sessionid);
//uint32_t sessions_datasize(void *vsesdata,uint8_t vmode);
//uint32_t sessions_datafill(uint8_t *ptr,void *vsesdata,uint8_t vmode);
void* sessions_new_session(uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashtime,uint32_t maxtrashtime,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng);
void sessions_chg_session(void *vsesdata,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashtime,uint32_t maxtrashtime,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng);
void sessions_sync_open_files(void *vsesdata,const uint8_t *ptr,uint32_t inodecnt);
uint32_t sessions_get_id(void *vsesdata);
uint64_t sessions_get_exportscsum(void *vsesdata);
uint32_t sessions_get_peerip(void *vsesdata);
uint32_t sessions_get_rootinode(void *vsesdata);
uint32_t sessions_get_sesflags(void *vsesdata);
uint16_t sessions_get_umask(void *vsesdata);
uint32_t sessions_get_disables(void *vsesdata);
uint8_t sessions_is_root_remapped(void *vsesdata);
uint8_t sessions_check_goal(void *vsesdata,uint8_t smode,uint8_t goal);
uint8_t sessions_check_trashtime(void *vsesdata,uint8_t smode,uint32_t trashtime);
void sessions_inc_stats(void *vsesdata,uint8_t statid);
void sessions_ugid_remap(void *vsesdata,uint32_t *auid,uint32_t *agid);

void sessions_cleanup(void);
int sessions_init(void);

uint8_t sessions_mr_seschanged(uint32_t sessionid,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashtime,uint32_t maxtrashtime,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng);
uint8_t sessions_mr_sesadd(uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashtime,uint32_t maxtrashtime,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng,uint32_t sessionid);
uint8_t sessions_mr_sesdel(uint32_t sessionid);
uint8_t sessions_mr_disconnected(uint32_t sessionid,uint32_t disctime);
uint8_t sessions_mr_session(uint32_t sessionid); // deprecated

void sessions_new(void);
uint8_t sessions_store(bio *fd);
int sessions_load(bio *fd,uint8_t mver);

/* import from old metadata */
void sessions_import(void);
void sessions_set_nextsessionid(uint32_t nsi);

#endif
