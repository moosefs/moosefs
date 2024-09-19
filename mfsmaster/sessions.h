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

#ifndef _SESSIONS_H_
#define _SESSIONS_H_

#include <inttypes.h>
#include "bio.h"

#define SES_OP_STATFS 0
#define SES_OP_GETATTR 1
#define SES_OP_SETATTR 2
#define SES_OP_LOOKUP 3
#define SES_OP_MKDIR 4
#define SES_OP_RMDIR 5
#define SES_OP_SYMLINK 6
#define SES_OP_READLINK 7
#define SES_OP_MKNOD 8
#define SES_OP_UNLINK 9
#define SES_OP_RENAME 10
#define SES_OP_LINK 11
#define SES_OP_READDIR 12
#define SES_OP_OPEN 13
#define SES_OP_READCHUNK 14
#define SES_OP_WRITECHUNK 15
#define SES_OP_READ 16
#define SES_OP_WRITE 17
#define SES_OP_FSYNC 18
#define SES_OP_SNAPSHOT 19
#define SES_OP_TRUNCATE 20
#define SES_OP_GETXATTR 21
#define SES_OP_SETXATTR 22
#define SES_OP_GETFACL 23
#define SES_OP_SETFACL 24
#define SES_OP_CREATE 25
#define SES_OP_LOCK 26
#define SES_OP_META 27

#define SESSION_STATS 28

#define SES_OP_STRINGS \
	"STATFS", \
	"GETATTR", \
	"SETATTR", \
	"LOOKUP", \
	"MKDIR", \
	"RMDIR", \
	"SYMLINK", \
	"READLINK", \
	"MKNOD", \
	"UNLINK", \
	"RENAME", \
	"LINK", \
	"READDIR", \
	"OPEN", \
	"READCHUNK", \
	"WRITECHUNK", \
	"READ", \
	"WRITE", \
	"FSYNC", \
	"SNAPSHOT", \
	"TRUNCATE", \
	"GETXATTR", \
	"SETXATTR", \
	"GETFACL", \
	"SETFACL", \
	"CREATE", \
	"LOCK", \
	"META"

void sessions_attach_session(void* vsesdata,uint32_t peerip,uint32_t version);
void sessions_close_session(void *vsesdata);
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
void* sessions_new_session(uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng);
uint32_t sessions_chg_session(void *vsesdata,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng);
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
uint8_t sessions_check_trashretention(void *vsesdata,uint8_t smode,uint32_t trashretention);
void sessions_inc_stats(void *vsesdata,uint8_t statid);
void sessions_add_stats(void *vsesdata,uint8_t statid,uint64_t value);
void sessions_ugid_remap(void *vsesdata,uint32_t *auid,uint32_t *agid);

void sessions_cleanup(void);
int sessions_init(void);

uint8_t sessions_mr_seschanged(uint32_t sessionid,uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng);
uint8_t sessions_mr_sesadd(uint64_t exportscsum,uint32_t rootinode,uint8_t sesflags,uint16_t umaskval,uint32_t rootuid,uint32_t rootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashretention,uint32_t maxtrashretention,uint32_t disables,uint32_t peerip,const uint8_t *info,uint32_t ileng,uint32_t sessionid);
uint8_t sessions_mr_sesdel(uint32_t sessionid);
uint8_t sessions_mr_connected(uint32_t sessionid);
uint8_t sessions_mr_disconnected(uint32_t sessionid,uint32_t disctime);
uint8_t sessions_mr_session(uint32_t sessionid); // deprecated

void sessions_new(void);
uint8_t sessions_store(bio *fd);
int sessions_load(bio *fd,uint8_t mver);

/* import from old metadata */
void sessions_import(void);
void sessions_set_nextsessionid(uint32_t nsi);

#endif
