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

#ifndef _MASTERCOMM_H_
#define _MASTERCOMM_H_

#include <inttypes.h>
#include <sys/types.h>
#include "MFSCommunication.h"

void fs_getmasterlocation(uint8_t loc[14]);
uint32_t fs_getsrcip(void);


void fs_atime(uint32_t inode);
void fs_mtime(uint32_t inode);
void fs_no_atime(uint32_t inode);
void fs_no_mtime(uint32_t inode);
void fs_fix_amtime(uint32_t inode,time_t *atime,time_t *mtime);

void fs_notify_sendremoved(uint32_t cnt,uint32_t *inodes);
//int fs_direct_connect(void);
//void fs_direct_close(int rfd);
//int fs_direct_write(int rfd,const uint8_t *buff,uint32_t size);
//int fs_direct_read(int rfd,uint8_t *buff,uint32_t size);
void fs_add_entry(uint32_t inode);
void fs_forget_entry(uint32_t inode);
int fs_isopen(uint32_t inode);
void fs_inc_acnt(uint32_t inode);
void fs_dec_acnt(uint32_t inode);
// uint8_t fs_is_sustained_entry(uint32_t inode);

void fs_get_fleng(uint32_t inode,uint64_t *fleng);
void fs_set_fleng(uint32_t inode,uint64_t fleng);
void fs_inc_fleng(uint32_t inode,uint64_t fleng);

uint8_t fs_simple_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);

uint8_t fs_get_cfg(const char *opt_name,char opt_value[256]);
void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *freespace,uint64_t *trashspace,uint64_t *sustainedspace,uint32_t *inodes);
uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint16_t modemask);
// uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE],uint16_t *lflags,uint8_t *csdataver,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize);
uint8_t fs_getattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_setattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t winattr,uint8_t sugidclearmode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_truncate(uint32_t inode,uint8_t flags,uint32_t uid,uint32_t gids,uint32_t *gid,uint64_t attrlength,uint8_t attr[ATTR_RECORD_SIZE],uint64_t *prevlength);
uint8_t fs_readlink(uint32_t inode,const uint8_t **path);
uint8_t fs_symlink(uint32_t parent,uint8_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_mknod(uint32_t parent,uint8_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t rdev,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_mkdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t copysgid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode);
uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode);
uint8_t fs_rename(uint32_t parent_src,uint8_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint8_t nleng,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_readdir(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t wantattr,uint8_t addtocache,const uint8_t **dbuff,uint32_t *dbuffsize);

// uint8_t fs_check(uint32_t inode,uint8_t dbuff[22]);

uint8_t fs_create(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE],uint8_t *oflags);
uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t flags,uint8_t attr[ATTR_RECORD_SIZE],uint8_t *oflags);

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint8_t chunkopflags,uint8_t *csdataver,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize);
uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint8_t chunkopflags,uint8_t *csdataver,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize);
uint8_t fs_writeend(uint64_t chunkid,uint32_t inode,uint32_t indx,uint64_t length,uint8_t chunkopflags);

//uint8_t fs_fsync_send(uint32_t inode);
//uint8_t fs_fsync_wait(void);

uint8_t fs_flock(uint32_t inode,uint32_t reqid,uint64_t owner,uint8_t cmd);
uint8_t fs_posixlock(uint32_t inode,uint32_t reqid,uint64_t owner,uint8_t cmd,uint8_t type,uint64_t start,uint64_t end,uint32_t pid,uint8_t *rtype,uint64_t *rstart,uint64_t *rend,uint32_t *rpid);

uint8_t fs_getfacl(uint32_t inode,/*uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,*/uint8_t acltype,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *maskperm,uint16_t *namedusers,uint16_t *namedgroups,const uint8_t **namedacls,uint32_t *namedaclssize);
uint8_t fs_setfacl(uint32_t inode,uint32_t uid,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t maskperm,uint16_t namedusers,uint16_t namedgroups,uint8_t *namedacls,uint32_t namedaclssize);

uint8_t fs_getxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name,uint8_t mode,const uint8_t **vbuff,uint32_t *vleng);
uint8_t fs_listxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t mode,const uint8_t **dbuff,uint32_t *dleng);
uint8_t fs_setxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name,uint32_t vleng,const uint8_t *value,uint8_t mode);
uint8_t fs_removexattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name);

//int fs_reinitchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid);

uint8_t fs_getsustained(const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_gettrash(uint32_t tid,const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[ATTR_RECORD_SIZE]);
uint8_t fs_gettrashpath(uint32_t inode,const uint8_t **path);
uint8_t fs_settrashpath(uint32_t inode,const uint8_t *path);
uint8_t fs_undel(uint32_t inode);
uint8_t fs_purge(uint32_t inode);

uint8_t fs_custom(uint32_t qcmd,const uint8_t *query,uint32_t queryleng,uint32_t *acmd,uint8_t *answer,uint32_t *answerleng);

// for hardlink emulation only
// uint8_t fs_append(uint32_t inode,uint32_t ainode,uint32_t uid,uint32_t gid);

uint32_t master_version(void);
uint8_t master_attrsize(void);

// called before fork
int fs_init_master_connection(const char *bindhostname,const char *masterhostname,const char *masterportname,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t donotrememberpassword,uint8_t bgregister);
// called after fork
void fs_init_threads(uint32_t retries,uint32_t to);
void fs_term(void);

#endif
