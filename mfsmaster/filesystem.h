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

#ifndef _FILESYSTEM_H_
#define _FILESYSTEM_H_

#include <stdio.h>
#include <inttypes.h>
#include "bio.h"

uint8_t fs_mr_access(uint32_t ts,uint32_t inode);
uint8_t fs_mr_append(uint32_t ts,uint32_t inode,uint32_t inode_src);
uint8_t fs_mr_acquire(uint32_t inode,uint32_t sessionid);
uint8_t fs_mr_attr(uint32_t ts,uint32_t inode,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t atime,uint32_t mtime);
// int fs_copy(uint32_t ts,inode,parent,strlen(name),name);
uint8_t fs_mr_create(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t inode);
uint8_t fs_mr_session(uint32_t sessionid);
uint8_t fs_mr_emptytrash(uint32_t ts,uint32_t freeinodes,uint32_t sustainedinodes);
uint8_t fs_mr_emptysustained(uint32_t ts,uint32_t freeinodes);
uint8_t fs_mr_freeinodes(uint32_t ts,uint32_t freeinodes);
uint8_t fs_mr_link(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint32_t nleng_dst,uint8_t *name_dst);
uint8_t fs_mr_length(uint32_t ts,uint32_t inode,uint64_t length);
uint8_t fs_mr_move(uint32_t ts,uint32_t parent_src,uint32_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint32_t nleng_dst,const uint8_t *name_dst,uint32_t inode);
uint8_t fs_mr_repair(uint32_t ts,uint32_t inode,uint32_t indx,uint32_t nversion);
// uint8_t fs_reinit(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid);
uint8_t fs_mr_release(uint32_t inode,uint32_t sessionid);
uint8_t fs_mr_symlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t inode);
uint8_t fs_mr_setpath(uint32_t inode,const uint8_t *path);
uint8_t fs_mr_snapshot(uint32_t ts,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,uint8_t *name_dst,uint8_t smode,uint8_t sesflags,uint32_t uid,uint32_t gids,uint32_t *gid,uint16_t requmask);
uint8_t fs_mr_unlink(uint32_t ts,uint32_t parent,uint32_t nleng,const uint8_t *name,uint32_t inode);
uint8_t fs_mr_purge(uint32_t ts,uint32_t inode);
uint8_t fs_mr_undel(uint32_t ts,uint32_t inode);
uint8_t fs_mr_trunc(uint32_t ts,uint32_t inode,uint32_t indx,uint64_t chunkid);
uint8_t fs_mr_write(uint32_t ts,uint32_t inode,uint32_t indx,uint8_t opflag,uint64_t chunkid);
uint8_t fs_mr_unlock(uint64_t chunkid);
uint8_t fs_mr_incversion(uint64_t chunkid);
uint8_t fs_mr_setgoal(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes);
uint8_t fs_mr_settrashtime(uint32_t ts,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes);
uint8_t fs_mr_seteattr(uint32_t ts,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t sinodes,uint32_t ncinodes,uint32_t nsinodes);
uint8_t fs_mr_setxattr(uint32_t ts,uint32_t inode,uint32_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint32_t mode);
uint8_t fs_mr_setacl(uint32_t ts,uint32_t inode,uint16_t mode,uint8_t changectime,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob);
uint8_t fs_mr_quota(uint32_t ts,uint32_t inode,uint8_t exceeded,uint8_t flags,uint32_t stimestamp,uint32_t sinodes,uint32_t hinodes,uint64_t slength,uint64_t hlength,uint64_t ssize,uint64_t hsize,uint64_t srealsize,uint64_t hrealsize);

//uint64_t fs_mr_getversion(void);

void fs_text_dump(FILE *fd);

// attr blob: [ type:8 goal:8 mode:16 uid:32 gid:32 atime:32 mtime:32 ctime:32 length:64 ]
void fs_stats(uint32_t stats[16]);
void fs_info(uint64_t *totalspace,uint64_t *availspace,uint64_t *trspace,uint32_t *trnodes,uint64_t *respace,uint32_t *renodes,uint32_t *inodes,uint32_t *dnodes,uint32_t *fnodes);
void fs_test_getdata(uint32_t *loopstart,uint32_t *loopend,uint32_t *files,uint32_t *ugfiles,uint32_t *mfiles,uint32_t *mtfiles,uint32_t *msfiles,uint32_t *chunks,uint32_t *ugchunks,uint32_t *mchunks,char **msgbuff,uint32_t *msgbuffleng);

// void fs_attrtoblob(uint8_t attr[32],uint8_t attrblob[32]);

uint32_t fs_getdirpath_size(uint32_t inode);
void fs_getdirpath_data(uint32_t inode,uint8_t *buff,uint32_t size);
uint8_t fs_getrootinode(uint32_t *rootinode,const uint8_t *path);

void fs_statfs(uint32_t rootinode,uint8_t sesflags,uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *sustainedspace,uint32_t *inodes);
uint8_t fs_access(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,int modemask);
uint8_t fs_lookup(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_getattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t attr[35]);
uint8_t fs_setattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t sugidclearmode,uint8_t attr[35]);

uint8_t fs_try_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[35],uint64_t *chunkid);
uint8_t fs_end_setlength(uint64_t chunkid);
uint8_t fs_do_setlength(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint64_t length,uint8_t attr[35]);

uint8_t fs_readlink(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,uint8_t **path);
uint8_t fs_symlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_mknod(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_mkdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t copysgid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_unlink(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid);
uint8_t fs_rmdir(uint32_t rootinode,uint8_t sesflags,uint32_t parent,uint16_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid);
uint8_t fs_rename(uint32_t rootinode,uint8_t sesflags,uint32_t parent_src,uint16_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_link(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_snapshot(uint32_t rootinode,uint8_t sesflags,uint32_t inode_src,uint32_t parent_dst,uint16_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t smode,uint16_t requmask);
uint8_t fs_append(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t inode_src,uint32_t uid,uint32_t gids,uint32_t *gid);

uint8_t fs_readdir_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t flags,uint32_t maxentries,uint64_t nedgeid,void **dnode,void **dedge,uint32_t *dbuffsize);
void fs_readdir_data(uint32_t rootinode,uint8_t sesflags,uint32_t uid,uint32_t gid,uint32_t auid,uint32_t agid,uint8_t flags,uint32_t maxentries,uint64_t *nedgeid,void *dnode,void *dedge,uint8_t *dbuff);

uint8_t fs_checkfile(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t chunkcount[11]);

uint8_t fs_opencheck(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint8_t flags,uint8_t attr[35]);

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length);
uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *chunkid,uint64_t *length,uint8_t *opflag);
// uint8_t fs_reinitchunk(uint32_t inode,uint32_t indx,uint64_t *chunkid);
uint8_t fs_writeend(uint32_t inode,uint64_t length,uint64_t chunkid);

uint8_t fs_repair(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *notchanged,uint32_t *erased,uint32_t *repaired);

uint8_t fs_getgoal(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t fgtab[10],uint32_t dgtab[10]);
uint8_t fs_setgoal(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t goal,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes);

uint8_t fs_gettrashtime_prepare(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,void **fptr,void **dptr,uint32_t *fnodes,uint32_t *dnodes);
void fs_gettrashtime_store(void *fptr,void *dptr,uint8_t *buff);
uint8_t fs_settrashtime(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint32_t trashtime,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes);

uint8_t fs_geteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t gmode,uint32_t feattrtab[16],uint32_t deattrtab[16]);
uint8_t fs_seteattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t eattr,uint8_t smode,uint32_t *sinodes,uint32_t *ncinodes,uint32_t *nsinodes);

uint8_t fs_listxattr_leng(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,void **xanode,uint32_t *xasize);
void fs_listxattr_data(void *xanode,uint8_t *xabuff);
uint8_t fs_setxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t anleng,const uint8_t *attrname,uint32_t avleng,const uint8_t *attrvalue,uint8_t mode);
uint8_t fs_getxattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t anleng,const uint8_t *attrname,uint32_t *avleng,uint8_t **attrvalue);

uint8_t fs_setacl(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t uid,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t mask,uint16_t namedusers,uint16_t namedgroups,const uint8_t *aclblob);
uint8_t fs_getacl_size(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t acltype,void **custom,uint32_t *aclblobsize);
void fs_getacl_data(void *custom,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *mask,uint16_t *namedusers,uint16_t *namedgroups,uint8_t *aclblob);

uint8_t fs_get_parents_count(uint32_t rootinode,uint32_t inode,uint32_t *cnt);
void fs_get_parents_data(uint32_t rootinode,uint32_t inode,uint8_t *buff);
uint8_t fs_get_paths_size(uint32_t rootinode,uint32_t inode,uint32_t *psize);
void fs_get_paths_data(uint32_t rootinode,uint32_t inode,uint8_t *buff);

// RESERVED
//uint8_t fs_acquire(uint32_t inode,uint32_t sessionid);
//uint8_t fs_release(uint32_t inode,uint32_t sessionid);
//uint32_t fs_newsessionid(void);

uint8_t fs_readsustained_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize);
void fs_readsustained_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff);

// TRASH
uint8_t fs_readtrash_size(uint32_t rootinode,uint8_t sesflags,uint32_t *dbuffsize);
void fs_readtrash_data(uint32_t rootinode,uint8_t sesflags,uint8_t *dbuff);
uint8_t fs_gettrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *pleng,const uint8_t **path);
uint8_t fs_settrashpath(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t pleng,const uint8_t *path);
uint8_t fs_purge(uint32_t rootinode,uint8_t sesflags,uint32_t inode);
uint8_t fs_undel(uint32_t rootinode,uint8_t sesflags,uint32_t inode);

// RESERVED+TRASH
uint8_t fs_getdetachedattr(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t attr[35],uint8_t dtype);

// EXTRA
uint8_t fs_get_dir_stats(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint32_t *inodes,uint32_t *dirs,uint32_t *files,uint32_t *chunks,uint64_t *length,uint64_t *size,uint64_t *rsize);

// QUOTA
uint8_t fs_quotacontrol(uint32_t rootinode,uint8_t sesflags,uint32_t inode,uint8_t delflag,uint8_t *flags,uint32_t *sinodes,uint64_t *slength,uint64_t *ssize,uint64_t *srealsize,uint32_t *hinodes,uint64_t *hlength,uint64_t *hsize,uint64_t *hrealsize,uint32_t *curinodes,uint64_t *curlength,uint64_t *cursize,uint64_t *currealsize);
//uint8_t fs_deletequota(uint32_t inode,uint8_t sflags,uint8_t hflags);
//uint8_t fs_setquota(uint32_t inode,uint8_t sflags,uint8_t hflags,uint32_t sinodes,uint64_t slength,uint64_t ssize,uint64_t srealsize,uint32_t hinodes,uint64_t hlength,uint64_t hsize,uint64_t hrealsize);
//uint8_t fs_getquota(uint32_t inode,uint8_t *sflags,uint8_t *hflags,uint32_t *sinodes,uint64_t *slength,uint64_t *ssize,uint64_t *srealsize,uint32_t *hinodes,uint64_t *hlength,uint64_t *hsize,uint64_t *hrealsize);
uint32_t fs_getquotainfo_size(void);
void fs_getquotainfo_data(uint8_t *buff);

// SPECIAL - LOG EMERGENCY INCREASE VERSION FROM CHUNKS-MODULE
void fs_incversion(uint64_t chunkid);

void fs_cs_disconnected(void);

void fs_new(void);

void fs_get_memusage(uint64_t allocated[8],uint64_t used[8]);

void fs_cleanup(void);
void fs_afterload(void);
int fs_check_consistency(int ignoreflag);

int fs_importnodes(bio *fd,uint32_t maxnodeid);

int fs_loadnodes(bio *fd,uint8_t mver);
int fs_loadedges(bio *fd,uint8_t mver,int ignoreflag);
int fs_loadfree(bio *fd,uint8_t mver);
int fs_loadquota(bio *fd,uint8_t mver,int ignoreflag);
uint8_t fs_storenodes(bio *fd);
uint8_t fs_storeedges(bio *fd);
uint8_t fs_storefree(bio *fd);
uint8_t fs_storequota(bio *fd);

uint8_t fs_mr_renumerate_edges(uint64_t expected_nextedgeid);
void fs_renumerate_edge_test(void);

int fs_strinit(void);

void fs_set_xattrflag(uint32_t inode);
void fs_del_xattrflag(uint32_t inode);

void fs_set_aclflag(uint32_t inode,uint8_t acltype);
void fs_del_aclflag(uint32_t inode,uint8_t acltype);

#endif
