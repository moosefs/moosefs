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

#ifndef _BGJOBS_H_
#define _BGJOBS_H_

#include <inttypes.h>

void job_stats(uint32_t *maxjobscnt);
void job_get_load_and_hlstatus(uint32_t *load,uint8_t *hlstatus);

void job_pool_disable_job(uint32_t jobid);
void job_pool_change_callback(uint32_t jobid,void (*callback)(uint8_t status,void *extra),void *extra);


uint32_t job_inval(void (*callback)(uint8_t status,void *extra),void *extra);
uint32_t job_chunkop(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length);

#define job_delete(_cb,_ex,_chunkid,_version) job_chunkop(_cb,_ex,_chunkid,_version,0,0,0,0)
#define job_create(_cb,_ex,_chunkid,_version) job_chunkop(_cb,_ex,_chunkid,_version,0,0,0,1)
#define job_test(_cb,_ex,_chunkid,_version) job_chunkop(_cb,_ex,_chunkid,_version,0,0,0,2)
#define job_version(_cb,_ex,_chunkid,_version,_newversion) (((_newversion)>0)?job_chunkop(_cb,_ex,_chunkid,_version,_newversion,0,0,0xFFFFFFFF):job_inval(_cb,_ex))
#define job_truncate(_cb,_ex,_chunkid,_version,_newversion,_length) (((_newversion)>0&&(_length)!=0xFFFFFFFF)?job_chunkop(_cb,_ex,_chunkid,_version,_newversion,0,0,_length):job_inval(_cb,_ex))
#define job_duplicate(_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion) (((_newversion>0)&&(_copychunkid)>0)?job_chunkop(_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion,0xFFFFFFFF):job_inval(_cb,_ex))
#define job_duptrunc(_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion,_length) (((_newversion>0)&&(_copychunkid)>0&&(_length)!=0xFFFFFFFF)?job_chunkop(_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion,_length):job_inval(_cb,_ex))

// uint32_t job_open(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version);
// uint32_t job_close(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid);
// uint32_t job_read(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff);
// uint32_t job_write(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff);

uint32_t job_serv_read(void (*callback)(uint8_t status,void *extra),void *extra,int sock,const uint8_t *packet,uint32_t length);
uint32_t job_serv_write(void (*callback)(uint8_t status,void *extra),void *extra,int sock,const uint8_t *packet,uint32_t length);

/* srcs: srccnt * (chunkid:64 version:32 ip:32 port:16) */
uint32_t job_replicate_raid(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t srccnt,const uint32_t xormasks[4],const uint8_t *srcs);
uint32_t job_replicate_simple(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t ip,uint16_t port);

uint32_t job_get_chunk_blocks(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t *blocks);
uint32_t job_get_chunk_checksum(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t *checksum);
uint32_t job_get_chunk_checksum_tab(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t *checksum_tab);

uint32_t job_chunk_move(void (*callback)(uint8_t status,void *extra),void *extra,void *fsrc,void *fdst);
// uint32_t job_mainserv(int sock);

int job_init(void);

#endif
