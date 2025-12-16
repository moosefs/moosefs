/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifndef _BGJOBS_H_
#define _BGJOBS_H_

#include "MFSCommunication.h"
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
#define job_split(_cb,_ex,_chunkid,_version,_newversion,_missingmask,_parts) job_chunkop(_cb,_ex,_chunkid,_version,_newversion,0,_parts,0x80000000|(_missingmask))

// uint32_t job_open(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version);
// uint32_t job_close(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid);
// uint32_t job_read(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff);
// uint32_t job_write(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff);

uint32_t job_serv_read(void (*callback)(uint8_t status,void *extra),void *extra,int sock,const uint8_t *packet,uint32_t length);
uint32_t job_serv_write(void (*callback)(uint8_t status,void *extra),void *extra,int sock,const uint8_t *packet,uint32_t length);

/* srcs: srccnt * (chunkid:64 version:32 ip:32 port:16) */
uint32_t job_replicate_simple(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t srcip,uint16_t srcport);
uint32_t job_replicate_split(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t srcip,uint16_t srcport,uint64_t srcchunkid,uint8_t partno,uint8_t parts);
uint32_t job_replicate_recover(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t parts,uint32_t srcip[MAX_EC_PARTS],uint16_t srcport[MAX_EC_PARTS],uint64_t srcchunkid[MAX_EC_PARTS]);
uint32_t job_replicate_join(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t parts,uint32_t srcip[MAX_EC_PARTS],uint16_t srcport[MAX_EC_PARTS],uint64_t srcchunkid[MAX_EC_PARTS]);

uint32_t job_get_chunk_info(void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t requested_info,uint8_t *info_buff);
#define job_get_chunk_blocks(_cb,_ex,_chunkid,_version,_blocks) job_get_chunk_info(_cb,_ex,_chunkid,_version,REQUEST_BLOCKS,_blocks)
#define job_get_chunk_checksum(_cb,_ex,_chunkid,_version,_checksum) job_get_chunk_info(_cb,_ex,_chunkid,_version,REQUEST_CHECKSUM,_checksum)
#define job_get_chunk_checksum_tab(_cb,_ex,_chunkid,_version,_checksumtab) job_get_chunk_info(_cb,_ex,_chunkid,_version,REQUEST_CHECKSUM_TAB,_checksumtab)

uint32_t job_chunk_move(void (*callback)(uint8_t status,void *extra),void *extra,void *fsrc,void *fdst);
// uint32_t job_mainserv(int sock);

int job_init(void);

#endif
