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

#ifndef _HDDSPACEMGR_H_
#define _HDDSPACEMGR_H_

#include <inttypes.h>

#include "MFSCommunication.h"

void hdd_stats(uint64_t *br,uint64_t *bw,uint32_t *opr,uint32_t *opw,uint32_t *dbr,uint32_t *dbw,uint32_t *dopr,uint32_t *dopw,uint32_t *movl,uint32_t *movh,uint64_t *rtime,uint64_t *wtime);
void hdd_op_stats(uint32_t *op_create,uint32_t *op_delete,uint32_t *op_version,uint32_t *op_duplicate,uint32_t *op_truncate,uint32_t *op_duptrunc,uint32_t *op_test);
uint32_t hdd_errorcounter(void);

/* lock/unlock pair */
uint32_t hdd_get_damaged_chunk_count(void);
void hdd_get_damaged_chunk_data(uint8_t *buff);
/* lock/unlock pair */
uint32_t hdd_get_lost_chunk_count(uint32_t limit);
void hdd_get_lost_chunk_data(uint8_t *buff,uint32_t limit);
/* lock/unlock pair */
uint32_t hdd_get_new_chunk_count(uint32_t limit);
void hdd_get_new_chunk_data(uint8_t *buff,uint32_t limit);
/* lock/unlock pair */
uint32_t hdd_get_changed_chunk_count(uint32_t limit);
void hdd_get_changed_chunk_data(uint8_t *buffl,uint8_t *buffn,uint32_t limit);
/* lock/unlock pair */
uint32_t hdd_diskinfo_size(void);
void hdd_diskinfo_data(uint8_t *buff);
uint32_t hdd_diskinfo_monotonic_size(void);
void hdd_diskinfo_monotonic_data(uint8_t *buff);
/* lock/unlock pair */
void hdd_get_chunks_begin(uint8_t partialmode);
void hdd_get_chunks_end(void);
uint32_t hdd_get_chunks_next_list_count(uint32_t stopcount);
void hdd_get_chunks_next_list_data(uint32_t stopcount,uint8_t *buff);
//uint32_t hdd_get_chunks_count();
//void hdd_get_chunks_data(uint8_t *buff);

//uint32_t get_changedchunkscount();
//void fill_changedchunksinfo(uint8_t *buff);
uint8_t hdd_spacechanged(void);
void hdd_get_space(uint64_t *usedspace,uint64_t *totalspace,uint32_t *chunkcount,uint64_t *tdusedspace,uint64_t *tdtotalspace,uint32_t *tdchunkcount);

uint8_t hdd_is_rebalance_on(void);

/* emergency chunk read - ignore errors, do retries */
int hdd_emergency_read(uint64_t chunkid,uint32_t *version,uint16_t blocknum,uint8_t buffer[MFSBLOCKSIZE],uint8_t retries,uint8_t *errorflags);

/* precache data */
void hdd_precache_data(uint64_t chunkid,uint32_t offset,uint32_t size);

/* I/O operations */
int hdd_open(uint64_t chunkid,uint32_t version);
int hdd_close(uint64_t chunkid);
int hdd_read(uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff);
int hdd_write(uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff);

/* chunk info */
// int hdd_check_version(uint64_t chunkid,uint32_t version);
int hdd_get_blocks(uint64_t chunkid,uint32_t version,uint8_t *blocks_buff);
int hdd_get_checksum(uint64_t chunkid, uint32_t version, uint8_t *checksum_buff);
int hdd_get_checksum_tab(uint64_t chunkid, uint32_t version, uint8_t *checksum_tab);

int hdd_move(void *fsrcv,void *fdstv);

/* chunk operations */

/* all chunk operations in one call */
// newversion>0 && length==0xFFFFFFFF && copychunkid==0       -> change version
// newversion>0 && length==0xFFFFFFFF && copychunkid>0        -> duplicate
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid==0     -> truncate
// newversion>0 && length<=MFSCHUNKSIZE && copychunkid>0      -> duplicate and truncate
// newversion==0 && length==0                                 -> delete
// newversion==0 && length==1                                 -> create
// newversion==0 && length==2                                 -> test
int hdd_chunkop(uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length);

#define hdd_delete(_chunkid,_version) hdd_chunkop(_chunkid,_version,0,0,0,0)
#define hdd_create(_chunkid,_version) hdd_chunkop(_chunkid,_version,0,0,0,1)
#define hdd_test(_chunkid,_version) hdd_chunkop(_chunkid,_version,0,0,0,2)
#define hdd_version(_chunkid,_version,_newversion) (((_newversion)>0)?hdd_chunkop(_chunkid,_version,_newversion,0,0,0xFFFFFFFF):MFS_ERROR_EINVAL)
#define hdd_truncate(_chunkid,_version,_newversion,_length) (((_newversion)>0&&(_length)!=0xFFFFFFFF)?hdd_chunkop(_chunkid,_version,_newversion,0,0,_length):MFS_ERROR_EINVAL)
#define hdd_duplicate(_chunkid,_version,_newversion,_copychunkid,_copyversion) (((_newversion>0)&&(_copychunkid)>0)?hdd_chunkop(_chunkid,_version,_newversion,_copychunkid,_copyversion,0xFFFFFFFF):MFS_ERROR_EINVAL)
#define hdd_duptrunc(_chunkid,_version,_newversion,_copychunkid,_copyversion,_length) (((_newversion>0)&&(_copychunkid)>0&&(_length)!=0xFFFFFFFF)?hdd_chunkop(_chunkid,_version,_newversion,_copychunkid,_copyversion,_length):MFS_ERROR_EINVAL)

/* meta id */
void hdd_setmetaid(uint64_t metaid);

/* restore */
int hdd_restore(void);

/* initialization */
int hdd_late_init(void);
int hdd_init(void);

/* debug only */
void hdd_test_show_chunks(void);
void hdd_test_show_openedchunks(void);
#endif
