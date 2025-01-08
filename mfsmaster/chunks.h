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
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifndef _CHUNKS_H_
#define _CHUNKS_H_

#include <stdio.h>
#include <inttypes.h>
#include "bio.h"
#include "storageclass.h"

#define MAXCSCOUNT 10000

#define CHUNK_OP_DELETE_TRY     0
#define CHUNK_OP_REPLICATE_TRY  1
#define CHUNK_OP_CREATE_TRY     2
#define CHUNK_OP_CHANGE_TRY     3
#define CHUNK_OP_SPLIT_TRY      4
#define CHUNK_OP_DELETE_OK      5
#define CHUNK_OP_REPLICATE_OK   6
#define CHUNK_OP_CREATE_OK      7
#define CHUNK_OP_CHANGE_OK      8
#define CHUNK_OP_SPLIT_OK       9
#define CHUNK_OP_DELETE_ERR    10
#define CHUNK_OP_REPLICATE_ERR 11
#define CHUNK_OP_CREATE_ERR    12
#define CHUNK_OP_CHANGE_ERR    13
#define CHUNK_OP_SPLIT_ERR     14

#define CHUNK_STATS_CNT        15

typedef enum {
	CHUNK_FLOOP_NOTFOUND,
	CHUNK_FLOOP_DELETED,
	CHUNK_FLOOP_MISSING_NOCOPY,
	CHUNK_FLOOP_MISSING_INVALID,
	CHUNK_FLOOP_MISSING_WRONGVERSION,
	CHUNK_FLOOP_MISSING_PARTIALEC,
	CHUNK_FLOOP_UNDERGOAL,
	CHUNK_FLOOP_OK
} chunkfloop;

int chunk_mr_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t opflag);
int chunk_mr_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid);
//int chunk_multi_reinitialize(uint32_t ts,uint64_t chunkid);
int chunk_mr_unlock(uint32_t ts,uint64_t chunkid);
int chunk_mr_increase_version(uint64_t chunkid);
int chunk_mr_set_version(uint64_t chunkid,uint32_t version);

int chunk_mr_nextchunkid(uint64_t nchunkid);
int chunk_mr_chunkadd(uint32_t ts,uint64_t chunkid,uint32_t version,uint32_t lockedto);
int chunk_mr_chunkdel(uint32_t ts,uint64_t chunkid,uint32_t version);

int chunk_mr_flagsclr(uint32_t ts,uint64_t chunkid);

void chunk_stats(uint32_t chunkops[CHUNK_STATS_CNT]);
void chunk_chart_data(uint64_t *copychunks,uint64_t *ec8chunks,uint64_t *ec4chunks,uint64_t *regendangered,uint64_t *regundergoal,uint64_t *allendangered,uint64_t *allundergoal);

uint32_t chunk_store_info(uint8_t *buff);
void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid,int16_t classid);
uint32_t chunk_count(void);
// counters order: UNDER_COPY,UNDER_EC,EXACT_COPY,EXACT_EC,OVER_COPY,OVER_EC
void chunk_sclass_inc_counters(uint8_t sclassid,uint8_t flags,uint8_t rlevel,uint64_t counters[6]);
uint8_t chunk_sclass_has_chunks(uint8_t sclassid);
void chunk_info(uint32_t *allchunks,uint32_t *copychunks,uint32_t *ec8chunks,uint32_t *ec4chunks,uint64_t *copies,uint64_t *ec8parts,uint64_t *ec4parts,uint64_t *hypotheticalcopies);
uint8_t chunk_counters_in_progress(void);

int chunk_get_archflag(uint64_t chunkid,uint8_t *archflag);
int chunk_set_archflag(uint64_t chunkid,uint8_t archflag,uint32_t *archflagchanged);
int chunk_set_trashflag(uint64_t chunkid,uint8_t trashflag);
int chunk_set_autoarch(uint64_t chunkid,uint32_t archreftime,uint32_t *archflagchanged,uint8_t intrash,uint32_t *trashflagchanged);
chunkfloop chunk_fileloop_task(uint64_t chunkid,uint8_t sclassid,uint8_t aftereof,uint32_t archreftime,uint32_t *archflagchanged,uint8_t intrash,uint32_t *trashflagchanged);
uint8_t chunk_remove_from_missing_log(uint64_t chunkid);

int chunk_read_check(uint32_t ts,uint64_t chunkid);
int chunk_multi_modify(uint8_t continueop,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t *opflag,uint32_t clientip);
int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t sclassid);
//int chunk_multi_reinitialize(uint64_t chunkid);
int chunk_repair(uint8_t sclassid,uint64_t ochunkid,uint8_t flags,uint32_t *nversion);

int chunk_locked_or_busy(void *cptr);

void chunk_do_extra_job(uint64_t chunkid);
/* ---- */

uint8_t chunk_get_version_and_csdata(uint8_t mode,uint64_t chunkid,uint32_t clientip,uint32_t *version,uint8_t *count,uint8_t cs_data[100*14],uint8_t *split);
uint8_t chunk_get_version_and_copies(uint8_t mode,uint64_t chunkid,uint32_t clientip,uint32_t *version,uint32_t *chunkmtime,uint8_t *count,uint8_t cs_data[100*8]);
uint8_t chunk_get_eights_copies(uint64_t chunkid,uint8_t *count);
uint8_t chunk_get_version(uint64_t chunkid,uint32_t *version);
uint8_t chunk_get_storage_status(uint64_t chunkid,uint8_t *fcopies,uint8_t *ec8parts,uint8_t *ec4parts);

/* ---- */

void chunk_got_status_data(uint64_t chunkid,const char *servdesc,uint16_t csid,uint8_t parts,uint8_t *ecid,uint32_t *version,uint8_t *damaged,uint16_t *blocks,uint8_t fixmode);

/* ---- */
uint8_t chunk_get_mfrstatus(uint16_t csid);

uint16_t chunk_server_connected(void *ptr);

void chunk_server_has_chunk(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint32_t version);
void chunk_damaged(uint16_t csid,uint64_t chunkid,uint8_t ecid);
void chunk_lost(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t report);
void chunk_server_register_end(uint16_t csid);
void chunk_server_disconnected(uint16_t csid);

void chunk_got_delete_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status);
void chunk_got_replicate_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t status);

void chunk_got_chunkop_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status);

void chunk_got_create_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status);
void chunk_got_duplicate_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status);
void chunk_got_setversion_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status);
void chunk_got_truncate_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status);
void chunk_got_duptrunc_status(uint16_t csid,uint64_t chunkid,uint8_t ecid,uint8_t status);
void chunk_got_localsplit_status(uint16_t csid,uint64_t chunkid,uint32_t version,uint8_t status);

/* ---- */
uint8_t chunk_labelset_can_be_fulfilled(storagemode *sm);
void chunk_labelset_fix_matching_servers(storagemode *sm);

uint8_t chunk_no_more_pending_jobs(void);

int chunk_change_file(uint64_t chunkid,uint8_t prevsclassid,uint8_t newsclassid);
int chunk_delete_file(uint64_t chunkid,uint8_t sclassid);
int chunk_add_file(uint64_t chunkid,uint8_t sclassid);
int chunk_unlock(uint32_t ts,uint64_t chunkid);

void chunk_get_memusage(uint64_t allocated[6],uint64_t used[6]);

uint8_t chunk_is_afterload_needed(uint8_t mver);
int chunk_load(bio *fd,uint8_t mver,int ignoreflag);
uint8_t chunk_store(bio *fd);
void chunk_cleanup(void);
void chunk_newfs(void);
int chunk_strinit(void);

#endif
