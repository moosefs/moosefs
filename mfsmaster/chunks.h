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

#ifndef _CHUNKS_H_
#define _CHUNKS_H_

#include <stdio.h>
#include <inttypes.h>
#include "bio.h"

#define MAXCSCOUNT 10000

#define CHUNK_OP_DELETE_TRY     0
#define CHUNK_OP_REPLICATE_TRY  1
#define CHUNK_OP_CREATE_TRY     2
#define CHUNK_OP_CHANGE_TRY     3
#define CHUNK_OP_DELETE_OK      4
#define CHUNK_OP_REPLICATE_OK   5
#define CHUNK_OP_CREATE_OK      6
#define CHUNK_OP_CHANGE_OK      7
#define CHUNK_OP_DELETE_ERR     8
#define CHUNK_OP_REPLICATE_ERR  9
#define CHUNK_OP_CREATE_ERR    10
#define CHUNK_OP_CHANGE_ERR    11

typedef enum {
	CHUNK_FLOOP_NOTFOUND,
	CHUNK_FLOOP_DELETED,
	CHUNK_FLOOP_MISSING_NOCOPY,
	CHUNK_FLOOP_MISSING_INVALID,
	CHUNK_FLOOP_MISSING_WRONGVERSION,
	CHUNK_FLOOP_UNDERGOAL_AFLAG_NOT_CHANGED,
	CHUNK_FLOOP_UNDERGOAL_AFLAG_CHANGED,
	CHUNK_FLOOP_OK_AFLAG_NOT_CHANGED,
	CHUNK_FLOOP_OK_AFLAG_CHANGED
} chunkfloop;

int chunk_mr_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t opflag);
int chunk_mr_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid);
//int chunk_multi_reinitialize(uint32_t ts,uint64_t chunkid);
int chunk_mr_unlock(uint64_t chunkid);
int chunk_mr_increase_version(uint64_t chunkid);
int chunk_mr_set_version(uint64_t chunkid,uint32_t version);

int chunk_mr_nextchunkid(uint64_t nchunkid);
int chunk_mr_chunkadd(uint32_t ts,uint64_t chunkid,uint32_t version,uint32_t lockedto);
int chunk_mr_chunkdel(uint32_t ts,uint64_t chunkid,uint32_t version);

// void chunk_text_dump(FILE *fd);

void chunk_stats(uint32_t chunkops[12]);
void chunk_store_info(uint8_t *buff);
uint32_t chunk_get_missing_count(void);
void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid);
uint32_t chunk_count(void);
void chunk_sclass_counters(uint8_t sclassid,uint8_t archflag,uint8_t goal,uint64_t *undergoal,uint64_t *exactgoal,uint64_t *overgoal);
void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regcopies);
uint8_t chunk_counters_in_progress(void);

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies);

int chunk_get_archflag(uint64_t chunkid,uint8_t *archflag);
int chunk_univ_archflag(uint64_t chunkid,uint8_t archflag,uint32_t *archflagchanged);
chunkfloop chunk_fileloop_task(uint64_t chunkid,uint8_t sclassid,uint8_t aftereof,uint8_t archflag);

int chunk_read_check(uint32_t ts,uint64_t chunkid);
int chunk_multi_modify(uint8_t continueop,uint64_t *nchunkid,uint64_t ochunkid,uint8_t sclassid,uint8_t *opflag,uint32_t clientip);
int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t sclassid);
//int chunk_multi_reinitialize(uint64_t chunkid);
int chunk_repair(uint8_t sclassid,uint64_t ochunkid,uint32_t *nversion);

int chunk_locked_or_busy(void *cptr);

/* ---- */

uint8_t chunk_get_version_and_csdata(uint8_t mode,uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t cs_data[100*14]);
uint8_t chunk_get_version_and_copies(uint64_t chunkid,uint32_t *version,uint8_t *count,uint8_t cs_data[100*7]);
uint8_t chunk_get_copies(uint64_t chunkid,uint8_t *count);
uint8_t chunk_get_version(uint64_t chunkid,uint32_t *version);

/* ---- */
uint8_t chunk_get_mfrstatus(uint16_t csid);

uint16_t chunk_server_connected(void *ptr);

void chunk_server_has_chunk(uint16_t csid,uint64_t chunkid,uint32_t version);
void chunk_damaged(uint16_t csid,uint64_t chunkid);
void chunk_lost(uint16_t csid,uint64_t chunkid);
void chunk_server_register_end(uint16_t csid);
void chunk_server_disconnected(uint16_t csid);

void chunk_got_delete_status(uint16_t csid,uint64_t chunkid,uint8_t status);
void chunk_got_replicate_status(uint16_t csid,uint64_t chunkid,uint32_t version,uint8_t status);

void chunk_got_chunkop_status(uint16_t csid,uint64_t chunkid,uint8_t status);

void chunk_got_create_status(uint16_t csid,uint64_t chunkid,uint8_t status);
void chunk_got_duplicate_status(uint16_t csid,uint64_t chunkid,uint8_t status);
void chunk_got_setversion_status(uint16_t csid,uint64_t chunkid,uint8_t status);
void chunk_got_truncate_status(uint16_t csid,uint64_t chunkid,uint8_t status);
void chunk_got_duptrunc_status(uint16_t csid,uint64_t chunkid,uint8_t status);

/* ---- */
uint8_t chunk_labelset_can_be_fulfilled(uint8_t labelcnt,uint32_t **labelmasks);

uint8_t chunk_no_more_pending_jobs(void);

int chunk_change_file(uint64_t chunkid,uint8_t prevsclassid,uint8_t newsclassid);
int chunk_delete_file(uint64_t chunkid,uint8_t sclassid);
int chunk_add_file(uint64_t chunkid,uint8_t sclassid);
int chunk_unlock(uint64_t chunkid);

void chunk_get_memusage(uint64_t allocated[3],uint64_t used[3]);

int chunk_load(bio *fd,uint8_t mver);
uint8_t chunk_store(bio *fd);
void chunk_cleanup(void);
void chunk_newfs(void);
int chunk_strinit(void);

#endif
