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

#ifndef _CHUNKS_H_
#define _CHUNKS_H_

#include <stdio.h>
#include <inttypes.h>
#include "bio.h"

#define MAXCSCOUNT 10000

/*
int chunk_create(uint64_t *chunkid,uint8_t goal);
int chunk_duplicate(uint64_t *chunkid,uint64_t oldchunkid,uint8_t goal);
int chunk_increase_version(uint64_t chunkid);
int chunk_truncate(uint64_t chunkid,uint32_t length);
int chunk_duptrunc(uint64_t *chunkid,uint64_t oldchunkid,uint32_t length,uint8_t goal);
int chunk_reinitialize(uint64_t chunkid);

void chunk_load_goal(void);
*/
int chunk_mr_multi_modify(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t opflag);
int chunk_mr_multi_truncate(uint32_t ts,uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal);
//int chunk_multi_reinitialize(uint32_t ts,uint64_t chunkid);
int chunk_mr_unlock(uint64_t chunkid);
int chunk_mr_increase_version(uint64_t chunkid);
int chunk_mr_set_version(uint64_t chunkid,uint32_t version);

int chunk_mr_nextchunkid(uint64_t nchunkid);
int chunk_mr_chunkadd(uint64_t chunkid,uint32_t version,uint32_t lockedto);
int chunk_mr_chunkdel(uint64_t chunkid,uint32_t version);

// void chunk_text_dump(FILE *fd);

void chunk_stats(uint32_t *del,uint32_t *repl);
void chunk_store_info(uint8_t *buff);
uint32_t chunk_get_missing_count(void);
void chunk_store_chunkcounters(uint8_t *buff,uint8_t matrixid);
uint32_t chunk_count(void);
void chunk_info(uint32_t *allchunks,uint32_t *allcopies,uint32_t *regcopies);
uint8_t chunk_counters_in_progress(void);

int chunk_get_validcopies(uint64_t chunkid,uint8_t *vcopies);

int chunk_multi_modify(uint64_t *nchunkid,uint64_t ochunkid,uint8_t goal,uint8_t *opflag);
int chunk_multi_truncate(uint64_t *nchunkid,uint64_t ochunkid,uint32_t length,uint8_t goal);
//int chunk_multi_reinitialize(uint64_t chunkid);
int chunk_repair(uint8_t goal,uint64_t ochunkid,uint32_t *nversion);

/* ---- */
uint8_t chunk_get_version_and_csdata(uint8_t mode,uint64_t chunkid,uint32_t cuip,uint32_t *version,uint8_t *count,uint8_t cs_data[100*10]);
/* ---- */
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
uint8_t chunk_no_more_pending_jobs(void);

int chunk_change_file(uint64_t chunkid,uint8_t prevgoal,uint8_t newgoal);
int chunk_delete_file(uint64_t chunkid,uint8_t goal);
int chunk_add_file(uint64_t chunkid,uint8_t goal);
int chunk_unlock(uint64_t chunkid);

void chunk_get_memusage(uint64_t allocated[3],uint64_t used[3]);

int chunk_load(bio *fd,uint8_t mver);
uint8_t chunk_store(bio *fd);
void chunk_cleanup(void);
void chunk_newfs(void);
int chunk_strinit(void);

#endif
