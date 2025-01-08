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

#ifndef _MATOCSSERV_H_
#define _MATOCSSERV_H_

#include <inttypes.h>
#include "chunks.h" // MAXCSCOUNT
#include "storageclass.h" // storagemode

enum {
	REPL_COPY_IO,
	REPL_COPY_ENDANGERED,
	REPL_COPY_UNDERGOAL,
	REPL_COPY_WRONGLABEL,
	REPL_COPY_REBALANCE,
	REPL_EC_ENDANGERED,
	REPL_EC_UNDERGOAL,
	REPL_EC_WRONGLABEL,
	REPL_EC_REBALANCE,
	LOCALSPLIT_TO_EC4,
	LOCALSPLIT_TO_EC8,
	JOIN_EC_IO,
	JOIN_EC_CHANGE,
	JOIN_EC_NOSERVERS,
	JOIN_EC_GENERIC,
	SPLIT_EC_GENERIC,
	RECOVER_IO,
	REPL_REASONS
};

enum {
	OP_GENERIC_IO,
	OP_DEL_INVALID,
	OP_DEL_NOTUSED,
	OP_DEL_OVERGOAL,
	OP_REASONS
};

#define REPL_REASONS_STRINGS \
	"COPY I/O", \
	"COPY ENDANGERED", \
	"COPY UNDERGOAL", \
	"COPY WRONG LABEL", \
	"COPY REBALANCE", \
	"EC ENDANGERED", \
	"EC UNDERGOAL", \
	"EC WRONG LABEL", \
	"EC REBALANCE", \
	"LOCAL-SPLIT TO EC4", \
	"LOCAL-SPLIT TO EC8", \
	"JOIN EC I/O", \
	"JOIN EC CHANGE", \
	"JOIN EC NOT ENOUGH SERVERS", \
	"JOIN EC GENERIC", \
	"SPLIT EC GENERIC", \
	"RECOVER EC I/O"

#define OP_REASONS_STRINGS \
	"GENERIC I/O", \
	"DELETE INVALID", \
	"DELETE NOT NEEDED", \
	"DELETE OVERGOAL"

uint8_t matocsserv_server_matches_labelexpr(void *e,const uint8_t labelexpr[SCLASS_EXPR_MAX_SIZE]);
uint32_t matocsserv_server_get_labelmask(void *e);
uint32_t matocsserv_server_get_ip(void *e);
uint16_t matocsserv_servers_matches_labelexpr(const uint8_t labelexpr[SCLASS_EXPR_MAX_SIZE]);
// void matocsserv_replallowed_servers_matches_labelexpr(const uint8_t labelexpr[SCLASS_EXPR_MAX_SIZE],uint16_t *replallowed,uint16_t *allvalid);
void matocsserv_recalculate_storagemode_scounts(storagemode *sm);
uint16_t matocsserv_servers_with_label(uint8_t label);
uint16_t matocsserv_servers_count(void);
uint16_t matocsserv_almostfull_servers(void);
uint16_t matocsserv_replallowed_servers(void);

uint8_t matocsserv_receiving_chunks_state(void);

// void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount);
// uint16_t matocsserv_getservers_ordered(uint16_t csids[MAXCSCOUNT],double maxusagediff,uint32_t *min,uint32_t *max);
uint16_t matocsserv_getservers_replpossible(uint16_t csids[MAXCSCOUNT]);
uint16_t matocsserv_getservers_replallowed(uint16_t csids[MAXCSCOUNT]);
void matocsserv_getservers_test(uint16_t *stdcscnt,uint16_t stdcsids[MAXCSCOUNT],uint16_t *olcscnt,uint16_t olcsids[MAXCSCOUNT],uint16_t *allcscnt,uint16_t allcsids[MAXCSCOUNT]);
uint16_t matocsserv_getservers_ordered(uint16_t csids[MAXCSCOUNT]);
uint16_t matocsserv_getservers_wrandom(uint16_t csids[MAXCSCOUNT],uint16_t *overloaded);
void matocsserv_useservers_wrandom(void* servers[MAXCSCOUNT],uint16_t cnt);
uint16_t matocsserv_getservers_lessrepl(uint16_t csids[MAXCSCOUNT],double replimit,uint8_t highpriority,uint8_t *allservflag);

#define CSSTATE_NO_SPACE 3
#define CSSTATE_LIMIT_REACHED 2
#define CSSTATE_OVERLOADED 1
#define CSSTATE_OK 0

void matocsserv_get_server_groups(uint16_t csids[MAXCSCOUNT],double replimit,uint16_t counters[4]);

uint64_t matocsserv_gettotalspace(void);
uint64_t matocsserv_getusedspace(void);
uint32_t matocsserv_getusagediff(void);

int matocsserv_have_availspace(void);
void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace,uint64_t *freespace);
char* matocsserv_getstrip(void *e);
int matocsserv_get_csdata(void *e,uint32_t clientip,uint32_t *servip,uint16_t *servport,uint32_t *servver,uint32_t *servlabelmask);
void matocsserv_getservdata(void *e,uint32_t *ver,uint64_t *uspc,uint64_t *tspc,uint32_t *chunkcnt,uint64_t *tduspc,uint64_t *tdtspc,uint32_t *tdchunkcnt,uint32_t *errcnt,uint32_t *load,uint8_t *hlstatus,uint32_t *labelmask,uint8_t *mfrstatus);

uint8_t matocsserv_csdb_force_disconnect(void *e,void *p);

void matocsserv_write_counters(void *e,uint8_t x);

uint8_t matocsserv_has_avail_space(void *e);
double matocsserv_get_usage(void *e);

double matocsserv_replication_write_counter(void *e,uint32_t now);
double matocsserv_replication_read_counter(void *e,uint32_t now);
uint16_t matocsserv_deletion_counter(void *e);

void matocsserv_broadcast_chunk_status(uint64_t chunkid);

int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,void *src,uint8_t reason);
int matocsserv_send_replicatechunk_split(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,void *src,uint8_t srcecid,uint8_t partno,uint8_t parts,uint8_t reason);
int matocsserv_send_replicatechunk_recover(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t parts,void **survivors,uint8_t *survivorecids,uint8_t reason);
int matocsserv_send_replicatechunk_join(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t parts,void **survivors,uint8_t *survivorecids,uint8_t reason);
int matocsserv_send_chunkop(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint8_t copyecid,uint32_t copyversion,uint32_t leng);
int matocsserv_send_deletechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t reason);
int matocsserv_send_createchunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version);
int matocsserv_send_setchunkversion(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint32_t oldversion);
int matocsserv_send_duplicatechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint64_t oldchunkid,uint8_t oldecid,uint32_t oldversion);
int matocsserv_send_truncatechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t length,uint32_t version,uint32_t oldversion);
int matocsserv_send_duptruncchunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint64_t oldchunkid,uint8_t oldecid,uint32_t oldversion,uint32_t length);
int matocsserv_send_localsplitchunk(void *e,uint64_t chunkid,uint32_t version,uint32_t missingmask,uint8_t parts,uint8_t reason);

int matocsserv_can_split_chunks(void *e,uint8_t ecmode);

void matocsserv_broadcast_regfirst_chunk(uint64_t chunkid);

uint8_t matocsserv_isvalid(void *e);

uint32_t matocsserv_get_min_cs_version(void);

void matocsserv_disconnection_finished(void *e);
int matocsserv_no_more_pending_jobs(void);
void matocsserv_disconnect_all(void);
void matocsserv_close_lsock(void);
int matocsserv_init(void);

#endif
