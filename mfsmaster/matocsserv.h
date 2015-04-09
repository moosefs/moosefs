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

#ifndef _MATOCSSERV_H_
#define _MATOCSSERV_H_

#include <inttypes.h>
#include "chunks.h" // MAXCSCOUNT

void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount);
uint16_t matocsserv_almostfull_servers(void);
uint16_t matocsserv_getservers_ordered(uint16_t csids[MAXCSCOUNT],double maxusagediff,uint32_t *min,uint32_t *max);
uint16_t matocsserv_getservers_wrandom(uint16_t csids[MAXCSCOUNT],double tolerance,uint16_t demand);
uint16_t matocsserv_getservers_lessrepl(uint16_t csids[MAXCSCOUNT],double replimit);
void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace);
char* matocsserv_getstrip(void *e);
int matocsserv_get_csdata(void *e,uint32_t *servip,uint16_t *servport,uint32_t *servver);
void matocsserv_getservdata(void *e,uint32_t *ver,uint64_t *uspc,uint64_t *tspc,uint32_t *chunkcnt,uint64_t *tduspc,uint64_t *tdtspc,uint32_t *tdchunkcnt,uint32_t *errcnt,uint32_t *load);
uint8_t matocsserv_can_create_chunks(void *e,double tolerance);
double matocsserv_replication_write_counter(void *e,uint32_t now);
double matocsserv_replication_read_counter(void *e,uint32_t now);
//uint16_t matocsserv_replication_read_counter(void *e);
//uint16_t matocsserv_replication_write_counter(void *e);
uint16_t matocsserv_deletion_counter(void *e);
int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint32_t version,void *src);
int matocsserv_send_replicatechunk_xor(void *e,uint64_t chunkid,uint32_t version,uint8_t cnt,void **src,uint64_t *srcchunkid,uint32_t *srcversion);
int matocsserv_send_chunkop(void *e,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t leng);
int matocsserv_send_deletechunk(void *e,uint64_t chunkid,uint32_t version);
int matocsserv_send_createchunk(void *e,uint64_t chunkid,uint32_t version);
int matocsserv_send_setchunkversion(void *e,uint64_t chunkid,uint32_t version,uint32_t oldversion);
int matocsserv_send_duplicatechunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion);
int matocsserv_send_truncatechunk(void *e,uint64_t chunkid,uint32_t length,uint32_t version,uint32_t oldversion);
int matocsserv_send_duptruncchunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion,uint32_t length);
uint8_t matocsserv_isvalid(void *e);
void matocsserv_disconnection_finished(void *e);
int matocsserv_no_more_pending_jobs(void);
void matocsserv_disconnect_all(void);
int matocsserv_init(void);

#endif
