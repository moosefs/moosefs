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

#ifndef _MATOCSSERV_H_
#define _MATOCSSERV_H_

#include <inttypes.h>
#include "chunks.h" // MAXCSCOUNT

uint8_t matocsserv_server_has_labels(void *e,uint32_t *labelmask);
uint32_t matocsserv_server_get_labelmask(void *e);
uint32_t matocsserv_server_get_ip(void *e);
uint16_t matocsserv_servers_with_labelsets(uint32_t *labelmask);
uint16_t matocsserv_servers_with_label(uint8_t label);
uint16_t matocsserv_servers_count(void);
uint16_t matocsserv_almostfull_servers(void);

// void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount);
// uint16_t matocsserv_getservers_ordered(uint16_t csids[MAXCSCOUNT],double maxusagediff,uint32_t *min,uint32_t *max);
void matocsserv_getservers_test(uint16_t *stdcscnt,uint16_t stdcsids[MAXCSCOUNT],uint16_t *olcscnt,uint16_t olcsids[MAXCSCOUNT],uint16_t *allcscnt,uint16_t allcsids[MAXCSCOUNT]);
uint16_t matocsserv_getservers_ordered(uint16_t csids[MAXCSCOUNT]);
uint16_t matocsserv_getservers_wrandom(uint16_t csids[MAXCSCOUNT],uint16_t *overloaded);
void matocsserv_useservers_wrandom(void* servers[MAXCSCOUNT],uint16_t cnt);
// uint16_t matocsserv_getservers_lessrepl(uint16_t csids[MAXCSCOUNT],double replimit,uint8_t highpriority,uint8_t *allservflag);

#define CSSTATE_NO_SPACE 3
#define CSSTATE_LIMIT_REACHED 2
#define CSSTATE_OVERLOADED 1
#define CSSTATE_OK 0

void matocsserv_get_server_groups(uint16_t csids[MAXCSCOUNT],double replimit,uint16_t counters[4]);


int matocsserv_have_availspace(void);
void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace,uint64_t *freespace);
char* matocsserv_getstrip(void *e);
int matocsserv_get_csdata(void *e,uint32_t *servip,uint16_t *servport,uint32_t *servver,uint32_t *servlabelmask);
void matocsserv_getservdata(void *e,uint32_t *ver,uint64_t *uspc,uint64_t *tspc,uint32_t *chunkcnt,uint64_t *tduspc,uint64_t *tdtspc,uint32_t *tdchunkcnt,uint32_t *errcnt,uint32_t *load,uint8_t *hlstatus,uint32_t *labelmask,uint8_t *mfrstatus);


void matocsserv_write_counters(void *e,uint8_t x);

uint8_t matocsserv_has_avail_space(void *e);
double matocsserv_get_usage(void *e);

double matocsserv_replication_write_counter(void *e,uint32_t now);
double matocsserv_replication_read_counter(void *e,uint32_t now);
uint16_t matocsserv_deletion_counter(void *e);

int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint32_t version,void *src);
int matocsserv_send_replicatechunk_raid(void *e,uint64_t chunkid,uint32_t version,uint8_t cnt,const uint32_t xormasks[4],void **src,uint64_t *srcchunkid,uint32_t *srcversion);
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
void matocsserv_close_lsock(void);
int matocsserv_init(void);

#endif
