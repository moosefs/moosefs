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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "charts.h"
#include "main.h"

#include "chunks.h"
#include "filesystem.h"
#include "matoclserv.h"
#include "memusage.h"
#include "cpuusage.h"
#include "matocsserv.h"
#include "csdb.h"

#include "chartsdefs.h"

static const uint32_t calcdefs[]=CALCDEFS
static const statdef statdefs[]=STATDEFS
static const estatdef estatdefs[]=ESTATDEFS

static uint64_t rss,virt;
static uint64_t scpu,ucpu;

void chartsdata_resusage(uint64_t *mem,uint64_t *syscpu,uint64_t *usrcpu) {
	*mem = rss;
	*syscpu = scpu;
	*usrcpu = ucpu;
}

void chartsdata_refresh(void) {
	uint64_t data[CHARTS];
	uint32_t fsdata[24];
	uint32_t fobj,mobj;
	uint64_t cldata[12];
	uint32_t chunkops[CHUNK_STATS_CNT];
	uint32_t i;
	uint64_t total,avail;
	uint32_t servers,disc_servers,mdisc_servers;

	for (i=0 ; i<CHARTS ; i++) {
		data[i]=CHARTS_NODATA;
	}

	cpu_used(&scpu,&ucpu);
	if (scpu>0 || ucpu>0) {
		data[CHARTS_UCPU] = (ucpu*6)/100;
		data[CHARTS_SCPU] = (scpu*6)/100;
	}

	if (mem_used(&rss,&virt)) {
		data[CHARTS_MEMORY_RSS] = rss;
		data[CHARTS_MEMORY_VIRT] = virt;
	}

	chunk_stats(chunkops);
	data[CHARTS_DELCHUNK]=chunkops[CHUNK_OP_DELETE_TRY];
	data[CHARTS_REPLCHUNK]=chunkops[CHUNK_OP_REPLICATE_TRY];
	data[CHARTS_CREATECHUNK]=chunkops[CHUNK_OP_CREATE_TRY];
	data[CHARTS_CHANGECHUNK]=chunkops[CHUNK_OP_CHANGE_TRY]+chunkops[CHUNK_OP_SPLIT_TRY];
	data[CHARTS_DELETECHUNK_OK]=chunkops[CHUNK_OP_DELETE_OK];
	data[CHARTS_REPLICATECHUNK_OK]=chunkops[CHUNK_OP_REPLICATE_OK];
	data[CHARTS_CREATECHUNK_OK]=chunkops[CHUNK_OP_CREATE_OK];
	data[CHARTS_CHANGECHUNK_OK]=chunkops[CHUNK_OP_CHANGE_OK];
	data[CHARTS_SPLITCHUNK_OK]=chunkops[CHUNK_OP_SPLIT_OK];
	data[CHARTS_DELETECHUNK_ERR]=chunkops[CHUNK_OP_DELETE_ERR];
	data[CHARTS_REPLICATECHUNK_ERR]=chunkops[CHUNK_OP_REPLICATE_ERR];
	data[CHARTS_CREATECHUNK_ERR]=chunkops[CHUNK_OP_CREATE_ERR];
	data[CHARTS_CHANGECHUNK_ERR]=chunkops[CHUNK_OP_CHANGE_ERR];
	data[CHARTS_SPLITCHUNK_ERR]=chunkops[CHUNK_OP_SPLIT_ERR];

	fs_stats(fsdata);
	for (i=0 ; i<16 ; i++) {
		data[CHARTS_STATFS+i]=fsdata[i];
	}
	for (i=0 ; i<8 ; i++) {
		data[CHARTS_SNAPSHOT+i]=fsdata[16+i];
	}
	matoclserv_stats(cldata);
	data[CHARTS_PACKETSRCVD] = cldata[0];
	data[CHARTS_PACKETSSENT] = cldata[1];
	data[CHARTS_BYTESRCVD] = cldata[2];
	data[CHARTS_BYTESSENT] = cldata[3];
	data[CHARTS_BYTESREAD] = cldata[4];
	data[CHARTS_BYTESWRITE] = cldata[5];
	data[CHARTS_READ] = cldata[6];
	data[CHARTS_WRITE] = cldata[7];
	data[CHARTS_FSYNC] = cldata[8];
	data[CHARTS_MOUNTS_BYTES_RECEIVED] = cldata[9];
	data[CHARTS_MOUNTS_BYTES_SENT] = cldata[10];
	data[CHARTS_LOCK] = cldata[11];

	matocsserv_getspace(&total,&avail,NULL);
	data[CHARTS_USED_SPACE]=total-avail;
	data[CHARTS_TOTAL_SPACE]=total;

	fs_charts_data(&fobj,&mobj);
	data[CHARTS_FILE_OBJECTS] = fobj;
	data[CHARTS_META_OBJECTS] = mobj;

	chunk_chart_data(data+CHARTS_COPY_CHUNKS,data+CHARTS_EC8_CHUNKS,data+CHARTS_EC4_CHUNKS,data+CHARTS_REG_ENDANGERED,data+CHARTS_REG_UNDERGOAL,data+CHARTS_ALL_ENDANGERED,data+CHARTS_ALL_UNDERGOAL);

	data[CHARTS_DELAY] = CHARTS_NODATA;

	csdb_get_server_counters(&servers,&disc_servers,&mdisc_servers);
	data[CHARTS_ALL_SERVERS] = servers;
	data[CHARTS_MDISC_SERVERS] = mdisc_servers;
	data[CHARTS_DISC_SERVERS] = disc_servers;

	data[CHARTS_USAGE_DIFF] = matocsserv_getusagediff();

	charts_add(data,main_time()-60);
}

void chartsdata_term(void) {
	chartsdata_refresh();
	charts_store();
	charts_term();
}

void chartsdata_store(void) {
	charts_store();
}

int chartsdata_init (void) {
	cpu_init();
	scpu = ucpu = 0;
	mem_used(&rss,&virt);

	main_time_register(60,0,chartsdata_refresh);
	main_time_register(3600,30,chartsdata_store);
	main_destruct_register(chartsdata_term);
	return charts_init(calcdefs,statdefs,estatdefs,CHARTS_FILENAME,0);
}
