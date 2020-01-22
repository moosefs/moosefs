/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <syslog.h>
#include <errno.h>


#include "charts.h"
#include "main.h"

#include "chunks.h"
#include "filesystem.h"
#include "matoclserv.h"
#include "memusage.h"
#include "cpuusage.h"
#include "matocsserv.h"

#include "chartsdefs.h"

#if 0
#define CHARTS_FILENAME "stats.mfs"

#define CHARTS_UCPU 0
#define CHARTS_SCPU 1
#define CHARTS_DELCHUNK 2
#define CHARTS_REPLCHUNK 3
#define CHARTS_STATFS 4
#define CHARTS_GETATTR 5
#define CHARTS_SETATTR 6
#define CHARTS_LOOKUP 7
#define CHARTS_MKDIR 8
#define CHARTS_RMDIR 9
#define CHARTS_SYMLINK 10
#define CHARTS_READLINK 11
#define CHARTS_MKNOD 12
#define CHARTS_UNLINK 13
#define CHARTS_RENAME 14
#define CHARTS_LINK 15
#define CHARTS_READDIR 16
#define CHARTS_OPEN 17
#define CHARTS_READ 18
#define CHARTS_WRITE 19
#define CHARTS_MEMORY_RSS 20
#define CHARTS_PACKETSRCVD 21
#define CHARTS_PACKETSSENT 22
#define CHARTS_BYTESRCVD 23
#define CHARTS_BYTESSENT 24
#define CHARTS_MEMORY_VIRT 25

#define CHARTS 26

/* name , join mode , percent , scale , multiplier , divisor */
#define STATDEFS { \
	{"ucpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"scpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"delete"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"replicate"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"statfs"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"getattr"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"setattr"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"lookup"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mkdir"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"rmdir"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"symlink"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"readlink"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mknod"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"unlink"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"rename"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"link"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"readdir"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"open"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"read"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"write"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"memoryrss"    ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"prcvd"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"psent"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"brcvd"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"bsent"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"memoryvirt"   ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{NULL           ,0              ,0,0                 ,   0, 0}  \
};

#define CALCDEFS { \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_MEMORY_VIRT,CHARTS_MEMORY_RSS))), \
	CHARTS_DEFS_END \
};

/* c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{CHARTS_DIRECT(CHARTS_UCPU)        ,CHARTS_DIRECT(CHARTS_SCPU)            ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{CHARTS_CALC(0)                    ,CHARTS_DIRECT(CHARTS_MEMORY_RSS)      ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_NONE                       ,CHARTS_NONE                           ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};
#endif
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
	uint32_t fsdata[16];
	uint32_t chunkops[12];
	uint32_t i;
	uint64_t total,avail;

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
	data[CHARTS_CHANGECHUNK]=chunkops[CHUNK_OP_CHANGE_TRY];
	data[CHARTS_DELETECHUNK_OK]=chunkops[CHUNK_OP_DELETE_OK];
	data[CHARTS_REPLICATECHUNK_OK]=chunkops[CHUNK_OP_REPLICATE_OK];
	data[CHARTS_CREATECHUNK_OK]=chunkops[CHUNK_OP_CREATE_OK];
	data[CHARTS_CHANGECHUNK_OK]=chunkops[CHUNK_OP_CHANGE_OK];
	data[CHARTS_DELETECHUNK_ERR]=chunkops[CHUNK_OP_DELETE_ERR];
	data[CHARTS_REPLICATECHUNK_ERR]=chunkops[CHUNK_OP_REPLICATE_ERR];
	data[CHARTS_CREATECHUNK_ERR]=chunkops[CHUNK_OP_CREATE_ERR];
	data[CHARTS_CHANGECHUNK_ERR]=chunkops[CHUNK_OP_CHANGE_ERR];

	fs_stats(fsdata);
	for (i=0 ; i<16 ; i++) {
		data[CHARTS_STATFS+i]=fsdata[i];
	}
	matoclserv_stats(data+CHARTS_PACKETSRCVD);

	matocsserv_getspace(&total,&avail,NULL);
	data[CHARTS_USED_SPACE]=total-avail;
	data[CHARTS_TOTAL_SPACE]=total;

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
