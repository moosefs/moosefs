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

#include "bgjobs.h"
#include "csserv.h"
#include "mainserv.h"
#include "masterconn.h"
#include "hddspacemgr.h"
#include "replicator.h"

#include "cpuusage.h"
#include "memusage.h"

#define CHARTS_FILENAME "csstats.mfs"

#define CHARTS_UCPU 0
#define CHARTS_SCPU 1
#define CHARTS_MASTERIN 2
#define CHARTS_MASTEROUT 3
#define CHARTS_CSREPIN 4
#define CHARTS_CSREPOUT 5
#define CHARTS_CSSERVIN 6
#define CHARTS_CSSERVOUT 7
#define CHARTS_HDRBYTESR 8
#define CHARTS_HDRBYTESW 9
#define CHARTS_HDRLLOPR 10
#define CHARTS_HDRLLOPW 11
#define CHARTS_DATABYTESR 12
#define CHARTS_DATABYTESW 13
#define CHARTS_DATALLOPR 14
#define CHARTS_DATALLOPW 15
#define CHARTS_HLOPR 16
#define CHARTS_HLOPW 17
#define CHARTS_RTIME 18
#define CHARTS_WTIME 19
#define CHARTS_REPL 20
#define CHARTS_CREATE 21
#define CHARTS_DELETE 22
#define CHARTS_VERSION 23
#define CHARTS_DUPLICATE 24
#define CHARTS_TRUNCATE 25
#define CHARTS_DUPTRUNC 26
#define CHARTS_TEST 27
#define CHARTS_LOAD 28
#define CHARTS_MEMORY_RSS 29
#define CHARTS_MEMORY_VIRT 30

#define CHARTS 32

/* name , join mode , percent , scale , multiplier , divisor */
#define STATDEFS { \
	{"ucpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,   60}, \
	{"scpu"         ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,   60}, \
	{"masterin"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"masterout"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csrepin"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csrepout"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csservin"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csservout"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"bytesr"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"bytesw"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"llopr"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"llopw"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"databytesr"   ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"databytesw"   ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"datallopr"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"datallopw"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"hlopr"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"hlopw"        ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"rntime"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MICRO,   1,60000}, \
	{"wntime"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MICRO,   1,60000}, \
	{"repl"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"create"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"delete"       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"version"      ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"duplicate"    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"truncate"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"duptrunc"     ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"test"         ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"load"         ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"memoryrss"    ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"memoryvirt"   ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{NULL           ,0              ,0,0                 ,   0,    0}  \
};

#define CALCDEFS { \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_MEMORY_VIRT,CHARTS_MEMORY_RSS))), \
	CHARTS_DEFS_END \
};

/* c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{CHARTS_DIRECT(CHARTS_UCPU)        ,CHARTS_DIRECT(CHARTS_SCPU)        ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{CHARTS_DIRECT(CHARTS_CSREPIN)     ,CHARTS_DIRECT(CHARTS_CSSERVIN)    ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{CHARTS_DIRECT(CHARTS_CSREPOUT)    ,CHARTS_DIRECT(CHARTS_CSSERVOUT)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{CHARTS_DIRECT(CHARTS_HDRBYTESR)   ,CHARTS_DIRECT(CHARTS_DATABYTESR)  ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{CHARTS_DIRECT(CHARTS_HDRBYTESW)   ,CHARTS_DIRECT(CHARTS_DATABYTESW)  ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{CHARTS_DIRECT(CHARTS_HDRLLOPR)    ,CHARTS_DIRECT(CHARTS_DATALLOPR)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_DIRECT(CHARTS_HDRLLOPW)    ,CHARTS_DIRECT(CHARTS_DATALLOPW)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_CALC(0)                    ,CHARTS_DIRECT(CHARTS_MEMORY_RSS)  ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_NONE                       ,CHARTS_NONE                       ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};

static const uint32_t calcdefs[]=CALCDEFS
static const statdef statdefs[]=STATDEFS
static const estatdef estatdefs[]=ESTATDEFS

void chartsdata_refresh(void) {
	uint64_t data[CHARTS];
	uint64_t bin,bout;
	uint32_t i,opr,opw,dbr,dbw,dopr,dopw,repl;
	uint32_t op_cr,op_de,op_ve,op_du,op_tr,op_dt,op_te;
	uint32_t jobs;
	uint64_t scpu,ucpu;
	uint64_t rss,virt;

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

	masterconn_stats(data+CHARTS_MASTERIN,data+CHARTS_MASTEROUT);
	job_stats(&jobs);
	data[CHARTS_LOAD]=jobs;
	csserv_stats(data+CHARTS_CSSERVIN,data+CHARTS_CSSERVOUT);
	mainserv_stats(&bin,&bout,&opr,&opw);
	data[CHARTS_CSSERVIN]+=bin;
	data[CHARTS_CSSERVOUT]+=bout;
	data[CHARTS_HLOPR]=opr;
	data[CHARTS_HLOPW]=opw;
	hdd_stats(&bin,&bout,&opr,&opw,&dbr,&dbw,&dopr,&dopw,data+CHARTS_RTIME,data+CHARTS_WTIME);
	data[CHARTS_HDRBYTESR]=bin;
	data[CHARTS_HDRBYTESW]=bout;
	data[CHARTS_HDRLLOPR]=opr;
	data[CHARTS_HDRLLOPW]=opw;
	data[CHARTS_DATABYTESR]=dbr;
	data[CHARTS_DATABYTESW]=dbw;
	data[CHARTS_DATALLOPR]=dopr;
	data[CHARTS_DATALLOPW]=dopw;
	replicator_stats(data+CHARTS_CSREPIN,data+CHARTS_CSREPOUT,&repl);
	data[CHARTS_REPL]=repl;
	hdd_op_stats(&op_cr,&op_de,&op_ve,&op_du,&op_tr,&op_dt,&op_te);
	data[CHARTS_CREATE]=op_cr;
	data[CHARTS_DELETE]=op_de;
	data[CHARTS_VERSION]=op_ve;
	data[CHARTS_DUPLICATE]=op_du;
	data[CHARTS_TRUNCATE]=op_tr;
	data[CHARTS_DUPTRUNC]=op_dt;
	data[CHARTS_TEST]=op_te;

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

	main_time_register(60,0,chartsdata_refresh);
	main_time_register(3600,0,chartsdata_store);
	main_destruct_register(chartsdata_term);
	return charts_init(calcdefs,statdefs,estatdefs,CHARTS_FILENAME);
}
