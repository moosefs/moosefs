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

#include "bgjobs.h"
#include "csserv.h"
#include "mainserv.h"
#include "masterconn.h"
#include "hddspacemgr.h"
#include "replicator.h"

#include "cpuusage.h"
#include "memusage.h"

#include "chartsdefs.h"

static const uint32_t calcdefs[]=CALCDEFS
static const statdef statdefs[]=STATDEFS
static const estatdef estatdefs[]=ESTATDEFS

void chartsdata_refresh(void) {
	uint64_t data[CHARTS];
	uint64_t bin,bout;
	uint32_t i,opr,opw,dbr,dbw,dopr,dopw,movl,movh,repl;
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
	hdd_stats(&bin,&bout,&opr,&opw,&dbr,&dbw,&dopr,&dopw,&movl,&movh,data+CHARTS_RTIME,data+CHARTS_WTIME);
	data[CHARTS_HDRBYTESR]=bin;
	data[CHARTS_HDRBYTESW]=bout;
	data[CHARTS_HDRLLOPR]=opr;
	data[CHARTS_HDRLLOPW]=opw;
	data[CHARTS_DATABYTESR]=dbr;
	data[CHARTS_DATABYTESW]=dbw;
	data[CHARTS_DATALLOPR]=dopr;
	data[CHARTS_DATALLOPW]=dopw;
	data[CHARTS_MOVELS]=movl;
	data[CHARTS_MOVEHS]=movh;
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
	data[CHARTS_CHANGE]=op_ve+op_du+op_tr+op_dt;

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
	main_time_register(3600,30,chartsdata_store);
	main_destruct_register(chartsdata_term);
	return charts_init(calcdefs,statdefs,estatdefs,CHARTS_FILENAME,0);
}
