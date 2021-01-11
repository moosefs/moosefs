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

#ifndef _CHARTS_H_
#define _CHARTS_H_

#include <stdio.h>
#include <inttypes.h>

#define CHARTS_MODE_ADD 0
#define CHARTS_MODE_MAX 1

#define CHARTS_SCALE_MICRO 0
#define CHARTS_SCALE_MILI 1
#define CHARTS_SCALE_NONE 2
#define CHARTS_SCALE_KILO 3
#define CHARTS_SCALE_MEGA 4
#define CHARTS_SCALE_GIGA 5

#define CHARTS_OP_CONST 1000
#define CHARTS_OP_ADD 1001
#define CHARTS_OP_SUB 1002
#define CHARTS_OP_MIN 1003
#define CHARTS_OP_MAX 1004
#define CHARTS_OP_MUL 1005
#define CHARTS_OP_DIV 1006
#define CHARTS_OP_NEG 1007

#define CHARTS_OP_END 1999
#define CHARTS_DEFS_END 2000

#define CHARTS_CONST(x) CHARTS_OP_CONST, x
#define CHARTS_ADD(x,y) x, y, CHARTS_OP_ADD
#define CHARTS_SUB(x,y) x, y, CHARTS_OP_SUB
#define CHARTS_MIN(x,y) x, y, CHARTS_OP_MIN
#define CHARTS_MAX(x,y) x, y, CHARTS_OP_MAX
#define CHARTS_MUL(x,y) x, y, CHARTS_OP_MUL
#define CHARTS_DIV(x,y) x, y, CHARTS_OP_DIV
#define CHARTS_NEG(x) x, CHARTS_OP_NEG
#define CHARTS_CALCDEF(x) x, CHARTS_OP_END

#define CHARTS_NONE 0
#define CHARTS_DIRECT_START 1
#define CHARTS_DIRECT(x) ((x)+CHARTS_DIRECT_START)
#define CHARTS_CALC_START 1000
#define CHARTS_CALC(x) ((x)+CHARTS_CALC_START)

#if defined(UINT64_C)
#  define CHARTS_NODATA UINT64_C(0xFFFFFFFFFFFFFFFF)
#elif defined(UINT64_MAX)
#  define CHARTS_NODATA UINT64_MAX
#else
#  define CHARTS_NODATA 0xFFFFFFFFFFFFFFFFULL
#endif

typedef struct _statdef {
	char *name;
	uint32_t statid;
	uint8_t mode;
	uint8_t percent;
	uint8_t scale;
	uint16_t multiplier;
	uint16_t divisor;
} statdef;

typedef struct _estatdef {
	char *name;
	uint32_t statid;
	uint32_t c1src;
	uint32_t c2src;
	uint32_t c3src;
	uint8_t mode;
	uint8_t percent;
	uint8_t scale;
	uint16_t multiplier;
	uint16_t divisor;
} estatdef;

#if 0
// definition examples:

// simple data charts (source data counters, represented by one color simple charts):

/* name , join mode , percent , scale , multiplier , divisor */
#define STATDEFS { \
	{"cpu_time_counter"                 ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"network_data_in_bits_per_second"  ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"count_per_second"                 ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"count_per_minute"                 ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"micro_time_per_second"            ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MICRO,   1,60}, \
	{NULL                               ,0              ,0,0                 ,   0, 0}  \
};

#define CALCDEFS { \
	CHARTS_CALCDEF(CHARTS_ADD(CHARTS_TOADD1,CHARTS_TOADD2)), \
	CHARTS_CALCDEF(CHARTS_ADD(CHARTS_TOADD1,CHARTS_ADD(CHARTS_TOADD2,CHARTS_TOADD3))), \
	CHARTS_CALCDEF(CHARTS_DIV(CHARTS_TODIVIDE,CHARTS_DIVISOR)), \
	CHARTS_CALCDEF(CHARTS_ADD(CHARTS_TOADD,CHARTS_MUL(CHARTS_CONST(300),CHARTS_TOADDMULTIPLIED))), \
	CHARTS_DEFS_END \
};

// enchanced data charts (up to 3 counters on one chart represented by three colors)
// c1_def - light
// c2_def - medium
// c3_def - dark
// source data for given color can be defined as simple chart or calculation from above definitions

/* c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{CHARTS_DIRECT(CHARTS_UCPU)        ,CHARTS_DIRECT(CHARTS_SCPU)        ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{CHARTS_CALC(0)                    ,CHARTS_DIRECT(CHARTS_SOMETHING)   ,CHARTS_CALC(1)                    ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{CHARTS_NONE                       ,CHARTS_CALC(1)                    ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{CHARTS_NONE                       ,CHARTS_NONE                       ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};

#endif

uint64_t charts_get (uint32_t chartnumber,uint32_t count);

uint32_t charts_monotonic_data (uint8_t *buff);
uint32_t charts_getmaxleng(void);
void charts_getdata(double *data,uint32_t *timestamp,uint32_t *rsec,uint32_t number);
uint32_t charts_makedata(uint8_t *buff,uint32_t number,uint32_t maxentries);
uint32_t charts_make_png(uint32_t chartid,uint32_t chartwidth,uint32_t chartheight);
void charts_get_png(uint8_t *buff);

void charts_add (uint64_t *data,uint32_t datats);
void charts_store (void);
int charts_init (const uint32_t *calcs,const statdef *stats,const estatdef *estats,const char *filename,uint8_t mode);
void charts_term (void);

#endif
