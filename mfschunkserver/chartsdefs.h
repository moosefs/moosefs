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

#ifndef _CHARTSDEFS_H_
#define _CHARTSDEFS_H_

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
#define CHARTS_MOVELS 31
#define CHARTS_MOVEHS 32
#define CHARTS_CHANGE 33

#define CHARTS 34

#define STRID(a,b,c,d) (((((uint8_t)a)*256U+(uint8_t)b)*256U+(uint8_t)c)*256U+(uint8_t)d)

/* name , statid , join mode , percent , scale , multiplier , divisor */
#define STATDEFS { \
	{"ucpu"         ,STRID('U','C','P','U'),CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,   60}, \
	{"scpu"         ,STRID('S','C','P','U'),CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,   60}, \
	{"masterin"     ,STRID('M','S','T','I'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"masterout"    ,STRID('M','S','T','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csrepin"      ,STRID('R','E','P','I'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csrepout"     ,STRID('R','E','P','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csservin"     ,STRID('S','R','V','I'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"csservout"    ,STRID('S','R','V','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,   60}, \
	{"hdrbytesr"    ,STRID('H','R','B','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"hdrbytesw"    ,STRID('H','R','B','W'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"hdrllopr"     ,STRID('H','R','O','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"hdrllopw"     ,STRID('H','R','O','W'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"databytesr"   ,STRID('D','T','B','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"databytesw"   ,STRID('D','T','B','W'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,   60}, \
	{"datallopr"    ,STRID('D','T','O','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"datallopw"    ,STRID('D','T','O','W'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"hlopr"        ,STRID('H','L','O','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"hlopw"        ,STRID('H','L','O','W'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"rtime"        ,STRID('T','I','M','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MICRO,   1,60000}, \
	{"wtime"        ,STRID('T','I','M','W'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MICRO,   1,60000}, \
	{"repl"         ,STRID('R','E','P','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"create"       ,STRID('C','R','E','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"delete"       ,STRID('D','E','L','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"version"      ,STRID('V','E','R','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"duplicate"    ,STRID('D','U','P','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"truncate"     ,STRID('T','R','U','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"duptrunc"     ,STRID('D','T','R','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"test"         ,STRID('T','S','T','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"load"         ,STRID('L','O','A','D'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"memoryrss"    ,STRID('M','E','M','R'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"memoryvirt"   ,STRID('M','E','M','V'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"movels"       ,STRID('M','O','V','L'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"movehs"       ,STRID('M','O','V','H'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{"change"       ,STRID('C','H','G','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1,    1}, \
	{NULL           ,0                     ,0              ,0,0                 ,   0,    0}  \
};

#define CALCDEFS { \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_MEMORY_VIRT,CHARTS_MEMORY_RSS))), \
	CHARTS_DEFS_END \
};

/* name , statid , c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{"cpu"          ,STRID('T','C','P','U'),CHARTS_DIRECT(CHARTS_UCPU)        ,CHARTS_DIRECT(CHARTS_SCPU)        ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"bwin"         ,STRID('B','W','I','N'),CHARTS_DIRECT(CHARTS_CSREPIN)     ,CHARTS_DIRECT(CHARTS_CSSERVIN)    ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"bwout"        ,STRID('B','W','O','U'),CHARTS_DIRECT(CHARTS_CSREPOUT)    ,CHARTS_DIRECT(CHARTS_CSSERVOUT)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"hddread"      ,STRID('H','D','B','R'),CHARTS_DIRECT(CHARTS_HDRBYTESR)   ,CHARTS_DIRECT(CHARTS_DATABYTESR)  ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"hddwrite"     ,STRID('H','D','B','W'),CHARTS_DIRECT(CHARTS_HDRBYTESW)   ,CHARTS_DIRECT(CHARTS_DATABYTESW)  ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"hddopsr"      ,STRID('H','D','O','R'),CHARTS_DIRECT(CHARTS_HDRLLOPR)    ,CHARTS_DIRECT(CHARTS_DATALLOPR)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"hddopsw"      ,STRID('H','D','O','W'),CHARTS_DIRECT(CHARTS_HDRLLOPW)    ,CHARTS_DIRECT(CHARTS_DATALLOPW)   ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mem"          ,STRID('T','M','E','M'),CHARTS_CALC(0)                    ,CHARTS_DIRECT(CHARTS_MEMORY_RSS)  ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"move"         ,STRID('M','O','V','E'),CHARTS_DIRECT(CHARTS_MOVELS)      ,CHARTS_DIRECT(CHARTS_MOVEHS)      ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{NULL           ,0                     ,CHARTS_NONE                       ,CHARTS_NONE                       ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};

#endif
