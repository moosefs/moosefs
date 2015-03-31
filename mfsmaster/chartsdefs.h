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

#ifndef _CHARTSDEFS_H_
#define _CHARTSDEFS_H_

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

#define STRID(a,b,c,d) (((((uint8_t)a)*256U+(uint8_t)b)*256U+(uint8_t)c)*256U+(uint8_t)d)

/* name , statid , join mode , percent , scale , multiplier , divisor */
#define STATDEFS { \
	{"ucpu"         ,STRID('U','C','P','U'),CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"scpu"         ,STRID('S','C','P','U'),CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"delete"       ,STRID('D','E','L','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"replicate"    ,STRID('R','E','P','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"statfs"       ,STRID('S','T','F','S'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"getattr"      ,STRID('G','A','T','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"setattr"      ,STRID('S','A','T','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"lookup"       ,STRID('L','O','O','K'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mkdir"        ,STRID('M','K','D','I'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"rmdir"        ,STRID('R','M','D','I'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"symlink"      ,STRID('S','L','N','K'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"readlink"     ,STRID('R','L','N','K'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mknod"        ,STRID('M','K','N','D'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"unlink"       ,STRID('U','N','L','K'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"rename"       ,STRID('R','E','N','A'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"link"         ,STRID('L','I','N','K'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"readdir"      ,STRID('R','D','I','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"open"         ,STRID('O','P','E','N'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"read"         ,STRID('R','E','A','D'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"write"        ,STRID('W','R','I','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"memoryrss"    ,STRID('M','E','M','R'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"prcvd"        ,STRID('P','R','C','V'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"psent"        ,STRID('P','S','N','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"brcvd"        ,STRID('B','R','C','V'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"bsent"        ,STRID('B','S','N','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"memoryvirt"   ,STRID('M','E','M','V'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{NULL           ,0                     ,0              ,0,0                 ,   0, 0}  \
};

#define CALCDEFS { \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_MEMORY_VIRT,CHARTS_MEMORY_RSS))), \
	CHARTS_DEFS_END \
};

/* name , statid , c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{"cpu"          ,STRID('T','C','P','U'),CHARTS_DIRECT(CHARTS_UCPU)        ,CHARTS_DIRECT(CHARTS_SCPU)            ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"mem"          ,STRID('T','M','E','M'),CHARTS_CALC(0)                    ,CHARTS_DIRECT(CHARTS_MEMORY_RSS)      ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{NULL           ,0                     ,CHARTS_NONE                       ,CHARTS_NONE                           ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};

#endif
