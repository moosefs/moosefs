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
#define CHARTS_READCHUNK 18
#define CHARTS_WRITECHUNK 19
#define CHARTS_MEMORY_RSS 20
#define CHARTS_PACKETSRCVD 21
#define CHARTS_PACKETSSENT 22
#define CHARTS_BYTESRCVD 23
#define CHARTS_BYTESSENT 24
#define CHARTS_MEMORY_VIRT 25
#define CHARTS_USED_SPACE 26
#define CHARTS_TOTAL_SPACE 27
#define CHARTS_CREATECHUNK 28
#define CHARTS_CHANGECHUNK 29
#define CHARTS_DELETECHUNK_OK 30
#define CHARTS_DELETECHUNK_ERR 31
#define CHARTS_REPLICATECHUNK_OK 32
#define CHARTS_REPLICATECHUNK_ERR 33
#define CHARTS_CREATECHUNK_OK 34
#define CHARTS_CREATECHUNK_ERR 35
#define CHARTS_CHANGECHUNK_OK 36
#define CHARTS_CHANGECHUNK_ERR 37
#define CHARTS_SPLITCHUNK_OK 38
#define CHARTS_SPLITCHUNK_ERR 39
#define CHARTS_FILE_OBJECTS 40
#define CHARTS_META_OBJECTS 41
#define CHARTS_EC8_CHUNKS 42
#define CHARTS_EC4_CHUNKS 43
#define CHARTS_COPY_CHUNKS 44
#define CHARTS_REG_ENDANGERED 45
#define CHARTS_REG_UNDERGOAL 46
#define CHARTS_ALL_ENDANGERED 47
#define CHARTS_ALL_UNDERGOAL 48
#define CHARTS_BYTESREAD 49
#define CHARTS_BYTESWRITE 50
#define CHARTS_READ 51
#define CHARTS_WRITE 52
#define CHARTS_FSYNC 53
#define CHARTS_LOCK 54
#define CHARTS_SNAPSHOT 55
#define CHARTS_TRUNCATE 56
#define CHARTS_GETXATTR 57
#define CHARTS_SETXATTR 58
#define CHARTS_GETFACL 59
#define CHARTS_SETFACL 60
#define CHARTS_CREATE 61
#define CHARTS_META 62
#define CHARTS_DELAY 63
#define CHARTS_ALL_SERVERS 64
#define CHARTS_MDISC_SERVERS 65
#define CHARTS_DISC_SERVERS 66
#define CHARTS_USAGE_DIFF 67
#define CHARTS_MOUNTS_BYTES_RECEIVED 68
#define CHARTS_MOUNTS_BYTES_SENT 69

#define CHARTS 70

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
	{"readchunk"    ,STRID('R','E','A','D'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"writechunk"   ,STRID('W','R','I','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"memoryrss"    ,STRID('M','E','M','R'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"prcvd"        ,STRID('P','R','C','V'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"psent"        ,STRID('P','S','N','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"brcvd"        ,STRID('B','R','C','V'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"bsent"        ,STRID('B','S','N','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,8000,60}, \
	{"memoryvirt"   ,STRID('M','E','M','V'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"usedspace"    ,STRID('U','S','P','C'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"totalspace"   ,STRID('T','S','P','C'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"create"       ,STRID('N','E','W','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"change"       ,STRID('I','N','T','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"delete_ok"    ,STRID('D','E','L','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"delete_err"   ,STRID('D','E','L','E'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"replicate_ok" ,STRID('R','E','P','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"replicate_err",STRID('R','E','P','E'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"create_ok"    ,STRID('N','E','W','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"create_err"   ,STRID('N','E','W','E'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"change_ok"    ,STRID('I','N','T','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"change_err"   ,STRID('I','N','T','E'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"split_ok"     ,STRID('S','P','L','O'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"split_err"    ,STRID('S','P','L','E'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"fileobjects"  ,STRID('F','I','L','O'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"metaobjects"  ,STRID('M','E','T','O'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chunksec8"    ,STRID('C','E','C','8'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chunksec4"    ,STRID('C','E','C','4'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chunkscopy"   ,STRID('C','C','P','Y'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chregdanger"  ,STRID('C','H','R','E'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chregunder"   ,STRID('C','H','R','U'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"challdanger"  ,STRID('C','H','A','E'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"challunder"   ,STRID('C','H','A','U'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"bytesread"    ,STRID('B','Y','T','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"byteswrite"   ,STRID('B','Y','T','W'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"read"         ,STRID('R','D','O','P'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"write"        ,STRID('W','R','O','P'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"fsync"        ,STRID('F','S','N','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"lock"         ,STRID('L','O','C','K'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"snapshot"     ,STRID('S','N','A','P'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"truncate"     ,STRID('T','R','U','N'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"getxattr"     ,STRID('G','X','A','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"setxattr"     ,STRID('S','X','A','T'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"getfacl"      ,STRID('G','F','A','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"setfacl"      ,STRID('S','F','A','C'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"fcreate"      ,STRID('C','R','E','A'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"meta"         ,STRID('M','E','T','A'),CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"delay"        ,STRID('D','E','L','Y'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"servers"      ,STRID('S','E','R','V'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"mdservers"    ,STRID('M','D','S','R'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"dservers"     ,STRID('D','S','R','V'),CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"udiff"        ,STRID('U','D','I','F'),CHARTS_MODE_MAX,0,CHARTS_SCALE_MILI ,   1, 1}, \
	{"mountbytrcvd" ,STRID('M','B','Y','R'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{"mountbytsent" ,STRID('M','B','Y','S'),CHARTS_MODE_ADD,0,CHARTS_SCALE_MILI ,1000,60}, \
	{NULL           ,0                     ,0              ,0,0                 ,   0, 0}  \
};

#define CALCDEFS { \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_MEMORY_VIRT,CHARTS_MEMORY_RSS))), \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_TOTAL_SPACE,CHARTS_USED_SPACE))), \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_ALL_SERVERS,CHARTS_DISC_SERVERS))), \
	CHARTS_CALCDEF(CHARTS_MAX(CHARTS_CONST(0),CHARTS_SUB(CHARTS_DISC_SERVERS,CHARTS_MDISC_SERVERS))), \
	CHARTS_DEFS_END \
};

/* name , statid , c1_def , c2_def , c3_def , join mode , percent , scale , multiplier , divisor */
#define ESTATDEFS { \
	{"cpu"            ,STRID('T','C','P','U'),CHARTS_DIRECT(CHARTS_UCPU)                 ,CHARTS_DIRECT(CHARTS_SCPU)                ,CHARTS_NONE                       ,CHARTS_MODE_ADD,1,CHARTS_SCALE_MICRO, 100,60}, \
	{"mem"            ,STRID('T','M','E','M'),CHARTS_CALC(0)                             ,CHARTS_DIRECT(CHARTS_MEMORY_RSS)          ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"space"          ,STRID('S','P','A','C'),CHARTS_CALC(1)                             ,CHARTS_DIRECT(CHARTS_USED_SPACE)          ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"delete_stat"    ,STRID('D','E','L','S'),CHARTS_DIRECT(CHARTS_DELETECHUNK_OK)       ,CHARTS_DIRECT(CHARTS_DELETECHUNK_ERR)     ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"replicate_stat" ,STRID('R','E','P','S'),CHARTS_DIRECT(CHARTS_REPLICATECHUNK_OK)    ,CHARTS_DIRECT(CHARTS_REPLICATECHUNK_ERR)  ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"create_stat"    ,STRID('N','E','W','S'),CHARTS_DIRECT(CHARTS_CREATECHUNK_OK)       ,CHARTS_DIRECT(CHARTS_CREATECHUNK_ERR)     ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"change_stat"    ,STRID('I','N','T','S'),CHARTS_DIRECT(CHARTS_CHANGECHUNK_OK)       ,CHARTS_DIRECT(CHARTS_CHANGECHUNK_ERR)     ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"split_stat"     ,STRID('S','P','L','S'),CHARTS_DIRECT(CHARTS_SPLITCHUNK_OK)        ,CHARTS_DIRECT(CHARTS_SPLITCHUNK_ERR)      ,CHARTS_NONE                       ,CHARTS_MODE_ADD,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"objects"        ,STRID('O','B','J','T'),CHARTS_DIRECT(CHARTS_FILE_OBJECTS)         ,CHARTS_DIRECT(CHARTS_META_OBJECTS)        ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"chunks"         ,STRID('C','H','N','K'),CHARTS_DIRECT(CHARTS_COPY_CHUNKS)          ,CHARTS_DIRECT(CHARTS_EC4_CHUNKS)          ,CHARTS_DIRECT(CHARTS_EC8_CHUNKS)  ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"regunder"       ,STRID('R','U','N','D'),CHARTS_DIRECT(CHARTS_REG_UNDERGOAL)        ,CHARTS_DIRECT(CHARTS_REG_ENDANGERED)      ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"allunder"       ,STRID('A','U','N','D'),CHARTS_DIRECT(CHARTS_ALL_UNDERGOAL)        ,CHARTS_DIRECT(CHARTS_ALL_ENDANGERED)      ,CHARTS_NONE                       ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{"cservers"       ,STRID('C','S','R','V'),CHARTS_CALC(2)                             ,CHARTS_DIRECT(CHARTS_MDISC_SERVERS)       ,CHARTS_CALC(3)                    ,CHARTS_MODE_MAX,0,CHARTS_SCALE_NONE ,   1, 1}, \
	{NULL             ,0                     ,CHARTS_NONE                                ,CHARTS_NONE                               ,CHARTS_NONE                       ,0              ,0,0                 ,   0, 0}  \
};

#endif
