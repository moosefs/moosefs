#!/usr/bin/env python3

# Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
# 
# This file is part of MooseFS.
# 
# MooseFS is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 2 (only).
# 
# MooseFS is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with MooseFS; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
# or visit http://www.gnu.org/licenses/gpl-2.0.html

import os
import sys

if sys.version_info[0]<3 or (sys.version_info[0]==3 and sys.version_info[1]<4):
	print("Unsupported python version, minimum required version is 3.4")
	sys.exit(1)

import struct
import time
import traceback
import codecs
import json
import getopt

from common.constants_ac import * # MFS_INLINE_IMPORT
from common.constants import *    # MFS_INLINE_IMPORT
from common.errors import *       # MFS_INLINE_IMPORT
from common.utils import *        # MFS_INLINE_IMPORT
from common.conn import *         # MFS_INLINE_IMPORT
from common.models import *       # MFS_INLINE_IMPORT
from common.cluster import *      # MFS_INLINE_IMPORT
from common.dataprovider import * # MFS_INLINE_IMPORT
from common.organization import * # MFS_INLINE_IMPORT

donotresolve = 0     #resolve or not various ip addresses
instancename = "My MooseFS"

masterhost = DEFAULT_MASTERNAME
masterport = DEFAULT_MASTER_CLIENT_PORT


frameset = -1
ptxtsep = "\t"
forceplaintext = 0
jsonmode = 0
colormode = 0
sectionset = []
# subsectionset = set([]) not used in CLI
clicommands = []

# order and data parameters
ICsclassid = -1
ICmatrix = 0
IMorder = 0
IMrev = 0
MForder = 0
MFrev = 0
CSorder = 0
CSrev = 0
MBorder = 0
MBrev = 0
HDdata = ""
HDorder = 0
HDrev = 0
HDperiod = 0
HDtime = 0
EXorder = 0
EXrev = 0
MSorder = 0
MSrev = 0
SCorder = 0
SCrev = 0
PAorder = 0
PArev = 0
OForder = 0
OFrev = 0
OFsessionid = 0
ALorder = 0
ALrev = 0
ALinode = 0
MOorder = 0
MOrev = 0
MOdata = 0
QUorder = 0
QUrev = 0
MCrange = 0
MCcount = 25
MCchdata = []
CCrange = 0
CCcount = 25
CCchdata = []

time_s = time.time()

# modes:
#  0 - percent (sum - cpu)
#  1 - ops/s (operations)
#  2 - humanized format in bytes (memory/disk space)
#  3,4 - not used in master
#  5 - in MB/s (data bytes read/written)
#  6 - raw data (number of chunks/files etc.)
#  7 - seconds
#  8 - percent (max - udiff)
mcchartslist = [
		('ucpu',0,0,'User cpu usage'),
		('scpu',1,0,'System cpu usage'),
		('delete',2,1,'Number of chunk deletion attempts'),
		('replicate',3,1,'Number of chunk replication attempts'),
		('statfs',4,1,'Number of statfs operations'),
		('getattr',5,1,'Number of getattr operations'),
		('setattr',6,1,'Number of setattr operations'),
		('lookup',7,1,'Number of lookup operations'),
		('mkdir',8,1,'Number of mkdir operations'),
		('rmdir',9,1,'Number of rmdir operations'),
		('symlink',10,1,'Number of symlink operations'),
		('readlink',11,1,'Number of readlink operations'),
		('mknod',12,1,'Number of mknod operations'),
		('unlink',13,1,'Number of unlink operations'),
		('rename',14,1,'Number of rename operations'),
		('link',15,1,'Number of link operations'),
		('readdir',16,1,'Number of readdir operations'),
		('open',17,1,'Number of open operations'),
		('read_chunk',18,1,'Number of chunk_read operations'),
		('write_chunk',19,1,'Number of chunk_write operations'),
		('memoryrss',20,2,'Resident memory usage'),
		('prcvd',21,1,'Packets received by master'),
		('psent',22,1,'Packets sent by master'),
		('brcvd',23,5,'Bytes received by master'),
		('bsent',24,5,'Bytes sent by master'),
		('memoryvirt',25,2,'Virtual memory usage'),
		('usedspace',26,2,'RAW disk space usage'),
		('totalspace',27,2,'RAW disk space connected'),
		('create',28,1,'Number of chunk creation attempts'),
		('change',29,1,'Number of chunk internal operation attempts'),
		('delete_ok',30,1,'Number of successful chunk deletions'),
		('delete_err',31,1,'Number of unsuccessful chunk deletions'),
		('replicate_ok',32,1,'Number of successful chunk replications'),
		('replicate_err',33,1,'Number of unsuccessful chunk replications'),
		('create_ok',34,1,'Number of successful chunk creations'),
		('create_err',35,1,'Number of unsuccessful chunk creations'),
		('change_ok',36,1,'Number of successful chunk internal operations'),
		('change_err',37,1,'Number of unsuccessful chunk internal operations'),
		('split_ok',38,1,'Number of successful chunk split operations'),
		('split_err',39,1,'Number of unsuccessful chunk split operations'),
		('fileobjects',40,6,'Number of file object'),
		('metaobjects',41,6,'Number of non-file objects (directories,symlinks,etc.)'),
		('chunksec8',42,6,'Total number of chunks stored in EC8 format'),
		('chunksec4',43,6,'Total number of chunks stored in EC4 format'),
		('chunkscopy',44,6,'Total number of chunks stored in COPY format'),
		('chregdanger',45,6,'Number of endangered chunks (mark for removal excluded)'),
		('chregunder',46,6,'Number of undergoal chunks (mark for removal excluded)'),
		('challdanger',47,6,'Number of endangered chunks (mark for removal included)'),
		('challunder',48,6,'Number of undergoal chunks (mark for removal included)'),
		('bytesread',49,5,'Traffic from cluster (data + overhead), bytes per second'),
		('byteswrite',50,5,'Traffic to cluster (data + overhead), bytes per second'),
		('read',51,1,'Number of read operations'),
		('write',52,1,'Number of write operations'),
		('fsync',53,1,'Number of fsync operations'),
		('lock',54,1,'Number of lock operations'),
		('snapshot',55,1,'Number of snapshot operations'),
		('truncate',56,1,'Number of truncate operations'),
		('getxattr',57,1,'Number of getxattr operations'),
		('setxattr',58,1,'Number of setxattr operations'),
		('getfacl',59,1,'Number of getfacl operations'),
		('setfacl',60,1,'Number of setfacl operations'),
		('createobj',61,1,'Number of create operations'),
		('meta',62,1,'Number of extra metadata operations (sclass,trashretention,eattr etc.)'),
		('servers',64,6,'Number of all registered chunk servers (both connected and disconnected)'),
		('mdservers',65,6,'Number of disconnected chunk servers that are in maintenance mode'),
		('dservers',66,6,'Number of disconnected chunk servers that are not in maintenance mode'),
		('udiff',67,8,'Difference in space usage percent between the most and least used chunk server'),
		('mountbytrcvd',68,5,'Traffic from cluster (data only), bytes per second'),
		('mountbytsent',69,5,'Traffic to cluster (data only), bytes per second'),
		('cpu',100,0,'Cpu usage (total sys+user)')
]
mcchartsabr = {
		'delete':['del'],
		'replicate':['rep','repl'],
		'memoryrss':['memrss','rmem','mem'],
		'memoryvirt':['memvirt','vmem']
}

# modes:
#  0 - percent (sum - cpu)
#  1 - ops/s (operations)
#  2 - humanized format in bytes (memory/disk space)
#  3 - threads (load)
#  4 - time
#  5 - in MB/s (data bytes read/written)
#  6 - raw data (number of chunks/files etc.)
#  7 - not used in CS
#  8 - percent (max - udiff)
ccchartslist = [
		('ucpu',0,0,'User cpu usage'),
		('scpu',1,0,'System cpu usage'),
		('masterin',2,5,'Data received from master'),
		('masterout',3,5,'Data sent to master'),
		('csrepin',4,5,'Data received by replicator'),
		('csrepout',5,5,'Data sent by replicator'),
		('csservin',6,5,'Data received by csserv'),
		('csservout',7,5,'Data sent by csserv'),
		('hdrbytesr',8,5,'Bytes read (headers)'),
		('hdrbytesw',9,5,'Bytes written (headers)'),
		('hdrllopr',10,1,'Low level reads (headers)'),
		('hdrllopw',11,1,'Low level writes (headers)'),
		('databytesr',12,5,'Bytes read (data)'),
		('databytesw',13,5,'Bytes written (data)'),
		('datallopr',14,1,'Low level reads (data)'),
		('datallopw',15,1,'Low level writes (data)'),
		('hlopr',16,1,'High level reads'),
		('hlopw',17,1,'High level writes'),
		('rtime',18,4,'Read time'),
		('wtime',19,4,'Write time'),
		('repl',20,1,'Replicate chunk ops'),
		('create',21,1,'Create chunk ops'),
		('delete',22,1,'Delete chunk ops'),
		('version',23,1,'Set version ops'),
		('duplicate',24,1,'Duplicate ops'),
		('truncate',25,1,'Truncate ops'),
		('duptrunc',26,1,'Duptrunc (duplicate+truncate) ops'),
		('test',27,1,'Test chunk ops'),
		('load',28,3,'Server load'),
		('memoryrss',29,2,'Resident memory usage'),
		('memoryvirt',30,2,'Virtual memory usage'),
		('movels',31,1,'Low speed move ops'),
		('movehs',32,1,'High speed move ops'),
		('split',34,1,'Split ops'),
		('usedspace',35,2,'Used HDD space in bytes (mark for removal excluded)'),
		('totalspace',36,2,'Total HDD space in bytes (mark for removal excluded)'),
		('chunkcount',37,6,'Number of stored chunks (mark for removal excluded)'),
		('tdusedspace',38,2,'Used HDD space in bytes on disks marked for removal'),
		('tdtotalspace',39,2,'Total HDD space in bytes on disks marked for removal'),
		('tdchunkcount',40,6,'Number of chunks stored on disks marked for removal'),
		('copychunks',41,6,'Number of stored chunks (all disks)'),
		('ec4chunks',42,6,'Number of stored chunk parts in EC4 format (all disks)'),
		('ec8chunks',43,6,'Number of stored chunk parts in EC8 format (all disks)'),
		('hddok',44,6,'Number of valid folders (hard drives)'),
		('hddmfr',45,6,'Number of folders (hard drives) that are marked for removal'),
		('hdddmg',46,6,'Number of folders (hard drives) that are marked as damaged'),
		('udiff',47,8,'Difference in usage percent between the most and least used disk'),
		('cpu',100,0,'Cpu usage (total sys+user)')
]
ccchartsabr = {
		'memoryrss':['memrss','rmem','mem'],
		'memoryvirt':['memvirt','vmem']
}

jcollect = {
		"version": VERSION,
		"timestamp": time_s,
		"shifted_timestamp": shiftts(time_s),
		"timestamp_str": time.ctime(time_s),
		"dataset":{},
		"errors":[]
}

mccharts = {}
cccharts = {}
for name,no,mode,desc in mcchartslist:
	mccharts[name] = (no,mode,desc)
for name,abrlist in mcchartsabr.items():
	for abr in abrlist:
		mccharts[abr] = mccharts[name]
for name,no,mode,desc in ccchartslist:
	cccharts[name] = (no,mode,desc)
for name,abrlist in ccchartsabr.items():
	for abr in abrlist:
		cccharts[abr] = cccharts[name]

lastsval = ''
lastorder = None
lastrev = None
lastid = None
lastmode = None
try:
	opts,args = getopt.getopt(sys.argv[1:],"hjvH:P:S:C:f:ps:no:rm:i:a:b:c:d:28")
except Exception:
	opts = [('-h',None)]
for opt,val in opts:
	if val==None:
		val=""
	if opt=='-h':
		print("usage:")
		print("\t%s [-hjpn28] [-H master_host] [-P master_port] [-f 0..3] -S(IN|IG|IM|IC|IL|MF|MU|CS|MB|HD|EX|MD|MS|MO|OF|AL|RP|SC|PA|QU|MC|CC) [-s separator] [-o order_id [-r]] [-m mode_id] [i id] [-a master_data_count] [-b master_data_desc] [-c chunkserver_data_count] [-d chunkserver_data_desc]" % sys.argv[0])
		print("\t%s [-hjpn28] [-H master_host] [-P master_port] [-f 0..3] -C(RC/ip/port|TR/ip/port|BW/ip/port|M[01]/ip/port|RS/sessionid)" % sys.argv[0])
		print("\t%s -v" % sys.argv[0])
		print("\ncommon:\n")
		print("\t-h : print this message and exit")
		print("\t-v : print version number and exit")
		print("\t-j : print result in JSON format")
		print("\t-p : force plain text format on tty devices")
		print("\t-s separator : field separator to use in plain text format on tty devices (forces -p)")
		print("\t-2 : force 256-color terminal color codes")
		print("\t-8 : force 8-color terminal color codes")
		print("\t-H master_host : master address (default: %s)" % DEFAULT_MASTERNAME)
		print("\t-P master_port : master client port (default: %u)" % DEFAULT_MASTER_CLIENT_PORT)
		print("\t-n : do not resolve ip addresses (default when output device is not tty)")
		print("\t-f frame charset number : set frame charset to be displayed as table frames in ttymode")
		print("\t\t-f0 : use simple ascii frames '+','-','|' (default for non utf-8 encodings)")
		if (sys.stdout.encoding=='UTF-8' or sys.stdout.encoding=='utf-8'):
			print("\t\t-f1 : use utf-8 frames: \u250f\u2533\u2513\u2523\u254b\u252b\u2517\u253b\u251b\u2501\u2503\u2578\u2579\u257a\u257b")
			print("\t\t-f2 : use utf-8 frames: \u250c\u252c\u2510\u251c\u253c\u2524\u2514\u2534\u2518\u2500\u2502\u2574\u2575\u2576\u2577")
			print("\t\t-f3 : use utf-8 frames: \u2554\u2566\u2557\u2560\u256c\u2563\u255a\u2569\u255d\u2550\u2551 (default for utf-8 encodings)")
		else:
			print("\t\t-f1 : use utf-8 frames (thick single)")
			print("\t\t-f2 : use utf-8 frames (thin single)")
			print("\t\t-f3 : use utf-8 frames (double - default for utf-8 encodings)")
		print("\nmonitoring:\n")
		print("\t-S data set : defines data set to be displayed")
		print("\t\t-SIN : show full master info (includes: SIG, SIM, SLI, SIC, SIL, SMF, SMU")
		print("\t\t-SIG : show only general cluster summary")
		print("\t\t-SIM : show only masters states")
		print("\t\t-SIC : show only chunks info (target/current redundancy level matrices)")
		print("\t\t-SIL : show only self-check loops info (with messages)")
		print("\t\t-SMF : show only missing chunks/files")
		print("\t\t-SMU : show only master memory usage")
		print("\t\t-SCS : show connected chunk servers")
		print("\t\t-SMB : show connected metadata backup servers")
		print("\t\t-SHD : show hdd data")
		print("\t\t-SEX : show exports")
		print("\t\t-SMD : show mounts details (includes: SMS, SMO, SOF, SAL)")
		print("\t\t-SMS : show only active mounts")
		print("\t\t-SMO : show only operations counters")
		print("\t\t-SOF : show only open files")
		print("\t\t-SAL : show only acquired locks")
		print("\t\t-SRP : show redundancy policy (includes: SSC, SPA)")
		print("\t\t-SSC : show storage classes")
		print("\t\t-SPA : show patterns overrides")
		print("\t\t-SQU : show quota info")
		print("\t\t-SMC : show master charts data (use with -a and -b)")
		print("\t\t-SCC : show chunkserver charts data (use with -c and -d)")
		print("\t-o order_id : sort data by column specified by 'order id' (depends on data set)")
		print("\t-r : reverse order")
		print("\t-m mode_id : show data specified by 'mode id' (depends on data set)")
		print("\t-i id : storage class id for -SIN/SIC, sessionid for -SOF or inode for -SAL")
		print("\t-a master_data_count : how many master data charts entries (-SMC) should be shown")
		print("\t-b master_data_desc : define master data charts (-SMC) columns\n\t\tuse 'all' as a chart name for all available charts\n\t\tprefix with '+' for raw data\n\t\t(pro only) prefix with 'ip:[port:]' for server choice (by default leader master will be used)")
		print("\t-c chunkserver_data_count : how many chunkserver data charts (-SCC) entries should be shown")
		print("\t-d chunkserver_data_desc : define chunkserver data charts (-SCC) columns\n\t\tuse 'all' as a chart name for all available charts\n\t\tprefix with '+' for raw data\n\t\tprefix with 'ip:[port:]' for server choice (by default all servers will be used)")
		print("\n\tavailable master data charts columns (-SMC):")
		for name,no,mode,desc in mcchartslist:
			if name in mcchartsabr:
				name = "%s,%s" % (name,",".join(mcchartsabr[name]))
			print("\t\t%s - %s" % (name,desc))
		print("\t\tall - Get all above columns")
		print("\n\tavailable chunkserver data charts columns (-SCC):")
		for name,no,mode,desc in ccchartslist:
			if name in ccchartsabr:
				name = "%s,%s" % (name,",".join(ccchartsabr[name]))
			print("\t\t%s - %s" % (name,desc))
		print("\t\tall - Get all above columns")
		print("\ncommands:\n")
		print("\t-C command : perform particular command")
		print("\t\t-CRC/ip/port : remove given chunkserver from the list of active chunkservers")
		print("\t\t-CTR/ip/port : temporarily remove given chunkserver from the list of active chunkservers (master elect only)")
		print("\t\t-CBW/ip/port : send given chunkserver back to work (from the grace state)")
		print("\t\t-CM1/ip/port : switch given chunkserver to maintenance mode")
		print("\t\t-CM0/ip/port : switch given chunkserver to standard mode (from maintenance mode)")
		print("\t\t-CRS/sessionid : remove given session")
		print("\nexamples:\n")
		print("\tmfscli -SIC -2")
		print("\t\tshows a table with chunk state matrix (the number of chunks for each combination of valid copies and the goal set by the user) using extended terminal colors (256-colors)")
		print("\tmfscli -SCS -f 1")
		print("\t\tshows a table with all chunkservers using unicode thick frames")
		print("\tmfscli -SMS -p -s ','")
		print("\t\tshows current sessions (mounts) using plain text format and comma as a separator")
		print("\tmfscli -SOF -i 123")
		print("\t\tshows files open by processes using session (mount) with id 123")
		print("\tmfscli -SMC -a 10 -b cpu,memoryrss,delete,replicate")
		print("\t\tshows the master cpu usage, amount of resident memory used by master, number of chunk deletions and number of chunk replications during last ten minutes")
		print("\tmfscli -SCC -c 15 -d 192.168.1.10:9422:cpu,192.168.1.11:9422:cpu")
		print("\t\tshows cpu usage of chunkservers located on machines with IP 192.168.1.10 and 192.168.1.11 during last fifteen minutes")
		print("\tmfscli -SCC -d mem")
		print("\t\tshows resident memory usage of all connected chunkservers (current mfscli)")
		print("\tmfscli -SIN -j")
		print("\t\tprints basic infos in JSON format")
		sys.exit(0)
	elif opt=='-v':
		print("version: %s" % VERSION)
		sys.exit(0)
	elif opt=='-2':
		colormode = 2
	elif opt=='-8':
		colormode = 1
	elif opt=='-j':
		jsonmode = 1
	elif opt=='-p':
		forceplaintext = 1
	elif opt=='-s':
		ptxtsep = val
		forceplaintext = 1
	elif opt=='-n':
		donotresolve = 1
	elif opt=='-f':
		frameset = int(val)
	elif opt=='-H':
		masterhost = val
	elif opt=='-P':
		masterport = int(val)
	elif opt=='-S':
		lastsval = val
		if 'IN' in val:
			sectionset.append("IN")
			if lastmode!=None:  ICmatrix = lastmode
			if lastid!=None:    ICsclassid = lastid
			if lastorder!=None: IMorder = lastorder
			if lastrev:         IMrev = 1
		if 'IM' in val:
			sectionset.append("IM")
			if lastorder!=None: IMorder = lastorder
			if lastrev:         IMrev = 1
		if 'IG' in val:
			sectionset.append("IG")
		if 'MU' in val:
			sectionset.append("MU")
		if 'IC' in val:
			sectionset.append("IC")
			if lastmode!=None: ICmatrix = lastmode
			if lastid!=None:   ICsclassid = lastid
		if 'IL' in val:
			sectionset.append("IL")
		if 'MF' in val:
			sectionset.append("MF")
			if lastorder!=None: MForder = lastorder
			if lastrev:         MFrev = 1
		if 'CS' in val:
			sectionset.append("CS")
			if lastorder!=None: CSorder = lastorder
			if lastrev:         CSrev = 1
		if 'MB' in val:
			sectionset.append("MB")
			if lastorder!=None: MBorder = lastorder
			if lastrev:         MBrev = 1
		if 'HD' in val:
			sectionset.append("HD")
			if lastorder!=None: HDorder = lastorder
			if lastrev:         HDrev = 1
			if lastmode!=None:
				if lastmode>=0 and lastmode<6:
					HDperiod,HDtime = divmod(lastmode,2)
		if 'EX' in val:
			sectionset.append("EX")
			if lastorder!=None: EXorder = lastorder
			if lastrev:         EXrev = 1

		if 'MD' in val:
			sectionset.append("MD")
		if 'MS' in val:
			sectionset.append("MS")
			if lastorder!=None: MSorder = lastorder
			if lastrev:         MSrev = 1
		if 'OF' in val:
			sectionset.append("OF")
			if lastorder!=None: OForder = lastorder
			if lastrev!=None:   OFrev = 1
			if lastid!=None:    OFsessionid = lastid
		if 'AL' in val:
			sectionset.append("AL")
			if lastorder!=None: ALorder = lastorder
			if lastrev!=None:   ALrev = 1
			if lastid!=None:    ALinode = lastid
		if 'MO' in val:
			sectionset.append("MO")
			if lastorder!=None: MOorder = lastorder
			if lastrev:         MOrev = 1
			if lastmode!=None:  MOdata = lastmode

		if 'RP' in val or 'RS' in val: # backward compatibility as RS->RP (4.57)
			sectionset.append("RP")
			sectionset.append("RS")
		if 'SC' in val:
			sectionset.append("SC")
			if lastorder!=None: SCorder = lastorder
			if lastrev:         SCrev = 1
		if 'PA' in val:
			sectionset.append("PA")
			if lastorder!=None: PAorder = lastorder
			if lastrev:         PArev = 1
		if 'QU' in val:
			sectionset.append("QU")
			if lastorder!=None: QUorder = lastorder
			if lastrev:         QUrev = 1
		if 'MC' in val:
			sectionset.append("MC")
			if lastmode!=None: MCrange = lastmode
		if 'CC' in val:
			sectionset.append("CC")
			if lastmode!=None: CCrange = lastmode
		lastorder = None
		lastrev = 0
		lastmode = None
	elif opt=='-o':
		if 'IM' in lastsval: IMorder = int(val)
		if 'MF' in lastsval: MForder = int(val)
		if 'CS' in lastsval: CSorder = int(val)
		if 'MB' in lastsval: MBorder = int(val)
		if 'HD' in lastsval: HDorder = int(val)
		if 'EX' in lastsval: EXorder = int(val)
		if 'MS' in lastsval: MSorder = int(val)
		if 'MO' in lastsval: MOorder = int(val)
		if 'SC' in lastsval: SCorder = int(val)
		if 'PA' in lastsval: PAorder = int(val)
		if 'OF' in lastsval: OForder = int(val)
		if 'AL' in lastsval: ALorder = int(val)
		if 'QU' in lastsval: QUorder = int(val)
		if lastsval=='': lastorder = int(val)
	elif opt=='-r':
		if 'IM' in lastsval: IMrev = 1
		if 'MF' in lastsval: MFrev = 1
		if 'CS' in lastsval: CSrev = 1
		if 'MB' in lastsval: MBrev = 1
		if 'HD' in lastsval: HDrev = 1
		if 'EX' in lastsval: EXrev = 1
		if 'MS' in lastsval: MSrev = 1
		if 'MO' in lastsval: MOrev = 1
		if 'SC' in lastsval: SCrev = 1
		if 'PA' in lastsval: PArev = 1
		if 'OF' in lastsval: OFrev = 1
		if 'AL' in lastsval: ALrev = 1
		if 'QU' in lastsval: QUrev = 1
		if lastsval=='': lastrev = 1
	elif opt=='-m':
		if 'HD' in lastsval:
			d = int(val)
			if d>=0 and d<6:
				HDperiod,HDtime = divmod(d,2)
		if 'MO' in lastsval: MOdata = int(val)
		if 'IN' in lastsval or 'IC' in lastsval: ICmatrix = int(val)
		if 'MC' in lastsval: MCrange = int(val)
		if 'CC' in lastsval: CCrange = int(val)
		if lastsval=='': lastmode = int(val)
	elif opt=='-i':
		if 'OF' in lastsval: OFsessionid = int(val)
		if 'AL' in lastsval: ALinode = int(val)
		if 'IN' in lastsval or 'IC' in lastsval:
			ICsclassid = int(val)
		if lastsval=='':
			lastid = int(val)
	elif opt=='-a':
		MCcount = safe_int(val)
	elif opt=='-b':
		for x in val.split(','):
			x = x.strip()
			if ':' in x:
				xs = x.split(':')
				if len(xs)==2:
					chhost = xs[0]
					chport = 9421
					x = xs[1]
				elif len(xs)==3:
					chhost = xs[0]
					chport = int(xs[1])
					x = xs[2]
				else:
					print("Wrong chart definition: %s" % x)
					sys.exit(0)
			else:
				chhost = None
				chport = None
			if x!='' and x[0]=='+':
				x = x[1:]
				rawmode = 1
			else:
				rawmode = 0
			if x in mccharts:
				MCchdata.append((chhost,chport,mccharts[x][0],mccharts[x][1],mccharts[x][2],x,rawmode))
			elif x=='all':
				for name,no,mode,desc in mcchartslist:
					if (no<100):
						MCchdata.append((chhost,chport,no,mode,desc,name,rawmode))
			else:
				print("Unknown master chart name: %s" % x)
				sys.exit(0)
	elif opt=='-c':
		CCcount = safe_int(val)
	elif opt=='-d':
		for x in val.split(','):
			x = x.strip()
			if ':' in x:
				xs = x.split(':')
				if len(xs)==2:
					chhost = xs[0]
					chport = 9422
					x = xs[1]
				elif len(xs)==3:
					chhost = xs[0]
					chport = int(xs[1])
					x = xs[2]
				else:
					print("Unknown chart name: %s" % x)
					sys.exit(0)
			else:
				chhost = None
				chport = None
			if x!='' and x[0]=='+':
				x = x[1:]
				rawmode = 1
			else:
				rawmode = 0
#				if chhost==None or chport==None:
#					print("in chunkserver chart data server ip/host must be specified")
#					sys.exit(0)
			if x in cccharts:
				CCchdata.append((chhost,chport,cccharts[x][0],cccharts[x][1],cccharts[x][2],x,rawmode))
			elif x=='all':
				for name,no,mode,desc in ccchartslist:
					if (no<100):
						CCchdata.append((chhost,chport,no,mode,desc,name,rawmode))
			else:
				print("Unknown chunkserver chart name: %s" % x)
				sys.exit(0)
	elif opt=='-C':
		clicommands.append(val)
sectionset = set(sectionset)
if len(sectionset)==0 and len(clicommands)==0:
	print("Specify data to be shown (option -S) or command (option -C). Use '-h' for help.")
	sys.exit(0)

ttymode = 1 if forceplaintext==0 and os.isatty(1) and jsonmode==0 else 0
# frames:
#  +-----+-----+-----+-----+
#  |     |     |     |     |
#  |     |     |     |  |  |
#  |  +- | -+- | -+  |  +- |
#  |  |  |  |  |  |  |  |  |
#  |     |     |     |     |
#  +-----+-----+-----+-----+
#  |     |     |     |     |
#  |  |  |  |  |  |  |  |  |
#  | -+- | -+  |  +- | -+- |
#  |  |  |  |  |     |     |
#  |     |     |     |     |
#  +-----+-----+-----+-----+
#  |     |     |     |     |
#  |  |  |     |  |  |     |
#  | -+  | -+- |  +  | -+  |
#  |     |     |  |  |     |
#  |     |     |     |     |
#  +-----+-----+-----+-----+
#  |     |     |     |     |
#  |  |  |     |     |     |
#  |  +  |  +- |  +  |  +  |
#  |     |     |  |  |     |
#  |     |     |     |     |
#  +-----+-----+-----+-----+
#  
if ttymode:
	try:
		import curses
		curses.setupterm()
		if curses.tigetnum("colors")>=256:
			colors256 = 1
		else:
			colors256 = 0
	except Exception:
		colors256 = 1 if 'TERM' in os.environ and '256' in os.environ['TERM'] else 0
	# colors: 0 - white,1 - red,2 - orange,3 - yellow,4 - green,5 - cyan,6 - blue,7 - violet,8 - gray
	CSI="\x1B["
	if colors256:
		ttyreset=CSI+"0m"
		colorcode=[CSI+"38;5;196m",CSI+"38;5;208m",CSI+"38;5;226m",CSI+"38;5;34m",CSI+"38;5;30m",CSI+"38;5;19m",CSI+"38;5;55m",CSI+"38;5;244m"]
	else:
		ttysetred=CSI+"31m"
		ttysetyellow=CSI+"33m"
		ttysetgreen=CSI+"32m"
		ttysetcyan=CSI+"36m"
		ttysetblue=CSI+"34m"
		ttysetmagenta=CSI+"35m"
		ttyreset=CSI+"0m"
		# no orange - use red, no gray - use white
		colorcode=[ttysetred,ttysetred,ttysetyellow,ttysetgreen,ttysetcyan,ttysetblue,ttysetmagenta,""]
else:
	colorcode=["","","","","","","",""]

if ttymode and (sys.stdout.encoding=='UTF-8' or sys.stdout.encoding=='utf-8'):
	if frameset>=0 and frameset<=3:
		tableframes=frameset
	else:
		tableframes=0
else:
	tableframes=0

# terminal encoding mambo jumbo (mainly replace unicode chars that can't be printed with '?' instead of throwing exception)
term_encoding = sys.stdout.encoding
if term_encoding==None:
	term_encoding = 'utf-8'
if sys.version_info[1]<7:
	sys.stdout = codecs.getwriter(term_encoding)(sys.stdout.detach(),'replace')
	sys.stdout.encoding = term_encoding
else:
	sys.stdout.reconfigure(errors='replace')
	
class Table:
	Needseparator = 0
	def __init__(self,title,ccnt,defattr=""):
		if tableframes==1:
			self.frames = ['\u250f', '\u2533', '\u2513', '\u2523', '\u254b', '\u252b', '\u2517', '\u253b', '\u251b', '\u2501', '\u2503', '\u2578', '\u2579', '\u257a', '\u257b', ' ']
		elif tableframes==2:
			self.frames = ['\u250c', '\u252c', '\u2510', '\u251c', '\u253c', '\u2524', '\u2514', '\u2534', '\u2518', '\u2500', '\u2502', '\u2574', '\u2575', '\u2576', '\u2577', ' ']
		elif tableframes==3:
			self.frames = ['\u2554', '\u2566', '\u2557', '\u2560', '\u256c', '\u2563', '\u255a', '\u2569', '\u255d', '\u2550', '\u2551', ' ', '\u2551', ' ', '\u2551', ' ']
		else:
			self.frames = ['+','+','+','+','+','+','+','+','+','-','|',' ','|',' ','|',' ']
		self.title = title
		self.ccnt = ccnt
		self.head = []
		self.body = []
		self.defattrs = []
		self.cwidth = []
		for _ in range(ccnt):
			self.defattrs.append(defattr)
			self.cwidth.append(0)
	def combineattr(self,attr,defattr):
		attrcolor = ""
		for c in ("0","1","2","3","4","5","6","7","8"):
			if c in defattr:
				attrcolor = c
		for c in ("0","1","2","3","4","5","6","7","8"):
			if c in attr:
				attrcolor = c
		attrjust = ""
		for c in ("l","L","r","R","c","C"):
			if c in defattr:
				attrjust = c
		for c in ("l","L","r","R","c","C"):
			if c in attr:
				attrjust = c
		return attrcolor+attrjust
	def header(self,*rowdata):
		ccnt = 0
		for celldata in rowdata:
			if type(celldata) is tuple:
				if len(celldata)==3:
					ccnt+=celldata[2]
				else:
					if celldata[0]!=None:
						cstr = myunicode(celldata[0])
						if len(cstr) > self.cwidth[ccnt]:
							self.cwidth[ccnt] = len(cstr)
					ccnt+=1
			else:
				if celldata!=None:
					cstr = myunicode(celldata)
					if len(cstr) > self.cwidth[ccnt]:
						self.cwidth[ccnt] = len(cstr)
				ccnt+=1
		if ccnt != self.ccnt:
			raise IndexError
		self.head.append(rowdata)
	def defattr(self,*rowdata):
		if len(rowdata) != self.ccnt:
			raise IndexError
		self.defattrs = rowdata
	def append(self,*rowdata):
		ccnt = 0
		rdata = []
		for celldata in rowdata:
			if type(celldata) is tuple:
				if celldata[0]!=None:
					cstr = myunicode(celldata[0])
				else:
					cstr = ""
				if len(celldata)==3:
					rdata.append((cstr,self.combineattr(celldata[1],self.defattrs[ccnt]),celldata[2]))
					ccnt+=celldata[2]
				else:
					if len(cstr) > self.cwidth[ccnt]:
						self.cwidth[ccnt] = len(cstr)
					if len(celldata)==2:
						rdata.append((cstr,self.combineattr(celldata[1],self.defattrs[ccnt])))
					else:
						rdata.append((cstr,self.defattrs[ccnt]))
					ccnt+=1
			else:
				if celldata!=None:
					cstr = myunicode(celldata)
					if ccnt >= len(self.cwidth):
						raise IndexError("ccnt: %u, self.ccnt: %u, len(self.cwidth): %u" % (ccnt, self.ccnt, len(self.cwidth)))
					if len(cstr) > self.cwidth[ccnt]:
						self.cwidth[ccnt] = len(cstr)
					rdata.append((cstr,self.defattrs[ccnt]))
				else:
					rdata.append(celldata)
				ccnt+=1
		if ccnt != self.ccnt:
			raise IndexError("ccnt: %u, self.ccnt: %u" % (ccnt, self.ccnt))
		self.body.append(rdata)
	def attrdata(self,cstr,cattr,cwidth):
		retstr = ""
		if "1" in cattr:
			retstr += colorcode[0]
			needreset = 1
		elif "2" in cattr:
			retstr += colorcode[1]
			needreset = 1
		elif "3" in cattr:
			retstr += colorcode[2]
			needreset = 1
		elif "4" in cattr:
			retstr += colorcode[3]
			needreset = 1
		elif "5" in cattr:
			retstr += colorcode[4]
			needreset = 1
		elif "6" in cattr:
			retstr += colorcode[5]
			needreset = 1
		elif "7" in cattr:
			retstr += colorcode[6]
			needreset = 1
		elif "8" in cattr:
			retstr += colorcode[7]
			needreset = 1
		else:
			needreset = 0
		if cstr=="--":
			retstr += " "+"-"*cwidth+" "
		elif cstr=="---":
			retstr += "-"*(cwidth+2)
		elif "L" in cattr or "l" in cattr:
			retstr += " "+cstr.ljust(cwidth)+" "
		elif "R" in cattr or "r" in cattr:
			retstr += " "+cstr.rjust(cwidth)+" "
		else:
			retstr += " "+cstr.center(cwidth)+" "
		if needreset:
			retstr += ttyreset
		return retstr
	def lines(self):
		outstrtab = []
		if ttymode:
			tabdata = []
			# upper frame
			tabdata.append((("---","",self.ccnt),))
			# title
			tabdata.append(((self.title,"",self.ccnt),))
			# header
			if len(self.head)>0:
				tabdata.append((("---","",self.ccnt),))
				tabdata.extend(self.head)
			# head and data separator
			tabdata.append((("---","",self.ccnt),))
			# data
			if len(self.body)==0:
				tabdata.append((("no data","",self.ccnt),))
			else:
				tabdata.extend(self.body)
			# bottom frame
			tabdata.append((("---","",self.ccnt),))
			# check col-spaned headers and adjust column widths if necessary
			for rowdata in tabdata:
				ccnt = 0
				for celldata in rowdata:
					if type(celldata) is tuple and len(celldata)==3 and celldata[0]!=None:
						cstr = myunicode(celldata[0])
						clen = len(cstr)
						cwidth = sum(self.cwidth[ccnt:ccnt+celldata[2]])+3*(celldata[2]-1)
						if clen > cwidth:
							add = clen - cwidth
							adddm = divmod(add,celldata[2])
							cadd = adddm[0]
							if adddm[1]>0:
								cadd+=1
							for i in range(celldata[2]):
								self.cwidth[ccnt+i] += cadd
						ccnt += celldata[2]
					else:
						ccnt += 1
			separators = []
			# before tab - no separators
			seplist = []
			for i in range(self.ccnt+1):
				seplist.append(0)
			separators.append(seplist)
			for rowdata in tabdata:
				seplist = [1]
				for celldata in rowdata:
					if type(celldata) is tuple and len(celldata)==3:
						for i in range(celldata[2]-1):
							seplist.append(1 if celldata[0]=='---' else 0)
					seplist.append(1)
				separators.append(seplist)
			# after tab - no separators
			seplist = []
			for i in range(self.ccnt+1):
				seplist.append(0)
			separators.append(seplist)
			# add upper and lower separators:
			updownsep = [[a*2 + b for (a,b) in zip(x,y)] for (x,y) in zip(separators[2:],separators[:-2])]
			# create table
			for (rowdata,sepdata) in zip(tabdata,updownsep):
				ccnt = 0
				line = ""
				nsep = 0 #self.frames[10]
				for celldata in rowdata:
					cpos = ccnt
					cattr = ""
					if type(celldata) is tuple:
						if celldata[1]!=None:
							cattr = celldata[1]
						if len(celldata)==3:
							cwidth = sum(self.cwidth[ccnt:ccnt+celldata[2]])+3*(celldata[2]-1)
							ccnt+=celldata[2]
						else:
							cwidth = self.cwidth[ccnt]
							ccnt+=1
						cstr = celldata[0]
					else:
						cstr = celldata
						cwidth = self.cwidth[ccnt]
						ccnt+=1
					if cstr==None:
						cstr = ""
					cstr = myunicode(cstr)
					if cstr=="---":
						if nsep==0:
							line += self.frames[(13,6,0,3)[sepdata[cpos]]]
						else:
							line += self.frames[(9,7,1,4)[sepdata[cpos]]]
						nsep = 1 #self.frames[4]
						for ci in range(cpos,ccnt-1):
							line += self.frames[9]*(self.cwidth[ci]+2)
							line += self.frames[(9,7,1,4)[sepdata[ci+1]]]
						line += self.frames[9]*(self.cwidth[ccnt-1]+2)
					else:
						if nsep==0:
							line += self.frames[(15,12,14,10)[sepdata[cpos]]]
						else:
							line += self.frames[(11,8,2,5)[sepdata[cpos]]]
						nsep = 0
						line += self.attrdata(cstr,cattr,cwidth)
				if nsep==0:
					line += self.frames[(15,12,14,10)[sepdata[ccnt]]]
				else:
					line += self.frames[(11,8,2,5)[sepdata[ccnt]]]
				outstrtab.append(line)
		else:
			for rowdata in self.body:
				row = []
				for celldata in rowdata:
					if type(celldata) is tuple:
						cstr = myunicode(celldata[0])
					elif celldata!=None:
						cstr = myunicode(celldata)
					else:
						cstr = ""
					row.append(cstr)
				outstrtab.append("%s:%s%s" % (self.title,ptxtsep,ptxtsep.join(row)))
		return outstrtab
	def __str__(self):
		if Table.Needseparator:
			sep = "\n"
		else:
			sep = ""
			Table.Needseparator = 1
		return sep+("\n".join(self.lines()))

def print_error(msg):
	if jsonmode:
		json_er_dict = {}
		json_er_errors = []
		json_er_errors.append({"message":msg})
		json_er_dict["errors"] = json_er_errors
		jcollect["errors"].append(json_er_dict)
	elif ttymode:
		tab = Table("Error",1)
		tab.header("message")
		tab.defattr("l")
		tab.append(msg)
		print("%s%s%s" % (colorcode[1],tab,ttyreset))
	else:
		print("""Oops, an error has occurred:""")
		print(msg)

def print_exception():
	exc_type, exc_value, exc_traceback = sys.exc_info()
	try:
		if jsonmode:
			json_er_dict = {}
			json_er_traceback = []
			for d in traceback.extract_tb(exc_traceback):
				json_er_traceback.append({"file":d[0] , "line":d[1] , "in":d[2] , "text":repr(d[3])})
			json_er_dict["traceback"] = json_er_traceback
			json_er_errors = []
			for d in traceback.format_exception_only(exc_type, exc_value):
				json_er_errors.append(repr(d.strip()))
			json_er_dict["errors"] = json_er_errors
			jcollect["errors"].append(json_er_dict)
		elif ttymode:
			tab = Table("Exception Traceback",4)
			tab.header("file","line","in","text")
			tab.defattr("l","r","l","l")
			for d in traceback.extract_tb(exc_traceback):
				tab.append(d[0],d[1],d[2],repr(d[3]))
			tab.append(("---","",4))
			tab.append(("Error","c",4))
			tab.append(("---","",4))
			for d in traceback.format_exception_only(exc_type, exc_value):
				tab.append((repr(d.strip()),"",4))
			print("%s%s%s" % (colorcode[1],tab,ttyreset))
		else:
			print("""---------------------------------------------------------------- error -----------------------------------------------------------------""")
			print(traceback.format_exc().strip())
			print("""----------------------------------------------------------------------------------------------------------------------------------------""")
	except Exception:
		print(traceback.format_exc().strip())

# connect to masters and find who is the leader etc
cl = Cluster(masterhost, masterport)

errmsg = None
if cl.master()==None:
	errmsg = """Can't connect to the MooseFS Master server (%s)""" % (masterhost)
if (cl.leaderfound() or cl.electfound() or cl.usurperfound() or cl.followerfound()):
	if cl.master().version_unknown():
		errmsg = """Can't detect the MooseFS Master server version (%s)""" % (masterhost)
	elif cl.master().version_less_than(3,0,0):
		errmsg = """This version of MooseFS Master server (%s) is not supported (pre 3.0.0)""" % (masterhost)
if errmsg:
	if jsonmode:
		jcollect["errors"].append(errmsg)
		print(json.dumps(jcollect))
	else:
		print(errmsg)
	sys.exit(1)

# initialize data provider
dataprovider = DataProvider(cl, donotresolve)

# CLI commands
if cl.leaderfound():
	for cmd in clicommands:
		cmddata = cmd.split('/')
		if cmddata[0]=='RC':
			cmd_success = 0
			try:
				csip = list(map(int,cmddata[1].split(".")))
				csport = int(cmddata[2])
				if len(csip)==4:
					data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_REMOVE,csip[0],csip[1],csip[2],csip[3],csport))
					if length==1:
						status = (struct.unpack(">B",data))[0]
						cmd_success = 1
				if cmd_success:
					if status==STATUS_OK:
						print("Chunkserver %s/%s has been removed" % (cmddata[1],cmddata[2]))
					elif status==ERROR_NOTFOUND:
						print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
					elif status==ERROR_ACTIVE:
						print("Chunkserver %s/%s can't be removed because is still active" % (cmddata[1],cmddata[2]))
					else:
						print("Can't remove chunkserver %s/%s (status:%u)" % (cmddata[1],cmddata[2],status))
				else:
					print("Can't remove chunkserver %s/%s" % (cmddata[1],cmddata[2]))
			except Exception:
				print_exception()
		if cmddata[0]=='BW':
			cmd_success = 0
			try:
				csip = list(map(int,cmddata[1].split(".")))
				csport = int(cmddata[2])
				if len(csip)==4:
					if cl.master().version_at_least(1,6,28):
						data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_BACKTOWORK,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
				if cmd_success:
					if status==STATUS_OK:
						print("Chunkserver %s/%s has back to work" % (cmddata[1],cmddata[2]))
					elif status==ERROR_NOTFOUND:
						print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
					else:
						print("Can't turn chunkserver %s/%s back to work (status:%u)" % (cmddata[1],cmddata[2],status))
				else:
					print("Can't turn chunkserver %s/%s back to work" % (cmddata[1],cmddata[2]))
			except Exception:
				print_exception()
		if cmddata[0]=='M1':
			cmd_success = 0
			try:
				csip = list(map(int,cmddata[1].split(".")))
				csport = int(cmddata[2])
				if len(csip)==4:
					if cl.master().version_at_least(2,0,11):
						data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEON,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
				if cmd_success:
					if status==STATUS_OK:
						print("Chunkserver %s/%s has been switched to maintenance mode" % (cmddata[1],cmddata[2]))
					elif status==ERROR_NOTFOUND:
						print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
					else:
						print("Can't switch chunkserver %s/%s to maintenance mode (status:%u)" % (cmddata[1],cmddata[2],status))
				else:
					print("Can't switch chunkserver %s/%s to maintenance mode" % (cmddata[1],cmddata[2]))
			except Exception:
				print_exception()
		if cmddata[0]=='M0':
			cmd_success = 0
			try:
				csip = list(map(int,cmddata[1].split(".")))
				csport = int(cmddata[2])
				if len(csip)==4:
					if cl.master().version_at_least(2,0,11):
						data,length = cl.master().command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEOFF,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
				if cmd_success:
					if status==STATUS_OK:
						print("Chunkserver %s/%s has been switched to standard mode" % (cmddata[1],cmddata[2]))
					elif status==ERROR_NOTFOUND:
						print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
					else:
						print("Can't switch chunkserver %s/%s to standard mode (status:%u)" % (cmddata[1],cmddata[2],status))
				else:
					print("Can't switch chunkserver %s/%s to standard mode" % (cmddata[1],cmddata[2]))
			except Exception:
				print_exception()
		if cmddata[0]=='RS':
			cmd_success = 0
			try:
				sessionid = int(cmddata[1])
				data,length = cl.master().command(CLTOMA_SESSION_COMMAND,MATOCL_SESSION_COMMAND,struct.pack(">BL",MFS_SESSION_COMMAND_REMOVE,sessionid))
				if length==1:
					status = (struct.unpack(">B",data))[0]
					cmd_success = 1
				if cmd_success:
					if status==STATUS_OK:
						print("Session %u has been removed" % (sessionid))
					elif status==ERROR_NOTFOUND:
						print("Session %u hasn't been found" % (sessionid))
					elif status==ERROR_ACTIVE:
						print("Session %u can't be removed because is still active" % (sessionid))
					else:
						print("Can't remove session %u (status:%u)" % (sessionid,status))
				else:
					print("Can't remove session %u" % (sessionid))
			except Exception:
				print_exception()
elif len(clicommands)>0:
	print("Can't perform any operation because there is no leading master")

# organization of sections
org = Organization(dataprovider, guimode=False)
if org.menu_tree==None: # missing menu_tree means we can't connect to the master
	print_error("Can't connect to MooseFS Master server (%s)" % masterhost)
	exit(1)
org.set_sections(sectionset)


#######################################################
###                 RENDER OUTPUT                   ###
#######################################################

if cl.leaderfound()==0:
	if jsonmode:
		if not cl.anyfound():
			jcollect["errors"].append("Can't find masters (resolve '%s')!" % (masterhost))
		else:
			jcollect["errors"].append("Working master servers not found! Maybe you are using wrong port number or wrong dns name")
	else:
		if not cl.anyfound():
			print("Can't find masters (resolve '%s')!" % (masterhost))
		else:
			print("Working master servers not found! Maybe you are using wrong port number or wrong dns name")
	Table.Needseparator=1

# parse cgi parameters, data ordering and fix order id's
if CSorder>=2: CSorder+=1
if   CSorder>=6 and CSorder<=9: CSorder+=4
elif CSorder>=10 and CSorder<=13: CSorder+=10

if MBorder>=2: MBorder+=1

if HDorder>=13 and HDorder<=15: HDorder+=7

if MSorder>=3: MSorder+=1
if MSorder==0: MSorder=2 # default order: host name

if   MOorder==2: MOorder = 3
elif MOorder>=3 and MOorder<=18: MOorder += 97
elif MOorder==19: MOorder = 150
elif MOorder!=1: MOorder = 0

if   QUorder>=2 and QUorder<=6: QUorder += 8
elif QUorder>=7 and QUorder<=10: QUorder += 14
elif QUorder>=11 and QUorder<=22: QUorder = (lambda x: x[0]+x[1]*10)(divmod(QUorder-11,3))+31
elif QUorder<1 or QUorder>22: QUorder = 0


#######################################################
###                 RENDER SECTIONS                 ###
#######################################################


# Info section
if jsonmode:
	json_in_dict = {}

#Cluster summary subsection
if org.shall_render("IG"):
	try:
		ci = dataprovider.get_clusterinfo()
		if jsonmode:
			json_ig_dict = {}
			json_ig_dict["strver"] = ci.strver
			if ci.strver.endswith(" PRO"):
				json_ig_dict["version"] = ci.strver[:-4]
				json_ig_dict["pro"] = True
			else:
				json_ig_dict["version"] = ci.strver
				json_ig_dict["pro"] = False
			if ci.memusage>0:
				json_ig_dict["memory_usage"] = ci.memusage
				json_ig_dict["memory_usage_human"] = humanize_number(ci.memusage," ")
			else:
				json_ig_dict["memory_usage"] = None
				json_ig_dict["memory_usage_human"] = ""
			if ci.syscpu>0 or ci.usercpu>0:
				json_ig_dict["cpu_usage_percent"] = ci.syscpu+ci.usercpu
				json_ig_dict["cpu_system_percent"] = ci.syscpu
				json_ig_dict["cpu_user_percent"] = ci.usercpu
			else:
				json_ig_dict["cpu_usage_percent"] = None
				json_ig_dict["cpu_system_percent"] = None
				json_ig_dict["cpu_user_percent"] = None
			json_ig_dict["total_space"] = ci.totalspace
			json_ig_dict["total_space_human"] = humanize_number(ci.totalspace," ")
			json_ig_dict["avail_space"] = ci.availspace
			json_ig_dict["avail_space_human"] = humanize_number(ci.availspace," ")
			json_ig_dict["free_space"] = ci.freespace
			json_ig_dict["free_space_human"] = humanize_number(ci.freespace," ")
			json_ig_dict["trash_space"] = ci.trspace
			json_ig_dict["trash_space_human"] = humanize_number(ci.trspace," ")
			json_ig_dict["trash_files"] = ci.trfiles
			json_ig_dict["sustained_space"] = ci.respace
			json_ig_dict["sustained_space_human"] = humanize_number(ci.respace," ")
			json_ig_dict["sustained_files"] = ci.refiles
			json_ig_dict["filesystem_objects"] = ci.nodes
			json_ig_dict["directories"] = ci.dirs
			json_ig_dict["files"] = ci.files
			json_ig_dict["chunks"] = ci.chunks
			if ci.metainfomode:
				json_ig_dict["copy_chunks"] = ci.copychunks
				json_ig_dict["ec8_chunks"] = ci.ec8chunks
				json_ig_dict["ec4_chunks"] = ci.ec4chunks
				json_ig_dict["full_chunk_copies"] = ci.chunkcopies
				json_ig_dict["ec8_chunk_parts"] = ci.chunkec8parts
				json_ig_dict["ec4_chunk_parts"] = ci.chunkec4parts
				json_ig_dict["hypothetical_chunk_copies"] = ci.chunkhypcopies
				json_ig_dict["ec_bytes_saved"] = ci.savedbyec
				if ci.savedbyec!=None:
					json_ig_dict["ec_bytes_saved_human"] = humanize_number(ci.savedbyec," ")
				else:
					json_ig_dict["ec_bytes_saved_human"] = ""
				json_ig_dict["redundancy_ratio"] = ci.dataredundancyratio
				json_ig_dict["chunks_in_ec_percent"] = ci.ecchunkspercent
			else:
				json_ig_dict["all_copies"] = ci.allcopies
				json_ig_dict["regular_copies"] = ci.regularcopies
			if ci.lastsuccessfulstore>0:
				json_ig_dict["last_metadata_save_time"] = ci.lastsuccessfulstore
				json_ig_dict["last_metadata_save_time_str"] = time.asctime(time.localtime(ci.lastsuccessfulstore))
				json_ig_dict["last_metadata_save_duration"] = ci.lastsaveseconds
				json_ig_dict["last_metadata_save_duration_human"] = timeduration_to_shortstr(ci.lastsaveseconds)
			else:
				json_ig_dict["last_metadata_save_time"] = None
				json_ig_dict["last_metadata_save_time_str"] = ""
				json_ig_dict["last_metadata_save_duration"] = None
				json_ig_dict["last_metadata_save_duration_human"] = ""
			if ci.lastsuccessfulstore>0 or ci.lastsavestatus>0:
				json_ig_dict["last_metadata_save_status"] = ci.lastsavestatus
				json_ig_dict["last_metadata_save_status_txt"] = "Saved in background" if ci.lastsavestatus==0 else "Downloaded from another master" if ci.lastsavestatus==1 else "Saved in foreground" if ci.lastsavestatus==2 else "CRC saved in background" if ci.lastsavestatus==3 else ("Unknown status: %u" % ci.lastsavestatus)
			else:
				json_ig_dict["last_metadata_save_status"] = None
				json_ig_dict["last_metadata_save_status_txt"] = ""

			json_in_dict["general"] = json_ig_dict
		else:
			if ttymode:
				tab = Table("Cluster summary",2)
			else:
				tab = Table("cluster summary",2)
			tab.defattr("l","r")
			tab.append("master version",ci.strver)
			if ci.memusage>0:
				if ttymode:
					tab.append("RAM used",humanize_number(ci.memusage," "))
				else:
					tab.append("RAM used",ci.memusage)
			else:
				tab.append("RAM used","not available")
			if ci.syscpu>0 or ci.usercpu>0:
				if ttymode:
					tab.append("CPU used","%.2f%%" % (ci.syscpu+ci.usercpu))
					tab.append("CPU used (system)","%.2f%%" % (ci.syscpu))
					tab.append("CPU used (user)","%.2f%%" % (ci.usercpu))
				else:
					tab.append("CPU used (system)","%.9f" % (ci.syscpu/100.0))
					tab.append("CPU used (user)","%.9f" % (ci.usercpu/100.0))
			else:
				tab.append("CPU used","not available")
			if ttymode:
				tab.append("total space",humanize_number(ci.totalspace," "))
				tab.append("avail space",humanize_number(ci.availspace," "))
				if ci.freespace!=None:
					tab.append("free space",humanize_number(ci.freespace," "))
				tab.append("trash space",humanize_number(ci.trspace," "))
			else:
				tab.append("total space",ci.totalspace)
				tab.append("avail space",ci.availspace)
				if ci.freespace!=None:
					tab.append("free space",ci.freespace)
				tab.append("trash space",ci.trspace)
			tab.append("trash files",ci.trfiles)
			if ttymode:
				tab.append("sustained space",humanize_number(ci.respace," "))
			else:
				tab.append("sustained space",ci.respace)
			tab.append("sustained files",ci.refiles)
			tab.append("all fs objects",ci.nodes)
			tab.append("directories",ci.dirs)
			tab.append("files",ci.files)
			tab.append("chunks",ci.chunks)
			if ci.metainfomode:
				if ttymode:
					if ci.ecchunkspercent!=None:
						tab.append("percent of chunks in EC format","%.1f%%" % ci.ecchunkspercent)
					if ci.dataredundancyratio!=None:
						tab.append("real data redundancy ratio","%.3f" % ci.dataredundancyratio)
					if ci.savedbyec!=None:
						tab.append("storage bytes saved by EC",humanize_number(ci.savedbyec," "))
				else:
					if ci.copychunks!=None:
						tab.append("chunks in copy format",ci.copychunks)
					if ci.ec8chunks!=None:
						tab.append("chunks in EC8 format",ci.ec8chunks)
					if ci.ec4chunks!=None:
						tab.append("chunks in EC4 format",ci.ec4chunks)
					if ci.chunkcopies!=None:
						tab.append("all full chunk copies",ci.chunkcopies)
					if ci.chunkec8parts!=None:
						tab.append("all chunk EC8 parts",ci.chunkec8parts)
					if ci.chunkec4parts!=None:
						tab.append("all chunk EC4 parts",ci.chunkec4parts)
					if ci.chunkhypcopies!=None:
						tab.append("hypothetical full chunk copies",ci.chunkhypcopies)
					if ci.savedbyec!=None:
						tab.append("storage bytes saved by EC",ci.savedbyec)
			else:
				if ci.allcopies!=None:
					tab.append("all chunk copies",ci.allcopies)
				if ci.regularcopies!=None:
					tab.append("regular chunk copies",ci.regularcopies)
			if ci.lastsuccessfulstore>0:
				if ttymode:
					tab.append("last successful store",time.asctime(time.localtime(ci.lastsuccessfulstore)))
					tab.append("last save duration",timeduration_to_shortstr(ci.lastsaveseconds))
				else:
					tab.append("last successful store",ci.lastsuccessfulstore)
					tab.append("last save duration","%.3f" % ci.lastsaveseconds)
			else:
				tab.append("last successful store","-")
				tab.append("last save duration","-")
			if ci.lastsuccessfulstore>0 or ci.lastsavestatus>0:
				tab.append("last save status",("Saved in background","4") if ci.lastsavestatus==0 else ("Downloaded from another master","4") if ci.lastsavestatus==1 else ("Saved in foreground","2") if ci.lastsavestatus==2 else ("CRC saved in background","5") if ci.lastsavestatus==3 else ("Unknown status: %u" % ci.lastsavestatus,"1"))
			else:
				tab.append("last save status","-")
			print(myunicode(tab))
	except Exception:
		print_exception()

#Metadata servers subsection
if org.shall_render("IM"):
	try:
		if jsonmode:
			json_im_array = []
		elif ttymode:
			tab = Table("Metadata Servers (masters)",15,"r")
			tab.header("ip","version","state","local time","metadata version","metadata id","metadata delay","RAM used","CPU used","last meta save","last save duration","last save status","last save version","last save checksum","exports checksum")
		else:
			tab = Table("metadata servers (masters)",15)

		# update master servers delay times prior to getting the list of master servers
		highest_saved_metaversion, highest_metaversion_checksum = cl.update_masterservers_delays()
		mservers = dataprovider.get_masterservers(IMorder, IMrev)
		for ms in mservers:
			if jsonmode:
				json_im_dict = {}
				json_im_dict["ip"] = ms.strip
				json_im_dict["strver"] = ms.strver
				if ms.strver.endswith(" PRO"):
					json_im_dict["version"] = ms.strver[:-4]
					json_im_dict["pro"] = True
				else:
					json_im_dict["version"] = ms.strver
					json_im_dict["pro"] = False
				json_im_dict["state"] = ms.statestr
				if ms.usectime==None:
					json_im_dict["localtime"] = None
					json_im_dict["localtime_str"] = ""
				else:
					json_im_dict["localtime"] = ms.usectime/1000000.0
					json_im_dict["localtime_str"] = time.asctime(time.localtime(ms.usectime//1000000))
				json_im_dict["metadata_version"] = ms.metaversion
				json_im_dict["metadata_id"] = ms.metaid
				json_im_dict["metadata_delay"] = ms.metadelay
				if ms.memusage>0:
					json_im_dict["memory_usage"] = ms.memusage
					json_im_dict["memory_usage_human"] = humanize_number(ms.memusage," ")
				else:
					json_im_dict["memory_usage"] = None
					json_im_dict["memory_usage_human"] = ""
				if ms.syscpu>0 or ms.usercpu>0:
					json_im_dict["cpu_usage_percent"] = ms.syscpu+ms.usercpu
					json_im_dict["cpu_system_percent"] = ms.syscpu
					json_im_dict["cpu_user_percent"] = ms.usercpu
				else:
					json_im_dict["cpu_usage_percent"] = None
					json_im_dict["cpu_system_percent"] = None
					json_im_dict["cpu_user_percent"] = None
				if ms.lastsuccessfulstore>0:
					json_im_dict["last_metadata_save_time"] = ms.lastsuccessfulstore
					json_im_dict["last_metadata_save_time_str"] = time.asctime(time.localtime(ms.lastsuccessfulstore))
					json_im_dict["last_metadata_save_duration"] = ms.lastsaveseconds
					json_im_dict["last_metadata_save_duration_human"] = timeduration_to_shortstr(ms.lastsaveseconds)
				else:
					json_im_dict["last_metadata_save_time"] = None
					json_im_dict["last_metadata_save_time_str"] = ""
					json_im_dict["last_metadata_save_duration"] = None
					json_im_dict["last_metadata_save_duration_human"] = ""
				if ms.lastsuccessfulstore>0 or ms.lastsavestatus>0:
					json_im_dict["last_metadata_save_status"] = ms.lastsavestatus
					json_im_dict["last_metadata_save_status_txt"] = "Saved in background" if ms.lastsavestatus==0 else "Downloaded from other master" if ms.lastsavestatus==1 else "Saved in foreground" if ms.lastsavestatus==2 else "CRC saved in background" if ms.lastsavestatus==3 else ("Unknown status: %u" % ms.lastsavestatus)
				else:
					json_im_dict["last_metadata_save_status"] = None
					json_im_dict["last_metadata_save_status_txt"] = ""
				if ms.lastsuccessfulstore>0 and (ms.lastsavestatus==0 or ms.lastsavestatus>=2) and ms.lastsavemetaversion!=None:
					json_im_dict["last_metadata_save_version"] = ms.lastsavemetaversion
				else:
					json_im_dict["last_metadata_save_version"] = None
				if ms.lastsuccessfulstore>0 and (ms.lastsavestatus==0 or ms.lastsavestatus>=2) and ms.lastsavemetaversion!=None and ms.lastsavemetachecksum!=None:
					json_im_dict["last_metadata_save_checksum"] = "%08X" % ms.lastsavemetachecksum
				else:
					json_im_dict["last_metadata_save_checksum"] = None
				if ms.exportschecksum!=None:
					json_im_dict["exports_checksum"] = "%016X" % ms.exportschecksum
				else:
					json_im_dict["exports_checksum"] = None
				json_im_array.append(json_im_dict)
			else:
				clist = [ms.strip,ms.strver,(ms.statestr,"c%u" % ms.statecolor)]
				if ms.usectime==None:
					clist.append("not available")
				else:
					if ttymode:
						clist.append((time.asctime(time.localtime(ms.usectime//1000000)),("1" if ms.secdelta>2.0 else "3" if ms.secdelta>1.0 else "4" if ms.secdelta>0.0 else "0")))
					else:
						clist.append("%.6lf" % (ms.usectime/1000000.0))
				clist.append(decimal_number(ms.metaversion))
				if ms.metaid!=None:
					clist.append((("%016X" % ms.metaid),("1" if ms.metaid != cl.master_metaid() else "4")))
				else:
					clist.append("-")
				if ms.metadelay==None:
					clist.append("not available")
				else:
					if ttymode:
						clist.append((("%.0f s" % ms.metadelay),("4" if ms.metadelay<1.0 else "3" if ms.metadelay<6.0 else "1")))
					else:
						clist.append(int(ms.metadelay))
				if ms.memusage>0:
					if ttymode:
						clist.append(humanize_number(ms.memusage," "))
					else:
						clist.append(ms.memusage)
				else:
					clist.append("not available")
				if ms.syscpu>0 or ms.usercpu>0:
					if ttymode:
						clist.append("all:%.2f%% sys:%.2f%% user:%.2f%%" % (ms.syscpu+ms.usercpu,ms.syscpu,ms.usercpu))
					else:
						clist.append("all:%.7f%% sys:%.7f%% user:%.7f%%" % (ms.syscpu+ms.usercpu,ms.syscpu,ms.usercpu))
				else:
					clist.append("not available")
				if ms.lastsuccessfulstore>0:
					if ttymode:
						clist.append(time.asctime(time.localtime(ms.lastsuccessfulstore)))
						clist.append(timeduration_to_shortstr(ms.lastsaveseconds))
					else:
						clist.append(ms.lastsuccessfulstore)
						clist.append("%.3f" % ms.lastsaveseconds)
				else:
					clist.append("-")
					clist.append("-")
				if ms.lastsuccessfulstore>0 or ms.lastsavestatus>0:
					clist.append(("Saved in background","4") if ms.lastsavestatus==0 else ("Downloaded from other master","4") if ms.lastsavestatus==1 else ("Saved in foreground","2") if ms.lastsavestatus==2 else ("CRC saved in background","5") if ms.lastsavestatus==3 else ("Unknown status: %u" % ms.lastsavestatus,"1"))
				else:
					clist.append("-")
				if ms.lastsuccessfulstore>0 and (ms.lastsavestatus==0 or ms.lastsavestatus>=2) and ms.lastsavemetaversion!=None:
					clist.append((decimal_number(ms.lastsavemetaversion),("4" if ms.lastsavemetaversion==highest_saved_metaversion else "8")))
				else:
					clist.append("-")
				if ms.lastsuccessfulstore>0 and (ms.lastsavestatus==0 or ms.lastsavestatus>=2) and ms.lastsavemetaversion!=None and ms.lastsavemetachecksum!=None:
					clist.append((("%08X" % ms.lastsavemetachecksum),("4" if ms.lastsavemetaversion==highest_saved_metaversion and ms.lastsavemetachecksum==highest_metaversion_checksum else "1" if ms.lastsavemetaversion==highest_saved_metaversion and ms.lastsavemetachecksum!=highest_metaversion_checksum else "8")))
				else:
					clist.append("-")
				if ms.exportschecksum!=None:
					clist.append((("%016X" % ms.exportschecksum),("1" if ms.exportschecksum != cl.master_exportschecksum() else "4")))
				else:
					clist.append("-")
				tab.append(*clist)

		if len(mservers)==0:
			if not jsonmode:
				tab.append(("""Servers not found! Check your DNS""","c",9))

		if jsonmode:
			json_in_dict["masters"] = json_im_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()


# Chunks matrix
if org.shall_render("IC"):
	try:
		sclass_dict = {}
		sclass_name = "Error"
		sclass_desc = "Error"
		if cl.master().has_feature(FEATURE_SCLASS_IN_MATRIX):
			sclass_dict = dataprovider.get_used_sclasses_names()
			if ICsclassid<0:
				sclass_name = ""
				sclass_desc = "all storage classes"
			elif ICsclassid in sclass_dict:
				sclass_name = sclass_dict[ICsclassid].name
				sclass_desc = "storage class '%s'" % sclass_name
			else:
				sclass_name = "[%u]" % ICsclassid
				sclass_desc = "non existent storage class id:%u" % ICsclassid
		(matrix,progressstatus)=dataprovider.get_matrix(ICsclassid)
		progressstr = "disconnections" if (progressstatus==1) else "connections" if (progressstatus==2) else "connections and disconnections"
		if len(matrix)==2 or len(matrix)==6 or len(matrix)==8:
			if jsonmode:
				json_ic_dict = {}
				json_ic_sum_dict = {}
				matrixkeys = ['allchunks','regularchunks','allchunks_copies','regularchunks_copies','allchunks_ec8','regularchunks_ec8','allchunks_ec4', 'regularchunks_ec4']
				sumkeys = ['missing','endangered','undergoal','stable','overgoal','pending_deletion','ready_to_remove']
				#sumlist has to be clean for JSON mode
				sumlist = []
			elif ttymode:
				tabtypesc = "" if (ICsclassid<0) else (" (data only for %s)" % sclass_desc)
				tabtypear = "Regular" if (ICmatrix&1) else "All"
				tabtypeec = "Both copies and EC chunks" if ((ICmatrix>>1)==0) else "Only chunks stored in full copies" if ((ICmatrix>>1)==1) else "Only chunks stored using Raid-like Erasure Codes in 8+N format" if ((ICmatrix>>1)==2) else "Only chunks stored using Raid-like Erasure Codes in 4+N format"
				tab = Table("%s chunks state matrix%s - %s mode" % (tabtypear,tabtypesc,tabtypeec),13,"r")
				if progressstatus>0:
					tab.header(("Warning: counters may not be valid - %s in progress" % progressstr,"1c",13))
					tab.header(("---","",13))
				if (ICmatrix>>1)==0:
					tab.header("target",("current redundancy level (RL = N means EC: (8+N) or (4+N) parts, GOAL: (1+N) copies)","",12))
				elif (ICmatrix>>1)==1:
					tab.header("target",("current number of valid copies","",12))
				elif (ICmatrix>>1)==2:
					tab.header("target",("current number of EC8 parts","",12))
				else:
					tab.header("target",("current number of EC4 parts","",12))
				tab.header("redundancy",("---","",12))
				if (ICmatrix>>1)==0:
					tab.header("level"," missing ","  RL = 0  ","  RL = 1  ","  RL = 2  ","  RL = 3  ","  RL = 4  ","  RL = 5  ","  RL = 6  ","  RL = 7  ","  RL = 8  ","  RL >= 9  ","   all   ")
				elif (ICmatrix>>1)==1:
					tab.header("level"," missing ","1+0 copies","1+1 copies","1+2 copies","1+3 copies","1+4 copies","1+5 copies","1+6 copies","1+7 copies","1+8 copies",">1+9 copies","   all   ")
				elif (ICmatrix>>1)==2:
					tab.header("level"," missing ","8+0 parts ","8+1 parts ","8+2 parts ","8+3 parts ","8+4 parts ","8+5 parts ","8+6 parts ","8+7 parts ","8+8 parts ",">8+9 parts ","   all   ")
				else:
					tab.header("level"," missing ","4+0 parts ","4+1 parts ","4+2 parts ","4+3 parts ","4+4 parts ","4+5 parts ","4+6 parts ","4+7 parts ","4+8 parts ",">4+9 parts ","   all   ")
			else:
				out = []
				if ICmatrix==0:
					mtypeprefix=("all chunks matrix:%s" % ptxtsep)
				elif ICmatrix==1:
					mtypeprefix=("regular chunks matrix:%s" % ptxtsep)
				elif ICmatrix==2:
					mtypeprefix=("all chunks matrix - copies only:%s" % ptxtsep)
				elif ICmatrix==3:
					mtypeprefix=("regular chunks matrix - copies only:%s" % ptxtsep)
				elif ICmatrix==4:
					mtypeprefix=("all chunks matrix - EC8 only:%s" % ptxtsep)
				elif ICmatrix==5:
					mtypeprefix=("regular chunks matrix - EC8 only:%s" % ptxtsep)
				elif ICmatrix==6:
					mtypeprefix=("all chunks matrix - EC4 only:%s" % ptxtsep)
				elif ICmatrix==7:
					mtypeprefix=("regular chunks matrix - EC4 only:%s" % ptxtsep)
				else:
					mtypeprefix=("UNKNOWN chunks matrix:%s" % ptxtsep)
				if ICsclassid>=0:
					mtypeprefix=("%s%s%s" % (mtypeprefix,sclass_desc,ptxtsep))
			classsum = []
			sumlist = []
			for i in range(len(matrix)):
				classsum.append(7*[0])
				sumlist.append(11*[0])
			left_col_once=1
			for goal in range(11):
				if goal==0:
					clist = ["deleted"]
				else:
					if (ICmatrix>>1)==0:
						clist = ["RL = %u" % (goal-1)]
					elif (ICmatrix>>1)==1:
						if goal>9:
							clist = ["-"]
						else:
							clist = ["1+%u copies" % (goal-1)]
					elif (ICmatrix>>1)==2:
						if goal==1:
							clist = ["-"]
						else:
							clist = ["8+%u parts" % (goal-1)]
					else:
						if goal==1:
							clist = ["-"]
						else:
							clist = ["4+%u parts" % (goal-1)]
				for actual in range(11):
					(col,_)=redundancy2colclass(goal,actual)
					for i in range(len(matrix)):
						classsum[i][col]+=matrix[i][goal][actual]
					if jsonmode:
						pass
					elif ttymode:
						if ICmatrix<len(matrix) and matrix[ICmatrix][goal][actual]>0:
							clist.append((matrix[ICmatrix][goal][actual],"1234678"[col]))
						else:
							clist.append("-")
					else:
						if ICmatrix<len(matrix) and matrix[ICmatrix][goal][actual]>0:
							if goal==0:
								goalstr = "missing"
							else:
								if ICmatrix>>1==0:
									goalstr = "%u" % (goal-1)
								elif ICmatrix>>1==1:
									goalstr = "1+%u" % (goal-1)
								elif ICmatrix>>1==2:
									goalstr = "8+%u" % (goal-1)
								else:
									goalstr = "4+%u" % (goal-1)
							if actual==0:
								vcstr = "missing"
							else:
								if ICmatrix>>1==0:
									vcstr = "%u" % (actual-1)
								elif ICmatrix>>1==1:
									vcstr = "1+%u" % (actual-1)
								elif ICmatrix>>1==2:
									vcstr = "8+%u" % (actual-1)
								else:
									vcstr = "4+%u" % (actual-1)
							if ICmatrix>>1==0:
								descstr = "redundancy level"
							elif ICmatrix>>1==1:
								descstr = "copies"
							elif ICmatrix>>1==2:
								descstr = "EC8 parts"
							else:
								descstr = "EC4 parts"
							out.append("""%s%s target/current/chunks:%s%s%s%s%s%u""" % (mtypeprefix,descstr,ptxtsep,goalstr,ptxtsep,vcstr,ptxtsep,matrix[ICmatrix][goal][actual]))
				if ttymode:
					clist.append(sum(matrix[ICmatrix][goal]))
					tab.append(*clist)
				if goal>0:
					for i in range(len(matrix)):
						sumlist[i] = [ a + b for (a,b) in zip(sumlist[i],matrix[i][goal])]
			if jsonmode:
				#Go through all available matrices:
				for mx in range(len(matrix)):
					classsum = []
					for i in range(len(matrix)):
						classsum.append(7*[0])
						sumlist.append(11*[0])
					cssumdict = {}
					mxarray = []
					if mx>7:
						mtypeprefix = "unknown"
					else:
						mtypeprefix = matrixkeys[mx]
					for goal in range(11):
						for actual in range(11):
							tccdict = {}
							(col,_)=redundancy2colclass(goal,actual)
							for i in range(len(matrix)):
								classsum[i][col]+=matrix[i][goal][actual]
							#Goal String
							if matrix[mx][goal][actual]>0:
								if goal==0:
									goalstr = "0"
								else:
									if mx>>1==0:
										goalstr = "%u" % (goal)
									elif mx>>1==1:
										goalstr = "1+%u" % (goal)
									elif mx>>1==2:
										goalstr = "8+%u" % (goal)
									else:
										goalstr = "4+%u" % (goal)
								if actual==0:
									vcstr = "0"
								else:
									if mx>>1==0:
										vcstr = "%u" % (actual)
									elif mx>>1==1:
										vcstr = "1+%u" % (actual)
									elif mx>>1==2:
										vcstr = "8+%u" % (actual)
									else:
										vcstr = "4+%u" % (actual)
								if mx>>1==0:
									descstr = "redundancy level"
								elif mx>>1==1:
									descstr = "copies"
								elif mx>>1==2:
									descstr = "EC 8 parts"
								else:
									descstr = "EC 4 parts"
								tccdict['target'] = goalstr
								tccdict['current'] = vcstr
								tccdict['chunks'] = matrix[mx][goal][actual]
								mxarray.append(tccdict)
					
					for index,key in enumerate(sumkeys):
						cssumdict[key] = classsum[mx][index]
					json_ic_sum_dict[matrixkeys[mx]] = cssumdict
					json_ic_dict[mtypeprefix]=mxarray

				json_ic_dict['summary'] = json_ic_sum_dict
				json_ic_dict['progress_status'] = progressstatus
				if progressstatus>0:
					json_ic_dict['progress_str'] = "counters may not be valid - %s in progress" % progressstr
				else:
					json_ic_dict['progress_str'] = "counters are valid"
				json_ic_dict['storage_class_id'] = ICsclassid
				json_ic_dict['storage_class_name'] = sclass_name
				json_ic_dict['storage_class_desc'] = sclass_desc
				#Add created dictionaries to the main dictionary
				json_in_dict['chunks'] = json_ic_dict
			elif ttymode:
				clist = ["all 1+"]
				for actual in range(11):
					clist.append(sumlist[ICmatrix][actual])
				clist.append(sum(sumlist[ICmatrix]))
				tab.append(*clist)
				tab.append(("---","",13))
				tab.append(("missing: %u / endangered: %u / undergoal: %u / stable: %u / overgoal: %u / pending deletion: %u / to be removed: %u" % (classsum[ICmatrix][0],classsum[ICmatrix][1],classsum[ICmatrix][2],classsum[ICmatrix][3],classsum[ICmatrix][4],classsum[ICmatrix][5],classsum[ICmatrix][6]),"c",13))
				print(myunicode(tab))
			else:
				if ICmatrix < len(classsum):
					out.append("%schunkclass missing:%s%u" % (mtypeprefix,ptxtsep,classsum[ICmatrix][0]))
					out.append("%schunkclass endangered:%s%u" % (mtypeprefix,ptxtsep,classsum[ICmatrix][1]))
					out.append("%schunkclass undergoal:%s%u" % (mtypeprefix,ptxtsep,classsum[ICmatrix][2]))
					out.append("%schunkclass stable:%s%u" % (mtypeprefix,ptxtsep,classsum[ICmatrix][3]))
					out.append("%schunkclass overgoal:%s%u" % (mtypeprefix,ptxtsep,classsum[ICmatrix][4]))
					out.append("%schunkclass pending deletion:%s%u" % (mtypeprefix,ptxtsep,classsum[ICmatrix][5]))
					out.append("%schunkclass to be removed:%s%u" % (mtypeprefix,ptxtsep,classsum[ICmatrix][6]))
				print(str(Table("",0))+"\n".join(out))
	except Exception:
		print_exception()

# Both filesystem self-chek and chunk house-keeping loop (CLI only)
if org.shall_render("IL"):
	try: # Filesystem self-check loop
		hsc = dataprovider.get_health_selfcheck()
		if jsonmode:
			if hsc.loopstart>0:
				json_il_dict = {}
				json_il_dict["start_time"] = hsc.loopstart
				json_il_dict["end_time"] = hsc.loopend
				json_il_dict["start_time_str"] = time.asctime(time.localtime(hsc.loopstart))
				json_il_dict["end_time_str"] = time.asctime(time.localtime(hsc.loopend))
				json_il_dict["files"] = hsc.files
				json_il_dict["undergoal_files"] = hsc.ugfiles
				json_il_dict["missing_files"] = hsc.mfiles
				if hsc.mtfiles!=None:
					json_il_dict["missing_trash_files"] = hsc.mtfiles
				if hsc.msfiles!=None:
					json_il_dict["missing_sustained_files"] = hsc.msfiles
				json_il_dict["chunks"] = hsc.chunks
				json_il_dict["undergoal_chunks"] = hsc.ugchunks
				json_il_dict["missing_chunks"] = hsc.mchunks
				json_in_dict['fs_loop'] = json_il_dict
			else:
				json_in_dict['fs_loop'] = None
		elif ttymode:
			tab = Table("Filesystem self-check",10,"r")
			tabwidth = 10
			tab.header("check loop start time","check loop end time","files","under-goal files","missing files","missing trash files","missing sustained files","chunks","under-goal chunks","missing chunks")
			if hsc.loopstart>0:
				if hsc.mtfiles!=None and hsc.msfiles!=None:
					tab.append((time.asctime(time.localtime(hsc.loopstart)),"c"),(time.asctime(time.localtime(hsc.loopend)),"c"),hsc.files,hsc.ugfiles,hsc.mfiles,hsc.mtfiles,hsc.msfiles,hsc.chunks,hsc.ugchunks,hsc.mchunks)
				else:
					tab.append((time.asctime(time.localtime(hsc.loopstart)),"c"),(time.asctime(time.localtime(hsc.loopend)),"c"),hsc.files,hsc.ugfiles,hsc.mfiles,'n/a','n/a',hsc.chunks,hsc.ugchunks,hsc.mchunks)
				if hsc.msgbuffleng>0:
					tab.append(("---","",tabwidth))
					if hsc.msgbuffleng==100000:
						tab.append(("Important messages (first 100k):","c",tabwidth))
					else:
						tab.append(("Important messages:","c",tabwidth))
					tab.append(("---","",tabwidth))
					for line in hsc.datastr.strip().split("\n"):
						tab.append((line.strip(),"l",tabwidth))
			else:
				tab.append(("no data","c",tabwidth))
			print(myunicode(tab))
		else:
			out = []
			if hsc.loopstart>0:
				out.append("""check loop%sstart:%s%u""" % (ptxtsep,ptxtsep,hsc.loopstart))
				out.append("""check loop%send:%s%u""" % (ptxtsep,ptxtsep,hsc.loopend))
				out.append("""check loop%sfiles:%s%u""" % (ptxtsep,ptxtsep,hsc.files))
				out.append("""check loop%sunder-goal files:%s%u""" % (ptxtsep,ptxtsep,hsc.ugfiles))
				out.append("""check loop%smissing files:%s%u""" % (ptxtsep,ptxtsep,hsc.mfiles))
				if hsc.mtfiles!=None:
					out.append("""check loop%smissing trash files:%s%u""" % (ptxtsep,ptxtsep,hsc.mtfiles))
				if hsc.msfiles!=None:
					out.append("""check loop%smissing sustained files:%s%u""" % (ptxtsep,ptxtsep,hsc.msfiles))
				out.append("""check loop%schunks:%s%u""" % (ptxtsep,ptxtsep,hsc.chunks))
				out.append("""check loop%sunder-goal chunks:%s%u""" % (ptxtsep,ptxtsep,hsc.ugchunks))
				out.append("""check loop%smissing chunks:%s%u""" % (ptxtsep,ptxtsep,hsc.mchunks))
				if hsc.msgbuffleng>0:
					for line in hsc.datastr.strip().split("\n"):
						out.append("check loop%simportant messages:%s%s" % (ptxtsep,ptxtsep,line.strip()))
			else:
				out.append("""check loop: no data""")
			print(str(Table("",0))+"\n".join(out))
	except Exception:
		print_exception()

	try: # Chunk operations loop
		ci = dataprovider.get_chunktestinfo()
		if type(ci)==ChunkTestInfo72:
			if jsonmode:
				if ci.loopstart>0:
					json_il_dict = {}
					json_il_dict["start_time"] = ci.loopstart
					json_il_dict["end_time"] = ci.loopend
					json_il_dict["start_time_str"] = time.asctime(time.localtime(ci.loopstart))
					json_il_dict["end_time_str"] = time.asctime(time.localtime(ci.loopend))
					json_il_dict["del_invalid"] = ci.del_invalid
					json_il_dict["ndel_invalid"] = ci.ndel_invalid
					json_il_dict["del_unused"] = ci.del_unused
					json_il_dict["ndel_unused"] = ci.ndel_unused
					json_il_dict["del_dclean"] = ci.del_dclean
					json_il_dict["ndel_dclean"] = ci.ndel_dclean
					json_il_dict["del_ogoal"] = ci.del_ogoal
					json_il_dict["ndel_ogoal"] = ci.ndel_ogoal
					json_il_dict["rep_ugoal"] = ci.rep_ugoal
					json_il_dict["nrep_ugoal"] = ci.nrep_ugoal
					json_il_dict["rep_wlab"] = ci.rep_wlab
					json_il_dict["nrep_wlab"] = ci.nrep_wlab
					json_il_dict["rebalance"] = ci.rebalance
					json_il_dict["locked_unused"] = ci.locked_unused
					json_il_dict["locked_used"] = ci.locked_used
					json_in_dict["chunk_loop"] = json_il_dict
				else:
					json_in_dict["chunk_loop"] = None
			elif ttymode:
				tab = Table("Chunk operations info",11,"r")
				tab.header(("loop time","",2),("deletions","",4),("replications","",3),("locked","",2))
				tab.header(("---","",11))
				tab.header("start","end","invalid","unused","disk clean","over goal","under goal","wrong labels","rebalance","unused","used")
				if ci.loopstart>0:
					tab.append((time.asctime(time.localtime(ci.loopstart)),"c"),(time.asctime(time.localtime(ci.loopend)),"c"),"%u/%u" % (ci.del_invalid,ci.del_invalid+ci.ndel_invalid),"%u/%u" % (ci.del_unused,ci.del_unused+ci.ndel_unused),"%u/%u" % (ci.del_dclean,ci.del_dclean+ci.ndel_dclean),"%u/%u" % (ci.del_ogoal,ci.del_ogoal+ci.ndel_ogoal),"%u/%u" % (ci.rep_ugoal,ci.rep_ugoal+ci.nrep_ugoal),"%u/%u/%u" % (ci.rep_wlab,ci.labels_dont_match,ci.rep_wlab+ci.nrep_wlab+ci.labels_dont_match),ci.rebalance,ci.locked_unused,ci.locked_used)
				else:
					tab.append(("no data yet","c",11))
				print(myunicode(tab))
			else:
				out = []
				if ci.loopstart>0:
					out.append("""chunk loop%sstart:%s%u""" % (ptxtsep,ptxtsep,ci.loopstart))
					out.append("""chunk loop%send:%s%u""" % (ptxtsep,ptxtsep,ci.loopend))
					out.append("""chunk loop%sdeletions%sinvalid:%s%u/%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.del_invalid,ci.del_invalid+ci.ndel_invalid))
					out.append("""chunk loop%sdeletions%sunused:%s%u/%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.del_unused,ci.del_unused+ci.ndel_unused))
					out.append("""chunk loop%sdeletions%sdisk clean:%s%u/%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.del_dclean,ci.del_dclean+ci.ndel_dclean))
					out.append("""chunk loop%sdeletions%sover goal:%s%u/%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.del_ogoal,ci.del_ogoal+ci.ndel_ogoal))
					out.append("""chunk loop%sreplications%sunder goal:%s%u/%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.rep_ugoal,ci.rep_ugoal+ci.nrep_ugoal))
					out.append("""chunk loop%sreplications%swrong labels:%s%u/%u/%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.rep_wlab,ci.labels_dont_match,ci.rep_wlab+ci.nrep_wlab+ci.labels_dont_match))
					out.append("""chunk loop%sreplications%srebalance:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.rebalance))
					out.append("""chunk loop%slocked%sunused:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.locked_unused))
					out.append("""chunk loop%slocked%sused:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.locked_used))
				else:
					out.append("""chunk loop%sno data yet""" % ptxtsep)
				print(str(Table("",0))+"\n".join(out))
		elif type(ci)==ChunkTestInfo96:
			if jsonmode:
				if ci.loopstart>0:
					json_il_dict = {}
					json_il_dict["start_time"] =ci.loopstart
					json_il_dict["end_time"] = ci.loopend
					json_il_dict["start_time_str"] = time.asctime(time.localtime(ci.loopstart))
					json_il_dict["end_time_str"] = time.asctime(time.localtime(ci.loopend))
					json_il_dict["fixed_chunks"] = ci.fixed
					json_il_dict["forced_keep"] = ci.forcekeep
					json_il_dict["locked_unused"] = ci.locked_unused
					json_il_dict["locked_used"] = ci.locked_used
					json_il_dict["del_invalid_copies"] = ci.delete_invalid
					json_il_dict["del_no_longer_needed"] = ci.delete_no_longer_needed
					json_il_dict["del_wrong_version"] = ci.delete_wrong_version
					json_il_dict["del_duplicate_ecpart"] = ci.delete_duplicated_ecpart
					json_il_dict["del_excess_ecpart"] = ci.delete_excess_ecpart
					json_il_dict["del_excess_copy"] = ci.delete_excess_copy
					json_il_dict["del_mfr_ecpart"] = ci.delete_diskclean_ecpart
					json_il_dict["del_mfr_copy"] = ci.delete_diskclean_copy
					json_il_dict["rep_dupserver_ecpart"] = ci.replicate_dupserver_ecpart
					json_il_dict["rep_needed_ecpart"] = ci.replicate_needed_ecpart
					json_il_dict["rep_needed_copy"] = ci.replicate_needed_copy
					json_il_dict["rep_wronglabels_ecpart"] = ci.replicate_wronglabels_ecpart
					json_il_dict["rep_wronglabels_copy"] = ci.replicate_wronglabels_copy
					json_il_dict["rep_split_copy_into_ecparts"] = ci.split_copy_into_ecparts
					json_il_dict["rep_join_ecparts_into_copy"] = ci.join_ecparts_into_copy
					json_il_dict["rep_recover_ecpart"] = ci.recover_ecpart
					json_il_dict["rep_calculate_ecchksum"] = ci.calculate_ecchksum
					json_il_dict["rep_replicate_rebalance"] = ci.replicate_rebalance
					json_in_dict["chunk_loop"] = json_il_dict
				else:
					json_in_dict["chunk_loop"] = None
			elif ttymode:
				if ci.loopstart>0:
					tab = Table("Chunks housekeeping operations",2)
					tab.defattr("l","r")
					tab.append("Loop start",time.asctime(time.localtime(ci.loopstart)))
					tab.append("Loop end",time.asctime(time.localtime(ci.loopend)))
					tab.append("Locked unused chunks",ci.locked_unused)
					tab.append("Locked chunks",ci.locked_used)
					tab.append("Fixed chunks",ci.fixed)
					tab.append("Forced keep mode",ci.forcekeep)
					tab.append(("---","",2))
					tab.append(("Chunk copies - deletions","c",2))
					tab.append(("---","",2))
					tab.append("Invalid",ci.delete_invalid)
					tab.append("Removed",ci.delete_no_longer_needed)
					tab.append("Wrong version",ci.delete_wrong_version)
					tab.append("Excess",ci.delete_excess_copy)
					tab.append("Marked for removal",ci.delete_diskclean_copy)
					tab.append(("---","",2))
					tab.append(("Chunk copies - replications","c",2))
					tab.append(("---","",2))
					tab.append("Needed",ci.replicate_needed_copy)
					tab.append("Wrong labels",ci.replicate_wronglabels_copy)
					tab.append(("---","",2))
					tab.append(("EC parts - deletions","c",2))
					tab.append(("---","",2))
					tab.append("Duplicated",ci.delete_duplicated_ecpart)
					tab.append("Excess",ci.delete_excess_ecpart)
					tab.append("Marked for removal",ci.delete_diskclean_ecpart)
					tab.append(("---","",2))
					tab.append(("EC parts - replications","c",2))
					tab.append(("---","",2))
					tab.append("Duplicated server",ci.replicate_dupserver_ecpart)
					tab.append("Needed",ci.replicate_needed_ecpart)
					tab.append("Wrong labels",ci.replicate_wronglabels_ecpart)
					tab.append("Recovered",ci.recover_ecpart)
					tab.append("Calculated checksums",ci.calculate_ecchksum)
					tab.append(("---","",2))
					tab.append(("Copies <-> EC parts","c",2))
					tab.append(("---","",2))
					tab.append("Split: copies -> EC parts",ci.split_copy_into_ecparts)
					tab.append("Join: EC parts -> copies",ci.join_ecparts_into_copy)
					tab.append(("---","",2))
					tab.append(("Replications","c",2))
					tab.append(("---","",2))
					tab.append("rebalance",ci.replicate_rebalance)
					print(myunicode(tab))
				else:
					tab = Table("Chunks housekeeping operations",1,"c")
					tab.append("no data yet")
					print(myunicode(tab))
			else:
				out = []
				if ci.loopstart>0:
					out.append("""chunk loop%sgeneral info%sloop start:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.loopstart))
					out.append("""chunk loop%sgeneral info%sloop end:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.loopend))
					out.append("""chunk loop%sgeneral info%slocked unused chunks:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.locked_unused))
					out.append("""chunk loop%sgeneral info%slocked chunks:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.locked_used))
					out.append("""chunk loop%sgeneral info%sfixed chunks:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.fixed))
					out.append("""chunk loop%sgeneral info%sforced keep mode:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.forcekeep))
					out.append("""chunk loop%sdeletions%sinvalid copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_invalid))
					out.append("""chunk loop%sdeletions%sremoved chunk copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_no_longer_needed))
					out.append("""chunk loop%sdeletions%swrong version copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_wrong_version))
					out.append("""chunk loop%sdeletions%sduplicated EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_duplicated_ecpart))
					out.append("""chunk loop%sdeletions%sexcess EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_excess_ecpart))
					out.append("""chunk loop%sdeletions%sexcess copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_excess_copy))
					out.append("""chunk loop%sdeletions%sMFR EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_diskclean_ecpart))
					out.append("""chunk loop%sdeletions%sMFR copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.delete_diskclean_copy))
					out.append("""chunk loop%sreplications%sdup server EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.replicate_dupserver_ecpart))
					out.append("""chunk loop%sreplications%sneeded EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.replicate_needed_ecpart))
					out.append("""chunk loop%sreplications%sneeded copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.replicate_needed_copy))
					out.append("""chunk loop%sreplications%swrong labels EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.replicate_wronglabels_ecpart))
					out.append("""chunk loop%sreplications%swrong labels copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.replicate_wronglabels_copy))
					out.append("""chunk loop%sreplications%srecovered EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.recover_ecpart))
					out.append("""chunk loop%sreplications%scalculated EC checksums:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.calculate_ecchksum))
					out.append("""chunk loop%sreplications%ssplit copies into EC parts:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.split_copy_into_ecparts))
					out.append("""chunk loop%sreplications%sjoined EC parts into copies:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.join_ecparts_into_copy))
					out.append("""chunk loop%sreplications%srebalance:%s%u""" % (ptxtsep,ptxtsep,ptxtsep,ci.replicate_rebalance))
				else:
					out.append("""chunk loop%sno data yet""" % ptxtsep)
				print(str(Table("",0))+"\n".join(out))
	except Exception:
		print_exception()

# Missing files (CLI only)
if org.shall_render("MF"):
	try:
		mchunks = dataprovider.get_missing_chunks(MForder,MFrev)
		mccnt = len(mchunks)
		
		if jsonmode:
			json_mf_array = []
		elif ttymode:
			tab = Table("Missing Files/Chunks (gathered by the previous filesystem self-check loop)",5)
			tab.header("path","inode","index","chunk id","type of missing chunk")
			tab.defattr("l","r","r","r","r")
		else:
			tab = Table("missing files",5)
		for mc in mchunks:
			if jsonmode:
				json_mf_dict = {}
				if len(mc.paths)==0:
					json_mf_dict["paths"] = " * unknown path * (deleted file)"
				else:
					json_mf_dict["paths"] = mc.paths
				json_mf_dict["inode"] = mc.inode
				json_mf_dict["index"] = mc.indx
				json_mf_dict["chunkid"] = mc.get_chunkid_str()
				json_mf_dict["type"] = mc.mtype
				json_mf_dict["type_txt"] = mc.get_mtype_str()
				json_mf_array.append(json_mf_dict)
			else:
				if len(mc.paths)==0:
					dline = [" * unknown path * (deleted file)",mc.inode,mc.indx, mc.get_chunkid_str()]
					dline.append(mc.get_mtype_str())
					tab.append(*dline)
				else:
					for path in mc.paths:
						dline = [path,mc.inode,mc.indx,mc.get_chunkid_str()]
						dline.append(mc.get_mtype_str())
						tab.append(*dline)
		if jsonmode:
			json_in_dict["missing"] = json_mf_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# memory usage table
if org.shall_render("MU"):
	try:
		mu = dataprovider.get_memory_usage()
		if jsonmode:
			json_mu_dict = {}
			for i,label in enumerate(mu.memlabels):
				json_label = label.replace(" ","_")
				json_mu_dict[json_label] = {"allocated": mu.memusage[i*2], "used": mu.memusage[1+i*2], "allocated_human": humanize_number(mu.memusage[i*2]," "), "used_human": humanize_number(mu.memusage[1+i*2]," ")}
			json_mu_dict["total"] = {"allocated": mu.totalallocated, "used": mu.totalused, "allocated_human": humanize_number(mu.totalallocated," "), "used_human": humanize_number(mu.totalused," ")}
			json_in_dict["memory"] = json_mu_dict
		else:
			if ttymode:
				tab = Table("Memory Usage Detailed Info",5)
				tab.defattr("l","r","r","r","r")
				tab.header("object name","memory used","memory allocated","utilization percent","percent of total allocated memory")
			else:
				tab = Table("memory usage detailed info",3)
				tab.defattr("l","r","r")
				tab.header("object name","memory used","memory allocated")
			for i,label in enumerate(mu.memlabels):
				if ttymode:
					if mu.memusage[i*2]>0:
						upercent = "%.2f %%" % (100.0 * mu.memusage[1+i*2] / mu.memusage[i*2])
					else:
						upercent = "-"
					if mu.totalallocated:
						tpercent = "%.2f %%" % (100.0 * mu.memusage[i*2] / mu.totalallocated)
					else:
						tpercent = "-"
					tab.append(label,humanize_number(mu.memusage[1+i*2]," "),humanize_number(mu.memusage[i*2]," "),upercent,tpercent)
				else:
					tab.append(label,mu.memusage[1+i*2],mu.memusage[i*2])
			if ttymode:
				tab.append(("---","",5))
				if mu.totalallocated:
					totalpercent = "%.2f %%" % (100.0 * mu.totalused / mu.totalallocated)
				else:
					totalpercent = "-"
				tab.append("total",humanize_number(mu.totalused," "),humanize_number(mu.totalallocated," "),totalpercent,"-")
			print(myunicode(tab))
	except Exception:
		print_exception()

if jsonmode and len(json_in_dict)>0:
	jcollect["dataset"]["info"] = json_in_dict

# Chunkservers section
if org.shall_render("CS"):
	try:
		servers,dservers = dataprovider.get_chunkservers_by_state(CSorder,CSrev)

		out = []
		if len(servers)>0:
			if jsonmode:
				json_cs_array = []
			elif ttymode:
				if cl.master().version_at_least(3,0,38):
					tab = Table("Chunk Servers",17,"r")
					tab.header("","","","","","","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",5))
					tab.header("ip/host","port","id","labels","version","queue","queue state","maintenance",("---","",9))
					tab.header("","","","","","","","","chunks","used","total","% used","status","chunks","used","total","% used")
				else: #if cl.master().version_at_least(2,1,0):
					tab = Table("Chunk Servers",15,"r")
					tab.header("","","","","","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",4))
					tab.header("ip/host","port","id","labels","version","queue","maintenance",("---","",8))
					tab.header("","","","","","","","chunks","used","total","% used","chunks","used","total","% used")
			else:
				if cl.master().version_at_least(3,0,38):
					tab = Table("chunk servers",15)
				else: #if cl.master().version_at_least(2,1,0):
					tab = Table("chunk servers",13)
			
			# iterate all connected servers
			for cs in servers:
				if cs.strip!=cs.stroip:
					cs.strip = "%s -> %s" % (cs.stroip,cs.strip)

				if jsonmode:
					#Connected chunk servers
					json_cs_dict = {}
					json_cs_dict["connected"] = True
					json_cs_dict["strver"] = cs.strver
					if cs.strver.endswith(" PRO"):
						json_cs_dict["version"] = cs.strver[:-4]
						json_cs_dict["pro"] = True
					else:
						json_cs_dict["version"] = cs.strver
						json_cs_dict["pro"] = False
					json_cs_dict["flags"] = cs.flags
					json_cs_dict["maintenance_mode_timeout"] =cs.mmto
					if cl.leaderfound()==0:
						json_cs_dict["maintenance_mode"] = "not available"
					elif cs.is_maintenance_off():
						json_cs_dict["maintenance_mode"] = "off"
					elif cs.is_maintenance_on():
						json_cs_dict["maintenance_mode"] = "on"
					else:
						json_cs_dict["maintenance_mode"] = "on (temp)"
					json_cs_dict["hostname"] = cs.host
					json_cs_dict["ip"] = cs.strip
					json_cs_dict["port"] = cs.port
					json_cs_dict["csid"] = cs.csid
					json_cs_dict["errors"] = cs.errcnt
					json_cs_dict["gracetime"] = cs.gracetime
					json_cs_dict["load"] = cs.queue
					json_cs_dict["queue"] = cs.queue
					json_cs_dict["queue_state"] = cs.queue_state
					json_cs_dict["queue_state_msg"] = cs.queue_state_msg.lower()
					if cs.labels==0xFFFFFFFF or cs.labels==0:
						labelstab = []
					else:
						labelstab = []
						for bit,char in enumerate(map(chr,range(ord('A'),ord('Z')+1))):
							if cs.labels & (1<<bit):
								labelstab.append(char)
					json_cs_dict["labels"] = labelstab
					json_cs_dict["labels_str"] = ",".join(labelstab)
					json_cs_dict["hdd_regular_used"] = cs.used
					json_cs_dict["hdd_regular_used_human"] = humanize_number(cs.used," ")
					json_cs_dict["hdd_regular_total"] = cs.total
					json_cs_dict["hdd_regular_total_human"] = humanize_number(cs.total," ")
					json_cs_dict["hdd_regular_free"] = cs.total-cs.used
					json_cs_dict["hdd_regular_free_human"] = humanize_number(cs.total-cs.used," ")
					if cs.total>0:
						json_cs_dict["hdd_regular_used_percent"] = (cs.used*100.0)/cs.total
					else:
						json_cs_dict["hdd_regular_used_percent"] = 0.0
					json_cs_dict["hdd_regular_chunks"] = cs.chunks
					json_cs_dict["hdd_removal_used"] = cs.tdused
					json_cs_dict["hdd_removal_used_human"] = humanize_number(cs.tdused," ")
					json_cs_dict["hdd_removal_total"] = cs.tdtotal
					json_cs_dict["hdd_removal_total_human"] = humanize_number(cs.tdtotal," ")
					json_cs_dict["hdd_removal_free"] = cs.tdtotal-cs.tdused
					json_cs_dict["hdd_removal_free_human"] = humanize_number(cs.tdtotal-cs.tdused," ")
					if cs.tdtotal>0:
						json_cs_dict["hdd_removal_used_percent"] = (cs.tdused*100.0)/cs.tdtotal
					else:
						json_cs_dict["hdd_removal_used_percent"] = 0.0
					json_cs_dict["hdd_removal_chunks"] = cs.tdchunks
					if cl.master().version_at_least(3,0,38):
						if cs.tdchunks==0 or cl.leaderfound()==0:
							json_cs_dict["hdd_removal_stat"] = "-"
						elif cs.mfrstatus==MFRSTATUS_INPROGRESS:
							json_cs_dict["hdd_removal_stat"] = "NOT READY (IN PROGRESS)"
						elif cs.mfrstatus==MFRSTATUS_READY:
							json_cs_dict["hdd_removal_stat"] = "READY"
						else: #MFRSTATUS_VALIDATING
							json_cs_dict["hdd_removal_stat"] = "NOT READY (VALIDATING)"
					else:
						json_cs_dict["hdd_removal_stat"] = None
					json_cs_array.append(json_cs_dict)
				elif ttymode:
					if cs.total>0:
						regperc = "%.2f%%" % ((cs.used*100.0)/cs.total)
					else:
						regperc = "-"
					if cs.tdtotal>0:
						tdperc = "%.2f%%" % ((cs.tdused*100.0)/cs.tdtotal)
					else:
						tdperc = "-"
					data = [cs.host,cs.port]
					data.append(cs.csid)
					data.append(cs.labelstr)
					data.append(cs.strver)
					if cl.master().version_at_least(3,0,38):
						data.append(cs.queue)
						data.append(cs.queue_state_str)
					else:
						data.append(cs.queue_cgi)
					if cl.leaderfound()==0:
						data.append("not available")
					elif cs.is_maintenance_off():
						data.append("off")
					elif cs.is_maintenance_on():
						data.append("on (%s)" % cs.mmto)
					else:
						data.append("on (temp ; %s)" % cs.mmto)
					data.extend([cs.chunks,humanize_number(cs.used," "),humanize_number(cs.total," "),regperc])
					if cl.master().version_at_least(3,0,38):
						if cs.tdchunks==0 or cl.leaderfound()==0:
							data.append("-")
						elif cs.mfrstatus==MFRSTATUS_INPROGRESS:
							data.append(("NOT READY",'3'))
						elif cs.mfrstatus==MFRSTATUS_READY:
							data.append(("READY",'4'))
						else:
							data.append("NOT READY")
					data.extend([cs.tdchunks,humanize_number(cs.tdused," "),humanize_number(cs.tdtotal," "),tdperc])
					tab.append(*data)
				else:
					data = [cs.host,cs.port]
					data.append(cs.csid)
					data.append(cs.labelstr)
					data.append(cs.strver)
					if cl.master().version_at_least(3,0,38):
						data.append(cs.queue)
						data.append(cs.queue_state_str)
					else:
						data.append(cs.queue_cgi)
					if cl.leaderfound()==0:
						data.append("-")
					elif cs.is_maintenance_off():
						data.append("maintenance_off")
					elif cs.is_maintenance_on():
						data.append("maintenance_on")
					else:
						data.append("maintenance_tmp_on")
					data.extend([cs.chunks,cs.used,cs.total])
					if cl.master().version_at_least(3,0,38):
						if cs.tdchunks==0 or cl.leaderfound()==0:
							data.append("-")
						elif cs.mfrstatus==MFRSTATUS_INPROGRESS:
							data.append("NOT READY")
						elif cs.mfrstatus==MFRSTATUS_READY:
							data.append("READY")
						else: #MFRSTATUS_VALIDATING
							data.append("NOT READY")
					data.extend([cs.tdchunks,cs.tdused,cs.tdtotal])
					tab.append(*data)

		if len(dservers)>0:
			if jsonmode:
				pass
			elif ttymode:
				if cl.master().version_at_least(3,0,38):
					tab.append(("---","",17))
					tab.append(("disconnected servers","1c",17))
					tab.append(("---","",17))
					tab.append(("ip/host","c"),("port","c"),("id","r"),("maintenance","c",2),("change maintenance command","c",6),("remove command","c",6))
					tab.append(("---","",17))
				else: #if cl.master().version_at_least(2,1,0):
					tab.append(("---","",15))
					tab.append(("disconnected servers","1c",15))
					tab.append(("---","",15))
					tab.append(("ip/host","c"),("port","c"),("id","r"),("maintenance","c"),("change maintenance command","c",5),("remove command","c",6))
					tab.append(("---","",15))
			else:
				try:
					print(myunicode(tab))
				except NameError: # tab may not be defined if no servers are connected
					pass 
				print("")
				tab = Table("Disconnected chunk servers",4)
			
			# iterate all disconnected servers
			for cs in dservers:
				if cs.strip!=cs.stroip:
					cs.strip = "%s -> %s" % (cs.stroip,cs.strip)
				if jsonmode:
					json_cs_dict = {}
					json_cs_dict["connected"] = False
					json_cs_dict["strver"] = None
					json_cs_dict["version"] = None
					json_cs_dict["pro"] = None
					json_cs_dict["flags"] = cs.flags
					json_cs_dict["maintenance_mode_timeout"] = cs.mmto
					if cl.leaderfound()==0:
						json_cs_dict["maintenance_mode"] = "not available"
					elif cs.is_maintenance_off():
						json_cs_dict["maintenance_mode"] = "off"
					elif cs.is_maintenance_on():
						json_cs_dict["maintenance_mode"] = "on"
					else:
						json_cs_dict["maintenance_mode"] = "on (temp)"
					json_cs_dict["hostname"] = cs.host
					json_cs_dict["ip"] = cs.strip
					json_cs_dict["port"] = cs.port
					json_cs_dict["csid"] = cs.csid
					json_cs_dict["errors"] = None
					json_cs_dict["load"] = None
					json_cs_dict["queue"] = None
					json_cs_dict["queue_state"] = "not available"
					json_cs_dict["queue_cgi"] = "not available"
					json_cs_dict["labels"] = []
					json_cs_dict["labels_str"] = "not available"
					json_cs_dict["hdd_regular_used"] = None
					json_cs_dict["hdd_regular_used_human"] = ""
					json_cs_dict["hdd_regular_total"] = None
					json_cs_dict["hdd_regular_total_human"] = ""
					json_cs_dict["hdd_regular_free"] = None
					json_cs_dict["hdd_regular_free_human"] = ""
					json_cs_dict["hdd_regular_used_percent"] = 0.0
					json_cs_dict["hdd_regular_chunks"] = None
					json_cs_dict["hdd_removal_used"] = None
					json_cs_dict["hdd_removal_used_human"] = ""
					json_cs_dict["hdd_removal_total"] = None
					json_cs_dict["hdd_removal_total_human"] = ""
					json_cs_dict["hdd_removal_free"] = None
					json_cs_dict["hdd_removal_free_human"] = ""
					json_cs_dict["hdd_removal_used_percent"] = 0.0
					json_cs_dict["hdd_removal_chunks"] = None
					json_cs_dict["hdd_removal_stat"] = None
					json_cs_array.append(json_cs_dict)
				elif ttymode:
					data = [cs.host,cs.port]
					data.append(cs.csid)
					if cl.leaderfound()==0:
						mm = "-"
						mmcmd = "not available"
					elif cs.is_maintenance_off():
						mm = "off"
						mmcmd = "%s -H %s -P %u -CM1/%s/%s" % (sys.argv[0],masterhost,masterport,cs.stroip,cs.port)
					elif cs.is_maintenance_on():
						mm = "on (%s)" % cs.mmto
						mmcmd = "%s -H %s -P %u -CM0/%s/%s" % (sys.argv[0],masterhost,masterport,cs.stroip,cs.port)
					else:
						mm = "on (temp ; %s)" % cs.mmto
						mmcmd = "%s -H %s -P %u -CM0/%s/%s" % (sys.argv[0],masterhost,masterport,cs.stroip,cs.port)
					if cl.master().version_at_least(3,0,38):
						data.append((mm,"c",2))
					else:
						data.append(mm)
					if cl.master().version_at_least(3,0,38):
						data.append((mmcmd,"l",6))
					else:
						data.append((mmcmd,"l",5))
					if cl.leaderfound() and cl.deputyfound()==0:
						rmcmd = "%s -H %s -P %u -CRC/%s/%s" % (sys.argv[0],masterhost,masterport,cs.stroip,cs.port)
					elif cl.master().version_at_least(3,0,67):
						rmcmd = "%s -H %s -P %u -CTR/%s/%s" % (sys.argv[0],masterhost,masterport,cs.stroip,cs.port)
					else:
						rmcmd = "not available"
					data.append((rmcmd,"l",6))
					tab.append(*data)
				else:
					if cl.leaderfound()==0:
						mm = "-"
					elif cs.is_maintenance_off():
						mm = "maintenance off"
					elif cs.is_maintenance_on():
						mm = "maintenance on (%s)" % cs.mmto
					else:
						mm = "maintenance on (temp ; %s)" % cs.mmto
					tab.append(cs.host,cs.port,cs.csid,mm)
		try:
			if jsonmode:
				jcollect["dataset"]["chunkservers"] = json_cs_array
			else:
				print(myunicode(tab))
		except NameError: # tab/json_cs_array may not be defined if no servers are available
			pass
	except Exception:
		print_exception()

#Metadata servers subsection (CLI only)
if org.shall_render("MB"):
	try:
		if jsonmode:
			json_mb_array = []
		elif ttymode:
			tab = Table("Metadata Backup Loggers",2,"r")
			tab.header("ip/host","version")
		else:
			tab = Table("metadata backup loggers",2)
		for ml in dataprovider.get_metaloggers(MBorder, MBrev):
			if jsonmode:
				json_mb_dict = {}
				json_mb_dict["hostname"] = ml.host
				json_mb_dict["ip"] = ml.strip
				json_mb_dict["strver"] = ml.strver
				if ml.strver.endswith(" PRO"):
					json_mb_dict["version"] = ml.strver[:-4]
					json_mb_dict["pro"] = True
				else:
					json_mb_dict["version"] = ml.strver
					json_mb_dict["pro"] = False
				json_mb_array.append(json_mb_dict)
			else:
				tab.append(ml.host,ml.strver)
		if jsonmode:
			jcollect["dataset"]["metaloggers"] = json_mb_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Disks section
if org.shall_render("HD"):
	try:
		(hdds, scanhdds) = dataprovider.get_hdds("ALL", HDperiod, HDtime, HDorder, HDrev)
		# preapare headers
		if len(hdds)>0 or len(scanhdds)>0:
			if jsonmode:
				json_hd_array = []
			elif ttymode:
				tab = Table("Disks",15,"r")
				tab.header(("","",4),("I/O stats last %s" % ("day" if HDperiod==2 else "hour" if HDperiod==1 else "min"),"",8),("","",3))
				tab.header(("info","",4),("---","",8),("space","",3))
				tab.header(("","",4),("transfer","",2),("%s time" % ("avg" if HDtime==1 else "max"),"",3),("# of ops","",3),("","",3))
				tab.header(("---","",15))
				if len(hdds)>0 or len(scanhdds)==0:
					tab.header("IP path","chunks","last error","status","read","write","read","write","fsync","read","write","fsync","used","total","used %")
					lscanning = 0
				else:
					tab.header("IP path","chunks","last error","status","read","write","read","write","fsync","read","write","fsync",("progress","c",3))
					lscanning = 1
			else:
				tab = Table("disks",14)

			# calculate some stats
			usedsum = {}
			totalsum = {}
			hostavg = {}
			for _,hdd in hdds+scanhdds:
				if hdd.hostkey not in usedsum:
					usedsum[hdd.hostkey]=0
					totalsum[hdd.hostkey]=0
					hostavg[hdd.hostkey]=0
				if hdd.flags&CS_HDD_SCANNING==0 and hdd.total and hdd.total>0:
					usedsum[hdd.hostkey]+=hdd.used
					totalsum[hdd.hostkey]+=hdd.total
					if totalsum[hdd.hostkey]>0:
						hostavg[hdd.hostkey] = (usedsum[hdd.hostkey] * 100.0) / totalsum[hdd.hostkey]
			
			for sf,hdd in hdds+scanhdds:
				if not hdd.is_valid():
					lerror= '-'
				elif not hdd.has_errors():
					lerror = 'no errors'
				else:
					errtimetuple = time.localtime(hdd.errtime)
					errtimelong = time.strftime("%Y-%m-%d %H:%M:%S",errtimetuple)
					errtimeshort = time.strftime("%Y-%m-%d %H:%M",errtimetuple)
					if jsonmode:
						lerror = time.asctime(time.localtime(hdd.errtime))
					elif ttymode:
						errtimetuple = time.localtime(hdd.errtime)
						lerror = errtimeshort
					else:
						lerror = hdd.errtime

				if jsonmode:
					json_hd_dict = {}
					json_hd_dict["hostname"] = hdd.hoststr
					json_hd_dict["ip"] = hdd.hostip
					json_hd_dict["port"] = hdd.port
					json_hd_dict["path"] = hdd.hddpath
					json_hd_dict["ip_path"] = hdd.ippath
					json_hd_dict["hostname_path"] = hdd.hostpath
					json_hd_dict["chunks"] = hdd.chunkscnt
					json_hd_dict["flags"] = hdd.flags
					json_hd_dict["mfrstatus"] = hdd.mfrstatus
					json_hd_dict["status"] = hdd.get_statuslist()
					json_hd_dict["status_str"] = hdd.get_status_str()
					json_hd_dict["last_error_time"] = hdd.errtime
					json_hd_dict["last_error_time_str"] = lerror
					json_hd_dict["last_error_chunkid"] = hdd.errchunkid
					if hdd.flags&CS_HDD_SCANNING:
						json_hd_dict["scan_progress"] = hdd.used
						json_hd_dict["used"] = None
						json_hd_dict["used_human"] = ""
						json_hd_dict["total"] = None
						json_hd_dict["total_human"] = ""
						json_hd_dict["used_percent"] = 0.0
					else:
						json_hd_dict["scan_progress"] = 100.0
						json_hd_dict["used"] = hdd.used
						json_hd_dict["used_human"] = humanize_number(hdd.used," ")
						json_hd_dict["total"] = hdd.total
						json_hd_dict["total_human"] = humanize_number(hdd.total," ")
						if hdd.total>0:
							json_hd_dict["used_percent"] = (hdd.used*100.0)/hdd.total
						else:
							json_hd_dict["used_percent"] = 0.0
					for i,name in enumerate(['min','hour','day']):
						json_hd_stats = {}
						json_hd_stats["rbw"] = hdd.rbw[i]
						json_hd_stats["wbw"] = hdd.wbw[i]
						json_hd_stats["read_avg_usec"] = hdd.usecreadavg[i]
						json_hd_stats["write_avg_usec"] = hdd.usecwriteavg[i]
						json_hd_stats["fsync_avg_usec"] = hdd.usecfsyncavg[i]
						json_hd_stats["read_max_usec"] = hdd.usecreadmax[i]
						json_hd_stats["write_max_usec"] = hdd.usecwritemax[i]
						json_hd_stats["fsync_max_usec"] = hdd.usecfsyncmax[i]
						json_hd_stats["read_ops"] = hdd.rops[i]
						json_hd_stats["write_ops"] = hdd.wops[i]
						json_hd_stats["fsync_ops"] = hdd.fsyncops[i]
						json_hd_stats["read_bytes"] = hdd.rbytes[i]
						json_hd_stats["write_bytes"] = hdd.wbytes[i]
						json_hd_dict["stats_%s" % name] = json_hd_stats
					json_hd_array.append(json_hd_dict)
				elif ttymode:
					rtime = hdd.usecreadmax[HDperiod] if HDtime==0 else hdd.usecreadavg[HDperiod]
					wtime = hdd.usecwritemax[HDperiod] if HDtime==0 else hdd.usecwriteavg[HDperiod]
					fsynctime = hdd.usecfsyncmax[HDperiod] if HDtime==0 else hdd.usecfsyncavg[HDperiod]
					if hdd.is_valid():
						chunkscnttxt = hdd.chunkscnt
						usedtxt = humanize_number(hdd.used," ")
						totaltxt = humanize_number(hdd.total," ")
					else:
						chunkscnttxt = '-'
						usedtxt = '-'
						totaltxt = '-'
					ldata = [hdd.ippath,chunkscnttxt,lerror,hdd.get_status_str()]
					if hdd.rbw[HDperiod]==0 and hdd.wbw[HDperiod]==0 and hdd.usecreadmax[HDperiod]==0 and hdd.usecwritemax[HDperiod]==0 and hdd.usecfsyncmax[HDperiod]==0 and hdd.rops[HDperiod]==0 and hdd.wops[HDperiod]==0:
						ldata.extend(("-","-","-","-","-","-","-","-"))
					else:
						ldata.extend(("%s/s" % humanize_number(hdd.rbw[HDperiod]," "),"%s/s" % humanize_number(hdd.wbw[HDperiod]," "),"%u us" % rtime,"%u us" % wtime,"%u us" % fsynctime,hdd.rops[HDperiod],hdd.wops[HDperiod],hdd.fsyncops[HDperiod]))
					if hdd.flags&CS_HDD_SCANNING:
						if lscanning==0:
							lscanning=1
							tab.append(("---","",15))
							tab.append("IP path","chunks","last error","status","read","write","read","write","fsync","read","write","fsync",("progress","c",3))
							tab.append(("---","",15))
						ldata.append(("%.0f%%" % hdd.used,"r",3))
					else:
						if hdd.total>0:
							perc = "%.2f%%" % ((hdd.used*100.0)/hdd.total)
						else:
							perc = "-"
						ldata.extend((usedtxt,totaltxt,perc))
					tab.append(*ldata)
				else:
					if not hdd.is_valid():
						hdd.chunkscnt = '-'
						hdd.used = '-'
						hdd.total = '-'
					rtime = hdd.usecreadmax[HDperiod] if HDtime==0 else hdd.usecreadavg[HDperiod]
					wtime = hdd.usecwritemax[HDperiod] if HDtime==0 else hdd.usecwriteavg[HDperiod]
					fsynctime = hdd.usecfsyncmax[HDperiod] if HDtime==0 else hdd.usecfsyncavg[HDperiod]
					ldata = [hdd.ippath,hdd.chunkscnt,lerror,hdd.get_status_str()]
					if hdd.rbw[HDperiod]==0 and hdd.wbw[HDperiod]==0 and hdd.usecreadmax[HDperiod]==0 and hdd.usecwritemax[HDperiod]==0 and hdd.usecfsyncmax[HDperiod]==0 and hdd.rops[HDperiod]==0 and hdd.wops[HDperiod]==0:
						ldata.extend(("-","-","-","-","-","-","-","-"))
					else:
						ldata.extend((hdd.rbw[HDperiod],hdd.wbw[HDperiod],rtime,wtime,fsynctime,hdd.rops[HDperiod],hdd.wops[HDperiod],hdd.fsyncops[HDperiod]))
					if hdd.flags&CS_HDD_SCANNING:
						ldata.extend(("progress:",hdd.used))
					else:
						ldata.extend((hdd.used,hdd.total))
					tab.append(*ldata)
			if jsonmode:
				jcollect["dataset"]["disks"] = json_hd_array
			else:
				print(myunicode(tab))
	except TimeoutError:
		print_error("Timeout connecting chunk servers. Hint: check if chunk servers are available from this server (the one hosting GUI/CLI).")
	except Exception:
		print_exception()

# Exports section
if org.shall_render("EX"):
	try:
		if jsonmode:
			json_ex_array = []
		elif ttymode:
			tab = Table("Exports",(19 if cl.master().has_feature(FEATURE_EXPORT_DISABLES) else 18 if cl.master().has_feature(FEATURE_EXPORT_UMASK) else 17))

			dline = ["r","r","l","c","c","c","c","c","c"]
			dline.append("c")
			dline.extend(("r","r","r","r"))
			dline.extend(("c","r","r"))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("c")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("c")
			tab.defattr(*dline)

			dline = [("ip range","",2),"","","","","","",""]
			dline.append("")
			dline.extend((("map root","",2),("map users","",2)))
			dline.extend(("",("trashretention limit","",2)))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)

			dline = [("---","",2),"path","minversion","alldirs","password","ro/rw","restrict ip","ignore gid"]
			dline.append("admin")
			dline.append(("---","",4))
			dline.append("allowed sclasses")
			dline.append(("---","",2))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("global umask")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("disables mask")
			tab.header(*dline)

			dline = ["from","to","","","","","","",""]
			dline.append("")
			dline.extend(("uid","gid","uid","gid"))
			dline.extend(("","min","max"))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)
		else:
			tab = Table("exports",(19 if cl.master().has_feature(FEATURE_EXPORT_DISABLES) else 18 if cl.master().has_feature(FEATURE_EXPORT_UMASK) else 17))

		for ee in dataprovider.get_exports(EXorder,EXrev):
			if jsonmode:
				json_ex_dict = {}
				json_ex_dict["ip_range_from"] = ee.stripfrom
				json_ex_dict["ip_range_to"] = ee.stripto
				json_ex_dict["meta"] = ee.meta
				json_ex_dict["path"] = "META" if ee.meta else ee.path
				json_ex_dict["export_flags"] = ee.exportflags
				json_ex_dict["session_flags"] = ee.sesflags
				json_ex_dict["minver"] = ee.strver
				json_ex_dict["alldirs"] = None if ee.meta else True if ee.is_alldirs() else False
				json_ex_dict["password"] = True if ee.is_password() else False
				json_ex_dict["access_rw"] = False if ee.is_readonly() else True
				json_ex_dict["restricted"] = False if ee.is_unrestricted() else True
				json_ex_dict["ignore_gid"] = None if ee.meta else True if ee.ignore_gid() else False
				json_ex_dict["admin"] = None if ee.meta else True if ee.is_admin() else False
				if ee.meta:
					json_ex_dict["map_root_uid"] = None
					json_ex_dict["map_root_gid"] = None
				else:
					json_ex_dict["map_root_uid"] = ee.rootuid
					json_ex_dict["map_root_gid"] = ee.rootgid
				if ee.meta or (ee.map_user())==0:
					json_ex_dict["map_user_uid"] = None
					json_ex_dict["map_user_gid"] = None
				else:
					json_ex_dict["map_user_uid"] = ee.mapalluid
					json_ex_dict["map_user_gid"] = ee.mapallgid
				json_ex_dict["allowed_storage_classes"] = ee.sclassgroups
				json_ex_dict["allowed_storage_classes_str"] = ee.sclassgroups_str
				if ee.mintrashretention!=None and ee.maxtrashretention!=None:
					json_ex_dict["trash_retention_min"] = ee.mintrashretention
					json_ex_dict["trash_retention_max"] = ee.maxtrashretention
				else:
					json_ex_dict["trash_retention_min"] = None
					json_ex_dict["trash_retention_max"] = None
				if cl.master().has_feature(FEATURE_EXPORT_UMASK):
					if ee.umaskval==None:
						json_ex_dict["global_umask"] = None
					else:
						json_ex_dict["global_umask"] = "%03o" % ee.umaskval
				else:
					json_ex_dict["global_umask"] = None
				if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
					json_ex_dict["disables_mask"] = None if ee.meta else "%08X" % ee.disables
					json_ex_dict["disables_str"] = "" if ee.meta else disablesmask_to_string(ee.disables)
					json_ex_dict["disables"] = [] if ee.meta else disablesmask_to_string_list(ee.disables)
				else:
					json_ex_dict["disables_mask"] = None
					json_ex_dict["disables_str"] = ""
					json_ex_dict["disables"] = []
				json_ex_array.append(json_ex_dict)
			elif ttymode:
				dline = [ee.stripfrom,ee.stripto,". (META)" if ee.meta else ee.path,ee.strver,"-" if ee.meta else "yes" if ee.is_alldirs() else "no","yes" if ee.is_password() else "no","ro" if ee.is_readonly() else "rw","no" if ee.is_unrestricted() else "yes","-" if ee.meta else "yes" if ee.ignore_gid() else "no"]
				dline.append("-" if ee.meta else "yes" if ee.is_admin() else "no")
				if ee.meta:
					dline.extend(("-","-"))
				else:
					dline.extend((ee.rootuid,ee.rootgid))
				if ee.meta or (ee.map_user())==0:
					dline.extend(("-","-"))
				else:
					dline.extend((ee.mapalluid,ee.mapallgid))
				dline.append("%04X (%s)" % (ee.sclassgroups,ee.sclassgroups_str))
				if ee.mintrashretention!=None and ee.maxtrashretention!=None:
					dline.extend((timeduration_to_shortstr(ee.mintrashretention),timeduration_to_shortstr(ee.maxtrashretention)))
				else:
					dline.extend(("-","-"))
				if cl.master().has_feature(FEATURE_EXPORT_UMASK):
					if ee.umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % ee.umaskval)
				if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X (%s)" % (ee.disables,disablesmask_to_string(ee.disables)))
				tab.append(*dline)
			else:
				dline = [ee.stripfrom,ee.stripto,". (META)" if ee.meta else ee.path,ee.strver,"-" if ee.meta else "yes" if ee.is_alldirs() else "no","yes" if ee.is_password() else "no","ro" if ee.is_readonly() else "rw","no" if ee.is_unrestricted() else "yes","-" if ee.meta else "yes" if ee.ignore_gid() else "no"]
				dline.append("-" if ee.meta else "yes" if ee.is_admin() else "no")
				if ee.meta:
					dline.extend(("-","-"))
				else:
					dline.extend((ee.rootuid,ee.rootgid))
				if ee.meta or (ee.map_user())==0:
					dline.extend(("-","-"))
				else:
					dline.extend((ee.mapalluid,ee.mapallgid))
				dline.append("%04X" % ee.sclassgroups)
				if ee.mintrashretention!=None and ee.maxtrashretention!=None:
					dline.extend((ee.mintrashretention,ee.maxtrashretention))
				else:
					dline.extend(("-","-"))
				if cl.master().has_feature(FEATURE_EXPORT_UMASK):
					if ee.umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % ee.umaskval)
				if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X" % ee.disables)
				tab.append(*dline)

		if jsonmode:
			jcollect["dataset"]["exports"] = json_ex_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Mounts - parameters section
if org.shall_render("MS"):
	try:
		if ttymode:
			tab = Table("Active mounts (parameters)",(20 if cl.master().has_feature(FEATURE_EXPORT_DISABLES) else 19 if cl.master().has_feature(FEATURE_EXPORT_UMASK) else 18 if cl.master().version_at_least(1,7,8) else 16 if cl.master().version_at_least(1,7,0) else 15 if cl.master().version_at_least(1,6,26) else 12))

			dline = ["r","r","l"]
			if cl.master().version_at_least(1,7,8):
				dline.extend(("r","r"))
			dline.extend(("r","l","c","c","c"))
			if cl.master().version_at_least(1,7,0):
				dline.append("c")
			dline.extend(("r","r","r","r"))
			if cl.master().version_at_least(1,6,26):
				dline.extend(("l","r","r"))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("c")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("c")
			tab.defattr(*dline)

			dline = ["","","","","","","",""]
			dline.extend(("",""))
			dline.append("")
			dline.extend((("map root","",2),("map users","",2)))
			dline.extend(("",("trashretention limit","",2)))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)

			dline = ["session id","ip/host","mount point"]
			dline.extend(("open files","# of connections"))
			dline.extend(("version","root dir","ro/rw","restrict ip","ignore gid"))
			dline.append("admin")
			dline.append(("---","",4))
			dline.append("allowed sclasses")
			dline.append(("---","",2))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("global umask")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("disables mask")
			tab.header(*dline)

			dline = ["","","","","","","",""]
			dline.extend(("",""))
			dline.append("")
			dline.extend(("uid","gid","uid","gid"))
			dline.extend(("","min","max"))
			if cl.master().has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)
		elif jsonmode:
			json_ms_array = []
		else:
			tab = Table("active mounts, parameters",(20 if cl.master().has_feature(FEATURE_EXPORT_DISABLES) else 19 if cl.master().has_feature(FEATURE_EXPORT_UMASK) else 18))

		sessions,dsessions = dataprovider.get_sessions_by_state(MSorder, MSrev)

		# Show active mounts
		for ses in sessions:
			if ttymode:
				dline = [ses.get_sessionstr(),ses.host,ses.info]
				dline.extend((ses.openfiles,ses.nsocks))
				dline.extend((ses.strver,". (META)" if ses.meta else ses.path,"ro" if ses.sesflags&1 else "rw","no" if ses.sesflags&2 else "yes","-" if ses.meta else "yes" if ses.sesflags&4 else "no"))
				dline.append("-" if ses.meta else "yes" if ses.sesflags&8 else "no")
				if ses.meta:
					dline.extend(("-","-"))
				else:
					dline.extend((ses.rootuid,ses.rootgid))
				if ses.meta or (ses.sesflags&16)==0:
					dline.extend(("-","-"))
				else:
					dline.extend((ses.mapalluid,ses.mapallgid))
				dline.append("%04X (%s)" % (ses.sclassgroups,ses.get_sclassgroups_str()))
				if ses.mintrashretention!=None and ses.maxtrashretention!=None:
					dline.extend((timeduration_to_shortstr(ses.mintrashretention),timeduration_to_shortstr(ses.maxtrashretention)))
				else:
					dline.extend(("-","-"))
				if cl.master().has_feature(FEATURE_EXPORT_UMASK):
					if ses.umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % ses.umaskval)
				if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X (%s)" % (ses.disables,disablesmask_to_string(ses.disables)))
				tab.append(*dline)
			elif jsonmode:
				json_ms_dict = {}
				json_ms_dict["connected"] = True
				json_ms_dict["temporary"] = ses.is_tmp()
				json_ms_dict["session_id"] = ses.sessionid
				json_ms_dict["session_id_str"] = ses.get_sessionstr()
				json_ms_dict["hostname"] = ses.host
				json_ms_dict["ip"] = ses.strip
				json_ms_dict["mount_point"] = ses.info
				json_ms_dict["open_files"] = ses.openfiles
				json_ms_dict["number_of_sockets"] = ses.nsocks
				json_ms_dict["seconds_to_expire"] = None
				json_ms_dict["strver"] = ses.strver
				if ses.strver.endswith(" PRO"):
					json_ms_dict["version"] = ses.strver[:-4]
					json_ms_dict["pro"] = True
				else:
					json_ms_dict["version"] = ses.strver
					json_ms_dict["pro"] = False
				json_ms_dict["meta"] = ses.meta
				json_ms_dict["path"] = "META" if ses.meta else ses.path
				json_ms_dict["session_flags"] = ses.sesflags
				json_ms_dict["access_rw"] = False if ses.sesflags&1 else True
				json_ms_dict["restricted"] = False if ses.sesflags&2 else True
				json_ms_dict["ignore_gid"] = None if ses.meta else True if ses.sesflags&4 else False
				json_ms_dict["admin"] = None if ses.meta else True if ses.sesflags&8 else False
				if ses.meta:
					json_ms_dict["map_root_uid"] = None
					json_ms_dict["map_root_gid"] = None
				else:
					json_ms_dict["map_root_uid"] = ses.rootuid
					json_ms_dict["map_root_gid"] = ses.rootgid
				if ses.meta or (ses.sesflags&16)==0:
					json_ms_dict["map_user_uid"] = None
					json_ms_dict["map_user_gid"] = None
				else:
					json_ms_dict["map_user_uid"] = ses.mapalluid
					json_ms_dict["map_user_gid"] = ses.mapallgid
				json_ms_dict["allowed_storage_classes"] = ses.sclassgroups
				json_ms_dict["allowed_storage_classes_str"] = ses.get_sclassgroups_str()
				if ses.mintrashretention!=None and ses.maxtrashretention!=None:
					json_ms_dict["trash_retention_min"] = ses.mintrashretention
					json_ms_dict["trash_retention_max"] = ses.maxtrashretention
				else:
					json_ms_dict["trash_retention_min"] = None
					json_ms_dict["trash_retention_max"] = None
				if cl.master().has_feature(FEATURE_EXPORT_UMASK):
					if ses.umaskval==None:
						json_ms_dict["global_umask"] = None
					else:
						json_ms_dict["global_umask"] = "%03o" % ses.umaskval
				else:
					json_ms_dict["global_umask"] = None
				if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
					json_ms_dict["disables_mask"] = None if ses.meta else "%08X" % ses.disables
					json_ms_dict["disables_str"] = "" if ses.meta else disablesmask_to_string(ses.disables)
					json_ms_dict["disables"] = [] if ses.meta else disablesmask_to_string_list(ses.disables)
				else:
					json_ms_dict["disables_mask"] = None
					json_ms_dict["disables_str"] = ""
					json_ms_dict["disables"] = []
				json_ms_array.append(json_ms_dict)
			else:
				dline = [ses.get_sessionstr(),ses.host,ses.info]
				dline.extend((ses.openfiles,ses.nsocks))
				dline.extend((ses.strver,". (META)" if ses.meta else ses.path,"ro" if ses.sesflags&1 else "rw","no" if ses.sesflags&2 else "yes","-" if ses.meta else "yes" if ses.sesflags&4 else "no"))
				dline.append("-" if ses.meta else "yes" if ses.sesflags&8 else "no")
				if ses.meta:
					dline.extend(("-","-"))
				else:
					dline.extend((ses.rootuid,ses.rootgid))
				if ses.meta or (ses.sesflags&16)==0:
					dline.extend(("-","-"))
				else:
					dline.extend((ses.mapalluid, ses.mapallgid))
				dline.append("%04X" % ses.sclassgroups)
				if ses.mintrashretention!=None and ses.maxtrashretention!=None:
					dline.extend((ses.mintrashretention, ses.maxtrashretention))
				else:
					dline.extend(("-","-"))
				if cl.master().has_feature(FEATURE_EXPORT_UMASK):
					if ses.umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % ses.umaskval)
				if cl.master().has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X" % ses.disables)
				tab.append(*dline)
		# Show inactive mounts		
		if len(dsessions)>0:
			if ttymode:
				tabcols = (20 if cl.master().has_feature(FEATURE_EXPORT_DISABLES) else 19 if cl.master().has_feature(FEATURE_EXPORT_UMASK) else 18 if cl.master().version_at_least(1,7,8) else 16 if cl.master().version_at_least(1,7,0) else 15 if cl.master().version_at_least(1,6,26) else 12)
				tab.append(("---","",tabcols))
				tab.append(("Inactive mounts (parameters)","1c",tabcols))
				tab.append(("---","",tabcols))
				dline = [("session id","c"),("ip/host","c"),("mount point","c"),("open files","c"),("expires","c"),("command to remove","c",tabcols-5)]
				tab.append(*dline)
				tab.append(("---","",tabcols))
			elif jsonmode:
				pass
			else:
				print(myunicode(tab))
				print("")
				tab = Table("inactive mounts, parameters",5)
		for ses in dsessions:
			if ttymode:
				tabcols = (20 if cl.master().has_feature(FEATURE_EXPORT_DISABLES) else 19 if cl.master().has_feature(FEATURE_EXPORT_UMASK) else 18 if cl.master().version_at_least(1,7,8) else 16 if cl.master().version_at_least(1,7,0) else 15 if cl.master().version_at_least(1,6,26) else 12)
				dline = [ses.sessionid,ses.host,ses.info,ses.openfiles,ses.expire,("%s -H %s -P %u -CRS/%u" % (sys.argv[0],masterhost,masterport,ses.sessionid),"l",tabcols-5)]
				tab.append(*dline)
			elif jsonmode:
				json_ms_dict = {}
				json_ms_dict["connected"] = False
				json_ms_dict["temporary"] = False
				json_ms_dict["session_id"] = ses.sessionid
				json_ms_dict["session_id_str"] = ("%u" % ses.sessionid)
				json_ms_dict["hostname"] = ses.host
				json_ms_dict["ip"] = ses.strip
				json_ms_dict["mount_point"] = ses.info
				json_ms_dict["open_files"] = ses.openfiles
				json_ms_dict["number_of_sockets"] = 0
				json_ms_dict["seconds_to_expire"] = ses.expire
				json_ms_dict["strver"] = None
				json_ms_dict["version"] = None
				json_ms_dict["pro"] = None
				json_ms_dict["meta"] = None
				json_ms_dict["path"] = None
				json_ms_dict["session_flags"] = None
				json_ms_dict["access_rw"] = None
				json_ms_dict["restricted"] = None
				json_ms_dict["ignore_gid"] = None
				json_ms_dict["admin"] = None
				json_ms_dict["map_root_uid"] = None
				json_ms_dict["map_root_gid"] = None
				json_ms_dict["map_user_uid"] = None
				json_ms_dict["map_user_gid"] = None
				json_ms_dict["allowed_storage_classes"] = None
				json_ms_dict["allowed_storage_classes_str"] = None
				json_ms_dict["trash_retention_min"] = None
				json_ms_dict["trash_retention_max"] = None
				json_ms_dict["global_umask"] = None
				json_ms_dict["disables_mask"] = None
				json_ms_dict["disables_str"] = ""
				json_ms_dict["disables"] = []
				json_ms_array.append(json_ms_dict)
			else:
				dline = [ses.sessionid,ses.host,ses.info,ses.openfiles,ses.expire]
				tab.append(*dline)
		if jsonmode:
			jcollect["dataset"]["mounts"] = json_ms_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Mounts - operations section
if org.shall_render("MO"):
		try:
			if jsonmode:
				if (dataprovider.stats_to_show<=16):
					json_stats_keys = ["statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","read","write"]
				else:
					json_stats_keys = ["statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","rchunk","wchunk","read","write","fsync","snapshot","truncate","getxattr","setxattr","getfacl","setfacl","create","lock","meta"]
				json_mo_array = []
			elif ttymode:
				tab = Table("Active mounts (operations)",3+dataprovider.stats_to_show)
				tab.header("","",("operations %s hour" % ("last" if MOdata==0 else "current"),"",1+dataprovider.stats_to_show))
				tab.header("host/ip","mount point",("---","",1+dataprovider.stats_to_show))
				if (dataprovider.stats_to_show<=16):
					tab.header("","","statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","read","write","total")
					tab.defattr("r","l","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r")
				else:
					tab.header("","","statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","rchunk","wchunk","read","write","fsync","snapshot","truncate","getxattr","setxattr","getfacl","setfacl","create","lock","meta","total")
					tab.defattr("r","l","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r")
			else:
				tab = Table("active mounts, operations",3+dataprovider.stats_to_show)
			for ses in dataprovider.get_sessions_order_by_mo(MOorder,MOrev,MOdata):
				if jsonmode:
					json_mo_dict = {}
					json_mo_dict["hostname"] = ses.host
					json_mo_dict["ip"] = ses.strip
					json_mo_dict["mount_point"] = ses.info
					json_stats_c_dict = {}
					json_stats_l_dict = {}
					for i,name in enumerate(json_stats_keys):
						json_stats_c_dict[name] = ses.stats_c[i]
						json_stats_l_dict[name] = ses.stats_l[i]
					json_stats_c_dict["total"] = sum(ses.stats_c)
					json_stats_l_dict["total"] = sum(ses.stats_l)
					json_mo_dict["stats_current_hour"] = json_stats_c_dict
					json_mo_dict["stats_last_hour"] = json_stats_l_dict
					json_mo_array.append(json_mo_dict)
				else:
					ldata = [ses.host,ses.info]
					if MOdata==0:
						ldata.extend(ses.stats_l)
						ldata.append(sum(ses.stats_l))
					else:
						ldata.extend(ses.stats_c)
						ldata.append(sum(ses.stats_c))
					tab.append(*ldata)
					
			if jsonmode:
				jcollect["dataset"]["operations"] = json_mo_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()

# Open files section
if org.shall_render("OF"):
	try:
		if jsonmode:
			json_of_array = []
		elif ttymode:
			tab = Table("Open Files",5)
			tab.header("session id","ip/host","mount point","inode","path")
			tab.defattr("r","r","l","r","l")
		else:
			tab = Table("open file",5)
		
		for of in dataprovider.get_openfiles(OFsessionid, OForder, OFrev):
			if jsonmode:
				json_of_dict = {}
				json_of_dict["session_id"] = of.sessionid
				json_of_dict["hostname"] = of.host
				json_of_dict["ip"] = of.ipnum
				json_of_dict["mount_point"] = of.info
				json_of_dict["inode"] = of.inode
				json_of_dict["paths"] = of.paths
				json_of_array.append(json_of_dict)
			else:
				if len(of.paths)==0:
					dline = [of.sessionid,of.host,of.info,of.inode,"unknown"]
					tab.append(*dline)
				else:
					for path in of.paths:
						dline = [of.sessionid,of.host,of.info,of.inode,path]
						tab.append(*dline)
		if jsonmode:
			jcollect["dataset"]["openfiles"] = json_of_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Acquired locks section CLI only
if org.shall_render("AL"):
	try:
		sessionsdata = {}
		for ses in dataprovider.get_sessions():
			if ses.sessionid>0 and ses.sessionid < 0x80000000:
				sessionsdata[ses.sessionid]=(ses.host,ses.sortip,ses.strip,ses.info,ses.openfiles)
			
		if jsonmode:
			json_al_array = []
		elif ttymode:
			tab = Table("Acquired Locks",10,"r")
			tab.header("inode","session id","ip/host","mount point","lock type","owner","pid","start","end","r/w")
		else:
			tab = Table("acquired locks",10)

		for al in dataprovider.get_acquiredlocks(ALinode, ALorder, ALrev):
			if jsonmode:
				json_al_dict = {}
				json_al_dict["inode"] = al.inode
				json_al_dict["session_id"] = al.sessionid
				json_al_dict["hostname"] = al.host
				json_al_dict["ip"] = al.ipnum
				json_al_dict["mount_point"] = al.info
				json_al_dict["lock_type"] = al.ctype
				if al.ctype==MFS_LOCK_TYPE_SHARED:
					json_al_dict["lock_type_str"] = "READ(SHARED)"
				elif al.ctype==MFS_LOCK_TYPE_EXCLUSIVE:
					json_al_dict["lock_type_str"] = "WRITE(EXCLUSIVE)"
				else:
					json_al_dict["lock_type_str"] = "UNKNOWN"
				json_al_dict["owner"] = al.owner
				json_al_dict["pid"] = al.pid
				json_al_dict["start"] = al.start
				json_al_dict["end"] = al.end
				if al.pid==0 and al.start==0 and al.end==0:
					json_al_dict["pid_str"] = "-1"
					json_al_dict["start_str"] = "0"
					json_al_dict["end_str"] = "EOF"
				elif al.end > 0x7FFFFFFFFFFFFFFF:
					json_al_dict["pid_str"] = "%u" % al.pid
					json_al_dict["start_str"] = "%u" % al.start
					json_al_dict["end_str"] = "EOF"
				else:
					json_al_dict["pid_str"] = "%u" % al.pid
					json_al_dict["start_str"] = "%u" % al.start
					json_al_dict["end_str"] = "%u" % al.end
				json_al_array.append(json_al_dict)
			else:
				if al.pid==0 and al.start==0 and al.end==0:
					pid = "-1"
					start = "0"
					end = "EOF"
				elif al.end > 0x7FFFFFFFFFFFFFFF:
					al.end = "EOF"
				if al.ctype==MFS_LOCK_TYPE_SHARED:
					ctypestr = "READ(SHARED)"
				elif al.ctype==MFS_LOCK_TYPE_EXCLUSIVE:
					ctypestr = "WRITE(EXCLUSIVE)"
				else:
					ctypestr = "UNKNOWN"
				dline = [al.inode,al.sessionid,al.host,al.info,al.locktype,al.owner,al.pid,al.start,al.end,ctypestr]
				tab.append(*dline)
		if jsonmode:
			jcollect["dataset"]["locks"] = json_al_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()
	
# Storage classes section
if org.shall_render("SC"):
	try:
		show_copy_and_ec = cl.master().version_at_least(4,5,0)
		show_arch_min_size = cl.master().version_at_least(4,34,0)
		show_labelmode_overrides = cl.master().has_feature(FEATURE_LABELMODE_OVERRIDES)
		show_export_group_and_priority = cl.master().has_feature(FEATURE_SCLASSGROUPS)

		if jsonmode:
			json_sc_array = []
		elif ttymode:
			if show_copy_and_ec:
				if show_arch_min_size:
					if show_labelmode_overrides:
						if show_export_group_and_priority:
							tab = Table("Storage Classes",24,"r")
							tab.header("","","","","","","","","","",("# of inodes","",2),("state statistics and definitions","",12))
							tab.header("","","","","","","","","","",("---","",14))
							tab.header("id","name","join priority","export group","admin only","labels mode","arch mode","arch delay","arch min size","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","","")
							tab.header("","","","","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","labels mode override","redundancy level","labels","distribution")
							tab.header("","","","","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","","")
							tab.defattr("r","l","r","r","c","c","c","r","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c","c")
						else:
							tab = Table("Storage Classes",22,"r")
							tab.header("","","","","","","","",("# of inodes","",2),("state statistics and definitions","",12))
							tab.header("","","","","","","","",("---","",14))
							tab.header("id","name","admin only","labels mode","arch mode","arch delay","arch min size","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","","")
							tab.header("","","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","labels mode override","redundancy level","labels","distribution")
							tab.header("","","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","","")
							tab.defattr("r","l","c","c","c","r","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c","c")
					else:
						tab = Table("Storage Classes",21,"r")
						tab.header("","","","","","","","",("# of inodes","",2),("state statistics and definitions","",11))
						tab.header("","","","","","","","",("---","",13))
						tab.header("id","name","admin only","labels mode","arch mode","arch delay","arch min size","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","")
						tab.header("","","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","redundancy level","labels","distribution")
						tab.header("","","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","")
						tab.defattr("r","l","c","c","c","r","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c")
				else:
					tab = Table("Storage Classes",20,"r")
					tab.header("","","","","","","",("# of inodes","",2),("state statistics and definitions","",11))
					tab.header("","","","","","","",("---","",13))
					tab.header("id","name","admin only","labels mode","arch mode","arch delay","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","")
					tab.header("","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","redundancy level","labels","distribution")
					tab.header("","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","")
					tab.defattr("r","l","c","c","c","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c")
			else:
				tab = Table("Storage Classes",17,"r")
				tab.header("","","","","","","",("# of inodes","",2),("state statistics and definitions","",8))
				tab.header("id","name","admin only","labels mode","arch mode","arch delay","min trashretention",("---","",10))
				tab.header("","","","","","","","files","dirs","state","chunks under","chunks exact","chunks over","can be fulfilled","redundancy level","labels","distribution")
				tab.defattr("r","l","c","c","c","r","r","r","r","c","r","r","r","c","c","c","c")
		else:
			if show_copy_and_ec:
				tab = Table("storage classes",13)
			else:
				tab = Table("storage classes",10)

		firstrow = 1
		for sc in dataprovider.get_sclasses(SCorder,SCrev):
			if jsonmode:
				json_sc_dict = {}

				json_sc_dict["sclassid"] = sc.sclassid
				json_sc_dict["sclassname"] = sc.sclassname
				if show_export_group_and_priority:
					json_sc_dict["sclassdesc"] = sc.sclassdesc
					json_sc_dict["priority"] = sc.priority
					json_sc_dict["export_group"] = sc.export_group
				else:
					json_sc_dict["sclassdesc"] = None
					json_sc_dict["priority"] = None
					json_sc_dict["export_group"] = None
				json_sc_dict["admin_only"] = True if sc.admin_only else False
				json_sc_dict["labels_mode"] = sc.labels_mode
				json_sc_dict["labels_mode_str"] = sc.get_labels_mode_str()
				json_sc_dict["arch_mode"] = sc.arch_mode
				json_sc_dict["arch_mode_str"] = sc.get_arch_mode_str()
				json_sc_dict["arch_delay"] = sc.arch_delay
				json_sc_dict["arch_delay_str"] = sc.get_arch_delay_str()
				json_sc_dict["arch_min_size"] = sc.arch_min_size
				json_sc_dict["arch_min_size_human"] = humanize_number(sc.arch_min_size," ")
				json_sc_dict["min_trashretention"] = sc.min_trashretention
				json_sc_dict["min_trashretention_str"] = sc.get_min_trashretention_str()
				json_sc_dict["files"] = sc.files
				json_sc_dict["dirs"] = sc.dirs

				json_sc_def_dict = {}
				# for defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled in sc.states:
				for st in sc.states:
					ec_data_parts = 0
					ec_chksum_parts = 0
					if st.ec_level!=None and st.ec_level>0:
						ec_data_parts = st.ec_level >> 4
						ec_chksum_parts = st.ec_level & 0xF
						if ec_data_parts==0:
							ec_data_parts = 8
					labels_mode_over_str = "LOOSE" if st.labelsmodeover==0 else "STD" if st.labelsmodeover==1 else "STRICT" if st.labelsmodeover==2 else sc.get_labels_mode_str()
					if st.defined:
						json_sc_state_dict = {}
						if show_copy_and_ec:
							json_sc_state_dict["chunks_undergoal_copy"] = st.counters[0]
							json_sc_state_dict["chunks_undergoal_ec"] = st.counters[1]
							json_sc_state_dict["chunks_exactgoal_copy"] = st.counters[2]
							json_sc_state_dict["chunks_exactgoal_ec"] = st.counters[3]
							json_sc_state_dict["chunks_overgoal_copy"] = st.counters[4]
							json_sc_state_dict["chunks_overgoal_ec"] = st.counters[5]
						else:
							json_sc_state_dict["chunks_undergoal_copy"] = st.counters[0]
							json_sc_state_dict["chunks_undergoal_ec"] = None
							json_sc_state_dict["chunks_exactgoal_copy"] = st.counters[1]
							json_sc_state_dict["chunks_exactgoal_ec"] = None
							json_sc_state_dict["chunks_overgoal_copy"] = st.counters[2]
							json_sc_state_dict["chunks_overgoal_ec"] = None
						json_sc_state_dict["can_be_fulfilled"] = st.canbefulfilled
						if st.canbefulfilled==3:
							json_sc_state_dict["can_be_fulfilled_str"] = "YES"
						elif st.canbefulfilled==2:
							json_sc_state_dict["can_be_fulfilled_str"] = "OVERLOADED"
						elif st.canbefulfilled==1:
							json_sc_state_dict["can_be_fulfilled_str"] = "NO SPACE"
						elif st.canbefulfilled==4:
							json_sc_state_dict["can_be_fulfilled_str"] = "EC ON HOLD"
						else:
							json_sc_state_dict["can_be_fulfilled_str"] = "NO"
						if st.ec_level!=None and st.ec_level>0:
							json_sc_state_dict["ec_data_parts"] = ec_data_parts
							json_sc_state_dict["ec_chksum_parts"] = ec_chksum_parts
							json_sc_state_dict["full_copies"] = 0
							json_sc_state_dict["redundancy_level_str"] = "EC %u+%u" % (ec_data_parts,ec_chksum_parts)
						else:
							json_sc_state_dict["ec_data_parts"] = 0
							json_sc_state_dict["ec_chksum_parts"] = 0
							json_sc_state_dict["full_copies"] = len(st.labellist)
							json_sc_state_dict["redundancy_level_str"] = "COPIES: %u (1+%u)" % (len(st.labellist),len(st.labellist)-1)
						json_sc_labels_array = []
						for labelstr,matchingservers,_ in st.labellist:
							json_sc_labels_dict = {}
							json_sc_labels_dict["definition"] = labelstr
							json_sc_labels_dict["matching_servers"] = matchingservers
							json_sc_labels_dict["available_servers"] = sc.availableservers
							json_sc_labels_array.append(json_sc_labels_dict)
						json_sc_state_dict["labels_list"] = json_sc_labels_array
						json_sc_state_dict["labels_str"] = "%s" % (",".join([x for x,_,_ in labellist_fold(st.labellist)]))
						json_sc_state_dict["uniqmask"] = st.uniqmask
						json_sc_state_dict["uniqmask_str"] = uniqmask_to_str(st.uniqmask)
						json_sc_state_dict["labels_mode"] = st.labelsmodeover
						json_sc_state_dict["labels_mode_str"] = labels_mode_over_str
						json_sc_def_dict[st.name.lower()] = json_sc_state_dict
				json_sc_dict["storage_modes"] = json_sc_def_dict
				json_sc_array.append(json_sc_dict)
			elif ttymode:
				if firstrow:
					firstrow = 0
				else:
					if show_copy_and_ec:
						if show_arch_min_size:
							if show_labelmode_overrides:
								if show_export_group_and_priority:
									tab.append(("---","",24))
								else:
									tab.append(("---","",22))
							else:
								tab.append(("---","",21))
						else:
							tab.append(("---","",20))
					else:
						tab.append(("---","",17))
				first = 1
				# for defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled in sc.states:
				for st in sc.states:
					ec_data_parts = 0
					ec_chksum_parts = 0
					if st.ec_level!=None and st.ec_level>0:
						ec_data_parts = st.ec_level >> 4
						ec_chksum_parts = st.ec_level & 0xF
						if ec_data_parts==0:
							ec_data_parts = 8
					if first:
						first = 0
						if show_export_group_and_priority:
							data = [sc.sclassid,sc.sclassname,sc.priority,sc.export_group,sc.get_admin_only_str(),sc.get_labels_mode_str(),sc.get_arch_mode_str(),sc.get_arch_delay_str(),humanize_number(sc.arch_min_size," "),sc.get_min_trashretention_str(),sc.files,sc.dirs]
						elif show_arch_min_size:
							data = [sc.sclassid,sc.sclassname,sc.get_admin_only_str(),sc.get_labels_mode_str(),sc.get_arch_mode_str(),sc.get_arch_delay_str(),humanize_number(sc.arch_min_size," "),sc.get_min_trashretention_str(),sc.files,sc.dirs]
						else:
							data = [sc.sclassid,sc.sclassname,sc.get_admin_only_str(),sc.get_labels_mode_str(),sc.get_arch_mode_str(),sc.get_arch_delay_str(),sc.get_min_trashretention_str(),sc.files,sc.dirs]
					else:
						data = ["","","","","","","","",""]
						if show_arch_min_size:
							data.append("")
						if show_export_group_and_priority:
							data.append("")
							data.append("")
					if st.defined:
						data.append(st.name)
					else:
						data.append((st.name,'8'))
					if show_copy_and_ec:
						if st.counters[0]!=None and st.counters[1]!=None and st.counters[2]!=None and st.counters[3]!=None and st.counters[4]!=None and st.counters[5]!=None and st.defined:
							data.append((st.counters[0],'3') if st.counters[0]>0 else "-")
							data.append((st.counters[1],'3') if st.counters[1]>0 else "-")
							if st.ec_level!=None and st.ec_level>0:
								data.append((st.counters[2],'5') if st.counters[2]>0 else "-")
								data.append((st.counters[3],'4'))
							else:
								data.append((st.counters[2],'4'))
								data.append((st.counters[3],'5') if st.counters[3]>0 else "-")
							data.append((st.counters[4],'6') if st.counters[4]>0 else "-")
							data.append((st.counters[5],'6') if st.counters[5]>0 else "-")
						else:
							data.extend(["-","-","-","-","-","-"])
					else:
						if st.counters[0]!=None and st.counters[1]!=None and st.counters[2]!=None and st.defined:
							data.append((st.counters[0],'3') if st.counters[0]>0 else "-")
							data.append((st.counters[1],'4'))
							data.append((st.counters[2],'6') if st.counters[2]>0 else "-")
						else:
							data.extend(["-","-","-"])
					if st.canbefulfilled==3:
						data.append(("YES",('4' if st.defined else '8')))
					elif st.canbefulfilled==2:
						data.append(("OVERLOADED",('3' if st.defined else '8')))
					elif st.canbefulfilled==1:
						data.append(("NO SPACE",('2' if st.defined else '8')))
					elif st.canbefulfilled==4:
						data.append(("EC ON HOLD",('2' if st.defined else '8')))
					else:
						data.append(("NO",('1' if st.defined else '8')))
					if show_labelmode_overrides:
						labels_mode_over_str = "LOOSE" if st.labelsmodeover==0 else "STD" if st.labelsmodeover==1 else "STRICT" if st.labelsmodeover==2 else sc.get_labels_mode_str()
						if (st.labelsmodeover>=0 and st.labelsmodeover<=2 and st.defined):
							if (st.labelsmodeover!=sc.labels_mode):
								data.append((labels_mode_over_str,'3'))
							else:
								data.append(labels_mode_over_str)
						else:
							data.append((labels_mode_over_str,'8'))
					if st.ec_level!=None and st.ec_level>0:
						rlstr = "EC %u+%u" % (ec_data_parts,ec_chksum_parts)
					else:
						rlstr = "COPIES: %u (1+%u)" % (len(st.labellist),len(st.labellist)-1)
					labstr = "%s" % (",".join([x for x,_,_ in labellist_fold(st.labellist)]))
					uniqstr = uniqmask_to_str(st.uniqmask)
					if st.defined:
						data.append(rlstr)
						data.append(labstr)
						data.append(uniqstr)
					else:
						data.append((rlstr,'8'))
						data.append((labstr,'8'))
						data.append((uniqstr,'8'))
					tab.append(*data)
			else:
				if show_copy_and_ec:
					if show_arch_min_size:
						if show_export_group_and_priority:
							data = ["COMMON",sc.sclassid,sc.sclassname,sc.priority,sc.export_group,sc.get_admin_only_str(),sc.get_labels_mode_str(),sc.get_arch_mode_str(),sc.get_arch_delay_str(),sc.arch_min_size,sc.get_min_trashretention_str(),sc.files,sc.dirs]
						else:
							data = ["COMMON",sc.sclassid,sc.sclassname,sc.get_admin_only_str(),sc.get_labels_mode_str(),sc.get_arch_mode_str(),sc.get_arch_delay_str(),sc.arch_min_size,sc.get_min_trashretention_str(),sc.files,sc.dirs,"",""]
					else:
						data = ["COMMON",sc.sclassid,sc.sclassname,sc.get_admin_only_str(),sc.get_labels_mode_str(),sc.get_arch_mode_str(),sc.get_arch_delay_str(),sc.get_min_trashretention_str(),sc.files,sc.dirs,"","",""]
				else:
					data = ["COMMON",sc.sclassid,sc.sclassname,sc.get_admin_only_str(),sc.get_labels_mode_str(),sc.get_arch_mode_str(),sc.get_arch_delay_str(),sc.get_min_trashretention_str(),sc.files,sc.dirs]
				tab.append(*data)
				# for defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled in sc.states:
				for st in sc.states:
					ec_data_parts = 0
					ec_chksum_parts = 0
					if st.ec_level!=None and st.ec_level>0:
						ec_data_parts = st.ec_level >> 4
						ec_chksum_parts = st.ec_level & 0xF
						if ec_data_parts==0:
							ec_data_parts = 8
					if st.defined:
						data = [st.name,sc.sclassid]
						if show_copy_and_ec:
							if st.counters[0]!=None and st.counters[1]!=None and st.counters[2]!=None and st.counters[3]!=None and st.counters[4]!=None and st.counters[5]!=None and st.defined:
								data.append(st.counters[0])
								data.append(st.counters[1])
								data.append(st.counters[2])
								data.append(st.counters[3])
								data.append(st.counters[4])
								data.append(st.counters[5])
							else:
								data.extend(["-","-","-","-","-","-"])
						else:
							if st.counters[0]!=None and st.counters[1]!=None and st.counters[2]!=None and st.defined:
								data.append(st.counters[0])
								data.append(st.counters[1])
								data.append(st.counters[2])
							else:
								data.extend(["-","-","-"])
						if st.canbefulfilled==3:
							data.append("YES")
						elif st.canbefulfilled==2:
							data.append("OVERLOADED")
						elif st.canbefulfilled==1:
							data.append("NO SPACE")
						elif st.canbefulfilled==4:
							data.append("EC ON HOLD")
						else:
							data.append("NO")
						if show_labelmode_overrides:
							data.append("LOOSE" if st.labelsmodeover==0 else "STD" if st.labelsmodeover==1 else "STRICT" if st.labelsmodeover==2 else sc.get_labels_mode_str())
						if st.ec_level!=None and st.ec_level>0:
							rlstr = "EC %u+%u" % (ec_data_parts,ec_chksum_parts)
						else:
							rlstr = "COPIES: %u (1+%u)" % (len(st.labellist),len(st.labellist)-1)
						labstr = "%s" % (",".join([x for x,_,_ in labellist_fold(st.labellist)]))
						uniqstr = uniqmask_to_str(st.uniqmask)
						data.append(rlstr)
						data.append(labstr)
						data.append(uniqstr)
						if not show_labelmode_overrides:
							data.append("")
						tab.append(*data)

		if jsonmode:
			jcollect["dataset"]["storage_classes"] = json_sc_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Override patterns section
if org.shall_render("PA"):
	try:
		if jsonmode:
			json_pa_array = []
		elif ttymode:
			tab = Table("Override Patterns",7)
			tab.header("pattern","euid","egid","priority","storage class","trash retention","extra attributes")
			tab.defattr("r","r","r","r","r","r","r")
		else:
			tab = Table("override patterns",7)

		for op in dataprovider.get_opatterns(PAorder, PArev):
			if jsonmode:
				json_pa_dict = {}
				json_pa_dict["pattern"] = op.globname
				json_pa_dict["euid"] = op.euidstr
				json_pa_dict["egid"] = op.egidstr
				json_pa_dict["priority"] = op.priority
				json_pa_dict["storage_class_name"] = op.sclassname
				json_pa_dict["trash_retention"] = op.trashretention
				if op.trashretention==None:
					json_pa_dict["trash_retention_human"] = ""
				else:
					json_pa_dict["trash_retention_human"] = hours_to_str(op.trashretention)
				json_pa_dict["set_eattr"] = op.seteattr
				json_pa_dict["clear_eattr"] = op.clreattr
				if op.seteattr|op.clreattr:
					json_pa_dict["eattr_str"] = op.eattrstr
				else:
					json_pa_dict["eattr_str"] = None
				json_pa_array.append(json_pa_dict)
			else:
				if op.sclassname==None:
					op.sclassname = "-"
				dline = [op.globname,op.euidstr,op.egidstr,op.priority,op.sclassname]
				if op.trashretention==None:
					dline.append("-")
				elif ttymode:
					dline.append(hours_to_str(op.trashretention))
				else:
					dline.append(op.trashretention)
				if op.eattrstr=="-":
					dline.append("-")
				elif ttymode:
					dline.append(op.eattrstr)
				else:
					dline.append("%02X,%02X:%s" % (op.seteattr,op.clreattr,op.eattrstr))
				tab.append(*dline)
		if jsonmode:
			jcollect["dataset"]["patterns"] = json_pa_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Quotas
if org.shall_render("QU"):
	try:
		if jsonmode:
			json_qu_array = []
		elif ttymode:
			tab = Table("Active quotas",23)
			tab.header("",("soft quota","",6),("hard quota","",4),("current values","",12))
			tab.header("",("---","",22))
			tab.header("path","","","","","","","","","","",("inodes","",3),("length","",3),("size","",3),("real size","",3))
			tab.header("","grace period","time to expire","inodes","length","size","real size","inodes","length","size","real size",("---","",12))
			tab.header("","","","","","","","","","","","value","% soft","% hard","value","% soft","% hard","value","% soft","% hard","value","% soft","% hard")
			tab.defattr("l","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r")
		else:
			tab = Table("active quotas",16)

		maxperc,quotas = dataprovider.get_quotas(QUorder, QUrev)
		for qu in quotas:
			graceperiod_default = 0
			if cl.master().has_feature(FEATURE_DEFAULT_GRACEPERIOD):
				if qu.exceeded & 2:
					qu.exceeded &= 1
					graceperiod_default = 1
			if jsonmode:
				json_qu_dict = {}
				json_qu_dict["path"] = qu.path
				json_qu_dict["exceeded"] = True if qu.exceeded else False
				json_qu_dict["grace_period_default"] = True if graceperiod_default else False
				json_qu_dict["grace_period"] = qu.graceperiod
				if qu.graceperiod>0:
					json_qu_dict["grace_period_str"] = timeduration_to_shortstr(qu.graceperiod) + (" (default)" if graceperiod_default else "")
				else:
					json_qu_dict["grace_period_str"] = "default"
				if qu.timetoblock<0xFFFFFFFF:
					json_qu_dict["time_to_expire"] = qu.timetoblock
					if qu.timetoblock>0:
						json_qu_dict["time_to_expire_str"] = timeduration_to_shortstr(qu.timetoblock)
					else:
						json_qu_dict["time_to_expire_str"] = "expired"
				else:
					json_qu_dict["time_to_expire"] = None
					json_qu_dict["time_to_expire_str"] = ""
				json_qu_dict["flags"] = qu.qflags
				json_qu_dict["soft_quota_inodes"] = qu.sinodes if qu.qflags&1 else None
				json_qu_dict["soft_quota_length"] = qu.slength if qu.qflags&2 else None
				json_qu_dict["soft_quota_length_human"] = humanize_number(qu.slength," ") if qu.qflags&2 else None
				json_qu_dict["soft_quota_size"] = qu.ssize if qu.qflags&4 else None
				json_qu_dict["soft_quota_size_human"] = humanize_number(qu.ssize," ") if qu.qflags&4 else None
				json_qu_dict["soft_quota_realsize"] = qu.srealsize if qu.qflags&8 else None
				json_qu_dict["soft_quota_realsize_human"] = humanize_number(qu.srealsize," ") if qu.qflags&8 else None
				json_qu_dict["hard_quota_inodes"] = qu.hinodes if qu.qflags&16 else None
				json_qu_dict["hard_quota_length"] = qu.hlength if qu.qflags&32 else None
				json_qu_dict["hard_quota_length_human"] = humanize_number(qu.hlength," ") if qu.qflags&32 else None
				json_qu_dict["hard_quota_size"] = qu.hsize if qu.qflags&64 else None
				json_qu_dict["hard_quota_size_human"] = humanize_number(qu.hsize," ") if qu.qflags&64 else None
				json_qu_dict["hard_quota_realsize"] = qu.hrealsize if qu.qflags&128 else None
				json_qu_dict["hard_quota_realsize_human"] = humanize_number(qu.hrealsize," ") if qu.qflags&128 else None
				json_qu_dict["current_quota_inodes"] = qu.cinodes
				json_qu_dict["current_quota_length"] = qu.clength
				json_qu_dict["current_quota_length_human"] = humanize_number(qu.clength," ")
				json_qu_dict["current_quota_size"] = qu.csize
				json_qu_dict["current_quota_size_human"] = humanize_number(qu.csize," ")
				json_qu_dict["current_quota_realsize"] = qu.crealsize
				json_qu_dict["current_quota_realsize_human"] = humanize_number(qu.crealsize," ")
				json_qu_array.append(json_qu_dict)
			elif ttymode:
				dline = [qu.path] #,"yes" if qu.exceeded else "no"]
				if qu.graceperiod>0:
					dline.append(timeduration_to_shortstr(qu.graceperiod) + (" (default)" if graceperiod_default else ""))
				else:
					dline.append("default")
				if qu.timetoblock<0xFFFFFFFF:
					if qu.timetoblock>0:
						dline.append((timeduration_to_shortstr(qu.timetoblock),"2"))
					else:
						dline.append(("expired","1"))
				else:
					dline.append("-")
				if qu.qflags&1:
					dline.append(qu.sinodes)
				else:
					dline.append("-")
				if qu.qflags&2:
					dline.append(humanize_number(qu.slength," "))
				else:
					dline.append("-")
				if qu.qflags&4:
					dline.append(humanize_number(qu.ssize," "))
				else:
					dline.append("-")
				if qu.qflags&8:
					dline.append(humanize_number(qu.srealsize," "))
				else:
					dline.append("-")
				if qu.qflags&16:
					dline.append(qu.hinodes)
				else:
					dline.append("-")
				if qu.qflags&32:
					dline.append(humanize_number(qu.hlength," "))
				else:
					dline.append("-")
				if qu.qflags&64:
					dline.append(humanize_number(qu.hsize," "))
				else:
					dline.append("-")
				if qu.qflags&128:
					dline.append(humanize_number(qu.hrealsize," "))
				else:
					dline.append("-")
				dline.append(qu.cinodes)
				if qu.qflags&1:
					if qu.sinodes>0:
						dline.append(("%.2f" % ((100.0*qu.cinodes)/qu.sinodes),"4" if qu.sinodes>=qu.cinodes else "2" if qu.timetoblock>0 else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				if qu.qflags&16:
					if qu.hinodes>0:
						dline.append(("%.2f" % ((100.0*qu.cinodes)/qu.hinodes),"4" if qu.hinodes>qu.cinodes else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				dline.append(humanize_number(qu.clength," "))
				if qu.qflags&2:
					if qu.slength>0:
						dline.append(("%.2f" % ((100.0*qu.clength)/qu.slength),"4" if qu.slength>=qu.clength else "2" if qu.timetoblock>0 else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				if qu.qflags&32:
					if qu.hlength>0:
						dline.append(("%.2f" % ((100.0*qu.clength)/qu.hlength),"4" if qu.hlength>qu.clength else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				dline.append(humanize_number(qu.csize," "))
				if qu.qflags&4:
					if qu.ssize>0:
						dline.append(("%.2f" % ((100.0*qu.csize)/qu.ssize),"4" if qu.ssize>=qu.csize else "2" if qu.timetoblock>0 else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				if qu.qflags&64:
					if qu.hsize>0:
						dline.append(("%.2f" % ((100.0*qu.csize)/qu.hsize),"4" if qu.hsize>qu.csize else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				dline.append(humanize_number(qu.crealsize," "))
				if qu.qflags&8:
					if qu.srealsize>0:
						dline.append(("%.2f" % ((100.0*qu.crealsize)/qu.srealsize),"4" if qu.srealsize>=qu.crealsize else "2" if qu.timetoblock>0 else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				if qu.qflags&128:
					if qu.hrealsize>0:
						dline.append(("%.2f" % ((100.0*qu.crealsize)/qu.hrealsize),"4" if qu.hrealsize>qu.crealsize else "1"))
					else:
						dline.append(("inf","1"))
				else:
					dline.append("-")
				tab.append(*dline)
			else:
				dline = [qu.path,"yes" if qu.exceeded else "no"]
				if qu.graceperiod>0:
					dline.append(qu.graceperiod)
				else:
					dline.append("default")
				if qu.timetoblock<0xFFFFFFFF:
					if qu.timetoblock>0:
						dline.append(qu.timetoblock)
					else:
						dline.append("expired")
				else:
					dline.append("-")
				dline.append(qu.sinodes if qu.qflags&1 else "-")
				dline.append(qu.slength if qu.qflags&2 else "-")
				dline.append(qu.ssize if qu.qflags&4 else "-")
				dline.append(qu.srealsize if qu.qflags&8 else "-")
				dline.append(qu.hinodes if qu.qflags&16 else "-")
				dline.append(qu.hlength if qu.qflags&32 else "-")
				dline.append(qu.hsize if qu.qflags&64 else "-")
				dline.append(qu.hrealsize if qu.qflags&128 else "-")
				dline.extend((qu.cinodes,qu.clength,qu.csize,qu.crealsize))
				tab.append(*dline)
		if jsonmode:
			jcollect["dataset"]["quotas"] = json_qu_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Master charts
if org.shall_render("MC"):
	out = []
	try:
		if jsonmode:
			json_mc_array = []
			if cl.master().version_at_least(4,31,0):
				chrange = MCrange
				if (chrange<0 or chrange>3) and chrange!=9:
					chrange = 0
				if MCcount<0 or MCcount>4095:
					MCcount = 4095
				for host,port,no,mode,desc,name,raw in MCchdata:
					json_mc_dict = {}
					if (host==None or port==None):
						json_mc_dict["master"] = "leader"
					else:
						json_mc_dict["master"] = "%s:%s" % (host,port)
					json_mc_dict["name"] = name
					json_mc_dict["description"] = desc
					json_mc_dict["raw_format"] = bool(raw)
					if host==None or port==None:
						perc,base,datadict = get_charts_multi_data(cl.master(),no*10+chrange,MCcount)
					else:
						try:
							conn = MFSConn(host,port)
							perc,base,datadict = get_charts_multi_data(conn,no*10+chrange,MCcount)
							del conn
						except Exception:
							perc,base,datadict = None,None,None
					if perc!=None and base!=None and datadict!=None:
						json_mc_dict["percent"] = bool(perc)
						json_mc_data = {}
						for chrng in datadict:
							ch1data,ch2data,ch3data,ts,mul,div = datadict[chrng]
							mul,div = adjust_muldiv(mul,div,base)
							json_mc_data_range = {}
							if ch1data!=None:
								json_mc_data_range["data_array_1"] = charts_convert_data(ch1data,mul,div,raw)
								if ch2data!=None:
									json_mc_data_range["data_array_2"] = charts_convert_data(ch2data,mul,div,raw)
									if ch3data!=None:
										json_mc_data_range["data_array_3"] = charts_convert_data(ch3data,mul,div,raw)
							json_mc_data_range["timestamp"] = shiftts(ts)
							json_mc_data_range["shifted_timestamp"] = ts
							timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
							json_mc_data_range["timestamp_str"] = timestring
							json_mc_data_range["time_step"] = [60,360,1800,86400][chrng]
							if raw:
								json_mc_data_range["multiplier"] = mul
								json_mc_data_range["divisor"] = div
							json_mc_data[chrng] = json_mc_data_range
						json_mc_dict["data_ranges"] = json_mc_data
					else:
						json_mc_dict["percent"] = None
						json_mc_dict["data_ranges"] = None
					json_mc_array.append(json_mc_dict)
			else:
				jcollect["errors"].append("Master chart data is not supported in your version of MFS - please upgrade! Minimal required version is 4.31.")
		else:
			if ttymode:
				tab = Table("Master chart data",len(MCchdata)+1,"r")
				hdrstr = ["host/port ->"]
				for host,port,no,mode,desc,name,raw in MCchdata:
					if (host==None or port==None):
						hdrstr.append("leader")
					else:
						hdrstr.append("%s:%s" % (host,port))
				tab.header(*hdrstr)
				tab.header(("---","",len(MCchdata)+1))
				hdrstr = ["Time"]
				for host,port,no,mode,desc,name,raw in MCchdata:
					if raw:
						if (no==0 or no==1 or no==100):
							hdrstr.append("%s (+)" % desc)
						else:
							hdrstr.append("%s (raw)" % desc)
					else:
						hdrstr.append(desc)
				tab.header(*hdrstr)
			else:
				tab = Table("Master chart data",len(MCchdata)+1)
			chrange = MCrange
			if chrange<0 or chrange>3:
				chrange = 0
			if MCcount<0 or MCcount>4095:
				MCcount = 4095
			chrangestep = [60,360,1800,86400][chrange]
			series = set()
			for host,port,no,mode,desc,name,raw in MCchdata:
				if no==100:
					series.add((host,port,0))
					series.add((host,port,1))
				else:
					series.add((host,port,no))
			for gpass in (1,2):
				MCresult = {}
				timestamp = 0
				entries = 0
				repeat = 0
				for host,port,x in series:
					if host==None or port==None:
						data,length = cl.master().command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LL",x*10+chrange,MCcount))
					else:
						try:
							conn = MFSConn(host,port)
							data,length = conn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LL",x*10+chrange,MCcount))
							del conn
						except Exception:
							data,length = None,0
					if length>=8:
						ts,e = struct.unpack(">LL",data[:8])
						if e*8+8==length and (entries==0 or entries==e):
							entries = e
							if timestamp==0 or timestamp==ts or gpass==2:
								timestamp=ts
								MCresult[(host,port,x)] = list(struct.unpack(">"+e*"Q",data[8:]))
							else:
								repeat = 1
								break
						else:
							MCresult[(host,port,x)]=None
					else:
						MCresult[(host,port,x)]=None
				if repeat:
					continue
				else:
					break
			for e in range(entries):
				ts = timestamp-chrangestep*e
				timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
				dline = [timestring]
				for host,port,no,mode,desc,name,raw in MCchdata:
					if no==100:
						datalist1 = MCresult[(host,port,0)]
						datalist2 = MCresult[(host,port,1)]
						if (datalist1!=None and datalist2!=None and datalist1[e]<((2**64)-1) and datalist2[e]<((2**64)-1)):
							data = datalist1[e]+datalist2[e]
						else:
							data = None
					else:
						datalist = MCresult[(host,port,no)]
						if datalist!=None and datalist[e]<((2**64)-1): data = datalist[e]
						else: data = None
					if data==None:
						dline.append("-")
					elif mode==0:
						cpu = (data/(10000.0*chrangestep))
						if raw: dline.append("%.8f%%" % (cpu))
						else: dline.append("%.2f%%" % (cpu))
					elif mode==1:
						if raw: dline.append("%u" % data)
						else:
							data = float(data)/float(chrangestep)
							dline.append("%.3f/s" % data)
					elif mode==2:
						if raw: dline.append("%u" % data)
						else: dline.append("%s" % humanize_number(data," "))
					elif mode==5:
						if raw:
							dline.append("%u" % data)
						else:
							data = float(data)/float(chrangestep)
							dline.append("%.3fMB/s" % (data/(1024.0*1024.0)))
					elif mode==6:
						dline.append("%u" % data)
					elif mode==7:
						dline.append("%us" % data)
					elif mode==8:
						diff = (data/1000.0)
						if raw: dline.append("%.8f%%" % (diff))
						else: dline.append("%.2f%%" % (diff))
				tab.append(*dline)
		if jsonmode:
			jcollect["dataset"]["mastercharts"] = json_mc_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# Chunkservers charts
if org.shall_render("CC"):
		out = []
		try:
			CCchdata_defined = []
			CCchdata_undefined = []
			for host,port,no,mode,desc,name,raw in CCchdata:
				if host==None or port==None:
					CCchdata_undefined.append((no,mode,desc,name,raw))
				else:
					CCchdata_defined.append((host,port,no,mode,desc,name,raw))
			CCchdata = []
			if len(CCchdata_undefined)>0:
				for cs in dataprovider.get_chunkservers(): # default sort by ip/port
					for no,mode,desc,name,raw in CCchdata_undefined:
						CCchdata.append((cs.host,cs.port,no,mode,desc,name,raw))
			for host,port,no,mode,desc,name,raw in CCchdata_defined:
				CCchdata.append((host,port,no,mode,desc,name,raw))
			if jsonmode:
				json_cc_array = []
				if cl.master().version_at_least(4,31,0):
					chrange = CCrange
					if (chrange<0 or chrange>3) and chrange!=9:
						chrange = 0
					if CCcount<0 or CCcount>4095:
						CCcount = 4095
					
					multiconn = MFSMultiConn(timeout=12)
					chartid_arr = []
					for host,port,no,_,_,_,_ in CCchdata:
						if not multiconn.is_registered(host,port):
							multiconn.register(host,port)
						chartid = no*10+chrange
						if not chartid in chartid_arr:
							chartid_arr.append(chartid)
					all_charts = get_charts_multi_data_async(multiconn,chartid_arr,CCcount)
					del multiconn

					for host,port,no,mode,desc,name,raw in CCchdata:
						json_cc_dict = {}
						json_cc_dict["chunkserver"] = "%s:%s" % (host,port)
						json_cc_dict["name"] = name
						json_cc_dict["description"] = desc
						json_cc_dict["raw_format"] = bool(raw)
						hostkey = "%s:%u" % (host,port)
						chartid = no*10+chrange
						chartkey = "%s:%u" % (hostkey,chartid)
						if not chartkey in all_charts:
							jcollect["errors"].append("Chunkserver's %s data for chart %s is not available. Probably this chunkserver is unreachable." % (hostkey,chartid))
							continue
						perc,base,datadict = all_charts[chartkey]
						if perc!=None and base!=None and datadict!=None:
							json_cc_dict["percent"] = bool(perc)
							json_cc_data = {}
							for chrng in datadict:
								ch1data,ch2data,ch3data,ts,mul,div = datadict[chrng]
								mul,div=adjust_muldiv(mul,div,base)
								json_cc_data_range = {}
								if ch1data!=None:
									json_cc_data_range["data_array_1"] = charts_convert_data(ch1data,mul,div,raw)
									if ch2data!=None:
										json_cc_data_range["data_array_2"] = charts_convert_data(ch2data,mul,div,raw)
										if ch3data!=None:
											json_cc_data_range["data_array_3"] = charts_convert_data(ch3data,mul,div,raw)
								json_cc_data_range["timestamp"] = shiftts(ts)
								json_cc_data_range["shifted_timestamp"] = ts
								timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
								json_cc_data_range["timestamp_str"] = timestring
								json_cc_data_range["time_step"] = [60,360,1800,86400][chrng]
								if raw:
									json_cc_data_range["multiplier"] = mul
									json_cc_data_range["divisor"] = div
								json_cc_data[chrng] = json_cc_data_range
							json_cc_dict["data_ranges"] = json_cc_data
						else:
							json_cc_dict["percent"] = None
							json_cc_dict["data_ranges"] = None
						json_cc_array.append(json_cc_dict)
				else:
					jcollect["errors"].append("Chunkserver chart data is not supported in your version of MFS - please upgrade! Minimal required version is 4.31.")
			else:
				if ttymode:
					tab = Table("Chunkserver chart data",len(CCchdata)+1,"r")
					hdrstr = ["host/port ->"]
					for host,port,no,mode,desc,name,raw in CCchdata:
						hdrstr.append("%s:%s" % (host,port))
					tab.header(*hdrstr)
					tab.header(("---","",len(CCchdata)+1))
					hdrstr = ["Time"]
					for host,port,no,mode,desc,name,raw in CCchdata:
						if raw:
							if mode==0: hdrstr.append("%s (+)" % desc)
							else: hdrstr.append("%s (raw)" % desc)
						else:
							hdrstr.append(desc)
					tab.header(*hdrstr)
				else:
					tab = Table("Chunkserver chart data",len(CCchdata)+1)
				chrange = CCrange
				if chrange<0 or chrange>3:
					chrange = 0
				if CCcount<0 or CCcount>4095:
					CCcount = 4095
				chrangestep = [60,360,1800,86400][chrange]
				series = set()
				for host,port,no,mode,desc,name,raw in CCchdata:
					if no==100:
						series.add((host,port,0))
						series.add((host,port,1))
					else:
						series.add((host,port,no))
				for gpass in (1,2):
					CCresult = {}
					timestamp = 0
					entries = 0
					repeat = 0
					for host,port,x in series:
						try:
							conn = MFSConn(host,port)
							data,length = conn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LL",x*10+chrange,CCcount))
							del conn
						except Exception:
							length = 0
						if length>=8:
							ts,e = struct.unpack(">LL",data[:8])
							if e*8+8==length and (entries==0 or entries==e):
								entries = e
								if timestamp==0 or timestamp==ts or gpass==2:
									timestamp=ts
									CCresult[(host,port,x)] = list(struct.unpack(">"+e*"Q",data[8:]))
								else:
									repeat = 1
									break
							else:
								CCresult[(host,port,x)]=None
						else:
							CCresult[(host,port,x)]=None
					if repeat:
						continue
					else:
						break
				for e in range(entries):
					ts = timestamp-chrangestep*e
					timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
					dline = [timestring]
					for host,port,no,mode,desc,name,raw in CCchdata:
						if no==100:
							datalist1 = CCresult[(host,port,0)]
							datalist2 = CCresult[(host,port,1)]
							if (datalist1!=None and datalist2!=None):
								data = datalist1[e]+datalist2[e]
							else:
								data = None
						else:
							datalist = CCresult[(host,port,no)]
							if datalist!=None:
								data = datalist[e]
							else:
								data = None
						if data==None:
							dline.append("-")
						elif mode==0:
							cpu = (data/(10000.0*chrangestep))
							if raw: dline.append("%.8f%%" % (cpu))
							else: dline.append("%.2f%%" % (cpu))
						elif mode==1:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								dline.append("%.3f/s" % data)
						elif mode==2:
							if raw: dline.append("%u" % data)
							else: dline.append("%s" % humanize_number(data," "))
						elif mode==3:
							dline.append("%u threads" % data)
						elif mode==4:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								data /= 10000000.0
								dline.append("%.2f%%" % (data))
						elif mode==5:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								dline.append("%.3fMB/s" % (data/(1024.0*1024.0)))
						elif mode==6:
							dline.append("%u" % data)
						elif mode==8:
							diff = (data/1000.0)
							if raw: dline.append("%.8f%%" % (diff))
							else: dline.append("%.2f%%" % (diff))
					tab.append(*dline)
			if jsonmode:
				jcollect["dataset"]["cscharts"] = json_cc_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()

if jsonmode and cl.master()!=None:
	print(json.dumps(jcollect))
 
