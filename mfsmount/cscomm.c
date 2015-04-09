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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <syslog.h>

#include "MFSCommunication.h"
#include "sockets.h"
#include "datapack.h"
#include "strerr.h"
#include "mfsstrerr.h"
#include "crc.h"

#define CSMSECTIMEOUT 5000

int cs_readblock(int fd,uint64_t chunkid,uint32_t version,uint32_t offset,uint32_t size,uint8_t *buff) {
	uint8_t *wptr,ibuff[28];
	const uint8_t *rptr;

	wptr = ibuff;
	put32bit(&wptr,CLTOCS_READ);
	put32bit(&wptr,20);
	put64bit(&wptr,chunkid);
	put32bit(&wptr,version);
	put32bit(&wptr,offset);
	put32bit(&wptr,size);
	if (tcptowrite(fd,ibuff,28,CSMSECTIMEOUT)!=28) {
		syslog(LOG_NOTICE,"readblock; tcpwrite error: %s",strerr(errno));
		return -1;
	}
	for (;;) {
		uint32_t cmd,l;
		uint64_t t64;
		uint16_t blockno,blockoffset;
		uint32_t breq,blocksize,blockcrc;
		if (tcptoread(fd,ibuff,8,CSMSECTIMEOUT)!=8) {
			syslog(LOG_NOTICE,"readblock; tcpread error: %s",strerr(errno));
			return -1;
		}
		rptr = ibuff;
		cmd = get32bit(&rptr);
		l = get32bit(&rptr);
		if (cmd==CSTOCL_READ_STATUS) {
			if (l!=9) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect message size (%"PRIu32"/9)",l);
				return -1;
			}
			if (tcptoread(fd,ibuff,9,CSMSECTIMEOUT)!=9) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS tcpread error: %s",strerr(errno));
				return -1;
			}
			rptr = ibuff;
			t64 = get64bit(&rptr);
			if (*rptr!=0) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS status: %s",mfsstrerr(*rptr));
				return -1;
			}
			if (t64!=chunkid) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect chunkid (got:%"PRIu64" expected:%"PRIu64")",t64,chunkid);
				return -1;
			}
			if (size!=0) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect data size (left: %"PRIu32")",size);
				return -1;
			}
			return 0;
		} else if (cmd==CSTOCL_READ_DATA) {
			if (l<20) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect message size (%"PRIu32"/>=20)",l);
				return -1;
			}
			if (tcptoread(fd,ibuff,20,CSMSECTIMEOUT)!=20) {
				syslog(LOG_NOTICE,"readblock; READ_DATA tcpread error: %s",strerr(errno));
				return -1;
			}
			rptr = ibuff;
			t64 = get64bit(&rptr);
			if (t64!=chunkid) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect chunkid (got:%"PRIu64" expected:%"PRIu64")",t64,chunkid);
				return -1;
			}
			blockno = get16bit(&rptr);
			blockoffset = get16bit(&rptr);
			blocksize = get32bit(&rptr);
			blockcrc = get32bit(&rptr);
			if (l!=20+blocksize) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect message size (%"PRIu32"/%"PRIu32")",l,20+blocksize);
				return -1;
			}
			if (blocksize==0) {
				syslog(LOG_NOTICE,"readblock; READ_DATA empty block");
				return -1;
			}
			if (blockno!=(offset>>MFSBLOCKBITS)) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block number (got:%"PRIu16" expected:%"PRIu32")",blockno,(offset>>MFSBLOCKBITS));
				return -1;
			}
			if (blockoffset!=(offset&MFSBLOCKMASK)) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block offset (got:%"PRIu16" expected:%"PRIu32")",blockoffset,(offset&MFSBLOCKMASK));
				return -1;
			}
			breq = MFSBLOCKSIZE - (uint32_t)blockoffset;
			if (size<breq) {
				breq=size;
			}
			if (blocksize!=breq) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block size (got:%"PRIu32" expected:%"PRIu32")",blocksize,breq);
				return -1;
			}
			if (tcptoread(fd,buff,blocksize,CSMSECTIMEOUT)!=(int32_t)blocksize) {
				syslog(LOG_NOTICE,"readblock; READ_DATA tcpread error: %s",strerr(errno));
				return -1;
			}
			if (blockcrc!=mycrc32(0,buff,blocksize)) {
				syslog(LOG_NOTICE,"readblock; READ_DATA crc checksum error");
				return -1;
			}
			offset+=blocksize;
			size-=blocksize;
			buff+=blocksize;
		} else if (cmd==ANTOAN_NOP) {
			if (l!=0) {
				syslog(LOG_NOTICE,"readblock; NOP incorrect message size (%"PRIu32"/0)",l);
				return -1;
			}
		} else if (cmd==ANTOAN_UNKNOWN_COMMAND || cmd==ANTOAN_BAD_COMMAND_SIZE) {
			syslog(LOG_NOTICE,"readblock; got UNKNOWN_COMMAND/BAD_COMMAND_SIZE !!!");
			return -1;
		} else {
			syslog(LOG_NOTICE,"readblock; unknown message");
			return -1;
		}
	}
	return 0;
}
/*
int cs_writestatus(int fd,uint64_t chunkid,uint32_t writeid) {
	uint8_t ibuff[21];
	const uint8_t *ptr;
	uint32_t t32;
	uint64_t t64;
	if (tcptoread(fd,ibuff,21,CSMSECTIMEOUT)!=21) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS tcpread error: %s",strerr(errno));
		return -1;
	}
	ptr = ibuff;
	t32 = get32bit(&ptr);
	if (t32!=CSTOCL_WRITE_STATUS) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS unknown message (%"PRIu32")",t32);
		return -1;
	}
	t32 = get32bit(&ptr);
	if (t32!=13) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS incorrect message size (%"PRIu32"/13)",t32);
		return -1;
	}
	t64 = get64bit(&ptr);
	t32 = get32bit(&ptr);
	if (*ptr!=0) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS status: %s",mfsstrerr(*ptr));
		return -1;
	}
	if (t64!=chunkid) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS incorrect chunkid (got:%"PRIu64" expected:%"PRIu64")",t64,chunkid);
		return -1;
	}
	if (t32!=writeid) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS incorrect writeid (got:%"PRIu32" expected:%"PRIu32")",t32,writeid);
		return -1;
	}
	return 0;
}

int cs_writeinit(int fd,const uint8_t *chain,uint32_t chainsize,uint64_t chunkid,uint32_t version) {
	uint8_t *ptr,*ibuff;
	uint32_t psize;
	psize = 12+chainsize;
	ibuff = malloc(8+psize);
	if (ibuff==NULL) {
		syslog(LOG_NOTICE,"writestatus; WRITE_INIT out of memory");
		return -1;
	}
	ptr = ibuff;
	put32bit(&ptr,CLTOCS_WRITE);
	put32bit(&ptr,psize);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	memcpy(ptr,chain,chainsize);
	psize+=8;
	if (tcptowrite(fd,ibuff,psize,CSMSECTIMEOUT)!=(int32_t)psize) {
		free(ibuff);
		syslog(LOG_NOTICE,"writestatus; WRITE_INIT tcpwrite error: %s",strerr(errno));
		return -1;
	}
	free(ibuff);
	return cs_writestatus(fd,chunkid,0);	// wait for connect status
}

int cs_writeblock(int fd,uint64_t chunkid,uint32_t writeid,uint16_t blockno,uint16_t offset,uint32_t size,uint8_t *buff) {
	uint8_t *ptr,ibuff[32];
	uint32_t crc,psize;
	ptr = ibuff;
	put32bit(&ptr,CLTOCS_WRITE_DATA);
	psize = 24+size;
	put32bit(&ptr,psize);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,writeid);
	put16bit(&ptr,blockno);
	put16bit(&ptr,offset);
	put32bit(&ptr,size);
	crc = mycrc32(0,buff,size);
	put32bit(&ptr,crc);
	if (tcptowrite(fd,ibuff,32,CSMSECTIMEOUT)!=32) {
		syslog(LOG_NOTICE,"writestatus; WRITE_DATA tcpwrite error: %s",strerr(errno));
		return -1;
	}
	if (tcptowrite(fd,buff,size,CSMSECTIMEOUT)!=(int32_t)size) {
		syslog(LOG_NOTICE,"writestatus; WRITE_DATA tcpwrite error: %s",strerr(errno));
		return -1;
	}
	return 0;
}
*/
