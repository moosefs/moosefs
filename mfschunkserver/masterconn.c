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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <time.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif

#include "MFSCommunication.h"
#include "datapack.h"
#include "masterconn.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "hddspacemgr.h"
#include "slogger.h"
#include "massert.h"
#include "random.h"
#include "bgjobs.h"
#include "csserv.h"
#include "clocks.h"
#include "md5.h"
#include "mfsalloc.h"

#define MaxPacketSize MATOCS_MAXPACKETSIZE

// has to be less than MaxPacketSize on master side divided by 8
#define LOSTCHUNKLIMIT 25000
// has to be less than MaxPacketSize on master side divided by 12
#define NEWCHUNKLIMIT 25000
// has to be less than MaxPacketSize on master side divided by 12
#define CHANGEDCHUNKLIMIT 25000


#define REPORT_LOAD_FREQ 5
#define REPORT_SPACE_FREQ 1

// force disconnection X seconds after term signal
#define FORCE_DISCONNECTION_TO 5.0

// mode
enum {FREE,CONNECTING,DATA,KILL,CLOSE};

enum {IJ_GET_CHUNK_BLOCKS,IJ_GET_CHUNK_CHECKSUM,IJ_GET_CHUNK_CHECKSUM_TAB};

// masterconn.registerstate
enum {UNREGISTERED,WAITING,INPROGRESS,REGISTERED};

typedef struct idlejob {
	uint32_t jobid;
	uint8_t op;
	uint8_t valid;
	uint64_t chunkid;
	uint32_t version;
	struct idlejob *next,**prev;
	uint8_t buff[1];
} idlejob;

typedef struct out_packetstruct {
	struct out_packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint32_t conncnt;
	uint8_t data[1];
} out_packetstruct;

typedef struct in_packetstruct {
	struct in_packetstruct *next;
	uint32_t type,leng;
	uint8_t data[1];
} in_packetstruct;

typedef struct masterconn {
	uint8_t mode;
	int sock;
	int32_t pdescpos;
	double lastread,lastwrite,conntime;
	uint8_t input_hdr[8];
	uint8_t *input_startptr;
	uint32_t input_bytesleft;
	uint8_t input_end;
	in_packetstruct *input_packet;
	in_packetstruct *inputhead,**inputtail;
	out_packetstruct *outputhead,**outputtail;

	uint32_t masterversion;
	uint32_t conncnt;
	uint32_t bindip;
	uint32_t masterip;
	uint16_t masterport;
	uint16_t timeout;
	uint8_t masteraddrvalid;
	uint8_t registerstate;
	uint8_t new_register_mode;

	uint8_t gotrndblob;
	uint8_t rndblob[32];
//	uint8_t accepted;
} masterconn;

static masterconn *masterconnsingleton=NULL;
static idlejob *idlejobs=NULL;
static uint8_t csidvalid = 0;
static void *reconnect_hook;
static void *manager_time_hook;
static double wantexittime = 0.0;

static uint64_t stats_bytesout=0;
static uint64_t stats_bytesin=0;

// from config
// static uint32_t BackLogsNumber;
static uint32_t ChunksPerRegisterPacket;
static char *MasterHost;
static char *MasterPort;
static char *BindHost;
static uint32_t Timeout;
static uint16_t ChunkServerID = 0;
static uint64_t MetaID = 0;
static char *AuthCode = NULL;
static uint32_t LabelMask = 0;

static uint64_t hddmetaid;

static int reconnectisneeded = 0;

// static FILE *logfd;

void masterconn_stats(uint64_t *bin,uint64_t *bout) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	stats_bytesin = 0;
	stats_bytesout = 0;
}

static inline void masterconn_initcsid(void) {
	int fd;
	uint8_t buff[10];
	const uint8_t *rptr;
	ssize_t ret;
	if (csidvalid) {
		return;
	}
	hddmetaid = 0;
	ChunkServerID = 0;
	MetaID = 0;
	csidvalid = 1;
	fd = open("chunkserverid.mfs",O_RDWR);
	if (fd>=0) {
		ret = read(fd,buff,10);
		rptr = buff;
		if (ret>=2) {
			ChunkServerID = get16bit(&rptr);
		}
		if (ret>=10) {
			MetaID = get64bit(&rptr);
		}
		close(fd);
	}
}

uint16_t masterconn_getcsid(void) {
	masterconn_initcsid();
	return ChunkServerID;
}

uint64_t masterconn_getmetaid(void) {
	masterconn_initcsid();
	return MetaID;
}

uint64_t masterconn_gethddmetaid() {
	return hddmetaid;
}

void masterconn_sethddmetaid(uint64_t metaid) {
	hddmetaid = metaid;
}

static inline void masterconn_setcsid(uint16_t csid,uint64_t metaid) {
	int fd;
	uint8_t buff[10],*wptr;
	if (ChunkServerID!=csid || MetaID!=metaid) {
		if (csid>0) {
			ChunkServerID = csid;
		}
		if (metaid>0) {
			MetaID = metaid;
		}
		wptr = buff;
		put16bit(&wptr,ChunkServerID);
		put64bit(&wptr,MetaID);
		fd = open("chunkserverid.mfs",O_CREAT | O_TRUNC | O_RDWR,0666);
		if (fd>=0) {
			if (write(fd,buff,10)!=10) {
				syslog(LOG_WARNING,"can't store chunkserver id (write error)");
			}
			close(fd);
		} else {
			syslog(LOG_WARNING,"can't store chunkserver id (open error)");
		}
		hdd_setmetaid(MetaID);
	}
}

uint32_t masterconn_getmasterip(void) {
	masterconn *eptr = masterconnsingleton;
	if (eptr->registerstate==REGISTERED && eptr->mode==DATA) {
		return eptr->masterip;
	}
	return 0;
}

uint16_t masterconn_getmasterport(void) {
	masterconn *eptr = masterconnsingleton;
	if (eptr->registerstate==REGISTERED && eptr->mode==DATA) {
		return eptr->masterport;
	}
	return 0;
}

void* masterconn_create_detached_packet(masterconn *eptr,uint32_t type,uint32_t size) {
	out_packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	psize = size+8;
	outpacket = malloc(offsetof(out_packetstruct,data)+psize);
#ifndef __clang_analyzer__
	passert(outpacket);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
	outpacket->bytesleft = psize;
	ptr = outpacket->data;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = outpacket->data;
	outpacket->next = NULL;
	outpacket->conncnt = eptr->conncnt;
	return outpacket;
}

uint8_t* masterconn_get_packet_data(void *packet) {
	out_packetstruct *outpacket = (out_packetstruct*)packet;
	return (outpacket->data+8);
}

void masterconn_delete_packet(void *packet) {
	free(packet);
}

void masterconn_attach_packet(masterconn *eptr,void *packet) {
	out_packetstruct *outpacket = (out_packetstruct*)packet;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
}

uint8_t* masterconn_create_attached_packet(masterconn *eptr,uint32_t type,uint32_t size) {
	out_packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	psize = size+8;
	outpacket = malloc(offsetof(out_packetstruct,data)+psize);
#ifndef __clang_analyzer__
	passert(outpacket);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
	outpacket->bytesleft = psize;
	ptr = outpacket->data;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = outpacket->data;
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

uint8_t masterconn_parselabels(void) {
	char *labelsstr,*p,c;
	uint32_t mask;
	uint8_t sep,perr;
	uint32_t newlabelmask;

	labelsstr = cfg_getstr("LABELS","");
	newlabelmask = 0;

	perr = 0;
	sep = 0;
	for (p=labelsstr ; *p ; p++) {
		c = *p;
		if (c>='A' && c<='Z') {
			mask = (1<<(c-'A'));
		} else if (c>='a' && c<='z') {
			mask = (1<<(c-'a'));
		} else {
			mask = 0;
		}
		if (mask) { // letter
			if (sep) {
				syslog(LOG_NOTICE,"LABELS: separator not found before label %c",c);
				perr = 1;
			} else {
				sep = 1;
			}
			if (newlabelmask & mask) {
				syslog(LOG_NOTICE,"LABELS: found duplicate label %c",c);
				perr = 1;
			}
			newlabelmask |= mask;
		} else if (c==',' || c==';') {
			if (sep) {
				sep = 0;
			} else {
				if (newlabelmask!=0) {
					syslog(LOG_NOTICE,"LABELS: more than one separator found");
				} else {
					syslog(LOG_NOTICE,"LABELS: found separator at the beginning of definition");
				}
				perr = 1;
			}
		} else if (c!=' ' && c!='\t') {
			syslog(LOG_NOTICE,"LABELS: unrecognized character %c",c);
			perr = 1;
		}
	}
	if (sep==0 && newlabelmask!=0) {
		syslog(LOG_NOTICE,"LABELS: found separator at the end of definition");
		perr = 1;
	}

	if (perr) {
		syslog(LOG_NOTICE,"in the current version of chunkserver the only correct LABELS format is a set of letters separated by ',' or ';' - please change your config file appropriately");
	}

	free(labelsstr);

	if (newlabelmask!=LabelMask) {
		LabelMask = newlabelmask;
		return 1;
	}
	return 0;
}

void masterconn_sendlabels(masterconn *eptr) {
	uint8_t *buff;

	buff = masterconn_create_attached_packet(eptr,CSTOMA_LABELS,4);
	put32bit(&buff,LabelMask);
}

void masterconn_sendregister(masterconn *eptr) {
	uint8_t *buff;
	uint32_t myip;
	uint16_t myport;
	uint64_t usedspace,totalspace;
	uint64_t tdusedspace,tdtotalspace;
	uint32_t chunkcount,tdchunkcount;


	myip = csserv_getlistenip();
	myport = csserv_getlistenport();
	if (eptr->new_register_mode) {
#ifdef MFSDEBUG
		syslog(LOG_NOTICE,"register ver. 6 - init + space info");
#endif
		hdd_get_space(&usedspace,&totalspace,&chunkcount,&tdusedspace,&tdtotalspace,&tdchunkcount);
		if (eptr->gotrndblob && AuthCode) {
			md5ctx md5c;
			buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1+16+4+4+2+2+2+8+8+4+8+8+4);
			put8bit(&buff,60);
			md5_init(&md5c);
			md5_update(&md5c,eptr->rndblob,16);
			md5_update(&md5c,(const uint8_t *)AuthCode,strlen(AuthCode));
			md5_update(&md5c,eptr->rndblob+16,16);
			md5_final(buff,&md5c);
			buff+=16;
		} else {
			buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1+4+4+2+2+2+8+8+4+8+8+4);
			put8bit(&buff,60);
		}
		put32bit(&buff,VERSHEX);
		put32bit(&buff,myip);
		put16bit(&buff,myport);
		put16bit(&buff,Timeout);
		put16bit(&buff,masterconn_getcsid());
		put64bit(&buff,usedspace);
		put64bit(&buff,totalspace);
		put32bit(&buff,chunkcount);
		put64bit(&buff,tdusedspace);
		put64bit(&buff,tdtotalspace);
		put32bit(&buff,tdchunkcount);
	} else {
#ifdef MFSDEBUG
		syslog(LOG_NOTICE,"register ver. 5 - init");
#endif
		buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1+4+4+2+2);
		put8bit(&buff,50);
		put32bit(&buff,VERSHEX);
		put32bit(&buff,myip);
		put16bit(&buff,myport);
		if (Timeout>0) {
			put16bit(&buff,Timeout);
		} else {
			put16bit(&buff,10);
		}
	}
}

void masterconn_sendchunksinfo(masterconn *eptr) {
	uint8_t *buff;
	uint32_t chunks;
	uint64_t usedspace,totalspace;
	uint64_t tdusedspace,tdtotalspace;
	uint32_t chunkcount,tdchunkcount;

#ifdef MFSDEBUG
	syslog(LOG_NOTICE,"register ver. %u - chunks info",(unsigned int)((eptr->new_register_mode)?6:5));
#endif
	hdd_get_chunks_begin(0);
	while ((chunks = hdd_get_chunks_next_list_count(ChunksPerRegisterPacket))) {
		buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1+chunks*(8+4));
		if (eptr->new_register_mode) {
			put8bit(&buff,61);
		} else {
			put8bit(&buff,51);
		}
		hdd_get_chunks_next_list_data(ChunksPerRegisterPacket,buff);
	}
	hdd_get_chunks_end();
	if (eptr->new_register_mode) {
#ifdef MFSDEBUG
		syslog(LOG_NOTICE,"register ver. 6 - end");
#endif
		buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1);
		put8bit(&buff,62);
	} else {
#ifdef MFSDEBUG
		syslog(LOG_NOTICE,"register ver. 5 - end + space info");
#endif
		hdd_get_space(&usedspace,&totalspace,&chunkcount,&tdusedspace,&tdtotalspace,&tdchunkcount);
		buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1+8+8+4+8+8+4);
		put8bit(&buff,52);
		put64bit(&buff,usedspace);
		put64bit(&buff,totalspace);
		put32bit(&buff,chunkcount);
		put64bit(&buff,tdusedspace);
		put64bit(&buff,tdtotalspace);
		put32bit(&buff,tdchunkcount);
	}
}

void masterconn_sendnextchunks(masterconn *eptr) {
	uint8_t *buff;
	uint32_t chunks;
	chunks = hdd_get_chunks_next_list_count(ChunksPerRegisterPacket);
	if (chunks==0) {
		hdd_get_chunks_end();
		buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1);
		put8bit(&buff,62);
		eptr->registerstate = REGISTERED;
	} else {
		buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1+chunks*(8+4));
		put8bit(&buff,61);
		hdd_get_chunks_next_list_data(ChunksPerRegisterPacket,buff);
	}
}

void masterconn_master_ack(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint8_t atype;
	uint64_t metaid;
	uint16_t csid;
	if (length!=33 && length!=17 && length!=15 && length!=9 && length!=7 && length!=5 && length!=1) {
		syslog(LOG_NOTICE,"MATOCS_MASTER_ACK - wrong size (%"PRIu32"/1|5|7|9|15|17|33)",length);
		eptr->mode = KILL;
		return;
	}
	atype = get8bit(&data);
	if (atype==0) {
		csid = 0;
		metaid = 0;
		if (length>=5) {
			eptr->masterversion = get32bit(&data);
		}
		if (length>=9) {
			if (Timeout==0) {
				eptr->timeout = get16bit(&data);
			} else {
				data+=2;
			}
			csid = get16bit(&data);
		}
		if (length>=17) {
			metaid = get64bit(&data);
			if (metaid>0 && MetaID>0 && metaid!=MetaID) { // wrong MFS instance - abort
				syslog(LOG_WARNING,"MATOCS_MASTER_ACK - wrong meta data id (file chunkserverid.mfs:%016"PRIX64" ; received from master:%016"PRIX64"). Can't connect to master",MetaID,metaid);
				eptr->registerstate = REGISTERED; // do not switch to register ver. 5
				eptr->mode = KILL;
				main_exit(); // this can't be fixed, so we should quit
				return;
			}
			if (metaid>0 && MetaID==0 && hddmetaid>0 && metaid!=hddmetaid) { // metaid from hard drives doesn't match master's metaid
				syslog(LOG_WARNING,"MATOCS_MASTER_ACK - wrong meta data id (files .metaid:%016"PRIX64" ; received from master:%016"PRIX64"). Can't connect to master",hddmetaid,metaid);
				eptr->registerstate = REGISTERED; // do not switch to register ver. 5
				eptr->mode = KILL;
				main_exit(); // this can't be fixed, so we should quit
				return;
			}
		}
		if (csid>0 || metaid>0) {
			masterconn_setcsid(csid,metaid);
		}
		if (eptr->masterversion<VERSION2INT(2,0,0)) {
			if (eptr->registerstate != REGISTERED) {
				if (eptr->registerstate == INPROGRESS) {
					hdd_get_chunks_end();
				}
				eptr->registerstate = REGISTERED;
				masterconn_sendchunksinfo(eptr);
			}
		} else {
			if (eptr->registerstate == UNREGISTERED || eptr->registerstate == WAITING) {
				hdd_get_chunks_begin(1);
				eptr->registerstate = INPROGRESS;
				if (eptr->masterversion>=VERSION2INT(2,1,0)) {
					masterconn_sendlabels(eptr);
				}
			}
			if (eptr->registerstate == INPROGRESS) {
				masterconn_sendnextchunks(eptr);
			}
		}
	} else if (atype==1 && length==5) {
		uint32_t mip;
		mip = get32bit(&data);
		if (mip) {
			// redirect to leader
			eptr->masterip = mip;
			eptr->new_register_mode = 3;
			if (eptr->registerstate == INPROGRESS) {
				hdd_get_chunks_end();
			}
			eptr->registerstate = WAITING;
#ifdef MFSDEBUG
			syslog(LOG_NOTICE,"masterconn: redirected to other master");
#endif
		} else {
			// leader not known - just reconnect
			eptr->masteraddrvalid = 0;
			syslog(LOG_NOTICE,"masterconn: follower doesn't know who is the leader, reconnect to another master");
		}
		eptr->mode = CLOSE;
	} else if (atype==2 && (length==7 || length==15)) {
#ifdef MFSDEBUG
		syslog(LOG_NOTICE,"masterconn: wait for acceptance");
#endif
		if (eptr->registerstate == INPROGRESS) {
			hdd_get_chunks_end();
		}
		eptr->registerstate = WAITING;
		eptr->masterversion = get32bit(&data);
		if (Timeout==0) {
			eptr->timeout = get16bit(&data);
		} else {
			data+=2;
		}
		if (length>=15) {
			metaid = get64bit(&data);
			if (metaid>0 && MetaID>0 && metaid!=MetaID) { // wrong MFS instance - abort
				syslog(LOG_WARNING,"MATOCS_MASTER_ACK - wrong meta data id. Can't connect to master");
				eptr->registerstate = REGISTERED; // do not switch to register ver. 5
				eptr->mode = KILL;
				return;
			}
		}
	} else if (atype==3 && length==33) {
#ifdef MFSDEBUG
		syslog(LOG_NOTICE,"masterconn: authorization needed");
#endif
		if (AuthCode==NULL) {
			syslog(LOG_WARNING,"MATOCS_MASTER_ACK - master needs authorization, but password was not defined");
			eptr->registerstate = REGISTERED; // do not switch to register ver. 5
			eptr->mode = KILL;
			return;
		}
		memcpy(eptr->rndblob,data,32);
		eptr->gotrndblob = 1;
		masterconn_sendregister(eptr);
	} else {
		syslog(LOG_NOTICE,"MATOCS_MASTER_ACK - bad type/length: %u/%u",atype,length);
		eptr->mode = KILL;
	}
}

/*
void masterconn_sendregister_v4(masterconn *eptr) {
	uint8_t *buff;
	uint32_t chunks,myip;
	uint16_t myport;
	uint64_t usedspace,totalspace;
	uint64_t tdusedspace,tdtotalspace;
	uint32_t chunkcount,tdchunkcount;

	myip = csserv_getlistenip();
	myport = csserv_getlistenport();
	hdd_get_space(&usedspace,&totalspace,&chunkcount,&tdusedspace,&tdtotalspace,&tdchunkcount);
	hdd_get_chunks_begin();
	chunks = hdd_get_chunks_count();
	buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1+4+4+2+2+8+8+4+8+8+4+chunks*(8+4));
	put8bit(&buff,4);
	put16bit(&buff,VERSMAJ);
	put8bit(&buff,VERSMID);
	put8bit(&buff,VERSMIN);
	put32bit(&buff,myip);
	put16bit(&buff,myport);
	put16bit(&buff,Timeout);
	put64bit(&buff,usedspace);
	put64bit(&buff,totalspace);
	put32bit(&buff,chunkcount);
	put64bit(&buff,tdusedspace);
	put64bit(&buff,tdtotalspace);
	put32bit(&buff,tdchunkcount);
	if (chunks>0) {
		hdd_get_chunks_data(buff);
	}
	hdd_get_chunks_end();
}
*/
/*
void masterconn_send_space(uint64_t usedspace,uint64_t totalspace,uint32_t chunkcount,uint64_t tdusedspace,uint64_t tdtotalspace,uint32_t tdchunkcount) {
	uint8_t *buff;
	masterconn *eptr = masterconnsingleton;

//	syslog(LOG_NOTICE,"%"PRIu64",%"PRIu64,usedspace,totalspace);
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		buff = masterconn_create_attached_packet(eptr,CSTOMA_SPACE,8+8+4+8+8+4);
		if (buff) {
			put64bit(&buff,usedspace);
			put64bit(&buff,totalspace);
			put32bit(&buff,chunkcount);
			put64bit(&buff,tdusedspace);
			put64bit(&buff,tdtotalspace);
			put32bit(&buff,tdchunkcount);
		}
	}
}
*/
/*
void masterconn_send_chunk_damaged(uint64_t chunkid) {
	uint8_t *buff;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		buff = masterconn_create_attached_packet(eptr,CSTOMA_CHUNK_DAMAGED,8);
		if (buff) {
			put64bit(&buff,chunkid);
		}
	}
}

void masterconn_send_chunk_lost(uint64_t chunkid) {
	uint8_t *buff;
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		buff = masterconn_create_attached_packet(eptr,CSTOMA_CHUNK_LOST,8);
		if (buff) {
			put64bit(&buff,chunkid);
		}
	}
}

void masterconn_send_error_occurred() {
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==DATA || eptr->mode==HEADER) {
		masterconn_create_attached_packet(eptr,CSTOMA_ERROR_OCCURRED,0);
	}
}
*/

void masterconn_send_disconnect_command(void) {
	masterconn *eptr = masterconnsingleton;
	uint8_t *buff;

	if (eptr->registerstate==REGISTERED && eptr->mode==DATA && eptr->masterversion>=VERSION2INT(3,0,75)) {
		syslog(LOG_NOTICE,"sending unregister command ...");
		buff = masterconn_create_attached_packet(eptr,CSTOMA_REGISTER,1);
		put8bit(&buff,63);
		eptr->mode = CLOSE;
	} else if (eptr->mode!=FREE) {
		syslog(LOG_NOTICE,"killing master connection");
		eptr->mode = KILL;
	}
}

void masterconn_check_hdd_space(void) {
	masterconn *eptr = masterconnsingleton;
	uint8_t *buff;
	if ((eptr->registerstate==REGISTERED || eptr->registerstate==INPROGRESS) && eptr->mode==DATA) {
		if (hdd_spacechanged()) {
			uint64_t usedspace,totalspace,tdusedspace,tdtotalspace;
			uint32_t chunkcount,tdchunkcount;
			buff = masterconn_create_attached_packet(eptr,CSTOMA_SPACE,8+8+4+8+8+4);
			hdd_get_space(&usedspace,&totalspace,&chunkcount,&tdusedspace,&tdtotalspace,&tdchunkcount);
			put64bit(&buff,usedspace);
			put64bit(&buff,totalspace);
			put32bit(&buff,chunkcount);
			put64bit(&buff,tdusedspace);
			put64bit(&buff,tdtotalspace);
			put32bit(&buff,tdchunkcount);
		}
	}
}

void masterconn_check_hdd_reports(void) {
	masterconn *eptr = masterconnsingleton;
	uint32_t errorcounter;
	uint32_t chunkcounter;
	uint8_t *buffl,*buffn,*buffd;

	if (reconnectisneeded) {
		masterconn_send_disconnect_command(); // closes connection
		eptr->masteraddrvalid = 0;
		reconnectisneeded = 0;
	}
	if (eptr->registerstate==REGISTERED && eptr->mode==DATA) {
		errorcounter = hdd_errorcounter();
		while (errorcounter) {
			masterconn_create_attached_packet(eptr,CSTOMA_ERROR_OCCURRED,0);
			errorcounter--;
		}
		chunkcounter = hdd_get_damaged_chunk_count();	// lock
		if (chunkcounter) {
			buffd = masterconn_create_attached_packet(eptr,CSTOMA_CHUNK_DAMAGED,8*chunkcounter);
			hdd_get_damaged_chunk_data(buffd);	// fill and unlock
		} else {
			hdd_get_damaged_chunk_data(NULL); // just unlock
		}
		chunkcounter = hdd_get_lost_chunk_count(LOSTCHUNKLIMIT);	// lock
		if (chunkcounter) {
			buffl = masterconn_create_attached_packet(eptr,CSTOMA_CHUNK_LOST,8*chunkcounter);
			hdd_get_lost_chunk_data(buffl,LOSTCHUNKLIMIT);	// fill and unlock
		} else {
			hdd_get_lost_chunk_data(NULL,0); // just unlock
		}
		chunkcounter = hdd_get_new_chunk_count(NEWCHUNKLIMIT);	// lock
		if (chunkcounter) {
			buffn = masterconn_create_attached_packet(eptr,CSTOMA_CHUNK_NEW,12*chunkcounter);
			hdd_get_new_chunk_data(buffn,NEWCHUNKLIMIT);	// fill and unlock
		} else {
			hdd_get_new_chunk_data(NULL,0); // just unlock
		}
		chunkcounter = hdd_get_changed_chunk_count(CHANGEDCHUNKLIMIT); // lock
		if (chunkcounter) {
			buffl = masterconn_create_attached_packet(eptr,CSTOMA_CHUNK_LOST,8*chunkcounter);
			buffn = masterconn_create_attached_packet(eptr,CSTOMA_CHUNK_NEW,12*chunkcounter);
			hdd_get_changed_chunk_data(buffl,buffn,CHANGEDCHUNKLIMIT); // fill and unlock
		} else {
			hdd_get_changed_chunk_data(NULL,NULL,0); // just unlock
		}
	}
}

void masterconn_reportload(void) {
	masterconn *eptr = masterconnsingleton;
	uint32_t load;
	uint8_t hltosend;
	uint8_t rebalance;
	uint8_t *buff;
	if (eptr->mode==DATA && eptr->masterversion>=VERSION2INT(1,6,28) && eptr->registerstate==REGISTERED) {
		job_get_load_and_hlstatus(&load,&hltosend);
		if (eptr->masterversion>=VERSION2INT(3,0,7)) {
			rebalance = hdd_is_rebalance_on();
			if (rebalance&2) { // in high speed rebalance force 'overloaded' status
				hltosend = HLSTATUS_OVERLOADED;
			}
			if (hltosend!=HLSTATUS_OVERLOADED && (rebalance&1)) { // not overloaded and in low speed rebalance - send 'rebalance' status
				hltosend = HLSTATUS_REBALANCE;
			}
			if (eptr->masterversion<VERSION2INT(3,0,62) && hltosend==HLSTATUS_REBALANCE) { // does master know about 'rebalance' status? if not then send 'overloaded'
				hltosend = HLSTATUS_OVERLOADED;
			}
			buff = masterconn_create_attached_packet(eptr,CSTOMA_CURRENT_LOAD,5);
			put32bit(&buff,load);
			put8bit(&buff,hltosend);
		} else {
			buff = masterconn_create_attached_packet(eptr,CSTOMA_CURRENT_LOAD,4);
			put32bit(&buff,load);
		}
	}
}

void masterconn_jobfinished(uint8_t status,void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
	if (eptr && eptr->conncnt==((out_packetstruct*)packet)->conncnt && eptr->mode==DATA) {
		ptr = masterconn_get_packet_data(packet);
		ptr[8]=status;
		masterconn_attach_packet(eptr,packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_chunkopfinished(uint8_t status,void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
	if (eptr && eptr->conncnt==((out_packetstruct*)packet)->conncnt && eptr->mode==DATA) {
		ptr = masterconn_get_packet_data(packet);
		ptr[32]=status;
		masterconn_attach_packet(eptr,packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_replicationfinished(uint8_t status,void *packet) {
	uint8_t *ptr;
	masterconn *eptr = masterconnsingleton;
//	syslog(LOG_NOTICE,"job replication status: %"PRIu8,status);
	if (eptr && eptr->conncnt==((out_packetstruct*)packet)->conncnt && eptr->mode==DATA) {
		ptr = masterconn_get_packet_data(packet);
		ptr[12]=status;
		masterconn_attach_packet(eptr,packet);
	} else {
		masterconn_delete_packet(packet);
	}
}

void masterconn_create(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"MATOCS_CREATE - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_CREATE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_create(masterconn_jobfinished,packet,chunkid,version);
}

void masterconn_delete(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"MATOCS_DELETE - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_DELETE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_delete(masterconn_jobfinished,packet,chunkid,version);
}

void masterconn_setversion(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t newversion;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+4) {
		syslog(LOG_NOTICE,"MATOCS_SET_VERSION - wrong size (%"PRIu32"/16)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	newversion = get32bit(&data);
	version = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_SET_VERSION,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_version(masterconn_jobfinished,packet,chunkid,version,newversion);
}

void masterconn_duplicate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint64_t copychunkid;
	uint32_t copyversion;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+8+4) {
		syslog(LOG_NOTICE,"MATOCS_DUPLICATE - wrong size (%"PRIu32"/24)",length);
		eptr->mode = KILL;
		return;
	}
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_DUPLICATE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,copychunkid);
	job_duplicate(masterconn_jobfinished,packet,chunkid,version,version,copychunkid,copyversion);
}

void masterconn_truncate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t leng;
	uint32_t newversion;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+4+4) {
		syslog(LOG_NOTICE,"MATOCS_TRUNCATE - wrong size (%"PRIu32"/20)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	leng = get32bit(&data);
	newversion = get32bit(&data);
	version = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_TRUNCATE,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	job_truncate(masterconn_jobfinished,packet,chunkid,version,newversion,leng);
}

void masterconn_duptrunc(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint64_t copychunkid;
	uint32_t copyversion;
	uint32_t leng;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+8+4+4) {
		syslog(LOG_NOTICE,"MATOCS_DUPTRUNC - wrong size (%"PRIu32"/28)",length);
		eptr->mode = KILL;
		return;
	}
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	leng = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_DUPTRUNC,8+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,copychunkid);
	job_duptrunc(masterconn_jobfinished,packet,chunkid,version,version,copychunkid,copyversion,leng);
}

void masterconn_chunkop(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version,newversion;
	uint64_t copychunkid;
	uint32_t copyversion;
	uint32_t leng;
	uint8_t *ptr;
	void *packet;

	if (length!=8+4+8+4+4+4) {
		syslog(LOG_NOTICE,"MATOCS_CHUNKOP - wrong size (%"PRIu32"/32)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	newversion = get32bit(&data);
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	leng = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_CHUNKOP,8+4+4+8+4+4+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	put32bit(&ptr,newversion);
	put64bit(&ptr,copychunkid);
	put32bit(&ptr,copyversion);
	put32bit(&ptr,leng);
	job_chunkop(masterconn_chunkopfinished,packet,chunkid,version,newversion,copychunkid,copyversion,leng);
}

void masterconn_replicate(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint32_t ip;
	uint16_t port;
	uint32_t xormasks[4];
	uint8_t *ptr;
	void *packet;

	if (!(length==18 || (length>=28+18 && length<=28+8*18 && (length-28)%18==0))) {
		syslog(LOG_NOTICE,"MATOCS_REPLICATE - wrong size (%"PRIu32"/18|12+n*18[n:1..100]|28+n*18[n:1..8])",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
	version = get32bit(&data);
	packet = masterconn_create_detached_packet(eptr,CSTOMA_REPLICATE,8+4+1);
	ptr = masterconn_get_packet_data(packet);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (length==8+4+4+2) {
		ip = get32bit(&data);
		port = get16bit(&data);
//		syslog(LOG_NOTICE,"start job replication (%08"PRIX64":%04"PRIX32":%04"PRIX32":%02"PRIX16")",chunkid,version,ip,port);
		job_replicate_simple(masterconn_replicationfinished,packet,chunkid,version,ip,port);
	} else {
		xormasks[0] = get32bit(&data);
		xormasks[1] = get32bit(&data);
		xormasks[2] = get32bit(&data);
		xormasks[3] = get32bit(&data);
		job_replicate_raid(masterconn_replicationfinished,packet,chunkid,version,(length-28)/18,xormasks,data);
	}
}

void masterconn_idlejob_finished(uint8_t status,void *ijp) {
	idlejob *ij = (idlejob*)ijp;
	masterconn *eptr = masterconnsingleton;
	uint8_t *ptr;

	if (eptr && eptr->mode == DATA && ij->valid) {
		switch (ij->op) {
			case IJ_GET_CHUNK_BLOCKS:
				ptr = masterconn_create_attached_packet(eptr,CSTOAN_CHUNK_BLOCKS,8+4+2+1);
				put64bit(&ptr,ij->chunkid);
				put32bit(&ptr,ij->version);
				memcpy(ptr,ij->buff,2);
				ptr+=2;
				put8bit(&ptr,status);
				break;
			case IJ_GET_CHUNK_CHECKSUM:
				if (status!=MFS_STATUS_OK) {
					ptr = masterconn_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+1);
				} else {
					ptr = masterconn_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM,8+4+4);
				}
				put64bit(&ptr,ij->chunkid);
				put32bit(&ptr,ij->version);
				if (status!=MFS_STATUS_OK) {
					put8bit(&ptr,status);
				} else {
					memcpy(ptr,ij->buff,4);
				}
				break;
			case IJ_GET_CHUNK_CHECKSUM_TAB:
				if (status!=MFS_STATUS_OK) {
					ptr = masterconn_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+1);
				} else {
					ptr = masterconn_create_attached_packet(eptr,CSTOAN_CHUNK_CHECKSUM_TAB,8+4+4*MFSBLOCKSINCHUNK);
				}
				put64bit(&ptr,ij->chunkid);
				put32bit(&ptr,ij->version);
				if (status!=MFS_STATUS_OK) {
					put8bit(&ptr,status);
				} else {
					memcpy(ptr,ij->buff,4*MFSBLOCKSINCHUNK);
				}
				break;
		}
	}
	if (ij->valid) {
		*(ij->prev) = ij->next;
		if (ij->next) {
			ij->next->prev = ij->prev;
		}
	}
	free(ij);
}

void masterconn_get_chunk_blocks(masterconn *eptr,const uint8_t *data,uint32_t length) {
	idlejob *ij;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_GET_CHUNK_BLOCKS - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	ij = malloc(offsetof(idlejob,buff)+2);
	ij->op = IJ_GET_CHUNK_BLOCKS;
	ij->chunkid = get64bit(&data);
	ij->version = get32bit(&data);
	ij->valid = 1;
	ij->next = idlejobs;
	ij->prev = &(idlejobs);
	idlejobs = ij;
	ij->jobid = job_get_chunk_blocks(masterconn_idlejob_finished,ij,ij->chunkid,ij->version,ij->buff);
}

void masterconn_get_chunk_checksum(masterconn *eptr,const uint8_t *data,uint32_t length) {
	idlejob *ij;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_GET_CHUNK_CHECKSUM - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	ij = malloc(offsetof(idlejob,buff)+4);
	ij->op = IJ_GET_CHUNK_CHECKSUM;
	ij->chunkid = get64bit(&data);
	ij->version = get32bit(&data);
	ij->valid = 1;
	ij->next = idlejobs;
	ij->prev = &(idlejobs);
	idlejobs = ij;
	ij->jobid = job_get_chunk_checksum(masterconn_idlejob_finished,ij,ij->chunkid,ij->version,ij->buff);
}

void masterconn_get_chunk_checksum_tab(masterconn *eptr,const uint8_t *data,uint32_t length) {
	idlejob *ij;

	if (length!=8+4) {
		syslog(LOG_NOTICE,"ANTOCS_GET_CHUNK_CHECKSUM_TAB - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	ij = malloc(offsetof(idlejob,buff)+4*MFSBLOCKSINCHUNK);
	ij->op = IJ_GET_CHUNK_CHECKSUM_TAB;
	ij->chunkid = get64bit(&data);
	ij->version = get32bit(&data);
	ij->valid = 1;
	ij->next = idlejobs;
	ij->prev = &(idlejobs);
	idlejobs = ij;
	ij->jobid = job_get_chunk_checksum_tab(masterconn_idlejob_finished,ij,ij->chunkid,ij->version,ij->buff);
}


void masterconn_gotpacket(masterconn *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			eptr->masteraddrvalid = 1;
			if (eptr->registerstate==UNREGISTERED) {
				eptr->registerstate=REGISTERED;
				masterconn_sendchunksinfo(eptr);
			}
			break;
		case ANTOAN_UNKNOWN_COMMAND: // for future use
			break;
		case ANTOAN_BAD_COMMAND_SIZE: // for future use
			break;
		case MATOCS_CREATE:
			masterconn_create(eptr,data,length);
			break;
		case MATOCS_DELETE:
			masterconn_delete(eptr,data,length);
			break;
		case MATOCS_SET_VERSION:
			masterconn_setversion(eptr,data,length);
			break;
		case MATOCS_DUPLICATE:
			masterconn_duplicate(eptr,data,length);
			break;
		case MATOCS_REPLICATE:
			masterconn_replicate(eptr,data,length);
			break;
		case MATOCS_CHUNKOP:
			masterconn_chunkop(eptr,data,length);
			break;
		case MATOCS_TRUNCATE:
			masterconn_truncate(eptr,data,length);
			break;
		case MATOCS_DUPTRUNC:
			masterconn_duptrunc(eptr,data,length);
			break;
		case ANTOCS_GET_CHUNK_BLOCKS:
			masterconn_get_chunk_blocks(eptr,data,length);
			break;
		case ANTOCS_GET_CHUNK_CHECKSUM:
			masterconn_get_chunk_checksum(eptr,data,length);
			break;
		case ANTOCS_GET_CHUNK_CHECKSUM_TAB:
			masterconn_get_chunk_checksum_tab(eptr,data,length);
			break;
		case MATOCS_MASTER_ACK:
			eptr->masteraddrvalid = 1;
			eptr->new_register_mode = 3;
			masterconn_master_ack(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->mode = KILL;
	}
}


void masterconn_connected(masterconn *eptr) {
	double now;

	now = monotonic_seconds();

	tcpnodelay(eptr->sock);
	eptr->mode = DATA;
	eptr->lastread = now;
	eptr->lastwrite = now;
	eptr->input_bytesleft = 8;
	eptr->input_startptr = eptr->input_hdr;
	eptr->input_end = 0;
	eptr->input_packet = NULL;
	eptr->inputhead = NULL;
	eptr->inputtail = &(eptr->inputhead);
	eptr->outputhead = NULL;
	eptr->outputtail = &(eptr->outputhead);
	eptr->conncnt++;
	eptr->masterversion = 0;
	eptr->gotrndblob = 0;
	memset(eptr->rndblob,0,32);
	eptr->registerstate = UNREGISTERED;

	masterconn_sendregister(eptr);
}

int masterconn_initconnect(masterconn *eptr) {
	int status;
	if (eptr->masteraddrvalid==0) {
		uint32_t mip,bip;
		uint16_t mport;
		if (tcpresolve(BindHost,NULL,&bip,NULL,1)<0) {
			bip = 0;
		}
		eptr->bindip = bip;
		if (tcpresolve(MasterHost,MasterPort,&mip,&mport,0)>=0) {
			if ((mip&0xFF000000)!=0x7F000000) {
//				eptr->new_register_mode = 3;
				eptr->masterip = mip;
				eptr->masterport = mport;
			} else {
				mfs_arg_syslog(LOG_WARNING,"master connection module: localhost (%u.%u.%u.%u) can't be used for connecting with master (use ip address of network controller)",(mip>>24)&0xFF,(mip>>16)&0xFF,(mip>>8)&0xFF,mip&0xFF);
				return -1;
			}
		} else {
			mfs_arg_syslog(LOG_WARNING,"master connection module: can't resolve master host/port (%s:%s)",MasterHost,MasterPort);
			return -1;
		}
	}
	eptr->masteraddrvalid = 0;
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
		mfs_errlog(LOG_WARNING,"master connection module: create socket error");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
		mfs_errlog(LOG_WARNING,"master connection module: set nonblock error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		return -1;
	}
	if (eptr->bindip>0) {
		if (tcpnumbind(eptr->sock,eptr->bindip,0)<0) {
			mfs_errlog(LOG_WARNING,"master connection module: can't bind socket to given ip");
			tcpclose(eptr->sock);
			eptr->sock = -1;
			return -1;
		}
	}
	status = tcpnumconnect(eptr->sock,eptr->masterip,eptr->masterport);
	if (status<0) {
		mfs_errlog(LOG_WARNING,"master connection module: connect failed");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		return -1;
	}
	if (status==0) {
		syslog(LOG_NOTICE,"connected to Master immediately");
		masterconn_connected(eptr);
	} else {
		eptr->mode = CONNECTING;
		eptr->conntime = monotonic_seconds();
		syslog(LOG_NOTICE,"connecting ...");
	}
	return 0;
}

void masterconn_connecttimeout(masterconn *eptr) {
	syslog(LOG_WARNING,"connection timed out");
	tcpclose(eptr->sock);
	eptr->sock = -1;
	eptr->mode = FREE;
	eptr->masteraddrvalid = 0;
}

void masterconn_connecttest(masterconn *eptr) {
	int status;

	status = tcpgetstatus(eptr->sock);
	if (status) {
		mfs_errlog_silent(LOG_WARNING,"connection failed, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->mode = FREE;
		eptr->masteraddrvalid = 0;
	} else {
		syslog(LOG_NOTICE,"connected to Master");
		masterconn_connected(eptr);
	}
}

void masterconn_read(masterconn *eptr,double now) {
	int32_t i;
	uint32_t type,leng;
	const uint8_t *ptr;
	uint32_t rbleng,rbpos;
	uint8_t err,hup;
	static uint8_t *readbuff = NULL;
	static uint32_t readbuffsize = 0;

	if (eptr == NULL) {
		if (readbuff != NULL) {
			free(readbuff);
		}
		readbuff = NULL;
		readbuffsize = 0;
		return;
	}

	if (readbuffsize==0) {
		readbuffsize = 65536;
		readbuff = malloc(readbuffsize);
		passert(readbuff);
	}

	rbleng = 0;
	err = 0;
	hup = 0;
	for (;;) {
		i = read(eptr->sock,readbuff+rbleng,readbuffsize-rbleng);
		if (i==0) {
			hup = 1;
			break;
		} else if (i<0) {
			if (ERRNO_ERROR) {
				err = 1;
			}
			break;
		} else {
			stats_bytesin+=i;
			rbleng += i;
			if (rbleng==readbuffsize) {
				readbuffsize*=2;
				readbuff = mfsrealloc(readbuff,readbuffsize);
				passert(readbuff);
			} else {
				break;
			}
		}
	}

	if (rbleng>0) {
		eptr->lastread = now;
	}

	rbpos = 0;
	while (rbpos<rbleng) {
		if ((rbleng-rbpos)>=eptr->input_bytesleft) {
			memcpy(eptr->input_startptr,readbuff+rbpos,eptr->input_bytesleft);
			i = eptr->input_bytesleft;
		} else {
			memcpy(eptr->input_startptr,readbuff+rbpos,rbleng-rbpos);
			i = rbleng-rbpos;
		}
		rbpos += i;
		eptr->input_startptr+=i;
		eptr->input_bytesleft-=i;

		if (eptr->input_bytesleft>0) {
			break;
		}

		if (eptr->input_packet == NULL) {
			ptr = eptr->input_hdr;
			type = get32bit(&ptr);
			leng = get32bit(&ptr);

			if (leng>MaxPacketSize) {
				syslog(LOG_WARNING,"Master packet too long (%"PRIu32"/%u) ; command:%"PRIu32,leng,MaxPacketSize,type);
				eptr->input_end = 1;
				return;
			}

			eptr->input_packet = malloc(offsetof(in_packetstruct,data)+leng);
			passert(eptr->input_packet);
			eptr->input_packet->next = NULL;
			eptr->input_packet->type = type;
			eptr->input_packet->leng = leng;

			eptr->input_startptr = eptr->input_packet->data;
			eptr->input_bytesleft = leng;
		}

		if (eptr->input_bytesleft>0) {
			continue;
		}

		if (eptr->input_packet != NULL) {
			*(eptr->inputtail) = eptr->input_packet;
			eptr->inputtail = &(eptr->input_packet->next);
			eptr->input_packet = NULL;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
		}
	}

	if (hup) {
		syslog(LOG_NOTICE,"connection was reset by Master");
		eptr->input_end = 1;
	} else if (err) {
		mfs_errlog_silent(LOG_NOTICE,"read from Master error");
		eptr->input_end = 1;
	}
}

void masterconn_parse(masterconn *eptr) {
	in_packetstruct *ipack;
	uint64_t starttime;
	uint64_t currtime;

	starttime = monotonic_useconds();
	currtime = starttime;
	while (eptr->mode==DATA && (ipack = eptr->inputhead)!=NULL && starttime+10000>currtime) {
		masterconn_gotpacket(eptr,ipack->type,ipack->data,ipack->leng);
		eptr->inputhead = ipack->next;
		free(ipack);
		if (eptr->inputhead==NULL) {
			eptr->inputtail = &(eptr->inputhead);
		} else {
			currtime = monotonic_useconds();
		}
	}
	if (eptr->mode==DATA && eptr->inputhead==NULL && eptr->input_end) {
		eptr->mode = KILL;
	}
}

void masterconn_write(masterconn *eptr,double now) {
	out_packetstruct *opack;
	int32_t i;
#ifdef HAVE_WRITEV
	struct iovec iovtab[100];
	uint32_t iovdata;
	uint32_t leng;
	uint32_t left;

	for (;;) {
		leng = 0;
		for (iovdata=0,opack=eptr->outputhead ; iovdata<100 && opack!=NULL ; iovdata++,opack=opack->next) {
			iovtab[iovdata].iov_base = opack->startptr;
			iovtab[iovdata].iov_len = opack->bytesleft;
			leng += opack->bytesleft;
		}
		if (iovdata==0) {
			return;
		}
		i = writev(eptr->sock,iovtab,iovdata);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"write to Master error");
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		stats_bytesout+=i;
		left = i;
		while (left>0 && eptr->outputhead!=NULL) {
			opack = eptr->outputhead;
			if (opack->bytesleft>left) {
				opack->startptr+=left;
				opack->bytesleft-=left;
				left = 0;
			} else {
				left -= opack->bytesleft;
				eptr->outputhead = opack->next;
				if (eptr->outputhead==NULL) {
					eptr->outputtail = &(eptr->outputhead);
				}
				free(opack);
			}
		}
		if ((uint32_t)i < leng) {
			return;
		}
	}
#else
	for (;;) {
		opack = eptr->outputhead;
		if (opack==NULL) {
			return;
		}
		i=write(eptr->sock,opack->startptr,opack->bytesleft);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_errlog_silent(LOG_NOTICE,"write to Master error");
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		stats_bytesout+=i;
		opack->startptr+=i;
		opack->bytesleft-=i;
		if (opack->bytesleft>0) {
			return;
		}
		eptr->outputhead = opack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(opack);
	}
#endif
}


void masterconn_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	masterconn *eptr = masterconnsingleton;

	eptr->pdescpos = -1;
	if (eptr->mode==FREE || eptr->sock<0) {
		return;
	}
	pdesc[pos].events = 0;
	if (eptr->mode==DATA && eptr->input_end==0) {
		pdesc[pos].events |= POLLIN;
	}
	if (((eptr->mode==DATA || eptr->mode==CLOSE) && eptr->outputhead!=NULL) || eptr->mode==CONNECTING) {
		pdesc[pos].events |= POLLOUT;
	}
	if (pdesc[pos].events!=0) {
		pdesc[pos].fd = eptr->sock;
		eptr->pdescpos = pos;
		pos++;
	}
	*ndesc = pos;
}

void masterconn_disconnection_check(void) {
	masterconn *eptr = masterconnsingleton;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;
	idlejob *ij,*nij;

	if (eptr->mode==KILL || (eptr->mode==CLOSE && eptr->outputhead==NULL)) {
		// masterconn_beforeclose(eptr);
		syslog(LOG_NOTICE,"closing connection with master");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		if (eptr->input_packet) {
			free(eptr->input_packet);
		}
		ipptr = eptr->inputhead;
		while (ipptr) {
			ipaptr = ipptr;
			ipptr = ipptr->next;
			free(ipaptr);
		}
		opptr = eptr->outputhead;
		while (opptr) {
			opaptr = opptr;
			opptr = opptr->next;
			free(opaptr);
		}
		for (ij=idlejobs ; ij ; ij=nij) {
			nij = ij->next;
			job_pool_disable_job(ij->jobid);
			ij->next = NULL;
			ij->prev = NULL;
			ij->valid = 0;
		}
		idlejobs = NULL;
		if (eptr->registerstate == INPROGRESS) {
			hdd_get_chunks_end();
		}
		if (eptr->registerstate == UNREGISTERED && eptr->mode==KILL) {
			if (eptr->new_register_mode>0) {
				eptr->new_register_mode--;
			} else {
				eptr->new_register_mode=3;
			}
			if (eptr->new_register_mode==0) {
				eptr->masteraddrvalid = 1; // switch to old register mode and try again using same address
			} else {
				eptr->masteraddrvalid = 0; // in new register mode always resolve master address
			}
		}
		eptr->mode = FREE;
	}
}

void masterconn_serve(struct pollfd *pdesc) {
	double now;
	masterconn *eptr = masterconnsingleton;

	now = monotonic_seconds();

	if (eptr->mode==CONNECTING) {
		if (eptr->sock>=0 && eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & (POLLOUT | POLLHUP | POLLERR))) { // FD_ISSET(eptr->sock,wset)) {
			masterconn_connecttest(eptr);
		} else if (eptr->conntime+1.0 < now) {
			masterconn_connecttimeout(eptr);
		}
	} else {
		if (eptr->pdescpos>=0) {
			if ((pdesc[eptr->pdescpos].revents & (POLLERR|POLLIN))==POLLIN && eptr->mode==DATA) {
				masterconn_read(eptr,now);
			}
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				syslog(LOG_NOTICE,"masterconn: connection closed by master");
				eptr->input_end = 1;
			}
			masterconn_parse(eptr);
		}
		if ((eptr->mode==DATA || eptr->mode==CLOSE) && eptr->lastwrite+1.0<now && eptr->outputhead==NULL) {
			masterconn_create_attached_packet(eptr,ANTOAN_NOP,0);
		}
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && (eptr->mode==DATA || eptr->mode==CLOSE)) {
				masterconn_write(eptr,now);
			}
		}
		if (eptr->mode==DATA && eptr->lastread+eptr->timeout<now) {
			syslog(LOG_NOTICE,"masterconn: connection timed out");
			eptr->mode = KILL;
		}
	}
	if (eptr->mode==CLOSE && wantexittime>0.0 && wantexittime+FORCE_DISCONNECTION_TO < now) {
		syslog(LOG_NOTICE,"masterconn: unregistering timed out");
		eptr->mode = KILL;
	}
	masterconn_disconnection_check();
}

void masterconn_forcereconnect(void) {
	reconnectisneeded = 1;
}

void masterconn_reconnect(void) {
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==FREE && wantexittime==0.0) {
		masterconn_initconnect(eptr);
	}
}

void masterconn_term(void) {
	masterconn *eptr = masterconnsingleton;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;

	if (eptr->mode!=FREE) {
		tcpclose(eptr->sock);
		if (eptr->mode!=CONNECTING) {
			if (eptr->input_packet) {
				free(eptr->input_packet);
			}
			ipptr = eptr->inputhead;
			while (ipptr) {
				ipaptr = ipptr;
				ipptr = ipptr->next;
				free(ipaptr);
			}
			opptr = eptr->outputhead;
			while (opptr) {
				opaptr = opptr;
				opptr = opptr->next;
				free(opaptr);
			}
		}
	}

	masterconn_read(NULL,0.0); // free internal read buffer

	free(eptr);

	free(MasterHost);
	free(MasterPort);
	free(BindHost);
	masterconnsingleton = NULL;
}

void masterconn_reload(void) {
	masterconn *eptr = masterconnsingleton;
	uint32_t ReconnectionDelay;
	char *newAuthCode;
	char *newMasterHost;
	char *newMasterPort;
	char *newBindHost;
	uint32_t newTimeout;

	ChunksPerRegisterPacket = cfg_getuint32("CHUNKS_PER_REGISTER_PACKET",10000);
	if (ChunksPerRegisterPacket<1000) {
		ChunksPerRegisterPacket = 1000;
	}
	if (ChunksPerRegisterPacket>100000) {
		ChunksPerRegisterPacket = 100000;
	}

	if (cfg_isdefined("AUTH_CODE")) {
		newAuthCode = cfg_getstr("AUTH_CODE","");
	} else {
		newAuthCode = NULL;
	}

	if ((AuthCode==NULL && newAuthCode==NULL) || (AuthCode!=NULL && newAuthCode!=NULL && strcmp(AuthCode,newAuthCode)==0)) {
		if (newAuthCode!=NULL) {
			free(newAuthCode);
		}
	} else {
		reconnectisneeded = 1;
		if (AuthCode) {
			free(AuthCode);
			AuthCode = NULL;
		}
		AuthCode = newAuthCode;
	}

	newMasterHost = cfg_getstr("MASTER_HOST",DEFAULT_MASTERNAME);
	newMasterPort = cfg_getstr("MASTER_PORT",DEFAULT_MASTER_CS_PORT);
	newBindHost = cfg_getstr("BIND_HOST","*");

	if (strcmp(newMasterHost,MasterHost)==0 && strcmp(newMasterPort,MasterPort)==0 && strcmp(newBindHost,BindHost)==0) {
		free(newMasterHost);
		free(newMasterPort);
		free(newBindHost);
	} else {
		reconnectisneeded = 1;

		free(MasterHost);
		free(MasterPort);
		free(BindHost);
		MasterHost = newMasterHost;
		MasterPort = newMasterPort;
		BindHost = newBindHost;
	}

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);
	main_time_change(reconnect_hook,ReconnectionDelay,0);

	newTimeout = cfg_getuint32("MASTER_TIMEOUT",0);

	if (newTimeout>65535) {
		newTimeout=65535;
	}
	if (newTimeout<10 && newTimeout>0) {
		newTimeout=10;
	}

	if (newTimeout != Timeout) {
		reconnectisneeded = 1;

		Timeout = newTimeout;
	}

	if (masterconn_parselabels()) { // labels changed
		if (reconnectisneeded==0) { // we don't want to restart connection
			if (eptr && eptr->mode==DATA && eptr->registerstate==REGISTERED) {
				masterconn_sendlabels(eptr);
			} else {
				reconnectisneeded = 1;
			}
		}
	}
}

void masterconn_wantexit(void) {
	masterconn_send_disconnect_command(); // closes connection
	wantexittime = monotonic_seconds();
}

int masterconn_canexit(void) {
	masterconn *eptr = masterconnsingleton;
	return (eptr->mode==FREE || eptr->outputhead==NULL)?1:0;
}

int masterconn_init(void) {
	uint32_t ReconnectionDelay;
	masterconn *eptr;

	masterconn_initcsid();

	manager_time_hook = NULL;

	ChunksPerRegisterPacket = cfg_getuint32("CHUNKS_PER_REGISTER_PACKET",10000);
	if (ChunksPerRegisterPacket<1000) {
		ChunksPerRegisterPacket = 1000;
	}
	if (ChunksPerRegisterPacket>100000) {
		ChunksPerRegisterPacket = 100000;
	}

	if (cfg_isdefined("AUTH_CODE")) {
		AuthCode = cfg_getstr("AUTH_CODE","");
	}

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);
	MasterHost = cfg_getstr("MASTER_HOST",DEFAULT_MASTERNAME);
	MasterPort = cfg_getstr("MASTER_PORT",DEFAULT_MASTER_CS_PORT);
	BindHost = cfg_getstr("BIND_HOST","*");
	Timeout = cfg_getuint32("MASTER_TIMEOUT",0);
//	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);

	if (Timeout>65535) {
		Timeout=65535;
	}
	if (Timeout<10 && Timeout>0) {
		Timeout=10;
	}
	masterconn_parselabels();
	eptr = masterconnsingleton = malloc(sizeof(masterconn));
	passert(eptr);

	eptr->masteraddrvalid = 0;
	eptr->new_register_mode = 3;
	eptr->masterversion = 0;
	eptr->mode = FREE;
	eptr->pdescpos = -1;
	eptr->conncnt = 0;
	if (Timeout>0) {
		eptr->timeout = Timeout;
	} else {
		eptr->timeout = 10;
	}
//	logfd = NULL;

	wantexittime = 0.0;

	if (masterconn_initconnect(eptr)<0) {
		return -1;
	}

	main_time_register(REPORT_LOAD_FREQ,0,masterconn_reportload);
	main_time_register(REPORT_SPACE_FREQ,0,masterconn_check_hdd_space);
	main_eachloop_register(masterconn_check_hdd_reports);
	reconnect_hook = main_time_register(ReconnectionDelay,rndu32_ranged(ReconnectionDelay),masterconn_reconnect);
	main_destruct_register(masterconn_term);
	main_poll_register(masterconn_desc,masterconn_serve);
	main_wantexit_register(masterconn_wantexit);
	main_canexit_register(masterconn_canexit);
	main_reload_register(masterconn_reload);
	return 0;
}
