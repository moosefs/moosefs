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
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
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
#include "crc.h"
#include "cfg.h"
#include "main.h"
#include "slogger.h"
#include "massert.h"
#include "sockets.h"
#include "clocks.h"
#include "mfsalloc.h"

#define MaxPacketSize ANTOMA_MAXPACKETSIZE

#define META_DL_BLOCK ((((MATOAN_MAXPACKETSIZE) - 1000) < 1000000) ? ((MATOAN_MAXPACKETSIZE) - 1000) : 1000000)

// mode
enum {FREE,CONNECTING,DATA,KILL};

typedef struct out_packetstruct {
	struct out_packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
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

	uint32_t bindip;
	uint32_t masterip;
	uint16_t masterport;
	uint8_t masteraddrvalid;

	uint8_t downloadretrycnt;
	uint8_t downloading;
	uint8_t oldmode;
	FILE *logfd;	// using stdio because this is text file
	int metafd;	// using standard unix I/O because this is binary file
	uint64_t filesize;
	uint64_t dloffset;
	uint64_t dlstartuts;
} masterconn;

static masterconn *masterconnsingleton=NULL;

// from config
static uint32_t BackLogsNumber;
static uint32_t BackMetaCopies;
static char *MasterHost;
static char *MasterPort;
static char *BindHost;
static uint32_t Timeout;
static void *reconnect_hook;
static void *download_hook;
static uint64_t lastlogversion=0;

static uint32_t stats_bytesout=0;
static uint32_t stats_bytesin=0;

void masterconn_stats(uint32_t *bin,uint32_t *bout) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	stats_bytesin = 0;
	stats_bytesout = 0;
}

void masterconn_findlastlogversion(void) {
	struct stat st;
	uint8_t buff[32800];	// 32800 = 32768 + 32
	uint64_t size;
	uint32_t buffpos;
	uint64_t lastnewline;
	int fd;

	lastlogversion = 0;

	if (stat("metadata_ml.mfs.back",&st)<0 || st.st_size==0 || (st.st_mode & S_IFMT)!=S_IFREG) {
		return;
	}

	fd = open("changelog_ml.0.back",O_RDWR);
	if (fd<0) {
		return;
	}
	fstat(fd,&st);
	size = st.st_size;
	memset(buff,0,32);
	lastnewline = 0;
	while (size>0 && size+200000>(uint64_t)(st.st_size)) {
		if (size>32768) {
			memcpy(buff+32768,buff,32);
			size-=32768;
			lseek(fd,size,SEEK_SET);
			if (read(fd,buff,32768)!=32768) {
				lastlogversion = 0;
				close(fd);
				return;
			}
			buffpos = 32768;
		} else {
			memmove(buff+size,buff,32);
			lseek(fd,0,SEEK_SET);
			if (read(fd,buff,size)!=(ssize_t)size) {
				lastlogversion = 0;
				close(fd);
				return;
			}
			buffpos = size;
			size = 0;
		}
		// size = position in file of first byte in buff
		// buffpos = position of last byte in buff to search
		while (buffpos>0) {
			buffpos--;
			if (buff[buffpos]=='\n') {
				if (lastnewline==0) {
					lastnewline = size + buffpos;
				} else {
					if (lastnewline+1 != (uint64_t)(st.st_size)) {	// garbage at the end of file - truncate
						if (ftruncate(fd,lastnewline+1)<0) {
							lastlogversion = 0;
							close(fd);
							return;
						}
					}
					buffpos++;
					while (buffpos<32800 && buff[buffpos]>='0' && buff[buffpos]<='9') {
						lastlogversion *= 10;
						lastlogversion += buff[buffpos]-'0';
						buffpos++;
					}
					if (buffpos==32800 || buff[buffpos]!=':') {
						lastlogversion = 0;
					}
					close(fd);
					return;
				}
			}
		}
	}
	close(fd);
	return;
}

uint8_t* masterconn_createpacket(masterconn *eptr,uint32_t type,uint32_t size) {
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

void masterconn_sendregister(masterconn *eptr) {
	uint8_t *buff;

	eptr->downloading=0;
	eptr->metafd=-1;
	eptr->logfd=NULL;

	if (lastlogversion>0) {
		buff = masterconn_createpacket(eptr,ANTOMA_REGISTER,1+4+2+8);
		put8bit(&buff,2);
		put16bit(&buff,VERSMAJ);
		put8bit(&buff,VERSMID);
		put8bit(&buff,VERSMIN);
		put16bit(&buff,Timeout);
		put64bit(&buff,lastlogversion+1);
	} else {
		buff = masterconn_createpacket(eptr,ANTOMA_REGISTER,1+4+2);
		put8bit(&buff,1);
		put16bit(&buff,VERSMAJ);
		put8bit(&buff,VERSMID);
		put8bit(&buff,VERSMIN);
		put16bit(&buff,Timeout);
	}
}


void masterconn_metachanges_log(masterconn *eptr,const uint8_t *data,uint32_t length) {
	char logname1[100],logname2[100];
	uint32_t i;
	uint64_t version;
	if (length==1 && data[0]==0x55) {
		if (eptr->logfd!=NULL) {
			fclose(eptr->logfd);
			eptr->logfd=NULL;
		}
		if (BackLogsNumber>0) {
			for (i=BackLogsNumber ; i>0 ; i--) {
				snprintf(logname1,100,"changelog_ml.%"PRIu32".mfs",i);
				snprintf(logname2,100,"changelog_ml.%"PRIu32".mfs",i-1);
				rename(logname2,logname1);
			}
		} else {
			unlink("changelog_ml.0.mfs");
		}
		return;
	}
	if (length<10) {
		syslog(LOG_NOTICE,"MATOAN_METACHANGES_LOG - wrong size (%"PRIu32"/9+data)",length);
		eptr->mode = KILL;
		return;
	}
	if (data[0]!=0xFF) {
		syslog(LOG_NOTICE,"MATOAN_METACHANGES_LOG - wrong packet");
		eptr->mode = KILL;
		return;
	}
	if (data[length-1]!='\0') {
		syslog(LOG_NOTICE,"MATOAN_METACHANGES_LOG - invalid string");
		eptr->mode = KILL;
		return;
	}

	data++;
	version = get64bit(&data);

	if (lastlogversion>0 && version!=lastlogversion+1) {
		syslog(LOG_WARNING, "some changes lost: [%"PRIu64"-%"PRIu64"], download metadata again",lastlogversion,version-1);
		if (eptr->logfd!=NULL) {
			fclose(eptr->logfd);
			eptr->logfd=NULL;
		}
		for (i=0 ; i<=BackLogsNumber ; i++) {
			snprintf(logname1,100,"changelog_ml.%"PRIu32".mfs",i);
			unlink(logname1);
		}
		lastlogversion = 0;
		eptr->mode = KILL;
		return;
	}

	if (eptr->logfd==NULL) {
		eptr->logfd = fopen("changelog_ml.0.mfs","a");
	}

	if (eptr->logfd) {
		fprintf(eptr->logfd,"%"PRIu64": %s\n",version,data);
		lastlogversion = version;
	} else {
		syslog(LOG_NOTICE,"lost MFS change %"PRIu64": %s",version,data);
	}
}

void masterconn_metachanges_flush(void) {
	masterconn *eptr = masterconnsingleton;
	if (eptr->logfd) {
		fflush(eptr->logfd);
	}
}

int masterconn_download_end(masterconn *eptr) {
	eptr->downloading=0;
	masterconn_createpacket(eptr,ANTOMA_DOWNLOAD_END,0);
	if (eptr->metafd>=0) {
		if (close(eptr->metafd)<0) {
			mfs_errlog_silent(LOG_NOTICE,"error closing metafile");
			eptr->metafd=-1;
			return -1;
		}
		eptr->metafd=-1;
	}
	return 0;
}

void masterconn_download_init(masterconn *eptr,uint8_t filenum) {
	uint8_t *ptr;
//	syslog(LOG_NOTICE,"download_init %d",filenum);
	if (eptr->mode==DATA && eptr->downloading==0) {
//		syslog(LOG_NOTICE,"sending packet");
		ptr = masterconn_createpacket(eptr,ANTOMA_DOWNLOAD_START,1);
		put8bit(&ptr,filenum);
		eptr->downloading=filenum;
	}
}

void masterconn_metadownloadinit(void) {
	masterconn_download_init(masterconnsingleton,1);
}

int masterconn_metadata_check(char *name) {
	int fd;
	char chkbuff[16];
	char eofmark[16];
	const uint8_t *rptr;
	uint64_t metaversion,metaid;
	fd = open(name,O_RDONLY);
	if (fd<0) {
		syslog(LOG_WARNING,"can't open downloaded metadata");
		return -1;
	}
	if (read(fd,chkbuff,8)!=8) {
		syslog(LOG_WARNING,"can't read downloaded metadata");
		close(fd);
		return -1;
	}
	if (memcmp(chkbuff,"MFSM NEW",8)==0) { // silently ignore "new file"
		close(fd);
		return -1;
	}
	if (memcmp(chkbuff,MFSSIGNATURE "M ",5)==0 && chkbuff[5]>='1' && chkbuff[5]<='9' && chkbuff[6]=='.' && chkbuff[7]>='0' && chkbuff[7]<='9') {
		uint8_t fver = ((chkbuff[5]-'0')<<4)+(chkbuff[7]-'0');
		if (fver<0x17) {
			memset(eofmark,0,16);
		} else {
			memcpy(eofmark,"[MFS EOF MARKER]",16);
			if (fver>=0x20) {
				if (read(fd,chkbuff,16)!=16) {
					syslog(LOG_WARNING,"can't read downloaded metadata");
					close(fd);
					return -1;
				}
				rptr = (uint8_t*)chkbuff;
				metaversion = get64bit(&rptr);
				metaid = get64bit(&rptr);
				syslog(LOG_NOTICE,"meta data version: %"PRIu64", meta data id: 0x%016"PRIX64,metaversion,metaid);
			}
		}
	} else {
		syslog(LOG_WARNING,"bad metadata file format");
		close(fd);
		return -1;
	}
	lseek(fd,-16,SEEK_END);
	if (read(fd,chkbuff,16)!=16) {
		syslog(LOG_WARNING,"can't read downloaded metadata");
		close(fd);
		return -1;
	}
	close(fd);
	if (memcmp(chkbuff,eofmark,16)!=0) {
		syslog(LOG_WARNING,"truncated metadata file !!!");
		return -1;
	}
	return 0;
}

void masterconn_download_next(masterconn *eptr) {
	uint8_t *ptr;
	uint8_t filenum;
	int64_t dltime;
	if (eptr->dloffset>=eptr->filesize) {	// end of file
		filenum = eptr->downloading;
		if (masterconn_download_end(eptr)<0) {
			return;
		}
		dltime = monotonic_useconds()-eptr->dlstartuts;
		if (dltime<=0) {
			dltime=1;
		}
		syslog(LOG_NOTICE,"%s downloaded %"PRIu64"B/%"PRIu64".%06"PRIu32"s (%.3lf MB/s)",(filenum==1)?"metadata":(filenum==11)?"changelog_0":(filenum==12)?"changelog_1":"???",eptr->filesize,dltime/1000000,(uint32_t)(dltime%1000000),(double)(eptr->filesize)/(double)(dltime));
		if (filenum==1) {
			if (masterconn_metadata_check("metadata_ml.tmp")==0) {
				if (BackMetaCopies>0) {
					char metaname1[100],metaname2[100];
					int i;
					for (i=BackMetaCopies-1 ; i>0 ; i--) {
						snprintf(metaname1,100,"metadata_ml.mfs.back.%"PRIu32,i+1);
						snprintf(metaname2,100,"metadata_ml.mfs.back.%"PRIu32,i);
						rename(metaname2,metaname1);
					}
					rename("metadata_ml.mfs.back","metadata_ml.mfs.back.1");
				}
				if (rename("metadata_ml.tmp","metadata_ml.mfs.back")<0) {
					syslog(LOG_NOTICE,"can't rename downloaded metadata - do it manually before next download");
				}
			}
			if (eptr->oldmode==0) {
				masterconn_download_init(eptr,11);
			}
		} else if (filenum==11) {
			if (rename("changelog_ml.tmp","changelog_ml_back.0.mfs")<0) {
				syslog(LOG_NOTICE,"can't rename downloaded changelog - do it manually before next download");
			}
			masterconn_download_init(eptr,12);
		} else if (filenum==12) {
			if (rename("changelog_ml.tmp","changelog_ml_back.1.mfs")<0) {
				syslog(LOG_NOTICE,"can't rename downloaded changelog - do it manually before next download");
			}
		}
	} else {	// send request for next data packet
		ptr = masterconn_createpacket(eptr,ANTOMA_DOWNLOAD_REQUEST,12);
		put64bit(&ptr,eptr->dloffset);
		if (eptr->filesize-eptr->dloffset>META_DL_BLOCK) {
			put32bit(&ptr,META_DL_BLOCK);
		} else {
			put32bit(&ptr,eptr->filesize-eptr->dloffset);
		}
	}
}

void masterconn_download_info(masterconn *eptr,const uint8_t *data,uint32_t length) {
	if (length!=1 && length!=8) {
		syslog(LOG_NOTICE,"MATOAN_DOWNLOAD_INFO - wrong size (%"PRIu32"/1|8)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	if (length==1) {
		eptr->downloading = 0;
		syslog(LOG_NOTICE,"download start error");
		return;
	}
	eptr->filesize = get64bit(&data);
	eptr->dloffset = 0;
	eptr->downloadretrycnt = 0;
	eptr->dlstartuts = monotonic_useconds();
	if (eptr->downloading==1) {
		eptr->metafd = open("metadata_ml.tmp",O_WRONLY | O_TRUNC | O_CREAT,0666);
	} else if (eptr->downloading==11 || eptr->downloading==12) {
		eptr->metafd = open("changelog_ml.tmp",O_WRONLY | O_TRUNC | O_CREAT,0666);
	} else {
		syslog(LOG_NOTICE,"unexpected MATOAN_DOWNLOAD_INFO packet");
		eptr->mode = KILL;
		return;
	}
	if (eptr->metafd<0) {
		mfs_errlog_silent(LOG_NOTICE,"error opening metafile");
		masterconn_download_end(eptr);
		return;
	}
	masterconn_download_next(eptr);
}

void masterconn_download_data(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t offset;
	uint32_t leng;
	uint32_t crc;
	ssize_t ret;
	if (eptr->metafd<0) {
		syslog(LOG_NOTICE,"MATOAN_DOWNLOAD_DATA - file not opened");
		eptr->mode = KILL;
		return;
	}
	if (length<16) {
		syslog(LOG_NOTICE,"MATOAN_DOWNLOAD_DATA - wrong size (%"PRIu32"/16+data)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	offset = get64bit(&data);
	leng = get32bit(&data);
	crc = get32bit(&data);
	if (leng+16!=length) {
		syslog(LOG_NOTICE,"MATOAN_DOWNLOAD_DATA - wrong size (%"PRIu32"/16+%"PRIu32")",length,leng);
		eptr->mode = KILL;
		return;
	}
	if (offset!=eptr->dloffset) {
		syslog(LOG_NOTICE,"MATOAN_DOWNLOAD_DATA - unexpected file offset (%"PRIu64"/%"PRIu64")",offset,eptr->dloffset);
		eptr->mode = KILL;
		return;
	}
	if (offset+leng>eptr->filesize) {
		syslog(LOG_NOTICE,"MATOAN_DOWNLOAD_DATA - unexpected file size (%"PRIu64"/%"PRIu64")",offset+leng,eptr->filesize);
		eptr->mode = KILL;
		return;
	}
#ifdef HAVE_PWRITE
	ret = pwrite(eptr->metafd,data,leng,offset);
#else /* HAVE_PWRITE */
	lseek(eptr->metafd,offset,SEEK_SET);
	ret = write(eptr->metafd,data,leng);
#endif /* HAVE_PWRITE */
	if (ret!=(ssize_t)leng) {
		mfs_errlog_silent(LOG_NOTICE,"error writing metafile");
		if (eptr->downloadretrycnt>=5) {
			masterconn_download_end(eptr);
		} else {
			eptr->downloadretrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	if (crc!=mycrc32(0,data,leng)) {
		syslog(LOG_NOTICE,"metafile data crc error");
		if (eptr->downloadretrycnt>=5) {
			masterconn_download_end(eptr);
		} else {
			eptr->downloadretrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	if (fsync(eptr->metafd)<0) {
		mfs_errlog_silent(LOG_NOTICE,"error syncing metafile");
		if (eptr->downloadretrycnt>=5) {
			masterconn_download_end(eptr);
		} else {
			eptr->downloadretrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	eptr->dloffset+=leng;
	eptr->downloadretrycnt=0;
	masterconn_download_next(eptr);
}

void masterconn_beforeclose(masterconn *eptr) {
	if (eptr->downloading==11 || eptr->downloading==12) {	// old (version less than 1.6.18) master patch
		syslog(LOG_WARNING,"old master detected - please upgrade your master server and then restart metalogger");
		eptr->oldmode=1;
	}
	if (eptr->metafd>=0) {
		close(eptr->metafd);
		eptr->metafd=-1;
		unlink("metadata_ml.tmp");
		unlink("changelog_ml.tmp");
	}
	if (eptr->logfd) {
		fclose(eptr->logfd);
		eptr->logfd = NULL;
	}
}

void masterconn_gotpacket(masterconn *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case ANTOAN_UNKNOWN_COMMAND: // for future use
			break;
		case ANTOAN_BAD_COMMAND_SIZE: // for future use
			break;
		case MATOAN_METACHANGES_LOG:
			masterconn_metachanges_log(eptr,data,length);
			break;
		case MATOAN_DOWNLOAD_INFO:
			masterconn_download_info(eptr,data,length);
			break;
		case MATOAN_DOWNLOAD_DATA:
			masterconn_download_data(eptr,data,length);
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

	masterconn_sendregister(eptr);
	if (lastlogversion==0) {
		masterconn_metadownloadinit();
	}
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
			eptr->masterip = mip;
			eptr->masterport = mport;
			eptr->masteraddrvalid = 1;
		} else {
			mfs_arg_syslog(LOG_WARNING,"can't resolve master host/port (%s:%s)",MasterHost,MasterPort);
			return -1;
		}
	}
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
		mfs_errlog(LOG_WARNING,"create socket, error");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
		mfs_errlog(LOG_WARNING,"set nonblock, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		return -1;
	}
	if (eptr->bindip>0) {
		if (tcpnumbind(eptr->sock,eptr->bindip,0)<0) {
			mfs_errlog(LOG_WARNING,"can't bind socket to given ip");
			tcpclose(eptr->sock);
			eptr->sock = -1;
			return -1;
		}
	}
	status = tcpnumconnect(eptr->sock,eptr->masterip,eptr->masterport);
	if (status<0) {
		mfs_errlog(LOG_WARNING,"connect failed, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->masteraddrvalid = 0;
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
	if ((eptr->mode==DATA && eptr->outputhead!=NULL) || eptr->mode==CONNECTING) {
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

	if (eptr->mode == KILL) {
		masterconn_beforeclose(eptr);
		tcpclose(eptr->sock);
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
				eptr->input_end = 1;
			}
			masterconn_parse(eptr);
		}
		if (eptr->mode==DATA && eptr->lastwrite+(Timeout/3.0)<now && eptr->outputhead==NULL) {
			masterconn_createpacket(eptr,ANTOAN_NOP,0);
		}
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode==DATA) {
				masterconn_write(eptr,now);
			}
		}
		if (eptr->mode==DATA && eptr->lastread+Timeout<now) {
			eptr->mode = KILL;
		}
	}
	masterconn_disconnection_check();
}

void masterconn_reconnect(void) {
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode==FREE) {
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
	uint32_t MetaDLFreq;

	free(MasterHost);
	free(MasterPort);
	free(BindHost);

	MasterHost = cfg_getstr("MASTER_HOST",DEFAULT_MASTERNAME);
	MasterPort = cfg_getstr("MASTER_PORT",DEFAULT_MASTER_CONTROL_PORT);
	BindHost = cfg_getstr("BIND_HOST","*");

	eptr->masteraddrvalid = 0;
	if (eptr->mode!=FREE) {
		eptr->mode = KILL;
	}

	Timeout = cfg_getuint32("MASTER_TIMEOUT",10);
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	BackMetaCopies = cfg_getuint32("BACK_META_KEEP_PREVIOUS",3);

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);
	MetaDLFreq = cfg_getuint32("META_DOWNLOAD_FREQ",24);

	if (Timeout>65535) {
		Timeout=65535;
	}
	if (Timeout<10) {
		Timeout=10;
	}
	if (BackLogsNumber<5) {
		BackLogsNumber=5;
	}
	if (BackLogsNumber>10000) {
		BackLogsNumber=10000;
	}
	if (MetaDLFreq>(BackLogsNumber/2)) {
		MetaDLFreq=BackLogsNumber/2;
	}
	if (BackMetaCopies>99) {
		BackMetaCopies=99;
	}

	main_time_change(reconnect_hook,ReconnectionDelay,0);
	main_time_change(download_hook,MetaDLFreq*3600,630);
}

int masterconn_init(void) {
	uint32_t ReconnectionDelay;
	uint32_t MetaDLFreq;
	masterconn *eptr;


	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);
	MasterHost = cfg_getstr("MASTER_HOST",DEFAULT_MASTERNAME);
	MasterPort = cfg_getstr("MASTER_PORT",DEFAULT_MASTER_CONTROL_PORT);
	BindHost = cfg_getstr("BIND_HOST","*");
	Timeout = cfg_getuint32("MASTER_TIMEOUT",10);
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	BackMetaCopies = cfg_getuint32("BACK_META_KEEP_PREVIOUS",3);
	MetaDLFreq = cfg_getuint32("META_DOWNLOAD_FREQ",24);

	if (Timeout>65535) {
		Timeout=65535;
	}
	if (Timeout<10) {
		Timeout=10;
	}
	if (BackLogsNumber<5) {
		BackLogsNumber=5;
	}
	if (BackLogsNumber>10000) {
		BackLogsNumber=10000;
	}
	if (MetaDLFreq>(BackLogsNumber/2)) {
		MetaDLFreq=BackLogsNumber/2;
	}
	eptr = masterconnsingleton = malloc(sizeof(masterconn));
	passert(eptr);

	eptr->masteraddrvalid = 0;
	eptr->mode = FREE;
	eptr->pdescpos = -1;
	eptr->logfd = NULL;
	eptr->metafd = -1;
	eptr->oldmode = 0;

	masterconn_findlastlogversion();
	if (masterconn_initconnect(eptr)<0) {
		return -1;
	}
	reconnect_hook = main_time_register(ReconnectionDelay,0,masterconn_reconnect);
	download_hook = main_time_register(MetaDLFreq*3600,630,masterconn_metadownloadinit);
	main_destruct_register(masterconn_term);
	main_poll_register(masterconn_desc,masterconn_serve);
	main_reload_register(masterconn_reload);
	main_time_register(1,0,masterconn_metachanges_flush);
	return 0;
}
