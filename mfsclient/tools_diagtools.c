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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <time.h>

#include "libmfstools.h"

#define FILEINFO_QUICK          0x01
#define FILEINFO_CRC            0x02
#define FILEINFO_SIGNATURE      0x04
#define FILEINFO_PATH           0x08
#define FILEINFO_WRONGCHUNKS    0x10

#define DIRINFO_INODES          0x01
#define DIRINFO_DIRS            0x02
#define DIRINFO_FILES           0x04
#define DIRINFO_CHUNKS          0x08
#define DIRINFO_LENGTH          0x10
#define DIRINFO_SIZE            0x20
#define DIRINFO_REALSIZE        0x40
#define DIRINFO_PRECISE         0x80

//DIAGNOSTIC TOOLS

static uint64_t sumlength,chunk_size_cnt,chunk_rsize_cnt;
static uint32_t dirnode_cnt,filenode_cnt,touched_inodes;

// ---- CHUNK/INODE SET ROUTINES ----

uint64_t sc_upper_power_of_two(uint64_t v) {//returns the lowest 2^n >= v
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v |= v >> 32;
	v++;
	return v;
}

// ---- CHUNK SET ----
uint64_t *chunkhash;
uint64_t chunkhashsize;
uint64_t chunkhashelems;

void sc_chunkset_init(uint64_t chunks) {
	if (chunks<100000) {
		chunks = 100000;
	}
	chunkhashsize = chunks;
	chunkhashsize *= 3;
	chunkhashsize /= 2;
	chunkhashsize = sc_upper_power_of_two(chunkhashsize);
	chunkhashelems = 0;
	chunkhash = malloc(sizeof(uint64_t)*chunkhashsize);
	memset(chunkhash,0,sizeof(uint64_t)*chunkhashsize);
}

void sc_chunkset_cleanup(void) {
	free(chunkhash);
	chunkhash = NULL;
	chunkhashsize = 0;
	chunkhashelems = 0;
}

uint8_t sc_chunkset_add(uint64_t chunkid) {
	uint64_t hash = hash64(chunkid);
	uint64_t disp = (hash>>32)|1;
	hash &= (chunkhashsize-1);
	disp &= (chunkhashsize-1);
	while (chunkhash[hash]!=0) {
		if (chunkhash[hash]==chunkid) {
			return 0;
		}
		hash += disp;
		hash &= (chunkhashsize-1);
	}
	chunkhash[hash] = chunkid;
	chunkhashelems++;
	massert(chunkhashelems*10 <= chunkhashsize*8,"chunk hash overloaded !!!");
	return 1;
}

// ---- INODE SET ----
uint32_t *inohash;
uint64_t inohashsize;
uint64_t inohashelems;

void sc_inoset_init(uint32_t inodes) {
	if (inodes<100000) {
		inodes = 100000;
	}
	inohashsize = inodes;
	inohashsize *= 3;
	inohashsize /= 2;
	inohashsize = sc_upper_power_of_two(inohashsize);
	inohashelems = 0;
	inohash = malloc(sizeof(uint32_t)*inohashsize);
	memset(inohash,0,sizeof(uint32_t)*inohashsize);
}

void sc_inoset_cleanup(void) {
	free(inohash);
	inohash = NULL;
	inohashsize = 0;
	inohashelems = 0;
}

uint8_t sc_inoset_add(uint32_t inode) {
	uint64_t hash = hash32mult(inode);
	uint64_t disp = (hash32(inode))|1;
	hash &= (inohashsize-1);
	disp &= (inohashsize-1);
	while (inohash[hash]!=0) {
		if (inohash[hash]==inode) {
			return 0;
		}
		hash += disp;
		hash &= (inohashsize-1);
	}
	inohash[hash] = inode;
	inohashelems++;
	massert(inohashelems*10 <= inohashsize*8,"inode hash overloaded !!!");
	return 1;
}

// ---- INODE QUEUE ----
typedef struct _inoqueue {
	uint32_t inode;
	struct _inoqueue *next;
} inoqueue;
static inoqueue *qhead,**qtail;

uint8_t sc_dequeue(uint32_t *inode) {
	inoqueue *iq;
	if (qhead==NULL) {
		return 0;
	} else {
		iq = qhead;
		qhead = iq->next;
		if (qhead==NULL) {
			qtail = &qhead;
		}
		*inode = iq->inode;
		free(iq);
		return 1;
	}
}

uint8_t sc_enqueue(uint32_t inode) {
	inoqueue *iq;
	if (sc_inoset_add(inode)) {
		iq = malloc(sizeof(inoqueue));
		iq->inode = inode;
		iq->next = NULL;
		*qtail = iq;
		qtail = &(iq->next);
		return 1;
	} else {
		return 0;
	}
}

void digest_to_str(char strdigest[33],uint8_t digest[16]) {
	uint32_t i;
	for (i=0 ; i<16 ; i++) {
		snprintf(strdigest+2*i,3,"%02X",digest[i]);
	}
	strdigest[32]='\0';
}

int get_chunk_info(const char *csstrip,uint32_t csip,uint16_t csport,uint8_t ecid,uint64_t chunkid,uint32_t version,uint8_t crcblock[4096],uint16_t *blocks,char **fpath) {
	uint8_t reqbuff[21],*wptr,*buff;
	const uint8_t *rptr;
	int fd;
	uint32_t cmd,leng;
	uint16_t cnt;
	uint8_t status;
	uint64_t combinedchunkid;
	uint32_t pleng;

	combinedchunkid = ecid;
	combinedchunkid <<= 56;
	combinedchunkid |= chunkid & UINT64_C(0x00FFFFFFFFFFFFFF);

	buff = NULL;
	fd = -1;
	cnt=0;
	while (cnt<10) {
		fd = tcpsocket();
		if (fd<0) {
			printf("can't create connection socket: %s\n",strerr(errno));
			goto error;
		}
		if (tcpnumtoconnect(fd,csip,csport,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
			tcpclose(fd);
			fd = -1;
			cnt++;
			if (cnt==10) {
				printf("can't connect to chunkserver %s:%"PRIu16": %s\n",csstrip,csport,strerr(errno));
				goto error;
			}
		} else {
			cnt=10;
		}
	}

	// packetserialiser not used (communication with chunkserver - not master) !!!

	*fpath = NULL;
	// get chunk path
	buff = NULL;
	wptr = reqbuff;
	put32bit(&wptr,ANTOCS_GET_CHUNK_INFO);
	put32bit(&wptr,13);
	put64bit(&wptr,combinedchunkid);
	put32bit(&wptr,version);
	put8bit(&wptr,REQUEST_BLOCKS|REQUEST_CHECKSUM_TAB|REQUEST_FILE_PATH);
	if (tcpwrite(fd,reqbuff,21)!=21) {
		printf("%s:%"PRIu16": cs query: send error\n",csstrip,csport);
		goto error;
	}
	do {
		if (tcpread(fd,reqbuff,8)!=8) { // do not write error - old chunkserver will not respond to this command - this is normal
//			printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
			goto error;
		}
		rptr = reqbuff;
		cmd = get32bit(&rptr);
		leng = get32bit(&rptr);
	} while (cmd==ANTOAN_NOP && leng==0);
	if (cmd!=CSTOAN_CHUNK_INFO) {
		printf("%s:%"PRIu16" cs query: wrong answer (type)\n",csstrip,csport);
		goto error;
	}
	if (leng < 13) {
		printf("%s:%"PRIu16" cs query: wrong answer (size)\n",csstrip,csport);
		goto error;
	}
	buff = malloc(leng);
	if (tcpread(fd,buff,leng) != (int32_t)leng) {
		printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
		goto error;
	}
	rptr = buff;
	if (combinedchunkid != get64bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (combinedchunkid)\n",csstrip,csport);
		goto error;
	}
	if (version != get32bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (version)\n",csstrip,csport);
		goto error;
	}
	if (leng==13) {
		status = get8bit(&rptr);
		printf("%s:%"PRIu16" cs query error: %s\n",csstrip,csport,mfsstrerr(status));
		goto error;
	}
	if (leng < (8+4+2+4096+4)) {
		printf("%s:%"PRIu16" cs query: wrong answer (size)\n",csstrip,csport);
		goto error;
	}

	*blocks = get16bit(&rptr);

	memcpy(crcblock,rptr,4096);
	rptr += 4096;

	pleng = get32bit(&rptr);
	if (leng != (8+4+2+4096+4+pleng)) {
		printf("%s:%"PRIu16" cs query: wrong answer (size)\n",csstrip,csport);
		goto error;
	}

	*fpath = malloc(pleng+1);
	memcpy(*fpath,rptr,pleng);
	(*fpath)[pleng] = '\0';
	free(buff);

	tcpclose(fd);
	return 0;
error:
	if (buff!=NULL) {
		free(buff);
	}
	if (fd>=0) {
		tcpclose(fd);
	}
	return -1;
}

int get_checksum_block(const char *csstrip,uint32_t csip,uint16_t csport,uint8_t ecid,uint64_t chunkid,uint32_t version,uint8_t crcblock[4096],uint16_t *blocks) {
	uint8_t reqbuff[21],*wptr,*buff;
	const uint8_t *rptr;
	int fd;
	uint32_t cmd,leng;
	uint16_t cnt;
	uint8_t status;
	uint64_t combinedchunkid;

	combinedchunkid = ecid;
	combinedchunkid <<= 56;
	combinedchunkid |= chunkid & UINT64_C(0x00FFFFFFFFFFFFFF);

	buff = NULL;
	fd = -1;
	cnt=0;
	while (cnt<10) {
		fd = tcpsocket();
		if (fd<0) {
			printf("can't create connection socket: %s\n",strerr(errno));
			goto error;
		}
		if (tcpnumtoconnect(fd,csip,csport,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
			tcpclose(fd);
			fd = -1;
			cnt++;
			if (cnt==10) {
				printf("can't connect to chunkserver %s:%"PRIu16": %s\n",csstrip,csport,strerr(errno));
				goto error;
			}
		} else {
			cnt=10;
		}
	}

	// packetserialiser not used (communication with chunkserver - not master) !!!

	// 1 - get checksum tab
	buff = NULL;
	wptr = reqbuff;
	put32bit(&wptr,ANTOCS_GET_CHUNK_CHECKSUM_TAB);
	put32bit(&wptr,12);
	put64bit(&wptr,combinedchunkid);
	put32bit(&wptr,version);
	if (tcpwrite(fd,reqbuff,20)!=20) {
		printf("%s:%"PRIu16": cs query: send error\n",csstrip,csport);
		goto error;
	}
	do {
		if (tcpread(fd,reqbuff,8)!=8) {
			printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
			goto error;
		}
		rptr = reqbuff;
		cmd = get32bit(&rptr);
		leng = get32bit(&rptr);
	} while (cmd==ANTOAN_NOP && leng==0);
	if (cmd!=CSTOAN_CHUNK_CHECKSUM_TAB) {
		printf("%s:%"PRIu16" cs query: wrong answer (type)\n",csstrip,csport);
		goto error;
	}
	if (leng != 13 && leng != (4096+12)) {
		printf("%s:%"PRIu16" cs query: wrong answer (size)\n",csstrip,csport);
		goto error;
	}
	buff = malloc(leng);
	if (tcpread(fd,buff,leng) != (int32_t)leng) {
		printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
		goto error;
	}
	rptr = buff;
	if (combinedchunkid != get64bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (chunkid)\n",csstrip,csport);
		goto error;
	}
	if (version != get32bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (version)\n",csstrip,csport);
		goto error;
	}
	leng -= 12;
	if (leng == 1) {
		printf("%s:%"PRIu16" cs query error: %s\n",csstrip,csport,mfsstrerr(*rptr));
		goto error;
	}
	memcpy(crcblock,rptr,4096);
	free(buff);

	// 2 - get number of blocks
	buff = NULL;
	wptr = reqbuff;
	put32bit(&wptr,ANTOCS_GET_CHUNK_BLOCKS);
	put32bit(&wptr,12);
	put64bit(&wptr,combinedchunkid);
	put32bit(&wptr,version);
	if (tcpwrite(fd,reqbuff,20)!=20) {
		printf("%s:%"PRIu16": cs query: send error\n",csstrip,csport);
		goto error;
	}
	do {
		if (tcpread(fd,reqbuff,8)!=8) {
			printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
			goto error;
		}
		rptr = reqbuff;
		cmd = get32bit(&rptr);
		leng = get32bit(&rptr);
	} while (cmd==ANTOAN_NOP && leng==0);
	if (cmd!=CSTOAN_CHUNK_BLOCKS) {
		printf("%s:%"PRIu16" cs query: wrong answer (type)\n",csstrip,csport);
		goto error;
	}
	if (leng != 15) {
		printf("%s:%"PRIu16" cs query: wrong answer (size)\n",csstrip,csport);
		goto error;
	}
	buff = malloc(leng);
	if (tcpread(fd,buff,leng) != (int32_t)leng) {
		printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
		goto error;
	}
	rptr = buff;
	if (combinedchunkid != get64bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (combinedchunkid)\n",csstrip,csport);
		goto error;
	}
	if (version != get32bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (version)\n",csstrip,csport);
		goto error;
	}
	*blocks = get16bit(&rptr);
	status = get8bit(&rptr);
	if (status!=MFS_STATUS_OK) {
		printf("%s:%"PRIu16" cs query error: %s\n",csstrip,csport,mfsstrerr(status));
		goto error;
	}
	free(buff);

	tcpclose(fd);
	return 0;
error:
	if (buff!=NULL) {
		free(buff);
	}
	if (fd>=0) {
		tcpclose(fd);
	}
	return -1;
}
//

int sc_node_info(const char *fname, uint32_t inode) {
	int32_t leng;
	uint16_t anstype;
	uint64_t fleng;
	uint64_t chunkid;
	uint32_t chunksize;
	uint32_t childinode;
	uint64_t continueid;
	uint8_t copies,eights;

//	printf("inode: %"PRIu32"\n",inode);
	continueid = 0;
	touched_inodes++;
	do {
		ps_comminit();
		ps_put32(inode);
		ps_put32(500); // 500 !!!
		ps_put64(continueid);

		if ((leng = ps_send_and_receive(CLTOMA_NODE_INFO, MATOCL_NODE_INFO, fname)) < 0) {
			goto error;
		}

		if (leng==1) {
			// error - ignore
			return 0;
		}
		if (leng<2) {
			fprintf(stderr,"%s: master query: wrong answer (size)\n",fname);
			return -1;
		}
		anstype = ps_get16();
		leng -= 2;
		if (anstype==1 && (leng%4)==0) { // directory
//			printf("directory\n");
			if (continueid==0) {
				dirnode_cnt++;
			}
			continueid = ps_get64();
//			printf("continueid: %"PRIX64"\n",continueid);
			leng -= 8;
			while (leng>0) {
				childinode = ps_get32();
				leng -= 4;
//				printf("inode: %"PRIu32"\n",childinode);
				if (sc_enqueue(childinode)==0) {
					touched_inodes++; // repeated nodes - increment here
				}
			}
		} else if (anstype==2 && (leng-16)%13==0) { // file (ver < 4.0)
//			printf("file\n");
			if (continueid==0) {
				filenode_cnt++;
			}
			continueid = ps_get64();
			fleng      = ps_get64();
			leng -= 16;
//			printf("continueid: %"PRIu64" ; fleng: %"PRIu64"\n",continueid,fleng);
			sumlength += fleng;
			while (leng>0) {
				chunkid   = ps_get64();
				chunksize = ps_get32();
				copies    = ps_get8();
				leng -= 13;
//				printf("chunk: %016"PRIX64" ; chunksize: %"PRIu32" ; copies:%"PRIu8"\n",chunkid,chunksize,copies);
				if (sc_chunkset_add(chunkid)) {
					chunk_size_cnt  += chunksize;
					chunk_rsize_cnt += copies * chunksize;
				}
			}
		} else if (anstype==3 && (leng-16)%13==0) { // file (ver >= 4.0)
//			printf("file\n");
			if (continueid==0) {
				filenode_cnt++;
			}
			continueid = ps_get64();
			fleng      = ps_get64();
			leng -= 16;
//			printf("continueid: %"PRIu64" ; fleng: %"PRIu64"\n",continueid,fleng);
			sumlength += fleng;
			while (leng>0) {
				chunkid   = ps_get64();
				chunksize = ps_get32();
				eights    = ps_get8();
				leng -= 13;
//				printf("chunk: %016"PRIX64" ; chunksize: %"PRIu32" ; copies:%"PRIu8"\n",chunkid,chunksize,copies);
				if (sc_chunkset_add(chunkid)) {
					chunk_size_cnt  += chunksize;
					chunk_rsize_cnt += (eights * chunksize) / 8;
				}
			}
		} else {
			continueid = 0;
		}

	} while (continueid!=0);
//OK:
	return 0;

error:
	return -1;
}

static const char* ecid_to_str(uint8_t ecid) {
	const char* ecid8names[17] = {
		"DE0","DE1","DE2","DE3","DE4","DE5","DE6","DE7",
		"CE0","CE1","CE2","CE3","CE4","CE5","CE6","CE7","CE8"
	};
	const char* ecid4names[13] = {
		"DF0","DF1","DF2","DF3",
		"CF0","CF1","CF2","CF3","CF4","CF5","CF6","CF7","CF8"
	};

	if (ecid&0x20) {
		if ((ecid&0x1F)<17) {
			return ecid8names[ecid&0x1F];
		}
	} else if (ecid&0x10) {
		if ((ecid&0xF)<13) {
			return ecid4names[ecid&0xF];
		}
	} else if (ecid==0) {
		return "COPY";
	}
	return "\?\?\?";
}

typedef struct _chunk_part {
	char csstrip[STRIPSIZE];
	uint32_t csip;
	uint16_t csport;
	uint8_t status;
	uint8_t ecid;
	int8_t validchecksum;
	uint16_t blocks;
	uint8_t crcblock[4096];
	char *fpath;
} chunk_part;

int chunk_part_cmp(const void *a,const void *b) {
	const chunk_part *aa = (const chunk_part*)a;
	const chunk_part *bb = (const chunk_part*)b;
	if (aa->ecid<bb->ecid) {
		return -1;
	} else if (aa->ecid>bb->ecid) {
		return 1;
	} else if (aa->csip<bb->csip) {
		return -1;
	} else if (aa->csip>bb->csip) {
		return 1;
	} else if (aa->csport<bb->csport) {
		return -1;
	} else if (aa->csport>bb->csport) {
		return 1;
	} else if (aa->status<bb->status) {
		return -1;
	} else if (aa->status>bb->status) {
		return 1;
	} else {
		return 0;
	}
}

#define ERROR_INVALID 1
#define ERROR_MISSING 2
#define ERROR_RECOVERABLEEC 4
#define ERROR_CHECKSUM 8

int chunk_parts_check (chunk_part *cptab,uint32_t copies) {
	uint32_t i;
	uint32_t ec4mask,ec8mask,mask;
	uint32_t ec4count,ec8count;
	uint32_t vc;
	uint32_t firstvalidcopy;
	uint8_t ecid;
	int result;
	ec4mask = 0;
	ec8mask = 0;
	ec4count = 0;
	ec8count = 0;
	vc = 0;
	firstvalidcopy = 0xFFFFFFFF;
	result = 0;
	for (i=0 ; i<copies ; i++) {
		if (cptab[i].status!=CHECK_VALID && cptab[i].status!=CHECK_MARKEDFORREMOVAL) {
			result |= ERROR_INVALID;
		} else {
			ecid = cptab[i].ecid;
			if (ecid&0x20) {
				if ((ecid&0x1F)<17) {
					mask = 1U<<(ecid&0x1F);
					if ((mask & ec8mask) == 0) {
						ec8mask |= mask;
						ec8count++;
					}
				}
			} else if (ecid&0x10) {
				if ((ecid&0xF)<13) {
					mask = 1U<<(ecid&0xF);
					if ((mask & ec4mask) == 0) {
						ec4mask |= mask;
						ec4count++;
					}
				}
			} else if (ecid==0) {
				if (cptab[i].validchecksum>0) {
					if (firstvalidcopy==0xFFFFFFFF) {
						firstvalidcopy=i;
					} else {
						if (cptab[i].blocks != cptab[firstvalidcopy].blocks) {
							result |= ERROR_CHECKSUM;
						}
						if (memcmp(cptab[i].crcblock,cptab[firstvalidcopy].crcblock,4*cptab[i].blocks)!=0) {
							result |= ERROR_CHECKSUM;
						}
					}
				}
				vc++;
			}
		}
	}
	if (vc==0) {
		if (ec4count<4 && ec8count<8) {
			result |= ERROR_MISSING;
		} else if ((ec8count>=8 && (ec8mask&0xFF)!=0xFF) || (ec4count>=4 && (ec4mask&0xF)!=0xF)) {
			result |= ERROR_RECOVERABLEEC;
		}
	}
	return result;
}

int16_t chunk_parts_calcsignature_block(chunk_part *cptab,uint32_t copies,uint8_t crcblock[4096]) {
	uint32_t i,j;
	uint32_t ec4mask,ec8mask,mask;
	uint16_t partblocks;
	uint8_t ecid;
	uint16_t blocks;

	ec4mask = 0;
	ec8mask = 0;

	for (i=0 ; i<copies ; i++) {
		if (cptab[i].validchecksum>0 && (cptab[i].status==CHECK_VALID || cptab[i].status==CHECK_MARKEDFORREMOVAL)) {
			ecid = cptab[i].ecid;
			if (ecid&0x20 && (ecid&0x1F)<8) {
				mask = 1U<<(ecid&0x1F);
				ec8mask |= mask;
			} else if (ecid&0x10 && (ecid&0xF)<4) {
				mask = 1U<<(ecid&0xF);
				ec4mask |= mask;
			} else if (ecid==0) {
				memcpy(crcblock,cptab[i].crcblock,cptab[i].blocks*4);
				return cptab[i].blocks*4;
			}
		}
	}
	if ((ec8mask&0xFF)==0xFF) {
		blocks = 0;
		ec8mask = 0;
		for (i=0 ; i<copies ; i++) {
			if (cptab[i].validchecksum>0 && (cptab[i].status==CHECK_VALID || cptab[i].status==CHECK_MARKEDFORREMOVAL)) {
				ecid = cptab[i].ecid;
				if ((ecid&0x20) && ((ecid&0x1F)<8)) {
					ecid &= 0x1F;
					mask = 1U<<ecid;
					if ((mask & ec8mask) == 0) {
						ec8mask |= mask;
						for (j=0 ; j<cptab[i].blocks ; j+=4) {
							partblocks = cptab[i].blocks - j;
							if (partblocks>4) {
								partblocks=4;
							}
							memcpy(crcblock+4*((j*2+ecid)*4),cptab[i].crcblock+4*j,4*partblocks);
							if (((j*2+ecid)*4)+partblocks > blocks) {
								blocks = ((j*2+ecid)*4)+partblocks;
							}
						}
					}
				}
			}
		}
		return blocks*4;
	}
	if ((ec4mask&0xF)==0xF) {
		blocks = 0;
		ec4mask = 0;
		for (i=0 ; i<copies ; i++) {
			if (cptab[i].validchecksum>0 && (cptab[i].status==CHECK_VALID || cptab[i].status==CHECK_MARKEDFORREMOVAL)) {
				ecid = cptab[i].ecid;
				if ((ecid&0x10) && ((ecid&0xF)<4)) {
					ecid &= 0xF;
					mask = 1U<<ecid;
					if ((mask & ec4mask) == 0) {
						ec4mask |= mask;
						for (j=0 ; j<cptab[i].blocks ; j+=4) {
							partblocks = cptab[i].blocks - j;
							if (partblocks>4) {
								partblocks=4;
							}
							memcpy(crcblock+4*((j+ecid)*4),cptab[i].crcblock+4*j,4*partblocks);
							if (((j+ecid)*4)+partblocks > blocks) {
								blocks = ((j+ecid)*4)+partblocks;
							}
						}
					}
				}
			}
		}
		return blocks*4;
	}
	return -1;
}

void chunk_parts_print(chunk_part *cptab,uint32_t copies,uint8_t fileinfomode) {
	uint32_t copy;
	uint32_t vcopies;
	md5ctx chunkctx;
	uint8_t currentdigest[16];
	char strdigest[33];
	char *strtype;

	vcopies = 0;
	for (copy=0 ; copy<copies ; copy++) {
		switch (cptab[copy].status) {
			case CHECK_VALID:
				strtype = "VALID";
				break;
			case CHECK_MARKEDFORREMOVAL:
				strtype = "MARKED FOR REMOVAL";
				break;
			case CHECK_WRONGVERSION:
				strtype = "WRONG VERSION";
				break;
			case CHECK_WV_AND_MFR:
				strtype = "WRONG VERSION , MARKED FOR REMOVAL";
				break;
			case CHECK_INVALID:
				strtype = "INVALID";
				break;
			default:
				strtype = "???";
		}
		if (cptab[copy].ecid==0) {
			vcopies++;
			printf("\t\tcopy %"PRIu32": %s:%"PRIu16" ; status:%s", vcopies, cptab[copy].csstrip, cptab[copy].csport, strtype);
		} else {
			printf("\t\tEC part %s: %s:%"PRIu16" ; status:%s", ecid_to_str(cptab[copy].ecid), cptab[copy].csstrip, cptab[copy].csport, strtype);
		}
		if (fileinfomode&(FILEINFO_CRC|FILEINFO_PATH)) {
			if (cptab[copy].validchecksum>0) {
				if (fileinfomode&FILEINFO_CRC) {
					md5_init(&chunkctx);
					md5_update(&chunkctx, cptab[copy].crcblock, 4*cptab[copy].blocks);
					md5_final(currentdigest, &chunkctx);
					digest_to_str(strdigest, currentdigest);
					printf(" ; blocks: %u ; checksum digest: %s", cptab[copy].blocks, strdigest);
				}
				if (fileinfomode&FILEINFO_PATH) {
					if (cptab[copy].fpath!=NULL) {
						printf(" ; path: %s",cptab[copy].fpath);
					} else {
						printf(" ; can't get path");
					}
				}
			} else {
				if (fileinfomode&FILEINFO_CRC) {
					printf(" ; can't get checksum");
				}
				if (fileinfomode&FILEINFO_PATH) {
					printf(" ; can't get path");
				}
			}
		}
		printf("\n");
	}
}

int file_info(uint8_t fileinfomode,const char *fname) {
	uint32_t indx,inode,version,reqversion;
	int32_t leng, tmp32, basesize;
	uint32_t chunks,copies,copy;
	uint8_t fcopies,ec8parts,ec4parts,ec8rlevel,ec4rlevel;
	uint8_t rlevel;
	uint16_t sstatus;
	uint32_t csip;
	uint64_t chunkid;
	uint64_t fleng;
	md5ctx filectx;
	uint8_t signatureblock[4096];
	int16_t signaturebytes;
	uint8_t currentdigest[16];
	uint8_t errcode;
	char strdigest[33];
	chunk_part *cptab;
	uint32_t cptab_size;
	uint32_t wrongchunks;
	uint32_t chunkmtime;
	uint8_t chunkstatus;
	uint8_t qmode;
	uint8_t nsep;

	cptab = NULL;
	cptab_size = 0;
	wrongchunks = 0;
	if (open_master_conn(fname,&inode,NULL,&fleng,0,0)<0) {
		return -1;
	}
	if(getmasterversion()>=VERSION2INT(4,0,0)){
		if (fileinfomode&FILEINFO_QUICK) {
			ps_comminit();
			if (getmasterversion()>=VERSION2INT(4,36,0)) {
				qmode = 1; // packet mode 3
			} else {
				qmode = 0; // packet mode 2
			}
			ps_put8(qmode+2);				//mode
			ps_put32(inode);

			if ((leng = ps_send_and_receive(CLTOMA_FUSE_CHECK, MATOCL_FUSE_CHECK, fname)) < 0) {
				goto error;
			}
			if (leng==1) {
				fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
				goto error;
			} else if (qmode==0 && leng%5 != 0) {
				fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
				goto error;
			} else if (qmode==1 && leng%6 != 0) {
				fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
				goto error;
			}


			printf("%s:\n",fname);
			for (tmp32=0 ; tmp32<leng ; tmp32 += (5+qmode)) {
				if (qmode) {
					sstatus = ps_get16();
				} else {
					sstatus = ps_get8();
				}
				chunks = ps_get32();
				if (sstatus==0) {
					printf("chunks not found:");
				} else if (sstatus==1) {
					printf("empty (zero) chunks:");
				} else if (sstatus<2774) {
					fcopies = (sstatus - 2) % 11;
					ec8parts = ((sstatus - 2) % 198) / 11;
					ec4parts = (sstatus - 2) / 198;
					ec8rlevel = (ec8parts<8) ? 0 : (ec8parts-8);
					ec4rlevel = (ec4parts<4) ? 0 : (ec4parts-4);
					rlevel = (fcopies<1) ? 0 : (fcopies-1);
					if (ec8rlevel>rlevel) {
						rlevel = ec8rlevel;
					}
					if (ec4rlevel>rlevel) {
						rlevel = ec4rlevel;
					}
					nsep = 0;
					if (fcopies>0) {
						if (fcopies==1) {
							printf("1 full copy");
						} else {
							printf("%"PRIu8" full copies",fcopies);
						}
						nsep = 1;
					}
					if (ec8parts>0) {
						if (nsep) {
							printf(" ; ");
						}
						if (ec8parts==1) {
							printf("1 EC8 part");
						} else {
							printf("%"PRIu8" EC8 parts",ec8parts);
						}
						nsep = 1;
					}
					if (ec4parts>0) {
						if (nsep) {
							printf(" ; ");
						}
						if (ec4parts==1) {
							printf("1 EC4 part");
						} else {
							printf("%"PRIu8" EC4 parts",ec4parts);
						}
						nsep = 1;
					}
					if (nsep) {
						printf(" (redundancy level: %"PRIu8"):",rlevel);
					} else {
						printf("no copies and EC parts :");
					}
					print_number_desc(" ","\n",chunks,PNUM_32BIT);
				}
			}
		} else { //=== if (!(fileinfomode&FILEINFO_QUICK))
			if (fileinfomode&FILEINFO_SIGNATURE) {
				md5_init(&filectx);
			}
			if (getmasterversion()>=VERSION2INT(4,47,0)) {
				qmode = 4;
				basesize = 16;
			} else {
				qmode = 1;
				basesize = 12;
			}
			printf("%s:\n",fname);
			indx = 0;
			while (1) {
				ps_comminit();
				ps_put8(qmode);
				ps_put32(inode);
				ps_put32(indx);

				if ((leng = ps_send_and_receive(CLTOMA_FUSE_CHECK, MATOCL_FUSE_CHECK, fname)) < 0) {
					goto error;
				}

				if (leng==1) {
					if ((errcode=ps_get8())==MFS_ERROR_NOCHUNK) {
						if (indx==0) {
							printf("\tno chunks - empty file\n");
						}
						break;
					}
					fprintf(stderr,"%s [%"PRIu32"]: %s\n", fname, indx, mfsstrerr(errcode));
					goto error;
				} else {
					if (leng<basesize || ((leng-basesize)%8)!=0) {
						fprintf(stderr,"%s [%"PRIu32"]: master query: wrong answer (leng)\n",fname,indx);
						goto error;
					}
					copies = (leng-basesize)/8;
					if (copies>cptab_size) {
						cptab_size = (copies|15)+1;
						if (cptab!=NULL) {
							free(cptab);
						}
						cptab = malloc(sizeof(chunk_part)*cptab_size);
						if (cptab==NULL) {
							fprintf(stderr,"out of memory !!!\n");
							goto error;
						}
					}
					chunkid = ps_get64();
					version = ps_get32();
					if (qmode>=4) {
						chunkmtime = ps_get32();
					} else {
						chunkmtime = 0;
					}
					if (chunkid==0 && version==0) {
						if ((fileinfomode&FILEINFO_WRONGCHUNKS)==0) {
							printf("\tchunk %"PRIu32": empty\n",indx);
						}
					} else {
						for (copy=0 ; copy<copies ; copy++) {
							csip = ps_get32();
							univmakestrip(cptab[copy].csstrip,csip);
							cptab[copy].csip = csip;
							cptab[copy].csport = ps_get16();
							cptab[copy].status = ps_get8();
							cptab[copy].ecid = ps_get8();
							cptab[copy].blocks = 0;
							memset(cptab[copy].crcblock,0,1024*sizeof(uint32_t));
							if (cptab[copy].fpath!=NULL) {
								free(cptab[copy].fpath);
							}
							cptab[copy].fpath = NULL;
							if (fileinfomode&(FILEINFO_CRC|FILEINFO_SIGNATURE|FILEINFO_PATH)) {
								reqversion = version & 0x7FFFFFFF;
								if (cptab[copy].status==CHECK_WRONGVERSION || cptab[copy].status==CHECK_WV_AND_MFR) {
									reqversion = 0;
								}
								if (get_chunk_info(cptab[copy].csstrip, cptab[copy].csip, cptab[copy].csport, cptab[copy].ecid, chunkid, reqversion, cptab[copy].crcblock, &(cptab[copy].blocks), &(cptab[copy].fpath))==0) {
									cptab[copy].validchecksum = 2;
								} else if (get_checksum_block(cptab[copy].csstrip, cptab[copy].csip, cptab[copy].csport, cptab[copy].ecid, chunkid, reqversion, cptab[copy].crcblock, &(cptab[copy].blocks))==0) {
									cptab[copy].validchecksum = 1;
								} else {
									cptab[copy].validchecksum = 0;
								}
							} else {
								cptab[copy].validchecksum = -1;
							}
						}
						chunkstatus = chunk_parts_check(cptab,copies);
						if ((fileinfomode&FILEINFO_WRONGCHUNKS)==0 || chunkstatus) {
							wrongchunks++;
							if (qmode>=4) {
								struct tm lt;
								time_t mtime;
								char asctimebuff[100];
								if (chunkmtime>0) {
									mtime = chunkmtime;
									localtime_r(&mtime,&lt);
									strftime(asctimebuff,100,"%Y-%m-%d %H:%M:%S",&lt);
								} else {
									strcpy(asctimebuff,"old chunk - not set");
								}
								printf("\tchunk %"PRIu32": %016"PRIX64"_%08"PRIX32" / (id:%"PRIu64" ver:%"PRIu32") ; mtime:%"PRIu32" (%s)\n",indx, chunkid, (version & 0x7FFFFFFF), chunkid, (version & 0x7FFFFFFF), chunkmtime, asctimebuff);
							} else {
								printf("\tchunk %"PRIu32": %016"PRIX64"_%08"PRIX32" / (id:%"PRIu64" ver:%"PRIu32")\n",indx, chunkid, (version & 0x7FFFFFFF), chunkid, (version & 0x7FFFFFFF));
							}
							if (copies>0) {
								qsort(cptab,copies,sizeof(chunk_part),chunk_part_cmp);
							}
							chunk_parts_print(cptab,copies,fileinfomode);
							if (chunkstatus&ERROR_CHECKSUM) {
								printf("\t\tfull copies have different checksums !!!\n");
							}
							if (chunkstatus&ERROR_MISSING) {
								printf("\t\tmissing data copies / data parts - unrecoverable\n");
							}
							if (chunkstatus&ERROR_RECOVERABLEEC) {
								printf("\t\tdata parts missing - recoverable\n");
							}
							if (version & 0x80000000) {
								printf("\t\tchunk marked as readable after repair - reading this chunk will receive zeros\n");
							}
						}
						if (fileinfomode&FILEINFO_SIGNATURE) {
							signaturebytes = chunk_parts_calcsignature_block(cptab,copies,signatureblock);
							if (signaturebytes<0) {
								printf("\t\tcouldn't add this chunk to signature !!!\n");
							} else {
								md5_update(&filectx, signatureblock, signaturebytes);
							}
						}
					}
				}
				indx++;
			}
			if ((fileinfomode&FILEINFO_WRONGCHUNKS) && wrongchunks==0 && indx>0) {
				printf("\tall chunks are ok\n");
			}
			if (fileinfomode&FILEINFO_SIGNATURE) {
				md5_final(currentdigest, &filectx);
				digest_to_str(strdigest, currentdigest);
				printf("%s signature: %s\n",fname,strdigest);
			}
		}
	} else { //masterversion < 4.0.0 			ps_comminit(); ...
		uint32_t fchunks;
		int32_t cmd;
		uint8_t fchunksvalid;
		uint8_t protover;

		if (fileinfomode&FILEINFO_PATH) {
			printf("chunk file path info is only supported in MFS 4+");
			goto error;
		}

		ps_comminit();
		ps_put32(inode);

		if ((leng = ps_send_and_receive(CLTOMA_FUSE_CHECK, MATOCL_FUSE_CHECK, fname)) < 0) {
			goto error;
		}

		if (leng==1) {
			fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
			goto error;
		} else if (leng!=44 && leng!=48) {
			fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
			goto error;
		}

		if (fileinfomode&FILEINFO_QUICK) {
			printf("%s:\n",fname);
		}
		fchunks = 0;
		fchunksvalid = 0;
		if (leng%3==0 && leng<=33) {
			for (cmd=0 ; cmd<leng ; cmd+=3) {//reuse cmd
				copies = ps_get8();
				chunks = ps_get16();
				if (fileinfomode&FILEINFO_QUICK) {
					if (copies==1) {
						printf("1 copy:");
					} else {
						printf("%"PRIu32" copies:",copies);
					}
					print_number_desc(" ","\n",chunks,PNUM_32BIT);
				}
				fchunks += chunks;
			}
		} else {
			for (cmd=0 ; cmd<11 ; cmd++) {
				chunks = ps_get32();
				if (chunks>0 && (fileinfomode&FILEINFO_QUICK)) {
					if (cmd==1) {
						printf(" chunks with 1 copy:    ");
					} else if (cmd>=10) {
						printf(" chunks with 10+ copies:");
					} else {
						printf(" chunks with %d copies:  ",cmd);
					}
					print_number_desc(" ","\n",chunks,PNUM_32BIT);
				}
				fchunks += chunks;
			}
			if (leng==48) {
				chunks = ps_get32();
				if (chunks>0 && (fileinfomode&FILEINFO_QUICK)) {
					printf(" empty (zero) chunks:   ");
					print_number_desc(" ","\n",chunks,PNUM_32BIT);
				}
				fchunks += chunks;
				fchunksvalid = 1;
			}
		}

//======================================================

		if ((fileinfomode&FILEINFO_QUICK)==0) {
			if (fchunksvalid==0) { // in this case fchunks doesn't include 'empty' chunks, so use file size to fix 'fchunks' if necessary
				if (fchunks < ((fleng+MFSCHUNKMASK)>>MFSCHUNKBITS)) {
					fchunks = ((fleng+MFSCHUNKMASK)>>MFSCHUNKBITS);
				}
			}
		//	printf("masterversion: %08X\n",masterversion);
			if (fileinfomode&FILEINFO_SIGNATURE) {
				md5_init(&filectx);
			}
			printf("%s:\n",fname);
			if (fchunks==0) {
				printf("\tno chunks - empty file\n");
			}
			for (indx=0 ; indx<fchunks ; indx++) {
				if (getmasterversion()<VERSION2INT(3,0,26)) {
					ps_comminit();
					ps_put32(inode);
					ps_put32(indx);
					if (getmasterversion()>VERSION2INT(3,0,3)) {
						ps_put8(0); // canmodatime
					}
					if ((leng = ps_send_and_receive(CLTOMA_FUSE_READ_CHUNK, MATOCL_FUSE_READ_CHUNK, fname)) < 0) {
						goto error;
					}
					if (leng==1) {
						fprintf(stderr,"%s [%"PRIu32"]: %s\n",fname,indx,mfsstrerr(ps_get8()));
						goto error;
					} else if (leng&1) {
						protover = ps_get8();
						if (protover!=1 && protover!=2) {
							fprintf(stderr,"%s [%"PRIu32"]: master query: unknown protocol id (%"PRIu8")\n",fname,indx,protover);
							goto error;
						}
						if (leng<21 || (protover==1 && ((leng-21)%10)!=0) || (protover==2 && ((leng-21)%14)!=0)) {
							fprintf(stderr,"%s [%"PRIu32"]: master query: wrong answer (leng)\n",fname,indx);
							goto error;
						}
					} else {
						if (leng<20 || ((leng-20)%6)!=0) {
							fprintf(stderr,"%s [%"PRIu32"]: master query: wrong answer (leng)\n",fname,indx);
							goto error;
						}
						protover = 0;
					}
					ps_dummyget(8);//rptr += 8; // fleng
					if (protover==2) {
						copies = (leng-21)/14;
					} else if (protover==1) {
						copies = (leng-21)/10;
					} else {
						copies = (leng-20)/6;
					}
				} else {//MV>=3.0.26
					ps_comminit();
					ps_put32(inode);
					ps_put32(indx);
					if ((leng = ps_send_and_receive(CLTOMA_FUSE_CHECK, MATOCL_FUSE_CHECK, fname)) < 0) {
						goto error;
					}
					if (leng==1) {
						fprintf(stderr,"%s [%"PRIu32"]: %s\n", fname, indx, mfsstrerr(ps_get8()));
						goto error;
					} else {
						if (leng<12 || ((leng-12)%7)!=0) {
							fprintf(stderr,"%s [%"PRIu32"]: master query: wrong answer (leng)\n", fname, indx);
							goto error;
						}
						protover = 255;
						copies = (leng-12)/7;
					}
				}
				if (copies>cptab_size) {
					cptab_size = (copies|15)+1;
					if (cptab!=NULL) {
						free(cptab);
					}
					cptab = malloc(sizeof(chunk_part)*cptab_size);
					if (cptab==NULL) {
						fprintf(stderr,"out of memory !!!\n");
						goto error;
					}
				}
				chunkid = ps_get64();
				version = ps_get32();


		//---------------------


				if (chunkid==0 && version==0) {
					if ((fileinfomode&FILEINFO_WRONGCHUNKS)==0) {
						printf("\tchunk %"PRIu32": empty\n",indx);
					}
				} else {
					for (copy=0 ; copy<copies ; copy++) {
						csip = ps_get32();
						univmakestrip(cptab[copy].csstrip,csip);
						cptab[copy].csip = csip;
						cptab[copy].csport = ps_get16();
						if (protover==255) {
							cptab[copy].status = ps_get8();
						} else if (protover==2) {
							cptab[copy].status = CHECK_VALID;
							ps_dummyget(8);//rptr+=8;
						} else if (protover==1) {
							cptab[copy].status = CHECK_VALID;
							ps_dummyget(4);//rptr+=4;
						} else {
							cptab[copy].status = CHECK_VALID;
						}
						cptab[copy].ecid = 0;
						cptab[copy].blocks = 0;
						memset(cptab[copy].crcblock,0,1024*sizeof(uint32_t));
						if (cptab[copy].fpath!=NULL) {
							free(cptab[copy].fpath);
						}
						cptab[copy].fpath = NULL;
						if (fileinfomode&(FILEINFO_CRC|FILEINFO_SIGNATURE)) {
							reqversion = version & 0x7FFFFFFF;
							if (cptab[copy].status==CHECK_WRONGVERSION || cptab[copy].status==CHECK_WV_AND_MFR) {
								reqversion = 0;
							}
							if (get_checksum_block(cptab[copy].csstrip, cptab[copy].csip, cptab[copy].csport, cptab[copy].ecid, chunkid, reqversion, cptab[copy].crcblock, &(cptab[copy].blocks))==0) {
								cptab[copy].validchecksum = 1;
							} else {
								cptab[copy].validchecksum = 0;
							}
						} else {
							cptab[copy].validchecksum = -1;
						}
					}
					chunkstatus = chunk_parts_check(cptab,copies);
					if ((fileinfomode&FILEINFO_WRONGCHUNKS)==0 || chunkstatus) {
						wrongchunks++;
						printf("\tchunk %"PRIu32": %016"PRIX64"_%08"PRIX32" / (id:%"PRIu64" ver:%"PRIu32")\n",indx,chunkid,version,chunkid,version);
						if (copies>0) {
							qsort(cptab,copies,sizeof(chunk_part),chunk_part_cmp);
						}
						chunk_parts_print(cptab,copies,fileinfomode);
						if (chunkstatus&ERROR_CHECKSUM) {
							printf("\t\tcopies have different checksums !!!\n");
						}
						if (chunkstatus&ERROR_MISSING) {
							printf("\t\tno valid copies !!!\n");
						}
					}
					if (fileinfomode&FILEINFO_SIGNATURE) {
						signaturebytes = chunk_parts_calcsignature_block(cptab,copies,signatureblock);
						if (signaturebytes<0) {
							printf("\t\tcouldn't add this chunk to signature !!!\n");
						} else {
							md5_update(&filectx, signatureblock, signaturebytes);
						}
					}
				}
			}
			if ((fileinfomode&FILEINFO_WRONGCHUNKS) && wrongchunks==0 && fchunks>0) {
				printf("\tall chunks are ok\n");
			}
			if (fileinfomode&FILEINFO_SIGNATURE) {
				md5_final(currentdigest, &filectx);
				digest_to_str(strdigest, currentdigest);
				printf("%s signature: %s\n",fname,strdigest);
			}
		}




	}//end-masterversion<4.0.0
//OK:
	if (cptab!=NULL) {
		free(cptab);
	}
	return 0;

error:
	if (cptab!=NULL) {
		free(cptab);
	}
	reset_master();
	return -1;
}

int dir_info(uint8_t dirinfomode,const char *fname) {
	uint32_t inode;
	int32_t	leng;
	uint32_t inodes,dirs,files,chunks;
	uint64_t length64,size,realsize;

	if (open_master_conn(fname,&inode,NULL,NULL,0,0)<0) {
		return -1;
	}

	ps_comminit();
	ps_put32(inode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETDIRSTATS, MATOCL_FUSE_GETDIRSTATS, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	} else if (leng!=56 && leng!=40) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n",fname);
		goto error;
	}

	inodes = ps_get32();
	dirs   = ps_get32();
	files  = ps_get32();
	if (leng==56) {
		ps_dummyget(8);
	}
	chunks = ps_get32();
	if (leng==56) {
		ps_dummyget(8);
	}
	length64 = ps_get64();
	size     = ps_get64();
	realsize = ps_get64();

	if (dirinfomode==0 || dirinfomode==DIRINFO_PRECISE) {
		printf("%s:\n",fname);
		print_number_desc(" inodes:       ","\n", inodes, PNUM_NONE);
		print_number_desc("  directories: ","\n", dirs, PNUM_NONE);
		print_number_desc("  files:       ","\n", files, PNUM_NONE);
		print_number_desc(" chunks:       ","\n", chunks, PNUM_NONE);
		print_number_desc(" length:       ","\n", length64, PNUM_BYTES);
		print_number_desc(" size:         ","\n", size, PNUM_BYTES);
		print_number_desc(" realsize:     ","\n", realsize, PNUM_BYTES);
	} else {
		if (dirinfomode&DIRINFO_INODES) {
			print_number(inodes,PNUM_NONE);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_DIRS) {
			print_number(dirs,PNUM_NONE);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_FILES) {
			print_number(files,PNUM_NONE);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_CHUNKS) {
			print_number(chunks,PNUM_NONE);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_LENGTH) {
			print_number(length64,PNUM_BYTES);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_SIZE) {
			print_number(size,PNUM_BYTES);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_REALSIZE) {
			print_number(realsize,PNUM_BYTES);
			printf("\t");
		}
		printf("%s\n",fname);
	}
	if (dirinfomode&DIRINFO_PRECISE) {
		uint16_t progress;
		double seconds,lseconds;
		uint8_t err;

		if (getmasterversion()<VERSION2INT(3,0,73)) {
			fprintf(stderr,"precise data calculation needs master at least in version 3.0.73 - upgrade your unit\n");
			goto error;
		}
		err = 0;
		qhead = NULL;
		qtail = &qhead;
		dirnode_cnt = 0;
		filenode_cnt = 0;
		touched_inodes = 0;
		sumlength = 0;
		chunk_size_cnt = 0;
		chunk_rsize_cnt = 0;
		lseconds = monotonic_seconds();

		sc_inoset_init(inodes);
		sc_chunkset_init(chunks);
		sc_enqueue(inode);
		while (sc_dequeue(&inode)) {
			if (err==0) {
				if (sc_node_info(fname,inode)<0) {
					err = 1;
				}
				progress = ((uint64_t)touched_inodes * 10000ULL) / (uint64_t)inodes;
				if (progress>9999) {
					progress=9999;
				}
				seconds = monotonic_seconds();
				if (lseconds+0.1<seconds) {
					lseconds = seconds;
					printf("\r%2u.%02u%% complete ",(unsigned int)(progress/100),(unsigned int)(progress%100));fflush(stdout);
				}
			}
		}
		printf("\r");
		if (err==0) {
			if (dirinfomode==DIRINFO_PRECISE) {
				printf("%s (precise data):\n",fname);
				print_number_desc(" inodes:       ","\n",inohashelems,PNUM_NONE);
				print_number_desc("  directories: ","\n",dirnode_cnt,PNUM_NONE);
				print_number_desc("  files:       ","\n",filenode_cnt,PNUM_NONE);
				print_number_desc(" chunks:       ","\n",chunkhashelems,PNUM_NONE);
				print_number_desc(" length:       ","\n",sumlength,PNUM_BYTES);
				print_number_desc(" size:         ","\n",chunk_size_cnt,PNUM_BYTES);
				print_number_desc(" realsize:     ","\n",chunk_rsize_cnt,PNUM_BYTES);
			} else {
				if (dirinfomode&DIRINFO_INODES) {
					print_number(inohashelems,PNUM_NONE);
					printf("\t");
				}
				if (dirinfomode&DIRINFO_DIRS) {
					print_number(dirnode_cnt,PNUM_NONE);
					printf("\t");
				}
				if (dirinfomode&DIRINFO_FILES) {
					print_number(filenode_cnt,PNUM_NONE);
					printf("\t");
				}
				if (dirinfomode&DIRINFO_CHUNKS) {
					print_number(chunkhashelems,PNUM_NONE);
					printf("\t");
				}
				if (dirinfomode&DIRINFO_LENGTH) {
					print_number(sumlength,PNUM_BYTES);
					printf("\t");
				}
				if (dirinfomode&DIRINFO_SIZE) {
					print_number(chunk_size_cnt,PNUM_BYTES);
					printf("\t");
				}
				if (dirinfomode&DIRINFO_REALSIZE) {
					print_number(chunk_rsize_cnt,PNUM_BYTES);
					printf("\t");
				}
				printf("%s (precise data)\n",fname);
			}
		}
		sc_inoset_cleanup();
		sc_chunkset_cleanup();
	}
//ok:
	return 0;

error:
	reset_master();
	return -1;
}

int file_paths(const char* fname) {
	const char *p;
	struct stat st;
	char cwdbuff[MAXPATHLEN];
	uint32_t arginode;
	uint32_t inode;
	int32_t leng;
	uint32_t pleng;
	int conn_status;

	p = fname;
	while (*p>='0' && *p<='9') {
		p++;
	}

	if (*p=='\0' && stat(fname,&st)<0 && errno==ENOENT) {
		arginode = strtoul(fname,NULL,10);
		p = getcwd(cwdbuff,MAXPATHLEN);
		conn_status = open_master_conn(p,&inode,NULL,NULL,0,0);
		inode = arginode;
	} else {
		conn_status = open_master_conn(fname,&inode,NULL,NULL,0,0);
	}
	if (conn_status<0) {
		return -1;
	}

	ps_comminit();
	ps_put32(inode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_PATHS, MATOCL_FUSE_PATHS, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n",fname,mfsstrerr(ps_get8()));
		goto error;
	}
	printf("%s:\n",fname);
	while (leng>=4) {
		pleng = ps_get32();
		leng -= 4;
		if ((uint32_t)leng>=pleng) {
			while (pleng) {
				putchar(ps_get8());
				pleng--;
				leng--;
			}
			putchar('\n');
		} else {
			leng=0;
		}
	}
//OK;
	return 0;

error:
	reset_master();
	return -1;
}

int file_repair(const char *fname,uint8_t flags) {
	uint32_t inode,uid,gid;
	int32_t leng;
	gid_t *grouplist;
	uint32_t i,gids;
	uint8_t addmaingroup;
	uint32_t notchanged,erased,repaired;

	if (open_master_conn(fname,&inode,NULL,NULL,0,1)<0) {
		return -1;
	}

	grouplist = malloc(sizeof(gid_t)*NGROUPS_MAX);

	if (getmasterversion()<VERSION2INT(4,27,0) && flags!=0) {
		fprintf(stderr,"flags are not supported in this version of master\n");
		goto error;
	}

	uid = getuid();
	gid = getgid();
	if (getmasterversion()>=VERSION2INT(2,0,0)) {
		gids = getgroups(NGROUPS_MAX,grouplist);
		addmaingroup = 1;
		for (i=0 ; i<gids ; i++) {
			if (grouplist[i]==gid) {
				addmaingroup = 0;
			}
		}
	} else {
		gids = 0;
		addmaingroup = 0;
	}

	ps_comminit();
	ps_put32(inode);
	ps_put32(uid);
	if (getmasterversion()<VERSION2INT(2,0,0)) {
		ps_put32(gid);
	} else {
		ps_put32(addmaingroup+gids);
		if (addmaingroup) {
			ps_put32(gid);
		}
		for (i=0 ; i<gids ; i++) {
			ps_put32(grouplist[i]);
		}
	}
	if (getmasterversion()>=VERSION2INT(4,27,0)) {
		ps_put8(flags);
	}

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_REPAIR, MATOCL_FUSE_REPAIR, fname)) < 0) {
		goto error;
	}

	if (leng==1) {
		fprintf(stderr,"%s: %s\n", fname, mfsstrerr(ps_get8()));
		goto error;
	} else if (leng!=12) {
		fprintf(stderr,"%s: master query: wrong answer (leng)\n", fname);
		goto error;
	}
	notchanged = ps_get32();
	erased     = ps_get32();
	repaired   = ps_get32();
	printf("%s:\n",fname);
	print_number_desc(" chunks not changed: ","\n",notchanged,PNUM_32BIT);
	print_number_desc(" chunks erased:      ","\n",erased,PNUM_32BIT);
	print_number_desc(" chunks repaired:    ","\n",repaired,PNUM_32BIT);

	free(grouplist);

//OK:
	return 0;

error:
	free(grouplist);
	reset_master();
	return -1;
}

//----------------------------------------------------------------------

static const char *checkfiletxt[] = {
	"check files",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] file [file ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	NULL
};

static const char *fileinfotxt[] = {
	"show files info (shows detailed info of each file chunk)",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-qcspw] file [file ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	" -q - quick info (show only number of valid copies)",
	" -c - receive chunk checksums from chunkservers",
	" -s - calculate file signature (using checksums)",
	" -p - return chunk file path",
	" -w - show only wrong chunks",
	NULL
};

static const char *dirinfotxt[] = {
	"show directories stats",
	"",
	"usage: "_EXENAME_" [-?] [-nhHkmg] [-idfclsr] [-p] object [object ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	" -i - show number of inodes",
	" -d - show number of directories",
	" -f - show number of files",
	" -c - show number of chunks",
	" -l - show length",
	" -s - show size",
	" -r - show realsize",
	" -p - precise mode",
	"",
	"If no 'show' switches (i,d,f,c,l,s and r) are present then show everything",
	"",
	"Meaning of some not obvious output data:",
	" 'length' is just sum of files lengths",
	" 'size' is sum of chunks lengths",
	" 'realsize' is estimated hdd usagesimple (usually size multiplied by current goal)",
	"",
	"Precise mode means that system takes into account repeated nodes/chunks",
	"and count them once, also uses current number of copies instead of goal.",
	NULL
};

static const char *filepathtxt[] = {
	"show all paths of given files or node numbers",
	"",
	"usage: "_EXENAME_" [-?] object/inode [object/inode ...]",
	"",
	_QMARKDESC_,
	"",
	"In case of converting node to path, tool has to be run in mfs-mounted directory",
	NULL
};

static const char *filerepairtxt[] = {
	"Finds best chunk copy. When no copy is available then marks chunk as read-as-zeros (write still returns error) until a valid copy is available again. If option '-d' is specified then such chunks are zeroed permanently.",
	"",
	"usage: mfsfilerepair [-?] [-nhHkmg?] [-d] file [file ...]",
	"",
	_QMARKDESC_,
	_NUMBERDESC_,
	" -d - delete unrepairable chunks permanently",
	NULL
};

void checkfileusage(void) {
	tcomm_print_help(checkfiletxt);
	exit(1);
}

void fileinfousage(void) {
	tcomm_print_help(fileinfotxt);
	exit(1);
}

void dirinfousage(void) {
	tcomm_print_help(dirinfotxt);
	exit(1);
}

void filepathusage(void) {
	tcomm_print_help(filepathtxt);
	exit(1);
}

void filerepairusage(void) {
	tcomm_print_help(filerepairtxt);
	exit(1);
}

int checkfileexe(int argc,char *argv[]) {
	int ch;
	int status = 0;

	while ((ch=getopt(argc,argv,"?nhHkmg"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			default:
				checkfileusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<1) {
		checkfileusage();
		return 1;
	}

	while (argc>0) {
		if (file_info(FILEINFO_QUICK,*argv)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int fileinfoexe(int argc,char *argv[]) {
	int ch;
	uint8_t fileinfomode = 0;
	int status = 0;

	while ((ch=getopt(argc,argv,"?nhHkmgqcspw"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			case 'q':
				fileinfomode |= FILEINFO_QUICK;
				break;
			case 'c':
				fileinfomode |= FILEINFO_CRC;
				break;
			case 's':
				fileinfomode |= FILEINFO_SIGNATURE;
				break;
			case 'p':
				fileinfomode |= FILEINFO_PATH;
				break;
			case 'w':
				fileinfomode |= FILEINFO_WRONGCHUNKS;
				break;
			default:
				fileinfousage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (fileinfomode&FILEINFO_QUICK) {
		if (fileinfomode&(FILEINFO_CRC|FILEINFO_SIGNATURE|FILEINFO_PATH)) {
			printf("\nOption '-q' is mutually exclusive with options '-c' '-p' and '-s'\n\n");
			exit(1);
		}
	}

	if (argc<1) {
		fileinfousage();
		return 1;
	}

	while (argc>0) {
		if (file_info(fileinfomode, *argv)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int dirinfoexe(int argc,char *argv[]) {
	int ch;
	uint8_t dirinfomode = 0;
	int status = 0;

	while ((ch=getopt(argc,argv,"?nhHkmgidfclsrp"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			case 'i':
				dirinfomode |= DIRINFO_INODES;
				break;
			case 'd':
				dirinfomode |= DIRINFO_DIRS;
				break;
			case 'f':
				dirinfomode |= DIRINFO_FILES;
				break;
			case 'c':
				dirinfomode |= DIRINFO_CHUNKS;
				break;
			case 'l':
				dirinfomode |= DIRINFO_LENGTH;
				break;
			case 's':
				dirinfomode |= DIRINFO_SIZE;
				break;
			case 'r':
				dirinfomode |= DIRINFO_REALSIZE;
				break;
			case 'p':
				dirinfomode |= DIRINFO_PRECISE;
				break;
			default:
				dirinfousage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<1) {
		dirinfousage();
		return 1;
	}

	while (argc>0) {
		if (dir_info(dirinfomode, *argv)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int filepathexe(int argc,char *argv[]) {
	int status = 0;

	while ((getopt(argc,argv,"?"))!=-1) {
		filepathusage();
	}
	argc -= optind;
	argv += optind;

	if (argc<1) {
		filepathusage();
		return 1;
	}

	while (argc>0) {
		if (file_paths(*argv)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

int filerepairexe(int argc,char *argv[]) {
	int ch;
	uint8_t repairflags = 0;
	int status = 0;

	while ((ch=getopt(argc,argv,"?nhHkmgd"))!=-1) {
		switch (ch) {
			case 'n':
			case 'h':
			case 'H':
			case 'k':
			case 'm':
			case 'g':
				set_hu_flags(ch);
				break;
			case 'd':
				repairflags = 1;
				break;
			default:
				filerepairusage();
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<1) {
		filerepairusage();
		return 1;
	}

	while (argc>0) {
		if (file_repair(*argv,repairflags)<0) {
			status = 1;
		}
		argc--;
		argv++;
	}

	return status;
}

static command commandlist[] = {
	{"checkfile", checkfileexe},
	{"fileinfo", fileinfoexe},
	{"dirinfo", dirinfoexe},
	{"filepaths", filepathexe},
	{"filerepair", filerepairexe},
	{NULL,NULL}
};

int main(int argc,char *argv[]) {
	return tcomm_find_and_execute(argc,argv,commandlist);
}
