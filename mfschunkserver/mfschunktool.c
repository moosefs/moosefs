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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "crc.h"
#include "datapack.h"
#include "MFSCommunication.h"

#define CHUNKCRCSIZE 4096

#define MODE_FAST 1
#define MODE_EMPTY 2
#define MODE_NAME 4
#define MODE_REPAIR 8

static inline int hdd_check_filename(const char *fname,uint64_t *chunkid,uint32_t *version) {
	uint64_t namechunkid;
	uint32_t nameversion;
	char ch;
	uint32_t i;

	if (strncmp(fname,"chunk_",6)!=0) {
		return -1;
	}
	namechunkid = 0;
	nameversion = 0;
	for (i=6 ; i<22 ; i++) {
		ch = fname[i];
		if (ch>='0' && ch<='9') {
			ch-='0';
		} else if (ch>='A' && ch<='F') {
			ch-='A'-10;
		} else {
			return -1;
		}
		namechunkid *= 16;
		namechunkid += ch;
	}
	if (fname[22]!='_') {
		return -1;
	}
	for (i=23 ; i<31 ; i++) {
		ch = fname[i];
		if (ch>='0' && ch<='9') {
			ch-='0';
		} else if (ch>='A' && ch<='F') {
			ch-='A'-10;
		} else {
			return -1;
		}
		nameversion *= 16;
		nameversion += ch;
	}
	if (strcmp(fname+31,".mfs")!=0) {
		return -1;
	}
	*chunkid = namechunkid;
	*version = nameversion;
	return 0;
}

int chunk_repair(const char *fname,uint8_t mode,uint8_t showok) {
	uint64_t namechunkid;
	uint32_t nameversion;
	char *newname;
	uint32_t i,j;
	int fd;
	uint8_t buff[MFSBLOCKSIZE];
	uint32_t crc[1024];
	uint32_t crcblock;
	uint16_t hdrsize;
	off_t s;
	const uint8_t *rp;
	uint8_t *wp;
	int ret=0;

	// check fname
	// name should be in format: ..../chunk_XXXXXXXXXXXXXXXX_YYYYYYYY.mfs
	if (mode&MODE_REPAIR) {
		fd = open(fname,O_RDWR);
	} else {
		fd = open(fname,O_RDONLY);
	}
	if (fd<0) {
		fprintf(stderr,"%s: error opening file !!!\n",fname);
		return ret | 16;
	}
	i = strlen(fname);
	if (i<35 || hdd_check_filename(fname+(i-35),&namechunkid,&nameversion)<0) {
		if (mode&MODE_NAME) {
			fprintf(stderr,"%s: wrong chunk name - try to fix it using header data\n",fname);
			if (read(fd,buff,20)!=20) {
				fprintf(stderr,"%s: error reading header !!!\n",fname);
				close(fd);
				return ret | 16;
			}
			if (memcmp(buff,MFSSIGNATURE "C 1.",7)!=0 || (buff[7]!='0' && buff[7]!='1')) {
				fprintf(stderr,"%s: wrong chunk header !!!\n",fname);
				close(fd);
				return ret | 16;
			}
			if (buff[7]=='1') { // this is chunk 1.1 - check 'empty' crc values
				mode |= MODE_EMPTY;
			}
			rp = buff+8;
			namechunkid = get64bit(&rp);
			nameversion = get32bit(&rp);
			j = i;
			while (j>0 && fname[j-1]!='/') {j--;}
			newname = malloc(j+35+1);
			if (j>0) {
				memcpy(newname,fname,j);
			}
			snprintf(newname+j,35+1,"chunk_%016"PRIX64"_%08"PRIX32".mfs",namechunkid,nameversion);
			newname[j+35] = 0;
			if (rename(fname,newname)<0) {
				fprintf(stderr,"%s->%s: rename error !!!\n",fname,newname);
				free(newname);
				close(fd);
				return ret | 16;
			}
			fprintf(stderr,"%s: changed name to: %s\n",fname,newname);
		} else {
			fprintf(stderr,"%s: wrong chunk name format !!! (skip header)\n",fname);
			ret |= 1;
		}
	} else {
		if (read(fd,buff,20)!=20) {
			fprintf(stderr,"%s: error reading header !!!\n",fname);
			close(fd);
			return ret | 16;
		}
		if (memcmp(buff,MFSSIGNATURE "C 1.",7)!=0 || (buff[7]!='0' && buff[7]!='1')) {
			fprintf(stderr,"%s: wrong chunk header !!!\n",fname);
			memcpy(buff,MFSSIGNATURE "C 1.0",8);
			ret |= 1;
		}
		if (buff[7]=='1') { // this is chunk 1.1 - check 'empty' crc values
			mode |= MODE_EMPTY;
		}
		rp = buff+8;
		wp = (uint8_t*)rp;
		if (get64bit(&rp)!=namechunkid) {
			fprintf(stderr,"%s: wrong chunk number in header !!!\n",fname);
			put64bit(&wp,namechunkid);
			ret |= 1;
		}
		wp = (uint8_t*)rp;
		if (get32bit(&rp)!=nameversion) {
			fprintf(stderr,"%s: wrong chunk version in header !!!\n",fname);
			put32bit(&wp,nameversion);
			ret |= 1;
		}
		if ((mode&MODE_REPAIR) && (ret&1)) {
			if (lseek(fd,0,SEEK_SET)!=0) {
				fprintf(stderr,"%s: error setting file pointer\n",fname);
				close(fd);
				return ret | 16;
			}
			if (write(fd,buff,20)!=20) {
				fprintf(stderr,"%s: error writing header !!!\n",fname);
				close(fd);
				return ret | 16;
			}
			ret |= 4;
		}
	}

	s = lseek(fd,0,SEEK_END);
	hdrsize = (s - CHUNKCRCSIZE) & MFSBLOCKMASK;
	if (hdrsize!=1024 && hdrsize!=4096) {
		fprintf(stderr,"%s: wrong file size\n",fname);
		close(fd);
		return ret | 16;
	}
	// read crc
	if (lseek(fd,hdrsize,SEEK_SET)!=hdrsize) {
		fprintf(stderr,"%s: error setting file pointer\n",fname);
		close(fd);
		return ret | 16;
	}
	if (read(fd,buff,CHUNKCRCSIZE)!=CHUNKCRCSIZE) {
		fprintf(stderr,"%s: error reading checksum block\n",fname);
		close(fd);
		return ret | 16;
	}
	rp = buff;
	for (i=0 ; i<1024 ; i++) {
		crc[i] = get32bit(&rp);
	}

	// check data crc
	if ((mode&(MODE_FAST|MODE_REPAIR))==MODE_FAST) {
		s = lseek(fd,-MFSBLOCKSIZE,SEEK_END);
		if (s<(hdrsize + CHUNKCRCSIZE)) {
			fprintf(stderr,"%s: wrong file size\n",fname);
			close(fd);
			return ret | 16;
		}
		s -= (hdrsize + CHUNKCRCSIZE);
		if ((s%MFSBLOCKSIZE)!=0) {
			fprintf(stderr,"%s: wrong file size\n",fname);
			close(fd);
			return ret | 16;
		}
		s >>= 16;
		if (read(fd,buff,MFSBLOCKSIZE)!=MFSBLOCKSIZE) {
			fprintf(stderr,"%s: error reading last data block\n",fname);
			close(fd);
			return ret | 16;
		}
		crcblock = mycrc32(0,buff,MFSBLOCKSIZE);
		if (crc[s]!=crcblock) {
			fprintf(stderr,"%s: crc error (last block ; header crc: %08"PRIX32" ; block crc: %08"PRIX32")\n",fname,crc[s],crcblock);
			ret |= 2;
		}
		if ((mode&MODE_EMPTY) && s<1023) {
			if (crc[s+1]!=mycrc32_zeroblock(0,MFSBLOCKSIZE) && crc[s+1]!=0) {
				fprintf(stderr,"%s: crc error (first empty block (%u) has 'non zero' crc: %08"PRIX32")\n",fname,(unsigned int)(s+1),crc[s+1]);
				ret |= 2;
			}
		}
	} else {
		if (lseek(fd,(hdrsize + CHUNKCRCSIZE),SEEK_SET)!=(hdrsize + CHUNKCRCSIZE)) {
			fprintf(stderr,"%s: error setting file pointer\n",fname);
			close(fd);
			return ret | 16;
		}
		for (i=0 ; i<1024 ; i++) {
			s = read(fd,buff,MFSBLOCKSIZE);
			if (s==0) {
				crcblock = mycrc32_zeroblock(0,MFSBLOCKSIZE);
				if ((mode&MODE_EMPTY)==0 || crc[i]==0) {
					crc[i] = crcblock;
				}
			} else {
				if (s!=MFSBLOCKSIZE) {
					fprintf(stderr,"%s: error reading data block: %"PRIu32"\n",fname,i);
					close(fd);
					return ret | 16;
				}
				crcblock = mycrc32(0,buff,MFSBLOCKSIZE);
			}
			if (crc[i]!=crcblock) {
				fprintf(stderr,"%s: crc error (block: %"PRIu32" ; header crc: %08"PRIX32" ; block crc: %08"PRIX32")\n",fname,i,crc[i],crcblock);
				crc[i] = crcblock;
				ret |= 2;
			}
		}
		if ((mode&MODE_REPAIR) && (ret&2)) {
			// write crc
			wp = buff;
			for (i=0 ; i<1024 ; i++) {
				put32bit(&wp,crc[i]);
			}
			if (lseek(fd,hdrsize,SEEK_SET)!=hdrsize) {
				fprintf(stderr,"%s: error setting file pointer\n",fname);
				close(fd);
				return ret | 16;
			}
			if (write(fd,buff,CHUNKCRCSIZE)!=CHUNKCRCSIZE) {
				fprintf(stderr,"%s: error writing checksum block\n",fname);
				close(fd);
				return ret | 16;
			}
			ret |= 8;
		}
	}
	close(fd);
	if (ret==0 && showok) {
		printf("%s: OK\n",fname);
	}
	if (ret&4) {
		printf("%s: header fixed\n",fname);
	}
	if (ret&8) {
		printf("%s: crc fixed\n",fname);
	}
	return ret;
}

void usage(const char *appname) {
	fprintf(stderr,"usage: %s [-frnex] chunk_file ...\n",appname);
	fprintf(stderr,"\n");
	fprintf(stderr,"-f: fast check (check only header and crc of last data block)\n");
	fprintf(stderr,"-r: repair (fix header info from file name and recalculate crc)\n");
	fprintf(stderr,"-n: when file name is wrong then try to fix it usng header\n");
	fprintf(stderr,"-e: force checking crc values for non existing blocks in chunks 1.0 - may fail in case of chunks that were truncated\n");
	fprintf(stderr,"-x: print 'OK' for good files\n");
}

int main(int argc,char *argv[]) {
	int ch;
	uint8_t mode = 0;
	uint8_t verbose = 0;
	uint8_t ret = 0;
	const char *appname;

	appname = argv[0];
	while ((ch = getopt(argc,argv,"hvfenrx?")) != -1) {
		switch(ch) {
			case 'h':
				usage(appname);
				return 0;
			case 'v':
				printf("version: %s\n",VERSSTR);
				return 0;
			case 'f':
				mode |= MODE_FAST;
				break;
			case 'e':
				mode |= MODE_EMPTY;
				break;
			case 'n':
				mode |= MODE_NAME;
				break;
			case 'r':
				mode |= MODE_REPAIR;
				break;
			case 'x':
				verbose++;
				break;
//			case 'l':
//				if (bindhost) {
//					free((char*)bindhost);
//				}
//				bindhost = strdup(optarg);
//				break;
			default:
				usage(argv[0]);
				return 1;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc==0) {
		usage(appname);
		return 0;
	}

	mycrc32_init();

	while (argc>0) {
		ret |= chunk_repair(*argv,mode,verbose);
		argv++;
		argc--;
	}
	return ret;
}
