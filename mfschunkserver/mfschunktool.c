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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "crc.h"
#include "datapack.h"
#include "MFSCommunication.h"

#define CHUNKHDRCRC 1024
#define CHUNKHDRSIZE ((CHUNKHDRCRC)+4*1024)

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

int chunk_repair(const char *fname,uint8_t fastmode,uint8_t showok,uint8_t repair) {
	uint64_t namechunkid;
	uint32_t nameversion;
	uint32_t i;
	int fd;
	uint8_t buff[MFSBLOCKSIZE];
	uint32_t crc[1024];
	uint32_t crcblock;
	off_t s;
	const uint8_t *rp;
	uint8_t *wp;
	int ret=0;

	// check fname
	// name should be in format: ..../chunk_XXXXXXXXXXXXXXXX_YYYYYYYY.mfs
	if (repair) {
		fd = open(fname,O_RDWR);
	} else {
		fd = open(fname,O_RDONLY);
	}
	if (fd<0) {
		fprintf(stderr,"%s: error opening file !!!\n",fname);
		return -1;
	}
	i = strlen(fname);
	if (i<35) {
		fprintf(stderr,"%s: wrong chunk name format !!! (skip header)\n",fname);
		ret |= 1;
	} else {
		if (hdd_check_filename(fname+(i-35),&namechunkid,&nameversion)<0) {
			fprintf(stderr,"%s: wrong chunk name format !!! (skip header)\n",fname);
			ret |= 1;
		} else {
			if (read(fd,buff,20)!=20) {
				fprintf(stderr,"%s: error reading header !!!\n",fname);
				close(fd);
				return -1;
			}
			if (memcmp(buff,MFSSIGNATURE "C 1.0",8)!=0) {
				fprintf(stderr,"%s: wrong chunk header !!!\n",fname);
				memcpy(buff,MFSSIGNATURE "C 1.0",8);
				ret |= 1;
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
			if (repair && (ret&1)) {
				if (lseek(fd,0,SEEK_SET)!=0) {
					fprintf(stderr,"%s: error setting file pointer\n",fname);
					close(fd);
					return -1;
				}
				if (write(fd,buff,20)!=20) {
					fprintf(stderr,"%s: error writing header !!!\n",fname);
					close(fd);
					return -1;
				}
				ret |= 4;
			}
		}
	}

	// read crc
	if (lseek(fd,CHUNKHDRCRC,SEEK_SET)!=CHUNKHDRCRC) {
		fprintf(stderr,"%s: error setting file pointer\n",fname);
		close(fd);
		return -1;
	}
	if (read(fd,buff,4096)!=4096) {
		fprintf(stderr,"%s: error reading checksum block\n",fname);
		close(fd);
		return -1;
	}
	rp = buff;
	for (i=0 ; i<1024 ; i++) {
		crc[i] = get32bit(&rp);
	}

	// check data crc
	if (fastmode && repair==0) {
		s = lseek(fd,-MFSBLOCKSIZE,SEEK_END);
		if (s<MFSHDRSIZE) {
			fprintf(stderr,"%s: wrong file size\n",fname);
			close(fd);
			return -1;
		}
		s -= MFSHDRSIZE;
		if ((s%MFSBLOCKSIZE)!=0) {
			fprintf(stderr,"%s: wrong file size\n",fname);
			close(fd);
			return -1;
		}
		s >>= 16;
		if (read(fd,buff,MFSBLOCKSIZE)!=MFSBLOCKSIZE) {
			fprintf(stderr,"%s: error reading last data block\n",fname);
			close(fd);
			return -1;
		}
		crcblock = mycrc32(0,buff,MFSBLOCKSIZE);
		if (crc[s]!=crcblock) {
			fprintf(stderr,"%s: crc error (last block ; header crc: %08"PRIX32" ; block crc: %08"PRIX32")\n",fname,crc[s],crcblock);
			ret |= 2;
		}
	} else {
		if (lseek(fd,MFSHDRSIZE,SEEK_SET)!=MFSHDRSIZE) {
			fprintf(stderr,"%s: error setting file pointer\n",fname);
			close(fd);
			return -1;
		}
		for (i=0 ; i<1024 ; i++) {
			s = read(fd,buff,MFSBLOCKSIZE);
			if (s==0) {
				break;
			}
			if (s!=MFSBLOCKSIZE) {
				fprintf(stderr,"%s: error reading data block: %"PRIu32"\n",fname,i);
				close(fd);
				return -1;
			}
			crcblock = mycrc32(0,buff,MFSBLOCKSIZE);
			if (crc[i]!=crcblock) {
				fprintf(stderr,"%s: crc error (block: %"PRIu32" ; header crc: %08"PRIX32" ; block crc: %08"PRIX32")\n",fname,i,crc[i],crcblock);
				crc[i] = crcblock;
				ret |= 2;
			}
		}
		if (repair && (ret&2)) {
			// write crc
			wp = buff;
			for (i=0 ; i<1024 ; i++) {
				put32bit(&wp,crc[i]);
			}
			if (lseek(fd,CHUNKHDRCRC,SEEK_SET)!=CHUNKHDRCRC) {
				fprintf(stderr,"%s: error setting file pointer\n",fname);
				close(fd);
				return -1;
			}
			if (write(fd,buff,4096)!=4096) {
				fprintf(stderr,"%s: error writing checksum block\n",fname);
				close(fd);
				return -1;
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
	fprintf(stderr,"usage: %s [-fr] chunk_file ...\n",appname);
	fprintf(stderr,"\n");
	fprintf(stderr,"-f: fast check (check only header and crc of last data block)\n");
	fprintf(stderr,"-r: repair (fix header info from file name and recalculate crc)\n");
}

int main(int argc,char *argv[]) {
	char ch;
	uint8_t fastmode = 0;
	uint8_t repair = 0;
	uint8_t verbose = 0;
	uint8_t ret = 0;
	const char *appname;

	appname = argv[0];
	while ((ch = getopt(argc,argv,"hvfrx?")) != -1) {
		switch(ch) {
			case 'h':
				usage(appname);
				return 0;
			case 'v':
				printf("version: %s\n",VERSSTR);
				return 0;
			case 'f':
				fastmode=1;
				break;
			case 'r':
				repair=1;
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
		ret |= chunk_repair(*argv,fastmode,verbose,repair);
		argv++;
		argc--;
	}
	return ret;
}
