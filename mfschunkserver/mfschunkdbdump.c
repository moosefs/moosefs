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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "datapack.h"

int chunkdb_dump(const char *fullname) {
	struct stat sb;
	int fd;
	uint8_t *chunkbuff;
	const uint8_t *rptr,*rptrmem,*endbuff;
	uint16_t pleng;
	uint64_t chunkid;
	uint32_t version;
	uint16_t blocks;
	uint16_t pathid;
	uint16_t hdrsize;
	uint32_t diskusage;
	uint8_t testedflag;
	uint8_t mode;
	uint8_t rsize;
	int res;

	fd = open(fullname,O_RDONLY);
	if (fd<0) {
		fprintf(stderr,"can't open chunkdb file\n");
		return -1;
	}
	if (fstat(fd,&sb)<0) {
		close(fd);
		fprintf(stderr,"can't stat chunkdb file\n");
		return -1;
	}
	chunkbuff = malloc(sb.st_size);
	if (chunkbuff==NULL) {
		fprintf(stderr,"can't allocate memory\n");
		close(fd);
		return -1;
	}
	if (read(fd,chunkbuff,sb.st_size)!=sb.st_size) {
		fprintf(stderr,"can't read chunkdb file\n");
		free(chunkbuff);
		close(fd);
		return -1;
	}
	close(fd);

	rptr = chunkbuff;
	endbuff = rptr+sb.st_size;

	if (memcmp(rptr,"MFS CHUNKDB",11)!=0 && rptr[11]!='1' && rptr[11]!='2' && rptr[11]!='3' && rptr[11]!='4') {
		fprintf(stderr,"wrong chunkdb header\n");
		free(chunkbuff);
		return -1;
	}
	mode = rptr[11]-'0';
	if (mode==1) {
		rsize = 16;
	} else if (mode==2) {
		rsize = 18;
	} else if (mode==3) {
		rsize = 19;
	} else {
		rsize = 23;
	}
	rptr+=12;

	pleng = get16bit(&rptr);
	if (rptr+pleng>endbuff) {
		fprintf(stderr,"wrong path in chunkdb file\n");
		free(chunkbuff);
		return -1;
	}
	printf("path: ");
	while (pleng>0) {
		putchar(*rptr);
		rptr++;
		pleng--;
	}
	printf("\n");
	rptrmem = rptr;
	chunkid = 0;
	version = 0;
	blocks = 0;
	hdrsize = 1024+4096;
	pathid = 0xFFFF;
	testedflag = 0;
	diskusage = 0;

	res = 0;

	while (rptr+rsize<=endbuff) {
		chunkid = get64bit(&rptr);
		version = get32bit(&rptr);
		blocks = get16bit(&rptr);
		if (blocks>1024 && blocks!=0xFFFF) {
			printf("chunk %016"PRIX64"_%08"PRIX32" - wrong block count (%u)\n",chunkid,version,blocks);
			res = -1;
		}
		if (mode>=2) {
			hdrsize = get16bit(&rptr);
		}
		pathid = get16bit(&rptr);
		if (pathid>255) {
			printf("chunk %016"PRIX64"_%08"PRIX32" - wrong pathid (%u)\n",chunkid,version,pathid);
			res = -1;
		}
		if (mode>=3) {
			testedflag = get8bit(&rptr);
		}
		if (mode>=4) {
			diskusage = get32bit(&rptr);
		}
		if (chunkid==0) {
			break;
		}
	}
	if (rptr!=endbuff || chunkid!=0 || version!=0 || blocks!=0 || pathid!=0 || testedflag!=0 || diskusage!=0) {
		printf("wrong chunkdb ending\n");
		res = -1;
	}

	rptr = rptrmem;

	while (rptr+rsize<=endbuff) {
		chunkid = get64bit(&rptr);
		version = get32bit(&rptr);
		blocks = get16bit(&rptr);
		if (mode>=2) {
			hdrsize = get16bit(&rptr);
		}
		pathid = get16bit(&rptr);
		if (mode>=3) {
			testedflag = get8bit(&rptr);
		}
		if (mode>=4) {
			diskusage = get32bit(&rptr);
		}
		if (chunkid==0) {
			break;
		}
		if (pathid<256) {
			printf("%02"PRIX16"/chunk_%016"PRIX64"_%08"PRIX32".mfs ; length: ",pathid,chunkid,version);
			if ((mode>=2 && hdrsize==0) || blocks==0xFFFF) {
				printf("-");
			} else {
				printf("%u",hdrsize+blocks*65536U);
			}
			printf(" ; blocks: ");
			if (blocks!=0xFFFF) {
				printf("%u",blocks);
			} else {
				printf("-");
			}
			if (mode>=2) {
				printf(" ; hdrsize: ");
				if (mode>=2 && hdrsize==0) {
					printf("-");
				} else {
					printf("%u",hdrsize);
				}
			}
			if (mode>=3) {
				printf(" ; tested: %u",testedflag);
			}
			if (mode>=4) {
				printf(" ; diskusage: ");
				if (diskusage>0) {
					printf("%u",diskusage);
				} else {
					printf("-");
				}
			}
			printf("\n");
		} else {
			printf("pathid: %04"PRIX16" ; chunkid: %016"PRIX64" ; version: %08"PRIX32" ; hdrsize: %u ; blocks: %u ; tested: %u ; diskusage: %u\n",pathid,chunkid,version,hdrsize,blocks,testedflag,diskusage);
		}
	}

	free(chunkbuff);
	return res;
}

int main(int argc,char *argv[]) {
	if (argc!=2) {
		fprintf(stderr,"usage: %s <chunkdb>\n",argv[0]);
		return 1;
	}
	if (chunkdb_dump(argv[1])<0) {
		fprintf(stderr,"file has errors\n");
		return 1;
	}
	return 0;
}
