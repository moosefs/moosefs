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
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <limits.h>
#include <errno.h>

#include "crc.h"
#include "datapack.h"
#include "clocks.h"
#include "MFSCommunication.h"
#include "idstr.h"

#define CHUNKCRCSIZE 4096

#define MODE_FAST 1
#define MODE_EMPTY 2
#define MODE_NAME 4
#define MODE_REPAIR 8

#define NAME_NONE 0
#define NAME_MFSDIR 1
#define NAME_CHUNK 2
#define NAME_ANY 3
#define NAME_ANYDIR 4
#define NAME_FIRST 5

static uint64_t lasttime = 0;
static uint64_t scanned = 0;
static uint32_t stdnlneeded = 0;
static uint32_t errnlneeded = 0;

static inline int check_timer(void) {
	uint64_t nsec;
	nsec = monotonic_nseconds();
	if (nsec>lasttime+1000000000 || nsec<lasttime) {
		lasttime = nsec;
		return 1;
	}
	return 0;
}

static inline void check_nl(void) {
	if (stdnlneeded) {
		printf("\33[2K\r"); // try to erase whole line using ANSI code
		while (stdnlneeded) {
			printf(" ");
			stdnlneeded--;
		}
		printf("\r");
	}
	if (errnlneeded) {
		fprintf(stderr,"\33[2K\r"); // try to erase whole line using ANSI code
		while (errnlneeded) {
			fprintf(stderr," ");
			errnlneeded--;
		}
		fprintf(stderr,"\r");
	}
}

static inline void move_file(const char *oldname,const char *newname,uint8_t *buff) {
	int ofd;
	int nfd;
	off_t s,pos;
	ssize_t bsize;

	ofd = open(oldname,O_RDONLY);
	if (ofd<0) {
		check_nl();
		printf("%s: error opening file !!!\n",oldname);
		return;
	}
	nfd = open(newname,O_RDWR|O_CREAT|O_EXCL,0666);
	if (nfd<0) {
		check_nl();
		printf("%s: error creating file !!!\n",newname);
		close(ofd);
		return;
	}

	s = lseek(ofd,0,SEEK_END);
	lseek(ofd,0,SEEK_SET);
	pos = 0;
	while (pos<s) {
		bsize = s - pos;
		if (bsize>MFSBLOCKSIZE) {
			bsize = MFSBLOCKSIZE;
		}
		if (read(ofd,buff,bsize)!=bsize) {
			check_nl();
			printf("%s: error reading file !!!\n",oldname);
			close(ofd);
			close(nfd);
			unlink(newname);
			return;
		}
		if (write(nfd,buff,bsize)!=bsize) {
			check_nl();
			printf("%s: error writing file !!!\n",newname);
			close(ofd);
			close(nfd);
			unlink(newname);
			return;
		}
		pos += bsize;
	}
	close(ofd); // we don't need to check close status here since the file has been correctly read
	if (close(nfd)<0) {
		check_nl();
		printf("%s: error writing file !!!\n",newname);
		close(nfd);
		unlink(newname);
		return;
	}
	if (unlink(oldname)<0) {
		printf("%s: error removing file !!!\n",oldname);
	}
}

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
	if (chunkid!=NULL) {
		*chunkid = namechunkid;
	}
	if (version!=NULL) {
		*version = nameversion;
	}
	return 0;
}

int chunk_repair(const char *fname,char **newname,uint8_t mode,uint8_t showok,uint8_t *buff) {
	uint64_t namechunkid;
	uint32_t nameversion;
	uint32_t i,j;
	int fd;
	uint32_t crc[1024];
	uint32_t crcblock;
	uint16_t hdrsize;
	off_t s;
	const uint8_t *rp;
	uint8_t *wp;
	int ret=0;

	*newname = NULL;

	// check fname
	// name should be in format: ..../chunk_XXXXXXXXXXXXXXXX_YYYYYYYY.mfs
	if (mode&MODE_REPAIR) {
		fd = open(fname,O_RDWR);
	} else {
		fd = open(fname,O_RDONLY);
	}
	if (fd<0) {
		check_nl();
		printf("%s: error opening file !!!\n",fname);
		return ret | 16;
	}
	i = strlen(fname);
	if (i<35 || hdd_check_filename(fname+(i-35),&namechunkid,&nameversion)<0) {
		if (mode&MODE_NAME) {
			check_nl();
			printf("%s: wrong chunk name - try to fix it using header data\n",fname);
			if (read(fd,buff,20)!=20) {
				printf("%s: error reading header !!!\n",fname);
				close(fd);
				return ret | 16;
			}
			if (memcmp(buff,MFSSIGNATURE "C 1.",7)!=0 || (buff[7]!='0' && buff[7]!='1')) {
				printf("%s: wrong chunk header !!!\n",fname);
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
			*newname = malloc(j+35+1);
			if (j>0) {
				memcpy(*newname,fname,j);
			}
			snprintf((*newname)+j,35+1,"chunk_%016"PRIX64"_%08"PRIX32".mfs",namechunkid,nameversion);
			(*newname)[j+35] = 0;
			if (rename(fname,(*newname))<0) {
				printf("%s->%s: rename error !!!\n",fname,(*newname));
				free(*newname);
				*newname = NULL;
				close(fd);
				return ret | 16;
			}
			printf("%s: changed name to: %s\n",fname,*newname);
		} else {
			check_nl();
			printf("%s: wrong chunk name format !!! (skip header)\n",fname);
			ret |= 1;
		}
	} else {
		*newname = strdup(fname);
		if (read(fd,buff,20)!=20) {
			check_nl();
			printf("%s: error reading header !!!\n",fname);
			close(fd);
			return ret | 16;
		}
		if (memcmp(buff,MFSSIGNATURE "C 1.",7)!=0 || (buff[7]!='0' && buff[7]!='1')) {
			check_nl();
			printf("%s: wrong chunk header !!!\n",fname);
			memcpy(buff,MFSSIGNATURE "C 1.0",8);
			ret |= 1;
		}
		if (buff[7]=='1') { // this is chunk 1.1 - check 'empty' crc values
			mode |= MODE_EMPTY;
		}
		rp = buff+8;
		wp = (uint8_t*)rp;
		if (get64bit(&rp)!=namechunkid) {
			check_nl();
			printf("%s: wrong chunk number in header !!!\n",fname);
			put64bit(&wp,namechunkid);
			ret |= 1;
		}
		wp = (uint8_t*)rp;
		if (get32bit(&rp)!=nameversion) {
			check_nl();
			printf("%s: wrong chunk version in header !!!\n",fname);
			put32bit(&wp,nameversion);
			ret |= 1;
		}
		if ((mode&MODE_REPAIR) && (ret&1)) {
			if (lseek(fd,0,SEEK_SET)!=0) {
				check_nl();
				printf("%s: error setting file pointer\n",fname);
				close(fd);
				return ret | 16;
			}
			if (write(fd,buff,20)!=20) {
				check_nl();
				printf("%s: error writing header !!!\n",fname);
				close(fd);
				return ret | 16;
			}
			ret |= 4;
		}
	}

	s = lseek(fd,0,SEEK_END);
	hdrsize = (s - CHUNKCRCSIZE) & MFSBLOCKMASK;
	if (hdrsize!=1024 && hdrsize!=4096) {
		check_nl();
		printf("%s: wrong file size\n",fname);
		close(fd);
		return ret | 16;
	}
	// read crc
	if (lseek(fd,hdrsize,SEEK_SET)!=hdrsize) {
		check_nl();
		printf("%s: error setting file pointer\n",fname);
		close(fd);
		return ret | 16;
	}
	if (read(fd,buff,CHUNKCRCSIZE)!=CHUNKCRCSIZE) {
		check_nl();
		printf("%s: error reading checksum block\n",fname);
		close(fd);
		return ret | 16;
	}
	rp = buff;
	for (i=0 ; i<1024 ; i++) {
		crc[i] = get32bit(&rp);
	}

	// check data crc
	if ((mode&(MODE_FAST|MODE_REPAIR))==MODE_FAST) {
		if (s<MFSBLOCKSIZE) { // this is empty file
			s = -1;
		} else {
			s = lseek(fd,-MFSBLOCKSIZE,SEEK_END);
			if (s<(hdrsize + CHUNKCRCSIZE)) {
				check_nl();
				printf("%s: wrong file size\n",fname);
				close(fd);
				return ret | 16;
			}
			s -= (hdrsize + CHUNKCRCSIZE);
			if ((s%MFSBLOCKSIZE)!=0) {
				check_nl();
				printf("%s: wrong file size\n",fname);
				close(fd);
				return ret | 16;
			}
			s >>= 16;
			if (read(fd,buff,MFSBLOCKSIZE)!=MFSBLOCKSIZE) {
				check_nl();
				printf("%s: error reading last data block\n",fname);
				close(fd);
				return ret | 16;
			}
			crcblock = mycrc32(0,buff,MFSBLOCKSIZE);
			if (crc[s]!=crcblock) {
				check_nl();
				printf("%s: crc error (last block ; header crc: %08"PRIX32" ; block crc: %08"PRIX32")\n",fname,crc[s],crcblock);
				ret |= 2;
			}
		}
		if ((mode&MODE_EMPTY) && s<1023) {
			if (crc[s+1]!=mycrc32_zeroblock(0,MFSBLOCKSIZE) && crc[s+1]!=0) {
				check_nl();
				printf("%s: crc error (first empty block (%u) has 'non zero' crc: %08"PRIX32")\n",fname,(unsigned int)(s+1),crc[s+1]);
				ret |= 2;
			}
		}
	} else {
		if (lseek(fd,(hdrsize + CHUNKCRCSIZE),SEEK_SET)!=(hdrsize + CHUNKCRCSIZE)) {
			check_nl();
			printf("%s: error setting file pointer\n",fname);
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
					check_nl();
					printf("%s: error reading data block: %"PRIu32"\n",fname,i);
					close(fd);
					return ret | 16;
				}
				crcblock = mycrc32(0,buff,MFSBLOCKSIZE);
			}
			if (crc[i]!=crcblock) {
				check_nl();
				printf("%s: crc error (block: %"PRIu32" ; header crc: %08"PRIX32" ; block crc: %08"PRIX32")\n",fname,i,crc[i],crcblock);
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
				check_nl();
				printf("%s: error setting file pointer\n",fname);
				close(fd);
				return ret | 16;
			}
			if (write(fd,buff,CHUNKCRCSIZE)!=CHUNKCRCSIZE) {
				check_nl();
				printf("%s: error writing checksum block\n",fname);
				close(fd);
				return ret | 16;
			}
			ret |= 8;
		}
	}
	close(fd);
	if (ret==0 && showok) {
		check_nl();
		printf("%s: OK\n",fname);
	}
	if (ret&4) {
		check_nl();
		printf("%s: header fixed\n",fname);
		if (*newname!=NULL) {
			free(*newname);
		}
		*newname = NULL; // do not move fixed files !!!
	}
	if (ret&8) {
		check_nl();
		printf("%s: crc fixed\n",fname);
		if (*newname!=NULL) {
			free(*newname);
		}
		*newname = NULL; // do not move fixed files !!!
	}
	return ret;
}

static inline int is_hex(char c) {
	return ((c>='0' && c<='9') || (c>='a' && c<='f') || (c>='A' && c<='F'));
}

int recursive_scan(const char *path,uint8_t pathtype,uint8_t mode,uint8_t verbose,uint8_t *auxbuff,const char *dmgpath) {
	struct stat st;
	int lfd;
	DIR *dd;
	struct dirent *de;
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
	struct dirent *destorage;
#endif
	char *subpath;
	char *newname;
	uint32_t dleng;
	uint32_t pleng;
	uint32_t fnleng;
	uint8_t goodname;
	uint8_t ret = 0;

	pleng = strlen(path);
	if (verbose==0 && check_timer()) {
		if (isatty(fileno(stdout))) {
			check_nl();
			printf("\robjects scanned: %"PRIu64" (path: %s) ... ",scanned,path);
			stdnlneeded = pleng+40;
			fflush(stdout);
		} else if (isatty(fileno(stderr))) {
			check_nl();
			fprintf(stderr,"\robjects scanned: %"PRIu64" (path: %s) ... ",scanned,path);
			errnlneeded = pleng+40;
			fflush(stderr);
		}
	}
	if (dmgpath!=NULL) {
		dleng = strlen(dmgpath);
		if (dleng<=pleng && memcmp(path,dmgpath,dleng)==0) { // path is inside of dmgpath
			return 0;
		}
	} else {
		dleng = 0;
	}
	if (stat(path,&st)<0) {
		check_nl();
		printf("%s: can't stat path\n",path);
		return 1;
	} else {
		if (S_ISDIR(st.st_mode) && (pathtype==NAME_MFSDIR || pathtype==NAME_ANY || pathtype==NAME_ANYDIR || pathtype==NAME_FIRST)) {
			subpath = malloc(pleng+1+256);
			if (subpath==NULL) {
				check_nl();
				printf("out of memory\n");
				return 1;
			}
			memcpy(subpath,path,pleng);
			subpath[pleng]='/';
			memcpy(subpath+pleng+1,".lock",5);
			subpath[pleng+6]=0;
			lfd = open(subpath,O_RDWR|O_CREAT|O_TRUNC,0640);
			if (lfd>=0 && lockf(lfd,F_TLOCK,0)<0) {
				check_nl();
				printf("found active lock file: %s - there is a working chunkserver using this disk - skip scanning\n",subpath);
				close(lfd);
				free(subpath);
				return 1;
			}
			dd = opendir(path);
			if (dd==NULL) {
				check_nl();
				printf("%s: can't open directory\n",path);
				if (lfd>=0) {
					close(lfd);
				}
				free(subpath);
				return 1;
			}

			/* size of name added to size of structure because on some os'es d_name has size of 1 byte */
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
			destorage = (struct dirent*)malloc(sizeof(struct dirent)+pathconf(path,_PC_NAME_MAX)+1);
			if (destorage==NULL) {
				check_nl();
				printf("out of memory\n");
				closedir(dd);
				if (lfd>=0) {
					close(lfd);
				}
				free(subpath);
				return 1;
			}
			while (readdir_r(dd,destorage,&de)==0 && de!=NULL) {
#else
			while ((de = readdir(dd)) != NULL) {
#endif
				fnleng = strlen(de->d_name);
				if (fnleng==1 && de->d_name[0]=='.') {
					continue;
				}
				if (fnleng==2 && de->d_name[0]=='.' && de->d_name[1]=='.') {
					continue;
				}
				if (fnleng==5 && memcmp(de->d_name,".lock",5)==0) {
					continue;
				}
				if (fnleng==7 && memcmp(de->d_name,".metaid",7)==0) {
					continue;
				}
				if (fnleng==8 && memcmp(de->d_name,".chunkdb",8)==0) { // TODO: in the future read this file and compare contents with actual chunk list - also fix this file !!!
					continue;
				}
				goodname = NAME_NONE;
				if ((mode&MODE_NAME)) {
					goodname = NAME_ANY;
				} else {
					if (fnleng==2 && is_hex(de->d_name[0]) && is_hex(de->d_name[1]) && (pathtype==NAME_ANYDIR || pathtype==NAME_FIRST)) {
						goodname = NAME_MFSDIR;
					} else if (fnleng==35 && hdd_check_filename(de->d_name,NULL,NULL)>=0 && (pathtype==NAME_MFSDIR || pathtype==NAME_FIRST)) {
						goodname = NAME_CHUNK;
					} else if (pathtype==NAME_ANYDIR || pathtype==NAME_FIRST) {
						goodname = NAME_ANYDIR;
					}
				}
				if (goodname != NAME_NONE) {
					if (fnleng>255) {
						check_nl();
						printf("%s/%s: name too long\n",path,de->d_name);
						ret |= 1;
					} else {
						memcpy(subpath+pleng+1,de->d_name,fnleng);
						subpath[pleng+fnleng+1]=0;
						ret |= recursive_scan(subpath,goodname,mode,verbose,auxbuff,dmgpath);
					}
				}
			}
#if !defined(__GLIBC__) || (__GLIBC__ < 2) || ((__GLIBC__ == 2) && (__GLIBC_MINOR__ < 23))
			free(destorage);
#endif
			closedir(dd);
			if (lfd>=0) {
				close(lfd);
			}
			free(subpath);
		} else if (S_ISREG(st.st_mode) && (pathtype==NAME_CHUNK || pathtype==NAME_ANY || pathtype==NAME_FIRST)) {
			ret = chunk_repair(path,&newname,mode,verbose,auxbuff);
			scanned++;
			if (ret!=0 && dmgpath!=NULL && newname!=NULL) { // rename newname -> dmgpath/chunk_XXXXXXXXXXXXXXXX_YYYYYYYY.mfs
				fnleng = strlen(newname);
				if (fnleng<35) {
					check_nl();
					printf("%s: wrong chunk file name\n",newname);
					free(newname);
					return ret;
				}
				subpath = malloc(dleng+35+1+1);
				if (subpath==NULL) {
					check_nl();
					printf("out of memory\n");
					free(newname);
					return ret;
				}
				memcpy(subpath,dmgpath,dleng);
				subpath[dleng]='/';
				memcpy(subpath+dleng+1,newname+(fnleng-35),35);
				subpath[dleng+35+1]=0;
				if (rename(newname,subpath)<0) {
					if (errno==EXDEV) {
						move_file(newname,subpath,auxbuff);
					} else {
						check_nl();
						printf("%s -> %s: rename error !!!\n",newname,subpath);
					}
				}
				free(subpath);
			}
			if (newname!=NULL) {
				free(newname);
			}
			return ret;
		} else {
			check_nl();
			if (mode&MODE_NAME) {
				printf("%s: not a directory nor a file\n",path);
			} else {
				printf("%s: not a correct moosefs path\n",path);
			}
			return 1;
		}
	}
	return ret;
}

void usage(const char *appname) {
	fprintf(stderr,"usage: %s [-frnex] [-m damaged_dir] chunk_file|mfs_hdd_path ...\n",appname);
	fprintf(stderr,"\n");
	fprintf(stderr,"-f: fast check (check only header and crc of last data block)\n");
	fprintf(stderr,"-r: repair (fix header info from file name and recalculate crc)\n");
	fprintf(stderr,"-n: when file name is wrong then try to fix it using header\n");
	fprintf(stderr,"-e: force checking crc values for non existing blocks in chunks 1.0 - may fail in case of chunks that were truncated\n");
	fprintf(stderr,"-x: print 'OK' for good files\n");
	fprintf(stderr,"-m damaged_dir: move all damaged chunks to given directory for future processing\n");
}

int main(int argc,char *argv[]) {
	int ch;
	uint8_t mode = 0;
	uint8_t verbose = 0;
	uint8_t ret = 0;
	uint8_t *auxbuff;
	const char *appname;
	char *dmgpath;
	char *wrkpath;
	struct stat st;

	dmgpath = NULL;
	appname = argv[0];
	while ((ch = getopt(argc,argv,"hvfenrm:x?")) != -1) {
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
			case 'm':
				if (dmgpath!=NULL) {
					free(dmgpath);
				}
				dmgpath = realpath(optarg,NULL);
				if (dmgpath==NULL) {
					fprintf(stderr,"%s: realpath error\n",optarg);
					return 1;
				}
				if (stat(dmgpath,&st)<0) {
					fprintf(stderr,"%s: stat error\n",dmgpath);
					return 1;
				}
				if (!S_ISDIR(st.st_mode)) {
					fprintf(stderr,"%s: not a directory\n",dmgpath);
					return 1;
				}
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
		if (dmgpath!=NULL) {
			free(dmgpath);
		}
		return 0;
	}

	mycrc32_init();

	auxbuff = malloc(MFSBLOCKSIZE);

	check_timer();

	while (argc>0) {
		wrkpath = realpath(*argv,NULL);
		if (wrkpath==NULL) {
			check_nl();
			printf("%s: realpath error\n",*argv);
			ret |= 1;
		} else {
			ret |= recursive_scan(wrkpath,NAME_FIRST,mode,verbose,auxbuff,dmgpath);
			free(wrkpath);
		}
		argv++;
		argc--;
	}

	free(auxbuff);
	if (dmgpath!=NULL) {
		free(dmgpath);
	}

	check_nl();

	return ret;
}
