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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "datapack.h"
#include "MFSCommunication.h"
#include "liset64.h"
#include "idstr.h"

// #define DEBUG 1

#define PROGRESS_PRINT 100000

//static uint8_t humode=0;
//static uint8_t numbermode=0;

enum {
	STATUS_OK = 0,
	STATUS_ENOENT = 1,
	STATUS_ANY = 2
};

#define PHN_USESI       0x01
#define PHN_USEIEC      0x00
char* mfs_humanize_number(uint64_t number,uint8_t flags) {
	static char numbuf[6];	// [ "xxx" , "xx" , "x" , "x.x" ] + ["" , "X" , "Xi"]
	uint64_t divisor;
	uint16_t b;
	uint8_t i;
	uint8_t scale;

	if (flags & PHN_USESI) {
		divisor = 1000;
	} else {
		divisor = 1024;
	}
	if (number>(UINT64_MAX/100)) {
		number /= divisor;
		number *= 100;
		scale = 1;
	} else {
		number *= 100;
		scale = 0;
	}
	while (number>=99950) {
		number /= divisor;
		scale+=1;
	}
	i=0;
	if (number<995 && scale>0) {
		b = ((uint32_t)number + 5) / 10;
		numbuf[i++]=(b/10)+'0';
		numbuf[i++]='.';
		numbuf[i++]=(b%10)+'0';
	} else {
		b = ((uint32_t)number + 50) / 100;
		if (b>=100) {
			numbuf[i++]=(b/100)+'0';
			b%=100;
		}
		if (b>=10 || i>0) {
			numbuf[i++]=(b/10)+'0';
			b%=10;
		}
		numbuf[i++]=b+'0';
	}
	if (scale>0) {
		if (flags&PHN_USESI) {
			numbuf[i++]="-kMGTPE"[scale];
		} else {
			numbuf[i++]="-KMGTPE"[scale];
			numbuf[i++]='i';
		}
	}
	numbuf[i++]='\0';
	return numbuf;
}

typedef struct _metasection {
	off_t offset;
	uint64_t length;
	uint8_t mver;
} metasection;

typedef struct _stats {
	uint32_t files;
	uint32_t dirs;
	uint64_t kchunks;
	uint64_t achunks;
	uint64_t length;
	uint64_t size;
	uint64_t keeprsize;
	uint64_t archrsize;
	uint64_t rsize;
} stats;

typedef struct _dirinfostate {
	stats s;
	int inode_liset;
	int chunk_liset;
	char *path;
	const char *pptr;
	uint32_t inode;
	uint16_t nleng;
	uint8_t wantchunks;
	uint8_t pathscan;
	uint8_t parent_found;
	uint8_t nextsection;
	uint8_t status;
	struct _dirinfostate *next;
} dirinfostate;

static dirinfostate *dishead;

static int achunk_liset;

static uint8_t sclass_keep_factor[256];
static uint8_t sclass_arch_factor[256];

int scan_sclass(FILE *fd,metasection *sdata) {
	uint8_t buff[11];
	const uint8_t *rptr;
	uint8_t i,l;
	uint8_t ogroup;
	uint8_t nleng;
	uint16_t sclassid;
	uint8_t createcnt;
	uint8_t keepcnt;
	uint8_t archcnt;
	uint32_t chunkcount;

	fseeko(fd,sdata->offset,SEEK_SET);

	for (sclassid=0 ; sclassid<256 ; sclassid++) {
		sclass_keep_factor[sclassid]=(sclassid<10)?sclassid:0;
		sclass_arch_factor[sclassid]=(sclassid<10)?sclassid:0;
	}

	if (sdata->mver>0x16) {
		fprintf(stderr,"unsupported sclass format\n");
		return -1;
	}
	if (sdata->mver<0x15) {
		for (i=0 ; i<26 ; i++) {
			l = fgetc(fd);
			if (l>128) {
				fprintf(stderr,"sclass section malformed\n");
				return -1;
			}
			fseeko(fd,l,SEEK_CUR);
		}
	}
	if (sdata->mver==0x10) {
		ogroup = 1;
	} else {
		ogroup = fgetc(fd);
	}
	if (ogroup<1) {
		fprintf(stderr,"sclass section malformed\n");
		return -1;
	}
	l = (sdata->mver==0x12)?11:(sdata->mver<=0x13)?3:(sdata->mver<=0x14)?5:(sdata->mver<=0x15)?8:10;
	while (1) {
		if (fread(buff,1,l,fd)!=l) {
			fprintf(stderr,"loading labelset: read error\n");
			return -1;
		}
		rptr = buff;
		sclassid = get16bit(&rptr);
		if (sdata->mver>0x15) {
			nleng = get8bit(&rptr);
			rptr+=4; // skip admin_only,create_mode,arch_delay
			createcnt = get8bit(&rptr);
			keepcnt = get8bit(&rptr);
			archcnt = get8bit(&rptr);
			chunkcount = 0;
		} else if (sdata->mver>0x14) {
			nleng = 0;
			rptr+=2; // skip create_mode,arch_delay
			createcnt = get8bit(&rptr);
			keepcnt = get8bit(&rptr);
			archcnt = get8bit(&rptr);
			chunkcount = 0;
		} else if (sdata->mver>0x13) {
			nleng = 0;
			rptr++; // skip create_mode
			createcnt = get8bit(&rptr);
			keepcnt = get8bit(&rptr);
			archcnt = keepcnt;
			chunkcount = 0;
		} else {
			nleng = 0;
			createcnt = get8bit(&rptr);
			keepcnt = createcnt;
			archcnt = createcnt;
			if (sdata->mver==0x12) {
				chunkcount = get32bit(&rptr);
			} else {
				chunkcount = 0;
			}
		}
		if (nleng>0) {
			fseeko(fd,nleng,SEEK_CUR);
		}
		if (sclassid==0 && createcnt==0 && keepcnt==0 && archcnt==0) {
			break;
		}
		if (keepcnt==0 || keepcnt>9 || archcnt==0 || archcnt>9) {
			fprintf(stderr,"sclass section malformed\n");
			return -1;
		}
		if (sdata->mver>0x14) {
			fseeko(fd,(createcnt+keepcnt+archcnt)*4*ogroup,SEEK_CUR);
		} else if (sdata->mver>0x13) {
			fseeko(fd,(createcnt+keepcnt)*4*ogroup,SEEK_CUR);
		} else {
			fseeko(fd,createcnt*4*ogroup,SEEK_CUR);
		}
		if (chunkcount>0) {
			fseeko(fd,chunkcount*8,SEEK_CUR);
		}
		sclass_keep_factor[sclassid] = keepcnt;
		sclass_arch_factor[sclassid] = archcnt;
	}
	return 0;
}

// mver 0x13 and 0x14
int scan_nodes(FILE *fd,metasection *sdata) {
	uint8_t buff[14];
	const uint8_t *rptr;
	uint8_t type;
	uint32_t inode;
	uint32_t ch;
	uint16_t sessions;
	uint64_t length,l;
	uint8_t sclass;
	uint8_t wantchunks;
	uint32_t progresscnt;
	dirinfostate *dis;

	fseeko(fd,sdata->offset,SEEK_SET);
	fseeko(fd,8,SEEK_CUR); // skip maxnodeid,hashelements
	progresscnt = 0;
	while (1) {
		progresscnt++;
		if (progresscnt>=PROGRESS_PRINT) {
			progresscnt = 0;
			fprintf(stderr,"node scan: %.2lf%%\r",100.0*(ftello(fd)-sdata->offset)/sdata->length);
			fflush(stderr);
		}
		type = fgetc(fd);
		if (type==0) { // end of section
			fprintf(stderr,"node scan: 100.00%%\n");
			return 0; // ok
		}
		if (fread(buff,1,5,fd)!=5) {
			fprintf(stderr,"error reading metadata file\n");
			return -1;
		}
		rptr = buff;
		inode = get32bit(&rptr);
		sclass = get8bit(&rptr);
		if (sdata->mver<=0x13) {
			fseeko(fd,27,SEEK_CUR); // 27 = 1+2+6*4 - flags,mode,uid,gid,atime,mtime,ctime,trashtime
		} else {
			fseeko(fd,26,SEEK_CUR); // 26 = 1+1+2+5*4+2 - flags,winattr,mode,uid,gid,atime,mtime,ctime,trashretention
		}
		if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
			fseeko(fd,4,SEEK_CUR);
		} else if (type==TYPE_SYMLINK) {
			uint32_t pleng;
			if (fread(buff,1,4,fd)!=4) {
				fprintf(stderr,"error reading metadata file\n");
				return -1;
			}
			rptr = buff;
			pleng = get32bit(&rptr);
			fseeko(fd,pleng,SEEK_CUR);
		} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
			if (sdata->mver<=0x13) {
				if (fread(buff,1,14,fd)!=14) {
					fprintf(stderr,"error reading metadata file\n");
					return -1;
				}
			} else {
				if (fread(buff,1,12,fd)!=12) {
					fprintf(stderr,"error reading metadata file\n");
					return -1;
				}
			}
			rptr = buff;
			length = get64bit(&rptr);
			ch = get32bit(&rptr);
			if (sdata->mver<0x13) {
				sessions = get16bit(&rptr);
			} else {
				sessions = 0;
			}
			wantchunks = 0;
			for (dis=dishead ; dis!=NULL ; dis=dis->next) {
				if ((dis->status==STATUS_OK || dis->status==STATUS_ANY) && liset_check(dis->inode_liset,inode)) {
					dis->s.length += length;
					dis->s.files++;
					wantchunks = 1;
					dis->wantchunks = 1;
				} else {
					dis->wantchunks = 0;
				}
			}
			if (wantchunks) {
				l = length;
				while (ch) {
					uint64_t chunkid;
					uint32_t chunksize;
					if (fread(buff,1,8,fd)!=8) {
						fprintf(stderr,"error reading metadata file\n");
						return -1;
					}
					rptr = buff;
					chunkid = get64bit(&rptr);
					if (chunkid>0) {
						if (l>MFSCHUNKSIZE) {
							chunksize = MFSCHUNKSIZE + MFSHDRSIZE;
							l -= MFSCHUNKSIZE;
						} else if (l>0) {
							chunksize = ((((l-1)&MFSCHUNKMASK)+MFSBLOCKSIZE)&MFSBLOCKNEGMASK)+MFSHDRSIZE;
							l = 0;
						} else {
							chunksize = MFSHDRSIZE;
						}
						for (dis=dishead ; dis!=NULL ; dis=dis->next) {
							if (dis->wantchunks && liset_check(dis->chunk_liset,chunkid)==0) {
								dis->s.size += chunksize;
								dis->s.keeprsize += chunksize * sclass_keep_factor[sclass];
								dis->s.archrsize += chunksize * sclass_arch_factor[sclass];
								if (liset_check(achunk_liset,chunkid)) {
									dis->s.achunks++;
									dis->s.rsize += chunksize * sclass_arch_factor[sclass];
								} else {
									dis->s.kchunks++;
									dis->s.rsize += chunksize * sclass_keep_factor[sclass];
								}
								liset_addval(dis->chunk_liset,chunkid);
							}
						}
					}
					ch--;
				}
			} else {
				fseeko(fd,ch*8,SEEK_CUR);
			}
			fseeko(fd,sessions*4U,SEEK_CUR);
		} else if (type==TYPE_DIRECTORY) {
			for (dis=dishead ; dis!=NULL ; dis=dis->next) {
				if ((dis->status==STATUS_OK || dis->status==STATUS_ANY) && liset_check(dis->inode_liset,inode)) {
					dis->s.dirs++;
				}
			}
		}
	}
	return -1;
}

int scan_edges(FILE *fd,metasection *sdata) {
	uint8_t edgebuff[18];
	uint8_t name[256];
	const uint8_t *rptr;
	uint16_t nleng;
	uint32_t parent,child;
	uint32_t progresscnt;
	dirinfostate *dis;

	for (dis=dishead ; dis!=NULL ; dis=dis->next) {
		dis->pptr = dis->path;
		dis->inode = MFS_ROOT_ID;
		dis->parent_found = 0;
		dis->pathscan = 1;
		dis->nextsection = 1;
	}

	fseeko(fd,sdata->offset,SEEK_SET);
	fseeko(fd,8,SEEK_CUR); // skip 'nextedgeid'
	progresscnt = 0;
	while (1) {
		progresscnt++;
		if (progresscnt>=PROGRESS_PRINT) {
			progresscnt = 0;
			fprintf(stderr,"edge scan: %.2lf%%\r",100.0*(ftello(fd)-sdata->offset)/sdata->length);
			fflush(stderr);
		}
		for (dis=dishead ; dis!=NULL ; dis=dis->next) {
			if (dis->status==STATUS_OK && dis->nextsection) {
				while (*dis->pptr=='/') {
					dis->pptr++;
				}
				dis->nleng = 0;
				while (dis->pptr[dis->nleng] && dis->pptr[dis->nleng]!='/') {
					dis->nleng++;
				}
				if (dis->nleng>255) {
					dis->status = STATUS_ENOENT;
				}
				if (dis->nleng==0) {
#ifdef DEBUG
					printf("path %s : inode found: %"PRIu32"\n",dis->path,dis->inode);
#endif
					liset_addval(dis->inode_liset,dis->inode);
					if (dishead->status==STATUS_ANY) { // all node
						liset_addval(dishead->inode_liset,dis->inode);
					}
					dis->pathscan = 0;
				}
				dis->nextsection=0;
			} // section: pptr,snleng
		}
		if (fread(edgebuff,1,18,fd)!=18) {
			fprintf(stderr,"error reading metadata file\n");
			return -1;
		}
		rptr = edgebuff;
		parent = get32bit(&rptr);
		child = get32bit(&rptr);
		rptr += 8; // skip 'edgeid'
		nleng = get16bit(&rptr);
		if (parent==0 && child==0) {
			fprintf(stderr,"edge scan: 100.00%%\n");
			return 0;
		}
		if (parent==0 && nleng>MFS_PATH_MAX) {
			fprintf(stderr,"path name too long (%"PRIu16")\n",nleng);
			return -1;
		}
		if (parent!=0 && nleng>MFS_NAME_MAX) {
			fprintf(stderr,"name name too long (%"PRIu16")\n",nleng);
			return -1;
		}
		if (parent==0) {
			fseeko(fd,nleng,SEEK_CUR);
		} else {
			if (fread(name,1,nleng,fd)!=nleng) {
				fprintf(stderr,"error reading metadata file\n");
				return -1;
			}
			for (dis=dishead ; dis!=NULL ; dis=dis->next) {
				if (dis->status==STATUS_OK) {
					if (dis->pathscan) {
						if (parent==dis->inode) {
							if (nleng==dis->nleng && memcmp(name,dis->pptr,nleng)==0) {
								dis->inode = child;
								dis->nextsection = 1;
								dis->parent_found = 0;
#ifdef DEBUG
								name[nleng]=0;
								printf("found edge %u -> %u (%s)\n",parent,child,name);
#endif
								dis->pptr += nleng;
							} else {
								dis->parent_found = 1;
							}
						} else {
							if (dis->parent_found) {
								dis->status = STATUS_ENOENT;
							}
						}
					} else {
						if (liset_check(dis->inode_liset,parent)) {
							liset_addval(dis->inode_liset,child);
							if (dishead->status==STATUS_ANY) { // all node
								liset_addval(dishead->inode_liset,child);
							}
						}
					}
				}
			}
		}
	}
}

int scan_chunks(FILE *fd,metasection *sdata) {
	uint8_t buff[17];
	const uint8_t *rptr;
	uint64_t chunkid;
	uint8_t flags;
	uint32_t progresscnt;

	if (sdata->mver>0x11) {
		fprintf(stderr,"loading chunks: unsupported format\n");
		return -1;
	}
	if (sdata->mver==0x10) { // no archive bit - just do nothhing
		return 0;
	}
	fseeko(fd,sdata->offset,SEEK_SET);
	fseeko(fd,8,SEEK_CUR); // skip nextchunkid
	progresscnt = 0;
	while (1) {
		progresscnt++;
		if (progresscnt>=PROGRESS_PRINT) {
			progresscnt = 0;
			fprintf(stderr,"chunk scan: %.2lf%%\r",100.0*(ftello(fd)-sdata->offset)/sdata->length);
			fflush(stderr);
		}
		if (fread(buff,1,17,fd)!=17) {
			fprintf(stderr,"error reading metadata file\n");
			return -1;
		}
		rptr = buff;
		chunkid = get64bit(&rptr);
		rptr+=8; // skip version,lockedto
		flags = get8bit(&rptr);
		if (chunkid==0) {
			fprintf(stderr,"chunk scan: 100.00%%\n");
			return 0;
		}
		if (flags!=0) { // arch
			liset_addval(achunk_liset,chunkid);
		}
	}
}

int calc_dirinfos(FILE *fd) {
	uint8_t hdr[16];
	uint8_t mver;
	const uint8_t *rptr;
	uint64_t sleng;
	metasection edge,node,scla,chnk;
	dirinfostate *dis;

	fseeko(fd,8+16,SEEK_SET);
	edge.offset = 0;
	node.offset = 0;
	scla.offset = 0;
	chnk.offset = 0;

	// find metadata file sections
	while (1) {
		if (fread(hdr,1,16,fd)!=16) {
			printf("can't read section header\n");
			return -1;
		}
		if (memcmp(hdr,"[MFS EOF MARKER]",16)==0) {
			break;
		}
		mver = (((hdr[5]-'0')&0xF)<<4)+(hdr[7]&0xF);
		rptr = hdr+8;
		sleng = get64bit(&rptr);
#if DEBUG>1
		printf("section %c%c%c%c ; version: %c.%c ; %"PRIu64" bytes\n",hdr[0],hdr[1],hdr[2],hdr[3],hdr[5],hdr[7],sleng);
#endif
		if (memcmp(hdr,"EDGE",4)==0) {
			edge.offset = ftello(fd);
			edge.length = sleng;
			edge.mver = mver;
#ifdef DEBUG
			printf("found EDGE section (%.2lf GiB)\n",sleng/(1024.0*1024.0*1024.0));
#endif
		} else if (memcmp(hdr,"NODE",4)==0) {
			node.offset = ftello(fd);
			node.length = sleng;
			node.mver = mver;
#ifdef DEBUG
			printf("found NODE section (%.2lf GiB)\n",sleng/(1024.0*1024.0*1024.0));
#endif
		} else if (memcmp(hdr,"SCLA",4)==0 || memcmp(hdr,"LABS",4)==0) {
			scla.offset = ftello(fd);
			scla.length = sleng;
			scla.mver = mver;
#ifdef DEBUG
			printf("found SCLA section (%.2lf kB)\n",sleng/1024.0);
#endif
		} else if (memcmp(hdr,"CHNK",4)==0) {
			chnk.offset = ftello(fd);
			chnk.length = sleng;
			chnk.mver = mver;
#ifdef DEBUG
			printf("found CHNK section (%.2lf GiB)\n",sleng/(1024.0*1024.0*1024.0));
#endif
		}
		fseeko(fd,sleng,SEEK_CUR);
	}

	if (edge.offset==0) {
		fprintf(stderr,"can't find EDGE section in metadata file\n");
		return -1;
	}
	if (node.offset==0) {
		fprintf(stderr,"can't find NODE section in metadata file\n");
		return -1;
	}
	if (scla.offset==0) {
		fprintf(stderr,"can't find SCLA(SS) section in metadata file\n");
		return -1;
	}
	if (chnk.offset==0) {
		fprintf(stderr,"can't find CH(U)NK section in metadata file\n");
		return -1;
	}
	if (node.mver<0x13 || node.mver>0x14 || edge.mver!=0x11 || chnk.mver!=0x11) { // MFS 3.x
		fprintf(stderr,"unsupported metadata format (MFS 3.x needed)\n");
		return -1;
	}

	// find keep/arch factors
	fseeko(fd,scla.offset,SEEK_SET);
	if (scan_sclass(fd,&scla)<0) {
		return -1;
	}

	// build inode set
	if (scan_edges(fd,&edge)<0) {
		return -1;
	}

	for (dis = dishead ; dis!=NULL ; dis=dis->next) {
		if (dis->status==STATUS_ENOENT) {
			fprintf(stderr,"path '%s': no such file or directory\n",dis->path);
		}
	}

	// add to achunk_liset chunks in arch mode
	if (scan_chunks(fd,&chnk)<0) {
		return -1;
	}

	// build chunk set / calculate results
	if (scan_nodes(fd,&node)<0) {
		return -1;
	}

	return 0;
}

void print_result_plain(FILE *ofd) {
	dirinfostate *dis;
	fprintf(ofd,"------------------------------\n");
	for (dis = dishead ; dis!=NULL ; dis=dis->next) {
		fprintf(ofd,"path: %s\n",dis->path);
		if (dis->status!=STATUS_ENOENT) {
			fprintf(ofd,"inodes: %"PRIu64"\n",liset_card(dis->inode_liset));
			fprintf(ofd," files: %"PRIu32"\n",dis->s.files);
			fprintf(ofd," dirs: %"PRIu32"\n",dis->s.dirs);
			fprintf(ofd,"chunks: %"PRIu64"\n",liset_card(dis->chunk_liset));
			fprintf(ofd," keep chunks: %"PRIu64"\n",dis->s.kchunks);
			fprintf(ofd," arch chunks: %"PRIu64"\n",dis->s.achunks);
			fprintf(ofd,"length: %"PRIu64" = %5sB\n",dis->s.length,mfs_humanize_number(dis->s.length,PHN_USEIEC));
			fprintf(ofd,"size: %"PRIu64" = %5sB\n",dis->s.size,mfs_humanize_number(dis->s.size,PHN_USEIEC));
			fprintf(ofd,"keep size: %"PRIu64" = %5sB\n",dis->s.keeprsize,mfs_humanize_number(dis->s.keeprsize,PHN_USEIEC));
			fprintf(ofd,"arch size: %"PRIu64" = %5sB\n",dis->s.archrsize,mfs_humanize_number(dis->s.archrsize,PHN_USEIEC));
			fprintf(ofd,"real size: %"PRIu64" = %5sB\n",dis->s.rsize,mfs_humanize_number(dis->s.rsize,PHN_USEIEC));
		} else {
			fprintf(ofd,"path not found !!!\n");
		}
		fprintf(ofd,"------------------------------\n");
	}
}

void print_result_json(FILE *ofd) {
	dirinfostate *dis;
	char c;
	int j;

	fprintf(ofd,"{\n");
	for (dis = dishead ; dis!=NULL ; dis=dis->next) {
		fprintf(ofd,"\t\"");
		for (j=0 ; dis->path[j]!=0 ; j++) {
			c = dis->path[j];
			switch (c) {
				case '\b':
					fprintf(ofd,"\\b");
					break;
				case '\n':
					fprintf(ofd,"\\n");
					break;
				case '\r':
					fprintf(ofd,"\\r");
					break;
				case '\f':
					fprintf(ofd,"\\f");
					break;
				case '\t':
					fprintf(ofd,"\\t");
					break;
				case '"':
					fprintf(ofd,"\\\"");
					break;
				case '\\':
					fprintf(ofd,"\\\\");
					break;
				default:
					fputc(c,ofd);
			}
		}
		fprintf(ofd,"\": {\n");
		fprintf(ofd,"\t\t\"inodes\": %"PRIu64",\n",liset_card(dis->inode_liset));
		fprintf(ofd,"\t\t\"files\": %"PRIu32",\n",dis->s.files);
		fprintf(ofd,"\t\t\"dirs\": %"PRIu32",\n",dis->s.dirs);
		fprintf(ofd,"\t\t\"chunks\": %"PRIu64",\n",liset_card(dis->chunk_liset));
		fprintf(ofd,"\t\t\"kchunks\": %"PRIu64",\n",dis->s.kchunks);
		fprintf(ofd,"\t\t\"achunks\": %"PRIu64",\n",dis->s.achunks);
		fprintf(ofd,"\t\t\"length\": %"PRIu64",\n",dis->s.length);
		fprintf(ofd,"\t\t\"size\": %"PRIu64",\n",dis->s.size);
		fprintf(ofd,"\t\t\"rsize\": %"PRIu64",\n",dis->s.rsize);
		fprintf(ofd,"\t\t\"ksize\": %"PRIu64",\n",dis->s.keeprsize);
		fprintf(ofd,"\t\t\"asize\": %"PRIu64",\n",dis->s.archrsize);
		fprintf(ofd,"\t\t\"error\": %u\n",(dis->status!=STATUS_ENOENT)?0:1);
		fprintf(ofd,"\t}%s\n",(dis->next!=NULL)?",":"");
	}
	fprintf(ofd,"}\n");
}

void print_result_csv(FILE *ofd,char s) {
	dirinfostate *dis;
	uint8_t i;
	char c;
	int j;

	fprintf(ofd,"path%cinodes%cfiles%cdirs%cchunks%ckeep_chunks%carch_chunks%clength%csize%creal_size%ckeep_size%carch_size\n",s,s,s,s,s,s,s,s,s,s,s);
	for (dis = dishead ; dis!=NULL ; dis=dis->next) {
		fputc('\"',ofd);
		for (j=0 ; dis->path[j]!=0 ; j++) {
			c = dis->path[j];
			switch (c) {
				case '\b':
					fprintf(ofd,"\\b");
					break;
				case '\n':
					fprintf(ofd,"\\n");
					break;
				case '\r':
					fprintf(ofd,"\\r");
					break;
				case '\f':
					fprintf(ofd,"\\f");
					break;
				case '\t':
					fprintf(ofd,"\\t");
					break;
				case '"':
					fprintf(ofd,"\\\"");
					break;
				case '\\':
					fprintf(ofd,"\\\\");
					break;
				default:
					fputc(c,ofd);
			}
		}
		fputc('\"',ofd);
		if (dis->status!=STATUS_ENOENT) {
			fprintf(ofd,"%c%"PRIu64,s,liset_card(dis->inode_liset));
			fprintf(ofd,"%c%"PRIu32,s,dis->s.files);
			fprintf(ofd,"%c%"PRIu32,s,dis->s.dirs);
			fprintf(ofd,"%c%"PRIu64,s,liset_card(dis->chunk_liset));
			fprintf(ofd,"%c%"PRIu64,s,dis->s.kchunks);
			fprintf(ofd,"%c%"PRIu64,s,dis->s.achunks);
			fprintf(ofd,"%c%"PRIu64,s,dis->s.length);
			fprintf(ofd,"%c%"PRIu64,s,dis->s.size);
			fprintf(ofd,"%c%"PRIu64,s,dis->s.rsize);
			fprintf(ofd,"%c%"PRIu64,s,dis->s.keeprsize);
			fprintf(ofd,"%c%"PRIu64,s,dis->s.archrsize);
		} else {
			for (i=0 ; i<11 ; i++) {
				fprintf(ofd,"%cerror",s);
			}
		}
		fprintf(ofd,"\n");
	}
}

void usage(const char *appname) {
	printf("usage: %s [-f J|C[separator]] [-o outputfile] [-a sum_name] metadata.mfs PATH ...\n",appname);
	exit(1);
}

int main(int argc,char *argv[]) {
	uint8_t hdr[8];
	uint8_t fver;
	int indx,ret;
	dirinfostate *dis,**disp;
	FILE *fd;
	FILE *ofd;
	char *outfname;
	char *allname;
	const char *appname;
	char sep;
	int ch;
	uint8_t format;

	appname = argv[0];
	outfname = NULL;
	allname = NULL;
	sep = ',';
	format = 0;
	while ((ch=getopt(argc,argv,"f:o:a:"))>=0) {
		switch(ch) {
			case 'f':
				if (optarg[0]=='j' || optarg[0]=='J') {
					format = 2;
				} else if (optarg[0]=='c' || optarg[0]=='C') {
					format = 1;
					if (optarg[1]!=0) {
						sep = optarg[1];
					}
				} else {
					fprintf(stderr,"unrecognized format - use 'j' for JSON and 'c' for CSV\n");
					usage(appname);
					return 1;
				}
				break;
			case 'o':
				if (outfname!=NULL) {
					usage(appname);
					return 1;
				}
				outfname = strdup(optarg);
				break;
			case 'a':
				if (allname!=NULL) {
					usage(appname);
					return 1;
				}
				allname = strdup(optarg);
				break;
			default:
				usage(appname);
				return 1;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc<1) {
		usage(appname);
		return 1;
	}

	ret = 1;
	ofd = NULL;
	fd = NULL;

	if (outfname!=NULL) {
		ofd = fopen(outfname,"w");
		if (ofd==NULL) {
			fprintf(stderr,"error opening output file '%s'\n",outfname);
			goto error;
		}
	} else {
		ofd = stdout;
	}

	fd = fopen(argv[0],"rb");
	if (fd==NULL) {
		fprintf(stderr,"error opening metadata file '%s'\n",argv[0]);
		goto error;
	}
	if (fread(hdr,1,8,fd)!=8) {
		fprintf(stderr,"%s: can't read metadata header\n",argv[0]);
		goto error;
	}
	if (memcmp(hdr,"MFSM NEW",8)==0) {
		fprintf(stderr,"%s: empty file\n",argv[0]);
		goto error;
	}
	if (memcmp(hdr,MFSSIGNATURE "M ",5)==0 && hdr[5]>='1' && hdr[5]<='9' && hdr[6]=='.' && hdr[7]>='0' && hdr[7]<='9') {
		fver = ((hdr[5]-'0')<<4)+(hdr[7]-'0');
	} else {
		fprintf(stderr,"%s: unrecognized file format\n",argv[0]);
		goto error;
	}
	if (fver<0x20) {
		fprintf(stderr,"%s: metadata file format too old %u.%u\n",argv[0],(unsigned)(fver>>4),(unsigned)(fver&0xF));
		goto error;
	}
	dishead = NULL;
	disp = &dishead;
	if (allname!=NULL) {
		dis = malloc(sizeof(dirinfostate));
		memset(dis,0,sizeof(dirinfostate));
		dis->inode_liset = liset_new();
		dis->chunk_liset = liset_new();
		dis->path = strdup(allname);
		dis->next = NULL;
		dis->status = STATUS_ANY;
		*disp = dis;
		disp = &(dis->next);
	}
	for (indx=1 ; indx<argc ; indx++) {
		dis = malloc(sizeof(dirinfostate));
		memset(dis,0,sizeof(dirinfostate));
		dis->inode_liset = liset_new();
		dis->chunk_liset = liset_new();
		dis->path = strdup(argv[indx]);
		dis->next = NULL;
		*disp = dis;
		disp = &(dis->next);
	}
	achunk_liset = liset_new();
	ret = 0;
	if (calc_dirinfos(fd)<0) {
		ret = 1;
	}
	fclose(fd);
	fd = NULL;
	if (ret==0) {
		if (format==2) {
			print_result_json(ofd);
		} else if (format==1) {
			print_result_csv(ofd,sep);
		} else {
			print_result_plain(ofd);
		}
	}
	liset_remove(achunk_liset);
	while (dishead!=NULL) {
		dis = dishead;
		liset_remove(dis->inode_liset);
		liset_remove(dis->chunk_liset);
		free(dis->path);
		dishead = dis->next;
		free(dis);
	}
error:
	if (fd!=NULL) {
		fclose(fd);
	}
	if (outfname!=NULL) {
		if (ofd!=NULL) {
			fclose(ofd);
		}
		free(outfname);
	}
	if (allname!=NULL) {
		free(allname);
	}
	return ret;
}
