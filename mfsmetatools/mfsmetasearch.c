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
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "datapack.h"
#include "MFSCommunication.h"
#include "searchexpr.h"
#include "liset64.h"
#include "idstr.h"

static void *expr;
static int chunkid_set;

#define PROGRESS_PRINT 100000

#define PHASHSIZE 0x1000000

typedef struct _parents {
	uint32_t inode;
	uint32_t parent;
	uint8_t show;
	uint8_t type;
	char *name;
	struct _parents *sibling;
	struct _parents *next;
} parents;

static parents* parenthash[PHASHSIZE];

static parents* pnode_find(uint32_t inode) {
	parents *p;
	uint32_t hash;
	hash = inode % PHASHSIZE;
	for (p=parenthash[hash] ; p!=NULL ; p=p->next) {
		if (p->inode==inode) {
			return p;
		}
	}
	return NULL;
}

static parents* pnode_create(uint32_t inode,uint8_t type) {
	parents *p;
	uint32_t hash;
	hash = inode % PHASHSIZE;
	for (p=parenthash[hash] ; p!=NULL ; p=p->next) {
		if (p->inode==inode) {
			return p;
		}
	}
	p = malloc(sizeof(parents));
	p->inode = inode;
	p->type = type;
	p->parent = 0;
	p->show = 0;
	p->name = NULL;
	p->sibling = NULL;
	p->next = parenthash[hash];
	parenthash[hash] = p;
	return p;
}

typedef struct _metasection {
	off_t offset;
	uint64_t length;
	uint8_t mver;
} metasection;


#define EDGEBLOCKSIZE 1000000

typedef struct _edgemarker {
	uint64_t pos;
	uint32_t cnt;
	struct _edgemarker *next;
} edgemarker;

static edgemarker *emarkerhead;
static uint32_t edgecount;

typedef struct _edgestackel {
	uint32_t parent;
	uint32_t child;
	char *name;
	struct _edgestackel *next;
} edgestackel;

static edgestackel *edgestack;

void process_edge(edgestackel *ese) {
	parents *p,*s;
	p = pnode_find(ese->child);
	if (p!=NULL) {
		if (ese->parent!=MFS_ROOT_ID) {
			pnode_create(ese->parent,TYPE_DIRECTORY);
		}
		if (p->parent==0) { // first parent
			p->parent = ese->parent;
			p->name = ese->name;
			ese->name = NULL;
		} else {
			if (p->show) {
				s = malloc(sizeof(parents));
				s->inode = p->inode;
				s->type = p->type;
				s->parent = ese->parent;
				s->show = 1;
				s->name = ese->name;
				ese->name = NULL;
				s->sibling = p->sibling;
				p->sibling = s;
			} else {
				fprintf(stderr,"duplicated edge detected (%"PRIu32"->%"PRIu32" ; name:%s) - ignoring\n",ese->parent,ese->child,ese->name);
			}
		}
	}
}

// fill edge markers
int scan_edges_pass1(FILE *fd,metasection *sdata) {
	uint8_t edgebuff[18];
	const uint8_t *rptr;
	uint16_t nleng;
	uint32_t parent,child;
	uint32_t progresscnt;
	edgemarker *em;
	uint32_t csize;

	fseeko(fd,sdata->offset,SEEK_SET);
	fseeko(fd,8,SEEK_CUR); // skip 'nextedgeid'
	progresscnt = 0;
	em = NULL;
	csize = 0;
	emarkerhead = NULL;
	edgecount = 0;
	while (1) {
		if (em==NULL) {
			em = malloc(sizeof(edgemarker));
			em->pos = ftello(fd);
			em->cnt = 0;
			em->next = emarkerhead;
			emarkerhead = em;
			csize = 0;
		}
		progresscnt++;
		if (progresscnt>=PROGRESS_PRINT) {
			progresscnt = 0;
			fprintf(stderr,"edge scan (pass1): %.2lf%%\r",100.0*(ftello(fd)-sdata->offset)/sdata->length);
			fflush(stderr);
		}
		if (fread(edgebuff,1,18,fd)!=18) {
			fprintf(stderr,"error reading metadata file (edge data)\n");
			return -1;
		}
		rptr = edgebuff;
		parent = get32bit(&rptr);
		child = get32bit(&rptr);
		rptr += 8; // skip 'edgeid'
		nleng = get16bit(&rptr);
		if (parent==0 && child==0) {
			fprintf(stderr,"edge scan (pass1): 100.00%%\n");
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
		fseeko(fd,nleng,SEEK_CUR); // skip name
		em->cnt++;
		edgecount++;
		csize += 18+nleng;
		if (csize > EDGEBLOCKSIZE) {
			em = NULL;
		}
	}
}

// read edges backwards
int scan_edges_pass2(FILE *fd) {
	uint8_t edgebuff[18];
	const uint8_t *rptr;
	uint16_t nleng;
	uint32_t parent,child;
	edgemarker *emp;
	edgestackel *ese;
	uint32_t progresscnt;

	progresscnt = 0;
	while (emarkerhead!=NULL) {
		emp = emarkerhead;
		edgestack = NULL;
		fseek(fd,emp->pos,SEEK_SET);
		progresscnt += emp->cnt;
		while (emp->cnt>0) {
			if (fread(edgebuff,1,18,fd)!=18) {
				fprintf(stderr,"error reading metadata file (edge data)\n");
				return -1;
			}
			rptr = edgebuff;
			parent = get32bit(&rptr);
			child = get32bit(&rptr);
			rptr += 8; // skip 'edgeid'
			nleng = get16bit(&rptr);
//			if (parent==0) {
//				fseeko(fd,nleng,SEEK_CUR);
//			} else {
				ese = malloc(sizeof(edgestackel));
				ese->parent = parent;
				ese->child = child;
				ese->name = malloc(nleng+1);
				if (fread(ese->name,1,nleng,fd)!=nleng) {
					fprintf(stderr,"error reading metadata file (edge name)\n");
					return -1;
				}
				ese->name[nleng]=0;
				ese->next = edgestack;
				edgestack = ese;
//			}
			emp->cnt--;
		}
		while (edgestack!=NULL) {
			ese = edgestack;
			process_edge(ese);
			if (ese->name!=NULL) {
				free(ese->name);
			}
			edgestack = ese->next;
			free(ese);
		}
		fprintf(stderr,"edge scan (pass2): %.2lf%%\r",(100.0*progresscnt)/edgecount);
		fflush(stderr);
		emarkerhead = emp->next;
		free(emp);
	}
	fprintf(stderr,"edge scan (pass2): 100.00%%\n");
	return 0;
}

// mver 0x13 and 0x14
int scan_nodes(FILE *fd,metasection *sdata) {
	uint8_t buff[46];
	const uint8_t *rptr;
	uint8_t type;
	uint32_t rsize;
	uint32_t ch;
	uint16_t sessions;
	uint8_t showinode;
	uint32_t progresscnt;
	parents *p;
	inodestr i;

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
		showinode = 0;
		memset(&i,0,sizeof(inodestr));
		type = fgetc(fd);
		if (type==0) { // end of section
			fprintf(stderr,"node scan: 100.00%%\n");
			return 0; // ok
		}
		if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV || type==TYPE_SYMLINK) {
			if (sdata->mver<=0x13) {
				rsize = 36;
			} else {
				rsize = 35;
			}
		} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
			if (sdata->mver<=0x13) {
				rsize = 46;
			} else {
				rsize = 43;
			}
		} else {
			if (sdata->mver<=0x13) {
				rsize = 32;
			} else {
				rsize = 31;
			}
		}
		if (fread(buff,1,rsize,fd)!=rsize) {
			fprintf(stderr,"error reading metadata file (node data)\n");
			return -1;
		}
		rptr = buff;
		i.inode = get32bit(&rptr);
//		printf("inode: %u ; type: %u\n",i.inode,type);
		if (expr!=NULL) {
			i.type = type;
			i.sclass = get8bit(&rptr);
			i.flags = get8bit(&rptr);
			if (sdata->mver>=0x14) {
				i.winattr = get8bit(&rptr);
			} else {
				i.winattr = 0;
			}
			i.mode = get16bit(&rptr);
			i.uid = get32bit(&rptr);
			i.gid = get32bit(&rptr);
			i.atime = get32bit(&rptr);
			i.mtime = get32bit(&rptr);
			i.ctime = get32bit(&rptr);
			if (sdata->mver>=0x14) {
				i.trashretention = get16bit(&rptr);
			} else {
				i.trashretention = get32bit(&rptr)/3600;
			}
			if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
				i.major = get16bit(&rptr);
				i.minor = get16bit(&rptr);
				showinode = expr_check(expr,&i);
			} else if (type==TYPE_SYMLINK) {
				uint32_t pleng;
				pleng = get32bit(&rptr);
				fseeko(fd,pleng,SEEK_CUR);
				showinode = expr_check(expr,&i);
			} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
				i.length = get64bit(&rptr);
				ch = get32bit(&rptr);
				if (sdata->mver<=0x13) {
					sessions = get16bit(&rptr);
				} else {
					sessions = 0;
				}
				if (expr_useschunkid(expr)) {
					while (ch) {
						if (fread(buff,1,8,fd)!=8) {
							fprintf(stderr,"error reading metadata file (chunkid)\n");
							return -1;
						}
						rptr = buff;
						i.chunkid = get64bit(&rptr);
						if (i.chunkid>0) {
							showinode |= expr_check(expr,&i);
						}
						ch--;
					}
				} else {
					showinode = expr_check(expr,&i);
					fseeko(fd,ch*8,SEEK_CUR);
				}
				fseeko(fd,sessions*4U,SEEK_CUR);
			} else {
				showinode = expr_check(expr,&i);
			}
		} else { // expr == NULL
			if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_SUSTAINED) {
				if (sdata->mver<=0x13) {
					rptr = buff + 40;
					ch = get32bit(&rptr);
					sessions = get16bit(&rptr);
				} else {
					rptr = buff + 39;
					ch = get32bit(&rptr);
					sessions = 0;
				}
				while (ch && showinode==0) {
					if (fread(buff,1,8,fd)!=8) {
						fprintf(stderr,"error reading metadata file (chunkid)\n");
						return -1;
					}
					rptr = buff;
					i.chunkid = get64bit(&rptr);
					if (liset_check(chunkid_set,i.chunkid)) {
						showinode = 1;
					}
					ch--;
				}
				if (ch>0) {
					fseeko(fd,ch*8,SEEK_CUR);
				}
				fseeko(fd,sessions*4U,SEEK_CUR);
			}
		}
		if (showinode) {
			p = pnode_create(i.inode,type);
			p->show = 1;
		}
	}
	return -1;
}

int get_data(FILE *fd) {
	uint8_t hdr[16];
	uint8_t mver;
	const uint8_t *rptr;
	uint64_t sleng;
	metasection edge,node; //,scla;

	fseeko(fd,8+16,SEEK_SET);
	edge.offset = 0;
	node.offset = 0;
	edge.mver = 0;
	node.mver = 0;
	edge.length = 0;
	node.length = 0;
//	scla.offset = 0;

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
/*
		} else if (memcmp(hdr,"SCLA",4)==0 || memcmp(hdr,"LABS",4)==0) {
			scla.offset = ftello(fd);
			scla.length = sleng;
			scla.mver = mver;
#ifdef DEBUG
			printf("found SCLA section (%.2lf kB)\n",sleng/1024.0);
#endif
*/
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
/*
	if (scla.offset==0) {
		fprintf(stderr,"can't find SCLA(SS) section in metadata file\n");
		return -1;
	}
*/
	if (node.mver<0x13 || node.mver>0x14 || edge.mver!=0x11) { // MFS >= 3.x
		fprintf(stderr,"unsupported metadata format (MFS 3.x or newer is needed)\n");
		return -1;
	}

	// prepare leaf nodes
	if (scan_nodes(fd,&node)<0) {
		return -1;
	}

	// prepare markers
	if (scan_edges_pass1(fd,&edge)<0) {
		return -1;
	}

	// read edges backwards
	if (scan_edges_pass2(fd)<0) {
		return -1;
	}

	return 0;
}

void print_escaped(FILE *ofd,const char *str) {
	char c;
	while ((c=*str++)!='\0') {
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
}

void print_path(FILE *ofd,parents *p,uint8_t escape) {
	static char **names=NULL;
	static uint32_t nsize=0;
	uint32_t pos;
	uint8_t isdir;
//	parents *p;

	isdir = (p!=NULL && p->type==TYPE_DIRECTORY)?1:0;

	pos = 0;
	while (p!=NULL && p->inode!=MFS_ROOT_ID) {
		if (pos>=nsize) {
			nsize += 50;
			if (names==NULL) {
				names = malloc(sizeof(char*)*nsize);
			} else {
				names = realloc(names,sizeof(char*)*nsize);
			}
		}
		names[pos++] = p->name;
		p = pnode_find(p->parent);
	}
	while (pos>0) {
		pos--;
		if (escape) {
			fputc('/',ofd);
			print_escaped(ofd,names[pos]);
		} else {
			fprintf(ofd,"/%s",names[pos]);
		}
	}
	if (isdir) {
		fputc('/',ofd);
	}
}

const char* type2str(uint8_t type) {
	switch (type) {
		case TYPE_DIRECTORY:
			return "directory";
		case TYPE_SOCKET:
			return "socket";
		case TYPE_FIFO:
			return "fifo";
		case TYPE_BLOCKDEV:
			return "blockdev";
		case TYPE_CHARDEV:
			return "chardev";
		case TYPE_SYMLINK:
			return "symlink";
		case TYPE_FILE:
			return "file";
		case TYPE_TRASH:
			return "file";
		case TYPE_SUSTAINED:
			return "file";
		default:
			return "#TYPE?";
	}
}

void print_result_plain(FILE *ofd) {
	uint32_t h;
	parents *p,*s;

	fprintf(ofd,"------------------------------\n");
	for (h=0 ; h<PHASHSIZE ; h++) {
		for (p=parenthash[h] ; p!=NULL ; p=p->next) {
			if (p->show) {
				s = p;
				while (s!=NULL) {
					fprintf(ofd,"inode: %u ; type: %s ; path: ",s->inode,type2str(s->type));
					if (s->type==TYPE_TRASH) {
						fprintf(ofd,"[TRASH] %s\n",s->name);
					} else if (s->type==TYPE_SUSTAINED) {
						fprintf(ofd,"[SUSTAINED] %s\n",s->name);
					} else {
						print_path(ofd,s,0);
						fprintf(ofd,"\n");
					}
					s = s->sibling;
				}
			}
		}
	}
}

void print_result_json(FILE *ofd) {
	uint32_t h;
	parents *p,*s;
	uint8_t needcomma;

	fprintf(ofd,"[");
	needcomma = 0;
	for (h=0 ; h<PHASHSIZE ; h++) {
		for (p=parenthash[h] ; p!=NULL ; p=p->next) {
			if (p->show) {
				if (needcomma) {
					fputc(',',ofd);
				}
				needcomma = 1;
				fprintf(ofd,"\n\t{\n\t\t\"inode\": %"PRIu32",\n\t\t\"type\": \"%s\",\n\t\t\"paths\": [",p->inode,type2str(p->type));
				s = p;
				while (s!=NULL) {
					fputc('"',ofd);
					if (s->type==TYPE_TRASH) {
						fprintf(ofd,"[TRASH] ");
						print_escaped(ofd,s->name);
					} else if (s->type==TYPE_SUSTAINED) {
						fprintf(ofd,"[SUSTAINED] ");
						print_escaped(ofd,s->name);
					} else {
						print_path(ofd,s,1);
					}
					fputc('"',ofd);
					if (s->sibling) {
						fputc(',',ofd);
					}
					s = s->sibling;
				}
				fprintf(ofd,"]\n");
				fprintf(ofd,"\t}");
			}
		}
	}
	fprintf(ofd,"\n]\n");
}

void print_result_csv(FILE *ofd,char sep) {
	uint32_t h;
	parents *p,*s;

	fprintf(ofd,"inode%ctype%cpaths\n",sep,sep);
	for (h=0 ; h<PHASHSIZE ; h++) {
		for (p=parenthash[h] ; p!=NULL ; p=p->next) {
			if (p->show) {
				s = p;
				while (s!=NULL) {
					fprintf(ofd,"%u%c%s%c",s->inode,sep,type2str(s->type),sep);
					if (s->type==TYPE_TRASH) {
						fprintf(ofd,"[TRASH] %s\n",s->name);
					} else if (s->type==TYPE_SUSTAINED) {
						fprintf(ofd,"[SUSTAINED] %s\n",s->name);
					} else {
						print_path(ofd,s,0);
						fprintf(ofd,"\n");
					}
					s = s->sibling;
				}
			}
		}
	}
}

int chunkid_load(const char *fname) {
	FILE *fd;
	char *lbuff;
	size_t lbsize;
	char *e;
	uint64_t chunkid;

	fd = fopen(fname,"r");
	if (fd==NULL) {
		return -1;
	}

	lbsize = 1024;
	lbuff = malloc(lbsize);
	while (getline(&lbuff,&lbsize,fd)!=-1) {
		e = lbuff;
		while (*e==' ' || *e=='\t') { // skip whites on the left side
			e++;
		}
		if (*e=='\r' || *e=='\n' || *e=='#' || *e==';') { // empty line or comment - just continue with next line
			continue;
		}
		if (e[0]=='0' && e[1]=='x') { // skip leading '0x' prefix
			e+=2;
		}
		chunkid = strtoul(e,&e,16);
		while (*e==' ' || *e=='\t') { // skip whites on the right side
			e++;
		}
		if (*e=='\r' || *e=='\n' || *e=='#' || *e==';') {
			liset_addval(chunkid_set,chunkid & UINT64_C(0x00FFFFFFFFFFFFFF));
		} else {
			fprintf(stderr,"parse error on line: %s",lbuff);
			free(lbuff);
			fclose(fd);
			return -1;
		}
	}
	free(lbuff);
	fclose(fd);
	return 0;
}

void usage(const char *appname) {
	printf("Finds files in metadata that match provided expression or chunkid file.\n");
	printf("\n");
	printf("usage: %s [-f J|C[separator]] [-o outputfile] [-e expr | -c chunkid_file] metadata.mfs\n\n",appname);
	printf("expr symbols:\n");
	printf("\tinode - inode number\n");
	printf("\ttype - inode type (file,directory,symlink,fifo,blockdev,chardev,socket,trash,sustained)\n");
	printf("\teattr - extra attributes (noowner,noattrcache,noentrycache,nodatacache,snapshot,undeletable,appendonly,immutable)\n");
	printf("\tsclass - number of storage class\n");
	printf("\tuid - user id\n");
	printf("\tgid - group id\n");
	printf("\tmode - access mode (sticky,sgid,suid,ur,uw,ux,gr,gw,gx,or,ow,ox)\n");
	printf("\tumode - access mode for uid (read,write,execute)\n");
	printf("\tgmode - access mode for gid (read,write,execute)\n");
	printf("\tomode - access mode for others (read,write,execute)\n");
	printf("\tatime - access time (unix timestamp in seconds)\n");
	printf("\tmtime - modify time (unix timestamp in seconds)\n");
	printf("\tctime - change time (unix timestamp in seconds)\n");
	printf("\ttretention - trash retention in hours\n");
	printf("\tlength - file length (zero for other objects)\n");
	printf("\tmajor - major id (for blockdev and chardev)\n");
	printf("\tminor - minor id (for blockdev and chardev)\n");
	printf("\tchunkid - number of chunk (expression will be tested for all file chunks and accepted if one of them is true)\n");
	exit(1);
}

int main(int argc,char *argv[]) {
	uint8_t hdr[8];
	uint8_t fver;
	int ret;
	FILE *fd;
	FILE *ofd;
	char *outfname;
	char *chunkidfname;
	char *exprstr;
	const char *appname;
	char sep;
	int ch;
	uint8_t format;

	expr = NULL;
	chunkid_set = -1;
	ret = 1;
	ofd = NULL;
	fd = NULL;

	chunkidfname = NULL;
	exprstr = NULL;

	appname = argv[0];
	outfname = NULL;
	sep = ',';
	format = 0;
	while ((ch=getopt(argc,argv,"f:o:c:e:"))>=0) {
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
					goto error;
				}
				break;
			case 'o':
				if (outfname!=NULL) {
					usage(appname);
					goto error;
				}
				outfname = strdup(optarg);
				break;
			case 'c':
				if (chunkidfname!=NULL) {
					usage(appname);
					goto error;
				}
				chunkidfname = strdup(optarg);
				break;
			case 'e':
				if (exprstr!=NULL) {
					usage(appname);
					goto error;
				}
				exprstr = strdup(optarg);
				break;
			default:
				usage(appname);
				goto error;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc!=1) {
		usage(appname);
		goto error;
	}

	if (exprstr==NULL && chunkidfname==NULL) {
		fprintf(stderr,"option '-e' or '-c' should be given\n");
		usage(appname);
		goto error;
	}

	if (exprstr!=NULL && chunkidfname!=NULL) {
		fprintf(stderr,"options '-e' and '-c' are mutually exclusive\n");
		usage(appname);
		goto error;
	}

	if (exprstr!=NULL) {
		expr = expr_new(exprstr);

		if (expr==NULL) {
			usage(appname);
			goto error;
		}

		if (outfname!=NULL) {
			printf("parsed expr: ");
			expr_print(expr);
		}
	} else {
		chunkid_set = liset_new();

		if (chunkid_load(chunkidfname)<0) {
			fprintf(stderr,"can't load chunk ids from file '%s'\n",chunkidfname);
			goto error;
		}

		printf("%"PRIu64" chunk ids have been loaded\n",liset_card(chunkid_set));
	}

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
	ret = 0;
	if (get_data(fd)<0) {
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
error:
	if (chunkid_set>=0) {
		liset_remove(chunkid_set);
	}
	if (expr!=NULL) {
		expr_free(expr);
	}
	if (fd!=NULL) {
		fclose(fd);
	}
	if (outfname!=NULL) {
		if (ofd!=NULL) {
			fclose(ofd);
		}
		free(outfname);
	}
	return ret;
}
