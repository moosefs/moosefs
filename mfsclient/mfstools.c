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

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <errno.h>

#include "labelparser.h"
#include "datapack.h"
#include "massert.h"
#include "strerr.h"
#include "mfsstrerr.h"
#include "sockets.h"
#include "hashfn.h"
#include "liset64.h"
#include "clocks.h"
#include "md5.h"
#include "MFSCommunication.h"
#include "idstr.h"

#include "mfstools_master.h"

#define tcpread(s,b,l) tcptoread(s,b,l,20000)
#define tcpwrite(s,b,l) tcptowrite(s,b,l,20000)

#define FILEINFO_QUICK 0x01
#define FILEINFO_CRC 0x02
#define FILEINFO_SIGNATURE 0x04

#define DIRINFO_INODES 0x01
#define DIRINFO_DIRS 0x02
#define DIRINFO_FILES 0x04
#define DIRINFO_CHUNKS 0x08
#define DIRINFO_LENGTH 0x10
#define DIRINFO_SIZE 0x20
#define DIRINFO_REALSIZE 0x40
#define DIRINFO_PRECISE 0x80

#define INODE_VALUE_MASK 0x1FFFFFFF
#define INODE_TYPE_MASK 0x60000000
#define INODE_TYPE_TRASH 0x20000000
#define INODE_TYPE_SUSTAINED 0x40000000
#define INODE_TYPE_SPECIAL 0x00000000

static const char* eattrtab[EATTR_BITS]={EATTR_STRINGS};
static const char* eattrdesc[EATTR_BITS]={EATTR_DESCRIPTIONS};

static uint8_t humode=0;
static uint8_t numbermode=0;

#define PHN_USESI       0x01
#define PHN_USEIEC      0x00
void print_humanized_number(const char *format,uint64_t number,uint8_t flags) {
	char numbuf[6];	// [ "xxx" , "xx" , "x" , "x.x" ] + ["" , "X" , "Xi"]
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
	printf(format,numbuf);
}

void print_number_only(uint64_t number,uint8_t bytesflag) {
	if (humode>0) {
		if (bytesflag) {
			if (humode==1 || humode==3) {
				print_humanized_number("%5sB",number,PHN_USEIEC);
			} else {
				print_humanized_number("%4sB",number,PHN_USESI);
			}
		} else {
			if (humode==1 || humode==3) {
				print_humanized_number("%5s",number,PHN_USEIEC);
			} else {
				print_humanized_number("%4s",number,PHN_USESI);
			}
		}
		if (humode>2) {
			printf(" (%"PRIu64")",number);
		}
	} else {
		if (numbermode==0) {
			printf("%"PRIu64,number);
		} else if (numbermode==1) {
			printf("%"PRIu64,number/1024);
		} else if (numbermode==2) {
			printf("%"PRIu64,number/(1024*1024));
		} else if (numbermode==3) {
			printf("%"PRIu64,number/(1024*1024*1024));
		}
	}
}

void print_number(const char *prefix,const char *suffix,uint64_t number,uint8_t mode32,uint8_t bytesflag,uint8_t dflag) {
	if (prefix) {
		printf("%s",prefix);
	}
	if (dflag) {
		if (humode>0) {
			if (bytesflag) {
				if (humode==1 || humode==3) {
					print_humanized_number("%5sB",number,PHN_USEIEC);
				} else {
					print_humanized_number("%4sB",number,PHN_USESI);
				}
			} else {
				if (humode==1 || humode==3) {
					print_humanized_number(" %5s",number,PHN_USEIEC);
				} else {
					print_humanized_number(" %4s",number,PHN_USESI);
				}
			}
			if (humode>2) {
				if (mode32) {
					printf(" (%10"PRIu32")",(uint32_t)number);
				} else {
					printf(" (%20"PRIu64")",number);
				}
			}
		} else {
			if (numbermode==0) {
				if (mode32) {
					printf("%10"PRIu32,(uint32_t)number);
				} else {
					printf("%20"PRIu64,number);
				}
			} else if (numbermode==1) {
				if (mode32) {
					printf("%7"PRIu32,((uint32_t)number)/1024);
				} else {
					printf("%17"PRIu64,number/1024);
				}
			} else if (numbermode==2) {
				if (mode32) {
					printf("%4"PRIu32,((uint32_t)number)/(1024*1024));
				} else {
					printf("%14"PRIu64,number/(1024*1024));
				}
			} else if (numbermode==3) {
				if (mode32) {
					printf("%1"PRIu32,((uint32_t)number)/(1024*1024*1024));
				} else {
					printf("%11"PRIu64,number/(1024*1024*1024));
				}
			}
		}
	} else {
		switch(humode) {
		case 0:
			if (numbermode==0) {
				if (mode32) {
					printf("         -");
				} else {
					printf("                   -");
				}
			} else if (numbermode==1) {
				if (mode32) {
					printf("      -");
				} else {
					printf("                -");
				}
			} else if (numbermode==2) {
				if (mode32) {
					printf("   -");
				} else {
					printf("             -");
				}
			} else if (numbermode==3) {
				if (mode32) {
					printf("-");
				} else {
					printf("          -");
				}
			}
			break;
		case 1:
			printf("     -");
			break;
		case 2:
			printf("    -");
			break;
		case 3:
			if (mode32) {
				printf("                  -");
			} else {
				printf("                            -");
			}
			break;
		case 4:
			if (mode32) {
				printf("                 -");
			} else {
				printf("                           -");
			}
			break;
		}
	}
	if (suffix) {
		printf("%s",suffix);
	}
}

int my_get_number(const char *str,uint64_t *ret,uint64_t max,uint8_t bytesflag) {
	uint64_t val,frac,fracdiv;
	double drval,mult;
	int f;
	val=0;
	frac=0;
	fracdiv=1;
	f=0;
	while (*str>='0' && *str<='9') {
		f=1;
		val*=10;
		val+=(*str-'0');
		str++;
	}
	if (*str=='.') {	// accept ".5" (without 0)
		str++;
		while (*str>='0' && *str<='9') {
			fracdiv*=10;
			frac*=10;
			frac+=(*str-'0');
			str++;
		}
		if (fracdiv==1) {	// if there was '.' expect number afterwards
			return -1;
		}
	} else if (f==0) {	// but not empty string
		return -1;
	}
	if (str[0]=='\0' || (bytesflag && str[0]=='B' && str[1]=='\0')) {
		mult=1.0;
	} else if (str[0]!='\0' && (str[1]=='\0' || (bytesflag && str[1]=='B' && str[2]=='\0'))) {
		switch(str[0]) {
		case 'k':
			mult=1e3;
			break;
		case 'M':
			mult=1e6;
			break;
		case 'G':
			mult=1e9;
			break;
		case 'T':
			mult=1e12;
			break;
		case 'P':
			mult=1e15;
			break;
		case 'E':
			mult=1e18;
			break;
		default:
			return -1;
		}
	} else if (str[0]!='\0' && str[1]=='i' && (str[2]=='\0' || (bytesflag && str[2]=='B' && str[3]=='\0'))) {
		switch(str[0]) {
		case 'K':
			mult=1024.0;
			break;
		case 'M':
			mult=1048576.0;
			break;
		case 'G':
			mult=1073741824.0;
			break;
		case 'T':
			mult=1099511627776.0;
			break;
		case 'P':
			mult=1125899906842624.0;
			break;
		case 'E':
			mult=1152921504606846976.0;
			break;
		default:
			return -1;
		}
	} else {
		return -1;
	}
	drval = round(((double)frac/(double)fracdiv+(double)val)*mult);
	*ret = drval;
	if (drval>max || ((*ret)==0 && drval>1.0)) { // when max==UINT64_MAX and drval==2^64 then drval>max is false because common type for uint64_t and double is double and we lost precision here - therefore the second condition
		return -2;
	}
	return 1;
}

int bsd_basename(const char *path,char *bname) {
	const char *endp, *startp;

	/* Empty or NULL string gets treated as "." */
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* All slashes becomes "/" */
	if (endp == path && *endp == '/') {
		(void)strcpy(bname, "/");
		return 0;
	}

	/* Find the start of the base */
	startp = endp;
	while (startp > path && *(startp - 1) != '/') {
		startp--;
	}
	if (endp - startp + 2 > PATH_MAX) {
		return -1;
	}
	(void)strncpy(bname, startp, endp - startp + 1);
	bname[endp - startp + 1] = '\0';
	return 0;
}

int bsd_dirname(const char *path,char *bname) {
	const char *endp;

	/* Empty or NULL string gets treated as "." */
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* Find the start of the dir */
	while (endp > path && *endp != '/') {
		endp--;
	}

	/* Either the dir is "/" or there are no slashes */
	if (endp == path) {
		(void)strcpy(bname, *endp == '/' ? "/" : ".");
		return 0;
	} else {
		do {
			endp--;
		} while (endp > path && *endp == '/');
	}

	if (endp - path + 2 > PATH_MAX) {
		return -1;
	}
	(void)strncpy(bname, path, endp - path + 1);
	bname[endp - path + 1] = '\0';
	return 0;
}

void dirname_inplace(char *path) {
	char *endp;

	if (path==NULL) {
		return;
	}
	if (path[0]=='\0') {
		path[0]='.';
		path[1]='\0';
		return;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* Find the start of the dir */
	while (endp > path && *endp != '/') {
		endp--;
	}

	if (endp == path) {
		if (path[0]=='/') {
			path[1]='\0';
		} else {
			path[0]='.';
			path[1]='\0';
		}
		return;
	} else {
		*endp = '\0';
	}
}

int inode_liset;
int chunk_liset;

/* ---- INODE QUEUE ---- */

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
	if (liset_addval(inode_liset,inode)==0) { // new element
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

static inline int parse_slice_expr(char *p,int64_t *slice_from,int64_t *slice_to) {
	if (*p==':') {
		*slice_from = 0;
		p++;
	} else {
		*slice_from = strtoll(p,&p,10);
		if (*p!=':') {
			return -1;
		}
		p++;
	}
	if (*p=='\0') {
		*slice_to = 0x80000000;
	} else {
		*slice_to = strtoll(p,&p,10);
	}
	if (*p!='\0') {
		return -1;
	}
	return 0;
}

// formats:
//  #     - number of seconds
//  #s    - number of seconds
//  #.#m  - number of minutes
//  #.#h  - number of hours
//  #.#d  - number of days
//  #.#w  - number of weeks
static inline uint32_t parse_period(char *str,char **endpos) {
	double base;
	double divisor;
	base = 0.0;
	while ((*str)>='0' && (*str)<='9') {
		base *= 10.0;
		base += (*str)-'0';
		str++;
	}
	if ((*str)=='.') {
		divisor = 0.1;
		str++;
		while ((*str)>='0' && (*str)<='9') {
			base += ((*str)-'0')*divisor;
			divisor /= 10.0;
			str++;
		}
	}
	while ((*str)==' ') {
		str++;
	}
	if ((*str)=='s') {
		str++;
	} else if ((*str)=='m') {
		str++;
		base *= 60.0;
	} else if ((*str)=='h') {
		str++;
		base *= 3600.0;
	} else if ((*str)=='d') {
		str++;
		base *= 86400.0;
	} else if ((*str)=='w') {
		str++;
		base *= 604800.0;
	}
	*endpos = str;
	if (base >= UINT32_MAX) {
		return UINT32_MAX;
	}
	if (base <= 0) {
		return 0;
	}
	return base;
}

int file_paths(const char* fname) {
	const char *p;
	struct stat st;
	char cwdbuff[MAXPATHLEN];
	uint32_t arginode;
	uint32_t inode;
	uint32_t leng,pleng;

	p = fname;
	while (*p>='0' && *p<='9') {
		p++;
	}

	if (*p=='\0' && stat(fname,&st)<0 && errno==ENOENT) {
		arginode = strtoul(fname,NULL,10);
		p = getcwd(cwdbuff,MAXPATHLEN);
		if (master_prepare_conn(p,&inode,NULL,NULL,0,0)<0) {
			return -1;
		}
		inode = arginode;
	} else {
		if (master_prepare_conn(fname,&inode,NULL,NULL,0,0)<0) {
			return -1;
		}
	}
	master_new_packet();
	master_put32bit(inode);
	if (master_send_and_receive(CLTOMA_FUSE_PATHS,MATOCL_FUSE_PATHS)<0) {
		return -1;
	}
	leng = master_get_leng();
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	}
	printf("%s:\n",fname);
	while (leng>=4) {
		pleng = master_get32bit();
		leng-=4;
		if (leng>=pleng) {
			while (pleng) {
				putchar(master_get8bit());
				pleng--;
				leng--;
			}
			putchar('\n');
		} else {
			leng=0;
		}
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

typedef struct _storage_class {
	uint8_t admin_only;
	uint8_t mode;
	uint16_t arch_delay;
	uint8_t create_labelscnt,keep_labelscnt,arch_labelscnt;
	uint32_t create_labelmasks[9][MASKORGROUP];
	uint32_t keep_labelmasks[9][MASKORGROUP];
	uint32_t arch_labelmasks[9][MASKORGROUP];
} storage_class;

static inline int deserialize_sc(storage_class *sc) {
	uint8_t i,og;
	sc->admin_only = master_get8bit();
	sc->mode = master_get8bit();
	sc->arch_delay = master_get16bit();
	sc->create_labelscnt = master_get8bit();
	sc->keep_labelscnt = master_get8bit();
	sc->arch_labelscnt = master_get8bit();
	if (sc->create_labelscnt>9 || sc->create_labelscnt<1 || sc->keep_labelscnt>9 || sc->keep_labelscnt<1 || sc->arch_labelscnt>9 || sc->arch_labelscnt<1) {
		return -1;
	}
	for (i=0 ; i<sc->create_labelscnt ; i++) {
		for (og=0 ; og<MASKORGROUP ; og++) {
			sc->create_labelmasks[i][og] = master_get32bit();
		}
	}
	for (i=0 ; i<sc->keep_labelscnt ; i++) {
		for (og=0 ; og<MASKORGROUP ; og++) {
			sc->keep_labelmasks[i][og] = master_get32bit();
		}
	}
	for (i=0 ; i<sc->arch_labelscnt ; i++) {
		for (og=0 ; og<MASKORGROUP ; og++) {
			sc->arch_labelmasks[i][og] = master_get32bit();
		}
	}
	return 0;
}

static inline void serialize_sc(const storage_class *sc) {
	uint8_t i,og;
	master_put8bit(sc->admin_only);
	master_put8bit(sc->mode);
	master_put16bit(sc->arch_delay);
	master_put8bit(sc->create_labelscnt);
	master_put8bit(sc->keep_labelscnt);
	master_put8bit(sc->arch_labelscnt);
	for (i=0 ; i<sc->create_labelscnt ; i++) {
		for (og=0 ; og<MASKORGROUP ; og++) {
			master_put32bit(sc->create_labelmasks[i][og]);
		}
	}
	for (i=0 ; i<sc->keep_labelscnt ; i++) {
		for (og=0 ; og<MASKORGROUP ; og++) {
			master_put32bit(sc->keep_labelmasks[i][og]);
		}
	}
	for (i=0 ; i<sc->arch_labelscnt ; i++) {
		for (og=0 ; og<MASKORGROUP ; og++) {
			master_put32bit(sc->arch_labelmasks[i][og]);
		}
	}
}

static inline void printf_sc(const storage_class *sc,char *endstr) {
	char labelsbuff[LABELS_BUFF_SIZE];
	if (sc->arch_delay==0) {
		if (sc->create_labelscnt==sc->keep_labelscnt) {
			printf("%"PRIu8,sc->create_labelscnt);
		} else {
			printf("%"PRIu8"->%"PRIu8,sc->create_labelscnt,sc->keep_labelscnt);
		}
		printf(" ; admin_only: %s",(sc->admin_only)?"YES":"NO");
		printf(" ; mode: %s",(sc->mode==SCLASS_MODE_LOOSE)?"LOOSE":(sc->mode==SCLASS_MODE_STRICT)?"STRICT":"STD");
		printf(" ; create_labels: %s",make_label_expr(labelsbuff,sc->create_labelscnt,(uint32_t (*)[MASKORGROUP])sc->create_labelmasks));
		printf(" ; keep_labels: %s",make_label_expr(labelsbuff,sc->keep_labelscnt,(uint32_t (*)[MASKORGROUP])sc->keep_labelmasks));
	} else {
		if (sc->create_labelscnt==sc->keep_labelscnt && sc->keep_labelscnt==sc->arch_labelscnt) {
			printf("%"PRIu8,sc->create_labelscnt);
		} else {
			printf("%"PRIu8"->%"PRIu8"->%"PRIu8,sc->create_labelscnt,sc->keep_labelscnt,sc->arch_labelscnt);
		}
		printf(" ; admin_only: %s",(sc->admin_only)?"YES":"NO");
		printf(" ; mode: %s",(sc->mode==SCLASS_MODE_LOOSE)?"LOOSE":(sc->mode==SCLASS_MODE_STRICT)?"STRICT":"STD");
		printf(" ; create_labels: %s",make_label_expr(labelsbuff,sc->create_labelscnt,(uint32_t (*)[MASKORGROUP])sc->create_labelmasks));
		printf(" ; keep_labels: %s",make_label_expr(labelsbuff,sc->keep_labelscnt,(uint32_t (*)[MASKORGROUP])sc->keep_labelmasks));
		printf(" ; arch_labels: %s",make_label_expr(labelsbuff,sc->arch_labelscnt,(uint32_t (*)[MASKORGROUP])sc->arch_labelmasks));
		printf(" ; arch_delay: %"PRIu16"d",sc->arch_delay);
	}
	printf("%s",endstr);
}

int get_sclass(const char *fname,uint8_t *goal,char storage_class_name[256],uint8_t mode) {
	uint32_t inode;
	uint8_t fn,dn,i;
	uint32_t cnt;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_put32bit(inode);
	master_put8bit(mode);
	if (master_send_and_receive(CLTOMA_FUSE_GETSCLASS,MATOCL_FUSE_GETSCLASS)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()<2) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	if (mode==GMODE_NORMAL) {
		fn = master_get8bit();
		dn = master_get8bit();
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			printf("%s: master query: wrong answer (fn,dn)\n",fname);
			master_error();
			return -1;
		}
		*goal = master_get8bit();
		if (*goal==0) {
			printf("%s: unsupported data format (upgrade master)\n",fname);
			return -1;
		} else if (*goal==0xFF) {
			master_getname(storage_class_name);
		}
		cnt = master_get32bit();
		if (cnt!=1) {
			printf("%s: master query: wrong answer (cnt)\n",fname);
			master_error();
			return -1;
		}
		if (*goal==0xFF) {
			printf("%s: '%s'\n",fname,storage_class_name);
		} else {
			printf("%s: %"PRIu8"\n",fname,*goal);
		}
	} else {
		fn = master_get8bit();
		dn = master_get8bit();
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			*goal = master_get8bit();
			if (*goal==0) {
				printf("%s: unsupported data format (upgrade master)\n",fname);
				return -1;
			} else if (*goal==0xFF) {
				master_getname(storage_class_name);
			}
			cnt = master_get32bit();
			if (*goal==0xFF) {
				printf(" files with storage class       '%s' :",storage_class_name);
			} else {
				printf(" files with goal                %"PRIu8" :",*goal);
			}
			print_number(" ","\n",cnt,1,0,1);
		}
		for (i=0 ; i<dn ; i++) {
			*goal = master_get8bit();
			if (*goal==0) {
				printf("%s: unsupported data format (upgrade master)\n",fname);
				return -1;
			} else if (*goal==0xFF) {
				master_getname(storage_class_name);
			}
			cnt = master_get32bit();
			if (*goal==0xFF) {
				printf(" directories with storage class '%s' :",storage_class_name);
			} else {
				printf(" directories with goal          %"PRIu8" :",*goal);
			}
			print_number(" ","\n",cnt,1,0,1);
		}
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

int get_trashtime(const char *fname,uint32_t *trashtime,uint8_t mode) {
	uint32_t inode;
	uint32_t fn,dn,i;
//	uint32_t trashtime;
	uint32_t cnt;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_put32bit(inode);
	master_put8bit(mode);
	if (master_send_and_receive(CLTOMA_FUSE_GETTRASHTIME,MATOCL_FUSE_GETTRASHTIME)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()<8 || master_get_leng()%8!=0) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	} else if (mode==GMODE_NORMAL && master_get_leng()!=16) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	if (mode==GMODE_NORMAL) {
		fn = master_get32bit();
		dn = master_get32bit();
		*trashtime = master_get32bit();
		cnt = master_get32bit();
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			printf("%s: master query: wrong answer (fn,dn)\n",fname);
			master_error();
			return -1;
		}
		if (cnt!=1) {
			printf("%s: master query: wrong answer (cnt)\n",fname);
			master_error();
			return -1;
		}
		printf("%s: %"PRIu32"\n",fname,*trashtime);
	} else {
		fn = master_get32bit();
		dn = master_get32bit();
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			*trashtime = master_get32bit();
			cnt = master_get32bit();
			printf(" files with trashtime        %10"PRIu32" :",*trashtime);
			print_number(" ","\n",cnt,1,0,1);
		}
		for (i=0 ; i<dn ; i++) {
			*trashtime = master_get32bit();
			cnt = master_get32bit();
			printf(" directories with trashtime  %10"PRIu32" :",*trashtime);
			print_number(" ","\n",cnt,1,0,1);
		}
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

int get_eattr(const char *fname,uint8_t *eattr,uint8_t mode) {
	uint32_t inode;
	uint8_t fn,dn,i,j;
	uint32_t fcnt[EATTR_BITS];
	uint32_t dcnt[EATTR_BITS];
//	uint8_t eattr;
	uint32_t cnt;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_put32bit(inode);
	master_put8bit(mode);
	if (master_send_and_receive(CLTOMA_FUSE_GETEATTR,MATOCL_FUSE_GETEATTR)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()%5!=2) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	} else if (mode==GMODE_NORMAL && master_get_leng()!=7) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	if (mode==GMODE_NORMAL) {
		fn = master_get8bit();
		dn = master_get8bit();
		*eattr = master_get8bit();
		cnt = master_get32bit();
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			printf("%s: master query: wrong answer (fn,dn)\n",fname);
			master_error();
			return -1;
		}
		if (cnt!=1) {
			printf("%s: master query: wrong answer (cnt)\n",fname);
			master_error();
			return -1;
		}
		printf("%s: ",fname);
		if (*eattr>0) {
			cnt=0;
			for (j=0 ; j<EATTR_BITS ; j++) {
				if ((*eattr) & (1<<j)) {
					printf("%s%s",(cnt)?",":"",eattrtab[j]);
					cnt=1;
				}
			}
			printf("\n");
		} else {
			printf("-\n");
		}
	} else {
		for (j=0 ; j<EATTR_BITS ; j++) {
			fcnt[j]=0;
			dcnt[j]=0;
		}
		fn = master_get8bit();
		dn = master_get8bit();
		for (i=0 ; i<fn ; i++) {
			*eattr = master_get8bit();
			cnt = master_get32bit();
			for (j=0 ; j<EATTR_BITS ; j++) {
				if ((*eattr) & (1<<j)) {
					fcnt[j]+=cnt;
				}
			}
		}
		for (i=0 ; i<dn ; i++) {
			*eattr = master_get8bit();
			cnt = master_get32bit();
			for (j=0 ; j<EATTR_BITS ; j++) {
				if ((*eattr) & (1<<j)) {
					dcnt[j]+=cnt;
				}
			}
		}
		printf("%s:\n",fname);
		for (j=0 ; j<EATTR_BITS ; j++) {
			if (eattrtab[j][0]) {
				printf(" not directory nodes with attribute %16s :",eattrtab[j]);
				print_number(" ","\n",fcnt[j],1,0,1);
				printf(" directories with attribute         %16s :",eattrtab[j]);
				print_number(" ","\n",dcnt[j],1,0,1);
			} else {
				if (fcnt[j]>0) {
					printf(" not directory nodes with attribute      'unknown-%u' :",j);
					print_number(" ","\n",fcnt[j],1,0,1);
				}
				if (dcnt[j]>0) {
					printf(" directories with attribute              'unknown-%u' :",j);
					print_number(" ","\n",dcnt[j],1,0,1);
				}
			}
		}
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

int set_sclass(const char *fname,uint8_t goal,const char src_storage_class_name[256],const char storage_class_name[256],uint8_t mode) {
	uint8_t nleng,snleng;
	uint32_t inode,uid;
	uint32_t changed,notchanged,notpermitted,quotaexceeded;

	nleng = strlen(storage_class_name);
	if ((mode&SMODE_TMASK)==SMODE_EXCHANGE) {
		snleng = strlen(src_storage_class_name);
	} else {
		snleng = 0;
	}
	if (master_prepare_conn(fname,&inode,NULL,NULL,0,1)<0) {
		return -1;
	}
	if (goal==0xFF && master_get_version()<VERSION2INT(3,0,75)) {
		printf("%s: storage classes not supported (master too old)\n",fname);
		return -1;
	}
	if (goal==0 || (goal>9 && goal!=0xFF)) {
		printf("%s: set storage class unsupported mode (internal error)\n",fname);
		return -1;
	}
	uid = getuid();
	master_new_packet();
	master_put32bit(inode);
	master_put32bit(uid);
	master_put8bit(goal);
	master_put8bit(mode);
	if (goal==0xFF) {
		if ((mode&SMODE_TMASK)==SMODE_EXCHANGE) {
			master_putname(snleng,src_storage_class_name);
		}
		master_putname(nleng,storage_class_name);
	}
	if (master_send_and_receive(CLTOMA_FUSE_SETSCLASS,MATOCL_FUSE_SETSCLASS)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()!=12 && master_get_leng()!=16) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	changed = master_get32bit();
	notchanged = master_get32bit();
	notpermitted = master_get32bit();
	if (master_get_leng()==16) {
		quotaexceeded = master_get32bit();
	} else {
		quotaexceeded = 0;
	}
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			if (goal==0xFF) {
				printf("%s: storage class: '%s'\n",fname,storage_class_name);
			} else {
				printf("%s: goal: %"PRIu8"\n",fname,goal);
			}
		} else {
			if (goal==0xFF) {
				printf("%s: storage class not changed\n",fname);
			} else {
				printf("%s: goal not changed\n",fname);
			}
		}
	} else {
		printf("%s:\n",fname);
		if (goal==0xFF) {
			print_number(" inodes with storage class changed:     ","\n",changed,1,0,1);
			print_number(" inodes with storage class not changed: ","\n",notchanged,1,0,1);
		} else {
			print_number(" inodes with goal changed:              ","\n",changed,1,0,1);
			print_number(" inodes with goal not changed:          ","\n",notchanged,1,0,1);
		}
		print_number(" inodes with permission denied:         ","\n",notpermitted,1,0,1);
		if (master_get_leng()==16) {
			print_number(" inodes with quota exceeded:            ","\n",quotaexceeded,1,0,1);
		}
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

int set_trashtime(const char *fname,uint32_t trashtime,uint8_t mode) {
	uint32_t inode,uid;
	uint32_t changed,notchanged,notpermitted;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,1)<0) {
		return -1;
	}
	uid = getuid();
	master_new_packet();
	master_put32bit(inode);
	master_put32bit(uid);
	master_put32bit(trashtime);
	master_put8bit(mode);
	if (master_send_and_receive(CLTOMA_FUSE_SETTRASHTIME,MATOCL_FUSE_SETTRASHTIME)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	changed = master_get32bit();
	notchanged = master_get32bit();
	notpermitted = master_get32bit();
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			printf("%s: %"PRIu32"\n",fname,trashtime);
		} else {
			printf("%s: trashtime not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		print_number(" inodes with trashtime changed:     ","\n",changed,1,0,1);
		print_number(" inodes with trashtime not changed: ","\n",notchanged,1,0,1);
		print_number(" inodes with permission denied:     ","\n",notpermitted,1,0,1);
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

int set_eattr(const char *fname,uint8_t eattr,uint8_t mode) {
	uint32_t inode,uid;
	uint32_t changed,notchanged,notpermitted;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,1)<0) {
		return -1;
	}
	uid = getuid();
	master_new_packet();
	master_put32bit(inode);
	master_put32bit(uid);
	master_put8bit(eattr);
	master_put8bit(mode);
	if (master_send_and_receive(CLTOMA_FUSE_SETEATTR,MATOCL_FUSE_SETEATTR)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	changed = master_get32bit();
	notchanged = master_get32bit();
	notpermitted = master_get32bit();
	if ((mode&SMODE_RMASK)==0) {
		if (changed) {
			printf("%s: attribute(s) changed\n",fname);
		} else {
			printf("%s: attribute(s) not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		print_number(" inodes with attributes changed:     ","\n",changed,1,0,1);
		print_number(" inodes with attributes not changed: ","\n",notchanged,1,0,1);
		print_number(" inodes with permission denied:      ","\n",notpermitted,1,0,1);
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

int archive_control(const char *fname,uint8_t archcmd) {
	uint32_t inode,uid;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,1)<0) {
		return -1;
	}
	uid = getuid();
	master_new_packet();
	master_put32bit(inode);
	master_put8bit(archcmd);
	if (archcmd!=ARCHCTL_GET) {
		master_put32bit(uid);
	}
	if (master_send_and_receive(CLTOMA_FUSE_ARCHCTL,MATOCL_FUSE_ARCHCTL)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	}
	if (archcmd==ARCHCTL_GET) {
		uint32_t archinodes,partinodes,notarchinodes;
		uint64_t archchunks,notarchchunks;
		if (master_get_leng()!=28) {
			printf("%s: master query: wrong answer (leng)\n",fname);
			master_error();
			return -1;
		}
		archchunks = master_get64bit();
		notarchchunks = master_get64bit();
		archinodes = master_get32bit();
		partinodes = master_get32bit();
		notarchinodes = master_get32bit();
		if (archinodes+partinodes+notarchinodes==1) {
			if (archinodes==1) {
				printf("%s: all chunks are archived\n",fname);
			} else if (notarchinodes==1) {
				printf("%s: all chunks are not archived\n",fname);
			} else {
				printf("%s: file is partially archived (archived chunks: %"PRIu64" ; not archived chunks: %"PRIu64")\n",fname,archchunks,notarchchunks);
			}
		} else {
			printf("%s:\n",fname);
			print_number(" files with all chunks archived:     ","\n",archinodes,1,0,1);
			print_number(" files with all chunks not archived: ","\n",notarchinodes,1,0,1);
			print_number(" files partially archived:           ","\n",partinodes,1,0,1);
			print_number(" archived chunks:                    ","\n",archchunks,1,0,1);
			print_number(" not archived chunks:                ","\n",notarchchunks,1,0,1);
		}
	} else {
		uint64_t changed,notchanged;
		uint32_t notpermitted;
		if (master_get_leng()!=20) {
			printf("%s: master query: wrong answer (leng)\n",fname);
			master_error();
			return -1;
		}
		changed = master_get64bit();
		notchanged = master_get64bit();
		notpermitted = master_get32bit();
		printf("%s:\n",fname);
		print_number(" chunks changed:               ","\n",changed,1,0,1);
		print_number(" chunks not changed:           ","\n",notchanged,1,0,1);
		print_number(" files with permission denied: ","\n",notpermitted,1,0,1);
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

int make_sc(const char *mfsmp,const char *scname,storage_class *sc) {
	uint32_t nleng;
	char cwdbuff[MAXPATHLEN];
	uint8_t status;

	nleng = strlen(scname);
	if (nleng>=256) {
		printf("%s: name too long\n",scname);
		return -1;
	}
	if (mfsmp==NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (master_prepare_conn(mfsmp,NULL,NULL,NULL,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_putname(nleng,scname);
	master_put8bit(0); // packet version
	serialize_sc(sc);
	if (master_send_and_receive(CLTOMA_SCLASS_CREATE,MATOCL_SCLASS_CREATE)<0) {
		return -1;
	}
	if (master_get_leng()!=1) {
		printf("master query: wrong answer (leng)\n");
		master_error();
		return -1;
	}
	status = master_get8bit();
	if (status!=MFS_STATUS_OK) {
		printf("storage class make %s: error: %s\n",scname,mfsstrerr(status));
		return -1;
	}
	printf("storage class make %s: ok\n",scname);
	if (master_end_packet()==0) { // pro forma
		printf("master query: packet size error\n");
		return -1;
	}
	return 0;
}

int change_sc(const char *mfsmp,const char *scname,uint16_t chgmask,storage_class *sc) {
	uint32_t nleng;
	char cwdbuff[MAXPATHLEN];

	nleng = strlen(scname);
	if (nleng>=256) {
		printf("%s: name too long\n",scname);
		return -1;
	}
	if (mfsmp==NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (master_prepare_conn(mfsmp,NULL,NULL,NULL,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_putname(nleng,scname);
	master_put8bit(0); // packet version
	master_put16bit(chgmask);
	serialize_sc(sc);
	if (master_send_and_receive(CLTOMA_SCLASS_CHANGE,MATOCL_SCLASS_CHANGE)<0) {
		return -1;
	}
	if (master_get_leng()==0) {
		printf("master query: wrong answer (leng)\n");
		master_error();
		return -1;
	}
	if (master_get_leng()==1) {
		if (chgmask==0) {
			printf("storage class show %s: error: %s\n",scname,mfsstrerr(master_get8bit()));
		} else {
			printf("storage class change %s: error: %s\n",scname,mfsstrerr(master_get8bit()));
		}
		return -1;
	}
	if (master_get8bit()!=0) {
		printf("master query: wrong answer (wrong data format)\n");
		master_error();
		return -1;
	}
	if (deserialize_sc(sc)<0) {
		printf("master query: wrong answer (deserialize storage class)\n");
		master_error();
		return -1;
	}
	printf("storage class change %s: ",scname);
	printf_sc(sc,"\n");
	if (master_end_packet()==0) {
		printf("master query: packet size error\n");
		return -1;
	}
	return 0;
}

int show_sc(const char *mfsmp,const char *scname) {
	storage_class sc;
	memset(&sc,0,sizeof(storage_class));
	return change_sc(mfsmp,scname,0,&sc);
}

int remove_sc(const char *mfsmp,const char *scname) {
	uint32_t nleng;
	char cwdbuff[MAXPATHLEN];
	uint8_t status;

	nleng = strlen(scname);
	if (nleng>=256) {
		printf("%s: name too long\n",scname);
		return -1;
	}
	if (mfsmp==NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (master_prepare_conn(mfsmp,NULL,NULL,NULL,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_putname(nleng,scname);
	if (master_send_and_receive(CLTOMA_SCLASS_DELETE,MATOCL_SCLASS_DELETE)<0) {
		return -1;
	}
	if (master_get_leng()!=1) {
		printf("master query: wrong answer (leng)\n");
		master_error();
		return -1;
	}
	status = master_get8bit();
	if (status!=MFS_STATUS_OK) {
		printf("storage class remove %s: error: %s\n",scname,mfsstrerr(status));
		return -1;
	}
	printf("storage class remove %s: ok\n",scname);
	if (master_end_packet()==0) { // pro forma
		printf("master query: packet size error\n");
		return -1;
	}
	return 0;
}

int copy_sc(const char *mfsmp,const char *oldscname,const char *newscname) {
	uint32_t onleng,nnleng;
	char cwdbuff[MAXPATHLEN];
	uint8_t status;

	onleng = strlen(oldscname);
	nnleng = strlen(newscname);
	if (onleng>=256) {
		printf("%s: name too long\n",oldscname);
		return -1;
	}
	if (nnleng>=256) {
		printf("%s: name too long\n",newscname);
		return -1;
	}
	if (mfsmp==NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (master_prepare_conn(mfsmp,NULL,NULL,NULL,0,0)<0) {
		return -1;
	}
	
	master_new_packet();
	master_putname(onleng,oldscname);
	master_putname(nnleng,newscname);
	if (master_send_and_receive(CLTOMA_SCLASS_DUPLICATE,MATOCL_SCLASS_DUPLICATE)<0) {
		return -1;
	}
	if (master_get_leng()!=1) {
		printf("master query: wrong answer (leng)\n");
		master_error();
		return -1;
	}
	status = master_get8bit();
	if (status!=MFS_STATUS_OK) {
		printf("storage class copy %s->%s: error: %s\n",oldscname,newscname,mfsstrerr(status));
		return -1;
	}
	printf("storage class copy %s->%s: ok\n",oldscname,newscname);
	if (master_end_packet()==0) { // pro forma
		printf("master query: packet size error\n");
		return -1;
	}
	return 0;
}

int move_sc(const char *mfsmp,const char *oldscname,const char *newscname) {
	uint32_t onleng,nnleng;
	char cwdbuff[MAXPATHLEN];
	uint8_t status;

	onleng = strlen(oldscname);
	nnleng = strlen(newscname);
	if (onleng>=256) {
		printf("%s: name too long\n",oldscname);
		return -1;
	}
	if (nnleng>=256) {
		printf("%s: name too long\n",newscname);
		return -1;
	}
	if (mfsmp==NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (master_prepare_conn(mfsmp,NULL,NULL,NULL,0,0)<0) {
		return -1;
	}

	master_new_packet();
	master_putname(onleng,oldscname);
	master_putname(nnleng,newscname);
	if (master_send_and_receive(CLTOMA_SCLASS_RENAME,MATOCL_SCLASS_RENAME)<0) {
		return -1;
	}
	if (master_get_leng()!=1) {
		printf("master query: wrong answer (leng)\n");
		master_error();
		return -1;
	}
	status = master_get8bit();
	if (status!=MFS_STATUS_OK) {
		printf("storage class move %s->%s: error: %s\n",oldscname,newscname,mfsstrerr(status));
		return -1;
	}
	printf("storage class move %s->%s: ok\n",oldscname,newscname);
	if (master_end_packet()==0) { // pro forma
		printf("master query: packet size error\n");
		return -1;
	}
	return 0;
}

int list_sc(const char *mfsmp,uint8_t longmode) {
	char scname[256];
	storage_class sc;
	char cwdbuff[MAXPATHLEN];

	if (mfsmp==NULL) {
		mfsmp = getcwd(cwdbuff,MAXPATHLEN);
	}
	if (master_prepare_conn(mfsmp,NULL,NULL,NULL,0,0)<0) {
		return -1;
	}

	master_new_packet();
	master_put8bit(longmode);
	if (master_send_and_receive(CLTOMA_SCLASS_LIST,MATOCL_SCLASS_LIST)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("storage class list: error: %s\n",mfsstrerr(master_get8bit()));
		return -1;
	}
	while (master_bytes_left()>0) {
		master_getname(scname);
		if (longmode&1) {
			if (deserialize_sc(&sc)<0) {
				printf("master query: wrong answer (deserialize storage class)\n");
				master_error();
				return -1;
			}
			printf("%s : ",scname);
			printf_sc(&sc,"\n");
		} else {
			printf("%s\n",scname);
		}
	}
	if (master_end_packet()==0) {
		printf("master query: packet size error\n");
		return -1;
	}
	return 0;
}

typedef struct _chunk_data {
	uint32_t ip;
	uint16_t port;
	uint8_t status;
} chunk_data;

int chunk_data_cmp(const void*a,const void*b) {
	chunk_data *aa = (chunk_data*)a;
	chunk_data *bb = (chunk_data*)b;

	if (aa->ip < bb->ip) {
		return -1;
	} else if (aa->ip > bb->ip) {
		return 1;
	} else if (aa->port < bb->port) {
		return -1;
	} else if (aa->port > bb->port) {
		return 1;
	}
	return 0;
}

int get_checksum_block(const char *csstrip,uint32_t csip,uint16_t csport,uint64_t chunkid,uint32_t version,uint8_t crcblock[4096],uint16_t *blocks) {
	uint8_t reqbuff[20],*wptr,*buff;
	const uint8_t *rptr;
	int fd;
	uint32_t cmd,leng;
	uint16_t cnt;
	uint8_t status;

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
			cnt++;
			if (cnt==10) {
				printf("can't connect to chunkserver %s:%"PRIu16": %s\n",csstrip,csport,strerr(errno));
				goto error;
			}
			tcpclose(fd);
			fd = -1;
		} else {
			cnt=10;
		}
	}

	// 1 - get checksum block
	buff = NULL;
	wptr = reqbuff;
	put32bit(&wptr,ANTOCS_GET_CHUNK_CHECKSUM_TAB);
	put32bit(&wptr,12);
	put64bit(&wptr,chunkid);
	put32bit(&wptr,version);
	if (tcpwrite(fd,reqbuff,20)!=20) {
		printf("%s:%"PRIu16": cs query: send error\n",csstrip,csport);
		goto error;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
		goto error;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=CSTOAN_CHUNK_CHECKSUM_TAB) {
		printf("%s:%"PRIu16" cs query: wrong answer (type)\n",csstrip,csport);
		goto error;
	}
	if (leng!=13 && leng!=(4096+12)) {
		printf("%s:%"PRIu16" cs query: wrong answer (size)\n",csstrip,csport);
		goto error;
	}
	buff = malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
		goto error;
	}
	rptr = buff;
	if (chunkid!=get64bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (chunkid)\n",csstrip,csport);
		goto error;
	}
	if (version!=get32bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (version)\n",csstrip,csport);
		goto error;
	}
	leng-=12;
	if (leng==1) {
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
	put64bit(&wptr,chunkid);
	put32bit(&wptr,version);
	if (tcpwrite(fd,reqbuff,20)!=20) {
		printf("%s:%"PRIu16": cs query: send error\n",csstrip,csport);
		goto error;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
		goto error;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=CSTOAN_CHUNK_BLOCKS) {
		printf("%s:%"PRIu16" cs query: wrong answer (type)\n",csstrip,csport);
		goto error;
	}
	if (leng!=15) {
		printf("%s:%"PRIu16" cs query: wrong answer (size)\n",csstrip,csport);
		goto error;
	}
	buff = malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s:%"PRIu16" cs query: receive error\n",csstrip,csport);
		goto error;
	}
	rptr = buff;
	if (chunkid!=get64bit(&rptr)) {
		printf("%s:%"PRIu16" cs query: wrong answer (chunkid)\n",csstrip,csport);
		goto error;
	}
	if (version!=get32bit(&rptr)) {
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

void digest_to_str(char strdigest[33],uint8_t digest[16]) {
	uint32_t i;
	for (i=0 ; i<16 ; i++) {
		snprintf(strdigest+2*i,3,"%02X",digest[i]);
	}
	strdigest[32]='\0';
}

int file_info(uint8_t fileinfomode,const char *fname) {
	uint32_t fchunks;
	uint8_t fchunksvalid;
	uint32_t indx,inode,version;
	uint32_t chunks,copies,vcopies,copy;
	char *strtype;
	char csstrip[16];
	chunk_data *cdtab;
	uint8_t protover;
	uint64_t chunkid;
	uint64_t fleng;
	uint8_t crcblock[4096];
	uint16_t blocks;
	md5ctx filectx,chunkctx;
	uint8_t chunkdigest[16],currentdigest[16];
	uint8_t firstdigest;
	uint8_t checksumerror;
	char strdigest[33];

	if (master_prepare_conn(fname,&inode,NULL,&fleng,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_put32bit(inode);
	if (master_send_and_receive(CLTOMA_FUSE_CHECK,MATOCL_FUSE_CHECK)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if ((master_get_leng()%3!=0 || master_get_leng()>33) && master_get_leng()!=44 && master_get_leng()!=48) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	if (fileinfomode&FILEINFO_QUICK) {
		printf("%s:\n",fname);
	}
	fchunks = 0;
	fchunksvalid = 0;
	if (master_get_leng()%3==0 && master_get_leng()<=33) {
		while (master_bytes_left()>0) {
			copies = master_get8bit();
			chunks = master_get16bit();
			if (fileinfomode&FILEINFO_QUICK) {
				if (copies==1) {
					printf("1 copy:");
				} else {
					printf("%"PRIu32" copies:",copies);
				}
				print_number(" ","\n",chunks,1,0,1);
			}
			fchunks += chunks;
		}
	} else {
		for (copies=0 ; copies<11 ; copies++) {
			chunks = master_get32bit();
			if (chunks>0 && (fileinfomode&FILEINFO_QUICK)) {
				if (copies==1) {
					printf(" chunks with 1 copy:    ");
				} else if (copies>=10) {
					printf(" chunks with 10+ copies:");
				} else {
					printf(" chunks with %u copies:  ",copies);
				}
				print_number(" ","\n",chunks,1,0,1);
			}
			fchunks += chunks;
		}
		if (master_get_leng()==48) {
			chunks = master_get32bit();
			if (chunks>0 && (fileinfomode&FILEINFO_QUICK)) {
				printf(" empty (zero) chunks:   ");
				print_number(" ","\n",chunks,1,0,1);
			}
			fchunks += chunks;
			fchunksvalid = 1;
		}
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
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
			master_new_packet();
			if (master_get_version()<VERSION2INT(3,0,26)) {
				uint32_t leng;
				master_put32bit(inode);
				master_put32bit(indx);
				if (master_get_version()>=VERSION2INT(3,0,3)) {
					master_put8bit(0); // canmodatime
				}
				if (master_send_and_receive(CLTOMA_FUSE_READ_CHUNK,MATOCL_FUSE_READ_CHUNK)<0) {
					return -1;
				}
				leng = master_get_leng();
				if (leng==1) {
					printf("%s [%"PRIu32"]: %s\n",fname,indx,mfsstrerr(master_get8bit()));
					return -1;
				} else if (leng&1) {
					protover = master_get8bit();
					if (protover!=1 && protover!=2) {
						printf("%s [%"PRIu32"]: master query: unknown protocol id (%"PRIu8")\n",fname,indx,protover);
						master_error();
						return -1;
					}
					if (leng<21 || (protover==1 && ((leng-21)%10)!=0) || (protover==2 && ((leng-21)%14)!=0)) {
						printf("%s [%"PRIu32"]: master query: wrong answer (leng)\n",fname,indx);
						master_error();
						return -1;
					}
				} else {
					if (leng<20 || ((leng-20)%6)!=0) {
						printf("%s [%"PRIu32"]: master query: wrong answer (leng)\n",fname,indx);
						master_error();
						return -1;
					}
					protover = 0;
				}
				(void)master_get64bit(); // fleng
				if (protover==2) {
					copies = (leng-21)/14;
				} else if (protover==1) {
					copies = (leng-21)/10;
				} else {
					copies = (leng-20)/6;
				}
			} else {
				master_put32bit(inode);
				master_put32bit(indx);
				if (master_send_and_receive(CLTOMA_FUSE_CHECK,MATOCL_FUSE_CHECK)<0) {
					return -1;
				}
				if (master_get_leng()==1) {
					printf("%s [%"PRIu32"]: %s\n",fname,indx,mfsstrerr(master_get8bit()));
					return -1;
				} else {
					if (master_get_leng()<12 || ((master_get_leng()-12)%7)!=0) {
						printf("%s [%"PRIu32"]: master query: wrong answer (leng)\n",fname,indx);
						master_error();
						return -1;
					}
					protover = 255;
					copies = (master_get_leng()-12)/7;
				}
			}
			chunkid = master_get64bit();
			version = master_get32bit();
			if (chunkid==0 && version==0) {
				printf("\tchunk %"PRIu32": empty\n",indx);
			} else {
				printf("\tchunk %"PRIu32": %016"PRIX64"_%08"PRIX32" / (id:%"PRIu64" ver:%"PRIu32")\n",indx,chunkid,version,chunkid,version);
				vcopies = 0;
				if (copies>0) {
					cdtab = malloc(copies*sizeof(chunk_data));
				} else {
					cdtab = NULL;
				}
				for (copy=0 ; copy<copies ; copy++) {
					cdtab[copy].ip = master_get32bit();
					cdtab[copy].port = master_get16bit();
					if (protover==255) {
						cdtab[copy].status = master_get8bit();
					} else {
						cdtab[copy].status = CHECK_VALID;
						if (protover>=1) {
							(void)master_get32bit();
						}
						if (protover>=2) {
							(void)master_get32bit();
						}
					}
				}
				if (copies>0) {
					qsort(cdtab,copies,sizeof(chunk_data),chunk_data_cmp);
				}
				firstdigest = 1;
				checksumerror = 0;
				for (copy=0 ; copy<copies ; copy++) {
					snprintf(csstrip,16,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,(uint8_t)((cdtab[copy].ip>>24)&0xFF),(uint8_t)((cdtab[copy].ip>>16)&0xFF),(uint8_t)((cdtab[copy].ip>>8)&0xFF),(uint8_t)(cdtab[copy].ip&0xFF));
					csstrip[15]=0;
					if (protover==255) {
						switch (cdtab[copy].status) {
							case CHECK_VALID:
								strtype = "VALID";
								vcopies++;
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
					} else if (protover==2) {
						strtype = "VALID";
						vcopies++;
					} else if (protover==1) {
						strtype = "VALID";
						vcopies++;
					} else {
						strtype = "VALID";
						vcopies++;
					}
					if (fileinfomode&(FILEINFO_CRC|FILEINFO_SIGNATURE)) {
						if (get_checksum_block(csstrip,cdtab[copy].ip,cdtab[copy].port,chunkid,version,crcblock,&blocks)==0) {
							md5_init(&chunkctx);
							md5_update(&chunkctx,crcblock,4*blocks);
							if ((fileinfomode&FILEINFO_SIGNATURE) && firstdigest) {
								md5_update(&filectx,crcblock,4*blocks);
							}
							md5_final(currentdigest,&chunkctx);
							if (firstdigest) {
								memcpy(chunkdigest,currentdigest,16);
							} else {
								if (memcmp(chunkdigest,currentdigest,16)!=0) {
									checksumerror = 1;
								}
							}
							firstdigest = 0;
							if (fileinfomode&FILEINFO_CRC) {
								digest_to_str(strdigest,currentdigest);
								printf("\t\tcopy %"PRIu32": %s:%"PRIu16" (status:%s ; blocks: %u ; checksum digest: %s)\n",copy+1,csstrip,cdtab[copy].port,strtype,blocks,strdigest);
							} else {
								printf("\t\tcopy %"PRIu32": %s:%"PRIu16" (status:%s)\n",copy+1,csstrip,cdtab[copy].port,strtype);
							}
						} else {
							if (fileinfomode&FILEINFO_CRC) {
								printf("\t\tcopy %"PRIu32": %s:%"PRIu16" (status:%s) - can't get checksum\n",copy+1,csstrip,cdtab[copy].port,strtype);
							} else {
								printf("\t\tcopy %"PRIu32": %s:%"PRIu16" (status:%s)\n",copy+1,csstrip,cdtab[copy].port,strtype);
							}
						}
					} else {
						printf("\t\tcopy %"PRIu32": %s:%"PRIu16" (status:%s)\n",copy+1,csstrip,cdtab[copy].port,strtype);
					}
				}
				if (checksumerror) {
					printf("\t\tcopies have different checksums !!!\n");
				}
				if ((fileinfomode&FILEINFO_SIGNATURE) && firstdigest) {
					printf("\t\tcouldn't add this chunk to signature !!!\n");
				}
				if (vcopies==0) {
					printf("\t\tno valid copies !!!\n");
				}
				if (cdtab!=NULL) {
					free(cdtab);
				}
			}
			if (master_end_packet()==0) {
				printf("%s: master query: packet size error\n",fname);
				return -1;
			}
		}
		if (fileinfomode&FILEINFO_SIGNATURE) {
			md5_final(currentdigest,&filectx);
			digest_to_str(strdigest,currentdigest);
			printf("%s signature: %s\n",fname,strdigest);
		}
	}
	return 0;
}

int append_file(const char *fname,const char *afname,int64_t slice_from,int64_t slice_to) {
	uint32_t inode,ainode,uid,gid;
	uint32_t slice_from_abs,slice_to_abs;
	uint8_t flags;
	gid_t grouplist[NGROUPS_MAX];
	uint32_t i,gids;
	uint8_t addmaingroup;
	mode_t dmode,smode;
	uint8_t status;

	if (slice_from < INT64_C(-0xFFFFFFFF) || slice_to > INT64_C(0xFFFFFFFF) || slice_to < INT64_C(-0xFFFFFFFF) || slice_to > INT64_C(0xFFFFFFFF)) {
		printf("bad slice indexes\n");
		return -1;
	}
	flags = 0;
	if (slice_from<0) {
		slice_from_abs = -slice_from;
		flags |= APPEND_SLICE_FROM_NEG;
	} else {
		slice_from_abs = slice_from;
	}
	if (slice_to<0) {
		slice_to_abs = -slice_to;
		flags |= APPEND_SLICE_TO_NEG;
	} else {
		slice_to_abs = slice_to;
	}

	if (master_prepare_conn(fname,&inode,&dmode,NULL,0,1)<0) {
		return -1;
	}
	if (master_prepare_conn(afname,&ainode,&smode,NULL,1,1)<0) {
		return -1;
	}

	if ((slice_from!=0 || slice_to!=0x80000000) && master_get_version()<VERSION2INT(3,0,92)) {
		printf("slices not supported in your master - please upgrade it\n");
		return -1;
	}
	if ((smode&S_IFMT)!=S_IFREG) {
		printf("%s: not a file\n",afname);
		return -1;
	}
	if ((dmode&S_IFMT)!=S_IFREG) {
		printf("%s: not a file\n",fname);
		return -1;
	}
	uid = getuid();
	gid = getgid();
	if (master_get_version()>=VERSION2INT(2,0,0)) {
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
	master_new_packet();
	if (master_get_version()>=VERSION2INT(3,0,92)) {
		master_put8bit(flags);
		master_put32bit(inode);
		master_put32bit(ainode);
		master_put32bit(slice_from_abs);
		master_put32bit(slice_to_abs);
	} else {
		master_put32bit(inode);
		master_put32bit(ainode);
	}
	master_put32bit(uid);
	if (master_get_version()<VERSION2INT(2,0,0)) {
		master_put32bit(gid);
	} else {
		master_put32bit(addmaingroup+gids);
		if (addmaingroup) {
			master_put32bit(gid);
		}
		for (i=0 ; i<gids ; i++) {
			master_put32bit(grouplist[i]);
		}
	}
	if (master_send_and_receive(CLTOMA_FUSE_APPEND_SLICE,MATOCL_FUSE_APPEND_SLICE)<0) {
		return -1;
	}
	if (master_get_leng()!=1) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	status = master_get8bit();
	if (status!=MFS_STATUS_OK) {
		printf("%s: %s\n",fname,mfsstrerr(status));
		return -1;
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

static uint32_t dirnode_cnt;
static uint32_t filenode_cnt;
static uint32_t touched_inodes;
static uint64_t sumlength;
static uint64_t chunk_size_cnt;
static uint64_t chunk_rsize_cnt;

int sc_node_info(uint32_t inode) {
	uint16_t anstype;
	uint64_t fleng;
	uint64_t chunkid;
	uint32_t chunksize;
	uint32_t childinode;
	uint64_t continueid;
	uint8_t copies;

//	printf("inode: %"PRIu32"\n",inode);
	continueid = 0;
	touched_inodes++;
	do {
		master_new_packet();
		master_put32bit(inode);
		master_put32bit(500); // 500 !!!
		master_put64bit(continueid);
		if (master_send_and_receive(CLTOMA_NODE_INFO,MATOCL_NODE_INFO)<0) {
			return -1;
		}
		if (master_get_leng()==1) {
			// error - ignore
			return 0;
		}
		if (master_get_leng()<2) {
			printf("inode: %"PRIu32" ; master query: wrong answer (size)\n",inode);
			master_error();
			return -1;
		}
		anstype = master_get16bit();
		if (anstype==1 && ((master_get_leng()-2)%4)==0) { // directory
//			printf("directory\n");
			if (continueid==0) {
				dirnode_cnt++;
			}
			continueid = master_get64bit();
//			printf("continueid: %"PRIX64"\n",continueid);
			while (master_bytes_left()>0) {
				childinode = master_get32bit();
//				printf("inode: %"PRIu32"\n",childinode);
				if (sc_enqueue(childinode)==0) {
					touched_inodes++; // repeated nodes - increment here
				}
			}
		} else if (anstype==2 && (master_get_leng()-2-16)%13==0) { // file
//			printf("file\n");
			if (continueid==0) {
				filenode_cnt++;
			}
			continueid = master_get64bit();
			fleng = master_get64bit();
//			printf("continueid: %"PRIu64" ; fleng: %"PRIu64"\n",continueid,fleng);
			sumlength += fleng;
			while (master_bytes_left()>0) {
				chunkid = master_get64bit();
				chunksize = master_get32bit();
				copies = master_get8bit();
//				printf("chunk: %016"PRIX64" ; chunksize: %"PRIu32" ; copies:%"PRIu8"\n",chunkid,chunksize,copies);
				if (liset_addval(chunk_liset,chunkid)==0) { // new chunk ?
					chunk_size_cnt += chunksize;
					chunk_rsize_cnt += copies * chunksize;
				}
			}
		} else {
			continueid = 0;
		}
		if (master_end_packet()==0) { // pro forma
			printf("master query: packet size error\n");
			return -1;
		}
	} while (continueid!=0);
	return 0;
}


int dir_info(uint8_t dirinfomode,const char *fname) {
	uint32_t inode;
	uint32_t inodes,dirs,files,chunks;
	uint64_t length,size,realsize;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,0)<0) {
		return -1;
	}
	master_new_packet();
	master_put32bit(inode);
	if (master_send_and_receive(CLTOMA_FUSE_GETDIRSTATS,MATOCL_FUSE_GETDIRSTATS)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()!=56 && master_get_leng()!=40) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	inodes = master_get32bit();
	dirs = master_get32bit();
	files = master_get32bit();
	if (master_get_leng()==56) {
		(void)master_get64bit();
	}
	chunks = master_get32bit();
	if (master_get_leng()==56) {
		(void)master_get64bit();
	}
	length = master_get64bit();
	size = master_get64bit();
	realsize = master_get64bit();
	if (dirinfomode==0 || dirinfomode==DIRINFO_PRECISE) {
		printf("%s:\n",fname);
		print_number(" inodes:       ","\n",inodes,0,0,1);
		print_number("  directories: ","\n",dirs,0,0,1);
		print_number("  files:       ","\n",files,0,0,1);
		print_number(" chunks:       ","\n",chunks,0,0,1);
		print_number(" length:       ","\n",length,0,1,1);
		print_number(" size:         ","\n",size,0,1,1);
		print_number(" realsize:     ","\n",realsize,0,1,1);
	} else {
		if (dirinfomode&DIRINFO_INODES) {
			print_number_only(inodes,0);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_DIRS) {
			print_number_only(dirs,0);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_FILES) {
			print_number_only(files,0);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_CHUNKS) {
			print_number_only(chunks,0);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_LENGTH) {
			print_number_only(length,1);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_SIZE) {
			print_number_only(size,1);
			printf("\t");
		}
		if (dirinfomode&DIRINFO_REALSIZE) {
			print_number_only(realsize,1);
			printf("\t");
		}
		printf("%s\n",fname);
	}
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	if (dirinfomode&DIRINFO_PRECISE) {
		uint16_t progress;
		double seconds,lseconds;
		uint8_t err;
	
		if (master_get_version()<VERSION2INT(3,0,73)) {
			printf("precise data calculation needs master at least in version 3.0.73 - upgrade your unit\n");
		} else {
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

			inode_liset = liset_new();
			chunk_liset = liset_new();
			sc_enqueue(inode);
			while (sc_dequeue(&inode)) {
				if (err==0) {
					if (sc_node_info(inode)<0) {
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
					print_number(" inodes:       ","\n",liset_card(inode_liset),0,0,1);
					print_number("  directories: ","\n",dirnode_cnt,0,0,1);
					print_number("  files:       ","\n",filenode_cnt,0,0,1);
					print_number(" chunks:       ","\n",liset_card(chunk_liset),0,0,1);
					print_number(" length:       ","\n",sumlength,0,1,1);
					print_number(" size:         ","\n",chunk_size_cnt,0,1,1);
					print_number(" realsize:     ","\n",chunk_rsize_cnt,0,1,1);
				} else {
					if (dirinfomode&DIRINFO_INODES) {
						print_number_only(liset_card(inode_liset),0);
						printf("\t");
					}
					if (dirinfomode&DIRINFO_DIRS) {
						print_number_only(dirnode_cnt,0);
						printf("\t");
					}
					if (dirinfomode&DIRINFO_FILES) {
						print_number_only(filenode_cnt,0);
						printf("\t");
					}
					if (dirinfomode&DIRINFO_CHUNKS) {
						print_number_only(liset_card(chunk_liset),0);
						printf("\t");
					}
					if (dirinfomode&DIRINFO_LENGTH) {
						print_number_only(sumlength,1);
						printf("\t");
					}
					if (dirinfomode&DIRINFO_SIZE) {
						print_number_only(chunk_size_cnt,1);
						printf("\t");
					}
					if (dirinfomode&DIRINFO_REALSIZE) {
						print_number_only(chunk_rsize_cnt,1);
						printf("\t");
					}
					printf("%s (precise data)\n",fname);
				}
			}
			liset_remove(chunk_liset);
			liset_remove(inode_liset);
		}
	}
	return 0;
}

int file_repair(const char *fname) {
	uint32_t inode,uid,gid;
	gid_t grouplist[NGROUPS_MAX];
	uint32_t i,gids;
	uint8_t addmaingroup;
	uint32_t notchanged,erased,repaired;

	if (master_prepare_conn(fname,&inode,NULL,NULL,0,1)<0) {
		return -1;
	}
	uid = getuid();
	gid = getgid();
	if (master_get_version()>=VERSION2INT(2,0,0)) {
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
	master_new_packet();
	master_put32bit(inode);
	master_put32bit(uid);
	if (master_get_version()<VERSION2INT(2,0,0)) {
		master_put32bit(gid);
	} else {
		master_put32bit(addmaingroup+gids);
		if (addmaingroup) {
			master_put32bit(gid);
		}
		for (i=0 ; i<gids ; i++) {
			master_put32bit(grouplist[i]);
		}
	}
	if (master_send_and_receive(CLTOMA_FUSE_REPAIR,MATOCL_FUSE_REPAIR)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	notchanged = master_get32bit();
	erased = master_get32bit();
	repaired = master_get32bit();
	printf("%s:\n",fname);
	print_number(" chunks not changed: ","\n",notchanged,1,0,1);
	print_number(" chunks erased:      ","\n",erased,1,0,1);
	print_number(" chunks repaired:    ","\n",repaired,1,0,1);
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

/*
int eattr_control(const char *fname,uint8_t mode,uint8_t eattr) {
	uint8_t reqbuff[22],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint8_t nodeeattr,functioneattr;
//	uint32_t curinodes;
//	uint64_t curlength,cursize,currealsize;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,NULL,0,(mode<2)?1:0);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_EATTR);
	put32bit(&wptr,14);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put32bit(&wptr,getuid());
	put8bit(&wptr,mode&1);
	put8bit(&wptr,(mode>1)?0:eattr);
	if (tcpwrite(fd,reqbuff,22)!=22) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_EATTR) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		close_master_conn(1);
		return -1;
	} else if (leng!=2) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	nodeeattr = get8bit(&rptr) & eattr;
	functioneattr = get8bit(&rptr) & eattr;
	free(buff);
	printf("%s:",fname);
	printf(" nodeeattr:");
	if (nodeeattr==0) {
		printf("-");
	} else {
		// as for now there is only one eattr: noowner
		if (nodeeattr&EATTR_NOOWNER) {
			printf("noowner");
		} else {
			printf("?");
		}
	}
	printf("; workingeattr:");
	if (functioneattr==0) {
		printf("-");
	} else {
		// as for now there is only one eattr: noowner
		if (functioneattr&EATTR_NOOWNER) {
			printf("noowner");
		} else {
			printf("?");
		}
	}
	printf("\n");
	return 0;
}
*/

int quota_control(const char *fname,uint8_t mode,uint8_t *qflags,uint32_t *graceperiod,uint32_t *sinodes,uint64_t *slength,uint64_t *ssize,uint64_t *srealsize,uint32_t *hinodes,uint64_t *hlength,uint64_t *hsize,uint64_t *hrealsize) {
	uint32_t inode;
	uint32_t curinodes;
	uint64_t curlength,cursize,currealsize;

//	printf("set quota: %s (soft:%1X,i:%"PRIu32",l:%"PRIu64",w:%"PRIu64",r:%"PRIu64"),(hard:%1X,i:%"PRIu32",l:%"PRIu64",w:%"PRIu64",r:%"PRIu64")\n",fname,sflags,sinodes,slength,ssize,srealsize,hflags,hinodes,hlength,hsize,hrealsize);
	if (mode==2) {
		*qflags = 0;
	}
	if (master_prepare_conn(fname,&inode,NULL,NULL,0,(*qflags)?1:0)<0) {
		return -1;
	}
	master_new_packet();
	master_put32bit(inode);
	master_put8bit(*qflags);
	if (mode==0) {
		if (master_get_version()>=VERSION2INT(3,0,9)) {
			master_put32bit(*graceperiod);
		}
		master_put32bit(*sinodes);
		master_put64bit(*slength);
		master_put64bit(*ssize);
		master_put64bit(*srealsize);
		master_put32bit(*hinodes);
		master_put64bit(*hlength);
		master_put64bit(*hsize);
		master_put64bit(*hrealsize);
	}
	if (master_send_and_receive(CLTOMA_FUSE_QUOTACONTROL,MATOCL_FUSE_QUOTACONTROL)<0) {
		return -1;
	}
	if (master_get_leng()==1) {
		printf("%s: %s\n",fname,mfsstrerr(master_get8bit()));
		return -1;
	} else if (master_get_leng()!=85 && master_get_leng()!=89) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		master_error();
		return -1;
	}
	*qflags = master_get8bit();
	if (master_get_leng()==89) {
		*graceperiod = master_get32bit();
	} else {
		*graceperiod = 0;
	}
	*sinodes = master_get32bit();
	*slength = master_get64bit();
	*ssize = master_get64bit();
	*srealsize = master_get64bit();
	*hinodes = master_get32bit();
	*hlength = master_get64bit();
	*hsize = master_get64bit();
	*hrealsize = master_get64bit();
	curinodes = master_get32bit();
	curlength = master_get64bit();
	cursize = master_get64bit();
	currealsize = master_get64bit();
	if ((*graceperiod)>0) {
		printf("%s: (current values | soft quota | hard quota) ; soft quota grace period: %u seconds\n",fname,*graceperiod);
	} else {
		printf("%s: (current values | soft quota | hard quota) ; soft quota grace period: default\n",fname);
	}
	print_number(" inodes   | ",NULL,curinodes,0,0,1);
	print_number(" | ",NULL,*sinodes,0,0,(*qflags)&QUOTA_FLAG_SINODES);
	print_number(" | "," |\n",*hinodes,0,0,(*qflags)&QUOTA_FLAG_HINODES);
	print_number(" length   | ",NULL,curlength,0,1,1);
	print_number(" | ",NULL,*slength,0,1,(*qflags)&QUOTA_FLAG_SLENGTH);
	print_number(" | "," |\n",*hlength,0,1,(*qflags)&QUOTA_FLAG_HLENGTH);
	print_number(" size     | ",NULL,cursize,0,1,1);
	print_number(" | ",NULL,*ssize,0,1,(*qflags)&QUOTA_FLAG_SSIZE);
	print_number(" | "," |\n",*hsize,0,1,(*qflags)&QUOTA_FLAG_HSIZE);
	print_number(" realsize | ",NULL,currealsize,0,1,1);
	print_number(" | ",NULL,*srealsize,0,1,(*qflags)&QUOTA_FLAG_SREALSIZE);
	print_number(" | "," |\n",*hrealsize,0,1,(*qflags)&QUOTA_FLAG_HREALSIZE);
	if (master_end_packet()==0) {
		printf("%s: master query: packet size error\n",fname);
		return -1;
	}
	return 0;
}

/*
int get_quota(const char *fname) {
	printf("get quota: %s\n",fname);
	return 0;
}

int delete_quota(const char *fname,uint8_t sflags,uint8_t hflags) {
	printf("delete quota: %s (soft:%1X,hard:%1X)\n",fname,sflags,hflags);
	return 0;
}
*/

int snapshot_ctl(const char *dstdir,const char *dstbase,const char *srcname,uint32_t srcinode,uint8_t smode) {
	uint32_t dstinode,uid,gid;
	gid_t grouplist[NGROUPS_MAX];
	uint32_t i,gids;
	uint8_t addmaingroup;
	uint32_t nleng;
	uint16_t umsk;
	uint8_t status;

	umsk = umask(0);
	umask(umsk);
	nleng = strlen(dstbase);
	if (nleng>255) {
		printf("%s: name too long\n",dstbase);
		return -1;
	}
	if (master_prepare_conn(dstdir,&dstinode,NULL,NULL,0,1)<0) {
		return -1;
	}
	uid = getuid();
	gid = getgid();
	if (master_get_version()>=VERSION2INT(2,0,0)) {
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
	master_new_packet();
	master_put32bit(srcinode);
	master_put32bit(dstinode);
	master_putname(nleng,dstbase);
	master_put32bit(uid);
	if (master_get_version()<VERSION2INT(2,0,0)) {
		master_put32bit(gid);
	} else {
		master_put32bit(addmaingroup+gids);
		if (addmaingroup) {
			master_put32bit(gid);
		}
		for (i=0 ; i<gids ; i++) {
			master_put32bit(grouplist[i]);
		}
	}
	master_put8bit(smode);
	if (master_get_version()>=VERSION2INT(1,7,0)) {
		master_put16bit(umsk);
	}
	if (master_send_and_receive(CLTOMA_FUSE_SNAPSHOT,MATOCL_FUSE_SNAPSHOT)<0) {
		return -1;
	}
	if (master_get_leng()!=1) {
		if (srcname==NULL) {
			printf("%s/%s: master query: wrong answer (leng)\n",dstdir,dstbase);
		} else {
			printf("%s->%s/%s: master query: wrong answer (leng)\n",srcname,dstdir,dstbase);
		}
		return -1;
	}
	status = master_get8bit();
	if (status!=0) {
		if (srcname==NULL) {
			printf("%s/%s: %s\n",dstdir,dstbase,mfsstrerr(status));
		} else {
			printf("%s->%s/%s: %s\n",srcname,dstdir,dstbase,mfsstrerr(status));
		}
		return -1;
	}
	if (master_end_packet()==0) {
		if (srcname==NULL) {
			printf("%s/%s: master query: packet size error\n",dstdir,dstbase);
		} else {
			printf("%s->%s/%s: master query: packet size error\n",srcname,dstdir,dstbase);
		}
		return -1;
	}
	return 0;
}

int remove_snapshot(const char *dstname,uint8_t smode) {
	char dstpath[PATH_MAX+1],base[PATH_MAX+1],dir[PATH_MAX+1];

	if (realpath(dstname,dstpath)==NULL) {
		printf("%s: realpath error on %s: %s\n",dstname,dstpath,strerr(errno));
	}
	memcpy(dir,dstpath,PATH_MAX+1);
	dirname_inplace(dir);
	if (bsd_basename(dstpath,base)<0) {
		printf("%s: basename error\n",dstpath);
		return -1;
	}
	return snapshot_ctl(dir,base,NULL,0,smode | SNAPSHOT_MODE_DELETE);
}

int make_snapshot(const char *dstname,char * const *srcnames,uint32_t srcelements,uint8_t smode) {
	char to[PATH_MAX+1],base[PATH_MAX+1],dir[PATH_MAX+1];
	char src[PATH_MAX+1];
	struct stat sst,dst;
	int status;
	uint32_t i,l;

	if (stat(dstname,&dst)<0) {	// dst does not exist
		if (errno!=ENOENT) {
			printf("%s: stat error: %s\n",dstname,strerr(errno));
			return -1;
		}
		if (srcelements>1) {
			printf("can snapshot multiple elements only into existing directory\n");
			return -1;
		}
		if (lstat(srcnames[0],&sst)<0) {
			printf("%s: lstat error: %s\n",srcnames[0],strerr(errno));
			return -1;
		}
		if (bsd_dirname(dstname,dir)<0) {
			printf("%s: dirname error\n",dstname);
			return -1;
		}
		if (stat(dir,&dst)<0) {
			printf("%s: stat error: %s\n",dir,strerr(errno));
			return -1;
		}
		if (sst.st_dev != dst.st_dev) {
			printf("(%s,%s): both elements must be on the same device\n",dstname,srcnames[0]);
			return -1;
		}
		if (realpath(dir,to)==NULL) {
			printf("%s: realpath error on %s: %s\n",dir,to,strerr(errno));
			return -1;
		}
		if (bsd_basename(dstname,base)<0) {
			printf("%s: basename error\n",dstname);
			return -1;
		}
		if (strlen(dstname)>0 && dstname[strlen(dstname)-1]=='/' && !S_ISDIR(sst.st_mode)) {
			printf("directory %s does not exist\n",dstname);
			return -1;
		}
		return snapshot_ctl(to,base,srcnames[0],sst.st_ino,smode);
	} else {	// dst exists
		if (realpath(dstname,to)==NULL) {
			printf("%s: realpath error on %s: %s\n",dstname,to,strerr(errno));
			return -1;
		}
		if (!S_ISDIR(dst.st_mode)) {	// dst id not a directory
			if (srcelements>1) {
				printf("can snapshot multiple elements only into existing directory\n");
				return -1;
			}
			if (lstat(srcnames[0],&sst)<0) {
				printf("%s: lstat error: %s\n",srcnames[0],strerr(errno));
				return -1;
			}
			if (sst.st_dev != dst.st_dev) {
				printf("(%s,%s): both elements must be on the same device\n",dstname,srcnames[0]);
				return -1;
			}
			memcpy(dir,to,PATH_MAX+1);
			dirname_inplace(dir);
//			if (bsd_dirname(to,dir)<0) {
//				printf("%s: dirname error\n",to);
//				return -1;
//			}
			if (bsd_basename(to,base)<0) {
				printf("%s: basename error\n",to);
				return -1;
			}
			return snapshot_ctl(dir,base,srcnames[0],sst.st_ino,smode);
		} else {	// dst is a directory
			status = 0;
			for (i=0 ; i<srcelements ; i++) {
				if (lstat(srcnames[i],&sst)<0) {
					printf("%s: lstat error: %s\n",srcnames[i],strerr(errno));
					status=-1;
					continue;
				}
				if (sst.st_dev != dst.st_dev) {
					printf("(%s,%s): both elements must be on the same device\n",dstname,srcnames[i]);
					status=-1;
					continue;
				}
				if (!S_ISDIR(sst.st_mode)) {	// src is not a directory
					if (!S_ISLNK(sst.st_mode)) {	// src is not a symbolic link
						if (realpath(srcnames[i],src)==NULL) {
							printf("%s: realpath error on %s: %s\n",srcnames[i],src,strerr(errno));
							status=-1;
							continue;
						}
						if (bsd_basename(src,base)<0) {
							printf("%s: basename error\n",src);
							status=-1;
							continue;
						}
					} else {	// src is a symbolic link
						if (bsd_basename(srcnames[i],base)<0) {
							printf("%s: basename error\n",srcnames[i]);
							status=-1;
							continue;
						}
					}
					if (snapshot_ctl(to,base,srcnames[i],sst.st_ino,smode)<0) {
						status=-1;
					}
				} else {	// src is a directory
					l = strlen(srcnames[i]);
					if (l>0 && srcnames[i][l-1]!='/') {	// src is a directory and name has trailing slash
						if (realpath(srcnames[i],src)==NULL) {
							printf("%s: realpath error on %s: %s\n",srcnames[i],src,strerr(errno));
							status=-1;
							continue;
						}
						if (bsd_basename(src,base)<0) {
							printf("%s: basename error\n",src);
							status=-1;
							continue;
						}
						if (snapshot_ctl(to,base,srcnames[i],sst.st_ino,smode)<0) {
							status=-1;
						}
					} else {	// src is a directory and name has not trailing slash
						memcpy(dir,to,PATH_MAX+1);
						dirname_inplace(dir);
						//if (bsd_dirname(to,dir)<0) {
						//	printf("%s: dirname error\n",to);
						//	status=-1;
						//	continue;
						//}
						if (bsd_basename(to,base)<0) {
							printf("%s: basename error\n",to);
							status=-1;
							continue;
						}
						if (snapshot_ctl(dir,base,srcnames[i],sst.st_ino,smode)<0) {
							status=-1;
						}
					}
				}
			}
			return status;
		}
	}
}

enum {
	MFSGETGOAL=1,
	MFSSETGOAL,
	MFSCOPYGOAL,
	MFSGETSCLASS,
	MFSSETSCLASS,
	MFSXCHGSCLASS,
	MFSCOPYSCLASS,
	MFSLISTSCLASS,
	MFSGETTRASHTIME,
	MFSSETTRASHTIME,
	MFSCOPYTRASHTIME,
	MFSCHECKFILE,
	MFSFILEINFO,
	MFSAPPENDCHUNKS,
	MFSDIRINFO,
	MFSFILEREPAIR,
	MFSMAKESNAPSHOT,
	MFSRMSNAPSHOT,
	MFSGETEATTR,
	MFSSETEATTR,
	MFSDELEATTR,
	MFSCOPYEATTR,
	MFSGETQUOTA,
	MFSSETQUOTA,
	MFSDELQUOTA,
	MFSCOPYQUOTA,
	MFSFILEPATHS,
	MFSCHKARCHIVE,
	MFSSETARCHIVE,
	MFSCLRARCHIVE,
	MFSSCADMIN,
	MFSMKSC,
	MFSCHSC,
	MFSRMSC,
	MFSCPSC,
	MFSMVSC,
	MFSLSSC
};

static inline void print_numberformat_options() {
	fprintf(stderr," -n - show numbers in plain format\n");
	fprintf(stderr," -h - \"human-readable\" numbers using base 2 prefixes (IEC 60027)\n");
	fprintf(stderr," -H - \"human-readable\" numbers using base 10 prefixes (SI)\n");
	fprintf(stderr," -k - show plain numbers in kibis (binary kilo - 1024)\n");
	fprintf(stderr," -m - show plain numbers in mebis (binary mega - 1024^2)\n");
	fprintf(stderr," -g - show plain numbers in gibis (binary giga - 1024^3)\n");
}

static inline void print_recursive_option() {
	fprintf(stderr," -r - do it recursively\n");
}

static inline void print_extra_attributes() {
	int j;
	fprintf(stderr,"\nattributes:\n");
	for (j=0 ; j<EATTR_BITS ; j++) {
		if (eattrtab[j][0]) {
			fprintf(stderr," %s - %s\n",eattrtab[j],eattrdesc[j]);
		}
	}
}

void usage(int f) {
	switch (f) {
		case MFSGETGOAL:
			fprintf(stderr,"get objects goal (desired number of copies)\n\nusage: mfsgetgoal [-nhHkmgr] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSSETGOAL:
			fprintf(stderr,"set objects goal (desired number of copies)\n\nusage: mfssetgoal [-nhHkmgr] GOAL[-|+] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," GOAL+ - increase goal to given value\n");
			fprintf(stderr," GOAL- - decrease goal to given value\n");
			fprintf(stderr," GOAL - just set goal to given value\n");
			break;
		case MFSCOPYGOAL:
			fprintf(stderr,"copy object goal (desired number of copies)\n\nusage: mfscopygoal [-nhHkmgr] srcname dstname [dstname ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSGETSCLASS:
			fprintf(stderr,"get objects storage class (desired number of copies / labels)\n\nusage: mfsgetsclass [-nhHkmgr] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSSETSCLASS:
			fprintf(stderr,"set objects storage class (desired number of copies / labels)\n\nusage: mfssetsclass [-nhHkmgr] STORAGE_CLASS_NAME name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSCOPYSCLASS:
			fprintf(stderr,"copy object storage class (desired number of copies / labels)\n\nusage: mfscopysclass [-nhHkmgr] srcname dstname [dstname ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSXCHGSCLASS:
			fprintf(stderr,"exchange objects storage class (desired number of copies / labels)\n\nusage: mfsxchgsclass [-nhHkmgr] OLD_STORAGE_CLASS_NAME NEW_STORAGE_CLASS_NAME name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSLISTSCLASS:
			fprintf(stderr,"lists available storage classes (same as mfsscadmin list)\n\nusage: mfslistsclass [-al] [mountpoint_or_any_subfolder]\n");
			fprintf(stderr," -a - lists all storage classes (including standard goal classes)\n");
			fprintf(stderr," -l - lists storage classes with definitions (long format)\n");
			fprintf(stderr,"If mountpoint_or_any_subfolder is not specified then current directory will be used\n");
			break;
		case MFSGETTRASHTIME:
			fprintf(stderr,"get objects trashtime (how many seconds file should be left in trash)\n\nusage: mfsgettrashtime [-nhHkmgr] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSSETTRASHTIME:
			fprintf(stderr,"set objects trashtime (how many seconds file should be left in trash)\n\nusage: mfssettrashtime [-nhHkmgr] SECONDS[-|+] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," SECONDS+ - increase trashtime to given value\n");
			fprintf(stderr," SECONDS- - decrease trashtime to given value\n");
			fprintf(stderr," SECONDS - just set trashtime to given value\n");
			break;
		case MFSCOPYTRASHTIME:
			fprintf(stderr,"copy objects trashtime (how many seconds file should be left in trash)\n\nusage: mfscopytrashtime [-nhHkmgr] srcname dstname [dstname ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSCHECKFILE:
			fprintf(stderr,"check files\n\nusage: mfscheckfile [-nhHkmg] name [name ...]\n");
			break;
		case MFSFILEINFO:
			fprintf(stderr,"show files info (shows detailed info of each file chunk)\n\nusage: mfsfileinfo [-qcs] name [name ...]\n");
			fprintf(stderr,"switches:\n");
			fprintf(stderr," -q - quick info (show only number of valid copies)\n");
			fprintf(stderr," -c - receive chunk checksums from chunkservers\n");
			fprintf(stderr," -s - calculate file signature (using checksums)\n");
			break;
		case MFSAPPENDCHUNKS:
			fprintf(stderr,"append file chunks to another file. If destination file doesn't exist then it's created as empty file and then chunks are appended\n\nusage: mfsappendchunks [-s slice_from:slice_to] dstfile name [name ...]\n");
			break;
		case MFSDIRINFO:
			fprintf(stderr,"show directories stats\n\nusage: mfsdirinfo [-nhHkmg] [-idfclsr] [-p] name [name ...]\n");
			print_numberformat_options();
			fprintf(stderr,"'show' switches:\n");
			fprintf(stderr," -i - show number of inodes\n");
			fprintf(stderr," -d - show number of directories\n");
			fprintf(stderr," -f - show number of files\n");
			fprintf(stderr," -c - show number of chunks\n");
			fprintf(stderr," -l - show length\n");
			fprintf(stderr," -s - show size\n");
			fprintf(stderr," -r - show realsize\n");
			fprintf(stderr,"'mode' switch:\n");
			fprintf(stderr," -p - precise mode\n");
			fprintf(stderr,"\nIf no 'show' switches are present then show everything\n");
			fprintf(stderr,"\nMeaning of some not obvious output data:\n 'length' is just sum of files lengths\n 'size' is sum of chunks lengths\n 'realsize' is estimated hdd usage (usually size multiplied by current goal)\n");
			fprintf(stderr,"\nPrecise mode means that system takes into account repeated nodes/chunks\nand count them once, also uses current number of copies instead of goal.\n");
			break;
		case MFSFILEREPAIR:
			fprintf(stderr,"repair given file. Use it with caution. It forces file to be readable, so it could erase (fill with zeros) file when chunkservers are not currently connected.\n\nusage: mfsfilerepair [-nhHkmg] name [name ...]\n");
			print_numberformat_options();
			break;
		case MFSMAKESNAPSHOT:
			fprintf(stderr,"make snapshot (lazy copy)\n\nusage: mfsmakesnapshot [-ocp] src [src ...] dst\n");
			fprintf(stderr," -o - allow to overwrite existing objects\n");
			fprintf(stderr," -c - 'cp' mode for attributes (create objects using current uid,gid,umask etc.)\n");
			fprintf(stderr," -p - preserve hardlinks\n");
			break;
		case MFSRMSNAPSHOT:
			fprintf(stderr,"remove snapshot (quick rm -r)\n\nusage: mfsrmsnapshot [-f] name [name ...]\n");
			fprintf(stderr," -f - remove as much as possible (according to access rights and snapshot flags)\n");
			break;
		case MFSGETEATTR:
			fprintf(stderr,"get objects extra attributes\n\nusage: mfsgeteattr [-nhHkmgr] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSSETEATTR:
			fprintf(stderr,"set objects extra attributes\n\nusage: mfsseteattr [-nhHkmgr] -f attrname [-f attrname ...] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," -f attrname - specify attribute to set\n");
			print_extra_attributes();
			break;
		case MFSDELEATTR:
			fprintf(stderr,"delete objects extra attributes\n\nusage: mfsdeleattr [-nhHkmgr] -f attrname [-f attrname ...] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," -f attrname - specify attribute to delete\n");
			print_extra_attributes();
			break;
		case MFSCOPYEATTR:
			fprintf(stderr,"copy objects extra attributes\n\nusage: mfscopyeattr [-nhHkmgr] srcname dstname [dstname ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSGETQUOTA:
			fprintf(stderr,"get quota for given directory (directories)\n\nusage: mfsgetquota [-nhHkmg] dirname [dirname ...]\n");
			print_numberformat_options();
			break;
		case MFSSETQUOTA:
			fprintf(stderr,"set quota for given directory (directories)\n\nusage: mfssetquota [-nhHkmg] [-iI inodes] [-p grace_period] [-lL length] [-sS size] [-rR realsize] dirname [dirname ...]\n");
			print_numberformat_options();
			fprintf(stderr," -p - set grace period in seconds for soft quota\n");
			fprintf(stderr," -i/-I - set soft/hard limit for number of filesystem objects\n");
			fprintf(stderr," -l/-L - set soft/hard limit for sum of files lengths\n");
			fprintf(stderr," -s/-S - set soft/hard limit for sum of file sizes (chunk sizes)\n");
			fprintf(stderr," -r/-R - set soft/hard limit for estimated hdd usage (usually size multiplied by goal)\n");
			fprintf(stderr,"\nAll numbers can have decimal point and SI/IEC symbol prefix at the end\ndecimal (SI): (k - 10^3 , M - 10^6 , G - 10^9 , T - 10^12 , P - 10^15 , E - 10^18)\nbinary (IEC 60027): (Ki - 2^10 , Mi - 2^20 , Gi - 2^30 , Ti - 2^40 , Pi - 2^50 , Ei - 2^60 )\n");
			break;
		case MFSDELQUOTA:
			fprintf(stderr,"delete quota for given directory (directories)\n\nusage: mfsdelquota [-nhHkmgailsrAILSR] dirname [dirname ...]\n");
			print_numberformat_options();
			fprintf(stderr," -i/-I - delete inodes soft/hard quota\n");
			fprintf(stderr," -l/-L - delete length soft/hard quota\n");
			fprintf(stderr," -s/-S - delete size soft/hard quota\n");
			fprintf(stderr," -r/-R - delete real size soft/hard quota\n");
			fprintf(stderr," -a/-A - delete all soft/hard quotas\n");
			break;
		case MFSCOPYQUOTA:
			fprintf(stderr,"copy quota settings from one directory to another directory (directories)\n\nusage: mfscopyquota [-nhHkmg] srcdirname dstdirname [dstdirname ...]\n");
			print_numberformat_options();
			break;
		case MFSFILEPATHS:
			fprintf(stderr,"show all paths of given files or node numbers\n\nusage: mfsfilepaths name/inode [name/inode ...]\n");
			fprintf(stderr,"\nIn case of converting node to path, tool has to be run in mfs-mounted directory\n");
			break;
		case MFSCHKARCHIVE:
			fprintf(stderr,"checks if archive flag is set or not (when directory is specified then command will check it recursivelly)\n\nusage: mfschgarchive [-nhHkmg] name [name ...]\n");
			print_numberformat_options();
			break;
		case MFSSETARCHIVE:
			fprintf(stderr,"set archive flags in chunks (recursivelly for directories) - moves files to archive (use 'archive' goal/labels instead of 'keep' goal/labels)\n\nusage: mfssetarchive [-nhHkmg] name [name ...]\n");
			print_numberformat_options();
			break;
		case MFSCLRARCHIVE:
			fprintf(stderr,"clear archive flags in chunks (recursivelly for directories) - moves files from archive (use 'keep' goal/labels instead of 'archive' goal/labels) - it also changes ctime, so files will move back to archive after time specified in mfssetgoal\n\nusage: mfsclrarchive [-nhHkmg] name [name ...]\n");
			print_numberformat_options();
			break;
		case MFSSCADMIN:
		case MFSMKSC:
		case MFSCHSC:
		case MFSRMSC:
		case MFSCPSC:
		case MFSMVSC:
		case MFSLSSC:
			fprintf(stderr,"mfs storage class admin tool\n\nusage:\n");
			fprintf(stderr,"\tmfsscadmin [/mountpoint] create|make [-a admin_only] [-m mode] [-C create_labels] -K keep_labels [-A archive_labels -d archive_delay] sclass [sclass ...]\n");
			fprintf(stderr,"\tmfsscadmin [/mountpoint] create|make [-a admin_only] [-m mode] LABELS sclass [sclass ...]\n");
			fprintf(stderr,"\tmfsscadmin [/mountpoint] modify|change [-f] [-a admin_only] [-m mode] [-C create_labels] [-K keep_labels] [-A archive_labels] [-d archive_delay] sclass [sclass ...]\n");
			fprintf(stderr,"\tmfsscadmin [/mountpoint] delete|remove sclass [sclass ...]\n");
			fprintf(stderr,"\tmfsscadmin [/mountpoint] copy|duplicate src_sclass dst_sclass [dst_sclass ...]\n");
			fprintf(stderr,"\tmfsscadmin [/mountpoint] rename src_sclass_name dst_sclass_name\n");
			fprintf(stderr,"\tmfsscadmin [/mountpoint] list [-l]\n");
			fprintf(stderr,"\n");
			fprintf(stderr,"create/modify options:\n");
			fprintf(stderr," -m - set mode (options are: 'l' for 'loose', 's' for 'strict' and 'd' or not specified for 'default')\n");
			fprintf(stderr,"    'default' mode: if there are overloaded servers, system will wait for them, but in case of no space available will use other servers (disregarding the labels).\n");
			fprintf(stderr,"    'strict' mode: the system will wait for overloaded servers, but when there is no space on servers with correct labels then will not use them (return ENOSPC or leave chunk as undergoal).\n");
			fprintf(stderr,"    'loose' mode: the system will disregard the labels in both cases.\n");
			fprintf(stderr," -a - set admin only mode ( 0 - anybody can use this storage class, 1 - only admin can use this storage class ) - by default it set to 0\n");
			fprintf(stderr," -C - set labels used for creation chunks - when not specified then 'keep' labels are used\n");
			fprintf(stderr," -K - set labels used for keeping chunks - must be specified\n");
			fprintf(stderr," -A - set labels used for archiving chunks - when not specified then 'keep' labels are used\n");
			fprintf(stderr," -d - set number of days used to switch labels from 'keep' to 'archive' - must be specified when '-A' option is given\n");
			fprintf(stderr," -f - force modification of classes 1 to 9 (modify only)\n");
			fprintf(stderr,"\n");
			fprintf(stderr,"list options:\n");
			fprintf(stderr," -l - lists storage classes with definitions (long format)\n");
			fprintf(stderr,"\n");
			fprintf(stderr,"If '/mountpoint' parameter is not specified then current directory is used (it might be any subfolder of mountpoint).\n");
			fprintf(stderr,"All actions but list need 'admin' flag specified in mfsexports.cfg\n");
			break;
	}
	exit(1);
}

int main(int argc,char **argv) {
	int l,f,status;
	int i,j,found;
	int ch;
	int longmode = 0;
	int snapmode = 0;
	int rflag = 0;
	uint8_t dirinfomode = 0;
	uint8_t fileinfomode = 0;
	uint64_t v;
	uint8_t eattr = 0,goal = 1,smode = SMODE_SET;
	storage_class sc;
	uint16_t chgmask = 0;
	uint32_t trashtime = 86400;
	uint32_t sinodes = 0,hinodes = 0;
	uint64_t slength = 0,hlength = 0,ssize = 0,hsize = 0,srealsize = 0,hrealsize = 0;
	uint32_t graceperiod = 0;
	uint8_t qflags = 0;
	uint32_t scnleng;
	int64_t slice_from = 0,slice_to = 0x80000000;
	char *scadmin_mp = NULL;
	char *appendfname = NULL;
	char *srcname = NULL;
	char *hrformat;
	char storage_class_name[256];
	char src_storage_class_name[256];
	char *p;

	memset(&sc,0,sizeof(sc));
	sc.mode = SCLASS_MODE_STD;
	strerr_init();
	master_init();

	l = strlen(argv[0]);
#define CHECKNAME(name) ((strcmp(argv[0],name)==0) || (l>(int)(sizeof(name)-1) && strcmp((argv[0])+(l-sizeof(name)),"/" name)==0))

	if (CHECKNAME("mfstools")) {
		if (argc==2 && strcmp(argv[1],"create")==0) {
			fprintf(stderr,"create symlinks\n");
#define SYMLINK(name)	if (symlink(argv[0],name)<0) { \
				perror("error creating symlink '"name"'"); \
			}
			SYMLINK("mfsgetgoal")
			SYMLINK("mfssetgoal")
			SYMLINK("mfscopygoal")
			SYMLINK("mfsgetsclass")
			SYMLINK("mfssetsclass")
			SYMLINK("mfscopysclass")
			SYMLINK("mfsxchgsclass")
			SYMLINK("mfslistsclass")
			SYMLINK("mfsgettrashtime")
			SYMLINK("mfssettrashtime")
			SYMLINK("mfscopytrashtime")
			SYMLINK("mfscheckfile")
			SYMLINK("mfsfileinfo")
			SYMLINK("mfsappendchunks")
			SYMLINK("mfsdirinfo")
			SYMLINK("mfsfilerepair")
			SYMLINK("mfsmakesnapshot")
			SYMLINK("mfsrmsnapshot")
			SYMLINK("mfsgeteattr")
			SYMLINK("mfsseteattr")
			SYMLINK("mfsdeleattr")
			SYMLINK("mfscopyeattr")
			SYMLINK("mfsgetquota")
			SYMLINK("mfssetquota")
			SYMLINK("mfsdelquota")
			SYMLINK("mfscopyquota")
			SYMLINK("mfsfilepaths")
			SYMLINK("mfschkarchive")
			SYMLINK("mfssetarchive")
			SYMLINK("mfsclrarchive")
			SYMLINK("mfsscadmin")
			// deprecated tools:
			SYMLINK("mfsrgetgoal")
			SYMLINK("mfsrsetgoal")
			SYMLINK("mfsrgettrashtime")
			SYMLINK("mfsrsettrashtime")
			return 0;
		} else if (argc==1) {
			fprintf(stderr,"mfs multi tool\n\nusage:\n\tmfstools create - create symlinks (mfs<toolname> -> %s)\n",argv[0]);
			fprintf(stderr,"\tmfstools mfs<toolname> ... - work as a given tool\n");
			fprintf(stderr,"\ntools:\n");
			fprintf(stderr,"\tmfsgetgoal\n\tmfssetgoal\n\tmfscopygoal\n");
			fprintf(stderr,"\tmfsgetsclass\n\tmfssetsclass\n\tmfscopysclass\n\tmfsxchgsclass\n\tmfslistsclass\n");
			fprintf(stderr,"\tmfsgettrashtime\n\tmfssettrashtime\n\tmfscopytrashtime\n");
			fprintf(stderr,"\tmfsgeteattr\n\tmfsseteattr\n\tmfsdeleattr\n\tmfscopyeattr\n");
			fprintf(stderr,"\tmfsgetquota\n\tmfssetquota\n\tmfsdelquota\n\tmfscopyquota\n");
			fprintf(stderr,"\tmfscheckfile\n\tmfsfileinfo\n\tmfsappendchunks\n\tmfsdirinfo\n");
			fprintf(stderr,"\tmfsfilerepair\n\tmfsmakesnapshot\n\tmfsfilepaths\n");
			fprintf(stderr,"\tmfschkarchive\n\tmfssetarchive\n\tmfsclrarchive\n");
			fprintf(stderr,"\tmfsscadmin\n");
			return 1;
		}
		argv++;
		argc--;
		l = 0;
	}
	if (CHECKNAME("mfsscadmin")) {
		f = MFSSCADMIN;
		if (argc<=1) {
			usage(MFSSCADMIN);
		} else {
			argv++;
			argc--;
			l = 0;
			if (*argv[0]=='/') {
				scadmin_mp = *argv;
				argv++;
				argc--;
				if (argc==0) {
					fprintf(stderr,"missing command after mountpoint\n\n");
					usage(MFSSCADMIN);
				}
			}
			if (CHECKNAME("create")) {
				f = MFSMKSC;
			} else if (CHECKNAME("make")) {
				f = MFSMKSC;
			} else if (CHECKNAME("modify")) {
				f = MFSCHSC;
			} else if (CHECKNAME("change")) {
				f = MFSCHSC;
			} else if (CHECKNAME("delete")) {
				f = MFSRMSC;
			} else if (CHECKNAME("remove")) {
				f = MFSRMSC;
			} else if (CHECKNAME("copy")) {
				f = MFSCPSC;
			} else if (CHECKNAME("duplicate")) {
				f = MFSCPSC;
			} else if (CHECKNAME("rename")) {
				f = MFSMVSC;
			} else if (CHECKNAME("list")) {
				f = MFSLSSC;
			} else {
				fprintf(stderr,"unknown storage class admin command\n\n");
				usage(MFSSCADMIN);
			}
		}
	} else if (CHECKNAME("mfsgetgoal")) {
		f=MFSGETGOAL;
	} else if (CHECKNAME("mfsrgetgoal")) {
		f=MFSGETGOAL;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfsgetgoal -r\"\n");
	} else if (CHECKNAME("mfssetgoal")) {
		f=MFSSETGOAL;
	} else if (CHECKNAME("mfsrsetgoal")) {
		f=MFSSETGOAL;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfssetgoal -r\"\n");
	} else if (CHECKNAME("mfscopygoal")) {
		f=MFSCOPYGOAL;
	} else if (CHECKNAME("mfsgetsclass")) {
		f=MFSGETSCLASS;
	} else if (CHECKNAME("mfssetsclass")) {
		f=MFSSETSCLASS;
	} else if (CHECKNAME("mfsxchgsclass")) {
		f=MFSXCHGSCLASS;
	} else if (CHECKNAME("mfscopysclass")) {
		f=MFSCOPYSCLASS;
	} else if (CHECKNAME("mfslistsclass")) {
		f=MFSLISTSCLASS;
	} else if (CHECKNAME("mfsgettrashtime")) {
		f=MFSGETTRASHTIME;
	} else if (CHECKNAME("mfsrgettrashtime")) {
		f=MFSGETTRASHTIME;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfsgettrashtime -r\"\n");
	} else if (CHECKNAME("mfssettrashtime")) {
		f=MFSSETTRASHTIME;
	} else if (CHECKNAME("mfsrsettrashtime")) {
		f=MFSSETTRASHTIME;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfssettrashtime -r\"\n");
	} else if (CHECKNAME("mfscopytrashtime")) {
		f=MFSCOPYTRASHTIME;
	} else if (CHECKNAME("mfscheckfile")) {
		f=MFSCHECKFILE;
	} else if (CHECKNAME("mfsfileinfo")) {
		f=MFSFILEINFO;
	} else if (CHECKNAME("mfsappendchunks")) {
		f=MFSAPPENDCHUNKS;
	} else if (CHECKNAME("mfsdirinfo")) {
		f=MFSDIRINFO;
	} else if (CHECKNAME("mfsgeteattr")) {
		f=MFSGETEATTR;
	} else if (CHECKNAME("mfsseteattr")) {
		f=MFSSETEATTR;
	} else if (CHECKNAME("mfsdeleattr")) {
		f=MFSDELEATTR;
	} else if (CHECKNAME("mfscopyeattr")) {
		f=MFSCOPYEATTR;
	} else if (CHECKNAME("mfsgetquota")) {
		f=MFSGETQUOTA;
	} else if (CHECKNAME("mfssetquota")) {
		f=MFSSETQUOTA;
	} else if (CHECKNAME("mfsdelquota")) {
		f=MFSDELQUOTA;
	} else if (CHECKNAME("mfscopyquota")) {
		f=MFSCOPYQUOTA;
	} else if (CHECKNAME("mfsfilerepair")) {
		f=MFSFILEREPAIR;
	} else if (CHECKNAME("mfsmakesnapshot")) {
		f=MFSMAKESNAPSHOT;
	} else if (CHECKNAME("mfsrmsnapshot")) {
		f=MFSRMSNAPSHOT;
	} else if (CHECKNAME("mfsfilepaths")) {
		f=MFSFILEPATHS;
	} else if (CHECKNAME("mfschkarchive")) {
		f=MFSCHKARCHIVE;
	} else if (CHECKNAME("mfssetarchive")) {
		f=MFSSETARCHIVE;
	} else if (CHECKNAME("mfsclrarchive")) {
		f=MFSCLRARCHIVE;
	} else if (CHECKNAME("mfsmksc")) {
		f=MFSMKSC;
	} else if (CHECKNAME("mfschsc")) {
		f=MFSCHSC;
	} else if (CHECKNAME("mfsrmsc")) {
		f=MFSRMSC;
	} else if (CHECKNAME("mfscpsc")) {
		f=MFSCPSC;
	} else if (CHECKNAME("mfsmvsc")) {
		f=MFSMVSC;
	} else if (CHECKNAME("mfslssc")) {
		f=MFSLSSC;
	} else {
		fprintf(stderr,"unknown binary name\n");
		return 1;
	}
//	argc--;
//	argv++;

	hrformat = getenv("MFSHRFORMAT");
	if (hrformat) {
		if (hrformat[0]>='0' && hrformat[0]<='4') {
			humode=hrformat[0]-'0';
		}
		if (hrformat[0]=='h') {
			if (hrformat[1]=='+') {
				humode=3;
			} else {
				humode=1;
			}
		}
		if (hrformat[0]=='H') {
			if (hrformat[1]=='+') {
				humode=4;
			} else {
				humode=2;
			}
		}
	}

	// parse options
	switch (f) {
	case MFSMAKESNAPSHOT:
		while ((ch=getopt(argc,argv,"ocp"))!=-1) {
			switch(ch) {
			case 'o':
				snapmode |= SNAPSHOT_MODE_CAN_OVERWRITE;
				break;
			case 'c':
				snapmode |= SNAPSHOT_MODE_CPLIKE_ATTR;
				break;
			case 'p':
				snapmode |= SNAPSHOT_MODE_PRESERVE_HARDLINKS;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (argc<2) {
			usage(f);
		}
		return make_snapshot(argv[argc-1],argv,argc-1,snapmode);
	case MFSRMSNAPSHOT:
		while ((ch=getopt(argc,argv,"f"))!=-1) {
			switch (ch) {
			case 'f':
				snapmode |= SNAPSHOT_MODE_FORCE_REMOVAL;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSCOPYGOAL:
	case MFSCOPYSCLASS:
	case MFSCOPYTRASHTIME:
	case MFSCOPYEATTR:
		while ((ch=getopt(argc,argv,"rnhHkmg"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			case 'r':
				rflag=1;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (argc<2) {
			usage(f);
		}
		srcname = *argv;
		argc--;
		argv++;
		break;
	case MFSCOPYQUOTA:
		while ((ch=getopt(argc,argv,"nhHkmg"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (argc<2) {
			usage(f);
		}
		srcname = *argv;
		argc--;
		argv++;
		break;
	case MFSSETGOAL:
	case MFSSETSCLASS:
	case MFSXCHGSCLASS:
		while ((ch=getopt(argc,argv,"rnhHkmg"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			case 'r':
				rflag=1;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (argc==0) {
			usage(f);
		}
		if (f==MFSSETGOAL) {
			p = argv[0];
			// [1-9] | [1-9]+ | [1-9]-
			if (p[0]>'0' && p[0]<='9' && (p[1]=='\0' || ((p[1]=='-' || p[1]=='+') && p[2]=='\0'))) {
				goal = p[0]-'0';
				srcname = NULL;
				if (p[1]=='-') {
					smode = SMODE_DECREASE;
				} else if (p[1]=='+') {
					smode = SMODE_INCREASE;
				} else {
					smode = SMODE_SET;
				}
			} else {
				printf("%s: wrong goal definition\n",p);
				usage(f);
			}
		} else {
			goal = 0xFF;
			smode = SMODE_SET;
			if (f==MFSXCHGSCLASS) {
				p = argv[0];
				scnleng = strlen(p);
				if (scnleng>=256) {
					printf("%s: storage class name too long\n",p);
					usage(f);
				}
				memcpy(src_storage_class_name,p,scnleng);
				src_storage_class_name[scnleng]=0;
				argc--;
				argv++;
				smode |= SMODE_EXCHANGE;
			}
			p = argv[0];
			scnleng = strlen(p);
			if (scnleng>=256) {
				printf("%s: storage class name too long\n",p);
				usage(f);
			}
			memcpy(storage_class_name,p,scnleng);
			storage_class_name[scnleng]=0;
		}
		argc--;
		argv++;
		break;
	case MFSGETGOAL:
	case MFSGETSCLASS:
	case MFSGETTRASHTIME:
	case MFSSETTRASHTIME:
	case MFSGETEATTR:
		while ((ch=getopt(argc,argv,"rnhHkmg"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			case 'r':
				rflag=1;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (f==MFSSETTRASHTIME) {
			p = argv[0];
			if (argc==0) {
				usage(f);
			}
			if (p[0]>='0' && p[0]<='9') {
				trashtime = parse_period(p,&p);
			}
			while (p[0]==' ') {
				p++;
			}
			if (p[0]=='\0' || ((p[0]=='-' || p[0]=='+') && p[1]=='\0')) {
				if (p[0]=='-') {
					smode=SMODE_DECREASE;
				} else if (p[0]=='+') {
					smode=SMODE_INCREASE;
				}
			} else {
				fprintf(stderr,"trashtime should be given as number of seconds optionally folowed by '-' or '+'\n");
				usage(f);
			}
			argc--;
			argv++;
		}
		break;
	case MFSSETEATTR:
	case MFSDELEATTR:
		while ((ch=getopt(argc,argv,"rnhHkmgf:"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			case 'r':
				rflag=1;
				break;
			case 'f':
				found=0;
				for (i=0 ; found==0 && i<EATTR_BITS ; i++) {
					if (strcmp(optarg,eattrtab[i])==0) {
						found=1;
						eattr|=1<<i;
					}
				}
				if (!found) {
					fprintf(stderr,"unknown flag\n");
					usage(f);
				}
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (eattr==0 && argc>=1) {
			if (f==MFSSETEATTR) {
				fprintf(stderr,"no attribute(s) to set\n");
			} else {
				fprintf(stderr,"no attribute(s) to delete\n");
			}
			usage(f);
		}
		if (f==MFSSETEATTR) {
			smode = SMODE_INCREASE;
		} else {
			smode = SMODE_DECREASE;
		}
		break;
	case MFSFILEREPAIR:
	case MFSGETQUOTA:
	case MFSCHECKFILE:
	case MFSCHKARCHIVE:
	case MFSSETARCHIVE:
	case MFSCLRARCHIVE:
		while ((ch=getopt(argc,argv,"nhHkmg"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSFILEINFO:
		humode = 0;
		numbermode = 0;
		while ((ch=getopt(argc,argv,"qcs"))!=-1) {
			switch(ch) {
			case 'q':
				fileinfomode |= FILEINFO_QUICK;
				break;
			case 'c':
				fileinfomode |= FILEINFO_CRC;
				break;
			case 's':
				fileinfomode |= FILEINFO_SIGNATURE;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSDIRINFO:
		while ((ch=getopt(argc,argv,"nhHkmgidfclsrp"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
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
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSSETQUOTA:
		if (getuid()) {
			fprintf(stderr,"only root can change quota\n");
			usage(f);
		}
		while ((ch=getopt(argc,argv,"nhHkmgp:i:I:l:L:s:S:r:R:"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			case 'p':
				graceperiod = parse_period(optarg,&p);
				if (p[0]!='\0') {
					fprintf(stderr,"bad grace period\n");
					usage(f);
				}
				break;
			case 'i':
				if (my_get_number(optarg,&v,UINT32_MAX,0)<0) {
					fprintf(stderr,"bad inodes limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SINODES) {
					fprintf(stderr,"'soft inodes' quota defined twice\n");
					usage(f);
				}
				sinodes = v;
				qflags |= QUOTA_FLAG_SINODES;
				break;
			case 'I':
				if (my_get_number(optarg,&v,UINT32_MAX,0)<0) {
					fprintf(stderr,"bad inodes limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HINODES) {
					fprintf(stderr,"'hard inodes' quota defined twice\n");
					usage(f);
				}
				hinodes = v;
				qflags |= QUOTA_FLAG_HINODES;
				break;
			case 'l':
				if (my_get_number(optarg,&v,UINT64_MAX,1)<0) {
					fprintf(stderr,"bad length limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SLENGTH) {
					fprintf(stderr,"'soft length' quota defined twice\n");
					usage(f);
				}
				slength = v;
				qflags |= QUOTA_FLAG_SLENGTH;
				break;
			case 'L':
				if (my_get_number(optarg,&v,UINT64_MAX,1)<0) {
					fprintf(stderr,"bad length limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HLENGTH) {
					fprintf(stderr,"'hard length' quota defined twice\n");
					usage(f);
				}
				hlength = v;
				qflags |= QUOTA_FLAG_HLENGTH;
				break;
			case 's':
				if (my_get_number(optarg,&v,UINT64_MAX,1)<0) {
					fprintf(stderr,"bad size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SSIZE) {
					fprintf(stderr,"'soft size' quota defined twice\n");
					usage(f);
				}
				ssize = v;
				qflags |= QUOTA_FLAG_SSIZE;
				break;
			case 'S':
				if (my_get_number(optarg,&v,UINT64_MAX,1)<0) {
					fprintf(stderr,"bad size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HSIZE) {
					fprintf(stderr,"'hard size' quota defined twice\n");
					usage(f);
				}
				hsize = v;
				qflags |= QUOTA_FLAG_HSIZE;
				break;
			case 'r':
				if (my_get_number(optarg,&v,UINT64_MAX,1)<0) {
					fprintf(stderr,"bad real size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SREALSIZE) {
					fprintf(stderr,"'soft realsize' quota defined twice\n");
					usage(f);
				}
				srealsize = v;
				qflags |= QUOTA_FLAG_SREALSIZE;
				break;
			case 'R':
				if (my_get_number(optarg,&v,UINT64_MAX,1)<0) {
					fprintf(stderr,"bad real size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HREALSIZE) {
					fprintf(stderr,"'hard realsize' quota defined twice\n");
					usage(f);
				}
				hrealsize = v;
				qflags |= QUOTA_FLAG_HREALSIZE;
				break;
			default:
				usage(f);
			}
		}
		if (qflags==0) {
			fprintf(stderr,"quota options not defined\n");
			usage(f);
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSDELQUOTA:
		if (getuid()) {
			fprintf(stderr,"only root can change quota\n");
			usage(f);
		}
		while ((ch=getopt(argc,argv,"nhHkmgiIlLsSrRaA"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'k':
				numbermode=1;
				break;
			case 'm':
				numbermode=2;
				break;
			case 'g':
				numbermode=3;
				break;
			case 'i':
				if (qflags & QUOTA_FLAG_SINODES) {
					fprintf(stderr,"'soft inodes' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SINODES;
				break;
			case 'I':
				if (qflags & QUOTA_FLAG_HINODES) {
					fprintf(stderr,"'hard inodes' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HINODES;
				break;
			case 'l':
				if (qflags & QUOTA_FLAG_SLENGTH) {
					fprintf(stderr,"'soft length' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SLENGTH;
				break;
			case 'L':
				if (qflags & QUOTA_FLAG_HLENGTH) {
					fprintf(stderr,"'hard length' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HLENGTH;
				break;
			case 's':
				if (qflags & QUOTA_FLAG_SSIZE) {
					fprintf(stderr,"'soft size' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SSIZE;
				break;
			case 'S':
				if (qflags & QUOTA_FLAG_HSIZE) {
					fprintf(stderr,"'hard size' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HSIZE;
				break;
			case 'r':
				if (qflags & QUOTA_FLAG_SREALSIZE) {
					fprintf(stderr,"'soft realsize' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SREALSIZE;
				break;
			case 'R':
				if (qflags & QUOTA_FLAG_HREALSIZE) {
					fprintf(stderr,"'hard realsize' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HREALSIZE;
				break;
			case 'a':
				if (qflags & QUOTA_FLAG_SALL) {
					fprintf(stderr,"'all soft quotas' defined together with other soft quota options\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SALL;
				break;
			case 'A':
				if (qflags & QUOTA_FLAG_HALL) {
					fprintf(stderr,"'all hard quotas' defined together with other hard quota options\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HALL;
				break;
			default:
				usage(f);
			}
		}
		if (qflags==0) {
			fprintf(stderr,"quota options not defined\n");
			usage(f);
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSMKSC:
	case MFSCHSC:
		while ((ch=getopt(argc,argv,(f==MFSCHSC)?"d:A:K:C:m:a:f":"d:A:K:C:m:a:"))!=-1) {
			switch (ch) {
			case 'd':
				if (chgmask & SCLASS_CHG_ARCH_DELAY) {
					fprintf(stderr,"option '-d' defined more than once\n");
					usage(f);
				}
				sc.arch_delay = strtoul(optarg,NULL,10);
				chgmask |= SCLASS_CHG_ARCH_DELAY;
				break;
			case 'A':
				if (chgmask & SCLASS_CHG_ARCH_MASKS) {
					fprintf(stderr,"option '-A' defined more than once\n");
					usage(f);
				}
				if (parse_label_expr(optarg,&(sc.arch_labelscnt),sc.arch_labelmasks)<0) {
					usage(f);
				}
				chgmask |= SCLASS_CHG_ARCH_MASKS;
				break;
			case 'K':
				if (chgmask & SCLASS_CHG_KEEP_MASKS) {
					fprintf(stderr,"option '-K' defined more than once\n");
					usage(f);
				}
				if (parse_label_expr(optarg,&(sc.keep_labelscnt),sc.keep_labelmasks)<0) {
					usage(f);
				}
				chgmask |= SCLASS_CHG_KEEP_MASKS;
				break;
			case 'C':
				if (chgmask & SCLASS_CHG_CREATE_MASKS) {
					fprintf(stderr,"option '-C' defined more than once\n");
					usage(f);
				}
				if (parse_label_expr(optarg,&(sc.create_labelscnt),sc.create_labelmasks)<0) {
					usage(f);
				}
				chgmask |= SCLASS_CHG_CREATE_MASKS;
				break;
			case 'm':
				if (chgmask & SCLASS_CHG_MODE) {
					fprintf(stderr,"option '-m' defined more than once\n");
					usage(f);
				}
				if (optarg[0]=='l' || optarg[0]=='L') {
					sc.mode = SCLASS_MODE_LOOSE;
				} else if (optarg[0]=='s' || optarg[0]=='S') {
					sc.mode = SCLASS_MODE_STRICT;
				} else if (optarg[0]=='d' || optarg[0]=='D') {
					sc.mode = SCLASS_MODE_STD;
				} else {
					fprintf(stderr,"unknown create mode (option '-m')\n");
					usage(f);
				}
				chgmask |= SCLASS_CHG_MODE;
				break;
			case 'a':
				if (chgmask & SCLASS_CHG_ADMIN_ONLY) {
					fprintf(stderr,"option '-a' defined more than once\n");
					usage(f);
				}
				if (optarg[0]=='0' || optarg[0]=='n' || optarg[0]=='N' || optarg[0]=='f' || optarg[0]=='F') {
					sc.admin_only = 0;
				} else if (optarg[0]=='1' || optarg[0]=='y' || optarg[0]=='Y' || optarg[0]=='t' || optarg[0]=='T') {
					sc.admin_only = 1;
				} else {
					fprintf(stderr,"unknown value for admin only flag (option '-a')\n");
					usage(f);
				}
				chgmask |= SCLASS_CHG_ADMIN_ONLY;
				break;
			case 'f':
				if (chgmask & SCLASS_CHG_FORCE) {
					fprintf(stderr,"option '-f' defined more than once\n");
					usage(f);
				}
				chgmask |= SCLASS_CHG_FORCE;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (f==MFSMKSC) {
			if ((chgmask & (SCLASS_CHG_ARCH_MASKS|SCLASS_CHG_KEEP_MASKS|SCLASS_CHG_CREATE_MASKS)) == 0) {
				if (argc<2) {
					usage(f);
				}
				if (parse_label_expr(argv[0],&(sc.create_labelscnt),sc.create_labelmasks)<0) {
					usage(f);
				}
				argc--;
				argv++;
				sc.keep_labelscnt = sc.create_labelscnt;
				sc.arch_labelscnt = sc.create_labelscnt;
				for (i=0 ; i<sc.create_labelscnt ; i++) {
					for (j=0 ; j<MASKORGROUP ; j++) {
						sc.arch_labelmasks[i][j] = sc.keep_labelmasks[i][j] = sc.create_labelmasks[i][j];
					}
				}
			} else {
				if ((chgmask & SCLASS_CHG_ARCH_MASKS) && ((chgmask & SCLASS_CHG_KEEP_MASKS)==0)) {
					fprintf(stderr,"option '-A' without '-K'\n");
					usage(f);
				}
				if ((chgmask & SCLASS_CHG_CREATE_MASKS) && ((chgmask & SCLASS_CHG_KEEP_MASKS)==0)) {
					fprintf(stderr,"option '-C' without '-K'\n");
					usage(f);
				}
				if ((chgmask & SCLASS_CHG_ARCH_MASKS) && ((chgmask & SCLASS_CHG_ARCH_DELAY)==0)) {
					fprintf(stderr,"option '-A' without '-d'\n");
					usage(f);
				}
				if ((chgmask & SCLASS_CHG_ARCH_DELAY) && ((chgmask & SCLASS_CHG_ARCH_MASKS)==0)) {
					fprintf(stderr,"option '-A' without '-d'\n");
					usage(f);
				}
				if ((chgmask & SCLASS_CHG_KEEP_MASKS) && ((chgmask & SCLASS_CHG_ARCH_MASKS)==0)) {
					sc.arch_labelscnt = sc.keep_labelscnt;
					for (i=0 ; i<sc.keep_labelscnt ; i++) {
						for (j=0 ; j<MASKORGROUP ; j++) {
							sc.arch_labelmasks[i][j] = sc.keep_labelmasks[i][j];
						}
					}
				}
				if ((chgmask & SCLASS_CHG_KEEP_MASKS) && ((chgmask & SCLASS_CHG_CREATE_MASKS)==0)) {
					sc.create_labelscnt = sc.keep_labelscnt;
					for (i=0 ; i<sc.keep_labelscnt ; i++) {
						for (j=0 ; j<MASKORGROUP ; j++) {
							sc.create_labelmasks[i][j] = sc.keep_labelmasks[i][j];
						}
					}
				}
			}
		}
		break;
	case MFSCPSC:
		if (getopt(argc,argv,"")!=-1) {
			usage(f);
		}
		argc -= optind;
		argv += optind;
		if (argc<2) {
			usage(f);
		}
		srcname = *argv;
		argc--;
		argv++;
		break;
	case MFSMVSC:
		if (getopt(argc,argv,"")!=-1) {
			usage(f);
		}
		argc -= optind;
		argv += optind;
		if (argc!=2) {
			usage(f);
		}
		return move_sc(scadmin_mp,argv[0],argv[1]);
	case MFSLSSC:
	case MFSLISTSCLASS:
		while ((ch=getopt(argc,argv,"l"))!=-1) {
			switch(ch) {
			case 'l':
				longmode |= 1;
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		if (f==MFSLISTSCLASS) {
			if (argc==1) {
				scadmin_mp = *argv;
			} else if (argc>1) {
				usage(f);
			}
		} else {
			if (argc>0) {
				usage(f);
			}
		}
		return list_sc(scadmin_mp,longmode);
	case MFSAPPENDCHUNKS:
		while ((ch=getopt(argc,argv,"s:"))!=-1) {
			switch(ch) {
			case 's':
				if (parse_slice_expr(optarg,&slice_from,&slice_to)<0) {
					usage(f);
				}
				break;
			default:
				usage(f);
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSRMSC:
	default:
		if (getopt(argc,argv,"")!=-1) {
			usage(f);
		}
		argc -= optind;
		argv += optind;
//		argc--;	// skip appname
//		argv++;
	}

	if (f==MFSAPPENDCHUNKS) {
		if (argc<=1) {
			usage(f);
		}
		appendfname = argv[0];
		i = open(appendfname,O_RDWR | O_CREAT,0666);
		if (i<0) {
			fprintf(stderr,"can't create/open file: %s\n",appendfname);
			return 1;
		}
		close(i);
		argc--;
		argv++;
	}

	if (argc<1) {
		usage(f);
	}
	status=0;
	if (f==MFSCOPYGOAL || f==MFSCOPYSCLASS) {
		if (get_sclass(srcname,&goal,storage_class_name,GMODE_NORMAL)<0) {
			return 1;
		}
		smode = SMODE_SET;
	} else if (f==MFSCOPYTRASHTIME) {
		if (get_trashtime(srcname,&trashtime,GMODE_NORMAL)<0) {
			return 1;
		}
		smode = SMODE_SET;
	} else if (f==MFSCOPYEATTR) {
		if (get_eattr(srcname,&eattr,GMODE_NORMAL)<0) {
			return 1;
		}
		smode = SMODE_SET;
	} else if (f==MFSCOPYQUOTA) {
		if (quota_control(srcname,2,&qflags,&graceperiod,&sinodes,&slength,&ssize,&srealsize,&hinodes,&hlength,&hsize,&hrealsize)<0) {
			return 1;
		}
	}
	while (argc>0) {
		switch (f) {
		case MFSGETGOAL:
		case MFSGETSCLASS:
			if (get_sclass(*argv,&goal,storage_class_name,(rflag)?GMODE_RECURSIVE:GMODE_NORMAL)<0) {
				status = 1;
			}
			break;
		case MFSSETGOAL:
		case MFSSETSCLASS:
		case MFSCOPYGOAL:
		case MFSCOPYSCLASS:
		case MFSXCHGSCLASS:
			if (set_sclass(*argv,goal,src_storage_class_name,storage_class_name,(rflag)?(smode | SMODE_RMASK):smode)<0) {
				status = 1;
			}
			break;
		case MFSGETTRASHTIME:
			if (get_trashtime(*argv,&trashtime,(rflag)?GMODE_RECURSIVE:GMODE_NORMAL)<0) {
				status = 1;
			}
			break;
		case MFSSETTRASHTIME:
		case MFSCOPYTRASHTIME:
			if (set_trashtime(*argv,trashtime,(rflag)?(smode | SMODE_RMASK):smode)<0) {
				status = 1;
			}
			break;
		case MFSCHECKFILE:
			if (file_info(FILEINFO_QUICK,*argv)<0) {
				status = 1;
			}
			break;
		case MFSFILEINFO:
			if (file_info(fileinfomode,*argv)<0) {
				status = 1;
			}
			break;
		case MFSAPPENDCHUNKS:
			if (append_file(appendfname,*argv,slice_from,slice_to)<0) {
				status = 1;
			}
			break;
		case MFSDIRINFO:
			if (dir_info(dirinfomode,*argv)<0) {
				status = 1;
			}
			break;
		case MFSFILEREPAIR:
			if (file_repair(*argv)<0) {
				status = 1;
			}
			break;
		case MFSGETEATTR:
			if (get_eattr(*argv,&eattr,(rflag)?GMODE_RECURSIVE:GMODE_NORMAL)<0) {
				status = 1;
			}
			break;
		case MFSSETEATTR:
		case MFSDELEATTR:
		case MFSCOPYEATTR:
			if (set_eattr(*argv,eattr,(rflag)?(smode | SMODE_RMASK):smode)<0) {
				status = 1;
			}
			break;
		case MFSGETQUOTA:
			if (quota_control(*argv,2,&qflags,&graceperiod,&sinodes,&slength,&ssize,&srealsize,&hinodes,&hlength,&hsize,&hrealsize)<0) {
				status = 1;
			}
			break;
		case MFSSETQUOTA:
		case MFSCOPYQUOTA:
			if (quota_control(*argv,0,&qflags,&graceperiod,&sinodes,&slength,&ssize,&srealsize,&hinodes,&hlength,&hsize,&hrealsize)<0) {
				status = 1;
			}
			break;
		case MFSDELQUOTA:
			if (quota_control(*argv,1,&qflags,&graceperiod,&sinodes,&slength,&ssize,&srealsize,&hinodes,&hlength,&hsize,&hrealsize)<0) {
				status = 1;
			}
			break;
		case MFSFILEPATHS:
			if (file_paths(*argv)<0) {
				status = 1;
			}
			break;
		case MFSCHKARCHIVE:
			if (archive_control(*argv,ARCHCTL_GET)<0) {
				status = 1;
			}
			break;
		case MFSSETARCHIVE:
			if (archive_control(*argv,ARCHCTL_SET)<0) {
				status = 1;
			}
			break;
		case MFSCLRARCHIVE:
			if (archive_control(*argv,ARCHCTL_CLR)<0) {
				status = 1;
			}
			break;
		case MFSRMSNAPSHOT:
			if (remove_snapshot(*argv,snapmode)<0) {
				status = 1;
			}
			break;
		case MFSMKSC:
			if (make_sc(scadmin_mp,*argv,&sc)<0) {
				status = 1;
			}
			break;
		case MFSCHSC:
			if (change_sc(scadmin_mp,*argv,chgmask,&sc)<0) {
				status = 1;
			}
			break;
		case MFSRMSC:
			if (remove_sc(scadmin_mp,*argv)<0) {
				status = 1;
			}
			break;
		case MFSCPSC:
			if (copy_sc(scadmin_mp,srcname,*argv)<0) {
				status = 1;
			}
			break;
		}
		argc--;
		argv++;
	}
	return status;
}
