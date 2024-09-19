#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "datapack.h"
#include "sockets.h"
#include "strerr.h"
#include "mfsstrerr.h"
#include "timeparser.h"

#include "tools_common.h"

// const char id[]="@(#) version: " VERSSTR ", written by Jakub Kruszona-Zawadzki";

static int current_master = -1;
static uint32_t current_masterip = 0;
static uint16_t current_masterport = 0;
static uint16_t current_mastercuid = 0;
static uint32_t current_masterversion = 0;
static uint64_t current_masterprocessid = 0;

#define PHN_USESI       0x01
#define PHN_USEIEC      0x00

#define INODE_VALUE_MASK        0x1FFFFFFF
#define INODE_TYPE_MASK         0x60000000
#define INODE_TYPE_TRASH        0x20000000
#define INODE_TYPE_SUSTAINED    0x40000000
#define INODE_TYPE_SPECIAL      0x00000000

static uint8_t humode = 0;
static uint8_t numbermode = 0;

static const char *appname;
static const char *subcommand;

uint32_t getmasterversion (void) {
	return current_masterversion;
}

void set_hu_flags(int ch) {
	switch (ch) {
		case 'n':
			humode = 0;
			break;
		case 'h':
			humode = 1;
			break;
		case 'H':
			humode = 2;
			break;
		case 'k':
			numbermode = 1;
			break;
		case 'm':
			numbermode = 2;
			break;
		case 'g':
			numbermode = 3;
			break;
	}
}

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

int my_get_number(const char *str,uint64_t *ret,uint64_t max,uint8_t bytesflag) {
	uint64_t pval,val,frac,fracdiv,mult;
	long double drval;
	int f;
	pval=0;
	val=0;
	frac=0;
	fracdiv=1;
	f=0;
	while (*str>='0' && *str<='9') {
		f=1;
		val*=10;
		val+=(*str-'0');
		str++;
		if (val<pval) {
			return -2;
		}
		pval = val;
	}
	if (*str=='.') {	// accept ".5" (without 0)
		str++;
		while (*str>='0' && *str<='9') {
			if (fracdiv<UINT64_C(1000000000000000000)) { // if there are more than 18 digits after '.' then ignore them silently
				fracdiv*=10;
				frac*=10;
				frac+=(*str-'0');
			}
			str++;
		}
		if (fracdiv==1) {	// if there was '.' expect number afterwards
			return -1;
		}
	} else if (f==0) {	// but not empty string
		return -1;
	}
	if (str[0]=='\0' || (bytesflag && str[0]=='B' && str[1]=='\0')) {	//
		mult = UINT64_C(1);
	} else if (str[0]!='\0' && (str[1]=='\0' || (bytesflag && str[1]=='B' && str[2]=='\0'))) {
		switch (str[0]) {
		case 'k':
			mult = UINT64_C(1000);
			break;
		case 'M':
			mult = UINT64_C(1000000);
			break;
		case 'G':
			mult = UINT64_C(1000000000);
			break;
		case 'T':
			mult = UINT64_C(1000000000000);
			break;
		case 'P':
			mult = UINT64_C(1000000000000000);
			break;
		case 'E':
			mult = UINT64_C(1000000000000000000);
			break;
		default:
			return -1;
		}
	} else if (str[0]!='\0' && str[1]=='i' && (str[2]=='\0' || (bytesflag && str[2]=='B' && str[3]=='\0'))) {
		switch (str[0]) {
		case 'K':
			mult = UINT64_C(1024);
			break;
		case 'M':
			mult = UINT64_C(1048576);
			break;
		case 'G':
			mult = UINT64_C(1073741824);
			break;
		case 'T':
			mult = UINT64_C(1099511627776);
			break;
		case 'P':
			mult = UINT64_C(1125899906842624);
			break;
		case 'E':
			mult = UINT64_C(1152921504606846976);
			break;
		default:
			return -1;
		}
	} else {
		return -1;
	}
	if (frac==0) { // no fraction ?
		if (val > (UINT64_MAX / mult)) {
			return -2;
		}
		val *= mult;
		if (val > max) {
			return -2;
		}
	} else {
		drval = frac;
		drval /= fracdiv;
		drval += val;
		drval *= mult;
		if (drval>UINT64_MAX) { // value too big - warning 2**64 can pass this because of rounding UINT64_MAX to closest double (and long double) which is 2**64 not (2**64-1) !!!
			return -2;
		}
		val = (uint64_t)(drval + 0.5);
		if (val < drval-1.0) { // arithmetic overflow - practically we need this mainly because of valuse close to 2**64 that can pass previous value
			return -2;
		}
		if (val > max) {
			return -2;
		}
	}
	*ret = val;
	return 1;
}

void print_number(uint64_t number, uint8_t flags) {
	if (humode>0) {
		if (flags & PNUM_BYTES) {
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
			if (flags & PNUM_32BIT) {
				printf((flags & PNUM_ALIGN)?" (%10"PRIu32")":" (%"PRIu32")",(uint32_t)number);
			} else {
				printf((flags & PNUM_ALIGN)?" (%20"PRIu64")":" (%"PRIu64")",number);
			}
		}
	} else {
		if (numbermode==0) {
			if (flags & PNUM_32BIT) {
				printf((flags & PNUM_ALIGN)?"%10"PRIu32:"%"PRIu32, (uint32_t)number);
			} else {
				printf((flags & PNUM_ALIGN)?"%20"PRIu64:"%"PRIu64, number);
			}
		} else if (numbermode==1) {
			if (flags & PNUM_32BIT) {
				printf((flags & PNUM_ALIGN)?"%7"PRIu32:"%"PRIu32, ((uint32_t)number)/1024);
			} else {
				printf((flags & PNUM_ALIGN)?"%17"PRIu64:"%"PRIu64, number/1024);
			}
		} else if (numbermode==2) {
			if (flags & PNUM_32BIT) {
				printf((flags & PNUM_ALIGN)?"%4"PRIu32:"%"PRIu32, ((uint32_t)number)/(1024*1024));
			} else {
				printf((flags & PNUM_ALIGN)?"%14"PRIu64:"%"PRIu64, number/(1024*1024));
			}
		} else if (numbermode==3) {
			if (flags & PNUM_32BIT) {
				printf((flags & PNUM_ALIGN)?"%1"PRIu32:"%"PRIu32, ((uint32_t)number)/(1024*1024*1024));
			} else {
				printf((flags & PNUM_ALIGN)?"%11"PRIu64:"%"PRIu64, number/(1024*1024*1024));
			}
		}
	}
}

void print_number_desc(const char *prefix, const char *suffix, uint64_t number, uint8_t flags) {
	if (prefix) {
		printf("%s",prefix);
	}
	print_number(number,flags|PNUM_ALIGN);
	if (suffix) {
		printf("%s",suffix);
	}
}

uint8_t print_number_size(uint8_t flags) {
	switch (humode) {
		case 0:
			if (numbermode==0) {
				return (flags&PNUM_32BIT)?10:20;
			} else if (numbermode==1) {
				return (flags&PNUM_32BIT)?7:17;
			} else if (numbermode==2) {
				return (flags&PNUM_32BIT)?4:14;
			} else if (numbermode==3) {
				return (flags&PNUM_32BIT)?1:11;
			}
			break;
		case 1:
			return 6;
		case 2:
			return 5;
		case 3:
			return (flags&PNUM_32BIT)?19:29;
		case 4:
			return (flags&PNUM_32BIT)?18:28;
	}
	return 0;
}

void print_number_quota(const char *prefix, const char *suffix, uint64_t number, uint8_t flags) {
	uint8_t tsize;

	if (prefix) {
		printf("%s",prefix);
	}
	if (flags & PNUM_VALID) {
		print_number(number,flags|PNUM_ALIGN);
	} else {
		tsize = print_number_size(flags);
		while (tsize>1) {
			putchar(' ');
			tsize--;
		}
		if (tsize>0) {
			putchar('-');
		}
	}
	if (suffix) {
		printf("%s",suffix);
	}
}

int bsd_basename(const char *path,char *bname) {	//snapshot:make/remove_snapshot()
	const char *endp, *startp;

	// Empty or NULL string gets treated as "."
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	// Strip trailing slashes
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	// All slashes becomes "/"
	if (endp == path && *endp == '/') {
		(void)strcpy(bname, "/");
		return 0;
	}

	// Find the start of the base
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

int bsd_dirname(const char *path,char *bname) {		//snapshot
	const char *endp;

	// Empty or NULL string gets treated as "."
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	// Strip trailing slashes
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	// Find the start of the dir
	while (endp > path && *endp != '/') {
		endp--;
	}

	// Either the dir is "/" or there are no slashes
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

void dirname_inplace(char *path) {					//snapshot
	char *endp;

	if (path==NULL) {
		return;
	}
	if (path[0]=='\0') {
		path[0]='.';
		path[1]='\0';
		return;
	}

	// Strip trailing slashes
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	// Find the start of the dir
	while (endp > path && *endp != '/') {
		endp--;
	}

	if (endp == path) {
		if (path[0]!='/') {
			path[0]='.';
		}
		path[1]='\0';
		return;
	} else {
		*endp = '\0';
	}
}

int mystrmatch(const char *pat, const char *str, uint8_t insens) { //return 0 if str matches to pat
	char p, s;
	uint32_t chrmask[8];

//	printf("pat: %s ; str: %s\n",pat,str);

	do {
		if ((*pat == '*' && !pat[1])    //pattern ended with '*'
		|| !(*pat|*str)) {            //end of both
			return 0;
		}

		if (*pat == '?') {
			if (!*str) {                //no expected character
				return -1;
			}
			pat++;
			str++;
			if (!(*pat|*str)) {
				return 0;
			}
			return mystrmatch(pat, str, insens);
		}


		//char range
		if (*pat == '[') {
			uint8_t neg;
			//printf("***range in regexp***\n");
			memset(chrmask,0,sizeof(chrmask)); // = 0;
			s = 0;
			neg = 0;
			if (pat[1]=='!') {
				pat++;
				neg = 1;
			}
			while ((p=*++pat) != ']') {
				if (p == '-') { //range up to
					++pat;
					if (!s
					|| !*pat
					|| *pat==']'
					|| *pat=='-'
					|| *pat<s) {
						fprintf(stderr,"error in range definition around '-' character\n");
						return -1;
					}
					//printf("range from: %c, to: %c\n", s, *pat);
					while(++s <= *pat) {
						chrmask[s>>5] |= 1<<(s&0x1F);
					}
				} else { //normal character to matching
					s=p;
					chrmask[s>>5] |= 1<<(s&0x1F);
				}
			}
			//printf("after ']': %c", pat[1]);
			s = *str;
			if (insens) {
				s = tolower(s);
			}
			if (neg) {
				if (chrmask[s>>5] & (1<<(s&0x1F))) {
					return -1;
				}
			} else {
				if (!(chrmask[s>>5] & (1<<(s&0x1F)))) {
					return -1;
				}
			}
			return mystrmatch(++pat, ++str, insens);
		}

		if (*pat == '*') {                //look for match after '*'
			while (*++pat == '*');    //skip '*'
			while (*pat == '?') {
				++pat;
				if (!*str++) {        //end of string - scname too short
					return -1;
				}
			}
			if (!*pat) {                //end of pattern
				return 0;
			}
			while (*str) {//after '*' and optional'?'
//				p = insens ? tolower(*pat) : *pat;
//				while (*str) {        //looking for same 1st character ???? while 1    <<<<<
//					if (p == (insens ? tolower(*str) : *str)) {
//						break;
//					}
//					if (!*++str) {                //end of string - no matching         <<<<<
//						return -1;
//					}
//				}
				if (mystrmatch(pat, str, insens) == 0) {
					return 0;
				}
				str++;
			};
			return -1;
		}
		//spec char
		if (*pat == '\\') {
			++pat;
		}
		//text compare
		p = *pat++;
		s = *str++;
		if (insens) {
			p = tolower(p);
			s = tolower(s);
		}
	} while (p == s);

	return -1;
}//*/

int master_register(int rfd) {
#define PAYLOAD 73
	uint32_t i;
	const uint8_t *rptr;
	uint8_t *wptr,regbuff[8+PAYLOAD];

	wptr = regbuff;
	put32bit(&wptr, CLTOMA_FUSE_REGISTER);
	put32bit(&wptr, PAYLOAD);

	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr += 64;

	put8bit(&wptr,REGISTER_TOOLS);
	put32bit(&wptr,current_mastercuid);
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);

	if (tcpwrite(rfd,regbuff,8+PAYLOAD) != 8+PAYLOAD) {
		fprintf(stderr,"register to master: send error\n");
		goto error;
	}
	if (tcpread(rfd,regbuff,9) != 9) {
		fprintf(stderr,"register to master: receive error\n");
		goto error;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCL_FUSE_REGISTER) {
		fprintf(stderr,"register to master: wrong answer (type)\n");
		goto error;
	}
	i = get32bit(&rptr);
	if (i!=1) {
		fprintf(stderr,"register to master: wrong answer (length)\n");
		goto error;
	}
	if (*rptr) {
		fprintf(stderr,"register to master: %s\n",mfsstrerr(*rptr));
		goto error;
	}

//ok:
	return 0;

error:
	return -1;
#undef PAYLOAD
}

int master_socket(void) {
	return current_master;
}

int master_reconnect(void) {
	int sd;
	uint8_t cnt;

	if (current_master>=0) {
		close(current_master);
		current_master = -1;
	}
	cnt = 0;
	while (cnt<10) {
		sd = tcpsocket();
		if (sd<0) {
			fprintf(stderr,"can't create connection socket: %s\n",strerr(errno));
			return -1;
		}
		tcpreuseaddr(sd);
		tcpnumbind(sd,0,0);
		if (tcpnumtoconnect(sd, current_masterip, current_masterport, (cnt%2) ? (300*(1<<(cnt>>1))) : (200*(1<<(cnt>>1))))<0) {
			cnt++;
			if (cnt==10) {
				fprintf(stderr,"can't connect to master: %s\n", strerr(errno));
				return -1;
			}
			tcpclose(sd);
		} else {
			cnt = 10;
		}
	}
	tcpnodelay(sd);
	if (current_masterversion<VERSION2INT(2,0,0)) {
		if (master_register(sd)<0) {
			fprintf(stderr,"can't register to master\n");
			return -1;
		}
	}
	current_master = sd;
	return 0;
}

void reset_master(void) {
	if (current_master<0) {
		return;
	}
	close(current_master);
	current_master = -1;
	current_masterprocessid = 0;
}

int open_master_conn(const char *name, uint32_t *inode, mode_t *mode, uint64_t *leng, uint8_t needsamedev, uint8_t needrwfs) {
	char rpath[PATH_MAX+1];
	struct stat stb;
	struct statvfs stvfsb;
	int fd;
	uint8_t masterinfo[22];
	const uint8_t *miptr;
	uint32_t pinode;
	uint64_t master_processid;
	int rpathlen;

	rpath[0]=0;
	if (realpath(name,rpath)==NULL) {
		fprintf(stderr,"%s: realpath error on (%s): %s\n", name, rpath, strerr(errno));
		return -1;
	}
//	p = rpath;
	if (needrwfs) {
		if (statvfs(rpath,&stvfsb)!=0) {
			fprintf(stderr,"%s: (%s) statvfs error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
		if (stvfsb.f_flag&ST_RDONLY) {
			fprintf(stderr,"%s: (%s) Read-only file system\n", name, rpath);
			return -1;
		}
	}
	if (lstat(rpath,&stb)!=0) {
		fprintf(stderr,"%s: (%s) lstat error: %s\n", name, rpath, strerr(errno));
		return -1;
	}
	pinode = stb.st_ino;
	if (inode!=NULL) {
		*inode = pinode;
	}
	if (mode!=NULL) {
		*mode = stb.st_mode;
	}
	if (leng!=NULL) {
		*leng = stb.st_size;
	}
	for (;;) {
		rpathlen = strlen(rpath);
		if (rpathlen+strlen("/.masterinfo")<PATH_MAX) {
			strcpy(rpath+rpathlen,"/.masterinfo");
			if (lstat(rpath,&stb)==0) {
				if ((stb.st_ino==0x7FFFFFFF || stb.st_ino==0x7FFFFFFE) && stb.st_nlink==1 && stb.st_uid==0 && stb.st_gid==0 && (stb.st_size==10 || stb.st_size==14 || stb.st_size==22)) {
					if (stb.st_ino==0x7FFFFFFE && inode!=NULL) {	// meta master
						if (((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_SUSTAINED) {
							fprintf(stderr,"%s: only files in 'trash' and 'sustained' are usable in mfsmeta\n",name);
							return -1;
						}
						(*inode)&=INODE_VALUE_MASK;
					}
					fd = open(rpath,O_RDONLY);
					if (stb.st_size==10) {
						if (read(fd,masterinfo,10)!=10) {
							fprintf(stderr,"%s: can't read '.masterinfo'\n",name);
							close(fd);
							return -1;
						}
					} else if (stb.st_size==14) {
						if (read(fd,masterinfo,14)!=14) {
							fprintf(stderr,"%s: can't read '.masterinfo'\n",name);
							close(fd);
							return -1;
						}
					} else if (stb.st_size==22) {
						if (read(fd,masterinfo,22)!=22) {
							fprintf(stderr,"%s: can't read '.masterinfo'\n",name);
							close(fd);
							return -1;
						}
					}
					close(fd);
					miptr = masterinfo;
					current_masterip   = get32bit(&miptr);
					current_masterport = get16bit(&miptr);
					current_mastercuid = get32bit(&miptr);
					if (stb.st_size>=14) {
						current_masterversion = get32bit(&miptr);
					} else {
						current_masterversion = 0;
					}
					if (stb.st_size>=22) {
						master_processid = get64bit(&miptr);
					} else {
						master_processid = stb.st_dev;
					}
					if (current_master>=0) {
						if (current_masterprocessid==master_processid) {
							return 0;
						}
						if (needsamedev) {
							fprintf(stderr,"%s: different master process id\n",name);
							return -1;
						}
					}
					if (current_master>=0) {
						close(current_master);
						current_master=-1;
					}
					current_masterprocessid = master_processid;
					if (current_masterip==0 || current_masterport==0 || current_mastercuid==0) {
						fprintf(stderr,"%s: incorrect '.masterinfo'\n",name);
						return -1;
					}
					return master_reconnect();
				}
			} else if (pinode==1) { // this is root inode - if there is no .masterinfo here then it is not MFS.
				fprintf(stderr,"%s: not MFS object\n", name);
				return -1;
			}
		} else if (pinode==1) { // found root inode, but path is still to long - give up
			fprintf(stderr,"%s: path too long\n", name);
			return -1;
		}
		rpath[rpathlen]='\0';
		if (rpath[0]!='/' || rpath[1]=='\0') { // went to '/' without success - this is not MFS
			fprintf(stderr,"%s: not MFS object\n", name);
			return -1;
		}
		dirname_inplace(rpath);
		if (lstat(rpath,&stb)!=0) {
			fprintf(stderr,"%s: (%s) lstat error: %s\n", name, rpath, strerr(errno));
			return -1;
		}
		pinode = stb.st_ino;
	}
	return -1;
}

int tparse_seconds(const char *str,uint32_t *ret) {
	uint32_t rettmp;
	switch (parse_speriod(str,&rettmp)) {
		case TPARSE_OK:
			*ret = rettmp;
			return 0;
		case TPARSE_UNEXPECTED_CHAR:
			if (rettmp) {
				fprintf(stderr,"unexpected char '%c' in time period '%s'\n",(char)rettmp,str);
			} else {
				fprintf(stderr,"unexpected end in time period '%s'\n",str);
			}
			return -1;
		case TPARSE_VALUE_TOO_BIG:
			if (rettmp) {
				fprintf(stderr,"value too big in '%c' section in time period '%s'\n",(char)rettmp,str);
			} else {
				fprintf(stderr,"value too big in time period '%s'\n",str);
			}
			return -1;
	}
	return -1;
}

int tparse_hours(const char *str,uint16_t *ret) {
	uint32_t rettmp;
	switch (parse_hperiod(str,&rettmp)) {
		case TPARSE_OK:
			if (rettmp>UINT16_MAX) {
				fprintf(stderr,"value too big in time period '%s'\n",str);
				return -1;
			}
			*ret = rettmp;
			return 0;
		case TPARSE_UNEXPECTED_CHAR:
			if (rettmp) {
				fprintf(stderr,"unexpected char '%c' in time period '%s'\n",(char)rettmp,str);
			} else {
				fprintf(stderr,"unexpected end in time period '%s'\n",str);
			}
			return -1;
		case TPARSE_VALUE_TOO_BIG:
			if (rettmp) {
				fprintf(stderr,"value too big in '%c' section in time period '%s'\n",(char)rettmp,str);
			} else {
				fprintf(stderr,"value too big in time period '%s'\n",str);
			}
			return -1;
	}
	return -1;
}

void tcomm_print_help(const char *strings[]) {
	const char *cstr;
	char c;
	int enlen,ndlen,rdlen,mplen,qdlen;

	enlen = strlen(_EXENAME_);
	ndlen = strlen(_NUMBERDESC_);
	rdlen = strlen(_RECURSIVEDESC_);
	mplen = strlen(_MOUNTPOINTDESC_);
	qdlen = strlen(_QMARKDESC_);

	while ((cstr=*strings)!=NULL) {
		strings++;
		while ((c=*cstr)!='\0') {
			if (c=='_' && strncmp(cstr,_EXENAME_,enlen)==0) {
				if (subcommand!=NULL) {
					fprintf(stderr,"%s %s",appname,subcommand);
				} else {
					fprintf(stderr,"%s",appname);
				}
				cstr+=enlen;
			} else if (c=='_' && strncmp(cstr,_NUMBERDESC_,ndlen)==0) {
				fprintf(stderr," -n - show numbers in plain format\n");
				fprintf(stderr," -h - \"human-readable\" numbers using base 2 prefixes (IEC 60027)\n");
				fprintf(stderr," -H - \"human-readable\" numbers using base 10 prefixes (SI)\n");
				fprintf(stderr," -k - show plain numbers in kibis (binary kilo - 1024)\n");
				fprintf(stderr," -m - show plain numbers in mebis (binary mega - 1024^2)\n");
				fprintf(stderr," -g - show plain numbers in gibis (binary giga - 1024^3)");
				cstr+=ndlen;
			} else if (c=='_' && strncmp(cstr,_RECURSIVEDESC_,rdlen)==0) {
				fprintf(stderr," -r - do it recursively");
				cstr+=rdlen;
			} else if (c=='_' && strncmp(cstr,_MOUNTPOINTDESC_,mplen)==0) {
				fprintf(stderr," -M - specify mount point where mfs is mounted (cwd will be used when not specified)");
				cstr+=mplen;
			} else if (c=='_' && strncmp(cstr,_QMARKDESC_,qdlen)==0) {
				fprintf(stderr," -? - print this help message");
				cstr+=qdlen;
			} else {
				fputc(c,stderr);
				cstr++;
			}
		}
		putchar('\n');
	}
}

static void tcomm_print_available_commands(const command *commands) {
	const command *cptr;
	fprintf(stderr,"available subcommands:\n");
	for (cptr = commands ; cptr->cmd!=NULL ; cptr++) {
		fprintf(stderr," %s\n",cptr->cmd);
	}
}

static char* tcomm_get_next_cmd(const char *ptr) {
	static char cmd[MAXCMD+1];
	static const char *cmdptr;
	char c;
	int cmdpos;
	if (ptr!=NULL) {
		cmdptr = ptr;
		return NULL;
	}
	if (!(*cmdptr)) {
		return NULL;
	}
	cmdpos = 0;
	while ((c=*cmdptr)!='\0') {
		if (c=='|' || c==',' || c==';') {
			cmd[cmdpos]='\0';
			if (cmdpos>0) {
				cmdptr++;
				return cmd;
			}
		} else if (cmdpos<MAXCMD && ((c>='a' && c<='z') || (c>='A' && c<='Z'))) {
			cmd[cmdpos++] = *cmdptr;
		}
		cmdptr++;
	}
	cmd[cmdpos]='\0';
	if (cmdpos>0) {
		return cmd;
	}
	return NULL;
}

void tcomm_env(void) {
	char *hrformat;
	hrformat = getenv("MFSHRFORMAT");
	if (hrformat) {
		if (hrformat[0]>='0' && hrformat[0]<='4') {
			humode = (hrformat[0]-'0');
		}
		if (hrformat[0]=='h') {
			if (hrformat[1]=='+') {
				humode = 3;
			} else {
				humode = 1;
			}
		}
		if (hrformat[0]=='H') {
			if (hrformat[1]=='+') {
				humode = 4;
			} else {
				humode = 2;
			} 
		}
		if (hrformat[0]=='n') {
			if (hrformat[1]=='1') {
				numbermode = 1;
			} else if (hrformat[1]=='2') {
				numbermode = 2;
			} else if (hrformat[1]=='3') {
				numbermode = 3;
			}
		}
	}
}

int tcomm_find_and_execute(int argc,char *argv[],const command *commands) {
	int argv0l,l;
	const char *argv0bin;
	char *cmd;
	const command *cptr;

	strerr_init();
	tcomm_env();

	if (argc<1) {
		fprintf(stderr,"binary name not defined\n");
		return 1;
	}
	argv0l = strlen(argv[0]);
	if (argv0l==0) {
		fprintf(stderr,"binary name is empty\n");
		return 1;
	}
	l = argv0l-1;
	while (l>=0 && argv[0][l]!='/') {
		l--;
	}
	l++;
	argv0bin = argv[0] + l;
//	argv0l -= l;

	// check binary first
	for (cptr = commands ; cptr->cmd!=NULL ; cptr++) {
		tcomm_get_next_cmd(cptr->cmd);
		while ((cmd = tcomm_get_next_cmd(NULL))!=NULL) {
			if (strstr(argv0bin,cmd)!=NULL) {
				appname = argv[0];
				subcommand = NULL;
				return cptr->handler(argc,argv);
			}
		}
	}

	if (argc<2) {
		fprintf(stderr,"can't find command using binary name and no subcommand specified\n");
		tcomm_print_available_commands(commands);
		return 1;
	}

	for (cptr = commands ; cptr->cmd!=NULL ; cptr++) {
		tcomm_get_next_cmd(cptr->cmd);
		while ((cmd = tcomm_get_next_cmd(NULL))!=NULL) {
			if (strcmp(cmd,argv[1])==0) {
				appname = argv[0];
				subcommand = argv[1];
				return cptr->handler(argc-1,argv+1);
			}
		}
	}

	fprintf(stderr,"unrecognized command name (%s)\n",argv[1]);
	tcomm_print_available_commands(commands);
	return 1;
}
