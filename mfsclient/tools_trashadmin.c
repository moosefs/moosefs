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

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <unistd.h>
#include <fnmatch.h>

#include <pwd.h>
#include <time.h>
#include <grp.h>

#include "config.h"

#include "strerr.h"
#include "mfsstrerr.h"
#include "datapack.h"

#include "tools_main.h"
#include "tools_trashadmin.h"

#include "globengine.h"

#define MATCH_FLAG          (1<<0)
#define CONFLICT_FLAG       (1<<1)

#define TR_LIST             0
#define TR_RECOVER          1
#define TR_DUPRES           2

#define DUPRES_PICK         0
#define DUPRES_EARLIEST     1
#define DUPRES_LATEST       2
#define DUPRES_ALL          3
#define DUPRES_NONE         4
#define DUPRES_DEFAULT      255

#define PATHMODE_DEFAULT    0
#define PATHMODE_DIRECT     1
#define PATHMODE_RESTORE    2

#define SESS_VMODE          0xFF
#define SESS_HASH           256

#define TIMEDATE_FORMAT "%Y-%m-%d %H:%M"

#define COLOR_RESET "\033(B\033[m"
#define COLOR_MATCH "\033[38;5;196m"
#define COLOR_DIRMATCH "\033[38;5;124m"
#define COLOR_DIRLS "\033[38;5;12m"
#define COLOR_FILELS "\033[38;5;15m"
#define COLOR_ARROW "\033[38;5;196m"

#define COLOR_DUPRESOPT "\033[38;5;26m"

typedef struct dirinfo_t {
	struct dirinfo_t *next;
	uint32_t namestart, namelen, filescount;
	uint64_t totalfleng;
	uint32_t ts;
} dirinfo_t;


typedef struct trashfile_s {
	uint32_t inode;

	uint32_t uid;
	uint32_t gid;
	uint32_t deltime;
	uint32_t nlink; // isn't number of hard links always 0?
	uint64_t fleng;
	uint16_t trashretention;

	uint16_t type_mode;
	uint8_t	fnmatch;
	dirinfo_t *dirinfo;
	char *destrelpath; // path suffix
	char path[1];
} trashfile_t;


typedef struct sessioninfo_s {
	struct sessioninfo_s *next;
	uint32_t sessionid;
	uint32_t ipaddr;
	uint32_t expire;
	uint32_t susfilecnt; // ??
	char info[1];
} sessioninfo_t;


typedef struct susfile_s {
//	struct susfile_s *next;
	uint32_t inode;
	uint32_t sessioncnt, *sessionids; // list maybe?
	char path[1];
} susfile_t;


typedef struct trashcv_s {
	// list
	uint32_t uid;
	uint8_t use_any_uid;
	uint8_t dupresolvingmode;
	uint8_t type_f, type_d;
	uint32_t mints, maxts;
	char *name;
	uint8_t print_long, query_long;
	uint8_t printonelevelonly; // hmm
	uint8_t expanddirs;

	// common
	char *starting_point,*starting_point_mfs,*mountpoint;
	dev_t mountpoint_dev;

	// recover/purge
	uint32_t *inodes;
	uint32_t inodescnt;
	char **dirpaths;
	uint32_t dirpathscnt;

	// recover
	uint32_t destinode;
	uint8_t pathmode;
	uint16_t cumask;
	int8_t copysgid;
	char *destpath, *destpath_mfs;

	uint8_t iomode;

	uint8_t dryrun; // todo: delete or fully implement
} trashcv_t;

static char* multi_path_join(int pcnt,const char **paths) {
	int *lentab;
	char *result;
	int rl,p;
	int i;

	lentab = malloc(sizeof(int)*pcnt);
	rl = pcnt;
	for (i=0 ; i<pcnt ; i++) {
		lentab[i] = strlen(paths[i]);
		rl+=lentab[i];
	}
	result = malloc(rl);
	p = 0;
	for (i=0 ; i<pcnt ; i++) {
		memcpy(result+p,paths[i],lentab[i]);
		p+=lentab[i];
		if (i+1<pcnt) {
			result[p++]='/';
		} else {
			result[p++]='\0';
		}
	}
	return result;
}

static char* path_join(int pcnt,...) {
	const char *ptr;
	const char **paths;
	int vpcnt;
	int i;
	char *result;
	va_list ap;

	paths = malloc(sizeof(char*)*pcnt);
	va_start(ap,pcnt);
	vpcnt = 0;
	for (i=0 ; i<pcnt ; i++) {
		ptr = va_arg(ap,const char*);
		if (ptr!=NULL && ptr[0]!='\0') {
			paths[vpcnt++]=ptr;
		}
	}
	result = multi_path_join(vpcnt,paths);
	va_end(ap);
	free(paths);
	return result;
}

static void trashcv_init(trashcv_t *tcv) {
	memset(tcv,0,sizeof(trashcv_t));
	tcv->dupresolvingmode = DUPRES_DEFAULT;
	tcv->type_f = 1;
	tcv->type_d = 1;
	tcv->maxts = ~0;
	tcv->copysgid = -1;
	tcv->uid = getuid(); // here we need caller uid not effective uid !!!
	if (tcv->uid == 0) {
		tcv->use_any_uid = 1;
	}
}

static void trasncv_cleanup(trashcv_t *tcv) {
	uint32_t i;
	if (tcv->starting_point!=NULL) {
		free(tcv->starting_point);
	}
	if (tcv->mountpoint!=NULL) {
		free(tcv->mountpoint);
	}
	if (tcv->starting_point_mfs!=NULL) {
		free(tcv->starting_point_mfs);
	}
	if (tcv->name!=NULL) {
		free(tcv->name);
	}
	if (tcv->destpath!=NULL) {
		free(tcv->destpath);
	}
	if (tcv->destpath_mfs!=NULL) {
		free(tcv->destpath_mfs);
	}
	if (tcv->dirpaths!=NULL) {
		for (i=0 ; i<tcv->dirpathscnt ; i++) {
			if (tcv->dirpaths[i]!=NULL) {
				free(tcv->dirpaths[i]);
			}
		}
		free(tcv->dirpaths);
	}
}

static void list_files(trashcv_t *tcv, trashfile_t **filestab, uint32_t filescount, uint8_t mode);

static int trashfile_cmp(const void *a, const void *b) {
	int res = strcmp((*(trashfile_t**)a)->path, (*(trashfile_t**)b)->path);
	if (res == 0) {
		return (*(trashfile_t**)a)->inode > (*(trashfile_t**)b)->inode;
	}
	return res;
}
/*
static int trashfile_dst_cmp(const void *a, const void *b) {
	int res = strcmp((*(trashfile_t**)a)->destrelpath, (*(trashfile_t**)b)->destrelpath);
	if (res == 0) {
		return (*(trashfile_t**)a)->inode > (*(trashfile_t**)b)->inode;
	}
	return res;
}
*/
int susfile_cmp(const void *a, const void *b) {
	int res = strcmp((*(susfile_t **)a)->path, (*(susfile_t **)b)->path);
	if (res == 0) {
		return (*(susfile_t**)a)->inode > (*(susfile_t**)b)->inode;
	}
	return res;
}

int uint32_t_cmp(const void *a, const void *b) {
	uint32_t _a = *(uint32_t*)a;
	uint32_t _b = *(uint32_t*)b;
	return (_a >= _b) - (_a <= _b);
}

int is_mfs_mountpoint(trashcv_t *tcv,char *path) {
	char *rpath;
	struct stat st;
	int l;

	if (stat(path,&st)<0) {
		return 0;
	}
	if (!S_ISDIR(st.st_mode)) {
		return 0;
	}
	l = strlen(path);
	if (l+13>PATH_MAX) {
		return 0;
	}
	rpath = malloc(l+13);
	memcpy(rpath,path,l);
	memcpy(rpath+l,"/.masterinfo",13);
	if (stat(rpath,&st)<0) {
		free(rpath);
		return 0;
	}
	free(rpath);
	if ((st.st_mode&S_IFMT)!=S_IFREG) {
		return 0;
	}
	if (!(st.st_ino==0x7FFFFFFF && st.st_nlink==1 && st.st_uid==0 && st.st_gid==0 && (st.st_size==10 || st.st_size==14))) {
		return 0;
	}
	tcv->mountpoint_dev = st.st_dev;
	return 1;
}

// devid is st_dev of longest existing path prefix
int myrealpath(const char *path,char *rpath,dev_t *devid) {
	char pathbuff[PATH_MAX];
	int p,pl,rpl;
	struct stat st;

	if (path==NULL) { // special case - treat it as '.'
		pathbuff[0]='.';
		pathbuff[1]='\0';
		pl = 1;
	} else {
		pl = strlen(path);
		if (path[0]=='/') {
			if (pl>=PATH_MAX) {
				return -1;
			}
			memcpy(pathbuff,path,pl+1);
		} else {
			if (pl+2>=PATH_MAX) {
				return -1;
			}
			pathbuff[0]='.';
			pathbuff[1]='/';
			memcpy(pathbuff+2,path,pl+1);
			pl += 2;
		}
	}
	p = pl;
	while (1) {
		if (stat(pathbuff,&st)>=0) { // object exists
			if (devid!=NULL) {
				*devid = st.st_dev;
			}
			if (realpath(pathbuff,rpath)!=NULL) {
				break;
			} else { // realpath should return path for any existing object, so this should never happened
				return -1;
			}
		}
		if (p<pl) {
			pathbuff[p]='/';
		}
		while (p>0 && pathbuff[p-1]!='/') {
			p--;
		}
		if (p<=1) {
			return -1;
		}
		if (pathbuff[p]=='.' && (pathbuff[p+1]=='/' || pathbuff[p+1]=='\0')) {
			return -1;
		}
		if (pathbuff[p]=='.' && pathbuff[p+1]=='.' && (pathbuff[p+2]=='/' || pathbuff[p+2]=='\0')) {
			return -1;
		}
		p--;
		pathbuff[p]='\0';
	}
	if (p<pl) {
		pathbuff[p]='/';
	}
	rpl = strlen(rpath);
	if (pl>p) {
		if (rpl + (pl-p) >= PATH_MAX) {
			return -1;
		}
		memcpy(rpath+rpl,pathbuff+p,1+pl-p);
		rpl += pl-p;
	}
	return rpl;
}

// if path is NULL then use cwd
// sets in tcv:
//	tcv->starting_point to realpath of given path (extended with non existent elements)
//	tcv->starting_poin_mfs to realpath of given path without mountpoint
//	tcv->mountpoint to realpath of mountpoint detected from given path
int resolve_paths(trashcv_t *tcv,char *path) {
	char rpath[PATH_MAX];
	int p,rpl;

	// find best realpath
	rpl = myrealpath(path,rpath,NULL);
	if (rpl<0) {
		return -1;
	}
	tcv->starting_point = malloc(rpl+1);
	memcpy(tcv->starting_point,rpath,rpl+1);
	// find mountpoint
	if (rpath[0]!='/') {
		return -1;
	}
	if (is_mfs_mountpoint(tcv,"/")) { // special case
		tcv->mountpoint = strdup("/");
		if (rpl>1) {
			tcv->starting_point_mfs = strdup(rpath+1);
		} else {
			tcv->starting_point_mfs = malloc(1);
			tcv->starting_point_mfs[0]='\0';
		}
		return 0;
	}
	p = 1;
	while (p<rpl) {
		while (rpath[p]!='/' && p<rpl) {
			p++;
		}
		if (rpath[p]=='/') {
			rpath[p]='\0';
		}
		if (is_mfs_mountpoint(tcv,rpath)) {
			tcv->mountpoint = strdup(rpath);
			if (p<rpl) {
				tcv->starting_point_mfs = strdup(rpath+p+1);
			} else {
				tcv->starting_point_mfs = malloc(1);
				tcv->starting_point_mfs[0]='\0';
			}
			return 0;
		}
		rpath[p]='/';
		p++;
	}
	return -1;
}

static int check_first_arg(cv_t *cv,trashcv_t *tcv) {
	if (cv->argc > 1 && cv->argv[1][0]!='-') {
		if (resolve_paths(tcv,cv->argv[1])<0) {
			if (tcv->starting_point==NULL) {
				fprintf(stderr,"wrong starting path: %s\n",cv->argv[1]);
			} else {
				fprintf(stderr,"given path (%s) is not an element of MFS subtree\n",cv->argv[1]);
			}
			return -1;
		}
		cv->argc--;
		cv->argv++;
	} else {
		if (resolve_paths(tcv,NULL)<0) {
			fprintf(stderr,"current working directory is not an element of MFS subtree\n");
			return -1;
		}
	}
//	printf("mountpoint: %s ; starting_point: %s ; starting_point_mfs: %s\n",tcv->mountpoint,tcv->starting_point,tcv->starting_point_mfs);
	return 0;
}

/*
char *mountpoint_realpath(char *path) {
	char rpath[PATH_MAX];
	struct stat st;
	int l;

	if (realpath(path,rpath)==NULL) {
		return NULL;
	}
	if (stat(rpath,&st)<0) {
		return NULL;
	}
	if ((st.st_mode&S_IFMT)!=S_IFDIR) {
		return NULL;
	}
	l = strlen(rpath);
	if (l+13>PATH_MAX) {
		return NULL;
	}
	memcpy(rpath+l,"/.masterinfo",13);
	if (stat(rpath,&st)<0) {
		return NULL;
	}
	if ((st.st_mode&S_IFMT)!=S_IFREG) {
		return NULL;
	}
	if (!(st.st_ino==0x7FFFFFFF && st.st_nlink==1 && st.st_uid==0 && st.st_gid==0 && (st.st_size==10 || st.st_size==14))) {
		return NULL;
	}
	rpath[l]='\0';
	return strdup(rpath);
}

char *realpatheasy(char *path) {
	char *res, *ne, *la, *tmp;
	int reslen = 0, brk=0;

	res = malloc(strlen(path) + 1);
	la = path;

	do {
		ne = strchr(la + 1, '/');
		if (ne == NULL) {
			ne = path + strlen(path);
			brk = 1;
		} else if (ne == path + strlen(path) - 1) {
			brk = 1;
		}
		if ((*(ne-1) == '.' && *(ne-2) == '/') || *(ne-1) == '/') {
			la = ne;
		} else if (*(ne-1) == '.' && *(ne-2) == '.' && *(ne-3) == '/') {
			if (reslen > 0) {
				do {
					reslen--;
				} while (reslen > 0 && *(res + reslen) != '/');
			}
		} else {
			memcpy(res + reslen, la, ne - la);
			reslen += ne - la;
		}
		la = ne;
	} while (!brk);
	if (reslen == 0) {
		res[0] = '/';
		reslen = 1;
	}
	res[reslen] = '\0';
	tmp = realloc(res, reslen + 1);
	res = tmp;
	return res;
}
*/

static int8_t readcopysgid(trashcv_t *tcv) {
	FILE *f;
	char *paramsfn, *linebuff=NULL;
	size_t linebufflen=0;
	int32_t linelen;
#ifdef __linux__
	int8_t res=1;
#else
	int8_t res=0;
#endif

	if (tcv!=NULL && tcv->mountpoint!=NULL) {
		paramsfn = path_join(2,tcv->mountpoint,".params");
		f = fopen(paramsfn, "r");
		free(paramsfn);
		if (f == NULL) {
			return res;
		}
		while ((linelen = getline(&linebuff, &linebufflen, f)) > 0) {
			if (strncmp(linebuff, "mfsmkdircopysgid", 16) == 0) {
				res = (linebuff[18] == '1');
				break;
			}
		}
		free(linebuff);
		fclose(f);
	}
	return res;
}


static uint32_t parse_absolute_ts(char *cp, uint8_t isendtime) {
	static const int monthlen[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	time_t result = 0, currts;
	struct tm timeinfo, *currtime;
	char lastsep = '\0';
	int32_t *timetab, datetab[] = { -1, -1, -1, -1, -1 };
	int32_t minvals[] = { 0, 0, 1, 0, 0 };
	int8_t timestatus = 0, datestatus = 0;
	int32_t currnum = -1;
	timetab = datetab + 3;

	time(&currts);
	currtime = localtime(&currts);
	memset(&timeinfo, 0, sizeof(struct tm));
	timeinfo.tm_isdst = -1; //currtime->tm_isdst;
	while (1) {
		if (*cp >= '0' && *cp <= '9') {
			if (currnum == -1) {
				currnum = 0;
			}
			currnum = currnum * 10 + ((*cp) - '0');
		} else {
			if (*cp == '/') {
				if (datestatus == 2) {
					fprintf(stderr, "error: can't process time format\n");
					exit(1);
				}
				if (lastsep == '/' || lastsep == '\0') {
					datetab[datestatus++] = currnum;
				} else {
					fprintf(stderr, "error: can't process time format\n");
					exit(1);
				}
			} else if (*cp == '-') {
				if (lastsep == '/' || lastsep == '\0') {
					datetab[datestatus++] = currnum;
				} else {
					fprintf(stderr, "error: can't process time format\n");
					exit(1);
				}
			} else if (*cp == ':') {
				if (lastsep == '-' || lastsep == '\0') {
					timetab[timestatus++] = currnum;
				} else {
					fprintf(stderr, "error: can't process time format\n");
					exit(1);
				}
			} else if (*cp == '\0') {
				if (lastsep == ':' || lastsep == '-') {
					timetab[timestatus++] = currnum;
				} else if (lastsep == '/') {
					datetab[datestatus++] = currnum;
				} else if (lastsep == '\0') {
					// should not happen
					fprintf(stderr, "error: only number given while parsing absolute time\n");
					exit(1);
				}
				break;
			} else {
				fprintf(stderr, "error: can't process time format\n");
				exit(1);
			}
			lastsep = *cp;
			currnum = -1;
		}
		cp++;
	}
	if (datestatus == 1) {
		datetab[2] = datetab[0];
		datetab[0] = -1;
	} else if (datestatus == 2) {
		datetab[2] = datetab[1];
		datetab[1] = datetab[0];
		datetab[0] = -1;
	}
	if (datetab[0] != -1) {
		if (datetab[0] < 100) {
			datetab[0] = 2000 + datetab[0];
		}
	}
	if (datetab[1] != -1) {
		if (datetab[1] == 0) {
			fprintf(stderr, "error: given month out of range\n");
			exit(1);
		}
		datetab[1]--;
	}

	int8_t status = 0, tohop = -1;
	for (int i = 0; i < 5; i++) {
		if (datetab[i] != -1) {
			if (status == 0) {
				status = 1;
			} else if (status == 2) {
				datetab[i] = minvals[i];
			}
		} else {
			if (status == 1) {
				status = 2;
				tohop = i - 1;
			}
			if (status > 0) {
				datetab[i] = minvals[i];
			}
		}
	}
	if (tohop == -1) {
		tohop = 4;
	}
	if (!isendtime) {
		tohop = -1;
	}
	//    for (int i=0; i<5; i++) {
	//        printf("%d ", datetab[i]);
	//    }
	//	printf("\n");

	if (datetab[0] != -1) {
		if (datetab[0] < 1970) {
			fprintf(stderr, "error: given year out of range\n");
			exit(1);
		}
	}
	if (datetab[1] != -1) {
		if (datetab[1] < 0 || datetab[1] >= 12) { // month is in [0; 11] range
			fprintf(stderr, "error: given month out of range\n");
			exit(1);
		}
	}
	if (datetab[2] != -1) {
		uint32_t leap = 0, year = 0, month = 0;
		year = (datetab[0] == -1) ? currtime->tm_year + 1900: datetab[0];
		month = (datetab[1] == -1) ? currtime->tm_mon : datetab[1];
		leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
		if (leap && month == 1) {
			if (datetab[2] < 1 || datetab[2] > 29) {
				fprintf(stderr, "error: given day of the month out of range\n");
				exit(1);
			}
		} else {
			if (datetab[2] < 1 || datetab[2] > monthlen[month]) {
				fprintf(stderr, "error: given day of the month out of range\n");
				exit(1);
			}
		}
	}
	if (datetab[3] != -1) {
		if (datetab[3] < 0 || datetab[3] >= 24) {
			fprintf(stderr, "error: given hour out of range\n");
			exit(1);
		}
	}
	if (datetab[4] != -1) {
		if (datetab[4] < 0 || datetab[4] >= 60) {
			fprintf(stderr, "error: given minute out of range\n");
			exit(1);
		}
	}

	timeinfo.tm_year = ((datetab[0] == -1) ? currtime->tm_year : datetab[0] - 1900) + (tohop == 0);
	timeinfo.tm_mon = ((datetab[1] == -1) ? currtime->tm_mon : datetab[1]) + (tohop == 1);
	timeinfo.tm_mday = ((datetab[2] == -1) ? currtime->tm_mday : datetab[2]) + (tohop == 2);
	timeinfo.tm_hour = ((datetab[3] == -1) ? currtime->tm_hour : datetab[3]) + (tohop == 3);
	timeinfo.tm_min = ((datetab[4] == -1) ? currtime->tm_min : datetab[4]) + (tohop == 4);
	if (tohop != -1) {
		timeinfo.tm_sec--;
	}
//	printf("time and date: %s", asctime(&timeinfo));
	result = mktime(&timeinfo);
//	printf("time and date: %s", asctime(&timeinfo));

	return result;
}


static uint32_t parse_relative_ts(char *s) {
	char *c, *tmp = s;
	uint32_t used[4] = {0, 0, 0, 0};
	uint32_t total = 0, val;
	while (1) {
		if (!isdigit(tmp[0])) {
			fprintf(stderr, "error: can't process relative time '%s': expected number, got '%c'\n", s, tmp[0]);
			exit(1);
		}
		val = strtol(tmp, &c, 10);
		if (*c == '\0') {
			fprintf(stderr, "error: cant'process relative time '%s': no unit found\n", s);
			exit(1);
		} else if (tolower(*c) == 'm') {
			if (used[0] == 1) {
				fprintf(stderr, "warning: range filter unit 'm' was used multiple times, make sure it's correct\n");
			}
			used[0]++;
			total += val * 60;
		} else if (tolower(*c) == 'h') {
			if (used[1] == 1) {
				fprintf(stderr, "warning: range filter unit 'h' was used multiple times, make sure it's correct\n");
			}
			used[1]++;
			total += val * 60 * 60;
		} else if (tolower(*c) == 'd') {
			if (used[2] == 1) {
				fprintf(stderr, "warning: range filter unit 'd' was used multiple times, make sure it's correct\n");
			}
			used[2]++;
			total += val * 60 * 60 * 24;
		} else if (tolower(*c) == 'w') {
			if (used[3] == 1) {
				fprintf(stderr, "warning: range filter unit 'w' was used multiple times, make sure it's correct\n");
			}
			used[3]++;
			total += val * 60 * 60 * 24 * 7;
		} else {
			fprintf(stderr, "error: cant'process relative time '%s': expected unit, got: '%c'\n", s, *c);
			exit(1);
		}
		tmp = c + 1;
		if (*tmp == '\0') {
			return total;
		}
	}
}

static uint32_t parse_ts(char *str, uint8_t isendtime) {
	uint32_t res = 0;
	time_t now;
	char *cp = str;
	uint8_t relative = 0, absolute = 0;

	while (*cp != '\0') {
		if (*cp == 'm' || *cp == 'h' || *cp == 'w' || *cp == 'd') {
			relative = 1;
		} else if (*cp == ':' || *cp == '/' || *cp == '-') {
			absolute = 1;
		}
		cp++;
	}
	if (relative && absolute) {
		fprintf(stderr, "error: wrong time format!\n");
		exit(1); // usagesimple?
	}

	cp = str;
	if (relative) {
		res = parse_relative_ts(cp);
		if (time(&now) == -1) {
			fprintf(stderr, "can't process relative timestamp (time: current calendar time is not available)\n");
			return 0;
		}
		res = now - res;
	} else if (absolute) {
		res = parse_absolute_ts(cp, isendtime);
	} else {
		res = strtoul(cp, NULL, 0);
	}
	return res;
}


static int32_t path_lcp(const char *a, const char *b) {
	int i, last_slash = -1;
	for (i=0; a[i] && b[i] && a[i] == b[i]; i++) {
		if (a[i] == '/') {
			last_slash = i;
		}
	}
	return last_slash;
}


static void merge_trash_lists(trashfile_t ***xtab, uint32_t *xlen, trashfile_t **ytab, uint32_t ylen) {
	uint32_t currx = 0, curry = 0, nextindex = 0;
	trashfile_t **restab;
	trashfile_t *xel, *yel;
	int cmpres;

	if (ylen == 0) {
		return;
	}
	if (*xlen == 0) {
		*xtab = ytab;
		*xlen = ylen;
		return;
	}

	if ((restab = (trashfile_t**)malloc(sizeof(trashfile_t*) * (*xlen + ylen))) == NULL) {
		fprintf(stderr, "error: can't malloc\n");
		exit(1);
	}
	while (currx < *xlen && curry < ylen) {
		xel = (*xtab)[currx];
		yel = ytab[curry];
		cmpres = strcmp(xel->path, yel->path);
		if (cmpres == 0) {
			cmpres = (xel->inode <= yel->inode) - (xel->inode >= yel->inode);
		}
		if (cmpres == 0) {
			int xdeep = xel->destrelpath - xel->path;
			int ydeep = yel->destrelpath - yel->path;
			if (xdeep <= ydeep) {
				restab[nextindex++] = xel;
				free(yel);

			} else {
				restab[nextindex++] = yel;
				free(xel);
			}
			currx++;
			curry++;
		} else if (cmpres < 0) {
			restab[nextindex++] = xel;
			currx++;
		} else {
			restab[nextindex++] = yel;
			curry++;
		}
	}
	while (currx < *xlen) {
		restab[nextindex++] = (*xtab)[currx++];
	}
	while (curry < ylen) {
		restab[nextindex++] = ytab[curry++];
	}
	free(ytab);
	free(*xtab);
	*xtab = (trashfile_t**)realloc(restab, sizeof(trashfile_t*) * nextindex);
	if (*xtab == NULL) {
		fprintf(stderr, "error: realloc\n");
		exit(1);
	}
	*xlen = nextindex;
}


// todo: maybe needed?
/*
void reduce_slashes(char *pat) {
	uint32_t patslashes, patlen, i, pos=0;

	patlen = strlen(pat);
	for (i=0; i<patlen; i++) {
		if (!(i < patlen - 1 && pat[i] == '/' && pat[i+1] == '/')) {
			pat[pos++] = pat[i];
		}
	}
	pat[pos] = '\0';
}
*/

static int getallgroups(uint32_t **groups,uint32_t *grpcnt) {
	gid_t gid,*gtmp;
	uint32_t *groupsaux;
	int i,gcnt;

	while (1) {
		gcnt = getgroups(0,NULL);
		if (gcnt<0) {
			fprintf(stderr,"can't get aux groups\n");
			return -1;
		}
		gtmp = malloc((gcnt+10)*sizeof(gid_t));
		gcnt = getgroups((gcnt+10),gtmp);
		if (gcnt>=0) {
			break;
		}
		free(gtmp);
	}
	gid = getegid();

	for (i=0 ; i<gcnt ; i++) {
		if (gtmp[i]==gid) {
			gtmp[i] = gtmp[--gcnt];
		}
	}

	groupsaux = malloc((1+gcnt)*sizeof(uint32_t));
	groupsaux[0] = gid;
	for (i=0 ; i<gcnt ; i++) {
		groupsaux[i+1] = gtmp[i];
	}
	free(gtmp);
	*groups = groupsaux;
	*grpcnt = 1+gcnt;
	return 0;
}

/*
// path[skip] == '/' or skip == strlen(path)
static char *path_from_pos(const char *path, uint32_t skip) {
	char *res;
	uint32_t len;
	if (strlen(path) == skip) {
		res = (char*) malloc(sizeof(char));
		res[0] = '\0';
	} else {
		len = strlen(path) - skip - 1;
		res = malloc(sizeof(char) * (len + 1));
		memcpy(res, path + skip + 1, sizeof(char) * len);
		res[len] = '\0';
	}
	return res;
}
*/

static uint32_t resolve_duplicates_one_name(trashcv_t *tcv, trashfile_t **filestab, uint32_t filescount, uint32_t **picked_indices) {
	char *linebuff, *p;
	size_t linebufflen;
	uint32_t i;
	uint32_t resmode;
	uint8_t rev = 0;
	uint32_t num;
	uint8_t nonempty;
	uint8_t *picked = NULL;
	uint8_t readagain;
	uint32_t pickedcount=0, nextindex=0;
	uint64_t besttime, currtime;
	uint32_t bestindex;

	if (tcv->dupresolvingmode == DUPRES_PICK) {
		picked = (uint8_t*)malloc(filescount * sizeof(uint8_t));
		memset(picked, 0, filescount * sizeof(uint8_t));

		printf("File " COLOR_MATCH "%s" COLOR_RESET " appears %u times\n", filestab[0]->path, filescount);
		printf("Press: " COLOR_DUPRESOPT "L" COLOR_RESET "/" COLOR_DUPRESOPT "l" COLOR_RESET " - latest for all/this; "
				COLOR_DUPRESOPT "E" COLOR_RESET "/" COLOR_DUPRESOPT "e" COLOR_RESET " - earliest for all/this; "
				COLOR_DUPRESOPT "A" COLOR_RESET "/" COLOR_DUPRESOPT "a" COLOR_RESET " - all for all/this; "
				COLOR_DUPRESOPT "N" COLOR_RESET "/" COLOR_DUPRESOPT "n" COLOR_RESET " - none for all/this; "
				"or list of " COLOR_DUPRESOPT "IDX" COLOR_RESET " (start with '-' for negation)\n");
		list_files(tcv, filestab, filescount, TR_DUPRES);
		do {
			rev = 0;
			readagain = 0;
			linebuff = NULL;
			linebufflen = 0;
			if (getline(&linebuff, &linebufflen, stdin) < 0) {
				fprintf(stderr, "error: getline error\n");
				exit(1);
			}
			p = linebuff;
			while ((*p) == ' ') {
				p++;
			}
			switch (*p) {
				case 'E':
					tcv->dupresolvingmode = DUPRES_EARLIEST;
					nobreak;
				case 'e':
					resmode = DUPRES_EARLIEST;
					break;
				case 'L':
					tcv->dupresolvingmode = DUPRES_LATEST;
					nobreak;
				case 'l':
					resmode = DUPRES_LATEST;
					break;
				case 'A':
					tcv->dupresolvingmode = DUPRES_ALL;
					nobreak;
				case 'a':
					resmode = DUPRES_ALL;
					break;
				case 'N':
					tcv->dupresolvingmode = DUPRES_NONE;
					nobreak;
				case 'n':
					resmode = DUPRES_NONE;
					break;
				case '-':
					rev = 1;
					p++;
					nobreak;
				default:
					resmode = DUPRES_PICK;
					nonempty = 0;
					num = 0;
					while ((*p) != '\0') {
						if ((*p) == ' ' || (*p) == '\n') {
							if (nonempty == 1) {
								if (num >= filescount) {
									fprintf(stderr, "Index to large (read %u, number of files %u). Try again.\n", num, filescount);
									readagain = 1;
									break;
								}
								picked[num] = 1;
								nonempty = 0;
								num = 0;
							}
						} else if ((*p) < '0' || (*p) > '9') {
							fprintf(stderr, "Wrong character (%c). Try again.\n", *p);
							readagain = 1;
							break;
						} else {
							num *= 10;
							num += (*p) - '0';
							nonempty = 1;
						}
						p++;
					}
					break;
			}
			free(linebuff);
		} while (readagain);
	} else {
		resmode = tcv->dupresolvingmode;
	}

	if (resmode == DUPRES_PICK) {
		for (i=0; i<filescount; i++) {
			if (picked[i]) {
				pickedcount++;
			}
		}
		if (rev) {
			pickedcount = filescount - pickedcount;
		}
		*picked_indices = (uint32_t*)malloc(sizeof(uint32_t) * pickedcount);
		for (i=0; i<filescount; i++) {
			if (picked[i] ^ rev) {
				(*picked_indices)[nextindex++] = i;
			}
		}
		free(picked);
		return pickedcount;
	} else if (resmode == DUPRES_EARLIEST || resmode == DUPRES_LATEST) {
		besttime = filestab[0]->deltime;
		bestindex = 0;

		for (i=1; i<filescount; i++) {
			currtime = filestab[i]->deltime;
			if (resmode == DUPRES_EARLIEST && currtime < besttime) {
				besttime = currtime;
				bestindex = i;
			} else if (resmode == DUPRES_LATEST && currtime > besttime) {
				besttime = currtime;
				bestindex = i;
			}
		}
		*picked_indices = (uint32_t*)malloc(sizeof(uint32_t));
		(*picked_indices)[0] = bestindex;
		return 1;
	} else if (resmode == DUPRES_ALL) {
		*picked_indices = (uint32_t*)malloc(sizeof(uint32_t) * filescount);
		for (i=0; i<filescount; i++) {
			(*picked_indices)[i] = i;
		}
		return filescount;
	} else if (resmode == DUPRES_NONE) {
		*picked_indices = NULL;
		return 0;
	}
	fprintf(stderr, "internal error: no duplicate resolve mode\n");
	exit(1);
}

static uint32_t resolve_duplicates(trashcv_t *tcv, trashfile_t **filestab, uint32_t filescount) {
	uint32_t i, j, firstfn = 0;
	uint32_t pickedcount;
	uint32_t *picked_indices = NULL;
	uint32_t newlen = 0;

	if (tcv->dupresolvingmode == DUPRES_ALL) {
		return filescount;
	}

	for (i=0; i<filescount; i++) {
		if (i == filescount-1 || strcmp(filestab[i]->path, filestab[i+1]->path) != 0) {
			if (i > firstfn) {
				pickedcount = resolve_duplicates_one_name(tcv, filestab + firstfn, i - firstfn + 1, &picked_indices);
				for (j=0; j<pickedcount; j++) {
					filestab[newlen++] = filestab[firstfn + picked_indices[j]];
				}
				free(picked_indices);
			} else {
				filestab[newlen++] = filestab[i];
			}
			firstfn = i+1;
		}
	}
	return newlen;
}

sessioninfo_t *sessinfohash[SESS_HASH];

static int getsessionlist(void) {
	int32_t leng;
	uint32_t sessid, ip, expire, ileng;
	sessioninfo_t *sessinfo;

	ps_comminit();
	ps_put8(SESS_VMODE);
	if ((leng = ps_send_and_receive(CLTOMA_SESSION_LIST, MATOCL_SESSION_LIST, NULL)) < 0) {
		fprintf(stderr, "error: can't get session list");
		goto error;
	}

	while ((leng = ps_bytesleft()) > 0) {
		sessid = ps_get32();
		ip = ps_get32();
		expire = ps_get32();
		ileng = ps_get32();

		if ((sessinfo = (sessioninfo_t*) malloc(sizeof(sessioninfo_t) + ileng)) == NULL) {
			fprintf(stderr, "error: malloc");
			goto error;
		}
		sessinfo->sessionid = sessid;
		sessinfo->ipaddr = ip;
		sessinfo->expire = expire;
		ps_getbytes((uint8_t*)(sessinfo->info),ileng);
		sessinfo->info[ileng] = '\0';

		sessinfo->next = sessinfohash[sessid % SESS_HASH];
		sessinfohash[sessid % SESS_HASH] = sessinfo;
	}
	return 0;
error:
	reset_master();
	return -1;
}


static sessioninfo_t *getsessioninfo(uint32_t sessionid) {
	sessioninfo_t *curr=sessinfohash[sessionid % SESS_HASH];
	while (curr && curr->sessionid != sessionid) {
		curr = curr->next;
	}
	return curr;
}

static int one_sustained_query(trashcv_t *tcv, const char *pattern, susfile_t ***suselementstab, uint32_t *suselementscount) {
	uint16_t part;
	int32_t leng;
	uint32_t listedfilescnt = 0, totalfilescnt = 0, matchingfilescnt = 0;
	uint32_t i;
	uint32_t inode, sessioncnt, *sessionids, strleng;

	uint32_t tablen = 100, tabused = 0;
	susfile_t **sustab = (susfile_t**) malloc(sizeof(susfile_t*) * tablen), **tmpsustab;
	susfile_t *currfile;

	for (part=0; part<256; part++) {
		ps_comminit();
		ps_put8((uint8_t) part);
		if (tcv->use_any_uid) {
			ps_put32(LIST_UID_ANY);
		} else {
			ps_put32(tcv->uid);
		}
		ps_putstr(pattern);

		if ((leng = ps_send_and_receive(CLTOMA_SUSTAINED_LIST, MATOCL_SUSTAINED_LIST, NULL)) < 0) {
			goto error;
		}
		if (leng==1) {
			fprintf(stderr, "error: %s\n", mfsstrerr(ps_get8()));
			goto error;
		}

		totalfilescnt += ps_get32();
		matchingfilescnt += (listedfilescnt = ps_get32());

		if (listedfilescnt == 0) continue;

		while (tablen < tabused + listedfilescnt) {
			tmpsustab = realloc(sustab, sizeof(susfile_t*) * 2 * tablen);
			if (tmpsustab == NULL) {
				fprintf(stderr, "error: realloc\n");
				goto error;
			}
			sustab = tmpsustab;
			tablen *= 2;
		}

		while (ps_bytesleft() > 0) {
			inode = ps_get32();
			sessioncnt = ps_get32();
		//	fprintf(stderr, " >>>>>>>>>>>>>>  %u %u\n", inode, sessioncnt);
			if ((sessionids = (uint32_t*) malloc(sizeof(uint32_t) * sessioncnt)) == NULL) {
				fprintf(stderr, "error: malloc");
				goto error;
			}
			for (i=0; i<sessioncnt; i++) {
				sessionids[i] = ps_get32();
			}
			strleng = ps_get32();

			if ((currfile = malloc(sizeof(susfile_t) + strleng)) == NULL) {
				fprintf(stderr, "error: malloc");
				goto error;
			}
			currfile->inode = inode;
			currfile->sessioncnt = sessioncnt;
			currfile->sessionids = sessionids;
			ps_getbytes((uint8_t*)(currfile->path),strleng);
			currfile->path[strleng] = '\0';
			sustab[tabused++] = currfile;
		}
	}
	(void)totalfilescnt;
	(void)matchingfilescnt;
	if (tabused > 0) {
		tmpsustab = realloc(sustab, sizeof(susfile_t*) * tabused);
		if (tmpsustab == NULL) {
			fprintf(stderr, "error: realloc");
			goto error;
		}
	} else {
		free(sustab);
		tmpsustab = NULL;
		tabused = 0;
	}
	sustab = tmpsustab;
	if (sustab) {
		qsort(sustab, tabused, sizeof(susfile_t*), susfile_cmp);
	}
	*suselementstab = sustab;
	*suselementscount = tabused;
	return 0;

error:
	for (i=0; i<tabused; i++) {
		free(sustab[i]);
	}
	free(sustab);

	reset_master();
	return -1;
}


static int one_list_query(trashcv_t *tcv, const char *pattern, trashfile_t ***trashelementstab, uint32_t *trashelementscount) {
	uint32_t inode;
	int32_t leng;
	uint32_t totalfilescnt = 0, usrfilescnt = 0;
	uint32_t matchingfilescnt = 0, listedfilescnt;
	uint16_t part, trashretention;
	uint32_t strleng;

	trashfile_t	*currfile;
	uint32_t uid, gid;
	uint32_t deltime, tmptime;
	uint32_t nlink;
	uint64_t fleng;
	uint16_t type_mode;

	uint32_t tablen = 100, tabused = 0;
	trashfile_t **filestab = (trashfile_t**) malloc(sizeof(trashfile_t*) * tablen), **tmpfilestab;

	uint32_t i;

	for (part=0; part<256; part++) {
		ps_comminit();
		ps_put8((uint8_t)part);
		ps_put8(tcv->query_long);
		if (tcv->use_any_uid) {
			ps_put32(LIST_UID_ANY);
		} else {
			ps_put32(tcv->uid);
		}
		ps_put32(tcv->mints);
		ps_put32(tcv->maxts);
		ps_putstr(pattern);

		if ((leng = ps_send_and_receive(CLTOMA_TRASH_LIST, MATOCL_TRASH_LIST, NULL)) < 0) {
			goto error;
		}
		if (leng==1) {
			fprintf(stderr, "error: %s\n", mfsstrerr(ps_get8()));
			goto error;
		}
		totalfilescnt += ps_get32();
		usrfilescnt += ps_get32();
		matchingfilescnt += (listedfilescnt = ps_get32()); // todo: use it

		if (listedfilescnt == 0) continue;

		while (tablen < tabused + listedfilescnt) {
			tmpfilestab = realloc(filestab, sizeof(trashfile_t*) * 2 * tablen);
			if (tmpfilestab == NULL) {
				fprintf(stderr, "error: no memory!\n");
				goto error;
			}
			filestab = tmpfilestab;
			tablen *= 2;
		}

		while (ps_bytesleft() > 0) {
			inode = ps_get32();
			if (tcv->query_long) {
				ps_dummyget(1);
				type_mode = ps_get16();
				uid = ps_get32();
				gid = ps_get32();
				deltime = ps_get32();
				tmptime = ps_get32();
				if (deltime < tmptime) {
					deltime = tmptime;
				}
				tmptime = ps_get32();
				if (deltime < tmptime) {
					deltime = tmptime;
				}
				nlink = ps_get32();
				fleng = ps_get64();
				ps_dummyget(1);
				trashretention = ps_get16();
			} else {
				type_mode = uid = gid = deltime = nlink = fleng = trashretention = 0L;
			}
			strleng = ps_get32();

			if ((currfile = malloc(sizeof(trashfile_t)+strleng)) == NULL) {
				fprintf(stderr, "error: no memory!");
				goto error;
			}
			currfile->fnmatch = 0;
			currfile->inode = inode;
			currfile->type_mode = type_mode;
			currfile->uid = uid;
			currfile->gid = gid;
			currfile->deltime = deltime;
			currfile->nlink = nlink;
			currfile->fleng = fleng;
			currfile->trashretention = trashretention;
			currfile->dirinfo = NULL;
			ps_getbytes((uint8_t*)(currfile->path),strleng);
			currfile->path[strleng] = '\0';
			filestab[tabused++] = currfile;
		}
	}
	(void)totalfilescnt;
	(void)usrfilescnt;
	(void)matchingfilescnt; // todo: use it

	if (tabused > 0) {
		tmpfilestab = realloc(filestab, sizeof(trashfile_t*) * tabused);
		if (tmpfilestab == NULL) {
			fprintf(stderr, "error: no memory!\n");
			goto error;
		}
	} else {
		free(filestab);
		tmpfilestab = NULL;
		tabused = 0;
	}
	filestab = tmpfilestab;
	if (filestab) {
		qsort(filestab, tabused, sizeof(trashfile_t*), trashfile_cmp);
	}
	*trashelementstab = filestab;
	*trashelementscount = tabused;
	return 0;

error:
	for (i=0; i<tabused; i++) {
		free(filestab[i]);
	}
	free(filestab);

	reset_master();
	return -1;
}


static int purge_one_file(trashcv_t *tcv, uint32_t inode, uint32_t uid) {
	int32_t leng;
	uint8_t status;

	if (tcv->dryrun) {
		printf(">>>>>>>>>>>> purging inode %u uid %u\n", inode, uid);
		return 0;
	}
	ps_comminit();
	ps_put32(inode);
	ps_put32(uid);
	if ((leng = ps_send_and_receive(CLTOMA_TRASH_REMOVE, MATOCL_TRASH_REMOVE, NULL)) != 1) {
		goto error;
	}
	status = ps_get8();
	if (status) {
		fprintf(stderr, "error %s\n", mfsstrerr(status));
		goto error;
	}

	return 0;
error:
	reset_master();
	return -1;
}

static int recover_one_file(trashcv_t *tcv, uint32_t inode, uint32_t destlen, char *destpath, uint32_t uid, uint32_t *gids, uint32_t gcnt) {
	uint8_t status;
	int32_t leng;
	char *newpath;

	if (tcv->dryrun) {
		printf(">>>>>>>>>>>> restoring inode %u to %s/%s\n", inode, tcv->destpath, destpath);
		return 0;
	}
	ps_comminit();
	ps_put32(inode);
	ps_put32(tcv->destinode);
	ps_put32(destlen);
	ps_putbytes((uint8_t*)destpath, destlen);
	ps_put16(tcv->cumask);
	ps_put32(uid);
	ps_put32(gcnt);
	for (uint32_t i=0; i<gcnt; i++) {
		ps_put32(gids[i]);
	}
	ps_put8(tcv->copysgid);

	if ((leng = ps_send_and_receive(CLTOMA_TRASH_RECOVER, MATOCL_TRASH_RECOVER, NULL)) < 0) {
		goto error;
	}
	if (leng==1) {
		status = ps_get8();
		if (status != 0) {
			fprintf(stderr, "got error %s\n", mfsstrerr(status));
			goto error;
		}
	} else {
		leng = ps_get32();
		newpath = (char*) malloc(sizeof(char) * (leng + 1));
		ps_getbytes((uint8_t*)newpath, leng);
		newpath[leng] = '\0';
		fprintf(stderr, "due to name conflicts, file %s/%s is put in %s/%s instead\n", tcv->mountpoint, destpath, tcv->destpath, newpath);
	}

	return 0;

error:
	reset_master();
	return -1;
}


typedef struct dir_stack_el {
	uint32_t firstindex;
	uint32_t namestart, namelen;
	uint64_t flengsumbefore;
	uint32_t ts;
	struct dir_stack_el *next;
} dir_stack_el;

static void prep_dirinfo_and_filter_by_name(trashcv_t *tcv, trashfile_t ***trashelementstab, uint32_t *trashelementscount) {
	uint32_t i, nextindex;
	int32_t lcp;
	void *glob;
	char *c, *fi, *path;
	uint8_t *filtermask;
	trashfile_t **tmptab;
	uint32_t startfrom;
	dir_stack_el *stack=NULL, *tmp, *curr;
	dirinfo_t *dirinfo;
	uint64_t flengsum = 0;
	uint32_t deltime;

	if (strlen(tcv->starting_point_mfs) == 0) {
		startfrom = 0;
	} else {
		startfrom = strlen(tcv->starting_point_mfs) + 1;
	}
	filtermask=(uint8_t*) malloc(sizeof(uint8_t) * (*trashelementscount));
	memset(filtermask, 0, sizeof(uint8_t) * (*trashelementscount));
	glob = glob_new((uint8_t*) tcv->name);

	for (i=0; i<*trashelementscount; i++) {
		if (tcv->type_f) {
			path = strdup((*trashelementstab)[i]->path); // why dup??
			c = strrchr(path, '/');
			if (c == NULL) {
				c = path;
			} else {
				c = c + 1;
			}
			if (tcv->printonelevelonly == 0 || c - path == 0 || c - path == (int)strlen(tcv->starting_point_mfs) + 1) {
				if (glob_match(glob, (uint8_t*)c, strlen(c))) {
					filtermask[i] = 1;
					(*trashelementstab)[i]->fnmatch = 1;
				}
			}
			free(path);
		}
		if (tcv->type_d) {
			path = strdup((*trashelementstab)[i]->path);

			if (i > 0) {
				lcp = path_lcp((*trashelementstab)[i-1]->path, (*trashelementstab)[i]->path);
				curr = stack;
				while (curr != NULL && lcp < (int32_t) curr->namestart) {
					dirinfo = (dirinfo_t*) malloc(sizeof(dirinfo_t));
					dirinfo->filescount = i - curr->firstindex;
					dirinfo->namestart = curr->namestart;
					dirinfo->namelen = curr->namelen;
					dirinfo->totalfleng = flengsum - curr->flengsumbefore;
					dirinfo->ts = curr->ts;
					dirinfo->next = (*trashelementstab)[curr->firstindex]->dirinfo;
					(*trashelementstab)[curr->firstindex]->dirinfo = dirinfo;
					tmp = curr->next;
					if (tmp != NULL) {
						if (curr->ts > tmp->ts) {
							tmp->ts = curr->ts;
						}
					}
					free(curr);
					curr = tmp;
				}
				stack = curr;
				c = path + lcp + 1;
			} else {
				c = path + startfrom;
			}
			while ((fi = strchr(c, '/'))) {
				if (tcv->printonelevelonly == 1 && c > path + startfrom) {
					break;
				}
				*fi = '\0'; // not needed?
				if (glob_match(glob, (uint8_t*)c, fi - c)) {
					tmp = (dir_stack_el*) malloc(sizeof(dir_stack_el));
					tmp->firstindex = i;
					tmp->namestart = c - path;
					tmp->namelen = fi - c;
					tmp->flengsumbefore = flengsum;
					tmp->ts = 0;
					tmp->next = stack;
					stack = tmp;
				}
				c = fi + 1;
			}
			if (stack != NULL) {
				filtermask[i] = 1;
				deltime = (*trashelementstab)[i]->deltime;
				if (deltime > stack->ts) {
					stack->ts = deltime;
				}
			}
			free(path);
		}
		flengsum += (*trashelementstab)[i]->fleng;
	}
	curr = stack;
	while (curr != NULL) {
		dirinfo = (dirinfo_t*) malloc(sizeof(dirinfo_t));
		dirinfo->filescount = (*trashelementscount) - curr->firstindex;
		dirinfo->namestart = curr->namestart;
		dirinfo->namelen = curr->namelen;
		dirinfo->totalfleng = flengsum - curr->flengsumbefore;
		dirinfo->ts = curr->ts;
		dirinfo->next = (*trashelementstab)[curr->firstindex]->dirinfo;
		(*trashelementstab)[curr->firstindex]->dirinfo = dirinfo;
		tmp = curr->next;
		if (tmp != NULL) {
			if (curr->ts > tmp->ts) {
				tmp->ts = curr->ts;
			}
		}
		free(curr);
		curr = tmp;
	}

	nextindex=0;
	for (i=0; i<*trashelementscount; i++) {
		if (filtermask[i]) {
			(*trashelementstab)[nextindex++] = (*trashelementstab)[i];
		} else {
			free((*trashelementstab)[i]);
		}
	}

	tmptab = (trashfile_t**)realloc(*trashelementstab, sizeof(trashfile_t*) * nextindex);
	if (tmptab == NULL && nextindex > 0) {
		fprintf(stderr, "error: realloc\n");
		exit(1);
	} else {
		*trashelementstab = tmptab;
	}
	*trashelementscount = nextindex;
	free(filtermask);
}

static int get_elements_from_path(trashcv_t *tcv, char *path, trashfile_t ***trashelementstab, uint32_t *trashelementscount) {
	char *pattern;

	one_list_query(tcv, path, trashelementstab, trashelementscount);
	if (*trashelementscount==0) { // no such file - then try using it as a directory
		pattern = path_join(2,path,"*");
		one_list_query(tcv, pattern, trashelementstab, trashelementscount);
		free(pattern);
	}
	return *trashelementscount;
}

static int find_trash_elements(trashcv_t *tcv, trashfile_t ***trashelementstab, uint32_t *trashelementscount) {
	trashfile_t **filestab = NULL, **allfilestab = NULL;
	uint32_t filescount = 0, allfilescount = 0;
	char *pattern;

	if (tcv->name==NULL || (tcv->name[0]=='*' && tcv->name[1]=='\0')) { // default pattern
		pattern = path_join(2,tcv->starting_point_mfs,"*");
		one_list_query(tcv, pattern, &filestab, &filescount);
		free(pattern);
		allfilestab = filestab;
		allfilescount = filescount;
	} else {
		if (tcv->type_f) {
			pattern = path_join(2,tcv->starting_point_mfs,tcv->name);
			one_list_query(tcv, pattern, &filestab, &filescount);
			free(pattern);
			merge_trash_lists(&allfilestab, &allfilescount, filestab, filescount);
		}
		if (tcv->type_d) {
			pattern = path_join(3,tcv->starting_point_mfs,tcv->name,"*");
			one_list_query(tcv, pattern, &filestab, &filescount);
			free(pattern);
			merge_trash_lists(&allfilestab, &allfilescount, filestab, filescount);
		}
		if (tcv->name[0] != '*') {
			if (tcv->type_f) {
				pattern = path_join(3,tcv->starting_point_mfs,"*",tcv->name);
				one_list_query(tcv, pattern, &filestab, &filescount);
				free(pattern);
				merge_trash_lists(&allfilestab, &allfilescount, filestab, filescount);
			}
			if (tcv->type_d) {
				pattern = path_join(4,tcv->starting_point_mfs,"*",tcv->name,"*");
				one_list_query(tcv, pattern, &filestab, &filescount);
				free(pattern);
				merge_trash_lists(&allfilestab, &allfilescount, filestab, filescount);
			}
		}
	}
	prep_dirinfo_and_filter_by_name(tcv, &allfilestab, &allfilescount);
	*trashelementstab = allfilestab;
	*trashelementscount = allfilescount;
	return 0;
}


static const char *columnnames[] = {
		"IDX", "INODE", "USER", "GROUP", "SIZE", "DELETION TIME", "PATH"
};


static void list_header(trashcv_t *tcv, uint8_t indent[5]) {
	int i;
	(void)tcv;
	if (indent[0] > 0) {
		printf("%*s  ", indent[0], columnnames[0]);
	}
	for (i=0; i<indent[1]-5; i++) {
		printf(" ");
	}
	printf("%s  ", columnnames[1]);
	printf("%*s", -indent[2]-2, columnnames[2]);
	printf("%*s", -indent[3]-2, columnnames[3]);
	printf("%*s%*s", indent[4], columnnames[4], indent[4]-2, "");
	printf("%s", columnnames[5]);
	if (indent[0] == 0) {
		printf("     %s", columnnames[6]);
	}
	printf("\n");
}

static void file_long_info(trashcv_t *tcv, trashfile_t *file, uint8_t indent[5]) {
	char timebuff[100];
	time_t maxtime;
	struct passwd *pwd;
	struct group *grp;
	(void) tcv;
	pwd = getpwuid(file->uid);
	grp = getgrgid(file->gid);

	maxtime = (time_t)file->deltime;
	strftime(timebuff, 100,  TIMEDATE_FORMAT, localtime(&maxtime));
	printf("%*u  ", indent[1], file->inode);
	if (pwd == NULL) {
		printf("%*u  ", -indent[2], file->uid);
	} else {
		printf("%*s  ", -indent[2], pwd->pw_name);
	}
	if (grp == NULL) {
		printf("%*u  ", -indent[3], file->gid);
	} else {
		printf("%*s  ", -indent[3], grp->gr_name);
	}
	printf("%*" PRIu64 "  %s  ", indent[4], file->fleng, timebuff);
}

static void dir_long_info(trashcv_t *tcv, dirinfo_t *dirinfo, uint8_t indent[5]) {
	char timebuff[100];
	time_t deltime = dirinfo->ts;
	(void) tcv;
	strftime(timebuff, 100,  TIMEDATE_FORMAT, localtime(&deltime));
	printf("%*s  %*s  %*s  %*" PRIu64 "  %s  ", indent[1], "?", -indent[2], "?", -indent[3], "?", indent[4], dirinfo->totalfleng, timebuff);
}

static void list_one_file(trashcv_t *tcv, trashfile_t *file, uint8_t indent[5]) {
	char *c;
	if (tcv->print_long) {
		file_long_info(tcv, file, indent);
	}
	printf("%s/", tcv->mountpoint);
	if ((c = strrchr(file->path, '/')) == NULL) {
		if (tcv->printonelevelonly == 0) {
			printf(COLOR_MATCH "%s" COLOR_RESET "\n", file->path);
		} else {
			printf(COLOR_FILELS "%s" COLOR_RESET "\n", file->path);
		}
	} else {
		*c = '\0';
		if (tcv->printonelevelonly == 0) {
			printf("%s/" COLOR_MATCH "%s" COLOR_RESET "\n", file->path, c+1);
		} else {
			printf("%s/" COLOR_FILELS "%s" COLOR_RESET "\n", file->path, c+1);
		}
		*c = '/';
	}
}

static void list_one_dir(trashcv_t *tcv, dirinfo_t *dirinfo, char *path, uint8_t indent[5]) {
	if (tcv->print_long) {
		dir_long_info(tcv, dirinfo, indent);
	}
	printf("%s/", tcv->mountpoint);
	path[dirinfo->namestart + dirinfo->namelen] = '\0';
	if (dirinfo->namestart == 0) {
		if (tcv->printonelevelonly == 0) {
			printf(COLOR_DIRMATCH "%s" COLOR_RESET "/\n", path);
		} else {
			printf(COLOR_DIRLS "%s" COLOR_RESET "/\n", path);
		}
	} else {
		path[dirinfo->namestart - 1] = '\0';
		if (tcv->printonelevelonly == 0) {
			printf("%s/" COLOR_DIRMATCH "%s" COLOR_RESET "/\n", path, path + dirinfo->namestart);
		} else {
			printf("%s/" COLOR_DIRLS "%s" COLOR_RESET "/\n", path, path + dirinfo->namestart);
		}
		path[dirinfo->namestart - 1] = '/';
	}
	path[dirinfo->namestart + dirinfo->namelen] = '/';
}

static int num_len(uint64_t x) {
	int res = 1;
	while (x >= 10) {
		res++;
		x /= 10;
	}
	return res;
}

static void list_files(trashcv_t *tcv, trashfile_t **filestab, uint32_t filescount, uint8_t mode) {
	uint32_t i;
	dirinfo_t *curr;
	char *path;
	void *glob = NULL;
	char *c, *fi;
	uint32_t startfrom = 0;
	uint8_t space_needed[5] = {0, strlen(columnnames[1]), strlen(columnnames[2]), strlen(columnnames[3]), strlen(columnnames[4])};
	struct passwd *pwd;
	struct group *grp;
	int usrlen, grplen;
	char idx_str[50];

	if (tcv->expanddirs) {
		glob = glob_new((uint8_t*) tcv->name);
		if (strlen(tcv->starting_point_mfs) == 0) {
			startfrom = 0;
		} else {
			startfrom = strlen(tcv->starting_point_mfs) + 1;
		}
	}

	// count indents
	if (mode == TR_LIST || mode == TR_DUPRES) {
		if (mode == TR_DUPRES) {
			space_needed[0] = num_len(filescount) + 2;
		}
		for (i=0; i<filescount; i++) {
			if (tcv->expanddirs) {
				if (num_len((uint64_t)(filestab[i]->inode)) > space_needed[1]) {
					space_needed[1] = num_len((uint64_t)(filestab[i]->inode));
				}
				pwd = getpwuid(filestab[i]->uid);
				if (pwd == NULL) {
					usrlen = num_len((uint64_t)(filestab[i]->uid));
				} else {
					usrlen = strlen(pwd->pw_name);
				}
				if (usrlen > space_needed[2]) {
					space_needed[2] = usrlen;
				}
				grp = getgrgid(filestab[i]->gid);
				if (grp == NULL) {
					grplen = num_len((uint64_t)(filestab[i]->gid));
				} else {
					grplen = strlen(grp->gr_name);
				}
				if (grplen > space_needed[3]) {
					space_needed[3] = strlen(grp->gr_name);
				}
				if (num_len(filestab[i]->fleng) > space_needed[4]) {
					space_needed[4] = num_len(filestab[i]->fleng);
				}
			} else {
				if (filestab[i]->dirinfo) {
					if (num_len(filestab[i]->dirinfo->totalfleng) > space_needed[4]) {
						space_needed[4] = num_len(filestab[i]->dirinfo->totalfleng);
					}
				}
				if (filestab[i]->fnmatch) {
					if (num_len((uint64_t)(filestab[i]->inode)) > space_needed[1]) {
						space_needed[1] = num_len((uint64_t)(filestab[i]->inode));
					}
					pwd = getpwuid(filestab[i]->uid);
					if (pwd == NULL) {
						usrlen = num_len((uint64_t)(filestab[i]->uid));
					} else {
						usrlen = strlen(pwd->pw_name);
					}
					if (usrlen > space_needed[2]) {
						space_needed[2] = usrlen;
					}
					grp = getgrgid(filestab[i]->gid);
					if (grp == NULL) {
						grplen = num_len((uint64_t)(filestab[i]->gid));
					} else {
						grplen = strlen(grp->gr_name);
					}
					if (grplen > space_needed[3]) {
						space_needed[3] = strlen(grp->gr_name);
					}
					if (num_len(filestab[i]->fleng) > space_needed[4]) {
						space_needed[4] = num_len(filestab[i]->fleng);
					}
				}
			}
		}
	}

	if ((tcv->print_long && isatty(1)) || mode == TR_DUPRES) {
		list_header(tcv, space_needed);
	}

	for (i=0; i<filescount; i++) {
		if (mode == TR_LIST) {
			if (tcv->expanddirs) {
				if (tcv->print_long) {
					file_long_info(tcv, filestab[i], space_needed);
				}
				c = strdup(filestab[i]->path);
				printf("%s/", tcv->mountpoint);
				if (startfrom > 0) {
					c[startfrom-1] = '\0';
					printf("%s/", c);
					c += startfrom;
				}
				while (1) {
					fi = strchr(c, '/');
					if (fi == NULL) {
						if (tcv->printonelevelonly == 0 && tcv->type_f && glob_match(glob, (uint8_t*)c, strlen(c))) {
							printf(COLOR_MATCH "%s" COLOR_RESET "\n", c);
						} else {
							printf("%s\n", c);
						}
						break;
					} else {
						*fi = '\0';
						if (tcv->printonelevelonly == 0 && tcv->type_d && glob_match(glob, (uint8_t*)c, fi - c)) {
							printf(COLOR_DIRMATCH "%s" COLOR_RESET "/", c);
						} else {
							printf("%s/", c);
						}
					}
					c = fi + 1;
				}
			} else {
				curr = filestab[i]->dirinfo;
				if (curr) {
					path = strdup(filestab[i]->path);
					while (curr) {
						list_one_dir(tcv, curr, path, space_needed);
						curr = curr->next;
					}
					free(path);
				}
				if (filestab[i]->fnmatch) {
					list_one_file(tcv, filestab[i], space_needed);
				}
			}
		} else if (mode == TR_DUPRES) {
			snprintf(idx_str,50,"(%u)",i);
			idx_str[49]='\0';
			printf("%*s  ", space_needed[0], idx_str);
			file_long_info(tcv, filestab[i], space_needed);
			printf("\n");
		} else if (mode == TR_RECOVER) {
			//space_needed[1] = space_needed[2] = space_needed[3] = space_needed[4] = 10;
			//file_long_info(tcv, filestab[i], space_needed);
			printf("%s/%s " COLOR_ARROW "=>" COLOR_RESET " %s/%s\n", tcv->mountpoint, filestab[i]->path, tcv->destpath, filestab[i]->destrelpath);
		}
	}
}


static int recover_list(trashcv_t *tcv, trashfile_t **trashlist, uint32_t trashcount) {
	uint32_t i;
	uint32_t gcnt;
	uint32_t *gids;
	if (getallgroups(&gids, &gcnt) < 0) {
		exit(1);
	}
	for (i=0; i<trashcount; i++) {
		recover_one_file(tcv, trashlist[i]->inode, strlen(trashlist[i]->destrelpath), trashlist[i]->destrelpath, tcv->uid, gids, gcnt);
	}
	free(gids);
	return 0;
}


static int purge_list(trashcv_t *tcv, trashfile_t **trashlist, uint32_t trashcount) {
	uint32_t i;
	for (i=0; i<trashcount; i++) {
		purge_one_file(tcv, trashlist[i]->inode, tcv->uid);
	}
	return 0;
}


int trashadmin_read_args(cv_t *cv, trashcv_t *tcv, const char *opts) {
	int ch, pos, i;
	int rpl,mpl;
	char *cp;
	uint8_t type_set = 0;
	struct passwd *pwd;
	int64_t uid, num;
	char pbuff[PATH_MAX];
	dev_t devid;
	struct stat st;

	mpl = strlen(tcv->mountpoint);

//	opterr = 0;
	while ((ch=getopt(cv->argc, cv->argv, opts)) != -1) {
		switch (ch) {
			case 'l':
				tcv->query_long = 1;
				tcv->print_long = 1;
				break;
			case 'x':
				tcv->expanddirs = 1;
				break;
			case 'u':
				if (tcv->uid != 0) {
					fprintf(stderr, "'-u' option is for root only\n");
					return 1;
				}
				tcv->use_any_uid = 0;
				uid = strtoll(optarg, &cp, 10);
				if (*cp != '\0') { // not a number? maybe this is username
					pwd = getpwnam(optarg);
					if (pwd == NULL) {
						fprintf(stderr, "error: can't find username %s\n", optarg);
						return -1;
					}
					tcv->uid = pwd->pw_uid;
				} else if (uid < 0) {
					fprintf(stderr, "error: uid can't be negative");
					return 1;
				} else {
					tcv->uid = uid;
				}
				break;
			case 'b':
				tcv->mints = parse_ts(optarg, 0);
				break;
			case 'e':
				tcv->maxts = parse_ts(optarg, 1);
				break;
			case 'n':
				if (tcv->name != NULL) {
					free(tcv->name);
				}
				tcv->name = strdup(optarg);
				break;
			case 't':
				if (!type_set) {
					tcv->type_f = 0;
					tcv->type_d = 0;
				}
				cp = optarg;
				while (*cp) {
					if (*cp == 'f') {
						tcv->type_f = 1;
					} else if(*cp == 'd') {
						tcv->type_d = 1;
					} else {
						fprintf(stderr, "wrong type!\n");
						return -1;
					}
					cp++;
				}
				break;
			case 'r':
				cp = optarg;
				if (strlen(optarg) != 1) {
					fprintf(stderr, "error: -r argument should be one of 'A', 'E', 'L', 'N', 'P'\n");
				}
				switch (toupper((*cp))) {
					case 'E':
						tcv->dupresolvingmode = DUPRES_EARLIEST;
						break;
					case 'L':
						tcv->dupresolvingmode = DUPRES_LATEST;
						break;
					case 'A':
						tcv->dupresolvingmode = DUPRES_ALL;
						break;
					case 'N':
						tcv->dupresolvingmode = DUPRES_NONE;
						break;
					case 'P':
						tcv->dupresolvingmode = DUPRES_PICK;
						break;
					default:
						fprintf(stderr, "error: -r argument should be one of 'A', 'E', 'L', 'N', 'P'\n");
						return 1;
				}
				break;
			case 'k':
				if (strlen(optarg) != 3) {
					fprintf(stderr, "umask should have length 3\n");
					return 1;
				}
				tcv->cumask = 0;
				for (i=0; i<=2; i++) {
					tcv->cumask *= 8;
					if (optarg[i] < '0' || optarg[i] > '7') {
						fprintf(stderr, "umask elements should be in range [0-7]\n");
						return 1;
					}
					tcv->cumask += optarg[i] - '0';
				}
				break;
			case 'c':
				if (optarg[0] != '0' && optarg[0] != '1') {
					fprintf(stderr, "wrong copysgid!\n");
					return 1;
				}
				tcv->copysgid = optarg[0] - '0';
				break;
			case 'd':
				if (tcv->destpath != NULL) {
					fprintf(stderr, "-d argument passed more than once\n");
					return 1;
				}
				if (strlen(optarg) == 0) {
					fprintf(stderr, "error: empty destination path!\n");
					return 1;
				}
				if (stat(optarg,&st) != 0) {
					fprintf(stderr, "error: stat destination path (%s): %s\n",optarg,strerr(errno));
					return -1;
				}
				if (!S_ISDIR(st.st_mode)) {
					fprintf(stderr, "error: destination path (%s) is not a directory!\n",optarg);
					return -1;
				}
				tcv->destinode = st.st_ino;
				if (realpath(optarg,pbuff)==NULL) {
					fprintf(stderr, "error resolving destination path: %s\n",strerr(errno));
					return -1;
				}
				rpl = strlen(pbuff);
				if (st.st_dev!=tcv->mountpoint_dev || rpl<mpl || memcmp(pbuff,tcv->mountpoint,mpl)!=0 || (pbuff[mpl]!='/' && pbuff[mpl]!=0)) {
					fprintf(stderr, "error: destination path (%s) aren't from the current or given MFS instance (%s)\n",pbuff,tcv->mountpoint);
					return -1;
				}
				tcv->destpath = strdup(pbuff);
				if (rpl==mpl) {
					tcv->destpath_mfs = malloc(1);
					tcv->destpath_mfs[0] = '\0';
				} else {
					tcv->destpath_mfs = strdup(pbuff+mpl+1);
				}
				break;
			case 'i':
				if (tcv->dirpaths || tcv->iomode) {
					fprintf(stderr, "error: only one of -p -i -s arguments should be given\n");
					return 1;
				}
				pos = optind - 1;
				while (pos < cv->argc && cv->argv[pos][0] != '-') {
					pos++;
				}
				if (pos == optind - 1) {
					fprintf(stderr, "error: no inodes given after -i argument\n");
					return 1;
				}
				// todo: bug bo path nie zaczyna sie na '-'?
				if (tcv->inodes) {
					tcv->inodes = realloc(tcv->inodes, sizeof(uint32_t) * (tcv->inodescnt + pos - optind + 1));
				} else {
					tcv->inodes = malloc(sizeof(uint32_t) * (pos - optind + 1));
				}
				if (tcv->inodes == NULL) {
					fprintf(stderr, "error: realloc");
					return -1;
				}
				pos = optind - 1;
				while (pos < cv->argc && cv->argv[pos][0] != '-') {
					if (cv->argv[pos][0] == '-') {
						fprintf(stderr, "error: inode starts with '-' sign (%s)\n", cv->argv[pos]);
					}
					tcv->inodes[tcv->inodescnt] = strtoul(cv->argv[pos], &cp, 10);
					if (*cp != '\0') {
						fprintf(stderr, "error: given inode is not a number (%s)\n", cv->argv[pos]);
						return 1;
					}
					pos++;
					tcv->inodescnt++;
				}
				optind = pos;
				break;

			case 'p':
				if (tcv->inodes || tcv->iomode) {
					fprintf(stderr, "error: only one of -p -i -s arguments should be given\n");
					return 1;
				}
				pos = optind - 1;
				while (pos < cv->argc && cv->argv[pos][0] != '-') {
					pos++;
				}
				if (pos == optind - 1) {
					fprintf(stderr, "error: no paths given after -p argument\n");
					return 1;
				}
				if (tcv->dirpaths) {
					tcv->dirpaths = realloc(tcv->dirpaths, sizeof(char*) * (tcv->dirpathscnt + pos - optind + 1));
				} else {
					tcv->dirpaths = malloc(sizeof(char*) * (pos - optind + 1));
				}
				if (tcv->dirpaths == NULL) {
					fprintf(stderr, "error: realloc\n");
					return -1;
				}
				pos = optind - 1;
				while (pos < cv->argc && cv->argv[pos][0] != '-') {
					rpl = myrealpath(cv->argv[pos],pbuff,&devid);
					if (rpl<0) {
						fprintf(stderr,"error: can't resolve path: %s\n",cv->argv[pos]);
						return -1;
					}
					if (devid!=tcv->mountpoint_dev || rpl<=mpl || memcmp(pbuff,tcv->mountpoint,mpl)!=0 || pbuff[mpl]!='/') {
						fprintf(stderr, "error: given paths aren't from the current or given MFS instance\n");
						return -1;
					}
					tcv->dirpaths[tcv->dirpathscnt] = strdup(pbuff+mpl+1);
					tcv->dirpathscnt++;
					pos++;
				}
				optind = pos;
				break;
			case 'm':
				num = strtol(optarg, &cp, 10);
				if (*cp != '\0' || (num != 0 && num != 1)) {
					fprintf(stderr, "unrecognized restore path option\n");
					return 1;
				}
				if (num == 0) {
					tcv->pathmode = PATHMODE_DIRECT;
				} else if(num == 1) {
					tcv->pathmode = PATHMODE_RESTORE;
				}
				break;
			case 's':
				if (tcv->inodes || tcv->dirpaths) {
					fprintf(stderr, "error: only one of -p -i -s arguments should be given\n");
					return 1;
				}
				tcv->iomode = 1;
				break;
			case '?':
				if (optopt != 0) {
					fprintf(stderr, "%s: invalid option -- '%c'\n", cv->argv[0], optopt);
				}
				nobreak;
			default:
				return 2;
		}
	}

	cv->argc -= optind;
	cv->argv += optind;
	return 0;
}


char* path_from_inode(uint32_t inode) {
	int leng;
	char *path, trashprefix[10];

	ps_comminit();
	ps_put32(inode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_PATHS, MATOCL_FUSE_PATHS, NULL)) < 0) {
		goto error;
	}
	if (leng == 1) {
		fprintf(stderr, "couldn't get path for inode %u: %s\n", inode, mfsstrerr(ps_get8()));
		goto error;
	}

	leng = ps_get32();
	if (leng <= 9) {
		fprintf(stderr, "file with inode %u is not in trash\n", inode);
		goto error;
	}
	ps_getbytes((uint8_t*)trashprefix,9);
	trashprefix[9] = '\0';
	if (strcmp(trashprefix, "./TRASH (") != 0) {
		fprintf(stderr, "file with inode %u is not in trash\n", inode);
		goto error;
	}

	path = (char*) malloc(sizeof(char) * (leng - 9));
	ps_getbytes((uint8_t*)path,leng-10);
	path[leng - 10] = '\0';

	return path;
error:
	return NULL;
}


trashfile_t* get_one_el_from_inode(uint32_t inode) {
	int leng;
	char *path;
	trashfile_t *res;
	uint32_t tmptime;

	path = path_from_inode(inode);
	if (path == NULL) {
		return NULL;
	}

	res = (trashfile_t*) malloc(sizeof(trashfile_t) + strlen(path));
	memset(res, 0, sizeof(trashfile_t) + strlen(path));
	res->inode = inode;
	memcpy(res->path, path, sizeof(char) * (strlen(path) + 1));
	ps_comminit();
	ps_put32(inode);

	if ((leng = ps_send_and_receive(CLTOMA_FUSE_GETATTR, MATOCL_FUSE_GETATTR, NULL)) < 0) {
		goto error;
	}
	if (leng == 1) {
		fprintf(stderr, "couldn't get attributes for inode %u: %s\n", inode, mfsstrerr(ps_get8()));
		goto error;
	}
	ps_dummyget(1);
	res->type_mode = ps_get16();
	res->uid = ps_get32();
	res->gid = ps_get32();
	res->deltime = ps_get32();
	tmptime = ps_get32();
	if (res->deltime < tmptime) {
		res->deltime = tmptime;
	}
	tmptime = ps_get32();
	if (res->deltime < tmptime) {
		res->deltime = tmptime;
	}
	res->nlink = ps_get32();
	res->fleng = ps_get64();
	ps_dummyget(1);
	return res;
error:
	return NULL;
}


static uint32_t get_elements_from_inodes(trashcv_t *tcv, trashfile_t ***trashelementstab, uint32_t *trashelementscount) {
	int nextpos = 0;
	trashfile_t *tmp;
	*trashelementstab = (trashfile_t**) malloc(sizeof(trashfile_t*) * tcv->inodescnt);
	for (int i=0; i<(int)tcv->inodescnt; i++) {
		tmp = get_one_el_from_inode(tcv->inodes[i]);
		if (tmp == NULL) {
			continue; // todo: or error and exit(1)?
		}
		(*trashelementstab)[nextpos] = tmp;
		(*trashelementstab)[nextpos]->fnmatch = 1; // todo: ???
		nextpos++;
	}
	*trashelementstab = (trashfile_t**) realloc(*trashelementstab, sizeof(trashfile_t*) * nextpos);
	*trashelementscount = nextpos;
	return *trashelementscount;
}


static void sort_unique(uint32_t **arr, uint32_t *len) {
	int i, nextpos = 0;
	qsort(*arr, *len, sizeof(uint32_t), uint32_t_cmp);
	for (i=0; i<(int)*len; i++) {
		if (i == 0 || (*arr)[i] != (*arr)[i-1]) {
			(*arr)[nextpos++] = (*arr)[i];
		}
	}
	*arr = realloc(*arr, sizeof(uint32_t) * nextpos);
	*len = nextpos;
}


static uint8_t confirmation(const char *s) {
	char *linebuff=NULL;
	size_t linebufflen=0;
	ssize_t linelen;
	do {
		fprintf(stderr, "%s (y/n)\n", s);
		linelen = getline(&linebuff, &linebufflen, stdin);
		if (linelen == 2) {
			if (tolower(linebuff[0]) == 'y') {
				return 1;
			} else if (tolower(linebuff[0]) == 'n') {
				return 0;
			}
		} else if (linelen == -1) {
			fprintf(stderr, "error: read EOF\n");
			return 0;
		}
	} while (1);
}

/*
static void calc_mountpoint(trashcv_t *tcv) {
	struct stat st;
	dev_t dev;
	uint32_t mountpointlen;
	char *tmppath, *p;

	dev = tcv->mount_stat->st_dev;

	tmppath = strdup(tcv->starting_point);
	p = tmppath + 1;
	while (1) {
		p = strchr(p + 1, '/');
		if (p == NULL) {
			mountpointlen = strlen(tcv->starting_point);
			break;
		}
		*p = '\0';
		if (stat(tmppath, &st) != 0) {
			fprintf(stderr, "error: can't stat %s\n", tmppath);
			exit(1);
		}
		*p = '/';
		if (st.st_dev == dev) {
			mountpointlen = p - tmppath;
			break;
		}
	}
	free(tmppath);

	tcv->mountpoint = (char*) malloc(sizeof(char) * (mountpointlen + 1));
	memcpy(tcv->mountpoint, tcv->starting_point, sizeof(char) * mountpointlen);
	tcv->mountpoint[mountpointlen] = '\0';
}
*/

static int master_connect(trashcv_t *tcv) {
	return open_master_conn(tcv->mountpoint, NULL, NULL, NULL, 0, 0);
}

//---------------------
//-		API
//---------------------

static const char
	*trashsustxt[] = {
		"lists sustained files",
		"",
		"usage: mfssustainedlist [listpath] [-u uid] [-x]",
		"",
		" -u - (root only option) filter for specific user - accepts uid or username; if not specified, lists all users files",
		" -x - show all sessions",
		"",
		"If listpath isn't given then current location is used",
		NULL,
	};

static const char *trashrecovertxt_1[] = {
		"recovers files from trash",
		"",
		"usage: mfstrashrecover [mountpoint] [-d destination] [-p path [path ...]] [-i inode [inode ...]] [-r duplicate_resolving] [-m path_restore_mode] [-k mask] [-c copysgid] [-s]",
		"",
		" -d - destination directory. When not specified, root node is assumed and -m 1 option is forced, restoring to original locations",
		" -p - space separated list of absolute paths to directories to recover",
		" -i - space separated list of inodes of files to recover",
		" -r - how to act in case of multiple files with exact same paths. Possible values:",
		"      (default) 'A' - recover all; 'E' - recover only the earliest; 'L' - recover only the latest; 'N' - recover none; 'P' - pick during run.",
		" -m - how to restore paths inside destination",
		"      0 - files and directories are put directly into destination - default option when destination is specified",
		"      1 - restores full original path inside destination - forced option when destination is not specified, restoring to original locations",
		" -k - umask for newly created directories (default 777)",
		"",
		"If mountpoint isn't given then current location is used",
		NULL,
	};

static const char *trashrecovertxt_2 = " -c - copysgid for newly created directories (default %u - it depends on system)";

static const char *trashrecovertxt_3[] = {
		" -s - pipe mode - allows to recover files by piping output from mfstrashlist (also ran in pipe mode) to this program. Disables -r option",
		"",
		"Only one of -p -i -s options should be used",
		NULL,
	};


static const char
	*trashpurgetxt[] = {
		"purges files from trash",
		"",
		"usage: mfstrashpurge [mountpoint] [-p path [path ...]] [-i inode [inode ...]] [-r duplicate_resolving] [-s]",
		"",
		" -p - space separated list of absolute paths to directories to purge",
		" -i - space separated list of inodes of files to purge",
		" -r - how to act in case of multiple files with exact same paths. Possible values:",
		"      (default) 'A' - purge all; E' - purge only the earliest; 'L' - purge only the latest; 'N' - purge none; 'P' - pick during run.",
		" -s - pipe mode - allows to purge files by piping output from mfstrashlist (also ran in pipe mode) to this program. Disables -r option",
		"",
		"Only one of -p -i -s options should be used",
		"",
		"If mountpoint isn't given then current location is used",
		NULL,
	};

static const char
	*trashlisttxt[] = {
		"lists trash content",
		"",
		"usage: mfstrashlist [listpath] [-n name] [-u uid] [-b del_time_interval_begin] [-e del_time_interval_end] [-t type] [-l] [-x] [-s] [-r duplicate_resolving]",
		"",
		" -n - search for files or directories with given name. Accepts patterns with '*', '?' and ranges. WARNING! These patterns must be enclosed in quotation marks (\")",
		"      When not specified only the top level content of path will be listed (-x option overwrites this)",
		" -u - (root only option) filter for specific user - accepts uid or username; if not specified, lists all users files",
		" -b - begin of deletion time range filter - includes only files deleted after this time",
		" -e - end of deletion time range filter - includes only files deleted before this time",
		" -t - sequence of character limiting output to files ('f') or directories ('d') (default 'fd')",
		" -l - prints more detailed info (inode, uid, gid, size, deletion time). As MFS trash doesn't store directories, only files, inode, uid, gid is unavailable for directories.",
		"      Size is sum of sizes and deletion time is maximum deletion time of files inside that matches applied filters",
		" -x - expand content of directories",
		" -s - pipe mode - allows to pipe output of this program to either mfstrashrecover or mfstrashpurge to do an action on found files",
		" -r - (only in pipe mode) how to act in case of multiple files with exact same paths. Possible values:",
		"      (default) 'A' - pass all; E' - pass only the earliest; 'L' - pass only the latest; 'N' - pass none",
		"",
		"If listpath isn't given then current location is used",
		"Listpath doesn't have to exist, as it could still exist in trash.",
		"",
		"Accepted time formats:",
		" - absolute - unix timestamp",
		" - absolute - mm/dd or yy/mm/dd-hh:mm or any substring that allows to deduce the rest - larger units are then set to current time and smaller accordingly to time range endpoint",
		"   eg: '15-' - current day; time 15:00 for begin or 15:59 for end. '/11/' - current year; 11/01 00:00 for begin 11/30 23:59 for end. '19//' - for year 2019. 5/3 - 3rd May",
		"  -relative - number followed by unit ('h' - hour, 'd' - day, 'w' - week). Possible to concatenate multiple times (eg: 1d5h - 1 day and 5 hours ago)",
		"",
		NULL,
	};


void sustainedhlp(void) {
	print_lines(trashsustxt);
}

void trashpurgehlp(void) {
	print_lines(trashpurgetxt);
}

void trashrecoverhlp(void) {
	print_lines(trashrecovertxt_1);
	fprintf(stderr, trashrecovertxt_2, readcopysgid(NULL));
	fprintf(stderr, "\n");
	print_lines(trashrecovertxt_3);
}

void trashlisthlp(void) {
	print_lines(trashlisttxt);
}

void trashpurgeexe(cv_t *cv) {
	trashfile_t **trashelementstab = NULL, **tmptab;
	uint32_t trashelementscnt = 0, tmpcnt;
	trashcv_t *tcv,tcvstorage;
	uint32_t i;
	uint8_t warning_triggered = 0;
	uint32_t ioinode;
	int ores;
	char *iopath;

	if (cv->argc == 1) {
		trashpurgehlp();
		exit(1);
	}
	tcv = &tcvstorage;
	trashcv_init(tcv);
	if (check_first_arg(cv,tcv)<0) {
		goto err;
	}
	ores = trashadmin_read_args(cv, tcv, "p:i:r:s?");
	if (ores<0) {
		goto err;
	}
	if (ores==1) {
		goto usage;
	}
	if (ores==2) {
		goto usagefull;
	}
	if (!tcv->iomode && tcv->dirpathscnt == 0 && tcv->inodescnt == 0) {
		fprintf(stderr, "no -i or -p or -s given\n");
		goto usage;
	}
	if ((tcv->dirpathscnt > 0 || tcv->inodescnt > 0) && tcv->dupresolvingmode == DUPRES_DEFAULT) {
		tcv->dupresolvingmode = DUPRES_ALL;
	}
	if (tcv->dirpathscnt > 0 && (tcv->dupresolvingmode == DUPRES_EARLIEST || tcv->dupresolvingmode == DUPRES_LATEST ||
			tcv->dupresolvingmode == DUPRES_PICK)) {
		tcv->query_long = 1;
	}
	if (tcv->iomode && tcv->dupresolvingmode != DUPRES_DEFAULT) {
		fprintf(stderr, "error: can't use -r option with -s option\n");
		goto usage;
	}
	if (cv->argc > 0) {
		fprintf(stderr, "error: invalid argument (%s)\n", cv->argv[0]);
		goto usage;
	}

	if (master_connect(tcv)<0) {
		goto err;
	}

	if (tcv->iomode) {
		if (scanf("%u", &trashelementscnt) != 1) {
			fprintf(stderr, "error: scanf\n");
			goto err;
		}
		trashelementstab = (trashfile_t**) malloc(sizeof(trashfile_t*) * trashelementscnt);
		for (i=0; i<trashelementscnt; i++) {
			if (scanf("%u", &ioinode) != 1) {
				fprintf(stderr, "error: scanf\n");
				goto err;
			}
			iopath = path_from_inode(ioinode);
			if (iopath == NULL) {
				continue; // todo: or error and exit(1)?
			}
			trashelementstab[i] = (trashfile_t*) malloc(sizeof(trashfile_t) + strlen(iopath));
			memset(trashelementstab[i], 0, sizeof(trashfile_t) + strlen(iopath));

			trashelementstab[i]->inode = ioinode;
			memcpy(trashelementstab[i]->path, iopath, sizeof(char) * (strlen(iopath) + 1));
			trashelementstab[i]->fnmatch = 1; // todo: ???
			trashelementstab[i]->destrelpath = trashelementstab[i]->path;
			free(iopath);
		}
	} else if (tcv->inodescnt > 0) {
		sort_unique(&(tcv->inodes), &(tcv->inodescnt));
		if (get_elements_from_inodes(tcv, &trashelementstab, &trashelementscnt) != tcv->inodescnt) {
//			fprintf(stderr, "warning: some inodes were not found in trash\n");
			warning_triggered = 1;
		}
	} else {
		for (i=0; i<tcv->dirpathscnt; i++) {
			if (get_elements_from_path(tcv, tcv->dirpaths[i], &tmptab, &tmpcnt) == 0) {
				fprintf(stderr, "warning: found no files under directory %s\n", tcv->dirpaths[i]);
				warning_triggered = 1;
			} else {
				merge_trash_lists(&trashelementstab, &trashelementscnt, tmptab, tmpcnt);
			}
		}
	}
	if (trashelementscnt == 0) {
		fprintf(stderr, "found no elements to purge!\n");
		goto ok;
	}
	if (warning_triggered) {
		if (!tcv->iomode && confirmation("warnings were triggered, continue anyway?") == 0) {
			goto ok;
		}
	}
	for (i=0; i<trashelementscnt; i++) {
		trashelementstab[i]->fnmatch = 1; // hmm
	}
	if (!tcv->iomode) {
		tmpcnt = resolve_duplicates(tcv, trashelementstab, trashelementscnt);
		if ((tmptab = realloc(trashelementstab, sizeof(trashfile_t*) * tmpcnt)) == NULL && tmpcnt > 0) {
			fprintf(stderr, "error: realloc\n");
			goto err;
		}
		if (tmpcnt == 0) {
			fprintf(stderr, "no elements to purge\n");
			goto ok;
		}
		trashelementstab = tmptab;
		trashelementscnt = tmpcnt;
	}
	//tcv->print_long = 1;
	list_files(tcv, trashelementstab, trashelementscnt, TR_LIST);
	if (!tcv->iomode && confirmation("purge files?") == 0) {
		goto ok;
	}
	purge_list(tcv, trashelementstab, trashelementscnt);
ok:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	return ;
err:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	exit(1);
	return; // pro forma
usage:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	usagesimple(cv);
	return; // pro forma
usagefull:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	usagefull(cv);
	return; // pro forma
}


void trashrecoverexe(cv_t *cv) {
	trashfile_t **trashelementstab = NULL, **tmptab;
	uint32_t trashelementscnt = 0, tmpcnt;
	trashcv_t *tcv,tcvstorage;
	uint32_t nameoffset = 0;
	uint32_t i, j;
	char *c;
	int ores;
	uint8_t warning_triggered = 0;
	uint32_t ioinode;
	char *iopath;

	if (cv->argc == 1) {
		trashrecoverhlp();
		exit(1);
	}
	tcv = &tcvstorage;
	trashcv_init(tcv);
	if (check_first_arg(cv,tcv)<0) {
		goto err;
	}
	ores = trashadmin_read_args(cv, tcv, "p:i:d:r:k:c:m:c:s?");
	if (ores<0) {
		goto err;
	}
	if (ores==1) {
		goto usage;
	}
	if (ores==2) {
		goto usagefull;
	}
	if (cv->argc > 0) {
		fprintf(stderr, "error: invalid argument (%s)\n", cv->argv[0]);
		goto usage;
	}
	if (!tcv->iomode && tcv->dirpathscnt == 0 && tcv->inodescnt == 0) {
		fprintf(stderr, "no -i or -p or -s given\n");
		goto usage;
	}
	if ((tcv->dirpathscnt > 0 || tcv->inodescnt > 0) && tcv->dupresolvingmode == DUPRES_DEFAULT) {
		tcv->dupresolvingmode = DUPRES_ALL;
	}
	if (tcv->dirpathscnt > 0 && (tcv->dupresolvingmode == DUPRES_EARLIEST || tcv->dupresolvingmode == DUPRES_LATEST ||
			tcv->dupresolvingmode == DUPRES_PICK)) {
		tcv->query_long = 1;
	}
	if (tcv->iomode && tcv->dupresolvingmode != DUPRES_DEFAULT) {
		fprintf(stderr, "error: can't use -r option with -s option\n");
		goto usage;
	}
	if (tcv->destpath == NULL && tcv->pathmode == PATHMODE_DIRECT) {
		fprintf(stderr, "error: can't use -m 0 option without -d option\n");
		goto usage;
	}

	if (master_connect(tcv)<0) {
		goto err;
	}

	if (tcv->destpath == NULL) { // destination path not defined
		tcv->destpath = strdup(tcv->mountpoint);
		tcv->destpath_mfs = malloc(1);
		tcv->destpath_mfs[0] = 0;
		tcv->destinode = 1; // restoring relative to root
		tcv->pathmode = PATHMODE_RESTORE;
	}

	if (tcv->copysgid == -1) { // copysgid not set
		tcv->copysgid = readcopysgid(tcv);
	}
	if (tcv->iomode) {
		if (scanf("%u", &trashelementscnt) != 1) {
			fprintf(stderr, "error: scanf\n");
			goto err;
		}
		trashelementstab = (trashfile_t**) malloc(sizeof(trashfile_t*) * trashelementscnt);
		for (i=0; i<trashelementscnt; i++) {
			if (scanf("%u", &ioinode) != 1) {
				fprintf(stderr, "error: scanf\n");
				goto err;
			}
			iopath = path_from_inode(ioinode);
			if (iopath == NULL) {
				continue; // todo: or error and exit(1)?
			}
			trashelementstab[i] = (trashfile_t*) malloc(sizeof(trashfile_t) + strlen(iopath));
			memset(trashelementstab[i], 0, sizeof(trashfile_t) + strlen(iopath));

			trashelementstab[i]->inode = ioinode;
			memcpy(trashelementstab[i]->path, iopath, sizeof(char) * (strlen(iopath) + 1));
			trashelementstab[i]->fnmatch = 1; // todo: ???
			trashelementstab[i]->destrelpath = trashelementstab[i]->path;
			free(iopath);
		}
	} else if (tcv->inodescnt > 0) {
		sort_unique(&(tcv->inodes), &(tcv->inodescnt));
		if (get_elements_from_inodes(tcv, &trashelementstab, &trashelementscnt) != tcv->inodescnt) {
			//fprintf(stderr, "warning: some inodes were not found in trash\n");
			warning_triggered = 1;
		}
		for (j=0; j<trashelementscnt; j++) {
			if (tcv->pathmode == PATHMODE_DIRECT) {
				c = strrchr(trashelementstab[j]->path, '/');
				if (c == NULL) {
					nameoffset = 0;
				} else {
					nameoffset = c - trashelementstab[j]->path + 1;
				}
			} else if (tcv->pathmode == PATHMODE_RESTORE) {
				nameoffset = 0;
			}
			trashelementstab[j]->destrelpath = trashelementstab[j]->path + nameoffset;
		}
	} else {
		for (i=0; i<tcv->dirpathscnt; i++) {
			if (get_elements_from_path(tcv, tcv->dirpaths[i], &tmptab, &tmpcnt) == 0) {
				fprintf(stderr, "warning: found no files under directory %s\n", tcv->dirpaths[i]);
				warning_triggered = 1;
			} else {
				if (tcv->pathmode == PATHMODE_DIRECT) {
					c = strrchr(tcv->dirpaths[i], '/');
					if (c == NULL) {
						nameoffset = 0;
					} else {
						nameoffset = c - tcv->dirpaths[i] + 1;
					}
				} else if (tcv->pathmode == PATHMODE_RESTORE) {
					nameoffset = 0;
				}
				for (j=0; j<tmpcnt; j++) {
					tmptab[j]->destrelpath = tmptab[j]->path + nameoffset;
				}
				merge_trash_lists(&trashelementstab, &trashelementscnt, tmptab, tmpcnt);
			}
		}
	}
	if (trashelementscnt == 0) {
		fprintf(stderr, "found no elements to recover!\n");
		goto ok;
	}
	if (warning_triggered) {
		if (!tcv->iomode && confirmation("warnings were triggered, continue anyway?") == 0) {
			goto ok;
		}
	}
	for (i=0; i<trashelementscnt; i++) {
		trashelementstab[i]->fnmatch = 1; // hmm
	}
	if (!tcv->iomode) {
		tmpcnt = resolve_duplicates(tcv, trashelementstab, trashelementscnt);
		if ((tmptab = realloc(trashelementstab, sizeof(trashfile_t*) * tmpcnt)) == NULL && tmpcnt > 0) {
			fprintf(stderr, "error: realloc\n");
			goto err;
		}
		if (tmpcnt == 0) {
			fprintf(stderr, "no elements to recover\n");
			goto ok;
		}
		trashelementstab = tmptab;
		trashelementscnt = tmpcnt;
	}
	list_files(tcv, trashelementstab, trashelementscnt, TR_RECOVER);
	if (!tcv->iomode && confirmation("recover files?") == 0) {
		goto ok;
	}
	recover_list(tcv, trashelementstab, trashelementscnt);
ok:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	return;
err:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	exit(1);
	return; // pro forma
usage:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	usagesimple(cv);
	return; // pro forma
usagefull:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	usagefull(cv);
	return; // pro forma
}


void trashlistexe(cv_t *cv) {
	trashfile_t **trashelementstab = NULL, **tmptab;
	uint32_t trashelementscnt = 0, tmpcnt;
	trashcv_t *tcv,tcvstorage;
	uint32_t i;
	int ores;

	tcv = &tcvstorage;
	trashcv_init(tcv);
	if (check_first_arg(cv,tcv)<0) {
		goto err;
	}
	ores = trashadmin_read_args(cv, tcv, ":u:b:e:t:n:slxr:?");
	if (ores<0) {
		goto err;
	}
	if (ores==1) {
		goto usage;
	}
	if (ores==2) {
		goto usagefull;
	}
	if (cv->argc > 0) {
		fprintf(stderr, "error: invalid argument '%s'\n", cv->argv[0]);
		goto usage;
	}
	if (tcv->iomode && (tcv->dupresolvingmode == DUPRES_PICK)) {
		fprintf(stderr, "error: -r P option is prohibited with -s option\n");
		goto usage;
	}
	if (!tcv->iomode && tcv->dupresolvingmode != DUPRES_DEFAULT) {
		fprintf(stderr, "error: -r option only available with -s option\n");
		goto usage;
	}
	if (tcv->iomode && tcv->dupresolvingmode == DUPRES_DEFAULT) {
		tcv->dupresolvingmode = DUPRES_ALL;
	}

	if (master_connect(tcv)<0) {
		goto err;
	}

	if (tcv->maxts < tcv->mints) {
		fprintf(stderr, "error: begin of time range filter > end of time range filter\n");
		goto usage;
	}
	if (tcv->name == NULL) {
		tcv->name = (char*) malloc(sizeof(char) * 2);
		tcv->name[0] = '*';
		tcv->name[1] = '\0';
		tcv->printonelevelonly = 1;
	} else if (strlen(tcv->name) == 0) {
		fprintf(stderr, "error: empty -n\n");
		goto usage;
	}

	find_trash_elements(tcv, &trashelementstab, &trashelementscnt);
	if (trashelementscnt == 0) {
		fprintf(stderr, "no elements found!\n");
		goto ok;
	}
	if (tcv->iomode) {
		tmpcnt = resolve_duplicates(tcv, trashelementstab, trashelementscnt);
		if ((tmptab = realloc(trashelementstab, sizeof(trashfile_t*) * tmpcnt)) == NULL && tmpcnt > 0) {
			fprintf(stderr, "error: realloc\n");
			goto err;
		}
		if (tmpcnt == 0) {
			fprintf(stderr, "\n");
			goto ok;
		}
		trashelementstab = tmptab;
		trashelementscnt = tmpcnt;

		printf("%u\n", trashelementscnt);
		for (i=0; i<trashelementscnt; i++) {
			printf("%u\n", trashelementstab[i]->inode);
		}
	} else {
		list_files(tcv, trashelementstab, trashelementscnt, TR_LIST);
	}
ok:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	return;
err:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	exit(1);
	return; // pro forma
usage:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	usagesimple(cv);
	return; // pro forma
usagefull:
	if (trashelementstab!=NULL) {
		free(trashelementstab);
	}
	trasncv_cleanup(tcv);
	usagefull(cv);
	return; // pro forma
}


#define SESSIONS_LINE_LIMIT 5

void sustainedexe(cv_t *cv) {
	susfile_t **suselementstab = NULL;
	uint32_t suselementscnt = 0;
	trashcv_t *tcv,tcvstorage;
	sessioninfo_t *sess;
	uint32_t i, j;
	uint32_t sesstoprint;
	int ores;
	char expire_str[100];

	tcv = &tcvstorage;
	trashcv_init(tcv);
	if (check_first_arg(cv,tcv)<0) {
		goto err;
	}
	ores = trashadmin_read_args(cv, tcv, "u:x?");
	if (ores<0) {
		goto err;
	}
	if (ores==1) {
		goto usage;
	}
	if (ores==2) {
		goto usagefull;
	}
	if (cv->argc > 0) {
		fprintf(stderr, "error: invalid argument '%s'\n", cv->argv[0]);
		goto usage;
	}
	if (master_connect(tcv)<0) {
		goto err;
	}

	one_sustained_query(tcv, "*", &suselementstab, &suselementscnt);
	getsessionlist();
	for (i=0; i<suselementscnt; i++) {
		printf("inode: %u  " COLOR_MATCH "%s" COLOR_RESET "\n", suselementstab[i]->inode, suselementstab[i]->path);
		if (suselementstab[i]->sessioncnt == 0) {
			printf("    no sessions found\n");
		} else {
			if (suselementstab[i]->sessioncnt > SESSIONS_LINE_LIMIT && tcv->expanddirs == 0) {
				sesstoprint = SESSIONS_LINE_LIMIT - 1;
			} else {
				sesstoprint = suselementstab[i]->sessioncnt;
			}
			for (j=0; j<sesstoprint; j++) {
				sess = getsessioninfo(suselementstab[i]->sessionids[j]);
				if (sess->expire == (uint32_t)-1) {
					snprintf(expire_str,100,"session active");
				} else if (sess->expire == 0) {
					snprintf(expire_str,100,"session expired");
				} else {
					snprintf(expire_str,100,"session expire in %u seconds", sess->expire);
				}
				expire_str[99]='\0';
				printf("    session %u  ip: %u.%u.%u.%u (%s)\t%s\n", sess->sessionid, (sess->ipaddr>>24)&0xFF, (sess->ipaddr>>16)&0xFF, (sess->ipaddr>>8)&0xFF, (sess->ipaddr)&0xFF, sess->info, expire_str);
			}
			if (sesstoprint != suselementstab[i]->sessioncnt) {
				printf("    ...and %u more sessions\n", suselementstab[i]->sessioncnt - sesstoprint);
			}
		}
	}
// ok:
	if (suselementstab!=NULL) {
		free(suselementstab);
	}
	trasncv_cleanup(tcv);
	return;
err:
	if (suselementstab!=NULL) {
		free(suselementstab);
	}
	trasncv_cleanup(tcv);
	exit(1);
	return; // pro forma
usage:
	if (suselementstab!=NULL) {
		free(suselementstab);
	}
	trasncv_cleanup(tcv);
	usagesimple(cv);
	return; // pro forma
usagefull:
	if (suselementstab!=NULL) {
		free(suselementstab);
	}
	trasncv_cleanup(tcv);
	usagefull(cv);
	return; // pro forma
}
