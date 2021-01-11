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
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>

#include "charts.h"
#include "strerr.h"
#include "crc.h"
#include "idstr.h"

#include "chartsdefs.h"

static const uint32_t calcdefs[]=CALCDEFS
static const statdef statdefs[]=STATDEFS
static const estatdef estatdefs[]=ESTATDEFS

// -i input_file (default - stats.mfs)
// -r 0..3 (range - default 0)
// -c stat1,stat2,... (txt version)
// -p png_filename (default NULL)
// -o txt_filename (default stdout)

#define HUMAN_DATE 1
#define HUMAN_SI 2
#define HUMAN_IEC 4

void usage(const char *appname) {
	uint32_t i,j;
	fprintf(stderr,"usage: %s -f fields [-i stats_file] [-r range] [-s separator] [-p png_filename [-x chart_width] [-y chart_height]] [-hHd]\n",appname);
	fprintf(stderr,"\t-f - field names separated by ',' or '*' for all fields\n");
	fprintf(stderr,"\t-i - name of mfs binary stats file (default:%s/%s)\n",DATA_PATH,CHARTS_FILENAME);
	fprintf(stderr,"\t-r - range type (see below)\n");
	fprintf(stderr,"\t-s - specify column separator (default: tabulator)\n");
	fprintf(stderr,"\t-p - optional PNG filename (field name will be added before .png ; YYYY -> YYYY_FIELD.png , YYYY.png -> YYYY_FIELD.png)\n");
	fprintf(stderr,"\t-x - PNG chart width (default: 1600)\n");
	fprintf(stderr,"\t-y - PNG chart height (should be divisible by 20 - if not then it will be adjusted; default:320)\n");
	fprintf(stderr,"\t-h - \"human-readable\" numbers using base 2 prefixes (IEC 60027)\n");
	fprintf(stderr,"\t-H - \"human-readable\" numbers using base 10 prefixes (SI)\n");
	fprintf(stderr,"\t-d - show timestamps in YYYY-MM-DD HH:MM format\n");
	fprintf(stderr,"\n");
	fprintf(stderr,"\tranges:\n");
	fprintf(stderr,"\t\t0 - short range (minute)\n");
	fprintf(stderr,"\t\t1 - medium range (6 minutes)\n");
	fprintf(stderr,"\t\t2 - long range (30 minutes)\n");
	fprintf(stderr,"\t\t3 - very long range (day)\n");
	fprintf(stderr,"\n");
	fprintf(stderr,"\tfield names:\n");
	j = 0;
	for (i=0 ; statdefs[i].name!=NULL ; i++) {
		if ((j%4)==0) {
			fprintf(stderr,"\t\t");
		}
		fprintf(stderr,"%15s,",statdefs[i].name);
		if ((j%4)==3) {
			fprintf(stderr,"\n");
		}
		j++;
	}
	for (i=0 ; estatdefs[i].name!=NULL ; i++) {
		if ((j%4)==0) {
			fprintf(stderr,"\t\t");
		}
		fprintf(stderr,"%15s,",estatdefs[i].name);
		if ((j%4)==3) {
			fprintf(stderr,"\n");
		}
		j++;
	}
	if ((j%4)!=0) {
		fprintf(stderr,"\n");
	}
}

int statsdump_parse (int32_t **stats,const char *opt) {
	char *str;
	char *p;
	uint32_t i;
	uint32_t cnt;
	uint32_t add;
	int found;
	static uint8_t *usedstats = NULL;
	static uint8_t *usedestats = NULL;
	static uint32_t statscnt = 0;
	static uint32_t estatscnt = 0;

	if (usedstats==NULL) {
		for (i=0 ; statdefs[i].name!=NULL ; i++) {
			statscnt++;
		}
		if (statscnt>0) {
			usedstats = malloc((statscnt+7)&0xFFF8);
			memset(usedstats,0,(statscnt+7)&0xFFF8);
		}
	}
	if (usedestats==NULL) {
		for (i=0 ; estatdefs[i].name!=NULL ; i++) {
			estatscnt++;
		}
		if (estatscnt>0) {
			usedestats = malloc((estatscnt+7)&0xFFF8);
			memset(usedestats,0,(estatscnt+7)&0xFFF8);
		}
	}
	if ((*stats)==NULL) {
		cnt=0;
	} else {
		for (cnt=0 ; (*stats)[cnt]>=0 ; cnt++) {}
	}
	add = 0;
	str = strdup(opt);
	for (p=strtok(str," ,;") ; p!=NULL ; p=strtok(NULL," ,;")) {
		if (p[0]=='*' && p[1]=='\0') {
			add += statscnt + estatscnt;
		} else {
			add++;
		}
	}
	strcpy(str,opt);
	*stats = realloc(*stats,(cnt+add+1)*sizeof(int32_t));
	if ((*stats)==NULL) {
		free(str);
		fprintf(stderr,"memory allocation error\n");
		return -1;
	}
	for (p=strtok(str," ,;") ; p!=NULL ; p=strtok(NULL," ,;")) {
		found = 0;
		if (p[0]=='*' && p[1]=='\0') {
			for (i=0 ; statdefs[i].name!=NULL ; i++) {
				if ((usedstats[i/8]&(1<<(i&7)))==0) {
					(*stats)[cnt] = i;
					cnt++;
					usedstats[i/8] |= (1<<(i&7));
				}
			}
			for (i=0 ; estatdefs[i].name!=NULL ; i++) {
				if ((usedestats[i/8]&(1<<(i&7)))==0) {
					(*stats)[cnt] = i+100;
					cnt++;
					usedestats[i/8] |= (1<<(i&7));
				}
			}
		} else {
			for (i=0 ; statdefs[i].name!=NULL && found==0 ; i++) {
				if (strcmp(p,statdefs[i].name)==0) {
					if ((usedstats[i/8]&(1<<(i&7)))==0) {
						(*stats)[cnt] = i;
						cnt++;
						usedstats[i/8] |= (1<<(i&7));
					}
					found = 1;
				}
			}
			for (i=0 ; estatdefs[i].name!=NULL && found==0 ; i++) {
				if (strcmp(p,estatdefs[i].name)==0) {
					if ((usedestats[i/8]&(1<<(i&7)))==0) {
						(*stats)[cnt] = i+100;
						cnt++;
						usedestats[i/8] |= (1<<(i&7));
					}
					found = 1;
				}
			}
			if (found==0) {
				fprintf(stderr,"statistic '%s' not found\n",p);
			}
		}
	}
	(*stats)[cnt] = -1;
	free(str);
	return 0;
}

void statsdump_print_date(uint32_t ts) {
/*
	static struct tm *structtime;
	time_t tts = ts;

	structtime=gmtime(&tts);
	strftime(buff,16,"%Y-%m-%d %H:%M",structtime);
*/
	uint32_t day,dsec,h,m,s,wd,y,yd,yp,mon;

	day = ts/86400;
	dsec = ts%86400;
	h = dsec/3600;
	m = (dsec%3600)/60;
	s = dsec%60;
	wd = (day+3)%7;
	y = (day/1461)*4+1970;
	yd = day%1461;
	yp = 0;
	if (yd>=730) {
		if (yd>=1096) {
			y+=3;
			yd-=1096;
		} else {
			y+=2;
			yd-=730;
			yp=1;
		}
	} else if (yd>=365) {
		y++;
		yd-=365;
	}
	if (yd>=181+yp) {
		if (yd>=273+yp) {
			if (yd>=334+yp) {
				mon=11;
				yd-=(334+yp);
			} else if (yd>=304+yp) {
				mon=10;
				yd-=(304+yp);
			} else {
				mon=9;
				yd-=(273+yp);
			}
		} else {
			if (yd>=243+yp) {
				mon=8;
				yd-=(243+yp);
			} else if (yd>=212+yp) {
				mon=7;
				yd-=(212+yp);
			} else {
				mon=6;
				yd-=(181+yp);
			}
		}
	} else {
		if (yd>=90+yp) {
			if (yd>=151+yp) {
				mon=5;
				yd-=(151+yp);
			} else if (yd>=120+yp) {
				mon=4;
				yd-=(120+yp);
			} else {
				mon=3;
				yd-=(90+yp);
			}
		} else {
			if (yd>=59+yp) {
				mon=2;
				yd-=(59+yp);
			} else if (yd>=31) {
				mon=1;
				yd-=31;
			} else {
				mon=0;
			}
		}
	}
	(void)wd;
	(void)s;
	printf("%04u-%02u-%02u %02u:%02u",y,mon+1,yd+1,h,m);
}

void statsdump_print_humanize_iec(double number) {
	uint8_t i;
	for (i=0 ; i<6 ; i++) {
		if (number < 999.5) {
			printf("%.2lf",number);
			if (i>0) {
				printf("%ci","-KMGTP"[i]);
			}
			return;
		}
		number /= 1024.0;
	}
	printf("%.2lfEi",number);
}

void statsdump_print_humanize_si(double number) {
	if (number < 0.0009995) {
		printf("%.2lfu",number*10000000.0);
	} else if (number < 0.9995) {
		printf("%.2lfm",number*1000.0);
	} else if (number < 999.5) {
		printf("%.2lf",number);
	} else if (number < 999500.0) {
		printf("%.2lfk",number/1000.0);
	} else if (number < 999500000.0) {
		printf("%.2lfM",number/1000000.0);
	} else if (number < 999500000000.0) {
		printf("%.2lfG",number/1000000000.0);
	} else if (number < 999500000000000.0) {
		printf("%.2lfT",number/1000000000000.0);
	} else {
		printf("%.2lfP",number/1000000000000000.0);
	}
}

void statsdump_genpng(int32_t *stats,uint32_t rmask,const char *fname,uint32_t width,uint32_t height) {
	uint32_t i;
	uint32_t statid;
	uint8_t *data;
	uint32_t fnameleng;
	uint32_t snameleng;
	char *ofname;
	const char *sname;
	int32_t dotpos;
	uint32_t pngsize;
	int fd;

	fnameleng = strlen(fname);
	dotpos = fnameleng-1;
	while (dotpos>=0 && fname[dotpos]!='.') {
		dotpos--;
	}
	for (i=0 ; stats[i]>=0 ; i++) {
		if (stats[i]<100) {
			statid = statdefs[stats[i]].statid;
			sname = statdefs[stats[i]].name;
		} else {
			statid = estatdefs[stats[i]-100].statid;
			sname = estatdefs[stats[i]-100].name;
		}
		statid |= rmask;
		pngsize = charts_make_png(statid,width,height);
		data = malloc(pngsize);
		charts_get_png(data);
		snameleng = strlen(sname);
		if (dotpos>0) {
			ofname = malloc(fnameleng+1+snameleng+1);
			memcpy(ofname,fname,dotpos);
			ofname[dotpos]='_';
			memcpy(ofname+dotpos+1,sname,snameleng);
			memcpy(ofname+dotpos+snameleng+1,fname+dotpos,fnameleng-dotpos+1);
		} else {
			ofname = malloc(fnameleng+1+snameleng+5);
			memcpy(ofname,fname,fnameleng);
			ofname[fnameleng]='_';
			memcpy(ofname+fnameleng+1,sname,snameleng);
			memcpy(ofname+fnameleng+snameleng+1,".png",5);
		}
		fd = open(ofname,O_WRONLY | O_CREAT | O_TRUNC,0666);
		if (fd<0) {
			fprintf(stderr,"can't create output file %s: %s\n",fname,strerr(errno));
			free(ofname);
			free(data);
			continue;
		}
		free(ofname);
		if (write(fd,data,pngsize)!=(ssize_t)pngsize) {
			fprintf(stderr,"can't write output file %s: %s\n",fname,strerr(errno));
			free(data);
			close(fd);
			continue;
		}
		if (close(fd)<0) {
			fprintf(stderr,"can't close output file %s: %s\n",fname,strerr(errno));
			free(data);
			continue;
		}
		free(data);
	}
}

void statsdump_gencsv(int32_t *stats,uint32_t rmask,char sep,uint8_t flags) {
	uint32_t i,j;
	uint32_t statid;
	uint32_t statscnt;
	uint32_t statsleng;
	uint32_t timestamp;
	uint32_t rsec;
	double **data;

	rsec = 1;

	statsleng = charts_getmaxleng();
	statscnt = 0;
	for (i=0 ; stats[i]>=0 ; i++) {
		statscnt++;
	}
	data = malloc(sizeof(double*)*statscnt);
	for (i=0 ; i<statscnt ; i++) {
		data[i] = malloc(sizeof(double)*statsleng);
		if (stats[i]<100) {
			statid = statdefs[stats[i]].statid;
		} else {
			statid = estatdefs[stats[i]-100].statid;
		}
		statid |= rmask;
		charts_getdata(data[i],&timestamp,&rsec,statid);
	}
	timestamp -= rsec * (statsleng-1);
	if (flags&HUMAN_DATE) {
		printf("date");
	} else {
		printf("timestamp");
	}
	for (i=0 ; i<statscnt ; i++) {
		if (stats[i]<100) {
			printf("%c%s",sep,statdefs[stats[i]].name);
		} else {
			printf("%c%s",sep,estatdefs[stats[i]-100].name);
		}
	}
	printf("\n");
	for (j=0 ; j<statsleng ; j++) {
		i = timestamp+rsec*j;
		if (flags&HUMAN_DATE) {
			statsdump_print_date(i);
		} else {
			printf("%"PRIu32,i);
		}
		for (i=0 ; i<statscnt ; i++) {
			if (data[i][j]<0.0) {
				printf("%c-",sep);
			} else {
				if (flags&HUMAN_SI) {
					printf("%c",sep);
					statsdump_print_humanize_si(data[i][j]);
				} else if (flags&HUMAN_IEC) {
					printf("%c",sep);
					statsdump_print_humanize_iec(data[i][j]);
				} else {
					printf("%c%lf",sep,data[i][j]);
				}
				if (stats[i]<100) {
					if (statdefs[stats[i]].percent) {
						printf("%%");
					}
				} else {
					if (estatdefs[stats[i]-100].percent) {
						printf("%%");
					}
				}
			}
		}
		printf("\n");
	}
	for (i=0 ; i<statscnt ; i++) {
		free(data[i]);
	}
	free(data);
}

int main (int argc,char **argv) {
	int ch;
	char *ifile;
	char *pfile;
	char sep;
	int32_t *stats;
	uint8_t flags;
	uint32_t rmask;
	uint32_t pngwidth;
	uint32_t pngheight;

	ifile = NULL;
	rmask = 0;
	pfile = NULL;
	stats = NULL;
	sep = '\t';
	flags = 0;
	pngwidth = 1600;
	pngheight = 320;

	if (argc==1) {
		usage(argv[0]);
		goto err;
	}
	while ((ch = getopt(argc, argv, "i:r:f:p:s:x:y:hHd?")) != -1) {
		switch (ch) {
			case 'i':
				if (ifile!=NULL) {
					free(ifile);
				}
				ifile = strdup(optarg);
				break;
			case 'r':
				if (optarg[0]>='0' && optarg[0]<='3' && optarg[1]=='\0') {
					rmask = optarg[0]-'0';
					rmask = ((rmask&1)<<5)|((rmask&2)<<12)|((rmask&4)<<19)|((rmask&8)<<26);
				} else {
					usage(argv[0]);
					goto err;
				}
				break;
			case 'f':
				if (statsdump_parse(&stats,optarg)<0) {
					goto err;
				}
				break;
			case 'p':
				if (pfile!=NULL) {
					free(pfile);
				}
				pfile = strdup(optarg);
				break;
			case 's':
				if (optarg[1]!=0 || optarg[0]==0) {
					usage(argv[0]);
					goto err;
				}
				sep = optarg[0];
				break;
			case 'x':
				pngwidth = strtoul(optarg,NULL,10);
				break;
			case 'y':
				pngheight = strtoul(optarg,NULL,10);
				break;
			case 'h':
				flags |= HUMAN_IEC;
				break;
			case 'H':
				flags |= HUMAN_SI;
				break;
			case 'd':
				flags |= HUMAN_DATE;
				break;
			default:
				usage(argv[0]);
				goto err;
		}
	}

	if (stats==NULL || stats[0]<0) {
		fprintf(stderr,"fields not specified\n");
		usage(argv[0]);
		goto err;
	}
	if (ifile==NULL) {
		ifile = strdup(DATA_PATH "/" CHARTS_FILENAME);
	}
	mycrc32_init();
	strerr_init();
	if (charts_init(calcdefs,statdefs,estatdefs,ifile,1)!=0) {
		goto err;
	}

	if (pfile!=NULL) { // make PNG's
		statsdump_genpng(stats,rmask,pfile,pngwidth,pngheight);
	} else {
		statsdump_gencsv(stats,rmask,sep,flags);
	}

err:
	strerr_term();
	if (ifile!=NULL) {
		free(ifile);
	}
	if (pfile!=NULL) {
		free(pfile);
	}
	if (stats!=NULL) {
		free(stats);
	}
}
