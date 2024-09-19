#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <inttypes.h>

#include "timeparser.h"

int snprint_time_common(char *where,size_t mleng,uint32_t period,uint32_t *timetab,char *timesym,uint32_t timecount) {
	uint64_t n,p10;
	uint32_t timebase;
	uint32_t i;
	uint32_t intpart,decpart;
	uint32_t resultlen;

	resultlen = 0;
	if (mleng<=0) {
		return resultlen;
	}
	if (period==0) {
		return snprintf(where,mleng,"0%c",timesym[timecount-1]);
	}
	for (i=0 ; i<timecount ; i++) {
		if (period >= timetab[i]) {
			timebase = timetab[i];
			n = period;
			n *= 10;
			p10 = n;
			if (timebase > 1) {
				n += timebase / 2;
				n /= timebase;
			}
			if (n * timebase != p10) {
				where[0] = '~';
				where++;
				mleng--;
				resultlen = 1;
				if (mleng==0) {
					return resultlen;
				}
			}
			intpart = n/10;
			decpart = n%10;
			if (decpart) {
				resultlen += snprintf(where,mleng,"%u.%u%c",intpart,decpart,timesym[i]);
			} else {
				resultlen += snprintf(where,mleng,"%u%c",intpart,timesym[i]);
			}
			return resultlen;
		}
	}
	// assert (resultlen==0)
	if (resultlen<mleng) {
		where[resultlen]=0;
	}
	return resultlen;
}

/* formats (seconds mode):
 * #
 * #.#[s,m,h,d,w]
 * [#w][#d][#h][#m][#s]
 */

/* formats (hours mode):
 * #
 * #.#[h,d,w]
 * [#w][#d][#h]
 */

int parse_period_common(const char *str,uint32_t *ret,uint8_t hmode) {
	uint64_t res,cur,ndiv,max;
	uint32_t mul;
	uint8_t mask;
	uint8_t cmask;

	mask = 0;
	res = 0;
	while ((*str)==' ' || (*str)=='\t') {
		str++;
	}
	while (1) {
		if ((*str)>='0' && (*str<='9')) {
			if ((mask&0xC0)==0xC0) {
				*ret = *str;
				return TPARSE_UNEXPECTED_CHAR;
			}
			cur = 0;
			ndiv = 1;
			while ((*str)>='0' && (*str)<='9' && cur<UINT32_MAX) {
				cur *= 10.0;
				cur += (*str)-'0';
				ndiv *= 10;
				str++;
			}
			if (cur > UINT32_MAX) {
				*ret = 0;
				return TPARSE_VALUE_TOO_BIG;
			}
			while ((*str)==' ' || (*str)=='\t') {
				str++;
			}
		} else {
			*ret = *str;
			return TPARSE_UNEXPECTED_CHAR;
		}
		switch (*str) {
			case '.':
				if (mask!=0) {
					*ret = *str;
					return TPARSE_UNEXPECTED_CHAR;
				}
				mask |= 0x80;
				res = cur;
				str++;
				cmask = 0x80;
				break;
			case 'w':
			case 'W':
				if (hmode) {
					mul = 7*24;
				} else {
					mul = 7*24*3600;
				}
				max = UINT32_MAX;
				cmask = 0x10;
				break;
			case 'd':
			case 'D':
				if (hmode) {
					mul = 24;
					max = 7*24;
				} else {
					mul = 24*3600;
					max = 7*24*3600;
				}
				cmask = 0x08;
				break;
			case 'h':
			case 'H':
				if (hmode) {
					mul = 1;
					max = 24;
				} else {
					mul = 3600;
					max = 24*3600;
				}
				cmask = 0x04;
				break;
			case 'm':
			case 'M':
				if (hmode) {
					*ret = *str;
					return TPARSE_UNEXPECTED_CHAR;
				}
				mul = 60;
				max = 3600;
				cmask = 0x02;
				break;
			case 's':
			case 'S':
				if (hmode) {
					*ret = *str;
					return TPARSE_UNEXPECTED_CHAR;
				}
				mul = 1;
				max = 60;
				cmask = 0x01;
				break;
			case '\0':
				if (mask==0) {
					*ret = cur;
					return TPARSE_OK;
				}
				*ret = 0;
				return TPARSE_UNEXPECTED_CHAR;
			default:
				*ret = *str;
				return TPARSE_UNEXPECTED_CHAR;
		}
		if (cmask!=0x80) {
			cur *= mul;
			if (mask==0x80) {
				res *= mul;
				cur = (cur * 10) / ndiv;
				cur = (cur/10) + (((cur%10)>=5)?1:0);
				res += cur;
				mask |= 0x40;
				str++;
			} else if (mask==0) {
				mask |= cmask;
				res = cur;
				str++;
			} else if ((mask & (0xC0 | ((cmask<<1)-1)))==0) {
				if (cur < max) {
					mask |= cmask;
					res += cur;
					str++;
				} else {
					*ret = *str;
					return TPARSE_VALUE_TOO_BIG;
				}
			} else {
				*ret = *str;
				return TPARSE_UNEXPECTED_CHAR;
			}
			if (res > UINT32_MAX) {
				*ret = 0;
				return TPARSE_VALUE_TOO_BIG;
			}
			while ((*str)==' ' || (*str)=='\t') {
				str++;
			}
			if (*str=='\0' || *str=='\r' || *str=='\n') {
				*ret = res;
				return TPARSE_OK;
			}
		}
	}
}

int snprint_speriod(char *where,size_t mleng,uint32_t period) {
	static uint32_t timetab[5] = {604800,86400,3600,60,1};
	static char timesym[5] = {'w','d','h','m','s'};
	return snprint_time_common(where,mleng,period,timetab,timesym,5);
}

int parse_speriod(const char *str,uint32_t *ret) {
	return parse_period_common(str,ret,0);
}

int snprint_hperiod(char *where,size_t mleng,uint32_t period) { //show period as days or hours; returns count of characters
	static uint32_t timetab[3] = {168,24,1};
	static char timesym[3] = {'w','d','h'};
	return snprint_time_common(where,mleng,period,timetab,timesym,3);
}

int parse_hperiod(const char *str,uint32_t *ret) {
	return parse_period_common(str,ret,1);
}

#if 0
// test

int main(int argc,char *argv[]) {
	uint32_t tval;
	char buff[20];
	int (*parse)(const char*,uint32_t*);
	int (*print)(char*,size_t,uint32_t);

	if (argc<3 || (argv[1][0]!='S' && argv[1][0]!='H')) {
		fprintf(stderr,"usage: %s [S|H] period ...\n",argv[0]);
		return 1;
	}
	if (argv[1][0]=='S') {
		parse = parse_speriod;
		print = snprint_speriod;
	} else {
		parse = parse_hperiod;
		print = snprint_hperiod;
	}
	argv+=2;
	argc-=2;
	while (argc>0) {
		switch (parse(argv[0],&tval)) {
			case TPARSE_OK:
				print(buff,20,tval);
				buff[19]='\0';
				printf("str '%s' -> value: %"PRIu32" -> str '%s'\n",argv[0],tval,buff);
				break;
			case TPARSE_UNEXPECTED_CHAR:
				if (tval) {
					printf("unexpected char '%c' in '%s'\n",(char)tval,argv[0]);
				} else {
					printf("unexpected end in '%s'\n",argv[0]);
				}
				break;
			case TPARSE_VALUE_TOO_BIG:
				if (tval) {
					printf("value too big in section '%c' in '%s'\n",(char)tval,argv[0]);
				} else {
					printf("value too big ('%s')\n",argv[0]);
				}
		}
		argc--;
		argv++;
	}
}
#endif
