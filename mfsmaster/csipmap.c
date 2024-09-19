#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

// #define TEST 1

#ifdef TEST
#define mfs_log(x,y,format,...) fprintf(stderr,format "\n",__VA_ARGS__)
#else
#include "mfslog.h"
#endif

#define MAX_SECTIONS 10

#define LSTATE_START 0
#define LSTATE_CLASS 1
#define LSTATE_MAP 2

#define HASHBITS 10
#define HASHSIZE (1<<(HASHBITS))
#define HASHMASK ((HASHSIZE)-1)

typedef struct _ipclass {
	uint32_t fromip;
	uint32_t toip;
	uint8_t section;
	struct _ipclass *next;
} ipclass;

static ipclass *ipclass_head;
static ipclass *load_head;
static uint8_t current_section;
static uint8_t lstate;

typedef struct _ipmap {
	uint32_t srcip;
	uint32_t dstip;
	uint8_t section;
	struct _ipmap *next;
} ipmap;

static ipmap *ipmap_hashtab[HASHSIZE];
static ipmap *load_hashtab[HASHSIZE];

static inline uint32_t csipmap_hash(uint32_t ip,uint8_t section) {
	return (ip ^ ((ip >> HASHBITS) * 17U) ^ (section * 33U)) & HASHMASK;
}

uint32_t csipmap_map(uint32_t servip,uint32_t clientip) {
	ipclass *ipcp;
	ipmap *ipmp;
	uint32_t hash;

	for (ipcp = ipclass_head ; ipcp!=NULL ; ipcp = ipcp->next) {
		if ((clientip >= ipcp->fromip) && (clientip <= ipcp->toip)) {
			break;
		}
	}
	if (ipcp==NULL) {
		return 0;
	}
	hash = csipmap_hash(servip,ipcp->section);
	for (ipmp = ipmap_hashtab[hash] ; ipmp!=NULL ; ipmp = ipmp->next) {
		if (ipmp->srcip == servip && ipmp->section==ipcp->section) {
			return ipmp->dstip;
		}
	}
	return 0;
}

static void csipmap_newrange(uint32_t fromip,uint32_t toip) {
	ipclass *ipcp;

	if (lstate==LSTATE_MAP) {
		current_section++;
	}
	lstate = LSTATE_CLASS;
	ipcp = malloc(sizeof(ipclass));
	ipcp->fromip = fromip;
	ipcp->toip = toip;
	ipcp->section = current_section;
	ipcp->next = load_head;
	load_head = ipcp;
}

static void csipmap_newclass(uint32_t ip,uint32_t mask) {
	csipmap_newrange(ip & mask,ip | ~mask);
}

static void csipmap_newmap(uint32_t src,uint32_t dst) {
	ipmap *ipmp;
	uint32_t hash;

	lstate = LSTATE_MAP;
	hash = csipmap_hash(src,current_section);
	for (ipmp = load_hashtab[hash] ; ipmp!=NULL ; ipmp = ipmp->next) {
		if (ipmp->srcip == src && ipmp->section==current_section) {
			// repeated source IP
			ipmp->dstip = dst;
			return;
		}
	}
	ipmp = malloc(sizeof(ipmap));
	ipmp->srcip = src;
	ipmp->dstip = dst;
	ipmp->section = current_section;
	ipmp->next = load_hashtab[hash];
	load_hashtab[hash] = ipmp;
}

// IP/MASK - CLASS
// IP/BITS - CLASS
//
// IP-IP - RANGE
//
// IP:IP - MAP

#define PARSE_OK 0
#define OCTET_TOOBIG 1
#define TOO_MANY_OCTESTS 2
#define BITS_TOOBIG 3
#define WRONG_MASK 4
#define BAD_ORDER 5
#define BAD_SECTION_ORDER 6
#define PARSE_ERROR 7

static uint8_t csipmap_parseline(const char *str) {
	uint32_t fip;
	char c;
	uint32_t octet,ip,octcnt,state;

	octet = 0;
	octcnt = 0;
	state = 0;
	ip = 0;
	fip = 0;
	while ((c = *(str++))!=0) {
		if (c==' ' || c=='\t' || c=='\r' || c=='\n') {
			continue;
		}
		if (c>='0' && c<='9') {
			octet *= 10;
			octet += c-'0';
			if (octet>255) {
				return OCTET_TOOBIG;
			}
			continue;
		}
		if (c=='#') {
			break;
		}
		if (c=='.') {
			octcnt++;
			if (octcnt>=4) {
				return TOO_MANY_OCTESTS;
			}
			ip *= 256;
			ip += octet;
			octet = 0;
		} else if (state==0) {
			if (c=='/') { // class - accept less than 4 octets here !!!
				while (octcnt<4) {
					ip *= 256;
					ip += octet;
					octet = 0;
					octcnt++;
				}
				state=1;
				fip = ip;
				ip = 0;
				octcnt = 0;
			} else if (c=='-' && octcnt==3) { // range
				ip *= 256;
				ip += octet;
				octet = 0;
				state=2;
				fip = ip;
				ip = 0;
				octcnt = 0;
			} else if (c==':' && octcnt==3) { // map
				ip *= 256;
				ip += octet;
				octet = 0;
				state=3;
				fip = ip;
				ip = 0;
				octcnt = 0;
			} else {
				return PARSE_ERROR;
			}
		} else {
			return PARSE_ERROR;
		}
	}
	if (state==0 && octet==0 && ip==0 && octcnt==0) { // empty line
		return PARSE_OK;
	}
	if (state==0) {
		if (octcnt==3) {
			ip *= 256;
			ip += octet;
			csipmap_newrange(ip,ip);
		} else {
			return PARSE_ERROR;
		}
	} else if (state==1) { // class
		if (octcnt==0) { // fip/octet
			if (octet>32) {
				return BITS_TOOBIG;
			}
			ip = UINT32_C(0xFFFFFFFF) << (32 - octet);
			csipmap_newclass(fip,ip);
		} else if (octcnt==3) { // fip/ip
			ip *= 256;
			ip += octet;
			csipmap_newclass(fip,ip);
			while (ip & 0x80000000) {
				ip <<= 1;
			}
			if (ip!=0) {
				return WRONG_MASK;
			}
		} else {
			return PARSE_ERROR;
		}
	} else if (state==2) { // range
		if (octcnt==3) { // fip-ip
			ip *= 256;
			ip += octet;
			if (ip<fip) {
				return BAD_ORDER;
			}
			csipmap_newrange(fip,ip);
		} else {
			return PARSE_ERROR;
		}
	} else if (state==3) {
		if (octcnt==3) { // fip-ip
			ip *= 256;
			ip += octet;
			if (lstate==LSTATE_START) {
				return BAD_SECTION_ORDER;
			}
			csipmap_newmap(fip,ip);
		} else {
			return PARSE_ERROR;
		}
	} else {
		return PARSE_ERROR;
	}
	return PARSE_OK;
}

static void csipmap_cleanup(uint8_t load_flag) {
	ipclass *ipcp,*ipcpn;
	ipmap *ipmp,*ipmpn;
	uint32_t hash;

	ipcp = load_flag?load_head:ipclass_head;
	while (ipcp!=NULL) {
		ipcpn = ipcp->next;
		free(ipcp);
		ipcp = ipcpn;
	}
	if (load_flag) {
		load_head = NULL;
	} else {
		ipclass_head = NULL;
	}
	for (hash = 0 ; hash < HASHSIZE ; hash++) {
		ipmp = (load_flag?load_hashtab:ipmap_hashtab)[hash];
		while (ipmp!=NULL) {
			ipmpn = ipmp->next;
			free(ipmp);
			ipmp = ipmpn;
		}
		(load_flag?load_hashtab:ipmap_hashtab)[hash] = NULL;
	}
}

static void csipmap_use_loaded_map(void) {
	uint32_t hash;

	ipclass_head = load_head;
	load_head = NULL;

	for (hash = 0 ; hash < HASHSIZE ; hash++) {
		ipmap_hashtab[hash] = load_hashtab[hash];
		load_hashtab[hash] = NULL;
	}
}

void csipmap_loadmap(const char *fname) {
	FILE *fd;
	char *linebuff;
	size_t lbsize;
	uint8_t status;
	uint32_t lno;

	fd = fopen(fname,"r");
	if (fd==NULL) {
		if (errno==ENOENT) {
			if (ipclass_head!=NULL) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfscsipmap configuration file (%s) not found - chunkserver ip mappings not changed",fname);
			} else {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mfscsipmap configuration file (%s) not found - no chunkserver ip mappings",fname);
			}
		} else {
			if (ipclass_head!=NULL) {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"can't open mfscsipmap configuration file (%s) - chunkserver ip mappings not changed, error",fname);
			} else {
				mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"can't open mfscsipmap configuration file (%s) - no chunkserver ip mappings, error",fname);
			}
		}
		return;
	}
	current_section = 0;
	lstate = LSTATE_START;
	lbsize = 10000;
	lno = 1;
	linebuff = malloc(lbsize);
	while (getline(&linebuff,&lbsize,fd)!=-1) {
		status = csipmap_parseline(linebuff);
		switch (status) {
			case OCTET_TOOBIG:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip number in file %s (line %u) - octet too big",fname,lno);
				break;
			case TOO_MANY_OCTESTS:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip number in file %s (line %u) - too many octets",fname,lno);
				break;
			case BITS_TOOBIG:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class in file %s (line %u) - too many bits",fname,lno);
				break;
			case WRONG_MASK:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class in file %s (line %u) - wrong mask",fname,lno);
				break;
			case BAD_ORDER:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip range in file %s (line %u) - incorrect ip order",fname,lno);
				break;
			case BAD_SECTION_ORDER:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing line in file %s (line %u) - ip mapping without ip class or ip range",fname,lno);
				break;
			case PARSE_ERROR:
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing line in file %s (line %u)",fname,lno);
				break;
		}
		if (current_section>=MAX_SECTIONS) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing line in file %s (line %u) - too many sections in file",fname,lno);
			csipmap_cleanup(1);
			fclose(fd);
			return;
		}
		if (status!=PARSE_OK) {
			csipmap_cleanup(1);
			fclose(fd);
			return;
		}
		lno++;
	}
	fclose(fd);
	if (lstate==LSTATE_CLASS) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing line in file %s (end of file) - ip class / ip range without mappings",fname);
		csipmap_cleanup(1);
		return;
	}
	current_section++;
	csipmap_cleanup(0);
	csipmap_use_loaded_map();
	return;
}

void csipmap_term(void) {
	csipmap_cleanup(0);
}

int csipmap_init(void) {
	uint32_t hash;
	ipclass_head = NULL;
	load_head = NULL;
	for (hash=0 ; hash<HASHSIZE ; hash++) {
		ipmap_hashtab[hash] = NULL;
		load_hashtab[hash] = NULL;
	}
	return 0;
}

#ifdef TEST

uint32_t csipmap_parseip(const char *str) {
	uint32_t ip;
	uint32_t octet;
	uint8_t n;
	char p;

	ip = 0;
	octet = 0;
	n = 0;
	while ((p=*str)!=0) {
		str++;
		if (p>='0' && p<='9') {
			octet *= 10U;
			octet += p - '0';
		} else if (p=='.') {
			ip *= 256U;
			ip += octet;
			octet = 0;
			n++;
		} else if (p!=' ' && p!='\t') {
			return 0;
		}
		if (octet>=256) {
			return 0;
		}
	}
	if (n==3) {
		return ip*256U+octet;
	}
	return 0;
}

void csipmap_printip(uint32_t ip) {
	printf("%u.%u.%u.%u",(ip>>24)&0xFF,(ip>>16)&0xFF,(ip>>8)&0xFF,ip&0xFF);
}

void csipmap_dump(void) {
	ipclass *ipcp;
	ipmap *ipmp;
	uint32_t hash;
	uint8_t section;

	for (section=0 ; section < current_section ; section++) {

		if (section) {
			printf("-------------------\n");
		}
		printf("SECTION %u\n\n",section);

		for (ipcp = ipclass_head ; ipcp != NULL ; ipcp = ipcp->next) {
			if (ipcp->section==section) {
				printf("RANGE: ");
				csipmap_printip(ipcp->fromip);
				printf(" - ");
				csipmap_printip(ipcp->toip);
				printf("\n");
			}
		}

		for (hash = 0 ; hash < HASHSIZE ; hash++) {
			for (ipmp = ipmap_hashtab[hash] ; ipmp != NULL ; ipmp = ipmp->next) {
				if (ipmp->section==section) {
					printf("MAP[%u]: ",hash);
					csipmap_printip(ipmp->srcip);
					printf(" : ");
					csipmap_printip(ipmp->dstip);
					printf("\n");
				}
			}
		}
	}
}

int main(int argc,char *argv[]) {
	uint32_t cip,sip,mip;
	if (argc<2 || (argc&1)) {
		fprintf(stderr,"usage: %s filename [clientip serverip] ...\n",argv[0]);
		return 1;
	}
	csipmap_init();
	csipmap_loadmap(argv[1]);
	csipmap_dump();
	if (argc>2) {
		printf("-------------------\n");
	}
	while (argc>2) {
		cip = csipmap_parseip(argv[2]);
		sip = csipmap_parseip(argv[3]);
		argv+=2;
		argc-=2;
		mip = csipmap_map(sip,cip);
		printf("CLIENT IP: ");
		csipmap_printip(cip);
		printf(" : SERVER IP: ");
		csipmap_printip(sip);
		if (mip!=0) {
			printf(" : MAPPED IP: ");
			csipmap_printip(mip);
			printf("\n");
		} else {
			printf(" : NOT CHANGED\n");
		}
	}
	csipmap_term();
	return 0;
}
#endif
