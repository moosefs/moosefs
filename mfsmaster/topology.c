/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>

#include "itree.h"

#include "main.h"
#include "cfg.h"
#include "slogger.h"
#include "massert.h"

static void *racktree;
static char *TopologyFileName;

/* hash is much faster than itree, but it is hard to define ip classes in hash tab

#define HASHSIZE 199

static uint32_t hashtab_ip[HASHSIZE];
static uint32_t hashtab_rid[HASHSIZE];

void hash_clear(void) {
	memset(hashtab_ip,0,sizeof(uint32_t)*HASHSIZE);
	memset(hashtab_rid,0,sizeof(uint32_t)*HASHSIZE);
}

void hash_insert(uint32_t ip,uint32_t rid) {
	uint32_t hash = ip%HASHSIZE;
	uint32_t disp = (ip%(HASHSIZE-2))+1;
	while (hashtab_ip[hash]!=0) {
		if (hashtab_ip[hash]==ip) {
			hashtab_rid[hash]=rid;
			return;
		}
		hash += disp;
		hash %= HASHSIZE;
	}
	hashtab_ip[hash] = ip;
	hashtab_rid[hash] = rid;
}

uint32_t hash_find(uint32_t ip) {
	uint32_t hash = ip%HASHSIZE;
	uint32_t disp = (ip%(HASHSIZE-2))+1;
	while (hashtab_ip[hash]!=0) {
		if (hashtab_ip[hash]==ip) {
			return hashtab_rid[hash];
		}
		hash += disp;
		hash %= HASHSIZE;
	}
	return 0;
}
*/

int topology_parsenet(char *net,uint32_t *fromip,uint32_t *toip) {
	uint32_t ip,i,octet;
	if (net[0]=='*' && net[1]==0) {
		*fromip = 0;
		*toip = 0xFFFFFFFFU;
		return 0;
	}
	ip=0;
	for (i=0 ; i<4; i++) {
		if (*net>='0' && *net<='9') {
			octet=0;
			while (*net>='0' && *net<='9') {
				octet*=10;
				octet+=(*net)-'0';
				net++;
				if (octet>255) {
					return -1;
				}
			}
		} else {
			return -1;
		}
		if (i<3) {
			if (*net!='.') {
				return -1;
			}
			net++;
		}
		ip*=256;
		ip+=octet;
	}
	if (*net==0) {
		*fromip = ip;
		*toip = ip;
		return 0;
	}
	if (*net=='/') {	// ip/bits and ip/mask
		*fromip = ip;
		ip=0;
		net++;
		for (i=0 ; i<4; i++) {
			if (*net>='0' && *net<='9') {
				octet=0;
				while (*net>='0' && *net<='9') {
					octet*=10;
					octet+=(*net)-'0';
					net++;
					if (octet>255) {
						return -1;
					}
				}
			} else {
				return -1;
			}
			if (i==0 && *net==0 && octet<=32) {	// bits -> convert to mask and skip rest of loop
				ip = 0xFFFFFFFF;
				if (octet<32) {
					ip<<=32-octet;
				}
				break;
			}
			if (i<3) {
				if (*net!='.') {
					return -1;
				}
				net++;
			}
			ip*=256;
			ip+=octet;
		}
		if (*net!=0) {
			return -1;
		}
		*fromip &= ip;
		*toip = *fromip | (ip ^ 0xFFFFFFFFU);
		return 0;
	}
	if (*net=='-') {	// ip1-ip2
		*fromip = ip;
		ip=0;
		net++;
		for (i=0 ; i<4; i++) {
			if (*net>='0' && *net<='9') {
				octet=0;
				while (*net>='0' && *net<='9') {
					octet*=10;
					octet+=*net-'0';
					net++;
					if (octet>255) {
						return -1;
					}
				}
			} else {
				return -1;
			}
			if (i<3) {
				if (*net!='.') {
					return -1;
				}
				net++;
			}
			ip*=256;
			ip+=octet;
		}
		if (*net!=0) {
			return -1;
		}
		*toip = ip;
		return 0;
	}
	return -1;
}

// as for now:
//
// 0 - same machine
// 1 - same rack, different machines
// 2 - different racks

uint8_t topology_distance(uint32_t ip1,uint32_t ip2) {
	uint32_t rid1,rid2;
	if (ip1==ip2) {
		return 0;
	}
//	rid1 = hash_find(ip1);
//	rid2 = hash_find(ip2);
	rid1 = itree_find(racktree,ip1);
	rid2 = itree_find(racktree,ip2);
//	rid1^=rid2;
//	rid2=0;
//	while(rid1) {
//		if (rid1&1) {
//			rid2++;
//		}
//		rid1>>=1;
//	}
//	return rid2;
	return (rid1==rid2)?1:2;
}

// format:
// network	rackid

/*
idea for the future:

distance(id1,id2) = number of one bits in id1^id2

network topology examples with rack ids

1. chain

A--B--C--D--E

A: 1111
B: 1110
C: 1100
D: 1000
E: 0000

2. star

      A
    B | C
     \|/
   D--*--E

A: 00001
B: 00010
C: 00100
D: 01000
E: 10000

3. two chains

  A--B--C--D--E--F
        |
  G--H--I--J--K--L--M--N

A: 1111111111001
B: 1111011111001
C: 1110011111001
D: 1100011111001
E: 1000011111001
F: 0000011111001
G: 1110011111110
H: 1110011111100
I: 1110011111000
J: 1110011110000
K: 1110011100000
L: 1110011000000
M: 1110010000000
N: 1110000000000

4. star and chain

         A
       B | C
        \|/
      D--*--E
        /|
       F |
         |
   G--H--I--J--K--L--M--N

A: 00000111111001
B: 00001011111001
C: 00010011111001
D: 00100011111001
E: 01000011111001
F: 10000011111001
G: 00000011111110
H: 00000011111100
I: 00000011111000
J: 00000011110000
K: 00000011100000
L: 00000011000000
M: 00000010000000
N: 00000000000000

5. two stars

         A
       B | C
        \|/
      D--*--E
        /|
       F |
         |
       G | H
        \|/
      I--*--J
        /|\
       K L M

A: 00000100000001
B: 00001000000001
C: 00010000000001
D: 00100000000001
E: 01000000000001
F: 10000000000001
G: 00000000000010
H: 00000000000100
I: 00000000001000
J: 00000000010000
K: 00000000100000
L: 00000001000000
M: 00000010000000

6. two stars with poor link

         A
       B | C
        \|/
      D--*--E
        /.
       F .
         .
       G . H
        \./
      I--*--J
        /|\
       K L M

A: 00000100000001111111
B: 00001000000001111111
C: 00010000000001111111
D: 00100000000001111111
E: 01000000000001111111
F: 10000000000001111111
G: 00000000000010000000
H: 00000000000100000000
I: 00000000001000000000
J: 00000000010000000000
K: 00000000100000000000
L: 00000001000000000000
M: 00000010000000000000
*/

int topology_parseline(char *line,uint32_t lineno,uint32_t *fip,uint32_t *tip,uint32_t *rid) {
	char *net;
	char *p;

	p = line;
	while (*p==' ' || *p=='\t') {
		p++;
	}
	if (*p==0 || *p=='#') { // empty line or line with comment only
		return -1;
	}
	net = p;
	while (*p && *p!=' ' && *p!='\t') {
		p++;
	}
	if (*p==0) {
		mfs_arg_syslog(LOG_WARNING,"mfstopology: incomplete definition in line: %"PRIu32,lineno);
		fprintf(stderr,"mfstopology: incomplete definition in line: %"PRIu32"\n",lineno);
		return -1;
	}
	*p=0;
	p++;
	if (topology_parsenet(net,fip,tip)<0) {
		mfs_arg_syslog(LOG_WARNING,"mfstopology: incorrect ip/network definition in line: %"PRIu32,lineno);
		fprintf(stderr,"mfstopology: incorrect ip/network definition in line: %"PRIu32"\n",lineno);
		return -1;
	}

	while (*p==' ' || *p=='\t') {
		p++;
	}

	if (*p<'0' || *p>'9') {
		mfs_arg_syslog(LOG_WARNING,"mfstopology: incorrect rack id in line: %"PRIu32,lineno);
		fprintf(stderr,"mfstopology: incorrect rack id in line: %"PRIu32"\n",lineno);
		return -1;
	}

	*rid = strtoul(p,&p,10);

	while (*p==' ' || *p=='\t') {
		p++;
	}

	if (*p && *p!='#') {
		mfs_arg_syslog(LOG_WARNING,"mfstopology: garbage found at the end of line: %"PRIu32,lineno);
		fprintf(stderr,"mfstopology: garbage found at the end of line: %"PRIu32"\n",lineno);
		return -1;
	}
	return 0;
}

void topology_load(void) {
	FILE *fd;
	char linebuff[10000];
	uint32_t s,lineno;
	uint32_t fip,tip,rid;
	void *newtree;

	fd = fopen(TopologyFileName,"r");
	if (fd==NULL) {
		if (errno==ENOENT) {
			if (racktree) {
				syslog(LOG_WARNING,"mfstopology configuration file (%s) not found - network topology not changed",TopologyFileName);
			} else {
				syslog(LOG_WARNING,"mfstopology configuration file (%s) not found - network topology not defined",TopologyFileName);
			}
			fprintf(stderr,"mfstopology configuration file (%s) not found - using defaults\n",TopologyFileName);
		} else {
			if (racktree) {
				mfs_arg_errlog(LOG_WARNING,"can't open mfstopology configuration file (%s) - network topology not changed, error",TopologyFileName);
			} else {
				mfs_arg_errlog(LOG_WARNING,"can't open mfstopology configuration file (%s) - network topology not defined, error",TopologyFileName);
			}
		}
		return;
	}

//	hash_clear();
	newtree = NULL;
	lineno = 1;
	while (fgets(linebuff,10000,fd)) {
		linebuff[9999]=0;
		s=strlen(linebuff);
		while (s>0 && (linebuff[s-1]=='\r' || linebuff[s-1]=='\n' || linebuff[s-1]=='\t' || linebuff[s-1]==' ')) {
			s--;
		}
		if (s>0) {
			linebuff[s]=0;
			if (topology_parseline(linebuff,lineno,&fip,&tip,&rid)>=0) {
				newtree = itree_add_interval(newtree,fip,tip,rid);
//				while (fip<=tip) {
//					hash_insert(fip,rid);
//					fip++;
//				}
			}
		}
		lineno++;
	}
	if (ferror(fd)) {
		fclose(fd);
		if (racktree) {
			syslog(LOG_WARNING,"error reading mfstopology file - network topology not changed");
		} else {
			syslog(LOG_WARNING,"error reading mfstopology file - network topology not defined");
		}
		itree_freeall(newtree);
		fprintf(stderr,"error reading mfstopology file - network topology not defined (using defaults)\n");
		return;
	}
	fclose(fd);
	itree_freeall(racktree);
	racktree = newtree;
	if (racktree) {
		racktree = itree_rebalance(racktree);
	}
	mfs_syslog(LOG_NOTICE,"topology file has been loaded");
}

//int topology_init(void) {
//	TopologyFileName = strdup("mfstopology.cfg");
//	racktree = NULL;
//	topology_load();
//	itree_show(racktree);
//	return 0;
//}

void topology_reload(void) {
	int fd;
	if (TopologyFileName) {
		free(TopologyFileName);
	}
	if (!cfg_isdefined("TOPOLOGY_FILENAME")) {
		TopologyFileName = strdup(ETC_PATH "/mfs/mfstopology.cfg");
		passert(TopologyFileName);
		if ((fd = open(TopologyFileName,O_RDONLY))<0 && errno==ENOENT) {
			free(TopologyFileName);
			TopologyFileName = strdup(ETC_PATH "/mfstopology.cfg");
			if ((fd = open(TopologyFileName,O_RDONLY))>=0) {
				mfs_syslog(LOG_WARNING,"default sysconf path has changed - please move mfstopology.cfg from "ETC_PATH"/ to "ETC_PATH"/mfs/");
			}
		}
		if (fd>=0) {
			close(fd);
		}
	} else {
		TopologyFileName = cfg_getstr("TOPOLOGY_FILENAME",ETC_PATH "/mfs/mfstopology.cfg");
	}
	topology_load();
}

void topology_term(void) {
	itree_freeall(racktree);
	if (TopologyFileName) {
		free(TopologyFileName);
	}
}

int topology_init(void) {
	TopologyFileName = NULL;
	racktree = NULL;
	topology_reload();
	main_reload_register(topology_reload);
	main_destruct_register(topology_term);
	return 0;
}
