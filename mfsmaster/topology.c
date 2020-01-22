/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
#include "mfsalloc.h"

static void *racktree;
static char *TopologyFileName;



// ************* NAME <-> ID MAP ** BEGIN ***************

#define HASHTABSIZE 4096

typedef struct _rackhashentry {
	char* rackname;
	uint32_t rackid;
	uint32_t hash;
	struct _rackhashentry *next;
} rackhashentry;

static rackhashentry* rackhashtab[HASHTABSIZE];
static rackhashentry** rackidtab = NULL;
static uint32_t rackidtabsize = 0;
static uint32_t rackidnext = 0;

static inline uint32_t topology_rackname_hash(char *rackname) {
	uint8_t p;
	uint32_t result = 55821;
	while ((p=*rackname)!=0) {
		rackname++;
		result = result*33+p;
	}
	return result;
}

static inline uint32_t topology_get_next_free_rackid(void) {
	uint32_t i;
	i = rackidtabsize;
	if (rackidtabsize==0) {
		rackidtabsize = 1024;
		rackidtab = malloc(sizeof(rackhashentry*)*rackidtabsize);
		passert(rackidtab);
		rackidnext = 1; // skip rackid=0
	} else if (rackidnext>=rackidtabsize) {
		rackidtabsize = rackidtabsize*3/2;
		rackidtab = mfsrealloc(rackidtab,sizeof(rackhashentry*)*rackidtabsize);
		passert(rackidtab);
	}
	while (i<rackidtabsize) {
		rackidtab[i] = NULL;
		i++;
	}
	return rackidnext++;
}

static uint32_t topology_rackname_to_rackid(char *rackname) {
	uint32_t hash,hashpos;
	rackhashentry *rhe;

	hash = topology_rackname_hash(rackname);
	hashpos = hash % HASHTABSIZE;
	for (rhe = rackhashtab[hashpos] ; rhe != NULL ; rhe = rhe->next) {
		if (rhe->hash==hash && strcmp(rhe->rackname,rackname)==0) {
			return rhe->rackid;
		}
	}

	rhe = malloc(sizeof(rackhashentry));
	rhe->rackname = strdup(rackname);
	rhe->rackid = topology_get_next_free_rackid();
	rhe->hash = hash;
	rhe->next = rackhashtab[hashpos];
	rackhashtab[hashpos] = rhe;
	rackidtab[rhe->rackid] = rhe;
	return rhe->rackid;
}

static char* topology_rackid_to_rackname(uint32_t rackid) {
	if (rackid==0) {
		return "";
	}
	if (rackid<rackidnext) {
		return rackidtab[rackid]->rackname;
	}
	return NULL;
}

static void topology_rackname_init(void) {
	uint32_t i;
	rackidtab = NULL;
	rackidtabsize = 0;
	rackidnext = 0;
	for (i=0 ; i<HASHTABSIZE ; i++) {
		rackhashtab[i] = NULL;
	}
}

static void topology_rackname_cleanup(void) {
	uint32_t i;
	for (i=1 ; i<rackidnext ; i++) {
		free(rackidtab[i]->rackname);
		free(rackidtab[i]);
	}
	free(rackidtab);
	topology_rackname_init();
}

static rackhashentry* rackhashtab_stash[HASHTABSIZE];
static rackhashentry** rackidtab_stash = NULL;
static uint32_t rackidtabsize_stash = 0;
static uint32_t rackidnext_stash = 0;


static void topology_rackname_stash(void) {
	uint32_t i;
	for (i=0 ; i<HASHTABSIZE ; i++) {
		rackhashtab_stash[i] = rackhashtab[i];
		rackhashtab[i] = NULL;
	}
	rackidtab_stash = rackidtab;
	rackidtab = NULL;
	rackidtabsize_stash = rackidtabsize;
	rackidtabsize = 0;
	rackidnext_stash = rackidnext;
	rackidnext = 0;
}

static void topology_rackname_restore(void) {
	topology_rackname_cleanup();
	uint32_t i;
	for (i=0 ; i<HASHTABSIZE ; i++) {
		rackhashtab[i] = rackhashtab_stash[i];
		rackhashtab_stash[i] = NULL;
	}
	rackidtab = rackidtab_stash;
	rackidtab_stash = NULL;
	rackidtabsize = rackidtabsize_stash;
	rackidtabsize_stash = 0;
	rackidnext = rackidnext_stash;
	rackidnext_stash = 0;
}

static void topology_rackname_cleanupstash(void) {
	uint32_t i;
	for (i=1 ; i<rackidnext_stash ; i++) {
		free(rackidtab_stash[i]->rackname);
		free(rackidtab_stash[i]);
	}
	free(rackidtab_stash);
	rackidtab_stash = NULL;
	rackidtabsize_stash = 0;
	rackidnext_stash = 0;
	for (i=0 ; i<HASHTABSIZE ; i++) {
		rackhashtab_stash[i] = NULL;
	}
}

// ************* NAME <-> ID MAP ** END *****************




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

uint32_t topology_get_rackid(uint32_t ip) {
	return itree_find(racktree,ip);
}

// as for now:
//
// 0 - same machine
// 1 - same rack, different machines
// 2 - different racks

uint8_t topology_distance(uint32_t ip1,uint32_t ip2) {
	uint32_t rid1,rid2;
	char *rname1,*rname2;
	int pos,lastbar;
	uint8_t l1,l2;

	if (ip1==ip2) {
		return 0;
	}
	rid1 = itree_find(racktree,ip1);
	rid2 = itree_find(racktree,ip2);
	if (rid1==rid2) {
		return 1;
	}
	rname1 = topology_rackid_to_rackname(rid1);
	rname2 = topology_rackid_to_rackname(rid2);

	if (rname1==NULL && rname2==NULL) { // safety guard - this may only happen when both rid1 and rid2 are 0 - it shouldn't pass rid1==rid2 condition
		return 1;
	}

	lastbar = 0;
	if (rname1!=NULL && rname2!=NULL) {
		pos = 0;
		while (1) {
			if ((rname1[pos]==0 && rname2[pos]=='|') || (rname1[pos]=='|' && rname2[pos]==0)) {
				lastbar = pos;
				break;
			}
			if (rname1[pos] != rname2[pos]) {
				break;
			}
			if (rname1[pos]=='|') {
				lastbar = pos;
			}
			if (rname1[pos] == 0) { // safety guard - this means that strings are identical - if that then they should have the same rackid
				return 1;
			}
			pos++;
		}
	}
	l1 = 0;
	l2 = 0;
	if (rname1!=NULL) {
		if (rname1[lastbar]=='|') {
			pos = lastbar+1;
		} else {
			pos = lastbar;
		}
		for ( ; rname1[pos] ; pos++) {
			if (rname1[pos]=='|') {
				l1++;
			}
		}
	}
	if (rname2!=NULL) {
		if (rname2[lastbar]=='|') {
			pos = lastbar+1;
		} else {
			pos = lastbar;
		}
		for ( ; rname2[pos] ; pos++) {
			if (rname2[pos]=='|') {
				l2++;
			}
		}
	}
	if (l1>l2) {
		return 2+l1;
	} else {
		return 2+l2;
	}
}

// format:
// network	rackid


// format (3.0.104+)
// network	rack_path_sparated_by_vertical_bar

int topology_parseline(char *line,uint32_t lineno,uint32_t *fip,uint32_t *tip,uint32_t *rid) {
	char c,*net,*rackname;
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

	if (*p==0 || *p=='#') {
		mfs_arg_syslog(LOG_WARNING,"mfstopology: incorrect rack id in line: %"PRIu32,lineno);
		fprintf(stderr,"mfstopology: incorrect rack id in line: %"PRIu32"\n",lineno);
		return -1;
	}

	rackname = p;

	while (*p && *p!=' ' && *p!='\t') {
		p++;
	}

	c = *p;
	*p = 0;
	*rid = topology_rackname_to_rackid(rackname);
	*p = c;

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

	topology_rackname_stash();
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
		topology_rackname_restore();
		fprintf(stderr,"error reading mfstopology file - network topology not defined (using defaults)\n");
		return;
	}
	fclose(fd);
	topology_rackname_cleanupstash();
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
	topology_rackname_cleanup();
}

int topology_init(void) {
	TopologyFileName = NULL;
	racktree = NULL;
	topology_rackname_init();
	topology_reload();
	main_reload_register(topology_reload);
	main_destruct_register(topology_term);
	return 0;
}
