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
#include <stddef.h>
#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif

#include "MFSCommunication.h"

#include "datapack.h"
#include "csdb.h"
#include "matocsserv.h"
#include "metadata.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "chunks.h"
#include "random.h"
#include "sizestr.h"
#include "slogger.h"
#include "massert.h"
#include "mfsstrerr.h"
#include "hashfn.h"
#include "clocks.h"
#include "storageclass.h"
#include "md5.h"
#include "mfsalloc.h"

#define MaxPacketSize CSTOMA_MAXPACKETSIZE

#define MANAGER_SWITCH_CONST 5

// ReserveSpaceMode
enum{RESERVE_BYTES,RESERVE_PERCENT,RESERVE_CHUNKSERVER_USED,RESERVE_CHUNKSERVER_TOTAL};

// matocsserventry.mode
enum{KILL,DATA,FINISH};

enum{UNREGISTERED,REGISTERED};

struct csdbentry;

typedef struct out_packetstruct {
	struct out_packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t data[1];
} out_packetstruct;

typedef struct in_packetstruct {
	struct in_packetstruct *next;
	uint32_t type,leng;
	uint8_t data[1];
} in_packetstruct;

typedef struct matocsserventry {
	uint8_t mode;
	int sock;
	int32_t pdescpos;
	double lastread,lastwrite;
	uint8_t input_hdr[8];
	uint8_t *input_startptr;
	uint32_t input_bytesleft;
	uint8_t input_end;
	in_packetstruct *input_packet;
	in_packetstruct *inputhead,**inputtail;
	out_packetstruct *outputhead,**outputtail;

	char *servstrip;		// human readable version of servip
	uint32_t version;		// chunkserver version
	uint32_t servip;		// ip to coonnect to
	uint16_t servport;		// port to connect to
	uint16_t timeout;		// communication timeout
	uint32_t load;			// current load
	uint8_t hlstatus;		// heavy load status (0 - unknown, 1 - ok, 2 - overloaded, 3 - internal rebalance is on)
	uint64_t usedspace;		// used hdd space in bytes
	uint64_t totalspace;		// total hdd space in bytes
	uint32_t chunkscount;
	uint64_t todelusedspace;
	uint64_t todeltotalspace;
	uint32_t todelchunkscount;
	uint32_t errorcounter;
	uint16_t writecounter;
	uint16_t rrepcounter;
	uint16_t wrepcounter;
	uint16_t delcounter;

	uint32_t labelmask;
	char *labelstr;

	uint32_t create_total_counter;
	uint32_t rrep_total_counter;
	uint32_t wrep_total_counter;
	uint32_t del_total_counter;
	double total_counter_begin;

	uint16_t csid;
	uint8_t registered;

	uint8_t wrepbalance;
	uint8_t rrepbalance;
	uint8_t rebalaneflags;

	uint8_t passwordrnd[32];

	uint32_t dist;
	uint8_t first;
	double corr;

	struct csdbentry *csptr;

	struct matocsserventry *next;
} matocsserventry;

static matocsserventry *matocsservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

static uint64_t gtotalspace = 0;
static uint64_t gusedspace = 0;
static uint64_t gavailspace = 0;
static uint64_t gfreespace = 0;

// from config
static char *ListenHost;
static char *ListenPort;
static uint32_t DefaultTimeout;
static char *AuthCode = NULL;
static uint32_t RemapMask = 0;
static uint32_t RemapSrc = 0;
static uint32_t RemapDst = 0;
static double ReserveSpaceValue = 0.0;
static uint8_t ReserveSpaceMode = RESERVE_BYTES;

/* replications DB */

#define REPHASHSIZE 256
#define REPHASHFN(chid,ver) (((chid)^(ver)^((chid)>>8))%(REPHASHSIZE))

typedef struct _repsrc {
	void *src;
	struct _repsrc *next;
} repsrc;

typedef struct _repdst {
	uint64_t chunkid;
	uint32_t version;
	void *dst;
	repsrc *srchead;
	struct _repdst *next;
} repdst;

static repdst* rephash[REPHASHSIZE];
static repsrc *repsrcfreehead=NULL;
static repdst *repdstfreehead=NULL;

repsrc* matocsserv_repsrc_malloc() {
	repsrc *r;
	if (repsrcfreehead) {
		r = repsrcfreehead;
		repsrcfreehead = r->next;
	} else {
		r = (repsrc*)malloc(sizeof(repsrc));
		passert(r);
	}
	return r;
}

void matocsserv_repsrc_free(repsrc *r) {
	r->next = repsrcfreehead;
	repsrcfreehead = r;
}

repdst* matocsserv_repdst_malloc() {
	repdst *r;
	if (repdstfreehead) {
		r = repdstfreehead;
		repdstfreehead = r->next;
	} else {
		r = (repdst*)malloc(sizeof(repdst));
		passert(r);
	}
	return r;
}

void matocsserv_repdst_free(repdst *r) {
	r->next = repdstfreehead;
	repdstfreehead = r;
}

void matocsserv_replication_init(void) {
	uint32_t hash;
	for (hash=0 ; hash<REPHASHSIZE ; hash++) {
		rephash[hash]=NULL;
	}
	repsrcfreehead=NULL;
	repdstfreehead=NULL;
}

int matocsserv_replication_find(uint64_t chunkid,uint32_t version,void *dst) {
	uint32_t hash = REPHASHFN(chunkid,version);
	repdst *r;
	for (r=rephash[hash] ; r ; r=r->next) {
		if (r->chunkid==chunkid && r->version==version && r->dst==dst) {
			return 1;
		}
	}
	return 0;
}

void matocsserv_replication_begin(uint64_t chunkid,uint32_t version,void *dst,uint8_t srccnt,void **src) {
	uint32_t hash = REPHASHFN(chunkid,version);
	uint8_t i;
	repdst *r;
	repsrc *rs;

	if (srccnt>0) {
		r = matocsserv_repdst_malloc();
		r->chunkid = chunkid;
		r->version = version;
		r->dst = dst;
		r->srchead = NULL;
		r->next = rephash[hash];
		rephash[hash] = r;
		for (i=0 ; i<srccnt ; i++) {
			rs = matocsserv_repsrc_malloc();
			rs->src = src[i];
			rs->next = r->srchead;
			r->srchead = rs;
			((matocsserventry *)(src[i]))->rrepcounter++;
			((matocsserventry *)(src[i]))->rrep_total_counter++;
		}
		((matocsserventry *)(dst))->wrepcounter++;
		((matocsserventry *)(dst))->wrep_total_counter++;
	}
}

uint32_t matocsserv_replication_print(char *buff,uint32_t bleng,uint64_t chunkid,uint32_t version,void *dst) {
	uint32_t hash = REPHASHFN(chunkid,version);
	repdst *r;
	repsrc *rs;
	uint32_t leng;

	leng = 0;
	for (r=rephash[hash] ; r ; r=r->next) {
		if (r->chunkid==chunkid && r->version==version && r->dst==dst) {
			for (rs=r->srchead ; rs ; rs=rs->next) {
				if (leng>0 && leng<bleng) {
					buff[leng++] = ',';
				}
				if (leng<bleng) {
					leng += snprintf(buff+leng,bleng-leng,"%s:%"PRIu16,((matocsserventry *)(rs->src))->servstrip,((matocsserventry *)(rs->src))->servport);
				}
			}
			if (leng<bleng) {
				leng += snprintf(buff+leng,bleng-leng," -> %s:%"PRIu16,((matocsserventry *)dst)->servstrip,((matocsserventry *)dst)->servport);
			}
			return leng;
		}
	}
	return 0;
}

void matocsserv_replication_end(uint64_t chunkid,uint32_t version,void *dst) {
	uint32_t hash = REPHASHFN(chunkid,version);
	repdst *r,**rp;
	repsrc *rs,*rsdel;

	rp = &(rephash[hash]);
	while ((r=*rp)!=NULL) {
		if (r->chunkid==chunkid && r->version==version && r->dst==dst) {
			rs = r->srchead;
			while (rs) {
				rsdel = rs;
				rs = rs->next;
				((matocsserventry *)(rsdel->src))->rrepcounter--;
				matocsserv_repsrc_free(rsdel);
			}
			((matocsserventry *)(dst))->wrepcounter--;
			*rp = r->next;
			matocsserv_repdst_free(r);
		} else {
			rp = &(r->next);
		}
	}
}

void matocsserv_replication_disconnected(void *srv) {
	uint32_t hash;
	repdst *r,**rp;
	repsrc *rs,*rsdel,**rsp;

	for (hash=0 ; hash<REPHASHSIZE ; hash++) {
		rp = &(rephash[hash]);
		while ((r=*rp)!=NULL) {
			if (r->dst==srv) {
				rs = r->srchead;
				while (rs) {
					rsdel = rs;
					rs = rs->next;
					((matocsserventry *)(rsdel->src))->rrepcounter--;
					matocsserv_repsrc_free(rsdel);
				}
				((matocsserventry *)(srv))->wrepcounter--;
				*rp = r->next;
				matocsserv_repdst_free(r);
			} else {
				rsp = &(r->srchead);
				while ((rs=*rsp)!=NULL) {
					if (rs->src==srv) {
						((matocsserventry *)(srv))->rrepcounter--;
						*rsp = rs->next;
						matocsserv_repsrc_free(rs);
					} else {
						rsp = &(rs->next);
					}
				}
				rp = &(r->next);
			}
		}
	}
}

/* replication DB END */

uint8_t matocsserv_check_password(const uint8_t rndcode[32],const uint8_t csdigest[16]) {
	md5ctx md5c;
	uint8_t digest[16];

	md5_init(&md5c);
	md5_update(&md5c,rndcode,16);
	md5_update(&md5c,(const uint8_t *)AuthCode,strlen(AuthCode));
	md5_update(&md5c,rndcode+16,16);
	md5_final(digest,&md5c);

	if (memcmp(digest,csdigest,16)==0) {
		return 1;
	}
	return 0;
}

void matocsserv_log_extra_info(void) {
	matocsserventry *eptr;
	double dur,usage;
	const char *hlstatus_name;
	uint8_t overloaded;
	uint8_t maintained;
	uint32_t now = main_time();
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->csptr!=NULL) {
			dur = monotonic_seconds() - eptr->total_counter_begin;
			if (dur<1.0) {
				dur = 1.0;
			}
			overloaded = csdb_server_is_overloaded(eptr->csptr,now)?1:0;
			maintained = csdb_server_is_being_maintained(eptr->csptr)?1:0;
			switch (eptr->hlstatus) {
				case HLSTATUS_DEFAULT:
					hlstatus_name = "DEFAULT";
					break;
				case HLSTATUS_OK:
					hlstatus_name = "OK";
					break;
				case HLSTATUS_OVERLOADED:
					hlstatus_name = "OVERLOADED";
					break;
				case HLSTATUS_REBALANCE:
					hlstatus_name = "REBALANCE";
					break;
				case HLSTATUS_GRACEFUL:
					hlstatus_name = "GRACEFUL";
					break;
				default:
					hlstatus_name = "UNKNOWN";
			}
			if (eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace) {
				usage = 100.0*(double)(eptr->usedspace)/(double)(eptr->totalspace);
			} else {
				if (eptr->totalspace>0) {
					usage = 100.0;
				} else {
					usage = 0.0;
				}
			}
			syslog(LOG_NOTICE,"cs %s:%u ; usedspace: %"PRIu64" ; totalspace: %"PRIu64" ; usage: %.2lf%% ; load: %"PRIu32" ; timeout: %"PRIu16" ; chunkscount: %"PRIu32" ; errorcounter: %"PRIu32" ; writecounter: %"PRIu16" ; rrepcounter: %"PRIu16" ; wrepcounter: %"PRIu16" ; delcounter: %"PRIu32" ; create_total: %"PRIu32" ; rrep_total: %"PRIu32" ; wrep_total: %"PRIu32" ; del_total: %"PRIu32" ; create/s: %.4lf ; rrep/s: %.4lf ; wrep/s: %.4lf ; del/s: %.4lf ; csid: %"PRIu16" ; dist: %"PRIu32" ; first: %"PRIu8" ; corr: %.4lf ; hlstatus: %"PRIu8" (%s) ; overloaded: %"PRIu8" ; maintained: %"PRIu8,eptr->servstrip,eptr->servport,eptr->usedspace,eptr->totalspace,usage,eptr->load,eptr->timeout,eptr->chunkscount,eptr->errorcounter,eptr->writecounter,eptr->rrepcounter,eptr->wrepcounter,eptr->delcounter,eptr->create_total_counter,eptr->rrep_total_counter,eptr->wrep_total_counter,eptr->del_total_counter,eptr->create_total_counter/dur,eptr->rrep_total_counter/dur,eptr->wrep_total_counter/dur,eptr->del_total_counter/dur,eptr->csid,eptr->dist,eptr->first,eptr->corr,eptr->hlstatus,hlstatus_name,overloaded,maintained);
			eptr->create_total_counter = 0;
			eptr->rrep_total_counter = 0;
			eptr->wrep_total_counter = 0;
			eptr->del_total_counter = 0;
			eptr->total_counter_begin = monotonic_seconds();
		}
	}
}

int matocsserv_space_compare(const void *a,const void *b) {
	const struct servsort {
		double space;
		uint16_t csid;
	} *aa=a,*bb=b;
	if (aa->space > bb->space) {
		return 1;
	}
	if (aa->space < bb->space) {
		return -1;
	}
	return 0;
}

/*
void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount) {
	matocsserventry *eptr;
	uint32_t j,k;
	double minspace=1.0,maxspace=0.0;
	double minspaceinc=1.0,maxspacedec=0.0;
	double spacedec,spaceinc,space;
	j = 0;
	k = 0;
	for (eptr = matocsservhead ; eptr && j<65535 && k<65535; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->csptr!=NULL) {
			if (eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace) {
				spaceinc = (double)(eptr->usedspace+(64*1024*1024)) / (double)(eptr->totalspace);
				if (spaceinc>1.0) {
					spaceinc = 1.0;
				}
				spacedec = (double)(eptr->usedspace-(64*1024*1024)) / (double)(eptr->totalspace);
				if (spacedec<0.0) {
					spacedec = 0.0;
				}
				space = (double)(eptr->usedspace) / (double)(eptr->totalspace);
				if (j==0) {
					minspace = maxspace = space;
					minspaceinc = spaceinc;
					maxspacedec = spacedec;
				} else if (space<minspace) {
					minspace = space;
					minspaceinc = spaceinc;
				} else if (space>maxspace) {
					maxspace = space;
					maxspacedec = spacedec;
				}
				j++;
			}
			k++;
		}
	}
	if (usablescount) {
		*usablescount = j;
	}
	if (totalscount) {
		*totalscount = k;
	}
	if (j==0) {
		if (minusage) {
			*minusage = 1.0;
		}
		if (maxusage) {
			*maxusage = 0.0;
		}
	} else if (maxspacedec<minspaceinc) { // trashing prevention
		if (minusage) {
			*minusage = (minspace + maxspace) / 2.0;
		}
		if (maxusage) {
			*maxusage = (minspace + maxspace) / 2.0;
		}
	} else {
		if (minusage) {
			*minusage = minspace;
		}
		if (maxusage) {
			*maxusage = maxspace;
		}
	}
}
*/

// uint16_t matocsserv_getservers_ordered(uint16_t csids[MAXCSCOUNT],double maxusagediff,uint32_t *pmin,uint32_t *pmax) {
uint16_t matocsserv_getservers_ordered(uint16_t csids[MAXCSCOUNT]) {
	static struct servsort {
		double space;
		uint16_t csid;
	} servtab[MAXCSCOUNT];
	matocsserventry *eptr;
	uint32_t i,scnt;

	scnt = 0;
	for (eptr = matocsservhead ; eptr && scnt<MAXCSCOUNT; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL && (eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK || eptr->hlstatus==HLSTATUS_REBALANCE) && csdb_server_is_being_maintained(eptr->csptr)==0) {
			servtab[scnt].csid = eptr->csid;
			servtab[scnt].space = (double)(eptr->usedspace) / (double)(eptr->totalspace);
			scnt++;
		}
	}
	if (scnt==0) {
		return 0;
	}

	qsort(servtab,scnt,sizeof(struct servsort),matocsserv_space_compare);
	
	for (i=0 ; i<scnt ; i++) {
		csids[i]=servtab[i].csid;
	}
	return scnt;
}

static int matocsserv_err_compare(const void *a,const void *b) {
	const struct rservsort {
		double err;
		matocsserventry *ptr;
	} *aa=a,*bb=b;
	if (aa->err < bb->err) {
		return -1;
	}
	if (aa->err > bb->err) {
		return 1;
	}
	return 0;
}

static inline void matocsserv_weighted_roundrobin_sort(matocsserventry* servers[MAXCSCOUNT],uint32_t cnt) {
	matocsserventry *eptr;
	double expdist;
	uint32_t i;
	uint64_t totalspace;
	static struct rservsort {
		double err;
		matocsserventry *ptr;
	} servtab[MAXCSCOUNT];

	totalspace = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			totalspace += eptr->totalspace;
		}
	}


	for (i=0 ; i<cnt ; i++) {
		if (servers[i]->first) {
			servtab[i].err = 1.0;
		} else {
			expdist = totalspace;
			expdist /= servers[i]->totalspace;
			servtab[i].err = (expdist + servers[i]->corr) / (servers[i]->dist + 1);
		}
		servtab[i].err += 1000.0 * (servers[i]->writecounter + servers[i]->wrepcounter);
		servtab[i].ptr = servers[i];
	}

	qsort(servtab,cnt,sizeof(struct rservsort),matocsserv_err_compare);

	for (i=0 ; i<cnt ; i++) {
		servers[i] = servtab[i].ptr;
	}
}


static inline void matocsserv_weighted_roundrobin_used(matocsserventry* servers[MAXCSCOUNT],uint32_t cnt) {
	static uint32_t fcnt = 0;
	matocsserventry *eptr;
	double expdist,dist;
	uint32_t i,totalcnt;
	uint64_t totalspace;

	totalspace = 0;
	totalcnt = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			totalspace += eptr->totalspace;
			totalcnt++;
			eptr->dist += cnt;
		}
	}

	fcnt += cnt;
	if (fcnt>(totalcnt*10)) { // correlation fixer
		fcnt = 0;
		for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
			if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
				dist = totalspace;
				dist /= eptr->totalspace;
				eptr->dist = rndu32_ranged(dist*1000)/1000;
				eptr->corr = 0.0;
			}
		}
		for (i=0 ; i<cnt ; i++) {
			servers[i]->create_total_counter++;
		}
	} else { // standard weighted round-robin
		for (i=0 ; i<cnt ; i++) {
			if (servers[i]->first) {
				servers[i]->first = 0;
			} else {
				expdist = totalspace;
				expdist /= servers[i]->totalspace;
				servers[i]->corr += expdist - (servers[i]->dist + i + 1 - cnt);
			}
			servers[i]->dist = cnt - i - 1;
			servers[i]->create_total_counter++;
		}
	}

}

#if 0
void matocsserv_recalc_createflag(double tolerance) {
	matocsserventry *eptr;
	double avg,m;
	uint32_t allcnt;
	uint32_t avgcnt;
	uint64_t tspace;
	uint64_t uspace;
	uint32_t onlyabove;
	static uint32_t updated = 0;
	static double lasttolerance = 0.0;

	if (updated == main_time() && lasttolerance == tolerance) {
		return;
	}
	updated = main_time();
	lasttolerance = tolerance;
	/* find avg usage */
	allcnt = 0;
	tspace = 0;
	uspace = 0;
	for (eptr = matocsservhead ; eptr && allcnt<MAXCSCOUNT ; eptr=eptr->next) {
		eptr->cancreatechunks = 0;
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>MFSCHUNKSIZE && eptr->csptr!=NULL) {
			uspace += eptr->usedspace;
			tspace += eptr->totalspace;
			allcnt++;
		}
	}
	avg = (double)uspace/(double)tspace;
	onlyabove = 0;
	if (allcnt>=5) {
		avgcnt = 0;
		for (eptr = matocsservhead ; eptr && allcnt<MAXCSCOUNT ; eptr=eptr->next) {
			if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>MFSCHUNKSIZE && eptr->csptr!=NULL) {
				m = (double)(eptr->usedspace)/(double)(eptr->totalspace);
				if (m > avg - tolerance) {
					avgcnt++;
				}
			}
		}
		if (avgcnt * 3 > allcnt * 2) {
			onlyabove = 1;
		}
	}
	for (eptr = matocsservhead ; eptr && allcnt<MAXCSCOUNT ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>MFSCHUNKSIZE && eptr->csptr!=NULL) {
			m = (double)(eptr->usedspace)/(double)(eptr->totalspace);
			if (m > avg + tolerance) {
				eptr->cancreatechunks = 2;
			} else if (onlyabove == 0 || m >= avg - tolerance) {
				eptr->cancreatechunks = 1;
			}
		}
	}
}
#endif

uint8_t matocsserv_server_has_labels(void *e,uint32_t *labelmask) {
	matocsserventry *eptr = (matocsserventry*)e;
	uint32_t i;
	if (labelmask[0]==0) { // '*'
		return 1;
	}
	for (i=0 ; i<MASKORGROUP && labelmask[i]!=0 ; i++) {
		if ((eptr->labelmask&labelmask[i])==labelmask[i]) {
			return 1;
		}
	}
	return 0;
}

uint16_t matocsserv_servers_with_labelsets(uint32_t *labelmask) {
	uint16_t cnt;
	matocsserventry *eptr;
	cnt = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			if (matocsserv_server_has_labels(eptr,labelmask)) {
				cnt++;
			}
		}
	}
	return cnt;
}

uint16_t matocsserv_servers_with_label(uint8_t label) {
	uint16_t cnt;
	matocsserventry *eptr;
	cnt = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			if (eptr->labelmask & (1<<label)) {
				cnt++;
			}
		}
	}
	return cnt;
}

uint32_t matocsserv_server_get_labelmask(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	return eptr->labelmask;
}

const char* matocsserv_server_get_labelstr(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (eptr->labelstr!=NULL) {
		return eptr->labelstr;
	} else {
		return "(undefined)";
	}
}

uint32_t matocsserv_server_get_ip(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	return eptr->servip;
}

uint16_t matocsserv_servers_count(void) {
	uint16_t cnt;
	matocsserventry *eptr;
	cnt = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			cnt++;
		}
	}
	return cnt;
}

uint16_t matocsserv_almostfull_servers(void) {
	uint16_t cnt;
	matocsserventry *eptr;
	cnt = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			if ((eptr->totalspace - eptr->usedspace)<=(eptr->totalspace/100)) {
				cnt++;
			}
		}
	}
	return cnt;
}

// std - standard servers that can be use for write
// ol - standard and overloaded servers - can't be used for write, but are present and have free space
// all - all correct servers - standard, overloaded and servers without free space
void matocsserv_getservers_test(uint16_t *stdcscnt,uint16_t stdcsids[MAXCSCOUNT],uint16_t *olcscnt,uint16_t olcsids[MAXCSCOUNT],uint16_t *allcscnt,uint16_t allcsids[MAXCSCOUNT]) {
	matocsserventry *eptr;
	uint32_t gracecnt;
	uint32_t stdcnt;
	uint32_t totalcnt;

	gracecnt = 0;
	stdcnt = 0;
	totalcnt = 0;

	*stdcscnt = 0;
	*olcscnt = 0;
	*allcscnt = 0;

	for (eptr = matocsservhead ; eptr && totalcnt<MAXCSCOUNT ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) { // is this correct CS?
			allcsids[*allcscnt] = eptr->csid;
			(*allcscnt)++;
			totalcnt++;
			if ((eptr->totalspace - eptr->usedspace)>(MFSCHUNKSIZE*(1U+eptr->writecounter*10U))) { // server have enough space
				if (eptr->hlstatus!=HLSTATUS_OVERLOADED) { // server is not overloaded ?
					if ((eptr->hlstatus!=HLSTATUS_DEFAULT && eptr->hlstatus!=HLSTATUS_OK && eptr->hlstatus!=HLSTATUS_REBALANCE) || csdb_server_is_being_maintained(eptr->csptr)) {
						gracecnt++;
						stdcsids[MAXCSCOUNT-gracecnt] = eptr->csid;
					} else {
						stdcsids[*stdcscnt] = eptr->csid;
						(*stdcscnt)++;
						stdcnt++;
					}
				} else {
					olcsids[*olcscnt] = eptr->csid;
					(*olcscnt)++;
				}
			}
		}
	}

	if ((gracecnt*5) > (gracecnt+stdcnt)) { // there are more than 20% CS in 'grace' state - add all of them to the list
		while (gracecnt>0) {
			stdcsids[*stdcscnt] = stdcsids[MAXCSCOUNT-gracecnt];
			(*stdcscnt)++;
			gracecnt--;
		}
	}
}

/* servers used when new chunk is created */
uint16_t matocsserv_getservers_wrandom(uint16_t csids[MAXCSCOUNT],uint16_t *overloaded) {
	matocsserventry* servtab[MAXCSCOUNT];
	matocsserventry *eptr;
	uint32_t i;
	uint32_t gracecnt;
	uint32_t stdcnt;
	uint32_t totalcnt;
	gracecnt = 0;
	stdcnt = 0;
	totalcnt = 0;
	*overloaded = 0;

	for (eptr = matocsservhead ; eptr && totalcnt<MAXCSCOUNT ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			if ((eptr->totalspace - eptr->usedspace)>(MFSCHUNKSIZE*(1U+eptr->writecounter*10U))) {
				if (eptr->hlstatus!=HLSTATUS_OVERLOADED) {
					totalcnt++;
					if ((eptr->hlstatus!=HLSTATUS_DEFAULT && eptr->hlstatus!=HLSTATUS_OK) || csdb_server_is_being_maintained(eptr->csptr)) {
						gracecnt++;
						servtab[MAXCSCOUNT-gracecnt] = eptr;
					} else {
						servtab[stdcnt] = eptr;
						stdcnt++;
					}
				} else {
					(*overloaded)++;
				}
			}
		}
	}

	if ((gracecnt*5) > (gracecnt+stdcnt)) { // there are more than 20% CS in 'grace' or 'rebalance' state - add all of them to the list
		while (gracecnt>0) {
			servtab[stdcnt] = servtab[MAXCSCOUNT-gracecnt];
			stdcnt++;
			gracecnt--;
		}
	}

	matocsserv_weighted_roundrobin_sort(servtab,stdcnt);

	for (i=0 ; i<stdcnt ; i++) {
		csids[i] = servtab[i]->csid;
	}

	return stdcnt;
}

void matocsserv_useservers_wrandom(void* servers[MAXCSCOUNT],uint16_t cnt) {
	matocsserv_weighted_roundrobin_used((matocsserventry **)servers,cnt);
}

void matocsserv_get_server_groups(uint16_t csids[MAXCSCOUNT],double replimit,uint16_t positions[4]) {
	matocsserventry *eptr;
	uint16_t i,j,r;
	uint16_t x;
	uint8_t csstate,stage;
	uint16_t counters[4];
	uint32_t now = main_time();
	double a;

	counters[CSSTATE_OK] = 0;
	counters[CSSTATE_OVERLOADED] = 0;
	counters[CSSTATE_LIMIT_REACHED] = 0;
	counters[CSSTATE_NO_SPACE] = 0;

	for (stage=0 ; stage<2 ; stage++) {
		i = 0;
		for (eptr = matocsservhead ; eptr && i<MAXCSCOUNT; eptr=eptr->next) {
			if (eptr->mode!=KILL && eptr->csptr!=NULL) {
				a = ((uint32_t)(eptr->csid*UINT32_C(0x9874BF31)+now*UINT32_C(0xB489FC37)))/4294967296.0;
				if (eptr->totalspace==0 || (eptr->totalspace - eptr->usedspace)<=(eptr->totalspace/100)) {
					csstate=CSSTATE_NO_SPACE;
				} else if (eptr->wrepcounter+a>=replimit) {
					csstate=CSSTATE_LIMIT_REACHED;
				} else if (!((eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK || eptr->hlstatus==HLSTATUS_REBALANCE) && csdb_server_is_being_maintained(eptr->csptr)==0)) {
					csstate=CSSTATE_OVERLOADED;
				} else {
					csstate=CSSTATE_OK;
				}
				if (stage==0) {
					counters[csstate]++;
				} else {
					csids[positions[csstate]] = eptr->csid;
					positions[csstate]++;
				}
				i++;
			}
		}
		if (stage==0) {
			positions[CSSTATE_OK] = 0;
			positions[CSSTATE_OVERLOADED] = counters[CSSTATE_OK];
			positions[CSSTATE_LIMIT_REACHED] = positions[CSSTATE_OVERLOADED] + counters[CSSTATE_OVERLOADED];
			positions[CSSTATE_NO_SPACE] = positions[CSSTATE_LIMIT_REACHED] + counters[CSSTATE_LIMIT_REACHED];
		}
	}

	massert(positions[CSSTATE_OK]==counters[CSSTATE_OK],"data integrity error");
	massert(positions[CSSTATE_OVERLOADED]==(counters[CSSTATE_OK]+counters[CSSTATE_OVERLOADED]),"data integrity error");
	massert(positions[CSSTATE_LIMIT_REACHED]==(counters[CSSTATE_OK]+counters[CSSTATE_OVERLOADED]+counters[CSSTATE_LIMIT_REACHED]),"data integrity error");
	massert(positions[CSSTATE_NO_SPACE]==(counters[CSSTATE_OK]+counters[CSSTATE_OVERLOADED]+counters[CSSTATE_LIMIT_REACHED]+counters[CSSTATE_NO_SPACE]),"data integrity error");

	// shuffle normal servers
	for (i=1 ; i<counters[CSSTATE_OK] ; i++) {
		r = rndu32_ranged(i+1);
		if (r!=i) {
			x = csids[i];
			csids[i] = csids[r];
			csids[r] = x;
		}
	}
	// shuffle overloaded servers
	for (i=1 ; i<counters[CSSTATE_OVERLOADED] ; i++) {
		r = positions[CSSTATE_OK] + rndu32_ranged(i+1);
		j = positions[CSSTATE_OK] + i;
		if (r!=j) {
			x = csids[j];
			csids[j] = csids[r];
			csids[r] = x;
		}
	}
}

/*
uint16_t matocsserv_getservers_lessrepl(uint16_t csids[MAXCSCOUNT],double replimit,uint8_t highpriority,uint8_t *allservflag) {
	matocsserventry *eptr;
	uint32_t j,k,r,hpadd;
	uint16_t x;
	uint32_t now = main_time();
	double a;

	j=0;
	k=0;
	hpadd = 0;
	*allservflag = 1; // 1 means that there are no servers which reached replication limit
	for (eptr = matocsservhead ; eptr && j<MAXCSCOUNT; eptr=eptr->next) {
		a = ((uint32_t)(eptr->csid*UINT32_C(0x9874BF31)+now*UINT32_C(0xB489FC37)))/4294967296.0;
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>(eptr->totalspace/100) && eptr->csptr!=NULL) {
			if ((eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK) && csdb_server_is_being_maintained(eptr->csptr)==0) {
				if (eptr->wrepcounter+a<replimit) {
					csids[j] = eptr->csid;
					j++;
				} else {
					*allservflag = 0;
				}
			} else {
				if (eptr->wrepcounter+a<replimit) {
					hpadd = 1;
				}
			}
		}
	}
	while (j>k+1) {
		r = k + rndu32_ranged(j-k);
		if (r!=k) {
			x = csids[k];
			csids[k] = csids[r];
			csids[r] = x;
		}
		k++;
	}
	if (highpriority && hpadd) {
		k = j;
		for (eptr = matocsservhead ; eptr && j<MAXCSCOUNT; eptr=eptr->next) {
			a = ((uint32_t)(eptr->csid*UINT32_C(0x9874BF31)+now*UINT32_C(0xB489FC37)))/4294967296.0;
			if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>(eptr->totalspace/100) && eptr->csptr!=NULL) {
				if (!((eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK) && csdb_server_is_being_maintained(eptr->csptr)==0)) {
					if (eptr->wrepcounter+a<replimit) {
						csids[j] = eptr->csid;
						j++;
					} else {
						*allservflag = 0;
					}
				}
			}
		}
		while (j>k+1) {
			r = k + rndu32_ranged(j-k);
			if (r!=k) {
				x = csids[k];
				csids[k] = csids[r];
				csids[r] = x;
			}
			k++;
		}
	}
	return j;
}
*/
void matocsserv_calculate_space(void) {
	matocsserventry *eptr;
	uint64_t tspace,uspace,rspace;
	uint64_t muspace,mtspace;
	tspace = 0;
	uspace = 0;
	muspace = 0;
	mtspace = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0) {
			tspace += eptr->totalspace;
			uspace += eptr->usedspace;
			if (eptr->usedspace > muspace) {
				muspace = eptr->usedspace;
			}
			if (eptr->totalspace > mtspace) {
				mtspace = eptr->totalspace;
			}
		}
	}
	switch (ReserveSpaceMode) {
		case RESERVE_PERCENT:
			rspace = ReserveSpaceValue * (tspace / 100.0);
			break;
		case RESERVE_CHUNKSERVER_USED:
			rspace = ReserveSpaceValue * muspace;
			break;
		case RESERVE_CHUNKSERVER_TOTAL:
			rspace = ReserveSpaceValue * mtspace;
			break;
		case RESERVE_BYTES:
		default:
			rspace = ReserveSpaceValue;
			break;
	}

	gtotalspace = tspace;
	gusedspace = uspace;
	gfreespace = tspace-uspace;
	if (rspace > gfreespace) {
		gavailspace = 0;
	} else {
		gavailspace = gfreespace - rspace;
	}
}


int matocsserv_have_availspace(void) {
	return (gavailspace>0)?1:0;
}

void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace,uint64_t *freespace) {
	if (totalspace!=NULL) {
		*totalspace = gtotalspace;
	}
	if (availspace!=NULL) {
		*availspace = gavailspace;
	}
	if (freespace!=NULL) {
		*freespace = gfreespace;
	}
}

char* matocsserv_getstrip(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	static char *empty="???";
	if (eptr->mode!=KILL && eptr->servstrip) {
		return eptr->servstrip;
	}
	return empty;
}

int matocsserv_get_csdata(void *e,uint32_t *servip,uint16_t *servport,uint32_t *servver,uint32_t *servlabelmask) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (eptr->mode!=KILL) {
		*servip = eptr->servip;
		if (servport!=NULL) {
			*servport = eptr->servport;
		}
		if (servver!=NULL) {
			*servver = eptr->version;
		}
		if (servlabelmask!=NULL) {
			*servlabelmask = eptr->labelmask;
		}
		return 0;
	}
	return -1;
}

void matocsserv_getservdata(void *e,uint32_t *ver,uint64_t *uspc,uint64_t *tspc,uint32_t *chunkcnt,uint64_t *tduspc,uint64_t *tdtspc,uint32_t *tdchunkcnt,uint32_t *errcnt,uint32_t *load,uint8_t *hlstatus,uint32_t *labelmask,uint8_t *mfrstatus) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (eptr->mode!=KILL) {
		*ver = eptr->version;
		*uspc = eptr->usedspace;
		*tspc = eptr->totalspace;
		*chunkcnt = eptr->chunkscount;
		*tduspc = eptr->todelusedspace;
		*tdtspc = eptr->todeltotalspace;
		*tdchunkcnt = eptr->todelchunkscount;
		*errcnt = eptr->errorcounter;
		*load = eptr->load;
		*hlstatus = eptr->hlstatus;
		*labelmask = eptr->labelmask;
		*mfrstatus = chunk_get_mfrstatus(eptr->csid);
	} else {
		*ver = 0;
		*uspc = 0;
		*tspc = 0;
		*chunkcnt = 0;
		*tduspc = 0;
		*tdtspc = 0;
		*tdchunkcnt = 0;
		*errcnt = 0;
		*load = 0;
		*hlstatus = HLSTATUS_DEFAULT;
		*labelmask = 0;
		*mfrstatus = 0;
	}
}

#if 0
uint8_t matocsserv_can_create_chunks(void *e,double tolerance) {
	matocsserventry *eptr = (matocsserventry *)e;
	matocsserv_recalc_createflag(tolerance);
	return eptr->cancreatechunks;
}
#endif

void matocsserv_write_counters(void *e,uint8_t x) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (x) {
		eptr->writecounter++;
	} else {
		if (eptr->writecounter==0) {
			syslog(LOG_NOTICE,"can't decrease write counter - structure error");
		} else {
			eptr->writecounter--;
		}
	}
}

void matocsserv_hlstatus_fix(void) {
	matocsserventry *eptr;
	uint32_t now = main_time();
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->csptr!=NULL) {
			if (eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_GRACEFUL) {
				if (csdb_server_is_overloaded(eptr->csptr,now)) {
					eptr->hlstatus = HLSTATUS_GRACEFUL;
				} else {
					eptr->hlstatus = HLSTATUS_DEFAULT;
				}
			}
		}
	}
}

uint8_t matocsserv_has_avail_space(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	return (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL && (eptr->totalspace - eptr->usedspace) > (eptr->totalspace/100))?1:0;
}

double matocsserv_get_usage(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (eptr->totalspace>0) {
		return (double)(eptr->usedspace) / (double)(eptr->totalspace);
	} else {
		return 1.0;
	}
}

double matocsserv_replication_write_counter(void *e,uint32_t now) {
	matocsserventry *eptr = (matocsserventry *)e;
	double a;
	a = ((uint32_t)(eptr->csid*UINT32_C(0x9874BF31)+now*UINT32_C(0xB489FC37)))/4294967296.0;
	return eptr->wrepcounter+a;
}

double matocsserv_replication_read_counter(void *e,uint32_t now) {
	matocsserventry *eptr = (matocsserventry *)e;
	double a;
	a = ((uint32_t)(eptr->csid*UINT32_C(0x9874BF31)+now*UINT32_C(0xB489FC37)))/4294967296.0;
	return eptr->rrepcounter+a;
}

uint16_t matocsserv_deletion_counter(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	return eptr->delcounter;
}

char* matocsserv_makestrip(uint32_t ip) {
	uint8_t *ptr,pt[4];
	uint32_t l,i;
	char *optr;
	ptr = pt;
	put32bit(&ptr,ip);
	l=0;
	for (i=0 ; i<4 ; i++) {
		if (pt[i]>=100) {
			l+=3;
		} else if (pt[i]>=10) {
			l+=2;
		} else {
			l+=1;
		}
	}
	l+=4;
	optr = malloc(l);
	passert(optr);
	snprintf(optr,l,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,pt[0],pt[1],pt[2],pt[3]);
	optr[l-1]=0;
	return optr;
}

uint8_t* matocsserv_createpacket(matocsserventry *eptr,uint32_t type,uint32_t size) {
	out_packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	psize = size+8;
	outpacket = malloc(offsetof(out_packetstruct,data)+psize);
#ifndef __clang_analyzer__
	passert(outpacket);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
	outpacket->bytesleft = psize;
	ptr = outpacket->data;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = outpacket->data;
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

/* for future use */
int matocsserv_send_chunk_checksum(void *e,uint64_t chunkid,uint32_t version) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,ANTOCS_GET_CHUNK_CHECKSUM,8+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
	}
	return 0;
}
/* for future use */
void matocsserv_got_chunk_checksum(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version,checksum;
	uint8_t status;
	if (length!=8+4+1 && length!=8+4+4) {
		syslog(LOG_NOTICE,"CSTOAN_CHUNK_CHECKSUM - wrong size (%"PRIu32"/13|16)",length);
		eptr->mode=KILL;
		return ;
	}
	passert(data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	if (length==8+4+1) {
		status = get8bit(&data);
//		chunk_got_checksum_status(eptr->csid,chunkid,version,status);
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" calculate checksum status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	} else {
		checksum = get32bit(&data);
//		chunk_got_checksum(eptr->csid,chunkid,version,checksum);
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" calculate checksum: %08"PRIX32,eptr->servstrip,eptr->servport,chunkid,checksum);
	}
	(void)version;
}

int matocsserv_send_createchunk(void *e,uint64_t chunkid,uint32_t version) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_CREATE,8+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
	}
	return 0;
}

void matocsserv_got_createchunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_CREATE - wrong size (%"PRIu32"/9)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	status = get8bit(&data);
	chunk_got_create_status(eptr->csid,chunkid,status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" creation status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	}
}

int matocsserv_send_deletechunk(void *e,uint64_t chunkid,uint32_t version) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_DELETE,8+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		eptr->delcounter++;
		eptr->del_total_counter++;
	}
	return 0;
}

void matocsserv_got_deletechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_DELETE - wrong size (%"PRIu32"/9)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	status = get8bit(&data);
	eptr->delcounter--;
	chunk_got_delete_status(eptr->csid,chunkid,status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" deletion status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	}
}

int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint32_t version,void *src) {
	matocsserventry *dsteptr = (matocsserventry *)e;
	matocsserventry *srceptr = (matocsserventry *)src;
	uint8_t *data;

	if (matocsserv_replication_find(chunkid,version,dsteptr)) {
		return -1;
	}
	if (dsteptr->mode!=KILL && srceptr->mode!=KILL) {
//		dsteptr->dist = 0;
		data = matocsserv_createpacket(dsteptr,MATOCS_REPLICATE,8+4+4+2);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put32bit(&data,srceptr->servip);
		put16bit(&data,srceptr->servport);
		matocsserv_replication_begin(chunkid,version,dsteptr,1,&src);
	}
	return 0;
}

int matocsserv_send_replicatechunk_xor(void *e,uint64_t chunkid,uint32_t version,uint8_t cnt,const uint32_t xormasks[4],void **src,uint64_t *srcchunkid,uint32_t *srcversion) {
	matocsserventry *dsteptr = (matocsserventry *)e;
	matocsserventry *srceptr;
	uint8_t i;
	uint8_t *data;

	if (matocsserv_replication_find(chunkid,version,dsteptr)) {
		return -1;
	}
	if (dsteptr->mode!=KILL) {
		for (i=0 ; i<cnt ; i++) {
			srceptr = (matocsserventry *)(src[i]);
			if (srceptr->mode==KILL) {
				return 0;
			}
		}
		data = matocsserv_createpacket(dsteptr,MATOCS_REPLICATE,8+4+16+cnt*(8+4+4+2));
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put32bit(&data,xormasks[0]);
		put32bit(&data,xormasks[1]);
		put32bit(&data,xormasks[2]);
		put32bit(&data,xormasks[3]);
		for (i=0 ; i<cnt ; i++) {
			srceptr = (matocsserventry *)(src[i]);
			put64bit(&data,srcchunkid[i]);
			put32bit(&data,srcversion[i]);
			put32bit(&data,srceptr->servip);
			put16bit(&data,srceptr->servport);
		}
		matocsserv_replication_begin(chunkid,version,dsteptr,cnt,src);
	}
	return 0;
}

void matocsserv_got_replicatechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t status;
	char servbuff[1000];
	uint32_t leng;

	if (length!=8+4+1) {
		syslog(LOG_NOTICE,"CSTOMA_REPLICATE - wrong size (%"PRIu32"/13)",length);
		eptr->mode=KILL;
		return;
	}
//	if (eptr->repcounter>0) {
//		eptr->repcounter--;
//	}
	passert(data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	status = get8bit(&data);
	if (status!=0) {
		leng = matocsserv_replication_print(servbuff,1000,chunkid,version,eptr);
		if (leng>=1000) {
			servbuff[999] = '\0';
		} else {
			servbuff[leng] = '\0';
		}
		if (leng>0) {
			syslog(LOG_NOTICE,"(%s) chunk: %016"PRIX64" replication status: %s",servbuff,chunkid,mfsstrerr(status));
		} else {
			syslog(LOG_NOTICE,"(unknown -> %s:%"PRIu16") chunk: %016"PRIX64" replication status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
		}
	}
	matocsserv_replication_end(chunkid,version,eptr);
	chunk_got_replicate_status(eptr->csid,chunkid,version,status);
}

int matocsserv_send_setchunkversion(void *e,uint64_t chunkid,uint32_t version,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_SET_VERSION,8+4+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put32bit(&data,oldversion);
	}
	return 0;
}

void matocsserv_got_setchunkversion_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_SET_VERSION - wrong size (%"PRIu32"/9)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	status = get8bit(&data);
	chunk_got_setversion_status(eptr->csid,chunkid,status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" set version status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	}
}


int matocsserv_send_duplicatechunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_DUPLICATE,8+4+8+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put64bit(&data,oldchunkid);
		put32bit(&data,oldversion);
	}
	return 0;
}

void matocsserv_got_duplicatechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_DUPLICATE - wrong size (%"PRIu32"/9)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	status = get8bit(&data);
	chunk_got_duplicate_status(eptr->csid,chunkid,status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" duplication status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	}
}

int matocsserv_send_truncatechunk(void *e,uint64_t chunkid,uint32_t length,uint32_t version,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_TRUNCATE,8+4+4+4);
		put64bit(&data,chunkid);
		put32bit(&data,length);
		put32bit(&data,version);
		put32bit(&data,oldversion);
	}
	return 0;
}

void matocsserv_got_truncatechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_TRUNCATE - wrong size (%"PRIu32"/9)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	status = get8bit(&data);
	chunk_got_truncate_status(eptr->csid,chunkid,status);
//	matocsserv_notify(&(eptr->duplication),eptr,chunkid,status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" truncate status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	}
}

int matocsserv_send_duptruncchunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion,uint32_t length) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_DUPTRUNC,8+4+8+4+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put64bit(&data,oldchunkid);
		put32bit(&data,oldversion);
		put32bit(&data,length);
	}
	return 0;
}

void matocsserv_got_duptruncchunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_DUPTRUNC - wrong size (%"PRIu32"/9)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	status = get8bit(&data);
	chunk_got_duptrunc_status(eptr->csid,chunkid,status);
//	matocsserv_notify(&(eptr->duplication),eptr,chunkid,status);
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" duplication with truncate status: %s",eptr->servstrip,eptr->servport,chunkid,mfsstrerr(status));
	}
}

int matocsserv_send_chunkop(void *e,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t leng) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_CHUNKOP,8+4+4+8+4+4);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put32bit(&data,newversion);
		put64bit(&data,copychunkid);
		put32bit(&data,copyversion);
		put32bit(&data,leng);
	}
	return 0;
}

void matocsserv_got_chunkop_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid,copychunkid;
	uint32_t version,newversion,copyversion,leng;
	uint8_t status;
	if (length!=8+4+4+8+4+4+1) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNKOP - wrong size (%"PRIu32"/33)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	newversion = get32bit(&data);
	copychunkid = get64bit(&data);
	copyversion = get32bit(&data);
	leng = get32bit(&data);
	status = get8bit(&data);
	if (newversion!=version) {
		chunk_got_chunkop_status(eptr->csid,chunkid,status);
	}
	if (copychunkid>0) {
		chunk_got_chunkop_status(eptr->csid,copychunkid,status);
	}
	if (status!=0) {
		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunkop(%016"PRIX64",%08"PRIX32",%08"PRIX32",%016"PRIX64",%08"PRIX32",%"PRIu32") status: %s",eptr->servstrip,eptr->servport,chunkid,version,newversion,copychunkid,copyversion,leng,mfsstrerr(status));
	}
}

void matocsserv_get_version(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;
	static const char vstring[] = VERSSTR;
	if (length!=0 && length!=4) {
		syslog(LOG_NOTICE,"ANTOAN_GET_VERSION - wrong size (%"PRIu32"/4|0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = matocsserv_createpacket(eptr,ANTOAN_VERSION,4+4+strlen(vstring));
		put32bit(&ptr,msgid);
	} else {
		ptr = matocsserv_createpacket(eptr,ANTOAN_VERSION,4+strlen(vstring));
	}
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	memcpy(ptr,vstring,strlen(vstring));
}

void matocsserv_get_config(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	char name[256];
	uint8_t nleng;
	uint32_t vleng;
	char *val;
	uint8_t *ptr;

	if (length<5) {
		syslog(LOG_NOTICE,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=5U+(uint32_t)nleng) {
		syslog(LOG_NOTICE,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	memcpy(name,data,nleng);
	name[nleng] = 0;
	val = cfg_getstr(name,"");
	vleng = strlen(val);
	if (vleng>255) {
		vleng=255;
	}
	ptr = matocsserv_createpacket(eptr,ANTOAN_CONFIG_VALUE,5+vleng);
	put32bit(&ptr,msgid);
	put8bit(&ptr,vleng);
	memcpy(ptr,val,vleng);
}

static uint32_t matocsserv_remap_ip(uint32_t csip) {
	if ((csip & RemapMask) == RemapSrc) {
		csip &= ~RemapMask;
		csip |= RemapDst;
	}
	return csip;
}

void matocsserv_register(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t chunkversion;
	uint32_t i,chunkcount;
	uint8_t rversion;
	uint16_t csid;
	double us,ts;

	if ((length&1)==0) {
		if (eptr->registered==REGISTERED) {
			syslog(LOG_WARNING,"got register message from registered chunk-server !!!");
			eptr->mode=KILL;
			return;
		}
		if (length<22 || ((length-22)%12)!=0) {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER (old ver.) - wrong size (%"PRIu32"/22+N*12)",length);
			eptr->mode=KILL;
			return;
		}
		passert(data);
		eptr->servip = get32bit(&data);
		eptr->servport = get16bit(&data);
		eptr->usedspace = get64bit(&data);
		eptr->totalspace = get64bit(&data);
		length-=22;
		rversion=0;
	} else {
		passert(data);
		rversion = get8bit(&data);

		if (eptr->registered==REGISTERED && rversion!=63) {
			syslog(LOG_WARNING,"got register message from registered chunk-server !!!");
			eptr->mode=KILL;
			return;
		}

#ifdef MFSDEBUG
		syslog(LOG_NOTICE,"got register packet: %u",rversion);
#endif
		if (rversion<=4) {
			syslog(LOG_NOTICE,"register packet version: %u",rversion);
		}
		if (rversion==1) {
			if (length<39 || ((length-39)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 1) - wrong size (%"PRIu32"/39+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			if (AuthCode) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 1) - authorization needed - packet version too old");
				eptr->mode=KILL;
				return;
			}
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			length-=39;
		} else if (rversion==2) {
			if (length<47 || ((length-47)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 2) - wrong size (%"PRIu32"/47+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			if (AuthCode) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 2) - authorization needed - packet version too old");
				eptr->mode=KILL;
				return;
			}
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			length-=47;
		} else if (rversion==3) {
			if (length<49 || ((length-49)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 3) - wrong size (%"PRIu32"/49+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			if (AuthCode) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 3) - authorization needed - packet version too old");
				eptr->mode=KILL;
				return;
			}
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->timeout = get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			length-=49;
		} else if (rversion==4) {
			if (length<53 || ((length-53)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 4) - wrong size (%"PRIu32"/53+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			if (AuthCode) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 4) - authorization needed - packet version too old");
				eptr->mode=KILL;
				return;
			}
			eptr->version = get32bit(&data);
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->timeout = get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			length-=53;
		} else if (rversion==50 || rversion==60) {
			if (rversion==50 && length!=13) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:BEGIN) - wrong size (%"PRIu32"/13)",length);
				eptr->mode=KILL;
				return;
			}
			if (rversion==60 && length!=55 && length!=71) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 6:BEGIN) - wrong size (%"PRIu32"/55|71)",length);
				eptr->mode=KILL;
				return;
			}
			if (AuthCode) {
					if (rversion==50) {
						syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:BEGIN) - authorization needed - packet version too old");
						eptr->mode=KILL;
						return;
					} else { // rversion==60
						if (length==55) { // no authorization data
							uint8_t *p;
							p = matocsserv_createpacket(eptr,MATOCS_MASTER_ACK,33);
							put8bit(&p,3);
							for (i=0 ; i<32 ; i++) {
								eptr->passwordrnd[i] = rndu8();
							}
							memcpy(p,eptr->passwordrnd,32);
							return;
						} else { // length==71
							if (matocsserv_check_password(eptr->passwordrnd,data)==0) {
								syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 6:BEGIN) - access denied - check password");
								eptr->mode = KILL;
								return;
							}
							data+=16;
						}
					}
				}
			eptr->version = get32bit(&data);
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			eptr->timeout = get16bit(&data);
			if (rversion==50) {
				if (eptr->timeout<10) {
					syslog(LOG_NOTICE,"CSTOMA_REGISTER communication timeout too small (%"PRIu16" seconds - should be at least 10 seconds)",eptr->timeout);
					eptr->mode=KILL;
					return;
				}
				csid = 0;
			} else { // rversion==60
				if (eptr->timeout==0) {
					eptr->timeout = DefaultTimeout;
				} else if (eptr->timeout<10) {
					syslog(LOG_NOTICE,"CSTOMA_REGISTER communication timeout too small (%"PRIu16" seconds - should be at least 10 seconds)",eptr->timeout);
					eptr->mode=KILL;
					return;
				}
				csid = get16bit(&data);
				eptr->usedspace = get64bit(&data);
				eptr->totalspace = get64bit(&data);
				eptr->chunkscount = get32bit(&data);
				eptr->todelusedspace = get64bit(&data);
				eptr->todeltotalspace = get64bit(&data);
				eptr->todelchunkscount = get32bit(&data);
			}
			if (eptr->servip==0) {
					tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
				}
				eptr->servip = matocsserv_remap_ip(eptr->servip);
				if (eptr->servstrip) {
					free(eptr->servstrip);
				}
				eptr->servstrip = matocsserv_makestrip(eptr->servip);
				if (((eptr->servip)&0xFF000000) == 0x7F000000) {
					syslog(LOG_NOTICE,"chunkserver connected using localhost (IP: %s) - you cannot use localhost for communication between chunkserver and master", eptr->servstrip);
					eptr->mode=KILL;
					return;
				}
			if ((eptr->csptr=csdb_new_connection(eptr->servip,eptr->servport,csid,eptr))==NULL) {
					syslog(LOG_WARNING,"can't accept chunkserver (ip: %s / port: %"PRIu16")",eptr->servstrip,eptr->servport);
					eptr->mode=KILL;
					return;
				}
			if (rversion==50) {
					syslog(LOG_NOTICE,"chunkserver register begin (packet version: 5) - ip: %s / port: %"PRIu16,eptr->servstrip,eptr->servport);
				} else {
					us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
					ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
					syslog(LOG_NOTICE,"chunkserver register begin (packet version: 6) - ip: %s / port: %"PRIu16", usedspace: %"PRIu64" (%.2lf GiB), totalspace: %"PRIu64" (%.2lf GiB)",eptr->servstrip,eptr->servport,eptr->usedspace,us,eptr->totalspace,ts);
				}
				if (eptr->version>=VERSION2INT(1,6,28)) { // if chunkserver version >= 1.6.28 then send back my version
					uint8_t *p;
					if (rversion==50) {
						p = matocsserv_createpacket(eptr,MATOCS_MASTER_ACK,5);
						put8bit(&p,0);
						put32bit(&p,VERSHEX);
					} else {
						uint8_t mode;
					mode = (eptr->version >= VERSION2INT(2,0,33))?1:0;
							p = matocsserv_createpacket(eptr,MATOCS_MASTER_ACK,mode?17:9);
							put8bit(&p,0);
							put32bit(&p,VERSHEX);
							put16bit(&p,eptr->timeout);
							put16bit(&p,csdb_get_csid(eptr->csptr));
							if (mode) {
								put64bit(&p,meta_get_id());
							}
				}
				}
				eptr->csid = chunk_server_connected(eptr);
			return;
		} else if (rversion==51 || rversion==61) {
			if (((length-1)%12)!=0) {
				if (rversion==51) {
					syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:CHUNKS) - wrong size (%"PRIu32"/1+N*12)",length);
				} else {
					syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 6:CHUNKS) - wrong size (%"PRIu32"/1+N*12)",length);
				}
				eptr->mode=KILL;
				return;
			}
			if (eptr->csptr==NULL) {
				if (rversion==51) {
					syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:CHUNKS) - CHUNKS packet before proper BEGIN packet");
				} else {
					syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 6:CHUNKS) - CHUNKS packet before proper BEGIN packet");
				}
				eptr->mode=KILL;
				return;
			}
			chunkcount = (length-1)/12;
			for (i=0 ; i<chunkcount ; i++) {
				chunkid = get64bit(&data);
				chunkversion = get32bit(&data);
				chunk_server_has_chunk(eptr->csid,chunkid,chunkversion);
			}
			if (eptr->version>=VERSION2INT(2,0,0) && rversion==61) {
				uint8_t *p;
//				if (eptr->version<VERSION2INT(2,0,0)) {
//					p = matocsserv_createpacket(eptr,MATOCS_MASTER_ACK,5);
//				} else {
					p = matocsserv_createpacket(eptr,MATOCS_MASTER_ACK,1);
//				}
				put8bit(&p,0);
//				if (eptr->version<VERSION2INT(2,0,0)) {
//					put32bit(&p,VERSHEX);
//				}
			}
			return;
		} else if (rversion==52) {
			if (length!=41) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:END) - wrong size (%"PRIu32"/41)",length);
				eptr->mode=KILL;
				return;
			}
			if (eptr->csptr==NULL) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 5:END) - END packet before proper BEGIN packet");
				eptr->mode=KILL;
				return;
			}
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
			ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
			syslog(LOG_NOTICE,"chunkserver register end (packet version: 5) - ip: %s / port: %"PRIu16", usedspace: %"PRIu64" (%.2lf GiB), totalspace: %"PRIu64" (%.2lf GiB)",eptr->servstrip,eptr->servport,eptr->usedspace,us,eptr->totalspace,ts);
			eptr->registered = REGISTERED;
			chunk_server_register_end(eptr->csid);
			return;
		} else if (rversion==62) {
			if (length!=1) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 6:END) - wrong size (%"PRIu32"/1)",length);
				eptr->mode=KILL;
				return;
			}
			if (eptr->csptr==NULL) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 6:END) - END packet before proper BEGIN packet");
				eptr->mode=KILL;
				return;
			}
			syslog(LOG_NOTICE,"chunkserver register end (packet version: 6) - ip: %s / port: %"PRIu16,eptr->servstrip,eptr->servport);
			eptr->registered = REGISTERED;
			chunk_server_register_end(eptr->csid);
		} else if (rversion==63) {
			if (length!=1) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 6:DISCONNECT) - wrong size (%"PRIu32"/1)",length);
				eptr->mode=KILL;
				return;
			}
			syslog(LOG_NOTICE,"chunkserver graceful disconnection (packet version: 6) - ip: %s / port: %"PRIu16,eptr->servstrip,eptr->servport);
			if (eptr->csptr!=NULL) {
				csdb_temporary_maintenance_mode(eptr->csptr);
			}
			eptr->mode=KILL;
		} else {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER - wrong version (%"PRIu8"/1..4)",rversion);
			eptr->mode=KILL;
			return;
		}
	}
	if (rversion<=4) {
		if (eptr->timeout<10) {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER communication timeout too small (%"PRIu16" seconds - should be at least 10 seconds)",eptr->timeout);
			if (eptr->timeout<3) {
				eptr->timeout=3;
			}
			return;
		}
		if (eptr->servip==0) {
			tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
		}
		eptr->servip = matocsserv_remap_ip(eptr->servip);
		if (eptr->servstrip) {
			free(eptr->servstrip);
		}
		eptr->servstrip = matocsserv_makestrip(eptr->servip);
		if (((eptr->servip)&0xFF000000) == 0x7F000000) {
			syslog(LOG_NOTICE,"chunkserver connected using localhost (IP: %s) - you cannot use localhost for communication between chunkserver and master", eptr->servstrip);
			eptr->mode=KILL;
			return;
		}
		if ((eptr->csptr=csdb_new_connection(eptr->servip,eptr->servport,0,eptr))==NULL) {
			syslog(LOG_WARNING,"can't accept chunkserver (ip: %s / port: %"PRIu16")",eptr->servstrip,eptr->servport);
			eptr->mode=KILL;
			return;
		}
		us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
		ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
		syslog(LOG_NOTICE,"chunkserver register - ip: %s / port: %"PRIu16", usedspace: %"PRIu64" (%.2lf GiB), totalspace: %"PRIu64" (%.2lf GiB)",eptr->servstrip,eptr->servport,eptr->usedspace,us,eptr->totalspace,ts);
//		eptr->creation = NULL;
//		eptr->setversion = NULL;
//		eptr->duplication = NULL;
		eptr->csid = chunk_server_connected(eptr);
		chunkcount = length/(8+4);
		for (i=0 ; i<chunkcount ; i++) {
			chunkid = get64bit(&data);
			chunkversion = get32bit(&data);
			chunk_server_has_chunk(eptr->csid,chunkid,chunkversion);
		}
		eptr->registered = REGISTERED;
		chunk_server_register_end(eptr->csid);
	}
}


void matocsserv_labels(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t i,l;

	if (length!=4) {
		syslog(LOG_NOTICE,"CSTOMA_LABELS - wrong size (%"PRIu32"/4)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	eptr->labelmask = get32bit(&data);
	if (eptr->labelstr!=NULL) {
		free(eptr->labelstr);
	}
	l = 0;
	for (i=0 ; i<(1+'Z'-'A') ; i++) {
		if (eptr->labelmask&(1U<<i)) {
			l++;
		}
	}
	if (l>0) {
		l = l*2;
	} else {
		l = 1;
	}
	eptr->labelstr = malloc(l);
	passert(eptr->labelstr);
	l = 0;
	for (i=0 ; i<(1+'Z'-'A') ; i++) {
		if (eptr->labelmask&(1U<<i)) {
			if (l>0) {
				eptr->labelstr[l++]=',';
			}
			eptr->labelstr[l++]='A'+i;
		}
	}
	eptr->labelstr[l]=0;
}

void matocsserv_space(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	if (length!=16 && length!=32 && length!=40) {
		syslog(LOG_NOTICE,"CSTOMA_SPACE - wrong size (%"PRIu32"/16|32|40)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	eptr->usedspace = get64bit(&data);
	eptr->totalspace = get64bit(&data);
	if (length==40) {
		eptr->chunkscount = get32bit(&data);
	}
	if (length>=32) {
		eptr->todelusedspace = get64bit(&data);
		eptr->todeltotalspace = get64bit(&data);
		if (length==40) {
			eptr->todelchunkscount = get32bit(&data);
		}
	}
}

void matocsserv_current_load(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	if (length!=4 &&length!=5) {
		syslog(LOG_NOTICE,"CSTOMA_CURRENT_LOAD - wrong size (%"PRIu32"/4|5)",length);
		eptr->mode=KILL;
		return;
	}
	passert(data);
	eptr->load = get32bit(&data);
	if (eptr->csptr) {
		csdb_server_load(eptr->csptr,eptr->load);
	}
	if (length==5) {
		eptr->hlstatus = get8bit(&data);
	}
}

void matocsserv_chunk_damaged(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t i;

	if (length%8!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_DAMAGED - wrong size (%"PRIu32"/N*8)",length);
		eptr->mode=KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/8 ; i++) {
		chunkid = get64bit(&data);
//		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk: %016"PRIX64" is damaged",eptr->servstrip,eptr->servport,chunkid);
		chunk_damaged(eptr->csid,chunkid);
	}
}

void matocsserv_chunks_lost(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t i;

	if (length%8!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_LOST - wrong size (%"PRIu32"/N*8)",length);
		eptr->mode=KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/8 ; i++) {
		chunkid = get64bit(&data);
//		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk lost: %016"PRIX64,eptr->servstrip,eptr->servport,chunkid);
		chunk_lost(eptr->csid,chunkid);
	}
}

void matocsserv_chunks_new(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t chunkversion;
	uint32_t i;

	if (length%12!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_NEW - wrong size (%"PRIu32"/N*12)",length);
		eptr->mode=KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/12 ; i++) {
		chunkid = get64bit(&data);
		chunkversion = get32bit(&data);
//		syslog(LOG_NOTICE,"(%s:%"PRIu16") chunk lost: %016"PRIX64,eptr->servstrip,eptr->servport,chunkid);
		chunk_server_has_chunk(eptr->csid,chunkid,chunkversion);
	}
}

void matocsserv_error_occurred(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CSTOMA_ERROR_OCCURRED - wrong size (%"PRIu32"/0)",length);
		eptr->mode=KILL;
		return;
	}
	eptr->errorcounter++;
}


uint8_t matocsserv_isvalid(void *e) {
	matocsserventry *eptr = (matocsserventry*)e;
	return (eptr->mode==KILL)?0:1;
}

void matocsserv_disconnection_finished(void *e) {
	matocsserventry *eptr = (matocsserventry*)e;
	syslog(LOG_NOTICE,"server ip: %s / port: %"PRIu16" has been fully removed from data structures",eptr->servstrip,eptr->servport);
	if (eptr->servstrip) {
		free(eptr->servstrip);
	}
	free(eptr);
}

void matocsserv_gotpacket(matocsserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	if (type!=CSTOMA_REGISTER && type!=ANTOAN_NOP && type!=ANTOAN_GET_VERSION && type!=ANTOAN_GET_CONFIG && eptr->csid==MAXCSCOUNT) {
		syslog(LOG_WARNING,"got command type %"PRIu32" from unregistered chunk server",type);
		eptr->mode = KILL;
		return;
	}
	switch (type) {
		case ANTOAN_NOP:
			break;
		case ANTOAN_UNKNOWN_COMMAND: // for future use
			break;
		case ANTOAN_BAD_COMMAND_SIZE: // for future use
			break;
		case ANTOAN_GET_VERSION:
			matocsserv_get_version(eptr,data,length);
			break;
		case ANTOAN_GET_CONFIG:
			matocsserv_get_config(eptr,data,length);
			break;
		case CSTOMA_REGISTER:
			matocsserv_register(eptr,data,length);
			break;
		case CSTOMA_SPACE:
			matocsserv_space(eptr,data,length);
			break;
		case CSTOMA_CURRENT_LOAD:
			matocsserv_current_load(eptr,data,length);
			break;
		case CSTOMA_CHUNK_DAMAGED:
			matocsserv_chunk_damaged(eptr,data,length);
			break;
		case CSTOMA_CHUNK_LOST:
			matocsserv_chunks_lost(eptr,data,length);
			break;
		case CSTOMA_CHUNK_NEW:
			matocsserv_chunks_new(eptr,data,length);
			break;
		case CSTOMA_ERROR_OCCURRED:
			matocsserv_error_occurred(eptr,data,length);
			break;
		case CSTOMA_LABELS:
			matocsserv_labels(eptr,data,length);
			break;
		case CSTOAN_CHUNK_CHECKSUM:
			matocsserv_got_chunk_checksum(eptr,data,length);
			break;
		case CSTOMA_CREATE:
			matocsserv_got_createchunk_status(eptr,data,length);
			break;
		case CSTOMA_DELETE:
			matocsserv_got_deletechunk_status(eptr,data,length);
			break;
		case CSTOMA_REPLICATE:
			matocsserv_got_replicatechunk_status(eptr,data,length);
			break;
		case CSTOMA_DUPLICATE:
			matocsserv_got_duplicatechunk_status(eptr,data,length);
			break;
		case CSTOMA_SET_VERSION:
			matocsserv_got_setchunkversion_status(eptr,data,length);
			break;
		case CSTOMA_TRUNCATE:
			matocsserv_got_truncatechunk_status(eptr,data,length);
			break;
		case CSTOMA_DUPTRUNC:
			matocsserv_got_duptruncchunk_status(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"master <-> chunkservers module: got unknown message (type:%"PRIu32")",type);
			eptr->mode=KILL;
	}
}

void matocsserv_read(matocsserventry *eptr,double now) {
	int32_t i;
	uint32_t type,leng;
	const uint8_t *ptr;
	uint32_t rbleng,rbpos;
	uint8_t err,hup;
	static uint8_t *readbuff = NULL;
	static uint32_t readbuffsize = 0;

	if (eptr == NULL) {
		if (readbuff != NULL) {
			free(readbuff);
		}
		readbuff = NULL;
		readbuffsize = 0;
		return;
	}

	if (readbuffsize==0) {
		readbuffsize = 65536;
		readbuff = malloc(readbuffsize);
		passert(readbuff);
	}

	rbleng = 0;
	err = 0;
	hup = 0;
	for (;;) {
		i = read(eptr->sock,readbuff+rbleng,readbuffsize-rbleng);
		if (i==0) {
			hup = 1;
			break;
		} else if (i<0) {
			if (ERRNO_ERROR) {
				err = 1;
			}
			break;
		} else {
			rbleng += i;
			if (rbleng==readbuffsize) {
				readbuffsize*=2;
				readbuff = mfsrealloc(readbuff,readbuffsize);
				passert(readbuff);
			} else {
				break;
			}
		}
	}

	if (rbleng>0) {
		eptr->lastread = now;
	}

	rbpos = 0;
	while (rbpos<rbleng) {
		if ((rbleng-rbpos)>=eptr->input_bytesleft) {
			memcpy(eptr->input_startptr,readbuff+rbpos,eptr->input_bytesleft);
			i = eptr->input_bytesleft;
		} else {
			memcpy(eptr->input_startptr,readbuff+rbpos,rbleng-rbpos);
			i = rbleng-rbpos;
		}
		rbpos += i;
		eptr->input_startptr+=i;
		eptr->input_bytesleft-=i;

		if (eptr->input_bytesleft>0) {
			break;
		}

		if (eptr->input_packet == NULL) {
			ptr = eptr->input_hdr;
			type = get32bit(&ptr);
			leng = get32bit(&ptr);

			if (leng>MaxPacketSize) {
				syslog(LOG_WARNING,"CS(%s) packet too long (%"PRIu32"/%u) ; command:%"PRIu32,eptr->servstrip,leng,MaxPacketSize,type);
				eptr->input_end = 1;
				return;
			}

			eptr->input_packet = malloc(offsetof(in_packetstruct,data)+leng);
			passert(eptr->input_packet);
			eptr->input_packet->next = NULL;
			eptr->input_packet->type = type;
			eptr->input_packet->leng = leng;

			eptr->input_startptr = eptr->input_packet->data;
			eptr->input_bytesleft = leng;
		}

		if (eptr->input_bytesleft>0) {
			continue;
		}

		if (eptr->input_packet != NULL) {
			*(eptr->inputtail) = eptr->input_packet;
			eptr->inputtail = &(eptr->input_packet->next);
			eptr->input_packet = NULL;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
		}
	}

	if (hup) {
		syslog(LOG_NOTICE,"connection with CS(%s) has been closed by peer",eptr->servstrip);
		eptr->input_end = 1;
	} else if (err) {
		mfs_arg_errlog_silent(LOG_NOTICE,"read from CS(%s) error",eptr->servstrip);
		eptr->input_end = 1;
	}
}

void matocsserv_parse(matocsserventry *eptr) {
	in_packetstruct *ipack;
	uint64_t starttime;
	uint64_t currtime;

	starttime = monotonic_useconds();
	currtime = starttime;
	while (eptr->mode==DATA && (ipack = eptr->inputhead)!=NULL && starttime+10000>currtime) {
		matocsserv_gotpacket(eptr,ipack->type,ipack->data,ipack->leng);
		eptr->inputhead = ipack->next;
		free(ipack);
		if (eptr->inputhead==NULL) {
			eptr->inputtail = &(eptr->inputhead);
		} else {
			currtime = monotonic_useconds();
		}
	}
	if (eptr->mode==DATA && eptr->inputhead==NULL && eptr->input_end) {
		eptr->mode = KILL;
	}
}

void matocsserv_write(matocsserventry *eptr,double now) {
	out_packetstruct *opack;
	int32_t i;
#ifdef HAVE_WRITEV
	struct iovec iovtab[100];
	uint32_t iovdata;
	uint32_t leng;
	uint32_t left;

	for (;;) {
		leng = 0;
		for (iovdata=0,opack=eptr->outputhead ; iovdata<100 && opack!=NULL ; iovdata++,opack=opack->next) {
			iovtab[iovdata].iov_base = opack->startptr;
			iovtab[iovdata].iov_len = opack->bytesleft;
			leng += opack->bytesleft;
		}
		if (iovdata==0) {
			return;
		}
		i = writev(eptr->sock,iovtab,iovdata);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_arg_errlog_silent(LOG_NOTICE,"write to CS(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		left = i;
		while (left>0 && eptr->outputhead!=NULL) {
			opack = eptr->outputhead;
			if (opack->bytesleft>left) {
				opack->startptr+=left;
				opack->bytesleft-=left;
				left = 0;
			} else {
				left -= opack->bytesleft;
				eptr->outputhead = opack->next;
				if (eptr->outputhead==NULL) {
					eptr->outputtail = &(eptr->outputhead);
				}
				free(opack);
			}
		}
		if ((uint32_t)i < leng) {
			return;
		}
	}
#else
	for (;;) {
		opack = eptr->outputhead;
		if (opack==NULL) {
			return;
		}
		i=write(eptr->sock,opack->startptr,opack->bytesleft);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_arg_errlog_silent(LOG_NOTICE,"write to CS(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		opack->startptr+=i;
		opack->bytesleft-=i;
		if (opack->bytesleft>0) {
			return;
		}
		eptr->outputhead = opack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(opack);
	}
#endif
}

void matocsserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	int events;
	matocsserventry *eptr;
	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		events = 0;
		if (eptr->mode!=KILL && eptr->input_end==0) {
			events |= POLLIN;
		}
		if (eptr->mode!=KILL && eptr->outputhead!=NULL) {
			events |= POLLOUT;
		}
		if (events) {
			pdesc[pos].events = events;
			pdesc[pos].fd = eptr->sock;
			eptr->pdescpos = pos;
			pos++;
		} else {
			eptr->pdescpos = -1;
		}
	}
	*ndesc = pos;
}

void matocsserv_disconnection_loop(void) {
	matocsserventry *eptr,**kptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;

	kptr = &matocsservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			double us,ts;
			us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
			ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
			syslog(LOG_NOTICE,"chunkserver disconnected - ip: %s / port: %"PRIu16", usedspace: %"PRIu64" (%.2lf GiB), totalspace: %"PRIu64" (%.2lf GiB)",eptr->servstrip,eptr->servport,eptr->usedspace,us,eptr->totalspace,ts);
			matocsserv_replication_disconnected(eptr);
			if (eptr->csid!=MAXCSCOUNT) {
				chunk_server_disconnected(eptr->csid);
			}
			csdb_lost_connection(eptr->csptr);
			tcpclose(eptr->sock);
			if (eptr->input_packet) {
				free(eptr->input_packet);
			}
			ipptr = eptr->inputhead;
			while (ipptr) {
				ipaptr = ipptr;
				ipptr = ipptr->next;
				free(ipaptr);
			}
			opptr = eptr->outputhead;
			while (opptr) {
				opaptr = opptr;
				opptr = opptr->next;
				free(opaptr);
			}
			*kptr = eptr->next;
			eptr->next = NULL;
			// if server has csid then do not free it here - it'll be freed after cleanup in chunk module - see matocsserv_disconnection_finished
			if (eptr->csid==MAXCSCOUNT) {
				if (eptr->servstrip) {
					free(eptr->servstrip);
				}
				free(eptr);
			}
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matocsserv_serve(struct pollfd *pdesc) {
	double now;
	uint32_t peerip;
	matocsserventry *eptr;
	int ns;
	static double lastaction = 0.0;
	double timeoutadd;

	now = monotonic_seconds();
// timeout fix
	if (lastaction>0.0) {
		timeoutadd = now-lastaction;
		if (timeoutadd>1.0) { // more than one second passed - then fix 'timeout' timestamps
			for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
				eptr->lastread += timeoutadd;
			}
		}
	}
	lastaction = now;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
//	if (FD_ISSET(lsock,rset)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"Master<->CS socket: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(matocsserventry));
			passert(eptr);
			eptr->next = matocsservhead;
			matocsservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			eptr->mode = DATA;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
			eptr->input_end = 0;
			eptr->input_packet = NULL;
			eptr->inputhead = NULL;
			eptr->inputtail = &(eptr->inputhead);
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);

			tcpgetpeer(eptr->sock,&peerip,NULL);
			eptr->servstrip = matocsserv_makestrip(peerip);
			eptr->version = 0;
			eptr->servip = 0;
			eptr->servport = 0;
			eptr->timeout = DefaultTimeout;
			eptr->load = 0;
			eptr->hlstatus = HLSTATUS_DEFAULT;
			eptr->usedspace = 0;
			eptr->totalspace = 0;
			eptr->chunkscount = 0;
			eptr->todelusedspace = 0;
			eptr->todeltotalspace = 0;
			eptr->todelchunkscount = 0;
			eptr->errorcounter = 0;
			eptr->writecounter = 0;
			eptr->rrepcounter = 0;
			eptr->wrepcounter = 0;
			eptr->delcounter = 0;

			eptr->labelmask = 0;
			eptr->labelstr = NULL;

			eptr->create_total_counter = 0;
			eptr->rrep_total_counter = 0;
			eptr->wrep_total_counter = 0;
			eptr->del_total_counter = 0;
			eptr->total_counter_begin = monotonic_seconds();

			eptr->csid = MAXCSCOUNT;
			eptr->registered = UNREGISTERED;

			memset(eptr->passwordrnd,0,32);

			eptr->dist = 0;
			eptr->first = 1;
			eptr->corr = 0.0;

			eptr->csptr = NULL;
		}
	}

// read
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
//		syslog(LOG_NOTICE,"server: %s:%u ; lastread: %.6lf ; lastwrite: %.6lf ; timeout: %u ; now: %.6lf",eptr->servstrip,eptr->servport,eptr->lastread,eptr->lastwrite,eptr->timeout,now);
		if (eptr->pdescpos>=0) {
			if ((pdesc[eptr->pdescpos].revents & (POLLERR|POLLIN))==POLLIN && eptr->mode!=KILL) {
				matocsserv_read(eptr,now);
			}
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->input_end = 1;
			}
		}
		matocsserv_parse(eptr);
	}

	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		if ((eptr->lastwrite+(eptr->timeout/3.0))<now && eptr->outputhead==NULL) {
			matocsserv_createpacket(eptr,ANTOAN_NOP,0);
		}
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				matocsserv_write(eptr,now);
			}
		}
		if ((eptr->lastread+eptr->timeout)<now) {
			syslog(LOG_NOTICE,"connection with %s:%u timed out",eptr->servstrip,eptr->servport);
			eptr->mode = KILL;
		}
		if (eptr->mode==FINISH && eptr->outputhead==NULL) {
			eptr->mode = KILL;
		}
	}
	matocsserv_disconnection_loop();
}

void matocsserv_keep_alive(void) {
	double now;
	matocsserventry *eptr;

	now = monotonic_seconds();
// read
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode == DATA && eptr->input_end==0) {
			matocsserv_read(eptr,now);
		}
	}
// write
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		if ((eptr->lastwrite+(eptr->timeout/3.0))<now && eptr->outputhead==NULL) {
			matocsserv_createpacket(eptr,ANTOAN_NOP,0);
		}
		if (eptr->mode == DATA && eptr->outputhead) {
			matocsserv_write(eptr,now);
		}
	}
}

void matocsserv_close_lsock(void) { // after fork
	if (lsock>=0) {
		close(lsock);
	}
}

void matocsserv_term(void) {
	matocsserventry *eptr,*eaptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;
	syslog(LOG_INFO,"master <-> chunkservers module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matocsservhead;
	while (eptr) {
		if (eptr->input_packet) {
			free(eptr->input_packet);
		}
		ipptr = eptr->inputhead;
		while (ipptr) {
			ipaptr = ipptr;
			ipptr = ipptr->next;
			free(ipaptr);
		}
		opptr = eptr->outputhead;
		while (opptr) {
			opaptr = opptr;
			opptr = opptr->next;
			free(opaptr);
		}
		if (eptr->servstrip) {
			free(eptr->servstrip);
		}
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matocsservhead = NULL;

	matocsserv_read(NULL,0.0); // free internal read buffer

	free(ListenHost);
	free(ListenPort);
}

int matocsserv_no_more_pending_jobs(void) {
	matocsserventry *eptr;
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->outputhead!=NULL) {
			return 0;
		}
		if ((eptr->rrepcounter | eptr->wrepcounter | eptr->delcounter)!=0) {
			return 0;
		}
	}
	return 1;
}


uint8_t matocsserv_parse_ip(const char *ipstr,uint32_t *ipnum) {
	uint32_t ip,octet,i;
	ip=0;
	while (*ipstr==' ' || *ipstr=='\t') {
		ipstr++;
	}
	for (i=0 ; i<4; i++) {
		if (*ipstr>='0' && *ipstr<='9') {
			octet=0;
			while (*ipstr>='0' && *ipstr<='9') {
				octet*=10;
				octet+=*ipstr-'0';
				ipstr++;
				if (octet>255) {
					return 0;
				}
			}
		} else {
			return 0;
		}
		if (i<3) {
			if (*ipstr!='.') {
				return 0;
			}
			ipstr++;
		} else {
			if (*ipstr!=0 && *ipstr!=' ' && *ipstr!='\t' && *ipstr!='\r' && *ipstr!='\n') {
				return 0;
			}
		}
		ip*=256;
		ip+=octet;
	}
	*ipnum = ip;
	return 1;
}

void matocsserv_reload_common(void) {
	uint8_t bits,err;
	char *srcip,*dstip;
	uint32_t srcclass,dstclass,mask;
	char *reservespace;
	const char *endptr;
	double reservespacevalue;

	DefaultTimeout = cfg_getuint32("MATOCS_TIMEOUT",10);
	if (DefaultTimeout>65535) {
		DefaultTimeout=65535;
	} else if (DefaultTimeout<10) {
		DefaultTimeout=10;
	}

	if (AuthCode) {
		free(AuthCode);
		AuthCode = NULL;
	}
	if (cfg_isdefined("AUTH_CODE")) {
		AuthCode = cfg_getstr("AUTH_CODE","");
	}

	reservespace = cfg_getstr("RESERVE_SPACE","0");
	reservespacevalue = sizestrtod(reservespace,&endptr);
	if (endptr[0]=='\0' || (endptr[0]=='B' && endptr[1]=='\0')) {
		ReserveSpaceValue = reservespacevalue;
		ReserveSpaceMode = RESERVE_BYTES;
	} else if (endptr[0]=='%' && endptr[1]=='\0') {
		ReserveSpaceValue = reservespacevalue;
		ReserveSpaceMode = RESERVE_PERCENT;
	} else if (endptr[0]=='U' && endptr[1]=='\0') {
		ReserveSpaceValue = reservespacevalue;
		ReserveSpaceMode = RESERVE_CHUNKSERVER_USED;
	} else if (endptr[0]=='C' && endptr[1]=='\0') {
		ReserveSpaceValue = reservespacevalue;
		ReserveSpaceMode = RESERVE_CHUNKSERVER_TOTAL;
	} else {
		mfs_arg_syslog(LOG_WARNING,"error parsing RESERVE_SPACE (\"%s\") ; error on '%c'",reservespace,endptr[0]);
	}
	free(reservespace);

	if (cfg_isdefined("REMAP_BITS") && cfg_isdefined("REMAP_SOURCE_IP_CLASS") && cfg_isdefined("REMAP_DESTINATION_IP_CLASS")) {
		bits = cfg_getuint8("REMAP_BITS",24);
		srcip = cfg_getstr("REMAP_SOURCE_IP_CLASS","0.0.0.0");
		dstip = cfg_getstr("REMAP_DESTINATION_IP_CLASS","0.0.0.0");
		err = 0;
		if (bits==0 || bits>32) {
			mfs_arg_syslog(LOG_WARNING,"wrong value for REMAP_BITS (%"PRIu8" ; shlould be between 1 and 32)",bits);
			err |= 1;
			mask = 0;
		} else {
			mask = UINT32_C(0xFFFFFFFF) << (32-bits);
		}
		if (matocsserv_parse_ip(srcip,&srcclass)==0) {
			mfs_arg_syslog(LOG_WARNING,"error parsing ip class from REMAP_SOURCE_IP_CLASS (%s)",srcip);
			err |= 2;
		}
		if (matocsserv_parse_ip(dstip,&dstclass)==0) {
			mfs_arg_syslog(LOG_WARNING,"error parsing ip class from REMAP_DESTINATION_IP_CLASS (%s)",dstip);
			err |= 4;
		}
		if ((err&3)==0 && ((srcclass & mask) != srcclass)) {
			char *maskedip;
			maskedip = matocsserv_makestrip(srcclass & mask);
			mfs_arg_syslog(LOG_WARNING,"found garbage bits at the end of REMAP_SOURCE_IP_CLASS (given: %s - should be: %s)",srcip,maskedip);
			free(maskedip);
			err |= 8;
		}
		if ((err&5)==0 && ((dstclass & mask) != dstclass)) {
			char *maskedip;
			maskedip = matocsserv_makestrip(dstclass & mask);
			mfs_arg_syslog(LOG_WARNING,"found garbage bits at the end of REMAP_DESTINATION_IP_CLASS (given: %s - should be: %s)",dstip,maskedip);
			free(maskedip);
			err |= 16;
		}
		if (err==0) {
			RemapMask = mask;
			RemapSrc = srcclass;
			RemapDst = dstclass;
		}
		free(srcip);
		free(dstip);
	} else {
		RemapMask = 0;
		RemapSrc = 0;
		RemapDst = 0;
	}
}

void matocsserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	int newlsock;

	matocsserv_reload_common();

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("MATOCS_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOCS_LISTEN_PORT",DEFAULT_MASTER_CS_PORT);
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_arg_syslog(LOG_NOTICE,"master <-> chunkservers module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_errlog(LOG_WARNING,"master <-> chunkservers module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpstrlisten(newlsock,ListenHost,ListenPort,100)<0) {
		mfs_arg_errlog(LOG_ERR,"master <-> chunkservers module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"master <-> chunkservers module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"master <-> chunkservers module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

int matocsserv_init(void) {
	matocsserv_reload_common();

	ListenHost = cfg_getstr("MATOCS_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOCS_LISTEN_PORT",DEFAULT_MASTER_CS_PORT);

	lsock = tcpsocket();
	if (lsock<0) {
		mfs_errlog(LOG_ERR,"master <-> chunkservers module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
		mfs_arg_errlog(LOG_ERR,"master <-> chunkservers module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"master <-> chunkservers module: can't set accept filter");
	}
	mfs_arg_syslog(LOG_NOTICE,"master <-> chunkservers module: listen on %s:%s",ListenHost,ListenPort);

	matocsserv_replication_init();
	matocsservhead = NULL;
	main_reload_register(matocsserv_reload);
	main_destruct_register(matocsserv_term);
	main_poll_register(matocsserv_desc,matocsserv_serve);
	main_keepalive_register(matocsserv_keep_alive);
	main_info_register(matocsserv_log_extra_info);
	main_time_register(1,0,matocsserv_hlstatus_fix);
	main_time_register(1,0,matocsserv_calculate_space);
//	main_time_register(TIMEMODE_SKIP_LATE,60,0,matocsserv_status);
	return 0;
}
