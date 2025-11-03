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
#include "mfslog.h"
#include "massert.h"
#include "mfsstrerr.h"
#include "hashfn.h"
#include "clocks.h"
#include "storageclass.h"
#include "labelparser.h"
#include "multilan.h"
#include "md5.h"
#include "bitops.h"
#include "mfsalloc.h"

#define MaxPacketSize CSTOMA_MAXPACKETSIZE

#define MANAGER_SWITCH_CONST 5

#define COMBINE_CHUNKID_AND_ECID(chunkid,ecid) (((chunkid)&UINT64_C(0x00FFFFFFFFFFFFFF))|(((uint64_t)(ecid))<<56))

#define GET_CHUNKID_AND_ECID(buff,chunkid,ecid) chunkid=get64bit(buff);(ecid)=(chunkid)>>56;(chunkid)&=UINT64_C(0x00FFFFFFFFFFFFFF)

#define PUT_CHUNKID_AND_ECID(buff,chunkid,ecid) put64bit(buff,COMBINE_CHUNKID_AND_ECID(chunkid,ecid))

#define FULL_REPLICATION_WEIGHT 8
#define EC_REPLICATION_WEIGHT 4
#define LOCALPART_REPLICATION_WEIGHT 1

#define NEWCHUNKDELAY 5
#define LOSTCHUNKDELAY 5

#define SOMETHING_OVER_ANY_LIMIT 10000000

// ReserveSpaceMode
enum{RESERVE_BYTES,RESERVE_PERCENT,RESERVE_CHUNKSERVER_USED,RESERVE_CHUNKSERVER_TOTAL};

// matocsserventry.mode
enum{KILL,DATA,FINISH};

enum{UNREGISTERED,WAITING,REGISTERED};

static const char *replreasons[REPL_REASONS]={REPL_REASONS_STRINGS};
static const char *opreasons[OP_REASONS]={OP_REASONS_STRINGS};

struct csdbentry;

/*

#define CC_CELL_SIZE 4
#define CC_TAB_SIZE 512

typedef struct check_chunk_cell {
	uint64_t chunkid[4];
} check_chunk_cell;
*/

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

	char *servdesc;			// string version of ip and port X.X.X.X:N
	uint32_t peerip;		// original peer ip (before all remaps)
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

	uint8_t lostchunkdelay;
	uint8_t newchunkdelay;
	uint8_t receivingchunks;

	uint8_t passwordrnd[32];

	uint32_t lreplreadok[REPL_REASONS];
	uint32_t lreplreaderr[REPL_REASONS];
	uint32_t lreplwriteok[REPL_REASONS];
	uint32_t lreplwriteerr[REPL_REASONS];
	uint32_t ldelok[OP_REASONS];
	uint32_t ldelerr[OP_REASONS];

	uint32_t replreadok[REPL_REASONS];
	uint32_t replreaderr[REPL_REASONS];
	uint32_t replwriteok[REPL_REASONS];
	uint32_t replwriteerr[REPL_REASONS];
	uint32_t delok[OP_REASONS];
	uint32_t delerr[OP_REASONS];

//	check_chunk_cell check_chunk_tab[CC_TAB_SIZE];

	uint32_t dist;
	uint8_t first;
	double corr;

	struct csdbentry *csptr;

	struct matocsserventry *next;
} matocsserventry;

static matocsserventry *matocsservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

static uint8_t receivingchunks;

static uint32_t gusagediff = 0;
static uint64_t gtotalspace = 0;
static uint64_t gusedspace = 0;
static uint64_t gavailspace = 0;
static uint64_t gfreespace = 0;

static uint16_t valid_servers_count;
static uint16_t almostfull_servers_count;
static uint16_t replallowed_servers_count;


// from config
static char *ListenHost;
static char *ListenPort;
static uint32_t listenip;
static uint16_t listenport;
static uint32_t DefaultTimeout;
static uint32_t ForceTimeout;
static char *AuthCode = NULL;
static uint32_t RemapMask = 0;
static uint32_t RemapSrc = 0;
static uint32_t RemapDst = 0;
static double ReserveSpaceValue = 0.0;
static uint8_t ReserveSpaceMode = RESERVE_BYTES;
static uint8_t ChunkServerCheck;

/* op DB */

#define OPHASHSIZE 256
#define OPHASHFN(chid) (((chid)^((chid)>>8))%(OPHASHSIZE))

enum {
	OPTYPE_DELETE,
	OPTYPE_CREATE,
	OPTYPE_SETVERSION,
	OPTYPE_TRUNCATE,
	OPTYPE_DUPLICATE,
	OPTYPE_DUPTRUNC
};

const char* optype_str[] = {
	"DELETE",
	"CREATE",
	"SETVERSION",
	"TRUNCATE",
	"DUPLICATE",
	"DUPTRUNC"
};

typedef struct _opsrv {
	uint64_t chunkid;
	uint32_t version;
	uint8_t optype;
	uint8_t reason;
	void *srv;
	struct _opsrv *next;
} opsrv;

static opsrv *ophash[OPHASHSIZE];
static opsrv *opsrvfreehead=NULL;

opsrv* matocsserv_opsrv_malloc(void) {
	opsrv *r;
	if (opsrvfreehead) {
		r = opsrvfreehead;
		opsrvfreehead = r->next;
	} else {
		r = (opsrv*)malloc(sizeof(opsrv));
		passert(r);
	}
	return r;
}

void matocsserv_opsrv_free(opsrv *r) {
	r->next = opsrvfreehead;
	opsrvfreehead = r;
}

void matocssrv_operation_init(void) {
	uint32_t hash;
	for (hash=0 ; hash<OPHASHSIZE ; hash++) {
		ophash[hash] = NULL;
	}
	opsrvfreehead = NULL;
}

int matocsserv_operation_find(uint64_t chunkid,void *srv) {
	uint32_t hash = OPHASHFN(chunkid);
	opsrv *r;
	for (r=ophash[hash] ; r ; r=r->next) {
		if (r->chunkid==chunkid && r->srv==srv) {
			return 1;
		}
	}
	return 0;
}

void matocsserv_operation_begin(uint64_t chunkid,uint32_t version,void *srv,uint8_t optype,uint8_t reason) {
	uint32_t hash = OPHASHFN(chunkid);
	opsrv *r;

	r = matocsserv_opsrv_malloc();
	r->chunkid = chunkid;
	r->version = version;
	r->srv = srv;
	r->optype = optype;
	r->reason = reason;
	r->next = ophash[hash];
	ophash[hash] = r;
	if (optype==OPTYPE_DELETE) {
		((matocsserventry *)(srv))->delcounter++;
		((matocsserventry *)(srv))->del_total_counter++;
	}
}

void matocsserv_operation_end(uint64_t chunkid,void *srv,uint8_t ok) {
	uint32_t hash = OPHASHFN(chunkid);
	opsrv *r,**rp;

	rp = &(ophash[hash]);
	while ((r=*rp)!=NULL) {
		if (r->chunkid==chunkid && r->srv==srv) {
			if (r->optype==OPTYPE_DELETE) {
				((matocsserventry *)(srv))->delcounter--;
				if (ok) {
					((matocsserventry *)(srv))->delok[r->reason]++;
				} else {
					((matocsserventry *)(srv))->delerr[r->reason]++;
				}
			}
			*rp = r->next;
			matocsserv_opsrv_free(r);
		} else {
			rp = &(r->next);
		}
	}
}

void matocsserv_operation_info(FILE *fd) {
	uint32_t hash;
	opsrv *r;
	matocsserventry *e;
	for (hash=0 ; hash<OPHASHSIZE ; hash++) {
		for (r=ophash[hash] ; r ; r=r->next) {
			fprintf(fd,"operation %s : chunk %"PRIX64"_%"PRIX32" ; reason: %s ; server: ",optype_str[r->optype],r->chunkid,r->version,opreasons[r->reason]);
			e = (matocsserventry*)(r->srv);
			fprintf(fd,"%s\n",e->servdesc);
		}
	}
}

void matocsserv_operation_disconnected(void *srv) {
	uint32_t hash;
	opsrv *r,**rp;

	for (hash=0 ; hash<OPHASHSIZE ; hash++) {
		rp = &(ophash[hash]);
		while ((r=*rp)!=NULL) {
			if (r->srv==srv) {
				if (r->optype==OPTYPE_DELETE) {
					((matocsserventry *)(srv))->delcounter--;
					((matocsserventry *)(srv))->delerr[r->reason]++;
				}
				*rp = r->next;
				matocsserv_opsrv_free(r);
			} else {
				rp = &(r->next);
			}
		}
	}
}

/* replications DB */

#define REPHASHSIZE 256
#define REPHASHFN(chid,ver) (((chid)^(ver)^((chid)>>8))%(REPHASHSIZE))

enum {
	REPTYPE_SIMPLE,
	REPTYPE_SPLIT,
	REPTYPE_RECOVER,
	REPTYPE_JOIN,
	REPTYPE_LOCALSPLIT
};

const char* reptype_str[] = {
	"SIMPLE",
	"SPLIT",
	"RECOVER",
	"JOIN",
	"LOCALSPLIT"
};

typedef struct _repsrc {
	void *src;
	struct _repsrc *next;
} repsrc;

typedef struct _repdst {
	uint64_t chunkid;
	uint32_t version;
	uint8_t rweight;
	uint8_t wweight;
	uint8_t reptype;
	uint8_t reason;
	void *dst;
	repsrc *srchead;
	struct _repdst *next;
} repdst;

static repdst* rephash[REPHASHSIZE];
static repsrc *repsrcfreehead=NULL;
static repdst *repdstfreehead=NULL;

repsrc* matocsserv_repsrc_malloc(void) {
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

repdst* matocsserv_repdst_malloc(void) {
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
		rephash[hash] = NULL;
	}
	repsrcfreehead = NULL;
	repdstfreehead = NULL;
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

void matocsserv_replication_begin(uint64_t chunkid,uint32_t version,void *dst,uint8_t srccnt,void **src,uint8_t rweight,uint8_t wweight,uint8_t reptype,uint8_t reason) {
	uint32_t hash = REPHASHFN(chunkid,version);
	uint8_t i;
	repdst *r;
	repsrc *rs;

	if (srccnt>0) {
		r = matocsserv_repdst_malloc();
		r->chunkid = chunkid;
		r->version = version;
		r->dst = dst;
		r->rweight = rweight;
		r->wweight = wweight;
		r->reptype = reptype;
		r->reason = reason;
		r->srchead = NULL;
		r->next = rephash[hash];
		rephash[hash] = r;
		for (i=0 ; i<srccnt ; i++) {
			rs = matocsserv_repsrc_malloc();
			rs->src = src[i];
			rs->next = r->srchead;
			r->srchead = rs;
			((matocsserventry *)(src[i]))->rrepcounter+=rweight;
			((matocsserventry *)(src[i]))->rrep_total_counter++;
		}
		((matocsserventry *)(dst))->wrepcounter+=wweight;
		((matocsserventry *)(dst))->wrep_total_counter++;
	}
}

void matocsserv_replication_end(uint64_t chunkid,uint32_t version,void *dst,uint8_t ok) {
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
				((matocsserventry *)(rsdel->src))->rrepcounter-=r->rweight;
				if (ok) {
					((matocsserventry *)(rsdel->src))->replreadok[r->reason]+=r->rweight;
				} else {
					((matocsserventry *)(rsdel->src))->replreaderr[r->reason]+=r->rweight;
				}
				matocsserv_repsrc_free(rsdel);
			}
			((matocsserventry *)(dst))->wrepcounter-=r->wweight;
			if (ok) {
				((matocsserventry *)(dst))->replwriteok[r->reason]+=r->wweight;
			} else {
				((matocsserventry *)(dst))->replwriteerr[r->reason]+=r->wweight;
			}
			*rp = r->next;
			matocsserv_repdst_free(r);
		} else {
			rp = &(r->next);
		}
	}
}

uint32_t matocsserv_replication_print(char *buff,uint32_t bleng,uint64_t chunkid,uint32_t version,void *dst,uint8_t *reptype) {
	uint32_t hash = REPHASHFN(chunkid,version);
	repdst *r;
	repsrc *rs;
	uint32_t leng;

	leng = 0;
	*reptype = 0;
	for (r=rephash[hash] ; r ; r=r->next) {
		if (r->chunkid==chunkid && r->version==version && r->dst==dst) {
			for (rs=r->srchead ; rs ; rs=rs->next) {
				if (leng>0 && leng<bleng) {
					buff[leng++] = ',';
				}
				if (leng<bleng) {
					leng += snprintf(buff+leng,bleng-leng,"%s",((matocsserventry *)(rs->src))->servdesc);
				}
			}
			if (leng<bleng) {
				leng += snprintf(buff+leng,bleng-leng," -> %s",((matocsserventry *)dst)->servdesc);
			}
			*reptype = r->reptype;
			return leng;
		}
	}
	return 0;
}

void matocsserv_replication_info(FILE *fd) {
	uint32_t hash;
	repdst *r;
	repsrc *rs;
	matocsserventry *e;
	for (hash=0 ; hash<REPHASHSIZE ; hash++) {
		for (r=rephash[hash] ; r ; r=r->next) {
			fprintf(fd,"operation REPLICATE_%s : chunk %"PRIX64"_%"PRIX32" ; reason: %s ; servers: ",reptype_str[r->reptype],r->chunkid,r->version,replreasons[r->reason]);
			for (rs=r->srchead ; rs ; rs=rs->next) {
				e = (matocsserventry*)(rs->src);
				fprintf(fd,"%s",e->servdesc);
				if (rs->next) {
					fprintf(fd,",");
				}
			}
			e = (matocsserventry*)(r->dst);
			fprintf(fd," -> %s\n",e->servdesc);
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
					((matocsserventry *)(rsdel->src))->rrepcounter-=r->rweight;
					((matocsserventry *)(rsdel->src))->replreaderr[r->reason]+=r->rweight;
					matocsserv_repsrc_free(rsdel);
				}
				((matocsserventry *)(srv))->wrepcounter-=r->wweight;
				((matocsserventry *)(srv))->replwriteerr[r->reason]+=r->wweight;
				*rp = r->next;
				matocsserv_repdst_free(r);
			} else {
				rsp = &(r->srchead);
				while ((rs=*rsp)!=NULL) {
					if (rs->src==srv) {
						((matocsserventry *)(srv))->rrepcounter-=r->rweight;
						((matocsserventry *)(srv))->replreaderr[r->reason]+=r->rweight;
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

static const char* matocsserv_ecid_to_str(uint8_t ecid) {
	const char* ecid8names[17] = {
		" (DE0)"," (DE1)"," (DE2)"," (DE3)"," (DE4)"," (DE5)"," (DE6)"," (DE7)",
		" (CE0)"," (CE1)"," (CE2)"," (CE3)"," (CE4)"," (CE5)"," (CE6)"," (CE7)"," (CE8)"
	};
	const char* ecid4names[13] = {
		" (DF0)"," (DF1)"," (DF2)"," (DF3)",
		" (CF0)"," (CF1)"," (CF2)"," (CF3)"," (CF4)"," (CF5)"," (CF6)"," (CF7)"," (CF8)"
	};

	if (ecid&0x20) {
		if ((ecid&0x1F)<17) {
			return ecid8names[ecid&0x1F];
		}
	} else if (ecid&0x10) {
		if ((ecid&0xF)<13) {
			return ecid4names[ecid&0xF];
		}
	} else if (ecid==0) {
		return " (COPY)";
	}
	return " (\?\?\?)";
}

/*
// add element
static void matocsserv_chunk_status_add(matocsserventry *eptr,uint64_t chunkid) {
	uint32_t hash;
	uint32_t i;
	check_chunk_cell *cc;

	hash = hash6432(chunkid)%CC_TAB_SIZE;
	cc = eptr->check_chunk_tab+hash;
	for (i=CC_CELL_SIZE-1 ; i>0 ; i--) {
		cc->chunkid[i] = cc->chunkid[i-1];
	}
	cc->chunkid[0] = chunkid;
}

// check and remove
static uint8_t matocsserv_chunk_status_check(matocsserventry *eptr,uint64_t chunkid) {
	uint32_t hash;
	uint32_t i,j;
	check_chunk_cell *cc;

	hash = hash6432(chunkid)%CC_TAB_SIZE;
	cc = eptr->check_chunk_tab+hash;
	j = 0;
	for (i=0 ; i<CC_CELL_SIZE ; i++) {
		if (cc->chunkid[i]==chunkid) {
			j++;
		}
		if (i<j) {
			if (j<CC_CELL_SIZE) {
				cc->chunkid[i] = cc->chunkid[j];
			} else {
				cc->chunkid[i] = 0;
			}
		}
		j++;
	}
	return (i<j)?1:0;
}

static void matocsserv_chunk_status_remove(matocsserventry *eptr,uint64_t chunkid) {
	matocsserv_chunk_status_check(eptr,chunkid);
}

// remove all
static void matocsserv_chunk_status_cleanup(matocsserventry *eptr) {
	uint32_t hash;
	uint32_t i;
	check_chunk_cell *cc;

	for (hash=0 ; hash<CC_TAB_SIZE ; hash++) {
		cc = eptr->check_chunk_tab+hash;
		for (i=0 ; i<CC_CELL_SIZE ; i++) {
			cc->chunkid[i] = 0;
		}
	}
}

---- */

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

void matocsserv_log_extra_info(FILE *fd) {
	matocsserventry *eptr;
	double dur,usage;
	const char *hlstatus_name;
	uint8_t overloaded;
	uint8_t maintained;
	uint8_t i;
	uint32_t now = main_time();
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->csptr!=NULL) {
			fprintf(fd,"[chunkserver %s]\n",eptr->servdesc);
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
				case HLSTATUS_LSREBALANCE:
					hlstatus_name = "LSREBALANCE";
					break;
				case HLSTATUS_GRACEFUL:
					hlstatus_name = "GRACEFUL";
					break;
				case HLSTATUS_HSREBALANCE:
					hlstatus_name = "HSREBALANCE";
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
			fprintf(fd,"usedspace: %"PRIu64"\ntotalspace: %"PRIu64"\nusage: %.2lf%%\n",eptr->usedspace,eptr->totalspace,usage);
			fprintf(fd,"load: %"PRIu32"\ntimeout: %"PRIu16"\nchunkscount: %"PRIu32"\n",eptr->load,eptr->timeout,eptr->chunkscount);
			fprintf(fd,"errorcounter: %"PRIu32"\nwritecounter: %"PRIu16"\nrrepcounter: %.3lf\nwrepcounter: %.3lf\ndelcounter: %"PRIu32"\n",eptr->errorcounter,eptr->writecounter,eptr->rrepcounter/(double)(FULL_REPLICATION_WEIGHT),eptr->wrepcounter/(double)(FULL_REPLICATION_WEIGHT),eptr->delcounter);
			fprintf(fd,"create_total: %"PRIu32"\nrrep_total: %"PRIu32"\nwrep_total: %"PRIu32"\ndel_total: %"PRIu32"\n",eptr->create_total_counter,eptr->rrep_total_counter,eptr->wrep_total_counter,eptr->del_total_counter);
			fprintf(fd,"create/s: %.4lf\nrrep/s: %.4lf\nwrep/s: %.4lf\ndel/s: %.4lf\n",eptr->create_total_counter/dur,eptr->rrep_total_counter/dur,eptr->wrep_total_counter/dur,eptr->del_total_counter/dur);
			fprintf(fd,"csid: %"PRIu16"\ndist: %"PRIu32"\nfirst: %"PRIu8"\ncorr: %.4lf\n",eptr->csid,eptr->dist,eptr->first,eptr->corr);
			fprintf(fd,"hlstatus: %"PRIu8" (%s)\noverloaded: %"PRIu8"\nmaintained: %"PRIu8"\n",eptr->hlstatus,hlstatus_name,overloaded,maintained);
			eptr->create_total_counter = 0;
			eptr->rrep_total_counter = 0;
			eptr->wrep_total_counter = 0;
			eptr->del_total_counter = 0;
			eptr->total_counter_begin = monotonic_seconds();
			fprintf(fd,"\n");
		}
	}
	fprintf(fd,"[replications/deletions stats]\n");
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->csptr!=NULL) {
			for (i=0 ; i<REPL_REASONS ; i++) {
				if (eptr->lreplreadok[i] || eptr->lreplreaderr[i]) {
					fprintf(fd,"cs %s ; replication source ; reason: %s ; ok/err: %.3lf/%.3lf\n",eptr->servdesc,replreasons[i],eptr->lreplreadok[i]/(double)(FULL_REPLICATION_WEIGHT),eptr->lreplreaderr[i]/(double)(FULL_REPLICATION_WEIGHT));
				}
				if (eptr->lreplwriteok[i] || eptr->lreplwriteerr[i]) {
					fprintf(fd,"cs %s ; replication target ; reason: %s ; ok/err: %.3lf/%.3lf\n",eptr->servdesc,replreasons[i],eptr->lreplwriteok[i]/(double)(FULL_REPLICATION_WEIGHT),eptr->lreplwriteerr[i]/(double)(FULL_REPLICATION_WEIGHT));
				}
			}
			for (i=0 ; i<OP_REASONS ; i++) {
				if (eptr->ldelok[i] || eptr->ldelerr[i]) {
					fprintf(fd,"cs %s ; deletion ; reason: %s ; ok/err: %u/%u\n",eptr->servdesc,opreasons[i],eptr->ldelok[i],eptr->ldelerr[i]);
				}
			}
		}
	}
	fprintf(fd,"\n");
	fprintf(fd,"[pending operations]\n");
	matocsserv_replication_info(fd);
	matocsserv_operation_info(fd);
	fprintf(fd,"\n");
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
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL && (eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK || eptr->hlstatus==HLSTATUS_LSREBALANCE) && csdb_server_is_being_maintained(eptr->csptr)==0) {
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
		servtab[i].err += 1000.0 * (servers[i]->writecounter + servers[i]->wrepcounter/(double)(FULL_REPLICATION_WEIGHT));
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

uint8_t matocsserv_server_matches_labelexpr(void *e,const uint8_t labelexpr[SCLASS_EXPR_MAX_SIZE]) {
	matocsserventry *eptr = (matocsserventry*)e;

	if (eptr!=NULL) {
		return labelmask_matches_labelexpr(eptr->labelmask,labelexpr);
	}
	return 0;
}

uint16_t matocsserv_servers_matches_labelexpr(const uint8_t labelexpr[SCLASS_EXPR_MAX_SIZE]) {
	uint16_t cnt;
	matocsserventry *eptr;
	cnt = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			if (matocsserv_server_matches_labelexpr(eptr,labelexpr)) {
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
			if ((eptr->totalspace - eptr->usedspace)>(MFSCHUNKSIZE*(1U+eptr->writecounter*10U))) {
				olcsids[*olcscnt] = eptr->csid;
				(*olcscnt)++;
				if (eptr->hlstatus!=HLSTATUS_OVERLOADED && eptr->hlstatus!=HLSTATUS_HSREBALANCE) { // server is not overloaded and have enough free space ?
					if ((eptr->hlstatus!=HLSTATUS_DEFAULT && eptr->hlstatus!=HLSTATUS_OK && eptr->hlstatus!=HLSTATUS_LSREBALANCE) || csdb_server_is_being_maintained(eptr->csptr)) {
						gracecnt++;
						stdcsids[MAXCSCOUNT-gracecnt] = eptr->csid;
					} else {
						stdcsids[*stdcscnt] = eptr->csid;
						(*stdcscnt)++;
						stdcnt++;
					}
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
	static matocsserventry **servtab;
	matocsserventry *eptr;
	uint32_t i;
	uint32_t gracecnt;
	uint32_t stdcnt;
	uint32_t totalcnt;

	if (csids==NULL || overloaded==NULL) {
		if (servtab!=NULL) {
			free(servtab);
		}
		servtab = NULL;
		return 0;
	}
	if (servtab==NULL) {
		servtab = malloc(sizeof(matocsserventry*) * MAXCSCOUNT);
		passert(servtab);
	}

	gracecnt = 0;
	stdcnt = 0;
	totalcnt = 0;
	*overloaded = 0;

	for (eptr = matocsservhead ; eptr && totalcnt<MAXCSCOUNT ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			if ((eptr->totalspace - eptr->usedspace)>(MFSCHUNKSIZE*(1U+eptr->writecounter*10U))) {
				if (eptr->hlstatus!=HLSTATUS_OVERLOADED && eptr->hlstatus!=HLSTATUS_HSREBALANCE) {
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

uint16_t matocsserv_getservers_replpossible(uint16_t csids[MAXCSCOUNT]) {
	uint16_t scount;
	uint8_t replpossible;
	matocsserventry *eptr;

	scount = 0;
	replpossible = 0;

	for (eptr = matocsservhead ; eptr && scount<MAXCSCOUNT; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->csptr!=NULL) {
			if (eptr->totalspace==0 || (eptr->totalspace - eptr->usedspace)<=(eptr->totalspace/100)) {
				// no space - replication not possible
				replpossible = 0;
			} else if (eptr->registered!=REGISTERED || (eptr->receivingchunks&TRANSFERRING_NEW_CHUNKS)!=0) {
				// currently busy, but potentially replication is possible - can wait
				replpossible = 1;
			} else if (!((eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK || eptr->hlstatus==HLSTATUS_LSREBALANCE) && csdb_server_is_being_maintained(eptr->csptr)==0)) {
				// overloaded / maintained - replication potentially possible
				replpossible = 1;
			} else {
				// standard server
				replpossible = 1;
			}
			if (replpossible) {
				csids[scount++] = eptr->csid;
			}
		}
	}
	return scount;
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
				} else if (eptr->wrepcounter/(double)(FULL_REPLICATION_WEIGHT)+a>=replimit || eptr->registered!=REGISTERED || (eptr->receivingchunks&TRANSFERRING_NEW_CHUNKS)!=0) {
					csstate=CSSTATE_LIMIT_REACHED;
				} else if (!((eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK || eptr->hlstatus==HLSTATUS_LSREBALANCE) && csdb_server_is_being_maintained(eptr->csptr)==0)) {
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

static inline uint8_t matocsserv_server_can_be_used_in_replication(matocsserventry *eptr) {
	if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>(eptr->totalspace/100) && eptr->csptr!=NULL && eptr->registered==REGISTERED && (eptr->receivingchunks&TRANSFERRING_NEW_CHUNKS)==0) {
		if ((eptr->hlstatus==HLSTATUS_DEFAULT || eptr->hlstatus==HLSTATUS_OK || eptr->hlstatus==HLSTATUS_LSREBALANCE) && csdb_server_is_being_maintained(eptr->csptr)==0) {
			return 2;
		} else {
			return 1;
		}
	}
	return 0;
}

void matocsserv_recalculate_storagemode_scounts(storagemode *sm) {
	matocsserventry *eptr;
	uint8_t datamatch,chksummatch;
	sm->replallowed = 0;
	sm->overloaded = 0;
	sm->allvalid = 0;
	sm->data_replallowed = 0;
	sm->data_overloaded = 0;
	sm->data_allvalid = 0;
	sm->chksum_replallowed = 0;
	sm->chksum_overloaded = 0;
	sm->chksum_allvalid = 0;
	sm->both_replallowed = 0;
	sm->both_overloaded = 0;
	sm->both_allvalid = 0;
	if (sm->has_labels && sm->labelscnt==1) {
		for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
			if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
				if (matocsserv_server_matches_labelexpr(eptr,sm->labelexpr[0])) {
					sm->allvalid++;
					switch (matocsserv_server_can_be_used_in_replication(eptr)) {
						case 2:
							sm->replallowed++;
							nobreak;
						case 1:
							sm->overloaded++;
					}
				}
			}
		}
		sm->valid_ec_counters = 1;
	} else if (sm->has_labels && sm->labelscnt==2) {
		for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
			if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
				datamatch = matocsserv_server_matches_labelexpr(eptr,sm->labelexpr[0])?1:0;
				chksummatch = matocsserv_server_matches_labelexpr(eptr,sm->labelexpr[1])?1:0;
				sm->allvalid++;
				if (datamatch & chksummatch) {
					sm->both_allvalid++;
				} else if (datamatch) {
					sm->data_allvalid++;
				} else if (chksummatch) {
					sm->chksum_allvalid++;
				}
				switch (matocsserv_server_can_be_used_in_replication(eptr)) {
					case 2:
						sm->replallowed++;
						if (datamatch & chksummatch) {
							sm->both_replallowed++;
						} else if (datamatch) {
							sm->data_replallowed++;
						} else if (chksummatch) {
							sm->chksum_replallowed++;
						}
						nobreak;
					case 1:
						sm->overloaded++;
						if (datamatch & chksummatch) {
							sm->both_overloaded++;
						} else if (datamatch) {
							sm->data_overloaded++;
						} else if (chksummatch) {
							sm->chksum_overloaded++;
						}
				}
			}
		}
		sm->valid_ec_counters = 2;
	} else {
		sm->valid_ec_counters = 0;
	}
}

/* all servers that can be used for low priority (not endangered) replications */
uint16_t matocsserv_getservers_replallowed(uint16_t csids[MAXCSCOUNT]) {
	matocsserventry *eptr;
	uint32_t j;
	j = 0;
	for (eptr = matocsservhead ; eptr && j<MAXCSCOUNT; eptr=eptr->next) {
		if (matocsserv_server_can_be_used_in_replication(eptr)==2) {
			csids[j] = eptr->csid;
			j++;
		}
	}
	return j;
}

/* servers that can be used for replication using given replication limit */
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
		switch (matocsserv_server_can_be_used_in_replication(eptr)) {
			case 1:
				if (eptr->wrepcounter/(double)(FULL_REPLICATION_WEIGHT)+a<replimit) {
					hpadd = 1;
				}
				break;
			case 2:
				if (eptr->wrepcounter/(double)(FULL_REPLICATION_WEIGHT)+a<replimit) {
					csids[j] = eptr->csid;
					j++;
				} else {
					*allservflag = 0;
				}
				break;
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
			if (matocsserv_server_can_be_used_in_replication(eptr)==1) {
				if (eptr->wrepcounter/(double)(FULL_REPLICATION_WEIGHT)+a<replimit) {
					csids[j] = eptr->csid;
					j++;
				} else {
					*allservflag = 0;
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

void matocsserv_recalculate_server_counters(void) {
	matocsserventry *eptr;
	valid_servers_count = 0;
	almostfull_servers_count = 0;
	replallowed_servers_count = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && eptr->csptr!=NULL) {
			valid_servers_count++;
			if ((eptr->totalspace - eptr->usedspace)<=(eptr->totalspace/100)) {
				almostfull_servers_count++;
			}
			if (matocsserv_server_can_be_used_in_replication(eptr)==2) {
				replallowed_servers_count++;
			}
		}
	}
}

uint16_t matocsserv_servers_count(void) {
	return valid_servers_count;
}

uint16_t matocsserv_almostfull_servers(void) {
	return almostfull_servers_count;
}

uint16_t matocsserv_replallowed_servers(void) {
	return replallowed_servers_count;
}

void matocsserv_calculate_space(void) {
	matocsserventry *eptr;
	uint64_t tspace,uspace,rspace;
	uint64_t muspace,mtspace;
	uint32_t usagemax,usagemin;
	double dusage;
	uint32_t mpusage;
	tspace = 0;
	uspace = 0;
	muspace = 0;
	mtspace = 0;
	usagemax = 0;
	usagemin = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0) {
			dusage = eptr->usedspace;
			dusage /= eptr->totalspace;
			if (dusage<0.0) {
				dusage = 0.0;
			}
			if (dusage>1.0) {
				dusage = 1.0;
			}
			mpusage = 100000 * dusage;
			if (usagemax==0) {
				usagemax = mpusage;
				usagemin = mpusage;
			} else {
				if (mpusage > usagemax) {
					usagemax = mpusage;
				}
				if (mpusage < usagemin) {
					usagemin = mpusage;
				}
			}
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
	gusagediff = usagemax - usagemin;
}

uint64_t matocsserv_gettotalspace(void) {
	return gtotalspace;
}

uint64_t matocsserv_getusedspace(void) {
	return gusedspace;
}

int matocsserv_have_availspace(void) {
	return (gavailspace>0)?1:0;
}

uint32_t matocsserv_getusagediff(void) {
	return gusagediff;
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
	if (eptr->mode!=KILL && eptr->servdesc) {
		return eptr->servdesc;
	}
	return empty;
}

int matocsserv_get_csdata(void *e,uint32_t clientip,uint32_t *servip,uint16_t *servport,uint32_t *servver,uint32_t *servlabelmask) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (eptr->mode!=KILL) {
		*servip = multilan_map(eptr->servip,clientip);
		*servport = eptr->servport;
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

int matocsserv_can_split_chunks(void *e,uint8_t ecmode) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (matocsserv_server_can_be_used_in_replication(eptr)!=2) {
		return 0;
	}
	(void)ecmode;
	return (eptr->mode!=KILL && eptr->version>=VERSION2INT(4,49,0))?1:0;
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
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't decrease write counter - structure error");
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

	if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>(eptr->totalspace/100) && eptr->csptr!=NULL && eptr->registered==REGISTERED && (eptr->receivingchunks&TRANSFERRING_NEW_CHUNKS)==0) {
		a = ((uint32_t)(eptr->csid*UINT32_C(0x9874BF31)+now*UINT32_C(0xB489FC37)))/4294967296.0;
		return eptr->wrepcounter/(double)(FULL_REPLICATION_WEIGHT)+a;
	} else {
		return SOMETHING_OVER_ANY_LIMIT;
	}
}

double matocsserv_replication_read_counter(void *e,uint32_t now) {
	matocsserventry *eptr = (matocsserventry *)e;
	double a;
	if (eptr->mode!=KILL && eptr->csptr!=NULL && (eptr->receivingchunks&TRANSFERRING_LOST_CHUNKS)==0) {
		a = ((uint32_t)(eptr->csid*UINT32_C(0x9874BF31)+now*UINT32_C(0xB489FC37)))/4294967296.0;
		return eptr->rrepcounter/(double)(FULL_REPLICATION_WEIGHT)+a;
	} else {
		return SOMETHING_OVER_ANY_LIMIT;
	}
}

uint16_t matocsserv_deletion_counter(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	return eptr->delcounter;
}

uint8_t* matocsserv_create_packet(matocsserventry *eptr,uint32_t type,uint32_t size) {
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
int matocsserv_send_chunk_checksum(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,ANTOCS_GET_CHUNK_CHECKSUM,8+4);
		PUT_CHUNKID_AND_ECID(&data,chunkid,ecid);
		put32bit(&data,version);
	}
	return 0;
}
/* for future use */
void matocsserv_got_chunk_checksum(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version,checksum;
	uint8_t ecid;
	uint8_t status;
	if (length!=8+4+1 && length!=8+4+4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOAN_CHUNK_CHECKSUM - wrong size (%"PRIu32"/13|16)",length);
		eptr->mode = KILL;
		return ;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	version = get32bit(&data);
	if (length==8+4+1) {
		status = get8bit(&data);
//			chunk_got_checksum_status(eptr->csid,chunkid,version,status);
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s calculate checksum status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),mfsstrerr(status));
	} else {
		checksum = get32bit(&data);
//			chunk_got_checksum(eptr->csid,chunkid,version,checksum);
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"(%s) chunk: %016"PRIX64"%s calculate checksum: %08"PRIX32,eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),checksum);
	}
	(void)version;
}

void matocsserv_broadcast_chunk_status(uint64_t chunkid) {
	matocsserventry *eptr;
	uint8_t *data;

	if (ChunkServerCheck>0) {
		for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
			if (eptr->mode==DATA && eptr->version>=VERSION2INT(4,32,0) && eptr->registered && (eptr->receivingchunks&(TRANSFERRING_LOST_CHUNKS|TRANSFERRING_NEW_CHUNKS))==0) {
				data = matocsserv_create_packet(eptr,MATOCS_CHUNK_STATUS,8);
				put64bit(&data,chunkid);
//				matocsserv_chunk_status_add(eptr,chunkid);
			}
		}
	}
}

void matocsserv_got_chunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t parts;
	uint32_t version[255];
	uint16_t blocks[255];
	uint8_t damaged[255];
	uint8_t ecid[255];

	if (length<8 || (length%8)!=0 || (length/8)>256) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CHUNK_STATUS - wrong size (%"PRIu32"/8+8*n)",length);
		eptr->mode = KILL;
		return;
	}
	chunkid = get64bit(&data);
//	if (matocsserv_chunk_status_check(eptr,chunkid)==0) {
//		return;
//	}
	if ((eptr->receivingchunks&(TRANSFERRING_LOST_CHUNKS|TRANSFERRING_NEW_CHUNKS))!=0 || eptr->registered==0 || ChunkServerCheck==0) {
		return;
	}
	length-=8;
	parts = 0;
	while (length>0) {
		ecid[parts] = get8bit(&data);
		damaged[parts] = get8bit(&data);
		blocks[parts] = get16bit(&data);
		version[parts] = get32bit(&data);
		length -= 8;
		parts++;
	}
	chunk_got_status_data(chunkid,eptr->servdesc,eptr->csid,parts,ecid,version,damaged,blocks,(ChunkServerCheck>1)?1:0);
}

int matocsserv_send_createchunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

//	matocsserv_chunk_status_remove(eptr,chunkid); // pro forma only
	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,MATOCS_CREATE,8+4);
		PUT_CHUNKID_AND_ECID(&data,chunkid,ecid);
		put32bit(&data,version);
	}
	return 0;
}

void matocsserv_got_createchunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint8_t status;
	if (length!=8+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CREATE - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	status = get8bit(&data);
	chunk_got_create_status(eptr->csid,chunkid,ecid,status);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s creation status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),mfsstrerr(status));
	}
//	matocsserv_chunk_status_remove(eptr,chunkid); // pro forma only
}

int matocsserv_send_deletechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t reason) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint64_t dstchunkid;
	uint8_t *data;

//	matocsserv_chunk_status_remove(eptr,chunkid);
	dstchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,ecid);
	if (matocsserv_operation_find(dstchunkid,eptr)) {
		return -1;
	}
	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,MATOCS_DELETE,8+4);
		put64bit(&data,dstchunkid);
		put32bit(&data,version);
		matocsserv_operation_begin(dstchunkid,version,eptr,OPTYPE_DELETE,reason);
	}
	return 0;
}

void matocsserv_got_deletechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint8_t status;
	if (length!=8+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_DELETE - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	status = get8bit(&data);
	matocsserv_operation_end(COMBINE_CHUNKID_AND_ECID(chunkid,ecid),eptr,(status==MFS_STATUS_OK));
	chunk_got_delete_status(eptr->csid,chunkid,ecid,status);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s deletion status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),mfsstrerr(status));
	}
//	matocsserv_chunk_status_remove(eptr,chunkid);
}

int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,void *src,uint8_t reason) {
	matocsserventry *dsteptr = (matocsserventry *)e;
	matocsserventry *srceptr = (matocsserventry *)src;
	uint64_t dstchunkid;
	uint8_t *data;

//	matocsserv_chunk_status_remove(dsteptr,chunkid);
	dstchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,ecid);

	if (matocsserv_replication_find(dstchunkid,version,dsteptr)) {
		return -1;
	}
	if (dsteptr->mode!=KILL && srceptr->mode!=KILL) {
//		dsteptr->dist = 0;
		data = matocsserv_create_packet(dsteptr,MATOCS_REPLICATE,8+4+4+2);
		put64bit(&data,dstchunkid);
		put32bit(&data,version);
		put32bit(&data,srceptr->servip);
		put16bit(&data,srceptr->servport);
		matocsserv_replication_begin(dstchunkid,version,dsteptr,1,&src,(ecid==0)?FULL_REPLICATION_WEIGHT:EC_REPLICATION_WEIGHT,(ecid==0)?FULL_REPLICATION_WEIGHT:EC_REPLICATION_WEIGHT,REPTYPE_SIMPLE,reason);
	}
	return 0;
}

int matocsserv_send_replicatechunk_split(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,void *src,uint8_t srcecid,uint8_t partno,uint8_t parts,uint8_t reason) {
	matocsserventry *dsteptr = (matocsserventry *)e;
	matocsserventry *srceptr = (matocsserventry *)src;
	uint64_t dstchunkid;
	uint8_t *data;

//	matocsserv_chunk_status_remove(dsteptr,chunkid);
	dstchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,ecid);

	if (matocsserv_replication_find(dstchunkid,version,dsteptr)) {
		return -1;
	}
	if (dsteptr->mode!=KILL && srceptr->mode!=KILL) {
		data = matocsserv_create_packet(dsteptr,MATOCS_REPLICATE_SPLIT,8+4+4+2+8+1+1);
		put64bit(&data,dstchunkid);
		put32bit(&data,version);
		put32bit(&data,srceptr->servip);
		put16bit(&data,srceptr->servport);
		PUT_CHUNKID_AND_ECID(&data,chunkid,srcecid);
		put8bit(&data,partno);
		put8bit(&data,parts);
		matocsserv_replication_begin(dstchunkid,version,dsteptr,1,&src,EC_REPLICATION_WEIGHT,EC_REPLICATION_WEIGHT,REPTYPE_SPLIT,reason);
	}
	return 0;
}

int matocsserv_send_replicatechunk_recover(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t parts,void **survivors,uint8_t *survivorecids,uint8_t reason) {
	matocsserventry *dsteptr = (matocsserventry *)e;
	matocsserventry *srceptr;
	uint64_t dstchunkid,srcchunkid;
	uint8_t *data;
	uint8_t i;

//	matocsserv_chunk_status_remove(dsteptr,chunkid);
	dstchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,ecid);

	if (matocsserv_replication_find(dstchunkid,version,dsteptr)) {
		return -1;
	}
	if (dsteptr->mode!=KILL) {
		for (i=0 ; i<parts ; i++) {
			srceptr = (matocsserventry *)(survivors[i]);
			if (srceptr->mode==KILL) {
				return 0;
			}
		}
		data = matocsserv_create_packet(dsteptr,MATOCS_REPLICATE_RECOVER,8+4+16+1+parts*(8+4+2));
		put64bit(&data,dstchunkid);
		put32bit(&data,version);
		if (parts==8) {
			put32bit(&data,0x88888888);
			put32bit(&data,0x44444444);
			put32bit(&data,0x22222222);
			put32bit(&data,0x11111111);
		} else if (parts==4) {
			put32bit(&data,0x8888);
			put32bit(&data,0x4444);
			put32bit(&data,0x2222);
			put32bit(&data,0x1111);
		} else {
			put32bit(&data,0);
			put32bit(&data,0);
			put32bit(&data,0);
			put32bit(&data,0);
		}
		put8bit(&data,parts);
		for (i=0 ; i<parts ; i++) {
			srceptr = (matocsserventry *)(survivors[i]);
			srcchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,survivorecids[i]);
			put32bit(&data,srceptr->servip);
			put16bit(&data,srceptr->servport);
			put64bit(&data,srcchunkid);
		}
		matocsserv_replication_begin(dstchunkid,version,dsteptr,parts,survivors,EC_REPLICATION_WEIGHT,EC_REPLICATION_WEIGHT,REPTYPE_RECOVER,reason);
	}
	return 0;
}

int matocsserv_send_replicatechunk_join(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint8_t parts,void **survivors,uint8_t *survivorecids,uint8_t reason) {
	matocsserventry *dsteptr = (matocsserventry *)e;
	matocsserventry *srceptr;
	uint64_t dstchunkid,srcchunkid;
	uint8_t *data;
	uint8_t i;

//	matocsserv_chunk_status_remove(dsteptr,chunkid);
	dstchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,ecid);

	if (matocsserv_replication_find(dstchunkid,version,dsteptr)) {
		return -1;
	}
	if (dsteptr->mode!=KILL) {
		for (i=0 ; i<parts ; i++) {
			srceptr = (matocsserventry *)(survivors[i]);
			if (srceptr->mode==KILL) {
				return 0;
			}
		}
		data = matocsserv_create_packet(dsteptr,MATOCS_REPLICATE_JOIN,8+4+1+parts*(8+4+2));
		put64bit(&data,dstchunkid);
		put32bit(&data,version);
		put8bit(&data,parts);
		for (i=0 ; i<parts ; i++) {
			srceptr = (matocsserventry *)(survivors[i]);
			srcchunkid = COMBINE_CHUNKID_AND_ECID(chunkid,survivorecids[i]);
			put32bit(&data,srceptr->servip);
			put16bit(&data,srceptr->servport);
			put64bit(&data,srcchunkid);
		}
		matocsserv_replication_begin(dstchunkid,version,dsteptr,parts,survivors,EC_REPLICATION_WEIGHT,FULL_REPLICATION_WEIGHT,REPTYPE_JOIN,reason);
	}
	return 0;
}

void matocsserv_got_replicatechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t reptype;
	uint64_t chunkid;
	uint32_t version;
	uint8_t ecid;
	uint8_t status;
	char servbuff[1000];
	uint32_t leng;

	if (length!=8+4+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REPLICATE - wrong size (%"PRIu32"/13)",length);
		eptr->mode = KILL;
		return;
	}
//	if (eptr->repcounter>0) {
//		eptr->repcounter--;
//	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	version = get32bit(&data);
	status = get8bit(&data);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=MFS_STATUS_OK) {
#endif
		leng = matocsserv_replication_print(servbuff,1000,COMBINE_CHUNKID_AND_ECID(chunkid,ecid),version,eptr,&reptype);
		if (leng>=1000) {
			servbuff[999] = '\0';
		} else {
			servbuff[leng] = '\0';
		}
		if (leng>0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s %s replication status: %s",servbuff,chunkid,matocsserv_ecid_to_str(ecid),reptype_str[reptype&3],mfsstrerr(status));
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(unknown -> %s) chunk: %016"PRIX64"%s %s replication status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),reptype_str[reptype&3],mfsstrerr(status));
		}
	}
	matocsserv_replication_end(COMBINE_CHUNKID_AND_ECID(chunkid,ecid),version,eptr,(status==MFS_STATUS_OK));
	chunk_got_replicate_status(eptr->csid,chunkid,ecid,version,status);
//	matocsserv_chunk_status_remove(eptr,chunkid);
}

int matocsserv_send_setchunkversion(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

//	matocsserv_chunk_status_remove(eptr,chunkid);
	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,MATOCS_SET_VERSION,8+4+4);
		PUT_CHUNKID_AND_ECID(&data,chunkid,ecid);
		put32bit(&data,version);
		put32bit(&data,oldversion);
	}
	return 0;
}

void matocsserv_got_setchunkversion_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint8_t status;
	if (length!=8+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_SET_VERSION - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	status = get8bit(&data);
	chunk_got_setversion_status(eptr->csid,chunkid,ecid,status);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s set version status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),mfsstrerr(status));
	}
//	matocsserv_chunk_status_remove(eptr,chunkid);
}

int matocsserv_send_duplicatechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint64_t oldchunkid,uint8_t oldecid,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

//	matocsserv_chunk_status_remove(eptr,chunkid);
	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,MATOCS_DUPLICATE,8+4+8+4);
		PUT_CHUNKID_AND_ECID(&data,chunkid,ecid);
		put32bit(&data,version);
		PUT_CHUNKID_AND_ECID(&data,oldchunkid,oldecid);
		put32bit(&data,oldversion);
	}
	return 0;
}

void matocsserv_got_duplicatechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint8_t status;
	if (length!=8+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_DUPLICATE - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	status = get8bit(&data);
	chunk_got_duplicate_status(eptr->csid,chunkid,ecid,status);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s duplication status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),mfsstrerr(status));
	}
//	matocsserv_chunk_status_remove(eptr,chunkid);
}

int matocsserv_send_truncatechunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t length,uint32_t version,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

//	matocsserv_chunk_status_remove(eptr,chunkid);
	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,MATOCS_TRUNCATE,8+4+4+4);
		PUT_CHUNKID_AND_ECID(&data,chunkid,ecid);
		put32bit(&data,length);
		put32bit(&data,version);
		put32bit(&data,oldversion);
	}
	return 0;
}

void matocsserv_got_truncatechunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint8_t status;
	if (length!=8+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_TRUNCATE - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	status = get8bit(&data);
	chunk_got_truncate_status(eptr->csid,chunkid,ecid,status);
//	matocsserv_notify(&(eptr->duplication),eptr,chunkid,status);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s truncate status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),mfsstrerr(status));
	}
//	matocsserv_chunk_status_remove(eptr,chunkid);
}

int matocsserv_send_duptruncchunk(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint64_t oldchunkid,uint8_t oldecid,uint32_t oldversion,uint32_t length) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

//	matocsserv_chunk_status_remove(eptr,chunkid);
	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,MATOCS_DUPTRUNC,8+4+8+4+4);
		PUT_CHUNKID_AND_ECID(&data,chunkid,ecid);
		put32bit(&data,version);
		PUT_CHUNKID_AND_ECID(&data,oldchunkid,oldecid);
		put32bit(&data,oldversion);
		put32bit(&data,length);
	}
	return 0;
}

void matocsserv_got_duptruncchunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint8_t status;
	if (length!=8+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_DUPTRUNC - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	status = get8bit(&data);
	chunk_got_duptrunc_status(eptr->csid,chunkid,ecid,status);
//	matocsserv_notify(&(eptr->duplication),eptr,chunkid,status);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64"%s duplication with truncate status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),mfsstrerr(status));
	}
//	matocsserv_chunk_status_remove(eptr,chunkid);
}

int matocsserv_send_localsplitchunk(void *e,uint64_t chunkid,uint32_t version,uint32_t missingmask,uint8_t parts,uint8_t reason) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;
	uint8_t pver;

//	matocsserv_chunk_status_remove(eptr,chunkid);
	if (eptr->mode!=KILL) {
		pver = (eptr->version>=VERSION2INT(4,25,0))?1:0;
		data = matocsserv_create_packet(eptr,MATOCS_LOCALSPLIT,8+4+4+pver);
		put64bit(&data,chunkid);
		put32bit(&data,version);
		put32bit(&data,missingmask);
		if (pver>0) {
			put8bit(&data,parts);
		}
		matocsserv_replication_begin(chunkid,version,eptr,1,&e,FULL_REPLICATION_WEIGHT,LOCALPART_REPLICATION_WEIGHT*bitcount(missingmask),REPTYPE_LOCALSPLIT,reason);
	}
	return 0;
}

void matocsserv_got_localsplitchunk_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t status;
	if (length!=8+4+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_LOCALSPLIT - wrong size (%"PRIu32"/13)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	chunkid = get64bit(&data);
	version = get32bit(&data);
	status = get8bit(&data);
	matocsserv_replication_end(chunkid,version,eptr,(status==MFS_STATUS_OK));
	chunk_got_localsplit_status(eptr->csid,chunkid,version,status);
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunk: %016"PRIX64" localsplit status: %s",eptr->servdesc,chunkid,mfsstrerr(status));
	}
//	matocsserv_chunk_status_remove(eptr,chunkid);
}

int matocsserv_send_chunkop(void *e,uint64_t chunkid,uint8_t ecid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint8_t copyecid,uint32_t copyversion,uint32_t leng) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

//	if (copychunkid>0) {
//		matocsserv_chunk_status_remove(eptr,copychunkid);
//	} else {
//		matocsserv_chunk_status_remove(eptr,chunkid);
//	}
	if (eptr->mode!=KILL) {
		data = matocsserv_create_packet(eptr,MATOCS_CHUNKOP,8+4+4+8+4+4);
		PUT_CHUNKID_AND_ECID(&data,chunkid,ecid);
		put32bit(&data,version);
		put32bit(&data,newversion);
		PUT_CHUNKID_AND_ECID(&data,copychunkid,copyecid);
		put32bit(&data,copyversion);
		put32bit(&data,leng);
	}
	return 0;
}

void matocsserv_got_chunkop_status(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t ecid,copyecid;
	uint64_t chunkid,copychunkid;
	uint32_t version,newversion,copyversion,leng;
	uint8_t status;
	if (length!=8+4+4+8+4+4+1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CHUNKOP - wrong size (%"PRIu32"/33)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
	version = get32bit(&data);
	newversion = get32bit(&data);
	GET_CHUNKID_AND_ECID(&data,copychunkid,copyecid);
	copyversion = get32bit(&data);
	leng = get32bit(&data);
	status = get8bit(&data);
	if (newversion!=version) {
		chunk_got_chunkop_status(eptr->csid,chunkid,ecid,status);
	}
	if (copychunkid>0) {
		chunk_got_chunkop_status(eptr->csid,copychunkid,copyecid,status);
	}
#ifdef MFSDEBUG
	if (1) {
#else
	if (status!=0) {
#endif
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"(%s) chunkop(%016"PRIX64"%s,%08"PRIX32",%08"PRIX32",%016"PRIX64",%08"PRIX32",%"PRIu32") status: %s",eptr->servdesc,chunkid,matocsserv_ecid_to_str(ecid),version,newversion,copychunkid,copyversion,leng,mfsstrerr(status));
	}
//	if (copychunkid>0) {
//		matocsserv_chunk_status_remove(eptr,copychunkid);
//	} else {
//		matocsserv_chunk_status_remove(eptr,chunkid);
//	}
}

void matocsserv_get_version(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;
	static const char vstring[] = VERSSTR;
	if (length!=0 && length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_VERSION - wrong size (%"PRIu32"/4|0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = matocsserv_create_packet(eptr,ANTOAN_VERSION,4+4+strlen(vstring));
		put32bit(&ptr,msgid);
	} else {
		ptr = matocsserv_create_packet(eptr,ANTOAN_VERSION,4+strlen(vstring));
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
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=5U+(uint32_t)nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	memcpy(name,data,nleng);
	name[nleng] = 0;
	val = cfg_getdefaultstr(name);
	if (val!=NULL) {
		vleng = strlen(val);
		if (vleng>255) {
			vleng=255;
		}
	} else {
		vleng = 0;
	}
	if (msgid==0) {
		ptr = matocsserv_create_packet(eptr,ANTOAN_CONFIG_VALUE,6+nleng+vleng);
		put32bit(&ptr,0);
		put8bit(&ptr,nleng);
		if (nleng>0) {
			memcpy(ptr,name,nleng);
			ptr+=nleng;
		}
	} else {
		ptr = matocsserv_create_packet(eptr,ANTOAN_CONFIG_VALUE,5+vleng);
		put32bit(&ptr,msgid);
	}
	put8bit(&ptr,vleng);
	if (vleng>0 && val!=NULL) {
		memcpy(ptr,val,vleng);
	}
	if (val!=NULL) {
		free(val);
	}
}

void matocsserv_get_config_file(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	char name[256];
	uint8_t nleng;
	cfg_buff *fdata;
	uint8_t *ptr;

	if (length<5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG_FILE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=5U+(uint32_t)nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG_FILE - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	memcpy(name,data,nleng);
	name[nleng] = 0;
	fdata = cfg_getdefaultfile(name,65535);
	if (fdata==NULL) {
		ptr = matocsserv_create_packet(eptr,ANTOAN_CONFIG_FILE_CONTENT,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,MFS_ERROR_ENOENT);
	} else {
		ptr = matocsserv_create_packet(eptr,ANTOAN_CONFIG_FILE_CONTENT,6+fdata->leng);
		put32bit(&ptr,msgid);
		put16bit(&ptr,fdata->leng);
		memcpy(ptr,fdata->data,fdata->leng);
		free(fdata);
	}
}

void matocsserv_syslog(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t priority;
	uint32_t timestamp;
	uint16_t msgsize;
	if (length<3) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOMA_SYSLOG - wrong size (%"PRIu32"/>=7)",length);
		eptr->mode = KILL;
		return;
	}
	priority = get8bit(&data);
	timestamp = get32bit(&data);
	msgsize = get16bit(&data);
	if (length!=3U+msgsize) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOMA_SYSLOG - wrong size (%"PRIu32"/7+msgsize(%"PRIu16"))",length,msgsize);
		eptr->mode = KILL;
		return;
	}
	(void)priority;
	(void)timestamp;
	// lc_log_new_pstr(eptr->modulelogname,priority,timestamp,msgsize,data);
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
	uint8_t ecid;
	uint16_t csid;
	double us,ts;

	if ((length&1)==0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER: chunkserver is too old");
		eptr->mode = KILL;
		return;
	} else {
		passert(data);
		rversion = get8bit(&data);

		if (eptr->registered==REGISTERED && rversion!=63) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"got register message from registered chunkserver !!!");
			eptr->mode = KILL;
			return;
		}

		if (rversion==60) {
			if (length!=55 && length!=71) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER (BEGIN) - wrong size (%"PRIu32"/55|71)",length);
				eptr->mode = KILL;
				return;
			}
			if (AuthCode) {
				if (length==55) { // no authorization data
					uint8_t *p;
					p = matocsserv_create_packet(eptr,MATOCS_MASTER_ACK,33);
					put8bit(&p,3);
					for (i=0 ; i<32 ; i++) {
						eptr->passwordrnd[i] = rndu8();
					}
					memcpy(p,eptr->passwordrnd,32);
					return;
				} else { // length==71
					if (matocsserv_check_password(eptr->passwordrnd,data)==0) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER (BEGIN) - access denied - check password");
						eptr->mode = KILL;
						return;
					}
					data+=16;
				}
			}
			eptr->version = get32bit(&data);
			eptr->servip = get32bit(&data);
			eptr->servport = get16bit(&data);
			if (ForceTimeout>0) {
				data+=2;
			} else {
				eptr->timeout = get16bit(&data);
			}
			if (sclass_ec_version()>0 && eptr->version<VERSION2INT(4,0,0)) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER: chunkserver is too old - erasure coding needs chunkservers at least 4.x");
				eptr->mode = KILL;
				return;
			}
			if (sclass_ec_version()>1 && eptr->version<VERSION2INT(4,26,0)) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER: chunkserver is too old - erasure coding 4+n needs chunkservers at least 4.26.x");
				eptr->mode = KILL;
				return;
			}
			if (eptr->timeout==0) {
				eptr->timeout = DefaultTimeout;
			} else if (eptr->timeout<10) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER communication timeout too small (%"PRIu16" seconds - should be at least 10 seconds)",eptr->timeout);
				eptr->mode = KILL;
				return;
			}
			csid = get16bit(&data);
			eptr->usedspace = get64bit(&data);
			eptr->totalspace = get64bit(&data);
			eptr->chunkscount = get32bit(&data);
			eptr->todelusedspace = get64bit(&data);
			eptr->todeltotalspace = get64bit(&data);
			eptr->todelchunkscount = get32bit(&data);
			if (eptr->servip==0) {
				eptr->servip = eptr->peerip;
			}
			eptr->servip = matocsserv_remap_ip(eptr->servip);
			if (eptr->servdesc) {
				free(eptr->servdesc);
			}
			eptr->servdesc = univallocstripport(eptr->servip,eptr->servport);
			if (((eptr->servip)&0xFF000000) == 0x7F000000) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"chunkserver connected using localhost (%s) - you cannot use localhost for communication between chunkserver and master", eptr->servdesc);
				eptr->mode = KILL;
				return;
			}
			if ((eptr->csptr=csdb_new_connection(eptr->servip,eptr->servport,csid,eptr))==NULL) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't accept chunkserver %s",eptr->servdesc);
				eptr->mode = KILL;
				return;
			}
			us = (double)(eptr->usedspace)/(double)(1024*1024*1024);
			ts = (double)(eptr->totalspace)/(double)(1024*1024*1024);
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunkserver %s register begin, usedspace: %"PRIu64" (%.2lf GiB), totalspace: %"PRIu64" (%.2lf GiB)",eptr->servdesc,eptr->usedspace,us,eptr->totalspace,ts);
			if (eptr->version>=VERSION2INT(1,6,28)) { // if chunkserver version >= 1.6.28 then send back my version
				uint8_t *p;
				uint8_t mode;
					mode = (eptr->version >= VERSION2INT(2,0,33))?1:0;
					p = matocsserv_create_packet(eptr,MATOCS_MASTER_ACK,mode?17:9);
					put8bit(&p,0);
					put32bit(&p,VERSHEX);
					put16bit(&p,eptr->timeout);
					put16bit(&p,csdb_get_csid(eptr->csptr));
					if (mode) {
						put64bit(&p,meta_get_id());
					}
			}
			eptr->csid = chunk_server_connected(eptr);
			return;
		} else if (rversion==61) {
			if (((length-1)%12)!=0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER (CHUNKS) - wrong size (%"PRIu32"/1+N*12)",length);
				eptr->mode = KILL;
				return;
			}
			if (eptr->csptr==NULL) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER (CHUNKS) - CHUNKS packet before proper BEGIN packet");
				eptr->mode = KILL;
				return;
			}
			eptr->newchunkdelay = NEWCHUNKDELAY;
			eptr->receivingchunks |= TRANSFERRING_NEW_CHUNKS;
			receivingchunks |= TRANSFERRING_NEW_CHUNKS;
			chunkcount = (length-1)/12;
			for (i=0 ; i<chunkcount ; i++) {
				GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
				chunkversion = get32bit(&data);
				chunk_server_has_chunk(eptr->csid,chunkid,ecid,chunkversion);
			}
			if (eptr->version>=VERSION2INT(2,0,0)) {
				uint8_t *p;
				p = matocsserv_create_packet(eptr,MATOCS_MASTER_ACK,1);
				put8bit(&p,0);
			}
			return;
		} else if (rversion==62) {
			if (length!=1) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER (END) - wrong size (%"PRIu32"/1)",length);
				eptr->mode = KILL;
				return;
			}
			if (eptr->csptr==NULL) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER (END) - END packet before proper BEGIN packet");
				eptr->mode = KILL;
				return;
			}
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunkserver %s register end",eptr->servdesc);
			eptr->registered = REGISTERED;
			chunk_server_register_end(eptr->csid);
		} else if (rversion==63) {
			if (length!=1) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER (DISCONNECT) - wrong size (%"PRIu32"/1)",length);
				eptr->mode = KILL;
				return;
			}
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunkserver %s graceful disconnection",eptr->servdesc);
			if (eptr->csptr!=NULL) {
				csdb_temporary_maintenance_mode(eptr->csptr);
			}
			eptr->mode = KILL;
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_REGISTER - register version not supported (%"PRIu8"/60..63)",rversion);
			eptr->mode = KILL;
			return;
		}
	}
}

uint8_t matocsserv_csdb_force_disconnect(void *e,void *p) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (eptr->registered==WAITING && eptr->csptr==p) {
		eptr->csptr = NULL;
		eptr->mode = KILL;
		return 1;
	}
	return 0;
}


void matocsserv_labels(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t i,l;

	if (length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_LABELS - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
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
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_SPACE - wrong size (%"PRIu32"/16|32|40)",length);
		eptr->mode = KILL;
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
	if (length<4 || length>6) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CURRENT_LOAD - wrong size (%"PRIu32"/4-6)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	eptr->load = get32bit(&data);
	if (eptr->csptr) {
		csdb_server_load(eptr->csptr,eptr->load);
	}
	if (length>=5) {
		eptr->hlstatus = get8bit(&data);
	}
	if (length>=6) {
		eptr->receivingchunks = get8bit(&data);
	}
}

void matocsserv_chunk_damaged(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint32_t i;

	if (length%8!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CHUNK_DAMAGED - wrong size (%"PRIu32"/N*8)",length);
		eptr->mode = KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/8 ; i++) {
		GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"(%s) chunk: %016"PRIX64" is damaged",eptr->servdesc,chunkid);
		chunk_damaged(eptr->csid,chunkid,ecid);
	}
}

uint8_t matocsserv_receiving_chunks_state(void) {
	return receivingchunks;
}

void matocsserv_chunks_delays(void) {
	matocsserventry *eptr;

	receivingchunks = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==DATA) {
			if (eptr->version<VERSION2INT(4,32,0)) {
				eptr->receivingchunks = 0;
				if (eptr->lostchunkdelay>0) {
					eptr->lostchunkdelay--;
					eptr->receivingchunks |= TRANSFERRING_LOST_CHUNKS;
					receivingchunks |= TRANSFERRING_LOST_CHUNKS;
				}
				if (eptr->newchunkdelay>0) {
					eptr->newchunkdelay--;
					eptr->receivingchunks |= TRANSFERRING_NEW_CHUNKS;
					receivingchunks |= TRANSFERRING_NEW_CHUNKS;
				}
			} else {
				receivingchunks |= eptr->receivingchunks;
			}
		}
	}
}

void matocsserv_nonexistent_chunks(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint32_t i;

	if (length%8!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CHUNK_DOESNT_EXIST - wrong size (%"PRIu32"/N*8)",length);
		eptr->mode = KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
	for (i=0 ; i<length/8 ; i++) {
		GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"(%s) chunk doesn't exist: %016"PRIX64,eptr->servdesc,chunkid);
		chunk_lost(eptr->csid,chunkid,ecid,1);
	}
}


void matocsserv_chunks_lost(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint32_t i;

	if (length%8!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CHUNK_LOST - wrong size (%"PRIu32"/N*8)",length);
		eptr->mode = KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
//	if (eptr->receivingchunks==0) {
//		matocsserv_chunk_status_cleanup(eptr);
//	}
	eptr->lostchunkdelay = LOSTCHUNKDELAY;
	eptr->receivingchunks |= TRANSFERRING_LOST_CHUNKS;
	receivingchunks |= TRANSFERRING_LOST_CHUNKS;
	for (i=0 ; i<length/8 ; i++) {
		GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"(%s) chunk lost: %016"PRIX64,eptr->servdesc,chunkid);
		chunk_lost(eptr->csid,chunkid,ecid,0);
	}
}

void matocsserv_chunks_new(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t ecid;
	uint32_t chunkversion;
	uint32_t i;

	if (length%12!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_CHUNK_NEW - wrong size (%"PRIu32"/N*12)",length);
		eptr->mode = KILL;
		return;
	}
	if (length>0) {
		passert(data);
	}
//	if (eptr->receivingchunks==0) {
//		matocsserv_chunk_status_cleanup(eptr);
//	}
	eptr->newchunkdelay = NEWCHUNKDELAY;
	eptr->receivingchunks |= TRANSFERRING_NEW_CHUNKS;
	receivingchunks |= TRANSFERRING_NEW_CHUNKS;
	for (i=0 ; i<length/12 ; i++) {
		GET_CHUNKID_AND_ECID(&data,chunkid,ecid);
		chunkversion = get32bit(&data);
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"(%s) new chunk: %016"PRIX64,eptr->servdesc,chunkid);
		chunk_server_has_chunk(eptr->csid,chunkid,ecid,chunkversion);
	}
}

void matocsserv_error_occurred(matocsserventry *eptr,const uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CSTOMA_ERROR_OCCURRED - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	eptr->errorcounter++;
}

void matocsserv_reason_counters(void) {
	matocsserventry *eptr;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==DATA) {
			memcpy(eptr->lreplreadok,eptr->replreadok,sizeof(uint32_t)*REPL_REASONS);
			memcpy(eptr->lreplreaderr,eptr->replreaderr,sizeof(uint32_t)*REPL_REASONS);
			memcpy(eptr->lreplwriteok,eptr->replwriteok,sizeof(uint32_t)*REPL_REASONS);
			memcpy(eptr->lreplwriteerr,eptr->replwriteerr,sizeof(uint32_t)*REPL_REASONS);
			memcpy(eptr->ldelok,eptr->delok,sizeof(uint32_t)*OP_REASONS);
			memcpy(eptr->ldelerr,eptr->delerr,sizeof(uint32_t)*OP_REASONS);
			memset(eptr->replreadok,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->replreaderr,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->replwriteok,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->replwriteerr,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->delok,0,sizeof(uint32_t)*OP_REASONS);
			memset(eptr->delerr,0,sizeof(uint32_t)*OP_REASONS);
		}
	}
}

void matocsserv_broadcast_regfirst_chunk(uint64_t chunkid) {
	matocsserventry *eptr;
	uint8_t *data;

	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==DATA && eptr->registered!=REGISTERED && eptr->version>=VERSION2INT(4,30,0)) {
			data = matocsserv_create_packet(eptr,MATOCS_REGISTER_FIRST,8);
			put64bit(&data,chunkid);
		}
	}
}

void matocsserv_broadcast_timeout(void) {
	matocsserventry *eptr;
	uint8_t *data;

	if (ForceTimeout>0) {
		for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
			if (eptr->mode==DATA && eptr->version>=VERSION2INT(4,12,0)) {
				eptr->timeout = ForceTimeout;
				data = matocsserv_create_packet(eptr,ANTOAN_FORCE_TIMEOUT,2);
				put16bit(&data,ForceTimeout);
			}
		}
	}
}


uint32_t matocsserv_get_min_cs_version(void) {
	matocsserventry *eptr;
	uint32_t minver = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->csptr!=NULL) {
			if (minver==0 || eptr->version < minver) {
				minver = eptr->version;
			}
		}
	}
	return minver;
}

uint8_t matocsserv_isvalid(void *e) {
	matocsserventry *eptr = (matocsserventry*)e;
	return (eptr->mode==KILL)?0:1;
}

void matocsserv_disconnection_finished(void *e) {
	matocsserventry *eptr = (matocsserventry*)e;
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"server %s has been fully removed from data structures",eptr->servdesc);
	if (eptr->servdesc) {
		free(eptr->servdesc);
	}
	free(eptr);
}

void matocsserv_gotpacket(matocsserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	if (type!=CSTOMA_REGISTER && type!=ANTOAN_NOP && eptr->csid==MAXCSCOUNT) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"got command type %"PRIu32" from unregistered chunk server",type);
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
		case ANTOAN_GET_CONFIG_FILE:
			matocsserv_get_config_file(eptr,data,length);
			break;
		case ANTOMA_SYSLOG:
			matocsserv_syslog(eptr,data,length);
			break;
		case CSTOMA_CHUNK_STATUS:
			matocsserv_got_chunk_status(eptr,data,length);
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
		case CSTOMA_CHUNK_DOESNT_EXIST:
			matocsserv_nonexistent_chunks(eptr,data,length);
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
		case CSTOMA_REPLICATE_SPLIT:
		case CSTOMA_REPLICATE_RECOVER:
		case CSTOMA_REPLICATE_JOIN:
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
		case CSTOMA_LOCALSPLIT:
			matocsserv_got_localsplitchunk_status(eptr,data,length);
			break;
		default:
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master <-> chunkservers module: got unknown message (type:%"PRIu32")",type);
			eptr->mode = KILL;
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
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CS(%s) packet too long (%"PRIu32"/%u) ; command:%"PRIu32,eptr->servdesc,leng,MaxPacketSize,type);
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
		mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"connection with CS(%s) has been closed by peer",eptr->servdesc);
		eptr->input_end = 1;
	} else if (err) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"read from CS(%s) error",eptr->servdesc);
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
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"write to CS(%s) error",eptr->servdesc);
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
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"write to CS(%s) error",eptr->servdesc);
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
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"chunkserver %s disconnected, usedspace: %"PRIu64" (%.2lf GiB), totalspace: %"PRIu64" (%.2lf GiB)",eptr->servdesc,eptr->usedspace,us,eptr->totalspace,ts);
			matocsserv_replication_disconnected(eptr);
			matocsserv_operation_disconnected(eptr);
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
				if (eptr->servdesc) {
					free(eptr->servdesc);
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
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"Master<->CS socket: accept error");
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

			tcpgetpeer(eptr->sock,&(eptr->peerip),NULL);
			eptr->servdesc = univallocstripport(eptr->peerip,0);
			eptr->version = 0;
			eptr->servip = 0;
			eptr->servport = 0;
			if (ForceTimeout>0) {
				eptr->timeout = ForceTimeout;
			} else {
				eptr->timeout = DefaultTimeout;
			}
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

			eptr->lostchunkdelay = LOSTCHUNKDELAY;
			eptr->newchunkdelay = NEWCHUNKDELAY;
			eptr->receivingchunks = (TRANSFERRING_NEW_CHUNKS|TRANSFERRING_LOST_CHUNKS);

			memset(eptr->passwordrnd,0,32);

			memset(eptr->lreplreadok,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->lreplreaderr,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->lreplwriteok,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->lreplwriteerr,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->ldelok,0,sizeof(uint32_t)*OP_REASONS);
			memset(eptr->ldelerr,0,sizeof(uint32_t)*OP_REASONS);

			memset(eptr->replreadok,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->replreaderr,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->replwriteok,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->replwriteerr,0,sizeof(uint32_t)*REPL_REASONS);
			memset(eptr->delok,0,sizeof(uint32_t)*OP_REASONS);
			memset(eptr->delerr,0,sizeof(uint32_t)*OP_REASONS);

//			matocsserv_chunk_status_cleanup(eptr);

			eptr->dist = 0;
			eptr->first = 1;
			eptr->corr = 0.0;

			eptr->csptr = NULL;
		}
	}

// read
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"server: %s ; lastread: %.6lf ; lastwrite: %.6lf ; timeout: %u ; now: %.6lf",eptr->servdesc,eptr->lastread,eptr->lastwrite,eptr->timeout,now);
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
		if (eptr->lastwrite+1.0<now && eptr->outputhead==NULL) {
			matocsserv_create_packet(eptr,ANTOAN_NOP,0);
		}
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				matocsserv_write(eptr,now);
			}
		}
		if ((eptr->lastread+eptr->timeout)<now) {
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
		if (eptr->lastwrite+2.0<now && eptr->outputhead==NULL) {
			matocsserv_create_packet(eptr,ANTOAN_NOP,0);
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
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"master <-> chunkservers module: closing %s:%s",ListenHost,ListenPort);
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
		if (eptr->servdesc) {
			free(eptr->servdesc);
		}
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matocsservhead = NULL;

	matocsserv_read(NULL,0.0); // free internal read buffer
	matocsserv_getservers_wrandom(NULL,NULL); // free internal read buffer

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

void matocsserv_disconnect_all(void) {
	matocsserventry *eptr;
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		eptr->mode = KILL;
	}
	matocsserv_disconnection_loop();
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
	char ipbuff[STRIPSIZE];
	char *srcip,*dstip;
	uint32_t srcclass,dstclass,mask;
	char *reservespace;
	const char *endptr;
	double reservespacevalue;

	ChunkServerCheck = cfg_getuint8("MATOCS_CHUNK_SERVER_CHECK_MODE",0); // debug option

	DefaultTimeout = cfg_getuint32("MATOCS_TIMEOUT",10);
	if (DefaultTimeout>65535) {
		DefaultTimeout=65535;
	} else if (DefaultTimeout<10) {
		DefaultTimeout=10;
	}

	ForceTimeout = cfg_getuint32("MATOCS_FORCE_TIMEOUT",0);
	if (ForceTimeout>0 && ForceTimeout<10) {
		ForceTimeout=10;
	}
	if (ForceTimeout>65535) {
		ForceTimeout=65535;
	}

	if (AuthCode) {
		free(AuthCode);
		AuthCode = NULL;
	}
	if (cfg_isdefined("AUTH_CODE")) {
		AuthCode = cfg_getstr("AUTH_CODE","mfspassword");
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
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing RESERVE_SPACE (\"%s\") ; error on '%c'",reservespace,endptr[0]);
	}
	free(reservespace);

	if (cfg_isdefined("REMAP_BITS") && cfg_isdefined("REMAP_SOURCE_IP_CLASS") && cfg_isdefined("REMAP_DESTINATION_IP_CLASS")) {
		// defaults defined here only to match examples in cfg file
		bits = cfg_getuint8("REMAP_BITS",24);
		srcip = cfg_getstr("REMAP_SOURCE_IP_CLASS","192.168.1.0");
		dstip = cfg_getstr("REMAP_DESTINATION_IP_CLASS","10.0.0.0");
		err = 0;
		if (bits==0 || bits>32) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"wrong value for REMAP_BITS (%"PRIu8" ; shlould be between 1 and 32)",bits);
			err |= 1;
			mask = 0;
		} else {
			mask = UINT32_C(0xFFFFFFFF) << (32-bits);
		}
		if (matocsserv_parse_ip(srcip,&srcclass)==0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class from REMAP_SOURCE_IP_CLASS (%s)",srcip);
			err |= 2;
		}
		if (matocsserv_parse_ip(dstip,&dstclass)==0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class from REMAP_DESTINATION_IP_CLASS (%s)",dstip);
			err |= 4;
		}
		if ((err&3)==0 && ((srcclass & mask) != srcclass)) {
			univmakestrip(ipbuff,srcclass & mask);
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"found garbage bits at the end of REMAP_SOURCE_IP_CLASS (given: %s - should be: %s)",srcip,ipbuff);
			err |= 8;
		}
		if ((err&5)==0 && ((dstclass & mask) != dstclass)) {
			univmakestrip(ipbuff,dstclass & mask);
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"found garbage bits at the end of REMAP_DESTINATION_IP_CLASS (given: %s - should be: %s)",dstip,ipbuff);
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
	uint32_t oldlistenip;
	uint16_t oldlistenport;
	int newlsock;

	matocsserv_reload_common();

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	oldlistenip = listenip;
	oldlistenport = listenport;

	ListenHost = cfg_getstr("MATOCS_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOCS_LISTEN_PORT",DEFAULT_MASTER_CS_PORT);
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"master <-> chunkservers module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"master <-> chunkservers module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"master <-> chunkservers module: socket address has changed, but can't be resolved (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpnumlisten(newlsock,listenip,listenport,100)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"master <-> chunkservers module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"master <-> chunkservers module: can't set accept filter");
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"master <-> chunkservers module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
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
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"master <-> chunkservers module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"master <-> chunkservers module: can't resolve %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpnumlisten(lsock,listenip,listenport,100)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"master <-> chunkservers module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"master <-> chunkservers module: can't set accept filter");
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"master <-> chunkservers module: listen on %s:%s",ListenHost,ListenPort);

	matocsserv_replication_init();
	matocsservhead = NULL;
	receivingchunks = (TRANSFERRING_NEW_CHUNKS|TRANSFERRING_LOST_CHUNKS);

	main_reload_register(matocsserv_reload);
	main_destruct_register(matocsserv_term);
	main_poll_register(matocsserv_desc,matocsserv_serve);
	main_keepalive_register(matocsserv_keep_alive);
	main_info_register(matocsserv_log_extra_info);
	main_time_register(1,0,matocsserv_hlstatus_fix);
	main_time_register(1,0,matocsserv_calculate_space);
	main_time_register(1,0,matocsserv_chunks_delays);
	main_time_register(60,0,matocsserv_reason_counters);
	main_time_register(10,0,matocsserv_broadcast_timeout);
	main_eachloop_register(matocsserv_recalculate_server_counters);
//	main_time_register(TIMEMODE_SKIP_LATE,60,0,matocsserv_status);
	return 0;
}
