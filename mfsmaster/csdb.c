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
#include <stdlib.h>
#include <inttypes.h>

#include "MFSCommunication.h"

#include "csdb.h"
#include "bio.h"
#include "changelog.h"
#include "datapack.h"
#include "slogger.h"
#include "hashfn.h"
#include "massert.h"
#include "matocsserv.h"
#include "cfg.h"
#include "main.h"
#include "metadata.h"

#define CSDB_OP_ADD 0
#define CSDB_OP_DEL 1
#define CSDB_OP_NEWIPPORT 2
#define CSDB_OP_NEWID 3
#define CSDB_OP_MAINTENANCEON 4
#define CSDB_OP_MAINTENANCEOFF 5

static uint32_t HeavyLoadGracePeriod;
static uint32_t HeavyLoadThreshold;
static double HeavyLoadRatioThreshold;

#define CSDBHASHSIZE 256
#define CSDBHASHFN(ip,port) (hash32((ip)^((port)<<16))%(CSDBHASHSIZE))

typedef struct csdbentry {
	uint32_t ip;
	uint16_t port;
	uint16_t csid;
	uint16_t number;
	uint32_t heavyloadts;		// last timestamp of heavy load state (load > thresholds)
	uint32_t load;
	uint8_t maintenance;
	void *eptr;
	struct csdbentry *next;
} csdbentry;

static csdbentry *csdbhash[CSDBHASHSIZE];
static csdbentry **csdbtab;
static uint32_t nextid;
static uint32_t disconnected_servers;
static uint32_t disconnected_servers_in_maintenance;
static uint32_t servers;
static uint32_t disconnecttime;
static uint32_t loadsum;

void csdb_disconnectcheck(void) {
	static uint8_t laststate=0;
	if (disconnected_servers && laststate==0) {
		disconnecttime = main_time();
		laststate = 1;
	} else if (disconnected_servers==0) {
		disconnecttime = 0;
		laststate = 0;
	}
}

uint16_t csdb_newid(void) {
	while (nextid<65536 && csdbtab[nextid]!=NULL) {
		nextid++;
	}
	return nextid;
}

void csdb_delid(uint16_t csid) {
	csdbtab[csid] = NULL;
	if (csid<nextid) {
		nextid = csid;
	}
}

static inline void csdb_makestrip(char strip[16],uint32_t ip) {
	snprintf(strip,16,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,(uint8_t)(ip>>24),(uint8_t)(ip>>16),(uint8_t)(ip>>8),(uint8_t)ip);
	strip[15]=0;
}

void* csdb_new_connection(uint32_t ip,uint16_t port,uint16_t csid,void *eptr) {
	uint32_t hash,hashid;
	csdbentry *csptr,**cspptr,*csidptr;
	char strip[16];
	char strtmpip[16];

	csdb_makestrip(strip,ip);
	if (csid>0) {
		csidptr = csdbtab[csid];
	} else {
		csidptr = NULL;
	}
	if (csidptr && csidptr->ip == ip && csidptr->port == port) { // fast find using csid
		if (csidptr->eptr!=NULL) {
			syslog(LOG_NOTICE,"csdb: found cs using ip:port and csid (%s:%"PRIu16",%"PRIu16"), but server is still connected",strip,port,csid);
			return NULL;
		}
		csidptr->eptr = eptr;
		disconnected_servers--;
		if (csidptr->maintenance) {
			disconnected_servers_in_maintenance--;
		}
		syslog(LOG_NOTICE,"csdb: found cs using ip:port and csid (%s:%"PRIu16",%"PRIu16")",strip,port,csid);
		return csidptr;
	}
	hash = CSDBHASHFN(ip,port);
	for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) { // slow find using (ip+port)
		if (csptr->ip == ip && csptr->port == port) {
			if (csptr->eptr!=NULL) {
				syslog(LOG_NOTICE,"csdb: found cs using ip:port (%s:%"PRIu16",%"PRIu16"), but server is still connected",strip,port,csid);
					return NULL;
			}
			csptr->eptr = eptr;
			disconnected_servers--;
			if (csptr->maintenance) {
				disconnected_servers_in_maintenance--;
			}
			return csptr;
		}
	}
	if (csidptr && csidptr->eptr==NULL) { // ip+port not found, but found csid - change ip+port
		csdb_makestrip(strtmpip,csidptr->ip);
			syslog(LOG_NOTICE,"csdb: found cs using csid (%s:%"PRIu16",%"PRIu16") - previous ip:port (%s:%"PRIu16")",strip,port,csid,strtmpip,csidptr->port);
			hashid = CSDBHASHFN(csidptr->ip,csidptr->port);
			cspptr = csdbhash + hashid;
			while ((csptr=*cspptr)) {
				if (csptr == csidptr) {
					*cspptr = csptr->next;
					csptr->next = csdbhash[hash];
					csdbhash[hash] = csptr;
					break;
				} else {
					cspptr = &(csptr->next);
				}
			}
			csidptr->ip = ip;
			csidptr->port = port;
			changelog("%"PRIu32"|CSDBOP(%u,%"PRIu32",%"PRIu16",%"PRIu16")",main_time(),CSDB_OP_NEWIPPORT,ip,port,csidptr->csid);
		csidptr->eptr = eptr;
		disconnected_servers--;
		if (csidptr->maintenance) {
			disconnected_servers_in_maintenance--;
		}
		return csidptr;
	}
	syslog(LOG_NOTICE,"csdb: server not found (%s:%"PRIu16",%"PRIu16"), add it to database",strip,port,csid);
	csptr = malloc(sizeof(csdbentry));
	passert(csptr);
	csptr->ip = ip;
	csptr->port = port;
	if (csid>0) {
		if (csdbtab[csid]==NULL) {
			csdbtab[csid] = csptr;
		} else {
			csid = 0;
		}
	}
	csptr->csid = csid;
	csptr->heavyloadts = 0;
	csptr->maintenance = 0;
	csptr->load = 0;
	csptr->eptr = eptr;
	csptr->next = csdbhash[hash];
	csdbhash[hash] = csptr;
	servers++;
	changelog("%"PRIu32"|CSDBOP(%u,%"PRIu32",%"PRIu16",%"PRIu16")",main_time(),CSDB_OP_ADD,ip,port,csptr->csid);
	return csptr;
}

void csdb_lost_connection(void *v_csptr) {
	csdbentry *csptr = (csdbentry*)v_csptr;
	if (csptr!=NULL) {
		csptr->eptr = NULL;
		disconnected_servers++;
		if (csptr->maintenance) {
			disconnected_servers_in_maintenance++;
		}
	}
	csdb_disconnectcheck();
}

void csdb_server_load(void *v_csptr,uint32_t load) {
	csdbentry *csptr = (csdbentry*)v_csptr;
	double loadavg;
	char strip[16];
	loadsum -= csptr->load;
	if (servers>1) {
		loadavg = loadsum / (servers-1);
	} else {
		loadavg = load;
	}
	csptr->load = load;
	loadsum += load;
	if (load>HeavyLoadThreshold && load>loadavg*HeavyLoadRatioThreshold) { // cs is in 'heavy load state'
		csdb_makestrip(strip,csptr->ip);
		syslog(LOG_NOTICE,"Heavy load server detected (%s:%u); load: %"PRIu32" ; threshold: %"PRIu32" ; loadavg (without this server): %.2lf ; ratio_threshold: %.2lf",strip,csptr->port,csptr->load,HeavyLoadThreshold,loadavg,HeavyLoadRatioThreshold);
		csptr->heavyloadts = main_time();
	}
}


uint16_t csdb_get_csid(void *v_csptr) {
	csdbentry *csptr = (csdbentry*)v_csptr;
	char strip[16];
	if (csptr->csid==0) {
		csptr->csid = csdb_newid();
		csdbtab[csptr->csid] = csptr;
		changelog("%"PRIu32"|CSDBOP(%u,%"PRIu32",%"PRIu16",%"PRIu16")",main_time(),CSDB_OP_NEWID,csptr->ip,csptr->port,csptr->csid);
		csdb_makestrip(strip,csptr->ip);
		syslog(LOG_NOTICE,"csdb: generate new server id for (%s:%"PRIu16"): %"PRIu16,strip,csptr->port,csptr->csid);
	}
	return csptr->csid;
}

uint8_t csdb_server_is_overloaded(void *v_csptr,uint32_t now) {
	csdbentry *csptr = (csdbentry*)v_csptr;
	return (csptr->heavyloadts+HeavyLoadGracePeriod<=now)?0:1;
}

uint8_t csdb_server_is_being_maintained(void *v_csptr) {
	csdbentry *csptr = (csdbentry*)v_csptr;
	return csptr->maintenance;
}

uint32_t csdb_servlist_size(void) {
	uint32_t hash;
	csdbentry *csptr;
	uint32_t i;
	i=0;
	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
			i++;
		}
	}
	return i*(4+4+2+2+8+8+4+8+8+4+4+4+4);
}

void csdb_servlist_data(uint8_t *ptr) {
	uint32_t hash;
	uint32_t now = main_time();
	uint32_t gracetime;
	uint8_t *p;
	csdbentry *csptr;
	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
			if (csptr->heavyloadts+HeavyLoadGracePeriod>now) {
				gracetime = csptr->heavyloadts+HeavyLoadGracePeriod-now; // seconds to be turned back to work
			} else {
				gracetime = 0; // Server is working properly and never was in heavy load state
			}
			p = ptr;
			if (csptr->eptr) {
				uint32_t version,chunkscount,tdchunkscount,errorcounter,load;
				uint64_t usedspace,totalspace,tdusedspace,tdtotalspace;
				matocsserv_getservdata(csptr->eptr,&version,&usedspace,&totalspace,&chunkscount,&tdusedspace,&tdtotalspace,&tdchunkscount,&errorcounter,&load);
				put32bit(&ptr,version&0xFFFFFF);
				put32bit(&ptr,csptr->ip);
				put16bit(&ptr,csptr->port);
				put16bit(&ptr,csptr->csid);
				put64bit(&ptr,usedspace);
				put64bit(&ptr,totalspace);
				put32bit(&ptr,chunkscount);
				put64bit(&ptr,tdusedspace);
				put64bit(&ptr,tdtotalspace);
				put32bit(&ptr,tdchunkscount);
				put32bit(&ptr,errorcounter);
				put32bit(&ptr,load);
				put32bit(&ptr,gracetime);
			} else {
				put32bit(&ptr,0x01000000);
				put32bit(&ptr,csptr->ip);
				put16bit(&ptr,csptr->port);
				put16bit(&ptr,csptr->csid);
				put64bit(&ptr,0);
				put64bit(&ptr,0);
				put32bit(&ptr,0);
				put64bit(&ptr,0);
				put64bit(&ptr,0);
				put32bit(&ptr,0);
				put32bit(&ptr,0);
				put32bit(&ptr,0);
				put32bit(&ptr,gracetime);
			}
			if (csptr->maintenance) {
				*p |= 2;
			}
		}
	}
}

uint8_t csdb_remove_server(uint32_t ip,uint16_t port) {
	uint32_t hash;
	csdbentry *csptr,**cspptr;

	hash = CSDBHASHFN(ip,port);
	cspptr = csdbhash + hash;
	while ((csptr=*cspptr)) {
		if (csptr->ip == ip && csptr->port == port) {
			if (csptr->eptr!=NULL) {
				return ERROR_ACTIVE;
			}
			if (csptr->csid>0) {
				csdb_delid(csptr->csid);
			}
			if (csptr->maintenance) {
				disconnected_servers_in_maintenance--;
			}
			*cspptr = csptr->next;
			free(csptr);
			servers--;
			disconnected_servers--;
			changelog("%"PRIu32"|CSDBOP(%u,%"PRIu32",%"PRIu16",0)",main_time(),CSDB_OP_DEL,ip,port);
			return STATUS_OK;
		} else {
			cspptr = &(csptr->next);
		}
	}
	return ERROR_NOTFOUND;
}

uint8_t csdb_mr_op(uint8_t op,uint32_t ip,uint16_t port, uint16_t csid) {
	uint32_t hash,hashid;
	csdbentry *csptr,**cspptr,*csidptr;

	switch (op) {
		case CSDB_OP_ADD:
			hash = CSDBHASHFN(ip,port);
			for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
				if (csptr->ip == ip && csptr->port == port) {
					return ERROR_MISMATCH;
				}
			}
			if (csid>0 && csdbtab[csid]!=NULL) {
				return ERROR_MISMATCH;
			}
			csptr = malloc(sizeof(csdbentry));
			passert(csptr);
			csptr->ip = ip;
			csptr->port = port;
			csptr->csid = csid;
			csdbtab[csid] = csptr;
			csptr->heavyloadts = 0;
			csptr->maintenance = 0;
			csptr->load = 0;
			csptr->eptr = NULL;
			csptr->next = csdbhash[hash];
			csdbhash[hash] = csptr;
			servers++;
			disconnected_servers++;
			meta_version_inc();
			return STATUS_OK;
		case CSDB_OP_DEL:
			hash = CSDBHASHFN(ip,port);
			cspptr = csdbhash + hash;
			while ((csptr=*cspptr)) {
				if (csptr->ip == ip && csptr->port == port) {
					if (csptr->eptr!=NULL) {
						return ERROR_MISMATCH;
					}
					if (csptr->csid>0) {
						csdb_delid(csptr->csid);
					}
					if (csptr->maintenance) {
						disconnected_servers_in_maintenance--;
					}
					*cspptr = csptr->next;
					free(csptr);
					servers--;
					disconnected_servers--;
					meta_version_inc();
					return STATUS_OK;
				} else {
					cspptr = &(csptr->next);
				}
			}
			return ERROR_MISMATCH;
		case CSDB_OP_NEWIPPORT:
			if (csid==0 || csdbtab[csid]==NULL) {
				return ERROR_MISMATCH;
			}
			csidptr = csdbtab[csid];

			hashid = CSDBHASHFN(csidptr->ip,csidptr->port);
			hash = CSDBHASHFN(ip,port);
			cspptr = csdbhash + hashid;
			while ((csptr=*cspptr)) {
				if (csptr == csidptr) {
					*cspptr = csptr->next;
					csptr->next = csdbhash[hash];
					csdbhash[hash] = csptr;
					break;
				} else {
					cspptr = &(csptr->next);
				}
			}
			if (csptr==NULL) {
				return ERROR_MISMATCH;
			}
			csptr->ip = ip;
			csptr->port = port;
			meta_version_inc();
			return STATUS_OK;
		case CSDB_OP_NEWID:
			if (csid==0 || csdbtab[csid]!=NULL) {
				return ERROR_MISMATCH;
			}
			hash = CSDBHASHFN(ip,port);
			for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
				if (csptr->ip == ip && csptr->port == port) {
					if (csptr->csid!=csid) {
						if (csptr->csid>0) {
							csdb_delid(csptr->csid);
						}
						csptr->csid = csid;
						csdbtab[csid] = csptr;
					}
					meta_version_inc();
					return STATUS_OK;
				}
			}
			return ERROR_MISMATCH;
		case CSDB_OP_MAINTENANCEON:
			hash = CSDBHASHFN(ip,port);
			for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
				if (csptr->ip == ip && csptr->port == port) {
					if (csptr->maintenance!=0) {
						return ERROR_MISMATCH;
					}
					csptr->maintenance = 1;
					if (csptr->eptr==NULL) {
						disconnected_servers_in_maintenance++;
					}
					meta_version_inc();
					return STATUS_OK;
				}
			}
			return ERROR_MISMATCH;
		case CSDB_OP_MAINTENANCEOFF:
			hash = CSDBHASHFN(ip,port);
			for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
				if (csptr->ip == ip && csptr->port == port) {
					if (csptr->maintenance!=1) {
						return ERROR_MISMATCH;
					}
					csptr->maintenance = 0;
					if (csptr->eptr==NULL) {
						disconnected_servers_in_maintenance--;
					}
					meta_version_inc();
					return STATUS_OK;
				}
			}
			return ERROR_MISMATCH;
	}
	return ERROR_MISMATCH;
}

uint8_t csdb_back_to_work(uint32_t ip,uint16_t port) {
	uint32_t hash;
	csdbentry *csptr;

	hash = CSDBHASHFN(ip,port);
	for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
		if (csptr->ip == ip && csptr->port == port) {
			csptr->heavyloadts = 0;
			return STATUS_OK;
		}
	}
	return ERROR_NOTFOUND;
}

uint8_t csdb_maintenance(uint32_t ip,uint16_t port,uint8_t onoff) {
	uint32_t hash;
	csdbentry *csptr;

	if (onoff!=0 && onoff!=1) {
		return ERROR_EINVAL;
	}
	hash = CSDBHASHFN(ip,port);
	for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
		if (csptr->ip == ip && csptr->port == port) {
			if (csptr->maintenance!=onoff) {
				csptr->maintenance = onoff;
				if (onoff) {
					changelog("%"PRIu32"|CSDBOP(%u,%"PRIu32",%"PRIu16",0)",main_time(),CSDB_OP_MAINTENANCEON,ip,port);
				} else {
					changelog("%"PRIu32"|CSDBOP(%u,%"PRIu32",%"PRIu16",0)",main_time(),CSDB_OP_MAINTENANCEOFF,ip,port);
				}
				if (csptr->eptr==NULL) {
					if (onoff) {
						disconnected_servers_in_maintenance++;
					} else {
						disconnected_servers_in_maintenance--;
					}
				}
			}
			return STATUS_OK;
		}
	}
	return ERROR_NOTFOUND;
}

/*
uint8_t csdb_find(uint32_t ip,uint16_t port,uint16_t csid) {
	uint32_t hash;
	csdbentry *csptr;

	hash = CSDBHASHFN(ip,port);
	for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
		if (csptr->ip == ip && csptr->port == port) {
			return 1;
		}
	}
	if (csid>0 && csdbtab[csid]!=NULL) {
		return 2;
	}
	return 0;
}
*/

uint8_t csdb_have_all_servers(void) {
	return (disconnected_servers>0)?0:1;
}

uint8_t csdb_have_more_than_half_servers(void) {
	return ((servers==0)||(disconnected_servers<((servers+1)/2)))?1:0;
}

uint8_t csdb_replicate_undergoals(void) {
	return (disconnected_servers>0 && disconnected_servers==disconnected_servers_in_maintenance)?0:1;
}

int csdb_compare(const void *a,const void *b) {
	const csdbentry *aa = *((const csdbentry**)a);
	const csdbentry *bb = *((const csdbentry**)b);
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

uint16_t csdb_sort_servers(void) {
	csdbentry **stab,*csptr;
	uint32_t i,hash;

	stab = malloc(sizeof(csdbentry*)*servers);
	i = 0;
	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
			if (i<servers) {
				stab[i] = csptr;
				i++;
			} else {
				syslog(LOG_WARNING,"internal error: wrong chunk servers count !!!");
				csptr->number = 0;
			}
		}
	}
	qsort(stab,servers,sizeof(csdbentry*),csdb_compare);
	for (i=0 ; i<servers ; i++) {
		stab[i]->number = i+1;
	}
	free(stab);

	return servers;
}

uint16_t csdb_getnumber(void *v_csptr) {
	csdbentry *csptr = (csdbentry*)v_csptr;
	if (csptr!=NULL) {
		return csptr->number;
	}
	return 0;
}

uint8_t csdb_store(bio *fd) {
	uint32_t hash;
	uint8_t wbuff[9*100],*ptr;
	csdbentry *csptr;
	uint32_t l;
	l=0;

	if (fd==NULL) {
		return 0x12;
	}
	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
			l++;
		}
	}
	ptr = wbuff;
	put32bit(&ptr,l);
	if (bio_write(fd,wbuff,4)!=4) {
		syslog(LOG_NOTICE,"write error");
		return 0xFF;
	}
	l=0;
	ptr=wbuff;
	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
			if (l==100) {
				if (bio_write(fd,wbuff,9*100)!=(9*100)) {
					syslog(LOG_NOTICE,"write error");
					return 0xFF;
				}
				l=0;
				ptr=wbuff;
			}
			put32bit(&ptr,csptr->ip);
			put16bit(&ptr,csptr->port);
			put16bit(&ptr,csptr->csid);
			put8bit(&ptr,csptr->maintenance);
			l++;
		}
	}
	if (l>0) {
		if (bio_write(fd,wbuff,9*l)!=(9*l)) {
			syslog(LOG_NOTICE,"write error");
			return 0xFF;
		}
	}
	return 0;
}

int csdb_load(bio *fd,uint8_t mver,int ignoreflag) {
	uint8_t rbuff[9*100];
	const uint8_t *ptr;
	csdbentry *csptr;
	uint32_t hash;
	uint32_t l,t,ip;
	uint16_t port,csid;
	uint8_t maintenance;
	uint8_t nl=1;
	uint32_t bsize;

	if (bio_read(fd,rbuff,4)!=4) {
		int err = errno;
		if (nl) {
			fputc('\n',stderr);
			// nl=0;
		}
		errno = err;
		mfs_errlog(LOG_ERR,"loading chunkservers: read error");
		return -1;
	}
	ptr=rbuff;
	t = get32bit(&ptr);
	if (mver<=0x10) {
		bsize = 6;
	} else if (mver<=0x11) {
		bsize = 8;
	} else {
		bsize = 9;
	}
	l=0;
	while (t>0) {
		if (l==0) {
			if (t>100) {
				if (bio_read(fd,rbuff,bsize*100)!=(bsize*100)) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						// nl=0;
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading chunkservers: read error");
					return -1;
				}
				l=100;
			} else {
				if (bio_read(fd,rbuff,bsize*t)!=(bsize*t)) {
					int err = errno;
					if (nl) {
						fputc('\n',stderr);
						// nl=0;
					}
					errno = err;
					mfs_errlog(LOG_ERR,"loading free nodes: read error");
					return -1;
				}
				l=t;
			}
			ptr = rbuff;
		}
		ip = get32bit(&ptr);
		port = get16bit(&ptr);
		if (mver>=0x11) {
			csid = get16bit(&ptr);
		} else {
			csid = 0;
		}
		if (mver>=0x12) {
			maintenance = get8bit(&ptr);
		} else {
			maintenance = 0;
		}
		hash = CSDBHASHFN(ip,port);
		for (csptr = csdbhash[hash] ; csptr ; csptr = csptr->next) {
			if (csptr->ip == ip && csptr->port == port) {
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				fprintf(stderr,"repeated chunkserver entry (ip:%"PRIu32",port:%"PRIu16")\n",ip,port);
				syslog(LOG_ERR,"repeated chunkserver entry (ip:%"PRIu32",port:%"PRIu16")",ip,port);
				if (ignoreflag==0) {
					fprintf(stderr,"use '-i' option to remove this chunkserver definition");
					return -1;
				}
			}
		}
		if (csid>0) {
			csptr = csdbtab[csid];
			if (csptr!=NULL) {
				if (nl) {
					fputc('\n',stderr);
					nl=0;
				}
				fprintf(stderr,"repeated chunkserver entry (csid:%"PRIu16")\n",csid);
				syslog(LOG_ERR,"repeated chunkserver entry (csid:%"PRIu16")",csid);
				if (ignoreflag==0) {
					fprintf(stderr,"use '-i' option to remove this chunkserver definition");
					return -1;
				}
			}
		}
		csptr = malloc(sizeof(csdbentry));
		passert(csptr);
		csptr->ip = ip;
		csptr->port = port;
		csptr->csid = csid;
		if (csid>0) {
			csdbtab[csid] = csptr;
		}
		csptr->number = 0;
		csptr->heavyloadts = 0;
		csptr->load = 0;
		csptr->eptr = NULL;
		csptr->maintenance = maintenance;
		csptr->next = csdbhash[hash];
		csdbhash[hash] = csptr;
		servers++;
		disconnected_servers++;
		if (maintenance) {
			disconnected_servers_in_maintenance++;
		}
		l--;
		t--;
	}
	return 0;
}

void csdb_cleanup(void) {
	uint32_t hash;
	csdbentry *csptr,*csnptr;

	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		csptr = csdbhash[hash];
		while (csptr) {
			csnptr = csptr->next;
			free(csptr);
			csptr = csnptr;
		}
		csdbhash[hash]=NULL;
	}
	for (hash=0 ; hash<65536 ; hash++) {
		csdbtab[hash] = NULL;
	}
	nextid = 1;
	disconnected_servers = 0;
	disconnected_servers_in_maintenance = 0;
	servers = 0;
}

uint32_t csdb_getdisconnecttime(void) {
	return disconnecttime;
}

void csdb_reload(void) {
	HeavyLoadGracePeriod = cfg_getuint32("CS_HEAVY_LOAD_GRACE_PERIOD",900);
	HeavyLoadThreshold = cfg_getuint32("CS_HEAVY_LOAD_THRESHOLD",150);
	HeavyLoadRatioThreshold = cfg_getdouble("CS_HEAVY_LOAD_RATIO_THRESHOLD",3.0);
}

int csdb_init(void) {
	uint32_t hash;
	csdb_reload();
	for (hash=0 ; hash<CSDBHASHSIZE ; hash++) {
		csdbhash[hash]=NULL;
	}
	csdbtab = malloc(sizeof(csdbentry*)*65536);
	passert(csdbtab);
	for (hash=0 ; hash<65536 ; hash++) {
		csdbtab[hash] = NULL;
	}
	nextid = 1;
	disconnected_servers = 0;
	disconnected_servers_in_maintenance = 0;
	servers = 0;
	disconnecttime = 0;
	loadsum = 0;
	main_reload_register(csdb_reload);
	main_time_register(1,0,csdb_disconnectcheck);
	return 0;
}
