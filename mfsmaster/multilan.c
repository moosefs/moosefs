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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <inttypes.h>

#include "MFSCommunication.h"

#include "cfg.h"
#include "main.h"
#include "massert.h"
#include "mfslog.h"
#include "csipmap.h"

static uint32_t MultiLanMask = 0;
static uint32_t MultiLanClasses = 0;
static uint32_t *MultiLanClassTab = NULL;

// adjust server ip to match client ip class
uint32_t multilan_map(uint32_t servip,uint32_t clientip) {
	uint8_t vmask = 0;
	uint32_t mapip;
	uint32_t i;

	if (clientip==0) {
		return servip;
	}

	mapip = csipmap_map(servip,clientip);
	if (mapip!=0) {
		return mapip;
	}

	if (MultiLanMask==0) {
		return servip;
	}

	for (i=0 ; i<MultiLanClasses && vmask!=3 ; i++) {
		if ((clientip & MultiLanMask) == (MultiLanClassTab[i] & MultiLanMask)) {
			vmask |= 1;
		}
		if ((servip & MultiLanMask) == (MultiLanClassTab[i] & MultiLanMask)) {
			vmask |= 2;
		}
	}
	if (vmask==3) {
		return (clientip & MultiLanMask) | (servip & ~MultiLanMask);
	}
	return servip;
}

uint32_t multilan_match(uint32_t servip,uint32_t *iptab,uint32_t iptablen) {
	uint32_t i,j;
	uint32_t res;
	uint8_t f;

	if (MultiLanMask==0 || iptablen==0) {
		return servip;
	}

	f = 0;
	for (i=0 ; i<MultiLanClasses && f==0 ; i++) {
		if ((servip & MultiLanMask) == (MultiLanClassTab[i] & MultiLanMask)) {
			f = 1;
		}
	}

	if (f==0) {
		return servip;
	}

	res = 0;
	for (j=0 ; j<iptablen ; j++) {
		if ((iptab[j] & ~MultiLanMask) == (servip & ~MultiLanMask)) { // found candidate
			for (i=0 ; i<MultiLanClasses ; i++) {
				if ((iptab[j] & MultiLanMask) == (MultiLanClassTab[i] & MultiLanMask)) { // class match
					if (res==0) { // first candidate
						res = iptab[j];
					} else { // found second - this is error case - return original
						return servip;
					}
				}
			}
		}
	}

	if (res==0) {
		return servip;
	}

	return res;
}

void multilan_term(void) {
	if (MultiLanClassTab!=NULL) {
		free(MultiLanClassTab);
	}
	csipmap_term();
}

// W.X.Y.Z, A.B.C.D
uint8_t multilan_parse_netlist(const char *netliststr,uint32_t commonmask,uint32_t *nets,uint32_t **nettab) {
	const char *rptr;
	char c;
	uint32_t ip,octet,i,j;
	uint32_t n,*t;

	n = 1;
	for (rptr = netliststr ; *rptr ; rptr++) {
		c = *rptr;
		if (c==',' || c==';') {
			n++;
		}
	}

	t = malloc(sizeof(uint32_t)*n);
	passert(t);

	j = 0;
	i = 0;
	ip = 0;
	octet = 0;
	for (rptr = netliststr ; j<n ; rptr++) {
		c = *rptr;
		if (c==0) {
			c=',';
		}
		if (c==' ' || c=='\t' || c=='\r' || c=='\n') {
			continue;
		}
		if (c>='0' && c<='9') {
			octet *= 10;
			octet += c-'0';
			if (octet>255) {
				free(t);
				return 1;
			}
			continue;
		}
		if (c=='.' || c==',' || c==';') {
			i++;
			if (i>4) {
				free(t);
				return 2;
			}
			ip *= 256;
			ip += octet;
			octet = 0;
		}
		if (c==',' || c==';') {
			while (i<4) {
				i++;
				ip *= 256;
			}
			if ((ip & commonmask) != ip) {
				free(t);
				return 3;
			}
			if (ip==0) {
				free(t);
				return 4;
			}
			t[j] = ip;
			ip = 0;
			i = 0;
			j++;
		}
		if (*rptr==0) {
			break;
		}
	}
	if (j<n) {
		free(t);
		return 5;
	}
	*nets = n;
	*nettab = t;
	return 0;
}

void multilan_reload (void) {
	uint8_t bits,err;
	char *netstr;
	char *ipmapfname;
	uint32_t mask,ipclass,i;
	uint32_t nets,*nettab;

	if (cfg_isdefined("MULTILAN_BITS") && cfg_isdefined("MULTILAN_CLASSES")) {
		// defaults here are not used and their values only have to match cfg file
		bits = cfg_getuint8("MULTILAN_BITS",24);
		netstr = cfg_getstr("MULTILAN_CLASSES","192.168.15.0, 10.10.10.0, 172.16.5.0");
		err = 0;
		if (bits==0 || bits>32) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"wrong value for MULTILAN_BITS (%"PRIu8" ; shlould be between 1 and 32)",bits);
			err = 1;
			mask = 0;
		} else {
			mask = UINT32_C(0xFFFFFFFF) << (32-bits);
		}
		if (err==0) {
			err = multilan_parse_netlist(netstr,mask,&nets,&nettab);
			switch (err) {
				case 1:
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class from MULTILAN_CLASSES - octet>255 (%s)",netstr);
					break;
				case 2:
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class from MULTILAN_CLASSES - too many octets (%s)",netstr);
					break;
				case 3:
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class from MULTILAN_CLASSES - garbage bits at the end of ip class (%s)",netstr);
					break;
				case 4:
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class from MULTILAN_CLASSES - found empty class (%s)",netstr);
					break;
				case 5:
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error parsing ip class from MULTILAN_CLASSES (%s)",netstr);
			}
		}
		if (err==0) {
			if (MultiLanClassTab!=NULL) {
				free(MultiLanClassTab);
			}
			MultiLanMask = mask;
			MultiLanClasses = nets;
			MultiLanClassTab = nettab;
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"accepted %u lans for multilan configuration with %u bits (mask: %u.%u.%u.%u)",MultiLanClasses,bits,(MultiLanMask>>24)&0xFF,(MultiLanMask>>16)&0xFF,(MultiLanMask>>8)&0xFF,MultiLanMask&0xFF);
			for (i=0 ; i<MultiLanClasses ; i++) {
				ipclass = MultiLanClassTab[i] & MultiLanMask;
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"class %u: %u.%u.%u.%u",i,(ipclass>>24)&0xFF,(ipclass>>16)&0xFF,(ipclass>>8)&0xFF,ipclass&0xFF);
			}
		}
		free(netstr);
	} else {
		if (MultiLanClassTab!=NULL) {
			free(MultiLanClassTab);
		}
		MultiLanMask = 0;
		MultiLanClasses = 0;
		MultiLanClassTab = NULL;
	}
	ipmapfname = cfg_getstr("MULTILAN_IPMAP_FILENAME",ETC_PATH "/mfs/mfsipmap.cfg");
	csipmap_loadmap(ipmapfname);
	free(ipmapfname);
}

int multilan_init(void) {
	if (csipmap_init()==0) {	
		multilan_reload();

		main_reload_register(multilan_reload);
		main_destruct_register(multilan_term);
		return 0;
	}
	return 1;
}
