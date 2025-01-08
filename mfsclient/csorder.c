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

#include <stdlib.h>
#include <pthread.h>
#include <inttypes.h>

#include "labelparser.h"
#include "csorder.h"
#include "csdb.h"
#include "massert.h"
#include "datapack.h"

/*
typedef struct _cspri {
	uint32_t ip;
	uint16_t port;
	uint32_t version;
	uint32_t labelmask;
	uint32_t priority;
} cspri;
*/

//static uint8_t labelscnt;
//static uint32_t labelmasks[9][MASKORGROUP];
parser_data pd;

int csorder_init(const char *labelexpr) {
	if (labelexpr==NULL) {
		pd.labelscnt = 0;
		return 0;
	} else {
	//	return parse_label_expr(labelexpr,&labelscnt,labelmasks);
		return parse_label_expr(labelexpr, &pd);

	}
}


static inline uint8_t csorder_server_matches_labelexpr(uint32_t labelmask,uint8_t *labelexpr) {
	static uint8_t stack[SCLASS_EXPR_MAX_SIZE];
	uint8_t n,r;
	uint8_t sp;

	sp = 0;
	if (*labelexpr) {
		while ((n=*labelexpr++)) {
			switch (n&SCLASS_EXPR_TYPE_MASK) {
				case SCLASS_EXPR_SYMBOL:
					if (n==SCLASS_EXPR_SYMBOL_ANY) {
						stack[sp++]=1;
					} else {
						n &= SCLASS_EXPR_VALUE_MASK;
						if (labelmask & (1<<n)) {
							stack[sp++]=1;
						} else {
							stack[sp++]=0;
						}
					}
					break;

				case SCLASS_EXPR_OP_AND:
					n &= SCLASS_EXPR_VALUE_MASK;
					n += 2;
					if (n>sp) {
							return 0;
					}
					r = 1;
					while (n>0) {
						if (stack[--sp]==0) {
							r = 0;
						}
						n--;
					}
					stack[sp++] = r;
					break;

				case SCLASS_EXPR_OP_OR:
					n &= SCLASS_EXPR_VALUE_MASK;
					n += 2;
					if (n>sp) {
						return 0;
					}
					r = 0;
					while (n>0) {
						if (stack[--sp]==1) {
							r = 1;
						}
						n--;
					}
					stack[sp++] = r;
					break;

				case SCLASS_EXPR_OP_ONE:
					n &= SCLASS_EXPR_VALUE_MASK;
					if (n==SCLASS_EXPR_OP_NOT) {
						if (sp==0) {
								return 0;
						}
						stack[sp-1] = 1 - stack[sp-1];
					}
					break;

				default:
					;
			}//switch()
		}//while(n...)
		if (sp==1) {
			return stack[0];
		}
		return 0;
	} else { // '*'
		return 1;
	}
}


uint8_t csorder_calc(uint32_t labelmask) {
	uint8_t i;//,j;
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"labelmask: %08"PRIX32,labelmask);
	for (i=0 ; i<pd.labelscnt ; i++) {
		if (csorder_server_matches_labelexpr( labelmask, pd.labelexpr[i])) {
			return i;
		}
	}
	return pd.labelscnt;
}

int csorder_cmp(const void *a,const void *b) {
	cspri *aa = (cspri*)a;
	cspri *bb = (cspri*)b;
	if (aa->priority < bb->priority) {
		return -1;
	} else if (aa->priority > bb->priority) {
		return 1;
	} else {
		return 0;
	}
}

/*
static inline void csorder_log_chain_element(uint8_t indx,cspri *chelem) {
	char labelstrbuff[26*2];
	uint32_t i,j;
	j = 0;
	for (i=0 ; i<26 ; i++) {
		if (chelem->labelmask & (1<<i)) {
			if (j>0) {
				labelstrbuff[j++]=',';
			}
			labelstrbuff[j++]='A'+i;
		}
	}
	if (j==0) {
		memcpy(labelstrbuff,"(empty)",7);
		j=7;
	}
	labelstrbuff[j]=0;
	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"chain_log: chunk[%"PRIu8"] = {ip:%u.%u.%u.%u ; port:%"PRIu16" ; labelmask: %s ; priority: %u.%u}",indx,(chelem->ip>>24)&0xFF,(chelem->ip>>16)&0xFF,(chelem->ip>>8)&0xFF,chelem->ip&0xFF,chelem->port,labelstrbuff,(chelem->priority>>24)&0xFF,chelem->priority&0xFFFFFF);
}
*/

uint32_t csorder_sort(cspri chain[100],uint8_t csdataver,const uint8_t *csdata,uint32_t csdatasize,uint8_t writeflag) {
	const uint8_t *cp,*cpe;
//	char labelsbuff[LABELS_BUFF_SIZE];
	uint32_t i;
//	uint32_t j;

//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"csorder_sort: csdataver: %"PRIu8" ; csdatasize: %"PRIu32" ; writeflag: %"PRIu8"\n",csdataver,csdatasize,writeflag);
	cp = csdata;
	cpe = csdata+csdatasize;
	i = 0;
	while (cp<cpe && i<100) {
		chain[i].ip = get32bit(&cp);
		chain[i].port = get16bit(&cp);
		if (csdataver>0) {
			chain[i].version = get32bit(&cp);
		} else {
			chain[i].version = 0;
		}
		if (csdataver>1) {
			chain[i].labelmask = get32bit(&cp);
		} else {
			chain[i].labelmask = 0;
		}
		chain[i].priority = csorder_calc(chain[i].labelmask);
		chain[i].priority <<= 24;
		if (writeflag) {
			chain[i].priority += i;
		} else {
			chain[i].priority += csdb_getopcnt(chain[i].ip,chain[i].port);
		}
//		csorder_log_chain_element(i,chain+i);
		i++;
	}
//	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"csorder_sort: sort using: %s",make_label_expr(labelsbuff,labelscnt,labelmasks));
	if (csdataver!=3) { // 3 - data split into eight parts - do not sort them !!!
		qsort(chain,i,sizeof(cspri),csorder_cmp);
	}
//	for (j=0 ; j<i ; j++) {
//		csorder_log_chain_element(j,chain+j);
//	}
	return i;
}
