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

#include <inttypes.h>
#include <stdio.h>

/*
     open(inode,sessionid) -> isset? (inode,sessionid)
     access(inode,sessionid) -> set (inode,sessionid)
     modify(inode,sessionid) -> clear (inode,!sessionid) and set (inode,sessionid)
*/

#define DCM_TAB_LENG 500000
#define DCM_INODEHASH_LENG ((DCM_TAB_LENG)/2)

#define DCM_INODE_HASH(inode) (((inode)*0x4A4FECD1)%DCM_INODEHASH_LENG)

#define DCM_NIL 0xFFFFFFFF

typedef struct _datacache_entry {
	uint32_t inode;
	unsigned cacheok:1;
	unsigned sessionid:31;
	uint32_t iprev,inext;
	uint32_t lruprev,lrunext;
} datacache_entry;

static datacache_entry dcm_tab[DCM_TAB_LENG];
static uint32_t dcm_inodehash[DCM_INODEHASH_LENG];
static uint32_t dcm_lru_first,dcm_lru_last;

// static uint32_t elcount;

int dcm_open(uint32_t inode,uint32_t sessionid) {
	uint32_t ih = DCM_INODE_HASH(inode);
	uint32_t p,pp,np;
	p = dcm_inodehash[ih];
	while (p<DCM_TAB_LENG) {
		if (dcm_tab[p].inode == inode && dcm_tab[p].sessionid == sessionid) {
			/* move element to the end of LRU list */
			if (dcm_lru_last!=p) {	/* do it only if needed */
				/* remove from LRU chain */
				pp = dcm_tab[p].lruprev;
				np = dcm_tab[p].lrunext;
				if (pp<DCM_TAB_LENG) {
					dcm_tab[pp].lrunext = np;
				} else {
					dcm_lru_first = np;
				}
				if (np<DCM_TAB_LENG) {
					dcm_tab[np].lruprev = pp;
				} else {
					dcm_lru_last = pp;
				}
				/* add at the end of LRU chain */
				dcm_tab[p].lruprev = dcm_lru_last;
				dcm_tab[p].lrunext = DCM_NIL;
				dcm_tab[dcm_lru_last].lrunext = p;
				dcm_lru_last = p;
			}
			return dcm_tab[p].cacheok;
		}
		p = dcm_tab[p].inext;
	}

	/* move one element from the beginning to the end */
	p = dcm_lru_first;

	/* remove from LRU chain */
	pp = dcm_tab[p].lruprev;
	np = dcm_tab[p].lrunext;
	if (pp<DCM_TAB_LENG) {
		dcm_tab[pp].lrunext = np;
	} else {
		dcm_lru_first = np;
	}
	if (np<DCM_TAB_LENG) {
		dcm_tab[np].lruprev = pp;
	} else {
		dcm_lru_last = pp;
	}
	/* add at the end of LRU chain */
	dcm_tab[p].lruprev = dcm_lru_last;
	dcm_tab[p].lrunext = DCM_NIL;
	dcm_tab[dcm_lru_last].lrunext = p;
	dcm_lru_last = p;

	/* then replace this element using current values */
	if (dcm_tab[p].inode>0) {	/* if element is not empty then remove it first */
		/* remove from old INODE chain */
		ih = DCM_INODE_HASH(dcm_tab[p].inode);
		pp = dcm_tab[p].iprev;
		np = dcm_tab[p].inext;
		if (pp<DCM_TAB_LENG) {
			dcm_tab[pp].inext = np;
		} else {
			dcm_inodehash[ih] = np;
		}
		if (np<DCM_TAB_LENG) {
			dcm_tab[np].iprev = pp;
		}
//	} else {
//		elcount++;
	}

	/* replace values and add to new INODE chain */
	dcm_tab[p].inode = inode;
	dcm_tab[p].cacheok = 0;
	dcm_tab[p].sessionid = sessionid;
	ih = DCM_INODE_HASH(inode);
	np = dcm_inodehash[ih];
	dcm_tab[p].inext = np;
	dcm_tab[p].iprev = DCM_NIL;
	dcm_inodehash[ih] = p;
	if (np<DCM_TAB_LENG) {
		dcm_tab[np].iprev = p;
	}
	return 0;
}

void dcm_access(uint32_t inode,uint32_t sessionid) {
	uint32_t ih = DCM_INODE_HASH(inode);
	uint32_t p,pp,np;
	p = dcm_inodehash[ih];
	while (p<DCM_TAB_LENG) {
		if (dcm_tab[p].inode == inode && dcm_tab[p].sessionid == sessionid) {
			/* move element to the end of LRU list */
			if (dcm_lru_last!=p) {	/* do it only if needed */
				/* remove from LRU chain */
				pp = dcm_tab[p].lruprev;
				np = dcm_tab[p].lrunext;
				if (pp<DCM_TAB_LENG) {
					dcm_tab[pp].lrunext = np;
				} else {
					dcm_lru_first = np;
				}
				if (np<DCM_TAB_LENG) {
					dcm_tab[np].lruprev = pp;
				} else {
					dcm_lru_last = pp;
				}
				/* add at the end of LRU chain */
				dcm_tab[p].lruprev = dcm_lru_last;
				dcm_tab[p].lrunext = DCM_NIL;
				dcm_tab[dcm_lru_last].lrunext = p;
				dcm_lru_last = p;
			}
			dcm_tab[p].cacheok = 1;
			return;
		}
		p = dcm_tab[p].inext;
	}
}

void dcm_modify(uint32_t inode,uint32_t sessionid) {
	uint32_t ih = DCM_INODE_HASH(inode);
	uint32_t p,pp,np;
	p = dcm_inodehash[ih];
	while (p<DCM_TAB_LENG) {
		if (dcm_tab[p].inode == inode && dcm_tab[p].sessionid != sessionid) {
			/* move to the beginning of LRU chain */
			if (p!=dcm_lru_first) {
				/* remove from LRU chain */
				pp = dcm_tab[p].lruprev;
				np = dcm_tab[p].lrunext;
				if (pp<DCM_TAB_LENG) {
					dcm_tab[pp].lrunext = np;
				} else {
					dcm_lru_first = np;
				}
				if (np<DCM_TAB_LENG) {
					dcm_tab[np].lruprev = pp;
				} else {
					dcm_lru_last = pp;
				}
				/* add at the beginning of LRU chain */
				dcm_tab[p].lruprev = DCM_NIL;
				dcm_tab[p].lrunext = dcm_lru_first;
				dcm_tab[dcm_lru_first].lruprev = p;
				dcm_lru_first = p;
			}
			/* remove from INODE chain */
			pp = dcm_tab[p].iprev;
			np = dcm_tab[p].inext;
			if (pp<DCM_TAB_LENG) {
				dcm_tab[pp].inext = np;
			} else {
				dcm_inodehash[ih] = np;
			}
			if (np<DCM_TAB_LENG) {
				dcm_tab[np].iprev = pp;
			}
			/* mark as 'empty' */
			dcm_tab[p].inode = 0;
			dcm_tab[p].inext = DCM_NIL;
			dcm_tab[p].iprev = DCM_NIL;
//			elcount--;
			/* p = INODE_NEXT(p) */
			p = np;
		} else {
			if (dcm_tab[p].inode == inode && dcm_tab[p].sessionid == sessionid) {
				/* move element to the end of LRU list */
				if (dcm_lru_last!=p) {	/* do it only if needed */
					/* remove from LRU chain */
					pp = dcm_tab[p].lruprev;
					np = dcm_tab[p].lrunext;
					if (pp<DCM_TAB_LENG) {
						dcm_tab[pp].lrunext = np;
					} else {
						dcm_lru_first = np;
					}
					if (np<DCM_TAB_LENG) {
						dcm_tab[np].lruprev = pp;
					} else {
						dcm_lru_last = pp;
					}
					/* add at the end of LRU chain */
					dcm_tab[p].lruprev = dcm_lru_last;
					dcm_tab[p].lrunext = DCM_NIL;
					dcm_tab[dcm_lru_last].lrunext = p;
					dcm_lru_last = p;
				}
				dcm_tab[p].cacheok = 1;
			}
			p = dcm_tab[p].inext;
		}
	}
}

int dcm_init(void) {
	uint32_t i;

	for (i=0 ; i<DCM_INODEHASH_LENG ; i++) {
		dcm_inodehash[i]=DCM_NIL;
	}
	for (i=0 ; i<DCM_TAB_LENG ; i++) {
		dcm_tab[i].inode=0;
		dcm_tab[i].sessionid=0;
		dcm_tab[i].iprev = DCM_NIL;
		dcm_tab[i].inext = DCM_NIL;
		dcm_tab[i].lruprev = i-1;
		dcm_tab[i].lrunext = i+1;
	}
	dcm_tab[0].lruprev = DCM_NIL;
	dcm_lru_first = 0;
	dcm_tab[DCM_TAB_LENG-1].lrunext = DCM_NIL;
	dcm_lru_last = DCM_TAB_LENG-1;
//	elcount = 0;
	return 0;
}

/*
 * consistency tests
 *
int dcm_test(void) {
	uint32_t i,p,j;
	for (i=0 ; i<DCM_INODEHASH_LENG ; i++) {
		p = dcm_inodehash[i];
		if (p<DCM_TAB_LENG) {
			if (dcm_tab[p].iprev!=DCM_NIL) {
				return 1;
			}
		} else {
			if (p!=DCM_NIL) {
				return 2;
			}
		}
	}
	if (dcm_lru_first<DCM_TAB_LENG) {
		if (dcm_tab[dcm_lru_first].lruprev!=DCM_NIL) {
			return 3;
		}
	} else {
		if (dcm_lru_first!=DCM_NIL) {
			return 4;
		}
	}
	if (dcm_lru_last<DCM_TAB_LENG) {
		if (dcm_tab[dcm_lru_last].lrunext!=DCM_NIL) {
			return 5;
		}
	} else {
		if (dcm_lru_last!=DCM_NIL) {
			return 6;
		}
	}
	for (i=0 ; i<DCM_TAB_LENG ; i++) {
		p = dcm_tab[i].iprev;
		if (p<DCM_TAB_LENG) {
			if (dcm_tab[p].inext!=i) {
				return 7;
			}
		} else if (p!=DCM_NIL) {
			return 8;
		}
		p = dcm_tab[i].inext;
		if (p<DCM_TAB_LENG) {
			if (dcm_tab[p].iprev!=i) {
				return 9;
			}
		} else if (p!=DCM_NIL) {
			return 10;
		}
		p = dcm_tab[i].lruprev;
		if (p<DCM_TAB_LENG) {
			if (dcm_tab[p].lrunext!=i) {
				return 11;
			}
		} else if (p!=DCM_NIL) {
			return 12;
		}
		p = dcm_tab[i].lrunext;
		if (p<DCM_TAB_LENG) {
			if (dcm_tab[p].lruprev!=i) {
				return 13;
			}
		} else if (p!=DCM_NIL) {
			return 14;
		}
	}
	p=0;
	for (i=dcm_lru_first ; i<DCM_TAB_LENG ; i=dcm_tab[i].lrunext) {
		p++;
	}
	if (p!=DCM_TAB_LENG) {
		return 15;
	}
	p=0;
	for (i=dcm_lru_last ; i<DCM_TAB_LENG ; i=dcm_tab[i].lruprev) {
		p++;
	}
	if (p!=DCM_TAB_LENG) {
		return 16;
	}
	p=0;
	for (j=0 ; j<DCM_INODEHASH_LENG ; j++) {
		for (i=dcm_inodehash[j] ; i<DCM_TAB_LENG ; i=dcm_tab[i].inext) {
			p++;
		}
	}
	if (p!=elcount) {
		return 17;
	}
	return 0;
}

uint32_t dcm_getelcount(void) {
	return elcount;
}
*/
