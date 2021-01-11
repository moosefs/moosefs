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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <inttypes.h>

typedef struct _statsnode {
	uint64_t counter;
	uint8_t printflag;
	uint8_t absolute;
	char *name;
	char *fullname;
	uint32_t nleng;	// : strlen(name)
	uint32_t fnleng; // : strlen(fullname)
	struct _statsnode *parent;
	struct _statsnode *firstchild;
	struct _statsnode *nextsibling;
} statsnode;

static statsnode *firstnode = NULL;
static uint32_t allactiveplengs = 0;
static uint32_t activenodes = 0;
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

void stats_counter_add(void *node,uint64_t delta) {
	statsnode *sn = (statsnode*)node;
	pthread_mutex_lock(&glock);
	while (sn) {
		sn->counter += delta;
		if (sn->absolute) {
			break;
		}
		sn = sn->parent;
	}
	pthread_mutex_unlock(&glock);
}

void stats_counter_sub(void *node,uint64_t delta) {
	statsnode *sn = (statsnode*)node;
	pthread_mutex_lock(&glock);
	while (sn) {
		sn->counter -= delta;
		if (sn->absolute) {
			break;
		}
		sn = sn->parent;
	}
	pthread_mutex_unlock(&glock);
}

void stats_counter_inc(void *node) {
	statsnode *sn = (statsnode*)node;
	pthread_mutex_lock(&glock);
	while (sn) {
		sn->counter++;
		if (sn->absolute) {
			break;
		}
		sn = sn->parent;
	}
	pthread_mutex_unlock(&glock);
}

void stats_counter_dec(void *node) {
	statsnode *sn = (statsnode*)node;
	pthread_mutex_lock(&glock);
	while (sn) {
		sn->counter--;
		if (sn->absolute) {
			break;
		}
		sn = sn->parent;
	}
	pthread_mutex_unlock(&glock);
}

void stats_counter_set(void *node,uint64_t value) {
	statsnode *sn = (statsnode*)node;
	pthread_mutex_lock(&glock);
	if (sn->absolute) {
		sn->counter = value;
	}
	pthread_mutex_unlock(&glock);
}

void* stats_get_subnode(void *node,const char *name,uint8_t absolute,uint8_t printflag) {
	statsnode *sn = (statsnode*)node;
	statsnode *a;
	pthread_mutex_lock(&glock);
	for (a=sn?sn->firstchild:firstnode ; a ; a=a->nextsibling) {
		if (strcmp(a->name,name)==0) {
			pthread_mutex_unlock(&glock);
			return a;
		}
	}
	a = malloc(sizeof(statsnode));
	a->nextsibling = sn?sn->firstchild:firstnode;
	a->firstchild = NULL;
	a->counter = 0;
	a->printflag = printflag;
	a->absolute = absolute;
	a->name = strdup(name);
	a->nleng = strlen(name);
	if (sn) {
		char *bstr;
		a->fnleng = sn->fnleng+1+a->nleng;
		bstr = malloc(a->fnleng+1);
		memcpy(bstr,sn->fullname,sn->fnleng);
		bstr[sn->fnleng]='.';
		memcpy(bstr+sn->fnleng+1,a->name,a->nleng);
		bstr[a->fnleng]=0;
		a->fullname = bstr;
	} else {
		a->fullname = a->name;
		a->fnleng = a->nleng;
	}
	if (sn) {
		sn->firstchild = a;
	} else {
		firstnode = a;
	}
	a->parent = sn;
	if (printflag) {
		activenodes++;
		allactiveplengs+=a->fnleng;
	}
	pthread_mutex_unlock(&glock);
	return a;
}

static inline void stats_reset(statsnode *n) {
	statsnode *a;
	if (n->absolute==0) {
		n->counter = 0;
	}
	for (a=n->firstchild ; a ; a=a->nextsibling) {
		stats_reset(a);
	}
}

void stats_reset_all(void) {
	statsnode *a;
	pthread_mutex_lock(&glock);
	for (a=firstnode ; a ; a=a->nextsibling) {
		stats_reset(a);
	}
	pthread_mutex_unlock(&glock);
}

static inline uint32_t stats_print_values(char *buff,uint32_t maxleng,statsnode *n) {
	statsnode *a;
	uint32_t l;
//	printf("node: %p ; name: %s ; printflag: %u ; absolute: %u ; counter: %"PRIu64"\n",n,n->fullname,n->printflag,n->absolute,n->counter);
	if (n->printflag) {
		if (n->absolute) {
			l = snprintf(buff,maxleng,"%s: [%"PRIu64"]\n",n->fullname,n->counter);
		} else {
			l = snprintf(buff,maxleng,"%s: %"PRIu64"\n",n->fullname,n->counter);
		}
	} else {
		l = 0;
	}
	for (a=n->firstchild ; a ; a=a->nextsibling) {
		if (maxleng>l) {
			l += stats_print_values(buff+l,maxleng-l,a);
		}
	}
	return l;
}

static inline uint32_t stats_print_total(char *buff,uint32_t maxleng) {
	statsnode *a;
	uint32_t l;
	l = 0;
	for (a=firstnode ; a ; a=a->nextsibling) {
		if (maxleng>l) {
			l += stats_print_values(buff+l,maxleng-l,a);
		}
	}
	return l;
}

void stats_show_all(char **buff,uint32_t *leng) {
	uint32_t rl;
	pthread_mutex_lock(&glock);
	rl = allactiveplengs + 50*activenodes;
	*buff = malloc(rl);
	if (*buff) {
		*leng = stats_print_total(*buff,rl);
	} else {
		*leng = 0;
	}
	pthread_mutex_unlock(&glock);
}

void stats_free(statsnode *n) {
	statsnode *a,*an;
	free(n->name);
	if (n->fullname != n->name) {
		free(n->fullname);
	}
	for (a=n->firstchild ; a ; a = an) {
		an = a->nextsibling;
		stats_free(a);
		free(a);
	}
}

void stats_term(void) {
	statsnode *a,*an;
	for (a=firstnode ; a ; a = an) {
		an = a->nextsibling;
		stats_free(a);
		free(a);
	}
}

/*
#ifdef TEST
int main(void) {
	void *n1,*n2,*n3;
	uint64_t *s;

	uint8_t *b;
	uint32_t l;

	n1 = stats_get_subnode(NULL,"ala");
	s = stats_get_counterptr(n1);
	(*s) += 200;
	n2 = stats_get_subnode(n1,"ma");
	n3 = stats_get_subnode(n2,"kota");
	s = stats_get_counterptr(n3);
	(*s) += 15;
	n3 = stats_get_subnode(n2,"psa");
	s = stats_get_counterptr(n3);
	(*s) = 10;
	n1 = stats_get_subnode(NULL,"krowa");
	n1 = stats_get_subnode(NULL,"pikus");
	n2 = stats_get_subnode(n1,"barbapapa");
	s = stats_get_counterptr(n3);
	(*s) = 0xFFFFFFFFFFFFFFFFULL;

	stats_show_all(&b,&l);
	b[l]=0;
	printf("%s",b);
	return 0;
}
*/

