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
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "labelparser.h"
#include "MFSCommunication.h"

char* make_label_expr(char *strbuff,uint8_t labelscnt,uint32_t labelmasks[9][MASKORGROUP]) {
	uint8_t i,j;
	char *p,c;

	p = strbuff;
	for (i=0 ; i<labelscnt ; i++) {
		if (i>0) {
			*p = ' ';
			p++;
			*p = ',';
			p++;
			*p = ' ';
			p++;
		}
		*p = '[';
		p++;
		for (j=0 ; j<MASKORGROUP ; j++) {
			if (labelmasks[i][j]==0) {
				break;
			}
			if (j>0) {
				*p = '+';
				p++;
			}
			for (c='A' ; c<='Z' ; c++) {
				if (labelmasks[i][j] & (1 << (c-'A'))) {
					*p = c;
					p++;
				}
			}
		}
		if (j==0) {
			*p = '*';
			p++;
		}
		*p = ']';
		p++;
	}
	*p = '\0';
	return strbuff;
}

/* grammar productions:
 *	A -> [1-9] E ',' A | [1-9] E ';' A
 *	E -> '*' | S
 *	S -> S '+' M | S '|' M | S '||' M | M
 *	M -> M '*' L | M '&' L | M '&&' L | M L | L
 *	L -> 'a' .. 'z' | 'A' .. 'Z' | '(' S ')' | '[' S ']'
 */

enum {
	OR,
	AND,
	REF,
	ANY,
	SYM
};

typedef struct _node {
	uint8_t op;
	uint8_t val;
	struct _node *arg1;
	struct _node *arg2;
} node;

typedef struct _expr {
	const char *str;
	node *terms[9];
	uint8_t erroroccured;
} expr;


static inline void expr_rfree(node *actnode) {
	if (actnode!=NULL) {
		if (actnode->op!=REF) {
			expr_rfree(actnode->arg1);
			expr_rfree(actnode->arg2);
		}
		free(actnode);
	}
}

static inline node* newnode(uint8_t op,int8_t val,node *arg1,node *arg2) {
	node *aux;
	aux = (node*)malloc(sizeof(node));
	aux->op = op;
	aux->val = val;
	aux->arg1 = arg1;
	aux->arg2 = arg2;
	return aux;
}

static inline node* expr_or(expr *e);

static inline void expr_eat_white(expr *e) {
	while (e->str[0]==' ' || e->str[0]=='\t') {
		e->str++;
	}
}

/* L -> 'a' .. 'z' | 'A' .. 'Z' | '(' S ')' | '[' S ']' */
static inline node* expr_sym(expr *e) {
	node *a;
	uint8_t v;
	expr_eat_white(e);
	if (e->str[0]=='(') {
		e->str++;
		expr_eat_white(e);
		a=expr_or(e);
		expr_eat_white(e);
		if (e->str[0]==')') {
			e->str++;
			return a;
		} else {
			if ((int8_t)(e->str[0])>=32) {
				printf("parse error, closing round bracket expected, next char: '%c'\n",e->str[0]);
			} else {
				printf("parse error, closing round bracket expected, next code: 0x%02"PRIX8"\n",(uint8_t)(e->str[0]));
			}
			expr_rfree(a);
			e->erroroccured = 1;
			return NULL;
		}
	}
	if (e->str[0]=='[') {
		e->str++;
		expr_eat_white(e);
		a=expr_or(e);
		expr_eat_white(e);
		if (e->str[0]==']') {
			e->str++;
			return a;
		} else {
			if ((int8_t)(e->str[0])>=32) {
				printf("parse error, closing round bracket expected, next char: '%c'\n",e->str[0]);
			} else {
				printf("parse error, closing round bracket expected, next code: 0x%02"PRIX8"\n",(uint8_t)(e->str[0]));
			}
			expr_rfree(a);
			e->erroroccured = 1;
			return NULL;
		}
	}
	if (e->str[0]>='A' && e->str[0]<='Z') {
		v = e->str[0]-'A';
		e->str++;
		return newnode(SYM,v,NULL,NULL);
	}
	if (e->str[0]>='a' && e->str[0]<='z') {
		v = e->str[0]-'a';
		e->str++;
		return newnode(SYM,v,NULL,NULL);
	}
	if ((int8_t)(e->str[0])>=32) {
		printf("parse error, next char: '%c'\n",e->str[0]);
	} else {
		printf("parse error, next code: 0x%02"PRIX8"\n",(uint8_t)(e->str[0]));
	}
	e->erroroccured = 1;
	return NULL;
}

/* M -> M '*' L | M '&' L | M '&&' L | M L | L */
static inline node* expr_and(expr *e) {
	node *a;
	node *b;
	expr_eat_white(e);
	a = expr_sym(e);
	expr_eat_white(e);
	if (e->str[0]=='&' && e->str[1]=='&') {
		e->str += 2;
		b = expr_and(e);
		return newnode(AND,0,a,b);
	} else if (e->str[0]=='&' || e->str[0]=='*') {
		e->str ++;
		b = expr_and(e);
		return newnode(AND,0,a,b);	
	} else if ((e->str[0]>='A' && e->str[0]<='Z') || (e->str[0]>='a' && e->str[0]<='z') || e->str[0]=='(' || e->str[0]=='[') {
		b = expr_and(e);
		return newnode(AND,0,a,b);
	} else {
		return a;
	}
}

/* S -> S '+' M | S '|' M | S '||' M | M */
static inline node* expr_or(expr *e) {
	node *a;
	node *b;
	expr_eat_white(e);
	a = expr_and(e);
	expr_eat_white(e);
	if (e->str[0]=='|' && e->str[1]=='|') {
		e->str += 2;
		b = expr_or(e);
		return newnode(OR,0,a,b);
	} else if (e->str[0]=='|' || e->str[0]=='+') {
		e->str ++;
		b = expr_or(e);
		return newnode(OR,0,a,b);
	} else {
		return a;
	}
}

/* E -> '*' | S */
static inline node* expr_first(expr *e) {
	expr_eat_white(e);
	if (e->str[0]=='*') {
		e->str++;
		return newnode(ANY,0,NULL,NULL);
	}
	return expr_or(e);
}

/* A -> [1-9] E ',' A | [1-9] E ';' A */
static inline void expr_top(expr *e) {
	uint32_t i;
	uint32_t g;
	uint8_t f;
	node *a;

	i = 0;
	while (i<9) {
		expr_eat_white(e);
		f = 0;
		if (e->str[0]>='1' && e->str[0]<='9') {
			g = e->str[0]-'0';
			e->str++;
			f = 1;
		} else {
			g = 1;
		}
		expr_eat_white(e);
		if (i==0 && f==1 && e->str[0]==0) { // number only
			a = newnode(ANY,0,NULL,NULL);
		} else {
			a = expr_first(e);
		}
		expr_eat_white(e);
		if (e->erroroccured) {
			expr_rfree(a);
			return;
		}
		if (i+g>9) {
			break;
		}
		f = 0;
		while (g>0) {
			if (f==1) {
				e->terms[i] = newnode(REF,0,a,NULL);
			} else {
				e->terms[i] = a;
				f = 1;
			}
			i++;
			g--;
		}
		if (e->str[0]==',' || e->str[0]==';') {
			e->str++;
		} else if (e->str[0]) {
			if ((int8_t)(e->str[0])>=32) {
				printf("parse error, next char: '%c'\n",e->str[0]);
			} else {
				printf("parse error, next code: 0x%02"PRIX8"\n",(uint8_t)(e->str[0]));
			}
			e->erroroccured = 1;
			return;
		} else {
			return;
		}
	}
	printf("parse error, too many copies\n");
	e->erroroccured = 1;
	return;
}

typedef struct _termval {
	uint8_t cnt;
	uint32_t *labelmasks;
} termval;

static int label_cmp(const void *a,const void *b) {
	uint32_t aa = *((const uint32_t*)a);
	uint32_t bb = *((const uint32_t*)b);
	return (aa>bb)?1:(aa<bb)?-1:0;
}

static inline termval* expr_eval(node *a) {
	termval *t1,*t2,*t;
	uint32_t i,j;
	t1 = NULL;
	t2 = NULL;
	t = NULL;
	if (a->op==REF) {
		return expr_eval(a->arg1);
	}
	if (a->op==ANY) {
		t1 = malloc(sizeof(termval));
		t1->cnt = 0;
		t1->labelmasks = NULL;
		return t1;
	}
	if (a->op==SYM) {
		t1 = malloc(sizeof(termval));
		t1->cnt = 1;
		t1->labelmasks = malloc(sizeof(uint32_t));
		t1->labelmasks[0] = 1 << a->val;
		return t1;
	}
	if (a->op==OR || a->op==AND) {
		t1 = expr_eval(a->arg1);
		t2 = expr_eval(a->arg2);
		if (t1==NULL || t2==NULL || t1->cnt==0 || t2->cnt==0) {
			if (t1) {
				free(t1->labelmasks);
				free(t1);
			}
			if (t2) {
				free(t2->labelmasks);
				free(t2);
			}
			return NULL;
		}
		t = malloc(sizeof(termval));
	}
	if (a->op==AND) {
		t->cnt = t1->cnt*t2->cnt;
		t->labelmasks = malloc(sizeof(uint32_t)*t->cnt);
		for (i=0 ; i<t1->cnt ; i++) {
			for (j=0 ; j<t2->cnt ; j++) {
				t->labelmasks[i*t2->cnt+j] = (t1->labelmasks[i] | t2->labelmasks[j]);
			}
		}
	} else if (a->op==OR) {
		t->cnt = t1->cnt+t2->cnt;
		t->labelmasks = malloc(sizeof(uint32_t)*t->cnt);
		memcpy(t->labelmasks,t1->labelmasks,sizeof(uint32_t)*t1->cnt);
		memcpy(t->labelmasks+t1->cnt,t2->labelmasks,sizeof(uint32_t)*t2->cnt);
	} else {
		if (t) { /* satisify cppcheck */
			free(t);
		}
		return NULL;
	}
	free(t1->labelmasks);
	free(t2->labelmasks);
	free(t1);
	free(t2);
	if (t->cnt>1) {
		qsort(t->labelmasks,t->cnt,sizeof(uint32_t),label_cmp);
		for (i=0 ; i+1<t->cnt ; i++) {
			while (t->labelmasks[i]==t->labelmasks[i+1] && i+1<t->cnt) {
				if (i+2<t->cnt) {
					memmove(t->labelmasks+i+1,t->labelmasks+i+2,sizeof(uint32_t)*(t->cnt-i-2));
				}
				t->cnt--;
			}
		}
	}
	if (t->cnt > MASKORGROUP) {
		printf("Too many 'or' groups (max: %u)\n",(unsigned)(MASKORGROUP));
		free(t->labelmasks);
		free(t);
		return NULL;
	}
	return t;
}

int parse_label_expr(char *exprstr,uint8_t *labelscnt,uint32_t labelmasks[9][MASKORGROUP]) {
	expr e;
	termval *t;
	uint32_t i,j;
	int res;

	res = 0;
	e.str = exprstr;
	e.erroroccured = 0;
	for (i=0 ; i<9 ; i++) {
		e.terms[i] = NULL;
	}
	expr_top(&e);
	if (e.erroroccured) {
		res = -1;
	}
	for (i=0 ; i<9 && res==0 && e.terms[i]!=NULL ; i++) {
		t = expr_eval(e.terms[i]);
		if (t==NULL) {
			res = -1;
		} else {
			for (j=0 ; j<MASKORGROUP ; j++) {
				if (j<t->cnt) {
					labelmasks[i][j] = t->labelmasks[j];
				} else {
					labelmasks[i][j] = 0;
				}
			}
			free(t->labelmasks);
			free(t);
		}
	}
	if (res==0) {
		*labelscnt = i;
	}
	for (i=0 ; i<9 ; i++) {
		expr_rfree(e.terms[i]);
	}
	return res;
}
