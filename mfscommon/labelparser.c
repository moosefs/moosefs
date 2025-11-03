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
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "labelparser.h"
#include "MFSCommunication.h"

/* simple version - just compare expressions byte by byte, in the future it can be smarter and returns true for example for A&B and B&A */
static inline uint8_t labelexpr_diff(const uint8_t *labelexpr1,const uint8_t *labelexpr2) {
	uint8_t n1,n2;
	while (1) {
		n1 = *labelexpr1;
		n2 = *labelexpr2;
		labelexpr1++;
		labelexpr2++;
		if (n1==0 || n2==0) {
			return (n1==0 && n2==0)?1:0;
		}
		if (n1!=n2) {
			return 0;
		}
	}
}

static inline uint8_t rpn_to_infix(const uint8_t *labelexpr,char outstr[SCLASS_EXPR_MAX_SIZE*2]) {
#ifndef __clang_analyzer__
	struct _expr_string {
		uint8_t level;
		uint8_t len;
		char *str;
	} stack[SCLASS_EXPR_MAX_SIZE];
	uint8_t n,r,l;
	uint8_t sp;
	char *str;

	sp = 0;
	if (*labelexpr) {
		while ((n=*labelexpr++)) {
			switch (n&SCLASS_EXPR_TYPE_MASK) {
				case SCLASS_EXPR_SYMBOL:
					if (n==SCLASS_EXPR_SYMBOL_ANY) {
						stack[sp].level = 0;
						stack[sp].len = 1;
						stack[sp].str = malloc(1);
						stack[sp].str[0] = '*';
					} else {
						n &= SCLASS_EXPR_VALUE_MASK;
						stack[sp].level = 0;
						stack[sp].len = 1;
						stack[sp].str = malloc(1);
						stack[sp].str[0] = 'A'+n;
					}
					sp++;
					break;
				case SCLASS_EXPR_OP_AND:
					n &= SCLASS_EXPR_VALUE_MASK;
					n += 2;
					if (n>sp) {
						while (sp>0) {
							sp--;
							if (stack[sp].str!=NULL) {
								free(stack[sp].str);
							}
						}
						return 0;
					}
					l = 0;
					for (r=n ; r!=0 ; r--) {
						if (stack[sp-r].level>1) {
							l += 2;
						}
						l += stack[sp-r].len;
					}
					l += n-1;
					str = malloc(l);
					l = 0;
					for (r=n ; r!=0 ; r--) {
						if (r!=n) {
							str[l++] = '&';
						}
						if (stack[sp-r].level>1) {
							str[l++] = '(';
						}
						memcpy(str+l,stack[sp-r].str,stack[sp-r].len);
						l+=stack[sp-r].len;
						free(stack[sp-r].str);
						if (stack[sp-r].level>1) {
							str[l++] = ')';
						}
					}
					sp -= n;
					stack[sp].level = 1;
					stack[sp].len = l;
					stack[sp].str = str;
					sp++;
					break;
				case SCLASS_EXPR_OP_OR:
					n &= SCLASS_EXPR_VALUE_MASK;
					n += 2;
					if (n>sp) {
						while (sp>0) {
							sp--;
							if (stack[sp].str!=NULL) {
								free(stack[sp].str);
							}
						}
						return 0;
					}
					l = 0;
					for (r=n ; r!=0 ; r--) {
						if (stack[sp-r].level>2) {
							l += 2;
						}
						l += stack[sp-r].len;
					}
					l += n-1;
					str = malloc(l);
					l = 0;
					for (r=n ; r!=0 ; r--) {
						if (r!=n) {
							str[l++] = '|';
						}
						if (stack[sp-r].level>2) {
							str[l++] = '(';
						}
						memcpy(str+l,stack[sp-r].str,stack[sp-r].len);
						l+=stack[sp-r].len;
						free(stack[sp-r].str);
						if (stack[sp-r].level>2) {
							str[l++] = ')';
						}
					}
					sp -= n;
					stack[sp].level = 2;
					stack[sp].len = l;
					stack[sp].str = str;
					sp++;
					break;
				case SCLASS_EXPR_OP_ONE:
					n &= SCLASS_EXPR_VALUE_MASK;
					if (n==SCLASS_EXPR_OP_NOT) {
						if (sp==0) {
							return 0;
						}
						l = stack[sp-1].len;
						if (stack[sp-1].level>0) {
							l += 2;
						}
						l++;
						str = malloc(l);
						l = 0;
						str[l++] = '~';
						if (stack[sp-1].level>0) {
							str[l++] = '(';
						}
						memcpy(str+l,stack[sp-1].str,stack[sp-1].len);
						l+=stack[sp-1].len;
						free(stack[sp-1].str);
						if (stack[sp-1].level>0) {
							str[l++] = ')';
						}
						stack[sp-1].level = 0;
						stack[sp-1].len = l;
						stack[sp-1].str = str;
					}
			}
		}
		if (sp==1) {
			memcpy(outstr,stack[0].str,stack[0].len);
			outstr[stack[0].len]=0;
			free(stack[0].str);
			return stack[0].len;
		}
		while (sp>0) {
			sp--;
			if (stack[sp].str!=NULL) {
				free(stack[sp].str);
			}
		}
		memcpy(outstr,"ERROR",5);
		outstr[5]=0;
		return 5;
	} else {
		outstr[0]='*';
		outstr[1]=0;
		return 1;
	}
#else
	(void)labelexpr;
	memcpy(outstr,"CLANG SUCKS !!!",15);
	outstr[15]=0;
	return 15;
#endif
}

char* make_label_expr(char *strbuff,const parser_data *pd) {
	uint8_t i,j,c;
	char *p;

	p = strbuff;
	if (pd->ec_data_chksum_parts) {
		i = (pd->ec_data_chksum_parts>>4);
		j = (pd->ec_data_chksum_parts&0xF);
		*p = '@';
		p++;
		if (i==8 || i==4) {
			*p = '0'+i;
			p++;
			*p = '+';
			p++;
		}
		*p = '0'+j;
		p++;
	} else if (pd->labelscnt==0) {
		*p = '-';
		p++;
		*p = '\0';
		return strbuff;
	}
	i = 0;
	while (i<pd->labelscnt) {
		if (i>0 || pd->ec_data_chksum_parts>0) {
			*p = ',';
			p++;
		}
		c=1;
		while (i+c<pd->labelscnt && labelexpr_diff(pd->labelexpr[i],pd->labelexpr[i+c])) {
			c++;
		}
		if (c>1) {
			*p = '0'+c;
			p++;
		}
		j = rpn_to_infix(pd->labelexpr[i],p);
		p+=j;
		i+=c;
	}
	if (pd->uniqmask!=0) {
		*p = '/';
		p++;
		if (pd->uniqmask & UNIQ_MASK_IP) {
			memcpy(p,"[IP]",4);
			p+=4;
		} else if (pd->uniqmask & UNIQ_MASK_RACK) {
			memcpy(p,"[RACK]",6);
			p+=6;
		} else {
			for (i=0 ; i<26 ; i++) {
				if (pd->uniqmask & (1<<i)) {
					if (i<24 && ((pd->uniqmask >> i) & 7) == 7) {
						*p = 'A'+i;
						p++;
						*p = '-';
						p++;
						while ((pd->uniqmask & (1<<i)) && i<26) {
							i++;
						}
						i--;
						*p = 'A'+i;
						p++;
					} else {
						*p = 'A'+i;
						p++;
					}
				}
			}
		}
	}
	if (pd->labels_mode==LABELS_MODE_STD || pd->labels_mode==LABELS_MODE_LOOSE || pd->labels_mode==LABELS_MODE_STRICT) {
		*p = ':';
		p++;
		if (pd->labels_mode==LABELS_MODE_STRICT) {
			memcpy(p,"STRICT",6);
			p+=6;
		} else if (pd->labels_mode==LABELS_MODE_LOOSE) {
			memcpy(p,"LOOSE",5);
			p+=5;
		} else {
			memcpy(p,"STD",3);
			p+=3;
		}
	}
	*p = '\0';
	return strbuff;
}

/* grammar productions:
 *	T -> C | C N | '-'
 *
 *	N -> '/' U | ':' D | '/' U ':' D
 *
 *	C -> A | ( '@' | '=' ) [ ('4' | '8') '+' ] '1' ... '9' [ ( ',' | ';' ) E [ ( ',' | ';' ) E ]]
 *
 *	A -> [ '1' ... '9' ] E ',' A | [ '1' ... '9' ] E ';' A
 *	E -> '*' | S
 *	S -> S '+' M | S '|' M | S '||' M | M
 *	M -> M '*' L | M '&' L | M '&&' L | M L | L
 *	L -> X | '-' L | '~' L | '!' L | '(' S ')' | '[' S ']'
 *
 *	U -> U R | R | I
 *	R -> X | X '-' X
 *	I -> '[IP]' | '[I]' | '[RACK]' | '[R]'
 *
 *	D -> 'S' | 'L' | 'D' | 'STRICT' | 'LOOSE' | 'STD'
 *
 *	X -> 'a' ... 'z' | 'A' ... 'Z'
 */

enum {
	OR,
	AND,
	NOT,
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
	uint32_t uniqmask;
	uint8_t labels_mode;
	node *terms[9];
	uint8_t erroroccured;
	uint8_t ec_data_chksum_parts;
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

static inline void expr_parse_error(expr *e,const char *extramsg) {
	if ((int8_t)(e->str[0])>=32) {
		printf("parse error, %snext char: '%c'\n",extramsg,e->str[0]);
	} else {
		printf("parse error, %snext code: 0x%02"PRIX8"\n",extramsg,(uint8_t)(e->str[0]));
	}
	e->erroroccured = 1;
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
	if (e->str[0]=='(') {
		e->str++;
		expr_eat_white(e);
		a=expr_or(e);
		expr_eat_white(e);
		if (e->str[0]==')') {
			e->str++;
			return a;
		} else {
			expr_rfree(a);
			expr_parse_error(e,"closing round bracket expected, ");
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
			expr_rfree(a);
			expr_parse_error(e,"closing square bracket expected, ");
			return NULL;
		}
	}
	if (e->str[0]=='*') {
		e->str++;
		return newnode(ANY,0,NULL,NULL);
	}
	if (e->str[0]=='!' || e->str[0]=='~' || e->str[0]=='-') {
		e->str++;
		expr_eat_white(e);
		a = expr_sym(e);
		expr_eat_white(e);
		return newnode(NOT,0,a,NULL);
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
	expr_parse_error(e,"");
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

/* U -> U R | R | I */
/* R -> X | X '-' X */
/* I -> '[IP]' | '[I]' | '[RACK]' | '[R]' */
/* X -> 'a' ... 'z' | 'A' ... 'Z' */
static inline void expr_uniqmask(expr *e) {
	uint8_t last;
	uint8_t range;
	uint8_t current;

	if (e->str[0]=='[') {
		e->str++;
		if (e->str[0]=='I' || e->str[0]=='i') {
			e->str++;
			if (e->str[0]=='P' || e->str[0]=='p') {
				e->str++;
			}
			if (e->str[0]!=']') {
				expr_parse_error(e,"");
			}
			e->str++;
			e->uniqmask = UNIQ_MASK_IP;
		} else if (e->str[0]=='R' || e->str[0]=='r') {
			e->str++;
			if (e->str[0]=='A' || e->str[0]=='a') {
				e->str++;
				if (e->str[0]=='C' || e->str[0]=='c') {
					e->str++;
					if (e->str[0]=='K' || e->str[0]=='k') {
						e->str++;
					}
				}
			}
			if (e->str[0]!=']') {
				expr_parse_error(e,"");
			}
			e->str++;
			e->uniqmask = UNIQ_MASK_RACK;
		} else {
			expr_parse_error(e,"");
		}
		return;
	}
	last = 0xFF;
	range = 0;
	while (1) {
		expr_eat_white(e);
		current = 0xFF;
		if (e->str[0]>='A' && e->str[0]<='Z') {
			current = (e->str[0]-'A');
			e->str++;
		} else if (e->str[0]>='a' && e->str[0]<='z') {
			current = (e->str[0]-'a');
			e->str++;
		} else if (e->str[0]=='-' && range==0 && last!=0xFF) {
			e->str++;
			range = 1;
		} else {
			if (range==1) {
				printf("parse error, expected character after '-'\n");
				e->erroroccured = 1;
			}
			return;
		}
		if (range==1 && current!=0xFF && last!=0xFF) {
			if (current < last) {
				while (current <= last) {
					e->uniqmask |= 1<<current;
					current++;
				}
			} else {
				while (last <= current) {
					e->uniqmask |= 1<<last;
					last++;
				}
			}
			last = 0xFF;
			range = 0;
		} else if (current!=0xFF) {
			e->uniqmask |= 1<<current;
			last = current;
		}
	}
}

/* D -> 'S' | 'L' | 'D' | 'STRICT' | 'LOOSE' | 'STD' */
static inline void expr_mode(expr *e) {
	if (e->str[0]=='S' || e->str[0]=='s') {
		if (memcmp(e->str,"STD",3)==0 || memcmp(e->str+1,"td",2)==0) {
			e->labels_mode = LABELS_MODE_STD;
			e->str+=3;
			return;
		}
		if (memcmp(e->str,"STRICT",6)==0 || memcmp(e->str+1,"trict",5)==0) {
			e->labels_mode = LABELS_MODE_STRICT;
			e->str+=6;
			return;
		}
		e->labels_mode = LABELS_MODE_STRICT;
		e->str++;
		return;
	}
	if (e->str[0]=='L' || e->str[0]=='l') {
		if (memcmp(e->str,"LOOSE",5)==0 || memcmp(e->str+1,"oose",4)==0) {
			e->labels_mode = LABELS_MODE_LOOSE;
			e->str+=5;
			return;
		}
		e->labels_mode = LABELS_MODE_LOOSE;
		e->str++;
		return;
	}
	if (e->str[0]=='D' || e->str[0]=='d') {
		e->labels_mode = LABELS_MODE_STD;
		e->str++;
		return;
	}
}

/* N -> '/' U | ':' D | '/' U ':' D */
static inline void expr_ending(expr *e) {
	if (e->str[0]==0) {
		return;
	}
	if (e->str[0]=='/') {
		e->str++;
		expr_uniqmask(e);
	}
	if (e->str[0]==':') {
		e->str++;
		expr_mode(e);
	}
	if (e->str[0]) {
		expr_parse_error(e,"");
	}
}

/* T -> C | C N | '-' */
/* C -> A | ( '@' | '=' ) [ ( '4' | '8' ) '+' ] '1' ... '9' [ ( ',' | ';' ) E [ ( ',' | ';' ) E ]] */
/* A -> [ '1' ... '9' ] E ',' A | [ '1' ... '9' ] E ';' A */
static inline void expr_top(expr *e) {
	uint32_t i;
	uint32_t g;
	uint8_t f;
	node *a;

	expr_eat_white(e);
	if (e->str[0]=='-') {
		const char *p = e->str;
		e->str++;
		expr_eat_white(e);
		if (e->str[0]==0) {
			return;
		}
		e->str = p;
	}
	if (e->str[0]=='@' || e->str[0]=='=') { // ec mode
		e->str++;
		expr_eat_white(e);
		if (e->str[0]>='1' && e->str[0]<='9') {
			e->ec_data_chksum_parts = e->str[0]-'0';
			e->str++;
		} else {
			printf("parse error, in ec mode expected number of checksums or data parts after '@'\n");
			e->erroroccured = 1;
			return;
		}
		expr_eat_white(e);
		if (e->str[0]=='+') {
			e->str++;
			expr_eat_white(e);
			if (e->str[0]>='1' && e->str[0]<='9' && (e->ec_data_chksum_parts==4 || e->ec_data_chksum_parts==8)) {
				e->ec_data_chksum_parts <<= 4;
				e->ec_data_chksum_parts |= (e->str[0]-'0')&0xF;
				e->str++;
				expr_eat_white(e);
			} else {
				printf("parse error, in ec mode expected number of checksums after '+' and data parts be set to '4' or '8'\n");
				e->erroroccured = 1;
				return;
			}
		}
		if (e->str[0]==',' || e->str[0]==';') {
			e->str++;
			expr_eat_white(e);
			e->terms[0] = expr_or(e);
			expr_eat_white(e);
			if (e->str[0]==',' || e->str[0]==';') {
				e->str++;
				expr_eat_white(e);
				e->terms[1] = expr_or(e);
				expr_eat_white(e);
			}
		} else {
			e->terms[0] = newnode(ANY,0,NULL,NULL);
		}
		expr_ending(e);
	} else { // copies
		e->ec_data_chksum_parts = 0;
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
			if (i==0 && f==1 && (e->str[0]==0 || e->str[0]=='/')) { // number only
				a = newnode(ANY,0,NULL,NULL);
			} else {
				a = expr_or(e);
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
				expr_ending(e);
				return;
			} else {
				return;
			}
		}
		printf("parse error, too many copies\n");
		e->erroroccured = 1;
	}
	return;
}

typedef struct _rpnbuff {
	uint8_t pos;
	uint8_t rpndata[SCLASS_EXPR_MAX_SIZE];
} rpnbuff;

static inline void expr_rpn_safe_add(rpnbuff *o,uint8_t d) {
	if (o->pos<SCLASS_EXPR_MAX_SIZE) {
		o->rpndata[o->pos++] = d;
	}
}

static inline uint8_t expr_rpn_top(rpnbuff *o) {
	if (o->pos>0) {
		return o->rpndata[o->pos-1];
	}
	return 0;
}

static inline void expr_rpn_exchg_top(rpnbuff *o,uint8_t d) {
	if (o->pos>0) {
		o->rpndata[o->pos-1] = d;
	}
}

void expr_convert_to_rpn(node *n,rpnbuff *o) {
	uint8_t t;
	switch (n->op) {
		case REF:
			expr_convert_to_rpn(n->arg1,o);
			return;
		case SYM:
			expr_rpn_safe_add(o,SCLASS_EXPR_SYMBOL|n->val);
			return;
		case OR:
			expr_convert_to_rpn(n->arg1,o);
			expr_convert_to_rpn(n->arg2,o);
			t = expr_rpn_top(o);
			if ((t&SCLASS_EXPR_TYPE_MASK)==SCLASS_EXPR_OP_OR && (t&SCLASS_EXPR_VALUE_MASK)<SCLASS_EXPR_VALUE_MASK) {
				expr_rpn_exchg_top(o,t+1);
			} else {
				expr_rpn_safe_add(o,SCLASS_EXPR_OP_OR);
			}
			return;
		case AND:
			expr_convert_to_rpn(n->arg1,o);
			expr_convert_to_rpn(n->arg2,o);
			t = expr_rpn_top(o);
			if ((t&SCLASS_EXPR_TYPE_MASK)==SCLASS_EXPR_OP_AND && (t&SCLASS_EXPR_VALUE_MASK)<SCLASS_EXPR_VALUE_MASK) {
				expr_rpn_exchg_top(o,t+1);
			} else {
				expr_rpn_safe_add(o,SCLASS_EXPR_OP_AND);
			}
			return;
		case NOT:
			expr_convert_to_rpn(n->arg1,o);
			expr_rpn_safe_add(o,SCLASS_EXPR_OP_NOT);
			return;
		case ANY:
			expr_rpn_safe_add(o,SCLASS_EXPR_SYMBOL_ANY);
			return;
	}
}

int parse_label_expr(const char *exprstr,parser_data *pd) {
	expr e;
	rpnbuff rpn;
	uint32_t i;
	int res;

	res = 0;
	memset(pd, 0, sizeof(parser_data));
	memset(&e, 0, sizeof(expr));
	e.labels_mode = LABELS_MODE_GLOBAL;
	e.str = exprstr;

	if (!(exprstr[1]=='\0' && (exprstr[0]=='-' || exprstr[0]=='~'))) {
		expr_top(&e);
		if (e.erroroccured) {
			res = -1;
		}
		for (i=0 ; i<9 && res==0 && e.terms[i]!=NULL ; i++) {
			rpn.pos = 0;
			expr_convert_to_rpn(e.terms[i],&rpn);
			if (rpn.pos==SCLASS_EXPR_MAX_SIZE) {
				printf("parse error, too many terms in expression\n");
				res = -1;
			} else {
				memset(pd->labelexpr[i],0,SCLASS_EXPR_MAX_SIZE);
				memcpy(pd->labelexpr[i],rpn.rpndata,rpn.pos);
			}
		}
		if (res==0) {
			pd->labelscnt = i;
			pd->uniqmask = e.uniqmask;
			pd->labels_mode = e.labels_mode;
			pd->ec_data_chksum_parts = e.ec_data_chksum_parts;
		}
		for (i=0 ; i<9 ; i++) {
			expr_rfree(e.terms[i]);
		}
	}
	return res;
}

uint8_t labelmask_matches_labelexpr(uint32_t labelmask,const uint8_t labelexpr[SCLASS_EXPR_MAX_SIZE]) {
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
			}
		}
		if (sp==1) {
			return stack[0];
		}
		return 0;
	} else { // '*'
		return 1;
	}
}
