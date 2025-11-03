#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "searchexpr.h"

enum {
	NUMBER,

	OP_MINUS,
	OP_SUB,
	OP_ADD,
	OP_MUL,
	OP_DIV,
	OP_MOD,
	OP_LSHIFT,
	OP_RSHIFT,
	OP_LE,
	OP_LT,
	OP_GE,
	OP_GT,
	OP_EQ,
	OP_NE,
	OP_BNOT,
	OP_BAND,
	OP_BXOR,
	OP_BOR,
	OP_LNOT,
	OP_LAND,
	OP_LXOR,
	OP_LOR,
	OP_COND,

	CONST_FILE,
	CONST_DIRECTORY,
	CONST_SYMLINK,
	CONST_FIFO,
	CONST_BLOCKDEV,
	CONST_CHARDEV,
	CONST_SOCKET,
	CONST_TRASH,
	CONST_SUSTAINED,
	CONST_SUID,
	CONST_SGID,
	CONST_STICKY,
	CONST_UREAD,
	CONST_UWRITE,
	CONST_UEXECUTE,
	CONST_GREAD,
	CONST_GWRITE,
	CONST_GEXECUTE,
	CONST_OREAD,
	CONST_OWRITE,
	CONST_OEXECUTE,
	CONST_READ,
	CONST_WRITE,
	CONST_EXECUTE,
	CONST_NOOWNER,
	CONST_NOACACHE,
	CONST_NOECACHE,
	CONST_NODATACACHE,
	CONST_SNAPSHOT,
	CONST_UNDELETABLE,
	CONST_APPENDONLY,
	CONST_IMMUTABLE,

	SYMBOL_INODE,
	SYMBOL_TYPE,
	SYMBOL_EATTR,
	SYMBOL_SCLASS,
	SYMBOL_UID,
	SYMBOL_GID,
	SYMBOL_MODE,
	SYMBOL_UMODE,
	SYMBOL_GMODE,
	SYMBOL_OMODE,
	SYMBOL_ATIME,
	SYMBOL_MTIME,
	SYMBOL_CTIME,
	SYMBOL_TRETENTION,
	SYMBOL_MAJOR,
	SYMBOL_MINOR,
	SYMBOL_LENGTH,
	SYMBOL_TIME,
	SYMBOL_CHUNK
};

typedef struct node {
	uint8_t type;
	uint64_t value;
	struct node *op1,*op2,*op3;
} node;

static inline node* expr_new_node(uint8_t type,uint64_t value,node *op1,node *op2,node *op3) {
	node *n;
	n = (node*)malloc(sizeof(node));
	n->type = type;
	n->value = value;
	n->op1 = op1;
	n->op2 = op2;
	n->op3 = op3;
	return n;
}

typedef struct expr {
	const char *str;
	uint32_t ppos;
	node *root;
	uint8_t parseerror;
	uint8_t useschunkid;
} expr;

static inline void expr_rfree(node *curnode) {
	if (curnode!=NULL) {
		expr_rfree(curnode->op1);
		expr_rfree(curnode->op2);
		expr_rfree(curnode->op3);
		free(curnode);
	}
}

static inline uint8_t expr_hex_digit(const char c) {
	if (c>='0' && c<='9') {
		return c-'0';
	} else if (c>='A' && c<='F') {
		return c+10-'A';
	} else if (c>='a' && c<='f') {
		return c+10-'a';
	}
	return 0xFF;
}

static inline uint8_t expr_dec_digit(const char c) {
	if (c>='0' && c<='9') {
		return c-'0';
	}
	return 0xFF;
}

static inline uint8_t expr_oct_digit(const char c) {
	if (c>='0' && c<='7') {
		return c-'0';
	}
	return 0xFF;
}

static inline uint8_t expr_bin_digit(const char c) {
	if (c>='0' && c<='1') {
		return c-'0';
	}
	return 0xFF;
}

static inline char expr_locase(const char c) {
	if (c>='A' && c<='Z') {
		return c+'a'-'A';
	}
	return c;
}

static inline void expr_print_error_position(expr *e) {
	uint32_t i;
	printf("%s\n",e->str);
	for (i=0 ; i<e->ppos ; i++) {
		printf(" ");
	}
	printf("^\n");
}

#define CSYMBOL (e->str[e->ppos])
#define NSYMBOL (e->str[e->ppos+1])
#define TSYMBOL (e->str[e->ppos+2])
#define EATSPACE while(CSYMBOL==' ' || CSYMBOL=='\t') e->ppos++
#define ADVANCE1 e->ppos++; EATSPACE
#define ADVANCE2 e->ppos+=2; EATSPACE

static inline void expr_eatprefix(expr *e,const char *str) {
	while (*str!='\0' && (expr_locase(CSYMBOL)==expr_locase(*str))) {
		e->ppos++; // do not use 'ADVANCE1' here - do not ignore white spaces inside of symbols
		str++;
	}
}

static inline node* expr_root(expr *e);

static inline node* expr_num(expr *e) {
	node *a;
	if (CSYMBOL=='(') {
		ADVANCE1;
		a=expr_root(e);
		if (CSYMBOL==')') {
			ADVANCE1;
			return a;
		} else {
			printf("parse error: closing round bracket expected\n");
			expr_rfree(a);
			e->parseerror = 1;
			expr_print_error_position(e);
			return NULL;
		}
	}
	if (CSYMBOL=='-') {
		ADVANCE1;
		a = expr_num(e);
		return expr_new_node(OP_MINUS,0,a,NULL,NULL);
	}
	if (CSYMBOL=='!') {
		ADVANCE1;
		a = expr_num(e);
		return expr_new_node(OP_LNOT,0,a,NULL,NULL);
	}
	if (CSYMBOL=='~') {
		ADVANCE1;
		a = expr_num(e);
		return expr_new_node(OP_BNOT,0,a,NULL,NULL);
	}
	if (CSYMBOL>='0' && CSYMBOL<='9') {
		uint64_t v;
		uint8_t d;
		v = 0;
		if (CSYMBOL=='0') {
			ADVANCE1;
			if (CSYMBOL=='x') { // hex const
				ADVANCE1;
				while ((d=expr_hex_digit(CSYMBOL))!=0xFF) {
					v*=16;
					v+=d;
					ADVANCE1;
				}
			} else if (CSYMBOL=='b') { // binary const
				ADVANCE1;
				while ((d=expr_bin_digit(CSYMBOL))!=0xFF) {
					v<<=1;
					v+=d;
					ADVANCE1;
				}
			} else { // oct const
				while ((d=expr_oct_digit(CSYMBOL))!=0xFF) {
					v*=8;
					v+=d;
					ADVANCE1;
				}
			}
		} else { // dec const
			while ((d=expr_dec_digit(CSYMBOL))!=0xFF) {
				v*=10;
				v+=d;
				ADVANCE1;
			}
		}
		return expr_new_node(NUMBER,v,NULL,NULL,NULL);
	}
	if ((CSYMBOL>='a' && CSYMBOL<='z') || (CSYMBOL>='A' && CSYMBOL<='Z')) {
		a = NULL;
		switch (expr_locase(CSYMBOL)) {
			case 'a':
				if (expr_locase(NSYMBOL)=='t') {
					expr_eatprefix(e,"atime");
					a = expr_new_node(SYMBOL_ATIME,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='p') {
					expr_eatprefix(e,"appendonly");
					a = expr_new_node(CONST_APPENDONLY,0,NULL,NULL,NULL);
				}
				break;
			case 'b':
				if (expr_locase(NSYMBOL)=='l') {
					expr_eatprefix(e,"blockdev");
					a = expr_new_node(CONST_BLOCKDEV,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='d') {
					expr_eatprefix(e,"bdev");
					a = expr_new_node(CONST_BLOCKDEV,0,NULL,NULL,NULL);
				}
				break;
			case 'c':
				if (expr_locase(NSYMBOL)=='t') {
					expr_eatprefix(e,"ctime");
					a = expr_new_node(SYMBOL_CTIME,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='h') {
					if (expr_locase(TSYMBOL)=='u') {
						expr_eatprefix(e,"chunkid");
						e->useschunkid = 1;
						a = expr_new_node(SYMBOL_CHUNK,0,NULL,NULL,NULL);
					} else if (expr_locase(TSYMBOL)=='a') {
						expr_eatprefix(e,"chardev");
						a = expr_new_node(CONST_CHARDEV,0,NULL,NULL,NULL);
					}
				} else if (expr_locase(NSYMBOL)=='d') {
					expr_eatprefix(e,"cdev");
					a = expr_new_node(CONST_CHARDEV,0,NULL,NULL,NULL);
				}
				break;
			case 'd':
				expr_eatprefix(e,"directory");
				a = expr_new_node(CONST_DIRECTORY,0,NULL,NULL,NULL);
				break;
			case 'e':
				if (expr_locase(NSYMBOL)=='x') {
					expr_eatprefix(e,"execute");
					a = expr_new_node(CONST_EXECUTE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='a') {
					expr_eatprefix(e,"eattr");
					a = expr_new_node(SYMBOL_EATTR,0,NULL,NULL,NULL);
				}
				break;
			case 'f':
				if (expr_locase(NSYMBOL)=='i') {
					if (expr_locase(TSYMBOL)=='l') {
						expr_eatprefix(e,"file");
						a = expr_new_node(CONST_FILE,0,NULL,NULL,NULL);
					} else if (expr_locase(TSYMBOL)=='f') {
						expr_eatprefix(e,"fifo");
						a = expr_new_node(CONST_FIFO,0,NULL,NULL,NULL);
					}
				} else if (expr_locase(NSYMBOL)=='o') {
					expr_eatprefix(e,"folder");
					a = expr_new_node(CONST_DIRECTORY,0,NULL,NULL,NULL);
				}
				break;
			case 'g':
				if (expr_locase(NSYMBOL)=='i') {
					expr_eatprefix(e,"gid");
					a = expr_new_node(SYMBOL_GID,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='m') {
					expr_eatprefix(e,"gmode");
					a = expr_new_node(SYMBOL_GMODE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='r') {
					expr_eatprefix(e,"gread");
					a = expr_new_node(CONST_GREAD,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='w') {
					expr_eatprefix(e,"gwrite");
					a = expr_new_node(CONST_GWRITE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='e') {
					expr_eatprefix(e,"gexecute");
					a = expr_new_node(CONST_GEXECUTE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='x') {
					expr_eatprefix(e,"gx");
					a = expr_new_node(CONST_GEXECUTE,0,NULL,NULL,NULL);
				}
				break;
			case 'i':
				if (expr_locase(NSYMBOL)=='n') {
					expr_eatprefix(e,"inode");
					a = expr_new_node(SYMBOL_INODE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='m') {
					expr_eatprefix(e,"immutable");
					a = expr_new_node(CONST_IMMUTABLE,0,NULL,NULL,NULL);
				}
				break;
			case 'l':
				expr_eatprefix(e,"length");
				a = expr_new_node(SYMBOL_LENGTH,0,NULL,NULL,NULL);
				break;
			case 'm':
				if (expr_locase(NSYMBOL)=='o') {
					expr_eatprefix(e,"mode");
					a = expr_new_node(SYMBOL_MODE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='t') {
					expr_eatprefix(e,"mtime");
					a = expr_new_node(SYMBOL_MTIME,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='a') {
					expr_eatprefix(e,"major");
					a = expr_new_node(SYMBOL_MAJOR,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='i') {
					expr_eatprefix(e,"minor");
					a = expr_new_node(SYMBOL_MINOR,0,NULL,NULL,NULL);
				}
				break;
			case 'n':
				if (expr_locase(NSYMBOL)=='o') {
					if (expr_locase(TSYMBOL)=='o') {
						expr_eatprefix(e,"noowner");
						a = expr_new_node(CONST_NOOWNER,0,NULL,NULL,NULL);
					} else if (expr_locase(TSYMBOL)=='a') {
						expr_eatprefix(e,"noacache");
						a = expr_new_node(CONST_NOACACHE,0,NULL,NULL,NULL);
					} else if (expr_locase(TSYMBOL)=='e') {
						expr_eatprefix(e,"noacache");
						a = expr_new_node(CONST_NOECACHE,0,NULL,NULL,NULL);
					} else if (expr_locase(TSYMBOL)=='d') {
						expr_eatprefix(e,"nodatacache");
						a = expr_new_node(CONST_NODATACACHE,0,NULL,NULL,NULL);
					}
				}
				break;
			case 'o':
				if (expr_locase(NSYMBOL)=='m') {
					expr_eatprefix(e,"omode");
					a = expr_new_node(SYMBOL_OMODE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='r') {
					expr_eatprefix(e,"oread");
					a = expr_new_node(CONST_OREAD,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='w') {
					expr_eatprefix(e,"owrite");
					a = expr_new_node(CONST_OWRITE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='e') {
					expr_eatprefix(e,"oexecute");
					a = expr_new_node(CONST_OEXECUTE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='x') {
					expr_eatprefix(e,"ox");
					a = expr_new_node(CONST_OEXECUTE,0,NULL,NULL,NULL);
				}
				break;
			case 'r':
				expr_eatprefix(e,"read");
				a = expr_new_node(CONST_READ,0,NULL,NULL,NULL);
				break;
			case 's':
				if (expr_locase(NSYMBOL)=='c') {
					expr_eatprefix(e,"sclass");
					a = expr_new_node(SYMBOL_SCLASS,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='o') {
					expr_eatprefix(e,"socket");
					a = expr_new_node(CONST_SOCKET,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='y') {
					expr_eatprefix(e,"symlink");
					a = expr_new_node(CONST_SYMLINK,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='u') {
					if (expr_locase(TSYMBOL)=='s') {
						expr_eatprefix(e,"sustained");
						a = expr_new_node(CONST_SUSTAINED,0,NULL,NULL,NULL);
					} else if (expr_locase(TSYMBOL)=='i') {
						expr_eatprefix(e,"suid");
						a = expr_new_node(CONST_SUID,0,NULL,NULL,NULL);
					}
				} else if (expr_locase(NSYMBOL)=='g') {
					expr_eatprefix(e,"sgid");
					a = expr_new_node(CONST_SGID,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='t') {
					expr_eatprefix(e,"sticky");
					a = expr_new_node(CONST_STICKY,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='n') {
					expr_eatprefix(e,"snapshot");
					a = expr_new_node(CONST_SNAPSHOT,0,NULL,NULL,NULL);
				}
				break;
			case 't':
				if (expr_locase(NSYMBOL)=='y') {
					expr_eatprefix(e,"type");
					a = expr_new_node(SYMBOL_TYPE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='i') {
					expr_eatprefix(e,"time");
					a = expr_new_node(SYMBOL_TIME,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='r') {
					if (expr_locase(TSYMBOL)=='a') {
						expr_eatprefix(e,"trash");
						a = expr_new_node(CONST_TRASH,0,NULL,NULL,NULL);
					} else if (expr_locase(TSYMBOL)=='e') {
						expr_eatprefix(e,"tretention");
						a = expr_new_node(SYMBOL_TRETENTION,0,NULL,NULL,NULL);
					}
				}
				break;
			case 'u':
				if (expr_locase(NSYMBOL)=='i') {
					expr_eatprefix(e,"uid");
					a = expr_new_node(SYMBOL_UID,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='m') {
					expr_eatprefix(e,"umode");
					a = expr_new_node(SYMBOL_UMODE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='r') {
					expr_eatprefix(e,"uread");
					a = expr_new_node(CONST_UREAD,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='w') {
					expr_eatprefix(e,"uwrite");
					a = expr_new_node(CONST_UWRITE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='e') {
					expr_eatprefix(e,"uexecute");
					a = expr_new_node(CONST_UEXECUTE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='x') {
					expr_eatprefix(e,"ux");
					a = expr_new_node(CONST_UEXECUTE,0,NULL,NULL,NULL);
				} else if (expr_locase(NSYMBOL)=='n') {
					expr_eatprefix(e,"undeletable");
					a = expr_new_node(CONST_UNDELETABLE,0,NULL,NULL,NULL);
				}
				break;
			case 'w':
				expr_eatprefix(e,"write");
				a = expr_new_node(CONST_WRITE,0,NULL,NULL,NULL);
				break;
			case 'x':
				ADVANCE1;
				a = expr_new_node(CONST_EXECUTE,0,NULL,NULL,NULL);
				break;
		}
		if (a==NULL || (CSYMBOL>='a' && CSYMBOL<='z') || (CSYMBOL>='A' && CSYMBOL<='Z')) {
			printf("parse error: unknown identifier\n");
			e->parseerror = 1;
			expr_rfree(a);
			expr_print_error_position(e);
			return NULL;
		}
		EATSPACE;
		return a;
	}
	printf("parse error: unexpected symbol\n");
	e->parseerror = 1;
	expr_print_error_position(e);
	return NULL;
}

static inline node* expr_md(expr *e) {
	node *a = expr_num(e);
	node *b;
	while (1) {
		if (CSYMBOL=='/') {
			ADVANCE1;
			b = expr_num(e);
			a = expr_new_node(OP_DIV,0,a,b,NULL);
		} else if (CSYMBOL=='*') {
			ADVANCE1;
			b = expr_num(e);
			a = expr_new_node(OP_MUL,0,a,b,NULL);
		} else if (CSYMBOL=='%') {
			ADVANCE1;
			b = expr_num(e);
			a = expr_new_node(OP_MOD,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

static inline node* expr_pm(expr *e) {
	node *a = expr_md(e);
	node *b;
	while (1) {
		if (CSYMBOL=='+') {
			ADVANCE1;
			b = expr_md(e);
			a = expr_new_node(OP_ADD,0,a,b,NULL);
		} else if (CSYMBOL=='-') {
			ADVANCE1;
			b = expr_md(e);
			a = expr_new_node(OP_SUB,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

static inline node* expr_shift(expr *e) {
	node *a = expr_pm(e);
	node *b;
	while (1) {
		if (CSYMBOL=='<' && NSYMBOL=='<') {
			ADVANCE2;
			b = expr_pm(e);
			a = expr_new_node(OP_LSHIFT,0,a,b,NULL);
		} else if (CSYMBOL=='>' && NSYMBOL=='>') {
			ADVANCE2;
			b = expr_pm(e);
			a = expr_new_node(OP_RSHIFT,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

static inline node* expr_lg(expr *e) {
	node *a = expr_shift(e);
	node *b;
	while (1) {
		if (CSYMBOL=='<' && NSYMBOL=='=') {
			ADVANCE2;
			b = expr_shift(e);
			a = expr_new_node(OP_LE,0,a,b,NULL);
		} else if (CSYMBOL=='<' && NSYMBOL!='<') {
			ADVANCE1;
			b = expr_shift(e);
			a = expr_new_node(OP_LT,0,a,b,NULL);
		} else if (CSYMBOL=='>' && NSYMBOL=='=') {
			ADVANCE2;
			b = expr_shift(e);
			a = expr_new_node(OP_GE,0,a,b,NULL);
		} else if (CSYMBOL=='>' && NSYMBOL!='>') {
			ADVANCE1;
			b = expr_shift(e);
			a = expr_new_node(OP_GT,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

static inline node* expr_eq(expr *e) {
	node *a = expr_lg(e);
	node *b;
	while (1) {
		if (CSYMBOL=='=' && NSYMBOL=='=') {
			ADVANCE2;
			b = expr_lg(e);
			a = expr_new_node(OP_EQ,0,a,b,NULL);
		} else if (CSYMBOL=='!' && NSYMBOL=='=') {
			ADVANCE2;
			b = expr_lg(e);
			a = expr_new_node(OP_NE,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

static inline node* expr_band(expr *e) {
	node *a = expr_eq(e);
	node *b;
	while (1) {
		if (CSYMBOL=='&' && NSYMBOL!='&') {
			ADVANCE1;
			b = expr_eq(e);
			a = expr_new_node(OP_BAND,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

static inline node* expr_bxor(expr *e) {
	node *a = expr_band(e);
	node *b;
	while (1) {
		if (CSYMBOL=='^' && NSYMBOL!='^') {
			ADVANCE1;
			b = expr_band(e);
			a = expr_new_node(OP_BXOR,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

static inline node* expr_bor(expr *e) {
	node *a = expr_bxor(e);
	node *b;
	while (1) {
		if (CSYMBOL=='|' && NSYMBOL!='|') {
			ADVANCE1;
			b = expr_bxor(e);
			a = expr_new_node(OP_BOR,0,a,b,NULL);
		} else {
			return a;
		}
	}
}

/* A&&B&&C -> (A&&(B&&C)) */
static inline node* expr_and(expr *e) {
	node *a = expr_bor(e);
	node *b;
	if (CSYMBOL=='&' && NSYMBOL=='&') {
		ADVANCE2;
		b = expr_and(e);
		return expr_new_node(OP_LAND,0,a,b,NULL);
	} else {
		return a;
	}
}

static inline node* expr_xor(expr *e) {
	node *a = expr_and(e);
	node *b;
	while (1) {
		if (CSYMBOL=='^' && NSYMBOL=='^') {
			ADVANCE2;
			b = expr_and(e);
			a = expr_new_node(OP_LXOR,0,a,b,NULL); //((a && !b) || (!a && b));
		} else {
			return a;
		}
	}
}

// R to L
// A||B||C -> (A||(B||C))
static inline node* expr_or(expr *e) {
	node *a = expr_xor(e);
	node *b;
	if (CSYMBOL=='|' && NSYMBOL=='|') {
		ADVANCE2;
		b = expr_or(e);
		return expr_new_node(OP_LOR,0,a,b,NULL);
	} else {
		return a;
	}
}

// R to L
// A?B:C?D:E => (A?B:(C?D:E))
// A?B?C:D:E => (A?(B?C:D):E)
// A?B?C:D:E?F:G => (A?(B?C:D):(E?F:G))
// A?B?C:D => (A?(B?C:D):NULL)
static inline node* expr_cond(expr *e) {
	node *a = expr_or(e);
	node *b,*c;
	if (CSYMBOL=='?') {
		ADVANCE1;
		b = expr_cond(e);
		if (CSYMBOL==':') {
			ADVANCE1;
			c = expr_cond(e);
			return expr_new_node(OP_COND,0,a,b,c);
		} else {
			return expr_new_node(OP_COND,0,a,b,NULL);
		}
	} else {
		return a;
	}
}

static inline node* expr_root(expr *e) {
	return expr_cond(e);
}

static inline uint64_t expr_eval_node(node *curnode,inodestr *inodedata) {
	uint64_t aux;
	if (curnode==NULL) {
		return 0;
	}
	switch (curnode->type) {
		case NUMBER:
			return curnode->value;
		case CONST_FILE:
			return TYPE_FILE;
		case CONST_DIRECTORY:
			return TYPE_DIRECTORY;
		case CONST_SYMLINK:
			return TYPE_SYMLINK;
		case CONST_FIFO:
			return TYPE_FIFO;
		case CONST_BLOCKDEV:
			return TYPE_BLOCKDEV;
		case CONST_CHARDEV:
			return TYPE_CHARDEV;
		case CONST_SOCKET:
			return TYPE_SOCKET;
		case CONST_TRASH:
			return TYPE_TRASH;
		case CONST_SUSTAINED:
			return TYPE_SUSTAINED;
		case CONST_SUID:
			return 04000;
		case CONST_SGID:
			return 02000;
		case CONST_STICKY:
			return 01000;
		case CONST_UREAD:
			return 00400;
		case CONST_UWRITE:
			return 00200;
		case CONST_UEXECUTE:
			return 00100;
		case CONST_GREAD:
			return 00040;
		case CONST_GWRITE:
			return 00020;
		case CONST_GEXECUTE:
			return 00010;
		case CONST_OREAD:
			return 00004;
		case CONST_OWRITE:
			return 00002;
		case CONST_OEXECUTE:
			return 00001;
		case CONST_READ:
			return MODE_MASK_R;
		case CONST_WRITE:
			return MODE_MASK_W;
		case CONST_EXECUTE:
			return MODE_MASK_X;
		case CONST_NOOWNER:
			return EATTR_NOOWNER;
		case CONST_NOACACHE:
			return EATTR_NOACACHE;
		case CONST_NOECACHE:
			return EATTR_NOECACHE;
		case CONST_NODATACACHE:
			return EATTR_NODATACACHE;
		case CONST_SNAPSHOT:
			return EATTR_SNAPSHOT;
		case CONST_UNDELETABLE:
			return EATTR_UNDELETABLE;
		case CONST_APPENDONLY:
			return EATTR_APPENDONLY;
		case CONST_IMMUTABLE:
			return EATTR_IMMUTABLE;
		case SYMBOL_INODE:
			return inodedata->inode;
		case SYMBOL_TYPE:
			return inodedata->type;
		case SYMBOL_EATTR:
			return inodedata->flags;
		case SYMBOL_SCLASS:
			return inodedata->sclass;
		case SYMBOL_UID:
			return inodedata->uid;
		case SYMBOL_GID:
			return inodedata->gid;
		case SYMBOL_MODE:
			return inodedata->mode;
		case SYMBOL_UMODE:
			return (inodedata->mode>>6)&7;
		case SYMBOL_GMODE:
			return (inodedata->mode>>3)&7;
		case SYMBOL_OMODE:
			return (inodedata->mode)&7;
		case SYMBOL_ATIME:
			return inodedata->atime;
		case SYMBOL_MTIME:
			return inodedata->mtime;
		case SYMBOL_CTIME:
			return inodedata->ctime;
		case SYMBOL_TRETENTION:
			return inodedata->trashretention;
		case SYMBOL_LENGTH:
			return inodedata->length;
		case SYMBOL_MAJOR:
			return inodedata->major;
		case SYMBOL_MINOR:
			return inodedata->minor;
		case SYMBOL_TIME:
			return inodedata->time;
		case SYMBOL_CHUNK:
			return inodedata->chunkid;
		case OP_MINUS:
			return -expr_eval_node(curnode->op1,inodedata);
		case OP_SUB:
			return expr_eval_node(curnode->op1,inodedata)-expr_eval_node(curnode->op2,inodedata);
		case OP_ADD:
			return expr_eval_node(curnode->op1,inodedata)+expr_eval_node(curnode->op2,inodedata);
		case OP_MUL:
			return expr_eval_node(curnode->op1,inodedata)*expr_eval_node(curnode->op2,inodedata);
		case OP_DIV:
			aux = expr_eval_node(curnode->op2,inodedata);
			if (aux!=0) {
				return expr_eval_node(curnode->op1,inodedata)/aux;
			} else {
				printf("division by zero !!! (evaluate expression)\n");
				return 0;
			}
		case OP_MOD:
			aux = expr_eval_node(curnode->op2,inodedata);
			if (aux!=0) {
				return expr_eval_node(curnode->op1,inodedata)%aux;
			} else {
				printf("division by zero !!! (evaluate expression)\n");
				return 0;
			}
		case OP_LSHIFT:
			return expr_eval_node(curnode->op1,inodedata)<<expr_eval_node(curnode->op2,inodedata);
		case OP_RSHIFT:
			return expr_eval_node(curnode->op1,inodedata)>>expr_eval_node(curnode->op2,inodedata);
		case OP_LE:
			return expr_eval_node(curnode->op1,inodedata)<=expr_eval_node(curnode->op2,inodedata);
		case OP_LT:
			return expr_eval_node(curnode->op1,inodedata)<expr_eval_node(curnode->op2,inodedata);
		case OP_GE:
			return expr_eval_node(curnode->op1,inodedata)>=expr_eval_node(curnode->op2,inodedata);
		case OP_GT:
			return expr_eval_node(curnode->op1,inodedata)>expr_eval_node(curnode->op2,inodedata);
		case OP_EQ:
			return expr_eval_node(curnode->op1,inodedata)==expr_eval_node(curnode->op2,inodedata);
		case OP_NE:
			return expr_eval_node(curnode->op1,inodedata)!=expr_eval_node(curnode->op2,inodedata);
		case OP_BNOT:
			return ~expr_eval_node(curnode->op1,inodedata);
		case OP_BAND:
			return expr_eval_node(curnode->op1,inodedata)&expr_eval_node(curnode->op2,inodedata);
		case OP_BXOR:
			return expr_eval_node(curnode->op1,inodedata)^expr_eval_node(curnode->op2,inodedata);
		case OP_BOR:
			return expr_eval_node(curnode->op1,inodedata)|expr_eval_node(curnode->op2,inodedata);
		case OP_LNOT:
			return !expr_eval_node(curnode->op1,inodedata);
		case OP_LAND:
			return expr_eval_node(curnode->op1,inodedata)&&expr_eval_node(curnode->op2,inodedata);
		case OP_LXOR:
			if (expr_eval_node(curnode->op1,inodedata)) {
				return expr_eval_node(curnode->op2,inodedata)?0:1;
			} else {
				return expr_eval_node(curnode->op2,inodedata)?1:0;
			}
		case OP_LOR:
			return expr_eval_node(curnode->op1,inodedata)||expr_eval_node(curnode->op2,inodedata);
		case OP_COND:
			return expr_eval_node(curnode->op1,inodedata)?expr_eval_node(curnode->op2,inodedata):expr_eval_node(curnode->op3,inodedata);
		default:
			return 0;
	}
}

static inline void expr_print_node(node *curnode) {
#define PRINT_TWOARG_ONECHR_EXPR(OPCHR) { \
	putchar('('); \
	expr_print_node(curnode->op1); \
	putchar(OPCHR); \
	expr_print_node(curnode->op2); \
	putchar(')'); \
	return; \
}

#define PRINT_TWOARG_TWOCHR_EXPR(OPCHR1,OPCHR2) { \
	putchar('('); \
	expr_print_node(curnode->op1); \
	putchar(OPCHR1); \
	putchar(OPCHR2); \
	expr_print_node(curnode->op2); \
	putchar(')'); \
	return; \
}

#define PRINT_ONEARG_EXPR(OPCHR) { \
	putchar('('); \
	putchar(OPCHR); \
	expr_print_node(curnode->op1); \
	putchar(')'); \
	return; \
}

	if (curnode==NULL) {
		printf("0");
		return;
	}
	switch (curnode->type) {
		case NUMBER:
			printf("%"PRIu64,curnode->value);
			return;
		case CONST_FILE:
			printf("file");
			return;
		case CONST_DIRECTORY:
			printf("directory");
			return;
		case CONST_SYMLINK:
			printf("symlink");
			return;
		case CONST_FIFO:
			printf("fifo");
			return;
		case CONST_BLOCKDEV:
			printf("blockdev");
			return;
		case CONST_CHARDEV:
			printf("chardev");
			return;
		case CONST_SOCKET:
			printf("socket");
			return;
		case CONST_TRASH:
			printf("trash");
			return;
		case CONST_SUSTAINED:
			printf("sustained");
			return;
		case CONST_SUID:
			printf("suid");
			return;
		case CONST_SGID:
			printf("sgid");
			return;
		case CONST_STICKY:
			printf("sticky");
			return;
		case CONST_UREAD:
			printf("ur");
			return;
		case CONST_UWRITE:
			printf("uw");
			return;
		case CONST_UEXECUTE:
			printf("ux");
			return;
		case CONST_GREAD:
			printf("gr");
			return;
		case CONST_GWRITE:
			printf("gw");
			return;
		case CONST_GEXECUTE:
			printf("gx");
			return;
		case CONST_OREAD:
			printf("or");
			return;
		case CONST_OWRITE:
			printf("ow");
			return;
		case CONST_OEXECUTE:
			printf("ox");
			return;
		case CONST_READ:
			printf("read");
			return;
		case CONST_WRITE:
			printf("write");
			return;
		case CONST_EXECUTE:
			printf("execute");
			return;
		case CONST_NOOWNER:
			printf("noowner");
			return;
		case CONST_NOACACHE:
			printf("noacache");
			return;
		case CONST_NOECACHE:
			printf("noecache");
			return;
		case CONST_NODATACACHE:
			printf("nodatacache");
			return;
		case CONST_SNAPSHOT:
			printf("snapshot");
			return;
		case CONST_UNDELETABLE:
			printf("undeletable");
			return;
		case CONST_APPENDONLY:
			printf("appendonly");
			return;
		case CONST_IMMUTABLE:
			printf("immutable");
			return;
		case SYMBOL_INODE:
			printf("inode");
			return;
		case SYMBOL_TYPE:
			printf("type");
			return;
		case SYMBOL_EATTR:
			printf("eattr");
			return;
		case SYMBOL_SCLASS:
			printf("sclass");
			return;
		case SYMBOL_UID:
			printf("uid");
			return;
		case SYMBOL_GID:
			printf("gid");
			return;
		case SYMBOL_MODE:
			printf("mode");
			return;
		case SYMBOL_UMODE:
			printf("umode");
			return;
		case SYMBOL_GMODE:
			printf("gmode");
			return;
		case SYMBOL_OMODE:
			printf("omode");
			return;
		case SYMBOL_ATIME:
			printf("atime");
			return;
		case SYMBOL_MTIME:
			printf("mtime");
			return;
		case SYMBOL_CTIME:
			printf("ctime");
			return;
		case SYMBOL_TRETENTION:
			printf("tretention");
			return;
		case SYMBOL_LENGTH:
			printf("length");
			return;
		case SYMBOL_MAJOR:
			printf("major");
			return;
		case SYMBOL_MINOR:
			printf("minor");
			return;
		case SYMBOL_TIME:
			printf("time");
			return;
		case SYMBOL_CHUNK:
			printf("chunkid");
			return;
		case OP_MINUS:
			PRINT_ONEARG_EXPR('-');
		case OP_SUB:
			PRINT_TWOARG_ONECHR_EXPR('-');
		case OP_ADD:
			PRINT_TWOARG_ONECHR_EXPR('+');
		case OP_MUL:
			PRINT_TWOARG_ONECHR_EXPR('*');
		case OP_DIV:
			PRINT_TWOARG_ONECHR_EXPR('/');
		case OP_MOD:
			PRINT_TWOARG_ONECHR_EXPR('%');
		case OP_LSHIFT:
			PRINT_TWOARG_TWOCHR_EXPR('<','<');
		case OP_RSHIFT:
			PRINT_TWOARG_TWOCHR_EXPR('>','>');
		case OP_LE:
			PRINT_TWOARG_TWOCHR_EXPR('<','=');
		case OP_LT:
			PRINT_TWOARG_ONECHR_EXPR('<');
		case OP_GE:
			PRINT_TWOARG_TWOCHR_EXPR('>','=');
		case OP_GT:
			PRINT_TWOARG_ONECHR_EXPR('>');
		case OP_EQ:
			PRINT_TWOARG_TWOCHR_EXPR('=','=');
		case OP_NE:
			PRINT_TWOARG_TWOCHR_EXPR('!','=');
		case OP_BNOT:
			PRINT_ONEARG_EXPR('~');
		case OP_BAND:
			PRINT_TWOARG_ONECHR_EXPR('&');
		case OP_BXOR:
			PRINT_TWOARG_ONECHR_EXPR('^');
		case OP_BOR:
			PRINT_TWOARG_ONECHR_EXPR('|');
		case OP_LNOT:
			PRINT_ONEARG_EXPR('!');
		case OP_LAND:
			PRINT_TWOARG_TWOCHR_EXPR('&','&');
		case OP_LXOR:
			PRINT_TWOARG_TWOCHR_EXPR('^','^');
		case OP_LOR:
			PRINT_TWOARG_TWOCHR_EXPR('|','|');
		case OP_COND:
			putchar('(');
			expr_print_node(curnode->op1);
			putchar('?');
			expr_print_node(curnode->op2);
			putchar(':');
			expr_print_node(curnode->op3);
			putchar(')');
			return;
		default:
			printf("#SYM?");
			return;
	}
}

// API

void* expr_new(const char *str) {
	expr *e = malloc(sizeof(expr));
	e->str = str;
	e->ppos = 0;
	EATSPACE;
	e->parseerror = 0;
	e->useschunkid = 0;
	e->root = expr_root(e);
	EATSPACE;
	if (e->parseerror==0 && CSYMBOL!='\0' && CSYMBOL!='\r' && CSYMBOL!='\n') {
		printf("parse error: garbage at the end of expression\n");
		e->parseerror = 1;
		expr_print_error_position(e);
	}
	if (e->parseerror) {
		expr_rfree(e->root);
		free(e);
		return NULL;
	}
	return e;
}

void expr_free(void *ev) {
	expr *e = (expr*)ev;
	expr_rfree(e->root);
	free(e);
}

void expr_print(void *ev) {
	expr *e = (expr*)ev;
	expr_print_node(e->root);
	putchar('\n');
}

uint8_t expr_useschunkid(void *ev) {
	expr *e = (expr*)ev;
	return e->useschunkid;
}

uint8_t expr_check(void *ev,inodestr *inodedata) {
	expr *e = (expr*)ev;
	return expr_eval_node(e->root,inodedata);
}
