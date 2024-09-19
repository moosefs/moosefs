#include <string.h>
#include <stdlib.h>
#include <inttypes.h>

#define GLOB_CACHE_SIZE 16

#include "globengine.h"

#ifdef GLOB_TEST
#include <stdio.h>
#include <time.h>

#define massert(x,y) {if (!(x)) {printf("%s\n",y); abort();}}
#define passert(p) {if (p==NULL) {printf("no memory\n"); abort();}}
#define monotonic_seconds() time(NULL)
#define main_destruct_register(x)

#else

#ifdef GLOB_TOOLS
#include "clocks.h"
#include "massert.h"
#define main_destruct_register(x)

#else
#include "clocks.h"
#include "main.h"
#include "massert.h"

#endif
#endif

typedef enum _atomtype {
	ATOM_STRING,
	ATOM_ASTERISK,
	ATOM_QMARK,
	ATOM_RANGE
} atomtype;

typedef struct _atom {
	atomtype type;
	uint32_t leng;
	uint8_t *str;
	uint32_t rangebits[8];
	struct _atom *next;
} atom;

typedef enum _patflags {
	PATFLAG_FIRSTASTERISK = 1,
	PATFLAG_LASTASTERISK = 2,
} patflags;

typedef struct _subpattern {
	atom *attab;
	uint8_t atelements;
	uint8_t leng;
} subpattern;

typedef struct _globpattern {
	subpattern *sptab;
	uint8_t spelements;
	uint32_t minleng;
	patflags flags;
} globpattern;


typedef struct _globcache {
	void *glob;
	uint8_t valid;
	uint8_t gnleng;
	uint8_t *gname;
	double mt;
} globcache;

static globcache globtab[GLOB_CACHE_SIZE];

static inline void parse_range(uint32_t rangebits[8],const uint8_t *start,const uint8_t *end) {
	const uint8_t *s = start, *e = end;
	uint32_t mask;
	uint8_t pos;
	uint8_t neg = 0;
	uint8_t i;

	if (s<e && *s=='!') {
		s++;
		neg = 1;
	}
	if (neg) {
		memset(rangebits,UINT32_C(0xFFFFFFFF),sizeof(uint32_t)*8);
	} else {
		memset(rangebits,0,sizeof(uint32_t)*8);
	}
	while (s<e) {
		if (s+2<e && s[1]=='-') {
			for (i=s[0] ; i<=s[2] ; i++) {
				pos = i>>5;
				mask = 1U<<(i&0x1F);
				if (neg) {
					rangebits[pos] &= ~mask;
				} else {
					rangebits[pos] |= mask;
				}
			}
			s+=3;
		} else {
			i = *s++;
			pos = i>>5;
			mask = 1U<<(i&0x1F);
			if (neg) {
				rangebits[pos] &= ~mask;
			} else {
				rangebits[pos] |= mask;
			}
		}
	}
}

static inline uint32_t unescape_string(const uint8_t *src,const uint8_t **srcend,uint8_t *dst) {
	uint32_t l;
	const uint8_t *r;
	l = 0;
	r = src;
	while (*r!=0) {
		if (*r=='\\') {
			if ((*(r+1)!=0)) {
				if (dst!=NULL) {
					dst[l]=*(r+1);
				}
				r+=2;
				l++;
			} else {
				if (dst!=NULL) {
					dst[l]=*r;
				}
				r++;
				l++;
			}
		} else if (*r=='[' || *r=='*' || *r=='?') {
			break;
		} else {
			if (dst!=NULL) {
				dst[l]=*r;
			}
			r++;
			l++;
		}
	}
	if (srcend) {
		*srcend = r;
	}
	return l;
}

static inline atom* pattern_to_atoms_list(const uint8_t *globstr) {
	const uint8_t *p = globstr;
	const uint8_t *r;
	uint8_t c;
	atom *head,**tail,*a;
	uint8_t last_asterisk = 0;
	uint32_t l;

	head = NULL;
	tail = &head;
	a = NULL;

	while ((c=*p++)!=0) {
		if (c=='*') {
			if (last_asterisk==0) {
				a = malloc(sizeof(atom));
				passert(a);
				memset(a,0,sizeof(atom));
				a->type = ATOM_ASTERISK;
				a->next = NULL;
				*tail = a;
				tail = &(a->next);
				last_asterisk = 1;
			}
			continue;
		} else if (c=='?') {
			a = malloc(sizeof(atom));
			passert(a);
			memset(a,0,sizeof(atom));
			a->type = ATOM_QMARK;
			a->next = NULL;
			*tail = a;
			tail = &(a->next);
			last_asterisk = 0;
			continue;
		} else if (c=='[') {
			r = p;
			while (*r!=0 && *r!=']') {
				r++;
			}
			if (*r==']') {
				a = malloc(sizeof(atom));
				passert(a);
				memset(a,0,sizeof(atom));
				a->type = ATOM_RANGE;
				parse_range(a->rangebits,p,r);
				p = r+1;
				a->next = NULL;
				*tail = a;
				tail = &(a->next);
				last_asterisk = 0;
				continue;
			}
		}
		if (c=='[') {
			l = unescape_string(p,NULL,NULL)+1;
		} else {
			l = unescape_string(p-1,NULL,NULL);
		}
		a = malloc(sizeof(atom));
		passert(a);
		memset(a,0,sizeof(atom));
		a->type = ATOM_STRING;
		a->leng = l;
		a->str = malloc(a->leng);
		passert(a->str);
		if (c=='[') {
			a->str[0] = '[';
			unescape_string(p,&p,a->str+1);
		} else {
			unescape_string(p-1,&p,a->str);
		}
		a->next = NULL;
		*tail = a;
		tail = &(a->next);
		last_asterisk = 0;
	}
	return head;
}

#ifdef GLOB_TEST
static inline void print_atom(const atom *a) {
	uint8_t i;
	uint8_t pos;
	uint32_t mask;
	switch (a->type) {
		case ATOM_ASTERISK:
			printf("ASTERISK (*)\n");
			break;
		case ATOM_QMARK:
			printf("QUESTION MARK (?)\n");
			break;
		case ATOM_RANGE:
			printf("RANGE (");
			i = 0;
			for (pos=0 ; pos<8 ; pos++) {
				for (mask=1 ; mask!=0 ; mask<<=1) {
					if (a->rangebits[pos]&mask) {
						if (i>32 && i<127) {
							printf("%c",i);
						} else {
							printf("+");
						}
					} else {
						printf("-");
					}
					i++;
				}
			}
			printf(")\n");
			break;
		case ATOM_STRING:
			printf("STRING (");
			for (i=0 ; i<a->leng ; i++) {
				printf("%c",a->str[i]);
			}
			printf(")\n");
			break;
	}
}

static inline void print_atoms_list(const atom *a) {
	while (a!=NULL) {
		print_atom(a);
		a = a->next;
	}
}
#endif

static inline uint8_t atom_range_match(uint32_t rangebits[8],uint8_t c) {
	uint8_t pos = c>>5;
	uint32_t mask = 1U<<(c&0x1F);
	return (rangebits[pos]&mask)?1:0;
}

static inline uint8_t subpattern_match_exact(subpattern *sp,const uint8_t *name,uint8_t nleng) {
	atom *a;
	uint8_t atpos;
	for (atpos=0 ; atpos<sp->atelements ; atpos++) {
		a = sp->attab+atpos;
		switch (a->type) {
			case ATOM_STRING:
				if (a->leng > nleng || memcmp(name,a->str,a->leng)!=0) {
					return 0;
				}
				name += a->leng;
				nleng -= a->leng;
				break;
			case ATOM_QMARK:
				if (nleng==0) {
					return 0;
				}
				name += 1;
				nleng -= 1;
				break;
			case ATOM_RANGE:
				if (nleng==0 || atom_range_match(a->rangebits,name[0])==0) {
					return 0;
				}
				name += 1;
				nleng -= 1;
				break;
			default:
				return 0;
		}
	}
	return 1;
}

static inline int subpattern_closest_match(subpattern *sp,const uint8_t *name,uint8_t nleng) {
	uint32_t pos;
	pos = 0;
	while (sp->leng <= nleng-pos) {
		if (subpattern_match_exact(sp,name+pos,nleng-pos)) {
			return pos;
		}
		pos++;
	}
	return -1;
}

static inline uint8_t pattern_match(globpattern *p,const uint8_t *name,uint8_t nleng) {
	int pos;
	uint8_t i;
	if (nleng < p->minleng) {
		return 0;
	}
	if (p->spelements==0) {
		if (p->flags & (PATFLAG_LASTASTERISK|PATFLAG_FIRSTASTERISK)) {
			return 1; // match
		}
		return 0; //doesn't match
	}
	if (p->spelements==1) {
		if (p->sptab[0].leng>nleng) {
			return 0;
		}
		if ((p->flags & (PATFLAG_LASTASTERISK|PATFLAG_FIRSTASTERISK)) == (PATFLAG_LASTASTERISK|PATFLAG_FIRSTASTERISK)) {
			if (subpattern_closest_match(p->sptab,name,nleng)>=0) {
				return 1;
			}
			return 0;
		}
		if (p->flags & PATFLAG_LASTASTERISK) {
			return subpattern_match_exact(p->sptab,name,nleng);
		}
		if (p->flags & PATFLAG_FIRSTASTERISK) {
			return subpattern_match_exact(p->sptab,name+(nleng-p->sptab[0].leng),p->sptab[0].leng);
		}
		if (p->sptab[0].leng!=nleng) {
			return 0;
		}
		return subpattern_match_exact(p->sptab,name,nleng);
	}
	if (p->flags & PATFLAG_FIRSTASTERISK) {
		pos = subpattern_closest_match(p->sptab,name,nleng);
		if (pos<0) {
			return 0;
		}
		name += pos+p->sptab[0].leng;
		nleng -= pos+p->sptab[0].leng;
	} else {
		if (subpattern_match_exact(p->sptab,name,nleng)==0) {
			return 0;
		}
		name += p->sptab[0].leng;
		nleng -= p->sptab[0].leng;
	}
	for (i=1 ; i<p->spelements-1 ; i++) {
		pos = subpattern_closest_match(p->sptab+i,name,nleng);
		if (pos<0) {
			return 0;
		}
		name += pos+p->sptab[0].leng;
		nleng -= pos+p->sptab[0].leng;
	}
	if (p->flags & PATFLAG_LASTASTERISK) {
		pos = subpattern_closest_match(p->sptab+i,name,nleng);
		return (pos<0)?0:1;
	} else {
		if (nleng<p->sptab[i].leng) {
			return 0;
		}
		return subpattern_match_exact(p->sptab+i,name+(nleng-p->sptab[i].leng),p->sptab[i].leng);
	}
	return 0;
}

static inline void atoms_list_to_pattern_structure(globpattern *p,atom *atlist) {
	atom *a,*an;
	uint8_t l,m;

	p->minleng = 0;
	p->flags = 0;
	p->spelements = 0;
	p->sptab = NULL;

	if (atlist==NULL) {
		return;
	}
	if (atlist->type==ATOM_ASTERISK) {
		p->flags |= PATFLAG_FIRSTASTERISK;
		an = atlist->next;
		free(atlist);
		atlist = an;
		if (atlist==NULL) {
			p->flags |= PATFLAG_LASTASTERISK;
			return;
		}
	}
	p->spelements = 1;
	for (a=atlist ; a!=NULL ; a=a->next) {
		if (a->type==ATOM_ASTERISK) {
			if (a->next!=NULL) {
				p->spelements++;
			} else {
				p->flags |= PATFLAG_LASTASTERISK;
			}
		}
	}
	p->sptab = malloc(sizeof(subpattern)*p->spelements);
	passert(p->sptab);
	for (l=0 ; l<p->spelements ; l++) {
		p->sptab[l].atelements = 0;
		p->sptab[l].leng = 0;
	}
	l = 0;
	for (a=atlist ; a!=NULL ; a=a->next) {
		if (a->type==ATOM_ASTERISK) {
			l++;
		} else {
			p->sptab[l].atelements++;
			if (a->type==ATOM_STRING) {
				p->sptab[l].leng+=a->leng;
			} else {
				p->sptab[l].leng+=1;
			}
		}
	}
	for (l=0 ; l<p->spelements ; l++) {
		p->sptab[l].attab = malloc(sizeof(atom)*p->sptab[l].atelements);
		passert(p->sptab[l].attab);
		p->minleng += p->sptab[l].leng;
	}
	l = 0;
	m = 0;
	for (a=atlist ; a!=NULL ; a=an) {
		if (a->type==ATOM_ASTERISK) {
			l++;
			m=0;
		} else {
			massert(l<p->spelements,"wrong pattern elements count");
			memcpy(p->sptab[l].attab+m,a,sizeof(atom));
			m++;
		}
		an = a->next;
		free(a);
	}
	return;
}

static inline void free_pattern_structure(globpattern *p) {
	uint8_t i,j;
	for (i=0 ; i<p->spelements ; i++) {
		for (j=0 ; j<p->sptab[i].atelements ; j++) {
			if (p->sptab[i].attab[j].type==ATOM_STRING) {
				free(p->sptab[i].attab[j].str);
			}
		}
		if (p->sptab[i].attab!=NULL) {
			free(p->sptab[i].attab);
		}
	}
	if (p->sptab!=NULL) {
		free(p->sptab);
	}
	p->spelements = 0;
	p->minleng = 0;
	p->sptab = NULL;
}

#ifdef GLOB_TEST
static inline void print_pattern_structure(globpattern *p) {
	char needsep;
	uint8_t i,j;

	printf("pattern flags: ");
	needsep = 0;
	if (p->flags & PATFLAG_FIRSTASTERISK) {
		printf("FIRSTASTERISK");
		needsep = 1;
	}
	if (p->flags & PATFLAG_LASTASTERISK) {
		if (needsep) {
			printf(",");
		}
		printf("LASTASTERISK");
		needsep = 1;
	}
	printf("\n");

	printf("subpatterns: %"PRIu8" ; minleng: %"PRIu32"\n",p->spelements,p->minleng);

	for (i=0 ; i<p->spelements ; i++) {
		printf("\tsubpattern %"PRIu8" ; leng: %"PRIu8" ; atoms: %"PRIu8"\n",i,p->sptab[i].leng,p->sptab[i].atelements);
		for (j=0 ; j<p->sptab[i].atelements ; j++) {
			printf("\t\tatom %"PRIu8": ",j);
			print_atom(p->sptab[i].attab+j);
		}
	}
}
#endif

void* glob_new(const uint8_t *globstr) {
	globpattern *p;
	p = malloc(sizeof(globpattern));
	passert(p);
	atoms_list_to_pattern_structure(p,pattern_to_atoms_list(globstr));
	return (void*)p;
}

void glob_free(void *glob) {
	globpattern *p = (globpattern*)glob;
	free_pattern_structure(p);
	free(p);
}

uint8_t glob_match(void *glob,const uint8_t *name,uint8_t nleng) {
	globpattern *p = (globpattern*)glob;
	return pattern_match(p,name,nleng);
}

void* glob_cache_get(uint8_t gnleng,const uint8_t *gname) {
	int i,j;
	j = 0;
	for (i=0 ; i<GLOB_CACHE_SIZE ; i++) {
		if (globtab[i].valid && globtab[i].gnleng==gnleng && (gnleng==0 || memcmp(globtab[i].gname,gname,gnleng)==0)) {
			globtab[i].mt = monotonic_seconds();
			return globtab[i].glob;
		}
		if (globtab[i].valid==0 || (globtab[i].mt < globtab[j].mt)) {
			j = i;
		}
	}
	if (globtab[j].valid) {
		glob_free(globtab[j].glob);
		free(globtab[j].gname);
	}
	globtab[j].gname = malloc(gnleng+1);
	passert(globtab[j].gname);
	memcpy(globtab[j].gname,gname,gnleng);
	globtab[j].gname[gnleng] = 0;
	globtab[j].glob = glob_new(globtab[j].gname);
	globtab[j].mt = monotonic_seconds();
	globtab[j].valid = 1;
	return globtab[j].glob;
}

void glob_cache_term(void) {
	int i;
	for (i=0 ; i<GLOB_CACHE_SIZE ; i++) {
		if (globtab[i].valid) {
			glob_free(globtab[i].glob);
			free(globtab[i].gname);
		}
		globtab[i].valid = 0;
	}
}

int glob_cache_init(void) {
	int i;
	for (i=0 ; i<GLOB_CACHE_SIZE ; i++) {
		globtab[i].glob = NULL;
		globtab[i].valid = 0;
		globtab[i].gnleng = 0;
		globtab[i].gname = NULL;
		globtab[i].mt = 0.0;
	}
	main_destruct_register(glob_cache_term);
	return 0;
}

#ifdef GLOB_TEST

#include <unistd.h>
#include <fnmatch.h>

void usage(const char *appname) {
	printf("usage: %s [-q] [-v] [-g GLOB] test_string1 test_string2 ...\n",appname);
}

int main(int argc,char *argv[]) {
	char *appname;
	uint8_t *globstr;
	void *vp,*cp;
	globpattern p;
	atom *a;
	uint8_t m1,m2,m3,m,n;
	int found;
	int ch;
	int verbose;
	int quiet;

	if (argc>0) {
		appname = strdup(argv[0]);
	} else {
		appname = strdup("(null)");
	}
	globstr = NULL;
	verbose = 0;
	quiet = 0;

	while ((ch = getopt(argc, argv, "g:vq")) != -1) {
		switch(ch) {
			case 'g':
				if (globstr!=NULL) {
					free(globstr);
				}
				globstr = (uint8_t*)strdup(optarg);
				break;
			case 'v':
				verbose = 1;
				break;
			case 'q':
				quiet = 1;
				break;
			case '?':
			default:
				usage(appname);
				return 1;
		}
	}

	if (globstr==NULL) {
		printf("GLOB not specified\n");
		usage(appname);
		return 1;
	}

	argc -= optind;
	argv += optind;

	glob_cache_init();

//	if (argc<1) {
//		usage(appname);
//		return 1;
//	}

// internal interface
	a = pattern_to_atoms_list(globstr);
	if (verbose) {
		print_atoms_list(a);
	}
	atoms_list_to_pattern_structure(&p,a);
	if (verbose) {
		print_pattern_structure(&p);
	}

// API
	vp = glob_new(globstr);
// API with cache
	cp = glob_cache_get(strlen((char*)globstr),globstr);

	found = 0;
	while (argc>0) {
		m1 = pattern_match(&p,(uint8_t*)(*argv),strlen(*argv))?1:0;
		m2 = glob_match(vp,(uint8_t*)(*argv),strlen(*argv))?1:0;
		m3 = glob_match(cp,(uint8_t*)(*argv),strlen(*argv))?1:0;

		if (m1!=m2) {
			printf("string '%s' \033[38;5;196minternal/API mismatch (intern vs API) !!!\033[m\n",*argv);
		}
		if (m2!=m3) {
			printf("string '%s' \033[38;5;196minternal/API mismatch (direct API vs cache API) !!!\033[m\n",*argv);
		}
		m = m3;
		n = (fnmatch((char*)globstr,*argv,0)==FNM_NOMATCH)?0:1;
		if (quiet) {
			if (n==m) {
				if (m) {
					if (found) {
						printf(" ");
					}
					printf("%s",*argv);
				}
			} else {
				printf("function mismatch");
			}
		} else {
			if (n==m) {
				printf("string '%s' \033[38;5;%um%s\033[m\n",*argv,m?30:26,m?"matches":"doesn't match");
			} else {
				printf("string '%s' \033[38;5;196mfunction mismatch:\033[m mfs:%u ; fnmatch:%u\n",*argv,m,n);
			}
		}
		if (m2) {
			found = 1;
		}
		argv++;
		argc--;
	}
	if (quiet && found) {
		printf("\n");
	}

	// cleanup
	if (globstr!=NULL) {
		free(globstr);
	}
	free_pattern_structure(&p);

	glob_free(vp);

	glob_cache_term();
	return found?0:1;
}
#endif
