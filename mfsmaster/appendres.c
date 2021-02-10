#include <stdlib.h>
#include <inttypes.h>

#define APPENDRES_HASHSIZE 1024

typedef struct _appendreservation {
	uint32_t inode;
	uint64_t vlength;
//	double ttl;
	struct _appendreservation *next;
} appendreservation;

static appendreservation* appendreshash[APPENDRES_HASHSIZE];

uint64_t appendres_getvleng(uint32_t inode) {
	uint32_t hash;
	appendreservation *arptr;

	hash = inode % APPENDRES_HASHSIZE;
	for (arptr = appendreshash[hash] ; arptr!=NULL ; arptr=arptr->next) {
		if (arptr->inode==inode) {
			return arptr->vlength;
		}
	}
	return 0;
}

void appendres_setvleng(uint32_t inode,uint64_t vlength) {
	uint32_t hash;
	appendreservation *arptr;

	hash = inode % APPENDRES_HASHSIZE;
	for (arptr = appendreshash[hash] ; arptr!=NULL ; arptr=arptr->next) {
		if (arptr->inode==inode) {
			arptr->vlength = vlength;
			return;
		}
	}
	arptr = malloc(sizeof(appendreservation));
	arptr->inode = inode;
	arptr->vlength = vlength;
	arptr->next = appendreshash[hash];
	appendreshash[hash] = arptr;
}

void appendres_setrleng(uint32_t inode,uint64_t rlength) {
	uint32_t hash;
	appendreservation *arptr,**arpptr;

	hash = inode % APPENDRES_HASHSIZE;
	arpptr = appendreshash+hash;
	while ((arptr = *arpptr)!=NULL) {
		if (arptr->inode==inode) {
			if (rlength>=arptr->vlength) {
				*arpptr = arptr->next;
				free(arptr);
			}
			return;
		} else {
			arpptr = &(arptr->next);
		}
	}
}

void appendres_clear(uint32_t inode) {
	uint32_t hash;
	appendreservation *arptr,**arpptr;

	hash = inode % APPENDRES_HASHSIZE;
	arpptr = appendreshash+hash;
	while ((arptr = *arpptr)!=NULL) {
		if (arptr->inode==inode) {
			*arpptr = arptr->next;
			free(arptr);
			return;
		} else {
			arpptr = &(arptr->next);
		}
	}
}

void appendres_cleanall(void) {
	uint32_t hash;
	appendreservation *arptr,*arnptr;

	for (hash=0 ; hash<APPENDRES_HASHSIZE ; hash++) {
		for (arptr = appendreshash[hash] ; arptr!=NULL ; arptr=arnptr) {
			arnptr = arptr->next;
			free(arptr);
		}
		appendreshash[hash] = NULL;
	}
}

void appendres_init(void) {
	uint32_t hash;

	for (hash=0 ; hash<APPENDRES_HASHSIZE ; hash++) {
		appendreshash[hash] = NULL;
	}
}
