/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
#include <time.h>
#include <inttypes.h>

#include "cuckoohash.h"
// #include "hashfn.h"
#include "random.h"
#include "massert.h"

/* treap */

typedef struct treap_node {
	hash_key_t key;
	void *val;
	uint32_t pri;
	struct treap_node *left,*right;
} treap_node;

static inline void tree_free(treap_node *node) {
	if (node!=NULL) {
		tree_free(node->left);
		tree_free(node->right);
		free(node);
	}
}

static inline void tree_rotate_with_left_child(treap_node **node) {
	treap_node *tmp,*n = (*node);
	tmp = n->left;
	n->left = tmp->right;
	tmp->right = n;
	*node = tmp;
}

static inline void tree_rotate_with_right_child(treap_node **node) {
	treap_node *tmp,*n = (*node);
	tmp = n->right;
	n->right = tmp->left;
	tmp->left = n;
	*node = tmp;
}

static inline uint8_t tree_insert(treap_node **node,hash_key_t key,void *val) {
	treap_node *n = (*node);
	if (n==NULL) {
		n = malloc(sizeof(treap_node));
		passert(n);
		n->left = NULL;
		n->right = NULL;
		n->key = key;
		n->val = val;
		n->pri = rndu32();
		*node = n;
		return 1;
	} else {
		if (key < n->key) {
			if (tree_insert(&(n->left),key,val)) {
				if (n->left->pri < n->pri) {
					tree_rotate_with_left_child(node);
				}
				return 1;
			}
		} else if (key > n->key) {
			if (tree_insert(&(n->right),key,val)) {
				if (n->right->pri < n->pri) {
					tree_rotate_with_right_child(node);
				}
				return 1;
			}
		}
	}
	return 0;
}

static inline uint8_t tree_delete(treap_node **node,hash_key_t key) {
	treap_node *n,*c;
	while ((n=*node)) {
		if (key < n->key) {
			node = &(n->left);
		} else if (key > n->key) {
			node = &(n->right);
		} else {
			if (n->left==NULL && n->right==NULL) {
				*node = NULL;
				free(n);
			} else if (n->left==NULL) {
				*node = n->right;
				free(n);
			} else if (n->right==NULL) {
				*node = n->left;
				free(n);
			} else {
				if (rndu8()&1) {
					node = &(n->left);
					while ((c=*node) && c->right!=NULL) {
						node = &(c->right);
					}
					n->key = c->key;
					n->val = c->val;
					*node = c->left;
					free(c);
				} else {
					node = &(n->right);
					while ((c=*node) && c->left!=NULL) {
						node = &(c->left);
					}
					n->key = c->key;
					n->val = c->val;
					*node = c->right;
					free(c);
				}
			}
			return 1;
		}
	}
	return 0;
}

void* tree_find(treap_node *n,hash_key_t key) {
	while (n) {
		if (key < n->key) {
			n = n->left;
		} else if (key > n->key) {
			n = n->right;
		} else {
			return n->val;
		}
	}
	return NULL;
}




/* cuckoo hash */

#define HASHFN1(x,mask) ((x)&(mask))
#define HASHFN2(x,mask) ((((x)*167)>>8)&(mask))
#define HASHTAB_MOVEFACTOR 3

#define HASHTAB_LOBITS 20
#define HASHTAB_HISIZE (0x80000000>>(HASHTAB_LOBITS))
#define HASHTAB_LOSIZE (1<<HASHTAB_LOBITS)
#define HASHTAB_MASK (HASHTAB_LOSIZE-1)

#define HASH_BUCKET_SIZE 6

typedef struct _hentry {
	uint8_t e;
	hash_key_t k[HASH_BUCKET_SIZE];
	void* v[HASH_BUCKET_SIZE];
} hentry;

typedef struct _htab {
	hentry* *hashtab;
	treap_node *groot;
	uint8_t rehashgarbage;
	uint32_t helements,telements;
	uint32_t size;
	uint32_t mask;
	uint32_t rehashpos;
} htab;

static inline int hash_cuckoo(htab *ht,hentry *he,hash_key_t x,void *v) {
	hentry *cuckoohe1,*cuckoohe2,*cuckoohe;
	uint32_t i,cuckoohash;
	for (i=0 ; i<he->e ; i++) {
		cuckoohash = HASHFN1(he->k[i],ht->mask);
		if (cuckoohash >= ht->rehashpos) {
			cuckoohash = cuckoohash & (ht->mask>>1);
		}
		cuckoohe1 = ht->hashtab[cuckoohash>>HASHTAB_LOBITS] + (cuckoohash&HASHTAB_MASK);
		cuckoohash = HASHFN2(he->k[i],ht->mask);
		if (cuckoohash >= ht->rehashpos) {
			cuckoohash = cuckoohash & (ht->mask>>1);
		}
		cuckoohe2 = ht->hashtab[cuckoohash>>HASHTAB_LOBITS] + (cuckoohash&HASHTAB_MASK);
		if (cuckoohe1!=he) {
			cuckoohe = cuckoohe1;
		} else if (cuckoohe2!=he) {
			cuckoohe = cuckoohe2;
		} else {
			cuckoohe = NULL;
		}
		if (cuckoohe!=NULL) {
			if (cuckoohe->e<HASH_BUCKET_SIZE) {
				cuckoohe->k[cuckoohe->e] = he->k[i];
				cuckoohe->v[cuckoohe->e] = he->v[i];
				cuckoohe->e++;
				he->k[i] = x;
				he->v[i] = v;
				ht->helements++;
				return 1;
			}
		}
	}
	return 0;
}

void chash_add(void *h,hash_key_t x,void *v);

static inline void tree_rebuild(htab *ht,treap_node *node) {
	if (node!=NULL) {
		tree_rebuild(ht,node->left);
		tree_rebuild(ht,node->right);
		chash_add(ht,node->key,node->val);
		free(node);
	}
}

static inline void hash_garbage_to_hash(htab *ht) {
	treap_node *nroot;
	nroot = ht->groot;
	ht->telements = 0;
	ht->groot = NULL;
	tree_rebuild(ht,nroot);
}

static inline void hash_rehash_job(htab *ht) {
	uint32_t i,lhash,hash1,hash2;
	hentry *he,*hen;
	if (ht->rehashpos < ht->size) {
		for (i=0 ; i<HASHTAB_MOVEFACTOR && ht->rehashpos < ht->size ; i++) {
			lhash = ht->rehashpos&(ht->mask>>1);
			he = ht->hashtab[lhash>>HASHTAB_LOBITS] + (lhash&HASHTAB_MASK);
			hen = ht->hashtab[ht->rehashpos>>HASHTAB_LOBITS] + (ht->rehashpos&HASHTAB_MASK);
			hen->e = 0;
			i = 0;
			while (i<he->e) {
				hash1 = HASHFN1(he->k[i],ht->mask);
				hash2 = HASHFN2(he->k[i],ht->mask);
				if (hash1==ht->rehashpos || hash2==ht->rehashpos) {
					hen->k[hen->e] = he->k[i];
					hen->v[hen->e] = he->v[i];
					hen->e++;
					he->e--;
					if (i<he->e) {
						he->k[i] = he->k[he->e];
						he->v[i] = he->v[he->e];
					}
				} else {
					i++;
				}
			}
			ht->rehashpos++;
		}
	} else if (ht->rehashgarbage) {
		hash_garbage_to_hash(ht);
		ht->rehashgarbage = 0;
	} else if (ht->helements * 3 > ht->size * 2 * HASH_BUCKET_SIZE) {
//		printf("rehash\n");
		ht->size *= 2;
		ht->mask = ht->size - 1;
		for (i=ht->rehashpos>>HASHTAB_LOBITS ; i<ht->size>>HASHTAB_LOBITS ; i++) {
			ht->hashtab[i] = malloc(sizeof(hentry)*HASHTAB_LOSIZE);
			passert(ht->hashtab[i]);
		}
		ht->rehashgarbage = 1;
	}
}

void* chash_new() {
	htab *ht;

	ht = malloc(sizeof(htab));
	passert(ht);
	ht->mask = HASHTAB_MASK;
	ht->size = HASHTAB_LOSIZE;
	ht->helements = 0;
	ht->telements = 0;
	ht->rehashgarbage = 0;
	ht->rehashpos = ht->size;
	ht->hashtab = malloc(sizeof(hentry*)*HASHTAB_HISIZE);
	passert(ht->hashtab);
	ht->hashtab[0] = malloc(sizeof(hentry)*HASHTAB_LOSIZE);
	passert(ht->hashtab[0]);
	memset(ht->hashtab[0],0,sizeof(hentry)*HASHTAB_LOSIZE);
	ht->groot = NULL;
	return ht;
}

/* delete all elements, but leave structure initialized */
void chash_erase(void *h) {
	htab *ht = (htab*)h;
	uint32_t i;

	tree_free(ht->groot);
	ht->groot = NULL;
	for (i=1 ; i<ht->size>>HASHTAB_LOBITS ; i++) {
		if (ht->hashtab[i]!=NULL) {
			free(ht->hashtab[i]);
			ht->hashtab[i]=NULL;
		}
	}
	memset(ht->hashtab[0],0,sizeof(hentry)*HASHTAB_LOSIZE);
	ht->mask = HASHTAB_MASK;
	ht->size = HASHTAB_LOSIZE;
	ht->helements = 0;
	ht->telements = 0;
	ht->rehashgarbage = 0;
	ht->rehashpos = ht->size;
}

/* delete elemnts and structure */
void chash_free(void *h) {
	htab *ht = (htab*)h;
	uint32_t i;

	tree_free(ht->groot);
	for (i=0 ; i<ht->size>>HASHTAB_LOBITS ; i++) {
		if (ht->hashtab[i]!=NULL) {
			free(ht->hashtab[i]);
		}
	}
	free(ht);
}

void* chash_find(void *h,hash_key_t x) {
	htab *ht = (htab*)h;
	hentry *he;
	uint32_t i,hash;

	hash = HASHFN1(x,ht->mask);
	if (hash >= ht->rehashpos) {
		hash = hash & (ht->mask>>1);
	}
	he = ht->hashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	for (i=0 ; i<he->e ; i++) {
		if (he->k[i]==x) {
			return he->v[i];
		}
	}

	hash = HASHFN2(x,ht->mask);
	if (hash >= ht->rehashpos) {
		hash = hash & (ht->mask>>1);
	}
	he = ht->hashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	for (i=0 ; i<he->e ; i++) {
		if (he->k[i]==x) {
			return he->v[i];
		}
	}

	// search "garbage"
	return tree_find(ht->groot,x);
}

void chash_delete(void *h,hash_key_t x) {
	htab *ht = (htab*)h;
	hentry *he;
	uint32_t i,hash;

	hash = HASHFN1(x,ht->mask);
	if (hash >= ht->rehashpos) {
		hash = hash & (ht->mask>>1);
	}
	he = ht->hashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	for (i=0 ; i<he->e ; i++) {
		if (he->k[i]==x) {
			he->e--;
			if (i<he->e) {
				he->k[i] = he->k[he->e];
				he->v[i] = he->v[he->e];
			}
			ht->helements--;
			return;
		}
	}

	hash = HASHFN2(x,ht->mask);
	if (hash >= ht->rehashpos) {
		hash = hash & (ht->mask>>1);
	}
	he = ht->hashtab[hash>>HASHTAB_LOBITS] + (hash&HASHTAB_MASK);
	for (i=0 ; i<he->e ; i++) {
		if (he->k[i]==x) {
			he->e--;
			if (i<he->e) {
				he->k[i] = he->k[he->e];
				he->v[i] = he->v[he->e];
			}
			ht->helements--;
			return;
		}
	}

	// delete from "garbage"
	ht->telements -= tree_delete(&(ht->groot),x);
}

void chash_add(void *h,hash_key_t x,void *v) {
	htab *ht = (htab*)h;
	hentry *he1,*he2;
	uint32_t i,hash1,hash2;

	hash_rehash_job(ht);

	hash1 = HASHFN1(x,ht->mask);
	if (hash1 >= ht->rehashpos) {
		hash1 = hash1 & (ht->mask>>1);
	}
	hash2 = HASHFN2(x,ht->mask);
	if (hash2 >= ht->rehashpos) {
		hash2 = hash2 & (ht->mask>>1);
	}
	he1 = ht->hashtab[hash1>>HASHTAB_LOBITS] + (hash1&HASHTAB_MASK);
	he2 = ht->hashtab[hash2>>HASHTAB_LOBITS] + (hash2&HASHTAB_MASK);
	for (i=0 ; i<he1->e ; i++) {
		if (he1->k[i]==x) {
			return;
		}
	}
	for (i=0 ; i<he2->e ; i++) {
		if (he2->k[i]==x) {
			return;
		}
	}
	if (tree_find(ht->groot,x)!=NULL) {
		return;
	}
	if (he1->e==HASH_BUCKET_SIZE && he2->e==HASH_BUCKET_SIZE) { // cuckoo
		if (hash_cuckoo(ht,he1,x,v)) {
			return;
		}
		if (hash_cuckoo(ht,he2,x,v)) {
			return;
		}
		ht->telements += tree_insert(&(ht->groot),x,v);
		return;
	}
	if (he1->e>he2->e) {
		he2->k[he2->e] = x;
		he2->v[he2->e] = v;
		he2->e++;
	} else {
		he1->k[he1->e] = x;
		he1->v[he1->e] = v;
		he1->e++;
	}
	ht->helements++;
}

uint32_t chash_get_elemcount(void *h) {
	htab *ht = (htab*)h;
	return ht->helements + ht->telements;
}

uint32_t chash_get_size(void *h) {
	htab *ht = (htab*)h;
//	printf("ht->size: %"PRIu32" ; ht->helements: %"PRIu32" ; ht->telements: %"PRIu32"\n",ht->size,ht->helements,ht->telements);
	return (ht->size * sizeof(hentry)) + (ht->telements * sizeof(treap_node));
}
