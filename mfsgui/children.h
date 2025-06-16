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

#ifndef _CHILDREN_H_
#define _CHILDREN_H_

#include <signal.h>

#define CHLDHASHSIZE 1024

typedef struct _chld {
	pid_t pid;
	struct _chld *next;
} chld;

static chld *chldhash[CHLDHASHSIZE];

static void children_add(pid_t p) {
	int h;
	chld *c;

	h = p % CHLDHASHSIZE;
	c = malloc(sizeof(chld));
	c->pid = p;
	c->next = chldhash[h];
	chldhash[h] = c;
}

static void children_remove(pid_t p) {
	int h;
	chld *c,**cp;

	h = p % CHLDHASHSIZE;
	cp = &(chldhash[h]);
	while ((c=*cp)!=NULL) {
		if (c->pid == p) {
			*cp = c->next;
			free(c);
		} else {
			cp = &(c->next);
		}
	}
}

static void children_kill(void) {
	int h;
	chld *c,*cn;

	for (h=0 ; h<CHLDHASHSIZE ; h++) {
		c = chldhash[h];
		while (c) {
			cn = c->next;
			kill(c->pid,SIGKILL);
			free(c);
			c = cn;
		}
		chldhash[h] = NULL;
	}
}

static void children_init(void) {
	int h;
	for (h=0 ; h<CHLDHASHSIZE ; h++) {
		chldhash[h] = NULL;
	}
}

#endif
