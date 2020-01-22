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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

static char *argv_start;
static uint32_t argv_leng;

void processname_init(int argc,char *argv[]) {
	extern char **environ;
	char **argp;
	char *lastpos;
	int i,j;

	argv_start = NULL;
	argv_leng = 0;

	if (argc==0 || argv[0]==NULL) { // ???
		return;
	}

	// copy environment
	argp = environ;
	for (i=0 ; argp[i]!=NULL ; i++) {}
	environ = malloc((i+1) * sizeof(char*));
	if (environ==NULL) {
		environ = argp;
		return;
	}
	for (i=0 ; argp[i]!=NULL ; i++) {
		environ[i] = strdup(argp[i]);
		if (environ[i]==NULL) { // strdup failed ?
			for (j=0 ; j<i ; j++) {
				free(environ[i]);
			}
			free(environ);
			environ = argp;
			return;
		}
	}
	environ[i] = NULL;

	lastpos = NULL;
	for (i=0 ; i<argc ; i++) {
		if (lastpos==NULL || lastpos+1==argv[i]) {
			lastpos = argv[i] + strlen(argv[i]);
		}
	}
	for (i=0 ; argp[i]!=NULL ; i++) {
		if (lastpos+1==argp[i]) {
			lastpos = argp[i] + strlen(argp[i]);
		}
	}

	argv_start = argv[0];
	argv_leng = (lastpos - argv_start) - 1;
}

void processname_set(char *name) {
#ifdef HAVE_SETPROCTITLE
	setproctitle("%s",name);
#else
	uint32_t l;
	if (argv_leng>0) {
		l = strlen(name);
		if (l>=argv_leng) {
			l = argv_leng-1;
		}
		if (l>0) {
			memcpy(argv_start,name,l);
		}
//		if (l<argv_leng) { // always true - so ignore this condition
		memset(argv_start+l,0,argv_leng-l);
//		}
	}
#endif
}

#if 0
int main(int argc,char *argv[]) {
	processname_init(argc,argv);
	processname_set("Process with very funny name shown in ps");

	sleep(10);
	return 0;
}
#endif
