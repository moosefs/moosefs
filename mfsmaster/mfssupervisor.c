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
 * along with this program; if not, see
 * <https://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mastersupervisor.h"
#include "strerr.h"
#include "idstr.h"

void usage(const char *appname) {
	printf(
		"usage: %s [-hvxs] [-B local address] [-H master host name] [-P master supervising port]\n"
		"\n"
		"-h : print help and exit\n"
		"-v : print version number and exit\n"
		"-x : produce more verbose output\n"
		"-s : force metadata store\n"
		"-B ip : local address to use for master connections (default: *, i.e. default local address)\n"
		"-H host : use given host to find your master servers (default: " DEFAULT_MASTERNAME ")\n"
		"-P port : use given port to connect to your master servers (default: " DEFAULT_MASTER_CONTROL_PORT ")\n"
		,appname);
	exit(1);
}

int main(int argc,char **argv) {
	uint8_t debug=0;
	uint8_t store=0;
	const char *masterhost = NULL;
	const char *masterport = NULL;
	const char *bindhost = NULL;
	int ch;
	int res;

	strerr_init();

	while ((ch = getopt(argc,argv,"hvxsH:P:B:?")) != -1) {
		switch(ch) {
			case 'h':
				usage(argv[0]);
				res = 0;
				goto exit;
			case 'v':
				printf("version: %s\n",VERSSTR);
				res = 0;
				goto exit;
			case 'x':
				debug = 1;
				break;
			case 's':
				store = 1;
				break;
			case 'H':
				if (masterhost) {
					free((char*)masterhost);
				}
				masterhost = strdup(optarg);
				break;
			case 'P':
				if (masterport) {
					free((char*)masterport);
				}
				masterport = strdup(optarg);
				break;
			case 'B':
				if (bindhost) {
					free((char*)bindhost);
				}
				bindhost = strdup(optarg);
				break;
			default:
				usage(argv[0]);
				res = 1;
				goto exit;
		}
	}
//	argc -= optind;
//	argv += optind;


	if (masterhost == NULL) {
		masterhost = strdup(DEFAULT_MASTERNAME);
	}
	if (masterport == NULL) {
		masterport = strdup(DEFAULT_MASTER_CONTROL_PORT);
	}
	if (bindhost == NULL) {
		bindhost = strdup("*");
	}

	res = msupervisor_simple(masterhost,masterport,bindhost,debug,store);

exit:
	if (masterhost!=NULL) {
		free((char*)masterhost);
	}
	if (masterport!=NULL) {
		free((char*)masterport);
	}
	if (bindhost!=NULL) {
		free((char*)bindhost);
	}
	return res;
}
