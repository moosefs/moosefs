/*
 * Copyright (C) 2024 Jakub Kruszona-Zawadzki, Saglabs SA
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

#ifndef _TOOLS_MAIN_H_
#define _TOOLS_MAIN_H_

#include <sys/types.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>

#include "sockets.h"
#include "MFSCommunication.h"

#include "tools_packetserializer.h"

#define tcpread(s,b,l) tcptoread(s,b,l,10000,30000)
#define tcpwrite(s,b,l) tcptowrite(s,b,l,10000,30000)

typedef struct mfscomm_s mfstcommand_t;

typedef struct commonvar_s {
	char **argv;
	int argc;
	mfstcommand_t *comm;
	mfstcommand_t *commstab;
	int status;
} cv_t;

//flags table of commands:
#define	HIDE_F		(1<<0)
#define	OBSOLETE_F	(1<<1)
#define	NEWGRP_F	(1<<2)
#define	TWINCOMM_F	(1<<3)
#define	ADMCOMM_F	(1<<4)
//#define	REQPARAM_F	(1<<5)

struct mfscomm_s {
	char    *name;
	uint8_t	minmasterver;               //required master version
	void    (*exefun)(cv_t * const cv);
	void    (*hlpfun)(void);
	uint8_t flags;
};

uint32_t getmasterversion(void);
uint32_t setmasterversion(uint32_t val);

int master_socket(void);
int master_reconnect(void);
int open_master_conn(const char *name, uint32_t *inode, mode_t *mode, uint64_t *leng, uint8_t needsamedev, uint8_t needrwfs);
//void close_master_conn(int err);
void reset_master(void);

void dirname_inplace(char *path);

void print_lines(const char** line);
void checkminarglist(cv_t *cv, int n);
void usagesimple(cv_t *cv);
void usagefull(cv_t *cv);
void exeviatable(cv_t *cv, char *);
mfstcommand_t *findcommbyname(const char *str, mfstcommand_t *commstab);

void listcommands(mfstcommand_t *comm);

#endif // _TOOLS_MAIN_H_
