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

#include <stdio.h>

#include "masterconn.h"

#define MODULE_OPTIONS_GETOPT ""
#define MODULE_OPTIONS_SWITCH
#define MODULE_OPTIONS_SYNOPSIS ""
#define MODULE_OPTIONS_DESC ""

/* Run Tab */
typedef int (*runfn)(void);
struct {
	runfn fn;
	char *name;
} RunTab[]={
	{masterconn_init,"connection with master"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{(runfn)0,"****"}
},RestoreRunTab[]={
	{(runfn)0,"****"}
};
