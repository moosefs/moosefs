/*
 * Copyright (C) 2021 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#include "bgjobs.h"
#include "random.h"
#include "hddspacemgr.h"
#include "masterconn.h"
#include "csserv.h"
#include "mainserv.h"
#include "chartsdata.h"

#define MODULE_OPTIONS_GETOPT ""
#define MODULE_OPTIONS_SWITCH
#define MODULE_OPTIONS_SYNOPIS ""
#define MODULE_OPTIONS_DESC ""

/* Run Tab */
typedef int (*runfn)(void);
struct {
	runfn fn;
	char *name;
} RunTab[]={
	{rnd_init,"random generator"},
	{hdd_init,"hdd space manager"},
	{mainserv_init,"main server threads"},
	{job_init,"jobs manager"},
	{csserv_init,"main server acceptor"},	/* it has to be before "masterconn" */
	{masterconn_init,"master connection module"},
	{chartsdata_init,"charts module"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{hdd_late_init,"hdd space manager - threads"},
	{(runfn)0,"****"}
},RestoreRunTab[]={
	{hdd_restore,"hdd space restore"},
	{(runfn)0,"****"}
};
