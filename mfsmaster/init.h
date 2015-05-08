/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>

#include "topology.h"
#include "exports.h"
#include "datacachemgr.h"
#include "matomlserv.h"
#include "matocsserv.h"
#include "matoclserv.h"
#include "metadata.h"
#include "random.h"
#include "changelog.h"
#include "chartsdata.h"
#include "missinglog.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " VERSSTR ", written by Jakub Kruszona-Zawadzki";

#define MODULE_OPTIONS_GETOPT "iaxe"
#define MODULE_OPTIONS_SWITCH \
	case 'i': \
		meta_setignoreflag(); \
		break; \
	case 'a': \
		meta_allowautorestore(); \
		break; \
	case 'e': \
		meta_emptystart(); \
		break; \
	case 'x': \
		meta_incverboselevel(); \
		break;
#define MODULE_OPTIONS_SYNOPIS "[-i] [-a] [-e] [-x [-x]] "
#define MODULE_OPTIONS_DESC "-i : ignore some metadata structure errors (attach orphans to root, ignore names without inode, etc.)\n-a : automatically restore metadata from change logs\n-e : start without metadata (download all from leader)\n-x : produce more verbose output\n-xx : even more verbose output\n"

/* Run Tab */
typedef int (*runfn)(void);
struct {
	runfn fn;
	char *name;
} RunTab[]={
	{changelog_init,"change log"},
	{rnd_init,"random generator"},
	{missing_log_init,"missing chunks/files log"}, // has to be before 'fs_init'
	{dcm_init,"data cache manager"}, // has to be before 'fs_init' and 'matoclserv_init'
	{exports_init,"exports manager"},
	{topology_init,"net topology module"},
	{meta_init,"metadata manager"},
	{chartsdata_init,"charts module"},
	{matomlserv_init,"communication with metalogger"},
	{matocsserv_init,"communication with chunkserver"},
	{matoclserv_init,"communication with clients"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{(runfn)0,"****"}
};
