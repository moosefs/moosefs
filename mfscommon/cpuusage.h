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

#ifndef _CPUUSAGE_H_
#define _CPUUSAGE_H_

#include <inttypes.h>

void cpu_init (void);

/* returns average number of nano seconds in every cpu ore spent in system / user space in every second */
/* return values should be between 0 and 1000000000*(number of cores) */
/* notice, that both zeros usually means that function was unable to obtain cpu usage */
void cpu_used (uint64_t *scpu,uint64_t *ucpu);

#endif
