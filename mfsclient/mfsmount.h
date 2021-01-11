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

#ifndef _MFSMOUNT_H_
#define _MFSMOUNT_H_

#include <inttypes.h>

void main_setparams(uint8_t sesflags,uint16_t umaskval,uint32_t maprootuid,uint32_t maprootgid,uint32_t mapalluid,uint32_t mapallgid,uint8_t mingoal,uint8_t maxgoal,uint32_t mintrashtime,uint32_t maxtrashtime,uint32_t disables);
uint32_t main_snprint_parameters(char *buff,uint32_t size);
uint32_t main_kernelversion(void);
#define MAKE_KERNEL_VERSION(x,y) ((x)*0x10000+(y))

#endif
