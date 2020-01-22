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

#ifndef _SQUEUE_H_
#define _SQUEUE_H_

#include <inttypes.h>

void* squeue_new(uint32_t length);
void squeue_delete(void *que);
void squeue_close(void *que);
int squeue_isempty(void *que);
uint32_t squeue_elements(void *que);
int squeue_isfull(void *que);
uint32_t squeue_sizeleft(void *que);
void squeue_put(void *que,void *element);
int squeue_tryput(void *que,void *element);
void squeue_get(void *que,void **element);
int squeue_tryget(void *que,void **element);

#endif
