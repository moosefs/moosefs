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

#ifndef _PCQUEUE_H_
#define _PCQUEUE_H_

#include <inttypes.h>

void* queue_new(uint32_t size);
void queue_delete(void *que);
void queue_close(void *que);
int queue_isempty(void *que);
uint32_t queue_elements(void *que);
int queue_isfull(void *que);
uint32_t queue_sizeleft(void *que);
void queue_put(void *que,uint32_t id,uint32_t op,uint8_t *data,uint32_t leng);
int queue_tryput(void *que,uint32_t id,uint32_t op,uint8_t *data,uint32_t leng);
void queue_get(void *que,uint32_t *id,uint32_t *op,uint8_t **data,uint32_t *leng);
int queue_tryget(void *que,uint32_t *id,uint32_t *op,uint8_t **data,uint32_t *leng);

#endif
