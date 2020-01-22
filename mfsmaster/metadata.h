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

#ifndef _METADATA_H_
#define _METADATA_H_

#include <stdio.h>
#include <inttypes.h>

uint64_t meta_version_inc(void);
uint64_t meta_version(void);
void meta_cleanup(void);
void meta_setignoreflag(void);
void meta_allowautorestore(void);
void meta_incverboselevel(void);
void meta_sendall(int socket);
int meta_downloadall(int socket);

void meta_text_dump(FILE *fd);

uint64_t meta_get_id(void);
void meta_set_id(uint64_t newmetaid);

uint8_t meta_mr_setmetaid(uint64_t newmetaid);

void meta_info(uint32_t *lsstore,uint32_t *lstime,uint8_t *lsstat);
int meta_init(void);

int meta_restore(void);


#endif
