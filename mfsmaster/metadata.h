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

#ifndef _METADATA_H_
#define _METADATA_H_

#include <stdio.h>
#include <inttypes.h>

uint64_t meta_version_inc(void);
uint64_t meta_version(void);
void meta_cleanup(void);
void meta_setignoreflag(void);
void meta_allowautorestore(void);
void meta_emptystart(void);
void meta_incverboselevel(void);
void meta_do_store_metadata(void);

uint64_t meta_get_id(void);
void meta_set_id(uint64_t newmetaid);

uint8_t meta_mr_setmetaid(uint64_t newmetaid);

void meta_info(uint32_t *lsstore,uint32_t *lstime,uint8_t *lsstat,uint64_t *lsmetavers,uint32_t *lschecksum);
int meta_init(void);

int meta_restore(void);

void meta_download_status(uint8_t status);

uint64_t meta_chlog_keep_version(void);

#endif
