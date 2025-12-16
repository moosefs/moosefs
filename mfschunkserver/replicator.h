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

#ifndef _REPLICATOR_H_
#define _REPLICATOR_H_

#include <inttypes.h>
#include "MFSCommunication.h"

typedef enum {SIMPLE,SPLIT,RECOVER,JOIN} repmodeenum;

void replicator_stats(uint64_t *bin,uint64_t *bout,uint32_t *repl);
uint8_t replicate(repmodeenum rmode,uint64_t chunkid,uint32_t version,uint8_t partno,uint8_t parts,const uint32_t srcip[MAX_EC_PARTS],const uint16_t srcport[MAX_EC_PARTS],const uint64_t srcchunkid[MAX_EC_PARTS]);

#endif
