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

#ifndef _TOOLS_PACKETSERIALISER_H_
#define _TOOLS_PACKETSERIALISER_H_

int32_t ps_bytesleft(void);
void ps_comminit(void);
int ps_status(void);

int32_t ps_send_and_receive(uint32_t command, uint32_t reqansid, const char *error_prefix);

void ps_put8(uint8_t val);
void ps_put16(uint16_t val);
void ps_put24(uint32_t val);
void ps_put32(uint32_t val);
void ps_put40(uint64_t val);
void ps_put48(uint64_t val);
void ps_put56(uint64_t val);
void ps_put64(uint64_t val);
void ps_putbytes(const uint8_t *src, uint32_t leng);

void ps_dummyget(uint16_t skip);
uint8_t ps_get8(void);
uint16_t ps_get16(void);
uint32_t ps_get24(void);
uint32_t ps_get32(void);
uint64_t ps_get40(void);
uint64_t ps_get48(void);
uint64_t ps_get56(void);
uint64_t ps_get64(void);
void ps_getbytes(uint8_t *dest, uint32_t leng);

uint8_t ps_putstr(const char *str);
uint8_t ps_getstr(char str[256]);

#endif
