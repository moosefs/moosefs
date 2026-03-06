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

#ifndef _OPLOG_H_
#define _OPLOG_H_

#include "fusecommon.h"

#include <inttypes.h>

#if defined(__printflike)
#	define PRINTF_LIKE(fmt, args) __printflike(fmt, args)
#elif defined(__GNUC__) || defined(__clang__)
#	define PRINTF_LIKE(fmt, args) __attribute__((format(printf, fmt, args)))
#else
#	define PRINTF_LIKE(fmt, args)
#endif

void oplog_printf(const struct fuse_ctx *ctx,const char *format,...) PRINTF_LIKE(2, 3);
void oplog_msg(const char *format,...) PRINTF_LIKE(1, 2);
unsigned long oplog_newhandle(int hflag);
void oplog_releasehandle(unsigned long fh);
void oplog_getdata(unsigned long fh,uint8_t **buff,uint32_t *leng,uint32_t maxleng);
void oplog_releasedata(unsigned long fh);

#endif
