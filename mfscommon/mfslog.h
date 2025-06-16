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
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifndef _MFSLOG_H_
#define _MFSLOG_H_

// MFSLOG_xxx are defined here:
#include "MFSCommunication.h"

// mode
// syslog only
#define MFSLOG_SYSLOG 0
// syslog only with error string at the end
#define MFSLOG_ERRNO_SYSLOG 1
// syslog and stderr
#define MFSLOG_SYSLOG_STDERR 2
// syslog and stderr with error string at the end
#define MFSLOG_ERRNO_SYSLOG_STDERR 3

int mfs_log_str_to_pri(const char *pristr);

void mfs_file_log(const char *file,int line,const char *func,int bt,const char *fmt,...);
#define mfs_dbg(...) mfs_file_log(__FILE__,__LINE__,__func__,0,__VA_ARGS__)
#define mfs_dbg_bt(...) mfs_file_log(__FILE__,__LINE__,__func__,1,__VA_ARGS__)

void mfs_log(int mode,int priority,const char *fmt,...);

void mfs_log_set_min_level(int minlevel);
void mfs_log_set_elevate_to(int elevateto);
void mfs_log_set_sink_function(void(*s)(const char *str));

void mfs_log_detach_stderr(void);
void mfs_log_detach_syslog(void);
void mfs_log_term(void);
int mfs_log_init(const char *ident,int daemon);

#endif
