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

#ifndef _MAIN_H_
#define _MAIN_H_

#include <poll.h>
#include <sys/types.h>
#include <inttypes.h>

// #define LOOP_DEBUG 1
// #define LOOP_TRIGGER 0.001

#ifdef LOOP_DEBUG

#include "clocks.h"

#define LOOP_VARS double start,end
#define LOOP_START start = monotonic_seconds()
#define LOOP_END(name) { \
	end = monotonic_seconds(); \
	if (end-start > LOOP_TRIGGER) { \
		syslog(LOG_WARNING,"long call detected: %s : %.2lfms",name,(end-start)*1000.0); \
	} \
}
#else
#define LOOP_VARS
#define LOOP_START
#define LOOP_END(name)
#endif

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

#define main_destruct_register(x) main_destruct_register_fname(x,STR(x))
#define main_canexit_register(x) main_canexit_register_fname(x,STR(x))
#define main_wantexit_register(x) main_wantexit_register_fname(x,STR(x))
#define main_reload_register(x) main_reload_register_fname(x,STR(x))
#define main_info_register(x) main_info_register_fname(x,STR(x))
#define main_chld_register(p,x) main_chld_register_fname(p,x,STR(x))
#define main_keepalive_register(x) main_keepalive_register_fname(x,STR(x))
#define main_poll_register(x,y) main_poll_register_fname(x,y,STR(x),STR(y))
#define main_eachloop_register(x) main_eachloop_register_fname(x,STR(x))
#define main_msectime_register(m,o,x) main_msectime_register_fname(m,o,x,STR(x))
#define main_time_register(s,o,x) main_time_register_fname(s,o,x,STR(x))

void main_destruct_register_fname (void (*fun)(void),const char *fname);
void main_canexit_register_fname (int (*fun)(void),const char *fname);
void main_wantexit_register_fname (void (*fun)(void),const char *fname);
void main_reload_register_fname (void (*fun)(void),const char *fname);
void main_info_register_fname (void (*fun)(void),const char *fname);
void main_chld_register_fname (pid_t pid,void (*fun)(int),const char *fname);
void main_keepalive_register_fname (void (*fun)(void),const char *fname);
void main_poll_register_fname (void (*desc)(struct pollfd *,uint32_t *),void (*serve)(struct pollfd *),const char *dname,const char *sname);
void main_eachloop_register_fname (void (*fun)(void),const char *fname);
void* main_msectime_register_fname (uint32_t mseconds,uint32_t offset,void (*fun)(void),const char *fname);
void* main_time_register_fname (uint32_t seconds,uint32_t offset,void (*fun)(void),const char *fname);

int main_msectime_change(void* x,uint32_t mseconds,uint32_t offset);
int main_time_change(void *x,uint32_t seconds,uint32_t offset);
void main_exit(void);
uint32_t main_time_refresh(void);
uint32_t main_time(void);
uint64_t main_utime(void);
void main_keep_alive(void);

#endif
