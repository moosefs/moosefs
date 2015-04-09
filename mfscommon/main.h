/*
 * Copyright (C) 2015 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifndef _MAIN_H_
#define _MAIN_H_

#if defined(_THREAD_SAFE) || defined(_REENTRANT) || defined(_USE_PTHREADS)
#  define USE_PTHREADS 1
#endif

#include <poll.h>
#include <sys/types.h>
#include <inttypes.h>
#ifdef USE_PTHREADS
#include <pthread.h>
#endif

void main_destruct_register (void (*fun)(void));
void main_canexit_register (int (*fun)(void));
void main_wantexit_register (void (*fun)(void));
void main_reload_register (void (*fun)(void));
void main_info_register (void (*fun)(void));
void main_chld_register (pid_t pid,void (*fun)(int));
void main_keepalive_register (void (*fun)(void));
void main_poll_register (void (*desc)(struct pollfd *,uint32_t *),void (*serve)(struct pollfd *));
void main_eachloop_register (void (*fun)(void));
void* main_msectime_register (uint32_t mseconds,uint32_t offset,void (*fun)(void));
int main_msectime_change(void* x,uint32_t mseconds,uint32_t offset);
void* main_time_register (uint32_t seconds,uint32_t offset,void (*fun)(void));
int main_time_change(void *x,uint32_t seconds,uint32_t offset);
void main_exit(void);
uint32_t main_time(void);
void main_keep_alive(void);
#ifdef USE_PTHREADS
int main_thread_create(pthread_t *th,const pthread_attr_t *attr,void *(*fn)(void *),void *arg);
int main_minthread_create(pthread_t *th,uint8_t detached,void *(*fn)(void *),void *arg);
#endif

#endif
