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

#ifndef _TOOLS_TRASHADMIN_H_
#define _TOOLS_TRASHADMIN_H_

#include "tools_main.h"

void trashpurgehlp(void);
void trashpurgeexe(cv_t *cv);
void trashrecoverhlp(void);
void trashrecoverexe(cv_t *cv);
void trashlisthlp(void);
void trashlistexe(cv_t *cv);
void sustainedhlp(void);
void sustainedexe(cv_t *cv);

#endif // _TOOLS_TRASHFILES_H_
