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

#ifndef _LABELPARSER_H_
#define _LABELPARSER_H_

#include <inttypes.h>

#include "MFSCommunication.h"

#define LABELS_BUFF_SIZE ((((26+1)*MASKORGROUP)+5)*9)

/* grammar productions:
 *	A -> [1-9] E ',' A | [1-9] E ';' A
 *	E -> '*' | S
 *	S -> S '+' M | S '|' M | S '||' M | M
 *	M -> M '*' L | M '&' L | M '&&' L | M L | L
 *	L -> 'a' .. 'z' | 'A' .. 'Z' | '(' S ')' | '[' S ']'
 */

char* make_label_expr(char *strbuff,uint8_t labelscnt,uint32_t labelmasks[9][MASKORGROUP]);
int parse_label_expr(char *exprstr,uint8_t *labelscnt,uint32_t labelmasks[9][MASKORGROUP]);

#endif
