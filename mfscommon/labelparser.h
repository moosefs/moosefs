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

#ifndef _LABELPARSER_H_
#define _LABELPARSER_H_

#include <inttypes.h>

#include "MFSCommunication.h"

#define LABELS_BUFF_SIZE (((SCLASS_EXPR_MAX_SIZE*2+1)*MAXLABELSCNT)+1+26)

typedef struct _parser_data {
	uint32_t uniqmask;
	uint8_t labels_mode;
	uint8_t ec_data_chksum_parts; // upper 4 bits - number of data parts (accepted values: 0,4,8 ; 0 - use default) ; lower 4 bits - number of checksums (and therefore redundancy level)
	uint8_t labelscnt;
	uint8_t labelexpr[MAXLABELSCNT][SCLASS_EXPR_MAX_SIZE];
} parser_data;

/* grammar productions:
 *	T -> C | C N | '-'
 *
 *	N -> '/' U | ':' D | '/' U ':' D | 
 *
 *	C -> A | ( '@' | '=' ) [ ('4' | '8') '+' ] '1' ... '9' [ ( ',' | ';' ) E [ ( ',' | ';' ) E ]]
 *
 *	A -> [ '1' ... '9' ] E ',' A | [ '1' ... '9' ] E ';' A
 *	E -> '*' | S
 *	S -> S '+' M | S '|' M | S '||' M | M
 *	M -> M '*' L | M '&' L | M '&&' L | M L | L
 *	L -> X | '-' L | '~' L | '!' L | '(' S ')' | '[' S ']'
 *
 *	U -> U R | R | I
 *	R -> X | X '-' X
 *	I -> '[IP]' | '[I]' | '[RACK]' | '[R]'
 *
 *	D -> 'S' | 'L' | 'D' | 'STRICT' | 'LOOSE' | 'STD'
 *
 *	X -> 'a' ... 'z' | 'A' ... 'Z'
 */

char* make_label_expr(char *strbuff,const parser_data *pd);
int parse_label_expr(const char *exprstr,parser_data *pd);
uint8_t labelmask_matches_labelexpr(uint32_t labelmask,const uint8_t labelexpr[SCLASS_EXPR_MAX_SIZE]);

#endif
