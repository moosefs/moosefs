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

#ifdef HAVE_CONFIG_H
#include "config.h"
#else
#define HAVE_ZLIB_H 1
#define HAVE_STRUCT_TM_TM_GMTOFF 1
#endif

#if defined(_THREAD_SAFE) || defined(_REENTRANT) || defined(_USE_PTHREADS)
#  define USE_PTHREADS 1
#endif

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <errno.h>
#include <inttypes.h>
#ifdef HAVE_ZLIB_H
#include <zlib.h>
#endif
#ifdef USE_PTHREADS
#include <pthread.h>
#endif

#include "charts.h"
#include "crc.h"
#include "datapack.h"
#include "massert.h"
#include "slogger.h"

#define USE_NET_ORDER 1

#define MAXLENG 4096
#define MINLENG 100
#define MAXHEIGHT 1000
#define MINHEIGHT 100

#define XPOS 43
#define YPOS 6
#define XADD 50
#define YADD 20
#define MAXXSIZE (MAXLENG+XADD)
#define MAXYSIZE (MAXHEIGHT+YADD)

//#define LENG 950
//#define DATA 100
//#define XPOS 43
//#define YPOS 6
//#define XSIZE (LENG+50)
//#define YSIZE (DATA+20)
//#define LONGRATIO 6

#define SHORTRANGE 0
#define MEDIUMRANGE 1
#define LONGRANGE 2
#define VERYLONGRANGE 3

#define RANGES 4

#define CHARTS_DEF_IS_DIRECT(x) ((x)>=CHARTS_DIRECT_START && (x)<CHARTS_DIRECT_START+statdefscount)
#define CHARTS_DIRECT_POS(x) ((x)-CHARTS_DIRECT_START)
#define CHARTS_DEF_IS_CALC(x) ((x)>=CHARTS_CALC_START && (x)<CHARTS_CALC_START+calcdefscount)
#define CHARTS_CALC_POS(x) ((x)-CHARTS_CALC_START)

#define CHARTS_IS_DIRECT_STAT(x) ((x)<statdefscount)
#define CHARTS_EXTENDED_START 100
#define CHARTS_IS_EXTENDED_STAT(x) ((x)>=CHARTS_EXTENDED_START && (x)<CHARTS_EXTENDED_START+estatdefscount)
#define CHARTS_EXTENDED_POS(x) ((x)-CHARTS_EXTENDED_START)

static uint32_t *calcdefs;
static uint32_t **calcstartpos;
static uint32_t calcdefscount;
static statdef *statdefs;
static uint32_t statdefscount;
static estatdef *estatdefs;
static uint32_t estatdefscount;
static char* statsfilename;

#ifdef USE_PTHREADS
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;
#endif

typedef uint64_t *stat_record[RANGES];

static stat_record *series;
static uint32_t pointers[RANGES];
static uint32_t timepoint[RANGES];

static uint64_t *monotonic;

//chart times (for subscripts)
static uint32_t shhour,shmin;
static uint32_t medhour,medmin;
static uint32_t lnghalfhour,lngmday,lngmonth,lngyear;
static uint32_t vlngmday,vlngmonth,vlngyear;

static uint8_t *chart;
static uint8_t *rawchart;
static uint8_t *compbuff;
static uint32_t rawchartsize = 0;
static uint32_t compbuffsize = 0;
static uint32_t compsize = 0;
#ifdef HAVE_ZLIB_H
static z_stream zstr;
#else
static uint8_t warning[50] = {
	0x89,0xCF,0x83,0x8E,0x45,0xE7,0x9F,0x3C,0xF7,0xDE,    /* 10001001 11001111 10000011 10001110 01000101 11100111 10011111 00111100 11110111 11011110 */
	0xCA,0x22,0x04,0x51,0x6D,0x14,0x50,0x41,0x04,0x11,    /* 11001010 00100010 00000100 01010001 01101101 00010100 01010000 01000001 00000100 00010001 */
	0xAA,0x22,0x04,0x11,0x55,0xE7,0x9C,0x38,0xE7,0x11,    /* 10101010 00100010 00000100 00010001 01010101 11100111 10011100 00111000 11100111 00010001 */
	0x9A,0x22,0x04,0x51,0x45,0x04,0x50,0x04,0x14,0x11,    /* 10011010 00100010 00000100 01010001 01000101 00000100 01010000 00000100 00010100 00010001 */
	0x89,0xC2,0x03,0x8E,0x45,0x04,0x5F,0x79,0xE7,0xDE     /* 10001001 11000010 00000011 10001110 01000101 00000100 01011111 01111001 11100111 11011110 */
};
#endif

#define COLOR_TRANSPARENT 0
#define COLOR_BKG 1
#define COLOR_AXIS 2
#define COLOR_AUX 3
#define COLOR_TEXT 4
#define COLOR_NODATA 5
#define COLOR_TEXTBKG 6
#if VERSMAJ==1
#define COLOR_DATA1 7
#define COLOR_DATA2 8
#define COLOR_DATA3 9
#endif

#define COLOR_DATA_BEGIN 50
#define COLOR_DATA_RANGE 50

static uint8_t data_colors[] = {
	0x04,0xec,0xf1,                                                                                   // DATA1_MIN
	0x23,0x86,0xb4,                                                                                   // DATA1_MAX
	0x23,0x86,0xb4,                                                                                   // DATA2_MIN
	0x15,0x2f,0x5f,                                                                                   // DATA2_MAX
	0x15,0x2f,0x5f,                                                                                   // DATA3_MIN
	0x01,0x04,0x2c                                                                                    // DATA3_MAX
};


static uint8_t png_header[] = {
	137, 80, 78, 71, 13, 10, 26, 10,                                                                  // signature

	0, 0, 0, 13, 'I', 'H', 'D', 'R',                                                                  // IHDR chunk
	0, 0, 0, 0, 0, 0, 0, 0,                                                                           // widht and height (big endian)
	8, 3, 0, 0, 0,                                                                                    // 8bits, indexed color mode, default compression, default filters, no interlace
	'C', 'R', 'C', '#',                                                                               // CRC32 placeholder

	0, 0, 0x3, 0x0, 'P', 'L', 'T', 'E',                                                               // PLTE chunk
	0xff,0xff,0xff,                                                                                   // color map 0 - background (transparent)
	0xff,0xff,0xff,                                                                                   // color map 1 - chart background (white)
	0x00,0x00,0x00,                                                                                   // color map 2 - axes (black)
	0x00,0x00,0x00,                                                                                   // color map 3 - auxiliary lines (black)
	0x5f,0x20,0x00,                                                                                   // color map 4 - texts (brown)
	0xC0,0xC0,0xC0,                                                                                   // color map 5 - nodata (grey)
	0xFF,0xFF,0xDE,                                                                                   // color map 6 - warning background (light yellow)
#if VERSMAJ==1
        0x00,0xff,0x00,                         // color map 7 - data1 (light green)
	0x00,0x96,0x00,                         // color map 8 - data2 (green)
	0x00,0x60,0x00,                         // color map 9 - data3 (dark green)
	                                                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c0A - c0F
#else
	                                          0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c07 - c0F
#endif
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c10 - c1F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c20 - c2F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c30 - c3F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c40 - c4F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c50 - c5F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c60 - c6F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c70 - c7F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c80 - c8F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // c90 - c9F
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // cA0 - cAF
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // cB0 - cBF
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // cC0 - cCF
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // cD0 - cDF
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // cE0 - cEF
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // cF0 - cFF
	'C', 'R', 'C', '#',                                                                               // CRC32 placeholder

	0, 0, 0, 1, 't', 'R', 'N', 'S',                                                                   // tRNS chunk
	0,                                                                                                // color 0 transparency - alpha = 0
	'C', 'R', 'C', '#',                                                                               // CRC32 placeholder

	0, 0, 0, 1, 'b', 'K', 'G', 'D',                                                                   // bKGD chunk
	0,                                                                                                // color 0 = background
	'C', 'R', 'C', '#',                                                                               // CRC32 placeholder

	0, 0, 0, 0, 'I', 'D', 'A', 'T'                                                                    // IDAT chunk
};

static uint8_t png_tailer[] = {
	'C', 'R', 'C', '#',                                                                               // CRC32 placeholder
	0, 0, 0, 0, 'I', 'E', 'N', 'D',                                                                   // IEND chunk
	'C', 'R', 'C', '#',                                                                               // CRC32 placeholder
};

static uint8_t png_1x1[] = {
	137, 80, 78, 71, 13, 10, 26, 10,                                                                  // signature

	0, 0, 0, 13, 'I', 'H', 'D', 'R',                                                                  // IHDR chunk
	0, 0, 0, 1,                                                                                       // width
	0, 0, 0, 1,                                                                                       // height
	8, 4, 0, 0, 0,                                                                                    // 8bits, grayscale with alpha color mode, default compression, default filters, no interlace
	0xb5, 0x1c, 0x0c, 0x02,                                                                           // CRC

	0, 0, 0, 11, 'I', 'D', 'A', 'T',                                                                  // IDAT chunk
	0x08, 0xd7, 0x63, 0x60, 0x60, 0x00,
	0x00, 0x00, 0x03, 0x00, 0x01,
	0x20, 0xd5, 0x94, 0xc7,                                                                           // CRC

	0, 0, 0, 0, 'I', 'E', 'N', 'D',                                                                   // IEND chunk
	0xae, 0x42, 0x60, 0x82                                                                            // CRC
};

static uint8_t font[25][9]={
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x11,0x11,0x11,0x11,0x0E,0x00,0x00},
	/* 00100 */
	/* 01100 */
	/* 10100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x04,0x0C,0x14,0x04,0x04,0x04,0x1F,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 00001 */
	/* 00010 */
	/* 00100 */
	/* 01000 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x01,0x02,0x04,0x08,0x1F,0x00,0x00},
	/* 11111 */
	/* 00010 */
	/* 00100 */
	/* 01110 */
	/* 00001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x02,0x04,0x0E,0x01,0x11,0x0E,0x00,0x00},
	/* 00010 */
	/* 00110 */
	/* 01010 */
	/* 10010 */
	/* 11111 */
	/* 00010 */
	/* 00010 */
	/* 00000 */
	/* 00000 */
	{0x02,0x06,0x0A,0x12,0x1F,0x02,0x02,0x00,0x00},
	/* 11111 */
	/* 10000 */
	/* 11110 */
	/* 00001 */
	/* 00001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x10,0x1E,0x01,0x01,0x11,0x0E,0x00,0x00},
	/* 00110 */
	/* 01000 */
	/* 10000 */
	/* 11110 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x06,0x08,0x10,0x1E,0x11,0x11,0x0E,0x00,0x00},
	/* 11111 */
	/* 00001 */
	/* 00010 */
	/* 00010 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x01,0x02,0x02,0x04,0x04,0x04,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x11,0x0E,0x11,0x11,0x0E,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 10001 */
	/* 01111 */
	/* 00001 */
	/* 00010 */
	/* 01100 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x11,0x0F,0x01,0x02,0x0C,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x00,0x00,0x00,0x04,0x04,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 00100 */
	/* 00000 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x04,0x00,0x04,0x00,0x00,0x00,0x00},
	/* 01000 */
	/* 01000 */
	/* 01001 */
	/* 01010 */
	/* 01100 */
	/* 01010 */
	/* 01001 */
	/* 00000 */
	/* 00000 */
	{0x08,0x08,0x09,0x0A,0x0C,0x0A,0x09,0x00,0x00},
	/* 10001 */
	/* 11011 */
	/* 10101 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 00000 */
	/* 00000 */
	{0x11,0x1B,0x15,0x11,0x11,0x11,0x11,0x00,0x00},
	/* 01110 */
	/* 10001 */
	/* 10000 */
	/* 10011 */
	/* 10001 */
	/* 10001 */
	/* 01110 */
	/* 00000 */
	/* 00000 */
	{0x0E,0x11,0x10,0x13,0x11,0x11,0x0E,0x00,0x00},
	/* 11111 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x04,0x04,0x04,0x04,0x04,0x04,0x00,0x00},
	/* 11110 */
	/* 10001 */
	/* 10001 */
	/* 11110 */
	/* 10000 */
	/* 10000 */
	/* 10000 */
	/* 00000 */
	/* 00000 */
	{0x1E,0x11,0x11,0x1E,0x10,0x10,0x10,0x00,0x00},
	/* 11111 */
	/* 10000 */
	/* 10000 */
	/* 11100 */
	/* 10000 */
	/* 10000 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x10,0x10,0x1C,0x10,0x10,0x1F,0x00,0x00},
	/* 11111 */
	/* 00001 */
	/* 00010 */
	/* 00100 */
	/* 01000 */
	/* 10000 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x01,0x02,0x04,0x08,0x10,0x1F,0x00,0x00},
	/* 10001 */
	/* 10001 */
	/* 01010 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00100 */
	/* 00000 */
	/* 00000 */
	{0x11,0x11,0x0A,0x04,0x04,0x04,0x04,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 11110 */
	/* 10101 */
	/* 10101 */
	/* 10101 */
	/* 10101 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x1E,0x15,0x15,0x15,0x15,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 10010 */
	/* 10010 */
	/* 10010 */
	/* 10010 */
	/* 11101 */
	/* 10000 */
	/* 10000 */
	{0x00,0x00,0x12,0x12,0x12,0x12,0x1D,0x10,0x10},
	/* 11001 */
	/* 11010 */
	/* 00010 */
	/* 00100 */
	/* 01000 */
	/* 01011 */
	/* 10011 */
	/* 00000 */
	/* 00000 */
	{0x19,0x1A,0x02,0x04,0x08,0x0B,0x13,0x00,0x00},
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	/* 00000 */
	{0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00},
	/* 11111 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 10001 */
	/* 11111 */
	/* 00000 */
	/* 00000 */
	{0x1F,0x11,0x11,0x11,0x11,0x11,0x1F,0x00,0x00}
};

#define FDOT 10
#define COLON 11
#define KILO 12
#define MEGA 13
#define GIGA 14
#define TERA 15
#define PETA 16
#define EXA 17
#define ZETTA 18
#define YOTTA 19
#define MILI 20
#define MICRO 21
#define PERCENT 22
#define SPACE 23
#define SQUARE 24

uint32_t getmonleng(uint32_t year,uint32_t month) {
	switch (month) {
	case 1:
	case 3:
	case 5:
	case 7:
	case 8:
	case 10:
	case 12:
		return 31;
	case 4:
	case 6:
	case 9:
	case 11:
		return 30;
	case 2:
		if (year%4) return 28;
		if (year%100) return 29;
		if (year%400) return 29;
		return 28;
	}
	return 0;
}

#define CHARTS_FILE_VERSION 0x00010000

void charts_store (void) {
	int fd;
	uint32_t s,i,j,p;
	uint64_t *tab;
#ifdef USE_NET_ORDER
	uint8_t *ptr;
	uint8_t hdr[16];
	uint8_t data[8*MAXLENG];
#else
	uint32_t hdr[4];
#endif
	char namehdr[100];

#ifdef USE_PTHREADS
	zassert(pthread_mutex_lock(&glock));
#endif

	fd = open(statsfilename,O_WRONLY | O_TRUNC | O_CREAT,0666);
	if (fd<0) {
		mfs_errlog(LOG_WARNING,"error creating charts data file");
#ifdef USE_PTHREADS
		zassert(pthread_mutex_unlock(&glock));
#endif
		return;
	}
#ifdef USE_NET_ORDER
	ptr = hdr;
	put32bit(&ptr,CHARTS_FILE_VERSION);
	put32bit(&ptr,MAXLENG);
	put32bit(&ptr,statdefscount);
	put32bit(&ptr,timepoint[SHORTRANGE]);
	if (write(fd,(void*)hdr,16)!=16) {
		mfs_errlog(LOG_WARNING,"error writing charts data file");
		close(fd);
#ifdef USE_PTHREADS
		zassert(pthread_mutex_unlock(&glock));
#endif
		return;
	}
#else
	hdr[0]=CHARTS_FILE_VERSION;
	hdr[1]=MAXLENG;
	hdr[2]=statdefscount;
	hdr[3]=timepoint[SHORTRANGE];
	if (write(fd,(void*)hdr,sizeof(uint32_t)*4)!=sizeof(uint32_t)*4) {
		mfs_errlog(LOG_WARNING,"error writing charts data file");
		close(fd);
#ifdef USE_PTHREADS
		zassert(pthread_mutex_unlock(&glock));
#endif
		return;
	}
#endif
	for (i=0 ; i<statdefscount ; i++) {
		s = strlen(statdefs[i].name);
		memset(namehdr,0,100);
		memcpy(namehdr,statdefs[i].name,(s>100)?100:s);
		if (write(fd,(void*)namehdr,100)!=100) {
			mfs_errlog(LOG_WARNING,"error writing charts data file");
			close(fd);
#ifdef USE_PTHREADS
			zassert(pthread_mutex_unlock(&glock));
#endif
			return;
		}
		for (j=0 ; j<RANGES ; j++) {
			tab = series[i][j];
			p = pointers[j]+1;
#ifdef USE_NET_ORDER
			ptr = data;
			for (s=0 ; s<MAXLENG ; s++) {
				put64bit(&ptr,tab[(p+s)%MAXLENG]);
			}
			if (write(fd,(void*)data,8*MAXLENG)!=(ssize_t)(8*MAXLENG)) {
				mfs_errlog(LOG_WARNING,"error writing charts data file");
				close(fd);
#ifdef USE_PTHREADS
				zassert(pthread_mutex_unlock(&glock));
#endif
				return;
			}
#else
			if (p<MAXLENG) {
				if (write(fd,(void*)(tab+p),sizeof(uint64_t)*(MAXLENG-p))!=(ssize_t)(sizeof(uint64_t)*(MAXLENG-p))) {
					mfs_errlog(LOG_WARNING,"error writing charts data file");
					close(fd);
#ifdef USE_PTHREADS
					zassert(pthread_mutex_unlock(&glock));
#endif
					return;
				}
			}
			if (write(fd,(void*)tab,sizeof(uint64_t)*p)!=(ssize_t)(sizeof(uint64_t)*p)) {
				mfs_errlog(LOG_WARNING,"error writing charts data file");
				close(fd);
#ifdef USE_PTHREADS
				zassert(pthread_mutex_unlock(&glock));
#endif
				return;
			}
#endif
		}
	}
	close(fd);
#ifdef USE_PTHREADS
	zassert(pthread_mutex_unlock(&glock));
#endif
}


int charts_load(uint8_t mode) {
	int fd;
	uint32_t i,j,k,fleng,fcharts;
	uint64_t *tab;
#ifdef USE_NET_ORDER
	uint32_t l;
	const uint8_t *ptr;
	uint8_t hdr[16];
	uint8_t data[8*MAXLENG];
#else
	uint32_t hdr[3];
#endif
	char namehdr[101];

	fd = open(statsfilename,O_RDONLY);
	if (fd<0) {
		if (mode==1) {
			fprintf(stderr,"file loading error: %s\n",strerr(errno));
			return -1;
		}
		if (errno!=ENOENT) {
			mfs_errlog(LOG_WARNING,"error reading charts data file");
		} else {
			mfs_syslog(LOG_NOTICE,"no charts data file - initializing empty charts");
		}
		return 0;
	}
#ifdef USE_NET_ORDER
	if (read(fd,(void*)hdr,16)!=16) {
		if (mode==1) {
			fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
			close(fd);
			return -1;
		}
		mfs_errlog(LOG_WARNING,"error reading charts data file");
		close(fd);
		return 0;
	}
	ptr = hdr;
	i = get32bit(&ptr);
	if (i!=CHARTS_FILE_VERSION) {
		if (mode==1) {
			fprintf(stderr,"unrecognized charts data file format\n");
			close(fd);
			return -1;
		}
		mfs_syslog(LOG_WARNING,"unrecognized charts data file format - initializing empty charts");
		close(fd);
		return 0;
	}
	fleng = get32bit(&ptr);
	fcharts = get32bit(&ptr);
	i = get32bit(&ptr);
	timepoint[SHORTRANGE]=i;
//	timepoint[MEDIUMRANGE]=i/6;
//	timepoint[LONGRANGE]=i/30;
//	timepoint[VERYLONGRANGE]=i/(24*60);
#else
	if (read(fd,(void*)hdr,sizeof(uint32_t))!=sizeof(uint32_t)) {
		if (mode==1) {
			fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
			close(fd);
			return -1;
		}
		mfs_errlog(LOG_WARNING,"error reading charts data file");
		close(fd);
		return 0;
	}
	if (hdr[0]!=CHARTS_FILE_VERSION) {
		if (mode==1) {
			fprintf(stderr,"unrecognized charts data file format\n");
			close(fd);
			return -1;
		}
		mfs_syslog(LOG_WARNING,"unrecognized charts data file format - initializing empty charts");
		close(fd);
		return 0;
	}
	if (read(fd,(void*)hdr,sizeof(uint32_t)*3)!=sizeof(uint32_t)*3) {
		if (mode==1) {
			fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
			close(fd);
			return -1;
		}
		mfs_errlog(LOG_WARNING,"error reading charts data file");
		close(fd);
		return 0;
	}
	fleng = hdr[0];
	fcharts = hdr[1];
	timepoint[SHORTRANGE]=hdr[2];
//	timepoint[MEDIUMRANGE]=hdr[2]/6;
//	timepoint[LONGRANGE]=hdr[2]/30;
//	timepoint[VERYLONGRANGE]=hdr[2]/(24*60);
#endif
	pointers[SHORTRANGE]=MAXLENG-1;
	pointers[MEDIUMRANGE]=MAXLENG-1;
	pointers[LONGRANGE]=MAXLENG-1;
	pointers[VERYLONGRANGE]=MAXLENG-1;
	for (i=0 ; i<fcharts ; i++) {
		if (read(fd,namehdr,100)!=100) {
			if (mode==1) {
				fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
				close(fd);
				return -1;
			}
			mfs_errlog(LOG_WARNING,"error reading charts data file");
			close(fd);
			return 0;
		}
		namehdr[100]=0;
		for (j=0 ; j<statdefscount && strcmp(statdefs[j].name,namehdr)!=0 ; j++) {}
		if (j>=statdefscount) {
			lseek(fd,RANGES*fleng*8,SEEK_CUR);
			// ignore data
		} else {
			for (k=0 ; k<RANGES ; k++) {
				tab = series[j][k];
				if (fleng>MAXLENG) {
					lseek(fd,(fleng-MAXLENG)*sizeof(uint64_t),SEEK_CUR);
				}
#ifdef USE_NET_ORDER
				if (fleng<MAXLENG) {
					if (read(fd,(void*)data,8*fleng)!=(ssize_t)(8*fleng)) {
						if (mode==1) {
							fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
							close(fd);
							return -1;
						}
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return 0;
					}
					ptr = data;
					for (l=MAXLENG-fleng ; l<MAXLENG ; l++) {
						tab[l] = get64bit(&ptr);
					}
				} else {
					if (read(fd,(void*)data,8*MAXLENG)!=(ssize_t)(8*MAXLENG)) {
						if (mode==1) {
							fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
							close(fd);
							return -1;
						}
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return 0;
					}
					ptr = data;
					for (l=0 ; l<MAXLENG ; l++) {
						tab[l] = get64bit(&ptr);
					}
				}
#else
				if (fleng<MAXLENG) {
					if (read(fd,(void*)(tab+(MAXLENG-fleng)),sizeof(uint64_t)*fleng)!=(ssize_t)(sizeof(uint64_t)*fleng)) {
						if (mode==1) {
							fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
							close(fd);
							return -1;
						}
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return 0;
					}
				} else {
					if (read(fd,(void*)tab,sizeof(uint64_t)*MAXLENG)!=(ssize_t)(sizeof(uint64_t)*MAXLENG)) {
						if (mode==1) {
							fprintf(stderr,"error reading charts data file: %s\n",strerr(errno));
							close(fd);
							return -1;
						}
						mfs_errlog(LOG_WARNING,"error reading charts data file");
						close(fd);
						return 0;
					}
				}
#endif
			}
		}
	}
	close(fd);
	if (mode==1) {
		return 0;
	}
	mfs_syslog(LOG_NOTICE,"stats file has been loaded");
	return 0;
}

uint8_t charts_filltab(uint64_t *datatab,uint32_t range,uint32_t type,uint32_t cno,uint32_t width) {
#if defined(INT64_MIN)
#  define STACK_NODATA INT64_MIN
#elif defined(INT64_C)
#  define STACK_NODATA (-INT64_C(9223372036854775807)-1)
#else
#  define STACK_NODATA (-9223372036854775807LL-1)
#endif

	uint32_t i,j;
	uint32_t pointer;
	uint32_t src,*ops;
	int64_t stack[50];
	uint32_t sp;

	if (range>=RANGES || cno==0 || cno>3) {
		return 0;
	}
	pointer = pointers[range];
	if (CHARTS_IS_DIRECT_STAT(type)) {
		if (cno==1) {
			for (i=0 ; i<width ; i++) {
				j = (MAXLENG-width+1+pointer+i)%MAXLENG;
				datatab[i] = series[type][range][j];
			}
			return 1;
		}
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		if (cno==1) {
			src = estatdefs[CHARTS_EXTENDED_POS(type)].c1src;
		} else if (cno==2) {
			src = estatdefs[CHARTS_EXTENDED_POS(type)].c2src;
		} else {
			src = estatdefs[CHARTS_EXTENDED_POS(type)].c3src;
		}
		if (CHARTS_DEF_IS_DIRECT(src)) {
			for (i=0 ; i<width ; i++) {
				j = (MAXLENG-width+1+pointer+i)%MAXLENG;
				datatab[i] = series[CHARTS_DIRECT_POS(src)][range][j];
			}
			return 1;
		} else if (CHARTS_DEF_IS_CALC(src)) {
			for (i=0 ; i<width ; i++) {
				j = (MAXLENG-width+1+pointer+i)%MAXLENG;
				sp=0;
				ops = calcstartpos[CHARTS_CALC_POS(src)];
				while (*ops!=CHARTS_OP_END) {
					if (CHARTS_IS_DIRECT_STAT(*ops)) {
						if (sp<50) {
							if (series[*ops][range][j]==CHARTS_NODATA) {
								stack[sp]=STACK_NODATA;
							} else {
								stack[sp]=series[*ops][range][j];
							}
							sp++;
						}
					} else if (*ops==CHARTS_OP_ADD) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]+=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_SUB) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]-=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_MIN) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else if (stack[sp-1]<stack[sp-2]) {
								stack[sp-2]=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_MAX) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else if (stack[sp-1]>stack[sp-2]) {
								stack[sp-2]=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_MUL) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]*=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_DIV) {
						if (sp>=2) {
							if (stack[sp-1]==STACK_NODATA || stack[sp-2]==STACK_NODATA || stack[sp-1]==0) {
								stack[sp-2]=STACK_NODATA;
							} else {
								stack[sp-2]/=stack[sp-1];
							}
							sp--;
						}
					} else if (*ops==CHARTS_OP_NEG) {
						if (sp>=1) {
							if (stack[sp-1]!=STACK_NODATA) {
								stack[sp-1]=-stack[sp-1];
							}
						}
					} else if (*ops==CHARTS_OP_CONST) {
						ops++;
						if (sp<50) {
							stack[sp]=*ops;
							sp++;
						}
					}
					ops++;
				}
				if (sp>=1 && stack[sp-1]>=0) {	// STACK_NODATA < 0, so this condition is enough for STACK_NODATA
					datatab[i]=stack[sp-1];
				} else {
					datatab[i]=CHARTS_NODATA;
				}
			}
			return 1;
		}
	}
	return 0;
}

uint64_t charts_get (uint32_t type,uint32_t numb) {
	uint64_t result=0,cnt;
	uint64_t *tab;
	uint32_t i,j;

	if (numb==0 || numb>MAXLENG) {
		return result;
	}
	if (CHARTS_IS_DIRECT_STAT(type)) {
#ifdef USE_PTHREADS
		zassert(pthread_mutex_lock(&glock));
#endif
		tab = series[type][SHORTRANGE];
		j = pointers[SHORTRANGE] % MAXLENG;
		if (statdefs[type].mode == CHARTS_MODE_ADD) {
			cnt=0;
			for (i=0 ; i<numb ; i++) {
				if (tab[j]!=CHARTS_NODATA) {
					result += tab[j];
					cnt++;
				}
				if (j>0) {
					j--;
				} else {
					j = MAXLENG-1;
				}
			}
			if (cnt>0) {
				result /= cnt;
			}
		} else {
			for (i=0 ; i<numb ; i++) {
				if (tab[j]!=CHARTS_NODATA && tab[j]>result) {
					result = tab[j];
				}
				if (j>0) {
					j--;
				} else {
					j = MAXLENG-1;
				}
			}
		}
#ifdef USE_PTHREADS
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
	return result;
}

void charts_inittimepointers (void) {
	time_t now;
	int32_t local;
	struct tm *ts;

	if (timepoint[SHORTRANGE]==0) {
		now = time(NULL);
		ts = localtime(&now);
#ifdef HAVE_STRUCT_TM_TM_GMTOFF
		local = now+ts->tm_gmtoff;
#else
		local = now;
#endif
	} else {
		now = timepoint[SHORTRANGE]*60;
		ts = gmtime(&now);
		local = now;
	}

	timepoint[SHORTRANGE] = local / 60;
	shmin = ts->tm_min;
	shhour = ts->tm_hour;
	timepoint[MEDIUMRANGE] = local / (60 * 6);
	medmin = ts->tm_min;
	medhour = ts->tm_hour;
	timepoint[LONGRANGE] = local / (60 * 30);
	lnghalfhour = ts->tm_hour*2;
	if (ts->tm_min>=30) {
		lnghalfhour++;
	}
	lngmday = ts->tm_mday;
	lngmonth = ts->tm_mon + 1;
	lngyear = ts->tm_year + 1900;
	timepoint[VERYLONGRANGE] = local / (60 * 60 * 24);
	vlngmday = ts->tm_mday;
	vlngmonth = ts->tm_mon + 1;
	vlngyear = ts->tm_year + 1900;
}

void charts_add (uint64_t *data,uint32_t datats) {
	uint32_t i,j;
	struct tm *ts;
	time_t now = datats;
	int32_t local;

	int32_t nowtime,delta;

	if (data) {
		for (j=0 ; j<statdefscount ; j++) {
			monotonic[j] += data[j];
		}
	}

	ts = localtime(&now);
#ifdef HAVE_STRUCT_TM_TM_GMTOFF
	local = now+ts->tm_gmtoff;
#else
	local = now;
#endif

#ifdef USE_PTHREADS
	zassert(pthread_mutex_lock(&glock));
#endif
// short range chart - every 1 min

	nowtime = local / 60;

	delta = nowtime - timepoint[SHORTRANGE];

	if (delta>0) {
		if (delta>MAXLENG) {
			delta=MAXLENG;
		}
		while (delta>0) {
			pointers[SHORTRANGE]++;
			pointers[SHORTRANGE]%=MAXLENG;
			for (i=0 ; i<statdefscount ; i++) {
				series[i][SHORTRANGE][pointers[SHORTRANGE]] = CHARTS_NODATA;
			}
			delta--;
		}
		timepoint[SHORTRANGE] = nowtime;
		shmin = ts->tm_min;
		shhour = ts->tm_hour;
	}
	if (delta<=0 && delta>-MAXLENG && data) {
		i = (pointers[SHORTRANGE] + MAXLENG + delta) % MAXLENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][SHORTRANGE][i]==CHARTS_NODATA) {   // no data
				series[j][SHORTRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {  // add mode
				series[j][SHORTRANGE][i] += data[j];
			} else if (data[j]>series[j][SHORTRANGE][i]) {   // max mode
				series[j][SHORTRANGE][i] = data[j];
			}
		}
	}

// medium range chart - every 6 min

	nowtime = local / (60 * 6);

	delta = nowtime - timepoint[MEDIUMRANGE];

	if (delta>0) {
		if (delta>MAXLENG) {
			delta=MAXLENG;
		}
		while (delta>0) {
			pointers[MEDIUMRANGE]++;
			pointers[MEDIUMRANGE]%=MAXLENG;
			for (i=0 ; i<statdefscount ; i++) {
				series[i][MEDIUMRANGE][pointers[MEDIUMRANGE]] = CHARTS_NODATA;
			}
			delta--;
		}
		timepoint[MEDIUMRANGE] = nowtime;
		medmin = ts->tm_min;
		medhour = ts->tm_hour;
	}
	if (delta<=0 && delta>-MAXLENG && data) {
		i = (pointers[MEDIUMRANGE] + MAXLENG + delta) % MAXLENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][MEDIUMRANGE][i]==CHARTS_NODATA) {  // no data
				series[j][MEDIUMRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {  // add mode
				series[j][MEDIUMRANGE][i] += data[j];
			} else if (data[j]>series[j][MEDIUMRANGE][i]) {  // max mode
				series[j][MEDIUMRANGE][i] = data[j];
			}
		}
	}


// long range chart - every 30 min

	nowtime = local / (60 * 30);

	delta = nowtime - timepoint[LONGRANGE];

	if (delta>0) {
		if (delta>MAXLENG) {
			delta=MAXLENG;
		}
		while (delta>0) {
			pointers[LONGRANGE]++;
			pointers[LONGRANGE]%=MAXLENG;
			for (i=0 ; i<statdefscount ; i++) {
				series[i][LONGRANGE][pointers[LONGRANGE]] = CHARTS_NODATA;
			}
			delta--;
		}
		timepoint[LONGRANGE] = nowtime;
		lnghalfhour = ts->tm_hour*2;
		if (ts->tm_min>=30) {
			lnghalfhour++;
		}
		lngmday = ts->tm_mday;
		lngmonth = ts->tm_mon + 1;
		lngyear = ts->tm_year + 1900;
	}
	if (delta<=0 && delta>-MAXLENG && data) {
		i = (pointers[LONGRANGE] + MAXLENG + delta) % MAXLENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][LONGRANGE][i]==CHARTS_NODATA) {    // no data
				series[j][LONGRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {  // add mode
				series[j][LONGRANGE][i] += data[j];
			} else if (data[j]>series[j][LONGRANGE][i]) {    // max mode
				series[j][LONGRANGE][i] = data[j];
			}
		}
	}
// long range chart - every 1 day

	nowtime = local / (60 * 60 * 24);

	delta = nowtime - timepoint[VERYLONGRANGE];

	if (delta>0) {
		if (delta>MAXLENG) {
			delta=MAXLENG;
		}
		while (delta>0) {
			pointers[VERYLONGRANGE]++;
			pointers[VERYLONGRANGE]%=MAXLENG;
			for (i=0 ; i<statdefscount ; i++) {
				series[i][VERYLONGRANGE][pointers[VERYLONGRANGE]] = CHARTS_NODATA;
			}
			delta--;
		}
		timepoint[VERYLONGRANGE] = nowtime;
		vlngmday = ts->tm_mday;
		vlngmonth = ts->tm_mon + 1;
		vlngyear = ts->tm_year + 1900;
	}
	if (delta<=0 && delta>-MAXLENG && data) {
		i = (pointers[VERYLONGRANGE] + MAXLENG + delta) % MAXLENG;
		for (j=0 ; j<statdefscount ; j++) {
			if (series[j][VERYLONGRANGE][i]==CHARTS_NODATA) {  // no data
				series[j][VERYLONGRANGE][i] = data[j];
			} else if (statdefs[j].mode==CHARTS_MODE_ADD) {    // add mode
				series[j][VERYLONGRANGE][i] += data[j];
			} else if (data[j]>series[j][VERYLONGRANGE][i]) {  // max mode
				series[j][VERYLONGRANGE][i] = data[j];
			}
		}
	}
#ifdef USE_PTHREADS
	zassert(pthread_mutex_unlock(&glock));
#endif
}

void charts_term (void) {
	uint32_t i,j;
#ifdef USE_PTHREADS
	zassert(pthread_mutex_lock(&glock));
	zassert(pthread_mutex_unlock(&glock));
#endif
	free(statsfilename);
	if (calcdefs) {
		free(calcdefs);
	}
	if (calcstartpos) {
		free(calcstartpos);
	}
	if (estatdefs) {
		free(estatdefs);
	}
	for (i=0 ; i<statdefscount ; i++) {
		free(statdefs[i].name);
	}
	if (statdefs) {
		free(statdefs);
	}
	for (i=0 ; i<statdefscount ; i++) {
		for (j=0 ; j<RANGES ; j++) {
			if (series[i][j]) {
				free(series[i][j]);
			}
		}
	}
	if (series) {
		free(series);
	}
	if (monotonic) {
		free(monotonic);
	}
	if (compbuff) {
		free(compbuff);
	}
	if (rawchart) {
		free(rawchart);
	}
	if (chart) {
		free(chart);
	}
#ifdef HAVE_ZLIB_H
	deflateEnd(&zstr);
#endif
}

static inline void png_make_palette(void) {
	uint32_t rng,indx;
	double r,g,b,dr,db,dg;
	for (rng=0 ; rng<3 ; rng++) {
		r = data_colors[rng*6+0];
		g = data_colors[rng*6+1];
		b = data_colors[rng*6+2];
		dr = data_colors[rng*6+3];
		dg = data_colors[rng*6+4];
		db = data_colors[rng*6+5];
		dr -= r;
		dg -= g;
		db -= b;
		dr /= COLOR_DATA_RANGE;
		dg /= COLOR_DATA_RANGE;
		db /= COLOR_DATA_RANGE;
		r += 0.5;
		g += 0.5;
		b += 0.5;
		for (indx = 0 ; indx < COLOR_DATA_RANGE ; indx++) {
			png_header[41+(COLOR_DATA_BEGIN+rng*COLOR_DATA_RANGE+indx)*3+0] = r;
			png_header[41+(COLOR_DATA_BEGIN+rng*COLOR_DATA_RANGE+indx)*3+1] = g;
			png_header[41+(COLOR_DATA_BEGIN+rng*COLOR_DATA_RANGE+indx)*3+2] = b;
			r+=dr;
			g+=dg;
			b+=db;
		}
	}
}

int charts_init (const uint32_t *calcs,const statdef *stats,const estatdef *estats,const char *filename,uint8_t mode) {
	uint32_t i,j;

	chart = malloc(MAXXSIZE*MAXYSIZE);
	passert(chart);
	rawchartsize = (1+MAXXSIZE)*(MAXYSIZE);
	rawchart = malloc(rawchartsize);
	passert(rawchart);
	compbuffsize = ((uint64_t)rawchartsize*UINT64_C(1001))/UINT64_C(1000)+16;
	compbuff = malloc(compbuffsize);
	passert(compbuff);

	statsfilename = strdup(filename);
	passert(statsfilename);

	for (i=0,calcdefscount=0 ; calcs[i]!=CHARTS_DEFS_END ; i++) {
		if (calcs[i]==CHARTS_OP_END) {
			calcdefscount++;
		}
	}
	if (i>0 && calcdefscount>0) {
		calcdefs = (uint32_t*)malloc(sizeof(uint32_t)*i);
		passert(calcdefs);
		calcstartpos = (uint32_t**)malloc(sizeof(uint32_t*)*calcdefscount);
		passert(calcstartpos);
		j=0;
		calcstartpos[j]=calcdefs;
		j++;
		for (i=0 ; calcs[i]!=CHARTS_DEFS_END ; i++) {
			calcdefs[i] = calcs[i];
			if (calcs[i]==CHARTS_OP_END) {
				if (j<calcdefscount) {
					calcstartpos[j]=calcdefs+i+1;
					j++;
				}
			}
		}
	} else {
		calcdefs = NULL;
		calcstartpos = NULL;
	}
	for (statdefscount=0 ; stats[statdefscount].divisor ; statdefscount++) {}
	if (statdefscount>0) {
		statdefs = (statdef*)malloc(sizeof(statdef)*statdefscount);
		passert(statdefs);
	} else {
		statdefs = NULL;
	}
	for (i=0 ; i<statdefscount ; i++) {
		statdefs[i].name = strdup(stats[i].name);
		passert(statdefs[i].name);
		statdefs[i].statid = stats[i].statid;
		statdefs[i].mode = stats[i].mode;
		statdefs[i].percent = stats[i].percent;
		statdefs[i].scale = stats[i].scale;
		statdefs[i].multiplier = stats[i].multiplier;
		statdefs[i].divisor = stats[i].divisor;
	}
	for (estatdefscount=0 ; estats[estatdefscount].divisor ; estatdefscount++) {}
	if (estatdefscount>0) {
		estatdefs = (estatdef*)malloc(sizeof(estatdef)*estatdefscount);
		passert(estatdefs);
	} else {
		estatdefs = NULL;
	}
	for (i=0 ; i<estatdefscount ; i++) {
		if (estats[i].name!=NULL) {
			estatdefs[i].name = strdup(estats[i].name);
			passert(estatdefs[i].name);
		} else {
			estatdefs[i].name = NULL;
		}
		estatdefs[i].statid = estats[i].statid;
		estatdefs[i].c1src = estats[i].c1src;
		estatdefs[i].c2src = estats[i].c2src;
		estatdefs[i].c3src = estats[i].c3src;
		estatdefs[i].mode = estats[i].mode;
		estatdefs[i].percent = estats[i].percent;
		estatdefs[i].scale = estats[i].scale;
		estatdefs[i].multiplier = estats[i].multiplier;
		estatdefs[i].divisor = estats[i].divisor;
	}

	if (statdefscount>0) {
		monotonic = (uint64_t*)malloc(sizeof(uint64_t)*statdefscount);
		passert(monotonic);
		series = (stat_record*)malloc(sizeof(stat_record)*statdefscount);
		passert(series);
		for (i=0 ; i<statdefscount ; i++) {
			monotonic[i] = 0;
			for (j=0 ; j<RANGES ; j++) {
				series[i][j] = malloc(MAXLENG*sizeof(uint64_t));
				passert(series[i][j]);
				memset(series[i][j],0xFF,MAXLENG*sizeof(uint64_t));
			}
		}
	} else {
		series = NULL;
		monotonic = NULL;
	}

	for (i=0 ; i<RANGES ; i++) {
		pointers[i]=0;
		timepoint[i]=0;
	}

	if (charts_load(mode)<0) {
		return -1;
	}
	charts_inittimepointers();
	if (mode==0) {
		charts_add(NULL,time(NULL));
	}

#ifdef HAVE_ZLIB_H
	zstr.zalloc = NULL;
	zstr.zfree = NULL;
	zstr.opaque = NULL;
	if (deflateInit(&zstr,Z_BEST_SPEED)!=Z_OK) {
	//if (deflateInit(&zstr,Z_DEFAULT_COMPRESSION)!=Z_OK) {
		return -1;
	}
#endif /* HAVE_ZLIB_H */
	png_make_palette();
	return 0;
}

#ifndef HAVE_ZLIB_H
static inline void charts_putwarning(uint32_t posx,uint32_t posy,uint8_t color) {
	uint8_t *w,c,fx,fy,b;
	uint32_t x,y;
	w = warning;
	for (fy=0 ; fy<11 ; fy++) {
		y = fy+posy;
		for (fx=0 ; fx<86 ; fx++) {
			x = fx+posx;
			if (x<MAXXSIZE && y<MAXYSIZE) {
				chart[(MAXXSIZE)*y+x] = (fy==0||fy==10||fx==0||fx==85)?COLOR_AXIS:COLOR_TEXTBKG;
			}
		}
	}
	y = posy+3;
	for (fy=0 ; fy<5 ; fy++) {
		x = posx+3;
		for (b=0 ; b<10 ; b++) {
			c = *w;
			w++;
			for (fx=0 ; fx<8 ; fx++) {
				if (c&0x80 && x<MAXXSIZE && y<MAXYSIZE) {
					chart[(MAXXSIZE)*y+x] = color;
				}
				c<<=1;
				x++;
			}
		}
		y++;
	}
}
#endif

static inline void charts_puttext(int32_t posx,int32_t posy,uint8_t color,uint8_t *data,uint32_t leng,int32_t minx,int32_t maxx,int32_t miny,int32_t maxy) {
	uint32_t i,fx,fy;
	uint8_t fp,fbits;
	int32_t px,x,y;
	for (i=0 ; i<leng ; i++) {
		px = i*6+posx;
		fp = data[i];
		if (fp>SQUARE) {
			fp=SQUARE;
		}
		for (fy=0 ; fy<9 ; fy++) {
			fbits = font[fp][fy];
			if (fbits) {
				for (fx=0 ; fx<5 ; fx++) {
					x = px+fx;
					y = posy+fy;
					if (fbits&0x10 && x>=minx && x<=maxx && y>=miny && y<=maxy) {
						chart[(MAXXSIZE)*y+x] = color;
					}
					fbits<<=1;
				}
			}
		}
	}
}

double charts_fixmax(uint64_t max,uint32_t ypts,uint8_t *scale,uint8_t *mode,uint16_t *base) {
	uint64_t cpmax,factor;
	uint8_t cmode,ascale;

	if (max==0) {
		max=1;
	}
	if (max<=9) {
		(*base) = ((max*100)+ypts-1)/ypts;
		if (((*base)*ypts)<1000) {
			(*mode) = 2;
			return ((*base)*ypts)/100;
		}
	}
	if (max<=99) {
		(*base) = ((max*10)+ypts-1)/ypts;
		if (((*base)*ypts)<1000) {
			(*mode) = 1;
			return ((*base)*ypts)/10;
		}
	}
	cpmax = 999;
	cmode = 0;
	ascale = 0;
	factor = ypts;
	while (1) {
		if (max<=cpmax) {
			(*base) = (max+factor-1)/factor;
			if (((*base)*ypts)<1000) {
				(*mode) = cmode;
				(*scale) += ascale;
				return ((*base)*factor);
			}
		}
		if (cmode==0) {
			cmode=2;
			ascale+=1;
		} else {
			cmode--;
		}
		factor *= 10U;
		if (cpmax*10U > cpmax) {
			cpmax *= 10U;
		} else {
			if (max+9<max) {
				(*base) = (max/10+factor/10-1)/(factor/10);
			} else {
				(*base) = ((max+9)/10+factor/10-1)/(factor/10);
			}
			(*mode) = cmode;
			(*scale) += ascale;
			return ((double)(*base)*(double)factor);
		}
	}
}

void charts_makechart(uint32_t type,uint32_t range,uint32_t width,uint32_t height) {
	static const uint8_t jtab[11]={MICRO,MILI,SPACE,KILO,MEGA,GIGA,TERA,PETA,EXA,ZETTA,YOTTA};
	int32_t i,j;
	uint32_t xy,xm,xd,xh,xs,xoff,xbold,ys,ypts;
	uint64_t max;
	double dmax;
	uint64_t d,c1d,c2d,c3d;
	uint64_t c1dispdata[MAXLENG];
	uint64_t c2dispdata[MAXLENG];
	uint64_t c3dispdata[MAXLENG];
	uint8_t scale,mode=0,percent=0;
	uint16_t base=0;
	uint8_t text[6];
	uint8_t colors;

	memset(chart,COLOR_TRANSPARENT,(MAXXSIZE)*(MAXYSIZE));

	colors = 0;
	if (charts_filltab(c1dispdata,range,type,1,width)) {
		colors = 1;
	}
	if (charts_filltab(c2dispdata,range,type,2,width)) {
		colors = 2;
	}
	if (charts_filltab(c3dispdata,range,type,3,width)) {
		colors = 3;
	}

	max = 0;
	for (i=0 ; i<(int32_t)width ; i++) {
		d = 0;
		if (colors>=1 && c1dispdata[i]!=CHARTS_NODATA) {
			d += c1dispdata[i];
		}
		if (colors>=2 && c2dispdata[i]!=CHARTS_NODATA) {
			d += c2dispdata[i];
		}
		if (colors>=3 && c3dispdata[i]!=CHARTS_NODATA) {
			d += c3dispdata[i];
		}
		if (d>max) {
			max=d;
		}
	}
	if (max>1000000000000000000ULL) {	// arithmetic overflow protection
		for (i=0 ; i<(int32_t)width ; i++) {
			if (colors>=1 && c1dispdata[i]!=CHARTS_NODATA) {
				c1dispdata[i]/=1000;
			}
			if (colors>=2 && c2dispdata[i]!=CHARTS_NODATA) {
				c2dispdata[i]/=1000;
			}
			if (colors>=3 && c3dispdata[i]!=CHARTS_NODATA) {
				c3dispdata[i]/=1000;
			}
		}
		max/=1000;
		scale=1;
	} else {
		scale=0;
	}

	// range scale
	if ((CHARTS_IS_DIRECT_STAT(type) && statdefs[type].mode==CHARTS_MODE_ADD) || (CHARTS_IS_EXTENDED_STAT(type) && estatdefs[CHARTS_EXTENDED_POS(type)].mode==CHARTS_MODE_ADD)) {
		switch (range) {
			case MEDIUMRANGE:
				max = (max+5)/6;
				break;
			case LONGRANGE:
				max = (max+29)/30;
				break;
			case VERYLONGRANGE:
				max = (max+1439)/(24*60);
				break;
		}
	}

	if (CHARTS_IS_DIRECT_STAT(type)) {
		scale += statdefs[type].scale;
		percent = statdefs[type].percent;
		max *= statdefs[type].multiplier;
		max /= statdefs[type].divisor;
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		scale += estatdefs[CHARTS_EXTENDED_POS(type)].scale;
		percent = estatdefs[CHARTS_EXTENDED_POS(type)].percent;
		max *= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
		max /= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
	}

	ypts = height/20;
	dmax = charts_fixmax(max,ypts,&scale,&mode,&base);

	if (CHARTS_IS_DIRECT_STAT(type)) {
		dmax *= statdefs[type].divisor;
		dmax /= statdefs[type].multiplier;
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		dmax *= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
		dmax /= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
	}

	// range scale
	if ((CHARTS_IS_DIRECT_STAT(type) && statdefs[type].mode==CHARTS_MODE_ADD) || (CHARTS_IS_EXTENDED_STAT(type) && estatdefs[CHARTS_EXTENDED_POS(type)].mode==CHARTS_MODE_ADD)) {
		switch (range) {
			case MEDIUMRANGE:
				dmax *= 6;
				break;
			case LONGRANGE:
				dmax *= 30;
				break;
			case VERYLONGRANGE:
				dmax *= (24*60);
				break;
		}
	}

//	m = 0;
	for (i=0 ; i<(int32_t)width ; i++) {
		j = 0;
		if (colors>=3 && c3dispdata[i]!=CHARTS_NODATA) {
			c3d = c3dispdata[i];
			j = 1;
		} else {
			c3d = 0;
		}
		if (colors>=2 && c2dispdata[i]!=CHARTS_NODATA) {
			c2d = c3d + c2dispdata[i];
			j = 1;
		} else {
			c2d = c3d;
		}
		if (colors>=1 && c1dispdata[i]!=CHARTS_NODATA) {
			c1d = c2d + c1dispdata[i];
			j = 1;
		} else {
			c1d = c2d;
		}

		if (j==0) {
			for (j=0 ; j<(int32_t)height ; j++) {
				chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = ((j+i)%3)?COLOR_BKG:COLOR_NODATA; //(((j+i)&3)&&((j+2+LENG-i)&3))?COLOR_BKG:COLOR_DATA1;
			}
		} else {
			c1d *= height;
			c1d /= dmax;
			c2d *= height;
			c2d /= dmax;
			c3d *= height;
			c3d /= dmax;

			j=0;
			while (height>=c1d+j) {
				chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_BKG;
				j++;
			}
			while (height>=c2d+j) {
#if VERSMAJ==1
				chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA1;
#else
				if (colors==1) {
					chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA_BEGIN + (height-1-j)*3*COLOR_DATA_RANGE/height;
				} else if (colors==2) {
					chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA_BEGIN + (height-1-j)*1.5*COLOR_DATA_RANGE/height;
				} else if (colors==3) {
					chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA_BEGIN + (height-1-j)*COLOR_DATA_RANGE/height;
				}
#endif
				j++;
			}
			while (height>=c3d+j) {
#if VERSMAJ==1
				chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA2;
#else
				if (colors==2) {
					chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA_BEGIN + 1.5*COLOR_DATA_RANGE + (height-1-j)*1.5*COLOR_DATA_RANGE/height;
				} else if (colors==3) {
					chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA_BEGIN + COLOR_DATA_RANGE + (height-1-j)*COLOR_DATA_RANGE/height;
				}
#endif
				j++;
			}
			while ((int32_t)height>j) {
#if VERSMAJ==1
				chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA3;
#else
				chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_DATA_BEGIN + 2*COLOR_DATA_RANGE + (height-1-j)*COLOR_DATA_RANGE/height;
#endif
				j++;
			}
		}
	}
	// axes
	for (i=-3 ; i<(int32_t)width+3 ; i++) {
		chart[(MAXXSIZE)*(height+YPOS)+(i+XPOS)] = COLOR_AXIS;
	}
	for (i=-2 ; i<(int32_t)height+5 ; i++) {
		chart[(MAXXSIZE)*(height-i+YPOS)+(XPOS-1)] = COLOR_AXIS;
		chart[(MAXXSIZE)*(height-i+YPOS)+(XPOS+width)] = COLOR_AXIS;
	}

	// x scale
	xy = xm = xd = xh = xs = 0;
	if (range<3) {
		if (range==2) {
			xs = 12;
			xoff = lnghalfhour%12;
			xbold = 4;
			xh = lnghalfhour/12;
			xd = lngmday;
			xm = lngmonth;
			xy = lngyear;
		} else if (range==1) {
			xs = 10;
			xoff = medmin/6;
			xbold = 6;
			xh = medhour;
		} else {
			xs = 60;
			xoff = shmin;
			xbold = 1;
			xh = shhour;
		}
//		k = MAXLENG;
		for (i=width-xoff-1 ; i>=0 ; i-=xs) {
			if (xh%xbold==0) {
				ys=2;
				if ((range==0 && xh%6==0) || (range==1 && xh==0) || (range==2 && xd==1)) {
					ys=1;
				}
				if (range<2) {
					text[0]=xh/10;
					text[1]=xh%10;
					text[2]=COLON;
					text[3]=0;
					text[4]=0;
					charts_puttext(XPOS+i-14,(YPOS+height)+4,COLOR_TEXT,text,5,XPOS,XPOS+width-1,0,MAXYSIZE-1);
				} else {
					text[0]=xm/10;
					text[1]=xm%10;
					text[2]=FDOT;
					text[3]=xd/10;
					text[4]=xd%10;
					charts_puttext(XPOS+i+10,(YPOS+height)+4,COLOR_TEXT,text,5,XPOS,XPOS+width-1,0,MAXYSIZE-1);
					xd--;
					if (xd==0) {
						xm--;
						if (xm==0) {
							xm=12;
							xy--;
						}
						xd = getmonleng(xy,xm);
					}
				}
				chart[(MAXXSIZE)*(YPOS+height+1)+(i+XPOS)] = COLOR_AXIS;
				chart[(MAXXSIZE)*(YPOS+height+2)+(i+XPOS)] = COLOR_AXIS;
			} else {
				ys=4;
			}
			for (j=0 ; j<(int32_t)height ; j+=ys) {
				if (ys>1 || (j%4)!=0) {
					chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_AUX;
				}
			}
			if (range<2) {
				if (xh==0) {
					xh=23;
				} else {
					xh--;
				}
			} else {
				if (xh==0) {
					xh=3;
				} else {
					xh--;
				}
			}
		}
		if (range==2) {
			i -= xs*xh;
			text[0]=xm/10;
			text[1]=xm%10;
			text[2]=FDOT;
			text[3]=xd/10;
			text[4]=xd%10;
			charts_puttext(XPOS+i+10,(YPOS+height)+4,COLOR_TEXT,text,5,XPOS,XPOS+width-1,0,MAXYSIZE-1);
		}
	} else {
		xy = lngyear;
		xm = lngmonth;
//		k = MAXLENG;
		for (i=width-lngmday ; i>=0 ; ) {
			text[0]=xm/10;
			text[1]=xm%10;
			charts_puttext(XPOS+i+(getmonleng(xy,xm)-11)/2+1,(YPOS+height)+4,COLOR_TEXT,text,2,XPOS,XPOS+width-1,0,MAXYSIZE-1);
			chart[(MAXXSIZE)*(YPOS+height+1)+(i+XPOS)] = COLOR_AXIS;
			chart[(MAXXSIZE)*(YPOS+height+2)+(i+XPOS)] = COLOR_AXIS;
			if (xm!=1) {
				for (j=0 ; j<(int32_t)height ; j+=2) {
					chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_AUX;
				}
			} else {
				for (j=0 ; j<(int32_t)height ; j++) {
					if ((j%4)!=0) {
						chart[(MAXXSIZE)*(j+YPOS)+(i+XPOS)] = COLOR_AUX;
					}
				}
			}
			xm--;
			if (xm==0) {
				xm=12;
				xy--;
			}
			i-=getmonleng(xy,xm);
//			k = i;
		}
		text[0]=xm/10;
		text[1]=xm%10;
		charts_puttext(XPOS+i+(getmonleng(xy,xm)-11)/2+1,(YPOS+height)+4,COLOR_TEXT,text,2,XPOS,XPOS+width-1,0,MAXYSIZE-1);
	}
	// y scale

/*
	// range scale
	if ((CHARTS_IS_DIRECT_STAT(type) && statdefs[type].mode==CHARTS_MODE_ADD) || (CHARTS_IS_EXTENDED_STAT(type) && estatdefs[CHARTS_EXTENDED_POS(type)].mode==CHARTS_MODE_ADD)) {
		switch (range) {
			case SHORTRANGE:
				ymax = max;
				break;
			case MEDIUMRANGE:
				ymax = max/6;
				break;
			case LONGRANGE:
				ymax = max/30;
				break;
			case VERYLONGRANGE:
				ymax = max/(24*60);
				break;
			default:
				ymax=0;
		}
	} else {
		ymax = max;
	}

	if (CHARTS_IS_DIRECT_STAT(type)) {
		scale = statdefs[type].scale;
		ymax *= statdefs[type].multiplier;
//		ymin *= statdefs[type].multiplier;
		ymax /= statdefs[type].divisor;
//		ymin /= statdefs[type].divisor;
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		scale = estatdefs[CHARTS_EXTENDED_POS(type)].scale;
		ymax *= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
//		ymin *= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
		ymax /= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
//		ymin /= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
	}
*/
//	for (i=0 ; i<LENG ; i+=2) {
//		for (j=DATA-20 ; j>=0 ; j-=20) {
//			chart[(XSIZE)*(j+YPOS)+(i+XPOS)] = 2;
//		}
//	}
	for (i=0 ; i<=(int32_t)ypts ; i++) {
		d = base*i;
		j=0;
		if (mode==0) {	// ###
			if (d>=10) {
				if (d>=100) {
					text[j++]=d/100;
					d%=100;
				}
				text[j++]=d/10;
			}
			text[j++]=d%10;
		} else if (mode==1) {	// ##.#
			if (d>=100) {
				text[j++]=d/100;
				d%=100;
			}
			text[j++]=d/10;
			text[j++]=FDOT;
			text[j++]=d%10;
		} else if (mode==2) {   // #.##
			text[j++]=d/100;
			d%=100;
			text[j++]=FDOT;
			text[j++]=d/10;
			text[j++]=d%10;
		}
		if (scale<11) {
			if (jtab[scale]!=SPACE) {
				text[j++]=jtab[scale];
			}
		} else {
			text[j++]=SQUARE;
		}
		if (percent) {
			text[j++]=PERCENT;
		}
		charts_puttext(XPOS - 4 - (j*6),(YPOS+height-(20*i))-3,COLOR_TEXT,text,j,0,MAXXSIZE-1,0,MAXYSIZE-1);
		chart[(MAXXSIZE)*(YPOS+height-20*i)+(XPOS-2)] = COLOR_AXIS;
		chart[(MAXXSIZE)*(YPOS+height-20*i)+(XPOS-3)] = COLOR_AXIS;
		if (i>0) {
			for (j=1 ; j<(int32_t)width ; j+=2) {
				chart[(MAXXSIZE)*(YPOS+height-20*i)+(XPOS+j)] = COLOR_AUX;
			}
		}
	}
}

uint32_t charts_monotonic_data (uint8_t *buff) {
	uint32_t i;
	if (buff==NULL) {
		return sizeof(uint16_t)+sizeof(uint64_t)*statdefscount;
	}
	put16bit(&buff,statdefscount);
	for (i=0 ; i<statdefscount ; i++) {
		put64bit(&buff,monotonic[i]);
	}
	return 0;
}

static inline void charts_statid_converter(uint32_t number,uint32_t *chtype,uint32_t *chrange) {
	uint32_t rmask;
	uint32_t i;
	if (number < 0x1000000) {
		*chtype = number / 10;
		*chrange = number % 10;
		return;
	} else {
		rmask = number & 0x20202020;
		rmask = ((rmask>>5)&1)|((rmask>>12)&2)|((rmask>>19)&4)|((rmask>>26)&8);
		number &= 0xDFDFDFDF;
		for (i=0 ; i<statdefscount ; i++) {
			if ((statdefs[i].statid&0xDFDFDFDF)==number) {
				*chtype = i;
				*chrange = rmask;
				return;
			}
		}
		for (i=0 ; i<estatdefscount ; i++) {
			if ((estatdefs[i].statid&0xDFDFDFDF)==number) {
				*chtype = CHARTS_EXTENDED_START+i;
				*chrange = rmask;
				return;
			}
		}
	}
	*chtype = 0xFFFFFFFF;
	*chrange = 0xFFFFFFFF;
	return;
}

double charts_data_multiplier(uint32_t type,uint32_t range) {
	double ret;
	ret = 1.0;
	if ((CHARTS_IS_DIRECT_STAT(type) && statdefs[type].mode==CHARTS_MODE_ADD) || (CHARTS_IS_EXTENDED_STAT(type) && estatdefs[CHARTS_EXTENDED_POS(type)].mode==CHARTS_MODE_ADD)) {
		switch (range) {
			case MEDIUMRANGE:
				ret /= 6.0;
				break;
			case LONGRANGE:
				ret /= 30.0;
				break;
			case VERYLONGRANGE:
				ret /= 1440.0;
				break;
		}
	}
	if (CHARTS_IS_DIRECT_STAT(type)) {
		ret *= statdefs[type].multiplier;
		ret /= statdefs[type].divisor;
		switch (statdefs[type].scale) {
			case CHARTS_SCALE_MICRO:
				ret /= 1000000.0;
				break;
			case CHARTS_SCALE_MILI:
				ret /= 1000.0;
				break;
			case CHARTS_SCALE_KILO:
				ret *= 1000.0;
				break;
			case CHARTS_SCALE_MEGA:
				ret *= 1000000.0;
				break;
			case CHARTS_SCALE_GIGA:
				ret *= 1000000000.0;
				break;
		}
	} else if (CHARTS_IS_EXTENDED_STAT(type)) {
		ret *= estatdefs[CHARTS_EXTENDED_POS(type)].multiplier;
		ret /= estatdefs[CHARTS_EXTENDED_POS(type)].divisor;
		switch (estatdefs[CHARTS_EXTENDED_POS(type)].scale) {
			case CHARTS_SCALE_MICRO:
				ret /= 1000000.0;
				break;
			case CHARTS_SCALE_MILI:
				ret /= 1000.0;
				break;
			case CHARTS_SCALE_KILO:
				ret *= 1000.0;
				break;
			case CHARTS_SCALE_MEGA:
				ret *= 1000000.0;
				break;
			case CHARTS_SCALE_GIGA:
				ret *= 1000000000.0;
				break;
		}
	}
	return ret;
}

uint32_t charts_getmaxleng(void) {
	return MAXLENG;
}

void charts_getdata(double *data,uint32_t *timestamp,uint32_t *rsec,uint32_t number) {
	uint32_t i,chtype,chrange;
	uint64_t tab1[MAXLENG];
	uint64_t tab2[MAXLENG];
	uint64_t tab3[MAXLENG];
	uint64_t d;
	uint32_t c;
	uint8_t nd;
	double mul;

	charts_statid_converter(number,&chtype,&chrange);
	if (data!=NULL && timestamp!=NULL && chrange<RANGES && (CHARTS_IS_DIRECT_STAT(chtype) || CHARTS_IS_EXTENDED_STAT(chtype))) {
#ifdef USE_PTHREADS
		zassert(pthread_mutex_lock(&glock));
#endif
		mul = charts_data_multiplier(chtype,chrange);
		c = 0;
		if (charts_filltab(tab1,chrange,chtype,1,MAXLENG)) {
			c = 1;
		}
		if (charts_filltab(tab2,chrange,chtype,2,MAXLENG)) {
			c = 2;
		}
		if (charts_filltab(tab3,chrange,chtype,3,MAXLENG)) {
			c = 3;
		}
		switch (chrange) {
			case SHORTRANGE:
				*rsec = 60;
				break;
			case MEDIUMRANGE:
				*rsec = 60*6;
				break;
			case LONGRANGE:
				*rsec = 60*30;
				break;
			case VERYLONGRANGE:
				*rsec = 60*60*24;
				break;
		}
		*timestamp = timepoint[chrange] * (*rsec);
		for (i=0 ; i<MAXLENG ; i++) {
			d = 0;
			nd = 1;
			if (c>=1 && tab1[i]!=CHARTS_NODATA) {
				d += tab1[i];
				nd = 0;
			}
			if (c>=2 && tab2[i]!=CHARTS_NODATA) {
				d += tab2[i];
				nd = 0;
			}
			if (c>=3 && tab3[i]!=CHARTS_NODATA) {
				d += tab3[i];
				nd = 0;
			}
			if (nd) {
				data[i] = -1.0;
			} else {
				data[i] = d * mul;
			}
		}
#ifdef USE_PTHREADS
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
}

uint32_t charts_makedata(uint8_t *buff,uint32_t number,uint32_t maxentries) {
	uint32_t i,j,ts,chtype,chrange;
	uint64_t *tab;

	charts_statid_converter(number,&chtype,&chrange);
	if (maxentries>MAXLENG) {
		maxentries = MAXLENG;
	}
	if (buff==NULL) {
		return (chrange<RANGES && CHARTS_IS_DIRECT_STAT(chtype))?maxentries*8+8:0;
	}
	if (chrange<RANGES && CHARTS_IS_DIRECT_STAT(chtype)) {
#ifdef USE_PTHREADS
		zassert(pthread_mutex_lock(&glock));
#endif
		tab = series[chtype][chrange];
		j = pointers[chrange] % MAXLENG;
		ts = timepoint[chrange];
		switch (chrange) {
			case SHORTRANGE:
				ts *= 60;
				break;
			case MEDIUMRANGE:
				ts *= 60*6;
				break;
			case LONGRANGE:
				ts *= 60*30;
				break;
			case VERYLONGRANGE:
				ts *= 60*60*24;
				break;
		}
		put32bit(&buff,ts);
		put32bit(&buff,maxentries);
		for (i=0 ; i<maxentries ; i++) {
			put64bit(&buff,tab[j]);
			if (j>0) {
				j--;
			} else {
				j = MAXLENG-1;
			}
		}
#ifdef USE_PTHREADS
		zassert(pthread_mutex_unlock(&glock));
#endif
	}
	return 0;
}

void charts_chart_to_rawchart(uint32_t chartwidth,uint32_t chartheight) {
	uint32_t y;
//	uint32_t x;
	uint8_t *cp,*rp;
	cp = chart;
	rp = rawchart;
	for (y=0 ; y<chartheight ; y++) {
		*rp=0;
		rp++;
		memcpy(rp,cp,chartwidth);
		rp+=chartwidth;
		cp+=MAXXSIZE;
	}
}

void charts_fill_crc(uint8_t *buff,uint32_t leng) {
	uint8_t *ptr,*eptr;
	uint32_t crc,chleng;
	ptr = buff+8;
	eptr = buff+leng;
	while (ptr+4<=eptr) {
		chleng = get32bit((const uint8_t **)&ptr);
		if (ptr+8+chleng<=eptr) {
			crc = mycrc32(0,ptr,chleng+4);
			ptr += chleng+4;
			if (memcmp(ptr,"CRC#",4)==0) {
				put32bit(&ptr,crc);
			} else {
				syslog(LOG_WARNING,"charts: unexpected data in generated png stream");
			}
		}
	}
}

#ifndef HAVE_ZLIB_H

#define MOD_ADLER 65521

static uint32_t charts_adler32(uint8_t *data,uint32_t len) {
	uint32_t a = 1, b = 0;
	uint32_t i;

	for (i=0 ; i<len ; i++) {
		a = (a + data[i]) % MOD_ADLER;
		b = (b + a) % MOD_ADLER;
	}

	return (b << 16) | a;
}

int charts_fake_compress(uint8_t *src,uint32_t srcsize,uint8_t *dst,uint32_t *dstsize) {
	uint32_t edstsize,adler;
	edstsize = 6+(65535+5)*(srcsize/65535);
	if (srcsize%65535) {
		edstsize+=5+(srcsize%65535);
	}
	if (edstsize>*dstsize) {
		return -1;
	}
	adler = charts_adler32(src,srcsize);
	*dst++=0x78;
	*dst++=0x9C;
	while (srcsize>65535) {
		*dst++ = 0x00;
		*dst++ = 0xFF;
		*dst++ = 0xFF;
		*dst++ = 0x00;
		*dst++ = 0x00;
		memcpy(dst,src,65535);
		dst+=65535;
		src+=65535;
		srcsize-=65535;
	}
	if (srcsize>0) {
		*dst++ = 0x01;
		*dst++ = srcsize&0xFF;
		*dst++ = srcsize>>8;
		*dst++ = (srcsize&0xFF)^0xFF;
		*dst++ = (srcsize>>8)^0xFF;
		memcpy(dst,src,srcsize);
		dst+=srcsize;
	}
	*dst++ = (adler>>24) & 0xFF;
	*dst++ = (adler>>16) & 0xFF;
	*dst++ = (adler>>8) & 0xFF;
	*dst++ = adler & 0xFF;
	*dstsize = edstsize;
	return 0;
}
#endif /* ! HAVE_ZLIB_H */

uint32_t charts_make_png(uint32_t number,uint32_t chartwidth,uint32_t chartheight) {
	uint32_t chtype,chrange;
	uint8_t *ptr;

#ifdef USE_PTHREADS
	zassert(pthread_mutex_lock(&glock));
#endif
	charts_statid_converter(number,&chtype,&chrange);
	if (chrange>=RANGES) {
		compsize = 0;
		return sizeof(png_1x1);
	}
	if (!(CHARTS_IS_DIRECT_STAT(chtype) || CHARTS_IS_EXTENDED_STAT(chtype))) {
		compsize = 0;
		return sizeof(png_1x1);
	}

	if (chartwidth==0 && chartheight==0) {
#if VERSMAJ==1
		chartwidth = 1000;
#else
		chartwidth = 950+XADD;
#endif
		chartheight = 100+YADD;
	}
	if (chartheight>MAXHEIGHT+YADD) {
		chartheight = MAXHEIGHT+YADD;
	}
	if (chartheight<MINHEIGHT+YADD) {
		chartheight = MINHEIGHT+YADD;
	}
	if (((chartheight-YADD) % 20)!=0) {
		chartheight -= ((chartheight-YADD) % 20);
	}
	if (chartwidth>MAXLENG+XADD) {
		chartwidth = MAXLENG+XADD;
	}
	if (chartwidth<MINLENG+XADD) {
		chartwidth = MINLENG+XADD;
	}

	charts_makechart(chtype,chrange,chartwidth-XADD,chartheight-YADD);
#ifndef HAVE_ZLIB_H
	charts_putwarning(XPOS+chartwidth-XADD-90,YPOS+5,COLOR_AXIS);
#endif
	charts_chart_to_rawchart(chartwidth,chartheight);

	ptr = png_header+16;
	put32bit(&ptr,chartwidth);
	put32bit(&ptr,chartheight);

#ifdef HAVE_ZLIB_H
	if (deflateReset(&zstr)!=Z_OK) {
		compsize = 0;
		return sizeof(png_1x1);
	}

//	syslog(LOG_NOTICE,"rawchartsize: %u ; compbuffsize: %u",rawchartsize,compbuffsize);
	zstr.next_in = rawchart;
	zstr.avail_in = rawchartsize;
	zstr.total_in = 0;
	zstr.next_out = compbuff;
	zstr.avail_out = compbuffsize;
	zstr.total_out = 0;

	if (deflate(&zstr,Z_FINISH)!=Z_STREAM_END) {
		compsize = 0;
		return sizeof(png_1x1);
	}

	compsize = zstr.total_out;
#else /* HAVE_ZLIB_H */
	compsize = compbuffsize;
	if (charts_fake_compress(rawchart,rawchartsize,compbuff,&compsize)<0) {
		compsize = 0;
		return sizeof(png_1x1);
	}
#endif /* HAVE_ZLIB_H */

	return sizeof(png_header)+compsize+sizeof(png_tailer);
}

void charts_get_png(uint8_t *buff) {
	uint8_t *ptr;
	if (compsize==0) {
		memcpy(buff,png_1x1,sizeof(png_1x1));
	} else {
		memcpy(buff,png_header,sizeof(png_header));
		ptr = buff+(sizeof(png_header)-8);
		put32bit(&ptr,compsize);
		memcpy(buff+sizeof(png_header),compbuff,compsize);
		memcpy(buff+sizeof(png_header)+compsize,png_tailer,sizeof(png_tailer));
		charts_fill_crc(buff,sizeof(png_header)+compsize+sizeof(png_tailer));
	}
	compsize=0;
#ifdef USE_PTHREADS
	zassert(pthread_mutex_unlock(&glock));
#endif
}
