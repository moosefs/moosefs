/*
 * Copyright (C) 2020 Jakub Kruszona-Zawadzki, Core Technology Sp. z o.o.
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

#ifndef _BIO_H_
#define _BIO_H_

#include <inttypes.h>

typedef struct _bio bio;

enum {BIO_READ,BIO_WRITE};

bio* bio_file_open(const char *fname,uint8_t direction,uint32_t buffersize);
bio* bio_socket_open(int socket,uint8_t direction,uint32_t buffersize,uint32_t msecto);
uint64_t bio_file_position(bio *b);
uint64_t bio_file_size(bio *b);
uint32_t bio_crc(bio *b);
int64_t bio_read(bio *b,void *dst,uint64_t len);
int64_t bio_write(bio *b,const void *src,uint64_t len);
int8_t bio_seek(bio *b,int64_t offset,int whence);
void bio_skip(bio *b,uint64_t len);
uint8_t bio_eof(bio *b);
uint8_t bio_error(bio *b);
int bio_descriptor(bio *b);
void bio_close(bio *b);


#endif
