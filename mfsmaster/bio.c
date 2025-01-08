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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>

#include "bio.h"
#include "massert.h"
#include "sockets.h"
#include "strerr.h"
#include "crc.h"

#define BIO_TYPE_FILE 0
#define BIO_TYPE_SOCKET 1
#define BIO_TYPE_NULL 2

struct _bio {
	uint8_t *buff;
	uint32_t size;
	uint32_t leng;
	uint32_t pos;
	uint32_t msecto;
	uint64_t fileposition;
	uint32_t crc;
	uint8_t direction;
	uint8_t type;
	uint8_t error;
	uint8_t eof;
	int lasterrno;
	int fd;
};

bio* bio_null_open(uint8_t direction) {
	bio *b;
	b = malloc(sizeof(bio));
	passert(b);
	b->buff = NULL;
	b->size = 0;
	b->leng = 0;
	b->pos = 0;
	b->msecto = 0;
	b->fileposition = 0;
	b->crc = 0;
	b->direction = direction;
	b->type = BIO_TYPE_NULL;
	b->error = 0;
	b->eof = 0;
	b->lasterrno = 0;
	b->fd = -1;
	return b;
}

bio* bio_file_open(const char *fname,uint8_t direction,uint32_t buffersize) {
	int fd;
	bio *b;
	if (direction==BIO_READ) {
		fd = open(fname,O_RDONLY);
	} else if (direction==BIO_WRITE) {
		fd = open(fname,O_WRONLY | O_CREAT | O_TRUNC,0666);
	} else {
		return NULL;
	}
	if (fd<0) {
		return NULL;
	}
	b = malloc(sizeof(bio));
	passert(b);
	b->buff = malloc(buffersize);
	passert(b->buff);
	b->size = buffersize;
	b->leng = 0;
	b->pos = 0;
	b->msecto = 0;
	b->fileposition = 0;
	b->crc = 0;
	b->direction = direction;
	b->type = BIO_TYPE_FILE;
	b->error = 0;
	b->eof = 0;
	b->lasterrno = 0;
	b->fd = fd;
	return b;
}

bio* bio_socket_open(int socket,uint8_t direction,uint32_t buffersize,uint32_t msecto) {
	bio *b;
	b = malloc(sizeof(bio));
	passert(b);
	b->buff = malloc(buffersize);
	passert(b->buff);
	b->size = buffersize;
	b->leng = 0;
	b->pos = 0;
	b->msecto = msecto;
	b->fileposition = 0;
	b->crc = 0;
	b->direction = direction;
	b->type = BIO_TYPE_SOCKET;
	b->error = 0;
	b->eof = 0;
	b->lasterrno = 0;
	b->fd = socket;
	return b;
}

static inline int32_t bio_internal_write(bio *b,const uint8_t *buff,uint32_t leng) {
	int32_t ret;
	if (b->type==BIO_TYPE_FILE) {
		ret = write(b->fd,buff,leng);
	} else if (b->type==BIO_TYPE_SOCKET) {
		ret = tcptowrite(b->fd,buff,leng,b->msecto,30*b->msecto);
//		if ((int32_t)leng!=ret) {
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"write %"PRIu32" -> %"PRId32" (error:%s)",leng,ret,strerr(errno));
//		}
	} else {
		ret = leng;
	}
	if (ret<(int32_t)leng) {
		if (b->lasterrno==0) {
			b->lasterrno = errno;
		}
		b->error=1;
		if (ret<0) {
			return 0;
		}
	}
	b->fileposition += ret;
	return ret;
}

static inline int32_t bio_internal_read(bio *b,uint8_t *buff,uint32_t leng) {
	int32_t ret;
	if (b->type==BIO_TYPE_FILE) {
		ret = read(b->fd,buff,leng);
	} else if (b->type==BIO_TYPE_SOCKET) {
		ret = tcptoread(b->fd,buff,leng,b->msecto,30*b->msecto);
//		if ((int32_t)leng!=ret) {
//			mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"read %"PRIu32" -> %"PRId32" (error:%s)\n",leng,ret,strerr(errno));
//		}
	} else {
		ret = 0;
	}
	if (ret<0) {
		if (b->lasterrno==0) {
			b->lasterrno = errno;
		}
		b->error = 1;
		return 0;
	} else if (ret==0) {
		b->eof = 1;
	}
	b->fileposition += ret;
	return ret;
}

static inline int bio_flush(bio *b) {
	if (b->direction==BIO_READ || b->error) {
		return -1;
	}
	if (b->type==BIO_TYPE_NULL) {
		return 0;
	}
	if (b->leng>0) {
		bio_internal_write(b,b->buff,b->leng);
		b->leng = 0;
	}
	return 0;
}

static inline int bio_fill(bio *b) {
	if (b->direction==BIO_WRITE || b->error || b->eof) {
		return -1;
	}
	if (b->type==BIO_TYPE_NULL) {
		return 0;
	}
	if (b->pos<b->leng) {
		memmove(b->buff,b->buff+b->pos,b->leng-b->pos);
		b->leng -= b->pos;
		b->pos = 0;
	} else {
		b->leng = bio_internal_read(b,b->buff,b->size);
		b->pos = 0;
	}
	return 0;
}

uint64_t bio_file_position(bio *b) {
	if (b->type!=BIO_TYPE_FILE) {
		return 0;
	}
	if (b->direction==BIO_WRITE) {
		return b->fileposition + b->leng;
	} else if (b->direction==BIO_READ) {
		return b->fileposition - (b->leng-b->pos);
	}
	return 0;
}

uint64_t bio_file_size(bio *b) {
	struct stat st;

	if (b->type!=BIO_TYPE_FILE) {
		return 0;
	}
	if (b->direction==BIO_WRITE) {
		bio_flush(b);
	}
	if (fstat(b->fd,&st)<0) {
		return 0;
	}
	return st.st_size;
}

uint32_t bio_crc(bio *b) {
	if (b->direction==BIO_WRITE) {
		uint32_t ret = b->crc;
		b->crc = 0;
		return ret;
	}
	return 0;
}

int64_t bio_read(bio *b,void *vdst,uint64_t len) {
	int64_t ret,i;
	uint8_t *dst = (uint8_t*)vdst;
	if (b->direction==BIO_WRITE || b->error || b->eof) {
		return -1;
	}
	if (b->type==BIO_TYPE_NULL) {
		return 0;
	}
	if (len>=b->size) {
		if (b->leng>b->pos) {
			memcpy(dst,b->buff+b->pos,b->leng-b->pos);
			ret = b->leng-b->pos;
			b->pos = b->leng;
		} else {
			ret = 0;
		}
		while (len-ret > 0x40000000) {
			i = bio_internal_read(b,dst+ret,0x40000000);
			ret += i;
			if (i<0x40000000) {
				return ret;
			}
		}
		if (len-ret==0) {
			return len;
		}
		return ret+bio_internal_read(b,dst+ret,len-ret);
	} else {
		if (b->leng==b->pos) {
			if (bio_fill(b)<0) {
				return -1;
			}
		}
		if (b->leng-b->pos < len) {
			memcpy(dst,b->buff+b->pos,b->leng-b->pos);
			ret = b->leng-b->pos;
			b->pos = b->leng;
			if (bio_fill(b)<0) {
				return -1;
			}
			if (b->leng < len-ret) { // eof
				memcpy(dst+ret,b->buff,b->leng);
				b->pos = b->leng;
				return ret+b->leng;
			} else {
				memcpy(dst+ret,b->buff,len-ret);
				b->pos += len-ret;
				return len;
			}
		} else {
			if (len>0) {
				memcpy(dst,b->buff+b->pos,len);
				b->pos += len;
			}
			return len;
		}
	}
}

int64_t bio_write(bio *b,const void *vsrc,uint64_t len) {
	int64_t ret,i;
	const uint8_t *src = (const uint8_t*)vsrc;
	if (b->direction==BIO_READ || b->error) {
		return -1;
	}
	b->crc ^= mycrc32(0,src,len);
	if (b->type==BIO_TYPE_NULL) { // bio_null - just calculate crc
		return len;
	}
	if (len>=b->size) {
		if (bio_flush(b)<0) {
			return -1;
		}
		ret = 0;
		while (len-ret > 0x40000000) {
			i = bio_internal_write(b,src+ret,0x40000000);
			if (i<0x40000000) {
				return -1;
			}
			ret += i;
		}
		if (len-ret==0) {
			return len;
		}
		i = bio_internal_write(b,src+ret,len-ret);
		if (i<(int64_t)(len-ret)) {
			return -1;
		}
		return len;
	} else {
		if (b->leng==b->size) {
			if (bio_flush(b)<0) {
				return -1;
			}
		}
		if (b->size-b->leng < len) {
			memcpy(b->buff+b->leng,src,b->size-b->leng);
			ret = b->size-b->leng;
			b->leng = b->size;
			if (bio_flush(b)<0) {
				return -1;
			}
			memcpy(b->buff,src+ret,len-ret);
			b->leng = len-ret;
			return len;
		} else {
			if (len>0) {
				memcpy(b->buff+b->leng,src,len);
				b->leng += len;
			}
			return len;
		}
	}
}

int8_t bio_seek(bio *b,int64_t offset,int whence) {
	int64_t p;
	if (b->type!=BIO_TYPE_FILE) {
		return -1;
	}
	if (b->direction==BIO_WRITE) {
		if (bio_flush(b)<0) {
			return -1;
		}
	} else if (b->direction==BIO_READ) {
		b->leng = 0;
		b->pos = 0;
	}
	p = lseek(b->fd,offset,whence);
	if (p<0) {
		return -1;
	}
	b->fileposition = p;
	return 0;
}

void bio_skip(bio *b,uint64_t len) {
	if (b->direction==BIO_WRITE) {
		return;
	}
	if (b->leng-b->pos >= len) {
		b->pos += len;
		return;
	} else {
		if (b->type!=BIO_TYPE_FILE) {
			while (len>0) {
				if (b->leng==b->pos) {
					if (bio_fill(b)<0) {
						return;
					}
				}
				if (b->leng-b->pos >= len) {
					b->pos += len;
					return;
				} else {
					len -= (b->leng-b->pos);
					b->pos = b->leng;
				}
			}
			return;
		} else {
			bio_seek(b,len,SEEK_CUR);
		}
	}
}

uint8_t bio_eof(bio *b) {
	return b->eof;
}

uint8_t bio_error(bio *b) {
	return b->error;
}

int bio_lasterrno(bio *b) {
	return b->lasterrno;
}

int bio_descriptor(bio *b) {
	return (b->fd);
}

void bio_sync(bio *b) {
	if (b->direction==BIO_WRITE) {
		bio_flush(b);
	}
}

void bio_shutdown(bio *b) {
	if (b->direction==BIO_WRITE) {
		bio_flush(b);
		if (b->type==BIO_TYPE_SOCKET) {
			tcpshutdown(b->fd);
		}
	}
}

void bio_wait(bio *b) {
	if (b->error==0 && b->direction==BIO_WRITE && b->type==BIO_TYPE_SOCKET) {
		tcptowait(b->fd,30*b->msecto);
	}
}

void bio_close(bio *b) {
	if (b->direction==BIO_WRITE) {
		bio_flush(b);
	}
	if (b->type==BIO_TYPE_FILE) {
		close(b->fd);
	} else if (b->type==BIO_TYPE_SOCKET) {
		tcpclose(b->fd);
	}
	if (b->buff!=NULL) {
		free(b->buff);
	}
	free(b);
}
