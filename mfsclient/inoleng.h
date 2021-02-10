#ifndef _INOLENG_H_
#define _INOLENG_H_

#include <inttypes.h>

void* inoleng_acquire(uint32_t inode);
void inoleng_release(void *ptr);
uint64_t inoleng_getfleng(void *ptr);
void inoleng_setfleng(void *ptr,uint64_t fleng);
void inoleng_update_fleng(uint32_t inode,uint64_t fleng);
void inoleng_write_start(void *ptr);
void inoleng_write_end(void *ptr);
void inoleng_read_start(void *ptr);
void inoleng_read_end(void *ptr);
void inoleng_io_wait(void *ptr);
void inoleng_term(void);
void inoleng_init(void);

#endif
