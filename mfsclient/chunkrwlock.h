#ifndef _CHUNKRWLOCK_H_
#define _CHUNKRWLOCK_H_

#include <inttypes.h>

void chunkrwlock_init(void);
void chunkrwlock_term(void);
void chunkrwlock_rlock(uint32_t inode,uint32_t indx);
void chunkrwlock_runlock(uint32_t inode,uint32_t indx);
void chunkrwlock_wlock(uint32_t inode,uint32_t indx);
void chunkrwlock_wunlock(uint32_t inode,uint32_t indx);

#endif
