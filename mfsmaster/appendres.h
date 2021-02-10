#ifndef _APPENDRES_H_
#define _APPENDRES_H_

#include <inttypes.h>

uint64_t appendres_getvleng(uint32_t inode);
void appendres_setvleng(uint32_t inode,uint64_t vlength);
void appendres_setrleng(uint32_t inode,uint64_t rlength);
void appendres_clear(uint32_t inode);
void appendres_cleanall(void);
void appendres_init(void);

#endif
