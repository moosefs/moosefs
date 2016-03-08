#ifndef _EXTRAPACKETS_H_
#define _EXTRAPACKETS_H_

#include <inttypes.h>

void ep_chunk_has_changed(uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version,uint64_t fleng,uint8_t truncflag);
void ep_fleng_has_changed(uint32_t inode,uint64_t fleng);
void ep_term(void);
void ep_init(void);

#endif
