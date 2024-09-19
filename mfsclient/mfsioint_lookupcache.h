#ifndef _MFSIOINT_LOOKUPCACHE_H_
#define _MFSIOINT_LOOKUPCACHE_H_

#include "MFSCommunication.h"

#include <inttypes.h>

uint8_t lcache_path_lookup(uint32_t base_inode,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gidcnt,uint32_t *gidtab,uint32_t *parent_inode,uint32_t *last_inode,uint8_t *nleng,uint8_t name[256],uint8_t attr[ATTR_RECORD_SIZE]);
void lcache_path_invalidate(uint32_t base_inode,uint32_t pleng,const uint8_t *path);
void lcache_inode_invalidate(uint32_t inode);
void lcache_term(void);
int lcache_init(double lc_retention);

#endif
