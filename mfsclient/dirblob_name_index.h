#ifndef _DIRBLOB_NAME_INDEX_H_
#define _DIRBLOB_NAME_INDEX_H_

#include <inttypes.h>

// BLOB (*ptr):
// nleng:8 name:nlengB inode:32 attr:...

void *name_index_create(uint32_t minelements);
void name_index_destroy(void *vidx);
void name_index_add(void *vidx, uint8_t *ptr);
uint8_t *name_index_find(void *vidx, const uint8_t *str, uint8_t len);

#endif
