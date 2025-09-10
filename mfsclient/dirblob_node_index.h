#ifndef _DIRBLOB_NODE_INDEX_H_
#define _DIRBLOB_NODE_INDEX_H_

#include <inttypes.h>

// BLOB (*ptr):
// nleng:8 name:nlengB inode:32 attr:...

void *node_index_create(uint32_t minelements);
void node_index_destroy(void *vidx);
void node_index_add(void *vidx, uint8_t *ptr);
void node_index_remove(void *vidx, uint32_t node);
uint8_t *node_index_find(void *vidx, uint32_t node);

#endif
