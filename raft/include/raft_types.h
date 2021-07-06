
#ifndef RAFT_DEFS_H_
#define RAFT_DEFS_H_

#include <stdint.h>

/**
 * Unique entry ids are mostly used for debugging and nothing else,
 * so there is little harm if they collide.
 */
typedef int raft_entry_id_t;

/**
 * Monotonic term counter.
 */
typedef long int raft_term_t;

/**
 * Monotonic log entry index.
 *
 * This is also used to as an entry count size type.
 */
typedef long int raft_index_t;

/**
 * Unique node identifier.
 */
typedef int raft_node_id_t;

#endif  /* RAFT_DEFS_H_ */
