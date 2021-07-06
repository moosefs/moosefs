/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Representation of a peer
 * @author Willem Thiart himself@willemthiart.com
 * @version 0.1
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "raft.h"

#define RAFT_NODE_VOTED_FOR_ME        (1 << 0)
#define RAFT_NODE_HAS_SUFFICIENT_LOG  (1 << 1)

typedef struct
{
    raft_server_t* server;
    void* udata;

    raft_index_t next_idx;
    raft_index_t match_idx;
    /* index of last offered log entry for this node */
    raft_index_t offered_idx;
    /* index of last committed log entry for this node */
    raft_index_t applied_idx;

    int flags;

    raft_node_id_t id;
} raft_node_private_t;

raft_node_t* raft_node_new(void* udata, raft_node_id_t id)
{
    raft_node_private_t* me;
    me = (raft_node_private_t*)calloc(1, sizeof(raft_node_private_t));
    if (!me)
        return NULL;
    me->udata = udata;
    me->next_idx = 1;
    me->match_idx = 0;
    me->offered_idx = -1;
    me->applied_idx = -2; /* start with addition not committed */
    me->id = id;
    me->flags = 0;
    return (raft_node_t*)me;
}

void raft_node_free(raft_node_t* me_)
{
    free(me_);
}

void raft_node_set_server(raft_node_t* me_, raft_server_t *server)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->server = server;
}

raft_index_t raft_node_get_next_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->next_idx;
}

void raft_node_set_next_idx(raft_node_t* me_, raft_index_t nextIdx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    /* log index begins at 1 */
    me->next_idx = nextIdx < 1 ? 1 : nextIdx;
}

raft_index_t raft_node_get_match_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->match_idx;
}

void raft_node_set_match_idx(raft_node_t* me_, raft_index_t matchIdx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->match_idx = matchIdx;
}

raft_index_t raft_node_get_offered_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->offered_idx;
}

void raft_node_set_offered_idx(raft_node_t* me_, raft_index_t offeredIdx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->offered_idx = offeredIdx;
}

raft_index_t raft_node_get_applied_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->applied_idx;
}

void raft_node_set_applied_idx(raft_node_t* me_, raft_index_t appliedIdx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->applied_idx = appliedIdx;
}

void* raft_node_get_udata(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->udata;
}

void raft_node_set_udata(raft_node_t* me_, void* udata)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->udata = udata;
}

void raft_node_vote_for_me(raft_node_t* me_, const int vote)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (vote)
        me->flags |= RAFT_NODE_VOTED_FOR_ME;
    else
        me->flags &= ~RAFT_NODE_VOTED_FOR_ME;
}

int raft_node_has_vote_for_me(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_VOTED_FOR_ME) != 0;
}

int raft_node_has_sufficient_logs(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_HAS_SUFFICIENT_LOG) != 0;
}

void raft_node_set_has_sufficient_logs(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->flags |= RAFT_NODE_HAS_SUFFICIENT_LOG;
}

int raft_node_is_voting(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (!me)
        return 0;
    if (me->offered_idx == -1)
        return raft_node_is_voting_committed(me_);
    raft_entry_t *ety = raft_get_entry_from_idx(me->server, me->offered_idx);
    return (!ety || ety->type == RAFT_LOGTYPE_ADD_NODE);
}

int raft_node_is_active(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (me->offered_idx == -1)
        return raft_node_is_addition_committed(me_);
    raft_entry_t *ety = raft_get_entry_from_idx(me->server, me->offered_idx);
    /* A node is active by default, unless explicitly marked for removal */
    return (!ety || ety->type != RAFT_LOGTYPE_REMOVE_NODE);
}

int raft_node_is_voting_committed(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (me->applied_idx < 0)
        return 0;
    raft_entry_t *ety = raft_get_entry_from_idx(me->server, me->applied_idx);
    return (!ety || ety->type == RAFT_LOGTYPE_ADD_NODE);
}

int raft_node_is_addition_committed(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (me->applied_idx < -1)
        return 0;
    raft_entry_t *ety = raft_get_entry_from_idx(me->server, me->applied_idx);
    /* Addition is committed if highest applied entry is not for removal */
    return (!ety || ety->type != RAFT_LOGTYPE_REMOVE_NODE);
}

raft_node_id_t raft_node_get_id(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (NULL == me) ? -1 : me->id;
}
