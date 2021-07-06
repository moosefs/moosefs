#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include <stdbool.h>
#include <assert.h>
#include <setjmp.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "CuTest.h"

#include "linked_list_queue.h"

#include "raft.h"
#include "mock_send_functions.h"

typedef struct
{
    void* outbox;
    void* inbox;
    void* raft;
} sender_t;

typedef struct
{
    void* data;
    int len;
    /* what type of message is it? */
    int type;
    /* who sent this? */
    raft_node_t* sender;
} msg_t;

static sender_t** __senders = NULL;
static int __nsenders = 0;

void senders_new()
{
    __senders = NULL;
    __nsenders = 0;
}

static int __append_msg(
    sender_t* me,
    void* data,
    int type,
    int len,
    raft_node_t* node,
    raft_server_t* raft
    )
{
    msg_t* m = malloc(sizeof(msg_t));
    m->type = type;
    m->len = len;
    m->data = malloc(len);
    m->sender = raft_get_node(raft, raft_get_nodeid(raft));
    memcpy(m->data, data, len);
    llqueue_offer(me->outbox, m);

    /* give to peer */
    sender_t* peer = raft_node_get_udata(node);
    if (peer)
    {
        msg_t* m2 = malloc(sizeof(msg_t));
        memcpy(m2, m, sizeof(msg_t));
        m2->sender = raft_get_node(peer->raft, raft_get_nodeid(raft));
        llqueue_offer(peer->inbox, m2);
    }

    return 1;
}

int sender_requestvote(raft_server_t* raft,
                       void* udata, raft_node_t* node, msg_requestvote_t* msg)
{
    return __append_msg(udata, msg, RAFT_MSG_REQUESTVOTE, sizeof(*msg), node,
                        raft);
}

int sender_requestvote_response(raft_server_t* raft,
                                void* udata, raft_node_t* node,
                                msg_requestvote_response_t* msg)
{
    return __append_msg(udata, msg, RAFT_MSG_REQUESTVOTE_RESPONSE, sizeof(*msg),
                        node, raft);
}

int sender_appendentries(raft_server_t* raft,
                         void* udata, raft_node_t* node, msg_appendentries_t* msg)
{
    msg_entry_t* entries = calloc(1, sizeof(msg_entry_t) * msg->n_entries);
    memcpy(entries, msg->entries, sizeof(msg_entry_t) * msg->n_entries);
    msg->entries = entries;
    return __append_msg(udata, msg, RAFT_MSG_APPENDENTRIES, sizeof(*msg), node,
                        raft);
}

int sender_appendentries_response(raft_server_t* raft,
                                  void* udata, raft_node_t* node,
                                  msg_appendentries_response_t* msg)
{
    return __append_msg(udata, msg, RAFT_MSG_APPENDENTRIES_RESPONSE,
                        sizeof(*msg), node, raft);
}

int sender_entries_response(raft_server_t* raft,
                            void* udata, raft_node_t* node, msg_entry_response_t* msg)
{
    return __append_msg(udata, msg, RAFT_MSG_ENTRY_RESPONSE, sizeof(*msg), node,
                        raft);
}

void* sender_new(void* address)
{
    sender_t* me = malloc(sizeof(sender_t));
    me->outbox = llqueue_new();
    me->inbox = llqueue_new();
    __senders = realloc(__senders, sizeof(sender_t*) * (++__nsenders));
    __senders[__nsenders - 1] = me;
    return me;
}

void* sender_poll_msg_data(void* s)
{
    sender_t* me = s;
    msg_t* msg = llqueue_poll(me->outbox);
    return NULL != msg ? msg->data : NULL;
}

void sender_set_raft(void* s, void* r)
{
    sender_t* me = s;
    me->raft = r;
}

int sender_msgs_available(void* s)
{
    sender_t* me = s;

    return 0 < llqueue_count(me->inbox);
}

void sender_poll_msgs(void* s)
{
    sender_t* me = s;
    msg_t* m;

    while ((m = llqueue_poll(me->inbox)))
    {
        switch (m->type)
        {
        case RAFT_MSG_APPENDENTRIES:
        {
            msg_appendentries_response_t response;
            raft_recv_appendentries(me->raft, m->sender, m->data, &response);
            __append_msg(me, &response, RAFT_MSG_APPENDENTRIES_RESPONSE,
                         sizeof(response), m->sender, me->raft);
        }
        break;
        case RAFT_MSG_APPENDENTRIES_RESPONSE:
            raft_recv_appendentries_response(me->raft, m->sender, m->data);
            break;
        case RAFT_MSG_REQUESTVOTE:
        {
            msg_requestvote_response_t response;
            raft_recv_requestvote(me->raft, m->sender, m->data, &response);
            __append_msg(me, &response, RAFT_MSG_REQUESTVOTE_RESPONSE,
                         sizeof(response), m->sender, me->raft);
        }
        break;
        case RAFT_MSG_REQUESTVOTE_RESPONSE:
            raft_recv_requestvote_response(me->raft, m->sender, m->data);
            break;
        case RAFT_MSG_ENTRY:
        {
            msg_entry_response_t response;
            raft_recv_entry(me->raft, m->data, &response);
            __append_msg(me, &response, RAFT_MSG_ENTRY_RESPONSE,
                         sizeof(response), m->sender, me->raft);
        }
        break;

        case RAFT_MSG_ENTRY_RESPONSE:
#if 0
            raft_recv_entry_response(me->raft, m->sender, m->data);
#endif
            break;
        }
    }
}
