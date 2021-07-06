#ifndef MOCK_SEND_FUNCTIONS_H
#define MOCK_SEND_FUNCTIONS_H

typedef enum
{
    RAFT_MSG_REQUESTVOTE,
    RAFT_MSG_REQUESTVOTE_RESPONSE,
    RAFT_MSG_APPENDENTRIES,
    RAFT_MSG_APPENDENTRIES_RESPONSE,
    RAFT_MSG_ENTRY,
    RAFT_MSG_ENTRY_RESPONSE,
} raft_message_type_e;

void senders_new();

void* sender_new(void* address);

void* sender_poll_msg_data(void* s);

void sender_poll_msgs(void* s);

int sender_msgs_available(void* s);

void sender_set_raft(void* s, void* r);

int sender_requestvote(raft_server_t* raft,
        void* udata, raft_node_t* node, msg_requestvote_t* msg);

int sender_requestvote_response(raft_server_t* raft,
        void* udata, raft_node_t* node, msg_requestvote_response_t* msg);

int sender_appendentries(raft_server_t* raft,
        void* udata, raft_node_t* node, msg_appendentries_t* msg);

int sender_appendentries_response(raft_server_t* raft,
        void* udata, raft_node_t* node, msg_appendentries_response_t* msg);

int sender_entries(raft_server_t* raft,
        void* udata, raft_node_t* node, msg_entry_t* msg);

int sender_entries_response(raft_server_t* raft,
        void* udata, raft_node_t* node, msg_entry_response_t* msg);

#endif /* MOCK_SEND_FUNCTIONS_H */
