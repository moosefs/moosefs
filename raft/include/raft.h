/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */

#ifndef RAFT_H_
#define RAFT_H_

#include "raft_types.h"

typedef enum {
    RAFT_ERR_NOT_LEADER=-2,
    RAFT_ERR_ONE_VOTING_CHANGE_ONLY=-3,
    RAFT_ERR_SHUTDOWN=-4,
    RAFT_ERR_NOMEM=-5,
    RAFT_ERR_NEEDS_SNAPSHOT=-6,
    RAFT_ERR_SNAPSHOT_IN_PROGRESS=-7,
    RAFT_ERR_SNAPSHOT_ALREADY_LOADED=-8,
    RAFT_ERR_LAST=-100,
} raft_error_e;

typedef enum {
    RAFT_MEMBERSHIP_ADD,
    RAFT_MEMBERSHIP_REMOVE,
} raft_membership_e;

#define RAFT_REQUESTVOTE_ERR_GRANTED          1
#define RAFT_REQUESTVOTE_ERR_NOT_GRANTED      0
#define RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE    -1

typedef enum {
    RAFT_STATE_NONE,
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_LEADER
} raft_state_e;

typedef enum {
    /**
     * Regular log type.
     * This is solely for application data intended for the FSM.
     */
    RAFT_LOGTYPE_NORMAL,
    /**
     * Membership change.
     * Non-voting nodes can't cast votes or start elections.
     * Nodes in this non-voting state are used to catch up with the cluster,
     * when trying to the join the cluster.
     */
    RAFT_LOGTYPE_ADD_NONVOTING_NODE,
    /**
     * Membership change.
     * Add a voting node.
     */
    RAFT_LOGTYPE_ADD_NODE,
    /**
     * Membership change.
     * Nodes become demoted when we want to remove them from the cluster.
     * Demoted nodes can't take part in voting or start elections.
     * Demoted nodes become inactive, as per raft_node_is_active.
     */
    RAFT_LOGTYPE_DEMOTE_NODE,
    /**
     * Membership change.
     * The node is removed from the cluster.
     * This happens after the node has been demoted.
     * Removing nodes is a 2 step process: first demote, then remove.
     */
    RAFT_LOGTYPE_REMOVE_NODE,
    /**
     * Users can piggyback the entry mechanism by specifying log types that
     * are higher than RAFT_LOGTYPE_NUM.
     */
    RAFT_LOGTYPE_NUM=100,
} raft_logtype_e;

typedef struct
{
    void *buf;

    unsigned int len;
} raft_entry_data_t;

/** Entry that is stored in the server's entry log. */
typedef struct
{
    /** the entry's term at the point it was created */
    raft_term_t term;

    /** the entry's unique ID */
    raft_entry_id_t id;

    /** type of entry */
    int type;

    raft_entry_data_t data;
} raft_entry_t;

/** Message sent from client to server.
 * The client sends this message to a server with the intention of having it
 * applied to the FSM. */
typedef raft_entry_t msg_entry_t;

/** Entry message response.
 * Indicates to client if entry was committed or not. */
typedef struct
{
    /** the entry's unique ID */
    raft_entry_id_t id;

    /** the entry's term */
    raft_term_t term;

    /** the entry's index */
    raft_index_t idx;
} msg_entry_response_t;

/** Vote/prevote request message.
 * Sent to nodes when a server wants to become leader.
 * This message could force a leader/candidate to become a follower. */
typedef struct
{
    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** candidate requesting vote */
    raft_node_id_t candidate_id;

    /** index of candidate's last log entry */
    raft_index_t last_log_idx;

    /** term of candidate's last log entry */
    raft_term_t last_log_term;

    /** true if this is a prevote request */
    int prevote;
} msg_requestvote_t;

/** Vote/prevote response message.
 * Indicates if node has accepted, or would accept, the server's vote request. */
typedef struct
{
    /** currentTerm, for candidate to update itself */
    raft_term_t term;

    /** true means candidate received vote */
    int vote_granted;

    /** true if this is a prevote response */
    int prevote;
} msg_requestvote_response_t;

/** Appendentries message.
 * This message is used to tell nodes if it's safe to apply entries to the FSM.
 * Can be sent without any entries as a keep alive message.
 * This message could force a leader/candidate to become a follower. */
typedef struct
{
    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** the index of the log just before the newest entry for the node who
     * receives this message */
    raft_index_t prev_log_idx;

    /** the term of the log just before the newest entry for the node who
     * receives this message */
    raft_term_t prev_log_term;

    /** the index of the entry that has been appended to the majority of the
     * cluster. Entries up to this index will be applied to the FSM */
    raft_index_t leader_commit;

    /** number of entries within this message */
    int n_entries;

    /** array of entries within this message */
    msg_entry_t* entries;
} msg_appendentries_t;

/** Appendentries response message.
 * Can be sent without any entries as a keep alive message.
 * This message could force a leader/candidate to become a follower. */
typedef struct
{
    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** true if follower contained entry matching prevLogidx and prevLogTerm */
    int success;

    /* Non-Raft fields follow: */
    /* Having the following fields allows us to do less book keeping in
     * regards to full fledged RPC */

    /** If success, this is the highest log IDX we've received and appended to
     * our log; otherwise, this is the our currentIndex */
    raft_index_t current_idx;

    /** The first idx that we received within the appendentries message */
    raft_index_t first_idx;
} msg_appendentries_response_t;

/** InstallSnapshot request message. */
typedef struct
{
    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** Index of the last entry represented by this snapshot */
    raft_index_t last_idx;

    /** Term of the last entry represented by this snapshot */
    raft_term_t last_term;
} msg_installsnapshot_t;

/** InstallSnapshot response message. */
typedef struct
{
    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** Same with the one in the request message */
    raft_index_t last_idx;

    /** True if the snapshot has been fully received */
    int complete;
} msg_installsnapshot_response_t;

typedef void* raft_server_t;
typedef void* raft_node_t;

/** Callback for sending request vote messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that we are sending this message to
 * @param[in] msg The request vote message to be sent
 * @return 0 on success */
typedef int (
*func_send_requestvote_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    msg_requestvote_t* msg
    );

/** Callback for sending append entries messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that we are sending this message to
 * @param[in] msg The appendentries message to be sent
 * @return 0 on success */
typedef int (
*func_send_appendentries_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    msg_appendentries_t* msg
    );

/** Callback for sending InstallSnapshot request messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that we are sending this message to
 * @param[in] msg The InstallSnapshot message to be sent
 * @return 0 on success */
typedef int (
*func_send_installsnapshot_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    msg_installsnapshot_t* msg
    );

/** Callback for receiving InstallSnapshot request messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that we are receiving this message from
 * @param[in] msg The InstallSnapshot message
 * @param[in] r The InstallSnapshot response to be sent
 * @return
 *  0 if this chunk is successful received
 *  1 if the whole snapshot is successfully received
 *  or an error */
typedef int (
*func_recv_installsnapshot_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    msg_installsnapshot_t* msg,
    msg_installsnapshot_response_t* r
    );

/** Callback for receiving InstallSnapshot response messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that we are sending this message to
 * @param[in] msg The InstallSnapshot message
 * @param[in] r The InstallSnapshot response to be sent
 * @return
 *  0 if this chunk is successful received
 *  1 if the whole snapshot is successfully received
 *  or an error */
typedef int (
*func_recv_installsnapshot_response_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    msg_installsnapshot_response_t* r
    );

/** Callback for detecting when non-voting nodes have obtained enough logs.
 * This triggers only when there are no pending configuration changes.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node
 * @return 0 does not want to be notified again; otherwise -1 */
typedef int (
*func_node_has_sufficient_logs_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node
    );

#ifndef HAVE_FUNC_LOG
#define HAVE_FUNC_LOG
/** Callback for providing debug logging information.
 * This callback is optional
 * @param[in] raft The Raft server making this callback
 * @param[in] node The node that is the subject of this log. Could be NULL.
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] buf The buffer that was logged */
typedef void (
*func_log_f
)    (
    raft_server_t* raft,
    raft_node_t* node,
    void *user_data,
    const char *buf
    );
#endif

/** Callback for saving who we voted for to disk.
 * For safety reasons this callback MUST flush the change to disk.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] vote The node we voted for
 * @return 0 on success */
typedef int (
*func_persist_vote_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_id_t vote
    );

/** Callback for saving current term (and nil vote) to disk.
 * For safety reasons this callback MUST flush the term and vote changes to
 * disk atomically.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] term Current term
 * @param[in] vote The node value dicating we haven't voted for anybody
 * @return 0 on success */
typedef int (
*func_persist_term_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_term_t term,
    raft_node_id_t vote
    );

/** Callback for saving changes to a range of log entries.
 *
 * This callback is used for:
 * <ul>
 *      <li>Appending entries to the log (ie. offer)</li>
 *      <li>Removing entries from the head of the log (ie. polling)</li>
 *      <li>Removing entries from the tail of the log (ie. popping)</li>
 *      <li>Applying entries</li>
 * </ul>
 *
 * For safety reasons this callback MUST flush the changes to disk.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entries The array of entries that the event is happening to.
 *    For offering, polling, and popping, the user is allowed to change the
 *    memory pointed to in the raft_entry_data_t struct. This MUST be done if
 *    the memory is temporary.
 * @param[in] entry_idx The lower bound of the index range (ie. the index of entries[0])
 * @param[in,out] n_entries The number of entries in the range
 * @return 0 on success */
typedef int (
*func_logentries_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entries,
    raft_index_t entry_idx,
    int *n_entries
    );

/** Callback for saving changes to one log entry. See also
 * func_logentries_event_f. */
typedef int (
*func_logentry_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
    );

/** Callback for being notified of membership changes.
 *
 * Implementing this callback is optional.
 *
 * Remove notification happens before the node is about to be removed.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that is the subject of this log. Could be NULL.
 * @param[in] entry The entry that was the trigger for the event. Could be NULL.
 * @param[in] type The type of membership change */
typedef void (
*func_membership_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t *node,
    raft_entry_t *entry,
    raft_membership_e type
    );

typedef struct
{
    /** Callback for sending request vote messages */
    func_send_requestvote_f send_requestvote;

    /** Callback for sending appendentries messages */
    func_send_appendentries_f send_appendentries;

    /** Callback for sending InstallSnapshot messages */
    func_send_installsnapshot_f send_installsnapshot;

    /** Callback for receiving InstallSnapshot messages */
    func_recv_installsnapshot_f recv_installsnapshot;

    /** Callback for receiving InstallSnapshot responses */
    func_recv_installsnapshot_response_f recv_installsnapshot_response;

    /** Callback for finite state machine application
     * Return 0 on success.
     * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
    func_logentry_event_f applylog;

    /** Callback for persisting vote data
     * For safety reasons this callback MUST flush the change to disk. */
    func_persist_vote_f persist_vote;

    /** Callback for persisting term (and nil vote) data
     * For safety reasons this callback MUST flush the term and vote changes to
     * disk atomically. */
    func_persist_term_f persist_term;

    /** Callback for adding entries to the log
     * For safety reasons this callback MUST flush the change to disk.
     * Return 0 on success.
     * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
    func_logentries_event_f log_offer;

    /** Callback for removing entries from the log head
     * For safety reasons this callback MUST flush the change to disk.
     * @note If memory was malloc'd in log_offer then this should be the right
     *  time to free the memory. */
    func_logentries_event_f log_poll;

    /** Callback for removing entries from the tail of the log in
     * descending order.
     * For safety reasons this callback MUST flush the change to disk.
     * @note If memory was malloc'd in log_offer then this should be the right
     *  time to free the memory. */
    func_logentries_event_f log_pop;

    /** Callback for determining which node this configuration log entry
     * affects. This call only applies to configuration change log entries.
     * @return the node ID of the node */
    func_logentry_event_f log_get_node_id;

    /** Callback for detecting when a non-voting node has sufficient logs. */
    func_node_has_sufficient_logs_f node_has_sufficient_logs;

    func_membership_event_f notify_membership_event;

    /** Callback for catching debugging log messages
     * This callback is optional */
    func_log_f log;
} raft_cbs_t;

typedef struct
{
    /** User data pointer for addressing.
     * Examples of what this could be:
     * - void* pointing to implementor's networking data
     * - a (IP,Port) tuple */
    void* udata_address;
} raft_node_configuration_t;

/** Initialise a new Raft server.
 *
 * Request timeout defaults to 200 milliseconds
 * Election timeout defaults to 1000 milliseconds
 *
 * @return newly initialised Raft server */
raft_server_t* raft_new();

/** De-initialise Raft server.
 * Frees all memory */
void raft_free(raft_server_t* me);

/** De-initialise Raft server. */
void raft_clear(raft_server_t* me);

/** Set callbacks and user data.
 *
 * @param[in] funcs Callbacks
 * @param[in] user_data "User data" - user's context that's included in a callback */
void raft_set_callbacks(raft_server_t* me, raft_cbs_t* funcs, void* user_data);

/** Add node.
 *
 * If a voting node already exists the call will fail.
 *
 * @note The order this call is made is important.
 *  This call MUST be made in the same order as the other raft nodes.
 *  This is because the node ID is assigned depending on when this call is made
 *
 * @param[in] user_data The user data for the node.
 *  This is obtained using raft_node_get_udata.
 *  Examples of what this could be:
 *  - void* pointing to implementor's networking data
 *  - a (IP,Port) tuple
 * @param[in] id The integer ID of this node
 *  This is used for identifying clients across sessions.
 * @param[in] is_self Set to 1 if this "node" is this server
 * @return
 *  node if it was successfully added;
 *  NULL if the node already exists */
raft_node_t* raft_add_node(raft_server_t* me, void* user_data, raft_node_id_t id, int is_self);

#define raft_add_peer raft_add_node

/** Add a node which does not participate in voting.
 * If a node already exists the call will fail.
 * Parameters are identical to raft_add_node
 * @return
 *  node if it was successfully added;
 *  NULL if the node already exists */
raft_node_t* raft_add_non_voting_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self);

/** Remove node.
 * @param node The node to be removed. */
void raft_remove_node(raft_server_t* me_, raft_node_t* node);

/** Set election timeout.
 * The amount of time that needs to elapse before we assume the leader is down
 * @param[in] msec Election timeout in milliseconds */
void raft_set_election_timeout(raft_server_t* me, int msec);

/** Set request timeout in milliseconds.
 * The amount of time before we resend an appendentries message
 * @param[in] msec Request timeout in milliseconds */
void raft_set_request_timeout(raft_server_t* me, int msec);

/** Process events that are dependent on time passing.
 * @param[in] msec_elapsed Time in milliseconds since the last call
 * @return
 *  0 on success;
 *  -1 on failure;
 *  RAFT_ERR_SHUTDOWN when server MUST shutdown */
int raft_periodic(raft_server_t* me, int msec_elapsed);

/** Receive an appendentries message.
 *
 * Will block (ie. by syncing to disk) if we need to append a message.
 *
 * Might call malloc once to increase the log entry array size.
 *
 * The log_offer callback will be called.
 *
 * @note The memory pointer (ie. raft_entry_data_t) for each msg_entry_t is
 *   copied directly. If the memory is temporary you MUST either make the
 *   memory permanent (ie. via malloc) OR re-assign the memory within the
 *   log_offer callback.
 *
 * @param[in] node The node who sent us this message
 * @param[in] ae The appendentries message
 * @param[out] r The resulting response
 * @return
 *  0 on success
 *  RAFT_ERR_NEEDS_SNAPSHOT
 *  */
int raft_recv_appendentries(raft_server_t* me,
                            raft_node_t* node,
                            msg_appendentries_t* ae,
                            msg_appendentries_response_t *r);

/** Receive a response from an appendentries message we sent.
 * @param[in] node The node who sent us this message
 * @param[in] r The appendentries response message
 * @return
 *  0 on success;
 *  -1 on error;
 *  RAFT_ERR_NOT_LEADER server is not the leader */
int raft_recv_appendentries_response(raft_server_t* me,
                                     raft_node_t* node,
                                     msg_appendentries_response_t* r);

/** Receive a requestvote message.
 * @param[in] node The node who sent us this message
 * @param[in] vr The requestvote message
 * @param[out] r The resulting response
 * @return 0 on success */
int raft_recv_requestvote(raft_server_t* me,
                          raft_node_t* node,
                          msg_requestvote_t* vr,
                          msg_requestvote_response_t *r);

/** Receive a response from a requestvote message we sent.
 * @param[in] node The node this response was sent by
 * @param[in] r The requestvote response message
 * @return
 *  0 on success;
 *  RAFT_ERR_SHUTDOWN server MUST shutdown; */
int raft_recv_requestvote_response(raft_server_t* me,
                                   raft_node_t* node,
                                   msg_requestvote_response_t* r);

/** Receive an InstallSnapshot message. */
int raft_recv_installsnapshot(raft_server_t* me,
                              raft_node_t* node,
                              msg_installsnapshot_t* is,
                              msg_installsnapshot_response_t *r);

/** Receive an InstallSnapshot message. */
int raft_recv_installsnapshot_response(raft_server_t* me,
                                       raft_node_t* node,
                                       msg_installsnapshot_response_t *r);

/** Receive an entry message from the client.
 *
 * Append the entry to the log and send appendentries to followers.
 *
 * Will block (ie. by syncing to disk) if we need to append a message.
 *
 * Might call malloc once to increase the log entry array size.
 *
 * The log_offer callback will be called.
 *
 * @note The memory pointer (ie. raft_entry_data_t) in msg_entry_t is
 *  copied directly. If the memory is temporary you MUST either make the
 *  memory permanent (ie. via malloc) OR re-assign the memory within the
 *  log_offer callback.
 *
 * Will fail:
 * <ul>
 *      <li>if the server is not the leader
 * </ul>
 *
 * @param[in] node The node who sent us this message
 * @param[in] ety The entry message
 * @param[out] r The resulting response
 * @return
 *  0 on success;
 *  RAFT_ERR_NOT_LEADER server is not the leader;
 *  RAFT_ERR_SHUTDOWN server MUST shutdown;
 *  RAFT_ERR_ONE_VOTING_CHANGE_ONLY there is a non-voting change inflight;
 *  RAFT_ERR_NOMEM memory allocation failure
 */
int raft_recv_entry(raft_server_t* me,
                    msg_entry_t* ety,
                    msg_entry_response_t *r);

/**
 * @return server's node ID; -1 if it doesn't know what it is */
raft_node_id_t raft_get_nodeid(raft_server_t* me);

/**
 * @return the server's node */
raft_node_t* raft_get_my_node(raft_server_t *me_);

/**
 * @return currently configured election timeout in milliseconds */
int raft_get_election_timeout(raft_server_t* me);

/**
 * @return number of nodes that this server has */
int raft_get_num_nodes(raft_server_t* me);

/**
 * @return number of voting nodes that this server has */
int raft_get_num_voting_nodes(raft_server_t* me_);

/**
 * @return number of items within log */
raft_index_t raft_get_log_count(raft_server_t* me);

/**
 * @return current term */
raft_term_t raft_get_current_term(raft_server_t* me);

/**
 * @return current log index */
raft_index_t raft_get_current_idx(raft_server_t* me);

/**
 * @return commit index */
raft_index_t raft_get_commit_idx(raft_server_t* me_);

/**
 * @return 1 if follower; 0 otherwise */
int raft_is_follower(raft_server_t* me);

/**
 * @return 1 if leader; 0 otherwise */
int raft_is_leader(raft_server_t* me);

/**
 * @return 1 if candidate; 0 otherwise */
int raft_is_candidate(raft_server_t* me);

/**
 * @return currently elapsed timeout in milliseconds */
int raft_get_timeout_elapsed(raft_server_t* me);

/**
 * @return request timeout in milliseconds */
int raft_get_request_timeout(raft_server_t* me);

/**
 * @return index of last applied entry */
raft_index_t raft_get_last_applied_idx(raft_server_t* me);

/**
 * Set index of the last applied entry */
void raft_set_last_applied_idx(raft_server_t* me_, raft_index_t idx);

/**
 * @return the node's next index */
raft_index_t raft_node_get_next_idx(raft_node_t* node);

/**
 * @return this node's user data */
raft_index_t raft_node_get_match_idx(raft_node_t* me);

/**
 * @return this node's user data */
void* raft_node_get_udata(raft_node_t* me);

/**
 * Set this node's user data */
void raft_node_set_udata(raft_node_t* me, void* user_data);

/**
 * @param[in] idx The entry's index
 * @return entry from index */
raft_entry_t* raft_get_entry_from_idx(raft_server_t* me, raft_index_t idx);

/**
 * @param[in] node The node's ID
 * @return node pointed to by node ID */
raft_node_t* raft_get_node(raft_server_t* me_, const raft_node_id_t id);

/**
 * Used for iterating through nodes
 * @param[in] node The node's idx
 * @return node pointed to by node idx */
raft_node_t* raft_get_node_from_idx(raft_server_t* me_, const raft_index_t idx);

/**
 * @return number of votes this server has received this election */
int raft_get_nvotes_for_me(raft_server_t* me);

/**
 * @return node ID of who I voted for */
raft_node_id_t raft_get_voted_for(raft_server_t* me);

/** Get what this node thinks the node ID of the leader is.
 * @return node of what this node thinks is the valid leader;
 *   -1 if the leader is unknown */
raft_node_id_t raft_get_current_leader(raft_server_t* me);

/** Get what this node thinks the node of the leader is.
 * @return node of what this node thinks is the valid leader;
 *   NULL if the leader is unknown */
raft_node_t* raft_get_current_leader_node(raft_server_t* me);

/**
 * @return callback user data */
void* raft_get_udata(raft_server_t* me);

/** Vote for a server.
 * This should be used to reload persistent state, ie. the voted-for field.
 * @param[in] node The server to vote for
 * @return
 *  0 on success */
int raft_vote(raft_server_t* me_, raft_node_t* node);

/** Vote for a server.
 * This should be used to reload persistent state, ie. the voted-for field.
 * @param[in] nodeid The server to vote for by nodeid
 * @return
 *  0 on success */
int raft_vote_for_nodeid(raft_server_t* me_, const raft_node_id_t nodeid);

/** Set the current term.
 * This should be used to reload persistent state, ie. the current_term field.
 * @param[in] term The new current term
 * @return
 *  0 on success */
int raft_set_current_term(raft_server_t* me, const raft_term_t term);

/** Set the commit idx.
 * This should be used to reload persistent state, ie. the commit_idx field.
 * @param[in] commit_idx The new commit index. */
void raft_set_commit_idx(raft_server_t* me, raft_index_t commit_idx);

/** Add entries to the server's log.
 * This should be used to reload persistent state, ie. the commit log.
 * @param[in] entries List of entries to be appended
 * @param[in,out] n Number of entries to append / successfully appended
 * @return
 *  0 on success;
 *  RAFT_ERR_SHUTDOWN server should shutdown
 *  RAFT_ERR_NOMEM memory allocation failure */
int raft_append_entries(raft_server_t* me, raft_entry_t* entries, int *n);

/** Confirm if a msg_entry_response has been committed.
 * @param[in] r The response we want to check
 * @return
 *  1 if committed
 *  0 if not committed
 *  -1 if overwritten or commitness unknown */
int raft_msg_entry_response_committed(raft_server_t* me_,
                                      const msg_entry_response_t* r);

/** Get node's ID.
 * @return ID of node */
raft_node_id_t raft_node_get_id(raft_node_t* me_);

/** Tell if we are a leader, candidate or follower.
 * @return get state of type raft_state_e. */
int raft_get_state(raft_server_t* me_);

/** The the most recent log's term
 * @return the last log term */
raft_term_t raft_get_last_log_term(raft_server_t* me_);

/** Turn a node into a voting node.
 * Voting nodes can take part in elections and in-regards to commiting entries,
 * are counted in majorities. */
void raft_node_set_voting(raft_node_t* node, int voting);

/** Tell if a node is a voting node or not.
 * @return 1 if this is a voting node. Otherwise 0. */
int raft_node_is_voting(raft_node_t* me_);

/** Check if a node has sufficient logs to be able to join the cluster.
 **/
int raft_node_has_sufficient_logs(raft_node_t* me_);

/** Apply all entries up to the commit index
 * @return
 *  0 on success;
 *  RAFT_ERR_SHUTDOWN when server MUST shutdown */
int raft_apply_all(raft_server_t* me_);

/** Become leader
 * WARNING: this is a dangerous function call. It could lead to your cluster
 * losing it's consensus guarantees. */
void raft_become_leader(raft_server_t* me);

/** Become follower. This may be used to give up leadership. It does not change
 * currentTerm. */
void raft_become_follower(raft_server_t* me);

void raft_election_start(raft_server_t* me);

/** Determine if entry is voting configuration change.
 * @param[in] ety The entry to query.
 * @return 1 if this is a voting configuration change. */
int raft_entry_is_voting_cfg_change(raft_entry_t* ety);

/** Determine if entry is configuration change.
 * @param[in] ety The entry to query.
 * @return 1 if this is a configuration change. */
int raft_entry_is_cfg_change(raft_entry_t* ety);

/** Begin snapshotting an index.
 *
 * While snapshotting, raft will:
 *  - not apply log entries
 *  - not start elections
 *
 * @return 0 on success
 *
 **/
int raft_begin_snapshot(raft_server_t *me_, raft_index_t idx);

/** Stop snapshotting.
 *
 * The user MUST include membership changes inside the snapshot. This means
 * that membership changes are included in the size of the snapshot. For peers
 * that load the snapshot, the user needs to deserialize the snapshot to
 * obtain the membership changes.
 *
 * The user MUST compact the log up to the commit index. This means all
 * log entries up to the commit index MUST be deleted (aka polled).
 *
 * @return
 *  0 on success
 *  -1 on failure
 **/
int raft_end_snapshot(raft_server_t *me_);

/** Get the entry index of the entry that was snapshotted
 **/
raft_index_t raft_get_snapshot_entry_idx(raft_server_t *me_);

/** Check is a snapshot is in progress
 **/
int raft_snapshot_is_in_progress(raft_server_t *me_);

/** Get last applied entry
 **/
raft_entry_t *raft_get_last_applied_entry(raft_server_t *me_);

raft_index_t raft_get_first_entry_idx(raft_server_t* me_);

/** Start loading snapshot
 *
 * This is usually the result of a snapshot being loaded.
 * We need to send an appendentries response.
 *
 * @param[in] last_included_term Term of the last log of the snapshot
 * @param[in] last_included_index Index of the last log of the snapshot 
 *
 * @return
 *  0 on success
 *  -1 on failure
 *  RAFT_ERR_SNAPSHOT_ALREADY_LOADED
 **/
int raft_begin_load_snapshot(raft_server_t *me_,
                             raft_term_t last_included_term,
		                     raft_index_t last_included_index);

/** Stop loading snapshot.
 *
 * @return
 *  0 on success
 *  -1 on failure
 **/
int raft_end_load_snapshot(raft_server_t *me_);

raft_index_t raft_get_snapshot_last_idx(raft_server_t *me_);

raft_term_t raft_get_snapshot_last_term(raft_server_t *me_);

void raft_set_snapshot_metadata(raft_server_t *me_, raft_term_t term, raft_index_t idx);

/** Check if a node is active.
 * Active nodes could become voting nodes.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_active(raft_node_t* me_);

/** Make the node active.
 *
 * The user sets this to 1 between raft_begin_load_snapshot and
 * raft_end_load_snapshot.
 *
 * @param[in] active Set a node as active if this is 1
 **/
void raft_node_set_active(raft_node_t* me_, int active);

/** Check if a node's voting status has been committed.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_voting_committed(raft_node_t* me_);

/** Check if a node's membership to the cluster has been committed.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_addition_committed(raft_node_t* me_);

#endif /* RAFT_H_ */
