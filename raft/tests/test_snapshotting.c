#include <stdbool.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "CuTest.h"

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"
#include "mock_send_functions.h"

static int __raft_persist_term(
    raft_server_t* raft,
    void *udata,
    raft_term_t term,
    raft_node_id_t vote
    )
{
    return 0;
}

static int __raft_persist_vote(
    raft_server_t* raft,
    void *udata,
    raft_node_id_t vote
    )
{
    return 0;
}

static int __raft_applylog(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t idx
    )
{
    return 0;
}

static int __raft_send_requestvote(raft_server_t* raft,
                            void* udata,
                            raft_node_t* node,
                            msg_requestvote_t* msg)
{
    return 0;
}

static int __raft_send_appendentries(raft_server_t* raft,
                              void* udata,
                              raft_node_t* node,
                              msg_appendentries_t* msg)
{
    return 0;
}

static int __raft_send_appendentries_capture(raft_server_t* raft,
                              void* udata,
                              raft_node_t* node,
                              msg_appendentries_t* msg)
{
    msg_appendentries_t* msg_captured = (msg_appendentries_t*)udata;
    memcpy(msg_captured, msg, sizeof(msg_appendentries_t));
    return 0;
}

static int __raft_send_installsnapshot(raft_server_t* raft,
                                       void* udata,
                                       raft_node_t* node,
                                       msg_installsnapshot_t* msg)
{
    return 0;
}

static int __raft_send_installsnapshot_capture(raft_server_t* raft,
                                               void* udata,
                                               raft_node_t* node,
                                               msg_installsnapshot_t* msg)
{
    msg_installsnapshot_t* msg_captured = (msg_installsnapshot_t*)udata;
    memcpy(msg_captured, msg, sizeof(msg_installsnapshot_t));
    return 0;
}

/* static raft_cbs_t generic_funcs = { */
/*     .persist_term = __raft_persist_term, */
/*     .persist_vote = __raft_persist_vote, */
/* }; */

static int raft_append_entry(raft_server_t* me_, raft_entry_t* ety)
{
    int k = 1;
    return raft_append_entries(me_, ety, &k);
}

static int max_election_timeout(int election_timeout)
{
        return 2 * election_timeout;
}

// TODO: don't apply logs while snapshotting
// TODO: don't cause elections while snapshotting

void TestRaft_leader_begin_snapshot_fails_if_no_logs_to_compact(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    raft_recv_entry(r, &ety, &cr);
    ety.id = 2;
    raft_recv_entry(r, &ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    /* no committed entries */
    CuAssertIntEquals(tc, -1, raft_begin_snapshot(r, 0));

    /* snapshot a committed entry */
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 1));
}

void TestRaft_leader_will_not_apply_entry_if_snapshot_is_in_progress(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    raft_recv_entry(r, &ety, &cr);
    ety.id = 1;
    raft_recv_entry(r, &ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 1));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, -1, raft_apply_entry(r));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_leader_snapshot_end_fails_if_snapshot_not_in_progress(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, -1, raft_end_snapshot(r));
}

void TestRaft_leader_snapshot_end_succeeds_if_log_compacted(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries = __raft_send_appendentries,
        .send_installsnapshot = __raft_send_installsnapshot
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    raft_recv_entry(r, &ety, &cr);
    ety.id = 2;
    raft_recv_entry(r, &ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, 1, raft_get_num_snapshottable_logs(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 1));

    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 0, raft_periodic(r, 1000));
}

void TestRaft_leader_snapshot_end_succeeds_if_log_compacted2(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries = __raft_send_appendentries,
        .send_installsnapshot = __raft_send_installsnapshot
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    raft_recv_entry(r, &ety, &cr);
    ety.id = 2;
    raft_recv_entry(r, &ety, &cr);
    ety.id = 3;
    raft_recv_entry(r, &ety, &cr);
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, 3, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_num_snapshottable_logs(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 2));

    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 2, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 0, raft_periodic(r, 1000));
}

void TestRaft_joinee_needs_to_get_snapshot(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    raft_recv_entry(r, &ety, &cr);
    ety.id = 2;
    raft_recv_entry(r, &ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, 1, raft_get_num_snapshottable_logs(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 1));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, -1, raft_apply_entry(r));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_follower_load_from_snapshot(CuTest * tc)
{
    raft_cbs_t funcs = {
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");
    raft_append_entry(r, &ety);
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));

    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 5, 5));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 5, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 5, raft_get_last_applied_idx(r));

    CuAssertIntEquals(tc, 0, raft_periodic(r, 1000));

    /* committed idx means snapshot was unnecessary */
    ety.id = 6;
    raft_append_entry(r, &ety);
    ety.id = 7;
    raft_append_entry(r, &ety);
    raft_set_commit_idx(r, 7);
    CuAssertIntEquals(tc, -1, raft_begin_load_snapshot(r, 6, 5));
    CuAssertIntEquals(tc, 7, raft_get_commit_idx(r));
}

void TestRaft_follower_load_from_snapshot_fails_if_already_loaded(CuTest * tc)
{
    raft_cbs_t funcs = {
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 5, 5));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 5, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 5, raft_get_last_applied_idx(r));

    CuAssertIntEquals(tc, RAFT_ERR_SNAPSHOT_ALREADY_LOADED, raft_begin_load_snapshot(r, 5, 5));
}

void TestRaft_leader_sends_appendentries_when_node_next_index_was_compacted(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
        .send_installsnapshot = __raft_send_installsnapshot_capture,
    };

    msg_installsnapshot_t is;

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, &is);

    raft_node_t* node;
    raft_add_node(r, NULL, 1, 1);
    node = raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* entry 1 */
    char *str = "aaa";
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = str;
    ety.data.len = 3;
    raft_append_entry(r, &ety);

    /* entry 2 */
    ety.term = 1;
    ety.id = 2;
    ety.data.buf = str;
    ety.data.len = 3;
    raft_append_entry(r, &ety);

    /* entry 3 */
    ety.term = 1;
    ety.id = 3;
    ety.data.buf = str;
    ety.data.len = 3;
    raft_append_entry(r, &ety);
    CuAssertIntEquals(tc, 3, raft_get_current_idx(r));

    /* compact entry 1 & 2 */
    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 2, 3));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));
    CuAssertIntEquals(tc, 3, raft_get_current_idx(r));

    /* node wants an entry that was compacted */
    raft_node_set_next_idx(node, raft_get_current_idx(r));

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);
    CuAssertIntEquals(tc, 0, raft_send_appendentries(r, node));
    CuAssertIntEquals(tc, 2, is.term);
    CuAssertIntEquals(tc, 3, is.last_idx);
    CuAssertIntEquals(tc, 2, is.last_term);
}

void TestRaft_recv_entry_fails_if_snapshot_in_progress(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    raft_recv_entry(r, &ety, &cr);
    ety.id = 2;
    raft_recv_entry(r, &ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 1));

    ety.id = 3;
    ety.type = RAFT_LOGTYPE_ADD_NODE;
    CuAssertIntEquals(tc, RAFT_ERR_SNAPSHOT_IN_PROGRESS, raft_recv_entry(r, &ety, &cr));
}
