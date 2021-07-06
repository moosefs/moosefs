
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

// TODO: leader doesn't timeout and cause election

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

int __raft_applylog(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t idx
    )
{
    return 0;
}

int __raft_applylog_shutdown(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t idx
    )
{
    return RAFT_ERR_SHUTDOWN;
}

int __raft_send_requestvote(raft_server_t* raft,
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

static int __raft_log_get_node_id(raft_server_t* raft,
        void *udata,
        raft_entry_t *entry,
        raft_index_t entry_idx)
{
    return atoi(entry->data.buf);
}

static int __raft_log_offer(raft_server_t* raft,
        void* udata,
        raft_entry_t *entries,
        raft_index_t entry_idx,
        int *n_entries)
{
    int i;
    for (i = 0; i < *n_entries; ++i)
    {
        raft_entry_t *ety = &entries[i];

        if (ety->type == RAFT_LOGTYPE_ADD_NONVOTING_NODE)
            raft_add_non_voting_node(raft, NULL, atoi(ety->data.buf), false);
    }
    return 0;
}

static int __raft_node_has_sufficient_logs(
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node)
{
    int *flag = (int*)user_data;
    *flag += 1;
    return 0;
}

raft_cbs_t generic_funcs = {
    .persist_term = __raft_persist_term,
    .persist_vote = __raft_persist_vote,
};

static int raft_append_entry(raft_server_t* me_, raft_entry_t* ety)
{
    int k = 1;
    return raft_append_entries(me_, ety, &k);
}

static int max_election_timeout(int election_timeout)
{
	return 2 * election_timeout;
}

void TestRaft_server_voted_for_records_who_we_voted_for(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_add_node(r, NULL, 2, 0);
    raft_vote(r, raft_get_node(r, 2));
    CuAssertTrue(tc, 2 == raft_get_voted_for(r));
}

void TestRaft_server_get_my_node(CuTest * tc)
{
    void *r = raft_new();
    raft_node_t* me = raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    CuAssertTrue(tc, me == raft_get_my_node(r));
}

void TestRaft_server_can_run_with_empty_configuration(CuTest * tc)
{
    void *r = raft_new();
    CuAssertIntEquals(tc, 0, raft_periodic(r, 1));

    msg_entry_response_t mr;
    msg_entry_t e[2] = { 0 };
    e[0].id = 1;
    e[1].id = 2;

    CuAssertTrue(tc, RAFT_ERR_NOT_LEADER == raft_recv_entry(r, &e[0], &mr));

    msg_appendentries_t ae = { 0 };
    msg_appendentries_response_t aer;
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;

    ae.entries = e;
    ae.n_entries = 2;
    raft_recv_appendentries(r, NULL, &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));

    CuAssertIntEquals(tc, 0, raft_periodic(r, 1));
    CuAssertTrue(tc, 2 == raft_get_current_idx(r));
}

void TestRaft_server_idx_starts_at_1(CuTest * tc)
{
    void *r = raft_new();
    CuAssertTrue(tc, 0 == raft_get_current_idx(r));

    raft_entry_t ety = {};
    ety.data.buf = "aaa";
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
}

void TestRaft_server_currentterm_defaults_to_0(CuTest * tc)
{
    void *r = raft_new();
    CuAssertTrue(tc, 0 == raft_get_current_term(r));
}

void TestRaft_server_set_currentterm_sets_term(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_set_current_term(r, 5);
    CuAssertTrue(tc, 5 == raft_get_current_term(r));
}

void TestRaft_server_voting_results_in_voting(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_add_node(r, NULL, 1, 0);
    raft_add_node(r, NULL, 9, 0);

    raft_vote(r, raft_get_node(r, 1));
    CuAssertTrue(tc, 1 == raft_get_voted_for(r));
    raft_vote(r, raft_get_node(r, 9));
    CuAssertTrue(tc, 9 == raft_get_voted_for(r));
}

void TestRaft_server_add_node_makes_non_voting_node_voting(CuTest * tc)
{
    void *r = raft_new();
    void* n1 = raft_add_non_voting_node(r, NULL, 9, 0);

    CuAssertTrue(tc, !raft_node_is_voting(n1));
    raft_add_node(r, NULL, 9, 0);
    CuAssertTrue(tc, raft_node_is_voting(n1));
    CuAssertIntEquals(tc, 1, raft_get_num_nodes(r));
}

void TestRaft_server_add_node_with_already_existing_id_is_not_allowed(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 9, 0);
    raft_add_node(r, NULL, 11, 0);

    CuAssertTrue(tc, NULL == raft_add_node(r, NULL, 9, 0));
    CuAssertTrue(tc, NULL == raft_add_node(r, NULL, 11, 0));
}

void TestRaft_server_add_non_voting_node_with_already_existing_id_is_not_allowed(CuTest * tc)
{
    void *r = raft_new();
    raft_add_non_voting_node(r, NULL, 9, 0);
    raft_add_non_voting_node(r, NULL, 11, 0);

    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 9, 0));
    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 11, 0));
}

void TestRaft_server_add_non_voting_node_with_already_existing_voting_id_is_not_allowed(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 9, 0);
    raft_add_node(r, NULL, 11, 0);

    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 9, 0));
    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 11, 0));
}

void TestRaft_server_remove_node(CuTest * tc)
{
    void *r = raft_new();
    void* n1 = raft_add_node(r, NULL, 1, 0);
    void* n2 = raft_add_node(r, NULL, 9, 0);

    raft_remove_node(r, n1);
    CuAssertTrue(tc, NULL == raft_get_node(r, 1));
    CuAssertTrue(tc, NULL != raft_get_node(r, 9));
    raft_remove_node(r, n2);
    CuAssertTrue(tc, NULL == raft_get_node(r, 9));
}

void TestRaft_election_start_does_not_increment_term(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_election_start(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
    CuAssertTrue(tc, raft_is_candidate(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
}

void TestRaft_become_prevoted_candidate_increments_term(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_become_candidate(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
    CuAssertTrue(tc, raft_is_candidate(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
    raft_become_prevoted_candidate(r);
    CuAssertTrue(tc, 2 == raft_get_current_term(r));
    CuAssertTrue(tc, raft_is_candidate(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
}

void TestRaft_set_state(CuTest * tc)
{
    void *r = raft_new();
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertTrue(tc, RAFT_STATE_LEADER == raft_get_state(r));
}

void TestRaft_server_starts_as_follower(CuTest * tc)
{
    void *r = raft_new();
    CuAssertTrue(tc, RAFT_STATE_FOLLOWER == raft_get_state(r));
}

void TestRaft_server_starts_with_election_timeout_of_1000ms(CuTest * tc)
{
    void *r = raft_new();
    CuAssertTrue(tc, 1000 == raft_get_election_timeout(r));
}

void TestRaft_server_starts_with_request_timeout_of_200ms(CuTest * tc)
{
    void *r = raft_new();
    CuAssertTrue(tc, 200 == raft_get_request_timeout(r));
}

void TestRaft_server_entry_append_increases_logidx(CuTest* tc)
{
    raft_entry_t ety = {};
    char *str = "aaa";

    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;

    void *r = raft_new();
    CuAssertTrue(tc, 0 == raft_get_current_idx(r));
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
}

void TestRaft_server_append_entry_means_entry_gets_current_term(CuTest* tc)
{
    raft_entry_t ety = {};
    char *str = "aaa";

    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;

    void *r = raft_new();
    CuAssertTrue(tc, 0 == raft_get_current_idx(r));
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
}

void TestRaft_server_append_entry_is_retrievable(CuTest * tc)
{
  raft_entry_t* kept;
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_set_state(r, RAFT_STATE_CANDIDATE);

    raft_set_current_term(r, 5);
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    raft_append_entry(r, &ety);

    CuAssertTrue(tc, NULL != (kept = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, NULL != kept->data.buf);
    CuAssertIntEquals(tc, ety.data.len, kept->data.len);
    CuAssertTrue(tc, kept->data.buf == ety.data.buf);
}

static int __raft_logentry_offer(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t ety_idx,
    int *n_entries
    )
{
    CuAssertIntEquals(udata, ety_idx, 1);
    ety->data.buf = udata;
    return 0;
}

void TestRaft_server_append_entry_user_can_set_data_buf(CuTest * tc)
{
    raft_cbs_t funcs = {
        .log_offer = __raft_logentry_offer,
        .persist_term = __raft_persist_term,
    };
    char *buf = "aaa";
    raft_entry_t* kept;

    void *r = raft_new();
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_set_callbacks(r, &funcs, tc);
    raft_set_current_term(r, 5);
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = buf;
    raft_append_entry(r, &ety);
    /* User's input entry is intact. */
    CuAssertTrue(tc, ety.data.buf == buf);
    CuAssertTrue(tc, NULL != (kept = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, NULL != kept->data.buf);
    /* Data buf is the one set by log_offer. */
    CuAssertTrue(tc, kept->data.buf == tc);
}

#if 0
/* TODO: no support for duplicate detection yet */
void
T_estRaft_server_append_entry_not_sucessful_if_entry_with_id_already_appended(
    CuTest* tc)
{
    void *r;
    raft_entry_t ety;
    char *str = "aaa";

    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;

    r = raft_new();
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    raft_append_entry(r, &ety);
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 2 == raft_get_current_idx(r));

    /* different ID so we can be successful */
    ety.id = 2;
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 3 == raft_get_current_idx(r));
}
#endif

void TestRaft_server_entry_is_retrieveable_using_idx(CuTest* tc)
{
    raft_entry_t e1 = {};
    raft_entry_t e2 = {};
    raft_entry_t *ety_appended;
    char *str = "aaa";
    char *str2 = "bbb";

    void *r = raft_new();

    e1.term = 1;
    e1.id = 1;
    e1.data.buf = str;
    e1.data.len = 3;
    raft_append_entry(r, &e1);

    /* different ID so we can be successful */
    e2.term = 1;
    e2.id = 2;
    e2.data.buf = str2;
    e2.data.len = 3;
    raft_append_entry(r, &e2);

    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 2)));
    CuAssertTrue(tc, !strncmp(ety_appended->data.buf, str2, 3));
}

void TestRaft_server_wont_apply_entry_if_we_dont_have_entry_to_apply(CuTest* tc)
{
    void *r = raft_new();
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    raft_apply_entry(r);
    CuAssertTrue(tc, 0 == raft_get_last_applied_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));
}

void TestRaft_server_wont_apply_entry_if_there_isnt_a_majority(CuTest* tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    raft_apply_entry(r);
    CuAssertTrue(tc, 0 == raft_get_last_applied_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    char *str = "aaa";
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = str;
    ety.data.len = 3;
    raft_append_entry(r, &ety);
    raft_apply_entry(r);
    /* Not allowed to be applied because we haven't confirmed a majority yet */
    CuAssertTrue(tc, 0 == raft_get_last_applied_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));
}

/* If commitidx > lastApplied: increment lastApplied, apply log[lastApplied]
 * to state machine (�5.3) */
void TestRaft_server_increment_lastApplied_when_lastApplied_lt_commitidx(
    CuTest* tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .applylog = __raft_applylog,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    /* must be follower */
    raft_set_state(r, RAFT_STATE_FOLLOWER);
    raft_set_current_term(r, 1);
    raft_set_last_applied_idx(r, 0);

    /* need at least one entry */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaa";
    ety.data.len = 3;
    raft_append_entry(r, &ety);

    raft_set_commit_idx(r, 1);

    /* let time lapse */
    raft_periodic(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_user_applylog_error_propogates_to_periodic(
    CuTest* tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .applylog = __raft_applylog_shutdown,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    /* must be follower */
    raft_set_state(r, RAFT_STATE_FOLLOWER);
    raft_set_current_term(r, 1);
    raft_set_last_applied_idx(r, 0);

    /* need at least one entry */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaa";
    ety.data.len = 3;
    raft_append_entry(r, &ety);

    raft_set_commit_idx(r, 1);

    /* let time lapse */
    CuAssertIntEquals(tc, RAFT_ERR_SHUTDOWN, raft_periodic(r, 1));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_server_apply_entry_increments_last_applied_idx(CuTest* tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_set_last_applied_idx(r, 0);

    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaa";
    ety.data.len = 3;
    raft_append_entry(r, &ety);
    raft_set_commit_idx(r, 1);
    raft_apply_entry(r);
    CuAssertTrue(tc, 1 == raft_get_last_applied_idx(r));
}

void TestRaft_server_periodic_elapses_election_timeout(CuTest * tc)
{
    void *r = raft_new();
    /* we don't want to set the timeout to zero */
    raft_set_election_timeout(r, 1000);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_periodic(r, 0);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_periodic(r, 100);
    CuAssertTrue(tc, 100 == raft_get_timeout_elapsed(r));
}

void TestRaft_server_election_timeout_does_not_promote_us_to_leader_if_there_is_are_more_than_1_nodes(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_election_timeout(r, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic(r, 1001);

    CuAssertTrue(tc, 0 == raft_is_leader(r));
}

void TestRaft_server_election_timeout_does_not_promote_us_to_leader_if_we_are_not_voting_node(CuTest * tc)
{
    void *r = raft_new();
    raft_add_non_voting_node(r, NULL, 1, 1);
    raft_set_election_timeout(r, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic(r, 1001);

    CuAssertTrue(tc, 0 == raft_is_leader(r));
    CuAssertTrue(tc, 0 == raft_get_current_term(r));
}

void TestRaft_server_election_timeout_does_not_start_election_if_there_are_no_voting_nodes(CuTest * tc)
{
    void *r = raft_new();
    raft_add_non_voting_node(r, NULL, 1, 1);
    raft_add_non_voting_node(r, NULL, 2, 0);
    raft_set_election_timeout(r, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic(r, 1001);

    CuAssertTrue(tc, 0 == raft_get_current_term(r));
}

void TestRaft_server_election_timeout_does_promote_us_to_leader_if_there_is_only_1_node(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_election_timeout(r, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic(r, 1001);

    CuAssertTrue(tc, 1 == raft_is_leader(r));
}

void TestRaft_server_election_timeout_does_promote_us_to_leader_if_there_is_only_1_voting_node(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_non_voting_node(r, NULL, 2, 0);
    raft_set_election_timeout(r, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic(r, 1001);

    CuAssertTrue(tc, 1 == raft_is_leader(r));
}

void TestRaft_server_recv_entry_auto_commits_if_we_are_the_only_node(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_election_timeout(r, 1000);
    raft_become_leader(r);
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    msg_entry_response_t cr;
    raft_recv_entry(r, &ety, &cr);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
    CuAssertTrue(tc, 1 == raft_get_commit_idx(r));
}

void TestRaft_server_recv_entry_fails_if_there_is_already_a_voting_change(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_election_timeout(r, 1000);
    raft_become_leader(r);
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.type = RAFT_LOGTYPE_ADD_NODE;
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    msg_entry_response_t cr;
    CuAssertTrue(tc, 0 == raft_recv_entry(r, &ety, &cr));
    CuAssertTrue(tc, 1 == raft_get_log_count(r));

    ety.id = 2;
    CuAssertTrue(tc, RAFT_ERR_ONE_VOTING_CHANGE_ONLY == raft_recv_entry(r, &ety, &cr));
    CuAssertTrue(tc, 1 == raft_get_commit_idx(r));
}

void TestRaft_server_cfg_sets_num_nodes(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    CuAssertTrue(tc, 2 == raft_get_num_nodes(r));
}

void TestRaft_server_cant_get_node_we_dont_have(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    CuAssertTrue(tc, NULL == raft_get_node(r, 0));
    CuAssertTrue(tc, NULL != raft_get_node(r, 1));
    CuAssertTrue(tc, NULL != raft_get_node(r, 2));
    CuAssertTrue(tc, NULL == raft_get_node(r, 3));
}

/* If term > currentTerm, set currentTerm to term (step down if candidate or
 * leader) */
void TestRaft_votes_are_majority_is_true(
    CuTest * tc
    )
{
    /* 1 of 3 = lose */
    CuAssertTrue(tc, 0 == raft_votes_is_majority(3, 1));

    /* 2 of 3 = win */
    CuAssertTrue(tc, 1 == raft_votes_is_majority(3, 2));

    /* 2 of 5 = lose */
    CuAssertTrue(tc, 0 == raft_votes_is_majority(5, 2));

    /* 3 of 5 = win */
    CuAssertTrue(tc, 1 == raft_votes_is_majority(5, 3));

    /* 2 of 1?? This is an error */
    CuAssertTrue(tc, 0 == raft_votes_is_majority(1, 2));
}

void TestRaft_server_recv_requestvote_response_dont_increase_votes_for_me_when_not_granted(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 0;
    int e = raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertIntEquals(tc, 0, e);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_requestvote_response_dont_increase_votes_for_me_when_term_is_not_equal(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 3);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 2;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_prevote_response_increase_prevotes_for_me(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));

    raft_become_candidate(r);
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));
    CuAssertIntEquals(tc, 1, raft_get_nvotes_for_me(r));

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    rvr.prevote = 1;
    int e = raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertIntEquals(tc, 0, e);
    /* got 2 prevotes, became prevoted candidate, incremented term, and voted for self */
    CuAssertTrue(tc, raft_is_prevoted_candidate(r));
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));
    CuAssertIntEquals(tc, 1, raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_requestvote_response_increase_votes_for_me(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));

    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 2;
    rvr.vote_granted = 1;
    int e = raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertIntEquals(tc, 0, e);
    CuAssertTrue(tc, 2 == raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_prevote_response_ignored_by_prevoted_candidate(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_nvotes_for_me(r));

    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    CuAssertIntEquals(tc, 1, raft_get_nvotes_for_me(r));

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    rvr.prevote = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertIntEquals(tc, 1, raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_requestvote_response_must_be_candidate_to_receive(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));

    raft_become_leader(r);

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 0 == raft_get_nvotes_for_me(r));
}

/* Reply false if term < currentTerm (�5.1) */
void TestRaft_server_recv_requestvote_reply_false_if_term_less_than_current_term(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_requestvote_response_t rvr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 2);

    /* term is less than current term */
    msg_requestvote_t rv;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    int e = raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 0, e);
    CuAssertIntEquals(tc, 0, rvr.vote_granted);
    CuAssertIntEquals(tc, 0, rvr.prevote);
}

void TestRaft_leader_recv_requestvote_does_not_step_down(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_requestvote_response_t rvr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_vote(r, raft_get_node(r, 1));
    raft_become_leader(r);
    CuAssertIntEquals(tc, 1, raft_is_leader(r));

    /* term is less than current term */
    msg_requestvote_t rv;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 1, raft_get_current_leader(r));
}

/* Reply true if term >= currentTerm (�5.1) */
void TestRaft_server_recv_requestvote_reply_true_if_term_greater_than_or_equal_to_current_term(
    CuTest * tc
    )
{
    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;

    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    /* term is less than current term */
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 2;
    rv.last_log_idx = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);

    CuAssertTrue(tc, 1 == rvr.vote_granted);
}

void TestRaft_server_recv_prevote_dont_grant_real_vote(
    CuTest * tc
    )
{
    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;

    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_set_election_timeout(r, 1000);
    raft_periodic(r, 900);

    /* grant prevote but not real vote */
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 2;
    rv.last_log_idx = 1;
    rv.candidate_id = 2;
    rv.prevote = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
    CuAssertIntEquals(tc, 1, rvr.prevote);
    CuAssertIntEquals(tc, 0, raft_get_timeout_elapsed(r));
    CuAssertIntEquals(tc, -1, raft_get_voted_for(r));

    /* grant real vote as advertised */
    rv.prevote = 0;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
    CuAssertIntEquals(tc, 0, rvr.prevote);
    CuAssertIntEquals(tc, 2, raft_get_voted_for(r));
}

void TestRaft_server_recv_requestvote_reset_timeout(
    CuTest * tc
    )
{
    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;

    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_set_election_timeout(r, 1000);
    raft_periodic(r, 900);

    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 2;
    rv.last_log_idx = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
    CuAssertIntEquals(tc, 0, raft_get_timeout_elapsed(r));
}

void TestRaft_server_recv_requestvote_candidate_step_down_if_term_is_higher_than_current_term(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));

    /* current term is less than term */
    msg_requestvote_t rv;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.candidate_id = 2;
    rv.term = 2;
    rv.last_log_idx = 1;
    msg_requestvote_response_t rvr;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));
    CuAssertIntEquals(tc, 2, raft_get_voted_for(r));
}

void TestRaft_server_recv_requestvote_depends_on_candidate_id(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));

    /* current term is less than term */
    msg_requestvote_t rv;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.candidate_id = 3;
    rv.term = 2;
    rv.last_log_idx = 1;
    msg_requestvote_response_t rvr;
    raft_recv_requestvote(r, NULL, &rv, &rvr);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));
    CuAssertIntEquals(tc, 3, raft_get_voted_for(r));
}

/* If votedFor is null or candidateId, and candidate's log is at
 * least as up-to-date as local log, grant vote (�5.2, �5.4) */
void TestRaft_server_recv_requestvote_dont_grant_vote_if_we_didnt_vote_for_this_candidate(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 0, 0);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    /* votedFor is null; vote for 0 */
    msg_requestvote_t rv = {};
    rv.term = 1;
    rv.candidate_id = 0;
    rv.last_log_idx = 1;
    rv.last_log_term = 1;
    msg_requestvote_response_t rvr;
    raft_recv_requestvote(r, raft_get_node(r, 0), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);

    /* votedFor is 0; vote for 0 */
    raft_recv_requestvote(r, raft_get_node(r, 0), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);

    /* votedFor is 0; don't vote for 2 */
    rv.candidate_id = 2;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);
}

/* If prevote is received within the minimum election timeout of
 * hearing from a current leader, it does not update its term or grant its
 * prevote (§4.2.3, §9.6).
 */
void TestRaft_server_recv_prevote_ignore_if_master_is_fresh(CuTest * tc)
{
    raft_cbs_t funcs = { 0
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_set_election_timeout(r, 1000);

    msg_appendentries_t ae = { 0 };
    msg_appendentries_response_t aer;
    ae.term = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);

    msg_requestvote_t rv = { 
        .term = 2,
        .candidate_id = 3,
        .last_log_idx = 0,
        .last_log_term = 1,
        .prevote = 1
    };
    msg_requestvote_response_t rvr;
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 != rvr.vote_granted);

    /* After election timeout passed, the same prevote should be accepted */
    raft_periodic(r, 1001);
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
}

/* If requestvote is received within the minimum election timeout of
 * hearing from a current leader, it does not update its term or grant its
 * vote (�6).
 */
void TestRaft_server_recv_requestvote_ignore_if_master_is_fresh(CuTest * tc)
{
    raft_cbs_t funcs = { 0
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_set_election_timeout(r, 1000);

    msg_appendentries_t ae = { 0 };
    msg_appendentries_response_t aer;
    ae.term = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);

    msg_requestvote_t rv = { 
        .term = 2,
        .candidate_id = 3,
        .last_log_idx = 0,
        .last_log_term = 1
    };
    msg_requestvote_response_t rvr;
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 != rvr.vote_granted);

    /* After election timeout passed, the same requestvote should be accepted */
    raft_periodic(r, 1001);
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
}

void TestRaft_follower_becomes_follower_is_follower(CuTest * tc)
{
    void *r = raft_new();
    raft_become_follower(r);
    CuAssertTrue(tc, raft_is_follower(r));
}

void TestRaft_follower_becomes_follower_does_not_clear_voted_for(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);

    raft_vote(r, raft_get_node(r, 1));
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));
    raft_become_follower(r);
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));
}

/* 5.1 */
void TestRaft_follower_recv_appendentries_reply_false_if_term_less_than_currentterm(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    /* no leader known at this point */
    CuAssertTrue(tc, -1 == raft_get_current_leader(r));

    /* term is low */
    msg_appendentries_t ae;
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;

    /*  higher current term */
    raft_set_current_term(r, 5);
    msg_appendentries_response_t aer;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 0 == aer.success);
    /* rejected appendentries doesn't change the current leader. */
    CuAssertTrue(tc, -1 == raft_get_current_leader(r));
}

void TestRaft_follower_recv_appendentries_does_not_need_node(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    msg_appendentries_t ae = {};
    ae.term = 1;
    msg_appendentries_response_t aer;
    raft_recv_appendentries(r, NULL, &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
}

/* TODO: check if test case is needed */
void TestRaft_follower_recv_appendentries_updates_currentterm_if_term_gt_currentterm(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /*  older currentterm */
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, -1 == raft_get_current_leader(r));

    /*  newer term for appendentry */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    /* no prev log idx */
    ae.prev_log_idx = 0;
    ae.term = 2;

    /*  appendentry has newer term, so we change our currentterm */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 2 == aer.term);
    /* term has been updated */
    CuAssertTrue(tc, 2 == raft_get_current_term(r));
    /* and leader has been updated */
    CuAssertIntEquals(tc, 2, raft_get_current_leader(r));
}

void TestRaft_follower_recv_appendentries_does_not_log_if_no_entries_are_specified(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);

    /*  log size s */
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 4;
    ae.leader_commit = 5;
    ae.n_entries = 0;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 0 == raft_get_log_count(r));
}

void TestRaft_follower_recv_appendentries_increases_log(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };
    raft_entry_t *log;
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_entry_t ety = {};
    msg_appendentries_response_t aer;
    char *str = "aaa";

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);

    /*  log size s */
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 3;
    ae.prev_log_term = 1;
    /* first appendentries msg */
    ae.prev_log_idx = 0;
    ae.leader_commit = 5;
    /* include one entry */
    memset(&ety, 0, sizeof(msg_entry_t));
    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    /* check that old terms are passed onto the log */
    ety.term = 2;
    ae.entries = &ety;
    ae.n_entries = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
    CuAssertTrue(tc, NULL != (log = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, 2 == log->term);
}

/*  5.3 */
void TestRaft_follower_recv_appendentries_reply_false_if_doesnt_have_log_at_prev_log_idx_which_matches_prev_log_term(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_t ety = {};
    char *str = "aaa";

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* term is different from appendentries */
    raft_set_current_term(r, 2);
    // TODO at log manually?

    /* log idx that server doesn't have */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    /* prev_log_term is less than current term (ie. 2) */
    ae.prev_log_term = 1;
    /* include one entry */
    memset(&ety, 0, sizeof(msg_entry_t));
    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ae.entries = &ety;
    ae.n_entries = 1;

    /* trigger reply */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* reply is false */
    CuAssertTrue(tc, 0 == aer.success);
}

static raft_entry_t* __create_mock_entries_for_conflict_tests(
        CuTest * tc,
        raft_server_t* r,
        char** strs)
{
    raft_entry_t ety = {};
    raft_entry_t *ety_appended;

    /* increase log size */
    char *str1 = strs[0];
    ety.data.buf = str1;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));

    /* this log will be overwritten by a later appendentries */
    char *str2 = strs[1];
    ety.data.buf = str2;
    ety.data.len = 3;
    ety.id = 2;
    ety.term = 1;
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 2)));
    CuAssertTrue(tc, !strncmp(ety_appended->data.buf, str2, 3));

    /* this log will be overwritten by a later appendentries */
    char *str3 = strs[2];
    ety.data.buf = str3;
    ety.data.len = 3;
    ety.id = 3;
    ety.term = 1;
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 3 == raft_get_log_count(r));
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 3)));
    CuAssertTrue(tc, !strncmp(ety_appended->data.buf, str3, 3));

    return ety_appended;
}

/* 5.3 */
void TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);

    char* strs[] = {"111", "222", "333"};
    raft_entry_t *ety_appended = __create_mock_entries_for_conflict_tests(tc, r, strs);

    /* pass a appendentry that is newer  */
    msg_entry_t mety = {};

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 2;
    /* entries from 2 onwards will be overwritten by this appendentries message */
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include one entry */
    memset(&mety, 0, sizeof(msg_entry_t));
    char *str4 = "444";
    mety.data.buf = str4;
    mety.data.len = 3;
    mety.id = 4;
    ae.entries = &mety;
    ae.n_entries = 1;

    /* str4 has overwritten the last 2 entries */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));
    /* str1 is still there */
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, !strncmp(ety_appended->data.buf, strs[0], 3));
    /* str4 has overwritten the last 2 entries */
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 2)));
    CuAssertTrue(tc, !strncmp(ety_appended->data.buf, str4, 3));
}

void TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx_at_idx_0(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);

    char* strs[] = {"111", "222", "333"};
    raft_entry_t *ety_appended = __create_mock_entries_for_conflict_tests(tc, r, strs);

    /* pass a appendentry that is newer  */
    msg_entry_t mety = {};

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 2;
    /* ALL append entries will be overwritten by this appendentries message */
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include one entry */
    memset(&mety, 0, sizeof(msg_entry_t));
    char *str4 = "444";
    mety.data.buf = str4;
    mety.data.len = 3;
    mety.id = 4;
    ae.entries = &mety;
    ae.n_entries = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
    /* str1 is gone */
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, !strncmp(ety_appended->data.buf, str4, 3));
}

void TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_greater_than_prev_log_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);

    char* strs[] = {"111", "222", "333"};
    raft_entry_t *ety_appended;

    __create_mock_entries_for_conflict_tests(tc, r, strs);
    CuAssertIntEquals(tc, 3, raft_get_log_count(r));

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    msg_entry_t e[1];
    memset(&e, 0, sizeof(msg_entry_t) * 1);
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, !strncmp(ety_appended->data.buf, strs[0], 3));
}

// TODO: add TestRaft_follower_recv_appendentries_delete_entries_if_term_is_different

void TestRaft_follower_recv_appendentries_add_new_entries_not_already_in_log(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[2];
    memset(&e, 0, sizeof(msg_entry_t) * 2);
    e[0].id = 1;
    e[1].id = 2;
    ae.entries = e;
    ae.n_entries = 2;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));
}

void TestRaft_follower_recv_appendentries_does_not_add_dupe_entries_already_in_log(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include 1 entry */
    msg_entry_t e[2];
    memset(&e, 0, sizeof(msg_entry_t) * 2);
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;
    memset(&aer, 0, sizeof(aer));
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    memset(&aer, 0, sizeof(aer));
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    /* still successful even when no raft_append_entry() happened! */
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));

    /* lets get the server to append 2 now! */
    e[1].id = 2;
    ae.n_entries = 2;
    memset(&aer, 0, sizeof(aer));
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
}

typedef enum {
    __RAFT_NO_ERR = 0,
    __RAFT_LOG_OFFER_ERR,
    __RAFT_LOG_POP_ERR
} __raft_error_type_e;

typedef struct {
    __raft_error_type_e type;
    raft_index_t idx;
} __raft_error_t;

static int __raft_log_offer_error(
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx,
    int *n_entries)
{
    __raft_error_t *error = user_data;

    if (__RAFT_LOG_OFFER_ERR == error->type && entry_idx <= error->idx
        && error->idx < (entry_idx + *n_entries)) {
        *n_entries = error->idx - entry_idx;
        return RAFT_ERR_NOMEM;
    }
    return 0;
}

static int __raft_log_pop_error(
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx,
    int *n_entries)
{
    __raft_error_t *error = user_data;

    if (__RAFT_LOG_POP_ERR == error->type && entry_idx <= error->idx
        && error->idx < (entry_idx + *n_entries)) {
        *n_entries = error->idx - entry_idx;
        return RAFT_ERR_NOMEM;
    }
    return 0;
}

void TestRaft_follower_recv_appendentries_partial_failures(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .log_offer = __raft_log_offer_error,
        .log_pop = __raft_log_pop_error,
    };

    void *r = raft_new();
    __raft_error_t error = {};
    raft_set_callbacks(r, &funcs, &error);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    /* Append entry 1 and 2 of term 1. */
    raft_entry_t ety = {};
    ety.data.buf = "1aa";
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;
    raft_append_entry(r, &ety);
    ety.data.buf = "1bb";
    ety.data.len = 3;
    ety.id = 2;
    ety.term = 1;
    raft_append_entry(r, &ety);
    CuAssertIntEquals(tc, 2, raft_get_current_idx(r));

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    /* To be received: entry 2 and 3 of term 2. */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    msg_entry_t e[2];
    memset(&e, 0, sizeof(msg_entry_t) * 2);
    e[0].term = 2;
    e[0].id = 2;
    e[1].term = 2;
    e[1].id = 3;
    ae.entries = e;
    ae.n_entries = 2;

    /* Ask log_pop to fail at entry 2. */
    error.type = __RAFT_LOG_POP_ERR;
    error.idx = 2;
    memset(&aer, 0, sizeof(aer));
    int err = raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, RAFT_ERR_NOMEM, err);
    CuAssertIntEquals(tc, 1, aer.success);
    CuAssertIntEquals(tc, 1, aer.current_idx);
    CuAssertIntEquals(tc, 2, raft_get_current_idx(r));
    raft_entry_t *tmp = raft_get_entry_from_idx(r, 2);
    CuAssertTrue(tc, NULL != tmp);
    CuAssertIntEquals(tc, 1, tmp->term);

    /* Ask log_offer to fail at entry 3. */
    error.type = __RAFT_LOG_OFFER_ERR;
    error.idx = 3;
    memset(&aer, 0, sizeof(aer));
    err = raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, RAFT_ERR_NOMEM, err);
    CuAssertIntEquals(tc, 1, aer.success);
    CuAssertIntEquals(tc, 2, aer.current_idx);
    CuAssertIntEquals(tc, 2, raft_get_current_idx(r));
    tmp = raft_get_entry_from_idx(r, 2);
    CuAssertTrue(tc, NULL != tmp);
    CuAssertIntEquals(tc, 2, tmp->term);

    /* No more errors. */
    memset(&error, 0, sizeof(error));
    memset(&aer, 0, sizeof(aer));
    err = raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, 0, err);
    CuAssertIntEquals(tc, 1, aer.success);
    CuAssertIntEquals(tc, 3, aer.current_idx);
    CuAssertIntEquals(tc, 3, raft_get_current_idx(r));
}

/* If leaderCommit > commitidx, set commitidx =
 *  min(leaderCommit, last log idx) */
void TestRaft_follower_recv_appendentries_set_commitidx_to_prevLogIdx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    e[1].term = 1;
    e[1].id = 2;
    e[2].term = 1;
    e[2].id = 3;
    e[3].term = 1;
    e[3].id = 4;
    ae.entries = e;
    ae.n_entries = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 4;
    ae.leader_commit = 5;
    /* receipt of appendentries changes commit idx */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    /* set to 4 because commitIDX is lower */
    CuAssertIntEquals(tc, 4, raft_get_commit_idx(r));
}

void TestRaft_follower_recv_appendentries_set_commitidx_to_LeaderCommit(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    e[1].term = 1;
    e[1].id = 2;
    e[2].term = 1;
    e[2].id = 3;
    e[3].term = 1;
    e[3].id = 4;
    ae.entries = e;
    ae.n_entries = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 3;
    ae.leader_commit = 3;
    /* receipt of appendentries changes commit idx */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    /* set to 3 because leaderCommit is lower */
    CuAssertIntEquals(tc, 3, raft_get_commit_idx(r));
}

void TestRaft_follower_recv_appendentries_failure_includes_current_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_entry_t ety = {};
    ety.data.buf = "aaa";
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;
    raft_append_entry(r, &ety);

    /* receive an appendentry with commit */
    msg_appendentries_t ae;
    memset(&ae, 0, sizeof(msg_appendentries_t));
    /* lower term means failure */
    ae.term = 0;
    ae.prev_log_term = 0;
    ae.prev_log_idx = 0;
    ae.leader_commit = 0;
    msg_appendentries_response_t aer;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 0 == aer.success);
    CuAssertIntEquals(tc, 1, aer.current_idx);

    /* try again with a higher current_idx */
    memset(&aer, 0, sizeof(aer));
    ety.id = 2;
    raft_append_entry(r, &ety);
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 0 == aer.success);
    CuAssertIntEquals(tc, 2, aer.current_idx);
}

void TestRaft_follower_becomes_candidate_when_election_timeout_occurs(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    /*  1 second election timeout */
    raft_set_election_timeout(r, 1000);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /*  max election timeout have passed */
    raft_periodic(r, max_election_timeout(1000) + 1);

    /* is a candidate now */
    CuAssertTrue(tc, 1 == raft_is_candidate(r));
}

void TestRaft_follower_dont_grant_prevote_if_candidate_has_a_less_complete_log(
    CuTest * tc)
{
    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;

    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /*  request prevote */
    /*  prevote indicates candidate's log is not complete compared to follower */
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    rv.candidate_id = 1;
    rv.last_log_idx = 1;
    rv.last_log_term = 1;
    rv.prevote = 1;

    raft_set_current_term(r, 1);

    /* server's idx are more up-to-date */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    raft_append_entry(r, &ety);
    ety.id = 101;
    ety.term = 2;
    raft_append_entry(r, &ety);

    /* prevote not granted */
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);

    /* approve vote, because last_log_term is higher */
    raft_set_current_term(r, 2);
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 2;
    rv.candidate_id = 1;
    rv.last_log_idx = 1;
    rv.last_log_term = 3;
    rv.prevote = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 1, rvr.vote_granted);
}

/* Candidate 5.2 */
void TestRaft_follower_dont_grant_vote_if_candidate_has_a_less_complete_log(
    CuTest * tc)
{
    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;

    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /*  request vote */
    /*  vote indicates candidate's log is not complete compared to follower */
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    rv.candidate_id = 1;
    rv.last_log_idx = 1;
    rv.last_log_term = 1;

    raft_set_current_term(r, 1);

    /* server's idx are more up-to-date */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    raft_append_entry(r, &ety);
    ety.id = 101;
    ety.term = 2;
    raft_append_entry(r, &ety);

    /* vote not granted */
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);

    /* approve vote, because last_log_term is higher */
    raft_set_current_term(r, 2);
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 2;
    rv.candidate_id = 1;
    rv.last_log_idx = 1;
    rv.last_log_term = 3;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 1, rvr.vote_granted);
}

void TestRaft_follower_recv_appendentries_heartbeat_does_not_overwrite_logs(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* The server sends a follow up AE
     * NOTE: the server has received a response from the last AE so
     * prev_log_idx has been incremented */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 2;
    e[1].term = 1;
    e[1].id = 3;
    e[2].term = 1;
    e[2].id = 4;
    e[3].term = 1;
    e[3].id = 5;
    ae.entries = e;
    ae.n_entries = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* receive a heartbeat
     * NOTE: the leader hasn't received the response to the last AE so it can
     * only assume prev_Log_idx is still 1 */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 1;
    /* receipt of appendentries changes commit idx */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 5, raft_get_current_idx(r));
}

void TestRaft_follower_recv_appendentries_does_not_deleted_commited_entries(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[5];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* Follow up AE. Node responded with success */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 2;
    e[1].term = 1;
    e[1].id = 3;
    e[2].term = 1;
    e[2].id = 4;
    e[3].term = 1;
    e[3].id = 5;
    ae.entries = e;
    ae.n_entries = 4;
    ae.leader_commit = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* The server sends a follow up AE */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    memset(&e, 0, sizeof(msg_entry_t) * 5);
    e[0].term = 1;
    e[0].id = 2;
    e[1].term = 1;
    e[1].id = 3;
    e[2].term = 1;
    e[2].id = 4;
    e[3].term = 1;
    e[3].id = 5;
    e[4].term = 1;
    e[4].id = 6;
    ae.entries = e;
    ae.n_entries = 5;
    ae.leader_commit = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 6, raft_get_current_idx(r));
    CuAssertIntEquals(tc, 4, raft_get_commit_idx(r));

    /* The server sends a follow up AE.
     * This appendentry forces the node to check if it's going to delete
     * commited logs */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 3;
    ae.prev_log_term = 1;
    /* include entries */
    memset(&e, 0, sizeof(msg_entry_t) * 5);
    e[0].term = 1;
    e[0].id = 5;
    e[1].term = 1;
    e[1].id = 6;
    e[2].term = 1;
    e[2].id = 7;
    ae.entries = e;
    ae.n_entries = 3;
    ae.leader_commit = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 6, raft_get_current_idx(r));
}

void TestRaft_candidate_becomes_candidate_is_candidate(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_node(r, NULL, 1, 1);

    raft_become_candidate(r);
    CuAssertTrue(tc, raft_is_candidate(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_increments_current_term(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_node(r, NULL, 1, 1);

    CuAssertTrue(tc, 0 == raft_get_current_term(r));
    raft_become_candidate(r);
    CuAssertTrue(tc, 0 == raft_get_current_term(r));
    raft_become_prevoted_candidate(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_votes_for_self(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    CuAssertTrue(tc, -1 == raft_get_voted_for(r));
    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    CuAssertTrue(tc, raft_get_nodeid(r) == raft_get_voted_for(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_resets_election_timeout(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_election_timeout(r, 1000);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_periodic(r, 900);
    CuAssertTrue(tc, 900 == raft_get_timeout_elapsed(r));

    raft_become_candidate(r);
    /* time is selected randomly */
    CuAssertTrue(tc, raft_get_timeout_elapsed(r) < 1000);
}

void TestRaft_follower_recv_appendentries_resets_election_timeout(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_set_election_timeout(r, 1000);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 1);

    raft_periodic(r, 900);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    raft_recv_appendentries(r, raft_get_node(r, 1), &ae, &aer);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_requests_votes_from_other_servers(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = sender_requestvote,
    };
    msg_requestvote_t* rv;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* set term so we can check it gets included in the outbound message */
    raft_set_current_term(r, 2);

    /* becoming candidate triggers prevote requests */
    raft_become_candidate(r);

    /* 2 nodes = 2 prevote requests */
    rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertTrue(tc, 2 == rv->term);
    CuAssertTrue(tc, rv->prevote);
    /*  TODO: there should be more items */
    rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertTrue(tc, 2 == rv->term);
    CuAssertTrue(tc, rv->prevote);

    /* becoming candidate triggers vote requests */
    raft_become_prevoted_candidate(r);

    /* 2 nodes = 2 vote requests */
    rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertTrue(tc, 3 == rv->term);
    CuAssertTrue(tc, !rv->prevote);
    /*  TODO: there should be more items */
    rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertTrue(tc, 3 == rv->term);
    CuAssertTrue(tc, !rv->prevote);
}

/* Candidate 5.2 */
void TestRaft_candidate_election_timeout_and_no_leader_results_in_new_election(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_requestvote_response_t vr;
    memset(&vr, 0, sizeof(msg_requestvote_response_t));
    vr.term = 0;
    vr.vote_granted = 1;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_election_timeout(r, 1000);

    /* server wants to be leader, so becomes candidate */
    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));

    /* clock over (ie. max election timeout + 1), causing new election */
    raft_periodic(r, max_election_timeout(1000) + 1);
    CuAssertTrue(tc, raft_is_candidate(r));

    /*  receiving this vote gives the server majority */
//    raft_recv_requestvote_response(r,1,&vr);
//    CuAssertTrue(tc, 1 == raft_is_leader(r));
}

/* Candidate 5.2 */
void TestRaft_candidate_receives_majority_of_votes_becomes_leader(CuTest * tc)
{
    msg_requestvote_response_t vr;

    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_add_node(r, NULL, 5, 0);
    CuAssertTrue(tc, 5 == raft_get_num_nodes(r));

    /* vote for self */
    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));

    /* a vote for us */
    memset(&vr, 0, sizeof(msg_requestvote_response_t));
    vr.term = 1;
    vr.vote_granted = 1;
    /* get one vote */
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &vr);
    CuAssertTrue(tc, 2 == raft_get_nvotes_for_me(r));
    CuAssertTrue(tc, 0 == raft_is_leader(r));

    /* get another vote
     * now has majority (ie. 3/5 votes) */
    raft_recv_requestvote_response(r, raft_get_node(r, 3), &vr);
    CuAssertTrue(tc, 1 == raft_is_leader(r));
}

/* Candidate 5.2 */
void TestRaft_candidate_will_not_respond_to_voterequest_if_it_has_already_voted(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_vote(r, raft_get_node(r, 1));

    memset(&rv, 0, sizeof(msg_requestvote_t));
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);

    /* we've vote already, so won't respond with a vote granted... */
    CuAssertTrue(tc, 0 == rvr.vote_granted);
}

/* Candidate 5.2 */
void TestRaft_candidate_requestvote_includes_logidx(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_requestvote = sender_requestvote,
        .log              = NULL,
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_state(r, RAFT_STATE_CANDIDATE);

    raft_set_callbacks(r, &funcs, sender);
    raft_set_current_term(r, 5);
    /* 3 entries */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    raft_append_entry(r, &ety);
    ety.id = 101;
    raft_append_entry(r, &ety);
    ety.id = 102;
    ety.term = 3;
    raft_append_entry(r, &ety);
    raft_send_requestvote(r, raft_get_node(r, 2));

    msg_requestvote_t* rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertIntEquals(tc, 3, rv->last_log_idx);
    CuAssertIntEquals(tc, 5, rv->term);
    CuAssertIntEquals(tc, 3, rv->last_log_term);
    CuAssertIntEquals(tc, 1, rv->candidate_id);
}

void TestRaft_candidate_recv_requestvote_response_becomes_follower_if_current_term_is_less_than_term(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_vote(r, 0);
    CuAssertTrue(tc, 0 == raft_is_follower(r));
    CuAssertTrue(tc, -1 == raft_get_current_leader(r));
    CuAssertTrue(tc, 1 == raft_get_current_term(r));

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 2;
    rvr.vote_granted = 0;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    CuAssertTrue(tc, 2 == raft_get_current_term(r));
    CuAssertTrue(tc, -1 == raft_get_voted_for(r));
}

/* Test the assertion after the __should_grant_vote call in
 * raft_recv_requestvote. */
void TestRaft_candidate_may_grant_prevote_if_term_not_less_than_current_term(
    CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);
    raft_become_candidate(r);

    /* prevoting candidate may grant prevote */
    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    rv.prevote = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, rvr.vote_granted);

    raft_become_prevoted_candidate(r);
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));

    /* prevoted candidate may grant prevote */
    rv.term = 2;
    rv.prevote = 1;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, rvr.vote_granted);
}

/* Candidate 5.2 */
void TestRaft_candidate_recv_appendentries_frm_leader_results_in_follower(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_vote(r, 0);
    CuAssertTrue(tc, 0 == raft_is_follower(r));
    CuAssertTrue(tc, -1 == raft_get_current_leader(r));
    CuAssertTrue(tc, 0 == raft_get_current_term(r));

    /* receive recent appendentries */
    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    /* after accepting a leader, it's available as the last known leader */
    CuAssertTrue(tc, 2 == raft_get_current_leader(r));
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
    CuAssertTrue(tc, -1 == raft_get_voted_for(r));
}

void TestRaft_candidate_recv_appendentries_from_same_term_results_in_step_down(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);
    raft_become_candidate(r);
    CuAssertTrue(tc, raft_is_candidate(r));
    CuAssertIntEquals(tc, -1, raft_get_voted_for(r));
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 0 == raft_is_candidate(r));
}

/* Candidate 5.2 */
void TestRaft_prevoted_candidate_recv_appendentries_from_same_term_results_in_step_down(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);
    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);
    CuAssertTrue(tc, 0 == raft_is_follower(r));
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 0 == raft_is_candidate(r));

    /* The election algorithm requires that votedFor always contains the node
     * voted for in the current term (if any), which is why it is persisted.
     * By resetting that to -1 we have the following problem:
     *
     *  Node self, other1 and other2 becomes candidates
     *  Node other1 wins election
     *  Node self gets appendentries
     *  Node self resets votedFor
     *  Node self gets requestvote from other2
     *  Node self votes for Other2
    */
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));
}

void TestRaft_leader_becomes_leader_is_leader(CuTest * tc)
{
    void *r = raft_new();
    raft_become_leader(r);
    CuAssertTrue(tc, raft_is_leader(r));
}

void TestRaft_leader_becomes_leader_does_not_clear_voted_for(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .persist_vote = __raft_persist_vote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_vote(r, raft_get_node(r, 1));
    CuAssertTrue(tc, 1 == raft_get_voted_for(r));
    raft_become_leader(r);
    CuAssertTrue(tc, 1 == raft_get_voted_for(r));
}

void TestRaft_leader_when_becomes_leader_all_nodes_have_nextidx_equal_to_lastlog_idx_plus_1(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* candidate to leader */
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_become_leader(r);

    int i;
    for (i = 2; i <= 3; i++)
    {
        raft_node_t* p = raft_get_node(r, i);
        CuAssertTrue(tc, raft_get_current_idx(r) + 1 ==
                     raft_node_get_next_idx(p));
    }
}

/* 5.2 */
void TestRaft_leader_when_it_becomes_a_leader_sends_empty_appendentries(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* candidate to leader */
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_become_leader(r);

    /* receive appendentries messages for both nodes */
    msg_appendentries_t* ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

/* 5.2
 * Note: commit means it's been appended to the log, not applied to the FSM */
void TestRaft_leader_responds_to_entry_msg_when_entry_is_committed(CuTest * tc)
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
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    raft_recv_entry(r, &ety, &cr);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));

    /* trigger response through commit */
    raft_apply_entry(r);
}

void TestRaft_non_leader_recv_entry_msg_fails(CuTest * tc)
{
    msg_entry_response_t cr;

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);

    /* entry message */
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    int e = raft_recv_entry(r, &ety, &cr);
    CuAssertTrue(tc, RAFT_ERR_NOT_LEADER == e);
}

/* 5.3 */
void TestRaft_leader_sends_appendentries_with_NextIdx_when_PrevIdx_gt_NextIdx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    raft_entry_t etys[3] = {};
    raft_index_t i;
    for (i = 0; i < 3; i++)
    {
        etys[i].term = 1;
        etys[i].id = i + 1;
        etys[i].data.buf = "aaa";
        etys[i].data.len = 3;
        raft_append_entry(r, &etys[i]);
    }

    raft_node_t* p = raft_get_node(r, 2);
    raft_node_set_next_idx(p, 4);

    /* receive appendentries messages */
    raft_send_appendentries(r, p);
    msg_appendentries_t* ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

void TestRaft_leader_sends_appendentries_with_leader_commit(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    raft_index_t i;

    for (i=0; i<10; i++)
    {
        raft_entry_t ety = {};
        ety.term = 1;
        ety.id = 1;
        ety.data.buf = "aaa";
        ety.data.len = 3;
        raft_append_entry(r, &ety);
    }

    raft_set_commit_idx(r, 10);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    msg_appendentries_t*  ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->leader_commit == 10);
}

void TestRaft_leader_sends_appendentries_with_prevLogIdx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1); /* me */
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    msg_appendentries_t*  ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->prev_log_idx == 0);

    raft_node_t* n = raft_get_node(r, 2);

    /* add 1 entry */
    /* receive appendentries messages */
    raft_entry_t ety = {};
    ety.term = 2;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    raft_append_entry(r, &ety);
    raft_node_set_next_idx(n, 1);
    raft_send_appendentries(r, raft_get_node(r, 2));
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->prev_log_idx == 0);
    CuAssertTrue(tc, ae->n_entries == 1);
    CuAssertTrue(tc, ae->entries[0].id == 100);
    CuAssertTrue(tc, ae->entries[0].term == 2);

    /* set next_idx */
    /* receive appendentries messages */
    raft_node_set_next_idx(n, 2);
    raft_send_appendentries(r, raft_get_node(r, 2));
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->prev_log_idx == 1);
}

void TestRaft_leader_sends_appendentries_when_node_has_next_idx_of_0(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    msg_appendentries_t*  ae = sender_poll_msg_data(sender);

    /* add an entry */
    /* receive appendentries messages */
    raft_node_t* n = raft_get_node(r, 2);
    raft_node_set_next_idx(n, 1);
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    raft_append_entry(r, &ety);
    raft_send_appendentries(r, n);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->prev_log_idx == 0);
}

/* 5.3 */
void TestRaft_leader_retries_appendentries_with_decremented_NextIdx_log_inconsistency(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    msg_appendentries_t* ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

/*
 * If there exists an N such that N > commitidx, a majority
 * of matchidx[i] = N, and log[N].term == currentTerm:
 * set commitidx = N (�5.2, �5.4).  */
void TestRaft_leader_append_entry_to_log_increases_idxno(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;
    msg_entry_t ety = {};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    raft_recv_entry(r, &ety, &cr);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
}

#if 0
// TODO no support for duplicates
void T_estRaft_leader_doesnt_append_entry_if_unique_id_is_duplicate(CuTest * tc)
{
    void *r;

    /* 2 nodes */
    raft_node_configuration_t cfg[] = {
        { (void*)1 },
        { (void*)2 },
        { NULL     }
    };

    msg_entry_t ety;
    ety.id = 1;
    ety.data = "entry";
    ety.data.len = strlen("entry");

    r = raft_new();
    raft_set_configuration(r, cfg, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    raft_recv_entry(r, 1, &ety);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));

    raft_recv_entry(r, 1, &ety);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
}
#endif

void TestRaft_leader_recv_appendentries_response_increase_commit_idx_when_majority_have_entry_and_atleast_one_newer_entry(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    msg_appendentries_response_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_add_node(r, NULL, 5, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);
    ety.id = 2;
    raft_append_entry(r, &ety);
    ety.id = 3;
    raft_append_entry(r, &ety);

    memset(&aer, 0, sizeof(msg_appendentries_response_t));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    aer.first_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    /* leader will now have majority followers who have appended this log */
    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    raft_periodic(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;
    aer.first_idx = 2;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    /* leader will now have majority followers who have appended this log */
    CuAssertIntEquals(tc, 2, raft_get_commit_idx(r));
    raft_periodic(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_last_applied_idx(r));
}

void TestRaft_leader_recv_appendentries_response_set_has_sufficient_logs_for_node(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_term = __raft_persist_term,
        .node_has_sufficient_logs = __raft_node_has_sufficient_logs,
        .log                         = NULL
    };
    msg_appendentries_response_t aer;

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_node_t* node = raft_add_non_voting_node(r, NULL, 5, 0);

    int has_sufficient_logs_flag = 0;
    raft_set_callbacks(r, &funcs, &has_sufficient_logs_flag);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);
    ety.id = 2;
    raft_append_entry(r, &ety);
    ety.id = 3;
    raft_append_entry(r, &ety);

    memset(&aer, 0, sizeof(msg_appendentries_response_t));

    raft_send_appendentries(r, raft_get_node(r, 5));
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;
    aer.first_idx = 1;

    raft_recv_appendentries_response(r, node, &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);

    raft_recv_appendentries_response(r, node, &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);
}

void TestRaft_leader_recv_appendentries_response_increase_commit_idx_using_voting_nodes_majority(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    msg_appendentries_response_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_non_voting_node(r, NULL, 4, 0);
    raft_add_non_voting_node(r, NULL, 5, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);

    memset(&aer, 0, sizeof(msg_appendentries_response_t));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    aer.first_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    /* leader will now have majority followers who have appended this log */
    raft_periodic(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_leader_recv_appendentries_response_duplicate_does_not_decrement_match_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    msg_appendentries_response_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);
    ety.id = 2;
    raft_append_entry(r, &ety);
    ety.id = 3;
    raft_append_entry(r, &ety);

    memset(&aer, 0, sizeof(msg_appendentries_response_t));

    /* receive msg 1 */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    aer.first_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_node_get_match_idx(raft_get_node(r, 2)));

    /* receive msg 2 */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;
    aer.first_idx = 2;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 2, raft_node_get_match_idx(raft_get_node(r, 2)));

    /* receive msg 1 - because of duplication ie. unreliable network */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    aer.first_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 2, raft_node_get_match_idx(raft_get_node(r, 2)));
}

void TestRaft_leader_recv_appendentries_response_do_not_increase_commit_idx_because_of_old_terms_with_majority(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    msg_appendentries_response_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_add_node(r, NULL, 5, 0);
    raft_set_callbacks(r, &funcs, sender);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);
    ety.id = 2;
    raft_append_entry(r, &ety);
    ety.term = 2;
    ety.id = 3;
    raft_append_entry(r, &ety);

    memset(&aer, 0, sizeof(msg_appendentries_response_t));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    aer.first_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_periodic(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_last_applied_idx(r));

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;
    aer.first_idx = 2;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_periodic(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_last_applied_idx(r));

    /* THIRD entry log application */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses
     * let's say that the nodes have majority within leader's current term */
    aer.term = 2;
    aer.success = 1;
    aer.current_idx = 3;
    aer.first_idx = 3;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 3, raft_get_commit_idx(r));
    raft_periodic(r, 1);
    CuAssertIntEquals(tc, 3, raft_get_last_applied_idx(r));
}

void TestRaft_leader_recv_appendentries_response_jumps_to_lower_next_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    msg_appendentries_response_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);

    /* append entries */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);
    ety.term = 2;
    ety.id = 2;
    raft_append_entry(r, &ety);
    ety.term = 3;
    ety.id = 3;
    raft_append_entry(r, &ety);
    ety.term = 4;
    ety.id = 4;
    raft_append_entry(r, &ety);

    msg_appendentries_t* ae;

    /* become leader sets next_idx to current_idx */
    raft_become_leader(r);
    raft_node_t* node = raft_get_node(r, 2);
    CuAssertIntEquals(tc, 5, raft_node_get_next_idx(node));
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));
    CuAssertIntEquals(tc, 4, ae->prev_log_term);
    CuAssertIntEquals(tc, 4, ae->prev_log_idx);

    /* receive mock success responses */
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 2;
    aer.success = 0;
    aer.current_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 2, raft_node_get_next_idx(node));

    /* see if new appendentries have appropriate values */
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));
    CuAssertIntEquals(tc, 1, ae->prev_log_term);
    CuAssertIntEquals(tc, 1, ae->prev_log_idx);

    CuAssertTrue(tc, NULL == sender_poll_msg_data(sender));
}

void TestRaft_leader_recv_appendentries_response_decrements_to_lower_next_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    msg_appendentries_response_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);

    /* append entries */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);
    ety.term = 2;
    ety.id = 2;
    raft_append_entry(r, &ety);
    ety.term = 3;
    ety.id = 3;
    raft_append_entry(r, &ety);
    ety.term = 4;
    ety.id = 4;
    raft_append_entry(r, &ety);

    msg_appendentries_t* ae;

    /* become leader sets next_idx to current_idx */
    raft_become_leader(r);
    raft_node_t* node = raft_get_node(r, 2);
    CuAssertIntEquals(tc, 5, raft_node_get_next_idx(node));
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));
    CuAssertIntEquals(tc, 4, ae->prev_log_term);
    CuAssertIntEquals(tc, 4, ae->prev_log_idx);

    /* receive mock success responses */
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 2;
    aer.success = 0;
    aer.current_idx = 4;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 4, raft_node_get_next_idx(node));

    /* see if new appendentries have appropriate values */
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));
    CuAssertIntEquals(tc, 3, ae->prev_log_term);
    CuAssertIntEquals(tc, 3, ae->prev_log_idx);

    /* receive mock success responses */
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 2;
    aer.success = 0;
    aer.current_idx = 4;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 3, raft_node_get_next_idx(node));

    /* see if new appendentries have appropriate values */
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));
    CuAssertIntEquals(tc, 2, ae->prev_log_term);
    CuAssertIntEquals(tc, 2, ae->prev_log_idx);

    CuAssertTrue(tc, NULL == sender_poll_msg_data(sender));
}

void TestRaft_leader_recv_appendentries_response_retry_only_if_leader(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);

    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));

    CuAssertTrue(tc, NULL != sender_poll_msg_data(sender));
    CuAssertTrue(tc, NULL != sender_poll_msg_data(sender));

    raft_become_follower(r);

    /* receive mock success responses */
    msg_appendentries_response_t aer;
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    aer.first_idx = 1;
    CuAssertTrue(tc, RAFT_ERR_NOT_LEADER == raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer));
    CuAssertTrue(tc, NULL == sender_poll_msg_data(sender));
}

void TestRaft_leader_recv_appendentries_response_without_node_fails(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    /* receive mock success responses */
    msg_appendentries_response_t aer;
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 0;
    aer.first_idx = 0;
    CuAssertIntEquals(tc, -1, raft_recv_appendentries_response(r, NULL, &aer));
}

void TestRaft_leader_recv_entry_resets_election_timeout(
    CuTest * tc)
{
    void *r = raft_new();
    raft_set_election_timeout(r, 1000);
    raft_set_state(r, RAFT_STATE_LEADER);

    raft_periodic(r, 900);

    /* entry message */
    msg_entry_t mety = {};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    msg_entry_response_t cr;
    raft_recv_entry(r, &mety, &cr);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));
}

void TestRaft_leader_recv_entry_is_committed_returns_0_if_not_committed(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* entry message */
    msg_entry_t mety = {};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    msg_entry_response_t cr;
    raft_recv_entry(r, &mety, &cr);
    CuAssertTrue(tc, 0 == raft_msg_entry_response_committed(r, &cr));

    raft_set_commit_idx(r, 1);
    CuAssertTrue(tc, 1 == raft_msg_entry_response_committed(r, &cr));
}

void TestRaft_leader_recv_entry_is_committed_returns_neg_1_if_invalidated(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* entry message */
    msg_entry_t mety = {};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    msg_entry_response_t cr;
    raft_recv_entry(r, &mety, &cr);
    CuAssertTrue(tc, 0 == raft_msg_entry_response_committed(r, &cr));
    CuAssertTrue(tc, cr.term == 1);
    CuAssertTrue(tc, cr.idx == 1);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    /* append entry that invalidates entry message */
    msg_appendentries_t ae;
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.leader_commit = 1;
    ae.term = 2;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    msg_appendentries_response_t aer;
    msg_entry_t e[1];
    memset(&e, 0, sizeof(msg_entry_t) * 1);
    e[0].term = 2;
    e[0].id = 999;
    e[0].data.buf = "aaa";
    e[0].data.len = 3;
    ae.entries = e;
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    CuAssertTrue(tc, 1 == raft_get_commit_idx(r));
    CuAssertTrue(tc, -1 == raft_msg_entry_response_committed(r, &cr));
}

void TestRaft_leader_recv_entry_fails_if_prevlogidx_less_than_commit(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);

    /* entry message */
    msg_entry_t mety = {};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    msg_entry_response_t cr;
    raft_recv_entry(r, &mety, &cr);
    CuAssertTrue(tc, 0 == raft_msg_entry_response_committed(r, &cr));
    CuAssertTrue(tc, cr.term == 2);
    CuAssertTrue(tc, cr.idx == 1);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    raft_set_commit_idx(r, 1);

    /* append entry that invalidates entry message */
    msg_appendentries_t ae;
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.leader_commit = 1;
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    msg_appendentries_response_t aer;
    msg_entry_t e[1];
    memset(&e, 0, sizeof(msg_entry_t) * 1);
    e[0].term = 2;
    e[0].id = 999;
    e[0].data.buf = "aaa";
    e[0].data.len = 3;
    ae.entries = e;
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, 0, aer.success);
}

void TestRaft_leader_recv_entry_does_not_send_new_appendentries_to_slow_nodes(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    raft_set_callbacks(r, &funcs, sender);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* make the node slow */
    raft_node_set_next_idx(raft_get_node(r, 2), 1);

    /* append entries */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);

    /* entry message */
    msg_entry_t mety = {};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    msg_entry_response_t cr;
    raft_recv_entry(r, &mety, &cr);

    /* check if the slow node got sent this appendentries */
    msg_appendentries_t* ae;
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL == ae);
}

void TestRaft_leader_recv_appendentries_response_failure_does_not_set_node_nextid_to_0(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* append entries */
    raft_entry_t ety = {};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    raft_append_entry(r, &ety);

    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));

    /* receive mock success response */
    msg_appendentries_response_t aer;
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 1;
    aer.success = 0;
    aer.current_idx = 0;
    aer.first_idx = 0;
    raft_node_t* p = raft_get_node(r, 2);
    raft_recv_appendentries_response(r, p, &aer);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));
    raft_recv_appendentries_response(r, p, &aer);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));
}

void TestRaft_leader_recv_appendentries_response_increment_idx_of_node(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    raft_node_t* p = raft_get_node(r, 2);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));

    /* receive mock success responses */
    msg_appendentries_response_t aer;
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 0;
    aer.first_idx = 0;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_node_get_next_idx(p));
}

void TestRaft_leader_recv_appendentries_response_drop_message_if_term_is_old(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);

    raft_node_t* p = raft_get_node(r, 2);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));

    /* receive OLD mock success responses */
    msg_appendentries_response_t aer;
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    aer.first_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));
}

void TestRaft_leader_recv_appendentries_response_steps_down_if_term_is_newer(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);

    raft_node_t* p = raft_get_node(r, 2);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));

    /* receive NEW mock failed responses */
    msg_appendentries_response_t aer;
    aer.term = 3;
    aer.success = 0;
    aer.current_idx = 2;
    aer.first_idx = 0;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    CuAssertTrue(tc, -1 == raft_get_current_leader(r));
}

void TestRaft_leader_recv_appendentries_steps_down_if_newer(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 5);
    /* check that node 1 considers itself the leader */
    CuAssertTrue(tc, 1 == raft_is_leader(r));
    CuAssertTrue(tc, 1 == raft_get_current_leader(r));

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 6;
    ae.prev_log_idx = 6;
    ae.prev_log_term = 5;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* after more recent appendentries from node 2, node 1 should
     * consider node 2 the leader. */
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    CuAssertTrue(tc, 2 == raft_get_current_leader(r));
}

void TestRaft_leader_recv_appendentries_steps_down_if_newer_term(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 5);

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 6;
    ae.prev_log_idx = 5;
    ae.prev_log_term = 5;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == raft_is_follower(r));
}

void TestRaft_leader_sends_empty_appendentries_every_request_timeout(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_election_timeout(r, 1000);
    raft_set_request_timeout(r, 500);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    /* candidate to leader */
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_become_leader(r);

    /* receive appendentries messages for both nodes */
    msg_appendentries_t* ae;
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);

    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL == ae);

    /* force request timeout */
    raft_periodic(r, 501);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

/* TODO: If a server receives a request with a stale term number, it rejects the request. */
#if 0
void T_estRaft_leader_sends_appendentries_when_receive_entry_msg(CuTest * tc)
#endif

void TestRaft_leader_recv_prevote_responds_without_granting(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_vote = __raft_persist_vote,
        .persist_term = __raft_persist_term,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_election_timeout(r, 1000);
    raft_set_request_timeout(r, 500);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_is_leader(r));

    /* receive request vote from node 3 */
    msg_requestvote_t rv;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    rv.prevote = 1;
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);
}

void TestRaft_leader_recv_requestvote_responds_without_granting(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_vote = __raft_persist_vote,
        .persist_term = __raft_persist_term,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_election_timeout(r, 1000);
    raft_set_request_timeout(r, 500);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_become_candidate(r);
    raft_become_prevoted_candidate(r);

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_is_leader(r));

    /* receive request vote from node 3 */
    msg_requestvote_t rv;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);
}

#if 0
/* This test is disabled because it violates the Raft paper's view on 
 * ignoring RequestVotes when a leader is established.
 */
void T_estRaft_leader_recv_requestvote_responds_with_granting_if_term_is_higher(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_vote = __raft_persist_vote,
        .persist_term = __raft_persist_term,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_election_timeout(r, 1000);
    raft_set_request_timeout(r, 500);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_election_start(r);

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_is_leader(r));

    /* receive request vote from node 3 */
    msg_requestvote_t rv;
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 2;
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
}
#endif

void TestRaft_leader_recv_appendentries_response_set_has_sufficient_logs_after_voting_committed(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_term = __raft_persist_term,
        .node_has_sufficient_logs = __raft_node_has_sufficient_logs,
        .log_get_node_id = __raft_log_get_node_id,
        .log_offer = __raft_log_offer
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);

    int has_sufficient_logs_flag = 0;
    raft_set_callbacks(r, &funcs, &has_sufficient_logs_flag);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    /* Add two non-voting nodes */
    raft_entry_t ety = {
        .term = 1, .id = 1,
        .data.buf = "2", .data.len = 2,
        .type = RAFT_LOGTYPE_ADD_NONVOTING_NODE
    };
    msg_entry_response_t etyr;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, &ety, &etyr));
    ety.id++;
    ety.data.buf = "3";
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, &ety, &etyr));

    msg_appendentries_response_t aer = {
        .term = 1, .success = 1, .current_idx = 2, .first_idx = 0
    };

    /* node 3 responds so it has sufficient logs and will be promoted */
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);

    ety.id++;
    ety.type = RAFT_LOGTYPE_ADD_NODE;
    raft_recv_entry(r, &ety, &etyr);

    /* we now get a response from node 2, but it's still behind */
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);

    /* both nodes respond to the promotion */
    aer.first_idx = 2;
    aer.current_idx = 3;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    raft_apply_all(r);

    /* voting change is committed, so next time we hear from node 2
     * it should be considered as having all logs and can be promoted
     * as well.
     */
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 2, has_sufficient_logs_flag);
}
