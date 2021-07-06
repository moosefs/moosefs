.. image:: https://travis-ci.org/willemt/raft.png
   :target: https://travis-ci.org/willemt/raft

.. image:: https://coveralls.io/repos/willemt/raft/badge.png
  :target: https://coveralls.io/r/willemt/raft

C implementation of the Raft consensus protocol, BSD licensed.

See `raft.h <https://github.com/willemt/raft/blob/master/include/raft.h>`_ for full documentation.

See `ticketd <https://github.com/willemt/ticketd>`_ for real life use of this library.

Networking is out of scope for this project. The implementor will need to do all the plumbing. The library doesn't assume a network layer with ordering or duplicate detection. This means you could use UDP for transmission.

There are no dependencies, however https://github.com/willemt/linked-List-queue is required for testing.

Building
========

.. code-block:: bash
   :class: ignore

   make tests

Quality Assurance
=================

We use the following methods to ensure that the library is safe:

* A `simulator <https://github.com/willemt/virtraft>`_ is used to test the Raft invariants on unreliable networks
* `Fuzzing/property-based-testing <https://github.com/willemt/virtraft/blob/master/tests/test_fuzzer.py>`_ via `Hypothesis <https://github.com/DRMacIver/hypothesis/>`_
* All bugs have regression tests
* Many unit tests
* `Usage <https://github.com/willemt/ticketd>`_

Single file amalgamation
========================

The source has been amalgamated into a single ``raft.h`` header file.
Use `clib <https://github.com/clibs/clib>`_ to download the source into your project's ``deps`` folder, ie:

.. code-block:: bash

    brew install clib
    clib install willemt/raft_amalgamation

The file is stored in the ``deps`` folder like below:

.. code-block:: bash

    deps/raft/raft.h

How to integrate with this library
==================================

See `ticketd <https://github.com/willemt/ticketd>`_ for an example of how to integrate with this library.

If you don't have access to coroutines it's easiest to use two separate threads - one for handling Raft peer traffic, and another for handling client traffic. 

Be aware that this library is not thread safe. You will need to ensure that the library's functions are called exclusively.

Initializing the Raft server
----------------------------

Instantiate a new Raft server using ``raft_new``.

.. code-block:: c

    void* raft = raft_new();

We tell the Raft server what the cluster configuration is by using the ``raft_add_node`` function. For example, if we have 5 servers [#]_ in our cluster, we call ``raft_add_node`` 5 [#]_ times.

.. code-block:: c

    raft_add_node(raft, connection_user_data, node_id, peer_is_self);

Where:

* ``connection_user_data`` is a pointer to user data.
* ``peer_is_self`` is boolean indicating that this is the current server's server index.
* ``node_id`` is the unique integer ID of the node. Peers use this to identify themselves. This SHOULD be a random integer.

.. [#] AKA "Raft peer"
.. [#] We have to also include the Raft server itself in the raft_add_node calls. When we call raft_add_node for the Raft server, we set peer_is_self to 1. 

Calling raft_periodic() periodically
------------------------------------

We need to call ``raft_periodic`` at periodic intervals.

.. code-block:: c

    raft_periodic(raft, 1000);

*Example using a libuv timer:*

.. code-block:: c

    static void __periodic(uv_timer_t* handle)
    {
        raft_periodic(sv->raft, PERIOD_MSEC);
    }

    uv_timer_t *periodic_req;
    periodic_req = malloc(sizeof(uv_timer_t));
    periodic_req->data = sv;
    uv_timer_init(&peer_loop, periodic_req);
    uv_timer_start(periodic_req, __periodic, 0, 1000);

Receiving the entry (ie. client sends entry to Raft cluster)
------------------------------------------------------------

Our Raft application receives log entries from the client.

When this happens we need to:

* Redirect the client to the Raft cluster leader (if necessary)
* Append the entry to our log
* Block until the log entry has been committed [#]_

.. [#] When the log entry has been replicated across a majority of servers in the Raft cluster

**Append the entry to our log**

We call ``raft_recv_entry`` when we want to append the entry to the log.

.. code-block:: c

    msg_entry_response_t response;
    e = raft_recv_entry(raft,  &entry, &response);

You should populate the ``entry`` struct with the log entry the client has sent. After the call completes the ``response`` parameter is populated and can be used by the ``raft_msg_entry_response_committed`` function to check if the log entry has been committed or not.

**Blocking until the log entry has been committed**

When the server receives a log entry from the client, it has to block until the entry is committed. This is necessary as our Raft server has to replicate the log entry with the other peers of the Raft cluster.

The ``raft_recv_entry`` function does not block! This means you will need to implement the blocking functionality yourself.  

*Example below is from the ticketd client thread. This shows that we need to block on client requests. ticketd does the blocking by waiting on a conditional, which is signalled by the peer thread. The separate thread is responsible for handling traffic between Raft peers.*

.. code-block:: c

    msg_entry_response_t response;

    e = raft_recv_entry(sv->raft, &entry, &response);
    if (0 != e)
        return h2oh_respond_with_error(req, 500, "BAD");

    /* block until the entry is committed */
    int done = 0;
    do {
        uv_cond_wait(&sv->appendentries_received, &sv->raft_lock);
        e = raft_msg_entry_response_committed(sv->raft, &r);
        switch (e)
        {
            case 0:
                /* not committed yet */
                break;
            case 1:
                done = 1;
                uv_mutex_unlock(&sv->raft_lock);
                break;
            case -1:
                uv_mutex_unlock(&sv->raft_lock);
                return h2oh_respond_with_error(req, 400, "TRY AGAIN");
        }
    } while (!done);

*Example from ticketd of the peer thread. When an appendentries response is received from a Raft peer, we signal to the client thread that an entry might be committed.*

.. code-block:: c

    e = raft_recv_appendentries_response(sv->raft, conn->node, &m.aer);
    uv_cond_signal(&sv->appendentries_received);

**Redirecting the client to the leader**

When we receive an entry log from the client it's possible we might not be a leader.

If we aren't currently the leader of the raft cluster, we MUST send a redirect error message to the client. This is so that the client can connect directly to the leader in future connections. This enables future requests to be faster (ie. no redirects are required after the first redirect until the leader changes).

We use the ``raft_get_current_leader`` function to check who is the current leader.

*Example of ticketd sending a 301 HTTP redirect response:*

.. code-block:: c

    /* redirect to leader if needed */
    raft_node_t* leader = raft_get_current_leader_node(sv->raft);
    if (!leader)
    {
        return h2oh_respond_with_error(req, 503, "Leader unavailable");
    }
    else if (raft_node_get_id(leader) != sv->node_id)
    {
        /* send redirect */
        peer_connection_t* conn = raft_node_get_udata(leader);
        char leader_url[LEADER_URL_LEN];
        static h2o_generator_t generator = { NULL, NULL };
        static h2o_iovec_t body = { .base = "", .len = 0 };
        req->res.status = 301;
        req->res.reason = "Moved Permanently";
        h2o_start_response(req, &generator);
        snprintf(leader_url, LEADER_URL_LEN, "http://%s:%d/",
                 inet_ntoa(conn->addr.sin_addr), conn->http_port);
        h2o_add_header(&req->pool,
                       &req->res.headers,
                       H2O_TOKEN_LOCATION,
                       leader_url,
                       strlen(leader_url));
        h2o_send(req, &body, 1, 1);
        return 0;
    }

Function callbacks
------------------

You provide your callbacks to the Raft server using ``raft_set_callbacks``.

The following callbacks MUST be implemented: ``send_requestvote``, ``send_appendentries``, ``applylog``, ``persist_vote``, ``persist_term``, ``log_offer``, and ``log_pop``.

*Example of function callbacks being set:*

.. code-block:: c

    raft_cbs_t raft_callbacks = {
        .send_requestvote            = __send_requestvote,
        .send_appendentries          = __send_appendentries,
        .applylog                    = __applylog,
        .persist_vote                = __persist_vote,
        .persist_term                = __persist_term,
        .log_offer                   = __raft_logentry_offer,
        .log_poll                    = __raft_logentry_poll,
        .log_pop                     = __raft_logentry_pop,
        .log                         = __raft_log,
    };

    char* user_data = "test";

    raft_set_callbacks(raft, &raft_callbacks, user_data);

**send_requestvote()**

For this callback we have to serialize a ``msg_requestvote_t`` struct, and then send it to the peer identified by ``node``.

*Example from ticketd showing how the callback is implemented:*

.. code-block:: c

    static int __send_requestvote(
        raft_server_t* raft,
        void *udata,
        raft_node_t* node,
        msg_requestvote_t* m
        )
    {
        peer_connection_t* conn = raft_node_get_udata(node);

        uv_buf_t bufs[1];
        char buf[RAFT_BUFLEN];
        msg_t msg = {
            .type              = MSG_REQUESTVOTE,
            .rv                = *m
        };
        __peer_msg_serialize(tpl_map("S(I$(IIII))", &msg), bufs, buf);
        int e = uv_try_write(conn->stream, bufs, 1);
        if (e < 0)
            uv_fatal(e);
        return 0;
    }

**send_appendentries()**

For this callback we have to serialize a ``msg_appendentries_t`` struct, and then send it to the peer identified by ``node``. This struct is more complicated to serialize because the ``m->entries`` array might be populated.

*Example from ticketd showing how the callback is implemented:*

.. code-block:: c

    static int __send_appendentries(
        raft_server_t* raft,
        void *user_data,
        raft_node_t* node,
        msg_appendentries_t* m
        )
    {
        uv_buf_t bufs[3];

        peer_connection_t* conn = raft_node_get_udata(node);

        char buf[RAFT_BUFLEN], *ptr = buf;
        msg_t msg = {
            .type              = MSG_APPENDENTRIES,
            .ae                = {
                .term          = m->term,
                .prev_log_idx  = m->prev_log_idx,
                .prev_log_term = m->prev_log_term,
                .leader_commit = m->leader_commit,
                .n_entries     = m->n_entries
            }
        };
        ptr += __peer_msg_serialize(tpl_map("S(I$(IIIII))", &msg), bufs, ptr);

        /* appendentries with payload */
        if (0 < m->n_entries)
        {
            tpl_bin tb = {
                .sz   = m->entries[0].data.len,
                .addr = m->entries[0].data.buf
            };

            /* list of entries */
            tpl_node *tn = tpl_map("IIIB",
                &m->entries[0].id,
                &m->entries[0].term,
                &m->entries[0].type,
                &tb);
            size_t sz;
            tpl_pack(tn, 0);
            tpl_dump(tn, TPL_GETSIZE, &sz);
            e = tpl_dump(tn, TPL_MEM | TPL_PREALLOCD, ptr, RAFT_BUFLEN);
            assert(0 == e);
            bufs[1].len = sz;
            bufs[1].base = ptr;
            e = uv_try_write(conn->stream, bufs, 2);
            if (e < 0)
                uv_fatal(e);

            tpl_free(tn);
        }
        else
        {
            /* keep alive appendentries only */
            e = uv_try_write(conn->stream, bufs, 1);
            if (e < 0)
                uv_fatal(e);
        }

        return 0;
    }


**applylog()**

This callback is all what is needed to interface the FSM with the Raft library. Depending on your application, you might want to save the commit_idx to disk inside this callback.

**persist_vote() & persist_term()**

These callbacks simply save data to disk, so that when the Raft server is rebooted it starts from a valid state. This is necessary to ensure safety.

**log_offer()**

For this callback the user needs to add a log entry. The log MUST be synced to disk before this callback can return.

**log_poll()**

For this callback the user needs to remove the eldest log entry [#]_. The log MUST be synced to disk before this callback can return.

This callback only needs to be implemented to support log compaction.

**log_pop()**

For this callback the user needs to remove the youngest log entry [#]_. The log MUST be synced to disk before this callback can return.

.. [#] The log entry at the front of the log
.. [#] The log entry at the back of the log

Receving traffic from peers
---------------------------

To receive ``Append Entries``, ``Append Entries response``, ``Request Vote``, and ``Request Vote response`` messages, you need to deserialize the bytes into the message's corresponding struct.

The table below shows the structs that you need to deserialize-to or deserialize-from:

+-------------------------+------------------------------+----------------------------------+
| Message Type            | Struct                       | Function                         |
+-------------------------+------------------------------+----------------------------------+
| Append Entries          | msg_appendentries_t          | raft_recv_appendentries          |
+-------------------------+------------------------------+----------------------------------+
| Append Entries response | msg_appendentries_response_t | raft_recv_appendentries_response |
+-------------------------+------------------------------+----------------------------------+
| Request Vote            | msg_requestvote_t            | raft_recv_requestvote            |
+-------------------------+------------------------------+----------------------------------+
| Request Vote response   | msg_requestvote_response_t   | raft_recv_requestvote_response   |
+-------------------------+------------------------------+----------------------------------+

*Example of how we receive an Append Entries message, and reply to it:*

.. code-block:: c

    msg_appendentries_t ae;
    msg_appendentries_response_t response;
    char buf_in[1024], buf_out[1024];
    size_t len_in, len_out;

    read(socket, buf_in, &len_in);

    deserialize_appendentries(buf_in, len_in, &ae);

    e = raft_recv_requestvote(sv->raft, conn->node, &ae, &response);

    serialize_appendentries_response(&response, buf_out, &len_out);

    write(socket, buf_out, &len_out);

Membership changes
------------------
Membership changes are managed on the Raft log. You need two log entries to add a server to the cluster. While to remove you only need one log entry. There are two log entries for adding a server because we need to ensure that the new server's log is up to date before it can take part in voting.

It's highly recommended that when a node is added to the cluster that its node ID is random. This is especially important if the server was once connected to the cluster.

**Adding a node**

1. Append the configuration change using ``raft_recv_entry``. Make sure the entry has the type set to ``RAFT_LOGTYPE_ADD_NONVOTING_NODE``

2. Once ``node_has_sufficient_logs`` callback fires, append a configuration finalization log entry using ``raft_recv_entry``. Make sure the entry has a type set to ``RAFT_LOGTYPE_ADD_NODE``

**Removing a node**

1. Append the configuration change using ``raft_recv_entry``. Make sure the entry has the type set to ``RAFT_LOGTYPE_REMOVE_NODE``

2. Once the ``RAFT_LOGTYPE_REMOVE_NODE`` configuration change log is applied in the ``applylog`` callback we shutdown the server if it is to be removed.

**Membership callback**

The ``notify_membership_event`` callback can be used to track nodes as they are added and removed as a result of configuration change log entries. A typical use case is to create and destroy connections to nodes, using connection information obtained from the configuration change log entry.

Log Compaction
--------------
The log compaction method supported is called "Snapshotting for memory-based state machines" (Ongaro, 2014)

The user has to implement the ``send_installsnapshot``, ``recv_installsnapshot``, and ``recv_installsnapshot_response`` callbacks, in a way similar to those for appendentries messages. The implementor has to serialize and deserialize the snapshot, send it in chunks, and determine the completeness of a snapshot transfer.

The process works like this:

1. Begin snapshotting with ``raft_begin_snapshot``.
2. Save the current membership details to the snapshot.
3. Save the finite state machine to the snapshot.
4. End snapshotting with ``raft_end_snapshot``.
5. When the ``send_installsnapshot`` callback fires, the user must propogate a chunk of the snapshot to the other node by ``msg_installsnapshot_t`` and implementation-specific additional arguments (e.g., the offset of the chunk).
6. When the peer receives the chunk, the user must call ``raft_recv_installsnapshot``. When ``recv_installsnapshot`` fires, the user must process the chunk and fill any implementation-specific arguments to the response.
7. When the ``recv_installsnapshot_response`` callback fires, the user must record the progress of the snapshot transfer, typically in the user data of the ``raft_node_t`` object for the peer.
8. Once the peer has the complete snapshot, the user must call ``raft_begin_load_snapshot``.
9. Peer calls ``raft_remove_node`` to remove all nodes.
10. Peer calls ``raft_add_node`` to add nodes as per the snapshot's membership info.
11. Finally, peer calls ``raft_node_set_active`` to nodes as per the snapshot's membership info.

When a node receives a snapshot it could reuse that snapshot itself for other nodes.

Roadmap
=======

* Batch friendly interfaces - we can speed up Raft by adding new APIs that support batching many log entries
* Implementing linearizable semantics (Ongaro, 2014)
* Processing read-only queries more efficiently (Ongaro, 2014)

References
==========
Ongaro, D. (2014). Consensus: bridging theory and practice. Retrieved from https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
