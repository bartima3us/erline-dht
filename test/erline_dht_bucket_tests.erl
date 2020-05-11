%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% erline_dht_bucket module unit tests.
%%% @end
%%% Created : 18. Aug 2019 16.11
%%%-------------------------------------------------------------------
-module(erline_dht_bucket_tests).
-author("bartimaeus").
-include_lib("eunit/include/eunit.hrl").

-type status()      :: suspicious | active | not_active.
-type request()     :: ping | find_node | get_peers | announce.
-type tx_id()       :: binary().
-type distance()    :: 0..160.

-record(get_peers_search, {
    info_hash       :: binary(),
    last_changed    :: calendar:datetime()
}).

-record(requested_node, {
    ip_port     :: {inet:ip_address(), inet:port_number()},
    tx_id       :: tx_id(),
    info_hash   :: binary()
}).

-record(node, {
    ip_port                         :: {inet:ip_address(), inet:port_number()},
    hash                            :: binary(),
    token_received                  :: binary(),
    token_sent                      :: binary(),
    last_changed                    :: calendar:datetime(),
    tx_id           = <<0,0>>       :: tx_id(),
    active_txs      = []            :: [{request(), tx_id()}],
    status          = suspicious    :: status(),
    distance                        :: distance()
}).

-record(bucket, {
    distance            :: distance(),
    check_timer         :: reference(),
    ping_timer          :: reference(),
    nodes       = []    :: [#node{}]
}).

-record(info_hash, {
    info_hash           :: binary(),
    peers       = []    :: [{inet:ip_address(), inet:port_number()}]
}).

-record(state, {
    name                                :: atom(),
    my_node_hash                        :: binary(),
    socket                              :: port(),
    k                                   :: pos_integer(),
    buckets                     = []    :: [#bucket{}],
    info_hashes                 = []    :: [#info_hash{}],
    get_peers_searches_timer            :: reference(),
    clear_not_assigned_nodes_timer      :: reference(),
    update_tokens_timer                 :: reference(),
    db_mod                              :: module(),
    event_mgr_pid                       :: pid(),
    not_assigned_clearing_threshold     :: pos_integer(),
    valid_tokens                = []    :: [binary()],
    peer_port                           :: inet:port_number()
}).

-define(NODES_LIST, [
    #node{ip_port = {{12,34,92,156}, 6863}, last_changed = {{2020,7,1},{12,0,0}}, status = active},
    #node{ip_port = {{12,34,92,157}, 6864}, last_changed = {{2020,7,1},{11,0,0}}, status = active},
    #node{ip_port = {{12,34,92,158}, 6865}, last_changed = {{2020,7,1},{14,0,0}}, status = active},
    #node{ip_port = {{12,34,92,159}, 6866}, last_changed = {{2020,7,1},{11,30,0}}, status = suspicious},
    #node{ip_port = {{12,34,92,160}, 6867}, last_changed = {{2020,7,1},{14,30,20}}, status = active},
    #node{ip_port = {{12,34,92,161}, 6868}, last_changed = {{2020,7,1},{15,0,0}}, status = active},
    #node{ip_port = {{12,34,92,162}, 6869}, last_changed = {{2020,7,1},{16,0,0}}, status = suspicious},
    #node{ip_port = {{12,34,92,163}, 6870}, last_changed = {{2020,7,1},{17,0,0}}, status = suspicious},
    #node{ip_port = {{12,34,92,164}, 6871}, last_changed = {{2020,7,1},{18,0,0}}, status = not_active},
    #node{ip_port = {{12,34,92,165}, 6872}, last_changed = {{2020,7,1},{19,0,0}}, status = not_active},
    #node{ip_port = {{12,34,92,166}, 6873}, last_changed = {{2020,7,1},{20,0,0}}, status = active}
]).


%%
%%
%%
handle_ping_query_test_() ->
    EventMgrPid = erlang:list_to_pid("<0.0.100>"),
    State = #state{
        event_mgr_pid = EventMgrPid,
        db_mod        = erline_dht_db_ets,
        socket        = sock,
        my_node_hash  = <<"h45h">>,
        buckets       = [
            #bucket{
                distance = 1,
                nodes    = [#node{ip_port = {{12,34,92,155}, 6862}}]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>], ok),
            ok = meck:expect(erline_dht_helper, notify, [EventMgrPid, {ping, q, {12,34,92,155}, 6862, <<"n0d3_h45h">>}], ok),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862], [])
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Handle ping query. Querying node is in the bucket.",
            fun() ->
                ?assertEqual(
                    #state{
                        event_mgr_pid = EventMgrPid,
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = [#node{ip_port = {{12,34,92,155}, 6862}, last_changed = {{2020,7,1},{12,0,0}}}]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_ping_query({12,34,92,155}, 6862, <<"n0d3_h45h">>, <<0,2>>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {ping, q, {12,34,92,155}, 6862, <<"n0d3_h45h">>}])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862])
                )
            end
        }]
    }.


%%
%%
%%
handle_ping_response_test_() ->
    EventMgrPid = erlang:list_to_pid("<0.0.100>"),
    State = #state{
        event_mgr_pid = EventMgrPid,
        db_mod        = erline_dht_db_ets,
        socket        = sock,
        my_node_hash  = <<"h45h">>,
        k             = 3,
        buckets       = [
            Bucket = #bucket{
                distance = 1,
                nodes    = [
                    #node{
                        hash     = <<"my_n0d3_h45h">>,
                        ip_port  = {{12,34,92,155}, 6862},
                        distance = 1
                    }
                ]
            },
            #bucket{
                distance = 2,
                nodes    = []
            },
            #bucket{ % No free space in this bucket
                distance = 3,
                nodes    = [
                    #node{
                        ip_port  = {{12,34,92,156}, 6863},
                        status   = active
                    },
                    #node{
                        ip_port  = {{12,34,92,157}, 6864},
                        status   = active
                    },
                    #node{
                        ip_port  = {{12,34,92,158}, 6865},
                        status   = active
                    }
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_helper, get_distance, fun
                (<<"h45h">>, <<"n3w_n0d3_h45h1">>) -> {error, {malformed_hashes, <<"h45h">>, <<"n3w_n0d3_h45h1">>}};
                (<<"h45h">>, <<"n3w_n0d3_h45h2">>) -> {ok, 1};
                (<<"h45h">>, <<"n3w_n0d3_h45h3">>) -> {ok, 2};
                (<<"h45h">>, <<"n3w_n0d3_h45h4">>) -> {ok, 3}; % No free space in this bucket
                (<<"h45h">>, <<"n3w_n0d3_h45h5">>) -> {ok, 1};
                (<<"h45h">>, <<"n3w_n0d3_h45h6">>) -> {ok, 3}  % No free space in this bucket
            end),
            ok = meck:expect(erline_dht_helper, notify, fun
                (MatchEventMgrPid, {ping, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h1">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {ping, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h2">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {ping, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h3">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {ping, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h4">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {ping, r, {12,34,92,185}, 6882, <<"n3w_n0d3_h45h5">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {ping, r, {12,34,92,186}, 6883, <<"n3w_n0d3_h45h6">>}) when MatchEventMgrPid =:= EventMgrPid -> ok
            end),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun
                ({12,34,92,155}, 6862) -> [];
                ({12,34,92,185}, 6882) -> [#node{ip_port = {{12,34,92,185}, 6882}}];
                ({12,34,92,186}, 6883) -> [#node{ip_port = {{12,34,92,186}, 6883}}]
            end),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'], true),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, [{12,34,92,185}, 6882], true),
            ok = meck:expect(erline_dht_message, send_find_node, fun
                ({12,34,92,185}, 6882, sock, <<0,0>>, <<"h45h">>, <<"h45h">>) -> ok;
                ({12,34,92,186}, 6883, sock, <<0,0>>, <<"h45h">>, <<"h45h">>) -> ok
            end)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Handle ping response. Bucket is found. Distance stay the same.",
            fun() ->
                ?assertEqual(
                    #state{
                        event_mgr_pid = EventMgrPid,
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        k             = 3,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{
                                        hash         = <<"n3w_n0d3_h45h2">>,
                                        ip_port      = {{12,34,92,155}, 6862},
                                        last_changed = {{2020,7,1},{12,0,0}},
                                        active_txs   = [{ping,<<0,2>>}],
                                        status       = active,
                                        distance     = 1
                                    }
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = []
                            },
                            #bucket{
                                distance = 3,
                                nodes    = [
                                    #node{
                                        ip_port  = {{12,34,92,156}, 6863},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,157}, 6864},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,158}, 6865},
                                        status   = active
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_ping_response({12,34,92,155}, 6862, <<"n3w_n0d3_h45h2">>, [{ping, <<0,2>>}], Bucket, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {ping, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h2">>}])
                )
            end
        }]
    }.


%%
%%
%%
handle_find_node_query_test_() ->
    EventMgrPid = erlang:list_to_pid("<0.0.100>"),
    State = #state{
        event_mgr_pid = EventMgrPid,
        db_mod        = erline_dht_db_ets,
        socket        = sock,
        my_node_hash  = <<"h45h">>,
        k             = 3,
        buckets       = [
            #bucket{
                distance = 1,
                nodes    = [
                    #node{ip_port = {{12,34,92,156}, 6863}, hash = <<"h45h1">>},
                    #node{ip_port = {{12,34,92,157}, 6864}, hash = <<"h45h2">>},
                    #node{ip_port = {{12,34,92,158}, 6865}, hash = <<"h45h3">>},
                    #node{ip_port = {{12,34,92,159}, 6866}, hash = <<"h45h4">>}
                ]
            },
            #bucket{
                distance = 2,
                nodes    = [
                    #node{ip_port = {{12,34,92,160}, 6868}, hash = <<"h45h5_false">>},
                    #node{ip_port = {{12,34,92,161}, 6868}, hash = <<"h45h6">>},
                    #node{ip_port = {{12,34,92,162}, 6869}, hash = <<"h45h7">>}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_message, respond_find_node, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, '_'], ok),
            ok = meck:expect(erline_dht_helper, notify, [EventMgrPid, {find_node, q, {12,34,92,155}, 6862, {<<"n0d3_h45h">>, <<"t4rg3t">>}}], ok),
            ok = meck:expect(erline_dht_helper, get_distance, fun
                (<<"t4rg3t">>, <<"h45h1">>)       -> {ok, 3};
                (<<"t4rg3t">>, <<"h45h2">>)       -> {ok, 4};
                (<<"t4rg3t">>, <<"h45h3">>)       -> {ok, 3};
                (<<"t4rg3t">>, <<"h45h4">>)       -> {ok, 2};
                (<<"t4rg3t">>, <<"h45h5_false">>) -> {error, {different_hash_length, <<"h45h0">>, <<"h45h5_false">>}};
                (<<"t4rg3t">>, <<"h45h6">>)       -> {ok, 7};
                (<<"t4rg3t">>, <<"h45h7">>)       -> {ok, 5}
            end),
            ok = meck:expect(erline_dht_helper, encode_compact_node_info, ['_'], <<"c0mp4ct_n0d35_1nf0">>),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862], [#node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>}]),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, [#node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>, last_changed = {{2020,7,1},{12,0,0}}}], true)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Handle find_node query. Querying node is in the not assigned nodes list.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:handle_find_node_query({12,34,92,155}, 6862, <<"n0d3_h45h">>, <<"t4rg3t">>, <<0,2>>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, respond_find_node, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {find_node, q, {12,34,92,155}, 6862, {<<"n0d3_h45h">>, <<"t4rg3t">>}}])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, encode_compact_node_info, ['_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [#node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>, last_changed = {{2020,7,1},{12,0,0}}}])
                )
            end
        }]
    }.


%%
%%
%%
handle_find_node_response_test_() ->
    Nodes = [
        #{ip => {12,34,92,156}, port => 6863, hash => <<"h45h1">>},
        #{ip => {12,34,92,157}, port => 6864, hash => <<"h45h2">>},
        #{ip => {12,34,92,158}, port => 6865, hash => <<"h45h3">>}
    ],
    EventMgrPid = erlang:list_to_pid("<0.0.100>"),
    State = #state{
        db_mod          = erline_dht_db_ets,
        event_mgr_pid   = EventMgrPid,
        socket          = sock,
        my_node_hash    = <<"h45h">>,
        buckets = [
            Bucket = #bucket{
                distance = 2,
                nodes    = [
                    #node{
                        hash       = <<"n0d3_h45h">>,
                        ip_port    = {{12,34,92,155}, 6862},
                        tx_id      = <<0,2>>,
                        active_txs = [{find_node, <<0,1>>}]
                    },
                    #node{ip_port = {{12,34,92,158}, 6865}}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>], ok),
            ok = meck:expect(erline_dht_helper, notify, [EventMgrPid, {find_node, r, {12,34,92,155}, 6862, Nodes}], ok),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_helper, get_distance, [<<"h45h">>, <<"n0d3_h45h">>], {ok, 2}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862], [])
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Handle find_node response.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod          = erline_dht_db_ets,
                        event_mgr_pid   = EventMgrPid,
                        socket          = sock,
                        my_node_hash    = <<"h45h">>,
                        buckets = [
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        hash            = <<"n0d3_h45h">>,
                                        ip_port         = {{12,34,92,155}, 6862},
                                        tx_id           = <<0,2>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}},
                                        status          = active,
                                        distance        = 2
                                    },
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_find_node_response({12,34,92,155}, 6862, <<"n0d3_h45h">>, Nodes, [{ping, <<0,3>>}], Bucket, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {find_node, r, {12,34,92,155}, 6862, Nodes}])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n0d3_h45h">>])
                )
            end
        }]
    }.


%%
%%
%%
handle_get_peers_query_test_() ->
    EventMgrPid = erlang:list_to_pid("<0.0.100>"),
    State = #state{
        event_mgr_pid = EventMgrPid,
        db_mod        = erline_dht_db_ets,
        socket        = sock,
        my_node_hash  = <<"h45h">>,
        k             = 3,
        buckets       = [
            #bucket{
                distance = 1,
                nodes    = [
                    #node{ip_port = {{12,34,92,156}, 6863}, hash = <<"h45h1">>},
                    #node{ip_port = {{12,34,92,157}, 6864}, hash = <<"h45h2">>},
                    #node{ip_port = {{12,34,92,158}, 6865}, hash = <<"h45h3">>},
                    #node{ip_port = {{12,34,92,159}, 6866}, hash = <<"h45h4">>}
                ]
            },
            #bucket{
                distance = 2,
                nodes    = [
                    #node{ip_port = {{12,34,92,160}, 6868}, hash = <<"h45h5_false">>},
                    #node{ip_port = {{12,34,92,161}, 6868}, hash = <<"h45h6">>},
                    #node{ip_port = {{12,34,92,162}, 6869}, hash = <<"h45h7">>}
                ]
            }
        ],
        info_hashes = [
            #info_hash{
                info_hash = <<"1nf0_h45h">>,
                peers     = [
                    {{12,34,92,163}, 6870},
                    {{12,34,92,164}, 6871},
                    {{12,34,92,165}, 6872}
                ]
            }
        ],
        valid_tokens = [<<"t0k3n">>]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_message, respond_get_peers, fun
                ({12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"t0k3n">>, <<"c0mp4ct_n0d35_1nf0">>) -> ok;
                ({12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"t0k3n">>, <<"c0mp4ct_p33r5">>) -> ok
            end),
            ok = meck:expect(erline_dht_helper, notify, fun
                (MatchEventMgrPid, {get_peers, q, {12,34,92,155}, 6862, {<<"n0d3_h45h">>, <<"1nf0_h45h00">>}}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {get_peers, q, {12,34,92,155}, 6862, {<<"n0d3_h45h">>, <<"1nf0_h45h">>}}) when MatchEventMgrPid =:= EventMgrPid -> ok
            end),
            ok = meck:expect(erline_dht_helper, get_distance, fun
                (<<"1nf0_h45h00">>, <<"h45h1">>)       -> {ok, 3};
                (<<"1nf0_h45h00">>, <<"h45h2">>)       -> {ok, 4};
                (<<"1nf0_h45h00">>, <<"h45h3">>)       -> {ok, 3};
                (<<"1nf0_h45h00">>, <<"h45h4">>)       -> {ok, 2};
                (<<"1nf0_h45h00">>, <<"h45h5_false">>) -> {error, {different_hash_length, <<"h45h0">>, <<"h45h5_false">>}};
                (<<"1nf0_h45h00">>, <<"h45h6">>)       -> {ok, 7};
                (<<"1nf0_h45h00">>, <<"h45h7">>)       -> {ok, 5};
                (<<"1nf0_h45h">>,   <<"h45h1">>)       -> {ok, 3};
                (<<"1nf0_h45h">>,   <<"h45h2">>)       -> {ok, 4};
                (<<"1nf0_h45h">>,   <<"h45h3">>)       -> {ok, 3};
                (<<"1nf0_h45h">>,   <<"h45h4">>)       -> {ok, 2};
                (<<"1nf0_h45h">>,   <<"h45h5_false">>) -> {error, {different_hash_length, <<"h45h0">>, <<"h45h5_false">>}};
                (<<"1nf0_h45h">>,   <<"h45h6">>)       -> {ok, 7};
                (<<"1nf0_h45h">>,   <<"h45h7">>)       -> {ok, 5}
            end),
            ok = meck:expect(erline_dht_helper, encode_compact_node_info, ['_'], <<"c0mp4ct_n0d35_1nf0">>),
            ok = meck:expect(erline_dht_helper, encode_peer_info, ['_'], <<"c0mp4ct_p33r5">>),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862], [#node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>}]),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, [#node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>, token_sent = <<"t0k3n">>, last_changed = {{2020,7,1},{12,0,0}}}], true)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Handle get_peers query. Info hash is not found.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:handle_get_peers_query({12,34,92,155}, 6862, <<"n0d3_h45h">>, <<"1nf0_h45h00">>, <<0,2>>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, respond_get_peers, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"t0k3n">>, <<"c0mp4ct_n0d35_1nf0">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {get_peers, q, {12,34,92,155}, 6862, {<<"n0d3_h45h">>, <<"1nf0_h45h00">>}}])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, encode_compact_node_info, ['_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [#node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>, token_sent = <<"t0k3n">>, last_changed = {{2020,7,1},{12,0,0}}}])
                ),
                ok = meck:reset([erline_dht_helper, erline_dht_db_ets])
            end
        },
        {"Handle get_peers query. Info hash is found.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:handle_get_peers_query({12,34,92,155}, 6862, <<"n0d3_h45h">>, <<"1nf0_h45h">>, <<0,2>>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, respond_get_peers, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"t0k3n">>, <<"c0mp4ct_p33r5">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {get_peers, q, {12,34,92,155}, 6862, {<<"n0d3_h45h">>, <<"1nf0_h45h">>}}])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, encode_peer_info, ['_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,155}, 6862])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [#node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>, token_sent = <<"t0k3n">>, last_changed = {{2020,7,1},{12,0,0}}}])
                )
            end
        }]
    }.


%%
%%
%%
handle_get_peers_response_test_() ->
    Nodes = [
        #{ip => {12,34,92,156}, port => 6863, hash => <<"h45h1">>},
        #{ip => {12,34,92,157}, port => 6864, hash => <<"h45h2">>},
        #{ip => {12,34,92,158}, port => 6865, hash => <<"h45h3">>}
    ],
    Peers = [
        #{ip => {12,34,92,156}, port => 6863},
        #{ip => {12,34,92,157}, port => 6864},
        #{ip => {12,34,92,158}, port => 6865}
    ],
    EventMgrPid = erlang:list_to_pid("<0.0.100>"),
    State = #state{
        db_mod          = erline_dht_db_ets,
        event_mgr_pid   = EventMgrPid,
        socket          = sock,
        my_node_hash    = <<"h45h">>,
        buckets = [
            #bucket{
                distance = 2,
                nodes    = [
                    #node{
                        ip_port    = {{12,34,92,153}, 6860},
                        tx_id      = <<0,2>>,
                        active_txs = [{find_node, <<0,1>>}]
                    },
                    #node{
                        ip_port    = {{12,34,92,154}, 6861},
                        tx_id      = <<0,4>>,
                        active_txs = [{find_node, <<0,3>>}]
                    }
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>], ok),
            ok = meck:expect(erline_dht_helper, notify, fun
                (MatchEventMgrPid, {get_peers, r, {12,34,92,154}, 6861, {nodes, <<"1nf0_h45h">>, MatchNodes}}) when
                    MatchEventMgrPid =:= EventMgrPid,
                    MatchNodes =:= Nodes
                    -> ok;
                (MatchEventMgrPid, {get_peers, r, {12,34,92,154}, 6861, {peers, <<"1nf0_h45h">>, MatchPeers}}) when
                    MatchEventMgrPid =:= EventMgrPid,
                    MatchPeers =:= Peers
                    -> ok
            end),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_helper, get_distance, [<<"h45h">>, <<"n0d3_h45h">>], {ok, 2}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun
                ({12,34,92,153}, 6860) -> [];
                ({12,34,92,154}, 6861) -> []
            end),
            ok = meck:expect(erline_dht_db_ets, get_info_hash, fun
                ({12,34,92,153}, 6860, <<0,5>>) -> false;
                ({12,34,92,154}, 6861, <<0,5>>) -> <<"1nf0_h45h">>
            end),
            ok = meck:expect(erline_dht_db_ets, get_requested_node, fun
                ({12,34,92,156}, 6863, <<"1nf0_h45h">>) -> [];
                ({12,34,92,157}, 6864, <<"1nf0_h45h">>) -> [#requested_node{ip_port = {{12,34,92,157}, 6864}}];
                ({12,34,92,158}, 6865, <<"1nf0_h45h">>) -> []
            end),
            ok = meck:expect(erline_dht_db_ets, insert_to_get_peers_searches, [#get_peers_search{info_hash = <<"1nf0_h45h">>, last_changed = {{2020,7,1},{12,0,0}}}], true)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Handle get_peers response. Info hash not found.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod          = erline_dht_db_ets,
                        event_mgr_pid   = EventMgrPid,
                        socket          = sock,
                        my_node_hash    = <<"h45h">>,
                        buckets = [
                            Bucket = #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        hash            = <<"n0d3_h45h">>,
                                        ip_port         = {{12,34,92,153}, 6860},
                                        tx_id           = <<0,2>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}},
                                        token_received  = <<"t0k3n">>,
                                        status          = active,
                                        distance        = 2
                                    },
                                    #node{
                                        ip_port    = {{12,34,92,154}, 6861},
                                        tx_id      = <<0,4>>,
                                        active_txs = [{find_node, <<0,3>>}]
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_get_peers_response({12,34,92,153}, 6860, {nodes, <<"n0d3_h45h">>, <<0,5>>, Nodes, <<"t0k3n">>}, [{ping, <<0,3>>}], Bucket, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n0d3_h45h">>])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Handle get_peers response. Info hash is found. Got nodes.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod          = erline_dht_db_ets,
                        event_mgr_pid   = EventMgrPid,
                        socket          = sock,
                        my_node_hash    = <<"h45h">>,
                        buckets = [
                            Bucket = #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,153}, 6860},
                                        tx_id      = <<0,2>>,
                                        active_txs = [{find_node, <<0,1>>}]
                                    },
                                    #node{
                                        hash            = <<"n0d3_h45h">>,
                                        ip_port         = {{12,34,92,154}, 6861},
                                        tx_id           = <<0,4>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}},
                                        token_received  = <<"t0k3n">>,
                                        status          = active,
                                        distance        = 2
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_get_peers_response({12,34,92,154}, 6861, {nodes, <<"n0d3_h45h">>, <<0,5>>, Nodes, <<"t0k3n">>}, [{ping, <<0,3>>}], Bucket, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_get_peers_searches, [#get_peers_search{info_hash = <<"1nf0_h45h">>, last_changed = {{2020,7,1},{12,0,0}}}])
                ),
                ?assertEqual(
                    3,
                    meck:num_calls(erline_dht_db_ets, get_requested_node, ['_', '_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {get_peers, r, {12,34,92,154}, 6861, {nodes, <<"1nf0_h45h">>, Nodes}}])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n0d3_h45h">>])
                ),
                ok = meck:reset([erline_dht_db_ets, erline_dht_helper])
            end
        },
        {"Handle get_peers response. Info hash is found. Got peers.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod          = erline_dht_db_ets,
                        event_mgr_pid   = EventMgrPid,
                        socket          = sock,
                        my_node_hash    = <<"h45h">>,
                        buckets = [
                            Bucket = #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,153}, 6860},
                                        tx_id      = <<0,2>>,
                                        active_txs = [{find_node, <<0,1>>}]
                                    },
                                    #node{
                                        hash            = <<"n0d3_h45h">>,
                                        ip_port         = {{12,34,92,154}, 6861},
                                        tx_id           = <<0,4>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}},
                                        token_received  = <<"t0k3n">>,
                                        status          = active,
                                        distance        = 2
                                    }
                                ]
                            }
                        ],
                        info_hashes = [
                            #info_hash{
                                info_hash = <<"1nf0_h45h">>,
                                peers = [
                                    {{12,34,92,158}, 6865},
                                    {{12,34,92,157}, 6864},
                                    {{12,34,92,156}, 6863}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_get_peers_response({12,34,92,154}, 6861, {peers, <<"n0d3_h45h">>, <<0,5>>, Peers, <<"t0k3n">>}, [{ping, <<0,3>>}], Bucket, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_get_peers_searches, [#get_peers_search{info_hash = <<"1nf0_h45h">>, last_changed = {{2020,7,1},{12,0,0}}}])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_db_ets, get_requested_node, ['_', '_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {get_peers, r, {12,34,92,154}, 6861, {peers, <<"1nf0_h45h">>, Peers}}])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n0d3_h45h">>])
                )
            end
        }]
    }.


%%
%%
%%
handle_announce_peer_response_test_() ->
    EventMgrPid = erlang:list_to_pid("<0.0.100>"),
    State = #state{
        event_mgr_pid = EventMgrPid,
        db_mod        = erline_dht_db_ets,
        socket        = sock,
        my_node_hash  = <<"h45h">>,
        k             = 3,
        buckets       = [
            Bucket = #bucket{
                distance = 1,
                nodes    = [
                    #node{
                        hash     = <<"my_n0d3_h45h">>,
                        ip_port  = {{12,34,92,155}, 6862},
                        distance = 1
                    }
                ]
            },
            #bucket{
                distance = 2,
                nodes    = []
            },
            #bucket{ % No free space in this bucket
                distance = 3,
                nodes    = [
                    #node{
                        ip_port  = {{12,34,92,156}, 6863},
                        status   = active
                    },
                    #node{
                        ip_port  = {{12,34,92,157}, 6864},
                        status   = active
                    },
                    #node{
                        ip_port  = {{12,34,92,158}, 6865},
                        status   = active
                    }
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_helper, get_distance, fun
                (<<"h45h">>, <<"n3w_n0d3_h45h1">>) -> {error, {malformed_hashes, <<"h45h">>, <<"n3w_n0d3_h45h1">>}};
                (<<"h45h">>, <<"n3w_n0d3_h45h2">>) -> {ok, 1};
                (<<"h45h">>, <<"n3w_n0d3_h45h3">>) -> {ok, 2};
                (<<"h45h">>, <<"n3w_n0d3_h45h4">>) -> {ok, 3}; % No free space in this bucket
                (<<"h45h">>, <<"n3w_n0d3_h45h5">>) -> {ok, 1};
                (<<"h45h">>, <<"n3w_n0d3_h45h6">>) -> {ok, 3}  % No free space in this bucket
            end),
            ok = meck:expect(erline_dht_helper, notify, fun
                (MatchEventMgrPid, {announce_peer, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h1">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {announce_peer, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h2">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {announce_peer, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h3">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {announce_peer, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h4">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {announce_peer, r, {12,34,92,185}, 6882, <<"n3w_n0d3_h45h5">>}) when MatchEventMgrPid =:= EventMgrPid -> ok;
                (MatchEventMgrPid, {announce_peer, r, {12,34,92,186}, 6883, <<"n3w_n0d3_h45h6">>}) when MatchEventMgrPid =:= EventMgrPid -> ok
            end),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun
                ({12,34,92,155}, 6862) -> [];
                ({12,34,92,185}, 6882) -> [#node{ip_port = {{12,34,92,185}, 6882}}];
                ({12,34,92,186}, 6883) -> [#node{ip_port = {{12,34,92,186}, 6883}}]
            end),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'], true),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, [{12,34,92,185}, 6882], true),
            ok = meck:expect(erline_dht_message, send_find_node, fun
                ({12,34,92,185}, 6882, sock, <<0,0>>, <<"h45h">>, <<"h45h">>) -> ok;
                ({12,34,92,186}, 6883, sock, <<0,0>>, <<"h45h">>, <<"h45h">>) -> ok
            end)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Handle announce_peer response. Bucket is found. Distance stay the same.",
            fun() ->
                ?assertEqual(
                    #state{
                        event_mgr_pid = EventMgrPid,
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        k             = 3,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{
                                        hash         = <<"n3w_n0d3_h45h2">>,
                                        ip_port      = {{12,34,92,155}, 6862},
                                        last_changed = {{2020,7,1},{12,0,0}},
                                        active_txs   = [{announce_peer,<<0,2>>}],
                                        status       = active,
                                        distance     = 1
                                    }
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = []
                            },
                            #bucket{
                                distance = 3,
                                nodes    = [
                                    #node{
                                        ip_port  = {{12,34,92,156}, 6863},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,157}, 6864},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,158}, 6865},
                                        status   = active
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_announce_peer_response({12,34,92,155}, 6862, <<"n3w_n0d3_h45h2">>, [{announce_peer, <<0,2>>}], Bucket, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {announce_peer, r, {12,34,92,155}, 6862, <<"n3w_n0d3_h45h2">>}])
                )
            end
        }]
    }.


%%
%%
%%
do_ping_async_test_() ->
    State = #state{
        my_node_hash = <<"h45h">>,
        socket       = sock,
        buckets      = [
            #bucket{
                distance = 1,
                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
            },
            Bucket = #bucket{
                distance = 2,
                nodes    = [
                    Node = #node{
                        ip_port    = {{12,34,92,155}, 6862},
                        tx_id      = <<0,2>>,
                        active_txs = [{find_node, <<0,1>>}]
                    },
                    #node{ip_port = {{12,34,92,158}, 6865}}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new(erline_dht_message),
            ok = meck:expect(erline_dht_message, send_ping, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>], ok)
        end,
        fun(_) ->
            true = meck:validate(erline_dht_message),
            ok = meck:unload(erline_dht_message)
        end,
        [{"Send ping request.",
            fun() ->
                ?assertEqual(
                    {ok, #state{
                        my_node_hash = <<"h45h">>,
                        socket       = sock,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,155}, 6862},
                                        tx_id      = <<0,3>>,
                                        active_txs = [{ping, <<0,2>>}, {find_node, <<0,1>>}]
                                    },
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    }},
                    erline_dht_bucket:do_ping_async(Bucket, Node, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_ping, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>])
                )
            end
        }]
    }.


%%
%%
%%
do_find_node_async_test_() ->
    State = #state{
        my_node_hash = <<"h45h">>,
        socket       = sock,
        buckets      = [
            #bucket{
                distance = 1,
                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
            },
            Bucket = #bucket{
                distance = 2,
                nodes    = [
                    Node = #node{
                        ip_port    = {{12,34,92,155}, 6862},
                        tx_id      = <<0,2>>,
                        active_txs = [{find_node, <<0,1>>}]
                    },
                    #node{ip_port = {{12,34,92,158}, 6865}}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new(erline_dht_message),
            ok = meck:expect(erline_dht_message, send_find_node, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"t4rg3t">>], ok)
        end,
        fun(_) ->
            true = meck:validate(erline_dht_message),
            ok = meck:unload(erline_dht_message)
        end,
        [{"Send find_node request.",
            fun() ->
                ?assertEqual(
                    {ok, #state{
                        my_node_hash = <<"h45h">>,
                        socket       = sock,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,155}, 6862},
                                        tx_id      = <<0,3>>,
                                        active_txs = [{find_node, <<0,2>>}, {find_node, <<0,1>>}]
                                    },
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    }},
                    erline_dht_bucket:do_find_node_async(Bucket, Node, <<"t4rg3t">>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_find_node, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"t4rg3t">>])
                )
            end
        }]
    }.


%%
%%
%%
do_get_peers_async_test_() ->
    State = #state{
        my_node_hash = <<"h45h">>,
        socket       = sock,
        buckets      = [
            #bucket{
                distance = 1,
                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
            },
            Bucket = #bucket{
                distance = 2,
                nodes    = [
                    Node = #node{
                        ip_port    = {{12,34,92,155}, 6862},
                        tx_id      = <<0,2>>,
                        active_txs = [{find_node, <<0,1>>}]
                    },
                    #node{ip_port = {{12,34,92,158}, 6865}}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new(erline_dht_message),
            ok = meck:expect(erline_dht_message, send_get_peers, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"1nf0h45h">>], ok)
        end,
        fun(_) ->
            true = meck:validate(erline_dht_message),
            ok = meck:unload(erline_dht_message)
        end,
        [{"Send get_peers request.",
            fun() ->
                ?assertEqual(
                    {ok, <<0,2>>, #state{
                        my_node_hash = <<"h45h">>,
                        socket       = sock,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,155}, 6862},
                                        tx_id      = <<0,3>>,
                                        active_txs = [{get_peers, <<0,2>>}, {find_node, <<0,1>>}]
                                    },
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    }},
                    erline_dht_bucket:do_get_peers_async(Bucket, Node, <<"1nf0h45h">>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_get_peers, [{12,34,92,155}, 6862, sock, <<0,2>>, <<"h45h">>, <<"1nf0h45h">>])
                )
            end
        }]
    }.


%%
%%
%%
do_announce_peer_async_test_() ->
    {ok, Socket} = gen_udp:open(0),
    State = #state{
        my_node_hash = <<"h45h">>,
        socket       = Socket,
        buckets      = [
            #bucket{
                distance = 1,
                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
            },
            Bucket = #bucket{
                distance = 2,
                nodes    = [
                    Node = #node{
                        ip_port    = {{12,34,92,155}, 6862},
                        tx_id      = <<0,2>>,
                        active_txs = [{find_node, <<0,1>>}]
                    },
                    #node{ip_port = {{12,34,92,158}, 6865}}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new(erline_dht_message),
            ok = meck:expect(erline_dht_message, send_announce_peer, [{12,34,92,155}, 6862, Socket, <<0,2>>, <<"h45h">>, <<"1nf0h45h">>, '_', '_', <<"t0k3n">>], ok)
        end,
        fun(_) ->
            true = meck:validate(erline_dht_message),
            ok = meck:unload(erline_dht_message)
        end,
        [{"Send announce_peer request. Implied port is 1.",
            fun() ->
                ?assertEqual(
                    {ok, #state{
                        my_node_hash = <<"h45h">>,
                        socket       = Socket,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,155}, 6862},
                                        tx_id      = <<0,3>>,
                                        active_txs = [{announce_peer, <<0,2>>}, {find_node, <<0,1>>}]
                                    },
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    }},
                    erline_dht_bucket:do_announce_peer_async(Bucket, Node, <<"1nf0h45h">>, <<"t0k3n">>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_announce_peer, [{12,34,92,155}, 6862, Socket, <<0,2>>, <<"h45h">>, <<"1nf0h45h">>, 1, '_', <<"t0k3n">>])
                ),
                ok = meck:reset(erline_dht_message)
            end
        },
        {"Send announce_peer request. Implied port is 0.",
            fun() ->
                ?assertEqual(
                    {ok, #state{
                        my_node_hash = <<"h45h">>,
                        socket       = Socket,
                        peer_port    = 6881,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,155}, 6862},
                                        tx_id      = <<0,3>>,
                                        active_txs = [{announce_peer, <<0,2>>}, {find_node, <<0,1>>}]
                                    },
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    }},
                    erline_dht_bucket:do_announce_peer_async(Bucket, Node, <<"1nf0h45h">>, <<"t0k3n">>, State#state{peer_port = 6881})
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_announce_peer, [{12,34,92,155}, 6862, Socket, <<0,2>>, <<"h45h">>, <<"1nf0h45h">>, 0, 6881, <<"t0k3n">>])
                )
            end
        }]
    }.


%%
%%
%%
handle_response_generic_test_() ->
    State = #state{
        db_mod        = erline_dht_db_ets,
        socket        = sock,
        my_node_hash  = <<"h45h">>,
        k             = 3,
        buckets       = [
            Bucket = #bucket{
                distance = 1,
                nodes    = [
                    #node{
                        hash     = <<"my_n0d3_h45h">>,
                        ip_port  = {{12,34,92,155}, 6862},
                        distance = 1
                    }
                ]
            },
            #bucket{
                distance = 2,
                nodes    = []
            },
            #bucket{ % No free space in this bucket
                distance = 3,
                nodes    = [
                    #node{
                        ip_port  = {{12,34,92,156}, 6863},
                        status   = active
                    },
                    #node{
                        ip_port  = {{12,34,92,157}, 6864},
                        status   = active
                    },
                    #node{
                        ip_port  = {{12,34,92,158}, 6865},
                        status   = active
                    }
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_helper, get_distance, fun
                (<<"h45h">>, <<"n3w_n0d3_h45h1">>) -> {error, {malformed_hashes, <<"h45h">>, <<"n3w_n0d3_h45h1">>}};
                (<<"h45h">>, <<"n3w_n0d3_h45h2">>) -> {ok, 1};
                (<<"h45h">>, <<"n3w_n0d3_h45h3">>) -> {ok, 2};
                (<<"h45h">>, <<"n3w_n0d3_h45h4">>) -> {ok, 3}; % No free space in this bucket
                (<<"h45h">>, <<"n3w_n0d3_h45h5">>) -> {ok, 1};
                (<<"h45h">>, <<"n3w_n0d3_h45h6">>) -> {ok, 3}  % No free space in this bucket
            end),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun
                ({12,34,92,155}, 6862) -> [];
                ({12,34,92,185}, 6882) -> [#node{ip_port = {{12,34,92,185}, 6882}}];
                ({12,34,92,186}, 6883) -> [#node{ip_port = {{12,34,92,186}, 6883}}]
            end),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'], true),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, [{12,34,92,185}, 6882], true),
            ok = meck:expect(erline_dht_message, send_find_node, fun
                ({12,34,92,185}, 6882, sock, <<0,0>>, <<"h45h">>, <<"h45h">>) -> ok;
                ({12,34,92,186}, 6883, sock, <<0,0>>, <<"h45h">>, <<"h45h">>) -> ok
            end)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_message, erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Generic handler. Handle response. New hash is malformed.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        k             = 3,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{
                                        hash         = <<"n3w_n0d3_h45h1">>,
                                        ip_port      = {{12,34,92,155}, 6862},
                                        last_changed = {{2020,7,1},{12,0,0}},
                                        active_txs   = [{ping,<<0,2>>}],
                                        status       = not_active,
                                        distance     = 1
                                    }
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = []
                            },
                            #bucket{
                                distance = 3,
                                nodes    = [
                                    #node{
                                        ip_port  = {{12,34,92,156}, 6863},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,157}, 6864},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,158}, 6865},
                                        status   = active
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_response_generic({12,34,92,155}, 6862, <<"n3w_n0d3_h45h1">>, [{ping, <<0,2>>}], Bucket, true, State)
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n3w_n0d3_h45h1">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Generic handler. Handle response. Bucket is found. Distance stay the same. Do find_nodes.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        k             = 3,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{
                                        hash         = <<"n3w_n0d3_h45h2">>,
                                        ip_port      = {{12,34,92,155}, 6862},
                                        last_changed = {{2020,7,1},{12,0,0}},
                                        active_txs   = [{ping,<<0,2>>}],
                                        status       = active,
                                        distance     = 1
                                    }
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = []
                            },
                            #bucket{
                                distance = 3,
                                nodes    = [
                                    #node{
                                        ip_port  = {{12,34,92,156}, 6863},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,157}, 6864},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,158}, 6865},
                                        status   = active
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_response_generic({12,34,92,155}, 6862, <<"n3w_n0d3_h45h2">>, [{ping, <<0,2>>}], Bucket, true, State)
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n3w_n0d3_h45h2">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Generic handler. Handle response. Bucket is found. Distance is different from the old one. Bucket has some free space. Do find_nodes.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        k             = 3,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = []
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        hash         = <<"n3w_n0d3_h45h3">>,
                                        ip_port      = {{12,34,92,155}, 6862},
                                        last_changed = {{2020,7,1},{12,0,0}},
                                        active_txs   = [{ping,<<0,2>>}],
                                        status       = active,
                                        distance     = 2
                                    }
                                ]
                            },
                            #bucket{
                                distance = 3,
                                nodes    = [
                                    #node{
                                        ip_port  = {{12,34,92,156}, 6863},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,157}, 6864},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,158}, 6865},
                                        status   = active
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_response_generic({12,34,92,155}, 6862, <<"n3w_n0d3_h45h3">>, [{ping, <<0,2>>}], Bucket, true, State)
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n3w_n0d3_h45h3">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Generic handler. Handle response. Bucket is found. Distance is different from the old one. Bucket has no free space. Do find_nodes.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        k             = 3,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = []
                            },
                            #bucket{
                                distance = 2,
                                nodes    = []
                            },
                            #bucket{
                                distance = 3,
                                nodes    = [
                                    #node{
                                        ip_port  = {{12,34,92,156}, 6863},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,157}, 6864},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,158}, 6865},
                                        status   = active
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_response_generic({12,34,92,155}, 6862, <<"n3w_n0d3_h45h4">>, [{ping, <<0,2>>}], Bucket, true, State)
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n3w_n0d3_h45h4">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'])
                ),
                ok = meck:reset([erline_dht_helper, erline_dht_db_ets])
            end
        },
        {"Generic handler. Handle response. Bucket is not found. Bucket has some free space. Do not need to do find_nodes.",
            fun() ->
                ?assertEqual(
                    #state{
                        db_mod        = erline_dht_db_ets,
                        socket        = sock,
                        my_node_hash  = <<"h45h">>,
                        k             = 3,
                        buckets       = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{
                                        hash         = <<"n3w_n0d3_h45h5">>,
                                        ip_port      = {{12,34,92,185}, 6882},
                                        last_changed = {{2020,7,1},{12,0,0}},
                                        status       = active,
                                        distance     = 1
                                    },
                                    #node{
                                        hash     = <<"my_n0d3_h45h">>,
                                        ip_port  = {{12,34,92,155}, 6862},
                                        distance = 1
                                    }
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = []
                            },
                            #bucket{
                                distance = 3,
                                nodes    = [
                                    #node{
                                        ip_port  = {{12,34,92,156}, 6863},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,157}, 6864},
                                        status   = active
                                    },
                                    #node{
                                        ip_port  = {{12,34,92,158}, 6865},
                                        status   = active
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_response_generic({12,34,92,185}, 6882, <<"n3w_n0d3_h45h5">>, [{ping, <<0,2>>}], false, false, State)
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n3w_n0d3_h45h5">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, [{12,34,92,185}, 6882])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_message, send_find_node, [{12,34,92,185}, 6882, sock, <<0,0>>, <<"h45h">>, <<"h45h">>])
                ),
                ok = meck:reset([erline_dht_helper, erline_dht_db_ets, erline_dht_message])
            end
        },
        {"Generic handler. Handle response. Bucket is not found. Bucket has no free space. Do not need to do find_nodes.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:handle_response_generic({12,34,92,186}, 6883, <<"n3w_n0d3_h45h6">>, [{ping, <<0,2>>}], false, false, State)
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h">>, <<"n3w_n0d3_h45h6">>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, [{12,34,92,186}, 6883])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_message, send_find_node, [{12,34,92,186}, 6883, sock, <<0,0>>, <<"h45h">>, <<"h45h">>])
                )
            end
        }]
    }.


%%
%%
%%
get_bucket_and_node_test_() ->
    State = #state{
        db_mod  = erline_dht_db_ets,
        buckets = [
            #bucket{
                distance = 1,
                nodes    = [#node{ip_port = {{12,34,92,154}, 6861}}]
            },
            FoundBucket = #bucket{
                distance = 2,
                nodes    = [
                    #node{ip_port = {{12,34,92,155}, 6862}},
                    #node{ip_port = {{12,34,92,158}, 6865}}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new(erline_dht_db_ets),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun
                ({12,34,92,156}, 6863) -> [];
                ({12,34,92,157}, 6864) -> [#node{ip_port = {{12,34,92,157}, 6864}}];
                ({12,34,92,158}, 6865) -> []
            end)
        end,
        fun(_) ->
            true = meck:validate(erline_dht_db_ets),
            ok = meck:unload(erline_dht_db_ets)
        end,
        [{"Node not found.",
            fun() ->
                ?assertEqual(
                    false,
                    erline_dht_bucket:get_bucket_and_node({12,34,92,156}, 6863, State)
                )
            end
        },
        {"Node is found in not assigned nodes list.",
            fun() ->
                ?assertEqual(
                    {ok, false, #node{ip_port = {{12,34,92,157}, 6864}}},
                    erline_dht_bucket:get_bucket_and_node({12,34,92,157}, 6864, State)
                )
            end
        },
        {"Node is found in a bucket.",
            fun() ->
                ?assertEqual(
                    {ok, FoundBucket, #node{ip_port = {{12,34,92,158}, 6865}}},
                    erline_dht_bucket:get_bucket_and_node({12,34,92,158}, 6865, State)
                )
            end
        }]
    }.


%%
%%
%%
update_tx_id_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"First transaction ID.",
            fun() ->
                ?assertEqual(
                    #node{tx_id = <<0,1>>},
                    erline_dht_bucket:update_tx_id(#node{})
                )
            end
        },
        {"Last transaction ID.",
            fun() ->
                ?assertEqual(
                    #node{tx_id = <<0,0>>},
                    erline_dht_bucket:update_tx_id(#node{tx_id = <<255,255>>})
                )
            end
        },
        {"Usual transaction ID.",
            fun() ->
                ?assertEqual(
                    #node{tx_id = <<12,141>>},
                    erline_dht_bucket:update_tx_id(#node{tx_id = <<12,140>>})
                )
            end
        }]
    }.


%%
%%
%%
update_node_test_() ->
    Params = [
        {hash, <<"n3w_h45h">>},
        {token_received, <<"t0k3n_r3c31v3d">>},
        {token_sent, <<"t0k3n_s3nt">>},
        {last_changed, {{2020,7,1},{12,0,0}}},
        {active_txs, [{find_node, <<0,2>>}, {ping, <<0,3>>}]},
        tx_id,
        {status, active}
    ],
    State = #state{
        my_node_hash = <<"my_h45h">>,
        db_mod       = erline_dht_db_ets,
        buckets      = [
            Bucket = #bucket{
                distance = 1,
                nodes    = [
                    Node = #node{
                        ip_port     = {{12,34,92,154}, 6861},
                        hash        = <<"0ld_h45h">>,
                        active_txs  = [{ping, <<0,1>>}],
                        tx_id       = <<0,3>>,
                        distance    = 1
                    },
                    #node{ip_port = {{12,34,92,155}, 6862}}
                ]
            },
            #bucket{
                distance = 2,
                nodes    = [
                    #node{ip_port = {{12,34,92,156}, 6863}},
                    #node{ip_port = {{12,34,92,158}, 6865}}
                ]
            }
        ]
    },
    UpdatedNode = #node{
        ip_port         = {{12,34,92,154}, 6861},
        token_received  = <<"t0k3n_r3c31v3d">>,
        token_sent      = <<"t0k3n_s3nt">>,
        hash            = <<"n3w_h45h">>,
        last_changed    = {{2020,7,1},{12,0,0}},
        active_txs      = [{find_node, <<0,2>>}, {ping, <<0,3>>}],
        tx_id           = <<0,4>>,
        status          = active,
        distance        = 2
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_helper, erline_dht_db_ets]),
            ok = meck:expect(erline_dht_helper, get_distance, [<<"my_h45h">>, <<"n3w_h45h">>], {ok, 2}),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, [UpdatedNode], true),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, [{12,34,92,154}, 6861], true)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_helper, erline_dht_db_ets]),
            ok = meck:unload([erline_dht_helper, erline_dht_db_ets])
        end,
        [{"Update node in a bucket.",
            fun() ->
                ?assertEqual(
                    #state{
                        my_node_hash = <<"my_h45h">>,
                        db_mod       = erline_dht_db_ets,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    UpdatedNode,
                                    #node{ip_port = {{12,34,92,155}, 6862}}
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{ip_port = {{12,34,92,156}, 6863}},
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:update_node(Bucket, Node, Params, State)
                )
            end
        },
        {"Update node in a not assigned nodes list.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:update_node(false, Node, Params, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [UpdatedNode])
                ),
                ok = meck:reset(erline_dht_db_ets)
            end
        },
        {"Update node in a bucket and assign to another bucket.",
            fun() ->
                ?assertEqual(
                    #state{
                        my_node_hash = <<"my_h45h">>,
                        db_mod       = erline_dht_db_ets,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{ip_port = {{12,34,92,155}, 6862}}
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    UpdatedNode,
                                    #node{ip_port = {{12,34,92,156}, 6863}},
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:update_node(Bucket, Node, Params ++ [{assign, 2}], State)
                )
            end
        },
        {"Update node in a not assigned nodes list and assign to a bucket.",
            fun() ->
                ?assertEqual(
                    #state{
                        my_node_hash = <<"my_h45h">>,
                        db_mod       = erline_dht_db_ets,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{
                                        ip_port     = {{12,34,92,154}, 6861},
                                        hash        = <<"0ld_h45h">>,
                                        active_txs  = [{ping, <<0,1>>}],
                                        tx_id       = <<0,3>>,
                                        distance    = 1
                                    },
                                    #node{ip_port = {{12,34,92,155}, 6862}}
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    UpdatedNode,
                                    #node{ip_port = {{12,34,92,156}, 6863}},
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:update_node(false, Node, Params ++ [{assign, 2}], State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, [{12,34,92,154}, 6861])
                ),
                ok = meck:reset(erline_dht_db_ets)
            end
        },
        {"Update node in a bucket and unassign it.",
            fun() ->
                ?assertEqual(
                    #state{
                        my_node_hash = <<"my_h45h">>,
                        db_mod       = erline_dht_db_ets,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{ip_port = {{12,34,92,155}, 6862}}
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{ip_port = {{12,34,92,156}, 6863}},
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:update_node(Bucket, Node, Params ++ [unassign], State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [UpdatedNode])
                ),
                ok = meck:reset(erline_dht_db_ets)
            end
        },
        {"Update node in a bucket and unassign it.",
            fun() ->
                ?assertEqual(
                    #state{
                        my_node_hash = <<"my_h45h">>,
                        db_mod       = erline_dht_db_ets,
                        buckets      = [
                            #bucket{
                                distance = 1,
                                nodes    = [
                                    #node{ip_port = {{12,34,92,155}, 6862}}
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{ip_port = {{12,34,92,156}, 6863}},
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:update_node(Bucket, Node, Params ++ [unassign], State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [UpdatedNode])
                ),
                ok = meck:reset(erline_dht_db_ets)
            end
        },
        {"Update node in a not assigned nodes list and try to unassign it (no success).",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:update_node(false, Node, Params ++ [unassign], State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [UpdatedNode])
                )
            end
        }]
    }.


%%
%%
%%
update_bucket_test_() ->
    State = #state{
        buckets = [
            #bucket{
                distance = 1,
                nodes    = [#node{ip_port = {{12,34,92,156}, 6863}}]
            },
            #bucket{
                distance = 2,
                nodes    = []
            }
        ]
    },
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Update bucket with all params.",
            fun() ->
                Params = [
                    check_timer,
                    ping_timer,
                    {nodes, [#node{ip_port = {{12,34,92,156}, 6863}}, #node{ip_port = {{12,34,92,157}, 6864}}]}
                ],
                NewState = #state{buckets = NewBuckets} = erline_dht_bucket:update_bucket(1, Params, State),
                {value, #bucket{check_timer = CheckTimer, ping_timer = PingTimer}} = lists:keysearch(1, #bucket.distance, NewBuckets),
                % Assertions
                ?assert(
                    erlang:is_reference(CheckTimer)
                ),
                ?assert(
                    erlang:is_reference(PingTimer)
                ),
                ?assertEqual(
                    #state{
                        buckets = [
                            #bucket{
                                check_timer = CheckTimer,
                                ping_timer  = PingTimer,
                                distance    = 1,
                                nodes       = [
                                    #node{ip_port = {{12,34,92,156}, 6863}},
                                    #node{ip_port = {{12,34,92,157}, 6864}}
                                ]
                            },
                            #bucket{
                                distance = 2,
                                nodes    = []
                            }
                        ]
                    },
                    NewState
                )
            end
        }]
    }.

%%
%%
%%
maybe_clear_bucket_test_() ->
    {setup,
        fun() ->
            ok = meck:new(erline_dht_db_ets),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun (_, _) -> [] end),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'], true)
        end,
        fun(_) ->
            true = meck:validate(erline_dht_db_ets),
            ok = meck:unload(erline_dht_db_ets)
        end,
        [{"Bucket is not full.",
            fun() ->
                State = #state{
                    k       = 5,
                    buckets = [
                        #bucket{
                            distance = 1,
                            nodes    = [#node{}, #node{}, #node{}, #node{}]
                        }
                    ]
                },
                % Assertions
                ?assertEqual(
                    {true, State},
                    erline_dht_bucket:maybe_clear_bucket(1, State)
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_node, ['_', '_'])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'])
                )
            end
        },
        {"Bucket is full. Not active node is not found.",
            fun() ->
                State = #state{
                    k       = 4,
                    buckets = [
                        #bucket{
                            distance = 1,
                            nodes    = [#node{status = active}, #node{}, #node{}, #node{}]
                        }
                    ]
                },
                % Assertions
                ?assertEqual(
                    {false, State},
                    erline_dht_bucket:maybe_clear_bucket(1, State)
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_node, ['_', '_'])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'])
                )
            end
        },
        {"Bucket is full. Not active node is moved to not assigned nodes list.",
            fun() ->
                State = #state{
                    k       = 4,
                    db_mod  = erline_dht_db_ets,
                    buckets = [
                        #bucket{
                            distance = 1,
                            nodes    = [
                                #node{status = active},
                                #node{},
                                NodeToRemove = #node{ip_port = {{12,34,92,156}, 6863}, status = not_active, last_changed = {{2020,7,1},{9,0,0}}},
                                #node{ip_port = {{12,34,92,157}, 6864}, status = not_active, last_changed = {{2020,7,1},{10,0,0}}}
                            ]
                        }
                    ]
                },
                NewState = #state{
                    k       = 4,
                    db_mod  = erline_dht_db_ets,
                    buckets = [
                        #bucket{
                            distance = 1,
                            nodes    = [
                                #node{status = active},
                                #node{},
                                #node{ip_port = {{12,34,92,157}, 6864}, status = not_active, last_changed = {{2020,7,1},{10,0,0}}}
                            ]
                        }
                    ]
                },
                % Assertions
                ?assertEqual(
                    {true, NewState},
                    erline_dht_bucket:maybe_clear_bucket(1, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_node, [{12,34,92,156}, 6863])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, [NodeToRemove])
                )
            end
        }]
    }.


%%
%%
%%
find_n_closest_nodes_test_() ->
    State = #state{
        buckets = [
            #bucket{
                distance = 1,
                nodes    = [
                    #node{ip_port = {{12,34,92,155}, 6862}, hash = <<"h45h_self">>},
                    #node{ip_port = {{12,34,92,156}, 6863}, hash = <<"h45h1">>},
                    #node{ip_port = {{12,34,92,157}, 6864}, hash = <<"h45h2">>},
                    #node{ip_port = {{12,34,92,158}, 6865}, hash = <<"h45h3">>},
                    #node{ip_port = {{12,34,92,159}, 6866}, hash = <<"h45h4">>}
                ]
            },
            #bucket{
                distance = 2,
                nodes    = [
                    #node{ip_port = {{12,34,92,160}, 6868}, hash = <<"h45h5_false">>},
                    #node{ip_port = {{12,34,92,161}, 6868}, hash = <<"h45h6">>},
                    #node{ip_port = {{12,34,92,162}, 6869}, hash = <<"h45h7">>}
                ]
            }
        ]
    },
    {setup,
        fun() ->
            ok = meck:new(erline_dht_helper),
            ok = meck:expect(erline_dht_helper, get_distance, fun
                (<<"h45h0">>, <<"h45h_self">>)   -> {ok, 1};
                (<<"h45h0">>, <<"h45h1">>)       -> {ok, 3};
                (<<"h45h0">>, <<"h45h2">>)       -> {ok, 4};
                (<<"h45h0">>, <<"h45h3">>)       -> {ok, 3};
                (<<"h45h0">>, <<"h45h4">>)       -> {ok, 2};
                (<<"h45h0">>, <<"h45h5_false">>) -> {error, {different_hash_length, <<"h45h0">>, <<"h45h5_false">>}};
                (<<"h45h0">>, <<"h45h6">>)       -> {ok, 7};
                (<<"h45h0">>, <<"h45h7">>)       -> {ok, 5}
            end)
        end,
        fun(_) ->
            true = meck:validate(erline_dht_helper),
            ok = meck:unload(erline_dht_helper)
        end,
        [{"Find N closest nodes.",
            fun() ->
                ?assertEqual(
                    [
                        {{{12,34,92,159}, 6866}, <<"h45h4">>},
                        {{{12,34,92,156}, 6863}, <<"h45h1">>},
                        {{{12,34,92,158}, 6865}, <<"h45h3">>},
                        {{{12,34,92,157}, 6864}, <<"h45h2">>}
                    ],
                    erline_dht_bucket:find_n_closest_nodes({12,34,92,155}, 6862, <<"h45h0">>, 4, State)
                ),
                ?assertEqual(
                    7,
                    meck:num_calls(erline_dht_helper, get_distance, [<<"h45h0">>, '_'])
                )
            end
        }]
    }.


%%
%%
%%
find_local_peers_by_info_hash_test_() ->
    State = #state{
        info_hashes = [
            #info_hash{
                info_hash = <<"h45h1">>,
                peers     = [{{12,34,92,156}, 6863}, {{12,34,92,157}, 6864}]
            }
        ]
    },
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Get peers from existing info hash peers list.",
            fun() ->
                ?assertEqual(
                    [
                        #{ip => {12,34,92,156}, port => 6863},
                        #{ip => {12,34,92,157}, port => 6864}
                    ],
                    erline_dht_bucket:find_local_peers_by_info_hash(<<"h45h1">>, State)
                )
            end
        },
        {"Info hash not found.",
            fun() ->
                ?assertEqual(
                    [],
                    erline_dht_bucket:find_local_peers_by_info_hash(<<"h45h2">>, State)
                )
            end
        }]
    }.


%%
%%
%%
add_peer_test_() ->
    StateWithInfoHash = #state{
        info_hashes = [
            #info_hash{
                info_hash = <<"h45h1">>,
                peers     = [{{12,34,92,156}, 6863}]
            }
        ]
    },
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Add new peer and new info hash.",
            fun() ->
                ?assertEqual(
                    StateWithInfoHash,
                    erline_dht_bucket:add_peer(<<"h45h1">>, {12,34,92,156}, 6863, #state{})
                )
            end
        },
        {"Add new peer to existing info hash.",
            fun() ->
                ?assertEqual(
                    #state{
                        info_hashes = [
                            #info_hash{
                                info_hash = <<"h45h1">>,
                                peers     = [{{12,34,92,157}, 6864}, {{12,34,92,156}, 6863}]
                            }
                        ]
                    },
                    erline_dht_bucket:add_peer(<<"h45h1">>, {12,34,92,157}, 6864, StateWithInfoHash)
                )
            end
        },
        {"Add existing peer to existing info hash.",
            fun() ->
                ?assertEqual(
                    StateWithInfoHash,
                    erline_dht_bucket:add_peer(<<"h45h1">>, {12,34,92,156}, 6863, StateWithInfoHash)
                )
            end
        }]
    }.


%%
%%
%%
insert_info_hash_test_() ->
    {ok, Socket} = gen_udp:open(0),
    State = #state{
        db_mod = erline_dht_db_ets,
        k      = 3,
        socket = Socket
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_helper, erline_dht_message]),
            ok = meck:expect(erline_dht_db_ets, get_requested_nodes, fun
                (<<"1nf0_h45h1">>) ->
                    [];
                (<<"1nf0_h45h2">>) ->
                    [
                        #requested_node{ip_port = {{12,34,92,156}, 6863}},
                        #requested_node{ip_port = {{12,34,92,157}, 6864}},
                        #requested_node{ip_port = {{12,34,92,158}, 6865}},
                        #requested_node{ip_port = {{12,34,92,159}, 6866}},
                        #requested_node{ip_port = {{12,34,92,160}, 6867}}
                    ]
            end),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun
                ({12,34,92,156}, 6863) -> [#node{ip_port = {{12,34,92,156}, 6863}, hash = <<"h45h1">>}];
                ({12,34,92,157}, 6864) -> [#node{ip_port = {{12,34,92,157}, 6864}, hash = <<"h45h2">>}];
                ({12,34,92,158}, 6865) -> [#node{ip_port = {{12,34,92,158}, 6865}, hash = <<"h45h3">>}];
                ({12,34,92,159}, 6866) -> [#node{ip_port = {{12,34,92,159}, 6866}, hash = <<"f4l53_h45h">>}];
                ({12,34,92,160}, 6867) -> []
            end),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'], true),
            ok = meck:expect(erline_dht_helper, get_distance, fun
                (<<"1nf0_h45h2">>, <<"f4l53_h45h">>) -> {error, {malformed_hashes, <<"1nf0_h45h2">>, <<"f4l53_h45h">>}};
                (<<"1nf0_h45h2">>, <<"h45h1">>)      -> {ok, 1};
                (<<"1nf0_h45h2">>, <<"h45h2">>)      -> {ok, 2};
                (<<"1nf0_h45h2">>, <<"h45h3">>)      -> {ok, 3}
            end),
            ok = meck:expect(erline_dht_message, send_announce_peer, ['_', '_', '_', '_', '_', '_', '_', '_', '_'], ok)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_db_ets, erline_dht_helper, erline_dht_message]),
            ok = meck:unload([erline_dht_db_ets, erline_dht_helper, erline_dht_message])
        end,
        [{"Requested nodes not found.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:insert_info_hash(<<"1nf0_h45h1">>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_requested_nodes, [<<"1nf0_h45h1">>])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_helper, get_distance, ['_', '_'])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_message, send_announce_peer, ['_', '_', '_', '_', '_', '_', '_', '_', '_'])
                ),
                ok = meck:reset(erline_dht_db_ets)
            end
        },
        {"Requested nodes found.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:insert_info_hash(<<"1nf0_h45h2">>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_requested_nodes, [<<"1nf0_h45h2">>])
                ),
                ?assertEqual(
                    4,
                    meck:num_calls(erline_dht_helper, get_distance, ['_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_announce_peer, [{12,34,92,156}, 6863, '_', '_', '_', '_', '_', '_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_announce_peer, [{12,34,92,157}, 6864, '_', '_', '_', '_', '_', '_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, send_announce_peer, [{12,34,92,158}, 6865, '_', '_', '_', '_', '_', '_', '_'])
                )
            end
        }]
    }.


%%
%%
%%
check_searches_test_() ->
    State = #state{
        db_mod = erline_dht_db_ets,
        k      = 3
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_helper]),
            ok = meck:expect(erline_dht_db_ets, get_all_get_peers_searches, [], [
                #get_peers_search{last_changed = {{2020,7,1},{10,0,0}}},
                #get_peers_search{last_changed = {{2020,7,1},{11,0,0}}},
                #get_peers_search{last_changed = {{2020,7,1},{12,0,0}}}
            ]),
            ok = meck:expect(erline_dht_db_ets, get_requested_nodes, ['_'], []),
            ok = meck:expect(erline_dht_db_ets, delete_get_peers_search, ['_'], true),
            ok = meck:expect(erline_dht_db_ets, delete_requested_nodes, ['_'], true),
            ok = meck:expect(erline_dht_helper, datetime_diff, fun
                (_, {{2020,7,1},{10,0,0}}) -> 500;
                (_, {{2020,7,1},{11,0,0}}) -> 550;
                (_, _) -> 10
            end),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}})
        end,
        fun(_) ->
            true = meck:validate([erline_dht_db_ets, erline_dht_helper]),
            ok = meck:unload([erline_dht_db_ets, erline_dht_helper])
        end,
        [{"After search is exhausted, insert infohash with contact information of itself and clear old peers searches.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:check_searches(State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_all_get_peers_searches, [])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_db_ets, delete_get_peers_search, ['_'])
                ),
                ?assertEqual(
                    2,
                    meck:num_calls(erline_dht_db_ets, delete_requested_nodes, ['_'])
                ),
                ?assertEqual(
                    3,
                    meck:num_calls(erline_dht_helper, local_time, [])
                )
            end
        }]
    }.


%%
%%
%%
update_tokens_test_() ->
    {setup,
        fun() ->
            ok = meck:new(erline_dht_helper),
            ok = meck:expect(erline_dht_helper, generate_random_binary, [20], meck:seq([<<"t0k3n1">>, <<"t0k3n2">>, <<"t0k3n3">>]))
        end,
        fun(_) ->
            true = meck:validate(erline_dht_helper),
            ok = meck:unload(erline_dht_helper)
        end,
        [{"Update tokens. 0 valid tokens.",
            fun() ->
                ?assertEqual(
                    #state{valid_tokens = [<<"t0k3n1">>]},
                    erline_dht_bucket:update_tokens(#state{})
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, generate_random_binary, [20])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Update tokens. 1 valid token.",
            fun() ->
                ?assertEqual(
                    #state{valid_tokens = [<<"t0k3n2">>, <<"t0k3n1">>]},
                    erline_dht_bucket:update_tokens(#state{valid_tokens = [<<"t0k3n1">>]})
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, generate_random_binary, [20])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Update tokens. 2 valid token.",
            fun() ->
                ?assertEqual(
                    #state{valid_tokens = [<<"t0k3n3">>, <<"t0k3n2">>]},
                    erline_dht_bucket:update_tokens(#state{valid_tokens = [<<"t0k3n2">>, <<"t0k3n1">>]})
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, generate_random_binary, [20])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        }]
    }.


%%
%%
%%
update_bucket_nodes_status_test_() ->
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_helper, erline_dht_message]),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun (_, _) -> [] end),
            ok = meck:expect(erline_dht_message, send_ping, ['_', '_', '_', '_', '_'], ok),
            ok = meck:expect(erline_dht_helper, datetime_diff, fun
                (_, {{2020,7,1},{12,0,0}}) -> 50;
                (_, {{2020,7,1},{11,0,0}}) -> 150;
                (_, {{2020,7,1},{14,0,0}}) -> 150;
                (_, {{2020,7,1},{17,0,0}}) -> 200;
                (_, {{2020,7,1},{18,0,0}}) -> 200;
                (_, _) -> 1000
            end),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}})
        end,
        fun(_) ->
            true = meck:validate([erline_dht_db_ets, erline_dht_helper, erline_dht_message]),
            ok = meck:unload([erline_dht_db_ets, erline_dht_helper, erline_dht_message])
        end,
        [{"Update nodes statuses.",
            fun() ->
                State = #state{
                    db_mod  = erline_dht_db_ets,
                    buckets = [
                        #bucket{
                            distance = 0,
                            nodes = ?NODES_LIST
                        },
                        #bucket{
                            distance = 1,
                            nodes = [
                                #node{ip_port = {{14,34,92,156}, 6863}, last_changed = {{2020,7,1},{12,0,0}}, status = active}
                            ]
                        }
                    ]
                },
                NewState = #state{
                    db_mod  = erline_dht_db_ets,
                    buckets = [
                        #bucket{
                            distance = 0,
                            nodes = [
                                #node{
                                    ip_port      = {{12,34,92,156}, 6863},
                                    last_changed = {{2020,7,1},{12,0,0}},
                                    status       = active
                                },
                                #node{
                                    ip_port      = {{12,34,92,157}, 6864},
                                    last_changed = {{2020,7,1},{11,0,0}},
                                    status       = active
                                },
                                #node{
                                    ip_port      = {{12,34,92,158}, 6865},
                                    last_changed = {{2020,7,1},{14,0,0}},
                                    status       = active
                                },
                                #node{ % Changed to not_active
                                    ip_port      = {{12,34,92,159}, 6866},
                                    last_changed = {{2020,7,1},{11,30,0}},
                                    status       = not_active
                                },
                                #node{ % Changed to suspicious, pinged one more time
                                    ip_port         = {{12,34,92,160}, 6867},
                                    last_changed    = {{2020,7,1},{14,30,20}},
                                    status          = suspicious,
                                    active_txs      = [{ping,<<0,0>>}],
                                    tx_id           = <<0,1>>
                                },
                                #node{ % Changed to suspicious, pinged one more time
                                    ip_port         = {{12,34,92,161}, 6868},
                                    last_changed    = {{2020,7,1},{15,0,0}},
                                    status          = suspicious,
                                    active_txs      = [{ping,<<0,0>>}],
                                    tx_id           = <<0,1>>
                                },
                                #node{ % Changed to not_active
                                    ip_port      = {{12,34,92,162}, 6869},
                                    last_changed = {{2020,7,1},{16,0,0}},
                                    status       = not_active
                                },
                                #node{
                                    ip_port      = {{12,34,92,163}, 6870},
                                    last_changed = {{2020,7,1},{17,0,0}},
                                    status       = suspicious
                                },
                                #node{
                                    ip_port      = {{12,34,92,164}, 6871},
                                    last_changed = {{2020,7,1},{18,0,0}},
                                    status       = not_active
                                },
                                #node{
                                    ip_port      = {{12,34,92,165}, 6872},
                                    last_changed = {{2020,7,1},{19,0,0}},
                                    status       = not_active
                                },
                                #node{ % Changed to suspicious, pinged one more time
                                    ip_port         = {{12,34,92,166}, 6873},
                                    last_changed    = {{2020,7,1},{20,0,0}},
                                    status          = suspicious,
                                    active_txs      = [{ping,<<0,0>>}],
                                    tx_id           = <<0,1>>
                                }
                            ]
                        },
                        #bucket{
                            distance = 1,
                            nodes = [
                                #node{ip_port = {{14,34,92,156}, 6863}, last_changed = {{2020,7,1},{12,0,0}}, status = active}
                            ]
                        }
                    ]
                },
                % Assertions
                ?assertEqual(
                    {NewState, true},
                    erline_dht_bucket:update_bucket_nodes_status(0, State)
                ),
                ?assertEqual(
                    11,
                    meck:num_calls(erline_dht_helper, datetime_diff, ['_', '_'])
                ),
                ?assertEqual(
                    3,
                    meck:num_calls(erline_dht_message, send_ping, ['_', '_', '_', '_', '_'])
                ),
                ?assertEqual(
                    11,
                    meck:num_calls(erline_dht_helper, local_time, [])
                )
            end
        }]
    }.


%%
%%
%%
init_not_active_nodes_replacement_test_() ->
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_message]),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_nodes, [0], ?NODES_LIST),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_node, fun (Ip, Port) -> [#node{ip_port = {Ip, Port}}] end),
            ok = meck:expect(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'], true),
            ok = meck:expect(erline_dht_message, send_ping, ['_', '_', '_', '_', '_'], ok)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_db_ets, erline_dht_message]),
            ok = meck:unload([erline_dht_db_ets, erline_dht_message])
        end,
        [{"Initiate not actives nodes replacement.",
            fun() ->
                State = #state{
                    k      = 1,
                    db_mod = erline_dht_db_ets
                },
                % Assertions
                ?assertEqual(
                    State,
                    erline_dht_bucket:init_not_active_nodes_replacement(0, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, get_not_assigned_nodes, ['_'])
                ),
                ?assertEqual(
                    10,
                    meck:num_calls(erline_dht_db_ets, insert_to_not_assigned_nodes, ['_'])
                ),
                ?assertEqual(
                    10,
                    meck:num_calls(erline_dht_message, send_ping, ['_', '_', '_', '_', '_'])
                )
            end
        }]
    }.


%%
%%
%%
clear_not_assigned_nodes_test_() ->
    State = #state{
        db_mod = erline_dht_db_ets
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_helper]),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_nodes, [0], ?NODES_LIST),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, ['_', '_'], true),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_dist_date, [undefined, {{2020,7,1},{12,0,0}}], ok),
            ok = meck:expect(erline_dht_helper, change_datetime, ['_', '_'], {{2020,7,1},{12,0,0}}),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}})
        end,
        fun(_) ->
            true = meck:validate([erline_dht_db_ets, erline_dht_helper]),
            ok = meck:unload([erline_dht_db_ets, erline_dht_helper])
        end,
        [{"Removable nodes are the same amount as threshold",
            fun() ->
                ?assertEqual(
                    ok,
                    erline_dht_bucket:clear_not_assigned_nodes(0, State#state{not_assigned_clearing_threshold = 3})
                ),
                ?assertEqual(
                    3,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, ['_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ok = meck:reset([erline_dht_db_ets, erline_dht_helper])
            end
        },
        {"Removable nodes are greater than threshold.",
            fun() ->
                ?assertEqual(
                    ok,
                    erline_dht_bucket:clear_not_assigned_nodes(0, State#state{not_assigned_clearing_threshold = 2})
                ),
                ?assertEqual(
                    3,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, ['_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ok = meck:reset([erline_dht_db_ets, erline_dht_helper])
            end
        },
        {"Removable nodes are lesser than threshold.",
            fun() ->
                ?assertEqual(
                    ok,
                    erline_dht_bucket:clear_not_assigned_nodes(0, State#state{not_assigned_clearing_threshold = 5})
                ),
                ?assertEqual(
                    3,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, ['_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ok = meck:reset([erline_dht_db_ets, erline_dht_helper])
            end
        },
        {"Threshold is not exceeded.",
            fun() ->
                ?assertEqual(
                    ok,
                    erline_dht_bucket:clear_not_assigned_nodes(0, State#state{not_assigned_clearing_threshold = 12})
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, ['_', '_'])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                ),
                ok = meck:reset([erline_dht_db_ets, erline_dht_helper])
            end
        },
        {"Clear nodes without distance only.",
            fun() ->
                ?assertEqual(
                    ok,
                    erline_dht_bucket:clear_not_assigned_nodes(State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_dist_date, [undefined, {{2020,7,1},{12,0,0}}])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
                )
            end
        }]
    }.

