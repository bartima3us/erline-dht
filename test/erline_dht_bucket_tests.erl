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
    token_sent                      :: binary(),
    token_received                  :: binary(),
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
    my_node_hash                        :: binary(),
    socket                              :: port(),
    k                                   :: pos_integer(),
    buckets                     = []    :: [#bucket{}],
    info_hashes                 = []    :: [#info_hash{}],
    get_peers_searches_timer            :: reference(),
    clear_not_assigned_nodes_timer      :: reference(),
    db_mod                              :: module(),
    event_mgr_pid                       :: pid(),
    not_assigned_clearing_threshold     :: pos_integer()
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
        socket        = sock,
        my_node_hash  = <<"h45h">>
    },
    {setup,
        fun() ->
            ok = meck:new([erline_dht_message, erline_dht_helper]),
            ok = meck:expect(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<"h45h">>, <<0,2>>], ok),
            ok = meck:expect(erline_dht_helper, notify, [EventMgrPid, {ping, q, {12,34,92,155}, 6862, <<"n0d3_h45h">>}], ok)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_message, erline_dht_helper]),
            ok = meck:unload([erline_dht_message, erline_dht_helper])
        end,
        [{"Handle ping query.",
            fun() ->
                ?assertEqual(
                    State,
                    erline_dht_bucket:handle_ping_query({12,34,92,155}, 6862, <<"n0d3_h45h">>, <<0,2>>, State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<"h45h">>, <<0,2>>])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {ping, q, {12,34,92,155}, 6862, <<"n0d3_h45h">>}])
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
            #bucket{
                distance = 2,
                nodes    = [
                    #node{
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
            ok = meck:expect(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<"h45h">>, <<0,2>>], ok),
            ok = meck:expect(erline_dht_helper, notify, [EventMgrPid, {find_node, r, {12,34,92,155}, 6862, Nodes}], ok),
            ok = meck:expect(erline_dht_helper, local_time, [], {{2020,7,1},{12,0,0}}),
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
                                        ip_port         = {{12,34,92,155}, 6862},
                                        tx_id           = <<0,2>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}}
                                    },
                                    #node{ip_port = {{12,34,92,158}, 6865}}
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_find_node_response({12,34,92,155}, 6862, Nodes, [{ping, <<0,3>>}], State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, notify, [EventMgrPid, {find_node, r, {12,34,92,155}, 6862, Nodes}])
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
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
            ok = meck:expect(erline_dht_message, respond_ping, [{12,34,92,155}, 6862, sock, <<"h45h">>, <<0,2>>], ok),
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
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port         = {{12,34,92,153}, 6860},
                                        tx_id           = <<0,2>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}},
                                        token_received  = <<"t0k3n">>
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
                    erline_dht_bucket:handle_get_peers_response({12,34,92,153}, 6860, {nodes, <<0,5>>, Nodes, <<"t0k3n">>}, [{ping, <<0,3>>}], State)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, local_time, [])
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
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,153}, 6860},
                                        tx_id      = <<0,2>>,
                                        active_txs = [{find_node, <<0,1>>}]
                                    },
                                    #node{
                                        ip_port         = {{12,34,92,154}, 6861},
                                        tx_id           = <<0,4>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}},
                                        token_received  = <<"t0k3n">>
                                    }
                                ]
                            }
                        ]
                    },
                    erline_dht_bucket:handle_get_peers_response({12,34,92,154}, 6861, {nodes, <<0,5>>, Nodes, <<"t0k3n">>}, [{ping, <<0,3>>}], State)
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
                            #bucket{
                                distance = 2,
                                nodes    = [
                                    #node{
                                        ip_port    = {{12,34,92,153}, 6860},
                                        tx_id      = <<0,2>>,
                                        active_txs = [{find_node, <<0,1>>}]
                                    },
                                    #node{
                                        ip_port         = {{12,34,92,154}, 6861},
                                        tx_id           = <<0,4>>,
                                        active_txs      = [{ping, <<0,3>>}],
                                        last_changed    = {{2020,7,1},{12,0,0}},
                                        token_received  = <<"t0k3n">>
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
                    erline_dht_bucket:handle_get_peers_response({12,34,92,154}, 6861, {peers, <<0,5>>, Peers, <<"t0k3n">>}, [{ping, <<0,3>>}], State)
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
clear_peers_searches_test_() ->
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_helper]),
            ok = meck:expect(erline_dht_db_ets, get_all_get_peers_searches, [], [
                #get_peers_search{last_changed = {{2020,7,1},{10,0,0}}},
                #get_peers_search{last_changed = {{2020,7,1},{11,0,0}}},
                #get_peers_search{last_changed = {{2020,7,1},{12,0,0}}}
            ]),
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
        [{"Clear old peers searches.",
            fun() ->
                ?assertEqual(
                    ok,
                    erline_dht_bucket:clear_peers_searches(#state{db_mod = erline_dht_db_ets})
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

