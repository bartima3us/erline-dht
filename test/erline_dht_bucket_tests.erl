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
    ip_port         :: {inet:ip_address(), inet:port_number()},
    transaction_id  :: tx_id(),
    info_hash       :: binary()
}).

-record(node, {
    ip_port                             :: {inet:ip_address(), inet:port_number()},
    hash                                :: binary(),
    token_sent                          :: binary(),
    token_received                      :: binary(),
    last_changed                        :: calendar:datetime(),
    transaction_id      = <<0,0>>       :: tx_id(),
    active_transactions = []            :: [{request(), tx_id()}],
    status              = suspicious    :: status(),
    distance                            :: distance()
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
update_transaction_id_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"First transaction ID.",
            fun() ->
                ?assertEqual(
                    #node{transaction_id = <<0,1>>},
                    erline_dht_bucket:update_transaction_id(#node{})
                )
            end
        },
        {"Last transaction ID.",
            fun() ->
                ?assertEqual(
                    #node{transaction_id = <<0,0>>},
                    erline_dht_bucket:update_transaction_id(#node{transaction_id = <<255,255>>})
                )
            end
        },
        {"Usual transaction ID.",
            fun() ->
                ?assertEqual(
                    #node{transaction_id = <<12,141>>},
                    erline_dht_bucket:update_transaction_id(#node{transaction_id = <<12,140>>})
                )
            end
        }]
    }.


%%
%%
%%
find_local_peers_by_info_hash_test_() ->
    StateWithInfoHash = #state{
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
                    erline_dht_bucket:find_local_peers_by_info_hash(<<"h45h1">>, StateWithInfoHash)
                )
            end
        },
        {"Info hash not found.",
            fun() ->
                ?assertEqual(
                    [],
                    erline_dht_bucket:find_local_peers_by_info_hash(<<"h45h2">>, StateWithInfoHash)
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
            end)
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
                )
            end
        }]
    }.


%%
%%
%%
update_bucket_nodes_status_test_() ->
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
            end)
        end,
        fun(_) ->
            true = meck:validate([erline_dht_db_ets, erline_dht_helper, erline_dht_message]),
            ok = meck:unload([erline_dht_db_ets, erline_dht_helper, erline_dht_message])
        end,
        [{"Update nodes statuses.",
            fun() ->
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
                                    ip_port             = {{12,34,92,160}, 6867},
                                    last_changed        = {{2020,7,1},{14,30,20}},
                                    status              = suspicious,
                                    active_transactions = [{ping,<<0,0>>}],
                                    transaction_id      = <<0,1>>
                                },
                                #node{ % Changed to suspicious, pinged one more time
                                    ip_port             = {{12,34,92,161}, 6868},
                                    last_changed        = {{2020,7,1},{15,0,0}},
                                    status              = suspicious,
                                    active_transactions = [{ping,<<0,0>>}],
                                    transaction_id      = <<0,1>>
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
                                    ip_port             = {{12,34,92,166}, 6873},
                                    last_changed        = {{2020,7,1},{20,0,0}},
                                    status              = suspicious,
                                    active_transactions = [{ping,<<0,0>>}],
                                    transaction_id      = <<0,1>>
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
                )
            end
        }]
    }.


%%
%%
%%
init_not_active_nodes_replacement_test_() ->
    State = #state{k = 1, db_mod = erline_dht_db_ets},
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
    State = #state{db_mod = erline_dht_db_ets},
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_helper]),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_nodes, [0], ?NODES_LIST),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, ['_', '_'], true),
            ok = meck:expect(erline_dht_db_ets, delete_from_not_assigned_nodes_by_dist_date, [undefined, {{2020,7,1},{12,0,0}}], ok),
            ok = meck:expect(erline_dht_helper, change_datetime, ['_', '_'], {{2020,7,1},{12,0,0}})
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
                ok = meck:reset(erline_dht_db_ets)
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
                ok = meck:reset(erline_dht_db_ets)
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
                ok = meck:reset(erline_dht_db_ets)
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
                ok = meck:reset(erline_dht_db_ets)
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
                )
            end
        }]
    }.

