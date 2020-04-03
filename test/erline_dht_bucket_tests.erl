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
-type active_tx()   :: {request(), tx_id()}.
-type distance()    :: 0..160.

-record(node, {
    ip_port                             :: {inet:ip_address(), inet:port_number()},
    hash                                :: binary(),
    token                               :: binary(),
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
clear_not_assigned_nodes_test_() ->
    NodesList = [
        #node{ip_port = {{12,34,92,156}, 6863}, last_changed = {{2020,7,1},{12,0,0}}},
        #node{ip_port = {{12,34,92,157}, 6864}, last_changed = {{2020,7,1},{11,0,0}}},
        #node{ip_port = {{12,34,92,158}, 6865}, last_changed = {{2020,7,1},{14,0,0}}},
        #node{ip_port = {{12,34,92,159}, 6866}, last_changed = {{2020,7,1},{11,30,0}}},
        #node{ip_port = {{12,34,92,160}, 6867}, last_changed = {{2020,7,1},{14,30,20}}},
        #node{ip_port = {{12,34,92,161}, 6868}, last_changed = {{2020,7,1},{15,0,0}}}
    ],
    State = #state{db_mod = erline_dht_db_ets},
    {setup,
        fun() ->
            ok = meck:new([erline_dht_db_ets, erline_dht_helper]),
            ok = meck:expect(erline_dht_db_ets, get_not_assigned_nodes, [0], NodesList),
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
                    1,
                    meck:num_calls(erline_dht_db_ets, delete_from_not_assigned_nodes_by_ip_port, ['_', '_'])
                ),
                ok = meck:reset(erline_dht_db_ets)
            end
        },
        {"Threshold is not exceeded.",
            fun() ->
                ?assertEqual(
                    ok,
                    erline_dht_bucket:clear_not_assigned_nodes(0, State#state{not_assigned_clearing_threshold = 7})
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

