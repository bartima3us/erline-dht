%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% erline_dht_message module unit tests.
%%% @end
%%% Created : 18. Aug 2019 16.11
%%%-------------------------------------------------------------------
-module(erline_dht_message_tests).
-author("bartimaeus").
-include_lib("eunit/include/eunit.hrl").


%%
%%
%%
do_ping_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Ping request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe">>,
                    erline_dht_message:ping_request(<<97,97>>, <<"abcdefghij0123456789">>)
                )
            end
        },
        {"Ping response.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re">>,
                    erline_dht_message:ping_response(<<97,97>>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        }]
    }.


%%
%%
%%
do_find_node_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Find node request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe">>,
                    erline_dht_message:find_node_request(<<97,97>>, <<"abcdefghij0123456789">>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        },
        {"Find node response.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:0123456789abcdefghij5:nodes9:def456...e1:t2:aa1:y1:re">>,
                    erline_dht_message:find_node_response(<<97,97>>, <<"0123456789abcdefghij">>, <<"def456...">>)
                )
            end
        }]
    }.


%%
%%
%%
do_get_peers_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Get peers request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe">>,
                    erline_dht_message:get_peers_request(<<97,97>>, <<"abcdefghij0123456789">>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        },
        {"Get peers response with list of peers.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re">>,
                    erline_dht_message:get_peers_response(<<97,97>>, <<"abcdefghij0123456789">>, <<"aoeusnth">>, [<<"axje.u">>, <<"idhtnm">>])
                )
            end
        },
        {"Get peers response with closest nodes.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:abcdefghij01234567895:nodes9:def456...5:token8:aoeusnthe1:t2:aa1:y1:re">>,
                    erline_dht_message:get_peers_response(<<97,97>>, <<"abcdefghij0123456789">>, <<"aoeusnth">>, <<"def456...">>)
                )
            end
        }]
    }.


%%
%%
%%
do_announce_peer_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Announce peer request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe">>,
                    erline_dht_message:announce_peer_request(<<97,97>>, <<"abcdefghij0123456789">>, 1, <<"mnopqrstuvwxyz123456">>, 6881, <<"aoeusnth">>)
                )
            end
        },
        {"Announce peer response.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re">>,
                    erline_dht_message:announce_peer_response(<<97,97>>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        }]
    }.


%%
%%
%%
do_error_response_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Error response.",
            fun() ->
                ?assertEqual(
                    <<"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee">>,
                    erline_dht_message:error_response(<<97,97>>, 201, <<"A Generic Error Ocurred">>)
                )
            end
        }]
    }.


%%
%%
%%
parse_response_dict_test_() ->
    {setup,
        fun() ->
            ok = meck:new(erline_dht_helper),
            ok = meck:expect(erline_dht_helper, decode_compact_node_info, [<<"n0d35_1nf0">>], [#{ip => {12,34,92,155}, port => 6862, hash => <<"n0d3_h45h">>}]),
            ok = meck:expect(erline_dht_helper, decode_peer_info, [[<<"p33r5_1nf0">>]], [#{ip => {12,34,92,155}, port => 6862}])
        end,
        fun(_) ->
            true = meck:validate(erline_dht_helper),
            ok = meck:unload(erline_dht_helper)
        end,
        [{"Parse ping in the response dict.",
            fun() ->
                ?assertEqual(
                    <<"h45h">>,
                    erline_dht_message:parse_response_dict(ping, <<0,1>>, dict:store(<<"id">>, <<"h45h">>, dict:new()))
                )
            end
        },
        {"Parse find_node in the response dict. Nodes are found.",
            fun() ->
                ?assertEqual(
                    [#{ip => {12,34,92,155}, port => 6862, hash => <<"n0d3_h45h">>}],
                    erline_dht_message:parse_response_dict(find_node, <<0,1>>, dict:store(<<"nodes">>, <<"n0d35_1nf0">>, dict:new()))
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, decode_compact_node_info, [<<"n0d35_1nf0">>])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Parse find_node in the response dict. Nodes are not found.",
            fun() ->
                ?assertEqual(
                    [],
                    erline_dht_message:parse_response_dict(find_node, <<0,1>>, dict:new())
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_helper, decode_compact_node_info, ['_'])
                )
            end
        },
        {"Parse get_peers in the response dict. Token is found. Peers are found.",
            fun() ->
                Dict =  dict:store(
                    <<"token">>,
                    <<"t0k3n">>,
                    dict:store(<<"values">>, {list, [<<"p33r5_1nf0">>]}, dict:new())
                ),
                ?assertEqual(
                    {peers, <<0,1>>, [#{ip => {12,34,92,155}, port => 6862}], <<"t0k3n">>},
                    erline_dht_message:parse_response_dict(get_peers, <<0,1>>, Dict)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, decode_peer_info, [[<<"p33r5_1nf0">>]])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Parse get_peers in the response dict. Token is not found. Peers are found.",
            fun() ->
                ?assertEqual(
                    {peers, <<0,1>>, [#{ip => {12,34,92,155}, port => 6862}], <<>>},
                    erline_dht_message:parse_response_dict(get_peers, <<0,1>>, dict:store(<<"values">>, {list, [<<"p33r5_1nf0">>]}, dict:new()))
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, decode_peer_info, [[<<"p33r5_1nf0">>]])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Parse get_peers in the response dict. Token is found. Nodes are found.",
            fun() ->
                Dict = dict:store(
                    <<"token">>,
                    <<"t0k3n">>,
                    dict:store(<<"nodes">>, <<"n0d35_1nf0">>, dict:new())
                ),
                ?assertEqual(
                    {nodes, <<0,1>>, [#{ip => {12,34,92,155}, port => 6862, hash => <<"n0d3_h45h">>}], <<"t0k3n">>},
                    erline_dht_message:parse_response_dict(get_peers, <<0,1>>, Dict)
                ),
                ?assertEqual(
                    1,
                    meck:num_calls(erline_dht_helper, decode_compact_node_info, [<<"n0d35_1nf0">>])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Parse get_peers in the response dict. Token is not found. Peers are not found. Nodes are not found.",
            fun() ->
                ?assertEqual(
                    {nodes, <<0,1>>, [], <<>>},
                    erline_dht_message:parse_response_dict(get_peers, <<0,1>>, dict:new())
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_helper, decode_compact_node_info, ['_'])
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(erline_dht_helper, decode_peer_info, ['_'])
                ),
                ok = meck:reset(erline_dht_helper)
            end
        },
        {"Parse announce_peer in the response dict.",
            fun() ->
                ?assertEqual(
                    <<"h45h">>,
                    erline_dht_message:parse_response_dict(announce_peer, <<0,1>>, dict:store(<<"id">>, <<"h45h">>, dict:new()))
                )
            end
        }]
    }.


%%
%%
%%
parse_krpc_arguments_test_() ->
    MakeRequestFun = fun (Query, ArgsList) ->
        Args = lists:foldl(fun ({Key, Val}, AccDict) ->
            dict:store(Key, Val, AccDict)
        end, dict:new(), ArgsList),
        QueryResult = dict:store(
            <<"a">>,
            {dict, Args},
            dict:store(<<"q">>, Query, dict:new())
        ),
        {QueryResult, Args}
    end,
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Parse KRPC ping. Success.",
            fun() ->
                {Result, _} = MakeRequestFun(<<"ping">>, [{<<"id">>, <<"h45h">>}]),
                ?assertEqual(
                    {ok, ping, q, <<"h45h">>, <<0,1>>},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC ping. Bad arguments.",
            fun() ->
                {Result, Args} = MakeRequestFun(<<"ping">>, []),
                ?assertEqual(
                    {error, {bad_args, Args, <<0,1>>}},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC find_node. Success.",
            fun() ->
                {Result, _} = MakeRequestFun(<<"find_node">>, [{<<"id">>, <<"h45h">>}, {<<"target">>, <<"t4rg3t">>}]),
                ?assertEqual(
                    {ok, find_node, q, {<<"h45h">>, <<"t4rg3t">>}, <<0,1>>},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC find_node. Bad arguments.",
            fun() ->
                {Result, Args} = MakeRequestFun(<<"find_node">>, [{<<"id">>, <<"h45h">>}]),
                ?assertEqual(
                    {error, {bad_args, Args, <<0,1>>}},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC get_peers. Success.",
            fun() ->
                {Result, _} = MakeRequestFun(<<"get_peers">>, [{<<"id">>, <<"h45h">>}, {<<"info_hash">>, <<"1nf0_h45h">>}]),
                ?assertEqual(
                    {ok, get_peers, q, {<<"h45h">>, <<"1nf0_h45h">>}, <<0,1>>},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC get_peers. Bad arguments.",
            fun() ->
                {Result, Args} = MakeRequestFun(<<"get_peers">>, [{<<"id">>, <<"h45h">>}]),
                ?assertEqual(
                    {error, {bad_args, Args, <<0,1>>}},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC announce_peer. Implied port is not found. Success.",
            fun() ->
                Params = [
                    {<<"id">>,        <<"h45h">>},
                    {<<"info_hash">>, <<"1nf0_h45h">>},
                    {<<"port">>,      5632},
                    {<<"token">>,     <<"t0k3n">>}
                ],
                {Result, _} = MakeRequestFun(<<"announce_peer">>, Params),
                ?assertEqual(
                    {ok, announce_peer, q, {<<"h45h">>, 0, <<"1nf0_h45h">>, 5632, <<"t0k3n">>}, <<0,1>>},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC announce_peer. Implied port is found. Success.",
            fun() ->
                Params = [
                    {<<"id">>,           <<"h45h">>},
                    {<<"info_hash">>,    <<"1nf0_h45h">>},
                    {<<"port">>,         5632},
                    {<<"token">>,        <<"t0k3n">>},
                    {<<"implied_port">>, 1}
                ],
                {Result, _} = MakeRequestFun(<<"announce_peer">>, Params),
                ?assertEqual(
                    {ok, announce_peer, q, {<<"h45h">>, 1, <<"1nf0_h45h">>, 5632, <<"t0k3n">>}, <<0,1>>},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"Parse KRPC announce_peer. Bad arguments.",
            fun() ->
                {Result, Args} = MakeRequestFun(<<"announce_peer">>, [{<<"id">>, <<"h45h">>}]),
                ?assertEqual(
                    {error, {bad_args, Args, <<0,1>>}},
                    erline_dht_message:parse_krpc_arguments(Result, <<0,1>>)
                )
            end
        },
        {"KRPC request parsing is unsucessful.",
            fun() ->
                ?assertEqual(
                    {error, {bad_query, dict:store(<<"q">>, <<"ping">>, dict:new()), <<0,1>>}},
                    erline_dht_message:parse_krpc_arguments(dict:store(<<"q">>, <<"ping">>, dict:new()), <<0,1>>)
                )
            end
        }]
    }.


%%
%%
%%
parse_krpc_response_test_() ->
    ActiveTxs = [{find_node, <<0,1>>}, {ping, <<"aa">>}],
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Parse KRPC response. Received query.",
            fun() ->
                ?assertEqual(
                    {ok, ping, q, <<"abcdefghij0123456789">>, <<"aa">>},
                    erline_dht_message:parse_krpc_response(<<"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe">>, ActiveTxs)
                )
            end
        },
        {"Parse KRPC response. Received normal response.",
            fun() ->
                ?assertEqual(
                    {ok, ping, r, <<"mnopqrstuvwxyz123456">>, [{find_node,<<0,1>>}]},
                    erline_dht_message:parse_krpc_response(<<"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re">>, ActiveTxs)
                )
            end
        },
        {"Parse KRPC response. Received error response.",
            fun() ->
                ?assertEqual(
                    {error, {krpc_error, 201, <<"A Generic Error Ocurred">>}, [{find_node,<<0,1>>}]},
                    erline_dht_message:parse_krpc_response(<<"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee">>, ActiveTxs)
                )
            end
        },
        {"Parse KRPC response. Received response with bad type.",
            fun() ->
                ?assertEqual(
                    {error, {bad_type,<<"k">>}, [{find_node,<<0,1>>}]},
                    erline_dht_message:parse_krpc_response(<<"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:ke">>, ActiveTxs)
                )
            end
        },
        {"Parse KRPC response. Received normal response with non existing tx.",
            fun() ->
                ?assertEqual(
                    {error, {non_existing_tx, <<"ab">>}},
                    erline_dht_message:parse_krpc_response(<<"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:ab1:y1:re">>, ActiveTxs)
                )
            end
        },
        {"Parse KRPC response. Bad response.",
            fun() ->
                ?assertEqual(
                    {error, {bad_response, dict:store(<<"y">>, <<"ping">>, dict:new())}},
                    erline_dht_message:parse_krpc_response(<<"d1:y4:pinge">>, ActiveTxs)
                )
            end
        },
        {"Parse KRPC response. Malformed bencoded string.",
            fun() ->
                ?assertEqual(
                    {error, {bad_response, <<"b4d_r35p0n53">>}},
                    erline_dht_message:parse_krpc_response(<<"b4d_r35p0n53">>, ActiveTxs)
                )
            end
        }]
    }.


