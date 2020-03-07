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


do_ping_test_() ->
    {setup,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [{"Ping request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe">>,
                    erline_dht_message:do_ping(<<97,97>>, <<"abcdefghij0123456789">>)
                )
            end
        },
        {"Ping response.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re">>,
                    erline_dht_message:do_ping_response(<<97,97>>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        }]
    }.


do_find_node_test_() ->
    {setup,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [{"Find node request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe">>,
                    erline_dht_message:do_find_node(<<97,97>>, <<"abcdefghij0123456789">>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        },
        {"Find node response.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:0123456789abcdefghij5:nodes9:def456...e1:t2:aa1:y1:re">>,
                    erline_dht_message:do_find_node_response(<<97,97>>, <<"0123456789abcdefghij">>, <<"def456...">>)
                )
            end
        }]
    }.


do_get_peers_test_() ->
    {setup,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [{"Get peers request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe">>,
                    erline_dht_message:do_get_peers(<<97,97>>, <<"abcdefghij0123456789">>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        },
        {"Get peers response with list of peers.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re">>,
                    erline_dht_message:do_get_peers_response(<<97,97>>, <<"abcdefghij0123456789">>, <<"aoeusnth">>, [<<"axje.u">>, <<"idhtnm">>])
                )
            end
        },
        {"Get peers response with closest nodes.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:abcdefghij01234567895:nodes9:def456...5:token8:aoeusnthe1:t2:aa1:y1:re">>,
                    erline_dht_message:do_get_peers_response(<<97,97>>, <<"abcdefghij0123456789">>, <<"aoeusnth">>, <<"def456...">>)
                )
            end
        }]
    }.


do_announce_peer_test_() ->
    {setup,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [{"Announce peer request.",
            fun() ->
                ?assertEqual(
                    <<"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe">>,
                    erline_dht_message:do_announce_peer(<<97,97>>, <<"abcdefghij0123456789">>, 1, <<"mnopqrstuvwxyz123456">>, 6881, <<"aoeusnth">>)
                )
            end
        },
        {"Announce peer response.",
            fun() ->
                ?assertEqual(
                    <<"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re">>,
                    erline_dht_message:do_announce_peer_response(<<97,97>>, <<"mnopqrstuvwxyz123456">>)
                )
            end
        }]
    }.


do_error_response_test_() ->
    {setup,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [{"Error response.",
            fun() ->
                ?assertEqual(
                    <<"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee">>,
                    erline_dht_message:do_error_response(<<97,97>>, 201, <<"A Generic Error Ocurred">>)
                )
            end
        }]
    }.


