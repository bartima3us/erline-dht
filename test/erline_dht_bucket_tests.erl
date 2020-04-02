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


update_transaction_id_test_() ->
    {setup,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
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


