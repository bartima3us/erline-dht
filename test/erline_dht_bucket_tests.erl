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

-record(node, {
    ip                              :: inet:ip_address(),
    port                            :: inet:port_number(),
    hash                            :: binary(),
    last_changed                    :: calendar:datetime(),
    transaction_id      = <<0,0>>   :: binary(),
    active_transactions = []        :: [binary()]
}).

-record(state, {
    nodes           = []    :: [#node{}],
    last_changed            :: calendar:datetime()
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
                    #state{transaction_id = <<0,1>>},
                    erline_dht_bucket:update_transaction_id(#state{})
                )
            end
        },
        {"Last transaction ID.",
            fun() ->
                ?assertEqual(
                    #state{transaction_id = <<0,0>>},
                    erline_dht_bucket:update_transaction_id(#state{transaction_id = <<255,255>>})
                )
            end
        },
        {"Usual transaction ID.",
            fun() ->
                ?assertEqual(
                    #state{transaction_id = <<12,141>>},
                    erline_dht_bucket:update_transaction_id(#state{transaction_id = <<12,140>>})
                )
            end
        }]
    }.


