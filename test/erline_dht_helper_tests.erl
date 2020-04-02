%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% erline_dht_helper_tests module unit tests.
%%% @end
%%% Created : 19. Aug 2019 22.32
%%%-------------------------------------------------------------------
-module(erline_dht_helper_tests).
-author("bartimaeus").
-include_lib("eunit/include/eunit.hrl").


get_distance_test_() ->
    {setup,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [{"Distance is 0.",
            fun() ->
                ?assertEqual(
                    {ok, 0},
                    erline_dht_helper:get_distance(
                        <<122,135,42,15,17,92,27,27,33,12,19,54,45,14,92,32,78,92,10,1>>,
                        <<122,135,42,15,17,92,27,27,33,12,19,54,45,14,92,32,78,92,10,1>>
                    )
                )
            end
        },
        {"Hashes can't be compared.",
            fun() ->
                ?assertEqual(
                    {error, {different_hash_length,
                         <<122,135,42,15,17,92,27,27,33,12,19,54,45,14,92,32, 78,92,10,1>>,
                         <<122,135,42,15,17,92,27,27,33,12,19,54,45,14,92,32,78>>}},
                    erline_dht_helper:get_distance(
                        <<122,135,42,15,17,92,27,27,33,12,19,54,45,14,92,32,78,92,10,1>>,
                        <<122,135,42,15,17,92,27,27,33,12,19,54,45,14,92,32,78>>
                    )
                )
            end
        },
        {"Distance is greater than 0.",
            fun() ->
                ?assertEqual(
                    {ok, 99},
                    erline_dht_helper:get_distance(
                        <<122,135,42,15,17,92,27,26,33,12,19,54,45,14,92,32,78,92,10,1>>,
                        <<122,135,42,15,17,92,27,28,33,12,19,54,45,14,92,32,78,92,10,1>>
                    )
                )
            end
        },
        {"Distance is greater than 0. First bit is different.",
            fun() ->
                ?assertEqual(
                    {ok, 160},
                    erline_dht_helper:get_distance(
                        <<127,135,42,15,17,92,27,26,33,12,19,54,45,14,92,32,78,92,10,1>>,
                        <<255,135,42,15,17,92,27,26,33,12,19,54,45,14,92,32,78,92,10,1>>
                    )
                )
            end
        },
        {"Distance is greater than 0. Last bit is different.",
            fun() ->
                ?assertEqual(
                    {ok, 1},
                    erline_dht_helper:get_distance(
                        <<122,135,42,15,17,92,27,26,33,12,19,54,45,14,92,32,78,92,10,255>>,
                        <<122,135,42,15,17,92,27,26,33,12,19,54,45,14,92,32,78,92,10,254>>
                    )
                )
            end
        }]
    }.


