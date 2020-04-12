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
        fun() -> ok end,
        fun(_) -> ok end,
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


decode_compact_node_info_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Parse node compact info.",
            fun() ->
                ?assertEqual(
                    [#{
                        hash   => <<169,246,141,183,17,96,15,191,158,252,221,69,218,231,8,97,231,8,214,43>>,
                        ip   => {12,34,92,156},
                        port => 1101
                    },
                    #{
                        hash   => <<169,246,141,183,17,96,15,191,158,252,221,69,218,231,8,97,231,8,214,41>>,
                        ip   => {65,12,45,225},
                        port => 8520
                    }],
                    erline_dht_helper:decode_compact_node_info(
                        <<169,246,141,183,17,96,15,191,158,252,221,69,218,231,8,97,231,8,214,41, 65,12,45,225,33,72,
                          169,246,141,183,17,96,15,191,158,252,221,69,218,231,8,97,231,8,214,43, 12,34,92,156,4,77>>
                    )
                )
            end
        }]
    }.


decode_peer_info_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Parse peer info.",
            fun() ->
                ?assertEqual(
                    [
                      #{ip => {12,34,92,156}, port => 1101},
                      #{ip => {11,54,87,145}, port => 8237},
                      #{ip => {65,12,45,225}, port => 8520}
                    ],
                    erline_dht_helper:decode_peer_info([
                        <<65,12,45,225, 33,72>>,
                        <<11,54,87,145, 32,45>>,
                        <<12,34,92,156, 4,77>>
                    ])
                )
            end
        }]
    }.


datetime_diff_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Get seconds diff between two datetimes.",
            fun() ->
                ?assertEqual(
                    3600,
                    erline_dht_helper:datetime_diff(
                        {{2020,7,1},{12,0,0}},
                        {{2020,7,1},{11,0,0}}
                    )
                )
            end
        }]
    }.


change_datetime_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [{"Get datetime changed by given seconds amount. Seconds are positive.",
            fun() ->
                ?assertEqual(
                    {{2020,7,1},{11,30,0}},
                    erline_dht_helper:change_datetime(
                        {{2020,7,1},{12,0,0}},
                        1800
                    )
                )
            end
        },
        {"Get datetime changed by given seconds amount. Seconds are negative.",
            fun() ->
                ?assertEqual(
                    {{2020,7,1},{12,30,0}},
                    erline_dht_helper:change_datetime(
                        {{2020,7,1},{12,0,0}},
                        -1800
                    )
                )
            end
        }]
    }.


