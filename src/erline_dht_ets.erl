%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 18. Aug 2019 23.53
%%%-------------------------------------------------------------------
-module(erline_dht_ets).
-author("bartimaeus").
-include("erline_dht.hrl").

-define(TABLE, 'erline_dht$k_buckets').

%% API
-export([
    new/0
]).

new() ->
    ets:new(?TABLE, [set, named_table]),
    KBuckets = lists:map(fun (KNum) ->
        #k_bucket{
            k = KNum
        }
    end, lists:seq(1, ?K)),
    true = ets:insert(?TABLE, KBuckets),
    ok.

