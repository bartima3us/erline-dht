%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 18. Aug 2019 23.54
%%%-------------------------------------------------------------------
-author("bartimaeus").

-define(K, 20). % @todo move to sys.config

-record(node, {
    ip          :: inet:ip4_address(),
    port        :: inet:port_number(),
    hash        :: binary(),
    torrents    :: [binary()]
}).

-record(k_bucket, {
    k           :: integer(),
    bucket = [] :: [#node{}]
}).


