%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 18. Aug 2019 23.54
%%%-------------------------------------------------------------------
-author("bartimaeus").

% @todo parameterized
-define(MY_NODE_ID, <<169,246,141,183,17,96,15,191,158,252,221,69,218,231,8,97,231,8,214,41>>).
%%-define(MY_NODE_ID, <<0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>).
-define(K, 8).

-record(node, {
    ip          :: inet:ip4_address(),
    port        :: inet:port_number(),
    hash        :: binary(),
    torrents    :: [binary()]
}).

-record(k_bucket, {
    distance    :: pos_integer(),
    bucket = [] :: [#node{}]
}).


