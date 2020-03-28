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
    distance                            :: distance() % Denormalized field. Mapping: #node.distance = #bucket.distance.
}).




