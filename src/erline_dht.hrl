%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 18. Aug 2019 23.54
%%%-------------------------------------------------------------------
-author("bartimaeus").

-type status()                      :: suspicious | active | not_active.
-type request()                     :: ping | find_node | get_peers | announce.
-type tx_id()                       :: binary().
-type active_tx()                   :: {request(), tx_id()}.
-type distance()                    :: non_neg_integer(). % Normally: 0..160
-type parsed_compact_node_info()    :: #{ip => inet:ip_address(), port => inet:port_number(), hash => binary()}.
-type parsed_peer_info()            :: #{ip => inet:ip_address(), port => inet:port_number()}.

-record(node, {
    ip_port                         :: {inet:ip_address(), inet:port_number()},
    hash                            :: binary(),
    token_sent                      :: binary(), % My token sent to this node
    token_received                  :: binary(), % Token received from this node
    last_changed                    :: calendar:datetime(),
    tx_id           = <<0,0>>       :: tx_id(),
    active_txs      = []            :: [{request(), tx_id()}],
    status          = suspicious    :: status(),
    distance                        :: distance() % Denormalized field. Mapping: #node.distance = #bucket.distance.
}).

-record(get_peers_search, {
    info_hash       :: binary(),
    last_changed    :: calendar:datetime()
}).

-record(requested_node, {
    ip_port     :: {inet:ip_address(), inet:port_number()},
    tx_id       :: tx_id(),
    info_hash   :: binary()
}).





