%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% ETS backend implementation.
%%% @end
%%% Created : 28. Mar 2020 10.32
%%%-------------------------------------------------------------------
-module(erline_dht_db_ets).
-author("bartimaeus").
-include("erline_dht.hrl").

%% Common API
-export([
    init/0
]).

%% NOT_ASSIGNED_NODES_TABLE API
-export([
    get_not_assigned_nodes/0,
    get_not_assigned_nodes/1,
    get_not_assigned_node/2,
    insert_to_not_assigned_nodes/1,
    delete_from_not_assigned_nodes/2
]).

%% REQUESTED_NODES_TABLE API
-export([
    insert_to_requested_nodes/1,
    get_requested_node/3,
    get_info_hash/3,
    delete_requested_nodes/1
]).

%% GET_PEERS_SEARCHES_TABLE API
-export([
    insert_to_get_peers_searches/1,
    get_all_get_peers_searches/0,
    delete_get_peers_search/1
]).

-define(NOT_ASSIGNED_NODES_TABLE, 'erline_dht$not_assigned_nodes').
-define(REQUESTED_NODES_TABLE, 'erline_dht$requested_nodes').
-define(GET_PEERS_SEARCHES_TABLE, 'erline_dht$get_peers_searches').

%%%===================================================================
%%% API
%%%===================================================================


%%
%%
%%
-spec init() -> ok.

init() ->
    % 'compressed' - trade-off. Example with 200k nodes (not compressed vs compressed):
    % Read performance: 7 microseconds vs 13 microseconds;
    % RAM consumption: 618 MB vs 266 MB
    ?NOT_ASSIGNED_NODES_TABLE = ets:new(?NOT_ASSIGNED_NODES_TABLE, [set, named_table, compressed, {keypos, #node.ip_port}]),
    ?REQUESTED_NODES_TABLE    = ets:new(?REQUESTED_NODES_TABLE, [set, named_table, {keypos, #requested_node.ip_port}]),
    ?GET_PEERS_SEARCHES_TABLE = ets:new(?GET_PEERS_SEARCHES_TABLE, [set, named_table, {keypos, #get_peers_search.info_hash}]),
    ok.

% ------------------------------------------------------------------------
% NOT_ASSIGNED_NODES_TABLE
% ------------------------------------------------------------------------

%%
%%
%%
-spec get_not_assigned_nodes() -> [#node{}].

get_not_assigned_nodes() ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{_ = '_'}).


%%
%%
%%
-spec get_not_assigned_nodes(
    Distance :: distance()
) -> [#node{}].

get_not_assigned_nodes(Distance) ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{distance = Distance, _ = '_'}).


%%
%%
%%
-spec get_not_assigned_node(
    Ip   :: inet:ip_address(),
    Port :: inet:port_number()
) -> [#node{}].

get_not_assigned_node(Ip, Port) ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{ip_port = {Ip, Port}, _ = '_'}).


%%
%%
%%
-spec insert_to_not_assigned_nodes(
    Node :: #node{}
) -> true.

insert_to_not_assigned_nodes(Node) ->
    true = ets:insert(?NOT_ASSIGNED_NODES_TABLE, Node).


%%
%%
%%
-spec delete_from_not_assigned_nodes(
    Ip   :: inet:ip_address(),
    Port :: inet:port_number()
) -> true.

delete_from_not_assigned_nodes(Ip, Port) ->
    true = ets:match_delete(?NOT_ASSIGNED_NODES_TABLE, #node{ip_port = {Ip, Port}, _ = '_'}).

% ------------------------------------------------------------------------
% REQUESTED_NODES_TABLE
% ------------------------------------------------------------------------


%%
%%
%%
-spec insert_to_requested_nodes(
    Node :: #requested_node{}
) -> true.

insert_to_requested_nodes(Node) ->
    true = ets:insert(?REQUESTED_NODES_TABLE, Node).


%%
%%
%%
-spec get_requested_node(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    InfoHash    :: binary()
) -> [#requested_node{}].

get_requested_node(Ip, Port, InfoHash) ->
    ets:match_object(?REQUESTED_NODES_TABLE, #requested_node{ip_port = {Ip, Port}, info_hash = InfoHash, _ = '_'}).


%%
%%
%%
-spec get_info_hash(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    TxId    :: tx_id()
) -> false | binary().

get_info_hash(Ip, Port, TxId) ->
    Request = #requested_node{ip_port = {Ip, Port}, transaction_id = TxId, _ = '_'},
    case ets:match_object(?REQUESTED_NODES_TABLE, Request) of
        [#requested_node{info_hash = InfoHash}] -> InfoHash;
        [] -> false
    end.


%%
%%
%%
-spec delete_requested_nodes(
    InfoHash :: binary()
) -> true.

delete_requested_nodes(InfoHash) ->
    true = ets:match_delete(?REQUESTED_NODES_TABLE, #requested_node{info_hash = InfoHash, _ = '_'}).

% ------------------------------------------------------------------------
% GET_PEERS_SEARCHES_TABLE
% ------------------------------------------------------------------------

%%
%%
%%
-spec insert_to_get_peers_searches(
    GetPeersSearch :: #get_peers_search{}
) -> true.

insert_to_get_peers_searches(GetPeersSearch) ->
    true = ets:insert(?GET_PEERS_SEARCHES_TABLE, GetPeersSearch).


%%
%%
%%
-spec get_all_get_peers_searches() -> [#get_peers_search{}].

get_all_get_peers_searches() ->
    ets:match_object(?GET_PEERS_SEARCHES_TABLE, #get_peers_search{_ = '_'}).


%%
%%
%%
-spec delete_get_peers_search(
    InfoHash :: binary()
) -> true.

delete_get_peers_search(InfoHash) ->
    true = ets:match_delete(?GET_PEERS_SEARCHES_TABLE, #get_peers_search{info_hash = InfoHash, _ = '_'}).



