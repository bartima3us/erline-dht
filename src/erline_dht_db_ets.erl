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
-include_lib("stdlib/include/ms_transform.hrl").

%% Common API
-export([
    init/1
]).

%% NOT_ASSIGNED_NODES_TABLE API
-export([
    get_not_assigned_nodes/1,
    get_not_assigned_nodes/2,
    get_not_assigned_node/3,
    insert_to_not_assigned_nodes/2,
    update_not_assigned_node/2,
    delete_from_not_assigned_nodes_by_ip_port/3,
    delete_from_not_assigned_nodes_by_dist_date/3
]).

%% REQUESTED_NODES_TABLE API
-export([
    insert_to_requested_nodes/2,
    get_requested_node/4,
    get_requested_nodes/2,
    get_info_hash/4,
    delete_requested_nodes/2
]).

%% GET_PEERS_SEARCHES_TABLE API
-export([
    insert_to_get_peers_searches/2,
    get_all_get_peers_searches/1,
    delete_get_peers_search/2
]).

-define(NOT_ASSIGNED_NODES_TABLE(N), get_table_name(N, not_assigned_nodes)).
-define(REQUESTED_NODES_TABLE(N), get_table_name(N, requested_nodes)).
-define(GET_PEERS_SEARCHES_TABLE(N), get_table_name(N, get_peers_searches)).

%%%===================================================================
%%% API
%%%===================================================================


%%
%%
%%
-spec init(
    NodeName :: atom()
) -> ok.

init(NodeName) ->
    % 'compressed' - trade-off. Example with 200k nodes (not compressed vs compressed):
    % Read performance: 7 microseconds vs 13 microseconds;
    % RAM consumption: 618 MB vs 266 MB
    ets:new(?NOT_ASSIGNED_NODES_TABLE(NodeName), [set, named_table, compressed, public, {keypos, #node.ip_port}]),
    ets:new(?REQUESTED_NODES_TABLE(NodeName), [set, named_table, {keypos, #requested_node.ip_port}]),
    ets:new(?GET_PEERS_SEARCHES_TABLE(NodeName), [set, named_table, {keypos, #get_peers_search.info_hash}]),
    ok.

% ------------------------------------------------------------------------
% NOT_ASSIGNED_NODES_TABLE
% ------------------------------------------------------------------------

%%
%%
%%
-spec get_not_assigned_nodes(
    NodeName :: atom()
) -> [#node{}].

get_not_assigned_nodes(NodeName) ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE(NodeName), #node{_ = '_'}).


%%
%%
%%
-spec get_not_assigned_nodes(
    NodeName :: atom(),
    Distance :: distance()
) -> [#node{}].

get_not_assigned_nodes(NodeName, Distance) ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE(NodeName), #node{distance = Distance, _ = '_'}).


%%
%%
%%
-spec get_not_assigned_node(
    NodeName :: atom(),
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number()
) -> [#node{}].

get_not_assigned_node(NodeName, Ip, Port) ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE(NodeName), #node{ip_port = {Ip, Port}, _ = '_'}).


%%
%%
%%
-spec insert_to_not_assigned_nodes(
    NodeName :: atom(),
    Node     :: #node{}
) -> true.

insert_to_not_assigned_nodes(NodeName, Node) ->
    ok = erline_dht_nan_cache:add(NodeName, 1),
    true = ets:insert(?NOT_ASSIGNED_NODES_TABLE(NodeName), Node).


%%
%%
%%
-spec update_not_assigned_node(
    NodeName :: atom(),
    Node     :: #node{}
) -> true.

update_not_assigned_node(NodeName, Node) ->
    true = ets:insert(?NOT_ASSIGNED_NODES_TABLE(NodeName), Node).


%%
%%
%%
-spec delete_from_not_assigned_nodes_by_ip_port(
    NodeName :: atom(),
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number()
) -> true.

delete_from_not_assigned_nodes_by_ip_port(NodeName, Ip, Port) ->
    ok = erline_dht_nan_cache:sub(NodeName, 1),
    true = ets:match_delete(?NOT_ASSIGNED_NODES_TABLE(NodeName), #node{ip_port = {Ip, Port}, _ = '_'}).


%%
%%
%%
-spec delete_from_not_assigned_nodes_by_dist_date(
    NodeName :: atom(),
    Distance :: distance(),
    Date     :: calendar:datetime()
) -> true.

delete_from_not_assigned_nodes_by_dist_date(NodeName, Distance, Date) ->
    MatchSpec = ets:fun2ms(fun
        (#node{distance = NodeDist, last_changed = LastChanged}) when
            NodeDist =:= Distance,
            LastChanged =< Date
            ->
            true;
        (#node{}) ->
            false
    end),
    DeletedAmount = ets:select_delete(?NOT_ASSIGNED_NODES_TABLE(NodeName), MatchSpec),
    ok = erline_dht_nan_cache:sub(NodeName, DeletedAmount),
    ok.


% ------------------------------------------------------------------------
% REQUESTED_NODES_TABLE
% ------------------------------------------------------------------------


%%
%%
%%
-spec insert_to_requested_nodes(
    NodeName :: atom(),
    Node     :: #requested_node{}
) -> true.

insert_to_requested_nodes(NodeName, Node) ->
    true = ets:insert(?REQUESTED_NODES_TABLE(NodeName), Node).


%%
%%
%%
-spec get_requested_node(
    NodeName    :: atom(),
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    InfoHash    :: binary()
) -> [#requested_node{}].

get_requested_node(NodeName, Ip, Port, InfoHash) ->
    ets:match_object(?REQUESTED_NODES_TABLE(NodeName), #requested_node{ip_port = {Ip, Port}, info_hash = InfoHash, _ = '_'}).


%%
%%
%%
-spec get_requested_nodes(
    NodeName :: atom(),
    InfoHash :: binary()
) -> [#requested_node{}].

get_requested_nodes(NodeName, InfoHash) ->
    ets:match_object(?REQUESTED_NODES_TABLE(NodeName), #requested_node{info_hash = InfoHash, _ = '_'}).


%%
%%
%%
-spec get_info_hash(
    NodeName :: atom(),
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number(),
    TxId     :: tx_id()
) -> false | binary().

get_info_hash(NodeName, Ip, Port, TxId) ->
    Request = #requested_node{ip_port = {Ip, Port}, tx_id = TxId, _ = '_'},
    case ets:match_object(?REQUESTED_NODES_TABLE(NodeName), Request) of
        [#requested_node{info_hash = InfoHash}] -> InfoHash;
        [] -> false
    end.


%%
%%
%%
-spec delete_requested_nodes(
    NodeName :: atom(),
    InfoHash :: binary()
) -> true.

delete_requested_nodes(NodeName, InfoHash) ->
    true = ets:match_delete(?REQUESTED_NODES_TABLE(NodeName), #requested_node{info_hash = InfoHash, _ = '_'}).

% ------------------------------------------------------------------------
% GET_PEERS_SEARCHES_TABLE
% ------------------------------------------------------------------------

%%
%%
%%
-spec insert_to_get_peers_searches(
    NodeName       :: atom(),
    GetPeersSearch :: #get_peers_search{}
) -> true.

insert_to_get_peers_searches(NodeName, GetPeersSearch) ->
    true = ets:insert(?GET_PEERS_SEARCHES_TABLE(NodeName), GetPeersSearch).


%%
%%
%%
-spec get_all_get_peers_searches(
    NodeName :: atom()
) -> [#get_peers_search{}].

get_all_get_peers_searches(NodeName) ->
    ets:match_object(?GET_PEERS_SEARCHES_TABLE(NodeName), #get_peers_search{_ = '_'}).


%%
%%
%%
-spec delete_get_peers_search(
    NodeName :: atom(),
    InfoHash :: binary()
) -> true.

delete_get_peers_search(NodeName, InfoHash) ->
    true = ets:match_delete(?GET_PEERS_SEARCHES_TABLE(NodeName), #get_peers_search{info_hash = InfoHash, _ = '_'}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%  @private
%%  @doc
%%  Make a table name.
%%  Example: 'erline_dht$node1$requested_nodes'
%%  @end
-spec get_table_name(
    NodeName :: atom(),
    Table    :: atom()
) -> TableName :: atom().

get_table_name(NodeName, Table) ->
    StringName = erlang:atom_to_list(?APP) ++ "$" ++ erlang:atom_to_list(NodeName) ++ "$" ++ erlang:atom_to_list(Table),
    erlang:list_to_atom(StringName).


