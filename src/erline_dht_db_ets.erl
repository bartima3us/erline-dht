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

%% API
-export([
    init/0,
    get_not_assigned_nodes/0,
    get_not_assigned_nodes_by_distance/1,
    get_not_assigned_nodes_by_ip_and_port/2,
    insert_to_not_assigned_nodes/1,
    delete_from_not_assigned_nodes/2
]).

-define(NOT_ASSIGNED_NODES_TABLE, 'erline_dht$not_assigned_nodes').

%%%===================================================================
%%% API
%%%===================================================================


%%
%%
%%
init() ->
    % 'compressed' - trade-off. Example with 200k nodes:
    % Read performance: 7 microseconds vs 13 microseconds;
    % RAM consumption: 618 MB vs 266 MB
    Options = [
        set,
        named_table,
        compressed,
        {keypos, #node.ip_port}
    ],
    ets:new(?NOT_ASSIGNED_NODES_TABLE, Options).


%%
%%
%%
get_not_assigned_nodes() ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{_ = '_'}).


%%
%%
%%
get_not_assigned_nodes_by_distance(Distance) ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{distance = Distance, _ = '_'}).


%%
%%
%%
get_not_assigned_nodes_by_ip_and_port(Ip, Port) ->
    ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{ip_port = {Ip, Port}, _ = '_'}).


%%
%%
%%
insert_to_not_assigned_nodes(NewNode) ->
    true = ets:insert(?NOT_ASSIGNED_NODES_TABLE, NewNode).


%%
%%
%%
delete_from_not_assigned_nodes(Ip, Port) ->
    true = ets:match_delete(?NOT_ASSIGNED_NODES_TABLE, #node{ip_port = {Ip, Port}, _ = '_'}).


