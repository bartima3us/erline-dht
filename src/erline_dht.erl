%%%-------------------------------------------------------------------
%% @doc erline_dht public API
%% @end
%%%-------------------------------------------------------------------

-module(erline_dht).

-behaviour(application).

-export([
    add_node_to_bucket/2,
    get_all_nodes_in_bucket/1,
    get_not_assigned_nodes/0
]).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    erline_dht_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.


%%
%%
%%
add_node_to_bucket(Ip, Port) ->
    erline_dht_bucket:add_node(Ip, Port).


%%
%%
%%
get_all_nodes_in_bucket(Distance) ->
    erline_dht_bucket:get_all_nodes_in_bucket(Distance).


%%
%%
%%
get_not_assigned_nodes() ->
    erline_dht_bucket:get_not_assigned_nodes().


%%====================================================================
%% Internal functions
%%====================================================================
