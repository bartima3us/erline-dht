%%%-------------------------------------------------------------------
%% @doc erline_dht public API
%% @end
%%%-------------------------------------------------------------------

-module(erline_dht).

-behaviour(application).

-export([
    get_env/1,
    get_env/2,
    add_node_to_bucket/2,
    get_all_nodes_in_bucket/1,
    get_not_assigned_nodes/0
]).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

-define(APP, erline_dht).

%%====================================================================
%% Application callbacks
%%====================================================================

%%
%%
%%
start(_StartType, _StartArgs) ->
    erline_dht_sup:start_link().


%%
%%
%%
stop(_State) ->
    ok.


%%====================================================================
%% API
%%====================================================================

%%
%%
%%
get_env(Var) ->
    case application:get_env(?APP, Var) of
        {ok, Val} -> Val;
        undefined -> undefined
    end.


%%
%%
%%
get_env(Var, Default) ->
    application:get_env(?APP, Var, Default).


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
