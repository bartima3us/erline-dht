%%%-------------------------------------------------------------------
%% @doc erline_dht public API
%% @end
%%%-------------------------------------------------------------------

-module(erline_dht).
-author("bartimaeus").
-include("erline_dht.hrl").

-behaviour(application).

-export([
    get_env/1,
    get_env/2,
    add_node/2,
    get_peers/1,
    get_peers/3,
    get_all_nodes_in_bucket/1,
    get_not_assigned_nodes/0,
    get_not_assigned_nodes/1,
    get_buckets_filling/0,
    get_port/0,
    get_event_mgr_pid/0
]).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

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

%%  @doc
%%  Get environment variable.
%%  @end
-spec get_env(
    Var :: atom()
) -> term().

get_env(Var) ->
    case application:get_env(?APP, Var) of
        {ok, Val} -> Val;
        undefined -> undefined
    end.


%%  @doc
%%  Get environment variable.
%%  @end
-spec get_env(
    Var     :: atom(),
    Default :: term()
) -> term().

get_env(Var, Default) ->
    application:get_env(?APP, Var, Default).


%%  @doc
%%  Add node with unknown hash to the bucket.
%%  @end
-spec add_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number()
) -> ok.

add_node(Ip, Port) ->
    erline_dht_bucket:add_node(Ip, Port).


%%  @doc
%%  Try to find peers for the info hash in the network.
%%  @end
-spec get_peers(
    InfoHash :: binary()
) -> ok.

get_peers(InfoHash) ->
    erline_dht_bucket:get_peers(InfoHash).


%%  @doc
%%  Try to get peers for the info hash from one node.
%%  @end
-spec get_peers(
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number(),
    InfoHash :: binary()
) -> ok.

get_peers(Ip, Port, InfoHash) ->
    erline_dht_bucket:get_peers(Ip, Port, InfoHash).


%%  @doc
%%  Return all nodes information in the bucket.
%%  @end
-spec get_all_nodes_in_bucket(
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        status          => status(),
        last_changed    => calendar:datetime()}].

get_all_nodes_in_bucket(Distance) ->
    erline_dht_bucket:get_all_nodes_in_bucket(Distance).


%%  @doc
%%  Return all not assigned nodes information.
%%  @end
-spec get_not_assigned_nodes() ->
    [#{ip              => inet:ip_address(),
       port            => inet:port_number(),
       hash            => binary(),
       last_changed    => calendar:datetime()}].

get_not_assigned_nodes() ->
    erline_dht_bucket:get_not_assigned_nodes().


%%  @doc
%%  Return not assigned nodes information.
%%  @end
-spec get_not_assigned_nodes(
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        last_changed    => calendar:datetime()}].

get_not_assigned_nodes(Distance) ->
    erline_dht_bucket:get_not_assigned_nodes(Distance).


%%  @doc
%%  Return amount of nodes in every bucket.
%%  @end
-spec get_buckets_filling() -> [#{distance => distance(), nodes => non_neg_integer()}].

get_buckets_filling() ->
    erline_dht_bucket:get_buckets_filling().


%%  @doc
%%  Return UDP socket port of the client.
%%  @end
-spec get_port() -> Port :: inet:port_number().

get_port() ->
    erline_dht_bucket:get_port().


%%  @doc
%%  Return event manager pid.
%%  @end
-spec get_event_mgr_pid() -> EventMgrPid :: pid().

get_event_mgr_pid() ->
    erline_dht_bucket:get_event_mgr_pid().


%%====================================================================
%% Internal functions
%%====================================================================
