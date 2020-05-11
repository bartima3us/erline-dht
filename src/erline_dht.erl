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
    start_node/1,
    start_node/2,
    stop_node/1,
    add_node/3,
    get_peers/2,
    get_peers/4,
    get_all_nodes_in_bucket/2,
    get_not_assigned_nodes/1,
    get_not_assigned_nodes/2,
    get_buckets_filling/1,
    get_port/1,
    set_peer_port/2,
    get_event_mgr_pid/1
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
%%  Explicitly start ErLine DHT node.
%%  @end
start_node(Name) ->
    erline_dht_sup:start(Name).


%%  @doc
%%  Explicitly start ErLine DHT node on the specified port.
%%  @end
start_node(Name, Port) ->
    erline_dht_sup:start(Name, Port).


%%  @doc
%%  Stop ErLine DHT node.
%%  @end
stop_node(Name) ->
    erline_dht_bucket:stop(Name).


%%  @doc
%%  Add node with unknown hash to the bucket.
%%  @end
-spec add_node(
    Name    :: atom(),
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number()
) -> ok.

add_node(Name, Ip, Port) ->
    erline_dht_bucket:add_node(Name, Ip, Port).


%%  @doc
%%  Try to find peers for the info hash in the network.
%%  @end
-spec get_peers(
    Name     :: atom(),
    InfoHash :: binary()
) -> ok.

get_peers(Name, InfoHash) ->
    erline_dht_bucket:get_peers(Name, InfoHash).


%%  @doc
%%  Try to get peers for the info hash from one node.
%%  @end
-spec get_peers(
    Name     :: atom(),
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number(),
    InfoHash :: binary()
) -> ok.

get_peers(Name, Ip, Port, InfoHash) ->
    erline_dht_bucket:get_peers(Name, Ip, Port, InfoHash).


%%  @doc
%%  Return all nodes information in the bucket.
%%  @end
-spec get_all_nodes_in_bucket(
    Name     :: atom(),
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        status          => status(),
        last_changed    => calendar:datetime()}].

get_all_nodes_in_bucket(Name, Distance) ->
    erline_dht_bucket:get_all_nodes_in_bucket(Name, Distance).


%%  @doc
%%  Return all not assigned nodes information.
%%  @end
-spec get_not_assigned_nodes(
    Name    :: atom()
) ->
    [#{ip              => inet:ip_address(),
       port            => inet:port_number(),
       hash            => binary(),
       last_changed    => calendar:datetime()}].

get_not_assigned_nodes(Name) ->
    erline_dht_bucket:get_not_assigned_nodes(Name).


%%  @doc
%%  Return not assigned nodes information.
%%  @end
-spec get_not_assigned_nodes(
    Name     :: atom(),
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        last_changed    => calendar:datetime()}].

get_not_assigned_nodes(Name, Distance) ->
    erline_dht_bucket:get_not_assigned_nodes(Name, Distance).


%%  @doc
%%  Return amount of nodes in every bucket.
%%  @end
-spec get_buckets_filling(
    Name    :: atom()
) -> [#{distance => distance(), nodes => non_neg_integer()}].

get_buckets_filling(Name) ->
    erline_dht_bucket:get_buckets_filling(Name).


%%  @doc
%%  Return UDP socket port of the client.
%%  @end
-spec get_port(
    Name    :: atom()
) -> Port :: inet:port_number().

get_port(Name) ->
    erline_dht_bucket:get_port(Name).


%%  @doc
%%  Set peer port.
%%  @end
-spec set_peer_port(
    Name    :: atom(),
    Port    :: inet:port_number()
) -> ok.

set_peer_port(Name, Port) ->
    erline_dht_bucket:set_peer_port(Name, Port).


%%  @doc
%%  Return event manager pid.
%%  @end
-spec get_event_mgr_pid(
    Name    :: atom()
) -> EventMgrPid :: pid().

get_event_mgr_pid(Name) ->
    erline_dht_bucket:get_event_mgr_pid(Name).


