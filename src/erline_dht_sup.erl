%%%-------------------------------------------------------------------
%% @doc erline_dht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(erline_dht_sup).

-behaviour(supervisor).
-include("erline_dht.hrl").

%% API
-export([
    start_link/0,
    start/1,
    start/2
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(BUCKET_ID(Name), {erline_dht_bucket, Name}).
-define(NAN_CACHE_ID(Name), {erline_dht_nan_cache, Name}).

-define(BUCKET_SPEC(Name, Port), #{
    id          => ?BUCKET_ID(Name),
    start       => {erline_dht_bucket, start_link, [Name, Port]},
    restart     => temporary,
    shutdown    => 5000,
    type        => worker,
    modules     => [erline_dht_bucket]
}).

-define(CACHE_SPEC(Name), #{
    id          => ?NAN_CACHE_ID(Name),
    start       => {erline_dht_nan_cache, start_link, [Name]},
    restart     => temporary,
    shutdown    => 5000,
    type        => worker,
    modules     => [erline_dht_nan_cache]
}).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%%
%%
%%
start(Name) ->
    start(Name, undefined).

start(Name, Port) ->
    Children = supervisor:which_children(?MODULE),
    case lists:keysearch(?BUCKET_ID(Name), 1, Children) of
        false      ->
            {ok, _} = supervisor:start_child(?MODULE, ?BUCKET_SPEC(node1, Port)),
            {ok, _} = supervisor:start_child(?MODULE, ?CACHE_SPEC(node1)),
            ok;
        {value, _} ->
            ok
    end.


%%
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
%%
init([]) ->
    Specs = case erline_dht:get_env(node1, auto_start) of
        true      -> [?BUCKET_SPEC(node1, undefined), ?CACHE_SPEC(node1)];
        undefined -> [?BUCKET_SPEC(node1, undefined), ?CACHE_SPEC(node1)];
        false     -> []
    end,
    {ok, {{one_for_all, 5, 10}, Specs}}.

%%====================================================================
%% Internal functions
%%====================================================================
