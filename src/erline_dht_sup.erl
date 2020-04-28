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
    start/0,
    start/1,
    stop/0
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(ID, erline_dht_bucket).
-define(SPEC(Port), #{
    id          => ?ID,
    start       => {erline_dht_bucket, start_link, [Port]},
    restart     => temporary,
    shutdown    => 5000,
    type        => worker,
    modules     => [erline_dht_bucket]
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
start() ->
    start(undefined).

start(Port) ->
    Children = supervisor:which_children(?MODULE),
    case lists:keysearch(?ID, 1, Children) of
        false      ->
            {ok, _} = supervisor:start_child(?MODULE, ?SPEC(Port)),
            ok;
        {value, _} ->
            ok
    end.


%%
%%
%%
stop() ->
    ok = erline_dht_bucket:stop().


%%
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
%%
init([]) ->
    Specs = case erline_dht:get_env(auto_start) of
        true      -> [?SPEC(undefined)];
        undefined -> [?SPEC(undefined)];
        false     -> []
    end,
    {ok, { {one_for_one, 5, 10}, Specs} }.

%%====================================================================
%% Internal functions
%%====================================================================
