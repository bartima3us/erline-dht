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
-define(ID(Name), {erline_dht_bucket, Name}).
-define(SPEC(Name, Port), #{
    id          => ?ID(Name),
    start       => {erline_dht_bucket, start_link, [Name, Port]},
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
start(Name) ->
    start(Name, undefined).

start(Name, Port) ->
    Children = supervisor:which_children(?MODULE),
    case lists:keysearch(?ID(Name), 1, Children) of
        false      ->
            {ok, _} = supervisor:start_child(?MODULE, ?SPEC(Name, Port)),
            ok;
        {value, _} ->
            ok
    end.


%%
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
%%
init([]) ->
    Specs = case erline_dht:get_env(auto_start) of
        true      -> [?SPEC(node1, undefined)];
        undefined -> [?SPEC(node1, undefined)];
        false     -> []
    end,
    {ok, { {one_for_one, 5, 10}, Specs} }.

%%====================================================================
%% Internal functions
%%====================================================================
