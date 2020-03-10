%%%-------------------------------------------------------------------
%% @doc erline_dht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(erline_dht_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    ServerSpec = #{
        id          => erline_dht_server,
        start       => {erline_dht_server, start_link, []},
        restart     => temporary,
        shutdown    => 5000,
        type        => worker,
        modules     => [erline_dht_server]
    },
    BucketSupSpec = #{
        id          => erline_dht_bucket_sup,
        start       => {erline_dht_bucket_sup, start_link, []},
        restart     => permanent,
        shutdown    => infinity,
        type        => supervisor,
        modules     => [erline_dht_bucket_sup]
    },
    {ok, { {one_for_one, 5, 10}, [ServerSpec, BucketSupSpec]} }.

%%====================================================================
%% Internal functions
%%====================================================================
