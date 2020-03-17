%%%-------------------------------------------------------------------
%% @doc erline_dht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(erline_dht_sup).

-behaviour(supervisor).
-include("erline_dht.hrl").

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
    BucketSpec = #{
        id          => erline_dht_bucket,
        start       => {erline_dht_bucket, start_link, [?K, ?MY_NODE_ID]},
        restart     => temporary,
        shutdown    => 5000,
        type        => worker,
        modules     => [erline_dht_bucket]
    },
    {ok, { {one_for_one, 5, 10}, [BucketSpec]} }.

%%====================================================================
%% Internal functions
%%====================================================================
