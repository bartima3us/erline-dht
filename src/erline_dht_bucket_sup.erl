%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% Mainline DHT bucket supervisor.
%%% @end
%%% Created : 07. Mar 2020 12.20
%%%-------------------------------------------------------------------
-module(erline_dht_bucket_sup).
-author("bartimaeus").

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_k_bucket/3
]).

%% Supervisor callbacks
-export([
    init/1
]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%%
%%
%%
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


%%
%%
%%
start_k_bucket(K, Distance, NodeId) ->
    Id = {erline_dht_bucket, Distance},
    Spec = #{
        id          => Id,
        start       => {erline_dht_bucket, start_link, [K, Distance, NodeId]},
        restart     => temporary,
        shutdown    => 5000,
        type        => worker,
        modules     => [erline_dht_bucket]
    },
    Children = supervisor:which_children(?SERVER),
    case lists:keysearch(Id, 1, Children) of
        false      ->
            {ok, _} = supervisor:start_child(?SERVER, Spec),
            ok;
        {value, _} ->
            ok
    end.


%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.

%%====================================================================
%% Internal functions
%%====================================================================
