%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% Not assigned nodes counter cache and reconciler.
%%% @end
%%% Created : 18. May 2020 20.57
%%%-------------------------------------------------------------------
-module(erline_dht_nan_cache).
-author("bartimaeus").
-include("erline_dht.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    add/2,
    sub/2,
    get_amount/1
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(RECONCILE_TIME, 10000).

-record(state, {
    name        :: atom(),
    db_mod      :: module(),
    amount = 0  :: integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(
    NodeName :: atom()
) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.

start_link(NodeName) ->
    gen_server:start_link({local, get_process_name(NodeName)}, ?MODULE, [NodeName], []).


%%
%%
%%
add(NodeName, Amount) ->
    gen_server:cast(get_process_name(NodeName), {add, Amount}).


%%
%%
%%
sub(NodeName, Amount) ->
    gen_server:cast(get_process_name(NodeName), {sub, Amount}).


%%
%%
%%
get_amount(NodeName) ->
    gen_server:call(get_process_name(NodeName), get_amount).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([NodeName]) ->
    DbMod = erline_dht:get_env(NodeName, db_mod, ?DEFAULT_DB_MOD),
    erlang:send_after(?RECONCILE_TIME, self(), reconcile),
    {ok, #state{name = NodeName, db_mod = DbMod}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(get_amount, _From, State = #state{amount = Amount}) ->
    {reply, Amount, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast({add, Amount}, State = #state{amount = CurrAmount}) ->
    {noreply, State#state{amount = CurrAmount + Amount}};

handle_cast({sub, Amount}, State = #state{amount = CurrAmount}) ->
    {noreply, State#state{amount = CurrAmount - Amount}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(reconcile, State = #state{db_mod = DbMod, name = NodeName}) ->
    NewAmount = erlang:length(DbMod:get_not_assigned_nodes(NodeName)),
    erlang:send_after(?RECONCILE_TIME, self(), reconcile),
    {noreply, State#state{amount = NewAmount}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%  @private
%%  @doc
%%  Get process name.
%%  @end
-spec get_process_name(
    NodeName :: atom()
) -> ProcessName :: atom().

get_process_name(NodeName) ->
    ServerString = erlang:atom_to_list(?SERVER),
    NodeNameString = erlang:atom_to_list(NodeName),
    erlang:list_to_atom(ServerString ++ "_" ++ NodeNameString).


