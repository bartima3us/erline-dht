%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% UDP acceptor server.
%%% @end
%%% Created : 16. Aug 2019 17.02
%%%-------------------------------------------------------------------
-module(erline_dht_server).
-author("bartimaeus").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    add_node/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ifdef(TEST).
-export([
    get_distance/2
]).
-endif.

-define(SERVER, ?MODULE).
-define(MY_NODE_ID, <<169,246,141,183,17,96,15,191,158,252,221,69,218,231,8,97,231,8,214,41>>).
-define(K, 8).

-record(state, {
    socket
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
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%
%%
%%
add_node(Ip, Port) ->
    gen_server:call(?SERVER, {add_node, Ip, Port}).


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
init([]) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, true}]),
    {ok, #state{socket = Socket}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({add_node, Ip, Port}, _From, State = #state{socket = Socket}) ->
    Response = case erline_dht_message:ping(Ip, Port, Socket, ?MY_NODE_ID, <<0,0>>, 2) of
        {ok, Hash} ->
            {ok, Distance} = erline_dht_helper:get_distance(?MY_NODE_ID, Hash),
            ok = erline_dht_bucket_sup:start_k_bucket(?K, Distance, ?MY_NODE_ID),
            {ok, Hash} = erline_dht_bucket:add_node(Distance, Ip, Port, Hash, <<0,1>>),
            {ok, {Distance, Hash}};
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Response, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
    {noreply, State}.


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
handle_info(_Info, State) ->
    {noreply, State}.


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


