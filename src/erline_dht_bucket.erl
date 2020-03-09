%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% Mainline DHT bucket implementation.
%%% @end
%%% Created : 03. Mar 2020 00.39
%%%-------------------------------------------------------------------
-module(erline_dht_bucket).
-author("bartimaeus").

-behaviour(gen_server).

%% API
-export([
    start_link/3,
    get_nodes/4,
    add_node/3,
    add_node/5,
    get_all_nodes/1,
    ping/3,
    find_node/3
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
    update_transaction_id/1
]).
-endif.

-define(SERVER, ?MODULE).
-define(REG(N), {n, l, {?SERVER, N}}).
-define(REF(N), {via, gproc, ?REG(N)}).

-define(CALL_TIMEOUT, 5000).
-define(NEXT_PING_LOW_TIME, 300000).
-define(NEXT_PING_HIGH_TIME, 840000).

-record(node, {
    ip_port                         :: {inet:ip_address(), inet:port_number()},
    hash                            :: binary(),
    last_changed                    :: calendar:datetime(),
    transaction_id      = <<0,0>>   :: binary(),
    active_transactions = []        :: [binary()],
    ping_timer                      :: reference()
}).

-record(state, {
    k                       :: non_neg_integer(),
    my_node_id              :: binary(),
    socket                  :: port(),
    nodes           = []    :: [#node{}],
    last_changed            :: calendar:datetime()
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
    K :: pos_integer(),
    Distance :: non_neg_integer(),
    NodeId :: binary()
) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.

start_link(K, Distance, NodeId) ->
    gen_server:start_link(?REF(Distance), ?MODULE, [K, NodeId], []).


% @todo force_add_node
% @todo call timeout as option

add_node(Distance, Ip, Port) ->
    gen_server:call(?REF(Distance), {add_node, Ip, Port}, ?CALL_TIMEOUT).


add_node(Distance, Ip, Port, Hash, TransactionId) ->
    gen_server:call(?REF(Distance), {add_node, Ip, Port, Hash, TransactionId}, ?CALL_TIMEOUT).


get_all_nodes(Distance) ->
    gen_server:call(?REF(Distance), get_all_nodes, ?CALL_TIMEOUT).


ping(Distance, Ip, Port) ->
    gen_server:call(?REF(Distance), {ping, Ip, Port}, ?CALL_TIMEOUT).


%%ping_response(NodeId) ->
%%    ok.


find_node(Distance, Ip, Port) ->
    gen_server:call(?REF(Distance), {find_node, Ip, Port}, ?CALL_TIMEOUT).


%%
%%
%%find_node_response() ->
%%    ok.


get_nodes(Distance, Ip, Port, InfoHash) ->
    gen_server:call(?REF(Distance), {get_nodes, Ip, Port, InfoHash}, ?CALL_TIMEOUT).


%%get_nodes_response() ->
%%    ok.
%%
%%
%%announce_node() ->
%%    ok.
%%
%%
%%announce_node_response() ->
%%    ok.


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
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).

init([K, MyNodeId]) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, true}]),
    {ok, #state{k = K, my_node_id = MyNodeId, socket = Socket}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({add_node, Ip, Port}, _From, State = #state{nodes = Nodes, k = K}) ->
    % @todo check maybe exists
    case erlang:length(Nodes) < K of
        true  ->
            NewNode = #node{
                ip_port = {Ip, Port}
            },
            case ping_and_update(Ip, Port, State#state{nodes = [NewNode | Nodes]}) of
                {Response = {ok, _Hash}, NewState} ->
                    {reply, Response, NewState};
                {Response = {error, _Reason}, NewState0} ->
                    NewState1 = delete_node(Ip, Port, NewState0),
                    {reply, Response, NewState1}
            end;
        false ->
            check
    end;

handle_call({add_node, Ip, Port, Hash, TransactionId}, _From, State = #state{nodes = Nodes, k = K}) ->
    % @todo check maybe exists
    case erlang:length(Nodes) < K of
        true  ->
            NewNode = #node{
                ip_port         = {Ip, Port},
                hash            = Hash,
                last_changed    = calendar:local_time(),
                transaction_id  = TransactionId,
                ping_timer      = schedule_next_ping(Ip, Port)
            },
            {reply, {ok, Hash}, State#state{nodes = [NewNode | Nodes]}};
        false ->
            check
    end;

%
%
handle_call(get_all_nodes, _From, State = #state{nodes = Nodes}) ->
    Response = lists:foldl(fun (Node, Acc) ->
        #node{
            ip_port         = {Ip, Port},
            hash            = Hash,
            last_changed    = LastChanged
        } = Node,
        [#{ip => Ip, port => Port, hash => Hash, last_changed => LastChanged} | Acc]
    end, [], Nodes),
    {reply, Response, State};

%
%
handle_call({ping, Ip, Port}, _From, State = #state{}) ->
    {Response, NewState} = ping_and_update(Ip, Port, State),
    {reply, Response, NewState};

%
%
handle_call({find_node, Ip, Port}, _From, State = #state{my_node_id = MyNodeId, socket = Socket}) ->
    Node = get_node(Ip, Port, State),
    #node{transaction_id  = TransactionId} = Node,
    Response = erline_dht_message:find_node(Ip, Port, Socket, MyNodeId, TransactionId, MyNodeId),
    NewState0 = update_transaction_id(Ip, Port, State),
    {reply, Response, NewState0}.

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
handle_info({ping, Ip, Port}, State) ->
    case ping_and_update(Ip, Port, State) of
        {{ok, _Hash}, NewState0} ->
            Node = get_node(Ip, Port, NewState0),
            ok = cancel_timer(Node),
            Params = [
                {last_changed, calendar:local_time()},
                {ping_timer,   schedule_next_ping(Node)}
            ],
            NewState1 = update_node(Ip, Port, Params, NewState0),
            {noreply, NewState1};
        {{error, _Error}, NewState0} ->
            NewState1 = delete_node(Ip, Port, NewState0),
            {noreply, NewState1}
    end.

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

%%
%%
%%
ping_and_update(Ip, Port, State) ->
    ping_and_update(Ip, Port, State, 2).

ping_and_update(Ip, Port, State = #state{my_node_id = MyNodeId, socket = Socket}, Tries) ->
    Node = get_node(Ip, Port, State),
    #node{transaction_id = TransactionId} = Node,
    NewState0 = update_transaction_id(Ip, Port, State),
    case erline_dht_message:ping(Ip, Port, Socket, MyNodeId, TransactionId, Tries) of
        {ok, NodeHash} ->
            Params = [
                {hash,         NodeHash},
                {last_changed, calendar:local_time()},
                {ping_timer,   schedule_next_ping(Node)}
            ],
            NewState1 = update_node(Ip, Port, Params, NewState0),
            {{ok, NodeHash}, NewState1};
        {error, Reason} ->
            {{error, Reason}, NewState0}
    end.


%%  @private
%%  Increase node current transaction ID by 1.
%%
update_transaction_id(Ip, Port, State = #state{nodes = Nodes}) ->
    Node = get_node(Ip, Port, State),
    NewNodes = lists:keyreplace({Ip, Port}, #node.ip_port, Nodes, update_transaction_id(Node)),
    State#state{nodes = NewNodes}.

update_transaction_id(Node = #node{transaction_id = undefined}) ->
    Node#node{transaction_id = <<0,0>>};

update_transaction_id(Node = #node{transaction_id = <<255,255>>}) ->
    Node#node{transaction_id = <<0,0>>};

update_transaction_id(Node = #node{transaction_id = LastTransactionIdBin}) ->
    <<LastTransactionIdInt:16>> = LastTransactionIdBin,
    NewTransactionIdInt = LastTransactionIdInt + 1,
    Node#node{transaction_id = <<NewTransactionIdInt:16>>}.


%%
%%
%%
get_node(Ip, Port, #state{nodes = Nodes}) ->
    {value, Node} = lists:keysearch({Ip, Port}, #node.ip_port, Nodes),
    Node.


%%
%%
%%
delete_node(Ip, Port, State = #state{nodes = Nodes}) ->
    Node = get_node(Ip, Port, State),
    ok = cancel_timer(Node),
    NewNodes = lists:keydelete({Ip, Port}, #node.ip_port, Nodes),
    State#state{nodes = NewNodes}.


%%
%%
cancel_timer(#node{ping_timer = undefined}) ->
    ok;

cancel_timer(#node{ping_timer = PingTimer}) ->
    erlang:cancel_timer(PingTimer),
    ok.

%%
%%
%%
update_node(Ip, Port, Params, State = #state{nodes = Nodes}) ->
    Node = get_node(Ip, Port, State),
    UpdatedNode = lists:foldl(fun
        ({hash, Hash}, AccNode)                -> AccNode#node{hash = Hash};
        ({last_changed, LastChanged}, AccNode) -> AccNode#node{last_changed = LastChanged};
        ({ping_timer, PingTimerRef}, AccNode)  -> AccNode#node{ping_timer = PingTimerRef}
    end, Node, Params),
    NewNodes = lists:keyreplace({Ip, Port}, #node.ip_port, Nodes, UpdatedNode),
    State#state{nodes = NewNodes}.


%%  @doc
%%  @private
%%  5-14 min.
%%
schedule_next_ping(Ip, Port) ->
    Time = crypto:rand_uniform(?NEXT_PING_LOW_TIME, ?NEXT_PING_HIGH_TIME),
    erlang:send_after(Time, self(), {ping, Ip, Port}).

schedule_next_ping(#node{ip_port = {Ip, Port}, ping_timer = undefined}) ->
    Time = crypto:rand_uniform(?NEXT_PING_LOW_TIME, ?NEXT_PING_HIGH_TIME),
    erlang:send_after(Time, self(), {ping, Ip, Port});

schedule_next_ping(#node{}) ->
    undefined.


