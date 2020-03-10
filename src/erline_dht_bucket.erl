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
    add_node_async/3,
    add_node_async/5,
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
    distance                :: non_neg_integer(),
    k                       :: pos_integer(),
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
    gen_server:start_link(?REF(Distance), ?MODULE, [Distance, K, NodeId], []).


% @todo force_add_node
% @todo call timeout as option

add_node(Distance, Ip, Port) ->
    gen_server:call(?REF(Distance), {add_node, Ip, Port}, ?CALL_TIMEOUT).


add_node(Distance, Ip, Port, Hash, TransactionId) ->
    gen_server:call(?REF(Distance), {add_node, Ip, Port, Hash, TransactionId}, ?CALL_TIMEOUT).


add_node_async(Distance, Ip, Port) ->
    gproc:whereis_name(?REG(Distance)) ! {add_node, Ip, Port},
    ok.


add_node_async(Distance, Ip, Port, Hash, TransactionId) ->
    gproc:whereis_name(?REG(Distance)) ! {add_node, Ip, Port, Hash, TransactionId},
    ok.


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

init([Distance, K, MyNodeId]) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, true}]),
    {ok, #state{distance = Distance, k = K, my_node_id = MyNodeId, socket = Socket}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({add_node, Ip, Port}, _From, State = #state{}) ->
    {Status, Hash, NewState} = do_add_node_without_hash(Ip, Port, State),
    {reply, {Status, Hash}, NewState};

handle_call({add_node, Ip, Port, Hash, TransactionId}, _From, State = #state{}) ->
    {Status, Hash, NewState} = do_add_node_with_hash(Ip, Port, Hash, TransactionId, State),
    {reply, {Status, Hash}, NewState};

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
    {Response, NewState} = do_ping(Ip, Port, State),
    {reply, Response, NewState};

%
%
handle_call({find_node, Ip, Port}, _From, State = #state{my_node_id = MyNodeId}) ->
    % @todo check if node exists
    case get_node(Ip, Port, State) of
        false ->
            {reply, {error, node_not_exist}, State};
        #node{transaction_id = TransactionId} ->
            {ok, Response, NewState} = do_find_node(Ip, Port, MyNodeId, TransactionId, State),
            {reply, {ok, Response}, NewState}
    end.

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
handle_info({add_node, Ip, Port}, State = #state{}) ->
    {_Status, _Response, NewState} = do_add_node_without_hash(Ip, Port, State),
    {noreply, NewState};

handle_info({add_node, Ip, Port, Hash, TransactionId}, State = #state{}) ->
    {_Status, _Response, NewState} = do_add_node_with_hash(Ip, Port, Hash, TransactionId, State),
    {noreply, NewState};

handle_info({find_node, Ip, Port, Target}, State = #state{my_node_id = MyNodeId}) ->
    % @todo can race condition crash this?
    #node{transaction_id = TransactionId} = get_node(Ip, Port, State),
    % @todo make active socket on async
    {ok, Response, NewState} = do_find_node(Ip, Port, Target, TransactionId, State),
    % Add founded nodes
    ok = lists:foreach(fun (#{hash := FoundHash, ip := FoundIp, port := FoundPort}) ->
        {ok, Distance} = erline_dht_helper:get_distance(MyNodeId, FoundHash),
        io:format("xxxxxxx Distance=~p~n", [Distance]),
        % Can't assume that node we got is live so we need to ping it.
        ok = add_node_async(Distance, FoundIp, FoundPort)
    end, Response),
    {noreply, NewState};

handle_info({ping, Ip, Port}, State) ->
    case do_ping(Ip, Port, State) of
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
find_node_async(Distance, Ip, Port, Hash) ->
    gproc:whereis_name(?REG(Distance)) ! {find_node, Ip, Port, Hash},
    ok.


%%
%%
%%
do_add_node_without_hash(Ip, Port, State = #state{distance = Distance, k = K, nodes = Nodes}) ->
    case get_node(Ip, Port, State) of
        false ->
            case erlang:length(Nodes) < K of
                true  ->
                    NewNode = #node{
                        ip_port = {Ip, Port}
                    },
                    % @todo make active socket on async
                    case do_ping(Ip, Port, State#state{nodes = [NewNode | Nodes]}) of
                        {Response = {ok, Hash}, NewState} ->
                            ok = find_node_async(Distance, Ip, Port, Hash),
                            {ok, Response, NewState};
                        {Response = {error, _Reason}, NewState0} ->
                            NewState1 = delete_node(Ip, Port, NewState0),
                            {ok, Response, NewState1}
                    end;
                false ->
                    % @todo implement check last changed
                    {error, bucket_is_full, State}
            end;
        #node{} ->
            % @todo check last changed and refresh?
            {error, node_already_added, State}
    end.


%%
%%
%%
do_add_node_with_hash(Ip, Port, Hash, TransactionId, State = #state{distance = Distance, k = K, nodes = Nodes}) ->
    case get_node(Ip, Port, State) of
        false ->
            case erlang:length(Nodes) < K of
                true  ->
                    NewNode = #node{
                        ip_port         = {Ip, Port},
                        hash            = Hash,
                        last_changed    = calendar:local_time(),
                        transaction_id  = TransactionId,
                        ping_timer      = schedule_next_ping(Ip, Port)
                    },
                    ok = find_node_async(Distance, Ip, Port, Hash),
                    {ok, Hash, State#state{nodes = [NewNode | Nodes]}};
                false ->
                    % @todo implement check last changed
                    {error, bucket_is_full, State}
            end;
        #node{} ->
            % @todo check last changed and refresh?
            {error, node_already_added, State}
    end.


%%
%%
%%
do_find_node(Ip, Port, Target, TransactionId, State = #state{socket = Socket, my_node_id = MyNodeId}) ->
    case erline_dht_message:find_node(Ip, Port, Socket, MyNodeId, TransactionId, Target) of
        {ok, Response} ->
            io:format("xxxxxxx do_find_node response=~p~n", [Response]),
            NewState = update_transaction_id(Ip, Port, State),
            {ok, Response, NewState};
        {error, _Reason} ->
            io:format("xxxxxxx do_find_node _Reason=~p~n", [_Reason]),
            {ok, [], State}
    end.


%%
%%
%%
do_ping(Ip, Port, State) ->
    do_ping(Ip, Port, State, 2).

do_ping(Ip, Port, State = #state{my_node_id = MyNodeId, socket = Socket}, Tries) ->
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
    case lists:keysearch({Ip, Port}, #node.ip_port, Nodes) of
        {value, Node} -> Node;
        false         -> false
    end.


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


