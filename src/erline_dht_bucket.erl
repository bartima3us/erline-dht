%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% Mainline DHT bucket implementation.
%%% @end
%%% Created : 14. Mar 2020 12.54
%%%-------------------------------------------------------------------
-module(erline_dht_bucket).
-author("bartimaeus").

-behaviour(gen_server).

%% API
-export([
    start_link/2,
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

-define(SERVER, ?MODULE).
-define(CHECK_NODE_TIMEOUT, 60000).
-define(NEXT_PING_LOW_TIME, 300000).
-define(NEXT_PING_HIGH_TIME, 840000).

-type status()      :: unknown | active | not_active.
-type request()     :: ping | find_node | get_peers | announce.
-type tx_id()       :: binary().
-type active_tx()   :: {request(), tx_id()}.

-record(node, {
    ip_port                         :: {inet:ip_address(), inet:port_number()},
    hash                            :: binary(),
    token                           :: binary(),
    last_changed                    :: calendar:datetime(),
    transaction_id      = <<0,0>>   :: tx_id(),
    active_transactions = []        :: [{request(), tx_id()}],
    ping_timer                      :: reference(),
    % active        - node responding.
    % unknown       - node not responding. <  1 min elapsed.
    % not_active    - node not responding. >= 1 min elapsed.
    status              = unknown   :: status()
}).

-record(bucket, {
    distance    :: 1..160,
    nodes = []  :: [#node{}]
}).

-record(state, {
    my_node_hash        :: binary(),
    socket              :: port(),
    k                   :: pos_integer(),
    buckets = []        :: [#bucket{}],
    not_assigned_nodes  :: [#node{}]
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
-spec(start_link(
    K :: pos_integer(),
    MyNodeHash :: binary()
) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).

start_link(K, MyNodeHash) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [K, MyNodeHash], []).


%%
%%
%%
add_node(Ip, Port) ->
    gen_server:cast(?SERVER, {add_node, Ip, Port}).


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
init([K, MyNodeHash]) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, true}]),
    {ok, #state{socket = Socket, k = K, my_node_hash = MyNodeHash}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast({add_node, Ip, Port}, State) ->
    ok = case get_bucket_and_node(Ip, Port, State) of
        false                  -> do_ping_async(Ip, Port, State);
        {ok, _Bucket, #node{}} -> ok % @todo fire event: {error, already_added}
    end,
    {noreply, State};

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


%%
%%
%%
-spec do_ping_async(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    State   :: #state{}
) -> {ok, State :: #state{}}.

do_ping_async(Ip, Port, State = #state{my_node_hash = MyNodeHash, socket = Socket}) ->
    {ok, _Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    #node{transaction_id = TxId, active_transactions = CurrActiveTx} = Node,
    NewState0 = update_transaction_id(Ip, Port, [{ping, TxId} | CurrActiveTx], State),
    ok = erline_dht_helper:socket_active(Socket),
    ok = erline_dht_message:send_ping(Ip, Port, Socket, MyNodeHash, TxId),
    {ok, NewState0}.


%%
%%
%%
-spec get_bucket_and_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    State   :: #state{}
) -> false | {ok, Bucket :: #bucket{} | false, Node :: #node{}}.

get_bucket_and_node(Ip, Port, #state{not_assigned_nodes = NotAssignedNodes, buckets = Buckets}) ->
    case lists:keysearch({Ip, Port}, #node.ip_port, NotAssignedNodes) of
        {value, Node} -> {ok, false, Node};
        false         -> get_bucket_and_node(Ip, Port, Buckets)
    end;

get_bucket_and_node(_Ip, _Port, []) ->
    false;

get_bucket_and_node(Ip, Port, [Bucket = #bucket{nodes = Nodes} | Buckets]) ->
    case lists:keysearch({Ip, Port}, #node.ip_port, Nodes) of
        {value, Node} -> {ok, Bucket, Node};
        false         -> get_bucket_and_node(Ip, Port, Buckets)
    end.


%%  @private
%%  Increase node current transaction ID by 1.
%%
-spec update_transaction_id(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    NewActiveTx :: [active_tx()],
    State       :: #state{}
) -> State :: #state{}.

update_transaction_id(Ip, Port, NewActiveTx, State = #state{}) ->
    Params = [
        transaction_id,
        {active_transactions, NewActiveTx}
    ],
    update_node(Ip, Port, Params, State).


-spec update_transaction_id(
    Node :: #node{}
) -> Node :: #node{}.

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
-spec update_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Params  :: [{hash, binary()} |
                {last_changed, calendar:datetime()} |
                {ping_timer, reference()} |
                {active_transactions, [active_tx()]} |
                transaction_id |
                {status | status()}],
    State   :: #state{}
) -> State :: #state{}.

update_node(Ip, Port, Params, State = #state{not_assigned_nodes = NotAssignedNodes, buckets = Buckets}) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    UpdatedNode = lists:foldl(fun
        ({hash, Hash}, AccNode)                     -> AccNode#node{hash = Hash};
        ({last_changed, LastChanged}, AccNode)      -> AccNode#node{last_changed = LastChanged};
        ({ping_timer, PingTimerRef}, AccNode)       -> AccNode#node{ping_timer = PingTimerRef};
        ({active_transactions, ActiveTx}, AccNode)  -> AccNode#node{active_transactions = ActiveTx};
        (transaction_id, AccNode)                   -> AccNode#node{transaction_id = update_transaction_id(Node)};
        ({status, Status}, AccNode)                 -> AccNode#node{status = Status}
    end, Node, Params),
    case Bucket of
        #bucket{distance = Dist, nodes = Nodes} ->
            NewNodes = lists:keyreplace({Ip, Port}, #node.ip_port, Nodes, UpdatedNode),
            NewBucket = Bucket#bucket{nodes = NewNodes},
            NewBuckets = lists:keyreplace(Dist, #bucket.distance, Buckets, NewBucket),
            State#state{buckets = NewBuckets};
        false ->
            NewNodes = lists:keyreplace({Ip, Port}, #node.ip_port, NotAssignedNodes, UpdatedNode),
            State#state{not_assigned_nodes = NewNodes}
    end.


%%
%%
%%
-spec delete_node(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    State       :: #state{}
) -> State :: #state{}.

delete_node(Ip, Port, State = #state{not_assigned_nodes = NotAssignedNodes, buckets = Buckets}) ->
    {ok, Bucket, _Node} = get_bucket_and_node(Ip, Port, State),
    case Bucket of
        #bucket{distance = Dist, nodes = Nodes} ->
            NewNodes = lists:keydelete({Ip, Port}, #node.ip_port, Nodes),
            NewBucket = Bucket#bucket{nodes = NewNodes},
            NewBuckets = lists:keyreplace(Dist, #bucket.distance, Buckets, NewBucket),
            State#state{buckets = NewBuckets};
        false ->
            NewNodes = lists:keydelete({Ip, Port}, #node.ip_port, NotAssignedNodes),
            State#state{not_assigned_nodes = NewNodes}
    end.


%%
%%
%%
-spec cancel_timer(
    Node :: #node{}
) -> ok.

cancel_timer(#node{ping_timer = undefined}) ->
    ok;

cancel_timer(#node{ping_timer = PingTimer}) ->
    erlang:cancel_timer(PingTimer),
    ok.


%%  @doc
%%  @private
%%  5-14 min.
%%
-spec schedule_next_ping(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number()
) -> reference().

schedule_next_ping(Ip, Port) ->
    Time = crypto:rand_uniform(?NEXT_PING_LOW_TIME, ?NEXT_PING_HIGH_TIME),
    erlang:send_after(Time, self(), {ping, Ip, Port}).

%%
%%
-spec schedule_next_ping(
    Node :: #node{}
) -> reference() | undefined.

schedule_next_ping(#node{ip_port = {Ip, Port}, ping_timer = undefined}) ->
    Time = crypto:rand_uniform(?NEXT_PING_LOW_TIME, ?NEXT_PING_HIGH_TIME),
    erlang:send_after(Time, self(), {ping, Ip, Port});

schedule_next_ping(#node{}) ->
    undefined.


