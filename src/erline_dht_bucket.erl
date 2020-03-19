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
    add_node/2,
    get_all_nodes/1
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
-type distance()    :: 1..160.

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
    distance            :: distance(),
    nodes       = []    :: [#node{}],
    buffer      = []    :: [#node{}]   % @todo implement
}).

-record(state, {
    my_node_hash                :: binary(),
    socket                      :: port(),
    k                           :: pos_integer(),
    buckets             = []    :: [#bucket{}],
    not_assigned_nodes  = []    :: [#node{}],       % @todo move to ETS
    not_active_nodes    = []    :: [{inet:ip_address(), inet:port_number()}]    % @todo move to ETS
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


get_all_nodes(Distance) ->
    gen_server:call(?SERVER, {get_all_nodes, Distance}).


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
    Buckets = lists:foldl(fun (Dist, AccBuckets) ->
        [#bucket{distance = Dist} | AccBuckets]
    end, [], lists:seq(0, 160)), % @todo from zero?
    NewState = #state{
        socket       = Socket,
        k            = K,
        my_node_hash = MyNodeHash,
        buckets      = lists:reverse(Buckets)
    },
    {ok, NewState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({get_all_nodes, Distance}, _From, State = #state{buckets = Buckets}) ->
    Response = case lists:keysearch(Distance, #bucket.distance, Buckets) of
        {value, #bucket{nodes = Nodes}} ->
            lists:map(fun (Node) ->
                #node{
                    ip_port         = {Ip, Port},
                    hash            = Hash,
                    last_changed    = LastChanged,
                    status          = Status
                } = Node,
                #{ip => Ip, port => Port, hash => Hash, status => Status, last_changed => LastChanged}
            end, Nodes);
        false ->
            []
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
handle_cast({add_node, Ip, Port}, State = #state{not_assigned_nodes = NotAssignedNodes}) ->
    NewState = case maybe_buckets_full(State) of
        false ->
            case get_bucket_and_node(Ip, Port, State) of
                false ->
                    NewNotAssignedNodes = [#node{ip_port = {Ip, Port}} | NotAssignedNodes],
                    {ok, NewState0} = do_ping_async(Ip, Port, State#state{not_assigned_nodes = NewNotAssignedNodes}),
                    NewState0;
                {ok, _Bucket, #node{}} ->
                    State % @todo fire event: {error, already_added}
            end;
        true ->
            io:format("xxxxxx Buckets are full~n"),
            % @todo fire event: {error, buckets_are_full}
            State
    end,
    {noreply, NewState};

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
handle_info({udp, Socket, Ip, Port, Response}, State = #state{socket = Socket, my_node_hash = MyNodeHash}) ->
    NewState = case get_bucket_and_node(Ip, Port, State) of
        {ok, Bucket, Node = #node{active_transactions = ActiveTx}} ->
            case erline_dht_message:parse_krpc_response(Response, ActiveTx) of
                {ok, ping, NewNodeHash, NewActiveTx} ->
                    case erline_dht_helper:get_distance(MyNodeHash, NewNodeHash) of
                        {ok, NewDist} ->
                            Params = [
                                {hash,                NewNodeHash},
                                {last_changed,        calendar:local_time()},
                                {ping_timer,          schedule_next_ping(Node)},
                                {status,              active}
                            ],
                            case Bucket of
                                % Node is already assigned to the bucket
                                #bucket{distance = CurrDist} ->
                                    case CurrDist =:= NewDist of
                                        % If hash is the same, update node data
                                        true ->
                                            io:format("xxxxxxxx dist is the same. Dist=~p~n", [CurrDist]),
                                            update_node(Ip, Port, Params ++ [{active_transactions, NewActiveTx}], State);
                                         % If hash is changed, move node to another bucket
                                        false ->
                                            io:format("xxxxxxxx dist is changed. Dist=~p~n", [NewDist]),
                                            update_node(Ip, Port, Params ++ [{assign, NewDist}, {active_transactions, NewActiveTx}], State)
                                    end;
                                % New node
                                false ->
                                    {ok, NewState0} = do_find_node_async(Ip, Port, MyNodeHash, State),
                                    io:format("xxxxxx new node=~p, NewDist=~p~n", [{Ip, Port}, NewDist]),
                                    case maybe_clear_bucket(NewDist, NewState0) of
                                        {ok, NewState1} ->
                                            update_node(Ip, Port, Params ++ [{assign, NewDist}], NewState1);
                                        % No place in the bucket. Add to buffer
                                        false ->
                                            NewState0 % @todo implement add to buffer
                                    end
                            end;
                        {error, {different_hash_length, _, _}} ->
                            % @todo delete node
                            io:format("xxxxx Diff hash~n"),
                            State
                    end;
                {ok, find_node, Nodes, NewActiveTx} ->
                    ok = lists:foreach(fun (#{ip := FoundIp, port := FoundPort}) ->
                        % Can't assume that node we got is live so we need to ping it.
                        ok = add_node(FoundIp, FoundPort)
                    end, Nodes),
                    io:format("xxxxxxx Find node resp ~p~n", [NewActiveTx]),
                    Params = [
                        {last_changed,        calendar:local_time()},
                        {active_transactions, NewActiveTx}
                    ],
                    update_node(Ip, Port, Params, State);
                {error, {krpc_error, Reason}} ->
                    io:format("xxxxxx krpc_error=~p~n", [Reason]),
                    State;
                {error, {non_existing_transaction, TxId}} ->
                    io:format("xxxxxx non_existing_transaction=~p | ~p~n", [{Ip, Port}, TxId]),
                    State;
                {error, not_implemented} ->
                    % @todo temporary. Remove later.
                    State;
                {error, {bad_response, BadResponse}} ->
                    io:format("xxxxxx BadResponse=~p~n", [BadResponse]),
                    State
            end;
        false ->
            io:format("xxxxxx Node not found=~p~n", [{Ip, Port}]),
            State
    end,
    {noreply, NewState}.

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
%%% Request functions
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
-spec do_find_node_async(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Target  :: binary(),
    State   :: #state{}
) -> {ok, State :: #state{}}.

do_find_node_async(Ip, Port, Target, State = #state{socket = Socket, my_node_hash = MyNodeHash}) ->
    {ok, _Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    #node{transaction_id = TxId, active_transactions = CurrActiveTx} = Node,
    NewState0 = update_transaction_id(Ip, Port, [{find_node, TxId} | CurrActiveTx], State),
    ok = erline_dht_helper:socket_active(Socket),
    ok = erline_dht_message:send_find_node(Ip, Port, Socket, MyNodeHash, TxId, Target),
    {ok, NewState0}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    Params  :: [{hash, Hash :: binary()} |
                {last_changed, LastChanged :: calendar:datetime()} |
                {ping_timer, PingTimerRef :: reference()} |
                {active_transactions, [ActiveTx :: active_tx()]} |
                transaction_id |
                {status, Status :: status()} |
                {assign, Dist :: distance()}],
    State   :: #state{}
) -> State :: #state{}.

update_node(Ip, Port, Params, State = #state{not_assigned_nodes = NotAssignedNodes, buckets = Buckets}) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    UpdatedNode = lists:foldl(fun
        ({hash, Hash}, AccNode)                     -> AccNode#node{hash = Hash};
        ({last_changed, LastChanged}, AccNode)      -> AccNode#node{last_changed = LastChanged};
        ({ping_timer, PingTimerRef}, AccNode)       -> AccNode#node{ping_timer = PingTimerRef};
        ({active_transactions, ActiveTx}, AccNode)  -> AccNode#node{active_transactions = ActiveTx};
        (transaction_id, AccNode)                   -> update_transaction_id(AccNode);
        ({status, Status}, AccNode)                 -> AccNode#node{status = Status};
        ({assign, _Dist}, AccNode)                  -> AccNode
    end, Node, Params),
    AddNodeToBucketFun = fun (Dist) ->
        {value, NewBucket} = lists:keysearch(Dist, #bucket.distance, Buckets),
        #bucket{nodes = NewBucketNodes} = NewBucket,
        NewBucketUpdated = NewBucket#bucket{nodes = [UpdatedNode | NewBucketNodes]},
        lists:keyreplace(Dist, #bucket.distance, Buckets, NewBucketUpdated)
    end,
    case Bucket of
        CurrBucket = #bucket{distance = CurrDist, nodes = Nodes} ->
            NewBuckets = case lists:keysearch(assign, 1, Params) of
                % Assign update node to the new bucket if there is assign param
                {value, {assign, NewDist}} ->
                    CurrBucketUpdated = CurrBucket#bucket{nodes = lists:keydelete({Ip, Port}, #node.ip_port, Nodes)},
                    NewBuckets0 = AddNodeToBucketFun(NewDist),
                    lists:keyreplace(CurrDist, #bucket.distance, NewBuckets0, CurrBucketUpdated);
                % Just put update node to the old bucket
                false ->
                    NewNodes = lists:keyreplace({Ip, Port}, #node.ip_port, Nodes, UpdatedNode),
                    NewBucket = Bucket#bucket{nodes = NewNodes},
                    lists:keyreplace(CurrDist, #bucket.distance, Buckets, NewBucket)
            end,
            State#state{buckets = NewBuckets};
        false ->
            case lists:keysearch(assign, 1, Params) of
                % Assign updated node to the bucket if there is assign param
                {value, {assign, NewDist}} ->
                    NewNotAssignedNodes = lists:keydelete({Ip, Port}, #node.ip_port, NotAssignedNodes),
                    NewBuckets = AddNodeToBucketFun(NewDist),
                    State#state{not_assigned_nodes = NewNotAssignedNodes, buckets = NewBuckets};
                % Just put updated node the not assigned nodes list
                false ->
                    NewNodes = lists:keyreplace({Ip, Port}, #node.ip_port, NotAssignedNodes, UpdatedNode),
                    State#state{not_assigned_nodes = NewNodes}
            end
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
    Node :: #node{}
) -> reference() | undefined.

schedule_next_ping(#node{ip_port = {Ip, Port}, ping_timer = undefined}) ->
    Time = crypto:rand_uniform(?NEXT_PING_LOW_TIME, ?NEXT_PING_HIGH_TIME),
    erlang:send_after(Time, self(), {ping, Ip, Port});

schedule_next_ping(#node{}) ->
    undefined.


%%
%%
%%
-spec maybe_clear_bucket(
    Distance :: distance(),
    State    :: #state{}
) -> {ok, State :: #state{}} | false.

maybe_clear_bucket(Distance, State = #state{k = K, buckets = Buckets}) ->
    {value, Bucket = #bucket{nodes = Nodes}} = lists:keysearch(Distance, #bucket.distance, Buckets),
    case erlang:length(Nodes) < K of
        true  ->
            {ok, State};
        false ->
            {NotRemovable, Removable} = lists:splitwith(fun
                (#node{status = active})     -> true;
                (#node{status = unknown})    -> true;
                (#node{status = not_active}) -> false
            end, Nodes),
            case erlang:length(Removable) of
                0 ->
                    false;
                _ ->
                    [RemovedNode | RemovableLeft] = lists:sort(
                        fun (#node{last_changed = LastChanged1}, #node{last_changed = LastChanged2}) ->
                            LastChanged1 =< LastChanged2
                        end,
                        Removable
                    ),
                    ok = cancel_timer(RemovedNode),
                    NewBucket = Bucket#bucket{nodes = NotRemovable ++ RemovableLeft},
                    NewBuckets = lists:keyreplace(Distance, #bucket.distance, Buckets, NewBucket),
                    {ok, State#state{buckets = NewBuckets}}
            end
    end.


%%
%%
%%
-spec maybe_buckets_full(
    State    :: #state{}
) -> boolean().

maybe_buckets_full(#state{k = K, buckets = Buckets}) -> maybe_buckets_full(Buckets, K).
maybe_buckets_full([], _) -> true;
maybe_buckets_full([#bucket{nodes = Nodes} | _], K) when length(Nodes) /= K -> false;
maybe_buckets_full([_ | Buckets], K) -> maybe_buckets_full(Buckets, K).


