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
    add_node/3,
    get_all_nodes_in_bucket/1,
    get_not_assigned_nodes/0,
    get_not_assigned_nodes/1,
    get_buckets_filling/0
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
-define(BUCKET_PING_LOW_TIME, 300000). % 6 min
-define(BUCKET_PING_HIGH_TIME, 840000). % 12 min
-define(BUCKET_CHECK_LOW_TIME, 60000). % 1 min
-define(BUCKET_CHECK_HIGH_TIME, 180000). % 3 min
-define(ACTIVE_TTL, 840). % 14 min
-define(SUSPICIOUS_TTL, 900). % 15 min (ACTIVE_TTL + 1 min)
-define(NOT_ASSIGNED_NODES_TABLE, 'erline_dht$not_assigned_nodes').

-type status()      :: suspicious | active | not_active.
-type request()     :: ping | find_node | get_peers | announce.
-type tx_id()       :: binary().
-type active_tx()   :: {request(), tx_id()}.
-type distance()    :: 1..160.

-record(node, {
    ip_port                             :: {inet:ip_address(), inet:port_number()},
    hash                                :: binary(),
    token                               :: binary(),
    last_changed                        :: calendar:datetime(),
    transaction_id      = <<0,0>>       :: tx_id(),
    active_transactions = []            :: [{request(), tx_id()}],
    status              = suspicious    :: status(),
    distance                            :: distance() % Denormalized field. Mapping: #node.distance = #bucket.distance.
}).

-record(bucket, {
    distance            :: distance(),
    check_timer         :: reference(),
    ping_timer          :: reference(),
    nodes       = []    :: [#node{}],
    buffer      = []    :: [#node{}]   % @todo implement
}).

-record(state, {
    my_node_hash                :: binary(),
    socket                      :: port(),
    k                           :: pos_integer(),
    buckets             = []    :: [#bucket{}]
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
    gen_server:cast(?SERVER, {add_node, Ip, Port, undefined}).


%%
%%
%%
add_node(Ip, Port, Hash) ->
    gen_server:cast(?SERVER, {add_node, Ip, Port, Hash}).


%%
%%
%%
get_all_nodes_in_bucket(Distance) ->
    gen_server:call(?SERVER, {get_all_nodes_in_bucket, Distance}).


%%
%%
%%
get_not_assigned_nodes() ->
    gen_server:call(?SERVER, {get_not_assigned_nodes, undefined}).


%%
%%
%%
get_not_assigned_nodes(Distance) ->
    gen_server:call(?SERVER, {get_not_assigned_nodes, Distance}).


%%
%%
%%
get_buckets_filling() ->
    gen_server:call(?SERVER, get_buckets_filling).



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
    Buckets = lists:foldl(fun (Distance, AccBuckets) ->
        NewBucket = #bucket{
            check_timer = schedule_bucket_check(Distance),
            ping_timer  = schedule_bucket_ping(Distance),
            distance    = Distance
        },
        [NewBucket | AccBuckets]
    end, [], lists:seq(0, erlang:bit_size(MyNodeHash))),
    ets:new(?NOT_ASSIGNED_NODES_TABLE, [set, named_table, {keypos, #node.ip_port}]),
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
handle_call({get_all_nodes_in_bucket, Distance}, _From, State = #state{buckets = Buckets}) ->
    Response = case lists:keysearch(Distance, #bucket.distance, Buckets) of
        {value, #bucket{nodes = Nodes}} ->
            lists:map(fun (Node) ->
                #node{
                    ip_port         = {Ip, Port},
                    hash            = Hash,
                    last_changed    = LastChanged,
                    status          = Status
                } = Node,
                #{
                    ip              => Ip,
                    port            => Port,
                    hash            => Hash,
                    status          => Status,
                    last_changed    => LastChanged
                }
            end, Nodes);
        false ->
            []
    end,
    {reply, Response, State};

handle_call({get_not_assigned_nodes, Distance}, _From, State = #state{}) ->
    Response = lists:filtermap(fun (Node) ->
        #node{
            ip_port         = {Ip, Port},
            hash            = Hash,
            last_changed    = LastChanged,
            distance        = NodeDist
        } = Node,
        Response = #{ip => Ip, port => Port, hash => Hash, last_changed => LastChanged, distance => NodeDist},
        case Distance =:= undefined orelse NodeDist =:= Distance of
            true  -> {true, Response};
            false -> false
        end
    end, ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{_ = '_'})),
    {reply, Response, State};

handle_call(get_buckets_filling, _From, State = #state{buckets = Buckets}) ->
    Response = lists:map(fun (#bucket{distance = Dist, nodes = Nodes}) ->
        #{distance => Dist, nodes => erlang:length(Nodes)}
    end, Buckets),
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
handle_cast({add_node, Ip, Port, Hash}, State = #state{my_node_hash = MyNodeHash}) ->
    NewState = case get_bucket_and_node(Ip, Port, State) of
        false ->
            Distance = case Hash of
                undefined ->
                    undefined;
                Hash ->
                    % @todo check if not over K * 100 nodes for Distance. Otherwise drop.
                    case erline_dht_helper:get_distance(MyNodeHash, Hash) of
                        {ok, Dist}       -> Dist;
                        {error, _Reason} -> undefined
                    end
            end,
            NewNode = #node{ip_port = {Ip, Port}, hash = Hash, distance = Distance},
            true = ets:insert(?NOT_ASSIGNED_NODES_TABLE, NewNode),
            {ok, NewState0} = do_ping_async(Ip, Port, State),
            NewState0;
        {ok, _Bucket, #node{}} ->
            State % @todo fire event: {error, already_added}
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
        {ok, Bucket, #node{active_transactions = ActiveTx}} ->
            case erline_dht_message:parse_krpc_response(Response, ActiveTx) of
                %
                % Handle ping query
                {ok, ping, q, NodeHash, GotTxId} ->
                    ok = erline_dht_message:respond_ping(Ip, Port, Socket, MyNodeHash, GotTxId),
                    ok = add_node(Ip, Port, NodeHash),
                    io:format("Got ping query from {~p, ~p}, tx=~p!~n", [Ip, Port, GotTxId]),
                    State;
                %
                % Handle ping response
                {ok, ping, r, NewNodeHash, NewActiveTx} ->
                    case erline_dht_helper:get_distance(MyNodeHash, NewNodeHash) of
                        {ok, NewDist} ->
                            Params = [
                                {hash,          NewNodeHash},
                                {last_changed,  calendar:local_time()},
                                {status,        active}
                            ],
                            case Bucket of
                                % Node is already assigned to the bucket
                                #bucket{distance = CurrDist} ->
                                    case CurrDist =:= NewDist of
                                        % If hash is the same, update node data
                                        true ->
                                            io:format("Dist is the same. Dist=~p~n", [CurrDist]),
                                            update_node(Ip, Port, Params ++ [{active_transactions, NewActiveTx}], State);
                                         % If hash is changed, move node to another bucket
                                        false -> % todo: maybe_clear_bucket
                                            io:format("Dist is changed. Dist=~p~n", [NewDist]),
                                            case maybe_clear_bucket(NewDist, State) of
                                                {true, NewState0}  -> update_node(Ip, Port, Params ++ [{assign, NewDist}, {active_transactions, NewActiveTx}], NewState0);
                                                {false, NewState0} -> update_node(Ip, Port, Params ++ [unassign, {active_transactions, NewActiveTx}], NewState0)
                                            end
                                    end;
                                % New node
                                false ->
                                    NewState0 = update_node(Ip, Port, [{active_transactions, NewActiveTx}], State),
%%                                    {ok, TargetHash} = erline_dht_helper:get_hash_of_distance(MyNodeHash, crypto:rand_uniform(1, erlang:bit_size(MyNodeHash))),
                                    {ok, NewState1} = do_find_node_async(Ip, Port, MyNodeHash, NewState0),
                                    io:format("New node=~p, Hash=~p, NewDist=~p~n", [{Ip, Port}, NewNodeHash, NewDist]),
                                    case maybe_clear_bucket(NewDist, NewState1) of
                                        {true, NewState2}  -> update_node(Ip, Port, Params ++ [{assign, NewDist}], NewState2);
                                        % No place in the bucket. Add to buffer
                                        {false, NewState2} -> update_node(Ip, Port, Params, NewState2) % @todo implement add to buffer
                                    end
                            end;
                        {error, _Reason} ->
                            Params = [
                                {hash,                NewNodeHash},
                                {last_changed,        calendar:local_time()},
                                {status,              not_active},
                                {active_transactions, NewActiveTx}
                            ],
                            update_node(Ip, Port, Params, State)
                    end;
                %
                % Handle find_node response
                {ok, find_node, r, Nodes, NewActiveTx} ->
                    ok = lists:foreach(fun (#{ip := FoundIp, port := FoundPort, hash := FoundedHash}) ->
                        % Can't assume that node we got is live so we need to ping it.
                        ok = add_node(FoundIp, FoundPort, FoundedHash)
                    end, Nodes),
                    Params = [
                        {last_changed,        calendar:local_time()},
                        {active_transactions, NewActiveTx}
                    ],
                    update_node(Ip, Port, Params, State);
                %
                % Handle announce_peer query
                {ok, announce_peer, q, _Data, GotTxId} ->
                    io:format("XXXXXXXXXXXXX Got announce_peer query from {~p, ~p}, tx=~p!~n", [Ip, Port, GotTxId]),
                    State;
                %
                % Handle errors
                {error, {krpc_error, Reason}} ->
                    io:format("krpc_error=~p~n", [Reason]),
                    State;
                {error, {non_existing_transaction, TxId}} ->
                    io:format("non_existing_transaction=~p | ~p~n", [{Ip, Port}, TxId]),
                    State;
                {error, {bad_query, _BadQuery}} ->
                    State;
                {error, {bad_response, BadResponse}} ->
                    io:format("BadResponse=~p~n", [BadResponse]),
                    State
            end;
        false ->
            % @todo add to bucket
            io:format("Node not found=~p~n", [{Ip, Port}]),
            State
    end,
    {noreply, NewState};

%
%
handle_info({bucket_check, Distance}, State = #state{buckets = CurrBuckets}) ->
    {value, #bucket{nodes = Nodes}} = lists:keysearch(Distance, #bucket.distance, CurrBuckets),
    {NewState0, BecomeNotActive} = lists:foldl(fun (Node, {CurrAccState, BecomeNotActiveAcc}) ->
        #node{ip_port = {Ip, Port}, last_changed = LastChanged, status = Status} = Node,
        case erline_dht_helper:datetime_diff(calendar:local_time(), LastChanged) of
            SecondsTillLastChanged when SecondsTillLastChanged > ?ACTIVE_TTL, Status =:= active ->
                io:format("Node {~p, ~p} in bucket ~p became suspicious.~n", [Ip, Port, Distance]),
                CurrAccState0 = update_node(Ip, Port, [{status, suspicious}], CurrAccState),
                % Try to ping once again
                {ok, NewAccState} = do_ping_async(Ip, Port, CurrAccState0),
                {NewAccState, BecomeNotActiveAcc};
            SecondsTillLastChanged when SecondsTillLastChanged > ?SUSPICIOUS_TTL, Status =:= suspicious ->
                io:format("Node {~p, ~p} in bucket ~p became not_active.~n", [Ip, Port, Distance]),
                {update_node(Ip, Port, [{status, not_active}], CurrAccState), BecomeNotActiveAcc + 1};
            _SecondsTillLastChanged ->
                {CurrAccState, BecomeNotActiveAcc}
        end
    end, {State, 0}, Nodes),
    NewState1 = update_bucket(Distance, [check_timer], NewState0),
    % Check if there are nodes which became not_active and try to replace them with nodes from not assigned nodes list
    NewState2 = case BecomeNotActive of
        0 ->
            NewState1;
        _ ->
            % @todo optimize
            lists:foldl(fun (#node{ip_port = {Ip, Port}}, CurrAccState) ->
                {ok, NewAccState} = do_ping_async(Ip, Port, CurrAccState),
                NewAccState
            end, NewState1, ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{distance = Distance, _ = '_'}))
    end,
    {noreply, NewState2};

%
%
handle_info({bucket_ping, Distance}, State = #state{buckets = Buckets}) ->
    io:format("Bucket (~p) ping.~n", [Distance]),
    {value, #bucket{nodes = Nodes}} = lists:keysearch(Distance, #bucket.distance, Buckets),
    NewState0 = lists:foldl(fun (#node{ip_port = {Ip, Port}}, CurrAccState) ->
        {ok, NewAccState} = do_ping_async(Ip, Port, CurrAccState),
        NewAccState
    end, State, Nodes),
    NewState1 = update_bucket(Distance, [ping_timer], NewState0),
    {noreply, NewState1}.

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
    Params = [
        transaction_id,
        {active_transactions, [{ping, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Ip, Port, Params, State),
    ok = erline_dht_helper:socket_active(Socket),
    ok = erline_dht_message:send_ping(Ip, Port, Socket, MyNodeHash, TxId),
    {ok, NewState}.


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
    Params = [
        transaction_id,
        {active_transactions, [{find_node, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Ip, Port, Params, State),
    ok = erline_dht_helper:socket_active(Socket),
    ok = erline_dht_message:send_find_node(Ip, Port, Socket, MyNodeHash, TxId, Target),
    {ok, NewState}.


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

get_bucket_and_node(Ip, Port, #state{buckets = Buckets}) ->
    case ets:match_object(?NOT_ASSIGNED_NODES_TABLE, #node{ip_port = {Ip, Port}, _ = '_'}) of
        [Node] -> {ok, false, Node};
        []     -> get_bucket_and_node(Ip, Port, Buckets)
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
                {active_transactions, [ActiveTx :: active_tx()]} |
                transaction_id |
                {status, Status :: status()} |
                {assign, Dist :: distance()} |
                unassign],
    State   :: #state{}
) -> State :: #state{}.

update_node(Ip, Port, Params, State = #state{my_node_hash = MyNodeHash, buckets = Buckets}) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    UpdatedNode = lists:foldl(fun
        ({hash, Hash}, AccNode) ->
            case erline_dht_helper:get_distance(MyNodeHash, Hash) of
                {ok, Distance}   -> AccNode#node{hash = Hash, distance = Distance};
                {error, _Reason} -> AccNode#node{hash = Hash}
            end;
        ({last_changed, LastChanged}, AccNode) ->
            AccNode#node{last_changed = LastChanged};
        ({active_transactions, ActiveTx}, AccNode) ->
            AccNode#node{active_transactions = ActiveTx};
        (transaction_id, AccNode) ->
            update_transaction_id(AccNode);
        ({status, Status}, AccNode) ->
            AccNode#node{status = Status};
        ({assign, _Dist}, AccNode) ->
            AccNode;
        (unassign, AccNode) ->
            AccNode
    end, Node, Params),
    AddNodeToBucketFun = fun (Dist) ->
        {value, NewBucket} = lists:keysearch(Dist, #bucket.distance, Buckets),
        #bucket{nodes = NewBucketNodes} = NewBucket,
        NewBucketUpdated = NewBucket#bucket{nodes = [UpdatedNode | NewBucketNodes]},
        lists:keyreplace(Dist, #bucket.distance, Buckets, NewBucketUpdated)
    end,
    case Bucket of
        CurrBucket = #bucket{distance = CurrDist, nodes = Nodes} ->
            % Check for `assign` param
            NewBuckets0 = case lists:keysearch(assign, 1, Params) of
                % Assign update node to the new bucket if there is assign param
                {value, {assign, NewDist}} ->
                    CurrBucketUpdated = CurrBucket#bucket{nodes = lists:keydelete({Ip, Port}, #node.ip_port, Nodes)},
                    lists:keyreplace(CurrDist, #bucket.distance, AddNodeToBucketFun(NewDist), CurrBucketUpdated);
                % Just put update node to the old bucket
                false ->
                    NewNodes0 = lists:keyreplace({Ip, Port}, #node.ip_port, Nodes, UpdatedNode),
                    lists:keyreplace(CurrDist, #bucket.distance, Buckets, Bucket#bucket{nodes = NewNodes0})
            end,
            % Check for `unassign` param
            NewBuckets1 = case lists:member(unassign, Params) of
                true ->
                    true = ets:insert(?NOT_ASSIGNED_NODES_TABLE, UpdatedNode),
                    NewNodes1 = lists:keydelete({Ip, Port}, #node.ip_port, Nodes),
                    lists:keyreplace(CurrDist, #bucket.distance, Buckets, Bucket#bucket{nodes = NewNodes1});
                false ->
                    NewBuckets0
            end,
            State#state{buckets = NewBuckets1};
        false ->
            case lists:keysearch(assign, 1, Params) of
                % Assign updated node to the bucket if there is assign param
                {value, {assign, NewDist}} ->
                    true = ets:match_delete(?NOT_ASSIGNED_NODES_TABLE, #node{ip_port = {Ip, Port}, _ = '_'}),
                    NewBuckets = AddNodeToBucketFun(NewDist),
                    State#state{buckets = NewBuckets};
                % Just put updated node in the not assigned nodes list
                false ->
                    true = ets:insert(?NOT_ASSIGNED_NODES_TABLE, UpdatedNode),
                    State
            end
    end.


%%
%%
%%
-spec update_bucket(
    Distance    :: distance(),
    Params  :: [check_timer |
                ping_timer |
                {nodes, [#node{}]}],
    State   :: #state{}
) -> State :: #state{}.

update_bucket(Distance, Params, State = #state{buckets = Buckets}) ->
    {value, Bucket} = lists:keysearch(Distance, #bucket.distance, Buckets),
    NewBucket = lists:foldl(fun
        (check_timer, AccBucket) ->
            AccBucket#bucket{check_timer = schedule_bucket_check(Distance)};
        (ping_timer, AccBucket) ->
            AccBucket#bucket{ping_timer = schedule_bucket_ping(Distance)};
        ({nodes, Nodes}, AccBucket) ->
            AccBucket#bucket{nodes = Nodes}
    end, Bucket, Params),
    State#state{buckets = lists:keyreplace(Distance, #bucket.distance, Buckets, NewBucket)}.

%%
%%
%%
-spec schedule_bucket_check(
    Distance :: distance()
) -> reference().

schedule_bucket_check(Distance) ->
    Time = crypto:rand_uniform(?BUCKET_CHECK_LOW_TIME, ?BUCKET_CHECK_HIGH_TIME),
    erlang:send_after(Time, self(), {bucket_check, Distance}).


%%
%%
%%
-spec schedule_bucket_ping(
    Distance :: distance()
) -> reference().

schedule_bucket_ping(Distance) ->
    Time = crypto:rand_uniform(?BUCKET_PING_LOW_TIME, ?BUCKET_PING_HIGH_TIME),
    erlang:send_after(Time, self(), {bucket_ping, Distance}).


%%
%%
%%
-spec maybe_clear_bucket(
    Distance :: distance(),
    State    :: #state{}
) -> {boolean(), State :: #state{}}.

maybe_clear_bucket(Distance, State = #state{k = K, buckets = Buckets}) ->
    {value, #bucket{nodes = Nodes}} = lists:keysearch(Distance, #bucket.distance, Buckets),
    case erlang:length(Nodes) < K of
        true  ->
            {true, State};
        false ->
            {_NotRemovable, Removable} = lists:partition(fun
                (#node{status = active})     -> true;
                (#node{status = suspicious}) -> true;
                (#node{status = not_active}) -> false
            end, Nodes),
            case Removable of
                [] ->
                    {false, State};
                [_|_] ->
                    % Find and remove node with oldest last_changed datetime
                    [#node{ip_port = {Ip, Port}} | _] = lists:sort(
                        fun (#node{last_changed = LastChanged1}, #node{last_changed = LastChanged2}) ->
                            LastChanged1 =< LastChanged2
                        end,
                        Removable
                    ),
                    NewState = update_node(Ip, Port, [unassign], State),
                    % Check once again
                    #state{buckets = NewBuckets} = NewState,
                    {value, #bucket{nodes = NewNodes}} = lists:keysearch(Distance, #bucket.distance, NewBuckets),
                    {(erlang:length(NewNodes) < K), NewState}
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


