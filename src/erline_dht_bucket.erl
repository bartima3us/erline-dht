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
-include("erline_dht.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    add_node/2,
    add_node/3,
    get_peers/1,
    get_peers/3,
    get_port/0,
    get_event_mgr_pid/0,
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

-ifdef(TEST).
-export([
    update_transaction_id/1,
    clear_peers_searches/1,
    update_bucket_nodes_status/2,
    init_not_active_nodes_replacement/2,
    clear_not_assigned_nodes/1,
    clear_not_assigned_nodes/2
]).
-endif.

-define(SERVER, ?MODULE).
-define(CHECK_NODE_TIMEOUT, 60000).
-define(BUCKET_PING_LOW_TIME, 300000). % 6 min
-define(BUCKET_PING_HIGH_TIME, 840000). % 12 min
-define(BUCKET_CHECK_LOW_TIME, 60000). % 1 min
-define(BUCKET_CHECK_HIGH_TIME, 180000). % 3 min
-define(GET_PEERS_SEARCH_CHECK_TIME, 60000). % 1 min
-define(CLEAR_NOT_ASSIGNED_NODES_TIME, 60000). % 2 min
-define(NOT_ACTIVE_NOT_ASSIGNED_NODES_TTL, 90). % 90 s
-define(ACTIVE_NOT_ASSIGNED_NODES_TTL, 300). % 5 min
-define(ACTIVE_TTL, 840). % 14 min
-define(GET_PEERS_SEARCH_TTL, 120). % 2 min
-define(SUSPICIOUS_TTL, 900). % 15 min (ACTIVE_TTL + 1 min)

-record(bucket, {
    distance            :: distance(),
    check_timer         :: reference(),
    ping_timer          :: reference(),
    nodes       = []    :: [#node{}]
}).

-record(info_hash, {
    info_hash           :: binary(),
    peers       = []    :: [{inet:ip_address(), inet:port_number()}]
}).

-record(state, {
    my_node_hash                        :: binary(),
    socket                              :: port(),
    k                                   :: pos_integer(),
    buckets                     = []    :: [#bucket{}],
    info_hashes                 = []    :: [#info_hash{}],  % @todo persist?
    get_peers_searches_timer            :: reference(),
    clear_not_assigned_nodes_timer      :: reference(),
    db_mod                              :: module(),
    event_mgr_pid                       :: pid(),
    not_assigned_clearing_threshold     :: pos_integer() % Not assigned nodes
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link(
    MyNodeHash :: binary()
) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).

start_link(MyNodeHash) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [MyNodeHash], []).


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
get_peers(InfoHash) ->
    gen_server:cast(?SERVER, {get_peers, undefined, undefined, InfoHash}).


%%
%%
%%
get_peers(Ip, Port, InfoHash) ->
    gen_server:cast(?SERVER, {get_peers, Ip, Port, InfoHash}).


%%  @doc
%%  Returns UDP port of the client.
%%  @end
-spec get_port() -> inet:port_number().

get_port() ->
    gen_server:call(?SERVER, get_port).


%%  @doc
%%  Returns event manager pid.
%%  @end
-spec get_event_mgr_pid() -> pid().

get_event_mgr_pid() ->
    gen_server:call(?SERVER, get_event_mgr_pid).


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
init([MyNodeHash]) ->
    SocketParams = [binary, {active, true}],
    {ok, Socket} = case gen_udp:open(erline_dht:get_env(port, 0), SocketParams) of
        {ok, SockPort}      -> {ok, SockPort};
        {error, eaddrinuse} -> gen_udp:open(0, SocketParams)
    end,
    Buckets = lists:foldl(fun (Distance, AccBuckets) ->
        NewBucket = #bucket{
            check_timer = schedule_bucket_check(Distance),
            ping_timer  = schedule_bucket_ping(Distance),
            distance    = Distance
        },
        [NewBucket | AccBuckets]
    end, [], lists:seq(0, erlang:bit_size(MyNodeHash))),
    DbMod = erline_dht:get_env(db_mod, erline_dht_db_ets),
    ok = DbMod:init(),
    {ok, EventMgrPid} = gen_event:start_link(),
    ClearNotAssignedRef = case erline_dht:get_env(limit_nodes, true) of
        true  -> schedule_clear_not_assigned_nodes();
        false -> undefined
    end,
    K = erline_dht:get_env(k, 8),
    NewState = #state{
        socket                          = Socket,
        k                               = K,
        my_node_hash                    = MyNodeHash,
        buckets                         = lists:reverse(Buckets),
        get_peers_searches_timer        = schedule_get_peers_searches_check(),
        clear_not_assigned_nodes_timer  = ClearNotAssignedRef,
        db_mod                          = DbMod,
        event_mgr_pid                   = EventMgrPid,
        not_assigned_clearing_threshold = K * 100
    },
    % Bootstrap
    AddBootstrapNodeFun = fun
        (Address, Port) when is_list(Address) ->
            ok = case inet:getaddr(Address, inet) of
                {ok, Ip} -> add_node(Ip, Port);
                _ -> ok
            end;
        (Ip, Port) when is_tuple(Ip) ->
            ok = add_node(Ip, Port)
    end,
    ok = lists:foreach(fun ({AutoBootstrapNode, Port}) ->
        ok = AddBootstrapNodeFun(AutoBootstrapNode, Port)
    end, erline_dht:get_env(auto_bootstrap_nodes, [])),
    {ok, NewState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(get_port, _From, State = #state{socket = Socket}) ->
    {ok, Port} = inet:port(Socket),
    {reply, Port, State};

handle_call(get_event_mgr_pid, _From, State = #state{event_mgr_pid = EventMgrPid}) ->
    {reply, EventMgrPid, State};

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

handle_call({get_not_assigned_nodes, Distance}, _From, State = #state{db_mod = DbMod}) ->
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
    end, DbMod:get_not_assigned_nodes()),
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
handle_cast({add_node, Ip, Port, Hash}, State = #state{}) ->
    #state{
        my_node_hash  = MyNodeHash,
        db_mod        = DbMod
    } = State,
    NewState = case get_bucket_and_node(Ip, Port, State) of
        false ->
            Distance = case Hash of
                undefined ->
                    undefined;
                Hash ->
                    case erline_dht_helper:get_distance(MyNodeHash, Hash) of
                        {ok, Dist}       -> Dist;
                        {error, _Reason} -> undefined
                    end
            end,
            NewNode = #node{
                ip_port      = {Ip, Port},
                hash         = Hash,
                distance     = Distance,
                last_changed = calendar:local_time()
            },
            true = DbMod:insert_to_not_assigned_nodes(NewNode),
            {ok, NewState0} = do_ping_async(Ip, Port, State),
            NewState0;
        {ok, _Bucket, #node{}} ->
            State
    end,
    {noreply, NewState};

%
%
handle_cast({get_peers, Ip, Port, InfoHash}, State = #state{}) ->
    #state{
        buckets       = Buckets,
        db_mod        = DbMod,
        event_mgr_pid = EventMgrPid
    } = State,
    NodesForSearch = case {Ip, Port} of
        {undefined, undefined} ->
            % First check in the local cache for a peer
            LocalPeers = find_local_peers_by_info_hash(InfoHash, State),
            ok = gen_event:notify(EventMgrPid, {peers, InfoHash, LocalPeers}),
            io:format("xxxxxx LocalPeers = ~p~n", [LocalPeers]),
            % Flatten all nodes in all buckets
            lists:foldl(fun (#bucket{nodes = Nodes}, NodesAcc) ->
                NodesAcc ++ Nodes
            end, [], Buckets);
        _ ->
            % ...or take particular node from state
            {ok, _Bucket, Node} = get_bucket_and_node(Ip, Port, State),
            [Node]
    end,
    % Make get_peers requests
    NewState = lists:foldl(fun (#node{ip_port = {RequestedIp, RequestedPort}}, AccState) ->
        {ok, TxId, NewState0} = do_get_peers_async(RequestedIp, RequestedPort, InfoHash, AccState),
        GetPeersSearch = #get_peers_search{
            info_hash       = InfoHash,
            last_changed    = calendar:local_time()
        },
        RequestedNode = #requested_node{
            ip_port         = {RequestedIp, RequestedPort},
            transaction_id  = TxId,
            info_hash       = InfoHash
        },
        true = DbMod:insert_to_get_peers_searches(GetPeersSearch),
        true = DbMod:insert_to_requested_nodes(RequestedNode),
        NewState0
    end, State, NodesForSearch),
    {noreply, NewState};

%
%
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
handle_info({udp, Socket, Ip, Port, Response}, State) ->
    #state{
        socket        = Socket,
        my_node_hash  = MyNodeHash,
        db_mod        = DbMod,
        event_mgr_pid = EventMgrPid
    } = State,
    {Bucket, ActiveTx} = case get_bucket_and_node(Ip, Port, State) of
        {ok, NodeBucket, #node{active_transactions = NodeActiveTx}} ->
            {NodeBucket, NodeActiveTx};
        false ->
            ok = add_node(Ip, Port),
            {false, []}
    end,
    NewState = case erline_dht_message:parse_krpc_response(Response, ActiveTx) of
        %
        % Handle ping query
        {ok, ping, q, NodeHash, GotTxId} ->
            ok = erline_dht_message:respond_ping(Ip, Port, Socket, MyNodeHash, GotTxId),
            ok = add_node(Ip, Port, NodeHash),
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
                                    update_node(Ip, Port, Params ++ [{active_transactions, NewActiveTx}], State);
                                 % If hash is changed, move node to another bucket
                                false ->
                                    case maybe_clear_bucket(NewDist, State) of
                                        {true, NewState0}  -> update_node(Ip, Port, Params ++ [{assign, NewDist}, {active_transactions, NewActiveTx}], NewState0);
                                        {false, NewState0} -> update_node(Ip, Port, Params ++ [unassign, {active_transactions, NewActiveTx}], NewState0)
                                    end
                            end;
                        % New node
                        false ->
                            NewState0 = update_node(Ip, Port, [{active_transactions, NewActiveTx}], State),
                            %{ok, TargetHash} = erline_dht_helper:get_hash_of_distance(MyNodeHash, crypto:rand_uniform(1, erlang:bit_size(MyNodeHash))),
                            {ok, NewState1} = do_find_node_async(Ip, Port, MyNodeHash, NewState0),
                            case maybe_clear_bucket(NewDist, NewState1) of
                                {true, NewState2}  -> update_node(Ip, Port, Params ++ [{assign, NewDist}], NewState2);
                                % No place in the bucket.
                                {false, NewState2} -> update_node(Ip, Port, Params, NewState2)
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
        % @todo implement
        % Handle find_node query
        {ok, find_node, q, _, _} ->
            State;
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
        % @todo implement
        % Handle find_node query
        {ok, get_peers, q, _, _} ->
            State;
        %
        % Handle get_peers response
        {ok, get_peers, r, {What, TxId, NodesOrValues, Token}, NewActiveTx} ->
            % Update last changed
            NewState0 = case DbMod:get_info_hash(Ip, Port, TxId) of
                false ->
                    ok = add_node(Ip, Port),
                    State;
                InfoHash ->
                    true = DbMod:insert_to_get_peers_searches(#get_peers_search{info_hash = InfoHash, last_changed = calendar:local_time()}),
                    case What of
                        % Continue search
                        nodes ->
                            ok = lists:foreach(fun (#{ip := FoundIp, port := FoundPort, hash := FoundedHash}) ->
                                case DbMod:get_requested_node(FoundIp, FoundPort, InfoHash) of
                                    [_|_]  ->
                                        ok;
                                    [] ->
                                        ok = add_node(FoundIp, FoundPort, FoundedHash),
                                        ok = get_peers(FoundIp, FoundPort, InfoHash)
                                end
                            end, NodesOrValues),
                            State;
                        % Stop search and save info hashes
                        values ->
                            io:format("GOT VALUES. Token=~p Vals=~p~n", [Token, NodesOrValues]),
                            ok = gen_event:notify(EventMgrPid, {peers, InfoHash, NodesOrValues}),
                            lists:foldl(fun (#{ip := FoundIp, port := FoundPort}, StateAcc) ->
                                ok = add_node(FoundIp, FoundPort),
                                add_peer(InfoHash, FoundIp, FoundPort, StateAcc)
                            end, State, NodesOrValues)
                    end
            end,
            Params = [
                {token,               Token},
                {last_changed,        calendar:local_time()},
                {active_transactions, NewActiveTx}
            ],
            update_node(Ip, Port, Params, NewState0);
        % @todo implement
        % Handle announce_peer query
        {ok, announce_peer, q, _Data, _GotTxId} ->
            update_node(Ip, Port, [{last_changed, calendar:local_time()}], State);
        % @todo implement
        % Handle announce_peer response
        {ok, announce_peer, r, _, _} ->
            State;
        %
        % Handle errors
        {error, {krpc_error, _Reason}, NewActiveTx} ->
            Params = [
                {last_changed,        calendar:local_time()},
                {active_transactions, NewActiveTx}
            ],
            update_node(Ip, Port, Params, State);
        {error, {bad_type, _BadType}, NewActiveTx} ->
            Params = [
                {last_changed,        calendar:local_time()},
                {active_transactions, NewActiveTx}
            ],
            update_node(Ip, Port, Params, State);
        {error, {non_existing_tx, _TxId}} ->
            State;
        {error, {bad_query, _BadQuery}} ->
            State;
        {error, {bad_response, _BadResponse}} ->
            State
    end,
    {noreply, NewState};

%
%
handle_info({bucket_check, Distance}, State = #state{}) ->
    % Check if there are nodes which became not_active and try to replace them with K * 10 freshest nodes from not assigned nodes list
    NewState1 = case update_bucket_nodes_status(Distance, State) of
        {NewState0, true}  -> init_not_active_nodes_replacement(Distance, NewState0);
        {NewState0, false} -> NewState0
    end,
    NewState = update_bucket(Distance, [check_timer], NewState1),
    {noreply, NewState};

%
%
handle_info({bucket_ping, Distance}, State = #state{buckets = Buckets}) ->
    {value, #bucket{nodes = Nodes}} = lists:keysearch(Distance, #bucket.distance, Buckets),
    NewState0 = lists:foldl(fun (#node{ip_port = {Ip, Port}}, CurrAccState) ->
        {ok, NewAccState} = do_ping_async(Ip, Port, CurrAccState),
        NewAccState
    end, State, Nodes),
    NewState1 = update_bucket(Distance, [ping_timer], NewState0),
    {noreply, NewState1};

%
%
handle_info(get_peers_searches_check, State = #state{}) ->
    ok = clear_peers_searches(State),
    NewState = State#state{get_peers_searches_timer = schedule_get_peers_searches_check()},
    {noreply, NewState};

%
%
handle_info(clear_not_assigned_nodes, State = #state{my_node_hash = MyNodeHash}) ->
    % Delete without distance
    erlang:spawn(fun () ->
        ok = clear_not_assigned_nodes(State)
    end),
    % Delete with distance
    ok = lists:foreach(fun (Distance) ->
        erlang:spawn(fun () ->
            ok = clear_not_assigned_nodes(Distance, State)
        end)
    end, lists:seq(0, erlang:bit_size(MyNodeHash))),
    NewState = State#state{clear_not_assigned_nodes_timer = schedule_clear_not_assigned_nodes()},
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
%%  @todo test
%%
-spec do_ping_async(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    State   :: #state{}
) -> {ok, NewState :: #state{}}.

do_ping_async(Ip, Port, State = #state{}) when is_tuple(Ip), is_integer(Port) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    do_ping_async(Bucket, Node, State);

do_ping_async(Bucket, Node = #node{ip_port = {Ip, Port}}, State = #state{}) ->
    #state{my_node_hash = MyNodeHash, socket = Socket} = State,
    #node{transaction_id = TxId, active_transactions = CurrActiveTx} = Node,
    Params = [
        transaction_id,
        {active_transactions, [{ping, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Bucket, Node, Params, State),
    ok = erline_dht_message:send_ping(Ip, Port, Socket, MyNodeHash, TxId),
    {ok, NewState}.


%%
%%  @todo test
%%
-spec do_find_node_async(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Target  :: binary(),
    State   :: #state{}
) -> {ok, NewState :: #state{}}.

do_find_node_async(Ip, Port, Target, State = #state{}) when is_tuple(Ip), is_integer(Port) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    do_find_node_async(Bucket, Node, Target, State);

do_find_node_async(Bucket, Node = #node{ip_port = {Ip, Port}}, Target, State = #state{}) ->
    #state{my_node_hash = MyNodeHash, socket = Socket} = State,
    #node{transaction_id = TxId, active_transactions = CurrActiveTx} = Node,
    Params = [
        transaction_id,
        {active_transactions, [{find_node, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Bucket, Node, Params, State),
    ok = erline_dht_message:send_find_node(Ip, Port, Socket, MyNodeHash, TxId, Target),
    {ok, NewState}.


%%
%%  @todo test
%%
-spec do_get_peers_async(
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number(),
    InfoHash :: binary(),
    State    :: #state{}
) -> {ok, TxId :: tx_id(), NewState :: #state{}}.

do_get_peers_async(Ip, Port, InfoHash, State = #state{}) when is_tuple(Ip), is_integer(Port) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    do_get_peers_async(Bucket, Node, InfoHash, State);

do_get_peers_async(Bucket, Node = #node{ip_port = {Ip, Port}}, InfoHash, State = #state{}) ->
    #state{my_node_hash = MyNodeHash, socket = Socket} = State,
    #node{transaction_id = TxId, active_transactions = CurrActiveTx} = Node,
    Params = [
        transaction_id,
        {active_transactions, [{get_peers, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Bucket, Node, Params, State),
    ok = erline_dht_message:send_get_peers(Ip, Port, Socket, MyNodeHash, TxId, InfoHash),
    {ok, TxId, NewState}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%
%%  @todo test
%%
-spec get_bucket_and_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    State   :: #state{}
) -> false | {ok, Bucket :: #bucket{} | false, Node :: #node{}}.

get_bucket_and_node(Ip, Port, #state{buckets = Buckets, db_mod = DbMod}) ->
    case DbMod:get_not_assigned_node(Ip, Port) of
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
) -> NewNode :: #node{}.

update_transaction_id(Node = #node{transaction_id = undefined}) ->
    Node#node{transaction_id = <<0,0>>};

update_transaction_id(Node = #node{transaction_id = <<255,255>>}) ->
    Node#node{transaction_id = <<0,0>>};

update_transaction_id(Node = #node{transaction_id = LastTransactionIdBin}) ->
    <<LastTransactionIdInt:16>> = LastTransactionIdBin,
    NewTransactionIdInt = LastTransactionIdInt + 1,
    Node#node{transaction_id = <<NewTransactionIdInt:16>>}.


%%
%%  @todo test
%%
-spec update_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Params  :: [{hash, Hash :: binary()} |
                {token, Token :: binary()} |
                {last_changed, LastChanged :: calendar:datetime()} |
                {active_transactions, [ActiveTx :: active_tx()]} |
                transaction_id |
                {status, Status :: status()} |
                {assign, Dist :: distance()} |
                unassign],
    State   :: #state{}
) -> NewState :: #state{}.

update_node(Ip, Port, Params, State = #state{}) when is_tuple(Ip), is_integer(Port) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    update_node(Bucket, Node, Params, State);

update_node(Bucket, Node = #node{ip_port = {Ip, Port}}, Params, State = #state{}) ->
    #state{
        my_node_hash = MyNodeHash,
        buckets      = Buckets,
        db_mod       = DbMod
    } = State,
    UpdatedNode = lists:foldl(fun
        ({hash, Hash}, AccNode) ->
            case erline_dht_helper:get_distance(MyNodeHash, Hash) of
                {ok, Distance}   -> AccNode#node{hash = Hash, distance = Distance};
                {error, _Reason} -> AccNode#node{hash = Hash}
            end;
        ({token, Token}, AccNode) ->
            AccNode#node{token = Token};
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
                    true = DbMod:insert_to_not_assigned_nodes(UpdatedNode),
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
                    true = DbMod:delete_from_not_assigned_nodes_by_ip_port(Ip, Port),
                    NewBuckets = AddNodeToBucketFun(NewDist),
                    State#state{buckets = NewBuckets};
                % Just put updated node in the not assigned nodes list
                false ->
                    true = DbMod:insert_to_not_assigned_nodes(UpdatedNode),
                    State
            end
    end.


%%
%%  @todo test
%%
-spec update_bucket(
    Distance    :: distance(),
    Params  :: [check_timer |
                ping_timer |
                {nodes, [#node{}]}],
    State   :: #state{}
) -> NewState :: #state{}.

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
%%  @todo test
%%
-spec schedule_bucket_check(
    Distance :: distance()
) -> reference().

schedule_bucket_check(Distance) ->
    Time = crypto:rand_uniform(?BUCKET_CHECK_LOW_TIME, ?BUCKET_CHECK_HIGH_TIME),
    erlang:send_after(Time, self(), {bucket_check, Distance}).


%%
%%  @todo test
%%
-spec schedule_bucket_ping(
    Distance :: distance()
) -> reference().

schedule_bucket_ping(Distance) ->
    Time = crypto:rand_uniform(?BUCKET_PING_LOW_TIME, ?BUCKET_PING_HIGH_TIME),
    erlang:send_after(Time, self(), {bucket_ping, Distance}).


%%
%%  @todo test
%%
-spec schedule_get_peers_searches_check() -> reference().

schedule_get_peers_searches_check() ->
    erlang:send_after(?GET_PEERS_SEARCH_CHECK_TIME, self(), get_peers_searches_check).


%%
%%  @todo test
%%
-spec schedule_clear_not_assigned_nodes() -> reference().

schedule_clear_not_assigned_nodes() ->
    erlang:send_after(?CLEAR_NOT_ASSIGNED_NODES_TIME, self(), clear_not_assigned_nodes).


%%
%%  @todo test
%%
-spec maybe_clear_bucket(
    Distance :: distance(),
    State    :: #state{}
) -> {boolean(), NewState :: #state{}}.

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
%%find_n_closest_nodes(Hash, N, State = #state{buckets = Buckets}) ->
%%    find_n_closest_nodes(Hash, N, Buckets, []).
%%
%%find_n_closest_nodes(_Hash, N, Buckets, Result) when N =:= 0; Buckets =:= [] ->
%%    Result.

%%find_n_closest_nodes(Hash, N, Buckets, Result)


%%
%%  @todo test
%%
-spec find_local_peers_by_info_hash(
    InfoHash :: binary(),
    State    :: #state{}
) -> [#{ip => inet:ip_address(), port => inet:port_number()}].

find_local_peers_by_info_hash(InfoHash, #state{info_hashes = InfoHashes}) ->
    case lists:keysearch(InfoHash, #info_hash.info_hash, InfoHashes) of
        {value, #info_hash{peers = Peers}} ->
            lists:map(fun ({Ip, Port}) ->
                #{ip => Ip, port => Port}
            end, Peers);
        false ->
            []
    end.


%%
%%  @todo test
%%
-spec add_peer(
    InfoHashBin :: binary(),
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    State       :: #state{}
) -> NewState   :: #state{}.

add_peer(InfoHashBin, Ip, Port, State = #state{info_hashes = InfoHashes}) ->
    case lists:keysearch(InfoHashBin, #info_hash.info_hash, InfoHashes) of
        {value, InfoHash = #info_hash{peers = Peers}} ->
            case lists:member({Ip, Port}, Peers) of
                true  ->
                    State;
                false ->
                    NewInfoHash = InfoHash#info_hash{peers = [{Ip, Port} | Peers]},
                    State#state{info_hashes = lists:keyreplace(InfoHashBin, #info_hash.info_hash, InfoHashes, NewInfoHash)}
            end;
        false ->
            NewInfoHash = #info_hash{info_hash = InfoHashBin, peers = [{Ip, Port}]},
            State#state{info_hashes = [NewInfoHash | InfoHashes]}
    end.


%%%%
%%%%
%%%%
%%-spec is_enough_space_for_node(
%%    Node    :: #node{},
%%    State   :: #state{}
%%) -> {boolean(), boolean()}.
%%
%%is_enough_space_for_node(#node{distance = Distance}, State = #state{}) ->
%%    #state{
%%        db_mod                  = DbMod,
%%        k                       = K,
%%        buckets                 = Buckets,
%%        max_nodes_for_distance  = MaxNodes
%%    } = State,
%%    IsEnoughSpaceInBucket = case Distance of
%%        undefined ->
%%            true;
%%        _ ->
%%            {value, #bucket{nodes = NodesInBucket}} = lists:keysearch(Distance, #bucket.distance, Buckets),
%%            (erlang:length(NodesInBucket) < K)
%%    end,
%%    IsEnoughSpaceInNotAssigned = (erlang:length(DbMod:get_not_assigned_nodes(Distance)) < MaxNodes),
%%    (IsEnoughSpaceInBucket orelse IsEnoughSpaceInNotAssigned).
%%
%%
%%
%%%%
%%%%
%%%%
%%-spec safe_insert_to_not_assigned_nodes(
%%    Node    :: #node{},
%%    State   :: #state{}
%%) -> ok.
%%
%%safe_insert_to_not_assigned_nodes(Node, #state{db_mod = DbMod, max_nodes_for_distance = MaxNodes}) ->
%%    #node{distance = Distance} = Node,
%%    case erlang:length(DbMod:get_not_assigned_nodes(Distance)) of
%%        CurrNum when CurrNum < MaxNodes ->
%%            true = DbMod:insert_to_not_assigned_nodes(Node),
%%            ok;
%%        _CurrNum ->
%%            ok
%%    end.


%%  @private
%%  @doc
%%  Clear old peers search list and used peers in that searches cache.
%%  @end
-spec clear_peers_searches(
    State :: #state{}
) -> ok.

clear_peers_searches(#state{db_mod = DbMod}) ->
    ok = lists:foreach(fun (#get_peers_search{info_hash = InfoHash, last_changed = LastChanged}) ->
        case erline_dht_helper:datetime_diff(calendar:local_time(), LastChanged) < ?GET_PEERS_SEARCH_TTL of
            true  ->
                ok;
            false ->
                true = DbMod:delete_get_peers_search(InfoHash),
                true = DbMod:delete_requested_nodes(InfoHash)
        end
    end, DbMod:get_all_get_peers_searches()).


%%  @private
%%  @doc
%%  Change active nodes to suspicious, suspicious - to not active.
%%  @end
-spec update_bucket_nodes_status(
    Distance :: distance(),
    State    :: #state{}
) -> {NewState :: #state{}, AtLeastOneNotActive :: boolean()}.

update_bucket_nodes_status(Distance, State = #state{buckets = CurrBuckets}) ->
    {value, #bucket{nodes = Nodes}} = lists:keysearch(Distance, #bucket.distance, CurrBuckets),
    {NewState, BecomeNotActive} = lists:foldl(fun (Node, {CurrAccState, BecomeNotActiveAcc}) ->
        #node{
            ip_port      = {Ip, Port},
            last_changed = LastChanged,
            status       = Status
        } = Node,
        case erline_dht_helper:datetime_diff(calendar:local_time(), LastChanged) of
            SecondsTillLastChanged when SecondsTillLastChanged > ?ACTIVE_TTL, Status =:= active ->
                CurrAccState0 = update_node(Ip, Port, [{status, suspicious}], CurrAccState),
                % Try to ping once again
                {ok, NewAccState} = do_ping_async(Ip, Port, CurrAccState0),
                {NewAccState, BecomeNotActiveAcc};
            SecondsTillLastChanged when SecondsTillLastChanged > ?SUSPICIOUS_TTL, Status =:= suspicious ->
                {update_node(Ip, Port, [{status, not_active}], CurrAccState), BecomeNotActiveAcc + 1};
            _SecondsTillLastChanged ->
                {CurrAccState, BecomeNotActiveAcc}
        end
    end, {State, 0}, Nodes),
    {NewState, (BecomeNotActive > 0)}.


%%  @private
%%  @doc
%%  Initiate ping and try to replace not active nodes into active ones.
%%  @end
-spec init_not_active_nodes_replacement(
    Distance :: distance(),
    State    :: #state{}
) -> NewState :: #state{}.

init_not_active_nodes_replacement(Distance, State = #state{k = K, db_mod = DbMod}) ->
    NotAssignedNodes = lists:sort(fun (#node{last_changed = LastChanged1}, #node{last_changed = LastChanged2}) ->
        LastChanged1 > LastChanged2
    end, DbMod:get_not_assigned_nodes(Distance)),
    lists:foldl(fun (Node, CurrAccState) ->
        {ok, NewAccState} = do_ping_async(false, Node, CurrAccState),
        NewAccState
    end, State, lists:sublist(NotAssignedNodes, K * 10)).


%%  @private
%%  @doc
%%  Clear not assigned nodes without distance which exceeded TTL.
%%  @end
-spec clear_not_assigned_nodes(
    State :: #state{}
) -> ok.

clear_not_assigned_nodes(#state{db_mod = DbMod}) ->
    Dt = erline_dht_helper:change_datetime(calendar:local_time(), ?NOT_ACTIVE_NOT_ASSIGNED_NODES_TTL),
    ok = DbMod:delete_from_not_assigned_nodes_by_dist_date(undefined, Dt).


%%  @private
%%  @doc
%%  Clear not assigned nodes with distance which exceeded TTL and threshold.
%%  @end
-spec clear_not_assigned_nodes(
    Distance :: distance(),
    State    :: #state{}
) -> ok.

clear_not_assigned_nodes(Distance, #state{db_mod = DbMod, not_assigned_clearing_threshold = Threshold}) ->
    Dt = erline_dht_helper:change_datetime(calendar:local_time(), ?ACTIVE_NOT_ASSIGNED_NODES_TTL),
    case DbMod:get_not_assigned_nodes(Distance) of
        Nodes when length(Nodes) > Threshold ->
            SortedNodes = lists:sort(fun (#node{last_changed = LastChanged1}, #node{last_changed = LastChanged2}) ->
                LastChanged1 >= LastChanged2
            end, Nodes),
            {_, NodesToRemove} = lists:split(Threshold, SortedNodes),
            ok = lists:foreach(fun (#node{ip_port = {Ip, Port}, last_changed = LastChanged}) ->
                true = case LastChanged =< Dt of
                    true  -> DbMod:delete_from_not_assigned_nodes_by_ip_port(Ip, Port);
                    false -> true
                end
            end, NodesToRemove);
        _ ->
            ok
    end.


