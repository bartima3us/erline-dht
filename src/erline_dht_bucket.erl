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
    add_node_without_ping/2,
    add_node/3,
    add_node_without_ping/3,
    get_peers/1,
    get_peers/3,
    get_port/0,
    get_event_mgr_pid/0,
    get_all_nodes_in_bucket/1,
    get_not_assigned_nodes/0,
    get_not_assigned_nodes/1,
    get_buckets_filling/0,
    set_peer_port/1,
    stop/0
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
    handle_ping_query/5,
    handle_ping_response/6,
    handle_find_node_query/6,
    handle_find_node_response/7,
    handle_get_peers_query/6,
    handle_get_peers_response/6,
    handle_announce_peer_response/6,
    do_ping_async/3,
    do_find_node_async/4,
    do_get_peers_async/4,
    do_announce_peer_async/5,
    handle_response_generic/7,
    get_bucket_and_node/3,
    update_tx_id/1,
    update_node/4,
    update_bucket/3,
    maybe_clear_bucket/2,
    find_n_closest_nodes/5,
    find_local_peers_by_info_hash/2,
    add_peer/4,
    insert_info_hash/2,
    check_searches/1,
    update_tokens/1,
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
-define(GET_PEERS_SEARCH_CHECK_TIME, 5000). % 5 s
-define(CLEAR_NOT_ASSIGNED_NODES_TIME, 60000). % 1 min
-define(UPDATE_TOKENS_TIME, 300000). % 5 min
-define(NOT_ACTIVE_NOT_ASSIGNED_NODES_TTL, 90). % 90 s
-define(ACTIVE_NOT_ASSIGNED_NODES_TTL, 300). % 5 min
-define(ACTIVE_TTL, 840). % 14 min
-define(GET_PEERS_SEARCH_TTL, 20). % 20 s
-define(SUSPICIOUS_TTL, 900). % 15 min (ACTIVE_TTL + 1 min)
-define(GET_NOT_ASSIGNED_NODES_CALL_TIMEOUT, 15000).

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
    info_hashes                 = []    :: [#info_hash{}],
    get_peers_searches_timer            :: reference(),
    clear_not_assigned_nodes_timer      :: reference(),
    update_tokens_timer                 :: reference(),
    db_mod                              :: module(),
    event_mgr_pid                       :: pid(),
    not_assigned_clearing_threshold     :: pos_integer(), % Not assigned nodes
    valid_tokens                = []    :: [binary()],
    peer_port                           :: inet:port_number()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link(
    Port :: inet:port_number() | undefined
) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.

start_link(Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Port], []).


%%  @doc
%%  Add node with unknown hash to the bucket.
%%  @end
-spec add_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number()
) -> ok.

add_node(Ip, Port) ->
    gen_server:cast(?SERVER, {add_node, Ip, Port, undefined, true}).


%%  @doc
%%  Add node with unknown hash to the bucket.
%%  Do not ping after addition.
%%  @end
-spec add_node_without_ping(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number()
) -> ok.

add_node_without_ping(Ip, Port) ->
    gen_server:cast(?SERVER, {add_node, Ip, Port, undefined, false}).


%%  @doc
%%  Add node with known hash to the bucket.
%%  @end
-spec add_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Hash    :: binary()
) -> ok.

add_node(Ip, Port, Hash) ->
    gen_server:cast(?SERVER, {add_node, Ip, Port, Hash, true}).


%%  @doc
%%  Add node with known hash to the bucket.
%%  Do not ping after addition.
%%  @end
-spec add_node_without_ping(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Hash    :: binary()
) -> ok.

add_node_without_ping(Ip, Port, Hash) ->
    gen_server:cast(?SERVER, {add_node, Ip, Port, Hash, false}).


%%  @doc
%%  Try to find peers for the info hash in the network.
%%  @end
-spec get_peers(
    InfoHash :: binary()
) -> ok.

get_peers(InfoHash) ->
    gen_server:cast(?SERVER, {get_peers, undefined, undefined, InfoHash}).


%%  @doc
%%  Try to get peers for the info hash from one node.
%%  @end
-spec get_peers(
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number(),
    InfoHash :: binary()
) -> ok.

get_peers(Ip, Port, InfoHash) ->
    gen_server:cast(?SERVER, {get_peers, Ip, Port, InfoHash}).


%%  @doc
%%  Return UDP socket port of the client.
%%  @end
-spec get_port() -> Port :: inet:port_number().

get_port() ->
    gen_server:call(?SERVER, get_port).


%%  @doc
%%  Return event manager pid.
%%  @end
-spec get_event_mgr_pid() -> EventMgrPid :: pid().

get_event_mgr_pid() ->
    gen_server:call(?SERVER, get_event_mgr_pid).


%%  @doc
%%  Return all nodes information.
%%  @end
-spec get_all_nodes_in_bucket(
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        status          => status(),
        last_changed    => calendar:datetime()}].

get_all_nodes_in_bucket(Distance) ->
    gen_server:call(?SERVER, {get_all_nodes_in_bucket, Distance}).


%%  @doc
%%  Return not assigned nodes information.
%%  @end
-spec get_not_assigned_nodes() -> 
    [#{ip              => inet:ip_address(),
       port            => inet:port_number(),
       hash            => binary(),
       last_changed    => calendar:datetime()}].

get_not_assigned_nodes() ->
    gen_server:call(?SERVER, {get_not_assigned_nodes, undefined}, ?GET_NOT_ASSIGNED_NODES_CALL_TIMEOUT).


%%  @doc
%%  Return not assigned nodes information.
%%  @end
-spec get_not_assigned_nodes(
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        last_changed    => calendar:datetime()}].

get_not_assigned_nodes(Distance) ->
    gen_server:call(?SERVER, {get_not_assigned_nodes, Distance}, ?GET_NOT_ASSIGNED_NODES_CALL_TIMEOUT).


%%  @doc
%%  Return amount of nodes in every bucket.
%%  @end
-spec get_buckets_filling() -> [#{distance => distance(), nodes => non_neg_integer()}].

get_buckets_filling() ->
    gen_server:call(?SERVER, get_buckets_filling).


%%  @doc
%%  Set peer port.
%%  @end
-spec set_peer_port(
    Port :: inet:port_number()
) -> ok.

set_peer_port(Port) ->
    gen_server:call(?SERVER, {set_peer_port, Port}).


%%  @doc
%%  Stop the process
%%  @end
-spec stop() -> ok.

stop() ->
    gen_server:stop(?SERVER).


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
init([PortArg]) ->
    % Get my node hash
    MyNodeHash = get_my_node_hash(),
    % Open UDP socket
    SocketParams = [binary, {active, true}],
    OpenSocketFun = fun (Port) ->
        case gen_udp:open(Port, SocketParams) of
            {ok, SockPort}      -> {ok, SockPort};
            {error, eaddrinuse} -> gen_udp:open(0, SocketParams)
        end
    end,
    {ok, Socket} = case PortArg of
        undefined -> OpenSocketFun(erline_dht:get_env(port, 0));
        PortArg   -> OpenSocketFun(PortArg)
    end,
    % Create buckets
    Buckets = lists:foldl(fun (Distance, AccBuckets) ->
        NewBucket = #bucket{
            check_timer = schedule_bucket_check(Distance),
            ping_timer  = schedule_bucket_ping(Distance),
            distance    = Distance
        },
        [NewBucket | AccBuckets]
    end, [], lists:seq(0, erlang:bit_size(MyNodeHash))),
    % Create DB
    DbMod = erline_dht:get_env(db_mod, erline_dht_db_ets),
    ok = DbMod:init(),
    % Start event manager
    {ok, EventMgrPid} = gen_event:start_link(),
    % Schedule nodes clearing scheduler if necessary
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
        update_tokens_timer             = schedule_update_tokens(),
        db_mod                          = DbMod,
        event_mgr_pid                   = EventMgrPid,
        not_assigned_clearing_threshold = K * 100,
        valid_tokens                    = [erline_dht_helper:generate_random_binary(20)]
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

handle_call({set_peer_port, Port}, _From, State = #state{}) ->
    {reply, ok, State#state{peer_port = Port}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast({add_node, Ip, Port, Hash, Ping}, State = #state{}) ->
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
                last_changed = erline_dht_helper:local_time()
            },
            true = DbMod:insert_to_not_assigned_nodes(NewNode),
            case Ping of
                true ->
                    {ok, NewState0} = do_ping_async(Ip, Port, State),
                    NewState0;
                false ->
                    State
            end;
        {ok, _Bucket, #node{}} ->
            State
    end,
    {noreply, NewState};

%
%
handle_cast({get_peers, Ip, Port, InfoHash}, State = #state{}) ->
    #state{
        socket        = Socket,
        buckets       = Buckets,
        db_mod        = DbMod,
        event_mgr_pid = EventMgrPid
    } = State,
    NodesForSearch = case {Ip, Port} of
        {undefined, undefined} ->
            % First check in the local cache for a peer
            LocalPeers = find_local_peers_by_info_hash(InfoHash, State),
            % First peers are from self node
            {ok, {LocalIp, LocalPort}} = inet:sockname(Socket),
            ok = erline_dht_helper:notify(EventMgrPid, {get_peers, r, LocalIp, LocalPort, {peers, InfoHash, LocalPeers}}),
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
            last_changed    = erline_dht_helper:local_time()
        },
        RequestedNode = #requested_node{
            ip_port     = {RequestedIp, RequestedPort},
            tx_id       = TxId,
            info_hash   = InfoHash
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
        event_mgr_pid = EventMgrPid
    } = State,
    {Bucket, ActiveTx} = case get_bucket_and_node(Ip, Port, State) of
        {ok, NodeBucket, #node{active_txs = NodeActiveTx}} ->
            {NodeBucket, NodeActiveTx};
        false ->
            ok = add_node(Ip, Port),
            {false, []}
    end,
    NewState = case erline_dht_message:parse_krpc_response(Response, ActiveTx) of
        %
        % Handle ping query
        {ok, ping, q, NodeHash, ReceivedTxId} ->
            handle_ping_query(Ip, Port, NodeHash, ReceivedTxId, State);
        %
        % Handle ping response
        {ok, ping, r, NewNodeHash, NewActiveTx} ->
            handle_ping_response(Ip, Port, NewNodeHash, NewActiveTx, Bucket, State);
        %
        % Handle find_node query
        {ok, find_node, q, {NodeHash, Target}, ReceivedTxId} ->
            handle_find_node_query(Ip, Port, NodeHash, Target, ReceivedTxId, State);
        %
        % Handle find_node response
        {ok, find_node, r, {NewNodeHash, Nodes}, NewActiveTx} ->
            handle_find_node_response(Ip, Port, NewNodeHash, Nodes, NewActiveTx, Bucket, State);
        %
        % Handle get_peers query
        {ok, get_peers, q, {NodeHash, InfoHash}, ReceivedTxId} ->
            handle_get_peers_query(Ip, Port, NodeHash, InfoHash, ReceivedTxId, State);
        %
        % Handle get_peers response
        {ok, get_peers, r, GetPeersResp, NewActiveTx} ->
            handle_get_peers_response(Ip, Port, GetPeersResp, NewActiveTx, Bucket, State);
        %
        % Handle announce_peer query
        {ok, announce_peer, q, {NodeHash, ImpliedPort, InfoHash, PeerPort, ReceivedToken}, ReceivedTxId} ->
            handle_announce_peer_query(Ip, Port, NodeHash, ImpliedPort, InfoHash, PeerPort, ReceivedToken, ReceivedTxId, State);
        %
        % Handle announce_peer response
        {ok, announce_peer, r, NodeHash, NewActiveTx} ->
            handle_announce_peer_response(Ip, Port, NodeHash, NewActiveTx, Bucket, State);
        %
        % Handle errors
        {error, {krpc_error, ErrorCode, ErrorReason}, NewActiveTx} ->
            ok = erline_dht_helper:notify(EventMgrPid, {error, r, Ip, Port, {ErrorCode, ErrorReason}}),
            update_node(Ip, Port, [{active_txs, NewActiveTx}], State);
        {error, {bad_type, _BadType}, NewActiveTx} ->
            update_node(Ip, Port, [{active_txs, NewActiveTx}], State);
        {error, {non_existing_tx, _TxId}} ->
            State;
        {error, {bad_args, _BadArgs, ReceivedTxId}} ->
            erline_dht_message:respond_error(Socket, Ip, Port, ReceivedTxId, 203, <<"Invalid arguments">>),
            State;
        {error, {bad_query, _BadQuery, ReceivedTxId}} ->
            erline_dht_message:respond_error(Socket, Ip, Port, ReceivedTxId, 204, <<"Method unknown">>),
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
    NewState0 = check_searches(State),
    NewState1 = NewState0#state{get_peers_searches_timer = schedule_get_peers_searches_check()},
    {noreply, NewState1};

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
    {noreply, NewState};

%
%
handle_info(update_tokens, State = #state{}) ->
    NewState0 = update_tokens(State),
    NewState1 = NewState0#state{update_tokens_timer = schedule_update_tokens()},
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
%%% Handle query and response functions
%%%===================================================================

%%  @private
%%  @doc
%%  Handle ping query from socket.
%%  @end
-spec handle_ping_query(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    NodeHash        :: binary(),
    ReceivedTxId    :: tx_id(),
    State           :: #state{}
) -> NewState :: #state{}.

handle_ping_query(Ip, Port, NodeHash, ReceivedTxId, State) ->
    #state{
        socket        = Socket,
        event_mgr_pid = EventMgrPid,
        my_node_hash  = MyNodeHash
    } = State,
    ok = erline_dht_helper:notify(EventMgrPid, {ping, q, Ip, Port, NodeHash}),
    ok = erline_dht_message:respond_ping(Ip, Port, Socket, ReceivedTxId, MyNodeHash),
    ok = add_node(Ip, Port, NodeHash),
    case get_bucket_and_node(Ip, Port, State) of
        {ok, Bucket, Node} -> update_node(Bucket, Node, [{last_changed, erline_dht_helper:local_time()}], State);
        false              -> State
    end.


%%  @private
%%  @doc
%%  Handle ping response from socket.
%%  @end
-spec handle_ping_response(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    NewNodeHash :: binary(),
    NewActiveTx :: [active_tx()],
    Bucket      :: #bucket{} | false,
    State       :: #state{}
) -> NewState :: #state{}.

handle_ping_response(Ip, Port, NewNodeHash, NewActiveTx, Bucket, State) ->
    #state{
        event_mgr_pid = EventMgrPid
    } = State,
    ok = erline_dht_helper:notify(EventMgrPid, {ping, r, Ip, Port, NewNodeHash}),
    handle_response_generic(Ip, Port, NewNodeHash, NewActiveTx, Bucket, true, State).


%%  @private
%%  @doc
%%  Handle find_node query from socket.
%%  @end
-spec handle_find_node_query(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    NodeHash        :: binary(),
    Target          :: binary(),
    ReceivedTxId    :: tx_id(),
    State           :: #state{}
) -> NewState :: #state{}.

handle_find_node_query(Ip, Port, NodeHash, Target, ReceivedTxId, State) ->
    #state{
        socket        = Socket,
        event_mgr_pid = EventMgrPid,
        my_node_hash  = MyNodeHash,
        k             = K
    } = State,
    ok = erline_dht_helper:notify(EventMgrPid, {find_node, q, Ip, Port, {NodeHash, Target}}),
    Nodes = find_n_closest_nodes(Ip, Port, Target, K, State),
    CompactNodesInfo = erline_dht_helper:encode_compact_node_info(Nodes),
    ok = erline_dht_message:respond_find_node(Ip, Port, Socket, ReceivedTxId, MyNodeHash, CompactNodesInfo),
    ok = add_node(Ip, Port, NodeHash),
    case get_bucket_and_node(Ip, Port, State) of
        {ok, Bucket, Node} -> update_node(Bucket, Node, [{last_changed, erline_dht_helper:local_time()}], State);
        false              -> State
    end.


%%  @private
%%  @doc
%%  Handle find_node response from socket.
%%  @end
-spec handle_find_node_response(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    NewNodeHash :: binary(),
    Nodes       :: [parsed_compact_node_info()],
    NewActiveTx :: [active_tx()],
    Bucket      :: binary(),
    State       :: #state{}
) -> NewState :: #state{}.

handle_find_node_response(Ip, Port, NewNodeHash, Nodes, NewActiveTx, Bucket, State) ->
    #state{
        event_mgr_pid = EventMgrPid
    } = State,
    ok = erline_dht_helper:notify(EventMgrPid, {find_node, r, Ip, Port, Nodes}),
    ok = lists:foreach(fun (#{ip := FoundIp, port := FoundPort, hash := FoundedHash}) ->
        % Can't assume that node we got is live so we need to ping it.
        ok = add_node(FoundIp, FoundPort, FoundedHash)
    end, Nodes),
    handle_response_generic(Ip, Port, NewNodeHash, NewActiveTx, Bucket, false, State).


%%  @private
%%  @doc
%%  Handle get_peers query from socket.
%%  @end
-spec handle_get_peers_query(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    NodeHash        :: binary(),
    InfoHash        :: binary(),
    ReceivedTxId    :: tx_id(),
    State           :: #state{}
) -> NewState :: #state{}.

handle_get_peers_query(Ip, Port, NodeHash, InfoHash, ReceivedTxId, State) ->
    #state{
        socket        = Socket,
        event_mgr_pid = EventMgrPid,
        my_node_hash  = MyNodeHash,
        k             = K,
        info_hashes   = InfoHashes,
        valid_tokens  = [Token | _]
    } = State,
    ok = erline_dht_helper:notify(EventMgrPid, {get_peers, q, Ip, Port, {NodeHash, InfoHash}}),
    PeersOrNodes = case lists:keysearch(InfoHash, #info_hash.info_hash, InfoHashes) of
        {value, #info_hash{peers = Peers}} ->
            erline_dht_helper:encode_peer_info(Peers);
        false ->
            Nodes = find_n_closest_nodes(Ip, Port, InfoHash, K, State),
            erline_dht_helper:encode_compact_node_info(Nodes)
    end,
    ok = erline_dht_message:respond_get_peers(Ip, Port, Socket, ReceivedTxId, MyNodeHash, Token, PeersOrNodes),
    ok = add_node(Ip, Port, NodeHash),
    Params = [
        {last_changed, erline_dht_helper:local_time()},
        {token_sent, Token}
    ],
    case get_bucket_and_node(Ip, Port, State) of
        {ok, Bucket, Node} -> update_node(Bucket, Node, Params, State);
        false              -> State
    end.


%%  @private
%%  @doc
%%  Handle ping query from socket.
%%  @end
-spec handle_get_peers_response(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    GetPeersResp    :: {What         :: nodes | peers,
                        TxId         :: tx_id(),
                        NodesOrPeers :: [parsed_compact_node_info() | parsed_peer_info()],
                        Token        :: binary()},
    NewActiveTx     :: [active_tx()],
    Bucket          :: #bucket{} | false,
    State           :: #state{}
) -> NewState :: #state{}.

handle_get_peers_response(Ip, Port, GetPeersResp, NewActiveTx, Bucket, State) ->
    {What, NewNodeHash, TxId, NodesOrPeers, Token} = GetPeersResp,
    #state{
        event_mgr_pid = EventMgrPid,
        db_mod        = DbMod
    } = State,
    NewState0 = case DbMod:get_info_hash(Ip, Port, TxId) of
        false ->
            % Responded with info hash which we do not have in a search state
            State;
        InfoHash ->
            % Update last_changed
            true = DbMod:insert_to_get_peers_searches(#get_peers_search{info_hash = InfoHash, last_changed = erline_dht_helper:local_time()}),
            case What of
                % Continue search
                nodes ->
                    ok = erline_dht_helper:notify(EventMgrPid, {get_peers, r, Ip, Port, {nodes, InfoHash, NodesOrPeers}}),
                    ok = lists:foreach(fun (#{ip := FoundIp, port := FoundPort, hash := FoundedHash}) ->
                        % Check whether we already have that node in ETS
                        case DbMod:get_requested_node(FoundIp, FoundPort, InfoHash) of
                            [_|_]  ->
                                ok;
                            [] ->
                                ok = add_node_without_ping(FoundIp, FoundPort, FoundedHash),
                                ok = get_peers(FoundIp, FoundPort, InfoHash)
                        end
                    end, NodesOrPeers),
                    State;
                % Stop search and save info hashes
                peers ->
                    io:format("GOT VALUES. Token=~p Vals=~p~n", [Token, NodesOrPeers]),
                    ok = erline_dht_helper:notify(EventMgrPid, {get_peers, r, Ip, Port, {peers, InfoHash, NodesOrPeers}}),
                    lists:foldl(fun (#{ip := FoundIp, port := FoundPort}, StateAcc) ->
                        ok = add_node_without_ping(FoundIp, FoundPort),
                        add_peer(InfoHash, FoundIp, FoundPort, StateAcc)
                    end, State, NodesOrPeers)
            end
    end,
    NewState1 = update_node(Ip, Port, [{token_received, Token}], NewState0),
    handle_response_generic(Ip, Port, NewNodeHash, NewActiveTx, Bucket, false, NewState1).


%%  @private
%%  @doc
%%  Handle announce_peer query from socket.
%%  @end
-spec handle_announce_peer_query(   % @todo tests
    Ip              :: inet:ip_address(),
    NodePort        :: inet:port_number(),
    NodeHash        :: binary(),
    ImpliedPort     :: 0 | 1,
    InfoHash        :: binary(),
    PeerPort        :: inet:port_number(),
    ReceivedToken   :: binary(),
    ReceivedTxId    :: tx_id(),
    State           :: #state{}
) -> NewState :: #state{}.

handle_announce_peer_query(Ip, NodePort, NodeHash, ImpliedPort, InfoHash, PeerPort, ReceivedToken, ReceivedTxId, State) ->
    #state{
        socket        = Socket,
        my_node_hash  = MyNodeHash,
        event_mgr_pid = EventMgrPid,
        valid_tokens  = ValidTokens
    } = State,
    ok = case ImpliedPort of
        0 -> erline_dht_helper:notify(EventMgrPid, {announce_peer, q, Ip, NodePort, {NodeHash, InfoHash, PeerPort, ReceivedToken}});
        _ -> erline_dht_helper:notify(EventMgrPid, {announce_peer, q, Ip, NodePort, {NodeHash, InfoHash, NodePort, ReceivedToken}})
    end,
    SentToken = case get_bucket_and_node(Ip, NodePort, State) of
        {ok, _, #node{token_sent = TokenSent}} -> TokenSent;
        _ -> undefined
    end,
    NewState0 = case SentToken =:= ReceivedToken andalso lists:member(ReceivedToken, ValidTokens) of
        true  ->
            ok = erline_dht_message:respond_announce_peer(Ip, NodePort, Socket, ReceivedTxId, MyNodeHash),
            % If ImpliedPort is present and non-zero, the port argument should be ignored and the source port of the UDP packet should be used as the peer's port instead.
            case ImpliedPort of
                0 -> add_peer(InfoHash, Ip, PeerPort, State);
                _ -> add_peer(InfoHash, Ip, NodePort, State)
            end;
        false ->
            erline_dht_message:respond_error(Socket, Ip, NodePort, ReceivedTxId, 203, <<"Bad Token">>),
            State
    end,
    ok = add_node(Ip, NodePort, NodeHash),
    case get_bucket_and_node(Ip, NodePort, NewState0) of
        {ok, Bucket, Node} -> update_node(Bucket, Node, [{last_changed, erline_dht_helper:local_time()}], NewState0);
        false              -> NewState0
    end.


%%  @private
%%  @doc
%%  Handle announce_peer response from socket.
%%  @end
-spec handle_announce_peer_response(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    NewNodeHash :: binary(),
    NewActiveTx :: [active_tx()],
    Bucket      :: #bucket{} | false,
    State       :: #state{}
) -> NewState :: #state{}.

handle_announce_peer_response(Ip, Port, NewNodeHash, NewActiveTx, Bucket, State) ->
    #state{
        event_mgr_pid = EventMgrPid
    } = State,
    ok = erline_dht_helper:notify(EventMgrPid, {announce_peer, r, Ip, Port, NewNodeHash}),
    handle_response_generic(Ip, Port, NewNodeHash, NewActiveTx, Bucket, false, State).


%%%===================================================================
%%% Request functions
%%%===================================================================

%%  @private
%%  @doc
%%  Send ping request.
%%  Do not wait for response.
%%  @end
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
    #node{tx_id = TxId, active_txs = CurrActiveTx} = Node,
    Params = [
        tx_id,
        {active_txs, [{ping, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Bucket, Node, Params, State),
    ok = erline_dht_message:send_ping(Ip, Port, Socket, TxId, MyNodeHash),
    {ok, NewState}.


%%  @private
%%  @doc
%%  Send find_node request.
%%  Do not wait for response.
%%  @end
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
    #node{tx_id = TxId, active_txs = CurrActiveTx} = Node,
    Params = [
        tx_id,
        {active_txs, [{find_node, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Bucket, Node, Params, State),
    ok = erline_dht_message:send_find_node(Ip, Port, Socket, TxId, MyNodeHash, Target),
    {ok, NewState}.


%%  @private
%%  @doc
%%  Send get_peers request.
%%  Do not wait for response.
%%  @end
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
    #node{tx_id = TxId, active_txs = CurrActiveTx} = Node,
    Params = [
        tx_id,
        {active_txs, [{get_peers, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Bucket, Node, Params, State),
    ok = erline_dht_message:send_get_peers(Ip, Port, Socket, TxId, MyNodeHash, InfoHash),
    {ok, TxId, NewState}.


%%  @private
%%  @doc
%%  Send announce_peer request.
%%  Do not wait for response.
%%  @end
-spec do_announce_peer_async(   % @todo tests
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number(),
    InfoHash :: binary(),
    Token    :: binary(),
    State    :: #state{}
) -> {ok, NewState :: #state{}}.

do_announce_peer_async(Ip, Port, InfoHash, Token, State = #state{}) when is_tuple(Ip), is_integer(Port) ->
    {ok, Bucket, Node} = get_bucket_and_node(Ip, Port, State),
    do_announce_peer_async(Bucket, Node, InfoHash, Token, State);

do_announce_peer_async(Bucket, Node = #node{ip_port = {Ip, Port}}, InfoHash, Token, State = #state{}) ->
    #state{
        my_node_hash = MyNodeHash,
        socket       = Socket,
        peer_port    = PeerPort0
    } = State,
    #node{tx_id = TxId, active_txs = CurrActiveTx} = Node,
    Params = [
        tx_id,
        {active_txs, [{announce_peer, TxId} | CurrActiveTx]}
    ],
    NewState = update_node(Bucket, Node, Params, State),
    {PeerPort, ImpliedPort} = case PeerPort0 of
        undefined -> 
            {ok, NodePort} = inet:port(Socket),
            {NodePort, 1};
        PeerPort0  ->
            {PeerPort0, 0}
    end,
    ok = erline_dht_message:send_announce_peer(Ip, Port, Socket, TxId, MyNodeHash, InfoHash, ImpliedPort, PeerPort, Token),
    {ok, NewState}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%  @private
%%  @doc
%%  Generic node and bucket update function for all responses.
%%  @end
-spec handle_response_generic(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    NewNodeHash :: binary(),
    NewActiveTx :: [active_tx()],
    Bucket      :: #bucket{} | false,
    DoFindNodes :: boolean(),
    State       :: #state{}
) -> NewState :: #state{}.

handle_response_generic(Ip, Port, NewNodeHash, NewActiveTx, Bucket, DoFindNodes, State = #state{my_node_hash  = MyNodeHash}) ->
    case erline_dht_helper:get_distance(MyNodeHash, NewNodeHash) of
        {ok, NewDist} ->
            Params = [
                {hash,          NewNodeHash},
                {last_changed,  erline_dht_helper:local_time()},
                {status,        active}
            ],
            case Bucket of
                % Node is already assigned to the bucket
                #bucket{distance = CurrDist} ->
                    case CurrDist =:= NewDist of
                        % If hash is the same, update node data
                        true ->
                            update_node(Ip, Port, Params ++ [{active_txs, NewActiveTx}], State);
                         % If hash is changed, move node to another bucket
                        false ->
                            case maybe_clear_bucket(NewDist, State) of
                                {true, NewState0}  -> update_node(Ip, Port, Params ++ [{assign, NewDist}, {active_txs, NewActiveTx}], NewState0);
                                {false, NewState0} -> update_node(Ip, Port, Params ++ [unassign, {active_txs, NewActiveTx}], NewState0)
                            end
                    end;
                % Not assigned to the bucket
                false ->
                    NewState0 = update_node(Ip, Port, [{active_txs, NewActiveTx}], State),
                    {ok, NewState1} = case DoFindNodes of
                        true  -> do_find_node_async(Ip, Port, MyNodeHash, NewState0); % Relevant for ping only
                        false -> {ok, NewState0}
                    end,
                    case maybe_clear_bucket(NewDist, NewState1) of
                        {true, NewState2}  -> update_node(Ip, Port, Params ++ [{assign, NewDist}], NewState2);
                        % No free space in the bucket.
                        {false, NewState2} -> update_node(Ip, Port, Params, NewState2)
                    end
            end;
        {error, _Reason} ->
            Params = [
                {hash,         NewNodeHash},
                {last_changed, erline_dht_helper:local_time()},
                {status,       not_active},
                {active_txs,   NewActiveTx}
            ],
            update_node(Ip, Port, Params, State)
    end.


%%  @private
%%  @doc
%%  Check for a node in the not assigned nodes list.
%%  If not found in the not assigned nodes list, check in a buckets.
%%  @end
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
%%  @doc
%%  Increase node current transaction ID by 1.
%%  @end
-spec update_tx_id(
    Node :: #node{}
) -> NewNode :: #node{}.

update_tx_id(Node = #node{tx_id = undefined}) ->
    Node#node{tx_id = <<0,0>>};

update_tx_id(Node = #node{tx_id = <<255,255>>}) ->
    Node#node{tx_id = <<0,0>>};

update_tx_id(Node = #node{tx_id = LastTxIdBin}) ->
    <<LastTxIdInt:16>> = LastTxIdBin,
    NewTxIdInt = LastTxIdInt + 1,
    Node#node{tx_id = <<NewTxIdInt:16>>}.


%%  @private
%%  @doc
%%  Update node by given parameters.
%%  @end
-spec update_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Params  :: [{hash, Hash :: binary()} |
                {token_received, TokenReceived :: binary()} |
                {token_sent, TokenSent :: binary()} |
                {last_changed, LastChanged :: calendar:datetime()} |
                {active_txs, [ActiveTx :: active_tx()]} |
                tx_id |
                {status, Status :: status()} |
                {assign, Distance :: distance()} |
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
                % @todo assign automatically to Distance bucket
                {ok, Distance}   -> AccNode#node{hash = Hash, distance = Distance};
                {error, _Reason} -> AccNode#node{hash = Hash}
            end;
        ({token_received, TokenReceived}, AccNode) ->
            AccNode#node{token_received = TokenReceived};
        ({token_sent, TokenSent}, AccNode) ->
            AccNode#node{token_sent = TokenSent};
        ({last_changed, LastChanged}, AccNode) ->
            AccNode#node{last_changed = LastChanged};
        ({active_txs, ActiveTx}, AccNode) ->
            AccNode#node{active_txs = ActiveTx};
        (tx_id, AccNode) ->
            update_tx_id(AccNode);
        ({status, Status}, AccNode) ->
            AccNode#node{status = Status};
        ({assign, Distance}, AccNode) ->
            AccNode#node{distance = Distance};
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
                % Assign updated node to the new bucket if there is assign param
                {value, {assign, NewDist}} ->
                    CurrBucketUpdated = CurrBucket#bucket{nodes = lists:keydelete({Ip, Port}, #node.ip_port, Nodes)},
                    lists:keyreplace(CurrDist, #bucket.distance, AddNodeToBucketFun(NewDist), CurrBucketUpdated);
                % Just put updated node to the old bucket
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


%%  @private
%%  @doc
%%  Update bucket by specified parameters.
%%  Return a new state with a new bucket.
%%  @end
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


%%  @private
%%  @doc
%%  Schedule next `{bucket_check, Distance}` message.
%%  @end
-spec schedule_bucket_check(
    Distance :: distance()
) -> TimerRef :: reference().

schedule_bucket_check(Distance) ->
    Time = rand:uniform(?BUCKET_CHECK_HIGH_TIME - ?BUCKET_CHECK_LOW_TIME) + ?BUCKET_CHECK_LOW_TIME,
    erlang:send_after(Time, self(), {bucket_check, Distance}).


%%  @private
%%  @doc
%%  Schedule next `{bucket_ping, Distance}` message.
%%  @end
-spec schedule_bucket_ping(
    Distance :: distance()
) -> TimerRef :: reference().

schedule_bucket_ping(Distance) ->
    Time = rand:uniform(?BUCKET_PING_HIGH_TIME - ?BUCKET_PING_LOW_TIME) + ?BUCKET_PING_LOW_TIME,
    erlang:send_after(Time, self(), {bucket_ping, Distance}).


%%  @private
%%  @doc
%%  Schedule next `get_peers_searches_check` message.
%%  @end
-spec schedule_get_peers_searches_check() -> TimerRef :: reference().

schedule_get_peers_searches_check() ->
    erlang:send_after(?GET_PEERS_SEARCH_CHECK_TIME, self(), get_peers_searches_check).


%%  @private
%%  @doc
%%  Schedule next `clear_not_assigned_nodes` message.
%%  @end
-spec schedule_clear_not_assigned_nodes() -> TimerRef :: reference().

schedule_clear_not_assigned_nodes() ->
    erlang:send_after(?CLEAR_NOT_ASSIGNED_NODES_TIME, self(), clear_not_assigned_nodes).


%%  @private
%%  @doc
%%  Schedule next `schedule_update_tokens` message.
%%  @end
-spec schedule_update_tokens() -> TimerRef :: reference().

schedule_update_tokens() ->
    erlang:send_after(?UPDATE_TOKENS_TIME, self(), update_tokens).


%%  @private
%%  @doc
%%  Move oldest not active node (if exists) from bucket to not assigned nodes if bucket is full.
%%  Return the fact - whether bucket has available space now or not.
%%  @end
-spec maybe_clear_bucket(
    Distance :: distance(),
    State    :: #state{}
) -> {IsEnoughSpaceInBucket :: boolean(), NewState :: #state{}}.

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


%%  @private
%%  @doc
%%  Find N closest nodes for the given hash.
%%  @end
-spec find_n_closest_nodes(
    ExcludeIp   :: inet:ip_address(),
    ExcludePort :: inet:port_number(),
    Hash        :: binary(),
    N           :: pos_integer(),
    State       :: #state{}
) -> ClosestNodes :: [{IpPort :: {inet:ip_address(), inet:port_number()}, NodeHash :: binary()}].

find_n_closest_nodes(ExcludeIp, ExcludePort, Hash, N, #state{buckets = Buckets}) ->
    AllNodes = lists:flatten(lists:map(fun (#bucket{nodes = Nodes}) ->
        Nodes
    end, Buckets)),
    NodesWithDist = lists:foldl(fun
        (#node{hash = NodeHash, ip_port = IpPort}, AccNodes) when IpPort =/= {ExcludeIp, ExcludePort} ->
            case erline_dht_helper:get_distance(Hash, NodeHash) of
                {ok, Distance}   -> [{Distance, IpPort, NodeHash} | AccNodes];
                {error, _Reason} -> AccNodes
            end;
        (#node{}, AccNodes) ->
            AccNodes
    end, [], AllNodes),
    [{IpPort0, Hash0} || {_, IpPort0, Hash0} <- lists:sublist(lists:usort(NodesWithDist), N)].


%%  @private
%%  @doc
%%  Find peers in the state by the specified info hash.
%%  @end
-spec find_local_peers_by_info_hash(
    InfoHash :: binary(),
    State    :: #state{}
) -> [Peer :: #{ip => inet:ip_address(), port => inet:port_number()}].

find_local_peers_by_info_hash(InfoHash, #state{info_hashes = InfoHashes}) ->
    case lists:keysearch(InfoHash, #info_hash.info_hash, InfoHashes) of
        {value, #info_hash{peers = Peers}} ->
            lists:map(fun ({Ip, Port}) ->
                #{ip => Ip, port => Port}
            end, Peers);
        false ->
            []
    end.


%%  @private
%%  @doc
%%  Add a new peer to info hashes list.
%%  Create info hash if it does not exist.
%%  @end
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


%%  @private
%%  @doc
%%  Insert the peer contact information for itself onto the responding nodes with IDs closest to the infohash of the file.
%%  @end
-spec insert_info_hash(
    InfoHash :: binary(),
    State    :: #state{}
) -> NewState :: #state{}.

insert_info_hash(InfoHash, State = #state{db_mod = DbMod, k = K}) ->
    NodesWithDist = lists:foldl(fun (#requested_node{ip_port = {Ip, Port}}, AccNodes) ->
            case get_bucket_and_node(Ip, Port, State) of
                {ok, _, #node{hash = NodeHash, token_received = TokenReceived}} ->
                    case erline_dht_helper:get_distance(InfoHash, NodeHash) of
                        {ok, Distance}   -> [{Distance, {Ip, Port}, TokenReceived} | AccNodes];
                        {error, _Reason} -> AccNodes
                    end;
                false ->
                    AccNodes
            end
    end, [], DbMod:get_requested_nodes(InfoHash)),
    lists:foldl(fun ({_, {Ip, Port}, TokenReceived}, StateAcc) ->
        {ok, NewStateAcc} = do_announce_peer_async(Ip, Port, InfoHash, TokenReceived, StateAcc),
        NewStateAcc
    end, State, lists:sublist(lists:usort(NodesWithDist), K)).


%%  @private
%%  @doc
%%  After the search is exhausted, insert the peer contact information for itself onto the responding nodes with IDs closest to the infohash of the file.
%%  Clear old peers search list and used peers in that searches cache after insertion.
%%  @end
-spec check_searches(State :: #state{}) -> NewState :: #state{}.

check_searches(State = #state{db_mod = DbMod}) ->
    lists:foldl(fun (#get_peers_search{info_hash = InfoHash, last_changed = LastChanged}, StateAcc) ->
        case erline_dht_helper:datetime_diff(erline_dht_helper:local_time(), LastChanged) < ?GET_PEERS_SEARCH_TTL of
            true  ->
                StateAcc;
            false ->
                NewStateAcc = insert_info_hash(InfoHash, StateAcc),
                true = DbMod:delete_get_peers_search(InfoHash),
                true = DbMod:delete_requested_nodes(InfoHash),
                NewStateAcc
        end
    end, State, DbMod:get_all_get_peers_searches()).


%%  @private
%%  @doc
%%  Update valid tokens.
%%  @end
-spec update_tokens(
    State :: #state{}
) -> NewState :: #state{}.

update_tokens(State = #state{valid_tokens = CurrValidTokens}) ->
    NewToken = erline_dht_helper:generate_random_binary(20),
    case CurrValidTokens of
        []              -> State#state{valid_tokens = [NewToken]};
        [KeptToken]     -> State#state{valid_tokens = [NewToken, KeptToken]};
        [KeptToken, _]  -> State#state{valid_tokens = [NewToken, KeptToken]}
    end.


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
        case erline_dht_helper:datetime_diff(erline_dht_helper:local_time(), LastChanged) of
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
    Dt = erline_dht_helper:change_datetime(erline_dht_helper:local_time(), ?NOT_ACTIVE_NOT_ASSIGNED_NODES_TTL),
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
    Dt = erline_dht_helper:change_datetime(erline_dht_helper:local_time(), ?ACTIVE_NOT_ASSIGNED_NODES_TTL),
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


%%  @private
%%  @doc
%%  Generate my node hash or retrieve it from config.
%%  @end
-spec get_my_node_hash() -> binary().

get_my_node_hash() ->
    case erline_dht:get_env(node_hash, 20) of
        HashOpt when is_integer(HashOpt) ->
            % Peer ID Conventions: http://www.bittorrent.org/beps/bep_0020.html
            {_, _, Vsn} = lists:keyfind(?APP, 1, application:loaded_applications()),
            FormattedVsn = re:replace(Vsn, "\\.", "-", [global, {return, binary}]),
            RandomPart = erline_dht_helper:generate_random_binary(HashOpt - (erlang:byte_size(FormattedVsn) + 1)),
            <<"M", FormattedVsn/binary, RandomPart/binary>>;
        HashOpt when is_binary(HashOpt) ->
            HashOpt
    end.


