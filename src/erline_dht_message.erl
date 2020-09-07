%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% Mainline DHT messages implementation.
%%% @end
%%% Created : 16. Aug 2019 17.10
%%%-------------------------------------------------------------------
-module(erline_dht_message).
-author("bartimaeus").
-include("erline_dht.hrl").

-export([
    send_ping/5,
    respond_ping/5,
    send_find_node/6,
    respond_find_node/6,
    send_get_peers/6,
    respond_get_peers/7,
    send_announce_peer/9,
    respond_announce_peer/5,
    respond_error/6,
    parse_krpc_response/3
]).

-ifdef(TEST).
-export([
    ping_request/2,
    ping_response/2,
    find_node_request/3,
    find_node_response/3,
    get_peers_request/3,
    get_peers_response/4,
    announce_peer_request/6,
    announce_peer_response/2,
    error_response/3,
    parse_response_dict/4,
    parse_krpc_arguments/2
]).
-endif.

% https://www.bittorrent.org/beps/bep_0005.html#errors
% 201 Generic Error
% 202 Server Error
% 203 Protocol Error, such as a malformed packet, invalid arguments, or bad token
% 204 Method Unknown
-type krpc_error_code() :: 201 | 202 | 203 | 204.

%%%===================================================================
%%% API
%%%===================================================================


%%  @doc
%%  Create `ping` request and send it.
%%  @end
-spec send_ping(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    Socket      :: port(),
    TxId        :: tx_id(),
    MyNodeHash  :: binary()
) -> ok.

send_ping(Ip, Port, Socket, TxId, MyNodeHash) ->
    Payload = ping_request(TxId, MyNodeHash),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `ping` response and send it.
%%  @end
-spec respond_ping(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    Socket      :: port(),
    TxId        :: tx_id(),
    MyNodeHash  :: binary()
) -> ok.

respond_ping(Ip, Port, Socket, TxId, MyNodeHash) ->
    Payload = ping_response(TxId, MyNodeHash),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `find_node` request and send it.
%%  @end
-spec send_find_node(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    Socket          :: port(),
    TxId            :: tx_id(),
    MyNodeHash      :: binary(),
    TargetNodeId    :: binary()
) -> ok.

send_find_node(Ip, Port, Socket, TxId, MyNodeHash, TargetNodeId) ->
    Payload = find_node_request(TxId, MyNodeHash, TargetNodeId),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `find_node` response and send it.
%%  @end
-spec respond_find_node(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    Socket      :: port(),
    TxId        :: tx_id(),
    MyNodeHash  :: binary(),
    Nodes       :: binary()
) -> ok.

respond_find_node(Ip, Port, Socket, TxId, MyNodeHash, Nodes) ->
    Payload = find_node_response(TxId, MyNodeHash, Nodes),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `get_peers` request and send it.
%%  @end
-spec send_get_peers(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    Socket          :: port(),
    TxId            :: tx_id(),
    MyNodeHash      :: binary(),
    TargetNodeId    :: binary()
) -> ok.

send_get_peers(Ip, Port, Socket, TxId, MyNodeHash, InfoHash) ->
    Payload = get_peers_request(TxId, MyNodeHash, InfoHash),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `get_peers` response and send it.
%%  @end
-spec respond_get_peers(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    Socket          :: port(),
    TxId            :: tx_id(),
    MyNodeHash      :: binary(),
    Token           :: binary(),
    NodesOrPeers    :: binary() | [binary()]
) -> ok.

respond_get_peers(Ip, Port, Socket, TxId, MyNodeHash, Token, NodesOrPeers) ->
    Payload = get_peers_response(TxId, MyNodeHash, Token, NodesOrPeers),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `announce_peer` request and send it.
%%  @end
-spec send_announce_peer(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    Socket      :: port(),
    TxId        :: tx_id(),
    MyNodeHash  :: binary(),
    InfoHash    :: binary(),
    ImpliedPort :: 0 | 1,
    PeerPort    :: inet:port_number(),
    Token       :: binary()
) -> ok.

send_announce_peer(Ip, Port, Socket, TxId, MyNodeHash, InfoHash, ImpliedPort, PeerPort, Token) ->
    Payload = announce_peer_request(TxId, MyNodeHash, ImpliedPort, InfoHash, PeerPort, Token),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `announce_peer` response and send it.
%%  @end
-spec respond_announce_peer(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    Socket      :: port(),
    TxId        :: tx_id(),
    MyNodeHash  :: binary()
) -> ok.

respond_announce_peer(Ip, Port, Socket, TxId, MyNodeHash) ->
    Payload = announce_peer_response(TxId, MyNodeHash),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `error` response and send it.
%%  @end
respond_error(Ip, Port, Socket, TxId, ErrorCode, ErrorDescription) ->
    Payload = error_response(TxId, ErrorCode, ErrorDescription),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Parse KRPC response. http://bittorrent.org/beps/bep_0005.html#krpc-protocol
%%  Validate everything because network is a Wild West!
%%  @end
-spec parse_krpc_response(
    NodeName  :: atom(),
    Response  :: binary(),
    ActiveTxs :: [active_tx()]
) ->
    {ok, ping, q, NodeHash :: binary(), TxId :: tx_id()} |
    {ok, ping, r, NodeHash :: binary(), NewActiveTx :: [active_tx()]} |
    {ok, find_node, q, {NodeHash :: binary(), Target :: binary()}, TxId :: tx_id()} |
    {ok, find_node, r, {NodeHash :: binary(), [parsed_compact_node_info()]}, NewActiveTx :: [active_tx()]} |
    {ok, get_peers, q, {NodeHash :: binary(), InfoHash :: binary()}, TxId :: tx_id()} |
    {ok, get_peers, r,
        {nodes, NodeHash :: binary(), TxId :: tx_id(), Nodes :: [parsed_compact_node_info()], PeerToken :: binary()} |
        {peers, NodeHash :: binary(), TxId :: tx_id(), Peers :: [parsed_peer_info()],         PeerToken :: binary()},
        NewActiveTx :: [active_tx()]} |
    {ok, announce_peer, q, {NodeHash :: binary(), ImpliedPort :: 0 | 1, InfoHash :: binary(), Port :: inet:port_number(), Token :: binary()}, TxId :: tx_id()} |
    {ok, announce_peer, r, NodeHash :: binary(), NewActiveTx :: [active_tx()]} |
    {error, {krpc_error, ErrorCode :: krpc_error_code(), ErrorReason :: binary()}, NewActiveTx :: [active_tx()]} |
    {error, {bad_query, Response :: term(), TxId :: tx_id()}} |
    {error, {bad_args, Args :: term(), TxId :: tx_id()}} |
    {error, {bad_type, BadType :: binary()}, NewActiveTx :: [active_tx()]} |
    {error, {non_existing_tx, TxId :: tx_id()}} |
    {error, {bad_response, Response :: term()}}.

parse_krpc_response(NodeName, Response, ActiveTxs) ->
    case erline_dht_bencoding:decode(Response) of
        {ok, {dict, ResponseDict}} ->
            case dict:find(<<"t">>, ResponseDict) of
                {ok, TxId} ->
                    case dict:find(<<"y">>, ResponseDict) of
                        % Received query from node
                        {ok, <<"q">>} ->
                            parse_krpc_arguments(ResponseDict, TxId);
                        % Received response from node
                        {ok, OtherY} ->
                            case lists:keysearch(TxId, 2, ActiveTxs) of
                                {value, ActiveTx = {ReqType, TxId}} ->
                                    case OtherY of
                                        <<"r">> ->
                                            case dict:find(<<"r">>, ResponseDict) of
                                                {ok, {dict, R}} ->
                                                    {ok, ReqType, r, parse_response_dict(ReqType, NodeName, TxId, R), ActiveTxs -- [ActiveTx]};
                                                MalformedR ->
                                                    {error, {bad_response, MalformedR}}
                                            end;
                                        <<"e">> ->
                                            % Example: {ok,{list,[202,<<"Server Error">>]}
                                            case dict:find(<<"e">>, ResponseDict) of
                                                {ok, {list, [RespErrCode, RespErrReason]}} ->
                                                    {error, {krpc_error, RespErrCode, RespErrReason}, ActiveTxs -- [ActiveTx]};
                                                MalformedE ->
                                                    {error, {bad_response, MalformedE}}
                                            end;
                                        BadType ->
                                            {error, {bad_type, BadType}, ActiveTxs -- [ActiveTx]}
                                    end;
                                false ->
                                    {error, {non_existing_tx, TxId}}
                            end
                    end;
                error ->
                     {error, {bad_response, ResponseDict}}
            end;
        {error, unparsed} ->
            {error, {bad_response, Response}}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%  @private
%%  @doc
%%  Get `ping` request. http://www.bittorrent.org/beps/bep_0005.html#ping
%%  @end
-spec ping_request(
    TxId    :: tx_id(),
    NodeId  :: binary()
) -> Request :: binary().

ping_request(TxId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Request = krpc_request(TxId, q, <<"ping">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @private
%%  @doc
%%  Do response to `ping` request. http://www.bittorrent.org/beps/bep_0005.html#ping
%%  @end
-spec ping_response(
    TxId    :: tx_id(),
    NodeId  :: binary()
) -> Response :: binary().

ping_response(TxId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Response = krpc_request(TxId, r, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  @doc
%%  Get `find_node` request. http://www.bittorrent.org/beps/bep_0005.html#find-node
%%  @end
-spec find_node_request(
    TxId    :: tx_id(),
    NodeId  :: binary(),
    Target  :: binary()
) -> Request :: binary().

find_node_request(TxId, NodeId, Target) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"target">>, Target}
    ],
    Request = krpc_request(TxId, q, <<"find_node">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @private
%%  @doc
%%  Do response to `find_node` request. http://www.bittorrent.org/beps/bep_0005.html#find-node
%%  @end
-spec find_node_response(
    TxId    :: tx_id(),
    NodeId  :: binary(),
    Nodes   :: binary()
) -> Response :: binary().

find_node_response(TxId, NodeId, Nodes) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"nodes">>, Nodes}
    ],
    Response = krpc_request(TxId, r, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  @doc
%%  Get `get_peers` request. http://www.bittorrent.org/beps/bep_0005.html#get-peers
%%  @end
-spec get_peers_request(
    TxId        :: tx_id(),
    NodeId      :: binary(),
    InfoHash    :: binary()
) -> Request :: binary().

get_peers_request(TxId, NodeId, InfoHash) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"info_hash">>, InfoHash}
    ],
    Request = krpc_request(TxId, q, <<"get_peers">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @private
%%  @doc
%%  Do response to `get_peers` request. http://www.bittorrent.org/beps/bep_0005.html#get-peers
%%  @end
-spec get_peers_response
    (
        TxId    :: tx_id(),
        NodeId  :: binary(),
        Token   :: binary(),
        Peers   :: [binary()]
    ) -> Response :: binary();
    (
        TxId    :: tx_id(),
        NodeId  :: binary(),
        Token   :: binary(),
        Nodes   :: binary()
    ) -> Response :: binary().

get_peers_response(TxId, NodeId, Token, Peers) when is_list(Peers) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"token">>, Token},
        {<<"values">>, {list, Peers}}
    ],
    Response = krpc_request(TxId, r, Args),
    erline_dht_bencoding:encode(Response);

get_peers_response(TxId, NodeId, Token, Nodes) when is_binary(Nodes) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"token">>, Token},
        {<<"nodes">>, Nodes}
    ],
    Response = krpc_request(TxId, r, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  @doc
%%  Get `announce_peer` request. http://www.bittorrent.org/beps/bep_0005.html#announce-peer
%%  @end
-spec announce_peer_request(
    TxId        :: tx_id(),
    NodeId      :: binary(),
    ImpliedPort :: 0 | 1,
    InfoHash    :: binary(),
    Port        :: inet:port_number(),
    Token       :: binary()
) -> Request :: binary().

announce_peer_request(TxId, NodeId, ImpliedPort, InfoHash, Port, Token) when
    ImpliedPort =:= 0;
    ImpliedPort =:= 1
    ->
    Args = [
        {<<"id">>, NodeId},
        {<<"implied_port">>, ImpliedPort},
        {<<"info_hash">>, InfoHash},
        {<<"port">>, Port},
        {<<"token">>, Token}
    ],
    Request = krpc_request(TxId, q, <<"announce_peer">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @private
%%  @doc
%%  Do response to `announce_peer` request. http://www.bittorrent.org/beps/bep_0005.html#announce-peer
%%  @end
-spec announce_peer_response(
    TxId    :: tx_id(),
    NodeId  :: binary()
) -> Response :: binary().

announce_peer_response(TxId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Response = krpc_request(TxId, r, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  @doc
%%  Do error response. http://www.bittorrent.org/beps/bep_0005.html#errors
%%  @end
-spec error_response(
    TxId                :: tx_id(),
    ErrorCode           :: krpc_error_code(),
    ErrorDescription    :: binary()
) -> Response :: binary().

error_response(TxId, ErrorCode, ErrorDescription) when
    ErrorCode =:= 201;
    ErrorCode =:= 202;
    ErrorCode =:= 203;
    ErrorCode =:= 204
    ->
    Response = krpc_request(TxId, e, [ErrorCode, ErrorDescription]),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  @doc
%%  Parse response dict (by KRPC type "r").
%%  @end
-spec parse_response_dict
    (
        Type     :: ping,
        NodeName :: atom(),
        TxId     :: tx_id(),
        Resp     :: dict:dict()
    ) -> NodeHash :: binary();
    (
        Type     :: find_node,
        NodeName :: atom(),
        TxId     :: tx_id(),
        Resp     :: dict:dict()
    ) -> {NodeHash :: binary(), CompactNodesInfo :: [parsed_compact_node_info()]};
    (
        Type     :: get_peers,
        NodeName :: atom(),
        TxId     :: tx_id(),
        Resp     :: dict:dict()
    ) -> {nodes, NodeHash :: binary(), TxId :: tx_id(), Nodes :: [parsed_compact_node_info()], PeerToken :: binary()} |
         {peers, NodeHash :: binary(), TxId :: tx_id(), Peers :: [parsed_peer_info()],         PeerToken :: binary()};
    (
        Type     :: announce_peer,
        NodeName :: atom(),
        TxId     :: tx_id(),
        Resp     :: dict:dict()
    ) -> NodeHash :: binary().

parse_response_dict(ping, _NodeName, _TxId, Resp) ->
    case dict:find(<<"id">>, Resp) of
        {ok, NodeHash} -> NodeHash;
        error          -> <<>>
    end;

parse_response_dict(find_node, NodeName, _TxId, Resp) ->
    NodeHash = case dict:find(<<"id">>, Resp) of
        {ok, NodeHash0} -> NodeHash0;
        error          -> <<>>
    end,
    case dict:find(<<"nodes">>, Resp) of
        {ok, CompactNodeInfo} ->
            {NodeHash, erline_dht_helper:decode_compact_node_info(NodeName, CompactNodeInfo)};
        error ->
            {NodeHash, []}
    end;

parse_response_dict(get_peers, NodeName, TxId, Resp) ->
    PeerToken = case dict:find(<<"token">>, Resp) of
        {ok, Token} -> Token;
        error       -> <<>>
    end,
    NodeHash = case dict:find(<<"id">>, Resp) of
        {ok, NodeHash0} -> NodeHash0;
        error          -> <<>>
    end,
    case dict:find(<<"values">>, Resp) of
        {ok, {list, PeerInfoList}} ->
            ParsedPeerInfoList = erline_dht_helper:decode_peer_info(PeerInfoList),
            {peers, NodeHash, TxId, ParsedPeerInfoList, PeerToken};
        error ->
            case dict:find(<<"nodes">>, Resp) of
                {ok, CompactNodeInfo} ->
                    ParsedCompactNodeInfo = erline_dht_helper:decode_compact_node_info(NodeName, CompactNodeInfo),
                    {nodes, NodeHash, TxId, ParsedCompactNodeInfo, PeerToken};
                error ->
                    {nodes, NodeHash, TxId, [], PeerToken}
            end
    end;

parse_response_dict(announce_peer, _NodeName, _TxId, Resp) ->
    case dict:find(<<"id">>, Resp) of
        {ok, NodeHash} -> NodeHash;
        error          -> <<>>
    end.


%%  @private
%%  @doc
%%  Parse arguments from KRPC request.
%%  @end
-spec parse_krpc_arguments(
    ResponseDict    :: dict:dict(),
    TxId            :: tx_id()
) ->
    {ok, ping, q, NodeHash :: binary(), TxId :: tx_id()} |
    {ok, find_node, q, {NodeHash :: binary(), Target :: binary()}, TxId :: tx_id()} |
    {ok, get_peers, q, {NodeHash :: binary(), InfoHash :: binary()}, TxId :: tx_id()} |
    {ok, announce_peer, q, {NodeHash :: binary(), ImpliedPort :: 0 | 1, InfoHash :: binary(), Port :: inet:port_number(), Token :: binary()}, TxId :: tx_id()} |
    {error, {bad_args, Args :: term(), TxId :: tx_id()}} |
    {error, {bad_query, Response :: term(), TxId :: tx_id()}}.

parse_krpc_arguments(ResponseDict, TxId) ->
    case {dict:find(<<"q">>, ResponseDict), dict:find(<<"a">>, ResponseDict)} of
        {{ok, <<"ping">>}, {ok, {dict, Args}}} ->
            case dict:find(<<"id">>, Args) of
                {ok, Hash} -> {ok, ping, q, Hash, TxId};
                error      -> {error, {bad_args, Args, TxId}}
            end;
        {{ok, <<"find_node">>}, {ok, {dict, Args}}} ->
            case {dict:find(<<"id">>, Args), dict:find(<<"target">>, Args)} of
                {{ok, Hash}, {ok, Target}} -> {ok, find_node, q, {Hash, Target}, TxId};
                _                          -> {error, {bad_args, Args, TxId}}
            end;
        {{ok, <<"get_peers">>}, {ok, {dict, Args}}} ->
            case {dict:find(<<"id">>, Args), dict:find(<<"info_hash">>, Args)} of
                {{ok, Hash}, {ok, InfoHash}} -> {ok, get_peers, q, {Hash, InfoHash}, TxId};
                _                            -> {error, {bad_args, Args, TxId}}
            end;
        {{ok, <<"announce_peer">>}, {ok, {dict, Args}}} ->
            ImpliedPort = case dict:find(<<"implied_port">>, Args) of
                {ok, ImpliedPort0} -> ImpliedPort0;
                error              -> 0
            end,
            case {dict:find(<<"id">>, Args),
                  dict:find(<<"info_hash">>, Args),
                  dict:find(<<"port">>, Args),
                  dict:find(<<"token">>, Args)}
            of
                {{ok, NodeHash}, {ok, InfoHash}, {ok, Port}, {ok, Token}} ->
                    {ok, announce_peer, q, {NodeHash, ImpliedPort, InfoHash, Port, Token}, TxId};
                _ ->
                    {error, {bad_args, Args, TxId}}
            end;
        _ ->
            {error, {bad_query, ResponseDict, TxId}}
    end.


%%  @private
%%  @doc
%%  Make KRPC query request. http://bittorrent.org/beps/bep_0005.html#krpc-protocol
%%  @end
-spec krpc_request(
    TxId   :: tx_id(),
    Type   :: q,
    Query  :: binary(),
    Args   :: [{Arg :: binary(), Val :: binary()}]
) -> {dict, Request :: dict:dict()}.

krpc_request(TxId, q, Query, Args) ->
    A = lists:foldl(fun ({Arg, Val}, AAcc) ->
        dict:store(Arg, Val, AAcc)
    end, dict:new(), Args),
    Type = <<"q">>,
    Req0 = dict:store(<<"t">>, TxId, dict:new()),
    Req1 = dict:store(<<"y">>, Type, Req0),
    Req2 = dict:store(Type, Query, Req1),
    Req3 = dict:store(<<"a">>, {dict, A}, Req2),
%%    Req4 = dict:store(<<"v">>, <<76,84,1,0>>, Req3), % Version. Optional.
    {dict, Req3}.

%%  @private
%%  @doc
%%  Make KRPC response or error request. http://bittorrent.org/beps/bep_0005.html#krpc-protocol
%%  @end
-spec krpc_request
    (
        TxId        :: tx_id(),
        Type        :: r,
        Response    :: [{Arg :: binary(), Val :: binary()}]
    ) -> {dict, Request :: dict:dict()};
    (
        TxId   :: tx_id(),
        Type            :: e,
        Error           :: [term()]
    ) -> {dict, Request :: dict:dict()}.

krpc_request(TxId, r, Response) ->
    % Normal response
    Type = <<"r">>,
    R = lists:foldl(fun ({Arg, Val}, RAcc) ->
        dict:store(Arg, Val, RAcc)
    end, dict:new(), Response),
    Req0 = dict:store(<<"t">>, TxId, dict:new()),
    Req1 = dict:store(<<"y">>, Type, Req0),
    Req2 = dict:store(<<"r">>, {dict, R}, Req1),
    {dict, Req2};

krpc_request(TxId, e, Error = [_Code, _Description]) ->
    Type = <<"e">>,
    Req0 = dict:store(<<"t">>, TxId, dict:new()),
    Req1 = dict:store(<<"y">>, Type, Req0),
    Req2 = dict:store(<<"e">>, {list, Error}, Req1),
    {dict, Req2}.


%%  @private
%%  @doc
%%  Send packets via UDP socket.
%%  @end
-spec socket_send(
    Socket  :: port(),
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Payload :: binary()
) -> ok.

socket_send(Socket, Ip, Port, Payload) ->
    socket_send(Socket, Ip, Port, Payload, 50).

socket_send(Socket, Ip, Port, Payload, Retries) ->
    case gen_udp:send(Socket, Ip, Port, Payload) of
        ok ->
            ok;
        {error, einval} -> % Received IP or port can be malformed
            ok;
        {error, eagain} -> % System call failed
            case Retries > 0 of
                true ->
                    timer:sleep(1000),
                    socket_send(Socket, Ip, Port, Payload, Retries - 1);
                false ->
                    {error, eagain}
            end;
        {error, enetunreach} -> % Network is unreachable
            case Retries > 0 of
                true ->
                    timer:sleep(5000),
                    socket_send(Socket, Ip, Port, Payload, Retries - 1);
                false ->
                    {error, enetunreach}
            end
    end.


