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
    error_response/3,
    parse_krpc_response/2
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
    announce_peer_response/2
]).
-endif.

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
    MyNodeId    :: binary()
) -> ok.

send_ping(Ip, Port, Socket, TxId, MyNodeId) ->
    Payload = ping_request(TxId, MyNodeId),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `ping` response and send it.
%%  @end
-spec respond_ping(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    Socket      :: port(),
    TxId        :: tx_id(),
    MyNodeId    :: binary()
) -> ok.

respond_ping(Ip, Port, Socket, TxId, MyNodeId) ->
    Payload = ping_response(TxId, MyNodeId),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `find_node` request and send it.
%%  @end
-spec send_find_node(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    Socket          :: port(),
    TxId            :: tx_id(),
    MyNodeId        :: binary(),
    TargetNodeId    :: binary()
) -> ok.

send_find_node(Ip, Port, Socket, TxId, MyNodeId, TargetNodeId) ->
    Payload = find_node_request(TxId, MyNodeId, TargetNodeId),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `find_node` response and send it.
%%  @end
-spec respond_find_node(
    Ip          :: inet:ip_address(),
    Port        :: inet:port_number(),
    Socket      :: port(),
    TxId        :: tx_id(),
    MyNodeId    :: binary(),
    Nodes       :: binary()
) -> ok.

respond_find_node(Ip, Port, Socket, TxId, MyNodeId, Nodes) ->
    Payload = find_node_response(TxId, MyNodeId, Nodes),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Create `get_peers` request and send it.
%%  @end
-spec send_get_peers(
    Ip              :: inet:ip_address(),
    Port            :: inet:port_number(),
    Socket          :: port(),
    TxId            :: tx_id(),
    MyNodeId        :: binary(),
    TargetNodeId    :: binary()
) -> ok.

send_get_peers(Ip, Port, Socket, TxId, MyNodeId, InfoHash) ->
    Payload = get_peers_request(TxId, MyNodeId, InfoHash),
    ok = socket_send(Socket, Ip, Port, Payload).


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


%%  @doc
%%  Parse KRPC response. http://bittorrent.org/beps/bep_0005.html#krpc-protocol
%%  @end
-spec parse_krpc_response(
    Response    :: binary(),
    ActiveTxs   :: [active_tx()]
) ->
    % @todo finish spec
    % @todo tests
    {ok, ping, q, Hash :: binary(), TxId :: tx_id()} |
    {ok, ping, r, Hash :: binary(), NewActiveTx :: [active_tx()]} |
    {ok, find_node, r, [parsed_compact_node_info()], NewActiveTx :: [active_tx()]} |
    {ok, get_peers, r,
        {nodes, TxId :: tx_id(), Nodes :: [parsed_compact_node_info()], PeerToken :: binary()} |
        {peers, TxId :: tx_id(), Peers :: [parsed_peer_info()],         PeerToken :: binary()},
        NewActiveTx :: [active_tx()]} |
    {error, {krpc_error, Error :: [term()]}, NewActiveTx :: [active_tx()]} | % Error :: [ErrorCode :: krpc_error_code(), Description :: binary()]
    {error, {bad_query, Response :: term()}} |
    {error, {bad_type, BadType :: binary()}, NewActiveTx :: [active_tx()]} |
    {error, {non_existing_tx, TxId :: tx_id()}} |
    {error, {bad_response, Response :: term()}}.

parse_krpc_response(Response, ActiveTxs) ->
    case erline_dht_bencoding:decode(Response) of
        {ok, {dict, ResponseDict}} ->
            case dict:find(<<"t">>, ResponseDict) of
                {ok, TxId} ->
                    case dict:find(<<"y">>, ResponseDict) of
                        % Received query from node
                        {ok, <<"q">>} ->
                            case {dict:find(<<"q">>, ResponseDict), dict:find(<<"a">>, ResponseDict)} of
                                {{ok, <<"ping">>}, {ok, {dict, Args}}} ->
                                    {ok, Hash} = dict:find(<<"id">>, Args),
                                    {ok, ping, q, Hash, TxId};
                                {{ok, <<"ping">>}, {ok, {dict, Args}}} ->
                                    {ok, Target} = dict:find(<<"target">>, Args),
                                    {ok, Hash} = dict:find(<<"hash">>, Args),
                                    {ok, find_node, q, {Hash, Target}, TxId};
                                % @todo implement requests handling
                                {{ok, <<"announce_peer">>}, {ok, {dict, Args}}} ->
                                    {ok, announce_peer, q, <<>>, TxId};
                                _ ->
                                    {error, {bad_query, ResponseDict}}
                            end;
                        % Received response from node
                        {ok, OtherY} ->
                            case lists:keysearch(TxId, 2, ActiveTxs) of
                                {value, ActiveTx = {ReqType, TxId}} ->
                                    case OtherY of
                                        <<"r">> ->
                                            {ok, {dict, R}} = dict:find(<<"r">>, ResponseDict),
                                            {ok, ReqType, r, parse_response_dict(ReqType, TxId, R), ActiveTxs -- [ActiveTx]};
                                        <<"e">> ->
                                            % Example: {ok,{list,[202,<<"Server Error">>]}
                                            {ok, {list, E}} = dict:find(<<"e">>, ResponseDict),
                                            {error, {krpc_error, E}, ActiveTxs -- [ActiveTx]};
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
%%  Parse response dict (by KRPC type "r").
%%  @end
-spec parse_response_dict
    % @todo finish spec
    % @todo tests
    (
        Type    :: ping,
        TxId    :: tx_id(),
        Resp    :: dict:dict()
    ) -> NodeHash :: binary();
    (
        Type    :: find_node,
        TxId    :: tx_id(),
        Resp    :: dict:dict()
    ) -> [parsed_compact_node_info()];
    (
        Type    :: get_peers,
        TxId    :: tx_id(),
        Resp    :: dict:dict()
    ) -> {nodes, TxId :: tx_id(), Nodes :: [parsed_compact_node_info()], PeerToken :: binary()} |
         {peers, TxId :: tx_id(), Peers :: [parsed_peer_info()],         PeerToken :: binary()}.

parse_response_dict(ping, _TxId, Resp) ->
    {ok, NodeHash} = dict:find(<<"id">>, Resp),
    NodeHash;

parse_response_dict(find_node, _TxId, Resp) ->
    case dict:find(<<"nodes">>, Resp) of
        {ok, CompactNodeInfo} ->
            erline_dht_helper:decode_compact_node_info(CompactNodeInfo);
        error ->
            []
    end;

parse_response_dict(get_peers, TxId, Resp) ->
    PeerToken = case dict:find(<<"token">>, Resp) of
        {ok, Token} -> Token;
        error       -> <<>>
    end,
    case dict:find(<<"values">>, Resp) of
        {ok, {list, PeerInfoList}} ->
            ParsedPeerInfoList = erline_dht_helper:decode_peer_info(PeerInfoList),
            {peers, TxId, ParsedPeerInfoList, PeerToken};
        error ->
            case dict:find(<<"nodes">>, Resp) of
                {ok, CompactNodeInfo} ->
                    ParsedCompactNodeInfo = erline_dht_helper:decode_compact_node_info(CompactNodeInfo),
                    {nodes, TxId, ParsedCompactNodeInfo, PeerToken};
                error ->
                    {nodes, TxId, [], PeerToken}
            end
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
%%    Req4 = dict:store(<<"v">>, <<76,84,1,0>>, Req3), % Version. Optional. @todo fix tests with it
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
    case gen_udp:send(Socket, Ip, Port, Payload) of
        ok ->
            ok;
        {error, einval} ->
            ok; % Ip or port can be malformed
        {error, eagain} ->
            ok;  % @todo ???
        {error, enetunreach} -> % @todo is this correct a way to handle network disconnections?
            timer:sleep(5000),
            socket_send(Socket, Ip, Port, Payload)
    end.


