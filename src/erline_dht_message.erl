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

-export([
    send_ping/5,
    respond_ping/5,
    send_find_node/6,
    ping_request/2,
    ping_response/2,
    find_node_request/3,
    find_node_response/3,
    get_peers_request/3,
    get_peers_response/4,
    announce_peer_request/6,
    announce_peer_response/2,
    error_response/3,
    parse_krpc_response/2
]).

% @todo make option
-define(RECEIVE_TIMEOUT, 2000).


%%%===================================================================
%%% API
%%%===================================================================


%%
%%
%%
send_ping(Ip, Port, Socket, MyNodeId, TransactionId) ->
    Payload = ping_request(TransactionId, MyNodeId),
    ok = socket_send(Socket, Ip, Port, Payload).


%%
%%
%%
respond_ping(Ip, Port, Socket, MyNodeId, TransactionId) ->
    Payload = ping_response(TransactionId, MyNodeId),
    ok = socket_send(Socket, Ip, Port, Payload).


%%
%%
%%
send_find_node(Ip, Port, Socket, MyNodeId, TransactionId, TargetNodeId) ->
    Payload = find_node_request(TransactionId, MyNodeId, TargetNodeId),
    ok = socket_send(Socket, Ip, Port, Payload).


%%  @doc
%%  Get `ping` request. http://www.bittorrent.org/beps/bep_0005.html#ping
%%
ping_request(TransactionId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Request = krpc_request(TransactionId, <<"q">>, <<"ping">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @doc
%%  Do response to `ping` request. http://www.bittorrent.org/beps/bep_0005.html#ping
%%
ping_response(TransactionId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @doc
%%  Get `find node` request. http://www.bittorrent.org/beps/bep_0005.html#find-node
%%
find_node_request(TransactionId, NodeId, Target) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"target">>, Target}
    ],
    Request = krpc_request(TransactionId, <<"q">>, <<"find_node">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @doc
%%  Do response to `find node` request. http://www.bittorrent.org/beps/bep_0005.html#find-node
%%
find_node_response(TransactionId, NodeId, Nodes) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"nodes">>, Nodes}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @doc
%%  Get `get peers` request. http://www.bittorrent.org/beps/bep_0005.html#get-peers
%%
get_peers_request(TransactionId, NodeId, InfoHash) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"info_hash">>, InfoHash}
    ],
    Request = krpc_request(TransactionId, <<"q">>, <<"get_peers">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @doc
%%  Do response to `get peers` request. http://www.bittorrent.org/beps/bep_0005.html#get-peers
%%
get_peers_response(TransactionId, NodeId, Token, Values) when is_list(Values) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"token">>, Token},
        {<<"values">>, {list, Values}}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response);

get_peers_response(TransactionId, NodeId, Token, Nodes) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"token">>, Token},
        {<<"nodes">>, Nodes}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @doc
%%  Get `announce peer` request. http://www.bittorrent.org/beps/bep_0005.html#announce-peer
%%
announce_peer_request(TransactionId, NodeId, ImpliedPort, InfoHash, Port, Token) when
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
    Request = krpc_request(TransactionId, <<"q">>, <<"announce_peer">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @doc
%%  Do response to `get peers` request. http://www.bittorrent.org/beps/bep_0005.html#announce-peer
%%
announce_peer_response(TransactionId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @doc
%%  Do error response. http://www.bittorrent.org/beps/bep_0005.html#errors
%%
error_response(TransactionId, ErrorCode, ErrorDescription) when
    ErrorCode =:= 201;
    ErrorCode =:= 202;
    ErrorCode =:= 203;
    ErrorCode =:= 204
    ->
    Response = krpc_request(TransactionId, <<"e">>, [ErrorCode, ErrorDescription]),
    erline_dht_bencoding:encode(Response).


%%
%%
%%
parse_krpc_response(Response, ActiveTx) ->
    ParseResponseFun = fun
        (ping, Resp) ->
            {ok, NodeHash} = dict:find(<<"id">>, Resp),
            NodeHash;
        (find_node, Resp) ->
            case dict:find(<<"nodes">>, Resp) of
                {ok, CompactNodeInfo} ->
                    erline_dht_helper:parse_compact_node_info(CompactNodeInfo);
                error ->
                    io:format("xxxxxx something went wrong1=~p~n", [Resp]),
                    []
            end
    end,
    case erline_dht_bencoding:decode(Response) of
        {ok, {dict, ResponseDict}} ->
            case dict:find(<<"t">>, ResponseDict) of
                {ok, TransactionId} ->
                    case dict:find(<<"y">>, ResponseDict) of
                        % Got query from node
                        {ok, <<"q">>} ->
                            case {dict:find(<<"q">>, ResponseDict), dict:find(<<"a">>, ResponseDict)} of
                                {{ok, <<"ping">>}, {ok, {dict, Args}}} ->
                                    {ok, Hash} = dict:find(<<"id">>, Args),
                                    {ok, ping, q, Hash, TransactionId};
                                _ ->
                                    {error, {bad_query, ResponseDict}}
                            end;
                        % Got response from node
                        {ok, OtherY} ->
                            case lists:keysearch(TransactionId, 2, ActiveTx) of
                                {value, {ReqType, TransactionId}} ->
                                    case OtherY of
                                        <<"r">> ->
                                            {ok, {dict, R}} = dict:find(<<"r">>, ResponseDict),
                                            NewActiveTx = ActiveTx -- [{ReqType, TransactionId}],
                                            {ok, ReqType, r, ParseResponseFun(ReqType, R), NewActiveTx};
                                        <<"e">> -> % @todo update tx ids?
                                            % Example: {ok,{list,[202,<<"Server Error">>]}
                                            {ok, {list, E}} = dict:find(<<"e">>, ResponseDict),
                                            {error, {krpc_error, E}}
                                    end;
                                false ->
                                    {error, {non_existing_transaction, TransactionId}}
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
%%  Make KRPC request. http://bittorrent.org/beps/bep_0005.html#krpc-protocol
%%
krpc_request(TransactionId, Type = <<"q">>, Query, Args) ->
    % Request
    A = lists:foldl(fun ({Arg, Val}, AAcc) ->
            dict:store(Arg, Val, AAcc)
        end, dict:new(), Args
    ),
    Req0 = dict:store(<<"t">>, TransactionId, dict:new()),
    Req1 = dict:store(<<"y">>, Type, Req0),
    Req2 = dict:store(Type, Query, Req1),
    Req3 = dict:store(<<"a">>, {dict, A}, Req2),
%%    Req4 = dict:store(<<"v">>, <<76,84,1,0>>, Req3), % Version. Optional. @todo fix tests with it
    {dict, Req3}.

krpc_request(TransactionId, Type = <<"r">>, Response) ->
    % Normal response
    R = lists:foldl(fun ({Arg, Val}, RAcc) ->
            dict:store(Arg, Val, RAcc)
        end, dict:new(), Response
    ),
    Req0 = dict:store(<<"t">>, TransactionId, dict:new()),
    Req1 = dict:store(<<"y">>, Type, Req0),
    Req2 = dict:store(<<"r">>, {dict, R}, Req1),
    {dict, Req2};

krpc_request(TransactionId, Type = <<"e">>, Error = [_Code, _Description]) ->
    % Error response
    Req0 = dict:store(<<"t">>, TransactionId, dict:new()),
    Req1 = dict:store(<<"y">>, Type, Req0),
    Req2 = dict:store(<<"e">>, {list, Error}, Req1),
    {dict, Req2}.


%%
%%
%%
socket_send(Socket, Ip, Port, Payload) ->
    case gen_udp:send(Socket, Ip, Port, Payload) of
        ok              -> ok;
        {error, einval} -> ok; % Ip or port can be malformed
        {error, eagain} -> ok  % @todo ???
    end.



