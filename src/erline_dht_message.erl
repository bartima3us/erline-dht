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
    ping/6,
    ping_request/2,
    ping_response/2,
    find_node_request/3,
    find_node_response/3,
    get_peers_request/3,
    get_peers_response/4,
    announce_peer_request/6,
    announce_peer_response/2,
    error_response/3
]).

-define(RECEIVE_TIMEOUT, 2000).


%%%===================================================================
%%% API
%%%===================================================================


%%
%%
%%
ping(Ip, Port, Socket, NodeId, TransactionId, Tries) ->
    PingPayload = ping_request(TransactionId, NodeId),
    ok = gen_udp:send(Socket, Ip, Port, PingPayload),
    receive
        {udp, Socket, Ip, Port, PingResp} ->
            ok = erline_dht_helper:socket_passive(Socket),
            {ok, {dict, PingRespDict}} = erline_dht_bencoding:decode(PingResp),
            case dict:find(<<"t">>, PingRespDict) of
                {ok, TransactionId} ->
                    {ok, {dict, R}} = dict:find(<<"r">>, PingRespDict),
                    {ok, _NodeHash} = dict:find(<<"id">>, R);
                _Other -> % @todo implement
                     {error, bad_response}
            end
    after ?RECEIVE_TIMEOUT ->
        case Tries > 1 of
            true  -> ping(Ip, Port, Socket, NodeId, TransactionId, Tries - 1);
            false -> {error, not_alive}
        end
    end.


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


