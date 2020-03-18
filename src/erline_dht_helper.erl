%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% Mainline DHT helper functions.
%%% @end
%%% Created : 07. Mar 2020 00.31
%%%-------------------------------------------------------------------
-module(erline_dht_helper).
-author("bartimaeus").

%% API
-export([
    socket_active/1,
    socket_active_once/1,
    socket_passive/1,
    get_distance/2,
    parse_compact_node_info/1
]).


%%  @doc
%%  Turn socket to active mode.
%%
socket_active(Socket) ->
    inet:setopts(Socket, [{active, true}]).


%%  @doc
%%  Turn socket to active once mode.
%%
socket_active_once(Socket) ->
    inet:setopts(Socket, [{active, once}]).


%%  @doc
%%  Turn socket to passive mode.
%%
socket_passive(Socket) ->
    inet:setopts(Socket, [{active, false}]).


%%  @doc
%%  Get distance (in integer) between two 20 bytes length hashes.
%%  1 is further than 160, but 0 is the nearest (1,2,...,160,0)!
%%
get_distance(Hash, NodeHash) when
    byte_size(Hash) =/= byte_size(NodeHash)
    ->
    {error, {different_hash_length, Hash, NodeHash}};

get_distance(Hash, NodeHash) when
    Hash =:= NodeHash
    ->
    {ok, 0};

get_distance(Hash, NodeHash) ->
    get_distance(Hash, NodeHash, 0).

get_distance(<<Hash:1/bytes, HashRest/binary>>, <<NodeHash:1/bytes, NodeHashRest/binary>>, Result) when
    Hash =:= NodeHash ->
    get_distance(HashRest, NodeHashRest, Result + 1);

get_distance(<<Hash:1/bytes, _HashRest/binary>>, <<NodeHash:1/bytes, _NodeHashRest/binary>>, Result) when
    Hash =/= NodeHash
    ->
    <<HashInt:8>> = Hash,
    <<NodeHashInt:8>> = NodeHash,
    DiffBitPosition = lists:foldl(fun
        (Shift, 0) ->
            case ((HashInt bxor NodeHashInt) bsl Shift) band 100000000 of
                0 -> 0;
                _ -> Shift
            end;
        (_Shift, Res) ->
            Res
    end, 0, lists:seq(0, 8)),
    {ok, Result * 8 + DiffBitPosition}.


%%
%%
%%
parse_compact_node_info(Info) ->
    parse_compact_node_info(Info, []).

parse_compact_node_info(<<>>, Result) ->
    Result;

parse_compact_node_info(<<Hash:20/binary, Ip:4/binary, Port:2/binary, Rest/binary>>, Result) ->
    <<PortInt:16>> = Port,
    <<Oct1:8, Oct2:8, Oct3:8, Oct4:8>> = Ip,
    Node = #{hash => Hash, ip => {Oct1, Oct2, Oct3, Oct4}, port => PortInt},
    parse_compact_node_info(Rest, [Node | Result]).


