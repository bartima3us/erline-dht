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
-include("erline_dht.hrl").

%% API
-export([
    get_distance/2,
    get_hash_of_distance/2,
    parse_compact_node_info/1,
    parse_peer_info/1,
    datetime_diff/2,
    change_datetime/2
]).


%%  @doc
%%  Get distance (in integer) between two same length hashes.
%%  1 is the farthest, 160 is the nearest.
%%  @end
get_distance(Hash, NodeHash) when
    is_binary(Hash),
    is_binary(NodeHash),
    erlang:byte_size(Hash) =/= erlang:byte_size(NodeHash)
    ->
    {error, {different_hash_length, Hash, NodeHash}};

get_distance(Hash, NodeHash) when is_binary(Hash), is_binary(NodeHash), Hash =:= NodeHash ->
    {ok, 0};

get_distance(Hash, NodeHash) when is_binary(Hash), is_binary(NodeHash) ->
    {ok, 161 - get_distance(Hash, NodeHash, 0)};

get_distance(Hash, NodeHash) ->
    {error, {malformed_hashes, Hash, NodeHash}}.

get_distance(<<Hash:1/binary, HashRest/binary>>, <<NodeHash:1/binary, NodeHashRest/binary>>, Result) when
    Hash =:= NodeHash ->
    get_distance(HashRest, NodeHashRest, Result + 1);

get_distance(<<Hash:1/binary, _HashRest/binary>>, <<NodeHash:1/binary, _NodeHashRest/binary>>, Result) when
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
    Result * 8 + DiffBitPosition.


%%  @doc
%%  Get a new hash of the specified distance by the specified hash.
%%  @end
%%  @todo not used anymore?
get_hash_of_distance(Hash, _Distance) when not is_binary(Hash) ->
    {error, {malformed_hash, Hash}};

get_hash_of_distance(Hash, Distance) when bit_size(Hash) < Distance ->
    {error, {hash_too_short, Hash}};

get_hash_of_distance(Hash, Distance) ->
    SamePartSize = Distance - 1,
    <<SamePart:SamePartSize/bits, ImportantBit:1/bits, Rest/bits>> = Hash,
    <<ImportantBitInt:1/integer>> = ImportantBit,
    DiffBitInt = erlang:abs(ImportantBitInt - 1),
    DiffBit = <<DiffBitInt:1>>,
    {ok, <<SamePart/bits, DiffBit/bits, Rest/bits>>}.


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


%%
%%
%%
parse_peer_info(PeerInfoList) ->
    parse_peer_info(PeerInfoList, []).

parse_peer_info([], Result) ->
    Result;

parse_peer_info([<<Ip:4/binary, Port:2/binary>> | PeerInfoList], Result) ->
    <<PortInt:16>> = Port,
    <<Oct1:8, Oct2:8, Oct3:8, Oct4:8>> = Ip,
    Peer = #{ip => {Oct1, Oct2, Oct3, Oct4}, port => PortInt},
    parse_peer_info(PeerInfoList, [Peer | Result]).


%%
%%
%%
datetime_diff(DateTime1, DateTime2) ->
    calendar:datetime_to_gregorian_seconds(DateTime1) - calendar:datetime_to_gregorian_seconds(DateTime2).


%%
%%
%%
change_datetime(DateTime, Seconds) ->
    calendar:gregorian_seconds_to_datetime(calendar:datetime_to_gregorian_seconds(DateTime) - Seconds).



