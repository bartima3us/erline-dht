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
    encode_compact_node_info/1,
    decode_compact_node_info/1,
    encode_peer_info/1,
    decode_peer_info/1,
    datetime_diff/2,
    change_datetime/2,
    notify/2,
    local_time/0,
    generate_random_binary/1
]).


%%  @doc
%%  Get distance (in integer) between two same length hashes.
%%  0 is the nearest (actually - the same), 160 is the farthest.
%%  @end
-spec get_distance(
    Hash1 :: binary(),
    Hash2 :: binary()
) -> {ok, Distance :: distance()} |
     {error, {malformed_hashes, Hash :: binary(), Hash2 :: binary}} |
     {error, {different_hash_length, Hash :: binary(), Hash2 :: binary}}.

get_distance(Hash1, Hash2) when
    is_binary(Hash1),
    is_binary(Hash2),
    erlang:byte_size(Hash1) =/= erlang:byte_size(Hash2)
    ->
    {error, {different_hash_length, Hash1, Hash2}};

get_distance(Hash1, Hash2) when is_binary(Hash1), is_binary(Hash2), Hash1 =:= Hash2 ->
    {ok, 0};

get_distance(Hash1, Hash2) when is_binary(Hash1), is_binary(Hash2) ->
    {ok, erlang:bit_size(Hash1) + 1 - get_distance(Hash1, Hash2, 0)};

get_distance(Hash1, Hash2) ->
    {error, {malformed_hashes, Hash1, Hash2}}.

get_distance(<<Hash1:1/binary, Hash1Rest/binary>>, <<Hash2:1/binary, Hash2Rest/binary>>, Result) when
    Hash1 =:= Hash2 ->
    get_distance(Hash1Rest, Hash2Rest, Result + 1);

get_distance(<<Hash1:1/binary, _Hash1Rest/binary>>, <<Hash2:1/binary, _Hash2Rest/binary>>, Result) when
    Hash1 =/= Hash2
    ->
    <<Hash1Int:8>> = Hash1,
    <<Hash2Int:8>> = Hash2,
    DiffBitPosition = lists:foldl(fun
        (Shift, 0) ->
            case ((Hash1Int bxor Hash2Int) bsl Shift) band 100000000 of
                0 -> 0;
                _ -> Shift
            end;
        (_Shift, Res) ->
            Res
    end, 0, lists:seq(0, 8)),
    Result * 8 + DiffBitPosition.


%%  @doc
%%  Encode compact node info list to binary.
%%  @end
-spec encode_compact_node_info(
    Nodes :: [{IpPort :: {inet:ip_address(), inet:port_number()}, NodeHash :: binary()}]
) -> CompactNodeInfo :: binary().

encode_compact_node_info(Nodes) ->
    lists:foldl(fun ({{Ip, Port}, Hash}, AccResult) ->
        {Oct1, Oct2, Oct3, Oct4} = Ip,
        <<AccResult/binary, Hash/binary, Oct1:8, Oct2:8, Oct3:8, Oct4:8, Port:16>>
    end, <<>>, Nodes).


%%  @doc
%%  Decode compact node info binary.
%%  Returns order list by closest nodes first.
%%  @end
-spec decode_compact_node_info(
    Info :: binary()
) -> [ParsedCompactNodeInfo :: parsed_compact_node_info()].

decode_compact_node_info(Info) ->
    decode_compact_node_info(Info, []).

decode_compact_node_info(<<>>, Result) ->
    Result;

decode_compact_node_info(<<Hash:20/binary, Ip:4/binary, Port:2/binary, Rest/binary>>, Result) ->
    <<PortInt:16>> = Port,
    <<Oct1:8, Oct2:8, Oct3:8, Oct4:8>> = Ip,
    Node = #{
        hash     => Hash,
        ip       => {Oct1, Oct2, Oct3, Oct4},
        port     => PortInt
    },
    decode_compact_node_info(Rest, [Node | Result]);

decode_compact_node_info(_Other, Result) ->
    Result.


%%  @doc
%%  Encode peer info list to list of binaries.
%%  @end
-spec encode_peer_info(
    Peers :: [IpPort :: {inet:ip_address(), inet:port_number()}]
) -> [PeerInfo :: binary()].

encode_peer_info(Peers) ->
    lists:map(fun ({Ip, Port}) ->
        {Oct1, Oct2, Oct3, Oct4} = Ip,
        <<Oct1:8, Oct2:8, Oct3:8, Oct4:8, Port:16>>
    end, Peers).


%%  @doc
%%  Decode peer info list of binaries.
%%  @end
-spec decode_peer_info(
    PeerInfoList :: [binary()]
) -> [ParsedPeerInfo :: parsed_peer_info()].

decode_peer_info(PeerInfoList) ->
    decode_peer_info(PeerInfoList, []).

decode_peer_info([], Result) ->
    Result;

decode_peer_info([<<Ip:4/binary, Port:2/binary>> | PeerInfoList], Result) ->
    <<PortInt:16>> = Port,
    <<Oct1:8, Oct2:8, Oct3:8, Oct4:8>> = Ip,
    Peer = #{ip => {Oct1, Oct2, Oct3, Oct4}, port => PortInt},
    decode_peer_info(PeerInfoList, [Peer | Result]);

decode_peer_info([_Other | PeerInfoList], Result) ->
    decode_peer_info(PeerInfoList, Result).


%%  @doc
%%  Return datetime's difference (DateTime1 - DateTime2) in seconds.
%%  @end
-spec datetime_diff(
    DateTime1 :: calendar:datetime(),
    DateTime2 :: calendar:datetime()
) -> Seconds :: integer().

datetime_diff(DateTime1, DateTime2) ->
    calendar:datetime_to_gregorian_seconds(DateTime1) - calendar:datetime_to_gregorian_seconds(DateTime2).


%%  @doc
%%  Return changed datetime with specified seconds subtracted.
%%  @end
-spec change_datetime(
    DateTime :: calendar:datetime(),
    Seconds  :: integer()
) -> NewDateTime :: calendar:datetime().

change_datetime(DateTime, Seconds) ->
    calendar:gregorian_seconds_to_datetime(calendar:datetime_to_gregorian_seconds(DateTime) - Seconds).


%%  @doc
%%  Encapsulation for mocking purposes.
%%  @end
-spec notify(
    Ref     :: gen_event:emgr_ref(),
    Event   :: term()
) -> ok.

notify(Ref, Event) ->
    gen_event:notify(Ref, Event).


%%  @doc
%%  Encapsulation for mocking purposes.
%%  @end
-spec local_time() -> calendar:datetime().

local_time() ->
    calendar:local_time().


%%  @doc
%%  Generate random binary of the specified length.
%%  @end
-spec generate_random_binary(
    Length :: non_neg_integer()
) -> binary().

generate_random_binary(Length) ->
    crypto:strong_rand_bytes(Length).


