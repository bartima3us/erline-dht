%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%% UDP acceptor server.
%%% @end
%%% Created : 16. Aug 2019 17.02
%%%-------------------------------------------------------------------
-module(erline_dht_server).
-author("bartimaeus").

-behaviour(gen_server).

%% API
-export([
    start_link/0
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
    get_distance/2
]).
-endif.

-define(SERVER, ?MODULE).

-record(state, {}).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


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
init([]) ->
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
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
handle_info(_Info, State) ->
    {noreply, State}.


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
%%% Internal functions
%%%===================================================================


%%  @private
%%  Get distance (in integer) between two 20 bytes length hashes.
%%
get_distance(Hash, PeerHash) when
    byte_size(Hash) =/= byte_size(PeerHash)
    ->
    {error, different_hash_length};

get_distance(Hash, PeerHash) when
    Hash =:= PeerHash
    ->
    {ok, 0};

get_distance(Hash, PeerHash) ->
    get_distance(Hash, PeerHash, 0).

get_distance(<<Hash:1/bytes, HashRest/binary>>, <<PeerHash:1/bytes, PeerHashRest/binary>>, Result) when
    Hash =:= PeerHash ->
    get_distance(HashRest, PeerHashRest, Result + 1);

get_distance(<<Hash:1/bytes, _HashRest/binary>>, <<PeerHash:1/bytes, _PeerHashRest/binary>>, Result) when
    Hash =/= PeerHash
    ->
    <<HashInt:8>> = Hash,
    <<PeerHashInt:8>> = PeerHash,
    DiffBitPosition = lists:foldl(fun
        (Shift, 0) ->
            case ((HashInt bxor PeerHashInt) bsl Shift) band 100000000 of
                0 -> 0;
                _ -> Shift
            end;
        (_Shift, Res) ->
            Res
    end, 0, lists:seq(0, 8)),
    {ok, Result * 8 + DiffBitPosition}.


