%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 16. Aug 2019 17.10
%%%-------------------------------------------------------------------
-module(erline_dht_message).
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
    update_transaction_id/1,
    do_ping/2,
    do_ping_response/2,
    do_find_node/3,
    do_find_node_response/3,
    do_get_peers/3,
    do_get_peers_response/4,
    do_announce_peer/6,
    do_announce_peer_response/2,
    do_error_response/3
]).
-endif.

-define(SERVER, ?MODULE).

-record(state, {
    last_transaction_id
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


ping(NodeId) ->
    ok.


ping_response(NodeId) ->
    ok.


find_node() ->
    ok.


find_node_response() ->
    ok.


get_peers() ->
    ok.


get_peers_response() ->
    ok.


announce_peer() ->
    ok.


announce_peer_response() ->
    ok.


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
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).

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
%%  Increase current transaction ID by 1.
%%
update_transaction_id(State = #state{last_transaction_id = undefined}) ->
    State#state{last_transaction_id = <<0,0>>};

update_transaction_id(State = #state{last_transaction_id = <<255,255>>}) ->
    State#state{last_transaction_id = <<0,0>>};

update_transaction_id(State = #state{last_transaction_id = LastTransactionIdBin}) ->
    <<LastTransactionIdInt:16>> = LastTransactionIdBin,
    NewTransactionIdInt = LastTransactionIdInt + 1,
    State#state{last_transaction_id = <<NewTransactionIdInt:16>>}.


%%  @private
%%  Do `ping` request. http://www.bittorrent.org/beps/bep_0005.html#ping
%%
do_ping(TransactionId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Request = krpc_request(TransactionId, <<"q">>, <<"ping">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @private
%%  Do response to `ping` request. http://www.bittorrent.org/beps/bep_0005.html#ping
%%
do_ping_response(TransactionId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  Do `find node` request. http://www.bittorrent.org/beps/bep_0005.html#find-node
%%
do_find_node(TransactionId, NodeId, Target) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"target">>, Target}
    ],
    Request = krpc_request(TransactionId, <<"q">>, <<"find_node">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @private
%%  Do response to `find node` request. http://www.bittorrent.org/beps/bep_0005.html#find-node
%%
do_find_node_response(TransactionId, NodeId, Nodes) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"nodes">>, Nodes}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  Do `get peers` request. http://www.bittorrent.org/beps/bep_0005.html#get-peers
%%
do_get_peers(TransactionId, NodeId, InfoHash) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"info_hash">>, InfoHash}
    ],
    Request = krpc_request(TransactionId, <<"q">>, <<"get_peers">>, Args),
    erline_dht_bencoding:encode(Request).


%%  @private
%%  Do response to `get peers` request. http://www.bittorrent.org/beps/bep_0005.html#get-peers
%%
do_get_peers_response(TransactionId, NodeId, Token, Values) when is_list(Values) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"token">>, Token},
        {<<"values">>, {list, Values}}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response);

do_get_peers_response(TransactionId, NodeId, Token, Nodes) ->
    Args = [
        {<<"id">>, NodeId},
        {<<"token">>, Token},
        {<<"nodes">>, Nodes}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  Do `announce peer` request. http://www.bittorrent.org/beps/bep_0005.html#announce-peer
%%
do_announce_peer(TransactionId, NodeId, ImpliedPort, InfoHash, Port, Token) when
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


%%  @private
%%  Do response to `get peers` request. http://www.bittorrent.org/beps/bep_0005.html#announce-peer
%%
do_announce_peer_response(TransactionId, NodeId) ->
    Args = [
        {<<"id">>, NodeId}
    ],
    Response = krpc_request(TransactionId, <<"r">>, Args),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  http://www.bittorrent.org/beps/bep_0005.html#errors
%%
do_error_response(TransactionId, ErrorCode, ErrorDescription) ->
    Response = krpc_request(TransactionId, <<"e">>, [ErrorCode, ErrorDescription]),
    erline_dht_bencoding:encode(Response).


%%  @private
%%  http://bittorrent.org/beps/bep_0005.html#krpc-protocol
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


