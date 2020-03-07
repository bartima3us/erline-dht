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
    socket_passive/1
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


