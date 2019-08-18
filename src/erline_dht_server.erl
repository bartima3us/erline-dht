%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2019, sarunas.bartusevicius@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 16. Aug 2019 17.02
%%%-------------------------------------------------------------------
-module(erline_dht_server).
-author("bartimaeus").

%% API
-export([
    start/0
]).

start() ->
%%    Ping = <<"{\"t\":\"aa\", \"y\":\"q\", \"q\":\"ping\", \"a\":{\"id\":\"abcdefghij0123456789\"}}">>,
%%    Ping = [{"t", "aa"}, {"y", "q"}],
%%    Ping = <<"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe">>,
    IdDict = dict:store("id", "abcdefghij0123456789", dict:new()),
    Dict0 = dict:store("t", "aa", dict:new()),
    Dict1 = dict:store("y", "q", Dict0),
    Dict2 = dict:store("q", "ping", Dict1),
    Dict3 = dict:store("a", {dict, IdDict}, Dict2),
    Ping = {dict, Dict3},
    io:format("~p~n", [erline_dht_bencoding:encode(Ping)]),
    ok.