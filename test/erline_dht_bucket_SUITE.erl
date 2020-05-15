%%%-------------------------------------------------------------------
%%% @author bartimaeus
%%% @copyright (C) 2020, sarunas.bartusevicius@gmail.com
%%% @doc
%%% Bucket integration testing.
%%% @end
%%% Created : 11. May 2020 21.44
%%%-------------------------------------------------------------------
-module(erline_dht_bucket_SUITE).
-author("bartimaeus").

%% API
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_ping/1
]).

%%%===================================================================
%%% API for CT.
%%%===================================================================

%%  @doc
%%  List of test cases.
%%
all() ->
    [
        test_ping
    ].


%%  @doc
%%  Init.
%%
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erline_dht),
    Config.


%%  @doc
%%  Clear.
%%
end_per_suite(_Config) ->
    application:stop(erline_dht),
    ok.



%%%===================================================================
%%% Testcases.
%%%===================================================================

%%  @doc
%%  Start, do ping, stop.
%%
test_ping(_Config) ->
    ok = meck:new(erline_dht_event_handler, [passthrough]),
    % Start nodes
    {ok, PidNode1} = erline_dht_bucket:start_link(node1, 0),
    {ok, PidNode2} = erline_dht_bucket:start_link(node2, 0),
    {ok, PidNode3} = erline_dht_bucket:start_link(node3, 0),
    % Get port
    PortNode2 = erline_dht_bucket:get_port(PidNode2),
    PortNode3 = erline_dht_bucket:get_port(PidNode3),
    % Get hash
    HashNode2 = erline_dht_bucket:get_hash(PidNode2),
    HashNode3 = erline_dht_bucket:get_hash(PidNode3),
    % Node1 event manager
    EventMgrPid1 = erline_dht:get_event_mgr_pid(node1),
    gen_event:add_handler(EventMgrPid1, erline_dht_event_handler, []),
    ok = erline_dht_bucket:add_node(node1, {127,0,0,1}, PortNode2),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode2, HashNode2}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode2, HashNode2}, '_']),
    % Node2 event manager
    EventMgrPid2 = erline_dht:get_event_mgr_pid(node2),
    gen_event:add_handler(EventMgrPid2, erline_dht_event_handler, []),
    ok = erline_dht_bucket:add_node(node2, {127,0,0,1}, PortNode3),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode3, HashNode3}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode3, HashNode3}, '_']),
    %
    true = meck:validate(erline_dht_event_handler),
    ok = meck:unload(erline_dht_event_handler),
    % Check whether each node has the other node in it's bucket
    2 = lists:foldl(fun(#{nodes := Nodes}, AccTotal) ->
        AccTotal + Nodes
    end, 0, erline_dht_bucket:get_buckets_filling(node1)),
    2 = lists:foldl(fun(#{nodes := Nodes}, AccTotal) ->
        AccTotal + Nodes
    end, 0, erline_dht_bucket:get_buckets_filling(node2)),
    % Stop the nodes
    ok = erline_dht_bucket:stop(PidNode1),
    ok = erline_dht_bucket:stop(PidNode2),
    ok = erline_dht_bucket:stop(PidNode3),
    ok.


