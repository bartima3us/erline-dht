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
    test_basic/1
]).

%%%===================================================================
%%% API for CT.
%%%===================================================================

%%  @doc
%%  List of test cases.
%%
all() ->
    [
        test_basic
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
%%  Start, do ping, do find_node, stop.
%%
test_basic(_Config) ->
    ok = meck:new(erline_dht_event_handler, [passthrough]),
    %
    % Start nodes
    {ok, PidNode1} = erline_dht_bucket:start_link(node1, 0),
    {ok, PidNode2} = erline_dht_bucket:start_link(node2, 0),
    {ok, PidNode3} = erline_dht_bucket:start_link(node3, 0),
    %
    % Get port
    PortNode1 = erline_dht_bucket:get_port(PidNode1),
    PortNode2 = erline_dht_bucket:get_port(PidNode2),
    PortNode3 = erline_dht_bucket:get_port(PidNode3),
    %
    % Get hash
    HashNode1 = erline_dht_bucket:get_hash(PidNode1),
    HashNode2 = erline_dht_bucket:get_hash(PidNode2),
    HashNode3 = erline_dht_bucket:get_hash(PidNode3),
    %
    % Node1 event manager
    EventMgrPid1 = erline_dht:get_event_mgr_pid(node1),
    gen_event:add_handler(EventMgrPid1, erline_dht_event_handler, []),
    %
    % Node2 event manager
    EventMgrPid2 = erline_dht:get_event_mgr_pid(node2),
    gen_event:add_handler(EventMgrPid2, erline_dht_event_handler, []),
    %
    % Node3 event manager
    EventMgrPid3 = erline_dht:get_event_mgr_pid(node3),
    gen_event:add_handler(EventMgrPid3, erline_dht_event_handler, []),
    %
    % Add node2 to node1 bucket
    ok = erline_dht_bucket:add_node(node1, {127,0,0,1}, PortNode2),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode2, HashNode2}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode2, HashNode2}, '_']),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{ping, r, {127,0,0,1}, PortNode1, HashNode1}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{ping, r, {127,0,0,1}, PortNode1, HashNode1}, '_']),
    %
    % Find node request from node1 to node2
    ok = meck:wait(erline_dht_event_handler, handle_event, [{find_node, q, {127,0,0,1}, PortNode2, {HashNode2, HashNode2}}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{find_node, q, {127,0,0,1}, PortNode2, {HashNode2, HashNode2}}, '_']),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{find_node, r, {127,0,0,1}, PortNode1, {HashNode1, []}}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{find_node, r, {127,0,0,1}, PortNode1, {HashNode1, []}}, '_']),
    %
    % Add node3 to node2 bucket
    ok = erline_dht_bucket:add_node(node2, {127,0,0,1}, PortNode3),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode3, HashNode3}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{ping, q, {127,0,0,1}, PortNode3, HashNode3}, '_']),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{ping, r, {127,0,0,1}, PortNode2, HashNode2}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{ping, r, {127,0,0,1}, PortNode2, HashNode2}, '_']),
    %
    % Find node request from node2 to node3
    ok = meck:wait(erline_dht_event_handler, handle_event, [{find_node, q, {127,0,0,1}, PortNode3, {HashNode3, HashNode3}}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{find_node, q, {127,0,0,1}, PortNode3, {HashNode3, HashNode3}}, '_']),
    ok = meck:wait(erline_dht_event_handler, handle_event, [{find_node, r, {127,0,0,1}, PortNode2, {HashNode2, [#{hash => HashNode1, ip => {127,0,0,1}, port => PortNode1}]}}, '_'], 10000),
    1 = meck:num_calls(erline_dht_event_handler, handle_event, [{find_node, r, {127,0,0,1}, PortNode2, {HashNode2, [#{hash => HashNode1, ip => {127,0,0,1}, port => PortNode1}]}}, '_']),
    %
    % Validate mock
    true = meck:validate(erline_dht_event_handler),
    ok = meck:unload(erline_dht_event_handler),
    %
    % Check whether each node has the other node in it's bucket
    2 = lists:foldl(fun(#{nodes := Nodes}, AccTotal) ->
        AccTotal + Nodes
    end, 0, erline_dht_bucket:get_buckets_filling(node1)),
    2 = lists:foldl(fun(#{nodes := Nodes}, AccTotal) ->
        AccTotal + Nodes
    end, 0, erline_dht_bucket:get_buckets_filling(node2)),
    2 = lists:foldl(fun(#{nodes := Nodes}, AccTotal) ->
        AccTotal + Nodes
    end, 0, erline_dht_bucket:get_buckets_filling(node3)),
    %
    % Stop the nodes
    ok = erline_dht_bucket:stop(PidNode1),
    ok = erline_dht_bucket:stop(PidNode2),
    ok = erline_dht_bucket:stop(PidNode3),
    ok.


