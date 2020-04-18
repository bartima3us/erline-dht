ErLine DHT
=====

- [Introduction](#introduction)
- [Implementation details](#implementation_details)
- [Usage](#usage)
- [Config](#config)
- [Events](#events)
- [Status](#status)
- [Tests](#tests)

## <a name="introduction">Introduction</a> ##

Kademlia based Mainline DHT (BEP5: https://www.bittorrent.org/beps/bep_0005.html) implementation in Erlang.<br/>
Derivative project from https://github.com/bartima3us/erl-bittorrent

## <a name="implementation_details">Implementation details</a> ##

* Buckets can contain K nodes where by default K=8.
* If bucket is full, all other nodes will be placed in the pseudo-bucket - not assigned nodes list.
* Every node in the bucket is pinged every 6-12 min (random amount from the range).
* Every bucket is checked every 1-3 min (random amount from the range).
* If during the check some `active` node last respond was more than 14 min ago - that node becomes `suspicious` and it is tried to ping one more time.
* If during the check some `suspicious` node last respond was more than 15 min ago - that node becomes `not active`.
* If node in the bucket becomes `not active`, it can be automatically replaced with `active` node from not assigned nodes list.

## <a name="usage">Usage</a> ##

Add a new node with unknown hash to the bucket:
```
erline_dht:add_node_to_bucket(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number()
) -> ok
```

Add a new node with known hash to the bucket (if bucket is full, node will be added to the not assigned nodes list):
```
erline_dht:add_node(
    Ip      :: inet:ip_address(),
    Port    :: inet:port_number(),
    Hash    :: binary()
) -> ok.
```

Try to find peers for the info hash in the network:
```
erline_dht:get_peers(
    InfoHash :: binary()
) -> ok.
```

Try to get peers for the info hash from one node:
```
erline_dht:get_peers(
    Ip       :: inet:ip_address(),
    Port     :: inet:port_number(),
    InfoHash :: binary()
) -> ok.
```

Return all nodes information in the bucket (`distance()` will be: `0..erlang:bit_size(YourNodeHash)` or by default: `0..160`):
```
erline_dht:get_all_nodes_in_bucket(
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        status          => active | suspicious | not_active,
        last_changed    => calendar:datetime()}].
```

Return all not assigned nodes information:
```
erline_dht:get_not_assigned_nodes() ->
    [#{ip              => inet:ip_address(),
       port            => inet:port_number(),
       hash            => binary(),
       last_changed    => calendar:datetime()}].
```

Return not assigned nodes information.
```
erline_dht:get_not_assigned_nodes(
    Distance :: distance()
) -> [#{ip              => inet:ip_address(),
        port            => inet:port_number(),
        hash            => binary(),
        last_changed    => calendar:datetime()}].
```

Return amount of nodes in every bucket:
```
erline_dht:get_buckets_filling() -> [#{distance => distance(), nodes => non_neg_integer()}].
```

Return UDP socket port of the client:
```
erline_dht:get_port() -> Port :: inet:port_number().
```

Return event manager pid:
```
erline_dht:get_event_mgr_pid() -> EventMgrPid :: pid()
```

## <a name="config">Config</a> ##

Default sys.config:
```
    {erline_dht, [
        {auto_bootstrap_nodes, [
            {"router.bittorrent.com", 6881},
            {"dht.transmissionbt.com", 6881},
            {"router.utorrent.com", 6881},
            {"router.bitcomet.com", 6881},
            {"dht.aelitis.com", 6881}
        ]},
        {db_mod, erline_dht_db_ets},
        {limit_nodes, true},
        {k, 8},
        {port, 0},
        {node_hash, 20}
    ]}
```

* ```auto_bootstrap_nodes``` - List of initial nodes used in bootstrapping process just after ErLineDHT start.
* ```db_mod``` - Module used for database queries encapsulation. ErLineDHT uses ETS by default but it can be easily changed by another engine.
* ```limit_nodes``` - If true - ErLine DHT will clear some old nodes from time to time. If false - ErLine DHT will keep all known nodes. **Warning!** Since there are approximately 10-25 million of Mainline DHT network users, keeping all nodes may consume a lot of memory.
* ```k``` - Amount of nodes in a single bucket.
* ```port``` - If 0 - ErLineDHT will open any free port. If non zero - first ErLineDHT will try to use specified port but in case it's already in use, ErLineDHT will open socket on any free port.
* ```node_hash``` - If integer() - ErLineDHT will generate random node hash by specified amount. If binary() - ErLineDHT will use specified node hash.

## <a name="events">Events</a> ##

All received queries and responses can be subscribed.

Get event manager pid and attach event handler

```
EventMgrPid = erline_dht:get_event_mgr_pid().
gen_event:add_handler(EventMgrPid, your_dht_event_handler, []).
```

Events which should be handled in the attached handler:

Received ping query from the node:
* ```{ping, q, Ip :: inet:ip_address(), Port :: inet:port_number(), NodeHash :: binary()}```

Received ping response from the node:
* ```{ping, r, Ip :: inet:ip_address(), Port :: inet:port_number(), NodeHash :: binary()}```

Received find_node query from the node:
* ```{find_node, q, Ip :: inet:ip_address(), Port :: inet:port_number(), {NodeHash :: binary(), Target :: binary()}}```

Received find_node response from the node:
* ```{find_node, r, Ip :: inet:ip_address(), Port :: inet:port_number(), {NodeHash :: binary(), Nodes :: [#{ip => inet:ip_address(), port => inet:port_number(), hash => binary()}]}}```

Received get_peers query from the node:
* ```{get_peers, q, Ip :: inet:ip_address(), Port :: inet:port_number(), {NodeHash :: binary(), InfoHash :: binary()}```

Received get_peers response with nodes list from the node:
* ```{get_peers, r, Ip :: inet:ip_address(), Port :: inet:port_number(), {nodes, NodeHash :: binary(), InfoHash :: binary(), Nodes :: [#{ip => inet:ip_address(), port => inet:port_number(), hash => binary()}]}}```

Received get_peers response with peers list from the node:
* ```{get_peers, r, Ip :: inet:ip_address(), Port :: inet:port_number(), {peers, NodeHash :: binary(), InfoHash :: binary(), Peers :: [#{ip => inet:ip_address(), port => inet:port_number()}]}}```

Received announce_peer query from the node:
* ```{announce_peer, q, Ip :: inet:ip_address(), Port :: inet:port_number(), {NodeHash :: binary(), ImpliedPort :: 0 | 1, InfoHash :: binary(), PeerPort :: inet:port_number(), Token :: binary()}}```

Received announce_peer response from the node:
* ```{announce_peer, r, Ip :: inet:ip_address(), Port :: inet:port_number(), NodeHash :: binary()}```

Received error response from the node:
* ```{error, r, Ip :: inet:ip_address(), Port :: inet:port_number(), {ErrorCode :: 201 | 202 | 203 | 204, ErrorReason :: binary()}```

## <a name="status">Status</a> ##

Working is in progress.

## <a name="tests">Tests</a> ##

EUnit and CT tests
```
$ make tests
```