ErLine DHT
=====

- [Introduction](#introduction)
- [Implementation details](#implementation_details)
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

## <a name="events">Events</a> ##

* ```{ping, q, Ip :: inet:ip_address(), Port :: inet:port_number(), NodeHash :: binary()}```
* ```{ping, r, Ip :: inet:ip_address(), Port :: inet:port_number(), NodeHash :: binary()}```
* ```{find_node, q, Ip :: inet:ip_address(), Port :: inet:port_number(), {NodeHash :: binary(), Target :: binary()}}```
* ```{find_node, r, Ip :: inet:ip_address(), Port :: inet:port_number(), Nodes :: [#{ip => inet:ip_address(), port => inet:port_number(), hash => binary()}]}```
* ```{get_peers, q, Ip :: inet:ip_address(), Port :: inet:port_number(), {NodeHash :: binary(), InfoHash :: binary()}```
* ```{get_peers, r, Ip :: inet:ip_address(), Port :: inet:port_number(), {nodes, InfoHash :: binary(), Nodes :: [#{ip => inet:ip_address(), port => inet:port_number(), hash => binary()}]}}```
* ```{get_peers, r, Ip :: inet:ip_address(), Port :: inet:port_number(), {peers, InfoHash :: binary(), Peers :: [#{ip => inet:ip_address(), port => inet:port_number()}]}}```
* ```{announce_peer, q, Ip :: inet:ip_address(), Port :: inet:port_number(), {ImpliedPort :: 0 | 1, InfoHash :: binary(), PeerPort :: inet:port_number(), Token :: binary()}}```
* ```{announce_peer, r, Ip :: inet:ip_address(), Port :: inet:port_number(), NodeHash :: binary()}```
* ```{error, r, Ip :: inet:ip_address(), Port :: inet:port_number(), {ErrorCode :: 201 | 202 | 203 | 204, ErrorReason :: binary()}```

## <a name="status">Status</a> ##

Working is in progress.

## <a name="tests">Tests</a> ##

EUnit tests
```
$ make tests
```