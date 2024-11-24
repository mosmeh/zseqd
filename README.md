# zseqd

[![build](https://github.com/mosmeh/zseqd/workflows/build/badge.svg)](https://github.com/mosmeh/zseqd/actions)

A distributed monotonically increasing sequence generator

## Features

- Monotonically increasing sequence numbers without large gaps or any duplicates
- Durable and highly available
- Low-latency in a geographically distributed environment

## How it works

Each node acquires a *lease* on a key before incrementing the sequence number. The lease is used to ensure that only one node has the exclusive access to the key at any given time, allowing the nodes to generate sequence values without coordination. When clients connect to their nearby node, this allows sequence generation without the latency of cross-region coordination.

The lease is automatically renewed by the node that holds it. If the node goes down, the lease will expire and another node will take over the sequence.

## Usage

zseqd uses etcd as the backend. Start the servers with the following commands:

```sh
etcd
cargo run -- --bind 127.0.0.1:12345 # Launch as many instances as you want
```

zseqd uses Redis protocol to communicate with clients and implements a subset of the Redis commands and some custom commands.

You can use `redis-cli` to interact with the server. For redirection to different nodes, cluster mode (`-c`) must be enabled:

```sh
redis-cli -c -p 12345

127.0.0.1:12345> INCR myseq
(integer) 1
127.0.0.1:12345> INCR myseq
(integer) 2
127.0.0.1:12345> INCRBY myseq 10
(integer) 12
```

When another node is holding the lease, the client will be redirected to the lease holder node:

```sh
redis-cli -c -p 54321

127.0.0.1:54321> INCR myseq
-> Redirected to slot [0] located at 127.0.0.1:12345
(integer) 13
```

You can explicitly revoke a lease on a key and allow another node to take over with `LEASE.REVOKE`:

```
127.0.0.1:12345> LEASE.REVOKE myseq
1
```
