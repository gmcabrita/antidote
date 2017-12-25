# IntraDC in Antidote

## Basic setup

Generates a consensus group for each partition (using the riak core ring's unique preflists).

The default leader for each partition is the primary `{partition(), node()}` in the preflist for the given partition.

Each group does replication in a way similar to chain replication where the leader contacts the next node in the chain, and the second node contacts the following node, etc.

As antidote uses riak core, a vnode for a given partition is only expected to run on its primary node (according to the preflist) so unlike in classic chain replication read operations must contact the the head of the chain (the leader of the group) instead of the last node in the chain. This must to be done because the materializer for a given partition must be running at the primary node of that partition's preference list -- otherwise riak core will automatically run hinted handoffs to migrate the vnode state over to where it should be (the primary).

## Failure modes

To detect node faults (crash, network partitions, etc) we issue heartbeats between nodes.

When a group leader fails it is replaced by the next node in the chain that is currently functioning. Once this node fully recovers it retakes its place and as leader and receives its missed operations (log) and state (materializer) via hinted handoff from the node which was replacing it.

When a node in the middle of a chain (a follower) fails it is simply taken off the chain until it recovers. Once it recovers it is put back into the chain and requests its missing operations (log) from its group primary.

## Local deployments for testing

While there is a very basic test suite for the IntraDC replication (in test/intra_dc_SUITE.erl) it is also interesting to run local deployments for quick experiments.

The deployment a local cluster can be done as follows:

```bash
# build releases, one for each node
./bin/build_releases.sh 5
# launch each of the nodes
./bin/launch-nodes.sh 5
# join the nodes into an antidote cluster
./bin/join_cluster_script.erl antidote1@127.0.0.1 antidote2@127.0.0.1 antidote3@127.0.0.1 antidote4@127.0.0.1 antidote5@127.0.0.1
# required for finishing the cluster join
./bin/join_dcs_script.erl antidote1@127.0.0.1
```

After this you will have a 5 node local cluster deployment and can issue transactions via rpc. Some escripts are available in `./txn.erl` and `./txn_ccrdt.erl` which use erlang rpc to execute transactions for quick experiments.
