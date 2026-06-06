Our project checkpoint


The particular software artifact you intend to complete. 

At a high level, our goal is to build a Dynamo inspired distributed key-value store in Rust with high write availability and eventual consistency under node failures. In our proposal we planned on making a consistent hash ring, replication with sloppy quorums and hinted handoffs as well as gossiping between does to know who is alive. We planned on simplifying Dynamo’s more complex components by forgoing vector clocks and merkle tree based anti entropy. To date we have a working distributed KV store with an HTTP interface, quorum based reads and writes with modifiable N, W and R parameters, last writer wins conflict resolution using physical timestamps, read repairs, hinted handoffs, and a background reconciliation mechanism triggered by changes in a live set of nodes. Dynamo also supports explicit node joining and leaving the cluster through admin commands. We chose to simply forgo this feature as well in order to simplify our design, although our system can be extended to support this feature as well.


High level description of design

	Our design was inspired from the labs with a layered approach. The underlying storage we use is RocksDB with a coordinator logic that sits above it. We have a StorageEngine trait with basic key value storage functions and have RocksDBStore and ReplicatedStore so that we can easily swap between the two. All of this is abstracted from the client. Each value stored in RocksDB is versioned using a physical timestamp which is generated at write time using SystemTimestamp used in LWW conflict resolution.  We expose seed nodes to clients and these seed nodes handle the coordination for any requests for the cluster. On request, the coordinator computes the primary node of the key in the request and then constructs a preference list to use as quorum for servicing the requests.


More specifically a PUT happens in 2 phases; first the coordinator attempts to write to all N preferred nodes in the preference list. If any of the writes to the preferred N nodes fails, the coordinator attempts to supplement these failed calls by durably storing hints on subsequent nodes on the hash ring. A PUT is only successful if it gets W ACKs from either preferred nodes or from writing hints. These are the unmodified write semantics from Dynamo.


A GET happens in 4 phases;  the coordinator reads from all N preferred replicas in parallel and waits until it has at least R successful responses (either key is present or key is not present). After collecting responses, the coordinator applies LWW by picking the VersionedValue with the highest timestamp among the replicas that returned an actual value. It then performs read repair in the background by sending a versioned write to any replica that is missing or stale relative to the winner, using a conditional “write-if-newer” mechanism to avoid overwriting a fresher concurrent write. In Dynamo, read repair is actually performed by the client making the GET request. In the event there are multiple different versions of a value for a key, GET returns all of them to the client and the client is expected to issue a PUT to reconcile the different versions. If we decide to implement vector clocks (if time allows), we will revise our GET implementation to mirror that of Dynamo.


Currently, hint creation on writes and delivery are implemented, but key range migration/transfer between nodes for more permanent failures are not implemented. There are also likely some subtle race conditions surrounding hint delivery and concurrent writes that we need to iron out. Finally, we need to perform more rigorous correctness testing of the system as a whole for various N,R,W parameters and cluster sizes.


Every server has a randomly assigned UUID on startup which we use to distinguish between temporary failures and restarts. This is important because it dictates whether sending hints to that node is enough or if we need to start a key range migration to the fresh new node. Memberlist handles propagating node UUIDs through its node metadata api. We simply make sure to maintain a set of UUIDs for each node, and when a node comes back to life, we check against our current snapshot of UUIDs to determine whether the node died and came back or was simply not responsive from some transient error.


The main software packages we leveraged during implementation are RocksDB, memberlist and hashring_coordinator. We moved away from the hashring created and ended up using hashring_coordinator instead because it had extra functions we were interested in like get_hash_rangess() to use for migration purposes and the ring.get() function which returned all nodes that store the key. We used memberlist for gossiping node liveness state for preferences list.


Your evaluation plan for this artifact. How will you measure success? What will you use for a testbed? Do you need any resources from us? 


Our evaluation metrics remain from the proposal.

Baseline performance in a healthy cluster

Write success rate under failure, ie killing 1,2,3... node(s) and performing writes. How close can we get to thrashing the system while maintaining read availability

Recovery speed, bringing the dead node back (data still intact) and measuring how long it takes for hints to propagate and the replica comes back up to speed

Latency and write success rate while varying R/W parameters


In addition to metric (3), we will also measure recovery speed when previously dead nodes have experienced some partial or total data loss. While measuring recovery speed with data intact is more a measure of hint delivery speed, recovery speed with data loss also measures the system’s speed in transferring key ranges between nodes.


We plan to roll our own testbed. We really only have key range migration, correctness testing, and evaluation left to do. We imagine correctness testing and building a sane harness for doing testing and evaluations will take up most of our time. As mentioned in class, distributed systems are notoriously difficult to test.