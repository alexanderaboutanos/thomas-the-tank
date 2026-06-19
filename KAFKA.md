# Kafka 

*summary of terms and ordering logic in kafka*

## Terms

- broker - single Kafka server (a machine/process)
- cluster - several brokers together
- topic - named data stream living on a cluster (note: multiple topics can exist on a single cluster)
- partition - a topic is split into partitions; each partition has replicas across several brokers (one leader + N followers). simultaneously the unit of *parallelism* and the unit of *ordering*.
- replicas 
    - leader - the one replica that handles all reads/writes for that partition
    - follower - copies the leader's log to stay in sync; never serves clients (promoted to leader if the leader's broker dies)
- key - hashes to a partition (`hash(key) % num_partitions`); same key → same partition → preserves per-key ordering (here the key is `engine_id`, e.g. `Thomas`)

Quick Hierarchy
- *cluster* has many ─►  brokers (servers)
- *topic* split into ─►  partitions
- *partition* lives on (leader + replicas) ─►  brokers
- *key* hashes to ─►  one partition

## Ordering

- ordering is guaranteed only *within* a partition, not across a topic
- this is why keying by `engine_id` matters: it keeps each engine's events in one partition, in order, while different engines spread across partitions for parallelism

## Question

How do I determine # of partitions within a topic? Balance four forces:

1. **Parallelism** — at most one consumer per group per partition, so partition count caps how many consumers can share the load (extra consumers sit idle). Rough sizing: `partitions = ceil(target_throughput / per-partition rate)`, plus headroom.
2. **Key cardinality** — you can't have more *active* partitions than distinct keys; the surplus sits empty. And with few keys, hashing is lumpy — some partitions get none, others get several.
3. **Cost of too many** — each partition costs file handles, memory, replication traffic, and slower rebalances (and on managed clusters, counts toward limits and billing). More isn't free; don't over-provision "to be safe."
4. **One-way + the ordering trap** — you can add partitions but never remove them, and adding them breaks per-key ordering: the mapping is `hash(key) % N`, so changing `N` reshuffles keys onto different partitions and splits a key's history across the change. For a keyed topic, pick a count you won't need to grow.
