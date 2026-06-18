# Kafka 

*summary of terms and ordering logic in kafka*

## Terms

- broker - single Kafka server (a machine/process)
- cluster - several brokers together
- topic - named data stream living on a cluster (note: multiple topics can exist on a single cluster)
- partition - a topic is split into partitions; each partition has replicas across several brokers (one leader + N followers)
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
