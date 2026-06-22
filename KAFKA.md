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
- consumer groups - ?

Quick Hierarchy

- *cluster* has many ─►  brokers (servers)
- *topic* split into ─►  partitions
- *partition* lives on (leader + replicas) ─►  brokers
- *key* hashes to ─►  one partition

## Ordering

- ordering is guaranteed only *within* a partition, not across a topic
- this is why keying by `engine_id` matters: it keeps each engine's events in one partition, in order, while different engines spread across partitions for parallelism

## Questions

How do I determine # of partitions within a topic? Balance four forces:

1. **Parallelism** — at most one consumer per group per partition, so partition count caps how many consumers can share the load (extra consumers sit idle). Rough sizing: `partitions = ceil(target_throughput / per-partition rate)`, plus headroom.
2. **Key cardinality** — you can't have more *active* partitions than distinct keys; the surplus sits empty. And with few keys, hashing is lumpy — some partitions get none, others get several.
3. **Cost of too many** — each partition costs file handles, memory, replication traffic, and slower rebalances (and on managed clusters, counts toward limits and billing). More isn't free; don't over-provision "to be safe."
4. **One-way + the ordering trap** — you can add partitions but never remove them, and adding them breaks per-key ordering: the mapping is `hash(key) % N`, so changing `N` reshuffles keys onto different partitions and splits a key's history across the change. For a keyed topic, pick a count you won't need to grow.

I have some messages in a topic that I want deleted. They're bad or stale. How do I purge them?
- use the delete records api call. but what does this do?
- this moves the log start offset of each partition to whatever value you give it. If you move it to the current offset, the consumers will have nothing to consume and the records will therefore be effectively deleted. done.
- at this point, we're still technically waiting for Kafka to actually remove the messages entirely from the cluster, and that occurs when enough messages build up to deserve a full "segment" delete. But all this happens in the backgorund. After the offset moves, the messages are effectively purged.

How long should messages be retained in a topic?
- well, we pay for storage on a cluster, so if messages build up and sit, the bill could get very large very fast. So set it low?
- setting it low also has risks. if consumers are all doing their job, you don't need a long window because messages will be gone and offset moved. but we all know that consumers can go down. In that instance, if consumers go down for 24 hours, you might not want valuable messages expiring on the topic.

How does Postgres differ from Snowflake?
- **OLTP** (transactional databases — Postgres, MySQL): built for lots of tiny, fast reads/writes. "Fetch *this one* engine's current status." Millisecond, high-concurrency, per-request.
- **OLAP** (analytical warehouses — Snowflake): built to **scan and aggregate billions of rows**. "Average speed across the whole fleet for six months." Seconds-to-minutes, heavy, columnar, not meant for thousands of concurrent tiny lookups.

Snowflake is firmly OLAP. So it *is* a database, but it's the analytical kind — optimized for big sweeping questions, not for serving a single user's button-click instantly.

What is tiered storage in a cluster?
- Kafka keeps recent ("hot") segments on fast local broker disk and offloads older ("cold") segments to cheap remote object storage (e.g. S3).
- Consumers read either tier transparently, so you can keep a long retention window without paying for expensive local disk.



## Avro

*why do we need this?*

- Size - JSON record repeats every field name in every message — {"engine_id": "thomas", "speed_mph": 22, ...} ships those keys thousands of times. Avro strips the field names out of the payload entirely. The message on the wire is just the values, tightly packed in binary, plus a tiny header. For high-volume telemetry, that's often a 5–10× shrink.
- Governance. the field names/types live in a schema instead of the payload, so you get a typed contract. Bad data (wrong type, missing required field) gets rejected at serialization time instead of silently downstream.

## Confluent

- holds the Kafka streaming cluster, the topic, the partitions, etc.
- also holds the schema registry, a collection of schemas (payload blueprints for messages) that both the produer and the consumer use.

