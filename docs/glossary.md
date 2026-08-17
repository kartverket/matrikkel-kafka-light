# Glossary

## Core Domain Terms

**Topic**  
A named, append-only log of records. A topic is the primary distribution boundary in the system, and ordering is defined within a topic.

**Record**  
The concrete unit stored in the distribution service. A record consists of the payload plus system and application metadata, and is assigned a sequence number within a topic.

**Payload**  
The actual data content carried in a record. Payloads may be large and are stored fully in PostgreSQL as BLOBS.

**Metadata**  
Additional information attached to a record alongside the payload. Typical examples include producer identity, timestamps, correlation identifiers, content type, and custom headers.

**Sequence Number**  
A monotonically increasing number assigned to each record within a topic. Sequence numbers define the global order of records in that topic.

**Offset**  
A consumer group's current position in a topic. The offset determines which record will be delivered next to that consumer group.

**Lease**  
The exclusive right for one consumer instance to poll and commit records for a given topic and consumer group. The lease is enforced by the distribution service.
A lease is given to a single consumer instance for each consumer group per topic.
Leases expire after a configurable time, and needs to be refreshed periodically by the consumer.

**Serde**  
Stands for "**Ser**ialization/**De**serialization", and is responsible for converting record payloads to and from byte arrays which can be stored.

**Partition (OUT-OF-SCOPE)**  
A subdivision of a topic used in systems such as Kafka to increase parallelism. Partitions are explicitly out of scope because this system requires global ordering within a topic.
This is not supported, nor planned for the future.

## Actors

**Producer**  
An application that publishes records to a topic. Producers do not control delivery to consumers directly; they only append records to the distribution service.

**Consumer**  
An application that reads records from a topic through a consumer group. Consumers are responsible for their own processing logic and idempotency.

**Consumer Group**  
The logical subscriber identity for a consuming application. Each consumer group receives every record on a topic independently of other consumer groups and maintains its own offset.
This could typically be the application name of the consumer.

**Consumer Instance**  
A single running process or pod belonging to a consumer group. Multiple instances may exist for failover, but only one instance is active per topic (e.g has acquired the **lease**) and consumer group at a time.
This could typically be the pod-name of the application. 

## Behavioral Semantics

**At-least-once Delivery**  
A delivery guarantee where a persisted record will not be lost, but may be delivered more than once in failure scenarios. Consumers must therefore tolerate duplicate delivery.

**Per-topic Global Ordering**  
The guarantee that records in a topic are processed in the exact order defined by their sequence numbers. This ordering applies independently for each consumer group.

**Tombstone**
A record with `payload = NULL` that represents deletion of keyed state when the topic allows tombstones. 
~~On delete topics, tombstones are retained like ordinary records. On compact topics, the latest tombstone for a key is kept as the retained delete marker.~~ (When compaction is implemented)

**Compaction (planned)**
Policies for how old data should be retained or deleted.  
Delete policy: Remove records by retention age, retained bytes, or both. 
Compact policy: Keeps just latest record for any given `recordKey`.