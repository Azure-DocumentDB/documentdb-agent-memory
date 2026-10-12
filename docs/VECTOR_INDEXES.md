# Vector indexes and scoped recall

The adapters store embeddings supplied by the application; they do not call an embedding model. Stored and query vectors must have the configured dimensions, finite numeric values and the same embedding space. Synthetic vectors used by tests prove query behavior, not semantic relevance.

## Defaults and deployment support

The small-memory validation configuration uses an IVF index (`vector-ivf`, cosine similarity, `numLists=1`). This is a demonstration default, not production index tuning. Check the current service documentation and the specific environment before selecting a different index.

Earlier experiments in this project used local DocumentDB0.117 and a managed M10 cluster: IVF worked in both; HNSW worked locally but was rejected by M10; DiskANN was absent in that local build. Those observations are historical, not universal guarantees. Separate October12,2026 vector-sample tests verified IVF, HNSW and DiskANN on managed Azure DocumentDB M30. The earlier blanket M40 minimum statement is therefore not retained.

A local container is not a certification of managed-service support, or vice versa. An index kind rejected by the current server must remain an error; never silently replace it with another kind or report success.

## Both namespace and thread are filter scopes

Per-thread recall filters on both `namespace` and `thread_id`. Schema setup creates ordinary indexes for both filter fields before the vector index. It also creates the shared compound lifecycle indexes described in [SCHEMA_V2.md](SCHEMA_V2.md).

A successful filtered query only constrains the database operation. Your application must separately authenticate and authorize callers before selecting a namespace/thread. Do not let an untrusted request choose arbitrary tenant scopes or supply raw filter objects.

## Setup must be nondestructive

Setup verifies existing index definitions and schema compatibility. It must refuse conflicting vector dimensions/kinds and legacy thread-only uniqueness without dropping or recreating indexes. Use a new database for schema v2 and migrate existing deployments through a separately reviewed process.

The relevant index names, options, test deployment and result counts are recorded in the public validation record after live tests. No tier, performance or production-readiness claim follows from local unit tests alone.

## TLS and lifecycle

Managed Azure connections must verify certificates and hostnames. Explicit local development helpers may trust the local container's self-signed certificate only for loopback addresses; they are not a production TLS configuration.

Summary deletion is part of thread deletion in both adapters. Quiesce writers before deleting; whole-thread deletion across collections is not advertised as transactional with concurrent writers. Rerun a failed deletion and check completion rather than claiming an interrupted cleanup erased all records.
