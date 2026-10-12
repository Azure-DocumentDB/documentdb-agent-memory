# Shared storage contract (schema v2)

The Python and TypeScript adapters have separate implementations. They agree on a database schema and are tested together; this is not one shared runtime implementation.

## Scope and responsibility

Every lifecycle key and query is scoped by both `namespace` and `thread_id`. LangGraph's `checkpoint_ns` is an additional dimension below the storage namespace/thread. The application must authenticate users and choose the storage namespace/thread they are allowed to access. A namespace string or vector filter is **not** an authorization system against a caller with arbitrary database access.

Use a fresh database for this version. Legacy thread-only unique indexes and unscoped records are not silently migrated. Setup must reject incompatible schemas without dropping indexes or rewriting data. Back up and migrate a previous deployment separately; do not delete old indexes merely to suppress a setup error.

## Collections

- `sessions`: one record per namespace/thread, plus the TypeScript message sequence allocator.
- `messages`: ordered chat records scoped by namespace/thread/sequence.
- `summaries`: explicitly supplied text and vectors scoped by namespace/thread. No embedding model is called automatically.
- `checkpoints`: LangGraph checkpoint payloads scoped by namespace/thread/checkpoint namespace/checkpoint ID.
- `checkpoint_writes`: pending LangGraph task/channel writes within a checkpoint.

TypeScript doesn't interpret serialized LangGraph checkpoints. Shared lifecycle deletion can remove them, while ordinary message/summary records have a documented cross-language representation.

## Concurrency and deletion

Message appends allocate sequence ranges with one atomic server operation. Concurrent acknowledged appends must retain distinct ordered sequence values. Failed inserts can leave sequence gaps. Unless an API explicitly documents a request-id protocol, retrying after an unknown network outcome is not an exactly-once guarantee.

Stop/quiesce writes to a thread before deletion. Deletion spans the five collections and can be retried; it is not claimed to be a distributed transaction or linearizable with simultaneous writes. After successful deletion, all matching thread records are absent and other namespaces/threads remain untouched. A partial database failure must be surfaced rather than reported as complete deletion.

## Retrieval

Summary vector search filters by both namespace and thread. Ordinary filter indexes must exist before the vector index/search. Default IVF with `numLists=1` is appropriate for the small validation fixture, not a production tuning recommendation. Query vectors must be finite, have the configured dimensions and share the same embedding space as stored vectors.

## Verification boundary

Genuine LangGraph graph and AI SDK application tests, database tests and mock-only tests are labeled separately in the release validation record. No live language-model call is needed to prove checkpointing or message persistence. This schema document alone does not establish any Azure/local deployment compatibility or production durability guarantee.
