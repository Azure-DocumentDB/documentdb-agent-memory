# langgraph-checkpoint-documentdb

LangGraph checkpointer backed by **Azure DocumentDB**. Develop against a local
container, ship to the managed service by changing one environment variable.

```bash
pip install langgraph-checkpoint-documentdb
```

## Quickstart

Start a local DocumentDB:

```bash
export DOCUMENTDB_USERNAME=appuser
export DOCUMENTDB_PASSWORD='choose-a-strong-password'
docker compose -f docker/docker-compose.yml up -d
```

Use it:

```python
from langgraph_checkpoint_documentdb import DocumentDBSaver, build_local_uri

uri = build_local_uri("appuser", "choose-a-strong-password")
saver = DocumentDBSaver.from_uri(uri)
saver.setup()   # creates collections + indexes, idempotent

saver.put("thread-1", "ckpt-1", {"channel_values": {"count": 1}}, {"step": 1})
print(saver.get("thread-1"))
```

Moving to Azure DocumentDB is a connection-string change:

```python
saver = DocumentDBSaver.from_uri(os.environ["DOCUMENTDB_URI"])
```

Get that URI with:

```bash
az cosmosdb mongocluster show -c <cluster> -g <group> \
  --query properties.connectionString -o tsv
```

> The local container serves a self-signed certificate, so `build_local_uri()`
> disables certificate validation. Never use it against the managed service,
> which presents a real certificate.

## Vector index kinds: read this before you deploy

`DocumentDBSaver` defaults to **IVF**, deliberately. IVF is the only kind that
works on both the local container and the entry-level managed tier.

| Kind | Local container | Managed Azure DocumentDB |
|---|---|---|
| `vector-ivf` *(default)* | supported | M10 and up |
| `vector-hnsw` | supported | **M30 and up** |
| `vector-diskann` | **not supported** | **M40 and up** |

The trap is HNSW: it works locally and is then rejected by an M10 cluster with
`hnsw index is not supported for this cluster tier`. Local is a *superset* of a
small managed cluster, so "it worked on my machine" is not evidence it will
deploy. Opting into a non-default kind means committing to a tier:

```python
from langgraph_checkpoint_documentdb import MemoryConfig, VectorIndexSpec

config = MemoryConfig(
    embedding_dimensions=1536,
    vector_index=VectorIndexSpec(
        path="embedding", dimensions=1536, kind="vector-hnsw"  # needs M30+
    ),
)
```

If the cluster rejects it you get a `VectorIndexUnsupported` naming the required
tier, rather than the raw server message.

## Pre-filtered search is handled for you

Filtering a vector search by field — "search only this thread's memories" —
requires a plain index on the filter field, created *before* the search runs.
Without it the server returns `The index for filter path 'thread_id' was not found`.

`setup()` creates those indexes for the shared schema's filter fields, so
per-session retrieval works out of the box. For your own filter fields:

```python
from langgraph_checkpoint_documentdb import ensure_filter_indexes

ensure_filter_indexes(saver.db, "summaries", ["user_id", "tenant_id"])
```

## API

| Method | Purpose |
|---|---|
| `setup()` | Create collections and indexes. Idempotent. |
| `put(thread_id, checkpoint_id, checkpoint, metadata=None)` | Upsert a checkpoint. |
| `get(thread_id, checkpoint_id=None)` | Fetch one checkpoint, or the latest. |
| `list(thread_id, limit=None)` | Checkpoints newest first. |
| `put_writes(thread_id, checkpoint_id, task_id, writes)` | Record pending writes. |
| `get_writes(thread_id, checkpoint_id)` | Writes in order. |
| `delete_thread(thread_id)` | Remove a thread everywhere. Returns counts. |

## Tests

Integration tests need a running instance and **skip** rather than fail without one.

```bash
pip install -e '.[dev]'
export DOCUMENTDB_PASSWORD='...'
pytest
```

## License

MIT
