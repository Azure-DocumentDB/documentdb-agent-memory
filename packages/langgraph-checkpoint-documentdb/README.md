# LangGraph checkpoint persistence on Azure DocumentDB

A genuine `BaseCheckpointSaver` for **LangGraph 1.2.15 / langgraph-checkpoint
4.3.0**, with namespace-scoped persistence, sync/async execution, interrupt/resume,
checkpoint history and replay. Python **3.12+**; tested dependency pins are in
`pyproject.toml`, the full hash-locked development environment in
`requirements-dev.lock`. This repository release does **not** imply publication on
PyPI. Install from this source tree or a wheel you build:

```sh
cd packages/langgraph-checkpoint-documentdb
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --require-hashes -r requirements-dev.lock
python -m pip install --no-deps -e .
```

## Real graph, no model required

Use a dedicated database and a storage namespace selected by trusted application
code. Environment connection factory options:

- `DOCUMENTDB_HOST`: exact Azure hostname, e.g.
  `your-cluster.global.mongocluster.cosmos.azure.com` (regional hostname also allowed).
- `DOCUMENTDB_AUTH=native`: additionally set `DOCUMENTDB_USERNAME` and
  `DOCUMENTDB_PASSWORD` via your secret manager/environment, not source code.
- `DOCUMENTDB_AUTH=entra`: uses `DefaultAzureCredential`; optional `AZURE_CLIENT_ID`
  selects a user-assigned managed identity. The principal must have the appropriate
  database data-plane role. No native password is needed.

Both modes use SRV discovery and verified TLS. Entra uses a PyMongo OIDC callback,
fixed audience `https://ossrdbms-aad.database.windows.net/.default` and positive
token expiry. The configured Azure SRV seed hostname is validated before credential
construction; discovered nodes use PyMongo's normal SRV DNS-parent validation and
verified TLS. Machine `OIDC_CALLBACK` does **not** accept `ALLOWED_HOSTS` in PyMongo
4.18.3 (that option is for human callbacks). This is not an exact allowlist of every
SRV-discovered server hostname. A supplied URI is accepted only by
`from_uri(uri, allowed_host=exact_host)` with strict native-auth/TLS options;
unknown/bypass options are rejected. Credentials are not included in our validation
errors; avoid logging connection strings or raw driver exceptions yourself.

```python
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command, interrupt
from langgraph_checkpoint_documentdb import DocumentDBSaver, MemoryConfig

class State(TypedDict):
    count: int

def approve(state: State):
    amount = interrupt("How much should we add?")
    return {"count": state["count"] + int(amount)}

with DocumentDBSaver.from_env(MemoryConfig(
    database="my_agent_memory", namespace="application-a"
)) as saver:
    saver.setup()  # required before ALL checkpoint operations
    builder = StateGraph(State)
    builder.add_node("approve", approve)
    builder.add_edge(START, "approve")
    builder.add_edge("approve", END)
    graph = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "conversation-1"}}
    paused = graph.invoke({"count": 1}, config)
    result = graph.invoke(Command(resume=3), config)
    assert result["count"] == 4
    history = list(graph.get_state_history(config))
```

A later process can create a new saver/client, call setup, compile the same graph,
and retrieve/resume the same thread. For replay, use `graph.invoke(None,
prior_snapshot.config)`; an interrupt is raised again and can be resumed with a
new `Command`. Re-submitting a resume command against an already resumed checkpoint
can use its stored pending writes; it is not the same as replay.

Use `await graph.ainvoke(...)`, `await graph.aget_state(...)`, `saver.aget_tuple`,
`aput`, `aput_writes`, `alist`, `adelete_thread`, or `aclose` for async usage. PyMongo
work (including cursor iteration and serialization) runs in worker threads via
`asyncio.to_thread`, **not** on the event loop. This is a synchronous-driver bridge,
not a native async driver. Cancellation of an await cannot cancel an already
running database operation. Quiesce operations before closing/deleting.

## Storage and lifecycle

Every saver has a fixed, nonempty **storage `namespace`**. It is separate from
LangGraph's `configurable.checkpoint_ns` (empty root namespace or a subgraph).
Thread/checkpoint/task identifiers must be strings, not query objects. Unique keys:

| Collection | Unique scope |
|---|---|
| sessions | namespace, thread_id |
| messages | namespace, thread_id, seq |
| summaries | namespace/thread plain index; no unique summary constraint |
| checkpoints | namespace, thread_id, checkpoint_ns, checkpoint_id |
| checkpoint_writes | namespace, thread_id, checkpoint_ns, checkpoint_id, task_id, idx |

`setup()` preflights **all five existing catalogs and row scopes before mutation**.
Legacy thread-only indexes, unscoped rows and incompatible vector catalogs fail
closed with an offline migration/new database instruction. Setup never drops or
rebuilds an index. Python and TypeScript use the same v2 names/options and can run
setup in either order. Do not change schemas concurrently with application writes.
Python never writes `sessions.next_seq`; that message allocator belongs to the
TypeScript adapter. LangGraph checkpoint payloads are opaque to TypeScript.

`delete_thread(thread_id)` returns `None`, as in the base contract, and removes the
matching storage namespace/thread from **all five collections**, including every
checkpoint namespace and summary. `delete_thread_counts` is a convenience returning
per-collection counts. Stop writers first: deletion is **not** a cross-collection
transaction, and concurrent deletes/writes are not linearizable. No TTL or automatic
retention is promised. A failure may leave partial lifecycle deletion; retry after
fixing it while writers remain stopped.

`get(config)` returns a Checkpoint; `get_tuple(config)` returns CheckpointTuple with
metadata, canonical config, parent_config and decoded pending_writes. `list` sorts
by checkpoint ID descending and supports `before`, typed metadata-equality `filter`
and `limit`. `list(None)` enumerates all threads **only in this saver's namespace**;
`list` with a thread but no `checkpoint_ns` includes that thread's subgraphs.
Metadata filters are evaluated after decoding and are not an indexed server search.

Normal pending writes are first-write-wins at `(task_id, idx)`; special error,
scheduled, interrupt and resume channels replace their reserved negative indices.
Reads use released `writes_sort_key(task_path, task_id, idx)` ordering. Whole
checkpoint values are stored (not channel blobs), preserving parent references.
The inherited integer `get_next_version` is monotonically increasing per channel.
New optional maintenance operations `copy_thread`, `prune`, `delete_for_runs` are
**not implemented** and raise the base `NotImplementedError`; do not assume all
optional methods in the current framework are supported.

## Serialization and connection trust

The default serializer is LangGraph's **strict-allowlist JsonPlusSerializer**, with
`pickle_fallback=False` and permissive module loading disabled explicitly regardless
of environment. It supports built-in safe types and framework messages, `Send`,
`Interrupt`, etc. Custom graph state classes need an explicit trusted allowlist via
inherited `saver.with_allowlist({("your_module", "YourType")})`. Never allowlist a
constructor named by untrusted stored data. Values are roundtrip-validated before
writes. Database access still requires authorization: serialization is not a
substitute for restricting DB writers, resource limits, or tenant authorization.
The storage namespace prevents accidental query crossing, not access by someone
holding unrestricted DB credentials.

An injected `MongoClient` is **caller-configured and caller-owned by default**;
its security/DNS/TLS policy is the caller's responsibility. Supply
`owns_client=True` to transfer lifecycle ownership. URI/env factories own their
client; `close()` is idempotent and the saver then rejects further operations.

For explicit loopback development only:

```python
from pymongo import MongoClient
from langgraph_checkpoint_documentdb import build_local_uri, DocumentDBSaver
uri = build_local_uri("appuser", "development-password", allow_self_signed=True)
saver = DocumentDBSaver(MongoClient(uri), owns_client=True)
```

Self-signed allowance requires an explicit opt-in and only literal loopback or
`localhost`. Arbitrary hosts are rejected. This does not certify a local container
as equivalent to a managed service; the managed factory never bypasses TLS checks.

## Vector summaries

The shared summary index defaults to **COS IVF, numLists=1, dimensions=1536**. Set
`MemoryConfig(embedding_dimensions=3)` only for a matching 3D dataset/test. Both
`namespace_filter` and `thread_id_filter` are created before the vector index.
`vector_search_stage(vector=..., k=..., namespace=..., thread_id=...,
dimensions=...)` always includes both scope filters and validates finite numeric
values, dimensions and topK. Use `dimensions` to require your exact index shape.

Other vector kinds are explicit `VectorIndexSpec` choices. Their availability
varies by deployment, region, current service version and tier. The package makes
no historical blanket M30/M40 guarantee; query current service documentation and
test your intended deployment. Synthetic-vector tests establish filtering/ranking
plumbing, not semantic retrieval quality.

## Tests and builds

```sh
python -m pytest -q              # offline, real framework + mongomock storage
python -m pip check
python -m pip_audit -r requirements-dev.lock --require-hashes
python -m build --no-isolation   # local wheel/sdist, never publishes
```

Mongomock is only a test dependency. It does not prove Azure behavior; its vector
catalog command is explicitly simulated in tests, and it cannot run vector search.

Run the **explicit managed gate**, once per auth mode, with all env above plus:

```sh
export DOCUMENTDB_LIVE=1
export DOCUMENTDB_DATABASE=readiness_agent_memory
export DOCUMENTDB_NAMESPACE=integration_run_unique
python -m pytest -q --live
```

This gate fails (not skips) when required configuration is absent. Database must
start `readiness_` or `test_`. It uses synthetic 3D vectors and unique namespaces,
real sync/async StateGraphs, interrupt/resume/replay, fresh client persistence and
negative thread/namespace retrieval. It never drops a database. Offline invocation
deselects the dedicated live-only case; this is **not a live success**. The shared
core cases run against mongomock offline and real Azure when `--live` is present.

## Migration from prototype 0.1

This is intentionally **API- and schema-breaking**. The old class was not a real
LangGraph saver. Remove custom `put(thread_id, checkpoint_id, dict)`/custom `get`
usage; use graph APIs or the released BaseCheckpointSaver signatures. Store
application messages through the TypeScript adapter rather than forging checkpoints.
`build_local_uri` now requires explicit insecure-TLS opt-in. `from_uri` requires an
exact managed allowed host; `vector_search_stage` requires both namespace and thread.
Historical `MIN_TIER`/`LOCAL_SUPPORTED` tables were removed as they overstated
current service guarantees. Legacy data is not silently migrated: back up and
migrate offline to a new v2 database. Do not mix prototype and v2 writers.

MIT license. No package registry publication is implied by these build instructions.
