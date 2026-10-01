# documentdb-agent-memory

Agent memory adapters for **Azure DocumentDB**. Develop against a local container,
ship to the managed service by changing one environment variable.

This is the monorepo — the single source of truth for every adapter. Each adapter
is also published to its own framework-named repository for discoverability; those
are generated mirrors, not places to commit.

## Adapters

| Package | Framework | Language | Status |
|---|---|---|---|
| [`langgraph-checkpoint-documentdb`](packages/langgraph-checkpoint-documentdb) | LangGraph | Python | in development |
| `vercel-ai-memory-documentdb` | Vercel AI SDK | TypeScript | planned |

Phase 2 targets: CrewAI, Pydantic AI, OpenAI Agents SDK sessions, Semantic Kernel.

## Why a shared repo

Every adapter writes the same collection shapes (`sessions`, `messages`,
`summaries`), so memory captured by one framework is readable by another. That
agreement is enforced by shared code and shared tests rather than convention.

It also means the platform rules below have exactly one implementation. They have
already changed once; six copies would be six chances to be wrong.

## Start a local DocumentDB

```bash
export DOCUMENTDB_USERNAME=appuser
export DOCUMENTDB_PASSWORD='choose-a-strong-password'
docker compose -f docker/docker-compose.yml up -d
```

No Azure account needed. Requires Docker with the Compose plugin; the container
is bound to loopback.

## Two platform rules worth knowing up front

**IVF is the default vector index, deliberately.** It is the only kind that works
on both the local container and the entry-level managed tier. HNSW works locally
and is then *rejected* by an M10 cluster — local is a superset of a small managed
cluster, so a green local run is not proof your index will deploy.

| Kind | Local | Managed |
|---|---|---|
| `vector-ivf` *(default)* | ✅ | M10+ |
| `vector-hnsw` | ✅ | M30+ |
| `vector-diskann` | ❌ | M40+ |

**Pre-filtered vector search needs a plain index on the filter field, created
first.** Per-session retrieval is exactly that case, so adapters create those
indexes automatically instead of documenting the requirement.

Full detail and reproduction steps: [docs/VECTOR_INDEXES.md](docs/VECTOR_INDEXES.md).

## Tests

Integration tests run against a real instance and **skip** rather than fail when
none is reachable, so `pytest` on a laptop with nothing running stays green.

```bash
cd packages/langgraph-checkpoint-documentdb
pip install -e '.[dev]'
export DOCUMENTDB_PASSWORD='...'
pytest
```

CI runs the same suite against the container as a service.

## License

MIT
