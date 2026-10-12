# documentdb-agent-memory

Two application memory adapters for Azure DocumentDB: a Python LangGraph checkpointer and a TypeScript message/summary store for applications using the AI SDK. This monorepo is their source of truth; it does not claim that framework-specific mirrors or packages have been published to registries.

| Package | Purpose |
| --- | --- |
| [langgraph-checkpoint-documentdb](packages/langgraph-checkpoint-documentdb) | Persist and resume real LangGraph checkpoints, including pending writes and sync/async calls. |
| [vercel-ai-memory-documentdb](packages/vercel-ai-memory-documentdb) | Explicitly load/store AI SDK-compatible conversation messages and scoped vector summaries. It is an application-level integration, not an official automatic persistence plugin. |

The packages have separate implementations of a shared [schema v2 contract](docs/SCHEMA_V2.md). Message and summary records share a shape; LangGraph checkpoint payloads remain opaque to the TypeScript adapter. Both adapters remove the same five collections' matching lifecycle records when deleting a quiescent thread.

## What this project does not provide

- No model hosting, automatic embedding generation, agent execution service or model credentials.
- No tenant authentication/authorization layer. Your application must authorize the namespace/thread before invoking storage APIs.
- No automatic migration from the old prototype's unscoped/thread-only schema.
- No exactly-once retry or transactional deletion guarantee across simultaneous writers and failures unless explicitly stated in a tested API contract.
- No blanket local-container/managed-service feature parity or production durability certification.

## Start with a fresh database

Use a new, explicitly selected database for schema v2. Setup verifies the schema and indexes; incompatible legacy indexes/records must fail rather than being dropped or rewritten. Read [SCHEMA_V2.md](docs/SCHEMA_V2.md) before adopting a previous prototype database.

Follow the package README for its supported versions, installation, auth configuration, examples and commands. Build and install from the local wheel/npm tarball when following this repository: repository visibility does not imply a package exists on PyPI or npm.

- [Python installation and genuine LangGraph examples](packages/langgraph-checkpoint-documentdb)
- [TypeScript installation and AI SDK application integration](packages/vercel-ai-memory-documentdb)
- [Vector indexes and namespace/thread-scoped retrieval](docs/VECTOR_INDEXES.md)

## Managed Azure and local development

For managed Azure, use an explicitly approved resource, strict certificate/hostname verification, narrowly configured firewall access, and the documented native or Entra authentication path. Use disposable test databases when exercising lifecycle tests. Resource and storage charges continue until the environment owner deletes their resources.

An optional local Compose file is provided with a pinned DocumentDB release-candidate image. It is a convenience for isolated synthetic evaluation, not a stable/GA database endorsement. It uses a project-scoped named volume and doesn't restart automatically. Start it only after checking disk/memory capacity and existing containers:

```bash
export DOCUMENTDB_USERNAME=appuser
# Set DOCUMENTDB_PASSWORD securely in your environment; do not put it in source control.
: "${DOCUMENTDB_PASSWORD:?Set a unique local test password}"
docker compose -p documentdb-memory-eval -f docker/docker-compose.yml up -d
```

The container binds loopback only. Self-signed certificate bypass, where supported by a package's explicit local helper, is restricted to loopback development. It must not be reused with managed or remote servers. `docker compose down` retains the named data volume; deliberate volume deletion permanently removes its data. Never stop or remove another project's containers/volumes as sample cleanup.

## Tests and release evidence

Offline tests, actual framework tests and real-database tests are separate. Explicitly requesting a real integration gate without its required consent, credentials or a reachable database must fail—not silently skip to green. See package test instructions and the dated `TESTED.md` release record for precisely what was exercised.

Database-dependent tests use synthetic data and require explicit isolated database selection. Checkpoints/messages must persist across newly created application processes; namespace separation, full cross-adapter deletion and concurrent appends need real service tests in addition to mocks.

The AI SDK test model is deterministic and local. Such tests prove application wiring and message compatibility, not model intelligence or a successful live model request.

## Contributing and security

Keep Python/TypeScript schema changes synchronized and run both adapters' tests plus cross-adapter lifecycle checks. Preserve backwards-incompatible changes as explicit versioned migrations; don't erase old data or weaken index validation to make a test pass.

Do not commit credentials, connection strings, private data, generated database dumps or test resource identifiers. Keep integration secrets in process environment or protected local files outside tracked source. Failures must be reported without printing credential-bearing driver exceptions.

## License

MIT. Each package retains its license. Published dependency names and versions are recorded independently; no framework or service endorsement is implied.
