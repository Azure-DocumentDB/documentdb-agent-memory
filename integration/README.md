# Opt-in real Azure cross-language acceptance

This harness uses the actual built TypeScript adapter, PyMongo Python adapter and
released LangGraph framework. Each stage is a **new OS process**, not just a new
client. It makes no LLM calls and uses no API/database mocks in live execution.
Offline tests validate only harness guards, environment translation and syntax.

## Prerequisites

- An explicitly approved, disposable Azure DocumentDB deployment with native or
  Entra/OIDC authentication and support for COS / 3-dimensional IVF `numLists=1`.
- Permission to create two fresh databases and their five collections/indexes,
  and read/write/delete test records. Database names are always generated as
  `readiness_memory_cross_<24-random-hex>_pyts` and `_tspy`; the harness cannot
  target a user-supplied existing database.
- An activated Python venv with the Python package's locked dependencies installed.
  The coordinator defaults to the Python executable used to launch it. You can
  override it with `MEMORY_CROSS_PYTHON=/absolute/path/to/venv/bin/python` or `--python`.
- Built TypeScript `packages/vercel-ai-memory-documentdb/dist` and that package's
  local `node_modules`, Node >=24.21.0. Build with the package's existing build
  command before execution. This harness never installs global or local deps.
- Quiescent test threads: **concurrent write/delete is unsupported**. All stages
  run serially and terminate before deletion/cleanup starts.

The coordinator resolves Python package source and TypeScript `dist` relative to
its own location, not the caller's current directory. Package compilation is the
caller's responsibility, so build after any TypeScript source changes.

## Authentication: privately inject one configuration

The harness reads environment only; it does not read a secret file, invoke Azure
resource-management commands, or print connection strings, passwords or tokens.
Do not put actual credentials into shell command history.

**Native, Python-style:**

- `DOCUMENTDB_AUTH=native`
- `DOCUMENTDB_HOST=<exact Azure mongocluster host>`
- `DOCUMENTDB_USERNAME`, `DOCUMENTDB_PASSWORD`

**Native, TypeScript-style:**

- `MONGO_AUTH_MODE=connection-string`
- `MONGO_CONNECTION_STRING=<managed SRV native URI>`

Only safe options compatible with both packages are accepted: verified TLS,
SCRAM-SHA-256, retryWrites=false, optional authSource=admin. The coordinator
creates canonical equivalent package environments; incompatible options and
conflicting supplied host/mode/credentials fail before subprocess launch.

**Entra/OIDC:**

- `DOCUMENTDB_AUTH=entra` and `DOCUMENTDB_HOST`, **or**
  `MONGO_AUTH_MODE=oidc` and `MONGO_HOST`.
- Existing Azure DefaultAzureCredential environment/session authorized for the
  deployment. Optional `AZURE_CLIENT_ID`; TypeScript also accepts optional
  `MONGO_OIDC_USERNAME` through its package helper.
- Unset native URI/username/password variables; mixed modes fail closed.

Both packages receive the same exact host/auth mode. Identity acquisition uses
their actual package helpers, not a harness token override. Success with one mode
is not evidence for the other: run separately for native and OIDC.

## Commands from repository root

Offline only (no cloud connections):

```sh
PYTHONDONTWRITEBYTECODE=1 python \
  -m unittest discover -s integration -p 'test_offline.py' -v
```

After approving your own disposable Azure test resources and privately configuring auth:

```sh
MEMORY_CROSS_LIVE=1 python integration/coordinator.py \
  --output /path/outside-the-repo/live-native.json
```

Use `live-oidc.json` for the separate OIDC run. Never enable the live variable for
an offline selftest. Without exact `MEMORY_CROSS_LIVE=1`, coordinator and both
workers fail closed **before any third-party client construction**. Exit 2 is
preflight-blocked, exit 1 is acceptance/cleanup failure, exit 0 is all stages plus
cleanup passed. No skipped live tests are represented as passes.

## Gates

For each fresh database:

1. Python setup then TypeScript setup; on the other database the reverse order.
   Repeat the first adapter setup and compare canonical live catalogs (index
   names, ordered key fields, uniqueness and exact vector options).
2. Actual deterministic LangGraph graphs create sessions, checkpoints and
   pending interrupt writes in namespaces A/B with the **same thread ID**.
3. TypeScript appends two messages to those Python-created sessions, first
   verifying `next_seq` is absent, then requiring sequence 0/1 and a summary.
4. Separate Node processes read the persisted messages/summaries. Python reads
   the same documented message representation, real checkpoints and pending
   writes. Both language paths perform scoped synthetic vector searches.
5. A same-namespace/different-thread neighbor is also populated. TypeScript
   deletes A's collision thread on the first database; Python does the reverse
   on the second. All five collection deletion counts must be positive, all A
   target records absent, B and the neighbor unchanged.
6. A Python process saves an interrupted graph and exits. A distinct Python
   process checks the **exact saved checkpoint ID**, restored values, pending
   writes and next node, then `Command(resume=5)` yields 16 rather than rerunning
   the prepared increment. The original checkpoint remains in state history.
7. A uniquely scoped legacy checkpoint fixture with an invalid checkpoint ID
   type is inserted. Both adapters must reject setup and preserve the catalog
   and fixture. This tests fail-closed preflight without creating unsafe unscoped
   data or requiring any index drops.

The separate TypeScript package gate owns 100 simultaneous cold-thread atomic
append stress. This integration harness does not replace it or claim crash,
server-restart, transactional deletion or semantic embedding quality evidence.

## Cleanup and evidence

A `finally` cleanup opens a fresh Python process for each touched database and
uses exact equality for each of the three generated namespaces across the five
known collections. It verifies zero matching records. Cleanup bypasses schema
setup so malformed **test-owned** legacy fixtures can still be removed. It never
drops a database, collection, or index, never uses an empty/prefix/regex delete
filter, and never removes records outside its generated namespaces. Empty test
databases/indexes remain; later cloud resource teardown belongs to the operator.

On a process timeout the coordinator terminates and waits for the worker before
cleanup. Hard-killing the coordinator/host can prevent `finally`; use the
credential-free output/run identifiers to inspect leftover generated scopes.
No durable success is reported if cleanup fails. Driver stdout/stderr and error
messages are captured and suppressed (they may carry secrets); failure output
contains only language/action, static error class, and exit status. JSON evidence
contains generated database/namespace IDs, stage PIDs, catalogs, counts and
checkpoint IDs, never credential values. An auth failure requires private
operator diagnosis rather than loosening this output policy.
