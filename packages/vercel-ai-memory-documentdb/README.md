# vercel-ai-memory-documentdb

**Application-level message memory for AI SDK 7**, backed by Azure DocumentDB.
This is not an official AI SDK persistence plugin, does not auto-attach memory to a
model, and does not provide LangGraph checkpoint execution. Your application
explicitly loads history, calls the SDK, and appends responses.

## Build and install locally

This repository does **not** imply publication on npm. Use Node **24.21.0 or newer**
(the tested/CI runtime is Node 24.21.0):

```sh
# In packages/vercel-ai-memory-documentdb
npm ci
npm test
npm pack
# From your application directory, use the resulting local artifact:
npm install /absolute/path/vercel-ai-memory-documentdb-0.1.0.tgz
```

Pinned/tested dependencies: `ai@7.0.138`, `mongodb@7.7.0`;
optional Entra authentication uses `@azure/identity@4.13.3`.
The lockfile is committed. No model-provider package or paid API call is needed for tests.

## Connect safely

```ts
import { DocumentDBMemory } from 'vercel-ai-memory-documentdb';

// MONGO_AUTH_MODE=connection-string
// MONGO_CONNECTION_STRING=mongodb+srv://<user>:<password>@<cluster>.global.mongocluster.cosmos.azure.com/?tls=true&retryWrites=false
const memory = await DocumentDBMemory.fromEnvironment(process.env, {
  database: 'agent_memory', namespace: 'application-tenant', embeddingDimensions: 1536,
});
await memory.setup(); // REQUIRED before any data operations; verifies shared schema
```

Managed native authentication accepts only the public Azure DocumentDB SRV DNS
suffix `.mongocluster.cosmos.azure.com` (including `.global.`), verifies TLS, and
rejects arbitrary hosts, URI databases, fragments, duplicate/unknown query options,
proxies, TLS bypass flags, or authentication overrides before connection.
Other cloud DNS suffixes are intentionally not supported by this factory.

For Entra set `MONGO_AUTH_MODE=oidc`, `MONGO_HOST` to the same strict Azure hostname,
and optionally `MONGO_OIDC_USERNAME` to the configured principal. Unset native
connection-string variables. The optional Azure Identity package uses
`DefaultAzureCredential`; configure the service-side principal/roles separately.
The driver uses `MONGODB-OIDC`, fixed Azure OSS RDBMS audience, validated token
expiry, callback cancellation, `auth: { username, password: '' }` when a principal
is supplied, and a narrow Azure DNS `ALLOWED_HOSTS` rule. It never falls back to
native credentials. No secrets are logged by the connection factory.

Local-only self-signed TLS development:

```ts
import { DocumentDBMemory, buildLocalUri } from 'vercel-ai-memory-documentdb';
const memory = DocumentDBMemory.fromUri(
  buildLocalUri('appuser', process.env.DOCUMENTDB_PASSWORD!),
  { database: 'agent_memory', namespace: 'dev', embeddingDimensions: 3 },
);
await memory.setup();
```

`buildLocalUri` accepts only `127.0.0.1`, `localhost`, or `[::1]`, valid ports,
and percent-encodes credentials. Certificate bypass is limited to these explicit
loopback connections; hostname verification is never disabled. The application is
responsible for the local service. The tests do not start Docker or cloud resources.

**Injected client boundary:** `new DocumentDBMemory(mongoClient, config)` trusts the
caller to secure endpoint, TLS, credentials, driver settings and database access.
Use acknowledged writes; the adapter explicitly requests `w: 1` for data mutations.
`close()` closes that client, so do not share it with independently owned stores.
A failed `setup()` also closes the client. Use `try/finally` around application work.

## Real AI SDK integration (explicit load → generate → append)

Your application supplies a configured model; this adapter never creates one:

```ts
import { generateText, stepCountIs, type LanguageModel } from 'ai';

async function reply(model: LanguageModel, threadId: string, userText: string) {
  await memory.appendMessages(threadId, [{ role: 'user', content: userText }]);
  const result = await generateText({
    model,
    ...await memory.getPrompt(threadId),
    stopWhen: stepCountIs(5), // configure tools yourself if needed
  });
  // AI SDK 7 response.messages contains only the LAST step. Save EVERY step,
  // including assistant tool calls and tool-result messages, exactly once here.
  await memory.appendMessages(threadId, result.steps.flatMap(step => step.response.messages));
  return result.text;
}
```

`getPrompt()` separates stored system messages into AI SDK 7's `instructions`
field; passing system messages in `messages` is rejected by this SDK version.
`getModelMessages()` returns SDK-shaped history without storage metadata;
`getMessages()` additionally returns `seq` and `created_at`.
On restart create another client, call `setup()`, then `getPrompt(threadId)`.
The permanent SDK integration test uses the **published** `ai/test`
`MockLanguageModelV4`: real `generateText`, tool execution, second generation step,
response append, close, new-client resume and another generation. It requires no
LLM, and an offline database fixture does not establish server atomicity.

### Supported message formats and bounds

Inputs use the installed SDK's `ModelMessage` union and runtime schema validation.
Supported: system/user/assistant string text; text/reasoning/tool-call/tool-result/
tool-approval parts as valid for each role; JSON-safe `providerOptions` and tool
inputs/outputs. A `tool` message must contain structured results, **not a string**.
Object properties with `undefined` values are omitted as in SDK responses.
Images, files, custom parts, binary data, URL/Date objects, cycles, nonfinite numbers,
and unsafe document keys are rejected rather than silently changed. This is a
JSON-safe subset, not complete multimodal AI SDK persistence. Tool-result output
must also be JSON-safe. No claim that arbitrary provider-specific content works.

Only `role`, `content`, `providerOptions` may be supplied at message top level.
Storage fields (`namespace`, `thread_id`, `seq`, timestamps, `_id`, allocator fields)
cannot be supplied by callers. Summary metadata cannot override storage, checkpoint,
message or summary fields. Namespace/thread IDs are nonblank strings, at most 512
characters, without control characters. Messages and metadata each have a 1 MB
UTF-8 JSON bound; batches have at most 1,000 messages and 8 MB JSON. Nesting max 64.
Summary text is nonblank and at most 1 MB UTF-8; vectors have the exact configured
dimension and finite numbers; `k` is an integer 1–100. Recent-message `limit` is
1–10,000. **Do not truncate a tool call/result pair**: the application owns context
budgeting, summarization and semantic correctness.

## Ordering, failure and deletion semantics

`setup()` creates/verifies the shared unique indexes before operations are enabled.
Sequences begin at zero. Each append reserves a unique consecutive range using a
server-side atomic session `findOneAndUpdate` / `$inc`, never a read-max/insert.
A Python-created session with no `next_seq` initializes from zero. The existing
schema must not have messages with a missing allocator; migrate legacy data offline.
Cold-session upsert collisions retry only server-rejected duplicate-key errors,
bounded to 20 attempts. Ordering is **reservation order**, not promise completion
order, client clock order, or a semantic ordering between simultaneous prompts.
Concurrent readers can temporarily see later batches before earlier inserts finish.
After all successful calls settle, reads sort unique sequence numbers ascending.

Reservation and insertion are **not a transaction**. Failed/partially acknowledged
inserts may leave sequence gaps or partial batches. `AppendMessagesError` reports
the reserved range and never returns a false inserted count. Driver retryable writes
are disabled by factories. No application retry is attempted after an unknown
network/write outcome: inspect/reconcile storage first; retrying the same messages
may duplicate them. This is **not exactly-once delivery**, and there is no batch-ID
idempotency API. Invalid/overflowed allocators fail closed.

`deleteThread` removes `(namespace, thread_id)` from **all five** collections:
`sessions`, `messages`, `summaries`, `checkpoints`, `checkpoint_writes`, and returns
per-collection counts. **Quiesce every TypeScript/Python writer before deletion.**
These are five scoped deletes, not distributed-atomic deletion. A failure can leave
partial cleanup; repeat while quiescent. Other namespace/thread pairs are untouched.

## Vector recall and shared schema

```ts
await memory.addSummary('thread-1', 'Deploy on Tuesdays', embedding, { source: 'app' });
const hits = await memory.searchSummaries('thread-1', queryEmbedding, 5);
```

The application supplies real embeddings and summary text. Synthetic test vectors
prove database storage, ranking/filter plumbing, **not semantic recall quality**.
The default is COS/IVF, `numLists: 1`, `embedding` field, 1536 dimensions; configure
dimensions to match the embedding model and existing index. Test fixtures use 3D.
Alternative kinds are opt-in; deployment/version capabilities must be checked.
There are no universal hardcoded tier guarantees or automatic downgrade.

Schema v2 is shared with `langgraph-checkpoint-documentdb`: namespace/thread scoped
session/message uniqueness, summary compound plus separate `namespace_filter` and
`thread_id_filter` indexes **before** `embedding_vector_ivf`, and checkpoint/write
uniqueness including LangGraph checkpoint namespace/ID/task/index. No TTL promise.
Repeated setup in either adapter order must agree on dimensions and vector options.
Setup first inspects the entire shared catalog and rejects legacy unscoped records,
thread-only unique indexes, incompatible keys/uniqueness/partial/sparse/collation/TTL
or vector options without dropping/rebuilding anything. Use a new database or
explicit offline migration. Setup is an administrative, database-wide schema check,
not a tenant-read API. Restrict database provisioning credentials accordingly.

The adapters share **storage/lifecycle**, not execution semantics. TypeScript does
not interpret serialized LangGraph checkpoint payloads, and Python checkpoint
resumption does not imply it consumes TypeScript chat history.

## Gates

```sh
npm ci
npm run typecheck
npm test          # offline assertions and real SDK with mock model; never a DB proof
npm audit
npm audit --omit=dev
npm pack
```

Run the live suite only against an assigned disposable database:

```sh
export MEMORY_LIVE_ALLOW_WRITES=true
export MEMORY_TEST_DATABASE=agentmemory_disposable_unique_run
export MONGO_AUTH_MODE=connection-string
# Supply MONGO_CONNECTION_STRING privately, or use the Entra variables above.
npm run test:live
```

The requested live suite **fails (never skips)** without config or connectivity.
It uses 3D IVF/COS fixtures, creates unique namespaces/threads, verifies 100 unbounded
concurrent appends to a cold session (100 successes, unique ordered history),
Python-created session allocation, namespace-vector isolation, all-five-collection
delete and the SDK tool/new-client flow. It cleans scoped records, not the database.
Parent/repository cross-adapter tests additionally verify both setup orders and
actual Python checkpoint lifecycle. Use separately assigned databases for tests
that deliberately introduce incompatible legacy catalog/data.

## License

MIT
