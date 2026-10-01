# vercel-ai-memory-documentdb

Memory adapter for the **Vercel AI SDK**, backed by **Azure DocumentDB**. Develop
against a local container, ship to the managed service by changing one
environment variable.

```bash
npm install vercel-ai-memory-documentdb
```

## Quickstart

Start a local DocumentDB:

```bash
export DOCUMENTDB_USERNAME=appuser
export DOCUMENTDB_PASSWORD='choose…word'
docker compose -f docker/docker-compose.yml up -d
```

Use it:

```ts
import { DocumentDBMemory, buildLocalUri } from "vercel-ai-memory-documentdb";

const memory = DocumentDBMemory.fromUri(
  buildLocalUri("appuser", process.env.DOCUMENTDB_PASSWORD!),
  { embeddingDimensions: 1536 },
);
await memory.setup(); // creates collections + indexes, idempotent

await memory.appendMessages("thread-1", [
  { role: "user", content: "What's the deploy process?" },
  { role: "assistant", content: "Push to main and CI handles it." },
]);

const history = await memory.getMessages("thread-1");
```

With the AI SDK:

```ts
import { openai } from "@ai-sdk/openai";
import { generateText } from "ai";

const threadId = "thread-1";
const history = await memory.getMessages(threadId, 20); // last 20 turns

const result = await generateText({
  model: openai("gpt-4.1-mini"),
  messages: [...history.map(({ role, content }) => ({ role, content }))],
});

await memory.appendMessages(threadId, [
  { role: "assistant", content: result.text },
]);
```

Moving to Azure DocumentDB is a connection-string change:

```ts
const memory = DocumentDBMemory.fromUri(process.env.DOCUMENTDB_URI!);
```

```bash
az cosmosdb mongocluster show -c <cluster> -g <group> \
  --query properties.connectionString -o tsv
```

> The local container serves a self-signed certificate, so `buildLocalUri()`
> disables certificate validation. Never use it against the managed service,
> which presents a real certificate.

## Semantic recall

Store condensed memories with embeddings and retrieve them by meaning. Retrieval
is scoped to a thread via vector pre-filter:

```ts
import { embed } from "ai";
import { openai } from "@ai-sdk/openai";

const { embedding } = await embed({
  model: openai.embedding("text-embedding-3-small"),
  value: "User prefers deploys on Tuesdays.",
});
await memory.addSummary(threadId, "User prefers deploys on Tuesdays.", embedding);

const { embedding: q } = await embed({
  model: openai.embedding("text-embedding-3-small"),
  value: "when should we ship?",
});
const hits = await memory.searchSummaries(threadId, q, 5);
```

`embeddingDimensions` must match your model — `text-embedding-3-small` is 1536.
A mismatch throws immediately rather than failing later at query time.

## Vector index kinds: read this before you deploy

This adapter defaults to **IVF**, deliberately. IVF is the only kind that works
on both the local container and the entry-level managed tier.

| Kind | Local container | Managed Azure DocumentDB |
|---|---|---|
| `vector-ivf` *(default)* | supported | M10 and up |
| `vector-hnsw` | supported | **M30 and up** |
| `vector-diskann` | **not supported** | **M40 and up** |

The trap is HNSW: it works locally and is then rejected by an M10 cluster with
`hnsw index is not supported for this cluster tier`. Local is a *superset* of a
small managed cluster, so "it worked on my machine" is not evidence it will
deploy. Opting into a non-default kind means committing to a tier:

```ts
const memory = DocumentDBMemory.fromUri(uri, {
  embeddingDimensions: 1536,
  vectorIndex: { path: "embedding", dimensions: 1536, kind: "vector-hnsw" }, // M30+
});
```

Rejections throw `VectorIndexUnsupportedError`, which names the required tier
instead of echoing the raw server message.

## Pre-filtered search is handled for you

Filtering a vector search by field — "search only this thread's memories" —
requires a plain index on the filter field, created *before* the search runs.
Without it the server returns `The index for filter path 'thread_id' was not found`.

`setup()` creates those indexes, so `searchSummaries()` works out of the box.
For your own filter fields:

```ts
import { ensureFilterIndexes } from "vercel-ai-memory-documentdb";

await ensureFilterIndexes(memory.db, "summaries", ["user_id", "tenant_id"]);
```

## API

| Method | Purpose |
|---|---|
| `setup()` | Create collections and indexes. Idempotent. |
| `appendMessages(threadId, messages)` | Append turns; sequence assigned server-side. |
| `getMessages(threadId, limit?)` | Messages in order; `limit` keeps the most recent. |
| `addSummary(threadId, text, embedding, metadata?)` | Store a condensed memory. |
| `searchSummaries(threadId, queryEmbedding, k?)` | Semantic recall, scoped to the thread. |
| `deleteThread(threadId)` | Remove a thread everywhere. Returns counts. |
| `close()` | Close the client. |

## Interoperability

Writes the same schema as `langgraph-checkpoint-documentdb`, so memory captured
by one framework is readable by the other.

## Tests

Integration tests need a running instance and **skip** rather than fail without one.

```bash
npm install
export DOCUMENTDB_PASSWORD='***'
npm test
```

## License

MIT
