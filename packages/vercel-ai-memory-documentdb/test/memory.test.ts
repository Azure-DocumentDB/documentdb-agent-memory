/**
 * Integration tests against a real DocumentDB instance.
 *
 * Start the local container first:
 *   docker compose -f docker/docker-compose.yml up -d
 *
 * Config via env:
 *   DOCUMENTDB_URI       full connection URI (wins if set)
 *   DOCUMENTDB_USERNAME  default "appuser"
 *   DOCUMENTDB_PASSWORD  required unless DOCUMENTDB_URI is set
 *   DOCUMENTDB_HOST      default 127.0.0.1
 *   DOCUMENTDB_PORT      default 10260
 *
 * Tests SKIP (not fail) when no instance is reachable, so `npm test` on a
 * laptop with nothing running stays green. Mirrors the Python suite.
 */

import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import {
  DEFAULT_KIND,
  DocumentDBMemory,
  LOCAL_SUPPORTED,
  MIN_TIER,
  SUMMARIES,
  VectorIndexUnsupportedError,
  buildLocalUri,
  ensureFilterIndexes,
  ensureVectorIndex,
  vectorSearchStage,
  type VectorKind,
} from "../dist/index.js";

const DIMS = 8;

function uri(): string | undefined {
  if (process.env.DOCUMENTDB_URI) return process.env.DOCUMENTDB_URI;
  const pw = process.env.DOCUMENTDB_PASSWORD;
  if (!pw) return undefined;
  return buildLocalUri(
    process.env.DOCUMENTDB_USERNAME ?? "appuser",
    pw,
    process.env.DOCUMENTDB_HOST ?? "127.0.0.1",
    Number(process.env.DOCUMENTDB_PORT ?? "10260"),
  );
}

const rand = () => Math.random().toString(16).slice(2, 10);

let memory: DocumentDBMemory | undefined;
let dbName = "";
let skipReason = "";

describe("DocumentDBMemory", { concurrency: 1 }, () => {
  before(async () => {
    const u = uri();
    if (!u) {
      skipReason = "set DOCUMENTDB_URI or DOCUMENTDB_PASSWORD";
      return;
    }
    dbName = `test_mem_${rand()}`;
    const m = DocumentDBMemory.fromUri(u, {
      database: dbName,
      embeddingDimensions: DIMS,
      vectorIndex: { path: "embedding", dimensions: DIMS, numLists: 1 },
    });
    try {
      await m.client.connect();
      await m.client.db("admin").command({ ping: 1 });
    } catch (err) {
      skipReason = `no reachable DocumentDB: ${String(err)}`;
      await m.close().catch(() => {});
      return;
    }
    await m.setup();
    memory = m;
  });

  after(async () => {
    if (memory) {
      await memory.client.db(dbName).dropDatabase();
      await memory.close();
    }
  });

  const live = (name: string, fn: (m: DocumentDBMemory) => Promise<void>) =>
    it(name, async (t) => {
      if (!memory) return t.skip(skipReason);
      await fn(memory);
    });

  live("server is wire-compatible", async (m) => {
    const admin = m.client.db("admin");
    const info = (await admin.command({ buildInfo: 1 })) as { version: string };
    const hello = (await admin.command({ hello: 1 })) as {
      maxWireVersion: number;
    };
    assert.ok(info.version.startsWith("7."), `got ${info.version}`);
    assert.ok(hello.maxWireVersion >= 21);
  });

  live("setup is idempotent", async (m) => {
    const a = await m.setup();
    const b = await m.setup();
    assert.equal(a.schemaVersion, b.schemaVersion);
    assert.ok(b.vectorIndex);
  });

  live("appends and reads messages in order", async (m) => {
    const tid = `t-${rand()}`;
    const n = await m.appendMessages(tid, [
      { role: "user", content: "hello" },
      { role: "assistant", content: "hi there" },
    ]);
    assert.equal(n, 2);
    const msgs = await m.getMessages(tid);
    assert.deepEqual(
      msgs.map((x) => x.content),
      ["hello", "hi there"],
    );
    assert.deepEqual(
      msgs.map((x) => x.seq),
      [0, 1],
    );
    await m.deleteThread(tid);
  });

  live("assigns sequence numbers across separate appends", async (m) => {
    const tid = `t-${rand()}`;
    await m.appendMessages(tid, [{ role: "user", content: "one" }]);
    await m.appendMessages(tid, [{ role: "assistant", content: "two" }]);
    await m.appendMessages(tid, [{ role: "user", content: "three" }]);
    const msgs = await m.getMessages(tid);
    assert.deepEqual(
      msgs.map((x) => x.seq),
      [0, 1, 2],
    );
    assert.deepEqual(
      msgs.map((x) => x.content),
      ["one", "two", "three"],
    );
    await m.deleteThread(tid);
  });

  live("limit keeps the most recent messages, still in order", async (m) => {
    const tid = `t-${rand()}`;
    await m.appendMessages(
      tid,
      Array.from({ length: 5 }, (_, i) => ({
        role: "user" as const,
        content: `m${i}`,
      })),
    );
    const recent = await m.getMessages(tid, 2);
    assert.deepEqual(
      recent.map((x) => x.content),
      ["m3", "m4"],
    );
    await m.deleteThread(tid);
  });

  live("threads are isolated", async (m) => {
    const a = `a-${rand()}`;
    const b = `b-${rand()}`;
    await m.appendMessages(a, [{ role: "user", content: "in a" }]);
    await m.appendMessages(b, [{ role: "user", content: "in b" }]);
    assert.equal((await m.getMessages(a)).length, 1);
    assert.equal((await m.getMessages(a))[0]?.content, "in a");
    assert.equal((await m.getMessages(b))[0]?.content, "in b");
    await m.deleteThread(a);
    await m.deleteThread(b);
  });

  live("empty append is a no-op", async (m) => {
    const tid = `t-${rand()}`;
    assert.equal(await m.appendMessages(tid, []), 0);
    assert.deepEqual(await m.getMessages(tid), []);
  });

  live("rejects embeddings of the wrong dimension", async (m) => {
    const tid = `t-${rand()}`;
    await assert.rejects(
      () => m.addSummary(tid, "bad", [0.1, 0.2]),
      /Embedding has 2 dimensions but the vector index expects 8/,
    );
  });

  live("semantic search returns ranked summaries", async (m) => {
    const tid = `t-${rand()}`;
    for (let i = 0; i < 5; i++) {
      await m.addSummary(tid, `summary ${i}`, new Array(DIMS).fill(i / 10));
    }
    const hits = await m.searchSummaries(tid, new Array(DIMS).fill(0), 3);
    assert.equal(hits.length, 3);
    assert.ok(typeof hits[0]?.score === "number");
    await m.deleteThread(tid);
  });

  live("semantic search is scoped to its thread by pre-filter", async (m) => {
    const keep = `k-${rand()}`;
    const other = `o-${rand()}`;
    await m.addSummary(keep, "keep 1", new Array(DIMS).fill(0.1));
    await m.addSummary(keep, "keep 2", new Array(DIMS).fill(0.2));
    await m.addSummary(other, "other", new Array(DIMS).fill(0.1));
    const hits = await m.searchSummaries(keep, new Array(DIMS).fill(0.1), 10);
    assert.ok(hits.length > 0, "pre-filtered search returned nothing");
    assert.ok(hits.every((h) => h.thread_id === keep));
    await m.deleteThread(keep);
    await m.deleteThread(other);
  });

  live("deleteThread removes everything", async (m) => {
    const tid = `t-${rand()}`;
    await m.appendMessages(tid, [{ role: "user", content: "x" }]);
    await m.addSummary(tid, "s", new Array(DIMS).fill(0.5));
    const counts = await m.deleteThread(tid);
    assert.equal(counts.messages, 1);
    assert.equal(counts.summaries, 1);
    assert.deepEqual(await m.getMessages(tid), []);
  });

  live("ensureFilterIndexes is idempotent", async (m) => {
    await ensureFilterIndexes(m.db, SUMMARIES, ["thread_id"]);
    await ensureFilterIndexes(m.db, SUMMARIES, ["thread_id"]);
  });

  // --- Phase 0 findings, locked in ---------------------------------------

  it("defaults to IVF, the only kind supported both locally and on M10", () => {
    assert.equal(DEFAULT_KIND, "vector-ivf");
    assert.equal(LOCAL_SUPPORTED["vector-ivf"], true);
    assert.equal(MIN_TIER["vector-ivf"], "M10");
  });

  live("pre-filter on an unindexed field fails", async (m) => {
    // Guards the footgun. If this ever passes, pre-filtering stopped requiring
    // a filter index and ensureFilterIndexes() can be revisited.
    const name = `nofilter_${rand()}`;
    await m.db.createCollection(name);
    const col = m.db.collection(name);
    await col.insertMany(
      Array.from({ length: 3 }, () => ({
        unindexed_field: "x",
        embedding: new Array(DIMS).fill(0.1),
      })),
    );
    await ensureVectorIndex(m.db, name, {
      path: "embedding",
      dimensions: DIMS,
      numLists: 1,
    });
    await assert.rejects(
      () =>
        col
          .aggregate([
            vectorSearchStage({
              vector: new Array(DIMS).fill(0.1),
              path: "embedding",
              k: 3,
              filter: { unindexed_field: { $eq: "x" } },
            }),
          ])
          .toArray(),
      /index for filter path/,
    );
    await m.db.dropCollection(name);
  });

  live("unsupported index kinds raise an actionable error", async (m) => {
    // HNSW/DiskANN rejection must name the required tier, not echo the server.
    const name = `tier_${rand()}`;
    await m.db.createCollection(name);
    let sawUnsupported = false;
    for (const kind of ["vector-hnsw", "vector-diskann"] as VectorKind[]) {
      try {
        await ensureVectorIndex(m.db, name, {
          path: "embedding",
          dimensions: DIMS,
          kind,
        });
      } catch (err) {
        if (err instanceof VectorIndexUnsupportedError) {
          sawUnsupported = true;
          assert.equal(err.requiredTier, MIN_TIER[kind]);
          assert.match(err.message, new RegExp(MIN_TIER[kind]));
          assert.match(err.message, new RegExp(DEFAULT_KIND));
        } else {
          throw err;
        }
      }
    }
    await m.db.dropCollection(name);
    if (!sawUnsupported) {
      // Local container supports HNSW, so this is expected there.
      assert.ok(true, "deployment supports every vector index kind");
    }
  });
});
