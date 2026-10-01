/**
 * Memory store for the Vercel AI SDK, backed by Azure DocumentDB.
 *
 * Persists chat messages per thread and retrieves semantically relevant
 * summaries via pre-filtered vector search, over the shared memory schema.
 */

import { MongoClient, type Db, type Document } from "mongodb";

import {
  MESSAGES,
  type MemoryConfig,
  type ResolvedMemoryConfig,
  type SchemaReport,
  SESSIONS,
  SUMMARIES,
  ensureSchema,
  resolveConfig,
  utcNow,
} from "./schema.js";
import { vectorSearchStage } from "./vector.js";

/** A chat message in the Vercel AI SDK shape. */
export interface StoredMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string;
  seq?: number;
  createdAt?: Date;
  [key: string]: unknown;
}

export interface SummaryHit {
  text: string;
  score: number;
  thread_id: string;
  [key: string]: unknown;
}

/**
 * Connection URI for the local documentdb-local container.
 *
 * The container serves TLS with a self-signed certificate, so validation is
 * disabled. Local development only -- the managed service presents a real
 * certificate and must not use these flags.
 */
export function buildLocalUri(
  username: string,
  password: string,
  host = "127.0.0.1",
  port = 10260,
): string {
  const u = encodeURIComponent(username);
  const p = encodeURIComponent(password);
  return (
    `mongodb://${u}:${p}@${host}:${port}/` +
    "?tls=true&tlsAllowInvalidCertificates=true" +
    "&directConnection=true&authMechanism=SCRAM-SHA-256"
  );
}

export class DocumentDBMemory {
  readonly client: MongoClient;
  readonly config: ResolvedMemoryConfig;
  readonly db: Db;

  constructor(client: MongoClient, config: MemoryConfig = {}) {
    this.client = client;
    this.config = resolveConfig(config);
    this.db = client.db(this.config.database);
  }

  static fromUri(uri: string, config: MemoryConfig = {}): DocumentDBMemory {
    return new DocumentDBMemory(
      new MongoClient(uri, { serverSelectionTimeoutMS: 20_000 }),
      config,
    );
  }

  /** Create collections and indexes. Idempotent. */
  async setup(): Promise<SchemaReport> {
    await this.client.connect();
    return ensureSchema(this.db, this.config);
  }

  // --- messages ---------------------------------------------------------

  /**
   * Append messages to a thread.
   *
   * Sequence numbers are assigned server-side from the current max, so callers
   * never manage ordering. The unique (thread_id, seq) index means a racing
   * duplicate fails loudly rather than silently reordering history.
   */
  async appendMessages(
    threadId: string,
    messages: readonly StoredMessage[],
  ): Promise<number> {
    if (messages.length === 0) return 0;

    const last = await this.db
      .collection(MESSAGES)
      .find({ thread_id: threadId }, { projection: { seq: 1 } })
      .sort({ seq: -1 })
      .limit(1)
      .toArray();
    let seq = (last[0]?.seq as number | undefined) ?? -1;

    const docs = messages.map((m) => ({
      ...m,
      thread_id: threadId,
      namespace: this.config.namespace,
      seq: ++seq,
      createdAt: m.createdAt ?? utcNow(),
    }));
    await this.db.collection(MESSAGES).insertMany(docs);

    await this.db.collection(SESSIONS).updateOne(
      { thread_id: threadId },
      {
        $set: { updated_at: utcNow(), last_seq: seq },
        $setOnInsert: {
          thread_id: threadId,
          namespace: this.config.namespace,
          created_at: utcNow(),
        },
      },
      { upsert: true },
    );
    return docs.length;
  }

  /** Load a thread's messages in order. `limit` keeps the most RECENT n. */
  async getMessages(
    threadId: string,
    limit?: number,
  ): Promise<StoredMessage[]> {
    if (limit !== undefined) {
      const recent = await this.db
        .collection(MESSAGES)
        .find({ thread_id: threadId }, { projection: { _id: 0 } })
        .sort({ seq: -1 })
        .limit(limit)
        .toArray();
      return recent.reverse() as unknown as StoredMessage[];
    }
    const all = await this.db
      .collection(MESSAGES)
      .find({ thread_id: threadId }, { projection: { _id: 0 } })
      .sort({ seq: 1 })
      .toArray();
    return all as unknown as StoredMessage[];
  }

  // --- summaries / semantic recall --------------------------------------

  /** Store a condensed memory with its embedding. */
  async addSummary(
    threadId: string,
    text: string,
    embedding: number[],
    metadata: Document = {},
  ): Promise<void> {
    const expected = this.config.vectorIndex.dimensions;
    if (embedding.length !== expected) {
      throw new Error(
        `Embedding has ${embedding.length} dimensions but the vector index ` +
          `expects ${expected}. Set embeddingDimensions to match your model ` +
          `(text-embedding-3-small is 1536).`,
      );
    }
    await this.db.collection(SUMMARIES).insertOne({
      ...metadata,
      thread_id: threadId,
      namespace: this.config.namespace,
      text,
      embedding,
      created_at: utcNow(),
    });
  }

  /**
   * Retrieve the most relevant summaries for a thread.
   *
   * Scoped to `thread_id` via vector pre-filter, which is why `setup()` creates
   * the filter index -- without it the server rejects the query outright.
   */
  async searchSummaries(
    threadId: string,
    queryEmbedding: number[],
    k = 5,
  ): Promise<SummaryHit[]> {
    const stage = vectorSearchStage({
      vector: queryEmbedding,
      path: this.config.vectorIndex.path,
      k,
      filter: { thread_id: { $eq: threadId } },
    });
    const results = await this.db
      .collection(SUMMARIES)
      .aggregate([
        stage,
        {
          $project: {
            _id: 0,
            text: 1,
            thread_id: 1,
            score: { $meta: "searchScore" },
          },
        },
      ])
      .toArray();
    return results as unknown as SummaryHit[];
  }

  // --- lifecycle --------------------------------------------------------

  /** Remove a thread everywhere. Returns per-collection deleted counts. */
  async deleteThread(threadId: string): Promise<Record<string, number>> {
    const q = { thread_id: threadId };
    const [messages, summaries, sessions] = await Promise.all([
      this.db.collection(MESSAGES).deleteMany(q),
      this.db.collection(SUMMARIES).deleteMany(q),
      this.db.collection(SESSIONS).deleteMany(q),
    ]);
    return {
      messages: messages.deletedCount,
      summaries: summaries.deletedCount,
      sessions: sessions.deletedCount,
    };
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}
