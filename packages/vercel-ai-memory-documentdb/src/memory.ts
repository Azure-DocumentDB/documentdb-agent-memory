import { modelMessageSchema, type ModelMessage, type SystemModelMessage } from "ai";
import { setTimeout as delay } from "node:timers/promises";
import { type MongoClient, type Db, type Document } from "mongodb";
import { clientFromEnvironment, clientFromUri } from "./auth.js";
import { COLLECTIONS, MESSAGES, SESSIONS, SUMMARIES, ensureSchema, resolveConfig, validateScope, type MemoryConfig, type ResolvedMemoryConfig, type SchemaReport } from "./schema.js";
import { positiveInteger, validateVector, vectorSearchStage } from "./vector.js";
export type StoredMessage = ModelMessage & { seq: number; created_at: Date };
export interface SummaryHit { text: string; score: number; namespace: string; thread_id: string; }
const RESERVED = new Set(["_id", "namespace", "thread_id", "seq", "next_seq", "last_seq", "created_at", "createdAt", "updated_at", "role", "content", "text", "embedding", "checkpoint_ns", "checkpoint_id", "task_id", "idx"]);
const MAX_BYTES = 1_000_000;
function jsonSafe(value: unknown, depth = 0): unknown {
  if (depth > 64) throw new Error("Message/metadata nesting exceeds 64");
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map(v => jsonSafe(v, depth + 1));
  if (value && typeof value === "object" && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)) {
    const out: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    for (const [key, val] of Object.entries(value)) {
      if (["__proto__", "prototype", "constructor"].includes(key) || key.startsWith("$") || key.includes(".") || key.includes("\0")) throw new Error("Unsafe document key");
      // SDK responses contain optional properties set to undefined. Omit object properties only.
      if (val !== undefined) out[key] = jsonSafe(val, depth + 1);
    }
    return out;
  }
  throw new Error("Only JSON-safe message/metadata values are supported (no files, binary, URL objects or cycles)");
}
export function normalizeMessage(message: ModelMessage): ModelMessage {
  if (!message || typeof message !== "object" || Object.keys(message).some(k => !["role", "content", "providerOptions"].includes(k))) throw new Error("Message has reserved or unsupported fields; supply role/content/providerOptions only");
  const clean = jsonSafe(message) as ModelMessage;
  if (Array.isArray(clean.content) && clean.content.some(p => !["text", "reasoning", "tool-call", "tool-result", "tool-approval-request", "tool-approval-response"].includes(p.type))) throw new Error("Unsupported message part: file/image/custom content is not persisted");
  if (!modelMessageSchema.safeParse(clean).success) throw new Error("Invalid AI SDK ModelMessage");
  if (Buffer.byteLength(JSON.stringify(clean)) > MAX_BYTES) throw new Error("Message exceeds 1000000 UTF-8 bytes");
  return JSON.parse(JSON.stringify(clean)) as ModelMessage;
}
export class AppendMessagesError extends Error {
  constructor(readonly reservedStart: number, readonly reservedCount: number, options?: ErrorOptions) { super("Message insert failed after reserving sequence range; insertion may be partial or committed. Inspect storage before retrying; no exactly-once guarantee.", options); this.name = "AppendMessagesError"; }
}
function duplicateKey(err: unknown): boolean { return Boolean(err && typeof err === "object" && "code" in err && err.code === 11000); }
export class DocumentDBMemory {
  readonly config: ResolvedMemoryConfig;
  readonly db: Db;
  private ready = false;
  /** Injected clients are caller-trusted: caller owns endpoint/TLS/auth and must disable unacknowledged writes. close() closes this client. */
  constructor(readonly client: MongoClient, config: MemoryConfig = {}) { this.config = resolveConfig(config); this.db = client.db(this.config.database); }
  static fromUri(uri: string, config: MemoryConfig = {}): DocumentDBMemory { const cfg = resolveConfig(config); return new DocumentDBMemory(clientFromUri(uri), cfg); }
  static async fromEnvironment(env: NodeJS.ProcessEnv = process.env, config: MemoryConfig = {}): Promise<DocumentDBMemory> { const cfg = resolveConfig(config); return new DocumentDBMemory(await clientFromEnvironment(env), cfg); }
  async setup(): Promise<SchemaReport> {
    this.ready = false;
    try { await this.client.connect(); const report = await ensureSchema(this.db, this.config); this.ready = true; return report; }
    catch (err) { await this.client.close().catch(() => {}); throw err; }
  }
  private scope(threadId: string): { namespace: string; thread_id: string } { return { namespace: this.config.namespace, thread_id: validateScope(threadId, "thread_id") }; }
  private requireSetup(): void { if (!this.ready) throw new Error("Call and await setup() before memory operations; shared unique indexes are required"); }
  /** Atomic reservation orders batches, not completion times. Failed inserts may leave gaps or partial batches. */
  async appendMessages(threadId: string, messages: readonly ModelMessage[]): Promise<number> {
    const scope = this.scope(threadId);
    if (!Array.isArray(messages) || messages.length > 1000) throw new Error("Batch must contain at most 1000 messages");
    const normalized = messages.map(normalizeMessage);
    this.requireSetup();
    if (!normalized.length) return 0;
    if (Buffer.byteLength(JSON.stringify(normalized)) > 8_000_000) throw new Error("Batch exceeds 8000000 UTF-8 bytes");
    let end: number | undefined;
    // Upsert equality scope is protected by the shared unique index. The bounded predicate
    // prevents unsafe-number overflow. Missing next_seq on Python-only sessions increments from 0.
    for (let attempt = 0; attempt < 20; attempt++) {
      try {
        const session = await this.db.collection(SESSIONS).findOneAndUpdate({ ...scope, $or: [
          { next_seq: { $exists: false } }, { next_seq: { $gte: 0, $lte: Number.MAX_SAFE_INTEGER - normalized.length, $type: "number" } },
        ] }, { $inc: { next_seq: normalized.length }, $set: { updated_at: new Date() }, $setOnInsert: { ...scope, created_at: new Date() } }, { upsert: true, returnDocument: "after", includeResultMetadata: false, writeConcern: { w: 1 } });
        if (!session || !Number.isSafeInteger(session.next_seq) || session.next_seq < normalized.length) throw new Error("Invalid session sequence allocator; inspect schema offline");
        end = session.next_seq as number;
        break;
      } catch (err) {
        if (!duplicateKey(err) || attempt === 19) throw err;
        // Retry ONLY a server-rejected duplicate-key allocation, never unknown network/write outcomes.
        await delay(Math.min(2 ** attempt, 50));
      }
    }
    if (end === undefined) throw new Error("Could not reserve message sequence range");
    const start = end - normalized.length;
    const docs = normalized.map((m, i) => ({ ...m, ...scope, seq: start + i, created_at: new Date() }));
    try {
      const result = await this.db.collection(MESSAGES).insertMany(docs, { ordered: true, writeConcern: { w: 1 } });
      if (!result.acknowledged || result.insertedCount !== docs.length) throw new Error("Message batch was not fully acknowledged");
      return result.insertedCount;
    } catch (cause) { throw new AppendMessagesError(start, docs.length, { cause }); }
  }
  async getMessages(threadId: string, limit?: number): Promise<StoredMessage[]> {
    const scope = this.scope(threadId);
    if (limit !== undefined) positiveInteger(limit, "limit", 10000);
    this.requireSetup();
    let cursor = this.db.collection(MESSAGES).find(scope, { projection: { _id: 0 } }).sort({ seq: limit === undefined ? 1 : -1 });
    if (limit !== undefined) cursor = cursor.limit(limit);
    const docs = await cursor.toArray();
    if (limit !== undefined) docs.reverse();
    return docs.map(d => ({ ...normalizeMessage({ role: d.role, content: d.content, ...(d.providerOptions ? { providerOptions: d.providerOptions } : {}) } as ModelMessage), seq: d.seq as number, created_at: d.created_at as Date }));
  }
  /** SDK-ready messages, without storage metadata. Limiting may truncate tool-call pairs; caller owns prompt budgeting. */
  async getModelMessages(threadId: string, limit?: number): Promise<ModelMessage[]> { return (await this.getMessages(threadId, limit)).map(({ seq: _seq, created_at: _created, ...message }) => message as ModelMessage); }
  /** AI SDK 7 requires system messages in instructions, not in messages. */
  async getPrompt(threadId: string): Promise<{ instructions: SystemModelMessage[]; messages: Exclude<ModelMessage, SystemModelMessage>[] }> {
    const history = await this.getModelMessages(threadId);
    return { instructions: history.filter((m): m is SystemModelMessage => m.role === "system"), messages: history.filter((m): m is Exclude<ModelMessage, SystemModelMessage> => m.role !== "system") };
  }
  async addSummary(threadId: string, text: string, embedding: number[], metadata: Document = {}): Promise<void> {
    const scope = this.scope(threadId);
    if (typeof text !== "string" || !text.trim() || Buffer.byteLength(text) > MAX_BYTES) throw new Error("Summary text must be nonempty and at most 1000000 UTF-8 bytes");
    validateVector(embedding, this.config.embeddingDimensions);
    if (!metadata || typeof metadata !== "object" || Array.isArray(metadata) || Object.keys(metadata).some(k => RESERVED.has(k))) throw new Error("Metadata contains reserved fields");
    const safe = jsonSafe(metadata) as Document;
    if (Buffer.byteLength(JSON.stringify(safe)) > MAX_BYTES) throw new Error("Metadata exceeds 1000000 UTF-8 bytes");
    this.requireSetup();
    await this.db.collection(SUMMARIES).insertOne({ ...safe, ...scope, text, embedding, created_at: new Date() }, { writeConcern: { w: 1 } });
  }
  async searchSummaries(threadId: string, queryEmbedding: number[], k = 5): Promise<SummaryHit[]> {
    const scope = this.scope(threadId);
    validateVector(queryEmbedding, this.config.embeddingDimensions);
    const stage = vectorSearchStage({ vector: queryEmbedding, path: this.config.vectorIndex.path, k, filter: { namespace: { $eq: scope.namespace }, thread_id: { $eq: scope.thread_id } } });
    this.requireSetup();
    return await this.db.collection(SUMMARIES).aggregate([stage, { $project: { _id: 0, text: 1, namespace: 1, thread_id: 1, score: { $meta: "searchScore" } } }]).toArray() as unknown as SummaryHit[];
  }
  /** Quiesce ALL TS/Python writers first. Five scoped deletes are not a distributed transaction. Retry is safe while quiescent. */
  async deleteThread(threadId: string): Promise<Record<string, number>> {
    const scope = this.scope(threadId), counts: Record<string, number> = {};
    this.requireSetup();
    for (const name of COLLECTIONS) counts[name] = (await this.db.collection(name).deleteMany(scope, { writeConcern: { w: 1 } })).deletedCount;
    return counts;
  }
  async close(): Promise<void> { this.ready = false; await this.client.close(); }
}
