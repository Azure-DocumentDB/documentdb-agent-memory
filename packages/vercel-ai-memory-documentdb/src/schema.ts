import type { Db, Document } from "mongodb";
import { type VectorIndexSpec, cosmosSearchOptions, ensureFilterIndexes, ensureVectorIndex, validateVectorSpec } from "./vector.js";

export const SESSIONS = "sessions";
export const MESSAGES = "messages";
export const SUMMARIES = "summaries";
export const CHECKPOINTS = "checkpoints";
export const CHECKPOINT_WRITES = "checkpoint_writes";
export const COLLECTIONS = [SESSIONS, MESSAGES, SUMMARIES, CHECKPOINTS, CHECKPOINT_WRITES] as const;
export const FILTER_FIELDS = ["namespace", "thread_id"] as const;
export const SCHEMA_VERSION = 2;
export const SHARED_INDEXES = [
  { collection: SESSIONS, name: "memory_v2_sessions_namespace_thread", key: { namespace: 1, thread_id: 1 }, unique: true },
  { collection: MESSAGES, name: "memory_v2_messages_namespace_thread_seq", key: { namespace: 1, thread_id: 1, seq: 1 }, unique: true },
  { collection: SUMMARIES, name: "memory_v2_summaries_namespace_thread", key: { namespace: 1, thread_id: 1 }, unique: false },
  { collection: CHECKPOINTS, name: "memory_v2_checkpoints_namespace_thread_ns_id", key: { namespace: 1, thread_id: 1, checkpoint_ns: 1, checkpoint_id: 1 }, unique: true },
  { collection: CHECKPOINT_WRITES, name: "memory_v2_writes_namespace_thread_ns_id_task_idx", key: { namespace: 1, thread_id: 1, checkpoint_ns: 1, checkpoint_id: 1, task_id: 1, idx: 1 }, unique: true },
] as const;

export function validateScope(value: string, label: string): string {
  if (typeof value !== "string" || !value.trim() || value.length > 512 || /[\u0000-\u001f\u007f]/.test(value)) throw new Error(`Invalid ${label}: expected nonempty string of at most 512 characters without controls`);
  return value;
}
export interface MemoryConfig { database?: string; embeddingDimensions?: number; vectorIndex?: VectorIndexSpec; namespace?: string; }
export interface ResolvedMemoryConfig { database: string; embeddingDimensions: number; vectorIndex: VectorIndexSpec; namespace: string; }
export function resolveConfig(config: MemoryConfig = {}): ResolvedMemoryConfig {
  const dimensions = config.embeddingDimensions ?? config.vectorIndex?.dimensions ?? 1536;
  const vectorIndex = { path: "embedding", dimensions, ...config.vectorIndex };
  validateVectorSpec(vectorIndex);
  if (vectorIndex.path !== "embedding" || dimensions !== vectorIndex.dimensions) throw new Error("Shared schema requires embedding path and matching dimensions");
  const database = config.database ?? "agent_memory";
  if (!/^[A-Za-z0-9_-]{1,63}$/.test(database)) throw new Error("Invalid database name");
  return { database, embeddingDimensions: dimensions, vectorIndex, namespace: validateScope(config.namespace ?? "default", "namespace") };
}
export interface SchemaReport { schemaVersion: number; filterIndexes: string[]; vectorIndex: string; collections: string[]; }
export class IncompatibleSchemaError extends Error {
  constructor(detail: string) { super(`Incompatible memory schema: ${detail}. Use a new database or explicitly migrate offline; setup never deletes or migrates data.`); this.name = "IncompatibleSchemaError"; }
}
export function sameKey(a: Document, b: Document): boolean { return JSON.stringify(Object.entries(a)) === JSON.stringify(Object.entries(b)); }
export function indexCompatible(actual: Document, wanted: Document): boolean {
  return sameKey(actual.key, wanted.key) && Boolean(actual.unique) === Boolean(wanted.unique) &&
    !actual.sparse && !actual.partialFilterExpression && actual.expireAfterSeconds === undefined &&
    (!actual.collation || actual.collation.locale === "simple") &&
    (!wanted.cosmosSearchOptions || JSON.stringify(Object.entries(actual.cosmosSearchOptions ?? {}).sort()) === JSON.stringify(Object.entries(wanted.cosmosSearchOptions).sort()));
}
export async function ensureSchema(db: Db, config: MemoryConfig = {}): Promise<SchemaReport> {
  const cfg = resolveConfig(config);
  const existing = new Set((await db.listCollections({}, { nameOnly: true }).toArray()).map(c => c.name));
  const vectorName = `${cfg.vectorIndex.path}_${(cfg.vectorIndex.kind ?? "vector-ivf").replace(/-/g, "_")}`;
  // Inspect the entire catalog/data before making any schema writes. No error-text swallowing.
  for (const name of COLLECTIONS) {
    if (!existing.has(name)) continue;
    const collection = db.collection(name);
    const malformed = await collection.findOne({ $or: [
      { namespace: { $exists: false } }, { thread_id: { $exists: false } },
      { namespace: { $not: { $type: "string" } } }, { thread_id: { $not: { $type: "string" } } },
      { namespace: "" }, { thread_id: "" },
    ] }, { projection: { _id: 1 } });
    if (malformed) throw new IncompatibleSchemaError(`${name} contains unscoped/invalid records`);
    if (name === CHECKPOINTS || name === CHECKPOINT_WRITES) {
      for (const field of ["checkpoint_ns", "checkpoint_id"]) {
        if (await collection.findOne({ [field]: { $not: { $type: "string" } } }, { projection: { _id: 1 } })) throw new IncompatibleSchemaError(`${name} contains invalid checkpoint dimensions`);
      }
    }
    const wanted: Document[] = SHARED_INDEXES.filter(i => i.collection === name).map(i => ({ name: i.name, key: i.key, unique: i.unique }));
    if (name === SUMMARIES) wanted.push(...FILTER_FIELDS.map(f => ({ name: `${f}_filter`, key: { [f]: 1 }, unique: false })), { name: vectorName, key: { embedding: "cosmosSearch" }, cosmosSearchOptions: cosmosSearchOptions(cfg.vectorIndex) });
    for (const index of await collection.listIndexes().toArray()) {
      if (index.name === "_id_") continue;
      if (index.expireAfterSeconds !== undefined || (index.collation && index.collation.locale !== "simple")) throw new IncompatibleSchemaError(`${name} has incompatible lifecycle/collation index ${index.name}`);
      const expected = wanted.find(i => i.name === index.name);
      if (expected && !indexCompatible(index, expected)) throw new IncompatibleSchemaError(`${name} index ${index.name} has incompatible options`);
      if (!expected && (index.unique || index.expireAfterSeconds !== undefined || Object.values(index.key).includes("cosmosSearch"))) throw new IncompatibleSchemaError(`${name} has legacy or conflicting index ${index.name}`);
      const equivalent = wanted.find(i => sameKey(i.key, index.key));
      if (!expected && equivalent) throw new IncompatibleSchemaError(`${name} index must use shared name ${equivalent.name}`);
    }
  }
  for (const name of COLLECTIONS) {
    if (!existing.has(name)) {
      try { await db.createCollection(name); } catch (err) { if (!(err && typeof err === "object" && "code" in err && err.code === 48)) throw err; }
    }
  }
  for (const index of SHARED_INDEXES) await db.collection(index.collection).createIndex(index.key, { name: index.name, unique: index.unique });
  const filterIndexes = await ensureFilterIndexes(db, SUMMARIES, FILTER_FIELDS);
  const vectorIndex = await ensureVectorIndex(db, SUMMARIES, cfg.vectorIndex);
  return { schemaVersion: SCHEMA_VERSION, filterIndexes, vectorIndex, collections: [...COLLECTIONS] };
}
export function utcNow(): Date { return new Date(); }
