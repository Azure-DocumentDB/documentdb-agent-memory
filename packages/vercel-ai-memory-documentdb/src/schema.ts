/**
 * Shared memory schema for Azure DocumentDB agent-memory adapters.
 *
 * This is the TypeScript mirror of the Python adapter's `schema.py`. The
 * collection names, field names and filter fields MUST stay identical so that
 * memory written by one framework is readable by another. Changing anything
 * here is a cross-adapter change.
 *
 * Collections
 * -----------
 * sessions   one document per conversation thread
 * messages   append-only turns, ordered within a thread
 * summaries  vector-indexed condensed memory, the only collection with embeddings
 */

import type { Db } from "mongodb";

import {
  type VectorIndexSpec,
  ensureFilterIndexes,
  ensureVectorIndex,
} from "./vector.js";

export const SESSIONS = "sessions";
export const MESSAGES = "messages";
export const SUMMARIES = "summaries";

/**
 * Fields used as vector pre-filters. Must have plain indexes before any
 * filtered `$search` call -- see vector.ts.
 */
export const FILTER_FIELDS = ["thread_id", "namespace"] as const;

/** Must match SCHEMA_VERSION in the Python adapter. */
export const SCHEMA_VERSION = 1;

export interface MemoryConfig {
  database?: string;
  embeddingDimensions?: number;
  vectorIndex?: VectorIndexSpec;
  namespace?: string;
}

export interface ResolvedMemoryConfig {
  database: string;
  embeddingDimensions: number;
  vectorIndex: VectorIndexSpec;
  namespace: string;
}

export function resolveConfig(config: MemoryConfig = {}): ResolvedMemoryConfig {
  const embeddingDimensions = config.embeddingDimensions ?? 1536;
  return {
    database: config.database ?? "agent_memory",
    embeddingDimensions,
    namespace: config.namespace ?? "default",
    vectorIndex: config.vectorIndex ?? {
      path: "embedding",
      dimensions: embeddingDimensions,
    },
  };
}

export interface SchemaReport {
  schemaVersion: number;
  filterIndexes: string[];
  vectorIndex: string;
}

/** Create collections and indexes. Idempotent; safe on every startup. */
export async function ensureSchema(
  db: Db,
  config: MemoryConfig = {},
): Promise<SchemaReport> {
  const cfg = resolveConfig(config);

  const existing = new Set(
    (await db.listCollections({}, { nameOnly: true }).toArray()).map(
      (c) => c.name,
    ),
  );
  for (const name of [SESSIONS, MESSAGES, SUMMARIES]) {
    if (!existing.has(name)) await db.createCollection(name);
  }

  await db
    .collection(SESSIONS)
    .createIndex({ thread_id: 1 }, { name: "thread_id_unique", unique: true });
  await db
    .collection(MESSAGES)
    .createIndex({ thread_id: 1, seq: 1 }, { name: "thread_seq", unique: true });

  // Filter indexes BEFORE the vector index -- order matters for pre-filtering.
  const filterIndexes = await ensureFilterIndexes(db, SUMMARIES, FILTER_FIELDS);
  const vectorIndex = await ensureVectorIndex(db, SUMMARIES, cfg.vectorIndex);

  return { schemaVersion: SCHEMA_VERSION, filterIndexes, vectorIndex };
}

export function utcNow(): Date {
  return new Date();
}
