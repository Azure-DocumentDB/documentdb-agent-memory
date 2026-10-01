/**
 * Vector index helpers for Azure DocumentDB / documentdb-local.
 *
 * Mirrors the Python adapter's `vector.py` exactly. Both exist because of
 * measured behavior, not docs -- see docs/VECTOR_INDEXES.md.
 *
 * Two rules drive this module:
 *
 * 1. IVF is the only kind that works on BOTH the local container and the
 *    entry-level managed tier (M10). HNSW is present locally but tier-gated in
 *    the cloud (M30+); DiskANN is absent locally and tier-gated (M40+). So IVF
 *    is the default -- anything else is opt-in and may fail on deploy.
 *
 * 2. Pre-filtered vector search requires a plain index on the filter field, and
 *    it must exist BEFORE the search runs. Per-session memory retrieval is
 *    exactly that case, so we always create it.
 */

import type { Db, Document } from "mongodb";

export type VectorKind = "vector-ivf" | "vector-hnsw" | "vector-diskann";

export const DEFAULT_KIND: VectorKind = "vector-ivf";

/** Minimum managed cluster tier for each kind. */
export const MIN_TIER: Record<VectorKind, string> = {
  "vector-ivf": "M10",
  "vector-hnsw": "M30",
  "vector-diskann": "M40",
};

/** Verified against ghcr.io/documentdb/documentdb/documentdb-local (0.117.0). */
export const LOCAL_SUPPORTED: Record<VectorKind, boolean> = {
  "vector-ivf": true,
  "vector-hnsw": true,
  "vector-diskann": false,
};

export type Similarity = "COS" | "L2" | "IP";

export interface VectorIndexSpec {
  path: string;
  dimensions: number;
  kind?: VectorKind;
  similarity?: Similarity;
  /** IVF */
  numLists?: number;
  /** HNSW */
  m?: number;
  efConstruction?: number;
  /** DiskANN */
  maxDegree?: number;
  lBuild?: number;
}

/**
 * Thrown when the target cluster rejects an index kind.
 *
 * Carries remediation inline because the server message alone ("hnsw index is
 * not supported for this cluster tier") never says which tier is required.
 */
export class VectorIndexUnsupportedError extends Error {
  readonly kind: VectorKind;
  readonly requiredTier: string;

  constructor(kind: VectorKind, serverMessage: string) {
    super(
      `Vector index kind '${kind}' was rejected by this deployment.\n` +
        `  Managed Azure DocumentDB: requires tier ${MIN_TIER[kind]} or higher.\n` +
        `  Local documentdb-local container: ${
          LOCAL_SUPPORTED[kind] ? "supported" : "NOT supported"
        }.\n` +
        `  '${DEFAULT_KIND}' works on both the local container and every managed ` +
        `tier from ${MIN_TIER[DEFAULT_KIND]} up, and is the default.\n` +
        `  Server said: ${serverMessage}`,
    );
    this.name = "VectorIndexUnsupportedError";
    this.kind = kind;
    this.requiredTier = MIN_TIER[kind];
  }
}

export function cosmosSearchOptions(spec: VectorIndexSpec): Document {
  const kind = spec.kind ?? DEFAULT_KIND;
  const opts: Document = {
    kind,
    similarity: spec.similarity ?? "COS",
    dimensions: spec.dimensions,
  };
  if (kind === "vector-ivf") {
    opts.numLists = spec.numLists ?? 100;
  } else if (kind === "vector-hnsw") {
    opts.m = spec.m ?? 16;
    opts.efConstruction = spec.efConstruction ?? 64;
  } else {
    opts.maxDegree = spec.maxDegree ?? 32;
    opts.lBuild = spec.lBuild ?? 50;
  }
  return opts;
}

function isUnsupportedKindError(message: string): boolean {
  return (
    message.includes("not supported for this cluster tier") ||
    message.includes("is not enabled for this cluster tier") ||
    message.includes("Invalid search index kind")
  );
}

function isAlreadyExistsError(message: string): boolean {
  return message.includes("already exists");
}

/**
 * Create plain indexes for every field used as a vector pre-filter.
 * Must run before any filtered `$search`. Idempotent.
 */
export async function ensureFilterIndexes(
  db: Db,
  collection: string,
  fields: readonly string[],
): Promise<string[]> {
  const created: string[] = [];
  for (const field of fields) {
    const name = `${field}_filter`;
    try {
      await db.command({
        createIndexes: collection,
        indexes: [{ name, key: { [field]: 1 } }],
      });
      created.push(name);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (!isAlreadyExistsError(message)) throw err;
    }
  }
  return created;
}

/** Create a vector index, translating tier rejections into actionable errors. */
export async function ensureVectorIndex(
  db: Db,
  collection: string,
  spec: VectorIndexSpec,
  name?: string,
): Promise<string> {
  const kind = spec.kind ?? DEFAULT_KIND;
  const indexName = name ?? `${spec.path}_${kind.replace(/-/g, "_")}`;
  try {
    await db.command({
      createIndexes: collection,
      indexes: [
        {
          name: indexName,
          key: { [spec.path]: "cosmosSearch" },
          cosmosSearchOptions: cosmosSearchOptions(spec),
        },
      ],
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (isUnsupportedKindError(message)) {
      throw new VectorIndexUnsupportedError(kind, message);
    }
    if (isAlreadyExistsError(message)) return indexName;
    throw err;
  }
  return indexName;
}

/** Build a `$search` stage for cosmosSearch vector similarity. */
export function vectorSearchStage(args: {
  vector: number[];
  path: string;
  k: number;
  filter?: Document;
}): Document {
  const inner: Document = { vector: args.vector, path: args.path, k: args.k };
  if (args.filter) inner.filter = args.filter;
  return { $search: { cosmosSearch: inner } };
}
