import type { Db, Document } from "mongodb";
import { indexCompatible, IncompatibleSchemaError } from "./schema.js";
export type VectorKind = "vector-ivf" | "vector-hnsw" | "vector-diskann";
export const DEFAULT_KIND: VectorKind = "vector-ivf";
export type Similarity = "COS" | "L2" | "IP";
export interface VectorIndexSpec { path: string; dimensions: number; kind?: VectorKind; similarity?: Similarity; numLists?: number; m?: number; efConstruction?: number; maxDegree?: number; lBuild?: number; }
export function positiveInteger(n: number, name: string, max: number): void {
  if (!Number.isSafeInteger(n) || n < 1 || n > max) throw new Error(`${name} must be an integer in 1..${max}`);
}
export function validateVector(vector: readonly number[], dimensions: number): void {
  if (!Array.isArray(vector) || vector.length !== dimensions || vector.some(n => typeof n !== "number" || !Number.isFinite(n))) throw new Error(`Embedding must contain exactly ${dimensions} finite numbers`);
}
export function validateVectorSpec(spec: VectorIndexSpec): void {
  positiveInteger(spec.dimensions, "dimensions", 16000);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(spec.path)) throw new Error("Invalid vector path");
  if (!["vector-ivf", "vector-hnsw", "vector-diskann"].includes(spec.kind ?? DEFAULT_KIND)) throw new Error("Invalid vector kind");
  if (!["COS", "IP", "L2"].includes(spec.similarity ?? "COS")) throw new Error("Invalid similarity");
  for (const field of ["numLists", "m", "efConstruction", "maxDegree", "lBuild"] as const) if (spec[field] !== undefined) positiveInteger(spec[field], field, 100000);
}
export function cosmosSearchOptions(spec: VectorIndexSpec): Document {
  validateVectorSpec(spec);
  const kind = spec.kind ?? DEFAULT_KIND;
  return { kind, similarity: spec.similarity ?? "COS", dimensions: spec.dimensions,
    ...(kind === "vector-ivf" ? { numLists: spec.numLists ?? 1 } : kind === "vector-hnsw" ? { m: spec.m ?? 16, efConstruction: spec.efConstruction ?? 64 } : { maxDegree: spec.maxDegree ?? 32, lBuild: spec.lBuild ?? 50 }) };
}
export class VectorIndexUnsupportedError extends Error {
  constructor(readonly kind: VectorKind) { super(`Vector index ${kind} is unsupported by this deployment. Check current service capabilities/tier; IVF is this adapter's default. No automatic downgrade was performed.`); this.name = "VectorIndexUnsupportedError"; }
}
async function ensureIndex(db: Db, collection: string, spec: Document): Promise<string> {
  const indexes = await db.collection(collection).listIndexes().toArray();
  const existing = indexes.find(i => i.name === spec.name);
  if (existing) {
    if (!indexCompatible(existing, spec)) throw new IncompatibleSchemaError(`${collection} index ${spec.name} differs`);
    return spec.name as string;
  }
  await db.command({ createIndexes: collection, indexes: [spec] });
  return spec.name as string;
}
export async function ensureFilterIndexes(db: Db, collection: string, fields: readonly string[]): Promise<string[]> {
  const names: string[] = [];
  for (const field of fields) {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field)) throw new Error("Invalid filter field");
    names.push(await ensureIndex(db, collection, { name: `${field}_filter`, key: { [field]: 1 } }));
  }
  return names;
}
export async function ensureVectorIndex(db: Db, collection: string, spec: VectorIndexSpec, name?: string): Promise<string> {
  const kind = spec.kind ?? DEFAULT_KIND;
  try { return await ensureIndex(db, collection, { name: name ?? `${spec.path}_${kind.replace(/-/g, "_")}`, key: { [spec.path]: "cosmosSearch" }, cosmosSearchOptions: cosmosSearchOptions(spec) }); }
  catch (err) {
    if (err instanceof Error && /not supported for this cluster tier|is not enabled for this cluster tier|Invalid search index kind/.test(err.message)) throw new VectorIndexUnsupportedError(kind);
    throw err;
  }
}
export function vectorSearchStage(args: { vector: number[]; path: string; k: number; filter?: Document }): Document {
  positiveInteger(args.k, "k", 100);
  validateVector(args.vector, args.vector.length);
  if (!args.vector.length) throw new Error("Embedding must not be empty");
  return { $search: { cosmosSearch: { vector: args.vector, path: args.path, k: args.k, ...(args.filter ? { filter: args.filter } : {}) } } };
}
