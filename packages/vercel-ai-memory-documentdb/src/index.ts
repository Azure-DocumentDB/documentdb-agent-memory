/** Vercel AI SDK memory adapter backed by Azure DocumentDB. */

export {
  DocumentDBMemory,
  buildLocalUri,
  type StoredMessage,
  type SummaryHit,
} from "./memory.js";

export {
  FILTER_FIELDS,
  MESSAGES,
  SCHEMA_VERSION,
  SESSIONS,
  SUMMARIES,
  ensureSchema,
  resolveConfig,
  utcNow,
  type MemoryConfig,
  type ResolvedMemoryConfig,
  type SchemaReport,
} from "./schema.js";

export {
  DEFAULT_KIND,
  LOCAL_SUPPORTED,
  MIN_TIER,
  VectorIndexUnsupportedError,
  cosmosSearchOptions,
  ensureFilterIndexes,
  ensureVectorIndex,
  vectorSearchStage,
  type Similarity,
  type VectorIndexSpec,
  type VectorKind,
} from "./vector.js";

export const VERSION = "0.1.0";
