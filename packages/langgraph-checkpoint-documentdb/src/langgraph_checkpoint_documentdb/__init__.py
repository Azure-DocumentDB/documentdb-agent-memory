"""LangGraph checkpointer backed by Azure DocumentDB."""

from .checkpointer import CHECKPOINTS, CHECKPOINT_WRITES, DocumentDBSaver, build_local_uri
from .schema import (
    FILTER_FIELDS,
    MESSAGES,
    SCHEMA_VERSION,
    SESSIONS,
    SUMMARIES,
    MemoryConfig,
    ensure_schema,
)
from .vector import (
    DEFAULT_KIND,
    LOCAL_SUPPORTED,
    MIN_TIER,
    VectorIndexSpec,
    VectorIndexUnsupported,
    ensure_filter_indexes,
    ensure_vector_index,
    vector_search_stage,
)

__version__ = "0.1.0"

__all__ = [
    "DocumentDBSaver",
    "build_local_uri",
    "CHECKPOINTS",
    "CHECKPOINT_WRITES",
    "MemoryConfig",
    "ensure_schema",
    "SCHEMA_VERSION",
    "SESSIONS",
    "MESSAGES",
    "SUMMARIES",
    "FILTER_FIELDS",
    "VectorIndexSpec",
    "VectorIndexUnsupported",
    "ensure_vector_index",
    "ensure_filter_indexes",
    "vector_search_stage",
    "DEFAULT_KIND",
    "MIN_TIER",
    "LOCAL_SUPPORTED",
    "__version__",
]
