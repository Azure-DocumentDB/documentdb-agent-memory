"""Shared memory schema for Azure DocumentDB agent-memory adapters.

Every adapter in this monorepo agrees on these collection shapes so that memory
written by one framework is readable by another. Changing anything here is a
cross-adapter change and must keep the whole test suite green.

Collections
-----------
sessions   one document per conversation thread
messages   append-only turns, ordered within a thread
summaries  vector-indexed condensed memory, the only collection with embeddings

Pre-filter fields (``thread_id``, ``namespace``) always get plain indexes --
see vector.py for why that is not optional.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pymongo.database import Database

from .vector import VectorIndexSpec, ensure_filter_indexes, ensure_vector_index

SESSIONS = "sessions"
MESSAGES = "messages"
SUMMARIES = "summaries"

#: Fields used as vector pre-filters. Must have plain indexes before any
#: filtered ``$search`` call.
FILTER_FIELDS = ("thread_id", "namespace")

SCHEMA_VERSION = 1


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class MemoryConfig:
    """Connection-independent configuration for the memory layer."""

    database: str = "agent_memory"
    embedding_dimensions: int = 1536
    vector_index: VectorIndexSpec | None = None
    namespace: str = "default"

    def resolved_vector_index(self) -> VectorIndexSpec:
        return self.vector_index or VectorIndexSpec(
            path="embedding", dimensions=self.embedding_dimensions
        )


def ensure_schema(db: Database, config: MemoryConfig | None = None) -> dict[str, Any]:
    """Create collections and indexes. Idempotent; safe to call on every startup."""
    cfg = config or MemoryConfig()
    report: dict[str, Any] = {"schema_version": SCHEMA_VERSION}

    existing = set(db.list_collection_names())
    for name in (SESSIONS, MESSAGES, SUMMARIES):
        if name not in existing:
            db.create_collection(name)

    db[SESSIONS].create_index([("thread_id", 1)], name="thread_id_unique", unique=True)
    db[MESSAGES].create_index(
        [("thread_id", 1), ("seq", 1)], name="thread_seq", unique=True
    )

    # Filter indexes BEFORE the vector index -- order matters for pre-filtering.
    report["filter_indexes"] = ensure_filter_indexes(db, SUMMARIES, FILTER_FIELDS)
    report["vector_index"] = ensure_vector_index(
        db, SUMMARIES, cfg.resolved_vector_index()
    )
    return report
