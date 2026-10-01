"""Vector index helpers for Azure DocumentDB / documentdb-local.

Everything in here exists because of empirically verified behavior, not docs.
See docs/VECTOR_INDEXES.md for the test matrix that produced these rules.

Two rules drive this module:

1. IVF is the only index kind that works on BOTH the local container and the
   entry-level managed tier (M10). HNSW is present locally but tier-gated in the
   cloud (M30+); DiskANN is absent locally and tier-gated in the cloud (M40+).
   So IVF is the default. Anything else is opt-in and may fail on deploy.

2. Pre-filtered vector search requires a plain index on the filter field, and it
   must exist BEFORE the search runs. Without it the server returns
   "The index for filter path 'X' was not found". Per-session memory retrieval is
   exactly the pre-filter case, so we always create it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal

from pymongo.database import Database
from pymongo.errors import OperationFailure

VectorKind = Literal["vector-ivf", "vector-hnsw", "vector-diskann"]

DEFAULT_KIND: VectorKind = "vector-ivf"

# Minimum managed cluster tier for each kind. Local container support differs and
# is tracked separately -- see LOCAL_SUPPORTED.
MIN_TIER: dict[VectorKind, str] = {
    "vector-ivf": "M10",
    "vector-hnsw": "M30",
    "vector-diskann": "M40",
}

# Verified against ghcr.io/documentdb/documentdb/documentdb-local (0.117.0).
LOCAL_SUPPORTED: dict[VectorKind, bool] = {
    "vector-ivf": True,
    "vector-hnsw": True,
    "vector-diskann": False,
}


class VectorIndexUnsupported(RuntimeError):
    """Raised when the target cluster rejects an index kind.

    Carries the remediation inline because the server message alone
    ("hnsw index is not supported for this cluster tier") does not say which
    tier is required.
    """


@dataclass(frozen=True)
class VectorIndexSpec:
    """Declarative vector index definition."""

    path: str
    dimensions: int
    kind: VectorKind = DEFAULT_KIND
    similarity: Literal["COS", "L2", "IP"] = "COS"
    # IVF
    num_lists: int = 100
    # HNSW
    m: int = 16
    ef_construction: int = 64
    # DiskANN
    max_degree: int = 32
    l_build: int = 50

    def cosmos_search_options(self) -> dict[str, Any]:
        opts: dict[str, Any] = {
            "kind": self.kind,
            "similarity": self.similarity,
            "dimensions": self.dimensions,
        }
        if self.kind == "vector-ivf":
            opts["numLists"] = self.num_lists
        elif self.kind == "vector-hnsw":
            opts["m"] = self.m
            opts["efConstruction"] = self.ef_construction
        elif self.kind == "vector-diskann":
            opts["maxDegree"] = self.max_degree
            opts["lBuild"] = self.l_build
        return opts


def _is_unsupported_kind_error(exc: OperationFailure) -> bool:
    msg = str(exc)
    return (
        "not supported for this cluster tier" in msg
        or "is not enabled for this cluster tier" in msg
        or "Invalid search index kind" in msg
    )


def ensure_filter_indexes(
    db: Database, collection: str, fields: Iterable[str]
) -> list[str]:
    """Create plain indexes for every field used as a vector pre-filter.

    Must run before any filtered ``$search``. Idempotent.
    """
    created: list[str] = []
    for field in fields:
        name = f"{field}_filter"
        try:
            db.command(
                {
                    "createIndexes": collection,
                    "indexes": [{"name": name, "key": {field: 1}}],
                }
            )
            created.append(name)
        except OperationFailure as exc:
            # Index already present in an equivalent form is not an error.
            if "already exists" not in str(exc):
                raise
    return created


def ensure_vector_index(
    db: Database,
    collection: str,
    spec: VectorIndexSpec,
    *,
    name: str | None = None,
) -> str:
    """Create a vector index, translating tier rejections into actionable errors."""
    index_name = name or f"{spec.path}_{spec.kind.replace('-', '_')}"
    try:
        db.command(
            {
                "createIndexes": collection,
                "indexes": [
                    {
                        "name": index_name,
                        "key": {spec.path: "cosmosSearch"},
                        "cosmosSearchOptions": spec.cosmos_search_options(),
                    }
                ],
            }
        )
    except OperationFailure as exc:
        if _is_unsupported_kind_error(exc):
            raise VectorIndexUnsupported(
                f"Vector index kind {spec.kind!r} was rejected by this deployment.\n"
                f"  Managed Azure DocumentDB: requires tier {MIN_TIER[spec.kind]} or higher.\n"
                f"  Local documentdb-local container: "
                f"{'supported' if LOCAL_SUPPORTED[spec.kind] else 'NOT supported'}.\n"
                f"  {DEFAULT_KIND!r} works on both the local container and every managed "
                f"tier from {MIN_TIER[DEFAULT_KIND]} up, and is the default.\n"
                f"  Server said: {exc}"
            ) from exc
        if "already exists" in str(exc):
            return index_name
        raise
    return index_name


def vector_search_stage(
    *,
    vector: list[float],
    path: str,
    k: int,
    filter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a ``$search`` stage for cosmosSearch vector similarity."""
    inner: dict[str, Any] = {"vector": vector, "path": path, "k": k}
    if filter:
        inner["filter"] = filter
    return {"$search": {"cosmosSearch": inner}}
