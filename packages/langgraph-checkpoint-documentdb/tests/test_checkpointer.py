"""Integration tests against a real DocumentDB instance.

Run the local container first:

    docker compose -f docker/docker-compose.yml up -d

Config via env:
    DOCUMENTDB_URI       full connection URI (wins if set)
    DOCUMENTDB_USERNAME  default "appuser"
    DOCUMENTDB_PASSWORD  required unless DOCUMENTDB_URI is set
    DOCUMENTDB_HOST      default 127.0.0.1
    DOCUMENTDB_PORT      default 10260

Tests skip (not fail) when no instance is reachable, so a plain `pytest` on a
laptop with nothing running stays green.
"""

from __future__ import annotations

import os
import uuid

import pytest
from pymongo.errors import PyMongoError

from langgraph_checkpoint_documentdb import (
    DEFAULT_KIND,
    LOCAL_SUPPORTED,
    MIN_TIER,
    DocumentDBSaver,
    MemoryConfig,
    VectorIndexSpec,
    VectorIndexUnsupported,
    build_local_uri,
    ensure_filter_indexes,
    ensure_vector_index,
    vector_search_stage,
)
from langgraph_checkpoint_documentdb.schema import SUMMARIES

pytestmark = pytest.mark.integration

DIMS = 8


def _uri() -> str | None:
    if os.environ.get("DOCUMENTDB_URI"):
        return os.environ["DOCUMENTDB_URI"]
    pw = os.environ.get("DOCUMENTDB_PASSWORD")
    if not pw:
        return None
    return build_local_uri(
        os.environ.get("DOCUMENTDB_USERNAME", "appuser"),
        pw,
        os.environ.get("DOCUMENTDB_HOST", "127.0.0.1"),
        int(os.environ.get("DOCUMENTDB_PORT", "10260")),
    )


@pytest.fixture(scope="session")
def saver():
    uri = _uri()
    if not uri:
        pytest.skip("set DOCUMENTDB_URI or DOCUMENTDB_PASSWORD")
    cfg = MemoryConfig(
        database=f"test_mem_{uuid.uuid4().hex[:8]}",
        embedding_dimensions=DIMS,
        vector_index=VectorIndexSpec(
            path="embedding", dimensions=DIMS, num_lists=1
        ),
    )
    s = DocumentDBSaver.from_uri(uri, cfg)
    try:
        s.client.admin.command("ping")
    except PyMongoError as exc:
        pytest.skip(f"no reachable DocumentDB: {exc}")
    s.setup()
    yield s
    s.client.drop_database(cfg.database)
    s.close()


def test_server_is_wire_compatible(saver):
    info = saver.client.admin.command("buildInfo")
    hello = saver.client.admin.command("hello")
    assert info["version"].startswith("7.")
    assert hello["maxWireVersion"] >= 21


def test_setup_is_idempotent(saver):
    first = saver.setup()
    second = saver.setup()
    assert first["schema_version"] == second["schema_version"]
    assert second["vector_index"]


def test_put_and_get_roundtrip(saver):
    tid = f"t-{uuid.uuid4().hex[:8]}"
    saver.put(tid, "ckpt-1", {"channel_values": {"count": 1}}, {"step": 1})
    got = saver.get(tid, "ckpt-1")
    assert got["checkpoint"]["channel_values"]["count"] == 1
    assert got["metadata"]["step"] == 1
    saver.delete_thread(tid)


def test_get_latest_without_id(saver):
    tid = f"t-{uuid.uuid4().hex[:8]}"
    saver.put(tid, "c1", {"channel_values": {"n": 1}})
    saver.put(tid, "c2", {"channel_values": {"n": 2}})
    assert saver.get(tid)["checkpoint"]["channel_values"]["n"] == 2
    saver.delete_thread(tid)


def test_put_is_upsert_not_duplicate(saver):
    tid = f"t-{uuid.uuid4().hex[:8]}"
    saver.put(tid, "c1", {"channel_values": {"n": 1}})
    saver.put(tid, "c1", {"channel_values": {"n": 99}})
    assert len(list(saver.list(tid))) == 1
    assert saver.get(tid, "c1")["checkpoint"]["channel_values"]["n"] == 99
    saver.delete_thread(tid)


def test_threads_are_isolated(saver):
    a, b = f"a-{uuid.uuid4().hex[:6]}", f"b-{uuid.uuid4().hex[:6]}"
    saver.put(a, "c1", {"channel_values": {"who": "a"}})
    saver.put(b, "c1", {"channel_values": {"who": "b"}})
    assert saver.get(a, "c1")["checkpoint"]["channel_values"]["who"] == "a"
    assert saver.get(b, "c1")["checkpoint"]["channel_values"]["who"] == "b"
    assert len(list(saver.list(a))) == 1
    saver.delete_thread(a)
    saver.delete_thread(b)


def test_writes_roundtrip_in_order(saver):
    tid = f"t-{uuid.uuid4().hex[:8]}"
    saver.put(tid, "c1", {"channel_values": {}})
    saver.put_writes(tid, "c1", "task-1", [("messages", "hello"), ("messages", "world")])
    writes = saver.get_writes(tid, "c1")
    assert [w["value"] for w in writes] == ["hello", "world"]
    assert [w["idx"] for w in writes] == [0, 1]
    saver.delete_thread(tid)


def test_delete_thread_removes_everything(saver):
    tid = f"t-{uuid.uuid4().hex[:8]}"
    saver.put(tid, "c1", {"channel_values": {}})
    saver.put_writes(tid, "c1", "task-1", [("messages", "x")])
    counts = saver.delete_thread(tid)
    assert counts["checkpoints"] == 1
    assert counts["writes"] == 1
    assert saver.get(tid, "c1") is None


# --- vector behavior: the Phase 0 findings, locked in --------------------------


def test_default_kind_is_ivf():
    """IVF is the only kind that works on both local and the entry managed tier."""
    assert DEFAULT_KIND == "vector-ivf"
    assert LOCAL_SUPPORTED["vector-ivf"] is True
    assert MIN_TIER["vector-ivf"] == "M10"


def test_vector_search_returns_ranked_results(saver):
    db = saver.db
    col = db[SUMMARIES]
    tid = f"v-{uuid.uuid4().hex[:8]}"
    col.insert_many(
        [
            {
                "thread_id": tid,
                "namespace": "default",
                "text": f"summary {i}",
                "embedding": [float(i)] * DIMS,
            }
            for i in range(5)
        ]
    )
    stage = vector_search_stage(vector=[0.0] * DIMS, path="embedding", k=3)
    results = list(col.aggregate([stage, {"$project": {"text": 1, "_id": 0}}]))
    assert len(results) == 3
    col.delete_many({"thread_id": tid})


def test_prefiltered_search_is_scoped_to_thread(saver):
    """Per-session retrieval -- the case that needs a filter index to exist."""
    db = saver.db
    col = db[SUMMARIES]
    keep, other = f"k-{uuid.uuid4().hex[:6]}", f"o-{uuid.uuid4().hex[:6]}"
    col.insert_many(
        [
            {"thread_id": keep, "namespace": "default", "embedding": [0.1] * DIMS},
            {"thread_id": keep, "namespace": "default", "embedding": [0.2] * DIMS},
            {"thread_id": other, "namespace": "default", "embedding": [0.1] * DIMS},
        ]
    )
    stage = vector_search_stage(
        vector=[0.1] * DIMS,
        path="embedding",
        k=10,
        filter={"thread_id": {"$eq": keep}},
    )
    results = list(col.aggregate([stage, {"$project": {"thread_id": 1, "_id": 0}}]))
    assert results, "pre-filtered search returned nothing"
    assert all(r["thread_id"] == keep for r in results)
    col.delete_many({"thread_id": {"$in": [keep, other]}})


def test_prefilter_without_filter_index_fails(saver):
    """Guards the footgun: filtering an unindexed field is a server error.

    If this ever starts passing, pre-filtering stopped requiring a filter index
    and ensure_filter_indexes() can be revisited.
    """
    db = saver.db
    name = f"nofilter_{uuid.uuid4().hex[:8]}"
    db.create_collection(name)
    col = db[name]
    col.insert_many(
        [{"unindexed_field": "x", "embedding": [0.1] * DIMS} for _ in range(3)]
    )
    ensure_vector_index(
        db, name, VectorIndexSpec(path="embedding", dimensions=DIMS, num_lists=1)
    )
    stage = vector_search_stage(
        vector=[0.1] * DIMS,
        path="embedding",
        k=3,
        filter={"unindexed_field": {"$eq": "x"}},
    )
    with pytest.raises(PyMongoError, match="index for filter path"):
        list(col.aggregate([stage]))
    db.drop_collection(name)


def test_ensure_filter_indexes_is_idempotent(saver):
    ensure_filter_indexes(saver.db, SUMMARIES, ["thread_id"])
    ensure_filter_indexes(saver.db, SUMMARIES, ["thread_id"])


def test_unsupported_kind_raises_actionable_error(saver):
    """HNSW/DiskANN rejection must name the required tier, not just echo the server."""
    db = saver.db
    name = f"tier_{uuid.uuid4().hex[:8]}"
    db.create_collection(name)
    unsupported = []
    for kind in ("vector-hnsw", "vector-diskann"):
        try:
            ensure_vector_index(
                db, name, VectorIndexSpec(path="embedding", dimensions=DIMS, kind=kind)
            )
        except VectorIndexUnsupported as exc:
            unsupported.append(kind)
            assert MIN_TIER[kind] in str(exc)
            assert DEFAULT_KIND in str(exc)
    db.drop_collection(name)
    if not unsupported:
        pytest.skip("this deployment supports every vector index kind")
