"""LangGraph checkpointer backed by Azure DocumentDB / documentdb-local.

Implements the ``BaseCheckpointSaver`` contract over the shared memory schema so
checkpoints written here interoperate with the other adapters in this monorepo.
"""

from __future__ import annotations

import urllib.parse
from typing import Any, Iterator, Sequence

from pymongo import ASCENDING, MongoClient
from pymongo.database import Database

from .schema import MESSAGES, SESSIONS, MemoryConfig, ensure_schema, utcnow

CHECKPOINTS = "checkpoints"
CHECKPOINT_WRITES = "checkpoint_writes"


def build_local_uri(
    username: str,
    password: str,
    host: str = "127.0.0.1",
    port: int = 10260,
) -> str:
    """Connection URI for the local documentdb-local container.

    The container serves TLS with a self-signed certificate, so certificate
    validation is disabled. This is for local development only -- the managed
    service presents a real certificate and must not use these flags.
    """
    pw = urllib.parse.quote_plus(password)
    user = urllib.parse.quote_plus(username)
    return (
        f"mongodb://{user}:{pw}@{host}:{port}/"
        "?tls=true&tlsAllowInvalidCertificates=true"
        "&directConnection=true&authMechanism=SCRAM-SHA-256"
    )


class DocumentDBSaver:
    """Persist LangGraph checkpoints in Azure DocumentDB.

    Usage::

        saver = DocumentDBSaver.from_uri(uri)
        saver.setup()
    """

    def __init__(self, client: MongoClient, config: MemoryConfig | None = None):
        self.client = client
        self.config = config or MemoryConfig()
        self.db: Database = client[self.config.database]

    @classmethod
    def from_uri(
        cls, uri: str, config: MemoryConfig | None = None, **kwargs: Any
    ) -> "DocumentDBSaver":
        kwargs.setdefault("serverSelectionTimeoutMS", 20000)
        return cls(MongoClient(uri, **kwargs), config)

    def setup(self) -> dict[str, Any]:
        """Create collections and indexes. Idempotent."""
        report = ensure_schema(self.db, self.config)
        self.db[CHECKPOINTS].create_index(
            [("thread_id", ASCENDING), ("checkpoint_ns", ASCENDING), ("checkpoint_id", ASCENDING)],
            name="thread_ns_checkpoint",
            unique=True,
        )
        self.db[CHECKPOINT_WRITES].create_index(
            [
                ("thread_id", ASCENDING),
                ("checkpoint_ns", ASCENDING),
                ("checkpoint_id", ASCENDING),
                ("task_id", ASCENDING),
                ("idx", ASCENDING),
            ],
            name="write_identity",
            unique=True,
        )
        return report

    # --- checkpoint persistence -------------------------------------------------

    def put(
        self,
        thread_id: str,
        checkpoint_id: str,
        checkpoint: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        checkpoint_ns: str = "",
        parent_checkpoint_id: str | None = None,
    ) -> dict[str, Any]:
        doc = {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_checkpoint_id,
            "checkpoint": checkpoint,
            "metadata": metadata or {},
            "namespace": self.config.namespace,
            "updated_at": utcnow(),
        }
        self.db[CHECKPOINTS].update_one(
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
            {"$set": doc, "$setOnInsert": {"created_at": utcnow()}},
            upsert=True,
        )
        self.db[SESSIONS].update_one(
            {"thread_id": thread_id},
            {
                "$set": {"last_checkpoint_id": checkpoint_id, "updated_at": utcnow()},
                "$setOnInsert": {
                    "thread_id": thread_id,
                    "namespace": self.config.namespace,
                    "created_at": utcnow(),
                },
            },
            upsert=True,
        )
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    def get(
        self,
        thread_id: str,
        checkpoint_id: str | None = None,
        checkpoint_ns: str = "",
    ) -> dict[str, Any] | None:
        query: dict[str, Any] = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}
        if checkpoint_id is not None:
            query["checkpoint_id"] = checkpoint_id
            return self.db[CHECKPOINTS].find_one(query, {"_id": 0})
        return self.db[CHECKPOINTS].find_one(
            query, {"_id": 0}, sort=[("updated_at", -1)]
        )

    def list(
        self,
        thread_id: str,
        checkpoint_ns: str = "",
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        cur = self.db[CHECKPOINTS].find(
            {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns},
            {"_id": 0},
            sort=[("updated_at", -1)],
        )
        if limit is not None:
            cur = cur.limit(limit)
        return iter(cur)

    def put_writes(
        self,
        thread_id: str,
        checkpoint_id: str,
        task_id: str,
        writes: Sequence[tuple[str, Any]],
        checkpoint_ns: str = "",
    ) -> None:
        if not writes:
            return
        for idx, (channel, value) in enumerate(writes):
            self.db[CHECKPOINT_WRITES].update_one(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                    "task_id": task_id,
                    "idx": idx,
                },
                {
                    "$set": {
                        "channel": channel,
                        "value": value,
                        "namespace": self.config.namespace,
                        "created_at": utcnow(),
                    }
                },
                upsert=True,
            )

    def get_writes(
        self, thread_id: str, checkpoint_id: str, checkpoint_ns: str = ""
    ) -> list[dict[str, Any]]:
        return list(
            self.db[CHECKPOINT_WRITES].find(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                },
                {"_id": 0},
                sort=[("idx", ASCENDING)],
            )
        )

    def delete_thread(self, thread_id: str) -> dict[str, int]:
        return {
            "checkpoints": self.db[CHECKPOINTS].delete_many({"thread_id": thread_id}).deleted_count,
            "writes": self.db[CHECKPOINT_WRITES].delete_many({"thread_id": thread_id}).deleted_count,
            "messages": self.db[MESSAGES].delete_many({"thread_id": thread_id}).deleted_count,
            "sessions": self.db[SESSIONS].delete_many({"thread_id": thread_id}).deleted_count,
        }

    def close(self) -> None:
        self.client.close()
