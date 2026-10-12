"""Shared schema v2: validate the entire existing catalog before any mutation."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from pymongo.database import Database
from pymongo.errors import CollectionInvalid
from .vector import VectorIndexSpec, ensure_filter_indexes, ensure_vector_index, validate_vector_catalog

SESSIONS, MESSAGES, SUMMARIES = 'sessions', 'messages', 'summaries'
CHECKPOINTS, CHECKPOINT_WRITES = 'checkpoints', 'checkpoint_writes'
COLLECTIONS = (SESSIONS, MESSAGES, SUMMARIES, CHECKPOINTS, CHECKPOINT_WRITES)
FILTER_FIELDS = ('namespace', 'thread_id')
SCHEMA_VERSION = 2
INDEXES = {
    SESSIONS: ('memory_v2_sessions_namespace_thread', [('namespace', 1), ('thread_id', 1)], True),
    MESSAGES: ('memory_v2_messages_namespace_thread_seq', [('namespace', 1), ('thread_id', 1), ('seq', 1)], True),
    SUMMARIES: ('memory_v2_summaries_namespace_thread', [('namespace', 1), ('thread_id', 1)], False),
    CHECKPOINTS: ('memory_v2_checkpoints_namespace_thread_ns_id', [('namespace', 1), ('thread_id', 1), ('checkpoint_ns', 1), ('checkpoint_id', 1)], True),
    CHECKPOINT_WRITES: ('memory_v2_writes_namespace_thread_ns_id_task_idx', [('namespace', 1), ('thread_id', 1), ('checkpoint_ns', 1), ('checkpoint_id', 1), ('task_id', 1), ('idx', 1)], True),
}


def text(value: Any, name: str, *, empty: bool = False) -> str:
    if not isinstance(value, str) or '\x00' in value or (not empty and not value.strip()):
        raise ValueError(f'{name} must be a {"possibly empty" if empty else "nonempty"} string without NUL')
    return value


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class MemoryConfig:
    database: str = 'agent_memory'
    embedding_dimensions: int = 1536
    vector_index: VectorIndexSpec | None = None
    namespace: str = 'default'

    def __post_init__(self):
        text(self.database, 'database')
        text(self.namespace, 'namespace')
        if any(c in self.database for c in '/\\. "$*<>:|?'):
            raise ValueError('invalid database name')
        if type(self.embedding_dimensions) is not int or self.embedding_dimensions <= 0:
            raise ValueError('embedding_dimensions must be a positive integer')
        if self.vector_index is not None and (not isinstance(self.vector_index, VectorIndexSpec) or self.vector_index.path != 'embedding' or self.vector_index.dimensions != self.embedding_dimensions):
            raise ValueError('vector_index must use embedding and configured dimensions')

    def resolved_vector_index(self) -> VectorIndexSpec:
        return self.vector_index or VectorIndexSpec(path='embedding', dimensions=self.embedding_dimensions)


class SchemaMismatchError(RuntimeError):
    pass


def mismatch() -> None:
    raise SchemaMismatchError('Incompatible/legacy memory schema. Use a new database or explicitly migrate offline; setup never drops or rebuilds existing data/indexes.')


def ensure_schema(db: Database, config: MemoryConfig | None = None) -> dict[str, Any]:
    cfg = config or MemoryConfig()
    existing = set(db.list_collection_names())
    # Global preflight is deliberately an administrative catalog check, not a
    # tenant data API. Any legacy unscoped row blocks setup before mutation.
    for collection in COLLECTIONS:
        if collection not in existing:
            continue
        expected_name, expected_keys, unique = INDEXES[collection]
        for idx in db[collection].list_indexes():
            if idx['name'] == '_id_':
                continue
            keys = list(idx['key'].items())
            if 'expireAfterSeconds' in idx or (idx.get('collation') and idx['collation'].get('locale') != 'simple'):
                mismatch()
            if idx.get('unique') and ('namespace', 1) not in keys:
                mismatch()
            if idx['name'] == expected_name or keys == expected_keys:
                if idx['name'] != expected_name or keys != expected_keys or bool(idx.get('unique', False)) != unique or any(k in idx for k in ('partialFilterExpression', 'sparse', 'expireAfterSeconds')):
                    mismatch()
            elif idx.get('unique'):
                mismatch()
        for field in ('namespace', 'thread_id'):
            if db[collection].find_one({'$or': [{field: {'$exists': False}}, {field: None}, {field: ''}, {field: {'$not': {'$type': 'string'}}}]}, {'_id': 1}):
                mismatch()
        if collection in (CHECKPOINTS, CHECKPOINT_WRITES):
            for field in ('checkpoint_ns', 'checkpoint_id'):
                if db[collection].find_one({field: {'$not': {'$type': 'string'}}}, {'_id': 1}):
                    mismatch()
    if SUMMARIES in existing:
        validate_vector_catalog(db, SUMMARIES, cfg.resolved_vector_index())
        for idx in db[SUMMARIES].list_indexes():
            for field in FILTER_FIELDS:
                name = f'{field}_filter'
                keys = list(idx['key'].items())
                if idx['name'] == name or keys == [(field, 1)]:
                    if idx['name'] != name or keys != [(field, 1)] or idx.get('unique') or idx.get('sparse') or idx.get('partialFilterExpression'):
                        mismatch()
    for collection in COLLECTIONS:
        if collection not in existing:
            try:
                db.create_collection(collection)
            except CollectionInvalid:
                pass  # concurrent compatible setup; create_index still validates
        name, keys, unique = INDEXES[collection]
        db[collection].create_index(keys, name=name, unique=unique)
    filters = ensure_filter_indexes(db, SUMMARIES, FILTER_FIELDS)
    vector = ensure_vector_index(db, SUMMARIES, cfg.resolved_vector_index())
    return {'schema_version': SCHEMA_VERSION, 'indexes': [v[0] for v in INDEXES.values()], 'filter_indexes': filters, 'vector_index': vector}
