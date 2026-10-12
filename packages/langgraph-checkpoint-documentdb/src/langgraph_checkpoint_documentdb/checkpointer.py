"""Genuine LangGraph checkpoint saver; all persistence is namespace scoped."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

from bson.binary import Binary
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver, ChannelVersions, Checkpoint, CheckpointMetadata,
    CheckpointTuple, WRITES_IDX_MAP, get_checkpoint_metadata, writes_sort_key,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from .connection import build_local_uri, client_from_env, managed_client
from .schema import CHECKPOINTS, CHECKPOINT_WRITES, COLLECTIONS, SESSIONS, MemoryConfig, ensure_schema, text, utcnow


class DocumentDBSaver(BaseCheckpointSaver[int]):
    """PyMongo saver with async I/O offloaded via ``asyncio.to_thread``.

    Injected clients are caller-configured/caller-owned by default. URI/env factories
    validate their transport and own the client. Quiesce operations before closing
    or deleting; cancelling an await does not cancel the underlying database call.
    """

    def __init__(self, client: MongoClient, config: MemoryConfig | None = None, *, owns_client: bool = False):
        super().__init__(serde=JsonPlusSerializer(pickle_fallback=False, allowed_msgpack_modules=None, allowed_json_modules=None))
        self.client = client
        if config is not None and not isinstance(config, MemoryConfig):
            raise ValueError('config must be a MemoryConfig')
        self.config = config or MemoryConfig()
        self.db = client[self.config.database]
        self._owns_client = owns_client
        self._closed = False
        self._ready = False

    @classmethod
    def from_uri(cls, uri: str, config: MemoryConfig | None = None, *, allowed_host: str) -> DocumentDBSaver:
        return cls(managed_client(uri, allowed_host=allowed_host), config, owns_client=True)

    @classmethod
    def from_env(cls, config: MemoryConfig | None = None) -> DocumentDBSaver:
        import os
        cfg = config or MemoryConfig(database=os.environ.get('DOCUMENTDB_DATABASE', 'agent_memory'), namespace=os.environ.get('DOCUMENTDB_NAMESPACE', 'default'))
        return cls(client_from_env(), cfg, owns_client=True)

    def _open(self) -> None:
        if self._closed:
            raise RuntimeError('DocumentDBSaver is closed')

    def _scope(self, thread_id: str) -> dict[str, str]:
        self._open()
        if not self._ready:
            raise RuntimeError('Call setup() before checkpoint operations')
        return {'namespace': self.config.namespace, 'thread_id': text(thread_id, 'thread_id')}

    def _key(self, config: RunnableConfig, *, require_id: bool = False) -> dict[str, str]:
        if not isinstance(config, dict) or not isinstance(config.get('configurable'), dict):
            raise ValueError('config.configurable must be an object with thread_id')
        c = config['configurable']
        key = self._scope(c.get('thread_id'))
        key['checkpoint_ns'] = text(c.get('checkpoint_ns', ''), 'checkpoint_ns', empty=True)
        cid = c.get('checkpoint_id')
        if require_id or cid is not None:
            key['checkpoint_id'] = text(cid, 'checkpoint_id')
        return key

    @staticmethod
    def _config(key: dict[str, Any]) -> RunnableConfig:
        return {'configurable': {k: key[k] for k in ('thread_id', 'checkpoint_ns', 'checkpoint_id')}}

    def _dump(self, value: Any) -> dict[str, Any]:
        tag, data = self.serde.dumps_typed(value)
        decoded = self.serde.loads_typed((tag, data))
        # JsonPlus strict mode safely returns raw fields for blocked constructors.
        # Canonical re-encoding catches that lossy fallback before persisting.
        if self.serde.dumps_typed(decoded) != (tag, data):
            raise ValueError('Value is not losslessly supported by the strict serializer; explicitly allowlist trusted custom types')
        return {'type': tag, 'data': Binary(data)}

    def _load(self, value: dict[str, Any]) -> Any:
        return self.serde.loads_typed((value['type'], bytes(value['data'])))

    def setup(self) -> dict[str, Any]:
        self._open()
        self._ready = False
        report = ensure_schema(self.db, self.config)
        self._ready = True
        return report

    def put(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, new_versions: ChannelVersions) -> RunnableConfig:
        key = self._key(config)
        if not isinstance(checkpoint, dict) or not isinstance(metadata, dict) or not isinstance(new_versions, dict):
            raise ValueError('checkpoint, metadata and new_versions must be objects')
        cid = text(checkpoint.get('id'), 'checkpoint.id')
        parent = key.pop('checkpoint_id', None)
        key['checkpoint_id'] = cid
        doc = {'checkpoint': self._dump(checkpoint), 'metadata': self._dump(get_checkpoint_metadata(config, metadata)), 'parent_checkpoint_id': parent, 'updated_at': utcnow()}
        self.db[CHECKPOINTS].update_one(key, {'$set': doc, '$setOnInsert': {'created_at': utcnow()}}, upsert=True)
        # Never write TS-owned next_seq; TS $inc initializes missing allocator.
        scope = self._scope(key['thread_id'])
        update = {'$set': {'updated_at': utcnow()}, '$setOnInsert': {'created_at': utcnow()}}
        try:
            self.db[SESSIONS].update_one(scope, update, upsert=True)
        except DuplicateKeyError:
            self.db[SESSIONS].update_one(scope, update)
        return self._config(key)

    def _tuple(self, doc: dict[str, Any]) -> CheckpointTuple:
        key = {k: doc[k] for k in ('namespace', 'thread_id', 'checkpoint_ns', 'checkpoint_id')}
        writes = list(self.db[CHECKPOINT_WRITES].find(key))
        writes.sort(key=lambda w: writes_sort_key(w.get('task_path', ''), w['task_id'], w['idx']))
        parent = doc.get('parent_checkpoint_id')
        return CheckpointTuple(config=self._config(key), checkpoint=self._load(doc['checkpoint']), metadata=self._load(doc['metadata']), parent_config=self._config({**key, 'checkpoint_id': parent}) if parent else None, pending_writes=[(w['task_id'], w['channel'], self._load(w['value'])) for w in writes])

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        doc = self.db[CHECKPOINTS].find_one(self._key(config), sort=[('checkpoint_id', -1)])
        return self._tuple(doc) if doc else None

    def list(self, config: RunnableConfig | None, *, filter: dict[str, Any] | None = None, before: RunnableConfig | None = None, limit: int | None = None) -> Iterator[CheckpointTuple]:
        self._open()
        if not self._ready:
            raise RuntimeError('Call setup() before checkpoint operations')
        if limit is not None and (type(limit) is not int or limit < 0):
            raise ValueError('limit must be a nonnegative integer')
        if filter is not None and not isinstance(filter, dict):
            raise ValueError('filter must be an object of metadata equality values')
        if filter and any(not isinstance(k, str) or k.startswith('$') or '.' in k for k in filter):
            raise ValueError('metadata filter keys must be plain field names')
        # list(None) enumerates this namespace only, per released base contract.
        query = self._key(config) if config is not None else {'namespace': self.config.namespace}
        if config is not None and 'checkpoint_ns' not in config['configurable']:
            query.pop('checkpoint_ns')
        if before is not None:
            bkey = self._key(before, require_id=True)
            if config is not None and bkey['thread_id'] != query['thread_id']:
                raise ValueError('before must belong to the same thread')
            query['checkpoint_id'] = {'$lt': bkey['checkpoint_id']}
        if limit == 0:
            return
        count = 0
        cursor = self.db[CHECKPOINTS].find(query).sort('checkpoint_id', -1)
        try:
            for doc in cursor:
                # Typed equality after decode; never execute caller Mongo operators.
                metadata = self._load(doc['metadata'])
                if filter and not all(k in metadata and metadata[k] == v for k, v in filter.items()):
                    continue
                yield self._tuple(doc)
                count += 1
                if limit is not None and count >= limit:
                    break

        finally:
            cursor.close()

    def put_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = '') -> None:
        key = self._key(config, require_id=True)
        text(task_id, 'task_id', empty=True)
        text(task_path, 'task_path', empty=True)
        if not isinstance(writes, Sequence) or isinstance(writes, (str, bytes)):
            raise ValueError('writes must be a sequence of (channel, value) pairs')
        prepared = []
        for idx, pair in enumerate(writes):
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise ValueError('each write must be a (channel, value) pair')
            channel, value = pair
            text(channel, 'channel')
            write_idx = WRITES_IDX_MAP.get(channel, idx)
            prepared.append(({**key, 'task_id': task_id, 'idx': write_idx}, {'channel': channel, 'value': self._dump(value), 'task_path': task_path}, write_idx))
        for write_key, doc, idx in prepared:
            # First-write-wins except reserved negative special-channel indices.
            update = {'$set' if idx < 0 else '$setOnInsert': doc}
            try:
                self.db[CHECKPOINT_WRITES].update_one(write_key, update, upsert=True)
            except DuplicateKeyError:
                if idx < 0:
                    self.db[CHECKPOINT_WRITES].update_one(write_key, update)
                elif not self.db[CHECKPOINT_WRITES].find_one(write_key):
                    raise

    def delete_thread(self, thread_id: str) -> None:
        self.delete_thread_counts(thread_id)

    def delete_thread_counts(self, thread_id: str) -> dict[str, int]:
        """Nontransactional five-collection lifecycle delete. Stop writers first."""
        scope = self._scope(thread_id)
        return {name: self.db[name].delete_many(scope).deleted_count for name in COLLECTIONS}

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await asyncio.to_thread(self.get_tuple, config)

    async def aput(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, new_versions: ChannelVersions) -> RunnableConfig:
        return await asyncio.to_thread(self.put, config, checkpoint, metadata, new_versions)

    async def aput_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = '') -> None:
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)

    async def alist(self, config: RunnableConfig | None, *, filter: dict[str, Any] | None = None, before: RunnableConfig | None = None, limit: int | None = None) -> AsyncIterator[CheckpointTuple]:
        iterator = self.list(config, filter=filter, before=before, limit=limit)
        sentinel = object()
        try:
            while (item := await asyncio.to_thread(next, iterator, sentinel)) is not sentinel:
                yield item
        finally:
            await asyncio.to_thread(iterator.close)

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.to_thread(self.delete_thread, thread_id)

    def close(self) -> None:
        if not self._closed and self._owns_client:
            self.client.close()
        self._closed = True

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

    def __enter__(self) -> DocumentDBSaver:
        self._open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
