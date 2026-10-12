"""Validated Azure DocumentDB vector helpers. Availability is deployment-specific."""
from __future__ import annotations
from dataclasses import dataclass
import math
from typing import Any, Iterable, Literal
from pymongo.database import Database
from pymongo.errors import OperationFailure

VectorKind = Literal['vector-ivf', 'vector-hnsw', 'vector-diskann']
DEFAULT_KIND: VectorKind = 'vector-ivf'


class VectorIndexUnsupported(RuntimeError):
    pass


@dataclass(frozen=True)
class VectorIndexSpec:
    path: str
    dimensions: int
    kind: VectorKind = DEFAULT_KIND
    similarity: Literal['COS', 'L2', 'IP'] = 'COS'
    num_lists: int = 1
    m: int = 16
    ef_construction: int = 64
    max_degree: int = 32
    l_build: int = 50

    def __post_init__(self):
        if not isinstance(self.path, str) or not self.path or self.path.startswith('$') or '\x00' in self.path:
            raise ValueError('invalid vector path')
        if self.kind not in ('vector-ivf', 'vector-hnsw', 'vector-diskann') or self.similarity not in ('COS', 'L2', 'IP'):
            raise ValueError('invalid vector kind/similarity')
        for value in (self.dimensions, self.num_lists, self.m, self.ef_construction, self.max_degree, self.l_build):
            if type(value) is not int or value <= 0:
                raise ValueError('vector dimensions/index parameters must be positive integers')

    def cosmos_search_options(self) -> dict[str, Any]:
        opts = {'kind': self.kind, 'similarity': self.similarity, 'dimensions': self.dimensions}
        if self.kind == 'vector-ivf':
            opts['numLists'] = self.num_lists
        elif self.kind == 'vector-hnsw':
            opts.update(m=self.m, efConstruction=self.ef_construction)
        else:
            opts.update(maxDegree=self.max_degree, lBuild=self.l_build)
        return opts


def validate_vector_catalog(db: Database, collection: str, spec: VectorIndexSpec, *, name: str | None = None) -> None:
    expected_name = name or f'{spec.path}_{spec.kind.replace("-", "_")}'
    for idx in db[collection].list_indexes():
        key = dict(idx['key'])
        if idx['name'] == expected_name or 'cosmosSearch' in key.values():
            options = idx.get('cosmosSearchOptions', {})
            expected = spec.cosmos_search_options()
            if idx['name'] != expected_name or key != {spec.path: 'cosmosSearch'} or options != expected or idx.get('unique') or idx.get('sparse') or idx.get('partialFilterExpression') or 'expireAfterSeconds' in idx:
                raise ValueError('Incompatible vector catalog; use a new database or migrate offline. No index was dropped.')


def ensure_filter_indexes(db: Database, collection: str, fields: Iterable[str]) -> list[str]:
    result = []
    for field in fields:
        if not isinstance(field, str) or not field or field.startswith('$'):
            raise ValueError('invalid filter field')
        name = f'{field}_filter'
        db[collection].create_index([(field, 1)], name=name)
        result.append(name)
    return result


def ensure_vector_index(db: Database, collection: str, spec: VectorIndexSpec, *, name: str | None = None) -> str:
    index_name = name or f'{spec.path}_{spec.kind.replace("-", "_")}'
    validate_vector_catalog(db, collection, spec, name=index_name)
    if any(idx['name'] == index_name for idx in db[collection].list_indexes()):
        return index_name
    try:
        db.command({'createIndexes': collection, 'indexes': [{'name': index_name, 'key': {spec.path: 'cosmosSearch'}, 'cosmosSearchOptions': spec.cosmos_search_options()}]})
    except OperationFailure as exc:
        if any(msg in str(exc) for msg in ('not supported', 'not enabled', 'Invalid search index kind')):
            raise VectorIndexUnsupported(f'{spec.kind} unavailable on this deployment; verify current Azure tier/region support. IVF is the default.') from exc
        raise
    return index_name


def vector_search_stage(*, vector: list[float], path: str = 'embedding', k: int, namespace: str, thread_id: str, dimensions: int | None = None) -> dict[str, Any]:
    from .schema import text
    text(namespace, 'namespace')
    text(thread_id, 'thread_id')
    text(path, 'path')
    if not isinstance(vector, list) or not vector or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector):
        raise ValueError('vector must be a nonempty finite numeric list')
    if dimensions is not None and (type(dimensions) is not int or dimensions < 1 or len(vector) != dimensions):
        raise ValueError('vector dimensions mismatch')
    if type(k) is not int or not 1 <= k <= 1000:
        raise ValueError('k must be an integer between 1 and 1000')
    return {'$search': {'cosmosSearch': {'vector': vector, 'path': path, 'k': k, 'filter': {'namespace': {'$eq': namespace}, 'thread_id': {'$eq': thread_id}}}}}
