"""Azure DocumentDB persistence for the released LangGraph checkpoint API."""
from .checkpointer import DocumentDBSaver
from .connection import AzureOIDCCallback, build_local_uri, build_native_uri, client_from_env, entra_client, validate_managed_uri
from .schema import CHECKPOINTS, CHECKPOINT_WRITES, COLLECTIONS, FILTER_FIELDS, MESSAGES, SCHEMA_VERSION, SESSIONS, SUMMARIES, MemoryConfig, SchemaMismatchError, ensure_schema
from .vector import DEFAULT_KIND, VectorIndexSpec, VectorIndexUnsupported, ensure_filter_indexes, ensure_vector_index, vector_search_stage

__version__ = '0.2.0'
__all__ = ['DocumentDBSaver', 'MemoryConfig', 'SchemaMismatchError', 'build_local_uri', 'build_native_uri', 'client_from_env', 'entra_client', 'validate_managed_uri', 'AzureOIDCCallback', 'VectorIndexSpec', 'VectorIndexUnsupported', 'ensure_schema', 'ensure_filter_indexes', 'ensure_vector_index', 'vector_search_stage', 'SCHEMA_VERSION', 'CHECKPOINTS', 'CHECKPOINT_WRITES', 'COLLECTIONS', 'SESSIONS', 'MESSAGES', 'SUMMARIES', 'FILTER_FIELDS', 'DEFAULT_KIND']
