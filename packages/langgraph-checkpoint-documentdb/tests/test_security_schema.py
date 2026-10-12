from types import SimpleNamespace
import time
import mongomock
import pytest
from langgraph_checkpoint_documentdb import *
from langgraph_checkpoint_documentdb.connection import AZURE_SCOPE
from langgraph_checkpoint_documentdb.schema import INDEXES

HOST = 'example.global.mongocluster.cosmos.azure.com'


def test_native_tls_exact_host_and_encoded_password():
    uri = build_native_uri(HOST, 'user@tenant', 'p@:/$?#')
    assert 'p%40%3A%2F%24%3F%23' in uri
    assert validate_managed_uri(uri, allowed_host=HOST) == uri
    for bad in [uri.replace('tls=true', 'tls=false'), uri + '&tlsAllowInvalidCertificates=true', uri + '&tls=true', uri.replace(HOST, 'evil.example'), uri.replace('mongodb+srv:', 'mongodb:')]:
        with pytest.raises(ValueError):
            validate_managed_uri(bad, allowed_host=HOST)
    with pytest.raises(ValueError):
        build_native_uri('evil.example', 'user', 'password')


def test_local_tls_opt_in_loopback_only():
    assert 'tlsAllowInvalidCertificates' not in build_local_uri('u', 'p')
    assert 'tlsAllowInvalidCertificates=true' in build_local_uri('u', 'p', allow_self_signed=True)
    assert '[::1]' in build_local_uri('u', 'p', '::1', allow_self_signed=True)
    for host in ['evil.example', '192.168.0.1', 'localhost.evil.example']:
        with pytest.raises(ValueError):
            build_local_uri('u', 'p', host, allow_self_signed=True)


def test_oidc_fixed_audience_positive_expiry():
    calls = []
    credential = SimpleNamespace(get_token=lambda scope: (calls.append(scope) or SimpleNamespace(token='synthetic-not-a-secret', expires_on=time.time()+600)))
    result = AzureOIDCCallback(credential).fetch(SimpleNamespace(idp_info={'audience': 'evil'}))
    assert calls == [AZURE_SCOPE] and 0 < result.expires_in_seconds <= 600
    credential.get_token = lambda scope: SimpleNamespace(token='expired-test', expires_on=0)
    with pytest.raises(ValueError, match='expired'):
        AzureOIDCCallback(credential).fetch(None)


def fake_vector_command(db, monkeypatch):
    def command(cmd):
        spec = cmd['indexes'][0]
        if spec['name'] in db[cmd['createIndexes']].index_information():
            return {'ok': 1}
        db[cmd['createIndexes']].create_index(list(spec['key'].items()), name=spec['name'], cosmosSearchOptions=spec['cosmosSearchOptions'])
        # mongomock discards unknown index options; preserve catalog only for tests.
        db[cmd['createIndexes']]._store.indexes[spec['name']]['cosmosSearchOptions'] = spec['cosmosSearchOptions']
        return {'ok': 1}
    monkeypatch.setattr(db, 'command', command)


def test_shared_schema_names_and_idempotence(monkeypatch):
    db = mongomock.MongoClient().test_schema
    fake_vector_command(db, monkeypatch)
    cfg = MemoryConfig(embedding_dimensions=3)
    ensure_schema(db, cfg)
    assert ensure_schema(db, cfg)['schema_version'] == 2
    for collection, (name, keys, unique) in INDEXES.items():
        idx = db[collection].index_information()[name]
        assert idx['key'] == keys and bool(idx.get('unique')) == unique
    info = db.summaries.index_information()
    assert info['embedding_vector_ivf']['cosmosSearchOptions']['numLists'] == 1
    assert info['namespace_filter']['key'] == [('namespace', 1)]
    assert info['thread_id_filter']['key'] == [('thread_id', 1)]


@pytest.mark.parametrize('legacy', ['index', 'row', 'vector'])
def test_incompatible_schema_fails_before_mutation(monkeypatch, legacy):
    db = mongomock.MongoClient().test_schema
    fake_vector_command(db, monkeypatch)
    if legacy == 'index':
        db.sessions.create_index('thread_id', unique=True, name='thread_id_unique')
    elif legacy == 'row':
        db.messages.insert_one({'thread_id': 'old', 'seq': 1})
    else:
        ensure_schema(db, MemoryConfig(embedding_dimensions=4))
    before = {n: db[n].index_information() for n in db.list_collection_names()}
    with pytest.raises((SchemaMismatchError, ValueError)):
        ensure_schema(db, MemoryConfig(embedding_dimensions=3))
    assert {n: db[n].index_information() for n in db.list_collection_names()} == before


def test_vector_filters_and_validation():
    stage = vector_search_stage(vector=[1., 0., 0.], k=3, namespace='A', thread_id='T', dimensions=3)
    assert stage['$search']['cosmosSearch']['filter'] == {'namespace': {'$eq': 'A'}, 'thread_id': {'$eq': 'T'}}
    for vector in [[float('nan')], [True], [], ['bad']]:
        with pytest.raises(ValueError):
            vector_search_stage(vector=vector, k=1, namespace='A', thread_id='T')
    with pytest.raises(ValueError):
        MemoryConfig(namespace={'$ne': None})


def test_untrusted_msgpack_constructor_is_blocked(monkeypatch):
    import ormsgpack
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    serializer = JsonPlusSerializer(pickle_fallback=False, allowed_msgpack_modules=None, allowed_json_modules=None)
    # Deliberately references a side-effect-capable constructor, never invoked.
    payload = ormsgpack.packb(ormsgpack.Ext(1, ormsgpack.packb(('os', 'system', 'must-not-run'))))
    from unittest.mock import Mock
    import os
    execute = Mock(side_effect=AssertionError('must not execute'))
    monkeypatch.setattr(os, 'system', execute)
    assert serializer.loads_typed(('msgpack', payload)) == 'must-not-run'
    execute.assert_not_called()


def test_entra_factory_actual_pymongo_options_no_dns(monkeypatch):
    """Actual pinned MongoClient option parsing, with ONLY transport disabled."""
    from unittest.mock import Mock
    import pymongo
    from pymongo.synchronous import srv_resolver
    import langgraph_checkpoint_documentdb.connection as connection
    import azure.identity
    inert = Mock()
    construct_credential = Mock(return_value=inert)
    monkeypatch.setattr(azure.identity, 'DefaultAzureCredential', construct_credential)
    monkeypatch.setenv('DOCUMENTDB_AUTH', 'entra')
    monkeypatch.setenv('DOCUMENTDB_HOST', HOST)
    monkeypatch.setenv('AZURE_CLIENT_ID', 'offline-test-principal')
    dns = Mock(side_effect=AssertionError('no DNS permitted'))
    monkeypatch.setattr(srv_resolver._SrvResolver, 'get_hosts', dns)
    # connect=False retains real option/auth parsing without sockets or DNS.
    def offline_real_client(*args, **kwargs):
        return pymongo.MongoClient(*args, **kwargs, connect=False)
    monkeypatch.setattr(connection, 'MongoClient', offline_real_client)
    client = connection.client_from_env()
    try:
        credentials = client.options.pool_options._credentials
        assert credentials.mechanism == 'MONGODB-OIDC'
        assert isinstance(credentials.mechanism_properties.callback, AzureOIDCCallback)
        assert credentials.mechanism_properties.callback.credential is inert
        context = client.options.pool_options._ssl_context
        import ssl
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname
        construct_credential.assert_called_once_with(managed_identity_client_id='offline-test-principal')
        inert.get_token.assert_not_called()
        dns.assert_not_called()
    finally:
        client.close()
    construct_credential.reset_mock()
    monkeypatch.setenv('DOCUMENTDB_HOST', 'untrusted.example')
    with pytest.raises(ValueError, match='Azure'):
        connection.client_from_env()
    construct_credential.assert_not_called()
