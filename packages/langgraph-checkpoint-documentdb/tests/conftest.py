import os
import uuid
import mongomock
import pytest
from langgraph_checkpoint_documentdb import DocumentDBSaver, MemoryConfig


def pytest_addoption(parser):
    parser.addoption('--live', action='store_true', help='Require managed Azure database; missing configuration FAILS')


def pytest_configure(config):
    if config.getoption('--live'):
        required = ['DOCUMENTDB_LIVE', 'DOCUMENTDB_DATABASE', 'DOCUMENTDB_NAMESPACE', 'DOCUMENTDB_HOST', 'DOCUMENTDB_AUTH']
        if any(not os.environ.get(k) for k in required) or os.environ.get('DOCUMENTDB_LIVE') != '1':
            raise pytest.UsageError('Live gate requires DOCUMENTDB_LIVE=1, DOCUMENTDB_DATABASE, DOCUMENTDB_NAMESPACE, DOCUMENTDB_HOST, DOCUMENTDB_AUTH; never skipped')
        if not os.environ['DOCUMENTDB_DATABASE'].startswith(('test_', 'readiness_')):
            raise pytest.UsageError('Live test database must start with test_ or readiness_')
        if os.environ['DOCUMENTDB_AUTH'] == 'native' and any(not os.environ.get(k) for k in ['DOCUMENTDB_USERNAME', 'DOCUMENTDB_PASSWORD']):
            raise pytest.UsageError('Native live gate requires username/password')


@pytest.fixture
def saver(request, monkeypatch):
    if request.config.getoption('--live'):
        cfg = MemoryConfig(database=os.environ['DOCUMENTDB_DATABASE'], namespace=os.environ['DOCUMENTDB_NAMESPACE'] + '-' + uuid.uuid4().hex, embedding_dimensions=3)
        instance = DocumentDBSaver.from_env(cfg)
        instance.client.admin.command('ping')
        instance.setup()
    else:
        instance = DocumentDBSaver(mongomock.MongoClient(), MemoryConfig(database='test_offline', namespace='offline', embedding_dimensions=3))
        from test_security_schema import fake_vector_command
        fake_vector_command(instance.db, monkeypatch)
        instance.setup()
    yield instance
    # Close lifecycle tests may have closed the factory-owned client. Reopen
    # solely for cleanup; never bypass the managed connection policy.
    cleanup = DocumentDBSaver.from_env(instance.config) if request.config.getoption('--live') and instance._closed else instance
    # No database drop. Only namespaces created by this individual test.
    for name in ('sessions', 'messages', 'summaries', 'checkpoints', 'checkpoint_writes'):
        for namespace in [instance.config.namespace, instance.config.namespace + '-other']:
            # administrative test teardown scoped to generated namespace
            cleanup.db[name].delete_many({'namespace': namespace})
    cleanup.close()
    instance.close()


def pytest_collection_modifyitems(config, items):
    if not config.getoption('--live'):
        removed = [item for item in items if item.path.name == 'test_live.py']
        items[:] = [item for item in items if item not in removed]
        config.hook.pytest_deselected(items=removed)
