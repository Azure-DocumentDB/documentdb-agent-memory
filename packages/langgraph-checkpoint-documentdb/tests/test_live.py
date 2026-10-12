"""Only collected by --live; absence is deselection, never an integration pass."""
import uuid
from langgraph_checkpoint_documentdb import DocumentDBSaver, vector_search_stage
from test_checkpointer import graph, config


def test_managed_client_recreation_and_scoped_vector_recall(saver):
    thread = 'live-' + uuid.uuid4().hex
    app = graph(saver)
    assert app.invoke({'count': 1, 'messages': []}, config(thread))['count'] == 4
    reopened = DocumentDBSaver.from_env(saver.config)
    try:
        reopened.setup()
        assert graph(reopened).get_state(config(thread)).values['count'] == 4
        assert graph(reopened).invoke({'count': 5}, config(thread))['count'] == 8
        saver.db.summaries.insert_many([
            {'namespace': saver.config.namespace, 'thread_id': thread, 'text': 'keep', 'embedding': [1., 0., 0.]},
            {'namespace': saver.config.namespace + '-other', 'thread_id': thread, 'text': 'different namespace', 'embedding': [1., 0., 0.]},
            {'namespace': saver.config.namespace, 'thread_id': thread + '-other', 'text': 'different thread', 'embedding': [1., 0., 0.]},
        ])
        stage = vector_search_stage(vector=[1., 0., 0.], k=3, namespace=saver.config.namespace, thread_id=thread, dimensions=3)
        found = list(saver.db.summaries.aggregate([stage]))
        assert found and all(x['text'] == 'keep' for x in found)
    finally:
        reopened.close()
