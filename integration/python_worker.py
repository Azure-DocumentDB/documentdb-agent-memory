#!/usr/bin/env python3
"""One OS-process stage. Driver errors are intentionally never printed."""
from __future__ import annotations
import json
import os
import sys
from coordinator import COLLECTIONS, validate_request


def execute(req):
    validate_request(req, os.environ)  # before third-party imports/client construction
    from typing import TypedDict
    from langgraph.graph import StateGraph, START, END
    from langgraph.types import Command, interrupt
    from langgraph_checkpoint_documentdb import DocumentDBSaver, MemoryConfig, SchemaMismatchError, vector_search_stage
    from langgraph_checkpoint_documentdb.connection import client_from_env

    action, namespace, thread = req['action'], req['namespace'], req['thread']
    config = MemoryConfig(database=req['database'], namespace=namespace, embedding_dimensions=3)
    client = client_from_env()
    saver = DocumentDBSaver(client, config)
    db = saver.db
    scope = {'namespace': namespace, 'thread_id': thread}
    cfg = {'configurable': {'thread_id': thread, 'checkpoint_ns': ''}}

    class State(TypedDict):
        count: int
        label: str
        prepared: bool

    def graph():
        builder = StateGraph(State)
        def prepare(state):
            return {'count': state['count'] + 1, 'prepared': True}
        def approve(state):
            answer = interrupt('approve deterministic increment')
            return {'count': state['count'] + int(answer)}
        builder.add_node('prepare', prepare)
        builder.add_node('approve', approve)
        builder.add_edge(START, 'prepare')
        builder.add_edge('prepare', 'approve')
        builder.add_edge('approve', END)
        return builder.compile(checkpointer=saver)

    def counts():
        return {c: db[c].count_documents(scope) for c in COLLECTIONS}

    def catalog():
        out = {}
        existing = set(db.list_collection_names())
        for name in COLLECTIONS:
            out[name] = [] if name not in existing else sorted([
                {'name': i['name'], 'key': [list(p) for p in i['key'].items()], 'unique': bool(i.get('unique')), 'vector': i.get('cosmosSearchOptions')}
                for i in db[name].list_indexes() if i['name'] != '_id_'
            ], key=lambda i: i['name'])
        return out

    def inspect():
        cp = saver.get_tuple(cfg)
        assert cp and cp.checkpoint['channel_values']['label'] == namespace and cp.pending_writes
        assert graph().get_state(cfg).next == ('approve',)
        messages = list(db.messages.find(scope).sort('seq', 1))
        assert [(m['seq'], m['role'], m['content']) for m in messages] == [(0, 'user', f'{namespace}:{thread}:persisted'), (1, 'assistant', 'deterministic reply')]
        summaries = list(db.summaries.find(scope))
        assert len(summaries) == 1 and summaries[0]['text'] == f'{namespace}:{thread}:summary' and summaries[0]['embedding'] == [1, 0, 0]
        stage = vector_search_stage(vector=[1., 0., 0.], dimensions=3, k=10, namespace=namespace, thread_id=thread)
        hits = list(db.summaries.aggregate([stage]))
        assert len(hits) == 1 and hits[0]['text'] == f'{namespace}:{thread}:summary'
        assert hits[0]['namespace'] == namespace and hits[0]['thread_id'] == thread
        result = counts()
        assert all(v > 0 for v in result.values())
        return {'counts': result, 'checkpoint_id': cp.checkpoint['id'], 'pending_writes': len(cp.pending_writes)}

    try:
        if action == 'cleanup':
            # Works even when schema preflight is blocked. No DB/collection/index drops.
            namespaces = [f'cross_{req["token"]}_{n}' for n in ('a', 'b', 'legacy')]
            removed = {}
            for name in COLLECTIONS:
                removed[name] = 0
                for ns in namespaces:
                    exact = {'namespace': ns}
                    removed[name] += db[name].delete_many(exact).deleted_count
                    assert db[name].count_documents(exact) == 0
            return {'removed': removed, 'remaining_generated_records': 0}
        if action == 'legacy-insert':
            saver.setup()
            # Namespace/thread remain valid for exact cleanup; malformed checkpoint dimension
            # represents a scoped pre-v2 payload and must block both adapters administratively.
            db.checkpoints.insert_one({**scope, 'checkpoint_ns': '', 'checkpoint_id': 7, 'fixture': 'scoped-legacy'})
            return {'scoped_legacy_fixture': True}
        if action == 'legacy-reject':
            before = catalog()
            try:
                saver.setup()
            except SchemaMismatchError:
                pass
            else:
                raise AssertionError('legacy setup unexpectedly succeeded')
            assert before == catalog()
            assert db.checkpoints.count_documents({**scope, 'checkpoint_id': 7}) == 1
            return {'legacy_rejected': True, 'catalog_unchanged': True}
        if action == 'setup-fresh':
            assert not db.list_collection_names(), 'generated database must be fresh'
        saver.setup()
        if action in ('setup', 'setup-fresh'):
            return {'catalog': catalog()}
        if action in ('seed', 'graph-save'):
            assert counts() == {c: 0 for c in COLLECTIONS}
            app = graph()
            result = app.invoke({'count': 10, 'label': namespace, 'prepared': False}, cfg)
            assert result['count'] == 11 and result['__interrupt__']
            paused = app.get_state(cfg)
            cp = saver.get_tuple(cfg)
            assert paused.next == ('approve',) and cp and cp.pending_writes
            assert cp.checkpoint['id'] == paused.config['configurable']['checkpoint_id']
            session = db.sessions.find_one(scope)
            assert session and 'next_seq' not in session
            return {'checkpoint_id': cp.checkpoint['id'], 'pending_writes': len(cp.pending_writes), 'count': 11, 'python_session_without_next_seq': True}
        if action == 'graph-resume':
            app = graph()
            paused = app.get_state(cfg)
            assert paused.config['configurable']['checkpoint_id'] == req['checkpoint_id']
            assert paused.values == {'count': 11, 'label': namespace, 'prepared': True}
            assert paused.next == ('approve',)
            cp = saver.get_tuple(cfg)
            assert cp and cp.pending_writes and cp.checkpoint['id'] == req['checkpoint_id']
            result = app.invoke(Command(resume=5), cfg)
            assert result['count'] == 16 and not result.get('__interrupt__')
            assert app.get_state(cfg).next == ()
            assert any(s.config['configurable']['checkpoint_id'] == req['checkpoint_id'] for s in app.get_state_history(cfg))
            return {'restored_checkpoint_id': req['checkpoint_id'], 'resumed_count': 16, 'prepared_node_not_rerun': True}
        if action == 'inspect':
            return inspect()
        if action == 'delete':
            before = inspect()['counts']
            deleted = saver.delete_thread_counts(thread)
            assert deleted == before and counts() == {c: 0 for c in COLLECTIONS}
            return {'counts': deleted}
        if action == 'absent':
            assert counts() == {c: 0 for c in COLLECTIONS}
            assert saver.get_tuple(cfg) is None
            return {'counts': counts()}
        raise AssertionError('unknown action')
    finally:
        client.close()


def main():
    req = json.load(sys.stdin)
    result = execute(req)
    print(json.dumps({'ok': True, 'pid': os.getpid(), 'action': req['action'], **result}))

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'ok': False, 'errorType': type(exc).__name__}))
        sys.exit(1)
