"""Real StateGraph contract tests, identical on mongomock and the explicit live gate."""
from datetime import datetime, timezone
from typing import Annotated, TypedDict
import asyncio
import time
import uuid
import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver, empty_checkpoint
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.types import Command, interrupt, Send
from langgraph_checkpoint_documentdb import DocumentDBSaver, MemoryConfig, COLLECTIONS


class State(TypedDict):
    count: int
    messages: Annotated[list, add_messages]


def graph(saver, *, paused=False):
    builder = StateGraph(State)
    def first(state):
        return {'count': state['count'] + 1, 'messages': [AIMessage(content='increment')]}
    def second(state):
        answer = interrupt('continue?') if paused else 2
        return {'count': state['count'] + int(answer), 'messages': [AIMessage(content='finish')]}
    builder.add_node('first', first)
    builder.add_node('second', second)
    builder.add_edge(START, 'first')
    builder.add_edge('first', 'second')
    builder.add_edge('second', END)
    return builder.compile(checkpointer=saver)


def config(thread='same', ns='', cid=None):
    c = {'configurable': {'thread_id': thread, 'checkpoint_ns': ns}, 'metadata': {'owner': 'test'}}
    if cid:
        c['configurable']['checkpoint_id'] = cid
    return c


def checkpoint(cid=None, **values):
    result = empty_checkpoint()
    if cid:
        result['id'] = cid
    result['channel_values'] = values
    return result


def test_actual_graph_sync_resume_replay(saver):
    assert isinstance(saver, BaseCheckpointSaver)
    app = graph(saver, paused=True)
    c = config()
    result = app.invoke({'count': 1, 'messages': [HumanMessage(content='hello')]}, c)
    assert result['count'] == 2 and result['__interrupt__']
    paused = app.get_state(c)
    assert paused.next == ('second',)
    assert saver.get_tuple(c).pending_writes
    # New saver/compiled graph retrieves persisted state, not process-local memory.
    reopened = DocumentDBSaver(saver.client, saver.config)
    reopened.setup()
    resumed = graph(reopened, paused=True).invoke(Command(resume=5), c)
    assert resumed['count'] == 7
    assert isinstance(resumed['messages'][0], HumanMessage)
    history = list(app.get_state_history(c))
    assert len(history) >= 4
    assert saver.get_tuple(c).parent_config
    replay_app = graph(reopened, paused=True)
    replay = replay_app.invoke(None, paused.config)
    assert replay['count'] == 2 and replay['__interrupt__']
    assert replay_app.invoke(Command(resume=3), c)['count'] == 5
    assert saver.get_next_version(None, None) == 1
    assert saver.get_next_version(3, None) == 4


async def test_actual_graph_async_resume_replay(saver):
    app = graph(saver, paused=True)
    c = config('async')
    result = await app.ainvoke({'count': 10, 'messages': []}, c)
    assert result['__interrupt__'] and result['count'] == 11
    paused = await app.aget_state(c)
    assert (await app.ainvoke(Command(resume=4), c))['count'] == 15
    assert (await app.ainvoke(None, paused.config))['__interrupt__']
    assert (await app.ainvoke(Command(resume=2), c))['count'] == 13
    assert len([v async for v in saver.alist(c)]) >= 4
    assert await saver.aget(c)
    await saver.adelete_thread('async')
    assert await saver.aget_tuple(c) is None


def test_typed_serialization_metadata_parent_and_filters(saver):
    c = config('serialization')
    value = {'bytes': b'\x00\xff', 'date': datetime.now(timezone.utc), 'uuid': uuid.uuid4(), 'message': HumanMessage(content='hi'), 'send': Send('node', {'x': 1}), 'set': {1, 2}}
    first = saver.put(c, checkpoint('001', **value), {'step': 1, 'source': 'input'}, {})
    second = saver.put(first, checkpoint('002', **value), {'step': 2, 'source': 'loop'}, {})
    assert saver.get_tuple(second).checkpoint['channel_values'] == value
    assert saver.get_tuple(second).parent_config == first
    assert saver.get_tuple(first).metadata['owner'] == 'test'
    assert [v.checkpoint['id'] for v in saver.list(c)] == ['002', '001']
    assert [v.checkpoint['id'] for v in saver.list(c, before=second)] == ['001']
    assert len(list(saver.list(c, filter={'step': 2}, limit=1))) == 1
    assert len(list(saver.list(c, filter={'step': {'$gt': 0}}))) == 0
    assert list(saver.list(c, limit=0)) == []


def test_pending_write_first_wins_special_channels_and_task_path_order(saver):
    c = saver.put(config(), checkpoint('writes'), {'step': 0}, {})
    saver.put_writes(c, [('x', 1), ('x', 2)], 'task-a', 'z')
    saver.put_writes(c, [('x', 999), ('x', 999)], 'task-a', 'z')
    saver.put_writes(c, [('__error__', 'old')], 'task-a', 'z')
    saver.put_writes(c, [('__error__', 'new')], 'task-a', 'z')
    saver.put_writes(c, [('x', 3)], 'task-z', 'a')
    assert saver.get_tuple(c).pending_writes == [('task-z', 'x', 3), ('task-a', '__error__', 'new'), ('task-a', 'x', 1), ('task-a', 'x', 2)]
    for channel in ('__scheduled__', '__interrupt__', '__resume__'):
        saver.put_writes(c, [(channel, 1)], 'special', 'b')
        saver.put_writes(c, [(channel, 2)], 'special', 'b')
    assert len([w for w in saver.get_tuple(c).pending_writes if w[0] == 'special']) == 3
    assert all(w[2] == 2 for w in saver.get_tuple(c).pending_writes if w[0] == 'special')


def test_storage_and_checkpoint_namespaces_and_delete_all_five(saver):
    other = DocumentDBSaver(saver.client, MemoryConfig(database=saver.config.database, namespace=saver.config.namespace + '-other', embedding_dimensions=3))
    other.setup()
    for adapter, label in [(saver, 'A'), (other, 'B')]:
        for ns in ('', 'subgraph'):
            c = adapter.put(config(ns=ns), checkpoint('same-id', label=label, ns=ns), {}, {})
            adapter.put_writes(c, [('x', label)], 'task')
        for collection in ('messages', 'summaries'):
            adapter.db[collection].insert_one({'namespace': adapter.config.namespace, 'thread_id': 'same', 'seq': 1, 'text': label})
    assert saver.get(config())['channel_values']['label'] == 'A'
    assert other.get(config())['channel_values']['label'] == 'B'
    assert saver.get(config(ns='subgraph'))['channel_values']['ns'] == 'subgraph'
    assert len(list(saver.list(None))) == 2
    saver.db.sessions.update_one({'namespace': saver.config.namespace, 'thread_id': 'same'}, {'$set': {'next_seq': 91}})
    saver.put(config(), checkpoint('same-id', label='A'), {}, {})
    assert saver.db.sessions.find_one({'namespace': saver.config.namespace, 'thread_id': 'same'})['next_seq'] == 91
    counts = saver.delete_thread_counts('same')
    assert set(counts) == set(COLLECTIONS) and all(v > 0 for v in counts.values())
    assert all(saver.db[name].count_documents({'namespace': saver.config.namespace, 'thread_id': 'same'}) == 0 for name in COLLECTIONS)
    assert all(saver.db[name].count_documents({'namespace': other.config.namespace, 'thread_id': 'same'}) > 0 for name in COLLECTIONS)


@pytest.mark.parametrize('field', ['thread_id', 'checkpoint_ns', 'checkpoint_id'])
def test_operator_input_rejected(saver, field):
    c = config(cid='id')
    c['configurable'][field] = {'$ne': None}
    with pytest.raises(ValueError):
        saver.get_tuple(c)
    with pytest.raises(ValueError):
        saver.put(c, checkpoint(), {}, {})
    with pytest.raises(ValueError):
        saver.delete_thread({'$ne': None})


def test_close_and_secure_serializer(saver):
    with pytest.raises(NotImplementedError):
        saver.serde.loads_typed(('pickle', b'unsafe'))
    assert saver.serde._allowed_msgpack_modules is None
    saver.close()
    saver.close()
    with pytest.raises(RuntimeError, match='closed'):
        saver.get_tuple(config())


async def test_async_does_not_block_event_loop(saver, monkeypatch):
    original = saver.get_tuple
    def slow(c):
        time.sleep(0.08)
        return original(c)
    monkeypatch.setattr(saver, 'get_tuple', slow)
    task = asyncio.create_task(saver.aget_tuple(config()))
    await asyncio.sleep(0.01)
    assert not task.done()
    await task


def test_uninitialized_saver_fails_closed(saver):
    unready = DocumentDBSaver(saver.client, saver.config)
    with pytest.raises(RuntimeError, match='setup'):
        unready.put(config(), checkpoint(), {}, {})
    with pytest.raises(RuntimeError, match='setup'):
        list(unready.list(None))


def test_injected_client_ownership(saver):
    from unittest.mock import MagicMock
    client = MagicMock()
    caller_owned = DocumentDBSaver(client)
    caller_owned.close()
    client.close.assert_not_called()
    owned = DocumentDBSaver(client, owns_client=True)
    owned.close()
    owned.close()
    client.close.assert_called_once()


def test_real_nested_graph_checkpoint_namespace(saver):
    child = graph(saver, paused=True)
    builder = StateGraph(State)
    builder.add_node('child', child)
    builder.add_edge(START, 'child')
    builder.add_edge('child', END)
    app = builder.compile(checkpointer=saver)
    c = config('nested')
    assert app.invoke({'count': 1, 'messages': []}, c)['__interrupt__']
    assert app.invoke(Command(resume=4), c)['count'] == 6
    rows = list(saver.list({'configurable': {'thread_id': 'nested'}}))
    assert any(v.config['configurable']['checkpoint_ns'].startswith('child:') for v in rows)
    assert any(v.config['configurable']['checkpoint_ns'] == '' for v in rows)


def test_actual_graph_failure_resumes_without_rerunning_successful_sibling(saver):
    import operator
    class ParallelState(TypedDict):
        values: Annotated[list, operator.add]
    calls = {'good': 0, 'flaky': 0}
    def good(state):
        calls['good'] += 1
        return {'values': ['good']}
    def flaky(state):
        calls['flaky'] += 1
        if calls['flaky'] == 1:
            raise RuntimeError('synthetic retryable failure')
        return {'values': ['flaky']}
    builder = StateGraph(ParallelState)
    builder.add_node('good', good)
    builder.add_node('flaky', flaky)
    builder.add_edge(START, 'good')
    builder.add_edge(START, 'flaky')
    builder.add_edge('good', END)
    builder.add_edge('flaky', END)
    app = builder.compile(checkpointer=saver)
    c = config('failure')
    with pytest.raises(RuntimeError, match='synthetic'):
        app.invoke({'values': []}, c)
    assert any(w[1] == '__error__' for w in saver.get_tuple(c).pending_writes)
    assert sorted(app.invoke(None, c)['values']) == ['flaky', 'good']
    assert calls == {'good': 1, 'flaky': 2}


def test_concurrent_pending_writes_are_idempotent(saver):
    from concurrent.futures import ThreadPoolExecutor
    c = saver.put(config('concurrent'), checkpoint('concurrent-id'), {}, {})
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: saver.put_writes(c, [('value', 'stable')], 'same-task'), range(24)))
    assert saver.get_tuple(c).pending_writes == [('same-task', 'value', 'stable')]


def test_write_input_validation_is_before_mutation(saver):
    c = saver.put(config('validated'), checkpoint('validated-id'), {}, {})
    with pytest.raises(ValueError):
        saver.put_writes(c, [('safe', 1), ({'$ne': None}, 2)], 'task')
    assert saver.get_tuple(c).pending_writes == []
    bad = checkpoint()
    bad['id'] = {'$ne': None}
    with pytest.raises(ValueError):
        saver.put(config('validated'), bad, {}, {})
    assert len(list(saver.list(config('validated')))) == 1


from dataclasses import dataclass

@dataclass
class CustomState:
    x: int


def test_nonallowlisted_custom_state_fails_before_write(saver):
    from dataclasses import make_dataclass
    Custom = make_dataclass('UnsafeState', [('x', int)])
    with pytest.raises(ValueError, match='strict serializer'):
        saver.put(config('custom'), checkpoint('custom-id', custom=Custom(1)), {}, {})
    assert saver.get_tuple(config('custom')) is None


def test_explicit_trusted_custom_type_allowlist(saver):
    trusted = saver.with_allowlist({(__name__, 'CustomState')})
    c = trusted.put(config('trusted'), checkpoint('trusted-id', custom=CustomState(3)), {}, {})
    assert trusted.get(c)['channel_values']['custom'] == CustomState(3)


async def test_async_close(saver):
    await saver.aclose()
    await saver.aclose()
    with pytest.raises(RuntimeError, match='closed'):
        await saver.aget_tuple(config())
