#!/usr/bin/env node
// Actual built TypeScript adapter, no API/database mocks or LLM calls.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

function guard(req) {
  assert.equal(process.env.MEMORY_CROSS_LIVE, '1', 'explicit live opt-in required');
  assert.match(req.token, /^[a-f0-9]{24}$/);
  assert.ok(['pyts', 'tspy'].map(s => `readiness_memory_cross_${req.token}_${s}`).includes(req.database));
  assert.ok(['a', 'b', 'legacy'].map(s => `cross_${req.token}_${s}`).includes(req.namespace));
  assert.ok(['collision', 'resume', 'neighbor'].includes(req.thread));
}
async function execute(req) {
  guard(req); // Before importing adapter, constructing clients or resolving cloud credentials.
  const { DocumentDBMemory, COLLECTIONS } = await import('../packages/vercel-ai-memory-documentdb/dist/index.js');
  const { IncompatibleSchemaError } = await import('../packages/vercel-ai-memory-documentdb/dist/schema.js');
  const m = await DocumentDBMemory.fromEnvironment(process.env, {
    database: req.database, namespace: req.namespace, embeddingDimensions: 3,
    vectorIndex: { path: 'embedding', dimensions: 3, kind: 'vector-ivf', similarity: 'COS', numLists: 1 },
  });
  const scope = { namespace: req.namespace, thread_id: req.thread };
  async function catalog() {
    const existing = new Set((await m.db.listCollections({}, { nameOnly: true }).toArray()).map(c => c.name));
    const out = {};
    for (const c of COLLECTIONS) {
      out[c] = !existing.has(c) ? [] : (await m.db.collection(c).listIndexes().toArray())
        .filter(i => i.name !== '_id_')
        .map(i => ({ name: i.name, key: Object.entries(i.key), unique: Boolean(i.unique), vector: i.cosmosSearchOptions ?? null }))
        .sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
    }
    return out;
  }
  async function counts() {
    const out = {};
    for (const c of COLLECTIONS) out[c] = await m.db.collection(c).countDocuments(scope);
    return out;
  }
  async function read() {
    const messages = await m.getMessages(req.thread);
    assert.deepEqual(messages.map(({ seq, role, content }) => ({ seq, role, content })), [
      { seq: 0, role: 'user', content: `${req.namespace}:${req.thread}:persisted` },
      { seq: 1, role: 'assistant', content: 'deterministic reply' },
    ]);
    const hits = await m.searchSummaries(req.thread, [1, 0, 0], 10);
    assert.equal(hits.length, 1);
    assert.equal(hits[0].text, `${req.namespace}:${req.thread}:summary`);
    assert.equal(hits[0].namespace, req.namespace);
    assert.equal(hits[0].thread_id, req.thread);
    const out = await counts();
    assert.ok(Object.values(out).every(v => v > 0));
    return { counts: out, sequences: messages.map(x => x.seq), vector_hits: 1 };
  }
  try {
    if (req.action === 'legacy-reject') {
      await m.client.connect();
      const before = await catalog();
      await assert.rejects(m.setup(), IncompatibleSchemaError);
      // setup failure closes TS client by contract; reconnect only to verify no mutation.
      await m.client.connect();
      assert.deepEqual(await catalog(), before);
      assert.equal(await m.db.collection('checkpoints').countDocuments({ ...scope, checkpoint_id: 7 }), 1);
      return { legacy_rejected: true, catalog_unchanged: true };
    }
    if (req.action === 'setup-fresh') {
      await m.client.connect();
      assert.equal((await m.db.listCollections({}, { nameOnly: true }).toArray()).length, 0);
    }
    await m.setup();
    if (['setup', 'setup-fresh'].includes(req.action)) return { catalog: await catalog() };
    if (req.action === 'append') {
      const session = await m.db.collection('sessions').findOne(scope);
      assert.ok(session && !Object.hasOwn(session, 'next_seq'));
      assert.equal(await m.appendMessages(req.thread, [
        { role: 'user', content: `${req.namespace}:${req.thread}:persisted` },
        { role: 'assistant', content: 'deterministic reply' },
      ]), 2);
      await m.addSummary(req.thread, `${req.namespace}:${req.thread}:summary`, [1, 0, 0]);
      return { ...await read(), allocator_initialized_from_python: true };
    }
    if (req.action === 'read') return await read();
    if (req.action === 'delete') {
      const before = (await read()).counts;
      const deleted = await m.deleteThread(req.thread);
      assert.deepEqual(deleted, before);
      assert.ok(Object.values(await counts()).every(v => v === 0));
      return { counts: deleted };
    }
    throw new Error('unknown action');
  } finally {
    await m.close();
  }
}
try {
  const req = JSON.parse(readFileSync(0, 'utf8'));
  const result = await execute(req);
  console.log(JSON.stringify({ ok: true, pid: process.pid, action: req.action, ...result }));
} catch (error) {
  // Do not log driver messages, cause chains, URIs, tokens or stack traces.
  const name = error?.constructor?.name;
  console.log(JSON.stringify({ ok: false, errorType: /^[A-Za-z][A-Za-z0-9_]{0,79}$/.test(name) ? name : 'Error' }));
  process.exitCode = 1;
}
