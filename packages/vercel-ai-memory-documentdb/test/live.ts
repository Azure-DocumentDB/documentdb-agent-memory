import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { COLLECTIONS, DocumentDBMemory } from "../src/index.js";
import { sdkFlow } from "./sdk-flow.js";
// Explicitly requested live suite NEVER skips. No automatic DB drops.
function liveConfig() {
  const database=process.env.MEMORY_TEST_DATABASE ?? "";
  if(process.env.MEMORY_LIVE_ALLOW_WRITES !== "true" || !/^agentmemory_disposable_[a-z0-9_]+$/.test(database) || database.length>63) throw new Error("Live gate requires MEMORY_LIVE_ALLOW_WRITES=true and safe MEMORY_TEST_DATABASE=agentmemory_disposable_<unique_suffix>");
  return {database,embeddingDimensions:3};
}
test("LIVE actual SDK tool step/resume, 100 cold concurrent appends, namespace-vector isolation and five-collection lifecycle",{timeout:180000},async()=>{
  const config=liveConfig(); const token=randomUUID(); const ns=`ts-live-${token}`;
  const make=async(namespace=ns)=>{const m=await DocumentDBMemory.fromEnvironment(process.env,{...config,namespace});await m.setup();return m;};
  const a=await make(); let b:DocumentDBMemory|undefined;
  const threads=["cold","scope","python-first","sdk"].map(t=>`${token}-${t}`);const [cold,scope,python,sdk]=threads as [string,string,string,string];
  try {
    b=await make(`${ns}-other`);
    await a.setup();await b.setup();
    const counts=await Promise.all(Array.from({length:100},(_,i)=>a.appendMessages(cold,[{role:"user",content:`message-${i}`}])));
    assert.equal(counts.reduce((x,y)=>x+y,0),100);
    const messages=await a.getMessages(cold);assert.equal(messages.length,100);assert.deepEqual(messages.map(m=>m.seq),Array.from({length:100},(_,i)=>i));assert.equal(new Set(messages.map(m=>m.content)).size,100);
    await a.db.collection("sessions").insertOne({namespace:ns,thread_id:python,created_at:new Date()});
    await a.appendMessages(python,[{role:"user",content:"Python session before allocator"}]);assert.equal((await a.getMessages(python))[0]!.seq,0);
    for(const m of [a,b]) {
      await m.appendMessages(scope,[{role:"user",content:m.config.namespace}]);
      await m.addSummary(scope,m.config.namespace,[1,0,0]);
      await m.db.collection("checkpoints").insertOne({namespace:m.config.namespace,thread_id:scope,checkpoint_ns:"",checkpoint_id:"cp-1",checkpoint:Buffer.from("opaque fixture")});
      await m.db.collection("checkpoint_writes").insertOne({namespace:m.config.namespace,thread_id:scope,checkpoint_ns:"",checkpoint_id:"cp-1",task_id:"task",idx:0,channel:"fixture",value:Buffer.from("opaque fixture")});
    }
    assert.equal((await a.getMessages(scope))[0]!.content,ns);assert.equal((await b.getMessages(scope))[0]!.content,`${ns}-other`);
    for(const m of [a,b]) {const hits=await m.searchSummaries(scope,[1,0,0],10);assert.equal(hits.length,1);assert.equal(hits[0]!.text,m.config.namespace);assert.equal(hits[0]!.namespace,m.config.namespace);assert.equal(hits[0]!.thread_id,scope);assert.equal(typeof hits[0]!.score,"number");}
    assert.deepEqual(await a.deleteThread(scope),{sessions:1,messages:1,summaries:1,checkpoints:1,checkpoint_writes:1});
    for(const c of COLLECTIONS) {assert.equal(await a.db.collection(c).countDocuments({namespace:ns,thread_id:scope}),0);assert.equal(await a.db.collection(c).countDocuments({namespace:`${ns}-other`,thread_id:scope}),1);}
    const sdkMemory=await make();await sdkFlow(sdkMemory,make,sdk);
    console.log(JSON.stringify({gate:"typescript-live",database:config.database,sdk:"ai@7.0.138 published MockLanguageModelV4",concurrentCalls:100,acknowledgedMessages:100,namespaces:2,collections:5,dimensions:3,realLLM:false}));
  } finally {
    try {for(const thread of threads) await a.deleteThread(thread);if(b)for(const thread of threads)await b.deleteThread(thread);}
    finally {await a.close();await b?.close();}
  }
});
