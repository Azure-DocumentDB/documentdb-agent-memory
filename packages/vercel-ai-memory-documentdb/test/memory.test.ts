import assert from "node:assert/strict";
import { test } from "node:test";
import type { ModelMessage } from "ai";
import { AppendMessagesError, COLLECTIONS, DocumentDBMemory, SHARED_INDEXES, cosmosSearchOptions, ensureSchema, normalizeMessage, resolveConfig } from "../src/index.js";
import { FakeStore } from "./fake.js";
import { sdkFlow } from "./sdk-flow.js";
const config = { database: "offline", namespace: "alpha", embeddingDimensions: 3 };
async function fixture(setup = true) { const store = new FakeStore(); const a=new DocumentDBMemory(store.client(),config); const b=new DocumentDBMemory(store.client(),{...config,namespace:"beta"}); if(setup) {await a.setup();await b.setup();store.calls=[];} return {store,a,b}; }
test("current SDK real generateText tool-step response append and new-client resume (offline DB fixture)", async () => { const {store,a}=await fixture(); await sdkFlow(a,async()=>{const m=new DocumentDBMemory(store.client(),config);await m.setup();return m;},"sdk"); });
test("two namespaces with identical thread IDs retain independent ranges/history/summaries", async()=>{
  const {a,b}=await fixture(); for(const m of [a,b]) {assert.equal(await m.appendMessages("same",[{role:"user",content:m.config.namespace}]),1); await m.addSummary("same",m.config.namespace,[1,0,0]);}
  assert.deepEqual((await a.getMessages("same")).map(m=>m.seq),[0]); assert.equal((await b.getMessages("same"))[0]!.content,"beta");
  assert.equal((await a.searchSummaries("same",[1,0,0]))[0]!.text,"alpha"); assert.equal((await b.searchSummaries("same",[1,0,0]))[0]!.text,"beta");
});
test("100 cold concurrent calls shape contract (fixture, not atomicity proof)",async()=>{
  const {a,store}=await fixture(); store.duplicateAllocations=2;
  const counts=await Promise.all(Array.from({length:100},(_,i)=>a.appendMessages("cold",[{role:"user",content:String(i)}])));
  assert.equal(counts.reduce((a,b)=>a+b,0),100); const rows=await a.getMessages("cold"); assert.deepEqual(rows.map(r=>r.seq),Array.from({length:100},(_,i)=>i)); assert.equal(new Set(rows.map(r=>r.content)).size,100);
  assert.equal(store.calls.some(c=>c.op==="find" && c.name==="messages" && c.query?.seq),false);
});
test("missing Python-created next_seq begins at zero; batched ranges contiguous",async()=>{
  const {a,store}=await fixture(); store.rows("sessions").push({namespace:"alpha",thread_id:"python",created_at:new Date()});
  await a.appendMessages("python",[{role:"user",content:"a"},{role:"assistant",content:"b"}]); await a.appendMessages("python",[{role:"user",content:"c"}]);
  assert.deepEqual((await a.getMessages("python")).map(r=>r.seq),[0,1,2]);
});
test("partial/failed inserts never return a false success count and reserve documented gaps",async()=>{
  const {a,store}=await fixture();store.failInsert=true;
  await assert.rejects(a.appendMessages("fail",[{role:"user",content:"a"}]),e=>e instanceof AppendMessagesError && e.reservedStart===0 && e.reservedCount===1);
  store.failInsert=false;await a.appendMessages("fail",[{role:"user",content:"b"}]);assert.deepEqual((await a.getMessages("fail")).map(r=>r.seq),[1]);
  store.partialInsert=true;await assert.rejects(a.appendMessages("fail",[{role:"user",content:"c"},{role:"user",content:"d"}]),AppendMessagesError);
});
test("recent-limit returns chronological order and rejects zero/negative/fractional",async()=>{
  const {a}=await fixture();await a.appendMessages("t",[0,1,2].map(i=>({role:"user",content:String(i)})));assert.deepEqual((await a.getMessages("t",2)).map(r=>r.seq),[1,2]);
  for(const n of [0,-1,1.5,10001,NaN]) await assert.rejects(a.getMessages("t",n));
});
test("quiescent delete scopes all five collections and leaves other namespaces/threads",async()=>{
  const {a,store}=await fixture();for(const name of COLLECTIONS) store.rows(name).push({namespace:"alpha",thread_id:"same"},{namespace:"beta",thread_id:"same"},{namespace:"alpha",thread_id:"other"});
  assert.deepEqual(await a.deleteThread("same"),Object.fromEntries(COLLECTIONS.map(n=>[n,1]))); for(const name of COLLECTIONS) assert.equal(store.rows(name).length,2);
  assert.ok(store.calls.filter(c=>c.op==="deleteMany").every(c=>c.query?.namespace==="alpha" && c.query.thread_id==="same"));
});
test("all scope methods validate namespace/thread including empty append",async()=>{
  for(const namespace of [""," ","x\0y","x".repeat(513)]) assert.throws(()=>resolveConfig({namespace}));
  const {a}=await fixture();for(const id of [""," ","x\ny","x".repeat(513)]) {
    await assert.rejects(a.appendMessages(id,[]));await assert.rejects(a.getMessages(id));await assert.rejects(a.addSummary(id,"ok",[1,0,0]));await assert.rejects(a.searchSummaries(id,[1,0,0]));await assert.rejects(a.deleteThread(id));
  }
});
test("message reserved metadata and unsupported tool-string/media values fail before allocation",async()=>{
  const {a,store}=await fixture();for(const key of ["_id","namespace","thread_id","seq","created_at","next_seq"]) await assert.rejects(a.appendMessages("t",[{role:"user",content:"x",[key]:"bad"} as ModelMessage]));
  for(const message of [{role:"tool",content:"not a tool result"},{role:"user",content:[{type:"image",image:"base64"}]},{role:"user",content:[{type:"text",text:3}]}]) assert.throws(()=>normalizeMessage(message as ModelMessage));
  assert.equal(store.calls.length,0);
});
test("vector/query/text/metadata bounds are enforced before writes",async()=>{
  const {a,store}=await fixture();for(const vector of [[1,2],[NaN,0,1],[Infinity,0,1]]) {await assert.rejects(a.addSummary("t","s",vector));await assert.rejects(a.searchSummaries("t",vector));}
  for(const k of [0,-1,1.5,101,NaN]) await assert.rejects(a.searchSummaries("t",[1,0,0],k));
  for(const text of [""," ","x".repeat(1_000_001)]) await assert.rejects(a.addSummary("t",text,[1,0,0]));
  for(const key of ["_id","namespace","thread_id","text","embedding","created_at","updated_at","seq","next_seq","role","checkpoint_id"]) await assert.rejects(a.addSummary("t","s",[1,0,0],{[key]:"bad"}));
  await assert.rejects(a.addSummary("t","s",[1,0,0],{"$set":{}})); await assert.rejects(a.addSummary("t","s",[1,0,0],{bad:NaN}));assert.equal(store.calls.length,0);
});
test("config rejects mismatched dimensions/path/invalid database and defaults to shared IVF numLists=1",()=>{
  assert.deepEqual(cosmosSearchOptions(resolveConfig().vectorIndex),{kind:"vector-ivf",similarity:"COS",dimensions:1536,numLists:1});
  for(const cfg of [{embeddingDimensions:0},{embeddingDimensions:3,vectorIndex:{path:"embedding",dimensions:4}},{vectorIndex:{path:"other",dimensions:3}},{database:"a/b"}])assert.throws(()=>resolveConfig(cfg));
});
test("setup creates all shared indexes/filter fields before vector; repeat compatible",async()=>{
  const {a,store}=await fixture(false);assert.equal((await a.setup()).schemaVersion,2);await a.setup();
  for(const idx of SHARED_INDEXES) assert.ok(store.indexes.get(idx.collection)?.some(i=>i.name===idx.name));
  const commands=store.calls.filter(c=>c.op==="command"); assert.equal(commands.length,3);
  assert.deepEqual(store.indexes.get("summaries")?.filter(i=>i.name!==SHARED_INDEXES[2].name).map(i=>i.name),["namespace_filter","thread_id_filter","embedding_vector_ivf"]);
});
test("legacy thread-only index fails nondestructively, closes client",async()=>{
  const {a,store}=await fixture(false);store.rows("sessions");store.indexes.set("sessions",[{name:"thread_id_unique",key:{thread_id:1},unique:true}]);
  await assert.rejects(a.setup(),/new database.*migrate offline/);assert.equal(store.closes,1);assert.equal(store.calls.some(c=>["createIndex","createCollection","command","deleteMany"].includes(c.op)),false);
});
test("unscoped legacy record fails before schema writes",async()=>{
  const {a,store}=await fixture(false);store.rows("messages").push({thread_id:"t",seq:0});await assert.rejects(a.setup(),/unscoped/);assert.equal(store.calls.some(c=>c.op==="createCollection"),false);
});
test("index incompatibility includes uniqueness, sparse, TTL, collation, vector dimensions",async()=>{
  for(const extra of [{unique:false},{sparse:true},{partialFilterExpression:{x:1}},{expireAfterSeconds:1},{collation:{locale:"en"}}]) {
    const {a,store}=await fixture(false);store.rows("sessions");store.indexes.set("sessions",[{name:SHARED_INDEXES[0].name,key:SHARED_INDEXES[0].key,unique:true,...extra}]);await assert.rejects(a.setup(),/incompatible/i);
  }
  const {a,store}=await fixture();await a.setup();const idx=store.indexes.get("summaries")!.find(i=>i.name==="embedding_vector_ivf")!;idx.cosmosSearchOptions.dimensions=8;await assert.rejects(ensureSchema(a.db,config),/incompatible/i);
});
test("setup closes client on connection failure",async()=>{const {a,store}=await fixture();store.failConnect=true;await assert.rejects(a.setup(),/connect failure/);assert.equal(store.closes,1);});
test("operations require setup, including forgotten unique-index initialization",async()=>{
  const {a,store}=await fixture(false);
  await assert.rejects(a.appendMessages("t",[{role:"user",content:"x"}]),/setup/);await assert.rejects(a.getMessages("t"),/setup/);await assert.rejects(a.addSummary("t","s",[1,0,0]),/setup/);await assert.rejects(a.searchSummaries("t",[1,0,0]),/setup/);await assert.rejects(a.deleteThread("t"),/setup/);assert.equal(store.calls.length,0);
});
test("unsafe sequence overflow is rejected without message insertion",async()=>{
  const {a,store}=await fixture();store.rows("sessions").push({namespace:"alpha",thread_id:"overflow",next_seq:Number.MAX_SAFE_INTEGER});
  await assert.rejects(a.appendMessages("overflow",[{role:"user",content:"x"}]));assert.equal(store.rows("messages").length,0);assert.equal(store.rows("sessions")[0]!.next_seq,Number.MAX_SAFE_INTEGER);
});
