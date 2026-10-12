import type { MongoClient, Document } from "mongodb";
// Offline shape fixture only. This is NOT evidence of real database atomicity.
export class FakeStore {
  data = new Map<string, Document[]>(); indexes = new Map<string, Document[]>(); calls: { op: string; name: string; query?: Document }[] = [];
  duplicateAllocations = 0; failInsert = false; partialInsert = false; failConnect = false; closes = 0;
  rows(name: string): Document[] { if (!this.data.has(name)) this.data.set(name, []); return this.data.get(name)!; }
  match(row: Document, query: Document): boolean {
    return Object.entries(query).every(([k,v]) => {
      if (k === "$or") return (v as Document[]).some(q => this.match(row,q));
      if (v && typeof v === "object") {
        if ("$exists" in v) return (row[k] !== undefined) === v.$exists;
        if ("$not" in v) return typeof row[k] !== v.$not.$type;
        if ("$eq" in v) return row[k] === v.$eq;
        if ("$gte" in v) return typeof row[k] === "number" && row[k] >= v.$gte && row[k] <= v.$lte;
      }
      return row[k] === v;
    });
  }
  cursor(rows: Document[]) {
    let values = [...rows];
    const result = { sort: (q: Document) => { const [key,dir] = Object.entries(q)[0]!; values.sort((a,b) => (a[key]-b[key])*Number(dir)); return result; }, limit: (n: number) => { values = values.slice(0,n); return result; }, toArray: async () => structuredClone(values) };
    return result;
  }
  collection(name: string) {
    const record = (op: string, query?: Document) => this.calls.push({op,name,...(query ? {query} : {})});
    return {
      find: (q: Document) => { record("find",q); return this.cursor(this.rows(name).filter(r => this.match(r,q))); },
      findOne: async (q: Document) => { record("findOne",q); return this.rows(name).find(r=>this.match(r,q)) ?? null; },
      listIndexes: () => this.cursor(this.indexes.get(name) ?? [{name:"_id_",key:{_id:1}}]),
      createIndex: async (key: Document, options: Document) => { record("createIndex"); this.indexes.set(name,[...(this.indexes.get(name) ?? []).filter(i=>i.name!==options.name),{key,...options}]); return options.name; },
      findOneAndUpdate: async (q: Document, update: Document) => {
        record("allocate",q);
        if (this.duplicateAllocations-- > 0) throw Object.assign(new Error("duplicate"),{code:11000});
        let row = this.rows(name).find(r=>this.match(r,q));
        if (!row) { if(this.rows(name).some(r=>r.namespace===q.namespace && r.thread_id===q.thread_id)) throw Object.assign(new Error("duplicate"),{code:11000}); row={...update.$setOnInsert}; this.rows(name).push(row!); }
        Object.assign(row!,update.$set); row!.next_seq = (row!.next_seq ?? 0)+update.$inc.next_seq;
        return {...row};
      },
      insertMany: async (docs: Document[]) => { record("insertMany"); if(this.failInsert) throw new Error("unknown insert outcome"); this.rows(name).push(...structuredClone(this.partialInsert ? docs.slice(0,1) : docs)); return {acknowledged:true, insertedCount:this.partialInsert ? 1 : docs.length}; },
      insertOne: async (doc: Document) => { record("insertOne"); this.rows(name).push(structuredClone(doc)); return {acknowledged:true}; },
      deleteMany: async (q: Document) => { record("deleteMany",q); const before=this.rows(name); const keep=before.filter(r=>!this.match(r,q)); this.data.set(name,keep); return {deletedCount:before.length-keep.length}; },
      aggregate: (pipeline: Document[]) => { const q=pipeline[0]!.$search.cosmosSearch; record("aggregate",q.filter); return this.cursor(this.rows(name).filter(r=>this.match(r,q.filter)).slice(0,q.k).map(r=>({...r,score:1}))); },
    };
  }
  client(): MongoClient {
    const db = { collection: (name:string) => this.collection(name), listCollections: () => this.cursor([...this.data.keys()].map(name=>({name}))), createCollection: async (name:string) => { this.calls.push({op:"createCollection",name}); this.rows(name); }, command: async (q:Document) => { this.calls.push({op:"command",name:q.createIndexes}); this.indexes.set(q.createIndexes,[...(this.indexes.get(q.createIndexes) ?? []),...q.indexes]); return {ok:1}; } };
    return { db: () => db, connect: async () => {if(this.failConnect) throw new Error("connect failure");}, close: async () => {this.closes++;} } as unknown as MongoClient;
  }
}
