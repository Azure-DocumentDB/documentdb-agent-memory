import assert from "node:assert/strict";
import { test } from "node:test";
import type { OIDCCallbackParams } from "mongodb";
import { AZURE_SCOPE, buildLocalUri, clientFromEnvironment, clientFromUri, tokenCallback, validateURI } from "../src/index.js";
const host="fixture.global.mongocluster.cosmos.azure.com";
const native=`mongodb+srv://user:password@${host}/?tls=true&retryWrites=false`;
test("native managed URI validates TLS/strict Azure hostname without connecting",async()=>{assert.deepEqual(validateURI(native),{host,local:false}); const c=clientFromUri(native);assert.equal(c.options.tls,true);assert.equal(c.options.rejectUnauthorized,true);assert.equal(c.options.retryWrites,false);await c.close();});
test("reject unsafe hosts/TLS/options before credential construction",async()=>{
  for(const uri of [native.replace(host,"example.com"),native.replace(host,host+".evil.com"),native+"&tls=false",native+"&tlsAllowInvalidCertificates=true",native+"&tlsInsecure=true",native+"&authMechanism=MONGODB-OIDC",native+"&proxyHost=evil",native+"&TLs=true",native+"#x",native.replace("/?","/database?"),native.replace("mongodb+srv:","mongodb:")]) assert.throws(()=>validateURI(uri));
  let created=0; await assert.rejects(clientFromEnvironment({MONGO_AUTH_MODE:"oidc",MONGO_HOST:"evil.example"},async()=>{created++;return {getToken:async()=>null};}));assert.equal(created,0);
});
test("local builder encodes real credentials, restricts loopback/self-signed exception",async()=>{
  const uri=buildLocalUri("u@ser","p@ss:word");assert.equal(decodeURIComponent(new URL(uri).password),"p@ss:word");assert.equal(validateURI(uri).local,true);
  for(const host of ["example.com","127.0.0.1.evil","10.0.0.1","0.0.0.0"])assert.throws(()=>buildLocalUri("u","p",host));
  for(const port of [0,-1,65536,1.5])assert.throws(()=>buildLocalUri("u","p","localhost",port));
  const c=clientFromUri(uri);assert.equal(c.options.rejectUnauthorized,false);assert.equal(c.options.tls,true);await c.close();
});
test("environment auth is explicit and rejects native/OIDC confusion",async()=>{
  await assert.rejects(clientFromEnvironment({}));await assert.rejects(clientFromEnvironment({MONGO_AUTH_MODE:"connection-string",MONGO_CONNECTION_STRING:buildLocalUri("u","p")}));await assert.rejects(clientFromEnvironment({MONGO_AUTH_MODE:"oidc",MONGO_HOST:host,MONGO_CONNECTION_STRING:native}));
});
test("MongoDB 7.7 Entra empty password and narrow ALLOWED_HOSTS shape",async()=>{
  let tokens=0;const c=await clientFromEnvironment({MONGO_AUTH_MODE:"oidc",MONGO_HOST:host,MONGO_OIDC_USERNAME:"principal"},async()=>({getToken:async()=>{tokens++;return null;}}));
  assert.equal(c.options.credentials?.username,"principal");assert.equal(c.options.credentials?.password,"");assert.equal(c.options.credentials?.mechanism,"MONGODB-OIDC");assert.deepEqual(c.options.credentials?.mechanismProperties.ALLOWED_HOSTS,["*.global.mongocluster.cosmos.azure.com"]);assert.equal(tokens,0);await c.close();
});
test("OIDC fixed audience, expiry/version/abort validation",async()=>{
  const params={version:1,timeoutContext:new AbortController().signal} as OIDCCallbackParams;
  let scope="";assert.deepEqual(await tokenCallback(params,{getToken:async(s)=>{scope=s;return {token:"fixture",expiresOnTimestamp:61000};}},()=>1000),{accessToken:"fixture",expiresInSeconds:60});assert.equal(scope,AZURE_SCOPE);
  for(const token of [null,{token:"",expiresOnTimestamp:61000},{token:"fixture",expiresOnTimestamp:1000},{token:"fixture",expiresOnTimestamp:NaN}])await assert.rejects(tokenCallback(params,{getToken:async()=>token},()=>1000));
  await assert.rejects(tokenCallback({...params,version:2} as unknown as OIDCCallbackParams,{getToken:async()=>null}));const ac=new AbortController();ac.abort();await assert.rejects(tokenCallback({...params,timeoutContext:ac.signal},{getToken:async()=>null}));
});
