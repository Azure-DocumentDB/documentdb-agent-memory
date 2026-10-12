import assert from "node:assert/strict";
import { generateText, jsonSchema, stepCountIs, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { DocumentDBMemory } from "../src/index.js";
const usage = { inputTokens: { total: 1, noCache: 1, cacheRead: 0, cacheWrite: 0 }, outputTokens: { total: 1, text: 1, reasoning: 0 } };
/** Actual published AI SDK generateText/tool processing, mock model, no LLM/network. */
export async function sdkFlow(memory: DocumentDBMemory, reopen: () => Promise<DocumentDBMemory>, threadId: string): Promise<void> {
  await memory.appendMessages(threadId, [{ role: "system", content: "Use the lookup tool." }, { role: "user", content: [{ type: "text", text: "Remember Tuesday." }] }]);
  const model = new MockLanguageModelV4({ doGenerate: [
    { content: [{ type: "tool-call", toolCallId: "lookup-1", toolName: "lookup", input: '{"day":"Tuesday"}' }], finishReason: { unified: "tool-calls", raw: "tool-calls" }, usage, warnings: [] },
    { content: [{ type: "text", text: "Tuesday is recorded." }], finishReason: { unified: "stop", raw: "stop" }, usage, warnings: [] },
  ] });
  let executed = false;
  const result = await generateText({ model, ...await memory.getPrompt(threadId), stopWhen: stepCountIs(2), maxRetries: 0,
    tools: { lookup: tool({ inputSchema: jsonSchema<{day:string}>({type:"object",properties:{day:{type:"string"}},required:["day"],additionalProperties:false}), execute: async ({day}) => { executed=true; return { day, found: true }; } }) },
  });
  assert.equal(executed, true); assert.equal(result.steps.length, 2); assert.equal(result.text, "Tuesday is recorded.");
  const responseMessages = result.steps.flatMap(step => step.response.messages);
  assert.ok(responseMessages.some(m=>m.role === "tool"));
  await memory.appendMessages(threadId, responseMessages);
  const expected = await memory.getModelMessages(threadId);
  await memory.close();
  const resumed = await reopen();
  try {
    const history = await resumed.getModelMessages(threadId);
    assert.deepEqual(history, expected);
    assert.ok(history.some(m=>m.role==="tool" && Array.isArray(m.content) && m.content[0]?.type==="tool-result"));
    const nextModel = new MockLanguageModelV4({ doGenerate: { content:[{type:"text",text:"Resumed."}], finishReason:{unified:"stop",raw:"stop"},usage,warnings:[] } });
    const continuation = await generateText({model:nextModel,...await resumed.getPrompt(threadId),maxRetries:0});
    assert.equal(continuation.text,"Resumed.");
    assert.ok(nextModel.doGenerateCalls[0]!.prompt.some(m=>m.role==="tool"));
    await resumed.appendMessages(threadId, continuation.response.messages);
  } finally { await resumed.close(); }
}
