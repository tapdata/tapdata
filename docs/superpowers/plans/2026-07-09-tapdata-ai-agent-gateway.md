# TapData AI Agent Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a backend chat gateway that lets the TapData frontend send user-provided OpenAI-compatible LLM config, stream assistant output, and let the LLM call existing TapData MCP tools.

**Architecture:** The frontend calls a TapData REST/SSE endpoint, not the LLM provider directly. The backend builds a per-request Spring AI `ChatClient` from the user-provided OpenAI-compatible config and registers MCP tools through Spring AI `ToolCallbackProvider`. Existing TapData `@McpTool` beans are adapted into Spring AI tool callbacks in-process for the first version, preserving the same tool schema and behavior as the external MCP server while leaving a clean path to replace that provider with Spring AI MCP Client when the agent talks to `/mcp`.

**Tech Stack:** Java 17, Spring MVC `StreamingResponseBody`, Spring AI `ChatClient`, Spring AI OpenAI model, Spring AI MCP annotations/tool callbacks, Jackson.

---

### Task 1: Stream Event Contract

**Files:**
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/AiStreamEventWriter.java`
- Test: `manager/mcp-tap-server/src/test/java/com/tapdata/tm/mcp/agent/AiStreamEventWriterTest.java`

- [x] **Step 1: Write failing tests for SSE formatting**

Tests assert that events are written as `event: <name>` plus JSON `data`, and that multiline JSON is split into multiple `data:` lines.

- [ ] **Step 2: Implement the writer**

The writer serializes payloads with Jackson, writes UTF-8 SSE frames, flushes per event, and exposes helpers for `message_delta`, `tool_call_start`, `tool_call_result`, `done`, and `error`.

### Task 2: Chat Request Model

**Files:**
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/AiChatRequest.java`
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/LlmConfig.java`
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/ChatMessage.java`
- Test: `manager/mcp-tap-server/src/test/java/com/tapdata/tm/mcp/agent/AiChatRequestTest.java`

- [ ] **Step 1: Write failing validation tests**

Tests assert `baseUrl`, `apiKey`, `model`, and at least one message are required; `chatPath` is intentionally absent.

- [ ] **Step 2: Implement DTO validation helpers**

Keep DTOs simple Java beans for Jackson compatibility and add normalization methods used by the service.

### Task 3: MCP Tool Callback Provider

**Files:**
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/AnnotatedMcpToolCallbackProvider.java`
- Test: `manager/mcp-tap-server/src/test/java/com/tapdata/tm/mcp/agent/AnnotatedMcpToolCallbackProviderTest.java`

- [x] **Step 1: Write failing tests for tool exposure and direct execution**

Tests build a dummy `@McpTool`, verify it becomes a Spring AI `ToolCallback`, and call it through the provider.

- [x] **Step 2: Implement the provider**

Use `SyncMcpAnnotationProviders.toolSpecifications(...)` so the frontend Agent path and external MCP path reuse the same annotated tool definitions. The provider passes TapData user/token context through Spring AI `ToolContext` into the MCP exchange and emits tool lifecycle SSE events.

### Task 4: Spring AI Chat Client Gateway

**Files:**
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/AiChatClientFactory.java`
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/SpringAiChatClientFactory.java`
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/AiAgentService.java`
- Test: `manager/mcp-tap-server/src/test/java/com/tapdata/tm/mcp/agent/AiAgentServiceTest.java`

- [x] **Step 1: Write failing tests for LLM delta streaming**

Tests use a fake Spring AI `ChatModel` through a real `ChatClient`, then verify assistant deltas are converted to TapData SSE frames.

- [x] **Step 2: Implement the Spring AI client factory**

Build `OpenAiChatModel` and `ChatClient` per request from frontend-provided `baseUrl`, `apiKey`/`authToken`, `model`, `temperature`, and `maxTokens`. No `chatPath` is exposed.

- [x] **Step 3: Implement the service stream**

Register Spring AI `ToolCallbackProvider` instances with `ChatClient.prompt().tools(...)`, pass user/token context with `toolContext(...)`, stream assistant content, and let Spring AI handle the tool-calling loop. Write tools execute directly when the LLM chooses them; there is no extra confirmation gate in this backend path.

### Task 5: REST/SSE Gateway

**Files:**
- Create: `manager/mcp-tap-server/src/main/java/com/tapdata/tm/mcp/agent/AiAgentController.java`
- Test: `manager/mcp-tap-server/src/test/java/com/tapdata/tm/mcp/agent/AiAgentControllerTest.java`

- [ ] **Step 1: Write failing controller tests**

Tests assert `POST /api/ai-agent/chat/stream` produces `text/event-stream` and passes the current user plus Authorization token into the service.

- [ ] **Step 2: Implement the controller**

Extend `BaseController`, resolve `getLoginUser()`, extract the current `Authorization` bearer value for MCP tools that proxy into TapData APIs, and return `StreamingResponseBody`.

### Task 6: Verification

**Files:**
- Modify only as needed after compilation feedback.

- [x] **Step 1: Run focused tests**

Run: `mvn -pl manager/mcp-tap-server -Dtest='Ai*Test,AnnotatedMcpToolCallbackProviderTest' test`

- [x] **Step 2: Run existing MCP tests**

Run: `mvn -pl manager/mcp-tap-server test`

- [ ] **Step 3: Report frontend follow-up**

If frontend write access is not approved, report the backend endpoint contract and the exact frontend repo files that need to remove `chatPath` and switch from direct provider calls to TapData SSE.
