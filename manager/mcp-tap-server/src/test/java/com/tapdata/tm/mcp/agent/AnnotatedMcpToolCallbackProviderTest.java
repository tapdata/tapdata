package com.tapdata.tm.mcp.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnnotatedMcpToolCallbackProviderTest {

    @Test
    void exposesMcpAnnotationsAsSpringAiToolCallbacks() {
        AnnotatedMcpToolCallbackProvider provider = AnnotatedMcpToolCallbackProvider
                .fromToolBeans(List.of(new EchoTool()), new ObjectMapper());

        ToolCallback[] callbacks = provider.getToolCallbacks();

        assertEquals(1, callbacks.length);
        assertEquals("echoName", callbacks[0].getToolDefinition().name());
        assertEquals("Echo a provided name.", callbacks[0].getToolDefinition().description());
        assertTrue(callbacks[0].getToolDefinition().inputSchema().contains("\"name\""));
    }

    @Test
    void callsMcpToolThroughSpringAiToolCallbackAndStreamsToolEvents() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        AnnotatedMcpToolCallbackProvider provider = AnnotatedMcpToolCallbackProvider
                .fromToolBeans(List.of(new EchoTool()), objectMapper);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AiStreamEventWriter writer = new AiStreamEventWriter(objectMapper, outputStream);
        ToolContext toolContext = new ToolContext(Map.of(
                AnnotatedMcpToolCallbackProvider.AGENT_CONTEXT_KEY,
                new AgentRequestContext("session-1", "user-1", "tap-token"),
                AnnotatedMcpToolCallbackProvider.STREAM_WRITER_KEY,
                writer
        ));

        String result = provider.getToolCallbacks()[0].call("{\"name\":\"Ada\"}", toolContext);

        assertTrue(result.contains("\"echo\":\"Ada\""));
        String stream = outputStream.toString(StandardCharsets.UTF_8);
        assertTrue(stream.contains("event: tool_call_start"));
        assertTrue(stream.contains("\"name\":\"echoName\""));
        assertTrue(stream.contains("event: tool_call_result"));
        assertTrue(stream.contains("\"echo\":\"Ada\""));
    }

    @Test
    void flushesToolCallStartBeforeMcpToolReturns() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        CountDownLatch toolEntered = new CountDownLatch(1);
        CountDownLatch allowToolToReturn = new CountDownLatch(1);
        CountDownLatch startEventFlushed = new CountDownLatch(1);
        AnnotatedMcpToolCallbackProvider provider = AnnotatedMcpToolCallbackProvider
                .fromToolBeans(List.of(new BlockingTool(toolEntered, allowToolToReturn)), objectMapper);
        FlushAwareOutputStream outputStream = new FlushAwareOutputStream(startEventFlushed);
        AiStreamEventWriter writer = new AiStreamEventWriter(objectMapper, outputStream);
        ToolContext toolContext = new ToolContext(Map.of(
                AnnotatedMcpToolCallbackProvider.STREAM_WRITER_KEY,
                writer
        ));
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread toolThread = new Thread(() -> {
            try {
                provider.getToolCallbacks()[0].call("{\"name\":\"Ada\"}", toolContext);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "ai-agent-tool-stream-test");

        toolThread.start();
        assertTrue(startEventFlushed.await(2, TimeUnit.SECONDS),
                "tool_call_start should be flushed before the MCP tool returns");
        assertTrue(toolEntered.await(2, TimeUnit.SECONDS));

        String partialStream = outputStream.content();
        assertTrue(partialStream.contains("event: tool_call_start"));
        assertFalse(partialStream.contains("event: tool_call_result"));
        assertNull(failure.get());

        allowToolToReturn.countDown();
        toolThread.join(2_000);

        assertFalse(toolThread.isAlive());
        assertNull(failure.get());
        assertTrue(outputStream.content().contains("event: tool_call_result"));
    }

    @Test
    void springCanInstantiateProviderBean() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(ObjectMapper.class);
            context.registerBean(EchoTool.class);
            context.register(AnnotatedMcpToolCallbackProvider.class);

            context.refresh();

            AnnotatedMcpToolCallbackProvider provider = context.getBean(AnnotatedMcpToolCallbackProvider.class);
            assertEquals(1, provider.getToolCallbacks().length);
            assertEquals(0, context.getBeansOfType(ToolCallbackProvider.class).size());
        }
    }

    static class EchoTool {
        @McpTool(name = "echoName", description = "Echo a provided name.")
        public Map<String, Object> echo(@McpToolParam(description = "Name to echo.") String name) {
            return Map.of("echo", name);
        }
    }

    static class BlockingTool {
        private final CountDownLatch toolEntered;
        private final CountDownLatch allowToolToReturn;

        BlockingTool(CountDownLatch toolEntered, CountDownLatch allowToolToReturn) {
            this.toolEntered = toolEntered;
            this.allowToolToReturn = allowToolToReturn;
        }

        @McpTool(name = "blockingEcho", description = "Echo a provided name after being released.")
        public Map<String, Object> echo(@McpToolParam(description = "Name to echo.") String name) {
            toolEntered.countDown();
            try {
                if (!allowToolToReturn.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to release test tool");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return Map.of("echo", name);
        }
    }

    static class FlushAwareOutputStream extends OutputStream {
        private final ByteArrayOutputStream delegate = new ByteArrayOutputStream();
        private final CountDownLatch startEventFlushed;

        FlushAwareOutputStream(CountDownLatch startEventFlushed) {
            this.startEventFlushed = startEventFlushed;
        }

        @Override
        public synchronized void write(int b) {
            delegate.write(b);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) {
            delegate.write(b, off, len);
        }

        @Override
        public synchronized void flush() throws IOException {
            if (content().contains("event: tool_call_start")) {
                startEventFlushed.countDown();
            }
        }

        synchronized String content() {
            return delegate.toString(StandardCharsets.UTF_8);
        }
    }
}
