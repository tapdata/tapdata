package com.tapdata.tm.mcp.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AiAgentServiceTest {

    @Test
    void streamsSpringAiChatClientContentAsSse() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        AiChatClientFactory factory = config -> ChatClient.create(new FakeChatModel());
        AiAgentService service = new AiAgentService(factory,
                AnnotatedMcpToolCallbackProvider.fromToolBeans(List.of(), objectMapper),
                objectMapper);

        AiChatRequest request = new AiChatRequest();
        LlmConfig llm = new LlmConfig();
        llm.setBaseUrl("https://api.openai.com/v1");
        llm.setApiKey("sk-test");
        llm.setModel("gpt-4.1");
        request.setLlm(llm);
        request.setMessages(List.of(new ChatMessage("user", "hello")));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        service.stream(request, new AgentRequestContext("session-1", "user-1", "tap-token"), outputStream);

        String stream = outputStream.toString(StandardCharsets.UTF_8);
        assertTrue(stream.contains("event: message_delta\ndata: {\"content\":\"hello\"}"));
        assertTrue(stream.contains("event: message_delta\ndata: {\"content\":\" world\"}"));
        assertTrue(stream.contains("event: done"));
    }

    @Test
    void flushesMessageDeltaBeforeLlmStreamCompletes() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        CountDownLatch firstChunkFlushed = new CountDownLatch(1);
        CountDownLatch allowStreamToComplete = new CountDownLatch(1);
        AiChatClientFactory factory = config -> ChatClient.create(new GatedChatModel(allowStreamToComplete));
        AiAgentService service = new AiAgentService(factory,
                AnnotatedMcpToolCallbackProvider.fromToolBeans(List.of(), objectMapper),
                objectMapper);
        FlushAwareOutputStream outputStream = new FlushAwareOutputStream(firstChunkFlushed);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread streamThread = new Thread(() -> {
            try {
                service.stream(validRequest(), new AgentRequestContext("session-1", "user-1", "tap-token"), outputStream);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "ai-agent-stream-flush-test");

        streamThread.start();
        assertTrue(firstChunkFlushed.await(2, TimeUnit.SECONDS),
                "first message_delta should be flushed while the LLM stream is still open");

        String partialStream = outputStream.content();
        assertTrue(partialStream.contains("event: message_delta\ndata: {\"content\":\"hello\"}"));
        assertFalse(partialStream.contains("event: done"));
        assertNull(failure.get());

        allowStreamToComplete.countDown();
        streamThread.join(2_000);

        assertFalse(streamThread.isAlive());
        assertNull(failure.get());
        String completedStream = outputStream.content();
        assertTrue(completedStream.contains("event: message_delta\ndata: {\"content\":\" world\"}"));
        assertTrue(completedStream.contains("event: done"));
    }

    @Test
    void reportsErrorWhenLlmStreamCompletesWithoutChunks() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        AiChatClientFactory factory = config -> ChatClient.create(new EmptyChatModel());
        AiAgentService service = new AiAgentService(factory,
                AnnotatedMcpToolCallbackProvider.fromToolBeans(List.of(), objectMapper),
                objectMapper);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        service.stream(validRequest(), new AgentRequestContext("session-1", "user-1", "tap-token"), outputStream);

        String stream = outputStream.toString(StandardCharsets.UTF_8);
        assertTrue(stream.contains("event: error"));
        assertTrue(stream.contains("LLM stream completed without any response chunks"));
        assertFalse(stream.contains("event: done"));
    }

    @Test
    void returnsQuietlyWhenStreamingThreadIsInterrupted() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        CountDownLatch subscribed = new CountDownLatch(1);
        AiChatClientFactory factory = config -> ChatClient.create(new NeverEndingChatModel(subscribed));
        AiAgentService service = new AiAgentService(factory,
                AnnotatedMcpToolCallbackProvider.fromToolBeans(List.of(), objectMapper),
                objectMapper);
        AiChatRequest request = validRequest();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptedAfterReturn = new AtomicBoolean(false);

        Thread streamThread = new Thread(() -> {
            try {
                service.stream(request, new AgentRequestContext("session-1", "user-1", "tap-token"), outputStream);
                interruptedAfterReturn.set(Thread.currentThread().isInterrupted());
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "ai-agent-stream-test");

        streamThread.start();
        assertTrue(subscribed.await(2, TimeUnit.SECONDS));
        streamThread.interrupt();
        streamThread.join(2_000);

        assertFalse(streamThread.isAlive());
        assertNull(failure.get());
        assertTrue(interruptedAfterReturn.get());
        assertFalse(outputStream.toString(StandardCharsets.UTF_8).contains("event: error"));
    }

    static class FakeChatModel implements ChatModel {
        @Override
        public ChatResponse call(Prompt prompt) {
            return response("hello world");
        }

        @Override
        public reactor.core.publisher.Flux<ChatResponse> stream(Prompt prompt) {
            return reactor.core.publisher.Flux.just(response("hello"), response(" world"));
        }

        private ChatResponse response(String content) {
            return new ChatResponse(List.of(new Generation(new AssistantMessage(content))));
        }
    }

    static class GatedChatModel implements ChatModel {
        private final CountDownLatch allowStreamToComplete;

        GatedChatModel(CountDownLatch allowStreamToComplete) {
            this.allowStreamToComplete = allowStreamToComplete;
        }

        @Override
        public ChatResponse call(Prompt prompt) {
            return response("hello world");
        }

        @Override
        public reactor.core.publisher.Flux<ChatResponse> stream(Prompt prompt) {
            return reactor.core.publisher.Flux.concat(
                    reactor.core.publisher.Flux.just(response("hello")),
                    reactor.core.publisher.Flux.defer(() -> {
                        try {
                            if (!allowStreamToComplete.await(2, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Timed out waiting to release test stream");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                        return reactor.core.publisher.Flux.just(response(" world"));
                    })
            );
        }

        private ChatResponse response(String content) {
            return new ChatResponse(List.of(new Generation(new AssistantMessage(content))));
        }
    }

    static class EmptyChatModel implements ChatModel {
        @Override
        public ChatResponse call(Prompt prompt) {
            return new ChatResponse(List.of());
        }

        @Override
        public reactor.core.publisher.Flux<ChatResponse> stream(Prompt prompt) {
            return reactor.core.publisher.Flux.empty();
        }
    }

    static class NeverEndingChatModel implements ChatModel {
        private final CountDownLatch subscribed;

        NeverEndingChatModel(CountDownLatch subscribed) {
            this.subscribed = subscribed;
        }

        @Override
        public ChatResponse call(Prompt prompt) {
            return new ChatResponse(List.of(new Generation(new AssistantMessage(""))));
        }

        @Override
        public reactor.core.publisher.Flux<ChatResponse> stream(Prompt prompt) {
            return reactor.core.publisher.Flux.<ChatResponse>never()
                    .doOnSubscribe(subscription -> subscribed.countDown());
        }
    }

    static class FlushAwareOutputStream extends OutputStream {
        private final ByteArrayOutputStream delegate = new ByteArrayOutputStream();
        private final CountDownLatch firstChunkFlushed;

        FlushAwareOutputStream(CountDownLatch firstChunkFlushed) {
            this.firstChunkFlushed = firstChunkFlushed;
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
            if (content().contains("event: message_delta\ndata: {\"content\":\"hello\"}")) {
                firstChunkFlushed.countDown();
            }
        }

        synchronized String content() {
            return delegate.toString(StandardCharsets.UTF_8);
        }
    }

    private AiChatRequest validRequest() {
        AiChatRequest request = new AiChatRequest();
        LlmConfig llm = new LlmConfig();
        llm.setBaseUrl("https://api.openai.com/v1");
        llm.setApiKey("sk-test");
        llm.setModel("gpt-4.1");
        request.setLlm(llm);
        request.setMessages(List.of(new ChatMessage("user", "hello")));
        return request;
    }
}
