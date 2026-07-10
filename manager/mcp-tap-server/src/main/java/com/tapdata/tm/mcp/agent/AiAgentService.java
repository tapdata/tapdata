package com.tapdata.tm.mcp.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AiAgentService {

    private final AiChatClientFactory chatClientFactory;
    private final AnnotatedMcpToolCallbackProvider toolCallbackProvider;
    private final ObjectMapper objectMapper;

    public AiAgentService(AiChatClientFactory chatClientFactory,
                          AnnotatedMcpToolCallbackProvider toolCallbackProvider,
                          ObjectMapper objectMapper) {
        this.chatClientFactory = chatClientFactory;
        this.toolCallbackProvider = toolCallbackProvider;
        this.objectMapper = objectMapper;
    }

    public void stream(AiChatRequest request, AgentRequestContext context, OutputStream outputStream) throws IOException {
        stream(request, context, outputStream, null);
    }

    public void stream(AiChatRequest request, AgentRequestContext context, OutputStream outputStream,
                       AiStreamEventWriter.ResponseBufferFlusher responseBufferFlusher) throws IOException {
        AiStreamEventWriter writer = new AiStreamEventWriter(objectMapper, outputStream, responseBufferFlusher);
        try {
            request.validate();
            List<Message> messages = request.getMessages()
                    .stream()
                    .map(this::toSpringAiMessage)
                    .collect(Collectors.toCollection(ArrayList::new));
            ChatClient chatClient = chatClientFactory.create(request.getLlm());

            var requestSpec = chatClient.prompt()
                    .messages(messages)
                    .toolContext(Map.of(
                            AnnotatedMcpToolCallbackProvider.AGENT_CONTEXT_KEY, context,
                            AnnotatedMcpToolCallbackProvider.STREAM_WRITER_KEY, writer
                    ));
            ToolCallback[] toolCallbacks = toolCallbackProvider.getToolCallbacks();
            if (toolCallbacks.length > 0) {
                requestSpec.tools((Object[]) toolCallbacks);
            }
            log.info("AI agent LLM stream starting: sessionId={}, userId={}, baseUrl={}, model={}, messages={}, tools={}",
                    context.sessionId(),
                    context.userId(),
                    request.getLlm().normalizedBaseUrl(),
                    request.getLlm().normalizedModel(),
                    messages.size(),
                    toolCallbacks.length);
            writer.send("started", Map.of("sessionId", context.sessionId()));

            AtomicInteger responseChunks = new AtomicInteger();
            AtomicInteger responseChars = new AtomicInteger();
            AtomicLong lastChunkAtNanos = new AtomicLong(System.nanoTime());
            requestSpec
                    .stream()
                    .chatResponse()
                    .doOnNext(response -> {
                        int chunkNumber = responseChunks.incrementAndGet();
                        String delta = contentOf(response);
                        if (delta == null || delta.isEmpty()) {
                            return;
                        }
                        responseChars.addAndGet(delta.length());
                        long now = System.nanoTime();
                        long sinceLastMs = TimeUnit.NANOSECONDS
                                .toMillis(now - lastChunkAtNanos.getAndSet(now));
                        if (chunkNumber <= 5 || chunkNumber % 20 == 0) {
                            log.info("AI agent LLM stream chunk: sessionId={}, chunk={}, chars={}, sinceLastMs={}",
                                    context.sessionId(), chunkNumber, delta.length(), sinceLastMs);
                        }
                        try {
                            writer.messageDelta(delta);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .doOnError(e -> log.warn("AI agent LLM stream failed: sessionId={}, error={}",
                            context.sessionId(), e.toString(), e))
                    .blockLast();
            if (responseChunks.get() == 0) {
                log.warn("AI agent LLM stream completed without chunks: sessionId={}, baseUrl={}, model={}",
                        context.sessionId(),
                        request.getLlm().normalizedBaseUrl(),
                        request.getLlm().normalizedModel());
                writer.error("LLM stream completed without any response chunks. Check provider streaming compatibility, llm.baseUrl, llm.model and api key.");
                return;
            }
            log.info("AI agent LLM stream completed: sessionId={}, chunks={}, chars={}",
                    context.sessionId(), responseChunks.get(), responseChars.get());
            writer.done();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (Exception e) {
            if (isInterrupted(e)) {
                log.info("AI agent LLM stream interrupted: sessionId={}", context.sessionId());
                Thread.currentThread().interrupt();
                return;
            }
            log.warn("AI agent stream failed before completion: sessionId={}, error={}",
                    context.sessionId(), e.toString(), e);
            writer.error(e.getMessage());
        }
    }

    private String contentOf(org.springframework.ai.chat.model.ChatResponse response) {
        if (response == null || response.getResult() == null || response.getResult().getOutput() == null) {
            return null;
        }
        return response.getResult().getOutput().getText();
    }

    private boolean isInterrupted(Throwable throwable) {
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private Message toSpringAiMessage(ChatMessage message) {
        String role = message.getRole() == null ? "user" : message.getRole().trim().toLowerCase();
        return switch (role) {
            case "system" -> new SystemMessage(message.getContent());
            case "assistant" -> new AssistantMessage(message.getContent());
            default -> new UserMessage(message.getContent());
        };
    }
}
