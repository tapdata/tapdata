package com.tapdata.tm.mcp.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.mcp.McpConfig;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.server.McpAsyncServerExchange;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpLoggableSession;
import io.modelcontextprotocol.spec.McpSchema;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.spring.SyncMcpAnnotationProviders;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.definition.DefaultToolDefinition;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Component
public class AnnotatedMcpToolCallbackProvider {

    public static final String AGENT_CONTEXT_KEY = "tapdataAgentRequestContext";
    public static final String STREAM_WRITER_KEY = "tapdataAiStreamWriter";

    private final ObjectMapper objectMapper;
    private final List<ToolCallback> toolCallbacks;

    @Autowired
    public AnnotatedMcpToolCallbackProvider(ApplicationContext applicationContext, ObjectMapper objectMapper) {
        this(selectToolBeans(applicationContext), objectMapper);
    }

    AnnotatedMcpToolCallbackProvider(List<Object> toolBeans, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.toolCallbacks = SyncMcpAnnotationProviders.toolSpecifications(toolBeans)
                .stream()
                .map(this::toToolCallback)
                .collect(Collectors.toList());
    }

    public static AnnotatedMcpToolCallbackProvider fromToolBeans(List<Object> toolBeans, ObjectMapper objectMapper) {
        return new AnnotatedMcpToolCallbackProvider(toolBeans, objectMapper);
    }

    public ToolCallback[] getToolCallbacks() {
        return toolCallbacks.toArray(new ToolCallback[0]);
    }

    private ToolCallback toToolCallback(McpServerFeatures.SyncToolSpecification spec) {
        ToolDefinition definition;
        try {
            definition = DefaultToolDefinition.builder()
                    .name(spec.tool().name())
                    .description(spec.tool().description())
                    .inputSchema(objectMapper.writeValueAsString(spec.tool().inputSchema()))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Build Spring AI tool definition failed for MCP tool: " + spec.tool().name(), e);
        }
        return new AnnotatedMcpToolCallback(definition, spec);
    }

    private class AnnotatedMcpToolCallback implements ToolCallback {
        private final ToolDefinition definition;
        private final McpServerFeatures.SyncToolSpecification spec;

        private AnnotatedMcpToolCallback(ToolDefinition definition, McpServerFeatures.SyncToolSpecification spec) {
            this.definition = definition;
            this.spec = spec;
        }

        @Override
        public ToolDefinition getToolDefinition() {
            return definition;
        }

        @Override
        public String call(String toolInput) {
            return call(toolInput, null);
        }

        @Override
        public String call(String toolInput, ToolContext toolContext) {
            Map<String, Object> arguments = parseArguments(toolInput);
            AiStreamEventWriter writer = streamWriter(toolContext);
            String eventId = definition.name() + "-" + UUID.randomUUID();
            try {
                if (writer != null) {
                    writer.toolCallStart(eventId, definition.name(), arguments);
                }
                McpSchema.CallToolResult result = spec.callHandler().apply(exchange(agentContext(toolContext)),
                        new McpSchema.CallToolRequest(spec.tool().name(), arguments));
                String text = resultToText(result);
                if (writer != null) {
                    writer.toolCallResult(eventId, definition.name(), parseJsonOrString(text));
                }
                return text;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private Map<String, Object> parseArguments(String toolInput) {
        if (toolInput == null || toolInput.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(toolInput, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid tool arguments JSON: " + toolInput, e);
        }
    }

    private String resultToText(McpSchema.CallToolResult result) {
        try {
            if (result.structuredContent() != null) {
                return objectMapper.writeValueAsString(result.structuredContent());
            }
            if (result.content() != null && result.content().size() == 1
                    && result.content().get(0) instanceof McpSchema.TextContent textContent) {
                return compactJsonIfPossible(textContent.text());
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("isError", Boolean.TRUE.equals(result.isError()));
            payload.put("content", result.content());
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new RuntimeException("Serialize MCP tool result failed", e);
        }
    }

    private String compactJsonIfPossible(String text) {
        try {
            Object parsed = objectMapper.readValue(text, new TypeReference<Object>() {
            });
            return objectMapper.writeValueAsString(parsed);
        } catch (Exception ignored) {
            return text;
        }
    }

    private Object parseJsonOrString(String value) {
        try {
            return objectMapper.readValue(value, new TypeReference<Object>() {
            });
        } catch (Exception ignored) {
            return value;
        }
    }

    private AgentRequestContext agentContext(ToolContext toolContext) {
        if (toolContext == null) {
            return null;
        }
        Object value = toolContext.getContext().get(AGENT_CONTEXT_KEY);
        return value instanceof AgentRequestContext context ? context : null;
    }

    private AiStreamEventWriter streamWriter(ToolContext toolContext) {
        if (toolContext == null) {
            return null;
        }
        Object value = toolContext.getContext().get(STREAM_WRITER_KEY);
        return value instanceof AiStreamEventWriter writer ? writer : null;
    }

    private McpSyncServerExchange exchange(AgentRequestContext context) {
        Map<String, Object> values = new LinkedHashMap<>();
        if (context != null) {
            values.put(McpConfig.USER_ID, context.userId());
            values.put(McpConfig.TOKEN, context.accessToken());
        }
        McpAsyncServerExchange asyncExchange = new McpAsyncServerExchange(
                context == null ? "ai-agent" : context.sessionId(),
                NoopLoggableSession.INSTANCE,
                McpSchema.ClientCapabilities.builder().build(),
                new McpSchema.Implementation("tapdata-ai-agent", "1.0.0"),
                McpTransportContext.create(values));
        return new McpSyncServerExchange(asyncExchange);
    }

    private static List<Object> selectToolBeans(ApplicationContext applicationContext) {
        List<Object> beans = new ArrayList<>();
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Class<?> type = applicationContext.getType(beanName);
            if (type != null && hasMcpToolMethod(type)) {
                beans.add(applicationContext.getBean(beanName));
            }
        }
        return beans;
    }

    private static boolean hasMcpToolMethod(Class<?> type) {
        AtomicBoolean found = new AtomicBoolean(false);
        ReflectionUtils.doWithMethods(type, method -> found.set(true),
                method -> AnnotatedElementUtils.hasAnnotation(method, McpTool.class));
        return found.get();
    }

    private enum NoopLoggableSession implements McpLoggableSession {
        INSTANCE;

        @Override
        public <T> Mono<T> sendRequest(String method, Object requestParams, TypeRef<T> typeRef) {
            return Mono.error(new UnsupportedOperationException("AI Agent internal MCP bridge does not support client requests."));
        }

        @Override
        public Mono<Void> sendNotification(String method, Object params) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> closeGracefully() {
            return Mono.empty();
        }

        @Override
        public void close() {
        }

        @Override
        public void setMinLoggingLevel(McpSchema.LoggingLevel minLoggingLevel) {
        }

        @Override
        public boolean isNotificationForLevelAllowed(McpSchema.LoggingLevel loggingLevel) {
            return true;
        }
    }
}
