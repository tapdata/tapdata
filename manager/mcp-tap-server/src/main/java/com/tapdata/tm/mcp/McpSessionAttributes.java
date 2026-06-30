package com.tapdata.tm.mcp;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.spec.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.function.ServerRequest;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class McpSessionAttributes implements SessionAttribute {

    private final ConcurrentHashMap<String, Map<String, Object>> sessionAttributes = new ConcurrentHashMap<>();

    public McpTransportContext extractContext(ServerRequest request) {
        return McpTransportContext.create(getSessionAttributes(
                request.headers().asHttpHeaders().getFirst(HttpHeaders.MCP_SESSION_ID)));
    }

    public void put(String sessionId, Map<String, Object> attributes) {
        if (!StringUtils.hasText(sessionId) || attributes == null) {
            return;
        }
        attributes.put("sessionId", sessionId);
        sessionAttributes.put(sessionId, attributes);
    }

    public void remove(String sessionId) {
        if (StringUtils.hasText(sessionId)) {
            sessionAttributes.remove(sessionId);
        }
    }

    public void clear() {
        sessionAttributes.clear();
    }

    @Override
    public Object getAttribute(String sessionId, String key) {
        return getSessionAttributes(sessionId).get(key);
    }

    @Override
    public Object setAttribute(String sessionId, String key, Object value) {
        return Optional.ofNullable(getSessionAttributesOrNull(sessionId))
                .map(attrs -> attrs.put(key, value))
                .orElse(null);
    }

    private Map<String, Object> getSessionAttributes(String sessionId) {
        return Optional.ofNullable(getSessionAttributesOrNull(sessionId)).orElse(Collections.emptyMap());
    }

    private Map<String, Object> getSessionAttributesOrNull(String sessionId) {
        if (!StringUtils.hasText(sessionId)) {
            return null;
        }
        if (sessionAttributes.containsKey(sessionId)) {
            return sessionAttributes.get(sessionId);
        }
        return sessionAttributes.values()
                .stream()
                .filter(c -> sessionId.equals(c.get("sessionId")))
                .findFirst()
                .orElse(null);
    }
}
