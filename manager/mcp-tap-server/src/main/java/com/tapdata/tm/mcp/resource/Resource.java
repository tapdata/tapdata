package com.tapdata.tm.mcp.resource;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Optional;

import static com.tapdata.tm.mcp.McpConfig.USER_ID;
import static com.tapdata.tm.mcp.Utils.getSession;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/27 14:38
 */
public abstract class Resource {
    @Getter
    private String uri;
    @Getter
    private String name;
    @Getter
    private String description;
    @Getter
    private String mimeType;
    @Getter
    private McpSchema.Annotations annotations;
    protected SessionAttribute sessionAttribute;
    protected UserService userService;

    public Resource(String uri, String name, String description, String mimeType, McpSchema.Annotations annotations,
                    SessionAttribute sessionAttribute, UserService userService) {
        this.uri = uri;
        this.name = name;
        this.description = description;
        this.mimeType = mimeType;
        this.annotations = annotations;
        this.sessionAttribute = sessionAttribute;
        this.userService = userService;
    }
    public String getUserId(McpSyncServerExchange exchange) {
        String sessionId = getSession(exchange).getId();
        return getUserId(sessionId);
    }
    public String getUserId(String sessionId) {
        if (sessionAttribute == null) {
            throw new RuntimeException("Not initialized sessionAttribute before call.");
        }
        return Optional.ofNullable(sessionAttribute.getAttribute(sessionId, USER_ID)).map(Object::toString).orElse(null);
    }
    public UserDetail getUserDetail(McpSyncServerExchange exchange) {
        String userId = getUserId(exchange);
        if (userId == null) {
            throw new RuntimeException("Not found userId in current session");
        }
        if (userService == null) {
            throw new RuntimeException("Not initialized userServices before call.");
        }
        return userService.loadUserById(toObjectId(userId));
    }

    public abstract McpSchema.ReadResourceResult call(McpSyncServerExchange exchange, McpSchema.ReadResourceRequest request);
}
