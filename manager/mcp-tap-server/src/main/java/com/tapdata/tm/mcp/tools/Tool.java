package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.tapdata.tm.mcp.McpConfig.TOKEN;
import static com.tapdata.tm.mcp.McpConfig.USER_ID;
import static com.tapdata.tm.mcp.Utils.getSession;
import static com.tapdata.tm.mcp.Utils.toJson;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:55
 */
@Slf4j
public abstract class Tool {
    protected SessionAttribute sessionAttribute;
    protected UserService userService;
    @Getter
    private String name;
    @Getter
    private String description;
    @Getter
    private String inputSchema;
    @Getter
    private McpSchema.JsonSchema jsonSchema;

    public Tool(String name, String description, String schema, SessionAttribute sessionAttribute, UserService userService) {
        this.name = name;
        this.description = description;
        this.inputSchema = schema;
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
    public String getAccessToken(McpSyncServerExchange exchange) {
        String sessionId = getSession(exchange).getId();
        return Optional.ofNullable(sessionAttribute.getAttribute(sessionId, TOKEN)).map(Object::toString).orElse(null);
    }

    public UserDetail getUserDetail(McpSyncServerExchange exchange) {
        String userId = getUserId(exchange);
        if (userId == null) {
            log.error("Not found userId in session");
            throw new RuntimeException("Not found userId in current session");
        }
        if (userService == null) {
            throw new RuntimeException("Not initialized userServices before call.");
        }
        return userService.loadUserById(toObjectId(userId));
    }

    protected McpSchema.CallToolResult makeCallToolResult(Object data) {
        String result = data instanceof String ? data.toString() : toJson(data);
        McpSchema.TextContent context = new McpSchema.TextContent(null, null, result);
        return new McpSchema.CallToolResult(Collections.singletonList(context), false);
    }

    public abstract McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params);
}
