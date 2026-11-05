package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.tapdata.tm.mcp.McpConfig.TOKEN;
import static com.tapdata.tm.mcp.McpConfig.USER_ID;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@Slf4j
@Component
public class McpToolSupport {

    private final SessionAttribute sessionAttribute;
    private final UserService userService;

    public McpToolSupport(SessionAttribute sessionAttribute, UserService userService) {
        this.sessionAttribute = sessionAttribute;
        this.userService = userService;
    }

    public String getUserId(McpSyncRequestContext context) {
        return getContextValue(context, USER_ID).orElseGet(() -> getSessionAttribute(sessionId(context), USER_ID));
    }

    public String getUserId(String sessionId) {
        if (sessionAttribute == null) {
            throw new RuntimeException("Not initialized sessionAttribute before call.");
        }
        return Optional.ofNullable(sessionAttribute.getAttribute(sessionId, USER_ID)).map(Object::toString).orElse(null);
    }

    public String getAccessToken(McpSyncRequestContext context) {
        return getContextValue(context, TOKEN).orElseGet(() -> getSessionAttribute(sessionId(context), TOKEN));
    }

    public UserDetail getUserDetail(McpSyncRequestContext context) {
        String userId = getUserId(context);
        if (userId == null) {
            log.error("Not found userId in session");
            throw new RuntimeException("Not found userId in current session");
        }
        if (userService == null) {
            throw new RuntimeException("Not initialized userServices before call.");
        }
        return userService.loadUserById(toObjectId(userId));
    }

    private Optional<String> getContextValue(McpSyncRequestContext context, String key) {
        if (context == null || context.transportContext() == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(context.transportContext().get(key)).map(Object::toString);
    }

    private String sessionId(McpSyncRequestContext context) {
        return context == null ? null : context.sessionId();
    }

    private String getSessionAttribute(String sessionId, String key) {
        if (sessionAttribute == null) {
            throw new RuntimeException("Not initialized sessionAttribute before call.");
        }
        return Optional.ofNullable(sessionAttribute.getAttribute(sessionId, key)).map(Object::toString).orElse(null);
    }
}
