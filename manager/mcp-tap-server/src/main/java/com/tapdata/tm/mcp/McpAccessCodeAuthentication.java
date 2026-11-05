package com.tapdata.tm.mcp;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import io.modelcontextprotocol.spec.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.tapdata.tm.mcp.McpConfig.TOKEN;
import static com.tapdata.tm.mcp.McpConfig.USER_ID;

@Component
public class McpAccessCodeAuthentication {

    private final AccessTokenService accessTokenService;
    private final UserService userService;
    private final UserLogService userLogService;
    private final McpSessionAttributes sessionAttributes;

    public McpAccessCodeAuthentication(AccessTokenService accessTokenService,
                                       UserService userService,
                                       UserLogService userLogService,
                                       McpSessionAttributes sessionAttributes) {
        this.accessTokenService = accessTokenService;
        this.userService = userService;
        this.userLogService = userLogService;
        this.sessionAttributes = sessionAttributes;
    }

    public ServerResponse filter(ServerRequest request, HandlerFunction<ServerResponse> next) throws Exception {
        Map<String, Object> authenticatedAttributes = authenticateInitializeRequest(request);
        if (isUnauthorized(authenticatedAttributes)) {
            return ServerResponse.status(HttpStatus.UNAUTHORIZED).build();
        }
        if (authenticatedAttributes != null && authenticatedAttributes.containsKey("error")) {
            return ServerResponse.status(HttpStatus.UNAUTHORIZED).body(authenticatedAttributes.get("error"));
        }

        ServerResponse response = next.handle(request);
        String sessionId = sessionId(request, response);
        if (StringUtils.hasText(sessionId) && authenticatedAttributes != null) {
            sessionAttributes.put(sessionId, authenticatedAttributes);
            Object userId = authenticatedAttributes.get(USER_ID);
            if (userId != null) {
                userLogService.addUserLog(Modular.MCP, Operation.CONNECTED, userId.toString(), null, null);
            }
        }
        if ("DELETE".equalsIgnoreCase(request.method().name())) {
            sessionAttributes.remove(sessionId);
        }

        logRequest(sessionId, request);
        return response;
    }

    private Map<String, Object> authenticateInitializeRequest(ServerRequest request) throws IOException {
        if (StringUtils.hasText(request.headers().asHttpHeaders().getFirst(HttpHeaders.MCP_SESSION_ID))) {
            return null;
        }
        if (!"POST".equalsIgnoreCase(request.method().name())) {
            return null;
        }

        String accessCode = Utils.getAccessCode(request);
        if (!StringUtils.hasText(accessCode)) {
            return Collections.emptyMap();
        }

        AccessTokenDto accessTokenDto = accessTokenService.generateToken(accessCode);
        if (accessTokenDto == null) {
            return Collections.singletonMap("error", "Not found user.");
        }

        UserDto userDetail = userService.getUserDetail(accessTokenDto.getUserId());
        boolean hasMcpRole = userDetail.getRoleMappings()
                .stream()
                .filter(r -> r.getRole() != null && r.getRole().getName() != null)
                .map(RoleMappingDto::getRole)
                .map(role -> role.getName().toLowerCase())
                .anyMatch(Arrays.asList("mcp", "admin")::contains);
        if (!hasMcpRole) {
            return Collections.singletonMap("error", "Not granted the mcp role");
        }

        Map<String, Object> attrs = new ConcurrentHashMap<>();
        attrs.put(TOKEN, accessTokenDto.getId());
        attrs.put(USER_ID, accessTokenDto.getUserId());
        return attrs;
    }

    private boolean isUnauthorized(Map<String, Object> attrs) {
        return attrs != null && attrs.isEmpty();
    }

    private String sessionId(ServerRequest request, ServerResponse response) {
        return Optional.ofNullable(response.headers().getFirst(HttpHeaders.MCP_SESSION_ID))
                .orElse(request.headers().asHttpHeaders().getFirst(HttpHeaders.MCP_SESSION_ID));
    }

    private void logRequest(String sessionId, ServerRequest request) {
        if (!"POST".equalsIgnoreCase(request.method().name())) {
            return;
        }
        Object userId = sessionAttributes.getAttribute(sessionId, USER_ID);
        if (userId == null) {
            return;
        }
        userLogService.addUserLog(Modular.MCP, Operation.READ, userId.toString(), null,
                "MCP " + request.method() + " " + request.uri());
    }
}
