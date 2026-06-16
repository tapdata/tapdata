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
import io.modelcontextprotocol.spec.McpStreamableServerSession;
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.util.ContentCachingRequestWrapper;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.tapdata.tm.mcp.McpConfig.TOKEN;
import static com.tapdata.tm.mcp.McpConfig.USER_ID;

public class StreamableMcpTransportProvider extends HttpServlet
        implements McpStreamableServerTransportProvider, SessionAttribute {

    private final HttpServletStreamableServerTransportProvider delegate;
    private final AccessTokenService accessTokenService;
    private final UserService userService;
    private final UserLogService userLogService;
    private final ConcurrentHashMap<String, Map<String, Object>> sessionAttributes = new ConcurrentHashMap<>();

    public StreamableMcpTransportProvider(String mcpEndpoint,
                                          AccessTokenService accessTokenService,
                                          UserService userService,
                                          UserLogService userLogService) {
        this.accessTokenService = accessTokenService;
        this.userService = userService;
        this.userLogService = userLogService;
        this.delegate = HttpServletStreamableServerTransportProvider.builder()
                .mcpEndpoint(mcpEndpoint)
                .contextExtractor(request -> io.modelcontextprotocol.common.McpTransportContext.create(
                        getSessionAttributes(request.getHeader(HttpHeaders.MCP_SESSION_ID))))
                .build();
    }

    @Override
    public void setSessionFactory(McpStreamableServerSession.Factory sessionFactory) {
        delegate.setSessionFactory(sessionFactory);
    }

    @Override
    public Mono<Void> notifyClients(String method, Object params) {
        return delegate.notifyClients(method, params);
    }

    @Override
    public Mono<Void> closeGracefully() {
        sessionAttributes.clear();
        return delegate.closeGracefully();
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ContentCachingRequestWrapper requestWrapper = new ContentCachingRequestWrapper(request, 1024 * 1024);
        SessionCaptureResponseWrapper responseWrapper = new SessionCaptureResponseWrapper(response);

        Map<String, Object> authenticatedAttributes = authenticateInitializeRequest(requestWrapper, responseWrapper);
        if (responseWrapper.isCommitted()) {
            return;
        }

        delegate.service(requestWrapper, responseWrapper);

        String sessionId = Optional.ofNullable(responseWrapper.getMcpSessionId())
                .orElse(requestWrapper.getHeader(HttpHeaders.MCP_SESSION_ID));
        if (StringUtils.hasText(sessionId) && authenticatedAttributes != null) {
            authenticatedAttributes.put("sessionId", sessionId);
            sessionAttributes.put(sessionId, authenticatedAttributes);
            Object userId = authenticatedAttributes.get(USER_ID);
            if (userId != null) {
                userLogService.addUserLog(Modular.MCP, Operation.CONNECTED, userId.toString(), null, null);
            }
        }

        logRequest(sessionId, requestWrapper);
    }

    private Map<String, Object> authenticateInitializeRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String sessionId = request.getHeader(HttpHeaders.MCP_SESSION_ID);
        if (StringUtils.hasText(sessionId)) {
            return null;
        }
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return null;
        }

        String accessCode = getAccessCode(request);
        if (!StringUtils.hasText(accessCode)) {
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            return null;
        }

        AccessTokenDto accessTokenDto = accessTokenService.generateToken(accessCode);
        if (accessTokenDto == null) {
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.getWriter().write("Not found user.");
            return null;
        }

        UserDto userDetail = userService.getUserDetail(accessTokenDto.getUserId());
        boolean hasMcpRole = userDetail.getRoleMappings()
                .stream()
                .filter(r -> r.getRole() != null && r.getRole().getName() != null)
                .map(RoleMappingDto::getRole)
                .map(role -> role.getName().toLowerCase())
                .anyMatch(Arrays.asList("mcp", "admin")::contains);
        if (!hasMcpRole) {
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.getWriter().write("Not granted the mcp role");
            return null;
        }

        Map<String, Object> attrs = new ConcurrentHashMap<>();
        attrs.put(TOKEN, accessTokenDto.getId());
        attrs.put(USER_ID, accessTokenDto.getUserId());
        return attrs;
    }

    private String getAccessCode(HttpServletRequest request) {
        String authorization = request.getHeader("Authorization");
        if (!StringUtils.hasText(authorization)) {
            return request.getParameter("accessCode");
        }
        String[] tmp = authorization.split(" ");
        return tmp.length > 1 ? tmp[1] : tmp[0];
    }

    private void logRequest(String sessionId, ContentCachingRequestWrapper request) {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return;
        }
        Object userId = getAttribute(sessionId, USER_ID);
        if (userId == null) {
            return;
        }
        byte[] content = request.getContentAsByteArray();
        if (content.length == 0) {
            return;
        }
        userLogService.addUserLog(Modular.MCP, Operation.READ, userId.toString(), null,
                new String(content, StandardCharsets.UTF_8));
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

    private static class SessionCaptureResponseWrapper extends HttpServletResponseWrapper {
        private String mcpSessionId;

        public SessionCaptureResponseWrapper(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setHeader(String name, String value) {
            captureMcpSessionId(name, value);
            super.setHeader(name, value);
        }

        @Override
        public void addHeader(String name, String value) {
            captureMcpSessionId(name, value);
            super.addHeader(name, value);
        }

        public String getMcpSessionId() {
            return mcpSessionId;
        }

        private void captureMcpSessionId(String name, String value) {
            if (HttpHeaders.MCP_SESSION_ID.equalsIgnoreCase(name)) {
                this.mcpSessionId = value;
            }
        }
    }
}
