package com.tapdata.tm.mcp;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStreamableServerSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.server.webmvc.transport.WebMvcStreamableServerTransportProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.util.Collections;
import java.util.List;

import static com.tapdata.tm.mcp.McpConfig.TOKEN;
import static com.tapdata.tm.mcp.McpConfig.USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class McpAccessCodeAuthenticationTest {

    @Mock
    private AccessTokenService accessTokenService;

    @Mock
    private UserService userService;

    @Mock
    private UserLogService userLogService;

    @Mock
    private McpStreamableServerSession.Factory sessionFactory;

    @Mock
    private McpStreamableServerSession mcpSession;

    private McpSessionAttributes sessionAttributes;
    private RouterFunction<ServerResponse> routerFunction;

    @BeforeEach
    void setUp() {
        sessionAttributes = new McpSessionAttributes();
        WebMvcStreamableServerTransportProvider transportProvider = WebMvcStreamableServerTransportProvider.builder()
                .jsonMapper(new JacksonMcpJsonMapper(JsonMapper.builder().build()))
                .mcpEndpoint("/mcp")
                .contextExtractor(sessionAttributes::extractContext)
                .build();
        transportProvider.setSessionFactory(sessionFactory);

        McpAccessCodeAuthentication authentication = new McpAccessCodeAuthentication(accessTokenService,
                userService, userLogService, sessionAttributes);
        routerFunction = transportProvider.getRouterFunction().filter(authentication::filter);
    }

    @Test
    void routerShouldAuthenticateInitializeRequestAndStoreSessionAttributes() throws Exception {
        AccessTokenDto accessTokenDto = new AccessTokenDto();
        accessTokenDto.setId("token-id");
        accessTokenDto.setUserId("user-id");
        when(accessTokenService.generateToken("access-code")).thenReturn(accessTokenDto);
        when(userService.getUserDetail("user-id")).thenReturn(userWithRole("mcp"));
        when(mcpSession.getId()).thenReturn("mcp-session-id");
        when(sessionFactory.startSession(any(McpSchema.InitializeRequest.class))).thenReturn(
                new McpStreamableServerSession.McpStreamableServerSessionInit(mcpSession, Mono.just(initializeResult())));

        MockHttpServletRequest request = initializeRequest();
        request.addHeader("Authorization", "Bearer access-code");

        ServerResponse response = handle(request);

        assertEquals(HttpStatus.OK.value(), response.statusCode().value());
        assertEquals("mcp-session-id", response.headers().getFirst(HttpHeaders.MCP_SESSION_ID));
        assertEquals("token-id", sessionAttributes.getAttribute("mcp-session-id", TOKEN));
        assertEquals("user-id", sessionAttributes.getAttribute("mcp-session-id", USER_ID));
        verify(userLogService).addUserLog(Modular.MCP, Operation.CONNECTED, "user-id", null, null);
        verify(userLogService).addUserLog(eq(Modular.MCP), eq(Operation.READ), eq("user-id"), eq((String) null), any(String.class));
    }

    @Test
    void routerShouldRejectInitializeRequestWithoutAccessCode() throws Exception {
        ServerResponse response = handle(initializeRequest());

        assertEquals(HttpStatus.UNAUTHORIZED.value(), response.statusCode().value());
        verify(sessionFactory, never()).startSession(any(McpSchema.InitializeRequest.class));
        verifyNoInteractions(userLogService);
    }

    @Test
    void routerShouldRejectInitializeRequestWithoutMcpRole() throws Exception {
        AccessTokenDto accessTokenDto = new AccessTokenDto();
        accessTokenDto.setId("token-id");
        accessTokenDto.setUserId("user-id");
        when(accessTokenService.generateToken("access-code")).thenReturn(accessTokenDto);
        when(userService.getUserDetail("user-id")).thenReturn(userWithRole("member"));

        MockHttpServletRequest request = initializeRequest();
        request.setParameter("accessCode", "access-code");
        MockHttpServletResponse response = write(handle(request), request);

        assertEquals(HttpStatus.UNAUTHORIZED.value(), response.getStatus());
        assertEquals("Not granted the mcp role", response.getContentAsString());
        verify(sessionFactory, never()).startSession(any(McpSchema.InitializeRequest.class));
    }

    @Test
    void routerShouldAllowFollowUpRequestWithExistingSessionId() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/mcp");
        request.addHeader(HttpHeaders.MCP_SESSION_ID, "mcp-session-id");

        ServerResponse response = handle(request);

        assertNotEquals(HttpStatus.UNAUTHORIZED.value(), response.statusCode().value());
        verifyNoInteractions(accessTokenService, userService, userLogService);
    }

    @Test
    void setAttributeShouldOnlyUpdateExistingSession() {
        assertNull(sessionAttributes.setAttribute("missing-session", "key", "value"));
        assertNull(sessionAttributes.getAttribute("missing-session", "key"));

        sessionAttributes.put("stored-session-id", new java.util.concurrent.ConcurrentHashMap<>());

        assertNull(sessionAttributes.setAttribute("stored-session-id", "key", "value"));
        assertEquals("value", sessionAttributes.getAttribute("stored-session-id", "key"));
    }

    @Test
    void clearShouldRemoveSessionAttributes() {
        sessionAttributes.put("stored-session-id", new java.util.concurrent.ConcurrentHashMap<>(
                Collections.singletonMap(USER_ID, "user-id")));

        sessionAttributes.clear();

        assertNull(sessionAttributes.getAttribute("stored-session-id", USER_ID));
    }

    private ServerResponse handle(MockHttpServletRequest request) throws Exception {
        ServerRequest serverRequest = ServerRequest.create(request, messageConverters());
        HandlerFunction<ServerResponse> handler = routerFunction.route(serverRequest).orElseThrow();
        return handler.handle(serverRequest);
    }

    private MockHttpServletResponse write(ServerResponse serverResponse, MockHttpServletRequest request) throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        ModelAndView modelAndView = serverResponse.writeTo(request, response, () -> messageConverters());
        assertNull(modelAndView);
        return response;
    }

    private List<HttpMessageConverter<?>> messageConverters() {
        return Collections.singletonList(new StringHttpMessageConverter());
    }

    private MockHttpServletRequest initializeRequest() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/mcp");
        request.addHeader("Accept", "application/json, text/event-stream");
        request.setContentType("application/json");
        request.setContent(("""
                {
                  "jsonrpc": "2.0",
                  "id": 1,
                  "method": "initialize",
                  "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                      "name": "test-client",
                      "version": "1.0.0"
                    }
                  }
                }
                """).getBytes());
        return request;
    }

    private McpSchema.InitializeResult initializeResult() {
        return new McpSchema.InitializeResult(
                "2025-06-18",
                McpSchema.ServerCapabilities.builder().tools(true).build(),
                new McpSchema.Implementation("mcp-tap-server", "1.0.0"),
                null);
    }

    private UserDto userWithRole(String roleName) {
        RoleDto role = new RoleDto();
        role.setName(roleName);
        RoleMappingDto roleMapping = new RoleMappingDto();
        roleMapping.setRole(role);
        UserDto userDto = new UserDto();
        userDto.setRoleMappings(Collections.singletonList(roleMapping));
        return userDto;
    }
}
