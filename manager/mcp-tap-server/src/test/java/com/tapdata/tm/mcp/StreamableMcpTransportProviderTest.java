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
import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStreamableServerSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.tapdata.tm.mcp.McpConfig.TOKEN;
import static com.tapdata.tm.mcp.McpConfig.USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamableMcpTransportProviderTest {

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

    private StreamableMcpTransportProvider transportProvider;

    @BeforeEach
    void setUp() {
        transportProvider = new StreamableMcpTransportProvider("/mcp", accessTokenService, userService, userLogService);
        transportProvider.setSessionFactory(sessionFactory);
    }

    @Test
    void serviceShouldAuthenticateInitializeRequestAndStoreSessionAttributes() throws Exception {
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
        MockHttpServletResponse response = new MockHttpServletResponse();

        transportProvider.service(request, response);

        assertEquals(HttpStatus.OK.value(), response.getStatus());
        assertEquals("mcp-session-id", response.getHeader(HttpHeaders.MCP_SESSION_ID));
        assertEquals("token-id", transportProvider.getAttribute("mcp-session-id", TOKEN));
        assertEquals("user-id", transportProvider.getAttribute("mcp-session-id", USER_ID));
        assertEquals("token-id", transportProvider.getAttribute(response.getHeader(HttpHeaders.MCP_SESSION_ID), TOKEN));
        verify(userLogService).addUserLog(Modular.MCP, Operation.CONNECTED, "user-id", null, null);
        verify(userLogService).addUserLog(eq(Modular.MCP), eq(Operation.READ), eq("user-id"), eq((String) null), any(String.class));
    }

    @Test
    void serviceShouldRejectInitializeRequestWithoutAccessCode() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();

        transportProvider.service(initializeRequest(), response);

        assertEquals(HttpStatus.UNAUTHORIZED.value(), response.getStatus());
        verify(sessionFactory, never()).startSession(any(McpSchema.InitializeRequest.class));
        verifyNoInteractions(userLogService);
    }

    @Test
    void serviceShouldRejectInitializeRequestWithoutMcpRole() throws Exception {
        AccessTokenDto accessTokenDto = new AccessTokenDto();
        accessTokenDto.setId("token-id");
        accessTokenDto.setUserId("user-id");
        when(accessTokenService.generateToken("access-code")).thenReturn(accessTokenDto);
        when(userService.getUserDetail("user-id")).thenReturn(userWithRole("member"));

        MockHttpServletRequest request = initializeRequest();
        request.setParameter("accessCode", "access-code");
        MockHttpServletResponse response = new MockHttpServletResponse();

        transportProvider.service(request, response);

        assertEquals(HttpStatus.UNAUTHORIZED.value(), response.getStatus());
        assertEquals("Not granted the mcp role", response.getContentAsString());
        verify(sessionFactory, never()).startSession(any(McpSchema.InitializeRequest.class));
    }

    @Test
    void setAttributeShouldOnlyUpdateExistingSession() {
        assertNull(transportProvider.setAttribute("missing-session", "key", "value"));
        assertNull(transportProvider.getAttribute("missing-session", "key"));

        Map<String, Map<String, Object>> sessionAttributes =
                (Map<String, Map<String, Object>>) ReflectionTestUtils.getField(transportProvider, "sessionAttributes");
        Map<String, Object> attrs = new ConcurrentHashMap<>();
        attrs.put("sessionId", "alias-session-id");
        sessionAttributes.put("stored-session-id", attrs);

        assertNull(transportProvider.setAttribute("stored-session-id", "key", "value"));
        assertEquals("value", transportProvider.getAttribute("stored-session-id", "key"));
        assertEquals("value", transportProvider.getAttribute("alias-session-id", "key"));
    }

    @Test
    void closeGracefullyShouldClearSessionAttributes() {
        Map<String, Map<String, Object>> sessionAttributes =
                (Map<String, Map<String, Object>>) ReflectionTestUtils.getField(transportProvider, "sessionAttributes");
        sessionAttributes.put("stored-session-id", new ConcurrentHashMap<>(Collections.singletonMap(USER_ID, "user-id")));

        StepVerifier.create(transportProvider.closeGracefully()).verifyComplete();

        assertNull(transportProvider.getAttribute("stored-session-id", USER_ID));
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
