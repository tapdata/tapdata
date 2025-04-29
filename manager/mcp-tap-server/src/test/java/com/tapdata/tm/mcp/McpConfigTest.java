package com.tapdata.tm.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.service.UserLogService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.RequestPath;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.web.servlet.function.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * @date 2025/04/21 09:08
 */
@ExtendWith(MockitoExtension.class)
class McpConfigTest {

    @Mock
    private AccessTokenService accessTokenService;

    @Mock
    private UserService userService;

    @Mock
    private UserLogService userLogService;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private McpConfig mcpConfig;

    @Mock
    private ServerRequest serverRequest;

    @Mock
    private HandlerFunction<ServerResponse> handlerFunction;

    @Mock
    private ServerRequest.Headers headers;

    @Mock
    private RequestPath requestPath;

    private MockHttpSession session;

    @Test
    void testWebMvcSseServerTransportProvider() {
        // 测试创建 SseServerTransportProvider
        SseServerTransportProvider provider = mcpConfig.webMvcSseServerTransportProvider(objectMapper, userLogService);
        assertNotNull(provider);
    }

    @Test
    void testMcpRouterFunction() {
        // 测试创建路由函数
        SseServerTransportProvider transportProvider = mock(SseServerTransportProvider.class);
        RouterFunction<ServerResponse> mockRouterFunction = mock(RouterFunction.class);
        when(mockRouterFunction.filter(any())).thenReturn(mockRouterFunction);
        when(transportProvider.getRouterFunction()).thenReturn(mockRouterFunction);

        var routerFunction = mcpConfig.mcpRouterFunction(transportProvider, accessTokenService, userService);
        assertNotNull(routerFunction);
    }

    @Test
    void testAuthFilter_SseEndpoint_Success() throws Exception {

        session = new MockHttpSession();
        when(serverRequest.session()).thenReturn(session);
        when(serverRequest.headers()).thenReturn(headers);

        // 模拟 SSE 端点请求
        when(requestPath.value()).thenReturn(mcpConfig.sseEndpoint);
        when(serverRequest.requestPath()).thenReturn(requestPath);
        when(headers.header("Authorization")).thenReturn(Collections.singletonList("Bearer test-token"));

        // 模拟访问令牌
        AccessTokenDto accessTokenDto = new AccessTokenDto();
        accessTokenDto.setId("token-id");
        accessTokenDto.setUserId("user-id");
        when(accessTokenService.generateToken("test-token")).thenReturn(accessTokenDto);

        // 模拟用户信息
        UserDto userDto = new UserDto();
        List<RoleMappingDto> roleMappings = new ArrayList<>();
        RoleMappingDto roleMapping = new RoleMappingDto();
        RoleDto role = new RoleDto();
        role.setName("admin");
        roleMapping.setRole(role);
        roleMappings.add(roleMapping);
        userDto.setRoleMappings(roleMappings);
        when(userService.getUserDetail("user-id")).thenReturn(userDto);

        // 模拟处理函数返回成功响应
        ServerResponse successResponse = ServerResponse.ok().build();
        when(handlerFunction.handle(any())).thenReturn(successResponse);

        // 执行过滤器
        ServerResponse response = mcpConfig.authFilter(serverRequest, handlerFunction);

        // 验证结果
        assertEquals(HttpStatus.OK, response.statusCode());
        assertEquals("token-id", session.getAttribute(McpConfig.TOKEN));
        assertEquals("user-id", session.getAttribute(McpConfig.USER_ID));
    }

    @Test
    void testAuthFilter_SseEndpoint_NoAccessCode() throws Exception {
        session = new MockHttpSession();
        when(serverRequest.headers()).thenReturn(headers);

        // 模拟 SSE 端点请求，但没有访问码
        when(requestPath.value()).thenReturn(mcpConfig.sseEndpoint);
        when(serverRequest.requestPath()).thenReturn(requestPath);
        when(headers.header("Authorization")).thenReturn(Collections.emptyList());
        when(serverRequest.param("accessCode")).thenReturn(java.util.Optional.empty());

        // 执行过滤器
        ServerResponse response = mcpConfig.authFilter(serverRequest, handlerFunction);

        // 验证结果
        assertEquals(HttpStatus.UNAUTHORIZED, response.statusCode());
    }

    @Test
    void testAuthFilter_SseEndpoint_InvalidToken() throws Exception {
        session = new MockHttpSession();
        when(serverRequest.headers()).thenReturn(headers);

        when(requestPath.value()).thenReturn(mcpConfig.sseEndpoint);
        // 模拟 SSE 端点请求，但令牌无效
        when(serverRequest.requestPath()).thenReturn(requestPath);
        when(headers.header("Authorization")).thenReturn(Collections.singletonList("Bearer invalid-token"));
        when(accessTokenService.generateToken("invalid-token")).thenReturn(null);

        // 执行过滤器
        ServerResponse response = mcpConfig.authFilter(serverRequest, handlerFunction);

        // 验证结果
        assertEquals(HttpStatus.UNAUTHORIZED, response.statusCode());
    }

    @Test
    void testAuthFilter_SseEndpoint_NoMcpRole() throws Exception {
        session = new MockHttpSession();
        when(serverRequest.headers()).thenReturn(headers);

        when(requestPath.value()).thenReturn(mcpConfig.sseEndpoint);
        // 模拟 SSE 端点请求
        when(serverRequest.requestPath()).thenReturn(requestPath);
        when(headers.header("Authorization")).thenReturn(Collections.singletonList("Bearer test-token"));

        // 模拟访问令牌
        AccessTokenDto accessTokenDto = new AccessTokenDto();
        accessTokenDto.setId("token-id");
        accessTokenDto.setUserId("user-id");
        when(accessTokenService.generateToken("test-token")).thenReturn(accessTokenDto);

        // 模拟用户信息，但没有 MCP 角色
        UserDto userDto = new UserDto();
        List<RoleMappingDto> roleMappings = new ArrayList<>();
        RoleMappingDto roleMapping = new RoleMappingDto();
        RoleDto role = new RoleDto();
        role.setName("user");
        roleMapping.setRole(role);
        roleMappings.add(roleMapping);
        userDto.setRoleMappings(roleMappings);
        when(userService.getUserDetail("user-id")).thenReturn(userDto);

        // 执行过滤器
        ServerResponse response = mcpConfig.authFilter(serverRequest, handlerFunction);

        // 验证结果
        assertEquals(HttpStatus.UNAUTHORIZED, response.statusCode());
    }

    @Test
    void testAuthFilter_NonSseEndpoint() throws Exception {
        // 模拟非 SSE 端点请求
        when(serverRequest.requestPath()).thenReturn(requestPath);
        when(serverRequest.methodName()).thenReturn("GET");
        when(serverRequest.uri()).thenReturn(new java.net.URI("http://test.com/other/endpoint"));

        // 模拟处理函数返回成功响应
        ServerResponse successResponse = ServerResponse.ok().build();
        when(handlerFunction.handle(any())).thenReturn(successResponse);

        // 执行过滤器
        ServerResponse response = mcpConfig.authFilter(serverRequest, handlerFunction);

        // 验证结果
        assertEquals(HttpStatus.OK, response.statusCode());
    }

    @Test
    void testAuthFilter_HandlerException() throws Exception {
        // 模拟非 SSE 端点请求
        when(serverRequest.requestPath()).thenReturn(requestPath);
        when(serverRequest.methodName()).thenReturn("GET");
        when(serverRequest.uri()).thenReturn(new java.net.URI("http://test.com/other/endpoint"));

        // 模拟处理函数抛出异常
        when(handlerFunction.handle(any())).thenThrow(new RuntimeException("Test exception"));

        // 执行过滤器并验证异常
        assertThrows(RuntimeException.class, () -> mcpConfig.authFilter(serverRequest, handlerFunction));
    }
} 