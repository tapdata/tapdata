package com.tapdata.tm.mcp;

import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.service.UserLogService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.server.common.autoconfigure.properties.McpServerStreamableHttpProperties;
import org.springframework.ai.mcp.server.webmvc.transport.WebMvcStreamableServerTransportProvider;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    private final McpConfig mcpConfig = new McpConfig();

    @Test
    void testWebMvcStreamableServerTransportProvider() {
        McpServerStreamableHttpProperties properties = new McpServerStreamableHttpProperties();
        WebMvcStreamableServerTransportProvider provider =
                mcpConfig.webMvcStreamableServerTransportProvider(JsonMapper.builder().build(), properties,
                        new McpSessionAttributes());

        assertNotNull(provider);
    }

    @Test
    void testWebMvcStreamableServerRouterFunction() {
        McpSessionAttributes sessionAttributes = new McpSessionAttributes();
        WebMvcStreamableServerTransportProvider transportProvider = WebMvcStreamableServerTransportProvider.builder()
                .mcpEndpoint("/mcp")
                .contextExtractor(sessionAttributes::extractContext)
                .build();
        McpAccessCodeAuthentication accessCodeAuthentication = new McpAccessCodeAuthentication(accessTokenService,
                userService, userLogService, sessionAttributes);

        RouterFunction<ServerResponse> routerFunction =
                mcpConfig.webMvcStreamableServerRouterFunction(transportProvider, accessCodeAuthentication);

        assertNotNull(routerFunction);
    }
}
