package com.tapdata.tm.mcp;

import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator;
import org.springframework.ai.mcp.server.common.autoconfigure.properties.McpServerStreamableHttpProperties;
import org.springframework.ai.mcp.server.webmvc.transport.WebMvcStreamableServerTransportProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;
import tools.jackson.databind.json.JsonMapper;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/24 18:55
 */
@Configuration
@EnableConfigurationProperties(McpServerStreamableHttpProperties.class)
public class McpConfig {

    public static final String USER_ID = "userId";
    public static final String TOKEN = "token";

    @Bean
    @Primary
    public WebMvcStreamableServerTransportProvider webMvcStreamableServerTransportProvider(
            @Qualifier("mcpServerJsonMapper") JsonMapper jsonMapper,
            McpServerStreamableHttpProperties serverProperties,
            McpSessionAttributes sessionAttributes) {
        return WebMvcStreamableServerTransportProvider.builder()
                .jsonMapper(new JacksonMcpJsonMapper(jsonMapper))
                .mcpEndpoint(serverProperties.getMcpEndpoint())
                .keepAliveInterval(serverProperties.getKeepAliveInterval())
                .disallowDelete(serverProperties.isDisallowDelete())
                .contextExtractor(sessionAttributes::extractContext)
                .securityValidator(ServerTransportSecurityValidator.NOOP)
                .build();
    }

    @Bean
    @Primary
    public RouterFunction<ServerResponse> webMvcStreamableServerRouterFunction(
            WebMvcStreamableServerTransportProvider transportProvider,
            McpAccessCodeAuthentication accessCodeAuthentication) {
        return transportProvider.getRouterFunction().filter(accessCodeAuthentication::filter);
    }
}
