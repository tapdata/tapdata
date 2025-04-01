package com.tapdata.tm.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.function.*;

import java.util.Map;

import static com.tapdata.tm.mcp.Utils.getAccessCode;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/24 18:55
 */
@Configuration
@Slf4j
public class McpConfig {

    public static final String USER_ID = "userId";
    public static final String TOKEN = "token";

    public final String sseEndpoint = "/mcp/sse";
    private AccessTokenService accessTokenService;


    @Bean
    public SseServerTransportProvider webMvcSseServerTransportProvider(ObjectMapper mapper) {
        return new SseServerTransportProvider(mapper, "/mcp/message", sseEndpoint);
    }

    @Bean
    public RouterFunction<ServerResponse> mcpRouterFunction(SseServerTransportProvider transportProvider, AccessTokenService accessTokenService) {
        this.accessTokenService = accessTokenService;
        RouterFunction<ServerResponse> router = transportProvider.getRouterFunction();
        return router.filter(this::authFilter);
    }

    public ServerResponse authFilter(ServerRequest request, HandlerFunction<ServerResponse> next) throws Exception {

        if (sseEndpoint.equals(request.requestPath().value())) {
            // auth request
            String accessCode = getAccessCode(request);
            if (!StringUtils.hasText(accessCode))
                return ServerResponse.status(HttpStatus.UNAUTHORIZED).build();

            AccessTokenDto accessTokenDto = accessTokenService.generateToken(accessCode);
            if (accessTokenDto == null)
                return ServerResponse.status(HttpStatus.UNAUTHORIZED).build();

            request.session().setAttribute(TOKEN, accessTokenDto.getId());
            request.session().setAttribute(USER_ID, accessTokenDto.getUserId());
        }

        try {
            log.info("{} {}", request.methodName(), request.uri());
            return next.handle(request);
        } catch (Exception e) {
            log.error("{} {}", request.methodName(), request.uri(), e);
            throw e;
        }
    }
}
