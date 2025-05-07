package com.tapdata.tm.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.service.UserLogService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.function.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.tapdata.tm.mcp.Utils.getAccessCode;
import static org.springframework.core.Ordered.HIGHEST_PRECEDENCE;

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
    public final String messageEndpoint = "/mcp/message";
    private AccessTokenService accessTokenService;
    private UserService userService;


    @Bean
    public SseServerTransportProvider webMvcSseServerTransportProvider(ObjectMapper mapper, UserLogService userLogService) {
        return new SseServerTransportProvider(mapper, messageEndpoint, sseEndpoint, userLogService);
    }

    @Bean
    public RouterFunction<ServerResponse> mcpRouterFunction(SseServerTransportProvider transportProvider,
                                                            AccessTokenService accessTokenService,
                                                            UserService userService) {
        this.accessTokenService = accessTokenService;
        this.userService = userService;
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
                return ServerResponse.status(HttpStatus.UNAUTHORIZED).body("Not found user.");

            UserDto userDetail = userService.getUserDetail(accessTokenDto.getUserId());
            List<String> allowedRoles = Arrays.asList("mcp", "admin");
            Optional<RoleMappingDto> hasMcpRole = userDetail.getRoleMappings()
                    .stream()
                    .filter(r -> r.getRole() != null && r.getRole().getName() != null)
                    .filter(r -> allowedRoles.contains(r.getRole().getName().toLowerCase())).findFirst();

            if (!hasMcpRole.isPresent())
                return ServerResponse.status(HttpStatus.UNAUTHORIZED).body("Not granted the mcp role");

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
