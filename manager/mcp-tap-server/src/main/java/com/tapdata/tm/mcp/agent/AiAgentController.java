package com.tapdata.tm.mcp.agent;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.config.security.UserDetail;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.UUID;

@RestController
@RequestMapping("/api/ai-agent")
public class AiAgentController extends BaseController {

    private final AiAgentService aiAgentService;

    public AiAgentController(AiAgentService aiAgentService) {
        this.aiAgentService = aiAgentService;
    }

    @PostMapping(value = "/chat/stream", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> stream(@RequestBody AiChatRequest request,
                                                        @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false)
                                                        String authorization,
                                                        HttpServletResponse response) {
        UserDetail user = getLoginUser();
        AgentRequestContext context = new AgentRequestContext("ai-agent-" + UUID.randomUUID(),
                user == null ? null : user.getUserId(), extractToken(authorization));
        StreamingResponseBody body = outputStream -> aiAgentService.stream(request, context, outputStream,
                response::flushBuffer);
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .header(HttpHeaders.CACHE_CONTROL, "no-cache, no-transform")
                .header("X-Accel-Buffering", "no")
                .body(body);
    }

    private String extractToken(String authorization) {
        if (StringUtils.isBlank(authorization)) {
            return null;
        }
        String value = authorization.trim();
        if (StringUtils.startsWithIgnoreCase(value, "Bearer ")) {
            return value.substring("Bearer ".length()).trim();
        }
        return value;
    }
}
