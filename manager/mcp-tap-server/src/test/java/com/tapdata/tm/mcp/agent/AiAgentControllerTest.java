package com.tapdata.tm.mcp.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.config.security.UserDetail;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AiAgentControllerTest {

    @Test
    void streamResponseDisablesProxyBuffering() {
        ObjectMapper objectMapper = new ObjectMapper();
        AiAgentService service = new AiAgentService(
                config -> {
                    throw new UnsupportedOperationException("stream body is not executed by this test");
                },
                AnnotatedMcpToolCallbackProvider.fromToolBeans(List.of(), objectMapper),
                objectMapper);
        AiAgentController controller = new AiAgentController(service) {
            @Override
            public UserDetail getLoginUser() {
                return null;
            }
        };

        ResponseEntity<StreamingResponseBody> response = controller.stream(new AiChatRequest(), "Bearer tap-token",
                new MockHttpServletResponse());

        assertEquals(MediaType.TEXT_EVENT_STREAM, response.getHeaders().getContentType());
        assertEquals("no-cache, no-transform", response.getHeaders().getFirst(HttpHeaders.CACHE_CONTROL));
        assertEquals("no", response.getHeaders().getFirst("X-Accel-Buffering"));
    }

    @Test
    void streamBodyFlushesServletResponseBuffer() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        AiAgentService service = new AiAgentService(
                config -> org.springframework.ai.chat.client.ChatClient.create(new AiAgentServiceTest.FakeChatModel()),
                AnnotatedMcpToolCallbackProvider.fromToolBeans(List.of(), objectMapper),
                objectMapper);
        AiAgentController controller = new AiAgentController(service) {
            @Override
            public UserDetail getLoginUser() {
                return null;
            }
        };
        CountingHttpServletResponse servletResponse = new CountingHttpServletResponse();

        ResponseEntity<StreamingResponseBody> response = controller.stream(validRequest(), "Bearer tap-token",
                servletResponse);
        response.getBody().writeTo(new ByteArrayOutputStream());

        assertTrue(servletResponse.flushBufferCalls > 0);
    }

    private AiChatRequest validRequest() {
        AiChatRequest request = new AiChatRequest();
        LlmConfig llm = new LlmConfig();
        llm.setBaseUrl("https://api.openai.com/v1");
        llm.setApiKey("sk-test");
        llm.setModel("gpt-4.1");
        request.setLlm(llm);
        request.setMessages(List.of(new ChatMessage("user", "hello")));
        return request;
    }

    static class CountingHttpServletResponse extends MockHttpServletResponse {
        private int flushBufferCalls;

        @Override
        public void flushBuffer() {
            flushBufferCalls++;
            super.flushBuffer();
        }
    }
}
