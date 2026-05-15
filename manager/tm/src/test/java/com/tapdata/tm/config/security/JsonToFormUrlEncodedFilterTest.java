package com.tapdata.tm.config.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * JsonToFormUrlEncodedFilter 测试类
 * 
 * @author test
 */
@ExtendWith(MockitoExtension.class)
class JsonToFormUrlEncodedFilterTest {

    private JsonToFormUrlEncodedFilter filter;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain filterChain;

    @BeforeEach
    void setUp() {
        filter = new JsonToFormUrlEncodedFilter();
    }

    @Nested
    @DisplayName("doFilterInternal 方法测试 - OAuth Token 端点")
    class OAuthTokenEndpointTest {

        @Test
        @DisplayName("当请求是 /oauth/token 且 Content-Type 是 application/json 时，应该转换请求")
        void shouldConvertJsonToFormWhenOAuthTokenWithJsonContentType() throws ServletException, IOException {
            // 准备
            String jsonBody = "{\"grant_type\":\"password\",\"username\":\"test\",\"password\":\"123456\"}";
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(
                new TestServletInputStream(jsonBody.getBytes(StandardCharsets.UTF_8))
            );

            ArgumentCaptor<HttpServletRequest> requestCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);

            // 执行
            filter.doFilterInternal(request, response, filterChain);

            // 验证
            verify(filterChain).doFilter(requestCaptor.capture(), eq(response));
            HttpServletRequest capturedRequest = requestCaptor.getValue();
            
            assertInstanceOf(JsonToFormRequestWrapper.class, capturedRequest, 
                "应该使用 JsonToFormRequestWrapper 包装请求");
            assertEquals("password", capturedRequest.getParameter("grant_type"));
            assertEquals("test", capturedRequest.getParameter("username"));
            assertEquals("123456", capturedRequest.getParameter("password"));
        }

        @Test
        @DisplayName("当请求是 /oauth/token 且 Content-Type 包含 charset 时，应该转换请求")
        void shouldConvertWhenContentTypeHasCharset() throws ServletException, IOException {
            // 准备
            String jsonBody = "{\"client_id\":\"test-client\",\"client_secret\":\"secret\"}";
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn("application/json;charset=UTF-8");
            when(request.getInputStream()).thenReturn(
                new TestServletInputStream(jsonBody.getBytes(StandardCharsets.UTF_8))
            );

            ArgumentCaptor<HttpServletRequest> requestCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);

            // 执行
            filter.doFilterInternal(request, response, filterChain);

            // 验证
            verify(filterChain).doFilter(requestCaptor.capture(), eq(response));
            HttpServletRequest capturedRequest = requestCaptor.getValue();
            
            assertEquals("test-client", capturedRequest.getParameter("client_id"));
            assertEquals("secret", capturedRequest.getParameter("client_secret"));
        }

        @Test
        @DisplayName("当 JSON 包含空值时，应该正确处理")
        void shouldHandleNullValuesInJson() throws ServletException, IOException {
            // 准备
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"scope\":null}";
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(
                new TestServletInputStream(jsonBody.getBytes(StandardCharsets.UTF_8))
            );

            // 执行
            filter.doFilterInternal(request, response, filterChain);

            // 验证
            verify(filterChain).doFilter(any(JsonToFormRequestWrapper.class), eq(response));
        }

        @Test
        @DisplayName("当 JSON 为空对象时，应该正确处理")
        void shouldHandleEmptyJsonObject() throws ServletException, IOException {
            // 准备
            String jsonBody = "{}";
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(
                new TestServletInputStream(jsonBody.getBytes(StandardCharsets.UTF_8))
            );

            // 执行
            filter.doFilterInternal(request, response, filterChain);

            // 验证
            verify(filterChain).doFilter(any(JsonToFormRequestWrapper.class), eq(response));
        }
    }

    @Nested
    @DisplayName("异常处理测试")
    class ExceptionHandlingTest {

        @Test
        @DisplayName("当 JSON 解析失败时，应该抛出异常")
        void shouldThrowExceptionWhenJsonParsingFails() throws IOException {
            // 准备
            String invalidJson = "{invalid json}";
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(
                new TestServletInputStream(invalidJson.getBytes(StandardCharsets.UTF_8))
            );

            // 执行 & 验证
            assertThrows(Exception.class, () -> {
                filter.doFilterInternal(request, response, filterChain);
            });
        }

        @Test
        @DisplayName("当读取输入流失败时，应该抛出异常")
        void shouldThrowExceptionWhenInputStreamFails() throws IOException {
            // 准备
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenThrow(new IOException("Stream error"));

            // 执行 & 验证
            assertThrows(IOException.class, () -> {
                filter.doFilterInternal(request, response, filterChain);
            });
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class EdgeCaseTest {

        @Test
        @DisplayName("当 JSON 包含特殊字符时，应该正确处理")
        void shouldHandleSpecialCharactersInJson() throws ServletException, IOException {
            // 准备
            String jsonBody = "{\"username\":\"test@example.com\",\"password\":\"p@ss!w0rd#123\"}";
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(
                new TestServletInputStream(jsonBody.getBytes(StandardCharsets.UTF_8))
            );

            ArgumentCaptor<HttpServletRequest> requestCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);

            // 执行
            filter.doFilterInternal(request, response, filterChain);

            // 验证
            verify(filterChain).doFilter(requestCaptor.capture(), eq(response));
            HttpServletRequest capturedRequest = requestCaptor.getValue();
            
            assertEquals("test@example.com", capturedRequest.getParameter("username"));
            assertEquals("p@ss!w0rd#123", capturedRequest.getParameter("password"));
        }

        @Test
        @DisplayName("当 JSON 包含中文字符时，应该正确处理")
        void shouldHandleChineseCharactersInJson() throws ServletException, IOException {
            // 准备
            String jsonBody = "{\"username\":\"测试用户\",\"description\":\"这是一个测试\"}";
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(
                new TestServletInputStream(jsonBody.getBytes(StandardCharsets.UTF_8))
            );

            ArgumentCaptor<HttpServletRequest> requestCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);

            // 执行
            filter.doFilterInternal(request, response, filterChain);

            // 验证
            verify(filterChain).doFilter(requestCaptor.capture(), eq(response));
            HttpServletRequest capturedRequest = requestCaptor.getValue();
            
            assertEquals("测试用户", capturedRequest.getParameter("username"));
            assertEquals("这是一个测试", capturedRequest.getParameter("description"));
        }
    }

    /**
     * 测试用的 ServletInputStream 实现
     */
    private static class TestServletInputStream extends jakarta.servlet.ServletInputStream {
        private final ByteArrayInputStream inputStream;

        public TestServletInputStream(byte[] data) {
            this.inputStream = new ByteArrayInputStream(data);
        }

        @Override
        public int read() {
            return inputStream.read();
        }

        @Override
        public boolean isFinished() {
            return inputStream.available() == 0;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(jakarta.servlet.ReadListener readListener) {
            // Not implemented for testing
        }
    }
}

