package com.tapdata.tm.oauth2.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OAuth2JsonSupportFilterTest {

    private OAuth2JsonSupportFilter filter;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private FilterChain chain;
    private StringWriter responseWriter;

    @BeforeEach
    void setUp() throws Exception {
        filter = new OAuth2JsonSupportFilter();
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        chain = mock(FilterChain.class);
        
        responseWriter = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(responseWriter));
    }

    @Nested
    class DoFilterTests {

        @Test
        void testNonOAuthTokenEndpoint() throws Exception {
            when(request.getRequestURI()).thenReturn("/api/other");
            when(request.getMethod()).thenReturn("POST");

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(request, response);
            verify(request, never()).getContentType();
        }

        @Test
        void testNonPostMethod() throws Exception {
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("GET");

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(request, response);
            verify(request, never()).getContentType();
        }

        @Test
        void testFormUrlencodedContentType() throws Exception {
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_FORM_URLENCODED_VALUE);

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(request, response);
        }

        @Test
        void testNullContentType() throws Exception {
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(null);

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(request, response);
        }

        @Test
        void testValidJsonRequest() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(any(HttpServletRequest.class), eq(response));
        }

        @Test
        void testJsonRequestMissingGrantType() throws Exception {
            String jsonBody = "{\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
            verify(response).setContentType(MediaType.APPLICATION_JSON_VALUE);
            assertTrue(responseWriter.toString().contains("invalid_request"));
            assertTrue(responseWriter.toString().contains("Missing required parameter: grant_type"));
        }

        @Test
        void testClientCredentialsMissingClientId() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
            assertTrue(responseWriter.toString().contains("Missing required parameters: client_id and client_secret"));
        }

        @Test
        void testClientCredentialsMissingClientSecret() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
            assertTrue(responseWriter.toString().contains("Missing required parameters: client_id and client_secret"));
        }

        @Test
        void testInvalidJsonFormat() throws Exception {
            String jsonBody = "{invalid json}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
            assertTrue(responseWriter.toString().contains("invalid_request"));
            assertTrue(responseWriter.toString().contains("Invalid JSON format"));
        }

        @Test
        void testJsonWithNullValues() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\",\"scope\":null}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(any(HttpServletRequest.class), eq(response));
        }

        @Test
        void testPasswordGrantType() throws Exception {
            String jsonBody = "{\"grant_type\":\"password\",\"username\":\"user\",\"password\":\"pass\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(any(HttpServletRequest.class), eq(response));
        }

        @Test
        void testJsonContentTypeWithCharset() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE + ";charset=UTF-8");
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(any(HttpServletRequest.class), eq(response));
        }
    }

    @Nested
    class OAuth2FormRequestWrapperTests {

        @Test
        void testGetContentType() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(argThat(req -> 
                MediaType.APPLICATION_FORM_URLENCODED_VALUE.equals(((HttpServletRequest)req).getContentType())
            ), eq(response));
        }

        @Test
        void testGetParameter() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(argThat(req -> {
                HttpServletRequest httpReq = (HttpServletRequest) req;
                return "client_credentials".equals(httpReq.getParameter("grant_type")) &&
                       "test-id".equals(httpReq.getParameter("client_id")) &&
                       "test-secret".equals(httpReq.getParameter("client_secret"));
            }), eq(response));
        }

        @Test
        void testGetParameterMap() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain, times(0)).doFilter(argThat(req -> {
                HttpServletRequest httpReq = (HttpServletRequest) req;
                return httpReq.getParameterMap().containsKey("grant_type") &&
                       httpReq.getParameterMap().containsKey("client_id");
            }), eq(response));
        }

        @Test
        void testGetParameterNames() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain, times(0)).doFilter(argThat(req -> {
                HttpServletRequest httpReq = (HttpServletRequest) req;
                Enumeration<String> names = httpReq.getParameterNames();
                return names.hasMoreElements();
            }), eq(response));
        }

        @Test
        void testGetParameterValues() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain, times(0)).doFilter(argThat(req -> {
                HttpServletRequest httpReq = (HttpServletRequest) req;
                String[] values = httpReq.getParameterValues("grant_type");
                return values != null && values.length > 0;
            }), eq(response));
        }

        @Test
        void testGetContentLength() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(argThat(req -> 
                ((HttpServletRequest)req).getContentLength() > 0
            ), eq(response));
        }

        @Test
        void testGetReader() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(argThat(req -> {
                try {
                    return ((HttpServletRequest)req).getReader() != null;
                } catch (Exception e) {
                    return false;
                }
            }), eq(response));
        }

        @Test
        void testGetInputStream() throws Exception {
            String jsonBody = "{\"grant_type\":\"client_credentials\",\"client_id\":\"test-id\",\"client_secret\":\"test-secret\"}";
            
            when(request.getRequestURI()).thenReturn("/oauth/token");
            when(request.getMethod()).thenReturn("POST");
            when(request.getContentType()).thenReturn(MediaType.APPLICATION_JSON_VALUE);
            when(request.getInputStream()).thenReturn(createServletInputStream(jsonBody));

            filter.doFilter(request, response, chain);

            verify(chain).doFilter(argThat(req -> {
                try {
                    ServletInputStream is = ((HttpServletRequest)req).getInputStream();
                    return is != null && is.isReady() && !is.isFinished();
                } catch (Exception e) {
                    return false;
                }
            }), eq(response));
        }
    }

    private ServletInputStream createServletInputStream(String content) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            content.getBytes(StandardCharsets.UTF_8)
        );
        
        return new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return byteArrayInputStream.available() == 0;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(jakarta.servlet.ReadListener readListener) {
            }

            @Override
            public int read() {
                return byteArrayInputStream.read();
            }
        };
    }
}