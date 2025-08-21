package com.tapdata.tm.oauth2.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StreamUtils;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * OAuth2 JSON支持过滤器
 * 拦截/oauth/token的JSON请求并转换为form-urlencoded格式
 */
@Slf4j
public class OAuth2JsonSupportFilter implements Filter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) 
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        // 只处理/oauth/token端点的POST请求
        if (!"/oauth/token".equals(httpRequest.getRequestURI()) || 
            !"POST".equalsIgnoreCase(httpRequest.getMethod())) {
            chain.doFilter(request, response);
            return;
        }
        
        String contentType = httpRequest.getContentType();
        log.debug("OAuth2 token request with Content-Type: {}", contentType);
        
        // 如果是application/json，转换为form-urlencoded
        if (contentType != null && contentType.startsWith(MediaType.APPLICATION_JSON_VALUE)) {
            log.debug("Converting JSON request to form-urlencoded format");
            
            try {
                // 读取原始请求体
                byte[] body = StreamUtils.copyToByteArray(httpRequest.getInputStream());
                String jsonBody = new String(body, StandardCharsets.UTF_8);
                
                // 解析JSON
                Map<String, Object> jsonMap = objectMapper.readValue(jsonBody, Map.class);
                log.debug("Parsed JSON request: {}", jsonMap);
                
                // 验证必需参数
                if (!jsonMap.containsKey("grant_type")) {
                    sendErrorResponse(httpResponse, "invalid_request", "Missing required parameter: grant_type");
                    return;
                }
                
                String grantType = (String) jsonMap.get("grant_type");
                if ("client_credentials".equals(grantType)) {
                    if (!jsonMap.containsKey("client_id") || !jsonMap.containsKey("client_secret")) {
                        sendErrorResponse(httpResponse, "invalid_request", 
                                        "Missing required parameters: client_id and client_secret");
                        return;
                    }
                }
                
                // 转换为form参数
                MultiValueMap<String, String> formParams = new LinkedMultiValueMap<>();
                for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
                    if (entry.getValue() != null) {
                        formParams.add(entry.getKey(), entry.getValue().toString());
                    }
                }
                
                // 创建包装的请求
                HttpServletRequestWrapper wrappedRequest = new OAuth2FormRequestWrapper(httpRequest, formParams);
                chain.doFilter(wrappedRequest, response);
                
            } catch (Exception e) {
                log.error("Error processing JSON OAuth2 request", e);
                sendErrorResponse(httpResponse, "invalid_request", "Invalid JSON format: " + e.getMessage());
            }
        } else {
            // 对于form-urlencoded或其他格式，直接传递
            chain.doFilter(request, response);
        }
    }
    
    /**
     * 发送错误响应
     */
    private void sendErrorResponse(HttpServletResponse response, String error, String description) throws IOException {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        
        String errorJson = String.format("{\"error\":\"%s\",\"error_description\":\"%s\"}", error, description);
        response.getWriter().write(errorJson);
        response.getWriter().flush();
    }

    /**
     * 内部类：包装HttpServletRequest以支持form参数
     */
    private static class OAuth2FormRequestWrapper extends HttpServletRequestWrapper {
        
        private final MultiValueMap<String, String> formParams;
        private final String formBody;
        
        public OAuth2FormRequestWrapper(HttpServletRequest request, MultiValueMap<String, String> formParams) {
            super(request);
            this.formParams = formParams;
            this.formBody = buildFormBody(formParams);
        }
        
        private String buildFormBody(MultiValueMap<String, String> params) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            
            for (Map.Entry<String, List<String>> entry : params.entrySet()) {
                String key = entry.getKey();
                for (String value : entry.getValue()) {
                    if (!first) {
                        sb.append("&");
                    }
                    sb.append(key).append("=").append(value);
                    first = false;
                }
            }
            
            return sb.toString();
        }
        
        @Override
        public String getContentType() {
            return MediaType.APPLICATION_FORM_URLENCODED_VALUE;
        }
        
        @Override
        public int getContentLength() {
            return formBody.getBytes(StandardCharsets.UTF_8).length;
        }
        
        @Override
        public long getContentLengthLong() {
            return formBody.getBytes(StandardCharsets.UTF_8).length;
        }
        
        @Override
        public String getParameter(String name) {
            List<String> values = formParams.get(name);
            return (values != null && !values.isEmpty()) ? values.get(0) : null;
        }
        
        @Override
        public Map<String, String[]> getParameterMap() {
            Map<String, String[]> paramMap = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : formParams.entrySet()) {
                paramMap.put(entry.getKey(), entry.getValue().toArray(new String[0]));
            }
            return paramMap;
        }
        
        @Override
        public Enumeration<String> getParameterNames() {
            return Collections.enumeration(formParams.keySet());
        }
        
        @Override
        public String[] getParameterValues(String name) {
            List<String> values = formParams.get(name);
            return (values != null) ? values.toArray(new String[0]) : null;
        }
        
        @Override
        public BufferedReader getReader() throws IOException {
            return new BufferedReader(new InputStreamReader(
                    new ByteArrayInputStream(formBody.getBytes(StandardCharsets.UTF_8)), 
                    StandardCharsets.UTF_8));
        }
        
        @Override
        public ServletInputStream getInputStream() throws IOException {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(formBody.getBytes(StandardCharsets.UTF_8));
            
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
                public void setReadListener(ReadListener readListener) {
                    // Not implemented for this use case
                }

                @Override
                public int read() throws IOException {
                    return byteArrayInputStream.read();
                }
            };
        }
    }
}
