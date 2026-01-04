package com.tapdata.tm.config.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.Map;

public class JsonToFormUrlEncodedFilter extends OncePerRequestFilter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    @NotNull HttpServletResponse response,
                                    @NotNull FilterChain filterChain)
            throws ServletException, IOException {

        if ("/oauth/token".equals(request.getRequestURI())
                && request.getContentType() != null
                && request.getContentType().contains(MediaType.APPLICATION_JSON_VALUE)) {

            Map<String, String> jsonBody =
                    objectMapper.readValue(request.getInputStream(), new TypeReference<>() {});

            HttpServletRequest wrappedRequest =
                    new JsonToFormRequestWrapper(request, jsonBody);

            filterChain.doFilter(wrappedRequest, response);
            return;
        }

        filterChain.doFilter(request, response);
    }
}
