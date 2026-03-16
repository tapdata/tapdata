package com.tapdata.tm.oauth2.filter;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.config.security.JsonToFormRequestWrapper;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OAuth2JsonSupportFilterTest {
    OAuth2JsonSupportFilter filter;
    ObjectMapper objectMapper;
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        filter = mock(OAuth2JsonSupportFilter.class);
        ReflectionTestUtils.setField(filter, "objectMapper", objectMapper);
    }

    @Nested
    class readBodyTest {
        @BeforeEach
        void init() throws IOException {
            when(filter.readBody(any(HttpServletRequest.class))).thenCallRealMethod();
        }

        @Test
        void testJsonToFormRequestWrapper() {
            JsonToFormRequestWrapper request = mock(JsonToFormRequestWrapper.class);
            when(request.originBody()).thenReturn(new HashMap<>());
            Assertions.assertDoesNotThrow(() -> filter.readBody(request));
        }
        @Test
        void testFormRequestWrapper() throws IOException {
            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getInputStream()).thenReturn(null);
            Assertions.assertThrows(JsonMappingException.class, () -> filter.readBody(request));
        }
    }
}