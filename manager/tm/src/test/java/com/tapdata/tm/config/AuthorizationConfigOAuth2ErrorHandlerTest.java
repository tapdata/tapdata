package com.tapdata.tm.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AuthorizationConfigOAuth2ErrorHandlerTest {

    private AuthenticationFailureHandler newHandler() throws Exception {
        Class<?> handlerClass = Class.forName("com.tapdata.tm.config.AuthorizationConfig$LoggingOAuth2ErrorResponseHandler");
        var ctor = handlerClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        return (AuthenticationFailureHandler) ctor.newInstance();
    }

    @Test
    void oauth2Error_addsHttpStatusCodeField_writerDelegate() throws Exception {
        AuthenticationFailureHandler handler = newHandler();

        AuthenticationFailureHandler delegate = (HttpServletRequest req, HttpServletResponse res, AuthenticationException ex) -> {
            res.setStatus(418);
            res.setContentType("application/json");
            res.getWriter().write("{\"error\":\"unsupported_grant_type\",\"error_description\":\"bad grant\"}");
        };
        ReflectionTestUtils.setField(handler, "delegate", delegate);

        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/oauth/token");
        MockHttpServletResponse response = new MockHttpServletResponse();

        OAuth2AuthenticationException exception =
                new OAuth2AuthenticationException(new OAuth2Error("unsupported_grant_type", "bad grant", null));

        handler.onAuthenticationFailure(request, response, exception);

        String body = response.getContentAsString(StandardCharsets.UTF_8);
        assertTrue(body.contains("\"error\":\"unsupported_grant_type\""));
        assertTrue(body.contains("\"error_description\":\"bad grant\""));
        assertTrue(body.contains("\"code\":418"));
        assertEquals(418, response.getStatus());
    }

    @Test
    void oauth2Error_addsHttpStatusCodeField_outputStreamDelegate() throws Exception {
        AuthenticationFailureHandler handler = newHandler();

        AuthenticationFailureHandler delegate = (HttpServletRequest req, HttpServletResponse res, AuthenticationException ex) -> {
            res.setStatus(400);
            res.setContentType("application/json");
            res.getOutputStream().write("{\"error\":\"invalid_request\"}".getBytes(StandardCharsets.UTF_8));
        };
        ReflectionTestUtils.setField(handler, "delegate", delegate);

        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/oauth/token");
        MockHttpServletResponse response = new MockHttpServletResponse();

        OAuth2AuthenticationException exception =
                new OAuth2AuthenticationException(new OAuth2Error("invalid_request", "missing grant_type", null));

        handler.onAuthenticationFailure(request, response, exception);

        String body = response.getContentAsString(StandardCharsets.UTF_8);
        assertTrue(body.contains("\"error\":\"invalid_request\""));
        assertTrue(body.contains("\"code\":400"));
        assertEquals(400, response.getStatus());
    }

    @Test
    void oauth2ExceptionWithNullError_doesNotFail_andAddsCode() throws Exception {
        AuthenticationFailureHandler handler = newHandler();

        AuthenticationFailureHandler delegate = (HttpServletRequest req, HttpServletResponse res, AuthenticationException ex) -> {
            res.setStatus(401);
            res.setContentType("application/json");
            res.getWriter().write("{\"error\":\"invalid_client\"}");
        };
        ReflectionTestUtils.setField(handler, "delegate", delegate);

        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/oauth/token");
        MockHttpServletResponse response = new MockHttpServletResponse();

        OAuth2AuthenticationException exception = mock(OAuth2AuthenticationException.class);
        when(exception.getError()).thenReturn(null);
        when(exception.getMessage()).thenReturn("no error object");

        handler.onAuthenticationFailure(request, response, exception);

        String body = response.getContentAsString(StandardCharsets.UTF_8);
        assertTrue(body.contains("\"error\":\"invalid_client\""));
        assertTrue(body.contains("\"code\":401"));
        assertEquals(401, response.getStatus());
    }

    @Test
    void nonOAuth2AuthenticationException_pathIsCovered_andAddsCode() throws Exception {
        AuthenticationFailureHandler handler = newHandler();

        AuthenticationFailureHandler delegate = (HttpServletRequest req, HttpServletResponse res, AuthenticationException ex) -> {
            res.setStatus(403);
            res.setContentType("application/json");
            res.getWriter().write("{\"error\":\"access_denied\"}");
        };
        ReflectionTestUtils.setField(handler, "delegate", delegate);

        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/oauth/token");
        MockHttpServletResponse response = new MockHttpServletResponse();

        AuthenticationException exception = new AuthenticationException("denied") {};
        handler.onAuthenticationFailure(request, response, exception);

        String body = response.getContentAsString(StandardCharsets.UTF_8);
        assertTrue(body.contains("\"error\":\"access_denied\""));
        assertTrue(body.contains("\"code\":403"));
        assertEquals(403, response.getStatus());
    }

    @Test
    void emptyBodyFromDelegate_returnsWithoutWriting() throws Exception {
        AuthenticationFailureHandler handler = newHandler();

        AuthenticationFailureHandler delegate = (HttpServletRequest req, HttpServletResponse res, AuthenticationException ex) -> res.setStatus(400);
        ReflectionTestUtils.setField(handler, "delegate", delegate);

        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/oauth/token");
        MockHttpServletResponse response = new MockHttpServletResponse();

        OAuth2AuthenticationException exception =
                new OAuth2AuthenticationException(new OAuth2Error("invalid_request", "missing", null));

        handler.onAuthenticationFailure(request, response, exception);

        assertEquals(0, response.getContentAsByteArray().length);
        assertEquals(400, response.getStatus());
    }

    @Test
    void committedResponse_doesNotRewriteBody() throws Exception {
        AuthenticationFailureHandler handler = newHandler();

        AuthenticationFailureHandler delegate = (HttpServletRequest req, HttpServletResponse res, AuthenticationException ex) -> {
            res.setStatus(400);
            res.getWriter().write("{\"error\":\"invalid_request\"}");
        };
        ReflectionTestUtils.setField(handler, "delegate", delegate);

        HttpServletRequest request = new MockHttpServletRequest("POST", "/oauth/token");
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.isCommitted()).thenReturn(true);
        when(response.getStatus()).thenReturn(400);

        OAuth2AuthenticationException exception =
                new OAuth2AuthenticationException(new OAuth2Error("invalid_request", "missing", null));

        assertDoesNotThrow(() -> handler.onAuthenticationFailure(request, response, exception));
        verify(response, never()).resetBuffer();
        verify(response, never()).getOutputStream();
    }
}

