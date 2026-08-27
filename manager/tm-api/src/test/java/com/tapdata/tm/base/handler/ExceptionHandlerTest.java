package com.tapdata.tm.base.handler;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.WebUtils;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

public class ExceptionHandlerTest {
    @DisplayName("test not NotAuthorized")
    @Test
    void test1() {
        try (MockedStatic<MessageUtil> messageUtilMockedStatic = mockStatic(MessageUtil.class);
             MockedStatic<WebUtils> webUtilsMockedStatic = mockStatic(WebUtils.class);) {
            BizException notAuthorized = mock(BizException.class);
            when(notAuthorized.getErrorCode()).thenReturn("NotAuthorized");
            HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
            HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
            webUtilsMockedStatic.when(() -> {
                WebUtils.getLocale(httpServletRequest);
            }).thenReturn(Locale.CHINA);
            messageUtilMockedStatic.when(() -> {
                MessageUtil.getMessage(any(Locale.class), anyString(), eq(null));
            }).thenReturn("not NotAuthorized");
            ExceptionHandler exceptionHandler = new ExceptionHandler();
            doNothing().when(httpServletResponse).setStatus(HttpStatus.SC_UNAUTHORIZED);
            ResponseMessage<?> responseMessage = exceptionHandler.handlerException(notAuthorized, httpServletRequest, httpServletResponse);
            assertEquals("not NotAuthorized", responseMessage.getMessage());
        }
    }
    @DisplayName("test not Login")
    @Test
    void test2(){
        try (MockedStatic<MessageUtil> messageUtilMockedStatic = mockStatic(MessageUtil.class);
             MockedStatic<WebUtils> webUtilsMockedStatic = mockStatic(WebUtils.class);) {
            BizException notAuthorized = mock(BizException.class);
            when(notAuthorized.getErrorCode()).thenReturn("NotLogin");
            HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
            HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
            webUtilsMockedStatic.when(() -> {
                WebUtils.getLocale(httpServletRequest);
            }).thenReturn(Locale.CHINA);
            messageUtilMockedStatic.when(() -> {
                MessageUtil.getMessage(any(Locale.class), anyString(), eq(null));
            }).thenReturn("NotLogin");
            ExceptionHandler exceptionHandler = new ExceptionHandler();
            doNothing().when(httpServletResponse).setStatus(HttpStatus.SC_UNAUTHORIZED);
            ResponseMessage<?> responseMessage = exceptionHandler.handlerException(notAuthorized, httpServletRequest, httpServletResponse);
            assertEquals("NotLogin", responseMessage.getMessage());
            assertNull(responseMessage.getStack());
        }
    }

    @DisplayName("test not actuator/prometheus")
    @Test
    void test3(){
        try (MockedStatic<MessageUtil> messageUtilMockedStatic = mockStatic(MessageUtil.class);
             MockedStatic<WebUtils> webUtilsMockedStatic = mockStatic(WebUtils.class);) {
            final NoResourceFoundException mock = mock(NoResourceFoundException.class);
            when(mock.getMessage()).thenReturn("actuator/prometheus");
            HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
            HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
            webUtilsMockedStatic.when(() -> {
                WebUtils.getLocale(httpServletRequest);
            }).thenReturn(Locale.CHINA);
            messageUtilMockedStatic.when(() -> {
                MessageUtil.getMessage(any(Locale.class), anyString(), eq(null));
            }).thenReturn("NotLogin");
            ExceptionHandler exceptionHandler = new ExceptionHandler();
            doNothing().when(httpServletResponse).setStatus(HttpStatus.SC_UNAUTHORIZED);
            ResponseMessage<?> responseMessage = exceptionHandler.handlerException(mock, httpServletRequest, httpServletResponse);
            assertEquals("No static resource actuator/prometheus", responseMessage.getMessage());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @DisplayName("DQL invalid arguments use HTTP 400")
    @Test
    void dqlInvalidArgumentsUseBadRequest() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events/recovery/preview");
        HttpServletResponse response = mock(HttpServletResponse.class);
        BizException exception = mock(BizException.class);
        when(exception.getErrorCode()).thenReturn("IllegalArgument");
        when(exception.getArgs()).thenReturn(new Object[]{"eventIds"});

        ResponseMessage<?> result = new ExceptionHandler().handlerException(exception, request, response);

        assertEquals("IllegalArgument", result.getCode());
        verify(response).setStatus(HttpStatus.SC_BAD_REQUEST);
    }

    @DisplayName("DQL cross-task validation uses HTTP 400")
    @Test
    void dqlCrossTaskValidationUsesBadRequest() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events/recovery/preview");
        HttpServletResponse response = mock(HttpServletResponse.class);
        BizException exception = mock(BizException.class);
        when(exception.getErrorCode()).thenReturn("DqlRecovery.CrossTaskNotAllowed");
        when(exception.getArgs()).thenReturn(new Object[]{"eventIds"});

        ResponseMessage<?> result = new ExceptionHandler().handlerException(exception, request, response);

        assertEquals("DqlRecovery.CrossTaskNotAllowed", result.getCode());
        verify(response).setStatus(HttpStatus.SC_BAD_REQUEST);
    }

    @DisplayName("DQL resources use HTTP 404 when not found")
    @Test
    void dqlResourcesUseNotFound() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events/DQL-1");
        HttpServletResponse response = mock(HttpServletResponse.class);
        BizException exception = mock(BizException.class);
        when(exception.getErrorCode()).thenReturn("DqlEvent.NotFound");
        when(exception.getArgs()).thenReturn(new Object[]{"DQL-1"});

        ResponseMessage<?> result = new ExceptionHandler().handlerException(exception, request, response);

        assertEquals("DqlEvent.NotFound", result.getCode());
        verify(response).setStatus(HttpStatus.SC_NOT_FOUND);
    }

    @DisplayName("DQL state and lock conflicts use HTTP 409")
    @Test
    void dqlConflictsUseConflict() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events/recovery");
        HttpServletResponse response = mock(HttpServletResponse.class);
        BizException exception = mock(BizException.class);
        when(exception.getErrorCode()).thenReturn("DqlRecovery.EventNotReprocessable");
        when(exception.getArgs()).thenReturn(new Object[]{"Some selected events cannot be reprocessed"});

        ResponseMessage<?> result = new ExceptionHandler().handlerException(exception, request, response);

        assertEquals("DqlRecovery.EventNotReprocessable", result.getCode());
        verify(response).setStatus(HttpStatus.SC_CONFLICT);
    }

    @DisplayName("DQL lock failures use HTTP 409")
    @Test
    void dqlLockFailuresUseConflict() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events/recovery");
        HttpServletResponse response = mock(HttpServletResponse.class);
        BizException exception = mock(BizException.class);
        when(exception.getErrorCode()).thenReturn("DqlRecovery.EventLockFailed");
        when(exception.getArgs()).thenReturn(new Object[]{"batch-1"});

        ResponseMessage<?> result = new ExceptionHandler().handlerException(exception, request, response);

        assertEquals("DqlRecovery.EventLockFailed", result.getCode());
        verify(response).setStatus(HttpStatus.SC_CONFLICT);
    }

    @DisplayName("DQL missing recovery batches use HTTP 404")
    @Test
    void dqlBatchNotFoundUsesNotFound() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events/recovery/batch-1");
        HttpServletResponse response = mock(HttpServletResponse.class);
        BizException exception = mock(BizException.class);
        when(exception.getErrorCode()).thenReturn("DqlRecovery.BatchNotFound");
        when(exception.getArgs()).thenReturn(new Object[]{"batch-1"});

        ResponseMessage<?> result = new ExceptionHandler().handlerException(exception, request, response);

        assertEquals("DqlRecovery.BatchNotFound", result.getCode());
        verify(response).setStatus(HttpStatus.SC_NOT_FOUND);
    }

    @DisplayName("DQL unexpected failures use HTTP 500")
    @Test
    void dqlUnexpectedFailuresUseServerError() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events");
        HttpServletResponse response = mock(HttpServletResponse.class);
        BizException exception = mock(BizException.class);
        when(exception.getErrorCode()).thenReturn("SystemError");
        when(exception.getArgs()).thenReturn(new Object[]{"database"});

        ResponseMessage<?> result = new ExceptionHandler().handlerException(exception, request, response);

        assertEquals("SystemError", result.getCode());
        verify(response).setStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    @DisplayName("DQL permission failures use HTTP 403 and a stable error code")
    @Test
    void dqlPermissionFailureUsesForbidden() throws Throwable {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/dql-events");
        HttpServletResponse response = mock(HttpServletResponse.class);

        ResponseMessage<?> result = new ExceptionHandler().handlerException(
                new RuntimeException("NoPermission"), request, response);

        assertEquals("NoPermission", result.getCode());
        verify(response).setStatus(HttpStatus.SC_FORBIDDEN);
    }

    @DisplayName("DQL status mapping does not change unrelated permission failures")
    @Test
    void nonDqlPermissionFailureKeepsExistingSemantics() throws Throwable {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/tasks");
        HttpServletResponse response = mock(HttpServletResponse.class);

        ResponseMessage<?> result = new ExceptionHandler().handlerException(
                new RuntimeException("NoPermission"), request, response);

        assertEquals("SystemError", result.getCode());
        verifyNoInteractions(response);
    }
}
