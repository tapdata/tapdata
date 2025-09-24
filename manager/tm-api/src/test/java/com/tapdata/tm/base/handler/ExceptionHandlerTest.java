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
}
