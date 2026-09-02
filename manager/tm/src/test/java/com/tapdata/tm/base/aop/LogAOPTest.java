package com.tapdata.tm.base.aop;

import com.tapdata.tm.base.annotation.IgnoreRequestBodyLog;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LogAOPTest {

    @Test
    @DisplayName("request logging omits arguments for endpoints carrying sensitive callback bodies")
    @SuppressWarnings("unchecked")
    void omitsSensitiveRequestBody() throws Exception {
        LogAOP aspect = new LogAOP();
        SensitiveController target = new SensitiveController();
        Logger logger = mock(Logger.class);
        when(logger.isDebugEnabled()).thenReturn(true);
        Map<String, Logger> loggerCache = (Map<String, Logger>) ReflectionTestUtils.getField(aspect, "loggerCache");
        loggerCache.put(target.getClass().getName(), logger);

        Method method = SensitiveController.class.getMethod("report", String.class, SecretRequest.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(signature.getMethod()).thenReturn(method);
        when(signature.getName()).thenReturn("report");
        when(signature.getParameterNames()).thenReturn(new String[]{"taskId", "request"});
        JoinPoint joinPoint = mock(JoinPoint.class);
        when(joinPoint.getTarget()).thenReturn(target);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"task-id", new SecretRequest("must-not-be-logged")});

        aspect.before(joinPoint);

        verify(logger).debug(eq("{}, params:[omitted]"), eq("report"));
        verify(logger, never()).debug(eq("{}, params:{}"), eq("report"), org.mockito.ArgumentMatchers.any());
    }

    private static class SensitiveController {
        @IgnoreRequestBodyLog
        public void report(String taskId, SecretRequest request) {
        }
    }

    private record SecretRequest(String password) {
    }
}
