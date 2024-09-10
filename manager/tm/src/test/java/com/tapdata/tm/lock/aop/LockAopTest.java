package com.tapdata.tm.lock.aop;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;
import com.tapdata.tm.lock.service.LockService;
import lombok.SneakyThrows;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class LockAopTest {
    private LockAop lockAop;
    private LockService lockService;

    @BeforeEach
    void beforeEach() {
        lockAop = mock(LockAop.class);
        lockService = mock(LockService.class);
        ReflectionTestUtils.setField(lockAop, "lockService", lockService);
    }

    @Nested
    class lockTest {
        private ProceedingJoinPoint pjp;
        private MethodSignature methodSignature;

        @BeforeEach
        @SneakyThrows
        void beforeEach() {
            pjp = mock(ProceedingJoinPoint.class);
            methodSignature = mock(MethodSignature.class);
            String[] parameters = {"taskId"};
            when(methodSignature.getParameterNames()).thenReturn(parameters);
            when((MethodSignature) pjp.getSignature()).thenReturn(methodSignature);
            when(pjp.getArgs()).thenReturn(parameters);
            doCallRealMethod().when(lockAop).lock(pjp);
        }

        @Lock(value = "taskId", type = LockType.PIPELINE_LIMIT)
        void test1Method() {
        }

        @Test
        @SneakyThrows
        @DisplayName("test lock method when value is not empty")
        void test1() {
            when(methodSignature.getMethod()).thenReturn(getClass().getDeclaredMethod("test1Method"));
            when(lockService.lock("PIPELINE_LIMIT_taskId", 10, 50)).thenReturn(true);
            lockAop.lock(pjp);
            verify(lockService, new Times(1)).lock("PIPELINE_LIMIT_taskId", 10, 50);
        }

        @Lock(value = "test", type = LockType.PIPELINE_LIMIT, valueType = Lock.CUSTOM)
        void test2Method() {
        }

        @Test
        @SneakyThrows
        @DisplayName("test lock method when custom is not empty")
        void test2() {
            when(methodSignature.getMethod()).thenReturn(getClass().getDeclaredMethod("test2Method"));
            when(lockService.lock("PIPELINE_LIMIT_test", 10, 50)).thenReturn(true);
            lockAop.lock(pjp);
            verify(lockService, new Times(1)).lock("PIPELINE_LIMIT_test", 10, 50);
        }

        @Lock(value = "", type = LockType.PIPELINE_LIMIT, valueType = Lock.CUSTOM)
        void test3Method() {
        }
        @Test
        @SneakyThrows
        @DisplayName("test lock method when custom and value is empty")
        void test3() {
            when(methodSignature.getMethod()).thenReturn(getClass().getDeclaredMethod("test3Method"));
            assertThrows(BizException.class, () -> lockAop.lock(pjp));
            verify(lockService, new Times(0)).lock(anyString(), anyInt(), anyInt());
        }

        @Lock(value = "taskId", type = LockType.PIPELINE_LIMIT, valueType = Lock.CUSTOM)
        void test4Method() {
        }
        @Test
        @SneakyThrows
        @DisplayName("test lock method when paramIndex is -1")
        void test4() {
            when(methodSignature.getMethod()).thenReturn(getClass().getDeclaredMethod("test4Method"));
            String[] parameters = {};
            when(methodSignature.getParameterNames()).thenReturn(parameters);
            assertThrows(BizException.class, () -> lockAop.lock(pjp));
            verify(lockService, new Times(0)).lock(anyString(), anyInt(), anyInt());
        }
    }
}
