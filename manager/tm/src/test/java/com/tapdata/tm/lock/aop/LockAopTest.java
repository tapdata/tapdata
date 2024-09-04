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

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class LockAopTest {
    private LockAop lockAop;
    private LockService lockService;
    @BeforeEach
    void beforeEach(){
        lockAop = mock(LockAop.class);
        lockService = mock(LockService.class);
        ReflectionTestUtils.setField(lockAop,"lockService",lockService);
    }
    @Nested
    class lockTest{
        private Lock annotation;
        private ProceedingJoinPoint pjp;
        private MethodSignature methodSignature;
        @BeforeEach
        @SneakyThrows
        void beforeEach(){
            pjp = mock(ProceedingJoinPoint.class);
            methodSignature = mock(MethodSignature.class);
            Method method = mock(Method.class);
            when(methodSignature.getMethod()).thenReturn(method);
            String[] parameters = {"taskId"};
            when(methodSignature.getParameterNames()).thenReturn(parameters);
            annotation = spy(Lock.class);
            when(method.getAnnotation(Lock.class)).thenReturn(annotation);
            when((MethodSignature)pjp.getSignature()).thenReturn(methodSignature);
            when(pjp.getArgs()).thenReturn(parameters);
            doCallRealMethod().when(lockAop).lock(pjp);
        }
        @Test
        @SneakyThrows
        @DisplayName("test lock method when value is not empty")
        void test1(){
            doReturn("taskId").when(annotation).value();
            doReturn(LockType.PIPELINE_LIMIT).when(annotation).type();
            when(lockService.lock("PIPELINE_LIMIT_taskId",0,0)).thenReturn(true);
            lockAop.lock(pjp);
            verify(lockService,new Times(1)).lock("PIPELINE_LIMIT_taskId",0,0);
        }
        @Test
        @SneakyThrows
        @DisplayName("test lock method when custom is not empty")
        void test2(){
            doReturn("").when(annotation).value();
            doReturn("test").when(annotation).customValue();
            doReturn(LockType.PIPELINE_LIMIT).when(annotation).type();
            when(lockService.lock("PIPELINE_LIMIT_test",0,0)).thenReturn(true);
            lockAop.lock(pjp);
            verify(lockService,new Times(1)).lock("PIPELINE_LIMIT_test",0,0);
        }
        @Test
        @SneakyThrows
        @DisplayName("test lock method when custom and value is empty")
        void test3(){
            doReturn("").when(annotation).value();
            doReturn("").when(annotation).customValue();
            doReturn(LockType.PIPELINE_LIMIT).when(annotation).type();
            assertThrows(BizException.class, ()->lockAop.lock(pjp));
            verify(lockService,new Times(0)).lock(anyString(),anyInt(),anyInt());
        }
        @Test
        @SneakyThrows
        @DisplayName("test lock method when paramIndex is -1")
        void test4(){
            doReturn("taskId").when(annotation).value();
            String[] parameters = {};
            when(methodSignature.getParameterNames()).thenReturn(parameters);
            doReturn(LockType.PIPELINE_LIMIT).when(annotation).type();
            assertThrows(BizException.class, ()->lockAop.lock(pjp));
            verify(lockService,new Times(0)).lock(anyString(),anyInt(),anyInt());
        }
    }
}
