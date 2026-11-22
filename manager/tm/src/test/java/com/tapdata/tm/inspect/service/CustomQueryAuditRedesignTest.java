package com.tapdata.tm.inspect.service;

import com.tapdata.tm.audit.*;
import com.tapdata.tm.commons.inspect.CustomQueryConfig;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.context.request.async.DeferredResult;
import java.lang.reflect.Method;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CustomQueryAuditRedesignTest {
    static class Endpoint {
        @Audited(value = AuditAction.INSPECT_TEST_QUERY, objectId = "'connection'")
        public DeferredResult<?> preview() { return null; }
    }
    @Test void auditWaitsForPreviewSuccessAndPreservesRequestContext() throws Throwable { verifyCompletion(false); }
    @Test void auditRecordsFailedPreviewInsteadOfPrematureSuccess() throws Throwable { verifyCompletion(true); }

    private void verifyCompletion(boolean failed) throws Throwable {
        AuditEventService service = mock(AuditEventService.class);
        when(service.currentOperationId()).thenReturn("operation");
        AuditAspect aspect = new AuditAspect(service,mock(AuditObjectResolver.class),mock(AuditOperationRegistry.class));
        DeferredResult<?> deferred = mock(DeferredResult.class);
        Method method = Endpoint.class.getMethod("preview");
        ProceedingJoinPoint invocation = mock(ProceedingJoinPoint.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(invocation.getSignature()).thenReturn(signature);when(signature.getMethod()).thenReturn(method);
        when(invocation.getArgs()).thenReturn(new Object[0]);when(invocation.getTarget()).thenReturn(new Endpoint());
        when(invocation.proceed()).thenReturn(deferred);
        AuditRequestContext request = new AuditRequestContext();request.setOperationId("operation");request.setSourceIp("127.0.0.1");
        AuditRequestContext.set(request);
        try {
            assertSame(deferred,aspect.around(invocation,method.getAnnotation(Audited.class)));
            verify(service,never()).record(any(),any());
            ArgumentCaptor<Runnable> completion = ArgumentCaptor.forClass(Runnable.class);
            verify(deferred).onCompletion(completion.capture());
            when(deferred.hasResult()).thenReturn(true);
            doReturn(failed ? new IllegalStateException("query error") : new CustomQueryConfig.Snapshot()).when(deferred).getResult();
            AuditRequestContext.clear();
            doAnswer(call -> { assertSame(request,AuditRequestContext.current());return null; }).when(service).record(any(),any());
            completion.getValue().run();
            ArgumentCaptor<AuditEvent> event = ArgumentCaptor.forClass(AuditEvent.class);
            verify(service).record(event.capture(),any());
            assertEquals(failed ? AuditOutcome.FAILED : AuditOutcome.SUCCESS,event.getValue().getOutcome());
            assertEquals("operation",event.getValue().getOperationId());assertEquals("connection",event.getValue().getObjectId());
            assertNull(AuditRequestContext.current());
        } finally { AuditRequestContext.clear(); }
    }
}
