package com.tapdata.tm.proxy.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.dto.WorkerExpireDto;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.modules.api.net.service.EngineMessageExecutionService;
import io.tapdata.pdk.apis.entity.message.EngineMessage;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RemoteCallerTest {

    static class CapturingServletOutputStream extends ServletOutputStream {
        private final ByteArrayOutputStream byteArrayOutputStream;
        private boolean throwOnWrite;

        CapturingServletOutputStream(ByteArrayOutputStream byteArrayOutputStream) {
            this.byteArrayOutputStream = byteArrayOutputStream;
        }

        void throwOnWrite(boolean throwOnWrite) {
            this.throwOnWrite = throwOnWrite;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
        }

        @Override
        public void write(int b) throws IOException {
            if (throwOnWrite) {
                throw new IOException("io");
            }
            byteArrayOutputStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (throwOnWrite) {
                throw new IOException("io");
            }
            byteArrayOutputStream.write(b, off, len);
        }
    }

    private static UserDetail userDetail() {
        return new UserDetail("u1", "c1", "name", "pwd", Collections.emptyList());
    }

    private static HttpServletRequest requestWithAsync() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        AsyncContext asyncContext = mock(AsyncContext.class);
        when(request.startAsync()).thenReturn(asyncContext);
        doNothing().when(asyncContext).setTimeout(anyLong());
        doNothing().when(asyncContext).addListener(any());
        return request;
    }

    private static HttpServletResponse responseWithStream(CapturingServletOutputStream servletOutputStream) throws IOException {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(servletOutputStream);
        return response;
    }

    private static RemoteCaller newRemoteCaller(ProductComponent productComponent, WorkerService workerService) {
        RemoteCaller remoteCaller = new RemoteCaller();
        ReflectionTestUtils.setField(remoteCaller, "productComponent", productComponent);
        ReflectionTestUtils.setField(remoteCaller, "workerService", workerService);
        return remoteCaller;
    }

    @Test
    void testCallMethodValidation() {
        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        assertThrows(BizException.class, () -> remoteCaller.callMethod(null, null, null, null));

        ServiceCaller noClass = new ServiceCaller();
        assertThrows(BizException.class, () -> remoteCaller.callMethod(noClass, null, null, null));

        ServiceCaller noMethod = new ServiceCaller();
        noMethod.className("C");
        assertThrows(BizException.class, () -> remoteCaller.callMethod(noMethod, null, null, null));
    }

    @Test
    void testCallMethodCloudSubscribeShareWorker() {
        ProductComponent productComponent = mock(ProductComponent.class);
        when(productComponent.isCloud()).thenReturn(true);
        WorkerService workerService = mock(WorkerService.class);
        WorkerExpireDto workerExpireDto = new WorkerExpireDto();
        workerExpireDto.setShareTmUserId("shareU");
        when(workerService.getShareWorker(any())).thenReturn(workerExpireDto);

        RemoteCaller remoteCaller = spy(newRemoteCaller(productComponent, workerService));
        doNothing().when(remoteCaller).executeServiceCaller(any(), any(), any(), any());

        ServiceCaller serviceCaller = spy(new ServiceCaller());
        serviceCaller.className("C").method("m").args(new Object[]{});

        remoteCaller.callMethod(serviceCaller, null, null, userDetail());

        verify(serviceCaller, times(1)).subscribeIds("userId_u1");
        verify(serviceCaller, times(1)).orSubscribeIdSets(eq(Set.of("userId_shareU")));
    }

    @Test
    void testExecuteServiceCallerAppendContextArgsNullAndNotNull() {
        ProductComponent productComponent = mock(ProductComponent.class);
        WorkerService workerService = mock(WorkerService.class);
        RemoteCaller remoteCaller = spy(newRemoteCaller(productComponent, workerService));
        doNothing().when(remoteCaller).executeEngineMessage(any(EngineMessage.class), any(), any());

        ServiceCaller argsNull = new ServiceCaller();
        argsNull.className("C").method("m");
        remoteCaller.executeServiceCaller(mock(HttpServletRequest.class), mock(HttpServletResponse.class), argsNull, userDetail());
        assertNotNull(argsNull.getId());
        assertEquals(Object.class.getName(), argsNull.getReturnClass());
        assertEquals(1, argsNull.getArgs().length);
        assertNotNull(argsNull.getArgs()[0]);

        ServiceCaller argsNotNull = new ServiceCaller();
        argsNotNull.className("C").method("m").args(new Object[]{"x"});
        argsNotNull.setReturnClass(" ");
        remoteCaller.executeServiceCaller(mock(HttpServletRequest.class), mock(HttpServletResponse.class), argsNotNull, null);
        assertEquals(Object.class.getName(), argsNotNull.getReturnClass());
        assertEquals(2, argsNotNull.getArgs().length);
        assertEquals("x", argsNotNull.getArgs()[0]);
        assertEquals(null, argsNotNull.getArgs()[1]);
    }

    @Test
    void testExecuteEngineMessageSuccessJson() throws Exception {
        EngineMessageExecutionService engineService = mock(EngineMessageExecutionService.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<Object, Throwable> callback = invocation.getArgument(1);
            callback.accept(Collections.singletonMap("k", "v"), null);
            return null;
        }).when(engineService).call(any(), any());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CapturingServletOutputStream servletOutputStream = new CapturingServletOutputStream(out);
        HttpServletResponse response = responseWithStream(servletOutputStream);
        HttpServletRequest request = requestWithAsync();

        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        EngineMessage message = mock(EngineMessage.class);
        when(message.getId()).thenReturn("job1");

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class);
             MockedStatic<TapSimplify> tapSimplifyMockedStatic = Mockito.mockStatic(TapSimplify.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(eq(EngineMessageExecutionService.class), eq(true))).thenReturn(engineService);
            tapSimplifyMockedStatic.when(() -> TapSimplify.toJson(any(), any(JsonParser.ToJsonFeature.class))).thenReturn("{}");

            remoteCaller.executeEngineMessage(message, request, response);
        }

        String body = out.toString(StandardCharsets.UTF_8);
        assertTrue(body.contains("\"code\"") && body.contains("ok"));
        verify(response, times(1)).setContentType("application/json; charset=utf-8");
    }

    @Test
    void testExecuteEngineMessageErrorCoreExceptionWithData() throws Exception {
        EngineMessageExecutionService engineService = mock(EngineMessageExecutionService.class);
        CoreException coreException = mock(CoreException.class);
        when(coreException.getCode()).thenReturn(123);
        when(coreException.getData()).thenReturn(Collections.singletonMap("a", 1));
        when(coreException.getMessage()).thenReturn("err");

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<Object, Throwable> callback = invocation.getArgument(1);
            callback.accept(null, coreException);
            return null;
        }).when(engineService).call(any(), any());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CapturingServletOutputStream servletOutputStream = new CapturingServletOutputStream(out);
        HttpServletResponse response = responseWithStream(servletOutputStream);
        HttpServletRequest request = requestWithAsync();

        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        EngineMessage message = mock(EngineMessage.class);
        when(message.getId()).thenReturn("job2");

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class);
             MockedStatic<TapSimplify> tapSimplifyMockedStatic = Mockito.mockStatic(TapSimplify.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(eq(EngineMessageExecutionService.class), eq(true))).thenReturn(engineService);
            tapSimplifyMockedStatic.when(() -> TapSimplify.toJson(any(), any(JsonParser.ToJsonFeature.class))).thenReturn("{\"data\":1}");
            remoteCaller.executeEngineMessage(message, request, response);
        }

        String body = out.toString(StandardCharsets.UTF_8);
        assertTrue(body.contains("123"));
        assertTrue(body.contains("err"));
        assertTrue(body.contains("\"data\""));
    }

    @Test
    void testExecuteEngineMessageCallThrowsAndWriteIOException() throws Exception {
        EngineMessageExecutionService engineService = mock(EngineMessageExecutionService.class);
        doThrow(new RuntimeException("boom")).when(engineService).call(any(), any());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CapturingServletOutputStream servletOutputStream = new CapturingServletOutputStream(out);
        servletOutputStream.throwOnWrite(true);
        HttpServletResponse response = responseWithStream(servletOutputStream);
        HttpServletRequest request = requestWithAsync();

        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        EngineMessage message = mock(EngineMessage.class);
        when(message.getId()).thenReturn("job3");

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(eq(EngineMessageExecutionService.class), eq(true))).thenReturn(engineService);
            remoteCaller.executeEngineMessage(message, request, response);
        }

        verify(response, times(1)).sendError(eq(500), anyString());
    }

    @Test
    void testExecuteEngineMessageFileMetaTransfer() throws Exception {
        EngineMessageExecutionService engineService = mock(EngineMessageExecutionService.class);
        FileMeta fileMeta = mock(FileMeta.class);
        when(fileMeta.isTransferFile()).thenReturn(true);
        when(fileMeta.getFilename()).thenReturn("a.txt");
        when(fileMeta.getFileSize()).thenReturn(3L);
        when(fileMeta.getCode()).thenReturn("ok");
        when(fileMeta.getFileInputStream()).thenReturn(new ByteArrayInputStream(new byte[]{1, 2, 3}));

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<Object, Throwable> callback = invocation.getArgument(1);
            callback.accept(fileMeta, null);
            return null;
        }).when(engineService).call(any(), any());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CapturingServletOutputStream servletOutputStream = new CapturingServletOutputStream(out);
        HttpServletResponse response = responseWithStream(servletOutputStream);
        HttpServletRequest request = requestWithAsync();

        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        EngineMessage message = mock(EngineMessage.class);
        when(message.getId()).thenReturn("job4");

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class);
             MockedStatic<TapSimplify> tapSimplifyMockedStatic = Mockito.mockStatic(TapSimplify.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(eq(EngineMessageExecutionService.class), eq(true))).thenReturn(engineService);
            tapSimplifyMockedStatic.when(() -> TapSimplify.toJson(any(), any(JsonParser.ToJsonFeature.class))).thenReturn("{}");
            remoteCaller.executeEngineMessage(message, request, response);
        }

        byte[] bytes = out.toByteArray();
        assertEquals(3, bytes.length);
        assertEquals(1, bytes[0]);
        assertEquals(2, bytes[1]);
        assertEquals(3, bytes[2]);

        verify(response, times(1)).setHeader(eq(HttpHeaders.CONTENT_DISPOSITION), eq("attachment; filename=a.txt"));
        verify(response, times(1)).setHeader(eq(HttpHeaders.CONTENT_LENGTH), eq("3"));
        verify(response, times(1)).setHeader(eq("X-FileMeta-Code"), eq("ok"));
    }

    @Test
    void testGetEngineMessageExecutionServiceNull() {
        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        EngineMessage message = mock(EngineMessage.class);
        when(message.getId()).thenReturn("job5");

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(eq(EngineMessageExecutionService.class), eq(true))).thenReturn(null);
            BizException ex = assertThrows(BizException.class, () -> remoteCaller.executeEngineMessage(message, requestWithAsync(), mock(HttpServletResponse.class)));
            assertEquals("commandExecutionService is null", ex.getMessage());
        }
    }

    @Test
    void testExecuteServiceCallerBiConsumerArgsNullReturn() {
        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        ServiceCaller serviceCaller = new ServiceCaller();
        serviceCaller.className("C").method("m");

        AtomicBoolean called = new AtomicBoolean(false);
        remoteCaller.executeServiceCaller(serviceCaller, (r, e) -> called.set(true));
        assertTrue(!called.get());
    }

    @Test
    void testExecuteServiceCallerBiConsumerArgsNotNullAndEngineErrorWrapped() {
        EngineMessageExecutionService engineService = mock(EngineMessageExecutionService.class);
        doThrow(new RuntimeException("boom")).when(engineService).call(any(), any());

        RemoteCaller remoteCaller = newRemoteCaller(mock(ProductComponent.class), mock(WorkerService.class));
        ServiceCaller serviceCaller = new ServiceCaller();
        serviceCaller.className("C").method("m").args(new Object[]{"x"});

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(eq(EngineMessageExecutionService.class), eq(true))).thenReturn(engineService);
            assertThrows(RuntimeException.class, () -> remoteCaller.executeServiceCaller(serviceCaller, (r, e) -> {
            }));
        }
    }
}
