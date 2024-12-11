package com.tapdata.tm.proxy.controller;

import io.tapdata.modules.api.net.data.FileMeta;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.test.util.ReflectionTestUtils;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/12/10 11:43
 */
public class ProxyControllerTest {

    @Test
    void testResponseForFileMeta() throws IOException {

        ProxyController proxyController = new ProxyController();

        byte[] data = new byte[1024];
        FileMeta fileMeta = FileMeta.builder().transferFile(true)
                .filename("test.log.zip")
                .fileSize(1024L)
                .fileInputStream(new ByteArrayInputStream(data))
                .code("ok").build();


        HttpServletResponse response = mock(HttpServletResponse.class);
        AtomicInteger counter = new AtomicInteger(0);
        when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setWriteListener(WriteListener listener) {

            }

            @Override
            public void write(int b) throws IOException {

            }

            @Override
            public void write(@NotNull byte[] b, int off, int len) throws IOException {
                counter.incrementAndGet();
            }
        });

        ReflectionTestUtils.invokeMethod(proxyController, "responseForFileMeta", fileMeta, response);

        Assertions.assertEquals(1, counter.get());
        verify(response, times(1)).setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.getMimeType());
        verify(response, times(1)).setHeader(HttpHeaders.CONTENT_LENGTH, "1024");

    }

    @Test
    void testDownloadFile() {
        ProxyController proxyController = new ProxyController();

        ProxyController spyProxyController = spy(proxyController);
        doNothing().when(spyProxyController).call(any(), any(), any());
        spyProxyController.downloadFile("test.log", "agentId", null, null);
        verify(spyProxyController, times(1)).call(any(), any(), any());
    }

}
