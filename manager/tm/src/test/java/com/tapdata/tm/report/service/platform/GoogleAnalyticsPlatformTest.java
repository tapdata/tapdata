package com.tapdata.tm.report.service.platform;

import lombok.SneakyThrows;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.*;
import org.mockito.internal.verification.Times;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class GoogleAnalyticsPlatformTest {
    @Nested
    class GoogleAnalyticsPlatformConstructMethodTest{
        @Test
        void testNormal(){
            GoogleAnalyticsPlatform platform = new GoogleAnalyticsPlatform();
            CloseableHttpClient client = platform.getClient();
            assertNotNull(client);
            try {
                client.close();
            } catch (IOException e) {
            }
        }
    }
    @Nested
    class SendRequestTest{
        private GoogleAnalyticsPlatform platform;
        private CloseableHttpClient client;
        @BeforeEach
        void beforeEach(){
            platform = mock(GoogleAnalyticsPlatform.class);
            client = mock(CloseableHttpClient.class);
            ReflectionTestUtils.setField(platform,"client",client);
            doCallRealMethod().when(platform).sendRequest(anyString(),anyString());
        }
        @Test
        @SneakyThrows
        @DisplayName("test sendRequest method normal")
        void test1(){
            when(client.execute(any(HttpPost.class))).thenReturn(null);
            platform.sendRequest("test_event","{\"test_param\":\"111\"}");
            verify(client,new Times(1)).execute(any(HttpPost.class));
        }
        @Test
        @SneakyThrows
        @DisplayName("test sendRequest method with exception")
        void test2(){
            Logger logger = mock(Logger.class);
            Field log = GoogleAnalyticsPlatform.class.getDeclaredField("log");
            log.setAccessible(true);
            Field modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(log, log.getModifiers() & ~Modifier.FINAL);
            log.set(platform, logger);
            when(client.execute(any(HttpPost.class))).thenThrow(IOException.class);
            platform.sendRequest("test_event","{\"test_param\":\"111\"}");
            verify(logger,new Times(1)).info(anyString(),any(Exception.class));
        }
    }
}
