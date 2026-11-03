package io.tapdata.common;

import com.tapdata.entity.Event;
import com.tapdata.entity.Setting;
import com.tapdata.entity.TapLog;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.simplejavamail.mailer.Mailer;
import org.springframework.test.util.ReflectionTestUtils;

import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.SendFailedException;
import javax.mail.internet.InternetAddress;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class BaseEmailExecutorTest {
    private BaseEmailExecutor baseEmailExecutor;
    private SettingService settingService;
    @BeforeEach
    void beforeEach(){
        baseEmailExecutor = mock(BaseEmailExecutor.class);
        settingService = mock(SettingService.class);
        ReflectionTestUtils.setField(baseEmailExecutor,"settingService",settingService);
    }
    @Nested
    class completeAsPartialSentTest{
        private Logger logger;
        private BaseEmailExecutor executor;

        @BeforeEach
        void setUp(){
            logger = mock(Logger.class);
            executor = mock(BaseEmailExecutor.class, CALLS_REAL_METHODS);
        }

        @Test
        void shouldMarkSuccessWhenOnlySomeRecipientsRejected() throws Exception {
            Event event = new Event();
            event.setName("warn-email");
            SendFailedException failure = new SendFailedException("Invalid Addresses", null,
                    new Address[]{new InternetAddress("ok@tapdata.io")},
                    null,
                    new Address[]{new InternetAddress("bad@tapdata.io")});

            boolean partial = executor.completeAsPartialSent(event, failure, logger);

            assertTrue(partial);
            assertEquals(Event.EVENT_STATUS_SUCCESSED, event.getEvent_status());
            assertTrue(event.getConsume_ts() > 0L);
            verify(logger).warn(eq("{} partial sent: {}"), eq(TapLog.ERROR_0001.getMsg()), contains("bad@tapdata.io"));
        }

        @Test
        void shouldNotMarkSuccessWhenEveryRecipientRejected() throws Exception {
            Event event = new Event();
            event.setName("warn-email");
            SendFailedException failure = new SendFailedException("Invalid Addresses", null,
                    null, null,
                    new Address[]{new InternetAddress("bad@tapdata.io")});

            boolean partial = executor.completeAsPartialSent(event, failure, logger);

            assertFalse(partial);
            assertEquals(Event.EVENT_STATUS_WAITING, event.getEvent_status());
            assertEquals(0L, event.getConsume_ts());
        }

        @Test
        void shouldNotMarkSuccessForNonSendFailure() {
            Event event = new Event();
            event.setName("warn-email");

            boolean partial = executor.completeAsPartialSent(event, new MessagingException("connect failed"), logger);

            assertFalse(partial);
            assertEquals(Event.EVENT_STATUS_WAITING, event.getEvent_status());
        }

        @Test
        void shouldDetectPartialDeliveryThroughCauseChain() throws Exception {
            Event event = new Event();
            event.setName("warn-email");
            SendFailedException inner = new SendFailedException("Invalid Addresses", null,
                    new Address[]{new InternetAddress("ok@tapdata.io")},
                    null,
                    new Address[]{new InternetAddress("bad@tapdata.io")});

            boolean partial = executor.completeAsPartialSent(event,
                    new RuntimeException("wrapped by mail library", inner), logger);

            assertTrue(partial);
            assertEquals(Event.EVENT_STATUS_SUCCESSED, event.getEvent_status());
        }
    }

    @Nested
    class initMailerTest{
        @Test
        void testForProxy(){
            Setting hostSett = mock(Setting.class);
            Setting portSett = mock(Setting.class);
            Setting userSett = mock(Setting.class);
            Setting passwordSett = mock(Setting.class);
            Setting encryptTypeSett = mock(Setting.class);
            Setting sendAddress = mock(Setting.class);
            Setting proxyHostSett = mock(Setting.class);
            Setting proxyPortSett = mock(Setting.class);
            when(settingService.getSetting("smtp.server.host")).thenReturn(hostSett);
            when(settingService.getSetting("smtp.server.port")).thenReturn(portSett);
            when(settingService.getSetting("smtp.server.user")).thenReturn(userSett);
            when(settingService.getSetting("smtp.server.password")).thenReturn(passwordSett);
            when(settingService.getSetting("email.server.tls")).thenReturn(encryptTypeSett);
            when(settingService.getSetting("email.send.address")).thenReturn(sendAddress);
            when(settingService.getSetting("smtp.proxy.host")).thenReturn(proxyHostSett);
            when(settingService.getSetting("smtp.proxy.port")).thenReturn(proxyPortSett);
            when(hostSett.getValue()).thenReturn("smtp.test.cn");
            when(portSett.getValue()).thenReturn("465");
            when(userSett.getValue()).thenReturn("test@tapdata.io");
            when(passwordSett.getValue()).thenReturn("test_passwd");
            when(proxyHostSett.getValue()).thenReturn("smtp.proxy.cn");
            when(proxyPortSett.getValue()).thenReturn("1025");
            doCallRealMethod().when(baseEmailExecutor).initMailer();
            Mailer actual = baseEmailExecutor.initMailer();
            assertNotNull(actual.getProxyConfig());
            assertEquals("smtp.proxy.cn",actual.getProxyConfig().getRemoteProxyHost());
        }
        @Test
        void testNormal(){
            Setting hostSett = mock(Setting.class);
            Setting portSett = mock(Setting.class);
            Setting userSett = mock(Setting.class);
            Setting passwordSett = mock(Setting.class);
            Setting encryptTypeSett = mock(Setting.class);
            Setting sendAddress = mock(Setting.class);
            Setting proxyHostSett = mock(Setting.class);
            Setting proxyPortSett = mock(Setting.class);
            when(settingService.getSetting("smtp.server.host")).thenReturn(hostSett);
            when(settingService.getSetting("smtp.server.port")).thenReturn(portSett);
            when(settingService.getSetting("smtp.server.user")).thenReturn(userSett);
            when(settingService.getSetting("smtp.server.password")).thenReturn(passwordSett);
            when(settingService.getSetting("email.server.tls")).thenReturn(encryptTypeSett);
            when(settingService.getSetting("email.send.address")).thenReturn(sendAddress);
            when(hostSett.getValue()).thenReturn("smtp.test.cn");
            when(portSett.getValue()).thenReturn("465");
            when(userSett.getValue()).thenReturn("test@tapdata.io");
            when(passwordSett.getValue()).thenReturn("test_passwd");
            when(proxyHostSett.getValue()).thenReturn("smtp.proxy.cn");
            when(proxyPortSett.getValue()).thenReturn("1025");
            doCallRealMethod().when(baseEmailExecutor).initMailer();
            Mailer actual = baseEmailExecutor.initMailer();
            assertNull(actual.getProxyConfig().getRemoteProxyHost());
        }
    }
}
