package io.tapdata.common;

import com.tapdata.entity.Setting;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.simplejavamail.mailer.Mailer;
import org.springframework.test.util.ReflectionTestUtils;

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
