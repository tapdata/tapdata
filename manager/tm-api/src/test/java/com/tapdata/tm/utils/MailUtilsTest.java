package com.tapdata.tm.utils;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.service.BlacklistService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.SendStatus;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.*;
@ExtendWith(MockitoExtension.class)
class MailUtilsTest {
    @Mock
    BlacklistService blacklistService;
    @Mock
    SettingsService settingsService;

    @InjectMocks
    MailUtils mailUtils;

    @BeforeEach
    void setup(){

    }

    @Test
    void testReadHtmlToString() {
        Assertions.assertNotEquals(MailUtils.readHtmlToString("mailTemplate.html"),"");
    }
    @Test
    void testSendHtmlMail_subjectNotNull(){
        try(MockedStatic<Session> sessionMockedStatic = Mockito.mockStatic(Session.class)){
            Session mockSession = mock(Session.class);
            Transport transport = mock(Transport.class);
            sessionMockedStatic.when(()->Session.getDefaultInstance(any(Properties.class))).thenReturn(mockSession);
            when(blacklistService.inBlacklist(anyString())).thenReturn(false);
            List<String> addressList = new ArrayList<>();
            addressList.add("test@qq.com");
            when(settingsService.getByCategoryAndKey(anyString(),anyString())).thenReturn("test").thenReturn(5678).thenReturn("test").thenReturn("test").thenReturn("123456");
            when(mockSession.getTransport(anyString())).thenReturn(transport);
            doNothing().when(transport).connect(anyString(),anyInt(),anyString(),anyString());
            doAnswer(invocationOnMock -> {
                InternetAddress[] internetAddressList = invocationOnMock.getArgument(1);
                Assertions.assertEquals(addressList.get(0),internetAddressList[0].getAddress());
                return null;
            }).when(transport).sendMessage(any(MimeMessage.class),any());
            mailUtils.sendHtmlMail("",addressList,"test","test","test","test");
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSendHtmlMail_subjectNotNull_notInBlacklistAddressIsNull(){
        when(blacklistService.inBlacklist(anyString())).thenReturn(true);
        List<String> addressList = new ArrayList<>();
        addressList.add("test@qq.com");
        SendStatus result = mailUtils.sendHtmlMail("",addressList,"test","test","test","test");
        Assertions.assertEquals("false",result.getStatus());
        Assertions.assertTrue(result.getErrorMessage().contains("blacklist"));
    }

    @Test
    void testSendHtmlMail(){
        try(MockedStatic<Session> sessionMockedStatic = Mockito.mockStatic(Session.class)){
            Session mockSession = mock(Session.class);
            Transport transport = mock(Transport.class);
            sessionMockedStatic.when(()->Session.getDefaultInstance(any(Properties.class))).thenReturn(mockSession);
            when(blacklistService.inBlacklist(anyString())).thenReturn(false);
            List<String> addressList = new ArrayList<>();
            addressList.add("test@qq.com");
            when(settingsService.getByCategoryAndKey(anyString(),anyString())).thenReturn("test").thenReturn(5678).thenReturn("test").thenReturn("test").thenReturn("123456");
            when(mockSession.getTransport(anyString())).thenReturn(transport);
            doNothing().when(transport).connect(anyString(),anyInt(),anyString(),anyString());
            doAnswer(invocationOnMock -> {
                InternetAddress[] internetAddressList = invocationOnMock.getArgument(1);
                Assertions.assertEquals(addressList.get(0),internetAddressList[0].getAddress());
                return null;
            }).when(transport).sendMessage(any(MimeMessage.class),any());
            mailUtils.sendHtmlMail(addressList,"test","test","test", SystemEnum.AGENT, MsgTypeEnum.ALARM);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSendHtmlMail_notInBlacklistAddressIsNull(){
        when(blacklistService.inBlacklist(anyString())).thenReturn(true);
        List<String> addressList = new ArrayList<>();
        addressList.add("test@qq.com");
        SendStatus result = mailUtils.sendHtmlMail(addressList,"test","test","test", SystemEnum.AGENT, MsgTypeEnum.ALARM);
        Assertions.assertEquals("false",result.getStatus());
        Assertions.assertTrue(result.getErrorMessage().contains("blacklist"));
    }

    @Test
    void testSendHtmlMail_sourceIdNotNull(){
        try(MockedStatic<Session> sessionMockedStatic = Mockito.mockStatic(Session.class)){
            Session mockSession = mock(Session.class);
            Transport transport = mock(Transport.class);
            sessionMockedStatic.when(()->Session.getDefaultInstance(any(Properties.class))).thenReturn(mockSession);
            when(blacklistService.inBlacklist(anyString())).thenReturn(false);
            List<String> addressList = new ArrayList<>();
            addressList.add("test@qq.com");
            when(settingsService.getByCategoryAndKey(anyString(),anyString())).thenReturn("test").thenReturn(5678).thenReturn("test").thenReturn("test").thenReturn("123456");
            Settings settings = new Settings();
            settings.setValue("test");
            when(settingsService.getByCategoryAndKey(any(CategoryEnum.class),any(KeyEnum.class))).thenReturn(settings);
            when(mockSession.getTransport(anyString())).thenReturn(transport);
            doNothing().when(transport).connect(anyString(),anyInt(),anyString(),anyString());
            doAnswer(invocationOnMock -> {
                InternetAddress[] internetAddressList = invocationOnMock.getArgument(1);
                Assertions.assertEquals(addressList.get(0),internetAddressList[0].getAddress());
                return null;
            }).when(transport).sendMessage(any(MimeMessage.class),any());
            mailUtils.sendHtmlMail(addressList,"test","test", SystemEnum.AGENT, MsgTypeEnum.ALARM,"test");
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSendHtmlMail_sourceIdNotNull_notInBlacklistAddressIsNull(){
        when(blacklistService.inBlacklist(anyString())).thenReturn(true);
        List<String> addressList = new ArrayList<>();
        addressList.add("test@qq.com");
        SendStatus result = mailUtils.sendHtmlMail(addressList,"test","test", SystemEnum.AGENT, MsgTypeEnum.ALARM,"test");
        Assertions.assertEquals("false",result.getStatus());
        Assertions.assertTrue(result.getErrorMessage().contains("blacklist"));
    }

    @Test
    void testCheckNotInBlacklistAddress(){
        SendStatus sendStatus = new SendStatus("false", "");
        List<String> toList = new ArrayList<>();
        toList.add("test@qq.com");
        when(blacklistService.inBlacklist(anyString())).thenReturn(true);
        List<String> notInBlacklistAddress = mailUtils.checkNotInBlacklistAddress(toList,sendStatus);
        Assertions.assertTrue(sendStatus.getErrorMessage().contains("blacklist"));
        Assertions.assertTrue(notInBlacklistAddress.isEmpty());
    }


    @Test
    void testCheckNotInBlacklistAddress_notInBlacklistAddressNotNull(){
        SendStatus sendStatus = new SendStatus("false", "");
        List<String> toList = new ArrayList<>();
        toList.add("test@qq.com");
        when(blacklistService.inBlacklist(anyString())).thenReturn(false);
        List<String> notInBlacklistAddress = mailUtils.checkNotInBlacklistAddress(toList,sendStatus);
        Assertions.assertFalse(sendStatus.getErrorMessage().contains("blacklist"));
        Assertions.assertFalse(notInBlacklistAddress.isEmpty());
    }
    @Test
    void testGetInternetAddress() throws UnsupportedEncodingException {
        List<String> notInBlacklistAddress = new ArrayList<>();
        notInBlacklistAddress.add("test@qq.com");
        InternetAddress[] result = mailUtils.getInternetAddress(notInBlacklistAddress);
        Assertions.assertEquals(notInBlacklistAddress.size(),result.length);
    }

    public void mockSlf4jLog(Object mockTo, Logger log) {
        try {
            Field logF = mockTo.getClass().getDeclaredField("log");
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(logF, logF.getModifiers() & ~Modifier.FINAL);
            logF.setAccessible(true);
            logF.set(mockTo, log);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    class sendEmailForProxyTest{
        private MailAccountDto parms;
        private List<String> adressees;
        private String title;
        private String content;
        @BeforeEach
        void beforeEach(){
            parms = mock(MailAccountDto.class);
            adressees = new ArrayList<>();
            adressees.add("test@tapdata.io");
            title = "【TAPDATA】";
            content = "test content";
            when(parms.getUser()).thenReturn("test@tapdata.io");
            when(parms.getPass()).thenReturn("testPasswd");
            when(parms.getHost()).thenReturn("test@tapdata.io");
            when(parms.getPort()).thenReturn(465);
            when(parms.getFrom()).thenReturn("from@tapdata.io");
        }
        @Test
        @DisplayName("test sendEmailSmtp method normal")
        void test1(){
            try (MockedStatic<Transport> mb = Mockito
                    .mockStatic(Transport.class)) {
                when(parms.getProtocol()).thenReturn("SSL");
                mb.when(()->Transport.send(any(MimeMessage.class))).thenAnswer(invocationOnMock -> {return null;});
                MailUtils.sendHtmlEmail(parms, adressees, title, content);
                mb.verify(() -> Transport.send(any(MimeMessage.class)),new Times(1));
            }
        }
        @Test
        @DisplayName("test sendEmailSmtp method with exception")
        void test2(){
            Logger log;
            log = mock(Logger.class);
            mockSlf4jLog(mailUtils, log);
            try (MockedStatic<Transport> mb = Mockito
                    .mockStatic(Transport.class)) {
                RuntimeException e = new RuntimeException("test ex");
                mb.when(()->Transport.send(any(MimeMessage.class))).thenThrow(e);
                when(parms.getProtocol()).thenReturn("NO_PROTOCOL");
                mailUtils.sendHtmlEmail(parms, adressees, title, content);
                mb.verify(() -> Transport.send(any(MimeMessage.class)),new Times(1));
                verify(log).error("mail send error：{}", "test ex", e);
            }
        }
    }


}
