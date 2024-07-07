package com.tapdata.tm.utils;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.service.BlacklistService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.SendStatus;
import io.tapdata.entity.error.CoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
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



    @Nested
    class SendValidateCodeForResetPWDTest {
        private String host;
        private Integer port;

        private String user;
        private String sendAddress;

        private String password;
        Transport transport;

        MailUtils mu;
        @BeforeEach
        void init() throws MessagingException {
            host = "127.0.0.1";
            port = 9999;
            user = "tapdata";
            sendAddress = "127.0.0.1";
            password = "123456";

            mu = mock(MailUtils.class);
            ReflectionTestUtils.setField(mu, "host", host);
            ReflectionTestUtils.setField(mu, "port", port);
            ReflectionTestUtils.setField(mu, "user", user);
            ReflectionTestUtils.setField(mu, "sendAddress", sendAddress);
            ReflectionTestUtils.setField(mu, "password", password);

            transport = mock(Transport.class);
            doNothing().when(transport).connect(anyString(), anyInt(), anyString(), anyString());

            doNothing().when(mu).initMailConfig();
            when(mu.sendValidateCodeForResetPWD(anyString(), anyString(), anyString())).thenCallRealMethod();
        }

        @Test
        void testSendValidateCodeForResetPWD() throws MessagingException {
            doNothing().when(transport).close();
            doNothing().when(transport).sendMessage(any(MimeMessage.class), any(Address[].class));
            Session session = mock(Session.class);
            InternetAddress[] internetAddressList = new InternetAddress[0];
            Address[] addresses = new Address[0];

            doNothing().when(session).setDebug(true);
            when(session.getTransport("smtp")).thenReturn(transport);
            try(MockedStatic<Session> s = mockStatic(Session.class);
                MockedConstruction<InternetAddress> i = mockConstruction(InternetAddress.class, (ic, c) -> {});
                MockedConstruction<MimeMessage> m = mockConstruction(MimeMessage.class, (mk,c) -> {
                    doNothing().when(mk).setFrom(any(InternetAddress.class));
                    doNothing().when(mk).setRecipients(Message.RecipientType.TO, internetAddressList);
                    doNothing().when(mk).setContent(anyString(), anyString());
                    doNothing().when(mk).setSentDate(any(Date.class));
                    doNothing().when(mk).saveChanges();
                    when(mk.getAllRecipients()).thenReturn(addresses);
                })) {
                s.when(() -> Session.getDefaultInstance(any(Properties.class))).thenReturn(session);
                SendStatus sendStatus = mu.sendValidateCodeForResetPWD("", "", "");
                Assertions.assertNotNull(sendStatus);
            }
        }

        @Test
        void testSendValidateCodeForResetPWDException() throws MessagingException {
            doNothing().when(transport).close();
            Session session = mock(Session.class);
            InternetAddress[] internetAddressList = new InternetAddress[0];
            Address[] addresses = new Address[0];

            doAnswer(a -> {
                throw new IOException("");
            }).when(transport).sendMessage(any(MimeMessage.class), any(Address[].class));

            doNothing().when(session).setDebug(true);
            when(session.getTransport("smtp")).thenReturn(transport);
            try(MockedStatic<Session> s = mockStatic(Session.class);
                MockedConstruction<InternetAddress> i = mockConstruction(InternetAddress.class, (ic, c) -> {});
                MockedConstruction<MimeMessage> m = mockConstruction(MimeMessage.class, (mk,c) -> {
                    doNothing().when(mk).setFrom(any(InternetAddress.class));
                    doNothing().when(mk).setRecipients(Message.RecipientType.TO, internetAddressList);
                    doNothing().when(mk).setContent(anyString(), anyString());
                    doNothing().when(mk).setSentDate(any(Date.class));
                    doNothing().when(mk).saveChanges();
                    when(mk.getAllRecipients()).thenReturn(addresses);
                })) {
                s.when(() -> Session.getDefaultInstance(any(Properties.class))).thenReturn(session);
                SendStatus sendStatus = mu.sendValidateCodeForResetPWD("", "", "");
                Assertions.assertNotNull(sendStatus);
            }
        }
        @Test
        void testSendValidateCodeForResetPWDCloseException() throws MessagingException {
            doAnswer(a -> {
                throw new MessagingException("");
            }).when(transport).close();
            doNothing().when(transport).sendMessage(any(MimeMessage.class), any(Address[].class));
            Session session = mock(Session.class);
            InternetAddress[] internetAddressList = new InternetAddress[0];
            Address[] addresses = new Address[0];
            doNothing().when(session).setDebug(true);
            when(session.getTransport("smtp")).thenReturn(transport);
            try(MockedStatic<Session> s = mockStatic(Session.class);
                MockedConstruction<InternetAddress> i = mockConstruction(InternetAddress.class, (ic, c) -> {});
                MockedConstruction<MimeMessage> m = mockConstruction(MimeMessage.class, (mk,c) -> {
                    doNothing().when(mk).setFrom(any(InternetAddress.class));
                    doNothing().when(mk).setRecipients(Message.RecipientType.TO, internetAddressList);
                    doNothing().when(mk).setContent(anyString(), anyString());
                    doNothing().when(mk).setSentDate(any(Date.class));
                    doNothing().when(mk).saveChanges();
                    when(mk.getAllRecipients()).thenReturn(addresses);
                })) {
                s.when(() -> Session.getDefaultInstance(any(Properties.class))).thenReturn(session);
                SendStatus sendStatus = mu.sendValidateCodeForResetPWD("", "", "");
                Assertions.assertNotNull(sendStatus);
            }
        }
    }
}
