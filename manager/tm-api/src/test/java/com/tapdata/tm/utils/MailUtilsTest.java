package com.tapdata.tm.utils;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.service.BlacklistService;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
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
        //doCallRealMethod().when(mailUtils).sendEmail(any(Document.class), any(SendStatus.class), anyList(), anyString(), anyString());
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
    void testSendHtmlMail_subjectNotNull_mailContentIsNull(){
        String mailContent = null;
        when(blacklistService.inBlacklist(anyString())).thenReturn(false);
        List<String> addressList = new ArrayList<>();
        addressList.add("test@qq.com");
        SendStatus result = mailUtils.sendHtmlMail("",addressList,"test","test","test",mailContent);
        Assertions.assertEquals("false",result.getStatus());
        Assertions.assertTrue(result.getErrorMessage().contains("mailContent"));
    }
    @Test
    void testSendHtmlMail_subjectNotNull_mailContentIsEmpty(){
        String mailContent = "";
        when(blacklistService.inBlacklist(anyString())).thenReturn(false);
        List<String> addressList = new ArrayList<>();
        addressList.add("test@qq.com");
        SendStatus result = mailUtils.sendHtmlMail("",addressList,"test","test","test",mailContent);
        Assertions.assertEquals("false",result.getStatus());
        Assertions.assertTrue(result.getErrorMessage().contains("mailContent"));
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
        Logger log;
        @BeforeEach
        void init() throws MessagingException, UnsupportedEncodingException {
            host = "127.0.0.1";
            port = 9999;
            user = "tapdata";
            sendAddress = "127.0.0.1";
            password = "123456";
            log = mock(Logger.class);

            mu = mock(MailUtils.class);
            ReflectionTestUtils.setField(mu, "host", host);
            ReflectionTestUtils.setField(mu, "port", port);
            ReflectionTestUtils.setField(mu, "user", user);
            ReflectionTestUtils.setField(mu, "sendAddress", sendAddress);
            ReflectionTestUtils.setField(mu, "password", password);

            transport = mock(Transport.class);
            mockSlf4jLog(mu, log);

            when(mu.emailSession()).thenCallRealMethod();
            when(mu.message(any(Session.class), anyList(), anyString(), anyString())).thenCallRealMethod();
            doCallRealMethod().when(mu).sendEmail(any(Document.class), any(SendStatus.class), anyList(), anyString(), anyString());
            when(mu.sendValidateCodeForResetPWD(anyString(), anyString(), anyString())).thenCallRealMethod();
        }

        @Test
        void testSendValidateCodeForResetPWD() throws MessagingException {
            when(mu.connectSMTP(any(Session.class))).thenCallRealMethod();
            doCallRealMethod().when(mu).closeTransport(any(Transport.class));
            doNothing().when(transport).connect(anyString(), anyInt(), anyString(), anyString());
            doNothing().when(mu).initMailConfig();
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
            doNothing().when(log).error(anyString(), any(Exception.class));
            when(mu.connectSMTP(any(Session.class))).thenCallRealMethod();
            doCallRealMethod().when(mu).closeTransport(any(Transport.class));
            doNothing().when(transport).connect(anyString(), anyInt(), anyString(), anyString());
            doNothing().when(mu).initMailConfig();
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
            when(mu.connectSMTP(any(Session.class))).thenCallRealMethod();
            doCallRealMethod().when(mu).closeTransport(any(Transport.class));
            doNothing().when(transport).connect(anyString(), anyInt(), anyString(), anyString());
            doNothing().when(mu).initMailConfig();
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

        @Test
        void testSendValidateCodeForResetPWDTransportIsNull() throws MessagingException {
            doNothing().when(log).error(anyString(), any(Exception.class));
            doCallRealMethod().when(mu).closeTransport(null);
            doNothing().when(mu).initMailConfig();
            Session session = mock(Session.class);
            InternetAddress[] internetAddressList = new InternetAddress[0];
            Address[] addresses = new Address[0];
            doNothing().when(session).setDebug(true);
            try(MockedStatic<Session> s = mockStatic(Session.class);
                MockedConstruction<InternetAddress> i = mockConstruction(InternetAddress.class, (ic, c) -> {});
                MockedConstruction<MimeMessage> m = mockConstruction(MimeMessage.class, (mk,c) -> {
                    doNothing().when(mk).setFrom(any(InternetAddress.class));
                    doNothing().when(mk).setRecipients(Message.RecipientType.TO, internetAddressList);
                    doNothing().when(mk).setContent(anyString(), anyString());
                    doNothing().when(mk).setSentDate(any(Date.class));
                    doAnswer(a -> {
                        throw new MessagingException("");
                    }).when(mk).saveChanges();
                    when(mk.getAllRecipients()).thenReturn(addresses);
                })) {
                s.when(() -> Session.getDefaultInstance(any(Properties.class))).thenReturn(session);
                SendStatus sendStatus = mu.sendValidateCodeForResetPWD("", "", "");
                Assertions.assertNotNull(sendStatus);
            }
        }

        @Test
        void testNotPort() throws MessagingException {
            when(mu.connectSMTP(any(Session.class))).thenCallRealMethod();
            doCallRealMethod().when(mu).closeTransport(any(Transport.class));
            ReflectionTestUtils.setField(mu, "port", null);
            doNothing().when(transport).connect(anyString(), anyString(), anyString());
            doNothing().when(mu).initMailConfig();
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
    class sendHtmlEmailTest{
        private MailAccountDto parms;
        private List<String> adressees;
        private String title;
        private String content;
        private Logger log;
        @BeforeEach
        void beforeEach(){
            parms = mock(MailAccountDto.class);
            adressees = new ArrayList<>();
            adressees.add("test@tapdata.io");
            title = "【TAPDATA】";
            content = "test content";
            log = mock(Logger.class);
            mockSlf4jLog(mailUtils, log);
        }
        @Test
        @DisplayName("test sendHtmlEmail method when addresses is null")
        void test1(){
            try (MockedStatic<StringUtils> mb = Mockito
                    .mockStatic(StringUtils.class)) {
                mb.when(()->StringUtils.isAnyBlank(anyString(),anyString(),anyString(),anyString())).thenReturn(false);
                adressees.clear();
                mailUtils.sendHtmlEmail(parms, adressees, title, content);
                mb.verify(() -> StringUtils.isAnyBlank(anyString(),anyString(),anyString(),anyString()),new Times(0));
            }
        }
        @Test
        @DisplayName("test sendHtmlEmail method when host is blank")
        void test2(){
            when(parms.getHost()).thenReturn("  ");
            mailUtils.sendHtmlEmail(parms, adressees, title, content);
            verify(log).error("mail account info empty, params:{}", JSON.toJSONString(parms));
        }
        @Test
        @DisplayName("test sendHtmlEmail method for proxy")
        void test3(){
            try (MockedStatic<Session> mb = Mockito
                    .mockStatic(Session.class)) {
                mb.when(()->Session.getInstance(any(Properties.class),any(Authenticator.class))).thenReturn(null);
                when(parms.getUser()).thenReturn("test@tapdata.io");
                when(parms.getPass()).thenReturn("testPasswd");
                when(parms.getHost()).thenReturn("test@tapdata.io");
                when(parms.getPort()).thenReturn(465);
                when(parms.getProxyHost()).thenReturn("smtp.test.cn");
                when(parms.getProxyPort()).thenReturn(1025);
                when(parms.getFrom()).thenReturn("from@tapdata.io");
                MailUtils.sendHtmlEmail(parms, adressees, title, content);
                mb.verify(() -> Session.getInstance(any(Properties.class),any(Authenticator.class)),new Times(1));
            }
        }
    }
    @Nested
    class filterBlackListTest{
        private List<String> addresses;
        @BeforeEach
        void beforeEach(){
            addresses = new ArrayList<>();
            addresses.add("test1@tapdata.io");
            addresses.add("test2@tapdata.io");
        }
        @Test
        @DisplayName("test filterBlackList method when address is null")
        void test1(){
            addresses.clear();
            List<String> actual = MailUtils.filterBlackList(addresses);
            assertNull(actual);
        }
        @Test
        @DisplayName("test filterBlackList method when blacklistService is not null")
        void test2(){
            try (MockedStatic<SpringContextHelper> mb = Mockito
                    .mockStatic(SpringContextHelper.class)) {
                BlacklistService blacklistService = mock(BlacklistService.class);
                mb.when(()->SpringContextHelper.getBean(BlacklistService.class)).thenReturn(blacklistService);

                when(blacklistService.inBlacklist("test1@tapdata.io")).thenReturn(true);
                List<String> actual = MailUtils.filterBlackList(addresses);
                String expected = "test2@tapdata.io";
                assertEquals(expected, actual.get(0));
            }

        }
        @Test
        @DisplayName("test filterBlackList method when blacklistService is not null and address all in black list")
        void test3(){
            try (MockedStatic<SpringContextHelper> mb = Mockito
                    .mockStatic(SpringContextHelper.class)) {
                BlacklistService blacklistService = mock(BlacklistService.class);
                mb.when(()->SpringContextHelper.getBean(BlacklistService.class)).thenReturn(blacklistService);

                when(blacklistService.inBlacklist("test1@tapdata.io")).thenReturn(true);
                when(blacklistService.inBlacklist("test2@tapdata.io")).thenReturn(true);
                List<String> actual = MailUtils.filterBlackList(addresses);
                assertNull(actual);
            }

        }
        @Test
        @DisplayName("test filterBlackList method when blacklistService is null")
        void test4(){
            List<String> actual = MailUtils.filterBlackList(addresses);
            assertEquals(addresses, actual);
        }
    }
    @Nested
    class sendEmailForProxyTest{
        private MailAccountDto parms;
        private List<String> adressees;
        private String title;
        private String content;
        private boolean flag;
        @BeforeEach
        void beforeEach(){
            parms = mock(MailAccountDto.class);
            adressees = new ArrayList<>();
            adressees.add("test@tapdata.io");
            title = "【TAPDATA】";
            content = "test content";
            flag = true;
            when(parms.getUser()).thenReturn("test@tapdata.io");
            when(parms.getPass()).thenReturn("testPasswd");
            when(parms.getHost()).thenReturn("test@tapdata.io");
            when(parms.getPort()).thenReturn(465);
            when(parms.getProxyHost()).thenReturn("smtp.test.cn");
            when(parms.getProxyPort()).thenReturn(1025);
            when(parms.getFrom()).thenReturn("from@tapdata.io");
        }
        @Test
        @DisplayName("test sendEmailForProxy method normal")
        void test1(){
            try (MockedStatic<Transport> mb = Mockito
                    .mockStatic(Transport.class)) {
                when(parms.getProtocol()).thenReturn("SSL");
                mb.when(()->Transport.send(any(MimeMessage.class))).thenAnswer(invocationOnMock -> {return null;});
                MailUtils.sendEmailForProxy(parms, adressees, title, content, flag);
                mb.verify(() -> Transport.send(any(MimeMessage.class)),new Times(1));
            }
        }
        @Test
        @DisplayName("test sendEmailForProxy method with exception")
        void test2(){
            Logger log;
            log = mock(Logger.class);
            mockSlf4jLog(mailUtils, log);
            try (MockedStatic<Transport> mb = Mockito
                    .mockStatic(Transport.class)) {
                RuntimeException e = new RuntimeException("test ex");
                mb.when(()->Transport.send(any(MimeMessage.class))).thenThrow(e);
                when(parms.getProtocol()).thenReturn("NO_PROTOCOL");
                mailUtils.sendEmailForProxy(parms, adressees, title, content, flag);
                mb.verify(() -> Transport.send(any(MimeMessage.class)),new Times(1));
                verify(log).error("mail send error：{}", "test ex", e);
            }
        }
    }
}
