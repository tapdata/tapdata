package com.tapdata.tm.utils;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.service.BlacklistServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.*;
@ExtendWith(MockitoExtension.class)
class MailUtilsTest {
    @Mock
    BlacklistServiceImpl blacklistService;
    @Mock
    SettingsServiceImpl settingsService;

    @InjectMocks
    MailUtilsImpl mailUtils;

    @BeforeEach
    void setup(){

    }

    @Test
    void testReadHtmlToString() {
        Assertions.assertNotEquals(MailUtilsImpl.readHtmlToString("mailTemplate.html"),"");
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


}
