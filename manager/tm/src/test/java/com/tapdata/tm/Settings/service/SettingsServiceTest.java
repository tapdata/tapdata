package com.tapdata.tm.Settings.service;

import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.SendMailResponseDto;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.dto.TestMailDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.repository.SettingsRepository;
import com.tapdata.tm.alarmMail.dto.AlarmMailDto;
import com.tapdata.tm.alarmMail.service.AlarmMailService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SettingsServiceTest {
    private SettingsServiceImpl settingsService;

    private MongoTemplate mongoTemplate;

    private UserService userService;
    private UserDetail userDetail;

    private AlarmMailService alarmMailService;

    private SettingsRepository mockSettingsRepository;
    @Nested
    class getMailAccountTest{
        @BeforeEach
        void beforeEach(){
            settingsService=spy(SettingsServiceImpl.class);
            mongoTemplate = mock(MongoTemplate.class);
            mockSettingsRepository = mock(SettingsRepository.class);
            userService = mock(UserService.class);
            alarmMailService = mock(AlarmMailService.class);
            settingsService.setMongoTemplate(mongoTemplate);
            settingsService.setSettingsRepository(mockSettingsRepository);
            ReflectionTestUtils.setField(settingsService,"alarmMailService",alarmMailService);
            userDetail = new UserDetail("123", "customerId", "username", "password", "customerType",
                    "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
            userDetail.setEmail("test@tapdata.io");
        }
        @Test
        void testGetMailAccount(){
            try (MockedStatic<SpringContextHelper> springContextHelperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)){
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(UserService.class)).thenReturn(userService);
                ArrayList<Settings> settingsArr = new ArrayList<>();
                Settings settings = new Settings();
                settings.setKey("smtp.server.host");
                settings.setValue("192.168.1.1");
                Settings settings1 =new Settings();
                settings1.setKey("email.receivers");
                settings1.setValue("test");
                settingsArr.add(settings1);
                settingsArr.add(settings);
                when(mongoTemplate.find(any(),eq(Settings.class))).thenReturn(settingsArr);
                MailAccountDto mailAccount = settingsService.getMailAccount("123");
                System.out.println(mailAccount);
                assertEquals("192.168.1.1",mailAccount.getHost());
                assertEquals("test",mailAccount.getReceivers().get(0));
            }
        }
        @Test
        void testGetMailAccountWithProxy(){
            ArrayList<Settings> settingsArr = new ArrayList<>();
            Settings settings = new Settings();
            settings.setKey("smtp.server.host");
            settings.setValue("192.168.1.1");
            Settings settings1 = new Settings();
            settings1.setKey("email.receivers");
            settings1.setValue("test");
            Settings settings2 = new Settings();
            settings2.setKey("smtp.proxy.host");
            settings2.setValue("smtp.proxy.cn");
            Settings settings3 = new Settings();
            settings3.setKey("smtp.proxy.port");
            settings3.setValue("1025");
            settingsArr.add(settings);
            settingsArr.add(settings1);
            settingsArr.add(settings2);
            settingsArr.add(settings3);
            when(mongoTemplate.find(any(),eq(Settings.class))).thenReturn(settingsArr);
            MailAccountDto mailAccount = settingsService.getMailAccount("123");
            assertEquals("smtp.proxy.cn",mailAccount.getProxyHost());
            assertEquals(1025,mailAccount.getProxyPort());
        }
        @Test
        void testGetMailAccountCloud(){
            try (MockedStatic<SpringContextHelper> springContextHelperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                springContextHelperMockedStatic.when(() -> SpringContextHelper.getBean(UserService.class)).thenReturn(userService);
                ArrayList<Settings> settingsArr = new ArrayList<>();
                Settings settings = new Settings();
                settings.setKey("smtp.server.host");
                settings.setValue("192.168.1.1");
                settingsArr.add(settings);
                Query query = Query.query(Criteria.where("category").is("System"));
                query.addCriteria(Criteria.where("key").is("buildProfile"));
                Settings systemSetting=new Settings();
                systemSetting.setCategory("System");
                systemSetting.setKey("buildProfile");
                systemSetting.setValue("CLOUD");
                when(mongoTemplate.find(any(),eq(Settings.class))).thenReturn(settingsArr);
                when(mongoTemplate.find(query,Settings.class)).thenReturn(Collections.singletonList(systemSetting));
                when(userService.loadUserById(MongoUtils.toObjectId("123"))).thenReturn(userDetail);
                MailAccountDto mailAccount = settingsService.getMailAccount("123");
                assertEquals("192.168.1.1",mailAccount.getHost());
                assertEquals("test@tapdata.io",mailAccount.getReceivers().get(0));
            }
        }

        @Test
        void testGetMailAccountCloud_EmailAddressListNotNull(){
            try (MockedStatic<SpringContextHelper> springContextHelperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                springContextHelperMockedStatic.when(() -> SpringContextHelper.getBean(UserService.class)).thenReturn(userService);
                ArrayList<Settings> settingsArr = new ArrayList<>();
                Settings settings = new Settings();
                settings.setKey("smtp.server.host");
                settings.setValue("192.168.1.1");
                settingsArr.add(settings);
                Query query = Query.query(Criteria.where("category").is("System"));
                query.addCriteria(Criteria.where("key").is("buildProfile"));
                Settings systemSetting=new Settings();
                systemSetting.setCategory("System");
                systemSetting.setKey("buildProfile");
                systemSetting.setValue("CLOUD");
                when(mongoTemplate.find(any(),eq(Settings.class))).thenReturn(settingsArr);
                when(mongoTemplate.find(query,Settings.class)).thenReturn(Collections.singletonList(systemSetting));
                when(userService.loadUserById(MongoUtils.toObjectId("123"))).thenReturn(userDetail);
                AlarmMailDto alarmMailDto = new AlarmMailDto();
                alarmMailDto.setEmailAddressList(Arrays.asList("test@qq.com"));
                when(alarmMailService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(alarmMailDto);
                MailAccountDto mailAccount = settingsService.getMailAccount("123");
                assertEquals(2,mailAccount.getReceivers().size());
            }
        }

        @Test
        void testFindALl_isDFS() {
            final Filter filter = new Filter();
            final Settings settings = new Settings();
            settings.setKey("buildProfile");
            settings.setValue("CLOUD");
            List<Settings> list = new ArrayList<>();
            list.add(settings);
            when(mongoTemplate.find(any(Query.class), eq(Settings.class))).thenReturn(list);
            final List<SettingsDto> result = settingsService.findALl("decode", filter);
            assertThat(result.get(0).getValue()).isEqualTo(settings.getValue());
        }

        @Test
        void testFindALl_isDASS() {
            final Filter filter = new Filter();
            final Settings settings = new Settings();
            settings.setKey("buildProfile");
            settings.setValue("DASS");
            List<Settings> list = new ArrayList<>();
            list.add(settings);
            when(mongoTemplate.find(any(Query.class), eq(Settings.class))).thenReturn(list);
            when(mockSettingsRepository.findAll()).thenReturn(list);
            final List<SettingsDto> result = settingsService.findALl("decode", filter);
            assertThat(result.get(0).getValue()).isEqualTo(settings.getValue());
        }
    }
    @Nested
    class getMailAccountWithTestMailDtoTest{
        private TestMailDto testMailDto;
        @BeforeEach
        void beforeEach(){
            settingsService = mock(SettingsServiceImpl.class);
            testMailDto = new TestMailDto();
            testMailDto.setSMTP_Server_Host("smtp.test.cn");
            testMailDto.setEmail_Communication_Protocol("SSH");
            testMailDto.setSMTP_Server_Port("");
            testMailDto.setEmail_Send_Address("test@tapdata.io");
            testMailDto.setSMTP_Server_User("test@tapdata.io");
            testMailDto.setSMTP_Server_password("test_passwd");
            testMailDto.setEmail_Receivers("test1@tapdata.io,test2@tapdata.io");
        }
        @Test
        void testGetMailAccountForProxy(){
            testMailDto.setSMTP_Proxy_Host("smtp.proxy.cn");
            testMailDto.setSMTP_Proxy_Port("1025");
            doCallRealMethod().when(settingsService).getMailAccount(testMailDto);
            MailAccountDto actual = settingsService.getMailAccount(testMailDto);
            assertNotNull(actual.getProxyHost());
            assertNotNull(actual.getProxyPort());
        }
        @Test
        void testGetMailAccountWithoutProxy(){
            doCallRealMethod().when(settingsService).getMailAccount(testMailDto);
            MailAccountDto actual = settingsService.getMailAccount(testMailDto);
            assertNull(actual.getProxyHost());
            assertEquals(0, actual.getProxyPort());
        }
    }
    @Nested
    class testSendMailTest{
        @Test
        void testSendMailNormal(){
            settingsService = mock(SettingsServiceImpl.class);
            try (MockedStatic<SettingUtil> mb = Mockito
                    .mockStatic(SettingUtil.class)) {
                mb.when(()->SettingUtil.getValue(anyString(),anyString())).thenReturn("123456");
                TestMailDto testMailDto = mock(TestMailDto.class);
                MailAccountDto mailAccountDto = mock(MailAccountDto.class);
                when(mailAccountDto.getPass()).thenReturn("*****");
                when(settingsService.getMailAccount(testMailDto)).thenReturn(mailAccountDto);
                doCallRealMethod().when(settingsService).testSendMail(testMailDto);
                SendMailResponseDto actual = settingsService.testSendMail(testMailDto);
                assertEquals(false,actual.isResult());
            }
        }
    }
}
