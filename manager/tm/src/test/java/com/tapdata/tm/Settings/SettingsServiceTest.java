package com.tapdata.tm.Settings;

import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarmMail.dto.AlarmMailDto;
import com.tapdata.tm.alarmMail.service.AlarmMailService;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SettingsServiceTest {
    private SettingsService settingsService;

    private MongoTemplate mongoTemplate;

    private UserService userService;
    private UserDetail userDetail;

    private AlarmMailService alarmMailService;
    @Nested
    class getMailAccountTest{
        @BeforeEach
        void beforeEach(){
            settingsService=spy(SettingsService.class);
            mongoTemplate = mock(MongoTemplate.class);
            userService = mock(UserService.class);
            alarmMailService = mock(AlarmMailService.class);
            settingsService.setMongoTemplate(mongoTemplate);
            settingsService.setUserService(userService);
            ReflectionTestUtils.setField(settingsService,"alarmMailService",alarmMailService);
            userDetail = new UserDetail("123", "customerId", "username", "password", "customerType",
                    "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
            userDetail.setEmail("test@tapdata.io");
        }
        @Test
        void testGetMailAccount(){
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
        @Test
        void testGetMailAccountCloud(){
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

        @Test
        void testGetMailAccountCloud_EmailAddressListNotNull(){
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
}
