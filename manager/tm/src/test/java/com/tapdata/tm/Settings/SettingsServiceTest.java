package com.tapdata.tm.Settings;

import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.repository.SettingsRepository;
import com.tapdata.tm.Settings.service.SettingsService;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class SettingsServiceTest {
    private SettingsService settingsService;

    private MongoTemplate mongoTemplate;

    private UserService userService;
    private UserDetail userDetail;
    private SettingsRepository mockSettingsRepository;
    @Nested
    class getMailAccountTest{
        @BeforeEach
        void beforeEach(){
            settingsService=spy(SettingsService.class);
            mongoTemplate = mock(MongoTemplate.class);
            mockSettingsRepository = mock(SettingsRepository.class);
            userService = mock(UserService.class);
            settingsService.setMongoTemplate(mongoTemplate);
            settingsService.setSettingsRepository(mockSettingsRepository);
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
}
