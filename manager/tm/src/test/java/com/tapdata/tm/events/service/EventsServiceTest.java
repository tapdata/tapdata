package com.tapdata.tm.events.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.entity.Events;
import com.tapdata.tm.events.repository.EventsRepository;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.sms.SmsService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.SendStatus;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class EventsServiceTest {

    @Mock
    private EventsRepository mockRepository;
    @Mock
    private MailUtils mockMailUtils;
    @Mock
    private MessageService mockMessageService;
    @Mock
    private SettingsService mockSettingsService;
    @Mock
    private SmsService mockSmsService;

    private EventsService eventsServiceUnderTest;

    private UserDetail userDetail;

    @BeforeEach
    void setUp() {
        eventsServiceUnderTest = new EventsService(mockRepository);
        eventsServiceUnderTest.mailUtils = mockMailUtils;
        eventsServiceUnderTest.messageService = mockMessageService;
        eventsServiceUnderTest.settingsService = mockSettingsService;
        eventsServiceUnderTest.smsService = mockSmsService;
        userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        userDetail.setEmail("test@test.com");
    }

    @Test
    void testCompleteInform() {
        try(MockedStatic<DataPermissionService> mockedStatic = Mockito.mockStatic(DataPermissionService.class);
            MockedStatic<SpringContextHelper> springContextHelperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)){
            mockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
            UserService mockUserService = mock(UserService.class);
            springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(UserService.class)).thenReturn(mockUserService);
            when(mockUserService.loadUserById(any(ObjectId.class))).thenReturn(userDetail);
            final MessageDto messageDto = new MessageDto();
            messageDto.setId(new ObjectId(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(), 0));
            messageDto.setUserId("65c04b559df82a775efedd90");
            messageDto.setSystem("agent");
            messageDto.setMsg("connected");
            messageDto.setServerName("serverName");
            messageDto.setMessageMetadata("messageMetadata");
            Events event1 = new Events();
            event1.setType("job-operation-notice-email");
            event1.setSendGroupId("test1");
            event1.setReceivers("test@qq.com");
            List<Events> eventsList = new ArrayList<>();
            eventsList.add(event1);
            when(mockRepository.findAll(any(Query.class))).thenReturn(eventsList);
            when(mockMessageService.findById(anyString())).thenReturn(messageDto);
            when(mockMailUtils.getMailContent(any(SystemEnum.class),any(MsgTypeEnum.class))).thenReturn("test");
            when(mockMailUtils.sendHtmlMail(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(new SendStatus("true",""));
            UpdateResult result = new UpdateResult() {
                @Override
                public boolean wasAcknowledged() {
                    return false;
                }
                @Override
                public long getMatchedCount() {
                    return 1;
                }
                @Override
                public long getModifiedCount() {
                    return 0;
                }
                @Override
                public BsonValue getUpsertedId() {
                    return null;
                }
            };
            when(mockRepository.update(any(Query.class),any(Update.class))).thenReturn(result);
            eventsServiceUnderTest.completeInform();
            verify(mockMailUtils,times(1)).sendHtmlMail(anyString(), anyString(), anyString(), anyString(), anyString());
        }
    }

    @Test
    void testCompleteInform_EventsIsNull() {
        when(mockRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
        eventsServiceUnderTest.completeInform();
        verify(mockMailUtils,times(0)).sendHtmlMail(anyString(), anyString(), anyString(), anyString(), anyString());
    }
}