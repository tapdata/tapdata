package com.tapdata.tm.events.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.entity.Events;
import com.tapdata.tm.events.entity.FailResult;
import com.tapdata.tm.events.repository.EventsRepository;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private EventsServiceImpl eventsServiceUnderTest;

    private UserDetail userDetail;

    @BeforeEach
    void setUp() {
        new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
        eventsServiceUnderTest = new EventsServiceImpl(mockRepository);
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

    @Test
    void testExcludeDeliveredRemovesAlreadyNotifiedAddresses() {
        FailResult failResult = new FailResult(1L, 0L, "partial", 0L,
                Arrays.asList("ok1@tapdata.io", "ok2@tapdata.io"));

        String remaining = EventsServiceImpl.excludeDelivered(
                "ok1@tapdata.io,busy@tapdata.io,ok2@tapdata.io", failResult);

        // 已送达的两人不再重发，只对临时失败地址重试
        assertEquals("busy@tapdata.io", remaining);
    }

    @Test
    void testExcludeDeliveredKeepsReceiversWhenNothingToExclude() {
        assertEquals("a@tapdata.io,b@tapdata.io",
                EventsServiceImpl.excludeDelivered("a@tapdata.io,b@tapdata.io", null));
        assertEquals("a@tapdata.io,b@tapdata.io",
                EventsServiceImpl.excludeDelivered("a@tapdata.io,b@tapdata.io",
                        new FailResult(1L, 0L, "failed", 0L)));
    }

    @Test
    void testExcludeDeliveredKeepsAllWhenEveryAddressAlreadyDelivered() {
        FailResult failResult = new FailResult(1L, 0L, "partial", 0L,
                Arrays.asList("a@tapdata.io", "b@tapdata.io"));

        // 全部已送达时不能返回空收件人，否则重试会变成无收件人发送
        assertEquals("a@tapdata.io,b@tapdata.io",
                EventsServiceImpl.excludeDelivered("a@tapdata.io,b@tapdata.io", failResult));
    }

    @Test
    void testBuildFailResultCarriesDeliveredAddressesOnlyWhenRetrying() {
        SendStatus partial = new SendStatus("false", "partial");
        partial.setDeliveredAddresses(Set.of("ok@tapdata.io"));

        FailResult retrying = EventsServiceImpl.buildFailResult(partial, 1);
        assertEquals("false", retrying.getRetry() == null ? null : partial.getStatus());
        assertEquals(List.of("ok@tapdata.io"), retrying.getDelivered_addresses());
        assertNotNull(retrying.getNext_retry());

        // 整体成功不登记重试信息，也不需要排除地址
        SendStatus success = new SendStatus("true", null);
        success.setDeliveredAddresses(Set.of("ok@tapdata.io"));
        FailResult ok = EventsServiceImpl.buildFailResult(success, 1);
        assertEquals(0L, ok.getRetry());
        assertTrue(ok.getDelivered_addresses().isEmpty());
    }
}
