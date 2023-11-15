package com.tapdata.tm.alarm.service.impl;

import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.dto.AlarmMessageDto;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.service.EventsService;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.mp.service.MpService;
import com.tapdata.tm.sms.SmsService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.CloudMailLimitUtils;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.mockito.Mockito.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class AlarmServiceImplTest {

    @Mock
    private MongoTemplate mockMongoTemplate;
    @Mock
    private TaskService mockTaskService;
    @Mock
    private InspectService mockInspectService;
    @Mock
    private AlarmSettingService mockAlarmSettingService;
    @Mock
    private MessageService mockMessageService;
    @Mock
    private SettingsService mockSettingsService;
    @Mock
    private UserService mockUserService;
    @Mock
    private SmsService mockSmsService;
    @Mock
    private MpService mockMpService;
    @Mock
    private MailUtils mockMailUtils;
    @Mock
    private EventsService mockEventsService;

    private AlarmServiceImpl alarmServiceImplUnderTest;

    private Method sendMail;


    @BeforeEach
    void setUp() throws NoSuchMethodException {
        alarmServiceImplUnderTest = new AlarmServiceImpl();
        alarmServiceImplUnderTest.setMongoTemplate(mockMongoTemplate);
        alarmServiceImplUnderTest.setTaskService(mockTaskService);
        alarmServiceImplUnderTest.setInspectService(mockInspectService);
        alarmServiceImplUnderTest.setAlarmSettingService(mockAlarmSettingService);
        alarmServiceImplUnderTest.setMessageService(mockMessageService);
        alarmServiceImplUnderTest.setSettingsService(mockSettingsService);
        alarmServiceImplUnderTest.setUserService(mockUserService);
        alarmServiceImplUnderTest.setSmsService(mockSmsService);
        alarmServiceImplUnderTest.setMpService(mockMpService);
        alarmServiceImplUnderTest.setMailUtils(mockMailUtils);
        alarmServiceImplUnderTest.setEventsService(mockEventsService);
        alarmServiceImplUnderTest.setEnableSms(false);
        // 获取类的Class对象
        Class<?> myClass = AlarmServiceImpl.class;
        sendMail = myClass.getDeclaredMethod("sendMail", AlarmInfo.class, AlarmMessageDto.class, UserDetail.class,MessageDto.class, String.class);
        sendMail.setAccessible(true);
    }
    @Test
    void testSendMail() throws InvocationTargetException, IllegalAccessException {
         UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        userDetail.setEmail("test@test.com");
        AlarmMessageDto.builder().agentId("agentId").taskId("taskId").name("name").emailOpen(true);
        MessageDto messageDto = new MessageDto();
        messageDto.setMsg("connected");
        messageDto.setMessageMetadata("{\"name\":\"name\",\"id\":\"id\"}");
        AlarmInfo alarmInfo = new AlarmInfo();
        AlarmSettingDto alarmSettingDto = new AlarmSettingDto();
        alarmSettingDto.setKey(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_UP);
        alarmSettingDto.setOpen(true);
        alarmSettingDto.setNotify(Arrays.asList(NotifyEnum.EMAIL));
        List<AlarmSettingDto> all =  Arrays.asList(alarmSettingDto);
        when(mockSettingsService.isCloud()).thenReturn(true);
        when(mockAlarmSettingService.findAllAlarmSetting(any(UserDetail.class))).thenReturn(all);
        sendMail.invoke(alarmServiceImplUnderTest,alarmInfo,AlarmMessageDto.builder().agentId("agentId").taskId("taskId").name("name").emailOpen(true).build(),userDetail,messageDto,"messageId");
        verify(mockMessageService,times(1)).update(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId("messageId"))), Update.update("isSend",true));
    }

    @Test
    void testSendMail_sendingEmailLimit() throws InvocationTargetException, IllegalAccessException {
        UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        userDetail.setEmail("test@test.com");
        AlarmMessageDto.builder().agentId("agentId").taskId("taskId").name("name").emailOpen(true);
        MessageDto messageDto = new MessageDto();
        messageDto.setMsg("connected");
        messageDto.setMessageMetadata("{\"name\":\"name\",\"id\":\"id\"}");
        AlarmInfo alarmInfo = new AlarmInfo();
        AlarmSettingDto alarmSettingDto = new AlarmSettingDto();
        alarmSettingDto.setKey(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_UP);
        alarmSettingDto.setOpen(true);
        alarmSettingDto.setNotify(Arrays.asList(NotifyEnum.EMAIL));
        List<AlarmSettingDto> all =  Arrays.asList(alarmSettingDto);
        when(mockSettingsService.isCloud()).thenReturn(true);
        when(mockAlarmSettingService.findAllAlarmSetting(any(UserDetail.class))).thenReturn(all);
        when(mockMessageService.checkMessageLimit(userDetail)).thenReturn(11L);
        sendMail.invoke(alarmServiceImplUnderTest,alarmInfo,AlarmMessageDto.builder().agentId("agentId").taskId("taskId").name("name").emailOpen(true).build(),userDetail,messageDto,"messageId");
        verify(mockMessageService,times(0)).update(Query.query(Criteria.where("_id").is(null)), Update.update("isSend",true));
    }

    @Test
    void testSendMail_sendingEmailBoundary() throws InvocationTargetException, IllegalAccessException {
        UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        userDetail.setEmail("test@test.com");
        AlarmMessageDto.builder().agentId("agentId").taskId("taskId").name("name").emailOpen(true);
        MessageDto messageDto = new MessageDto();
        messageDto.setMsg("connected");
        messageDto.setMessageMetadata("{\"name\":\"name\",\"id\":\"id\"}");
        AlarmInfo alarmInfo = new AlarmInfo();
        AlarmSettingDto alarmSettingDto = new AlarmSettingDto();
        alarmSettingDto.setKey(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_UP);
        alarmSettingDto.setOpen(true);
        alarmSettingDto.setNotify(Arrays.asList(NotifyEnum.EMAIL));
        List<AlarmSettingDto> all =  Arrays.asList(alarmSettingDto);

        when(mockSettingsService.isCloud()).thenReturn(true);
        when(mockAlarmSettingService.findAllAlarmSetting(any(UserDetail.class))).thenReturn(all);
        when(mockMessageService.checkMessageLimit(userDetail)).thenReturn(Long.valueOf(CloudMailLimitUtils.getCloudMailLimit()));
        sendMail.invoke(alarmServiceImplUnderTest,alarmInfo,AlarmMessageDto.builder().agentId("agentId").taskId("taskId").name("name").emailOpen(true).build(),userDetail,messageDto,"messageId");
        verify(mockMessageService,times(0)).update(Query.query(Criteria.where("_id").is(null)), Update.update("isSend",true));
    }
}
