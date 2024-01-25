package com.tapdata.tm.alarm.service.impl;

import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.constant.AlarmMailTemplate;
import com.tapdata.tm.alarm.dto.AlarmMessageDto;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
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
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static com.tapdata.tm.commons.task.constant.AlarmKeyEnum.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

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
        when(mockSettingsService.getMailAccount(null)).thenReturn(mock(MailAccountDto.class));
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
        when(mockMessageService.checkMessageLimit(userDetail)).thenReturn(Long.valueOf(CommonUtils.getPropertyInt("cloud_mail_limit",10)));
        sendMail.invoke(alarmServiceImplUnderTest,alarmInfo,AlarmMessageDto.builder().agentId("agentId").taskId("taskId").name("name").emailOpen(true).build(),userDetail,messageDto,"messageId");
        verify(mockMessageService,times(0)).update(Query.query(Criteria.where("_id").is(null)), Update.update("isSend",true));
    }
    @Nested
    class CheckOpenTest{
        @Test
        @DisplayName("check open for task stop test")
        void test1(){
            TaskDto taskDto = new TaskDto();
            List<AlarmSettingVO> alarmSettings = new ArrayList<>();
            alarmSettings.add(mock(AlarmSettingVO.class));
            taskDto.setAlarmSettings(alarmSettings);
            taskDto.setDag(mock(DAG.class));
            String nodeId = "111";
            AlarmKeyEnum key = TASK_STATUS_STOP;
            NotifyEnum type = null;
            UserDetail userDetail = mock(UserDetail.class);
            List<AlarmSettingDto> settingDtos = new ArrayList<>();
            AlarmSettingDto alarmSettingDto = mock(AlarmSettingDto.class);
            when(alarmSettingDto.getKey()).thenReturn(key);
            when(alarmSettingDto.isOpen()).thenReturn(true);
            settingDtos.add(alarmSettingDto);
            when(mockAlarmSettingService.findAllAlarmSetting(userDetail)).thenReturn(settingDtos);
            boolean actual = alarmServiceImplUnderTest.checkOpen(taskDto, nodeId, key, type, userDetail);
            assertEquals(true,actual);
        }
        @Test
        @DisplayName("check open for task error test")
        void test2(){
            TaskDto taskDto = new TaskDto();
            List<AlarmSettingVO> alarmSettings = new ArrayList<>();
            alarmSettings.add(mock(AlarmSettingVO.class));
            taskDto.setAlarmSettings(alarmSettings);
            taskDto.setDag(mock(DAG.class));
            String nodeId = "111";
            AlarmKeyEnum key = TASK_STATUS_ERROR;
            NotifyEnum type = null;
            UserDetail userDetail = mock(UserDetail.class);
            boolean actual = alarmServiceImplUnderTest.checkOpen(taskDto, nodeId, key, type, userDetail);
            assertEquals(false,actual);
        }
    }
    @Nested
    class GetTaskTitleAndContentTest{
        private AlarmInfo info;
        private SimpleDateFormat dateFormat;
        private Date date;
        private String exceptedTitle;
        private String exceptedContent;
        private String exceptedSmsEvent;
        private Map<String, String> actual;
        @BeforeEach
        void buildInfo(){
            info = new AlarmInfo();
            info.setName("test task");
            date = new Date();
            info.setLastOccurrenceTime(date);
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        @AfterEach
        void buildAssertion(){
            assertEquals(exceptedTitle,actual.get("title"));
            assertEquals(exceptedContent,actual.get("content"));
            assertEquals(exceptedSmsEvent,actual.get("smsEvent"));
        }
        @Test
        @DisplayName("get task title and content for task stop")
        void test1(){
            info.setMetric(TASK_STATUS_STOP);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_MANUAL_TITLE, "test task");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_MANUAL, "test task", dateFormat.format(date));
            exceptedSmsEvent = "任务停止";
        }
        @Test
        @DisplayName("get task title and content for task error")
        void test2(){
            info.setMetric(TASK_STATUS_ERROR);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_ERROR_TITLE, "test task");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_ERROR, "test task", dateFormat.format(date));
            exceptedSmsEvent = "任务错误";
        }
        @Test
        @DisplayName("get task title and content for task full complete")
        void test3(){
            info.setMetric(TASK_FULL_COMPLETE);
            Map<String,Object> map = new HashMap<>();
            map.put("snapDoneDate","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.TASK_FULL_COMPLETE_TITLE, "test task");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.TASK_FULL_COMPLETE, "test task", "test");
            exceptedSmsEvent = "全量结束";
        }
        @Test
        @DisplayName("get task title and content for task full complete")
        void test4(){
            info.setMetric(TASK_INCREMENT_START);
            Map<String,Object> map = new HashMap<>();
            map.put("cdcTime","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.TASK_INCREMENT_START_TITLE, "test task");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.TASK_INCREMENT_START, "test task", "test");
            exceptedSmsEvent = "增量开始";
        }
        @Test
        @DisplayName("get task title and content for task increment delay")
        void test5(){
            info.setMetric(TASK_INCREMENT_DELAY);
            Map<String,Object> map = new HashMap<>();
            map.put("currentValue","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.TASK_INCREMENT_DELAY_START_TITLE, "test task");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.TASK_INCREMENT_DELAY_START, "test task", "test");
            exceptedSmsEvent = "增量延迟";
        }
        @Test
        @DisplayName("get task title and content for data node average handle consume")
        void test6(){
            info.setMetric(DATANODE_AVERAGE_HANDLE_CONSUME);
            Map<String,Object> map = new HashMap<>();
            map.put("currentValue","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.AVERAGE_HANDLE_CONSUME_TITLE, "test task");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.AVERAGE_HANDLE_CONSUME, "test task",null, "test",null,dateFormat.format(date));
            exceptedSmsEvent = "当前任务运行超过阈值";
        }
        @Test
        @DisplayName("get task title and content for process node average handle consume")
        void test7(){
            info.setMetric(PROCESSNODE_AVERAGE_HANDLE_CONSUME);
            Map<String,Object> map = new HashMap<>();
            map.put("currentValue","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.AVERAGE_HANDLE_CONSUME_TITLE, "test task");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.AVERAGE_HANDLE_CONSUME, "test task",null,null,"test",dateFormat.format(date));
            exceptedSmsEvent = "当前任务运行超过阈值";
        }
        @Test
        @DisplayName("get task title and content for inspect task error")
        void test8(){
            info.setMetric(INSPECT_TASK_ERROR);
            Map<String,Object> map = new HashMap<>();
            map.put("inspectName","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.INSPECT_TASK_ERROR_TITLE, "test");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.INSPECT_TASK_ERROR_CONTENT, "test",null);
            exceptedSmsEvent = "校验任务异常";
        }
        @Test
        @DisplayName("get task title and content for inspect count error")
        void test9(){
            info.setMetric(INSPECT_COUNT_ERROR);
            Map<String,Object> map = new HashMap<>();
            map.put("inspectName","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.INSPECT_COUNT_ERROR_TITLE, "test");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.INSPECT_COUNT_ERROR_CONTENT, "test",null);
            exceptedSmsEvent = "快速count校验不一致告警";
        }
        @Test
        @DisplayName("get task title and content for inspect value error for join")
        void test10(){
            info.setMetric(INSPECT_VALUE_ERROR);
            info.setSummary("INSPECT_VALUE_JOIN_ERROR");
            Map<String,Object> map = new HashMap<>();
            map.put("inspectName","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.INSPECT_VALUE_ERROR_JOIN_TITLE, "test");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.INSPECT_VALUE_ERROR_JOIN_CONTENT, "test",null);
            exceptedSmsEvent = "关联字段值校验结果不一致告警";
        }
        @Test
        @DisplayName("get task title and content for inspect value error for all")
        void test11(){
            info.setMetric(INSPECT_VALUE_ERROR);
            Map<String,Object> map = new HashMap<>();
            map.put("inspectName","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = MessageFormat.format(AlarmMailTemplate.INSPECT_VALUE_ERROR_ALL_TITLE, "test");
            exceptedContent = MessageFormat.format(AlarmMailTemplate.INSPECT_VALUE_ERROR_ALL_CONTENT, "test",null);
            exceptedSmsEvent = "表全字段值校验结果不一致告警";
        }
        @Test
        @DisplayName("get task title and content for default")
        void test12(){
            info.setMetric(SYSTEM_FLOW_EGINGE_UP);
            Map<String,Object> map = new HashMap<>();
            map.put("inspectName","test");
            info.setParam(map);
            actual = alarmServiceImplUnderTest.getTaskTitleAndContent(info);
            exceptedTitle = "test task发生异常";
            exceptedContent = null;
            exceptedSmsEvent = "异常";
        }
    }
}