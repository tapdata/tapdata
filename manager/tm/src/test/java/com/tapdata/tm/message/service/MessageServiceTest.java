package com.tapdata.tm.message.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.service.EventsService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MessageMetadata;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.mp.service.MpService;
import com.tapdata.tm.sms.SmsService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.user.entity.Connected;
import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.*;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageServiceTest {

    @Mock
    private MessageRepository mockRepository;
    @Mock
    private MessageRepository mockMessageRepository;
    @Mock
    private UserService mockUserService;
    @Mock
    private TaskRepository mockTaskRepository;
    @Mock
    private MailUtils mockMailUtils;
    @Mock
    private EventsService mockEventsService;
    @Mock
    private SettingsService mockSettingsService;
    @Mock
    private SmsService mockSmsService;
    @Mock
    private MpService mockMpService;
    @Mock
    private AlarmService mockAlarmService;

    private MessageService messageServiceUnderTest;

    private Method privateMethod;

    private Method informUser;

    private Method informUser2;

    private UserDetail userDetail;

    @BeforeEach
    void setUp() throws NoSuchMethodException {
        messageServiceUnderTest = new MessageService(mockRepository);
        ReflectionTestUtils.setField(messageServiceUnderTest, "smsService", mockSmsService);
        ReflectionTestUtils.setField(messageServiceUnderTest, "mpService", mockMpService);
        ReflectionTestUtils.setField(messageServiceUnderTest, "alarmService", mockAlarmService);
        messageServiceUnderTest.messageRepository = mockMessageRepository;
        messageServiceUnderTest.userService = mockUserService;
        messageServiceUnderTest.taskRepository = mockTaskRepository;
        messageServiceUnderTest.mailUtils = mockMailUtils;
        messageServiceUnderTest.settingsService = mockSettingsService;
        messageServiceUnderTest.eventsService = mockEventsService;
        // 获取类的Class对象
        Class<?> myClass = MessageService.class;
        // 获取私有方法的名称
        String methodName = "informUserEmail";
        // 使用getDeclaredMethod()来获取私有方法
        privateMethod = myClass.getDeclaredMethod(methodName, MsgTypeEnum.class, SystemEnum.class,String.class, String.class,String.class,UserDetail.class);
        // 设置私有方法可访问
        privateMethod.setAccessible(true);
        informUser = myClass.getDeclaredMethod("informUser", MsgTypeEnum.class, SystemEnum.class, MessageMetadata.class, String.class,String.class,UserDetail.class);
        informUser.setAccessible(true);

        informUser2 = myClass.getDeclaredMethod("informUser", MessageDto.class);
        informUser2.setAccessible(true);

        userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        userDetail.setEmail("test@test.com");
    }

    /**
     * Use case for normal email sending
     */
    @Test
    void testAddSync_SendingEmail() {
        // Setup
        final Settings settings = new Settings();
        settings.setId("id");
        settings.setCategory("category");
        settings.setCategory_sort(0);
        settings.setDefault_value("default_value");
        settings.setValue("{\"runNotification\":[{\"label\":\"jobStarted\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"jobPaused\",\"notice\":true,\"email\":true},{\"label\":\"jobDeleted\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"jobStateError\",\"notice\":true,\"email\":true},{\"label\":\"jobEncounterError\",\"notice\":true,\"email\":true,\"noticeInterval\":\"noticeInterval\",\"Interval\":12,\"util\":\"hour\"}," +
                "{\"label\":\"CDCLagTime\",\"notice\":true,\"email\":true,\"lagTime\":\"lagTime\",\"lagTimeInterval\":12,\"lagTimeUtil\":\"second\",\"noticeInterval\":\"noticeInterval\",\"noticeIntervalInterval\":24,\"noticeIntervalUtil\":\"hour\"}," +
                "{\"label\":\"inspectCount\",\"notice\":true,\"email\":true},{\"label\":\"inspectValue\",\"notice\":true,\"email\":true},{\"label\":\"inspectDelete\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"inspectError\",\"notice\":true,\"email\":true}],\"systemNotification\":[],\"agentNotification\":[{\"label\":\"serverDisconnected\",\"notice\":true,\"email\":true},{\"label\":\"agentStarted\",\"notice\":true,\"email\":false}," +
                "{\"label\":\"agentStopped\",\"notice\":true,\"email\":true},{\"label\":\"agentCreated\",\"notice\":true,\"email\":false}," +
                "{\"label\":\"agentDeleted\",\"notice\":true,\"email\":true}]}");
        when(mockSettingsService.getByCategoryAndKey(CategoryEnum.NOTIFICATION, KeyEnum.NOTIFICATION))
                .thenReturn(settings);
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setLevel(Level.RECOVERY.getValue());
        messageEntity.setServerName("serverName");
        messageEntity.setMsg("");
        messageEntity.setTitle("title");
        messageEntity.setSourceId("sourceId");
        messageEntity.setSystem("");
        messageEntity.setCreateAt(new Date());
        messageEntity.setLastUpdAt(new Date());
        messageEntity.setUserId(userDetail.getUserId());
        messageEntity.setRead(false);
        messageEntity.setIsDeleted((false));
        messageEntity.setId(MongoUtils.toObjectId("6552e10c61030809c4b3af30"));
        when(mockSettingsService.isCloud()).thenReturn(true);
        // Configure AlarmService.getMailAccount(...).
        final MailAccountDto mailAccountDto = MailAccountDto.builder()
                .host("host")
                .port(0)
                .from("from")
                .user("user")
                .pass("pass")
                .receivers(Arrays.asList("value"))
                .protocol("protocol")
                .build();
        when(mockRepository.save(any(MessageEntity.class),any(UserDetail.class))).thenAnswer(invocationOnMock -> {
            MessageEntity message = invocationOnMock.getArgument(0,MessageEntity.class);
            message.setId(MongoUtils.toObjectId("6552e10c61030809c4b3af30"));
            return message;
        });
        // Run the test
        messageServiceUnderTest.addSync("serverName", "sourceId", MsgTypeEnum.STARTED, "title", Level.RECOVERY,
                userDetail);

        // Verify the results，method call once
        verify(mockMailUtils,times(1)).sendHtmlMail("test@test.com","username", "serverName", SystemEnum.SYNC, MsgTypeEnum.STARTED, "sourceId");
    }

    /**
     * Use case for email sending limit
     */
    @Test
    void testAddSync_SendingEmailLimit() {
        // Setup
        final Settings settings = new Settings();
        settings.setId("id");
        settings.setCategory("category");
        settings.setCategory_sort(0);
        settings.setDefault_value("default_value");
        settings.setValue("{\"runNotification\":[{\"label\":\"jobStarted\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"jobPaused\",\"notice\":true,\"email\":true},{\"label\":\"jobDeleted\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"jobStateError\",\"notice\":true,\"email\":true},{\"label\":\"jobEncounterError\",\"notice\":true,\"email\":true,\"noticeInterval\":\"noticeInterval\",\"Interval\":12,\"util\":\"hour\"}," +
                "{\"label\":\"CDCLagTime\",\"notice\":true,\"email\":true,\"lagTime\":\"lagTime\",\"lagTimeInterval\":12,\"lagTimeUtil\":\"second\",\"noticeInterval\":\"noticeInterval\",\"noticeIntervalInterval\":24,\"noticeIntervalUtil\":\"hour\"}," +
                "{\"label\":\"inspectCount\",\"notice\":true,\"email\":true},{\"label\":\"inspectValue\",\"notice\":true,\"email\":true},{\"label\":\"inspectDelete\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"inspectError\",\"notice\":true,\"email\":true}],\"systemNotification\":[],\"agentNotification\":[{\"label\":\"serverDisconnected\",\"notice\":true,\"email\":true},{\"label\":\"agentStarted\",\"notice\":true,\"email\":false}," +
                "{\"label\":\"agentStopped\",\"notice\":true,\"email\":true},{\"label\":\"agentCreated\",\"notice\":true,\"email\":false}," +
                "{\"label\":\"agentDeleted\",\"notice\":true,\"email\":true}]}");
        when(mockSettingsService.getByCategoryAndKey(CategoryEnum.NOTIFICATION, KeyEnum.NOTIFICATION))
                .thenReturn(settings);
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setLevel(Level.RECOVERY.getValue());
        messageEntity.setServerName("serverName");
        messageEntity.setMsg("");
        messageEntity.setTitle("title");
        messageEntity.setSourceId("sourceId");
        messageEntity.setSystem("");
        messageEntity.setCreateAt(new Date());
        messageEntity.setLastUpdAt(new Date());
        messageEntity.setUserId(userDetail.getUserId());
        messageEntity.setRead(false);
        messageEntity.setIsDeleted((false));
        messageEntity.setId(MongoUtils.toObjectId("6552e10c61030809c4b3af30"));
        when(mockSettingsService.isCloud()).thenReturn(true);
        // Configure AlarmService.getMailAccount(...).
        final MailAccountDto mailAccountDto = MailAccountDto.builder()
                .host("host")
                .port(0)
                .from("from")
                .user("user")
                .pass("pass")
                .receivers(Arrays.asList("value"))
                .protocol("protocol")
                .build();
        when(mockRepository.save(any(MessageEntity.class),any(UserDetail.class))).thenAnswer(invocationOnMock -> {
            MessageEntity message = invocationOnMock.getArgument(0,MessageEntity.class);
            message.setId(MongoUtils.toObjectId("6552e10c61030809c4b3af30"));
            return message;
        });
        //More than 10 times
        when(mockRepository.count(any(Query.class))).thenReturn(11L);
        // Run the test
        messageServiceUnderTest.addSync("serverName", "sourceId", MsgTypeEnum.STARTED, "title", Level.RECOVERY,
                userDetail);
        // Verify the results，method not called
        verify(mockMailUtils,times(0)).sendHtmlMail("test@test.com","username", "serverName", SystemEnum.SYNC, MsgTypeEnum.STARTED, "sourceId");
    }

    @Test
    void testAddSync_SendingEmailBoundary() {
        // Setup
        final Settings settings = new Settings();
        settings.setId("id");
        settings.setCategory("category");
        settings.setCategory_sort(0);
        settings.setDefault_value("default_value");
        settings.setValue("{\"runNotification\":[{\"label\":\"jobStarted\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"jobPaused\",\"notice\":true,\"email\":true},{\"label\":\"jobDeleted\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"jobStateError\",\"notice\":true,\"email\":true},{\"label\":\"jobEncounterError\",\"notice\":true,\"email\":true,\"noticeInterval\":\"noticeInterval\",\"Interval\":12,\"util\":\"hour\"}," +
                "{\"label\":\"CDCLagTime\",\"notice\":true,\"email\":true,\"lagTime\":\"lagTime\",\"lagTimeInterval\":12,\"lagTimeUtil\":\"second\",\"noticeInterval\":\"noticeInterval\",\"noticeIntervalInterval\":24,\"noticeIntervalUtil\":\"hour\"}," +
                "{\"label\":\"inspectCount\",\"notice\":true,\"email\":true},{\"label\":\"inspectValue\",\"notice\":true,\"email\":true},{\"label\":\"inspectDelete\",\"notice\":true,\"email\":true}," +
                "{\"label\":\"inspectError\",\"notice\":true,\"email\":true}],\"systemNotification\":[],\"agentNotification\":[{\"label\":\"serverDisconnected\",\"notice\":true,\"email\":true},{\"label\":\"agentStarted\",\"notice\":true,\"email\":false}," +
                "{\"label\":\"agentStopped\",\"notice\":true,\"email\":true},{\"label\":\"agentCreated\",\"notice\":true,\"email\":false}," +
                "{\"label\":\"agentDeleted\",\"notice\":true,\"email\":true}]}");
        when(mockSettingsService.getByCategoryAndKey(CategoryEnum.NOTIFICATION, KeyEnum.NOTIFICATION))
                .thenReturn(settings);
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setLevel(Level.RECOVERY.getValue());
        messageEntity.setServerName("serverName");
        messageEntity.setMsg("");
        messageEntity.setTitle("title");
        messageEntity.setSourceId("sourceId");
        messageEntity.setSystem("");
        messageEntity.setCreateAt(new Date());
        messageEntity.setLastUpdAt(new Date());
        messageEntity.setUserId(userDetail.getUserId());
        messageEntity.setRead(false);
        messageEntity.setIsDeleted((false));
        messageEntity.setId(MongoUtils.toObjectId("6552e10c61030809c4b3af30"));
        when(mockSettingsService.isCloud()).thenReturn(true);
        // Configure AlarmService.getMailAccount(...).
        final MailAccountDto mailAccountDto = MailAccountDto.builder()
                .host("host")
                .port(0)
                .from("from")
                .user("user")
                .pass("pass")
                .receivers(Arrays.asList("value"))
                .protocol("protocol")
                .build();
        when(mockRepository.save(any(MessageEntity.class),any(UserDetail.class))).thenAnswer(invocationOnMock -> {
            MessageEntity message = invocationOnMock.getArgument(0,MessageEntity.class);
            message.setId(MongoUtils.toObjectId("6552e10c61030809c4b3af30"));
            return message;
        });
        //More than 10 times
        when(mockRepository.count(any(Query.class))).thenReturn(Long.valueOf(CommonUtils.getPropertyInt("cloud_mail_limit",MailUtils.CLOUD_MAIL_LIMIT)));
        // Run the test
        messageServiceUnderTest.addSync("serverName", "sourceId", MsgTypeEnum.STARTED, "title", Level.RECOVERY,
                userDetail);
        // Verify the results，method not called
        verify(mockMailUtils,times(0)).sendHtmlMail("test@test.com","username", "serverName", SystemEnum.SYNC, MsgTypeEnum.STARTED, "sourceId");
    }
    /**
     * Use case for CheckMessageLimit
     */
    @Test
    void testCheckMessageLimit() {
        final Long result = messageServiceUnderTest.checkMessageLimit(userDetail);
        // Verify the results
        assertThat(result).isZero();
    }

    /**
     * Use case for CheckMessageLimit
     */
    @Test
    void testCheckMessageLimit_countTest() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date today = calendar.getTime();
        Query inputQuery = Query.query(Criteria.where("user_id").is(userDetail.getUserId())
                .and("isSend").is(true).and("createTime").gte(today));
        long except = 10L;
        when(mockRepository.count(inputQuery)).thenReturn(except);
        final Long result = messageServiceUnderTest.checkMessageLimit(userDetail);
        // Verify the results
        assertThat(result).isEqualTo(10);
    }

    /**
     * Check input parameters
     */
    @Test
    void testCheckMessageLimit_checkParameter(){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(new Date().getTime()), ZoneId.systemDefault());
        LocalDateTime startOfDay = localDateTime.with(LocalTime.MIN);
        Date exceptDate = Date.from(startOfDay.atZone(ZoneId.systemDefault()).toInstant());
        when(mockRepository.count(any(Query.class))).thenAnswer(invocationOnMock -> {
            Query query = invocationOnMock.getArgument(0,Query.class);
            Document document = (Document) query.getQueryObject().get("createTime");
            Date resultDate = (Date) document.get("$gte");
            assertThat(resultDate).isEqualTo(exceptDate);
            return null;
        });
        messageServiceUnderTest.checkMessageLimit(userDetail);
    }
    /**
     * Use case for normal email sending
     */
    @Test
    void testInformUserEmail_SendingEmail() throws InvocationTargetException, IllegalAccessException {
        when(mockSettingsService.isCloud()).thenReturn(true);
        privateMethod.invoke(messageServiceUnderTest,MsgTypeEnum.STARTED,SystemEnum.SYNC,"serverName","sourceId","messageId",userDetail);
        verify(mockMailUtils,times(1)).sendHtmlMail("test@test.com","Hi, username: ", "serverName", null,SystemEnum.SYNC, MsgTypeEnum.STARTED);
    }
    /**
     * Use case for email sending limit
     */
    @Test
    void testInformUserEmail_SendingEmailLimit() throws InvocationTargetException, IllegalAccessException {
        when(mockSettingsService.isCloud()).thenReturn(true);
        //More than 10 times
        when(mockRepository.count(any(Query.class))).thenReturn(11L);
        privateMethod.invoke(messageServiceUnderTest,MsgTypeEnum.STARTED,SystemEnum.SYNC,"serverName","sourceId","messageId",userDetail);
        verify(mockMailUtils,times(0)).sendHtmlMail("test@test.com","Hi, username: ", "serverName", null,SystemEnum.SYNC, MsgTypeEnum.STARTED);
    }

    @Test
    void testInformUserEmail_SendingEmailBoundary() throws InvocationTargetException, IllegalAccessException {
        when(mockSettingsService.isCloud()).thenReturn(true);
        //More than 10 times
        when(mockRepository.count(any(Query.class))).thenReturn(Long.valueOf(CommonUtils.getPropertyInt("cloud_mail_limit",MailUtils.CLOUD_MAIL_LIMIT)));
        privateMethod.invoke(messageServiceUnderTest,MsgTypeEnum.STARTED,SystemEnum.SYNC,"serverName","sourceId","messageId",userDetail);
        verify(mockMailUtils,times(0)).sendHtmlMail("test@test.com","Hi, username: ", "serverName", null,SystemEnum.SYNC, MsgTypeEnum.STARTED);
    }

    @Test
    void testInformUserEmail_SendingEmailByDass() throws InvocationTargetException, IllegalAccessException {
        when(mockSettingsService.isCloud()).thenReturn(false);
        privateMethod.invoke(messageServiceUnderTest,MsgTypeEnum.STARTED,SystemEnum.SYNC,"serverName","sourceId","messageId",userDetail);
        verify(mockMailUtils,times(1)).sendHtmlMail("test@test.com","Hi, username: ", "serverName", null,SystemEnum.SYNC, MsgTypeEnum.STARTED);
    }

    /**
     * Use case for normal email sending
     */
    @Test
    void testInformUser_SendingEmail() throws InvocationTargetException, IllegalAccessException {
        Notification notification = new Notification();
        notification.setConnected(new Connected(true,false,false));
        userDetail.setNotification(notification);
        when(mockSettingsService.isCloud()).thenReturn(true);
        informUser.invoke(messageServiceUnderTest,MsgTypeEnum.CONNECTED,SystemEnum.SYNC,new MessageMetadata("name","id"),"sourceId","messageId",userDetail);
        String resultClickHref = null + "monitor?id=" + "sourceId" + "{sourceId}&isMoniting=true&mapping=cluster-clone";
        verify(mockMailUtils,times(1)).sendHtmlMail("test@test.com","Hi, username: ", "name", resultClickHref,SystemEnum.SYNC, MsgTypeEnum.CONNECTED);
    }
    /**
     * Use case for email sending limit
     */
    @Test
    void testInformUser_SendingEmailLimit() throws InvocationTargetException, IllegalAccessException {
        Notification notification = new Notification();
        notification.setConnected(new Connected(true,false,false));
        userDetail.setNotification(notification);
        when(mockSettingsService.isCloud()).thenReturn(true);
        when(mockRepository.count(any(Query.class))).thenReturn(11L);
        informUser.invoke(messageServiceUnderTest,MsgTypeEnum.CONNECTED,SystemEnum.SYNC,new MessageMetadata("name","id"),"sourceId","messageId",userDetail);
        String resultClickHref = null + "monitor?id=" + "sourceId" + "{sourceId}&isMoniting=true&mapping=cluster-clone";
        //More than 10 times
        verify(mockMailUtils,times(0)).sendHtmlMail("test@test.com","Hi, username: ", "name", resultClickHref,SystemEnum.SYNC, MsgTypeEnum.CONNECTED);
    }

    @Test
    void testInformUser_SendingEmailBoundary() throws InvocationTargetException, IllegalAccessException {
        Notification notification = new Notification();
        notification.setConnected(new Connected(true,false,false));
        userDetail.setNotification(notification);
        when(mockSettingsService.isCloud()).thenReturn(true);
        when(mockRepository.count(any(Query.class))).thenReturn(Long.valueOf(CommonUtils.getPropertyInt("cloud_mail_limit",MailUtils.CLOUD_MAIL_LIMIT)));
        informUser.invoke(messageServiceUnderTest,MsgTypeEnum.CONNECTED,SystemEnum.SYNC,new MessageMetadata("name","id"),"sourceId","messageId",userDetail);
        String resultClickHref = null + "monitor?id=" + "sourceId" + "{sourceId}&isMoniting=true&mapping=cluster-clone";
        //More than 10 times
        verify(mockMailUtils,times(0)).sendHtmlMail("test@test.com","Hi, username: ", "name", resultClickHref,SystemEnum.SYNC, MsgTypeEnum.CONNECTED);
    }

    /**
     * Use case for normal email sending
     */
    @Test
    void testInformUser_SendingEmailByDass() throws InvocationTargetException, IllegalAccessException {
        Notification notification = new Notification();
        notification.setConnected(new Connected(true,false,false));
        userDetail.setNotification(notification);
        when(mockSettingsService.isCloud()).thenReturn(false);
        informUser.invoke(messageServiceUnderTest,MsgTypeEnum.CONNECTED,SystemEnum.SYNC,new MessageMetadata("name","id"),"sourceId","messageId",userDetail);
        String resultClickHref = null + "monitor?id=" + "sourceId" + "{sourceId}&isMoniting=true&mapping=cluster-clone";
        verify(mockMailUtils,times(1)).sendHtmlMail("test@test.com","Hi, username: ", "name", resultClickHref,SystemEnum.SYNC, MsgTypeEnum.CONNECTED);
    }
    @Test
    void testInformUser_SendingEmailByMessageDto() throws InvocationTargetException, IllegalAccessException{
        Notification notification = new Notification();
        notification.setConnected(new Connected(true,false,false));
        userDetail.setNotification(notification);
        when(mockSettingsService.isCloud()).thenReturn(true);
        MessageDto messageDto = new MessageDto();
        messageDto.setMsg("connected");
        messageDto.setSystem("agent");
        messageDto.setMessageMetadata("{\"name\":\"name\",\"id\":\"id\"}");
        messageDto.setUserId("62bc5008d4958d013d97c7a6");
        messageDto.setSourceModule("agent");
        when(mockUserService.loadUserById(new ObjectId("62bc5008d4958d013d97c7a6"))).thenReturn(userDetail);
        informUser2.invoke(messageServiceUnderTest,messageDto);
        verify(mockMailUtils,times(1)).sendHtmlMail("【Tapdata】","test@test.com", "Hi, : ", "name",null,"Instance online");
    }

    @Test
    void testInformUser_SendingEmailLimitByMessageDto() throws InvocationTargetException, IllegalAccessException {
        Notification notification = new Notification();
        notification.setConnected(new Connected(true, false, false));
        userDetail.setNotification(notification);
        when(mockSettingsService.isCloud()).thenReturn(true);
        MessageDto messageDto = new MessageDto();
        messageDto.setMsg("connected");
        messageDto.setSystem("agent");
        messageDto.setMessageMetadata("{\"name\":\"name\",\"id\":\"id\"}");
        messageDto.setUserId("62bc5008d4958d013d97c7a6");
        messageDto.setSourceModule("agent");
        when(mockUserService.loadUserById(new ObjectId("62bc5008d4958d013d97c7a6"))).thenReturn(userDetail);
        when(mockRepository.count(any(Query.class))).thenReturn(11L);
        informUser2.invoke(messageServiceUnderTest, messageDto);
        verify(mockMailUtils, times(0)).sendHtmlMail("【Tapdata】", "test@test.com", "Hi, : ", "name", null, "Instance online");

    }
    @Test
    void testInformUser_SendingEmailBoundaryByMessageDto() throws InvocationTargetException, IllegalAccessException{
        Notification notification = new Notification();
        notification.setConnected(new Connected(true,false,false));
        userDetail.setNotification(notification);
        when(mockSettingsService.isCloud()).thenReturn(true);
        MessageDto messageDto = new MessageDto();
        messageDto.setMsg("connected");
        messageDto.setSystem("agent");
        messageDto.setMessageMetadata("{\"name\":\"name\",\"id\":\"id\"}");
        messageDto.setUserId("62bc5008d4958d013d97c7a6");
        messageDto.setSourceModule("agent");
        when(mockUserService.loadUserById(new ObjectId("62bc5008d4958d013d97c7a6"))).thenReturn(userDetail);
        when(mockRepository.count(any(Query.class))).thenReturn(Long.valueOf(CommonUtils.getPropertyInt("cloud_mail_limit",MailUtils.CLOUD_MAIL_LIMIT)));
        informUser2.invoke(messageServiceUnderTest,messageDto);
        verify(mockMailUtils,times(0)).sendHtmlMail("【Tapdata】","test@test.com", "Hi, : ", "name",null,"Instance online");
    }
    @Test
    void testCheckSending(){
        when(mockSettingsService.isCloud()).thenReturn(false);
        boolean result = messageServiceUnderTest.checkSending(userDetail);
        assertThat(result).isTrue();
    }

    @Test
    void testCheckSending_Sending(){
        when(mockSettingsService.isCloud()).thenReturn(true);
        boolean result = messageServiceUnderTest.checkSending(userDetail);
        assertThat(result).isTrue();
    }

    @Test
    void testCheckSending_SendingLimit(){
        when(mockSettingsService.isCloud()).thenReturn(true);
        when(mockRepository.count(any(Query.class))).thenReturn(11L);
        boolean result = messageServiceUnderTest.checkSending(userDetail);
        assertThat(result).isFalse();
    }

    @Test
    void testCheckSending_SendingBoundary(){
        when(mockSettingsService.isCloud()).thenReturn(true);
        when(mockRepository.count(any(Query.class))).thenReturn(Long.valueOf(CommonUtils.getPropertyInt("cloud_mail_limit",MailUtils.CLOUD_MAIL_LIMIT)));
        boolean result = messageServiceUnderTest.checkSending(userDetail);
        assertThat(result).isFalse();
    }
}
