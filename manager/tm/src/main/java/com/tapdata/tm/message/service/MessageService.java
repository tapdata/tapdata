package com.tapdata.tm.message.service;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.NotificationDto;
import com.tapdata.tm.Settings.dto.NotificationSettingDto;
import com.tapdata.tm.Settings.dto.RunNotificationDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.constant.Type;
import com.tapdata.tm.events.service.EventsService;
import com.tapdata.tm.message.constant.*;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.message.vo.MessageListVo;
import com.tapdata.tm.mp.service.MpService;
import com.tapdata.tm.sms.SmsService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.*;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Service
@Slf4j
public class MessageService extends BaseService<MessageDto,MessageEntity,ObjectId,MessageRepository> {

    @Autowired
    MessageRepository messageRepository;
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    MailUtils mailUtils;
    @Autowired
    EventsService eventsService;
    @Autowired
    SettingsService settingsService;
    @Autowired
    private SmsService smsService;
    @Autowired
    private MpService mpService;
    @Autowired
    private CircuitBreakerRecoveryService circuitBreakerRecoveryService;

    private final static String MAIL_SUBJECT = "【Tapdata】";
    private final static String MAIL_CONTENT = "尊敬的用户您好，您在Tapdata Cloud上创建的Agent:";
    private final static String WILL_RELEASE_AGENT_MAIL_CONTENT = "因超过一周未使用即将在明天晚上20:00自动回收，如果您需要继续使用可以在清理前登录系统自动延长使用时间。";
    private final static String RELEASE_AGENT_MAIL_CONTENT = "因超过一周未使用已自动回收，如果您需要继续使用可通过新手引导再次创建。";

    //mail title
    private final static String WILL_RELEASE_AGENT_TITLE = "Tapdata】测试Agent资源即将回收提醒";
    private final static String RELEASE_AGENT_TITLE = "【Tapdata】测试Agent资源回收提醒";

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final static String FEISHU_ADDRESS = "http://34.96.213.48:30008/send_to/feishu/group";




    public MessageService(@NonNull MessageRepository repository) {
        super(repository, MessageDto.class, MessageEntity.class);
    }


    /**
     * 因为message表id不一样，导致这里不能直接用传来的limit 和skip来查询
     *
     * @param filter
     * @param userDetail
     * @return
     */
    public Page<MessageListVo> findMessage(Locale locale, Filter filter, UserDetail userDetail) {
        TmPageable tmPageable = new TmPageable();

        //page由limit 和skip计算的来
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        Query query = parseWhereCondition(filter.getWhere(), userDetail);
        if (!filter.getWhere().containsKey("msg")) {
            List<String> collect = Arrays.stream(MsgTypeEnum.values()).filter(t -> t != MsgTypeEnum.ALARM).map(MsgTypeEnum::getValue).collect(Collectors.toList());
            query.addCriteria(Criteria.where("msg").in(collect));
        }
        return getMessageListVoPage(locale, query, tmPageable);
    }

    @Override
    protected void beforeSave(MessageDto dto, UserDetail userDetail) {

    }

    @NotNull
    private Page<MessageListVo> getMessageListVoPage(Locale locale, Query query, TmPageable tmPageable) {
        long total = messageRepository.getMongoOperations().count(query, MessageEntity.class);
        query.with(Sort.by("createTime").descending());
        query.with(tmPageable);
        List<MessageEntity> messageEntityList = messageRepository.getMongoOperations().find(query, MessageEntity.class);

        ExpressionParser parser = new SpelExpressionParser();
        TemplateParserContext parserContext = new TemplateParserContext();
        List<MessageListVo> messageListVoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(messageEntityList, MessageListVo.class);
        messageListVoList.forEach(messageListVo -> {
            if (StringUtils.isEmpty(messageListVo.getServerName())) {
                messageListVo.setServerName(messageListVo.getAgentName());
            }
            if (MsgTypeEnum.ALARM.getValue().equals(messageListVo.getMsg()) && Objects.nonNull(messageListVo.getParam())) {
                String template = MessageUtil.getAlarmMsg(locale, messageListVo.getTemplate());
                String content = parser.parseExpression(template, parserContext).getValue(messageListVo.getParam(), String.class);
                messageListVo.setTitle(content);
            }
        });

        return new Page<>(total, messageListVoList);
    }

    public Page<MessageListVo> list(Locale locale, MsgTypeEnum type, String level, Boolean read, Integer page, Integer size, UserDetail userDetail) {
        Query query = new Query(Criteria.where("user_id").is(userDetail.getUserId()).and("msg").is(type.getValue()));
        if (StringUtils.isNotBlank(level)) {
            query.addCriteria(Criteria.where("level").is(level));
        }
        if (Objects.nonNull(read)) {
            query.addCriteria(Criteria.where("read").is(read));
        }

        TmPageable tmPageable = new TmPageable();
        tmPageable.setPage(page);
        tmPageable.setSize(size);

        return getMessageListVoPage(locale, query, tmPageable);
    }

    /**
     * 只用于获取消息通知铃铛的红色数字
     *
     * @param where
     * @param userDetail
     * @return
     */
    @Override
    public long count(Where where, UserDetail userDetail) {
        Query query = parseWhereCondition(where, userDetail);
        return messageRepository.getMongoOperations().count(query, MessageEntity.class);
    }

    public void add(String jobName, String serverName, MsgTypeEnum msgTypeEnum, SystemEnum systemEnum, String sourceId, String title, Level level, UserDetail userDetail) {
        try {
            MessageEntity messageEntity = new MessageEntity();
            MessageMetadata messageMetadata = new MessageMetadata(jobName, sourceId);
            messageEntity.setMessageMetadata(messageMetadata);
            messageEntity.setLevel(level.getValue());
            messageEntity.setServerName(serverName);
            messageEntity.setMsg(msgTypeEnum.getValue());
            messageEntity.setTitle(title);
            messageEntity.setSystem(systemEnum.getValue());
            messageEntity.setSourceId(sourceId);
            messageEntity.setCreateAt(new Date());
            messageEntity.setLastUpdAt(new Date());

            messageEntity.setUserId(userDetail.getUserId());
            messageEntity.setCustomId(userDetail.getCustomerId());
            repository.save(messageEntity,userDetail);
            informUser(msgTypeEnum, systemEnum, messageMetadata, sourceId, messageEntity.getId().toString(), userDetail);
        } catch (Exception e) {
            log.error("新增消息异常，", e);
        }
    }

    /**
     * 增加迁移任务消息
     *
     * @param serverName
     * @param msgTypeEnum
     * @param level
     * @param userDetail
     */
    @Async("NotificationExecutor")
    public void addMigration(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail) {
        MessageEntity saveMessage = new MessageEntity();

        if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
            log.info("任务恢复运行，只add message， 不用发邮件短信");
            return;
        }
        NotificationDto notificationDto = needInform(SystemEnum.MIGRATION, msgTypeEnum);
        Notification userNotification = userDetail.getNotification();
        if (null != notificationDto) {
            saveMessage = addMessage(serverName, sourceId, SystemEnum.MIGRATION, msgTypeEnum, "", level, userDetail, notificationDto.getNotice());

            if (notificationDto.getEmail()) {
                SendStatus sendStatus = new SendStatus("false", "");
                eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), saveMessage.getId().toString(), userDetail.getUserId(), sendStatus, 0, Type.NOTICE_MAIL);
            }
        }
        if (null != userNotification) {
            //用户设置优先
            if (userNotification.getStoppedByError().getEmail()) {
                log.info("dataflow出错，email 通知");
                MessageEntity finalSaveMessage = saveMessage;
                if (MsgTypeEnum.DELETED.equals(msgTypeEnum) || MsgTypeEnum.PAUSED.equals(msgTypeEnum)) {
                    log.info("任务删除或停止，不用发邮件短信");
                    return;
                }
                FunctionUtils.isTureOrFalse(settingsService.isCloud()).trueOrFalseHandle(() -> {
                    informUserEmail(msgTypeEnum, SystemEnum.MIGRATION, serverName, sourceId, finalSaveMessage.getId().toString(), userDetail);
                }, () -> {
                    MailAccountDto mailAccount = settingsService.getMailAccount(userDetail.getUserId());

                    String mailTitle = getMailTitle(msgTypeEnum);
                    MailUtils.sendHtmlEmail(mailAccount, mailAccount.getReceivers(), mailTitle, serverName + mailTitle);
                });
            }
            if (userNotification.getStoppedByError().getSms()) {
                log.info("dataflow出错，sms 通知");
                informUserSms(msgTypeEnum, SystemEnum.MIGRATION, serverName, sourceId, saveMessage.getId().toString(), userDetail);

            }
        }
    }

    private String getMailTitle(MsgTypeEnum msgTypeEnum) {
        String title = "";
        switch (msgTypeEnum) {
            case STOPPED_BY_ERROR:
                title = "Task error";
                break;
            case CONNECTED:
                title = "Task running";
                break;
            case PAUSED:
                title = "Task pause";
                break;
            case DELETED:
                title = "Task delete";
                break;
        }
        return title;
    }

    /**
     * @param serverName
     * @param sourceId
     * @param msgTypeEnum
     * @param title
     * @param userDetail
     * @return
     */
    public MessageDto addTrustAgentMessage(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, String title, UserDetail userDetail) {
        MessageEntity saveMessage = null;
        saveMessage = addMessage(serverName, sourceId, SystemEnum.AGENT, msgTypeEnum, title, Level.INFO, userDetail);
        asynInformUserEmail(msgTypeEnum, SystemEnum.AGENT, saveMessage.getId().toString(), userDetail);
        asynInformUserSms(saveMessage.getId().toString(), userDetail);
        MessageDto messageDto = new MessageDto();
        BeanUtil.copyProperties(saveMessage, messageDto);
        return messageDto;
    }

    /**
     * 增加迁移任务消息
     *
     * @param serverName
     * @param msgTypeEnum
     * @param level
     * @param userDetail
     */
    public void addInspect(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail) {
        MessageEntity saveMessage = new MessageEntity();
        Notification userNotification = userDetail.getNotification();
//        saveMessage = addMessage(serverName, sourceId, SystemEnum.MIGRATION, msgTypeEnum, "", level, userDetail);
        if (null != userNotification) {
            //用户设置优先
            if (userNotification.getStoppedByError().getEmail()) {
                log.info("dataflow出错，email 通知");
            }
            if (userNotification.getStoppedByError().getSms()) {
                log.info("dataflow出错，sms 通知");

            }
        }

        //系统设置，企业版
        NotificationDto notificationDto = needInform(SystemEnum.INSPECT, msgTypeEnum);
        if (null != notificationDto) {
            if (notificationDto.getNotice()) {
                saveMessage = addMessage(serverName, sourceId, SystemEnum.INSPECT, msgTypeEnum, null, level, userDetail);
            }
            if (notificationDto.getEmail()) {
                SendStatus sendStatus = new SendStatus("", "false");
                eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), saveMessage.getId().toString(), userDetail.getUserId(), sendStatus, 0, Type.NOTICE_MAIL);
            }
        }
    }


    /**
     * 增加迁移任务消息
     *
     * @param serverName
     * @param msgTypeEnum
     * @param title
     * @param level
     * @param userDetail
     */
    public void addSync(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail) {
        NotificationDto notificationDto = needInform(SystemEnum.SYNC, msgTypeEnum);
        if (null == notificationDto) {
            return;
        }
        MessageEntity saveMessage = addMessage(serverName, sourceId, SystemEnum.SYNC, msgTypeEnum, "", level, userDetail, notificationDto.getNotice());

        if (notificationDto.getEmail()) {
            FunctionUtils.isTureOrFalse(settingsService.isCloud()).trueOrFalseHandle(() -> {
                if(checkSending(userDetail)){
                    SendStatus sendStatus = new SendStatus("", "false");
                    if (!MsgTypeEnum.DELETED.equals(msgTypeEnum)) {
                        sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), userDetail.getUsername(), serverName, SystemEnum.SYNC, msgTypeEnum, sourceId);
                    }
                    eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), saveMessage.getId().toString(), userDetail.getUserId(), sendStatus, 0, Type.NOTICE_MAIL);
                    update(Query.query(Criteria.where("_id").is(saveMessage.getId())),Update.update("isSend",true));
                }
            }, () -> {
                MailAccountDto mailAccount = settingsService.getMailAccount(userDetail.getUserId());
                String mailTitle = getMailTitle(msgTypeEnum);
                MailUtils.sendHtmlEmail(mailAccount, mailAccount.getReceivers(), mailTitle, serverName + mailTitle);
            });
        }
    }


    private List getNotificationTypeList(NotificationSettingDto.NotificationSettingEnum notificationSettingEnum) {
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.NOTIFICATION, KeyEnum.NOTIFICATION);
        NotificationSettingDto notificationSettingDto = JSONObject.parseObject(settings.getValue().toString(), NotificationSettingDto.class);
        Map map = BeanUtil.beanToMap(notificationSettingDto);
        return (List) map.get(notificationSettingEnum.getValue());
    }

    private NotificationDto needInform(SystemEnum systemEnum, MsgTypeEnum msgTypeEnum) {
        NotificationDto notificationDto = null;
        if (systemEnum.equals(SystemEnum.INSPECT)) {
            List<NotificationDto> runNotificationList = getNotificationTypeList(NotificationSettingDto.NotificationSettingEnum.RUN_NOTIFICATION);
            if (msgTypeEnum.equals(MsgTypeEnum.INSPECT_VALUE)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.INSPECT_VALUE.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.INSPECT_ERROR)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.INSPECT_ERROR.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.DELETED)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.INSPECT_DELETE.getValue()))).collect(Collectors.toList()).get(0);
            }
        } else if (systemEnum.equals(SystemEnum.SYNC)) {
            List<NotificationDto> runNotificationList = getNotificationTypeList(NotificationSettingDto.NotificationSettingEnum.RUN_NOTIFICATION);
            if (msgTypeEnum.equals(MsgTypeEnum.STARTED)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_STARTED.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.STOPPED_BY_ERROR)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_ENCOUNTER_ERROR.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.PAUSED)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_PAUSED.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.ERROR)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_ENCOUNTER_ERROR.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.DELETED)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_DELETED.getValue()))).collect(Collectors.toList()).get(0);
            }
        } else if (systemEnum.equals(SystemEnum.MIGRATION)) {
            List<NotificationDto> runNotificationList = getNotificationTypeList(NotificationSettingDto.NotificationSettingEnum.RUN_NOTIFICATION);
            if (msgTypeEnum.equals(MsgTypeEnum.STARTED)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_STARTED.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.STOPPED_BY_ERROR)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_ENCOUNTER_ERROR.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.PAUSED)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_PAUSED.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.ERROR)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_ENCOUNTER_ERROR.getValue()))).collect(Collectors.toList()).get(0);
            } else if (msgTypeEnum.equals(MsgTypeEnum.DELETED)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.JOB_DELETED.getValue()))).collect(Collectors.toList()).get(0);
            }
        } else if (systemEnum.equals(SystemEnum.AGENT)) {
            List<NotificationDto> runNotificationList = getNotificationTypeList(NotificationSettingDto.NotificationSettingEnum.AGENT_NOTIFICATION);
            if (msgTypeEnum.equals(MsgTypeEnum.WILL_RELEASE_AGENT)) {
                notificationDto = new NotificationDto();
                notificationDto.setNotice(true);
                notificationDto.setEmail(true);
                notificationDto.setSms(true);
            } else if (msgTypeEnum.equals(MsgTypeEnum.RELEASE_AGENT)) {
                notificationDto = runNotificationList.stream().filter(item -> (item.getLabel().equals(RunNotificationDto.Label.AGENT_DELETED.getValue()))).collect(Collectors.toList()).get(0);
            }
        }
        return notificationDto;
    }


    private MessageEntity addMessage(String serverName, String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail) {
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setLevel(level.getValue());
        messageEntity.setServerName(serverName);
        messageEntity.setMsg(msgTypeEnum.getValue());
        messageEntity.setTitle(title);
        messageEntity.setSourceId(sourceId);
        messageEntity.setSystem(systemEnum.getValue());
        messageEntity.setCreateAt(new Date());
        messageEntity.setLastUpdAt(new Date());
        messageEntity.setUserId(userDetail.getUserId());
        messageEntity.setRead(false);
        repository.save(messageEntity,userDetail);
        return messageEntity;
    }

    protected MessageEntity addMessage(String serverName, String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail, Boolean isNotify) {
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setLevel(level.getValue());
        messageEntity.setServerName(serverName);
        messageEntity.setMsg(msgTypeEnum.getValue());
        messageEntity.setTitle(title);
        messageEntity.setSourceId(sourceId);
        messageEntity.setSystem(systemEnum.getValue());
        messageEntity.setCreateAt(new Date());
        messageEntity.setLastUpdAt(new Date());
        messageEntity.setUserId(userDetail.getUserId());
        messageEntity.setRead(false);
        messageEntity.setIsDeleted((!isNotify));
        repository.save(messageEntity,userDetail);
        return messageEntity;
    }

    public void addMessage(MessageEntity messageEntity,UserDetail userDetail) {
        repository.save(messageEntity,userDetail);
    }

    /**
     * 仅做记录记录只用，不会显示再界面上
     *
     * @param serverName
     * @param sourceId
     * @param systemEnum
     * @param msgTypeEnum
     * @param title
     * @param level
     * @param userDetail
     * @return
     */
    private MessageEntity addDeleteMessage(String serverName, String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail) {
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setLevel(level.getValue());
        messageEntity.setServerName(serverName);
        messageEntity.setMsg(msgTypeEnum.getValue());
        messageEntity.setTitle(title);
        messageEntity.setSourceId(sourceId);
        messageEntity.setSystem(systemEnum.getValue());
        messageEntity.setCreateAt(new Date());
        messageEntity.setLastUpdAt(new Date());
        messageEntity.setUserId(userDetail.getUserId());
        messageEntity.setRead(false);
        messageEntity.setIsDeleted(true);
        repository.getMongoOperations().save(messageEntity);
        return messageEntity;
    }


    private void informUserEmail(MsgTypeEnum msgType, SystemEnum systemEnum, String serverName, String sourceId, String messageId, UserDetail userDetail) {
        //目前msgNotification 只会有三种情况
//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());
        if (null == userDetail.getEmail()) {
            log.error("用户没有设置邮箱  userDetail:{}", JsonUtil.toJson(userDetail));
            return;
        }
        //发送邮件
        Integer retry = 1;
        log.info("发送邮件通知");
        String username = "Hi, " + (userDetail.getUsername() == null ? "" : userDetail.getUsername()) + ": ";

//        Object hostUrl = settingsService.getByCategoryAndKey(CategoryEnum.SMTP, KeyEnum.EMAIL_HREF);
        String clickHref = mailUtils.getHrefClick(sourceId, systemEnum, msgType);
        if(checkSending(userDetail)){
            SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), username, serverName, clickHref, systemEnum, msgType);
            eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_MAIL);
            update(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(messageId))),Update.update("isSend",true));
        }
    }

    /**
     * 异步发邮件
     *
     * @param msgType
     * @param systemEnum
     * @param messageId
     * @param userDetail
     */
    private void asynInformUserEmail(MsgTypeEnum msgType, SystemEnum systemEnum, String messageId, UserDetail userDetail) {
        String email = userDetail.getEmail();
        if (null == email) {
            log.error("用户没有设置邮箱  userDetail:{}", JsonUtil.toJson(userDetail));
            return;
        }
        //发送邮件
        log.info("asynInformUserEmail");
        String title = "";
        String conetnt = "";
        if (SystemEnum.AGENT.equals(systemEnum)) {
            if (MsgTypeEnum.WILL_RELEASE_AGENT.equals(msgType)) {
                title = WILL_RELEASE_AGENT_TITLE;
                conetnt = WILL_RELEASE_AGENT_MAIL_CONTENT;
            } else if (MsgTypeEnum.RELEASE_AGENT.equals(msgType)) {
                title = RELEASE_AGENT_TITLE;
                conetnt = RELEASE_AGENT_MAIL_CONTENT;
            }
        }
        eventsService.asynAdd(title, conetnt, email, messageId, userDetail.getUserId(), Type.NOTICE_MAIL);
    }

    private void asynInformUserSms(String messageId, UserDetail userDetail) {
        String phone = userDetail.getPhone();
        if (StringUtils.isEmpty(phone)) {
            log.error("用户没有设置邮箱  userDetail:{}", JsonUtil.toJson(userDetail));
            return;
        }
        //发送邮件
        log.info("asynInformUserSms ");
        eventsService.asynAdd("", "", phone, messageId, userDetail.getUserId(), Type.NOTICE_SMS);
    }


    private void informUserSms(MsgTypeEnum msgType, SystemEnum systemEnum, String servername, String sourceId, String messageId, UserDetail userDetail) {
        String smsContent = "";
        //发送短信
        log.info("发送短信通知");
        String phone = userDetail.getPhone();
//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());
        if (StringUtils.isEmpty(phone)) {
            log.error("用户没有设置 手机  userDetail:{}", JsonUtil.toJson(userDetail));
            return;
        }

        Integer retry = 1;
        String smsTemplateCode = smsService.getTemplateCode(msgType);
        SendStatus sendStatus = smsService.sendShortMessage(smsTemplateCode, phone, systemEnum.getValue(), servername);
        eventsService.recordEvents(MAIL_SUBJECT, smsContent, phone, messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_SMS);
    }


    private void informUser(MsgTypeEnum msgType, SystemEnum systemEnum, MessageMetadata messageMetadata, String sourceId, String messageId, UserDetail userDetail) {
/*        String msgType = messageDto.getMsg();
        String userId = messageDto.getUserId();
        String system = messageDto.getSystem();*/
//        UserDetail userDetail = userService.loadUserById(new ObjectId(userId));
        Notification notification = userDetail.getNotification();

        //判断notification是否为空，如果为空，则按照系统配置发送通知
        Boolean sendEmail = false;
        Boolean sendSms = false;
        if (null != notification) {
            Object eventType = BeanUtil.getProperty(notification, msgType.getValue());
            sendEmail = BeanUtil.getProperty(eventType, "email");
            sendSms = BeanUtil.getProperty(eventType, "sms");
        } else {
            //如果用户的设置通知为空,就全全局设置  setting中取
            sendEmail = getDefaultNotification(systemEnum.getValue(), msgType.getValue(), "email");
            sendSms = getDefaultNotification(systemEnum.getValue(), msgType.getValue(), "notice");
        }

        //目前msgNotification 只会有三种情况
        String metadataName = messageMetadata.getName();
        String smsContent = "";

        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            smsContent = "尊敬的用户，你好，您在Tapdata Cloud 上创建的任务:" + metadataName + " 正在运行";
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsContent = "尊敬的用户，你好，您在Tapdata Cloud 上创建的任务:" + metadataName + " 出错，请即使处理";
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {

        }

//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());
        //发送邮件
        if (sendEmail) {
            Integer retry = 1;
            log.info("发送邮件通知");
            String username = "Hi, " + (userDetail.getUsername() == null ? "" : userDetail.getUsername()) + ": ";

            Object hostUrl = settingsService.getByCategoryAndKey("SMTP", "emailHref");
            String clickHref = hostUrl + "monitor?id=" + sourceId + "{sourceId}&isMoniting=true&mapping=cluster-clone";
            if(checkSending(userDetail)){
                SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), username, metadataName, clickHref, systemEnum, msgType);
                eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_MAIL);
                update(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(messageId))),Update.update("isSend",true));
            }

        }

        //发送短信
        if (sendSms) {
            Integer retry = 1;
            String phone = userDetail.getPhone();
            String smsTemplateCode = smsService.getTemplateCode(msgType);
            SendStatus sendStatus = smsService.sendShortMessage(smsTemplateCode, phone, systemEnum.getValue(), metadataName);
            eventsService.recordEvents(MAIL_SUBJECT, smsContent, phone, messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_SMS);
        }
    }


    /**
     * 只有三种情况会新增一条message
     * 1、agent启动。TCM触发，TCM通过接口创建消息。
     * 2、agent停止。TCM触发，TCM通过接口创建消息。
     * 3、任务出错。通过dataFlows数据中status属性改为error，做为判断依据，创建消息通知。  这个时候需要发送邮件，或者短信通知用户
     *
     * @param messageDto
     * @return
     */
    public MessageDto add(MessageDto messageDto, UserDetail userDetail) {
        try {
            MessageEntity messageEntity = new MessageEntity();
            BeanUtil.copyProperties(messageDto, messageEntity, "messageMetadata");
            MessageMetadata messageMetadata = JSONUtil.toBean(messageDto.getMessageMetadata(), MessageMetadata.class);
            messageEntity.setMessageMetadata(messageMetadata);
            messageEntity.setUserId(userDetail.getUserId());
            Date now = new Date();
            messageEntity.setCreateAt(now);
            messageEntity.setServerName(messageDto.getAgentName());
            messageEntity.setLastUpdAt(now);
            messageEntity.setLastUpdBy(userDetail.getUsername());
            repository.save(messageEntity, userDetail);
            messageDto.setId(messageEntity.getId());
            WorkerService workerService = SpringContextHelper.getBean(WorkerService.class);
            Long agentCount = workerService.getLastCheckAvailableAgentCount();
            if(SourceModuleEnum.AGENT.getValue().equalsIgnoreCase(messageDto.getSourceModule())
                    && MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(messageDto.getMsg())){
                if(!scheduledExecutorService.isShutdown()){
                    scheduledExecutorService.schedule(()->{
                        try{
                            checkAagentConnectedMessage(now,agentCount,FEISHU_ADDRESS);
                        }catch (Exception e){
                            log.error("Delayed message sending failed {}.",e.getMessage());
                        }finally {
                            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                        }

                    }, 3, TimeUnit.MINUTES);
                    scheduledExecutorService.shutdown();
                }
            }else{
                informUser(messageDto);
            }
        } catch (Exception e) {
            log.error("新增消息异常，", e);
        }
        return messageDto;
    }

    /**
     * 等待3个心跳周期，观察在该周期内，新增离线agent的数量是否超过（10）个
     */
    public void checkAagentConnectedMessage(Date date,Long agentCount,String address){
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MINUTE, 3);
        Date nowEnd = cal.getTime();
        Query query = new Query(Criteria.where("createTime").gte(date).lte(nowEnd).
                and("msg").is(MsgTypeEnum.CONNECTION_INTERRUPTED.getValue()).
                and("system").is(SourceModuleEnum.AGENT.getValue()).
                and("isSend").is(false));
        List<MessageEntity> messageEntities = messageRepository.findAll(query);
        List<MessageDto> messageDtoList = messageEntities.stream().map(messageEntity -> {
            MessageDto messageDto = new MessageDto();
            BeanUtil.copyProperties(messageEntity,messageDto);
            messageDto.setMessageMetadata(JSONObject.toJSONString(messageEntity.getMessageMetadata()));
            return messageDto;
        }).collect(Collectors.toList());
        if(messageDtoList.size() >= CommonUtils.getPropertyLong("offline_agent_count", 10)){
            log.info("agent offline exceeds limit");
            updateMany(query,Update.update("isSend",true));
            Map<String,String> map = new HashMap<>();
            String content = "最近3分钟，累计超过"+messageDtoList.size()+"个Agent离线，已自动启动Agent离线告警熔断机制，请尽快检查相关服务是否正常!";
            map.put("title", "Agent离线告警通知熔断提醒");
            map.put("content", content);
            map.put("color", "red");
            map.put("groupId","oc_d6bc5fe48d56453264ec73a2fb3eec70");
            HttpUtils.sendPostData(address,JSONObject.toJSONString(map));
            circuitBreakerRecoveryService.checkServiceStatus(agentCount,address);
        }else{
            if(CollectionUtils.isNotEmpty(messageDtoList)){
                messageDtoList.forEach(this::informUser);
                log.info("send agent offline notification");
            }
        }
    }


    /**
     * 根据设置通知用户,短信或者邮件
     */
    private void informUser(MessageDto messageDto) {

        if (!settingsService.isCloud()) {
            return;
        }

        String msgType = messageDto.getMsg();
        String userId = messageDto.getUserId();
        String system = messageDto.getSystem();
        update(Query.query(Criteria.where("_id").is(messageDto.getId())),Update.update("isSend",true));
        UserService userService = SpringContextHelper.getBean(UserService.class);
        UserDetail userDetail = userService.loadUserById(new ObjectId(userId));

//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());

        Notification notification = userDetail.getNotification();

        //判断notification是否为空，如果为空，则按照系统配置发送通知
        Boolean sendEmail = false;
        Boolean sendSms = false;
        Boolean sendWeChat = false;
        if (null != notification) {
            Object eventType = BeanUtil.getProperty(notification, msgType);
            if (eventType == null) {
                eventType = BeanUtil.getProperty(notification, "connected");
            }
            sendEmail = BeanUtil.getProperty(eventType, "email");
            sendSms = BeanUtil.getProperty(eventType, "sms");
            sendWeChat = BeanUtil.getProperty(eventType, "weChat");
        } else {
            //如果用户的设置通知为空,就全全局设置  setting中取
            sendEmail = getDefaultNotification(system, msgType, "email");
            sendSms = getDefaultNotification(system, msgType, "notice");
            sendWeChat = getDefaultNotification(system, msgType, "weChat");
        }

        MessageMetadata messageMetadata = JSONUtil.toBean(messageDto.getMessageMetadata(), MessageMetadata.class);

        //目前msgNotification 只会有三种情况
        String metadataName = messageMetadata.getName();
        String emailTip = "";
        String smsContent = "";
        String title = "";
        String content = "";
        String weChatContent = "";
        String subject = "";

        if (SourceModuleEnum.AGENT.getValue().equalsIgnoreCase(messageDto.getSourceModule())) {
            if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
                emailTip = "Agent online";
                subject = String.format("Your deployed Agent %s is online", metadataName);
                smsContent = "尊敬的用户，你好，您在 Tapdata Cloud V3.0 上创建的实例:" + metadataName + " 已上线运行";
                title = "实例 " + metadataName + "已上线运行";
                content = "尊敬的用户，你好，您在 Tapdata Cloud V3.0 上创建的实例:" + metadataName + " 已上线运行";
                weChatContent = "实例:" + metadataName + " 已上线运行";
            } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
                subject = String.format("Your deployed Agent %s is offline", metadataName);
                emailTip = "Agent offline";
                smsContent = "尊敬的用户，你好，您在 Tapdata Cloud V3.0 上创建的实例:" + metadataName + " 已离线，请及时处理";
                title = "实例 " + metadataName + "已离线";
                content = "尊敬的用户，你好，您在 Tapdata Cloud V3.0 上创建的实例:" + metadataName + " 已离线，请及时处理";
                weChatContent = "实例:" + metadataName + " 已离线，请及时处理";
            } else if (MsgTypeEnum.EXPIRING.getValue().equals(msgType)) {

                cn.hutool.json.JSONObject metadata = JSONUtil.parseObj(messageDto.getMessageMetadata());
                String notifyType = metadata.getStr("notifyType");
                String paymentMethod = metadata.getStr("paymentMethod");
                String subscribeType = metadata.getStr("subscribeType");
                int expiringDays = -1;
                if ("NOTIFY_7_DAY".equalsIgnoreCase(notifyType) ){
                    expiringDays = 7;
                } else if ("NOTIFY_1_DAY".equalsIgnoreCase(notifyType)) {
                    expiringDays = 1;
                }
                if (expiringDays == -1) {
                    return;
                }
                if ("one_time".equals(subscribeType)) { // 一次性订阅
                    subject = String.format("The instance (%s) you subscribed to will expire in %s days to avoid affecting the running of your tasks. Please renew in time.", metadataName, expiringDays);
                    emailTip = subject;
                    smsContent = String.format("尊敬的用户，你好，您在Tapdata Cloud上订阅的实例:%s 还有%s天到期，避免影响您的任务正常运行，请及时续订", metadataName, expiringDays);
                    title = String.format("您订阅的实例(%s)，还有 %s 天到期，避免影响您的任务正常运行，请及时续订", metadataName, expiringDays);
                    content = String.format("尊敬的用户，你好，您在 Tapdata Cloud上订阅的实例(%s)，还有 %s 天到期，避免影响您的任务正常运行，请及时续订", metadataName, expiringDays);
                    weChatContent = String.format("您订阅的实例(%s)，还有 %s 天到期，请及时续订", metadataName, expiringDays);
                } else {
                    // 连续订阅或者其他未知订阅方法取消通知
                    return;
                }
            } else if (MsgTypeEnum.EXPIRED.getValue().equals(msgType)) {
                cn.hutool.json.JSONObject metadata = JSONUtil.parseObj(messageDto.getMessageMetadata());
                String paymentMethod = metadata.getStr("paymentMethod");
                String subscribeType = metadata.getStr("subscribeType");

                if ("one_time".equals(subscribeType)) { // 一次性订阅
                    subject = String.format("The instance (%s) you subscribed to has expired and will be released in 1 day.", metadataName);
                    emailTip = subject;
                    smsContent = String.format("尊敬的用户，你好，您在Tapdata Cloud上订阅的实例:%s 已经到期，实例将在1天以后释放，如需继续使使用请及时续订", metadataName);
                    title = String.format("您订阅的实例(%s)已经到期，实例将在1天以后释放，如需继续使使用请及时续订，请及时续订", metadataName);
                    // content = String.format("尊敬的用户，你好，您在Tapdata Cloud上订阅的实例(%s)已经到期，实例将在1天以后释放，如需继续使使用请及时续订，请及时续订", metadataName);
                    content = subject;
                    weChatContent = String.format("您订阅的实例(%s)，已经到期，实例将在1天以后释放，请及时续订", metadataName);
                } else {
                    // 连续订阅
                    subject = String.format("The instance (%s) you subscribed to has expired. Please keep your auto-renewal account with sufficient balance and the system will automatically renew it for you. If the renewal is not completed, please contact customer service for assistance.", metadataName);
                    emailTip = subject;
                    smsContent = String.format("尊敬的用户，你好，您在Tapdata Cloud上订阅的实例:%s 已经到期，请保持自动续订账户余额充足，系统将会自动为您续订；如果没有完成续订请联系客服协助处理", metadataName);
                    title = String.format("您订阅的实例(%s)已经到期，请保持自动续订账户余额充足，系统将会自动为您续订；如果没有完成续订请联系客服协助处理", metadataName);
                    //content = String.format("尊敬的用户，你好，您在Tapdata Cloud上订阅的实例(%s)已经到期，请保持自动续订账户余额充足，系统将会自动为您续订；如果没有完成续订请联系客服协助处理", metadataName);
                    content = subject;
                    weChatContent = String.format("您订阅的实例(%s)，已经到期，，请保持自动续订账户余额充足，系统将会自动为您续订；如果没有完成续订请联系客服协助处理", metadataName);
                }
            }
        } else {
            if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
                emailTip = "The state changes to running";
                smsContent = "尊敬的用户，你好，您在Tapdata Cloud 上创建的任务:" + metadataName + " 正在运行";
                title = "任务:" + metadataName + " 正在运行";
                content = "尊敬的用户，你好，您在Tapdata Cloud 上创建的任务:" + metadataName + " 正在运行";
                weChatContent = "任务:" + metadataName + " 正在运行";
            } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
                emailTip = "State has changed from running to offline, could affect the normal operation of your task, please handle in time.";
                smsContent = "尊敬的用户，你好，您在Tapdata Cloud 上创建的任务:" + metadataName + " 出错，请及时处理";
                title = "任务:" + metadataName + " 出错";
                content = "您在Tapdata Cloud 上创建的任务:" + metadataName + " 出错，请及时处理";
                weChatContent = "任务:" + metadataName + " 出错，请及时处理";
            } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {

            }
        }

        //发送邮件
        if (sendEmail) {
            Integer retry = 1;
            log.info("发送邮件通知{}", userDetail.getEmail());
            String username = "Hi, " + (messageDto.getUsername() == null ? "" : messageDto.getUsername()) + ": ";
//            Object hostUrl = settingsService.getByCategoryAndKey("SMTP", "emailHref");
//            String clickHref = hostUrl + "monitor?id=" + sourceId + "{sourceId}&isMoniting=true&mapping=cluster-clone";
            MsgTypeEnum msgTypeEnum = MsgTypeEnum.getEnumByValue(msgType);
            String clickHref = mailUtils.getAgentClick(metadataName, msgTypeEnum);
            if(checkSending(userDetail)){
                SendStatus sendStatus = mailUtils.sendHtmlMail(subject + MAIL_SUBJECT, userDetail.getEmail(), username, metadataName, clickHref, emailTip);
                eventsService.recordEvents(subject + MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageDto, sendStatus, retry, Type.NOTICE_MAIL);
            }
        }

        //发送短信
        if (sendSms) {
            Integer retry = 1;
            String phone = userDetail.getPhone();
            log.info("发送短信通知{}", phone);
            String smsTemplateCode = smsService.getTemplateCode(messageDto);
            SendStatus sendStatus = smsService.sendShortMessage(smsTemplateCode, phone, system, metadataName);
            eventsService.recordEvents(MAIL_SUBJECT, smsContent, phone, messageDto, sendStatus, retry, Type.NOTICE_SMS);

        }

        // 发送微信通知
        if (sendWeChat) {
            String openId = userDetail.getOpenid();
            if (StringUtils.isBlank(openId)) {
                log.error("Current user ({}, {}) can't bind weChat, cancel push message.", userDetail.getUsername(), userDetail.getUserId());
            } else {
                log.info("Send alarm message ({}, {}) to user ({}, {}).",
                        title, weChatContent, userDetail.getUsername(), userDetail.getUserId());
            }
            log.info("Send weChat message {}", openId);
            SendStatus status = mpService.sendAlarmMsg(openId, title, weChatContent, new Date());
            eventsService.recordEvents(MAIL_SUBJECT, weChatContent, openId, messageDto, status, 0, Type.NOTICE_WECHAT);
        }
    }


    public Boolean read(List<String> ids, UserDetail userDetail,Locale locale) {
        Boolean result = false;
        try {
            if (CollectionUtils.isNotEmpty(ids)) {
                List<ObjectId> objectIds = ids.stream().map(ObjectId::new).collect(Collectors.toList());
                Update update = Update.update("read", true);
                messageRepository.update(new Query(Criteria.where("_id").in(objectIds)), update, userDetail);
            }
            result = true;
        } catch (Exception e) {
            log.error("read exception", e);
        }
        return result;
    }


    public Boolean readAll(UserDetail userDetail,Locale locale) {
        String userId = userDetail.getUserId();
        Criteria criteria = new Criteria().orOperator(Criteria.where("oldUserId").is(userId), Criteria.where("user_id").is(userId));
        Query query = Query.query(criteria);
        Update update = Update.update("read", true);
        repository.getMongoOperations().updateMulti(query, update, MessageEntity.class);
        return true;
    }


    /**
     * 删除单条
     *
     * @param idList
     * @param userDetail
     * @return
     */
    public Boolean delete(List<String> idList, UserDetail userDetail,Locale locale) {
        if (!CollectionUtils.isEmpty(idList)) {
            Query query = Query.query(Criteria.where("id").in(idList));
            Update update = Update.update("isDeleted", true);
            repository.getMongoOperations().updateMulti(query, update, MessageEntity.class);
        }
        return true;
    }


    /**
     * 删除某个用户名下的所有通知
     *
     * @param userDetail
     */
    public Boolean deleteByUserId(UserDetail userDetail,Locale locale) {
        Update update = Update.update("isDeleted", true);
        Criteria criteria = new Criteria().orOperator(Criteria.where("oldUserId").is(userDetail.getUserId()), Criteria.where("user_id").is(userDetail.getUserId()));
        Query query = new Query(criteria);
        UpdateResult wr = messageRepository.getMongoOperations().updateMulti(query, update, MessageEntity.class);
        return true;
    }


    public MessageDto findById(String id) {
        MessageEntity message = messageRepository.getMongoOperations().findById(id, MessageEntity.class);
        MessageDto messageDto = null;
        if (null != message) {
            messageDto = new MessageDto();
            BeanUtils.copyProperties(message, messageDto);
            if (null != message.getMessageMetadata()) {
                messageDto.setMessageMetadata(JSON.toJSONString(message.getMessageMetadata()));
                messageDto.setMessageMetadataObject(message.getMessageMetadata());
            }
        }
        return messageDto;
    }


    private String getHrefClick(String sourceId) {
        Object hostUrl = settingsService.getByCategoryAndKey("SMTP", "emailHref");
        String clickHref = hostUrl + "monitor?id=" + sourceId + "{sourceId}&isMoniting=true&mapping=cluster-clone";
        return clickHref;
    }

    private Boolean getDefaultNotification(String system, String msgType, String notificationType) {
        Object defaultNotification = settingsService.getByCategoryAndKey("notification", "notification");
        Map<String, List> defaultSettingMap = JsonUtil.parseJson((String) defaultNotification, Map.class);

        String key = "";
        String label = "";
        if (system.equals(SystemEnum.AGENT.getValue())) {
            key = "agentNotification";
            if (msgType.equals(MsgTypeEnum.CONNECTED.getValue())) {
                label = "agentStarted";
            } else if (msgType.equals(MsgTypeEnum.CONNECTION_INTERRUPTED.getValue())) {
                label = "agentStopped";
            }
        } else if (system.equals(SystemEnum.DATAFLOW.getValue())) {
            key = "runNotification";
            if (msgType.equals(MsgTypeEnum.CONNECTED.getValue())) {
                label = "jobStarted";
            } else if (msgType.equals(MsgTypeEnum.CONNECTION_INTERRUPTED.getValue())) {
                label = "jobStateError";
            }
        }

        List<Map> agentNotificationMapList = defaultSettingMap.get(key);
        if (CollectionUtils.isNotEmpty(agentNotificationMapList)) {
            for (Map<String, Boolean> agentNotificationMap : agentNotificationMapList) {
                if (label.equals(agentNotificationMap.get("label"))) {
                    return agentNotificationMap.get(notificationType);
                }
            }
        }
        return false;
    }

    private Query parseWhereCondition(Where where, UserDetail userDetail) {
        Query query = new Query(Criteria.where("user_id").is(userDetail.getUserId())
                .and("isDeleted").is(false));

        where.forEach((prop, value) -> {
            if (!query.getQueryObject().containsKey(prop)) {
                query.addCriteria(Criteria.where(prop).is(value));
            }
        });
        return query;
    }

    public Long checkMessageLimit(UserDetail userDetail){
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date today = calendar.getTime();
        return repository.count(Query.query(Criteria.where("user_id").is(userDetail.getUserId())
                .and("isSend").is(true).and("createTime").gte(today)));
    }

    public boolean checkSending(UserDetail userDetail){
        return !settingsService.isCloud() || checkMessageLimit(userDetail) < CommonUtils.getPropertyInt("cloud_mail_limit",MailUtils.CLOUD_MAIL_LIMIT);
    }



}
