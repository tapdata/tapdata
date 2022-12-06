package com.tapdata.tm.message.service;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
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
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.constant.Type;
import com.tapdata.tm.events.service.EventsService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MessageMetadata;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.message.vo.MessageListVo;
import com.tapdata.tm.task.constant.TaskEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SendStatus;
import com.tapdata.tm.utils.SmsUtils;
import lombok.NonNull;
import lombok.Setter;
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
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MessageService extends BaseService {

    MessageRepository messageRepository;
    UserService userService;
    TaskRepository taskRepository;
    MailUtils mailUtils;
    EventsService eventsService;
    SettingsService settingsService;

    private final static String MAIL_SUBJECT = "【Tapdata】";
    private final static String MAIL_CONTENT = "尊敬的用户您好，您在Tapdata Cloud上创建的Agent:";
    private final static String WILL_RELEASE_AGENT_MAIL_CONTENT = "因超过一周未使用即将在明天晚上20:00自动回收，如果您需要继续使用可以在清理前登录系统自动延长使用时间。";
    private final static String RELEASE_AGENT_MAIL_CONTENT = "因超过一周未使用已自动回收，如果您需要继续使用可通过新手引导再次创建。";

    //mail title
    private final static String WILL_RELEASE_AGENT_TITLE = "Tapdata】测试Agent资源即将回收提醒";
    private final static String RELEASE_AGENT_TITLE = "【Tapdata】测试Agent资源回收提醒";


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
    public Page<MessageListVo> find(Filter filter, UserDetail userDetail) {
        TmPageable tmPageable = new TmPageable();

        //page由limit 和skip计算的来
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        Query query = parseWhereCondition(filter.getWhere(), userDetail);
        List<String> collect = Arrays.stream(MsgTypeEnum.values()).filter(t -> t != MsgTypeEnum.ALARM).map(MsgTypeEnum::getValue).collect(Collectors.toList());
        query.addCriteria(Criteria.where("msg").in(collect));
        return getMessageListVoPage(query, tmPageable);
    }

    @NotNull
    private Page<MessageListVo> getMessageListVoPage(Query query, TmPageable tmPageable) {
        long total = messageRepository.getMongoOperations().count(query, MessageEntity.class);
        query.with(Sort.by("createTime").descending());
        query.with(tmPageable);
        List<MessageEntity> messageEntityList = messageRepository.getMongoOperations().find(query, MessageEntity.class);

        List<MessageListVo> messageListVoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(messageEntityList, MessageListVo.class);
        messageListVoList.forEach(messageListVo -> {
            if (StringUtils.isEmpty(messageListVo.getServerName())) {
                messageListVo.setServerName(messageListVo.getAgentName());
            }
        });

        return new Page<>(total, messageListVoList);
    }

    public Page<MessageListVo> list(MsgTypeEnum type, String level, Boolean read, Integer page, Integer size, UserDetail userDetail) {
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

        return getMessageListVoPage(query, tmPageable);
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
            repository.getMongoOperations().save(messageEntity);
            informUser(msgTypeEnum, systemEnum, messageMetadata, sourceId, messageEntity.getId().toString(), userDetail);
        } catch (Exception e) {
            log.error("新增消息异常，", e);
        }
    }

    /**
     * 增加迁移任务消息
     *
     * @param serverName
     */
    public void addMigration(String serverName, String sourceId, String userId) {
        TaskEntity taskEntity = taskRepository.findById(sourceId).get();
        if (null == taskEntity) {
            log.error("数据有误，找不到  {}  对应的", sourceId);
            return;
        }
//        * 每个子任务包含以下的状态： 调度中，调度失败，带运行，运行中，停止中，已停止，已完成，错误。
        String status = taskEntity.getStatus();

        Level level = Level.INFO;
        MsgTypeEnum msgTypeEnum = MsgTypeEnum.CONNECTED;
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));
        if (TaskEnum.STATUS_SCHEDULE_FAILED.getValue().equals(status) ||
                TaskEnum.STATUS_ERROR.getValue().equals(status)) {
            level = Level.ERROR;
        }
        addMigration(serverName, sourceId, msgTypeEnum, level, userDetail);
    }

    /**
     * 增加迁移任务消息
     *
     * @param serverName
     * @param msgTypeEnum
     * @param level
     * @param userDetail
     */
    public void addMigration(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail) {
        MessageEntity saveMessage = new MessageEntity();

        if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
            log.info("任务恢复运行，只add message， 不用发邮件短信");
            return;
        }

        NotificationDto notificationDto = needInform(SystemEnum.MIGRATION, msgTypeEnum);
        Notification userNotification = userDetail.getNotification();
        if (null != userNotification) {
            //用户设置优先
            if (userNotification.getStoppedByError().getEmail()) {
                log.info("dataflow出错，email 通知");
                informUserEmail(msgTypeEnum, SystemEnum.MIGRATION, serverName, sourceId, saveMessage.getId().toString(), userDetail);
            }
            if (userNotification.getStoppedByError().getSms()) {
                log.info("dataflow出错，sms 通知");
                informUserSms(msgTypeEnum, SystemEnum.MIGRATION, serverName, sourceId, saveMessage.getId().toString(), userDetail);

            }
        }
        if (null != notificationDto) {
            saveMessage = addMessage(serverName, sourceId, SystemEnum.MIGRATION, msgTypeEnum, "", level, userDetail, notificationDto.getNotice());

            if (notificationDto.getEmail()) {
                SendStatus sendStatus = new SendStatus("false", "");
                eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), saveMessage.getId().toString(), userDetail.getUserId(), sendStatus, 0, Type.NOTICE_MAIL);
            }
        }
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
            SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), userDetail.getUsername(), serverName, SystemEnum.SYNC, msgTypeEnum, sourceId);
            eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), saveMessage.getId().toString(), userDetail.getUserId(), sendStatus, 0, Type.NOTICE_MAIL);
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
        repository.getMongoOperations().save(messageEntity);
        return messageEntity;
    }

    private MessageEntity addMessage(String serverName, String sourceId, SystemEnum systemEnum, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail, Boolean isNotify) {
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
        repository.getMongoOperations().save(messageEntity);
        return messageEntity;
    }

    public void addMessage(MessageEntity messageEntity) {
        repository.getMongoOperations().save(messageEntity);
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

        SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), username, serverName, clickHref, systemEnum, msgType);
        eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_MAIL);
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
        String smsTemplateCode = SmsUtils.getTemplateCode(msgType);
        SendStatus sendStatus = SmsUtils.sendShortMessage(smsTemplateCode, phone, systemEnum.getValue(), servername);
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

            SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), username, metadataName, clickHref, systemEnum, msgType);
            eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_MAIL);

        }

        //发送短信
        if (sendSms) {
            Integer retry = 1;
            String phone = userDetail.getPhone();
            String smsTemplateCode = SmsUtils.getTemplateCode(msgType);
            SendStatus sendStatus = SmsUtils.sendShortMessage(smsTemplateCode, phone, systemEnum.getValue(), metadataName);
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
    @Deprecated
    public MessageDto add(MessageDto messageDto) {
        try {
            MessageEntity messageEntity = new MessageEntity();
            BeanUtil.copyProperties(messageDto, messageEntity, "messageMetadata");

            MessageMetadata messageMetadata = JSONUtil.toBean(messageDto.getMessageMetadata(), MessageMetadata.class);
            messageEntity.setMessageMetadata(messageMetadata);

            String userId = messageDto.getUserId();
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));
            if (null != userDetail) {
                messageEntity.setUserId(userId);
                messageEntity.setCreateAt(new Date());
                messageEntity.setServerName(messageDto.getAgentName());
                messageEntity.setLastUpdAt(new Date());
                messageEntity.setLastUpdBy(userDetail.getUsername());
                repository.getMongoOperations().save(messageEntity);
                messageDto.setId(messageEntity.getId().toString());
                informUser(messageDto);
            } else {
                log.error("找不到用户信息. userId:{}", userId);
            }

        } catch (Exception e) {
            log.error("新增消息异常，", e);
        }
        return messageDto;
    }


    /**
     * 根据设置通知用户,短信或者邮件
     */
    @Deprecated
    private void informUser(MessageDto messageDto) {
        String msgType = messageDto.getMsg();
        String userId = messageDto.getUserId();
        String system = messageDto.getSystem();


        UserDetail userDetail = userService.loadUserById(new ObjectId(userId));

//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());

        Notification notification = userDetail.getNotification();

        //判断notification是否为空，如果为空，则按照系统配置发送通知
        Boolean sendEmail = false;
        Boolean sendSms = false;
        if (null != notification) {
            Object eventType = BeanUtil.getProperty(notification, msgType);
            sendEmail = BeanUtil.getProperty(eventType, "email");
            sendSms = BeanUtil.getProperty(eventType, "sms");
        } else {
            //如果用户的设置通知为空,就全全局设置  setting中取
            sendEmail = getDefaultNotification(system, msgType, "email");
            sendSms = getDefaultNotification(system, msgType, "notice");
        }

        MessageMetadata messageMetadata = JSONUtil.toBean(messageDto.getMessageMetadata(), MessageMetadata.class);

        //目前msgNotification 只会有三种情况
        String metadataName = messageMetadata.getName();
        String emailTip = "";
        String smsContent = "";

        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            emailTip = "状态变为运行中";
            smsContent = "尊敬的用户，你好，您在Tapdata Cloud 上创建的任务:" + metadataName + " 正在运行";
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            emailTip = "状态已由运行中变为离线，可能会影响您的任务正常运行，请及时处理。";
            smsContent = "尊敬的用户，你好，您在Tapdata Cloud 上创建的任务:" + metadataName + " 出错，请即使处理";
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {

        }


        //发送邮件
        if (sendEmail) {
            Integer retry = 1;
            log.info("发送邮件通知");
            String username = "Hi, " + (messageDto.getUsername() == null ? "" : messageDto.getUsername()) + ": ";
//            Object hostUrl = settingsService.getByCategoryAndKey("SMTP", "emailHref");
//            String clickHref = hostUrl + "monitor?id=" + sourceId + "{sourceId}&isMoniting=true&mapping=cluster-clone";
            MsgTypeEnum msgTypeEnum = MsgTypeEnum.getEnumByValue(msgType);
            String clickHref = mailUtils.getAgentClick(metadataName, msgTypeEnum);
            SendStatus sendStatus = mailUtils.sendHtmlMail(MAIL_SUBJECT, userDetail.getEmail(), username, metadataName, clickHref, emailTip);
            eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageDto, sendStatus, retry, Type.NOTICE_MAIL);

        }

        //发送短信
        if (sendSms) {
            Integer retry = 1;
            String phone = userDetail.getPhone();
            String smsTemplateCode = SmsUtils.getTemplateCode(messageDto);
            SendStatus sendStatus = SmsUtils.sendShortMessage(smsTemplateCode, phone, system, metadataName);
            eventsService.recordEvents(MAIL_SUBJECT, smsContent, phone, messageDto, sendStatus, retry, Type.NOTICE_SMS);

        }
    }


    public Boolean read(List<String> ids, UserDetail userDetail) {
        Boolean result = false;
        try {
            if (CollectionUtils.isNotEmpty(ids)) {
                ids.forEach(id -> {
                    Update update = Update.update("read", true);
                    messageRepository.getMongoOperations().updateFirst(new Query(Criteria.where("_id").is(id)), update, MessageEntity.class);
                });
            }
            result = true;
        } catch (Exception e) {
            log.error("read exception", e);
        }
        return result;
    }


    public Boolean readAll(UserDetail userDetail) {
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
    public Boolean delete(List<String> idList, UserDetail userDetail) {
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
    public Boolean deleteByUserId(UserDetail userDetail) {
        Update update = Update.update("isDeleted", true);
        Criteria criteria = new Criteria().orOperator(Criteria.where("oldUserId").is(userDetail.getUserId()), Criteria.where("user_id").is(userDetail.getUserId()));
        Query query = new Query(criteria);
        UpdateResult wr = messageRepository.getMongoOperations().updateMulti(query, update, MessageEntity.class);
        return true;
    }

    @Override
    protected void beforeSave(BaseDto dto, UserDetail userDetail) {

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


}