package com.tapdata.tm.message.service;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.DateUtil;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.dto.AgentNotificationDto;
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
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
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
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.constant.SubTaskEnum;
import com.tapdata.tm.task.entity.SubTaskEntity;
import com.tapdata.tm.task.repository.SubTaskRepository;
import com.tapdata.tm.task.service.SubTaskService;
import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SendStatus;
import com.tapdata.tm.utils.SmsUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
public class MessageService extends BaseService {

    @Autowired
    MessageRepository messageRepository;

    @Autowired
    UserService userService;


    @Autowired(required = false)
    SubTaskRepository subTaskRepository;

    @Autowired
    MailUtils mailUtils;

    @Autowired
    EventsService eventsService;

    @Autowired
    SettingsService settingsService;

    @Autowired
    private MessageQueueService messageQueueService;

    private final static String MAIL_SUBJECT = "???Tapdata???";


    /*
            emailObj.agent.event_data.willReleaseAgent.title = '???Tapdata?????????Agent????????????????????????';
            emailObj.agent.event_data.willReleaseAgent.message = willReleaseAgent;
            emailObj.agent.event_data.releaseAgent.title = '???Tapdata?????????Agent??????????????????';
            emailObj.agent.event_data.releaseAgent.message = releaseAgent;

        */
    private final static String MAIL_CONTENT = "??????????????????????????????Tapdata Cloud????????????Agent:";
    private final static String WILL_RELEASE_AGENT_MAIL_CONTENT = "?????????????????????????????????????????????20:00???????????????????????????????????????????????????????????????????????????????????????????????????";
    private final static String RELEASE_AGENT_MAIL_CONTENT = "?????????????????????????????????????????????????????????????????????????????????????????????????????????";


    //mail title
    private final static String WILL_RELEASE_AGENT_TITLE = "Tapdata?????????Agent????????????????????????";
    private final static String RELEASE_AGENT_TITLE = "???Tapdata?????????Agent??????????????????";


    public MessageService(@NonNull MessageRepository repository) {
        super(repository, MessageDto.class, MessageEntity.class);
    }


    /**
     * ??????message???id????????????????????????????????????????????????limit ???skip?????????
     *
     * @param filter
     * @param userDetail
     * @return
     */
    public Page<MessageListVo> find(Filter filter, UserDetail userDetail) {
        TmPageable tmPageable = new TmPageable();

        //page???limit ???skip????????????
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        Query query = parseWhereCondition(filter.getWhere(), userDetail);

        Long total = messageRepository.getMongoOperations().count(query, MessageEntity.class);
        List<MessageEntity> messageEntityList = messageRepository.getMongoOperations().find(query.with(tmPageable), MessageEntity.class);

        List<MessageListVo> messageListVoList = new ArrayList<>();
        messageListVoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(messageEntityList, MessageListVo.class);
        messageListVoList.forEach(messageListVo -> {
            if (StringUtils.isEmpty(messageListVo.getServerName())) {
                messageListVo.setServerName(messageListVo.getAgentName());
            }
        });

        Page<MessageListVo> result = new Page(total, messageListVoList);
        return result;
    }

    /**
     * ????????????????????????????????????????????????
     *
     * @param filter
     * @param userDetail
     * @return
     */
    @Override
    public long count(Where where, UserDetail userDetail) {
        Query query = parseWhereCondition(where, userDetail);
        Long total = 0L;
        total = messageRepository.getMongoOperations().count(query, MessageEntity.class);
        return total;
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
            log.error("?????????????????????", e);
        }
    }

    /**
     * ????????????????????????
     *
     * @param serverName
     */
    public void addMigration(String serverName, String sourceId, String userId) {
        SubTaskEntity subTaskEntity = subTaskRepository.findById(sourceId).get();
        if (null == subTaskEntity) {
            log.error("????????????????????????  {}  ??????????????????", sourceId);
            return;
        }
//        * ??????????????????????????????????????? ????????????????????????????????????????????????????????????????????????????????????????????????
        String status = subTaskEntity.getStatus();

        Level level = Level.INFO;
        MsgTypeEnum msgTypeEnum = MsgTypeEnum.CONNECTED;
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(userId));
        if (SubTaskEnum.STATUS_SCHEDULE_FAILED.getValue().equals(status) ||
                SubTaskEnum.STATUS_ERROR.getValue().equals(status)) {
            level = Level.ERROR;
        }
        addMigration(serverName, sourceId, msgTypeEnum, level, userDetail);
    }

    /**
     * ????????????????????????
     *
     * @param serverName
     * @param msgTypeEnum
     * @param level
     * @param userDetail
     */
    public void addMigration(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, Level level, UserDetail userDetail) {
        MessageEntity saveMessage = new MessageEntity();

        if (MsgTypeEnum.CONNECTED.equals(msgTypeEnum)) {
            log.info("????????????????????????add message??? ?????????????????????");
            return;
        }

        NotificationDto notificationDto = needInform(SystemEnum.MIGRATION, msgTypeEnum);
        Notification userNotification = userDetail.getNotification();
        if (null != userNotification) {
            //??????????????????
            if (userNotification.getStoppedByError().getEmail()) {
                log.info("dataflow?????????email ??????");
                informUserEmail(msgTypeEnum, SystemEnum.MIGRATION, serverName, sourceId, saveMessage.getId().toString(), userDetail);
            }
            if (userNotification.getStoppedByError().getSms()) {
                log.info("dataflow?????????sms ??????");
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


 /*   @Deprecated
    public MessageDto addAgentMess(String serverName, String sourceId, MsgTypeEnum msgTypeEnum, String title, Level level, UserDetail userDetail) {
        NotificationDto notificationDto = needInform(SystemEnum.MIGRATION, msgTypeEnum);
        MessageEntity saveMessage = null;
        Notification userNotification = userDetail.getNotification();
        saveMessage = addMessage(serverName, sourceId, SystemEnum.MIGRATION, msgTypeEnum, title, level, userDetail);
        if (null != userNotification) {
            //??????????????????
            if (userNotification.getStoppedByError().getEmail()) {
                log.info("dataflow?????????email ??????");
                asynInformUserEmail(msgTypeEnum, SystemEnum.AGENT, saveMessage.getId().toString(), userDetail);
            }
            if (userNotification.getStoppedByError().getSms()) {
                log.info("dataflow?????????sms ??????");
                asynInformUserSms(saveMessage.getId().toString(), userDetail);
            }
        } else if (null != notificationDto) {
            if (notificationDto.getNotice()) {
                saveMessage = addMessage(serverName, sourceId, SystemEnum.MIGRATION, msgTypeEnum, title, level, userDetail);
            }
            if (notificationDto.getEmail()) {
//                UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());
                SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), userDetail.getUsername(), serverName, SystemEnum.MIGRATION, msgTypeEnum, sourceId);
                eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), saveMessage.getId().toString(), userDetail.getUserId(), sendStatus, 0, Type.NOTICE_MAIL);
            }
        }

        MessageDto messageDto = new MessageDto();
        BeanUtil.copyProperties(saveMessage, messageDto);
        return messageDto;
    }*/


    /**
     * ????????????????????????
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
            //??????????????????
            if (userNotification.getStoppedByError().getEmail()) {
                log.info("dataflow?????????email ??????");
            }
            if (userNotification.getStoppedByError().getSms()) {
                log.info("dataflow?????????sms ??????");

            }
        }

        //????????????????????????
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
     * ????????????????????????
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

    /**
     * ???????????????????????????????????????????????????
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
        //??????msgNotification ?????????????????????
//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());
        if (null == userDetail.getEmail()) {
            log.error("????????????????????????  userDetail:{}", JsonUtil.toJson(userDetail));
            return;
        }
        //????????????
        Integer retry = 1;
        log.info("??????????????????");
        String username = "Hi, " + (userDetail.getUsername() == null ? "" : userDetail.getUsername()) + ": ";

//        Object hostUrl = settingsService.getByCategoryAndKey(CategoryEnum.SMTP, KeyEnum.EMAIL_HREF);
        String clickHref = mailUtils.getHrefClick(sourceId, systemEnum, msgType);

        SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), username, serverName, clickHref, systemEnum, msgType);
        eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_MAIL);
    }

    /**
     * ???????????????
     *
     * @param msgType
     * @param systemEnum
     * @param messageId
     * @param userDetail
     */
    private void asynInformUserEmail(MsgTypeEnum msgType, SystemEnum systemEnum, String messageId, UserDetail userDetail) {
        String email = userDetail.getEmail();
        if (null == email) {
            log.error("????????????????????????  userDetail:{}", JsonUtil.toJson(userDetail));
            return;
        }
        //????????????
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
            log.error("????????????????????????  userDetail:{}", JsonUtil.toJson(userDetail));
            return;
        }
        //????????????
        log.info("asynInformUserSms ");
        eventsService.asynAdd("", "", phone, messageId, userDetail.getUserId(), Type.NOTICE_SMS);
    }


    private void informUserSms(MsgTypeEnum msgType, SystemEnum systemEnum, String servername, String sourceId, String messageId, UserDetail userDetail) {
        String smsContent = "";
        //????????????
        log.info("??????????????????");
        String phone = userDetail.getPhone();
//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());
        if (StringUtils.isEmpty(phone)) {
            log.error("?????????????????? ??????  userDetail:{}", JsonUtil.toJson(userDetail));
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

        //??????notification???????????????????????????????????????????????????????????????
        Boolean sendEmail = false;
        Boolean sendSms = false;
        if (null != notification) {
            Object eventType = BeanUtil.getProperty(notification, msgType.getValue());
            sendEmail = BeanUtil.getProperty(eventType, "email");
            sendSms = BeanUtil.getProperty(eventType, "sms");
        } else {
            //?????????????????????????????????,??????????????????  setting??????
            sendEmail = getDefaultNotification(systemEnum.getValue(), msgType.getValue(), "email");
            sendSms = getDefaultNotification(systemEnum.getValue(), msgType.getValue(), "notice");
        }

        //??????msgNotification ?????????????????????
        String metadataName = messageMetadata.getName();
        String smsContent = "";

        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            smsContent = "?????????????????????????????????Tapdata Cloud ??????????????????:" + metadataName + " ????????????";
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            smsContent = "?????????????????????????????????Tapdata Cloud ??????????????????:" + metadataName + " ????????????????????????";
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {

        }

//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());
        //????????????
        if (sendEmail) {
            Integer retry = 1;
            log.info("??????????????????");
            String username = "Hi, " + (userDetail.getUsername() == null ? "" : userDetail.getUsername()) + ": ";

            Object hostUrl = settingsService.getByCategoryAndKey("SMTP", "emailHref");
            String clickHref = hostUrl + "monitor?id=" + sourceId + "{sourceId}&isMoniting=true&mapping=cluster-clone";

            SendStatus sendStatus = mailUtils.sendHtmlMail(userDetail.getEmail(), username, metadataName, clickHref, systemEnum, msgType);
            eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_MAIL);

        }

        //????????????
        if (sendSms) {
            Integer retry = 1;
            String phone = userDetail.getPhone();
            String smsTemplateCode = SmsUtils.getTemplateCode(msgType);
            SendStatus sendStatus = SmsUtils.sendShortMessage(smsTemplateCode, phone, systemEnum.getValue(), metadataName);
            eventsService.recordEvents(MAIL_SUBJECT, smsContent, phone, messageId, userDetail.getUserId(), sendStatus, retry, Type.NOTICE_SMS);
        }
    }


    /**
     * ?????????????????????????????????message
     * 1???agent?????????TCM?????????TCM???????????????????????????
     * 2???agent?????????TCM?????????TCM???????????????????????????
     * 3????????????????????????dataFlows?????????status????????????error?????????????????????????????????????????????  ?????????????????????????????????????????????????????????
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
                log.error("?????????????????????. userId:{}", userId);
            }

        } catch (Exception e) {
            log.error("?????????????????????", e);
        }
        return messageDto;
    }


    /**
     * ????????????????????????,??????????????????
     */
    @Deprecated
    private void informUser(MessageDto messageDto) {
        String msgType = messageDto.getMsg();
        String userId = messageDto.getUserId();
        String system = messageDto.getSystem();


        UserDetail userDetail = userService.loadUserById(new ObjectId(userId));

//        UserInfoDto userInfoDto = tcmService.getUserInfo(userDetail.getExternalUserId());

        Notification notification = userDetail.getNotification();

        //??????notification???????????????????????????????????????????????????????????????
        Boolean sendEmail = false;
        Boolean sendSms = false;
        if (null != notification) {
            Object eventType = BeanUtil.getProperty(notification, msgType);
            sendEmail = BeanUtil.getProperty(eventType, "email");
            sendSms = BeanUtil.getProperty(eventType, "sms");
        } else {
            //?????????????????????????????????,??????????????????  setting??????
            sendEmail = getDefaultNotification(system, msgType, "email");
            sendSms = getDefaultNotification(system, msgType, "notice");
        }

        MessageMetadata messageMetadata = JSONUtil.toBean(messageDto.getMessageMetadata(), MessageMetadata.class);

        //??????msgNotification ?????????????????????
        String metadataName = messageMetadata.getName();
        String emailTip = "";
        String smsContent = "";

        if (MsgTypeEnum.CONNECTED.getValue().equals(msgType)) {
            emailTip = "?????????????????????";
            smsContent = "?????????????????????????????????Tapdata Cloud ??????????????????:" + metadataName + " ????????????";
        } else if (MsgTypeEnum.CONNECTION_INTERRUPTED.getValue().equals(msgType)) {
            emailTip = "????????????????????????????????????????????????????????????????????????????????????????????????";
            smsContent = "?????????????????????????????????Tapdata Cloud ??????????????????:" + metadataName + " ????????????????????????";
        } else if (MsgTypeEnum.STOPPED_BY_ERROR.getValue().equals(msgType)) {

        }


        //????????????
        if (sendEmail) {
            Integer retry = 1;
            log.info("??????????????????");
            String username = "Hi, " + (messageDto.getUsername() == null ? "" : messageDto.getUsername()) + ": ";
//            Object hostUrl = settingsService.getByCategoryAndKey("SMTP", "emailHref");
//            String clickHref = hostUrl + "monitor?id=" + sourceId + "{sourceId}&isMoniting=true&mapping=cluster-clone";
            MsgTypeEnum msgTypeEnum = MsgTypeEnum.getEnumByValue(msgType);
            String clickHref = mailUtils.getAgentClick(metadataName, msgTypeEnum);
            SendStatus sendStatus = mailUtils.sendHtmlMail(MAIL_SUBJECT, userDetail.getEmail(), username, metadataName, clickHref, emailTip);
            eventsService.recordEvents(MAIL_SUBJECT, MAIL_CONTENT, userDetail.getEmail(), messageDto, sendStatus, retry, Type.NOTICE_MAIL);

        }

        //????????????
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
     * ????????????
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
     * ???????????????????????????????????????
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
        Query query = new Query();
        query.addCriteria(Criteria.where("isDeleted").ne(true));
        String userId = userDetail.getUserId();
        query.addCriteria(new Criteria().orOperator(Criteria.where("oldUserId").is(userId), Criteria.where("user_id").is(userId)));

        where.forEach((prop, value) -> {
            if (!query.getQueryObject().containsKey(prop)) {
                query.addCriteria(Criteria.where(prop).is(value));
            }
        });
        return query;
    }


}