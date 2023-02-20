package com.tapdata.tm.events.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.constant.Type;
import com.tapdata.tm.events.dto.EventsDto;
import com.tapdata.tm.events.entity.EventData;
import com.tapdata.tm.events.entity.Events;
import com.tapdata.tm.events.entity.FailResult;
import com.tapdata.tm.events.repository.EventsRepository;
import com.tapdata.tm.message.constant.MessageMetadata;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.sms.SmsService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SendStatus;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
@Service
public class EventsService extends BaseService<EventsDto, Events, ObjectId, EventsRepository> {

    public EventsService(@NonNull EventsRepository repository) {
        super(repository, EventsDto.class, Events.class);
    }

    @Autowired
    MailUtils mailUtils;

    @Autowired
    @Lazy
    MessageService messageService;

    @Autowired
    UserService userService;

    @Autowired
    SettingsService settingsService;

    @Autowired
    SmsService smsService;

    @Override
    protected void beforeSave(EventsDto dto, UserDetail userDetail) {

    }


    public List<EventsDto> findByGroupId(String groupId) {
        Query query = new Query();
        query.addCriteria(Criteria.where("groupId").is(groupId));
        return findAll(query);
    }

    public Events save(Events events) {
        return repository.getMongoOperations().save(events);
    }


    public Events recordEvents(String title, String content, String to, String messageId, String userId, SendStatus sendStatus, Integer retry, String eventType) {
        Events retEvents = null;
        try {
            String sendGroupId = messageId;
            Events eventsEntity = new Events();
            EventData eventData = new EventData(title, content);
            eventsEntity.setEvent_data(eventData);
            eventsEntity.setGroupId(IdUtil.simpleUUID());
            eventsEntity.setType(eventType);
            eventsEntity.setReceivers(to);
            eventsEntity.setUserId(userId);
            eventsEntity.setTag("user");
            //用message的id来作为groupid
            eventsEntity.setSendGroupId(sendGroupId);
            eventsEntity.setEvent_status(sendStatus.getStatus());
            eventsEntity.setLastUpdAt(new Date());
            eventsEntity.setCreateAt(new Date());

            FailResult failResult = new FailResult();
            if ("false".equals(sendStatus.getStatus())) {
                //发送失败
                failResult.setRetry(retry.longValue());
                failResult.setFail_message(sendStatus.getErrorMessage());
                failResult.setNext_retry(DateUtil.offsetMinute(new Date(), 1).getTime());
                failResult.setTs(new Date().getTime());
            } else {
                failResult.setRetry(0L);
                failResult.setFail_message("");
            }
            eventsEntity.setFailed_result(failResult);
            log.info(JSON.toJSONString(eventsEntity));
            retEvents = save(eventsEntity);
        } catch (Exception e) {
            log.error("记录events 异常", e);
        }
        return retEvents;
    }

    public Events asynAdd(String title, String content, String to, String messageId, String userId, String type) {
        Events retEvents = null;
        try {
            Events eventsEntity = new Events();
            EventData eventData = new EventData(title, content);
            eventsEntity.setEvent_data(eventData);
            eventsEntity.setGroupId(IdUtil.simpleUUID());
            eventsEntity.setType(type);
            eventsEntity.setReceivers(to);
            eventsEntity.setUserId(userId);
            eventsEntity.setTag("user");
            //用message的id来作为groupid
            eventsEntity.setSendGroupId(messageId);
            eventsEntity.setEvent_status("false");
            eventsEntity.setLastUpdAt(new Date());
            eventsEntity.setCreateAt(new Date());

            FailResult failResult = new FailResult();
            //发送失败
            failResult.setRetry(0L);
            failResult.setFail_message("init add");
            failResult.setNext_retry(DateUtil.offsetMinute(new Date(), 1).getTime());
            failResult.setTs(new Date().getTime());
            eventsEntity.setFailed_result(failResult);
            log.info(JSON.toJSONString(eventsEntity));
            retEvents = save(eventsEntity);
        } catch (Exception e) {
            log.error("记录events 异常", e);
        }
        return retEvents;
    }


    public Events recordEvents(String title, String content, String to, MessageDto messageDto, SendStatus sendStatus, Integer retry, String eventType) {
        Events retEvents = null;
        try {
            String sendGroupId = messageDto.getId().toString();
            Events eventsEntity = new Events();
            EventData eventData = new EventData(title, content);
            eventsEntity.setEvent_data(eventData);
            eventsEntity.setGroupId(IdUtil.simpleUUID());
            eventsEntity.setType(eventType);
//            eventsEntity.setName("job-operation-notice-" + eventType);
            eventsEntity.setReceivers(to);
            eventsEntity.setUserId(messageDto.getUserId());
            eventsEntity.setTag("user");
            //用message的id来作为groupid
            eventsEntity.setSendGroupId(sendGroupId);
            eventsEntity.setEvent_status(sendStatus.getStatus());
            eventsEntity.setLastUpdAt(new Date());
            eventsEntity.setCreateAt(new Date());

            FailResult failResult = new FailResult();
            if ("false".equals(sendStatus.getStatus())) {
                //发送失败
                failResult.setRetry(retry.longValue());
                failResult.setFail_message(sendStatus.getErrorMessage());
                failResult.setNext_retry(DateUtil.offsetMinute(new Date(), 1).getTime());
                failResult.setTs(new Date().getTime());
            } else {
                failResult.setRetry(0L);
                failResult.setFail_message("");
            }
            eventsEntity.setFailed_result(failResult);
            log.info(JSON.toJSONString(eventsEntity));
            retEvents = save(eventsEntity);
        } catch (Exception e) {
            log.error("记录events 异常", e);
        }
        return retEvents;
    }


    /**
     * 多个实例竞争获取event,只有一个实例能获取到
     *
     * @return
     */
    /*@Deprecated
    public Events findOneUnLock() {
        Criteria criteria = new Criteria().orOperator(Criteria.where("type").is(Type.NOTICE_MAIL), Criteria.where("type").is(Type.NOTICE_SMS));
        criteria.and("failed_result.retry").lt(5);
        criteria.and("event_status").is("false");
        criteria.and("lock").ne(false);
        Query query = Query.query(criteria).with(Sort.by("createTime").descending());

        Update update = new Update();
        update.set("lock", true);
        Events lockEvent = repository.getMongoOperations().findAndModify(query, update, Events.class);
        return lockEvent;
    }*/

    /**
     * 定时器调用该方法，继续发送失败的邮件和短信
     */
    public void completeInform() {
        List<EventsDto> allFailedEvents = findFailEvent();

        if (CollectionUtils.isEmpty(allFailedEvents)) {
            log.info("没有需要重发的邮件");
            return;
        }
        //按 发送短信，和发送邮件  进行分组
        Map<String, List<EventsDto>> nameToEvents = allFailedEvents.stream().collect(Collectors.groupingBy(p -> p.getType(), Collectors.toList()));
        log.info("需要进行重发的记录数  {}", allFailedEvents.size());

        List<EventsDto> failMailEvents = nameToEvents.getOrDefault(Type.NOTICE_MAIL, Collections.emptyList());
        List<EventsDto> failSmsEvents = nameToEvents.getOrDefault(Type.NOTICE_SMS, Collections.emptyList());


        failMailEvents.forEach(failEvent -> {
            String messageId = failEvent.getSendGroupId();
            log.info("messageId:{},还没有发送成功，现在重新发送", messageId);
            MessageDto failMessageDto = messageService.findById(messageId);
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(failMessageDto.getUserId()));
            SendStatus sendStatus = sendEmail(failMessageDto, userDetail, failEvent);
            if ("false".equals(sendStatus.getStatus())) {
                updateFailInfo(failEvent.getId(), userDetail, sendStatus);
            } else {
                updateEventStatus(failEvent.getId(), "true", userDetail);
            }
        });


        failSmsEvents.forEach(failEvent -> {
            String messageId = failEvent.getSendGroupId();
            log.info("messageId:{},还没有发送成功，现在重新发送", messageId);
            MessageDto failMessageDto = messageService.findById(messageId);
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(failMessageDto.getUserId()));
            SendStatus sendStatus = sendSms(failMessageDto, userDetail, failEvent);
            if ("false".equals(sendStatus.getStatus())) {
                updateFailInfo(failEvent.getId(), userDetail, sendStatus);
            } else {
                updateEventStatus(failEvent.getId(), "true", userDetail);
            }
        });

    }


    /**
     * 原子操作，fail_result retrye 加1
     */
    private UpdateResult updateFailInfo(ObjectId id, UserDetail userDetail, SendStatus sendStatus) {
        Update update = new Update();
        Query query = Query.query(Criteria.where("id").is(id));
        update.inc("failed_result.retry", 1);
        update.set("failed_result.fail_message", sendStatus.getErrorMessage());
        update.set("event_status", sendStatus.getStatus());
        return super.update(query, update);
    }


    /**
     * 如果重试发送成功，就只更新event_status
     */
    public Long updateEventStatus(ObjectId id, String eventStatus, UserDetail userDetail) {
        Update update = new Update();
        update.set("event_status", eventStatus);
        update.inc("failed_result.retry", 1);
        update.set("failed_result.fail_message", "");
        UpdateResult updateResult = super.update(Query.query(Criteria.where("id").is(id)), update);
        return updateResult.getModifiedCount();
    }


    /**
     * 只查找fail_result 的 retry次数小于5的记录，进行重发
     *
     * @return
     */
    private List<EventsDto> findFailEvent() {
        Criteria criteria = new Criteria().orOperator(Criteria.where("type").is(Type.NOTICE_SMS), Criteria.where("type").is(Type.NOTICE_MAIL));
        criteria.and("failed_result.retry").lt(5);
        criteria.and("event_status").is("false");
        Query query = Query.query(criteria);
        List<EventsDto> allFailedEvents = findAll(query);
        return allFailedEvents;
    }


    /**
     * @param messageDto
     * @param userDetail
     * @param events
     * @return
     */
    private SendStatus sendEmail(MessageDto messageDto, UserDetail userDetail, EventsDto events) {
        String username = "Hi, " + (userDetail.getUsername() == null ? "" : userDetail.getEmail()) + ": ";

        String serverName = messageDto.getServerName();
        MsgTypeEnum msgTypeEnum = MsgTypeEnum.getEnumByValue(messageDto.getMsg());
        SystemEnum systemEnum = SystemEnum.getEnumByValue(messageDto.getSystem());
        String toAddress = events.getReceivers();

//        String emailHref = mailUtils.getHrefClick(messageDto.getSourceId(), systemEnum, msgTypeEnum);
        String mailContent = mailUtils.getMailContent(systemEnum, msgTypeEnum);
        String title = mailUtils.getMailTitle(systemEnum, msgTypeEnum);
        SendStatus sendStatus = mailUtils.sendHtmlMail(toAddress, username, serverName, title, mailContent);
        return sendStatus;
    }

    private SendStatus sendSms(MessageDto messageDto, UserDetail userDetail, EventsDto events) {
        SendStatus sendStatus = new SendStatus("false", "");
        String metadata = messageDto.getMessageMetadata();
        MessageMetadata messageMetadata = JSONUtil.toBean(metadata, MessageMetadata.class);
        String metadataName = messageMetadata.getName();

        String receiver = events.getReceivers();
        String smsTemplateCode = smsService.getTemplateCode(messageDto.getMsg());
        //msg为STOPPED_BY_ERROR  还没有设计发送短信
        if (StringUtils.isNotEmpty(smsTemplateCode)) {
            sendStatus = smsService.sendShortMessage(smsTemplateCode, receiver, messageDto.getSystem(), metadataName);
        } else {
//            log.info("msg为STOPPED_BY_ERROR  还没有设计发送短信");
            sendStatus.setErrorMessage("msg为STOPPED_BY_ERROR  还没有设计发送短信");
        }
        return sendStatus;
    }

}
