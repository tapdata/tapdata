package com.tapdata.tm.message.service;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.Settings.dto.NotificationSettingDto;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.entity.Connected;
import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.entity.StoppedByError;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MessageServiceTest extends BaseJunit {

    @Autowired
    MessageService messageService;


    @Autowired
    MessageRepository messageRepository;

    @Autowired
    MessageQueueService messageQueueService;

    @Autowired
    UserService userService;

    @Test
    void find() {
        Filter filter = JsonUtil.parseJson("{\"where\":{\"isDeleted\":{\"neq\":true}},\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0}", Filter.class);
        printResult(messageService.find(filter, null));
    }

    @Test
    void findById() {
        printResult(messageService.findById("616531ff0aaffb66c4df4a25"));
    }

    @Test
    void findAll() {
        TmPageable tmPageable = new TmPageable();
        tmPageable.setPage(1);
        tmPageable.setSize(10);
        Query query = new Query();
        query.addCriteria(new Criteria().orOperator(Criteria.where("user_id").is("6193700c1516f86b493d21f2"), Criteria.where("userId").is("6193700c1516f86b493d21f2")));

        List<MessageEntity> messageEntityList = messageRepository.getMongoOperations().find(query.with(tmPageable), MessageEntity.class);
        printResult(messageEntityList);
    }

    @Test
    void delete() {
        List<String> idList = new ArrayList<>();
        idList.add("60ff7733e2f6520013c17cba");
        idList.add("60fff610e2f6520013c1e348");
        Update update = Update.update("isDeleted", false);

//        messageRepository.getMongoOperations().updateFirst(new Query(Criteria.where("_id").in(idList)), update, MessageEntity.class);
        messageRepository.getMongoOperations().updateFirst(new Query(Criteria.where("_id").is("60fff610e2f6520013c1e348")), update, MessageEntity.class);


/*        List<String> idList=new ArrayList<>();
        idList.add("60505d2462ed30ccd9addb48");
        idList.add("60505a9c62ed305c91add964");
        messageService.delete(idList, getUser());*/
    }


    @Test
    void read() {
    }

    @Test
    void save() {
      /*  MessageDto messageDto = new MessageDto();
        SourceModuleEnum sourceModule = SourceModuleEnum.AGENT;

        messageDto.setSourceModuleEnum(sourceModule);
        messageDto.setMsg("connectionInterrupted");

        MessageMetadata messageMetadata = new MessageMetadata("12122", "45535343fdfsdfsdf");


        messageDto.setUserId("61407a8cfa67f20019f68f9f");
        messageDto.setMessageMetadata(JSON.toJSONString(messageMetadata));

        printResult(messageDto);
        printResult(messageService.add(messageDto));*/

        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId("61407a8cfa67f20019f68f9f"));
//        messageService.add("mysq-1207", "61aecb370865be74694dca03", MsgTypeEnum.CONNECTED, SystemEnum.DATAFLOW, "61aecb370865be74694dca03", userDetail);
    }

    @Test
    void deleteByUserId() {
        messageService.deleteByUserId(getUser());

/*
  605075356cac5546a23ced76
  Update update = Update.update("read", true);
        Query query = new Query();
        query.addCriteria(Criteria.where("userId").is(getUser().getUserId()));
        messageRepository.getMongoOperations().updateFirst(query, update, MessageEntity.class);*/

    }

    @Test
    void getByUserId() {
//        printResult(messageService.getByUserId("61387c17b2bb089d9e7a38bb"));
    }

    @Test
    void deleteById() {
    }

    @Test
    void beanToMap() {
        Notification notification = new Notification();


        Connected connected = new Connected();
        connected.setEmail(false);
        connected.setSms(true);


        StoppedByError stoppedByError = new StoppedByError();
        stoppedByError.setEmail(false);
        stoppedByError.setSms(true);

        notification.setConnected(connected);
        notification.setStoppedByError(stoppedByError);

        Object connected1 = BeanUtil.getProperty(notification, "connected");
        Object stoppedByError1 = BeanUtil.getProperty(notification, "stoppedByError");
        printResult(connected1);
        printResult(stoppedByError1);

        printResult(BeanUtil.getProperty(connected1, "email"));
        printResult(BeanUtil.getProperty(connected1, "sms"));

        printResult(BeanUtil.getProperty(stoppedByError1, "email"));
        printResult(BeanUtil.getProperty(stoppedByError1, "sms"));



/*        Map map = BeanUtil.beanToMap(notification);
        Connected connected1= (Connected) map.get("connected");
        System.out.println(connected1.getEmail());
        System.out.println(connected1.getSms());*/

    }

    @Test
    public void testMessageQueue() {
        MessageQueueDto messageQueue = new MessageQueueDto();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("_id", "60b064e9a65d8e852c8523bc");
        messageQueue.setData(data);

        messageQueue.setType("test");
        messageQueue.setSender("test");
        messageQueue.setReceiver("test");
        messageQueueService.save(messageQueue);
    }


    @Test
    public void informUser() throws InterruptedException {
        MessageDto messageDto = messageService.findById("618cb1eb0303312e0ee78546");
//        messageService.informUser(messageDto);
    }

    @Test
    public void getDefaultNotification() throws InterruptedException {
//        Boolean messageDto=messageService.getDefaultNotification("agentNotification","agentStarted","notice");
//
//        printResult(messageDto);
    }

    @Test
    public void getNotificationType() throws InterruptedException {
//        printResult(messageService.getNotificationType(NotificationSettingDto.NotificationSettingEnum.RUN_NOTIFICATION));
    }


    @Test
    public void addInspect()   {
        messageService.addInspect("test_inspect", "asdad", MsgTypeEnum.INSPECT_VALUE, Level.ERROR, getUser());
    }

    @Test
    public void addMigration()   {
        UserDetail userDetail=userService.loadUserById(MongoUtils.toObjectId("61408608c4e5c40012663090"));
//        messageService.addMigration("nok","614fe38c77945f29d25342b6",MsgTypeEnum.STOPPED_BY_ERROR,"",Level.ERROR,userDetail);
    }

}
