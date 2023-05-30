package com.tapdata.tm.schedule;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.websocket.MessageInfo;
import com.tapdata.tm.commons.websocket.MessageInfoBuilder;
import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.ws.endpoint.WebSocketClusterServer;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.endpoint.WebSocketServer;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

/**
 * Watch message queue collection, read and process new document.
 */
@Service
@Slf4j
public class MessageQueueWatch {

    @Autowired
    private WebSocketServer webSocketServer;

    @Autowired
    private MessageQueueService messageQueueService;


    private ObjectId offset;

    private ObjectId getLastOffset() {
        Query query = Query.query(new Criteria());
        query.fields().include("_id");
        query.with(Sort.by(Sort.Order.desc("_id")));
        query.limit(1);
        List<MessageQueueDto> result = messageQueueService.findAll(query);
        if (result.size() > 0) {
            return result.get(0).getId();
        }
        return null;
    }

    @Scheduled(fixedDelay = 1000)
    public void fetchNewMessage() {

        List<String> clientIds = WebSocketManager.getAllClientId();
        Set<String> agentClientIds = WebSocketClusterServer.agentMap.keySet();

        if (clientIds == null) {
            clientIds = new ArrayList<>();
        }
        if (agentClientIds.size() > 0) {
            clientIds.addAll(agentClientIds);
        }

        if (clientIds.size() == 0) {
            offset = getLastOffset();
            return;
        }

        Criteria criteria;
        if (offset == null) {
            criteria = Criteria.where("createTime").gte(new Date(System.currentTimeMillis() - 1000 * 60));
        } else {
            criteria = Criteria.where("_id").gt(offset);
        }

        criteria.and("receiver").in(clientIds);
        Query query = Query.query(criteria);
        query.with(Sort.by(Sort.Order.asc("_id")));

        List<MessageQueueDto> messages = messageQueueService.findAll(query);
        messages.forEach(messageQueueDto -> {
            List<String> types = Lists.newArrayList(MessageType.PIPE.getType(), MessageType.TEST_RUN.getType());

            if (types.contains(messageQueueDto.getType()) && StringUtils.isNotBlank(messageQueueDto.getReceiver())) {
                Object dataObject = messageQueueDto.getData();
                //todo  这个地方需要优化
                // 这里主要是因为  存到messageQueue里，再查出来的话，id会变成ObjectId ,所以做这样的处理
                if (dataObject instanceof Map) {
                    if (((Map) dataObject).containsKey("id") && ((Map<?, ?>) dataObject).get("id") instanceof Map
                            && ((Map) ((Map<?, ?>) dataObject).get("id")).containsKey("$oid")) {
                        ((Map) dataObject).put("id", ((Map) ((Map<?, ?>) dataObject).get("id")).get("$oid"));
                    }
//                        Map<String, Object> dataMap = JsonUtil.parseJson((String) dataObject, Map.class);

                    messageQueueDto.setData(dataObject);
                }
                try {
                    WebSocketManager.sendMessage(messageQueueDto.getReceiver(), JsonUtil.toJsonUseJackson(messageQueueDto));

                } catch (IOException e) {
                    log.error("Send message to client {} failed", messageQueueDto.getReceiver(), e);
                }
            } else if (messageQueueDto.getType().equals(MessageInfoV1.VERSION)) {
                MessageInfo messageInfo = MessageInfoBuilder.parse(messageQueueDto.getData().toString());
                if (messageInfo != null) {
                    if (MessageInfoV1.RETURN_TYPE.equals(((MessageInfoV1) messageInfo).getType())) {
                        if (WebSocketManager.containsResultCallback(messageInfo.getReqId())) {
                            webSocketServer.handlerResult((MessageInfoV1) messageInfo);
                        }
                    } else if (StringUtils.isNotBlank(messageQueueDto.getReceiver())) {
                        try {
                            WebSocketManager.sendMessage(messageQueueDto.getReceiver(), messageInfo);
                        } catch (IOException e) {
                            log.error("Send message to client {} failed", messageQueueDto.getReceiver(), e);
                        }
                    }
                }
            } else if (MessageType.PIPE_CLUSTER.getType().equals(messageQueueDto.getType())
                    && StringUtils.isNotBlank(messageQueueDto.getReceiver())) {
                try {
                    Object dataObject = messageQueueDto.getData();
                    WebSocketClusterServer.sendMessage(messageQueueDto.getReceiver(), dataObject.toString());
                } catch (IOException e) {
                    log.error("Send cluster message to client {} failed", messageQueueDto.getReceiver(), e);
                }
            }
        });

        if (messages.size() > 0) {
            offset = messages.get(messages.size() - 1).getId();
        }
    }


}
