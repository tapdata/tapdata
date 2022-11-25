package com.tapdata.tm.ws.cs;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.websocket.MessageInfo;
import com.tapdata.tm.commons.websocket.MessageInfoBuilder;
import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.schedule.MessageQueueWatch;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.endpoint.WebSocketServer;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

import java.util.Map;

/**
 * Remove change stream for message queue
 * @see MessageQueueWatch
 */
@Deprecated
@Slf4j
public class MongoMessageListener implements MessageListener<ChangeStreamDocument<Document>, Document> {

    private WebSocketServer webSocketServer;

    public MongoMessageListener(WebSocketServer webSocketServer) {
        this.webSocketServer = webSocketServer;
    }

    @Override
    public void onMessage(Message<ChangeStreamDocument<Document>, Document> message) {
        log.info("Received Message in collection: {},message raw: {}, message body:{}",
                message.getProperties().getCollectionName(), message.getRaw(), message.getBody());
        try {
            if (message.getBody() != null) {
                MessageQueueDto messageQueueDto = JsonUtil.parseJson(message.getBody().toJson(), MessageQueueDto.class);
                if (MessageType.PIPE.getType().equals(messageQueueDto.getType()) && StringUtils.isNotBlank(messageQueueDto.getReceiver())) {
                    Object dataObject = messageQueueDto.getData();
                    //todo  这个地方需要优化
                    // 这里主要是因为  存到messageQueue里，再查出来的话，id会变成ObjectId ,所以做这样的处理
                    if (dataObject instanceof Map) {
                        if (((Map) dataObject).containsKey("id") && ((Map<?, ?>) dataObject).get("id") instanceof Map
                                && ((Map) ((Map<?, ?>) dataObject).get("id")).containsKey("$oid")){
                            ((Map) dataObject).put("id",((Map) ((Map<?, ?>) dataObject).get("id")).get("$oid"));
                        }
//                        Map<String, Object> dataMap = JsonUtil.parseJson((String) dataObject, Map.class);

                        messageQueueDto.setData(dataObject);
                    }
                    WebSocketManager.sendMessage(messageQueueDto.getReceiver(), JsonUtil.toJsonUseJackson(messageQueueDto));
                } else if ( messageQueueDto.getType().equals(MessageInfoV1.VERSION)) {
                    MessageInfo messageInfo = MessageInfoBuilder.parse(messageQueueDto.getData().toString());
                    if (messageInfo != null) {
                        if (MessageInfoV1.RETURN_TYPE.equals(((MessageInfoV1)messageInfo).getType())) {
                            if (WebSocketManager.containsResultCallback(messageInfo.getReqId())) {
                                webSocketServer.handlerResult((MessageInfoV1) messageInfo);
                            }
                        } else if (StringUtils.isNotBlank(messageQueueDto.getReceiver())){
                            WebSocketManager.sendMessage(messageQueueDto.getReceiver(), messageInfo);
                        }
                    }
                } /*else if (messageQueueDto.getData() != null && MessageType.EDIT_FLUSH.getType().equals(messageQueueDto.getData().get("type"))) {
                    Object data = messageQueueDto.getData();
                    EditFlushHandler.sendEditFlushMessage((String) data.get("taskId"), data.get("data"));
                }*/
            }
        } catch (Exception e) {
            log.error("ChangeStream handle message error,message: {}", e.getMessage());
        }
    }
}
