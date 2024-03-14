package com.tapdata.tm.messagequeue.service;

import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.websocket.MessageInfo;
import com.tapdata.tm.messagequeue.MessageQueue;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.repository.MessageQueueRepository;
import org.bson.types.ObjectId;

import java.util.Map;

public interface MessageQueueService extends IBaseService<MessageQueueDto, MessageQueue, ObjectId, MessageQueueRepository> {
    void save(MessageQueueDto dto);

    <T> void sendMessage(String receive, MessageInfo messageInfo);

    void sendMessage(MessageQueueDto messageDto);

    void sendPipeMessage(Map<String, Object> map, String sender, String receiver);
}
