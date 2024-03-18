package com.tapdata.tm.messagequeue.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.websocket.MessageInfo;
import com.tapdata.tm.messagequeue.MessageQueue;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.repository.MessageQueueRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.Map;

public abstract class MessageQueueService extends BaseService<MessageQueueDto, MessageQueue, ObjectId, MessageQueueRepository> {
    public MessageQueueService(@NonNull MessageQueueRepository repository) {
        super(repository, MessageQueueDto.class, MessageQueue.class);
    }
    public abstract void save(MessageQueueDto dto);

    public abstract <T> void sendMessage(String receive, MessageInfo messageInfo);

    public abstract void sendMessage(MessageQueueDto messageDto);

    public abstract void sendPipeMessage(Map<String, Object> map, String sender, String receiver);
}
