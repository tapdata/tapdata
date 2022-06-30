package com.tapdata.tm.messagequeue.repository;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.messagequeue.entity.MessageQueue;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

class MessageQueueRepositoryTest extends BaseJunit {

    @Autowired
    MessageQueueRepository messageQueueRepository;

    @Test
    void save() {
        MessageQueue messageQueue=new MessageQueue();
        messageQueue.setType("asdas");
        messageQueueRepository.save(messageQueue);
    }
}