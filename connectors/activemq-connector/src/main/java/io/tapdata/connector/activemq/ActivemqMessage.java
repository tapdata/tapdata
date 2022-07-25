package io.tapdata.connector.activemq;

import io.tapdata.common.message.AbstractMqMessage;

import javax.jms.Message;

public class ActivemqMessage extends AbstractMqMessage {

    private Message message;

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
