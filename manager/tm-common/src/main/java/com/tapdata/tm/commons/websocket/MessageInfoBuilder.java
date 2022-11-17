package com.tapdata.tm.commons.websocket;

import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;

import java.security.InvalidParameterException;
import java.util.UUID;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/14 下午2:26
 */
public class MessageInfoBuilder {

    private String version;
    private String reqId;
    private String type;
    private String body;
    private String beanName;
    private String methodName;

    public MessageInfoBuilder version(String version) {
        this.version = version;
        return this;
    }

    public MessageInfoBuilder reqId(String reqId) {
        this.reqId = reqId;
        return this;
    }

    public MessageInfoBuilder type(String type) {
        this.type = type;
        return this;
    }

    public MessageInfoBuilder body(String body) {
        this.body = body;
        return this;
    }

    public MessageInfoBuilder call(String beanName, String methodName) {
        this.beanName = beanName;
        this.methodName = methodName;
        return this;
    }

    public static MessageInfoBuilder newMessage() {
        return new MessageInfoBuilder()
                .version(MessageInfoV1.VERSION)
                .reqId(generatorReqId());
    }
    public static MessageInfoBuilder returnMessage(MessageInfoV1 messageInfo) {
        return new MessageInfoBuilder()
                .version(MessageInfoV1.VERSION)
                .reqId(messageInfo.getReqId())
                .type(MessageInfoV1.RETURN_TYPE);
    }

    public static MessageInfo parse(String message) {
        if (message.startsWith("v1")) {
            return MessageInfoV1.parse(message);
        }
        return null;
    }

    public MessageInfo build() {
        if (reqId == null)
            reqId = generatorReqId();

        if (version == null)
            version(MessageInfoV1.VERSION);

        if (type == null) {
            if (beanName != null && methodName != null) {
                type(beanName + '/' + methodName);
            } else {
                throw new InvalidParameterException("Message type can't not be empty.");
            }
        }

        if (body == null) {
            body = "";
        }

        MessageInfoV1 messageInfo = new MessageInfoV1();
        messageInfo.setVersion(version);
        messageInfo.setReqId(reqId);
        messageInfo.setType(type);
        messageInfo.setBody(body);
        return messageInfo;
    }

    public static String generatorReqId() {
        return UUID.randomUUID().toString();
    }
}
