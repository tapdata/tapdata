package com.tapdata.tm.websocket.v1;

import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/13 下午6:52
 */
public class TestParseMessage {

    @Test
    public void testParse() {

        String message = "v1:bf18022c-a5b4-42b3-8925-59190b3eeaeb:dataFlowService/start:{\"name\": \"value\"}";
        MessageInfoV1 messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNotNull(messageInfo);
        Assertions.assertEquals("v1", messageInfo.getVersion());
        Assertions.assertEquals("bf18022c-a5b4-42b3-8925-59190b3eeaeb", messageInfo.getReqId());
        Assertions.assertEquals("dataFlowService/start", messageInfo.getType());
        Assertions.assertEquals("{\"name\": \"value\"}", messageInfo.getBody());
        Assertions.assertEquals("dataFlowService", messageInfo.getBeanName());
        Assertions.assertEquals("start", messageInfo.getMethodName());

        message = "v1::dataFlowService/start:{\"name\": \"value\"}";
        messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNotNull(messageInfo);
        Assertions.assertEquals("v1", messageInfo.getVersion());
        Assertions.assertEquals("", messageInfo.getReqId());
        Assertions.assertEquals("dataFlowService/start", messageInfo.getType());
        Assertions.assertEquals("{\"name\": \"value\"}", messageInfo.getBody());
        Assertions.assertEquals("dataFlowService", messageInfo.getBeanName());
        Assertions.assertEquals("start", messageInfo.getMethodName());


        message = "v1:1:2:3";
        messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNotNull(messageInfo);
        Assertions.assertEquals("v1", messageInfo.getVersion());
        Assertions.assertEquals("1", messageInfo.getReqId());
        Assertions.assertEquals("2", messageInfo.getType());
        Assertions.assertEquals("3", messageInfo.getBody());
        Assertions.assertNull(messageInfo.getBeanName());
        Assertions.assertNull(messageInfo.getMethodName());

        message = "v1:1:2";
        messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNull(messageInfo);

        message = "v1:1:2:";
        messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNotNull(messageInfo);
        Assertions.assertEquals("v1", messageInfo.getVersion());
        Assertions.assertEquals("1", messageInfo.getReqId());
        Assertions.assertEquals("2", messageInfo.getType());
        Assertions.assertEquals("", messageInfo.getBody());

        message = "v1:::";
        messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNotNull(messageInfo);
        Assertions.assertEquals("v1", messageInfo.getVersion());
        Assertions.assertEquals("", messageInfo.getReqId());
        Assertions.assertEquals("", messageInfo.getType());
        Assertions.assertEquals("", messageInfo.getBody());

        message = "v1:::";
        messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNotNull(messageInfo);
        Assertions.assertEquals("v1", messageInfo.getVersion());
        Assertions.assertEquals("", messageInfo.getReqId());
        Assertions.assertEquals("", messageInfo.getType());
        Assertions.assertEquals("", messageInfo.getBody());

        message = "v1::::::";
        messageInfo = MessageInfoV1.parse(message);
        Assertions.assertNotNull(messageInfo);
        Assertions.assertEquals("v1", messageInfo.getVersion());
        Assertions.assertEquals("", messageInfo.getReqId());
        Assertions.assertEquals("", messageInfo.getType());
        Assertions.assertEquals(":::", messageInfo.getBody());
    }
}
