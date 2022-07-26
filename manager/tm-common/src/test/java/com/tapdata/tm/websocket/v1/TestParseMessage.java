package com.tapdata.tm.websocket.v1;

import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/13 下午6:52
 */
public class TestParseMessage {

    @Test
    public void testParse() {

        String message = "v1:bf18022c-a5b4-42b3-8925-59190b3eeaeb:dataFlowService/start:{\"name\": \"value\"}";
        MessageInfoV1 messageInfo = MessageInfoV1.parse(message);
        Assert.assertNotNull(messageInfo);
        Assert.assertEquals("v1", messageInfo.getVersion());
        Assert.assertEquals("bf18022c-a5b4-42b3-8925-59190b3eeaeb", messageInfo.getReqId());
        Assert.assertEquals("dataFlowService/start", messageInfo.getType());
        Assert.assertEquals("{\"name\": \"value\"}", messageInfo.getBody());
        Assert.assertEquals("dataFlowService", messageInfo.getBeanName());
        Assert.assertEquals("start", messageInfo.getMethodName());

        message = "v1::dataFlowService/start:{\"name\": \"value\"}";
        messageInfo = MessageInfoV1.parse(message);
        Assert.assertNotNull(messageInfo);
        Assert.assertEquals("v1", messageInfo.getVersion());
        Assert.assertEquals("", messageInfo.getReqId());
        Assert.assertEquals("dataFlowService/start", messageInfo.getType());
        Assert.assertEquals("{\"name\": \"value\"}", messageInfo.getBody());
        Assert.assertEquals("dataFlowService", messageInfo.getBeanName());
        Assert.assertEquals("start", messageInfo.getMethodName());


        message = "v1:1:2:3";
        messageInfo = MessageInfoV1.parse(message);
        Assert.assertNotNull(messageInfo);
        Assert.assertEquals("v1", messageInfo.getVersion());
        Assert.assertEquals("1", messageInfo.getReqId());
        Assert.assertEquals("2", messageInfo.getType());
        Assert.assertEquals("3", messageInfo.getBody());
        Assert.assertNull(messageInfo.getBeanName());
        Assert.assertNull(messageInfo.getMethodName());

        message = "v1:1:2";
        messageInfo = MessageInfoV1.parse(message);
        Assert.assertNull(messageInfo);

        message = "v1:1:2:";
        messageInfo = MessageInfoV1.parse(message);
        Assert.assertNotNull(messageInfo);
        Assert.assertEquals("v1", messageInfo.getVersion());
        Assert.assertEquals("1", messageInfo.getReqId());
        Assert.assertEquals("2", messageInfo.getType());
        Assert.assertEquals("", messageInfo.getBody());

        message = "v1:::";
        messageInfo = MessageInfoV1.parse(message);
        Assert.assertNotNull(messageInfo);
        Assert.assertEquals("v1", messageInfo.getVersion());
        Assert.assertEquals("", messageInfo.getReqId());
        Assert.assertEquals("", messageInfo.getType());
        Assert.assertEquals("", messageInfo.getBody());

        message = "v1:::";
        messageInfo = MessageInfoV1.parse(message);
        Assert.assertNotNull(messageInfo);
        Assert.assertEquals("v1", messageInfo.getVersion());
        Assert.assertEquals("", messageInfo.getReqId());
        Assert.assertEquals("", messageInfo.getType());
        Assert.assertEquals("", messageInfo.getBody());

        message = "v1::::::";
        messageInfo = MessageInfoV1.parse(message);
        Assert.assertNotNull(messageInfo);
        Assert.assertEquals("v1", messageInfo.getVersion());
        Assert.assertEquals("", messageInfo.getReqId());
        Assert.assertEquals("", messageInfo.getType());
        Assert.assertEquals(":::", messageInfo.getBody());
    }
}
