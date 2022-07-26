package com.tapdata.tm.websocket;

import com.tapdata.tm.commons.websocket.MessageInfo;
import com.tapdata.tm.commons.websocket.MessageInfoBuilder;
import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.*;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/14 下午4:44
 */
public class TestWebSocket {

    private ListenableFuture<WebSocketSession> connect(String url, String userId, WebSocketHandler webSocketHandler) {

        WebSocketClient client = new StandardWebSocketClient();
        WebSocketHttpHeaders webSocketHttpHeaders = new WebSocketHttpHeaders();
        webSocketHttpHeaders.add(WebSocketHttpHeaders.USER_AGENT, "FlowEngine/v2.1.4");
        webSocketHttpHeaders.add("user_id", userId);
        return client.doHandshake(webSocketHandler, webSocketHttpHeaders,
                URI.create(UriUtils.decode(url, StandardCharsets.UTF_8)));
    }

    @Test
    public void test() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        String url = "ws://localhost:3000/ws/agent?agentId=621ee2403420bd2637459f24-1ft4a7jc1";
        String userId = "60b064e9a65d8e852c8523bc";
        List<MessageInfo> returnMessage = new ArrayList<>();
        ListenableFuture<WebSocketSession> listenableFuture = connect(url, userId, new TextWebSocketHandler(){
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                returnMessage.add(MessageInfoBuilder.parse(message.getPayload()));
            }
        });

        WebSocketSession session = listenableFuture.get(10, TimeUnit.SECONDS);
        MessageInfo message = MessageInfoBuilder.newMessage().type("ping").build();
        TextMessage textMessage = new TextMessage(message.toTextMessage());
        session.sendMessage(textMessage);

        /*for (int i = 0; i < 100; i++) {
            session.sendMessage(new TextMessage(MessageInfoBuild.newMessage().type("ping").build().toTextMessage()));
        }*/

        Thread.sleep(5000);

        Assert.assertEquals(1, returnMessage.size());
        Assert.assertEquals(returnMessage.get(0).getReqId(), message.getReqId());
        Assert.assertEquals(returnMessage.get(0).getVersion(), "v1");
        Assert.assertEquals(returnMessage.get(0).getClass(), MessageInfoV1.class);
        Assert.assertEquals(((MessageInfoV1)returnMessage.get(0)).getType(), "return");

    }
}
