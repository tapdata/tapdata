package com.tapdata.tm.ws.endpoint;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.websocket.ReturnCallback;
import com.tapdata.tm.commons.websocket.v1.MessageInfoV1;
import com.tapdata.tm.commons.websocket.v1.ResultWrap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class WebSocketServerTest {
    private WebSocketServer webSocketServer;
    @BeforeEach
    void beforeEach(){
        webSocketServer = mock(WebSocketServer.class);
    }
    @Nested
    class HandlerResultTest{
        private MessageInfoV1 messageInfo;
        @Test
        void testHandlerResultNormal(){
            try (MockedStatic<WebSocketManager> mb = Mockito
                    .mockStatic(WebSocketManager.class)) {
                messageInfo = mock(MessageInfoV1.class);
                when(messageInfo.getType()).thenReturn("return");
                when(messageInfo.getReqId()).thenReturn("111");
                when(messageInfo.getBody()).thenReturn("test");
                ReturnCallback<String> callback = new ReturnCallback<String>() {
                    @Override
                    public void success(String s) {
                    }
                    @Override
                    public void error(String code, String message) {
                    }
                };
                mb.when(()->WebSocketManager.getAndRemoveResultCallback("111")).thenReturn(callback);
                try (MockedStatic<JsonUtil> jsonUtilMockedStatic = Mockito
                        .mockStatic(JsonUtil.class)) {
                    jsonUtilMockedStatic.when(()->JsonUtil.parseJsonUseJackson("test", ResultWrap.class)).thenReturn(mock(ResultWrap.class));
                    doCallRealMethod().when(webSocketServer).handlerResult(messageInfo);
                    webSocketServer.handlerResult(messageInfo);
                    jsonUtilMockedStatic.verify(()->JsonUtil.parseJsonUseJackson(anyString(), any(Class.class)),new Times(1));
                }
            }
        }
    }
}
