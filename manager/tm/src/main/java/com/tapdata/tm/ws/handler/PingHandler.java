package com.tapdata.tm.ws.handler;

import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;

import java.util.Map;

@WebSocketMessageHandler(type = MessageType.PING)
public class PingHandler implements WebSocketHandler {
    @Override
    public void handleMessage(WebSocketContext context) throws Exception {
        MessageInfo messageInfo = context.getMessageInfo();

        Map<String, Object> data = messageInfo.getData();
    }
}
