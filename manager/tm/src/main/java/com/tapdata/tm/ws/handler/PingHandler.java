package com.tapdata.tm.ws.handler;

import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;

@WebSocketMessageHandler(type = MessageType.PING)
public class PingHandler implements WebSocketHandler {
    @Override
    public void handleMessage(WebSocketContext context) throws Exception {
        //todo
    }
}
