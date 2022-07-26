package com.tapdata.tm.ws.handler;

import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/16 下午1:14
 */
@WebSocketMessageHandler(type = MessageType.PING)
@Slf4j
public class PingHandler implements WebSocketHandler {
    @Override
    public void handleMessage(WebSocketContext context) throws Exception {
        MessageInfo messageInfo = context.getMessageInfo();
        if (messageInfo == null){
            try {
                WebSocketManager.sendMessage(context.getSender(), "{\"type\":\"pong\"}");
            } catch (Exception e) {
                log.error("WebSocket send message failed, message: {}", e.getMessage(), e);
            }
            return;
        }
    }
}
