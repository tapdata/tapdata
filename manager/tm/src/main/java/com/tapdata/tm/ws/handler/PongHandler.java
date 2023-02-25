package com.tapdata.tm.ws.handler;

import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/16 下午1:14
 */
@WebSocketMessageHandler(type = MessageType.PONG)
@Slf4j
public class PongHandler implements WebSocketHandler {
    @Override
    public void handleMessage(WebSocketContext context) throws Exception {}
}
