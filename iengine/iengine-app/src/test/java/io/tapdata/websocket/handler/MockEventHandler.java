package io.tapdata.websocket.handler;

import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/11 11:41
 */
@EventHandlerAnnotation(type = "test")
public class MockEventHandler extends BaseEventHandler {

    @Override
    public Object handle(Map event) {
        return new HashMap<String, Object>();
    }

    @Override
    public Object handle(Map event, SendMessage sendMessage) {
        Object result = handle(event);
        WebSocketEventResult eventResult = WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.TEST_CONNECTION_RESULT, result);
        try {
            sendMessage.send(eventResult);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return eventResult;
    }
}
