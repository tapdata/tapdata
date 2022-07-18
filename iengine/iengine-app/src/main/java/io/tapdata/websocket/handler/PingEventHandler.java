package io.tapdata.websocket.handler;

import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;

import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/20 11:12 下午
 * @description
 */
@EventHandlerAnnotation(type = "ping")
public class PingEventHandler extends BaseEventHandler implements WebSocketEventHandler {

	@Override
	public Object handle(Map event) {
		return null;
	}
}
