package io.tapdata.websocket;

import java.io.IOException;

/**
 * @author samuel
 * @Description
 * @create 2020-12-22 17:23
 **/
@FunctionalInterface
public interface SendMessage<T extends WebSocketEventResult> {

	public void send(T data) throws IOException;
}
