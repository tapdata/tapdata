/**
 * @title: WebSocketHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.handler;

import com.tapdata.tm.ws.dto.WebSocketContext;

public interface WebSocketHandler {

	void handleMessage(WebSocketContext context) throws Exception;
}
