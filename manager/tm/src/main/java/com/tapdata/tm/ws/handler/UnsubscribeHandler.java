/**
 * @title: UnsubscribeHandler
 * @description:
 * @author lk
 * @date 2021/9/29
 */
package com.tapdata.tm.ws.handler;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;

@WebSocketMessageHandler(type = MessageType.UNSUBSCRIBE)
@Slf4j
public class UnsubscribeHandler implements WebSocketHandler {


	@Override
	public void handleMessage(WebSocketContext context) {
		String messageType = context.getMessageInfo().getMessageType();
		if (StringUtils.isBlank(messageType)){
			try {
				WebSocketManager.sendMessage(context.getSender(), "MessageType is blank");
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}
		Arrays.stream(messageType.split(",")).forEach(type -> {
			if (MessageType.LOGS.getType().equals(type)) {
				LogsHandler.removeSession(context.getSessionId());
				log.info("LogsHandler unsubscribed successfully");
			} else if (MessageType.WATCH.getType().equals(type)) {
				WatchHandler.removeSession(context.getSessionId());
				log.info("WatchHandler unsubscribed successfully");
			} else if (MessageType.NOTIFICATION.getType().equals(type)) {
				NotificationHandler.removeSession(context.getSessionId());
				log.info("NotificationHandler unsubscribed successfully");
			} else if (MessageType.DATA_FLOW_INSIGHT.getType().equals(type)) {
				//DataFlowInsightHandler.removeSession(context.getSessionId());
				log.info("DataFlowInsightHandler unsubscribed successfully");
			} else if (MessageType.EDIT_FLUSH.getType().equals(type)) {
				EditFlushHandler.removeSession(context.getSessionId());
				log.info("DataFlowInsightHandler unsubscribed successfully");
			} else if (MessageType.TRANSFORMER_STATUS_PUSH.getType().equals(type)) {
				TransformerStatusPushHandler.removeSession(context.getSessionId());
				log.info("TransformerHandler unsubscribed successfully");
			}

		});
		try {
			WebSocketManager.sendMessage(context.getSender(), String.format("MessageType [%s] unsubscribed successfully", messageType));
			log.info("MessageType {} unsubscribed successfully,sessionId: {}", messageType, context.getSessionId());
		} catch (Exception e) {
			log.error("WebSocket send message failed, message: {}", e.getMessage());
		}
	}
}
