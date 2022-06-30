/**
 * @title: PipeHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.handler;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

@WebSocketMessageHandler(type = MessageType.PIPE)
@Slf4j
public class PipeHandler implements WebSocketHandler {

	private final MessageQueueService queueService;

	public PipeHandler(MessageQueueService queueService) {
		this.queueService = queueService;
	}

	@Override
	public void handleMessage(WebSocketContext context) {
		MessageInfo messageInfo = context.getMessageInfo();
		if (StringUtils.isNotBlank(messageInfo.getReceiver())){
			if (messageInfo.getReceiver().equals(messageInfo.getSender())){
				log.warn("The message ignore,the sender is the same as the receiver");
			}else {
				MessageQueueDto messageDto = new MessageQueueDto();
				BeanUtils.copyProperties(messageInfo, messageDto);
				queueService.sendMessage(messageDto);
			}
		}else {
			log.warn("WebSocket send message failed, receiver is blank, context: {}", JsonUtil.toJson(context));
		}
	}
}
