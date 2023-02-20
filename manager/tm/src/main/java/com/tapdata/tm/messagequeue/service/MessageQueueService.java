/**
 * @title: MessageQueueService
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.messagequeue.service;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.websocket.MessageInfo;
import com.tapdata.tm.commons.websocket.ReturnCallback;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.entity.MessageQueue;
import com.tapdata.tm.messagequeue.repository.MessageQueueRepository;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import java.io.IOException;
import java.util.Date;

import com.tapdata.tm.ws.endpoint.WebSocketServer;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

@Service
@Slf4j
public class MessageQueueService extends BaseService<MessageQueueDto, MessageQueue, ObjectId, MessageQueueRepository> {

	public MessageQueueService(MessageQueueRepository repository) {
		super(repository, MessageQueueDto.class, MessageQueue.class);
	}

	@Override
	protected void beforeSave(MessageQueueDto dto, UserDetail userDetail) {

	}

	public void save(MessageQueueDto dto){
		repository.save(convertToEntity(MessageQueue.class, dto));
	}

	public <T> void sendMessage(String receive, MessageInfo messageInfo){
		sendMessage(receive, messageInfo, null);
	}

	/**
	 * Not yet mature, temporarily not open for use
	 * @param receive
	 * @param messageInfo
	 * @param returnCallback
	 * @param <T>
	 */
	private <T> void sendMessage(String receive, MessageInfo messageInfo, ReturnCallback<T> returnCallback){
		if (messageInfo == null){
			throw new BizException("MessageQueue is null");
		}
		if (WebSocketManager.containsSession(receive)) {
			try {
				WebSocketManager.sendMessage(receive, messageInfo, returnCallback);
			} catch (Exception e) {
				log.error("Send message to WebSocket client failed, receive {}, message: {}", receive, messageInfo, e);
				if (returnCallback != null) {
					returnCallback.error("PushMessageFailed", e.getMessage());
				}
			}
		} else if (StringUtils.isNotBlank(receive)) {
			log.info("WebSocket client not found, save message data to queue, receive: {}, message: {}", receive, messageInfo);
			MessageQueueDto messageQueueDto = new MessageQueueDto();
			messageQueueDto.setReceiver(receive);
			messageQueueDto.setType("v1");
			messageQueueDto.setData(messageInfo.toTextMessage());
			messageQueueDto.setCreateAt(new Date());
			save(messageQueueDto);
			WebSocketManager.cacheReturnCallback(receive, messageInfo, returnCallback);
		}
	}
	public void sendMessage(MessageQueueDto messageDto){
		if (messageDto == null){
			throw new BizException("MessageQueue is null");
		}
		if(WebSocketManager.containsSession(messageDto.getReceiver())){
			try {

				WebSocketManager.sendMessage(messageDto.getReceiver(), JsonUtil.toJsonUseJackson(messageDto));
			} catch (Exception e) {
				log.error("WebSocket handle message failed,message: {}", e.getMessage(), e);
				try {
					WebSocketManager.sendMessage(messageDto.getSender(), WebSocketResult.fail(String.format("WebSocket handle message failed,message: %s", e.getMessage())));
				} catch (IOException ignored) {
					log.error("WebSocket send message failed,message: {}", e.getMessage(), e);
				}
				throw new BizException(e);
			}
		}else if (StringUtils.isNotBlank(messageDto.getReceiver())){
			log.info("SessionId not found, save message data to db, messageQueue: {}", JsonUtil.toJson(messageDto));
			save(messageDto);
		} /*else if (messageDto.getData() != null && MessageType.EDIT_FLUSH.getType().equals(messageDto.getData().get("type"))) {
			log.info("SessionId, receiver not found, type is edit flush,  save message data to db, messageQueue: {}", JsonUtil.toJson(messageDto));
			save(messageDto);
		}*/
	}

	public void sendPipeMessage(Map<String, Object> map, String sender, String receiver) {
		MessageQueueDto queueDto = new MessageQueueDto();
		queueDto.setSender(sender);
		queueDto.setReceiver(receiver);
		queueDto.setData(map);
		queueDto.setType("pipe");
		sendMessage(queueDto);
	}
}
