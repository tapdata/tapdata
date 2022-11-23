/**
 * @title: TestConnectionHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.handler;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@WebSocketMessageHandler(type = MessageType.LOADJAR)
@Slf4j
public class LoadJarHandler implements WebSocketHandler {

	private final MessageQueueService messageQueueService;

	private final UserService userService;

	private final WorkerService workerService;

	public LoadJarHandler(MessageQueueService messageQueueService, UserService userService, WorkerService workerService) {
		this.messageQueueService = messageQueueService;
		this.userService = userService;
		this.workerService = workerService;
	}
	@Override
	public void handleMessage(WebSocketContext context) throws Exception{
		MessageInfo messageInfo = context.getMessageInfo();
		messageInfo.getData().put("type", messageInfo.getType());
		messageInfo.setType("pipe");
		String userId = context.getUserId();
		if (StringUtils.isBlank(userId)){
			WebSocketManager.sendMessage(context.getSender(), "UserId is blank");
			return;
		}
		Map<String, Object> data = messageInfo.getData();
		UserDetail userDetail = userService.loadUserById(toObjectId(userId));
		if (userDetail == null){
			WebSocketManager.sendMessage(context.getSender(), "UserDetail is null");
			return;
		}

		SchedulableDto schedulableDto = new SchedulableDto();
		schedulableDto.setUserId(userDetail.getUserId());
		workerService.scheduleTaskToEngine(schedulableDto, userDetail, "testConnection", "testConnection");
		String receiver = schedulableDto.getAgentId();
		if (StringUtils.isBlank(receiver)){
			log.warn("Receiver is blank,context: {}", JsonUtil.toJson(context));
			data.put("status", "error");
			data.put("msg", "Worker not found,receiver is blank");
			sendMessage(context.getSender(), context);
			return;
		}
		sendMessage(receiver, context);
	}

	private void sendMessage(String receiver, WebSocketContext context){
		MessageQueueDto messageQueueDto = new MessageQueueDto();
		messageQueueDto.setSender(context.getSender());
		messageQueueDto.setReceiver(receiver);
		messageQueueDto.setData(context.getMessageInfo().getData());
		messageQueueDto.setType(context.getMessageInfo().getType());
		messageQueueService.sendMessage(messageQueueDto);
	}
}
