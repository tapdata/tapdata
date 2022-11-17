package com.tapdata.tm.ws.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.ping.PingDto;
import com.tapdata.tm.commons.ping.PingType;
import com.tapdata.tm.commons.util.ErrorUtil;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@WebSocketMessageHandler(type = MessageType.PING)
@Slf4j
public class PingHandler implements WebSocketHandler {
	private final WorkerService workerService;
	private final UserService userService;

	public PingHandler(WorkerService workerService, UserService userService) {
		this.workerService = workerService;
		this.userService = userService;
	}

	@Override
	public void handleMessage(WebSocketContext context) throws Exception {
		MessageInfo messageInfo = context.getMessageInfo();
		if (messageInfo != null) {
			Map<String, Object> data = messageInfo.getData();
			if (null == data || !data.containsKey("pingType")) {
				return;
			}
			PingDto pingDto = JsonUtil.map2PojoUseJackson(data, new TypeReference<PingDto>() {
			});
			PingType pingType = pingDto.getPingType();
			if (null == pingType) {
				return;
			}
			switch (pingType) {
				case WEBSOCKET_HEALTH:
					try {
						WebSocketManager.sendMessage(context.getSender(), "{\"type\":\"pong\"}");
					} catch (Exception e) {
						log.error("WebSocket send message failed, message: {}", e.getMessage(), e);
					}
					break;
				case TASK_PING:
					break;
				case WORKER_PING:
					Object pingDtoData = pingDto.getData();
					if (pingDtoData instanceof Map) {
						WorkerDto workerDto = JsonUtil.map2PojoUseJackson((Map<String, Object>) pingDtoData, new TypeReference<WorkerDto>() {
						});
						UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(context.getUserId()));
						try {
							workerService.health(workerDto, userDetail);
							pingDto.ok();
						} catch (Exception e) {
							pingDto.fail(e);
						}
						WebSocketManager.sendMessage(context.getSender(), WebSocketResult.ok(pingDto, "pong"));
					}
					break;
				default:
					break;
			}
		}
	}
}
