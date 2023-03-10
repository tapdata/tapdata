package com.tapdata.tm.ws.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.ping.PingDto;
import com.tapdata.tm.commons.ping.PingType;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.WorkerSingletonLock;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/16 下午1:14
 */
@WebSocketMessageHandler(type = MessageType.PING)
@Slf4j
public class PingHandler implements WebSocketHandler {
	public static final String PING_RESULT_TYPE = "pong";
	private final WorkerService workerService;
	private final UserService userService;
	private final TaskService taskService;

	public PingHandler(WorkerService workerService, UserService userService, TaskService taskService) {
		this.workerService = workerService;
		this.userService = userService;
		this.taskService = taskService;
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
					pingDto.ok();
					sendResponse(context, WebSocketResult.ok(pingDto, PING_RESULT_TYPE));
					break;
				case TASK_PING:
					taskPingTime(context, pingDto);
					sendResponse(context, WebSocketResult.ok(pingDto, PING_RESULT_TYPE));
					break;
				case WORKER_PING:
					workerHealth(context, pingDto);
					sendResponse(context, WebSocketResult.ok(pingDto, PING_RESULT_TYPE));
					break;
				default:
					log.warn("Unrecognized ping type: {}, websocket context: {}", pingType.name(), context);
					break;
			}
		}
	}

	private void taskPingTime(WebSocketContext context, PingDto pingDto) {
		try {
			Object pingDtoData = pingDto.getData();
			if (pingDtoData instanceof Map) {
				String whereJson = ((Map<?, ?>) pingDtoData).get("where").toString();
				String reqBody = ((Map<?, ?>) pingDtoData).get("update").toString();
				Where where = BaseController.parseWhere(whereJson);
				Document update = Document.parse(reqBody);
				if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
					Document _body = new Document();
					_body.put("$set", update);
					update = _body;
				} else if (update.containsKey("$set")) {
					Document ping = (Document) update.get("$set");
					if (ping.containsKey("pingTime")) {
						ping.put("pingTime", System.currentTimeMillis());
					}
				}
				UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(context.getUserId()));
				long count = taskService.updateByWhere(where, update, userDetail);
				if (count > 0) {
					pingDto.ok();
				} else {
					pingDto.fail(String.format("Update task ping time failed, effect count: %s, query: %s, update: %s", count, whereJson, reqBody));
				}
			} else {
				pingDto.fail(String.format("Expect ping dto data type: %s, actual: %s", Map.class.getName(), pingDtoData.getClass().getName()));
			}
		} catch (Exception e) {
			pingDto.fail(e);
		}
	}

	private void workerHealth(WebSocketContext context, PingDto pingDto) {
		Object pingDtoData = pingDto.getData();
		if (pingDtoData instanceof Map) {
			try {
				WorkerDto workerDto = JsonUtil.map2PojoUseJackson((Map<String, Object>) pingDtoData, new TypeReference<WorkerDto>() {
				});
				UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(context.getUserId()));
				if (null == workerService.health(workerDto, userDetail)) {
					pingDto.fail(WorkerSingletonLock.STOP_AGENT);
				} else {
					pingDto.ok();
				}
			} catch (Exception e) {
				pingDto.fail(e);
			}
		} else {
			pingDto.fail(String.format("Expect ping dto data type: %s, actual: %s", Map.class.getName(), pingDtoData.getClass().getName()));
		}
	}

	private static void sendResponse(WebSocketContext context, WebSocketResult webSocketResult) {
		try {
			WebSocketManager.sendMessage(context.getSender(), webSocketResult);
		} catch (Exception e) {
			log.error("WebSocket send message failed, error message: {}, response body: {}", e.getMessage(), webSocketResult, e);
		}
	}
}
