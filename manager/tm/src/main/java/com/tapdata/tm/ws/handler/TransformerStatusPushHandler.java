package com.tapdata.tm.ws.handler;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.TransformCache;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;


@WebSocketMessageHandler(type = MessageType.TRANSFORMER_STATUS_PUSH)
@Slf4j
public class TransformerStatusPushHandler implements WebSocketHandler {

	private final MongoTemplate mongoTemplate;
	private final TaskService taskService;

	public static final Map<String, List<TransformCache>> transformMap = new ConcurrentHashMap<>();

	public static AtomicLong cacheEmptyTime = new AtomicLong();

	public TransformerStatusPushHandler(MongoTemplate mongoTemplate, TaskService taskService) {
		this.mongoTemplate = mongoTemplate;
		this.taskService = taskService;
	}

	@Override
	public void handleMessage(WebSocketContext context) throws Exception {
		MessageInfo messageInfo = context.getMessageInfo();
		if (messageInfo == null){
			try {
				WebSocketManager.sendMessage(context.getSender(), "Message data cannot be null");
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}

		Map<String, Object> data = messageInfo.getData();
		if (data == null) {
			try {
				WebSocketManager.sendMessage(context.getSender(), "data is illegal");
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}
		Object id = data.get("dataFlowId");

		if (id == null) {
			try {
				WebSocketManager.sendMessage(context.getSender(), "DataFlowId is illegal");
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}

		String dataFlowId = (String)id;

		if (StringUtils.isBlank(dataFlowId)){
			try {
				WebSocketManager.sendMessage(context.getSender(), "DataFlowId is illegal");
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}else {
			TaskEntity taskEntity = mongoTemplate.findOne(Query.query(Criteria.where("_id").is(toObjectId(dataFlowId)).and("user_id").is(context.getUserId())), TaskEntity.class);
			if (taskEntity == null){
				WebSocketManager.sendMessage(context.getSender(), "task info was not found");
				return;
			}
		}
		if (!transformMap.containsKey(dataFlowId)){
			transformMap.putIfAbsent(dataFlowId, new ArrayList<>());
		}
		String stageId = (String) data.get("stageId");
		TransformCache transformCache = new TransformCache(context.getSessionId(), context.getSender());
		if (StringUtils.isNotBlank(stageId)) {
			transformCache.setStageId(stageId);
		}
		List<TransformCache> transformCaches = transformMap.get(dataFlowId);
		boolean isExist = transformCaches.stream().anyMatch(t -> context.getSessionId().equals(t.getSessionId()));
		if (isExist){
			log.info("transformCache always exists,sender: {},dataFlowId: {}, sessionId: {}", context.getSender(), dataFlowId, context.getSessionId());
			transformMap.values().stream()
					.filter(CollectionUtils::isNotEmpty)
					.forEach(value -> value.removeIf(cache -> context.getSessionId().equals(cache.getSessionId())));
		}
		transformCaches.add(transformCache);
	}



	public static void sendTransformMessage(String dataFlowId,  Object data){
		List<TransformCache> transformCaches = transformMap.get(dataFlowId);
		if (CollectionUtils.isEmpty(transformCaches)) {
			return;
		}
		for (TransformCache transformCach : transformCaches) {
			sendTransformMessage(dataFlowId, transformCach.getSender(), data);
		}
	}
	public static void sendTransformMessage(String dataFlowId, String receiver, Object data){
		try {
			Map<String, Object> map = new HashMap<>();
			map.put("type", MessageType.TRANSFORMER_STATUS_PUSH.getType());
			map.put("dataFlowId", dataFlowId);
			map.put("data", data);
			WebSocketManager.sendMessage(receiver, JsonUtil.toJsonUseJackson(map));
		} catch (Exception e) {
			log.error("WebSocket send log message failed,message: {}", e.getMessage());
		}
	}

	public static void removeSession(String id){
		if (MapUtils.isNotEmpty(transformMap) && StringUtils.isNotBlank(id)){
			transformMap.values().stream()
					.filter(CollectionUtils::isNotEmpty)
					.forEach(value -> value.removeIf(logsCache -> id.equals(logsCache.getSessionId())));
			transformMap.entrySet().removeIf(entry -> CollectionUtils.isEmpty(entry.getValue()));
		}
		if (MapUtils.isEmpty(transformMap)){
			cacheEmptyTime.set(System.currentTimeMillis());
		}
	}


}
