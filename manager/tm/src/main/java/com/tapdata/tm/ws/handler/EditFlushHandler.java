package com.tapdata.tm.ws.handler;

import com.mongodb.client.model.changestream.FullDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.changestream.config.ChangeStreamManager;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.cs.EditFlushListener;
import com.tapdata.tm.ws.dto.EditFlushCache;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @Author: Zed
 * @Date: 2021/11/25
 * @Description: 前端编辑的刷新控制处理，包括最新的版本推送，模型推演的进度推送
 */
@WebSocketMessageHandler(type = MessageType.EDIT_FLUSH)
@Slf4j
public class EditFlushHandler implements WebSocketHandler {

	private final MongoTemplate mongoTemplate;
	private final TaskService taskService;

	private static MessageListenerContainer container;

	public static AtomicLong cacheEmptyTime = new AtomicLong();

	/** key: task, value */
	public static final Map<String, List<EditFlushCache>> editFlushMap = new ConcurrentHashMap<>();

	public EditFlushHandler(MongoTemplate mongoTemplate, TaskService taskService) {
		this.mongoTemplate = mongoTemplate;
		this.taskService = taskService;
	}

	@Override
	public void handleMessage(WebSocketContext context) {
		MessageInfo messageInfo = context.getMessageInfo();
		if (messageInfo == null){
			try {
				WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("Message data cannot be null"));
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}

        if (StringUtils.isBlank(messageInfo.getTaskId())){
            try {
                WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("task id is blank"));
            } catch (Exception e) {
                log.error("WebSocket send message failed, message: {}", e.getMessage());
            }
            return;
        }

        if (messageInfo.getData() == null || messageInfo.getData().get("opType") == null) {

            try {
                WebSocketManager.sendMessage(context.getSender(), "operation type is blank");
            } catch (Exception e) {
                log.error("WebSocket send message failed, message: {}", e.getMessage());
            }
            return;
        }

		// 当前固定取_id为key
        String taskId = messageInfo.getTaskId();
        if (!editFlushMap.containsKey(taskId)){
            editFlushMap.put(taskId, new ArrayList<>());
        }
        EditFlushCache editFlushCache = new EditFlushCache(context.getSessionId(), context.getSender());
        List<EditFlushCache> editFlushCaches = editFlushMap.get(taskId);

        boolean isExist = editFlushCaches.stream().anyMatch(e -> context.getSessionId().equals(e.getSessionId()));
        if (isExist){
            log.info("edit flush always exists,sender: {},taskId: {}, sessionId: {}", context.getSender(), taskId, context.getSessionId());
            editFlushMap.values().stream()
                    .filter(CollectionUtils::isNotEmpty)
                    .forEach(value -> value.removeIf(cache -> context.getSessionId().equals(cache.getSessionId())));
        }
        editFlushCaches.add(editFlushCache);
        startChangeStream();



        //每次创建连接的时候，需要将当前最新的版本发送给前端
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        if (taskDto != null) {
            sendEditFlushMessage(editFlushCache.getReceiver(), taskId, taskDto);
        }
    }

    public  void startChangeStream(){
        if (container == null || !container.isRunning()){
            log.info("Enable changestream monitoring task connections");
            container = ChangeStreamManager.start("Task",
                    null,
                    FullDocument.UPDATE_LOOKUP, mongoTemplate, new EditFlushListener(), TaskEntity.class);
        }else {
            log.info("Messagelistenercontainer is already running,collectionName: Task");
        }
        cacheEmptyTime.set(0);
    }

    public static void stopChangeStream(){
        if (container != null && container.isRunning() && System.currentTimeMillis() - cacheEmptyTime.get() > 1000 * 60 * 10){
            log.info("Disable changestream monitoring Task");
            try {
                container.stop();
            }catch (Exception e){
                log.error("Disable changestream failed,message: {}", e.getMessage());
            }

        }
    }

    public static void removeSession(String id){
        if (MapUtils.isNotEmpty(editFlushMap) && StringUtils.isNotBlank(id)){
            editFlushMap.values().stream()
                    .filter(CollectionUtils::isNotEmpty)
                    .forEach(value -> value.removeIf(editFlushCache -> id.equals(editFlushCache.getSessionId())));
            editFlushMap.entrySet().removeIf(entry -> CollectionUtils.isEmpty(entry.getValue()));
        }
        if (MapUtils.isEmpty(editFlushMap)){
            cacheEmptyTime.set(System.currentTimeMillis());
        }
    }

	public static void sendEditFlushMessage(String receiver, String taskId, Object data){
        sendEditFlushMessage(receiver, taskId, data, "updateVersion");
    }

    public static void sendEditFlushMessage(String taskId, Object data){
        List<EditFlushCache> editFlushCaches = editFlushMap.get(taskId);
        if (CollectionUtils.isEmpty(editFlushCaches)) {
            return;
        }
        for (EditFlushCache cache : editFlushCaches) {
            sendEditFlushMessage(cache.getReceiver(), taskId, data, "transformRate");
        }
    }

    public static void sendEditFlushMessage(String taskId, Object data, String opType){
        List<EditFlushCache> editFlushCaches = editFlushMap.get(taskId);
        if (CollectionUtils.isEmpty(editFlushCaches)) {
            return;
        }
        for (EditFlushCache cache : editFlushCaches) {
            sendEditFlushMessage(cache.getReceiver(), taskId, data, opType);
        }
    }

	public static void sendEditFlushMessage(String receiver, String taskId, Object data, String opType){

		try {
            Map<String, Object> map = new HashMap<>();
			map.put("type", MessageType.EDIT_FLUSH.getType());
			map.put("opType", opType);
			map.put("taskId", taskId);
			map.put("data", data);
			WebSocketManager.sendMessage(receiver, JsonUtil.toJsonUseJackson(map));
		} catch (Exception e) {
			log.error("WebSocket send watch message failed,message: {}", e.getMessage());
		}
	}
}
