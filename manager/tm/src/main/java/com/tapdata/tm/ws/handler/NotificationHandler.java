/**
 * @title: NotificationHandler
 * @description:
 * @author lk
 * @date 2021/9/18
 */
package com.tapdata.tm.ws.handler;

import com.mongodb.client.model.changestream.FullDocument;
import com.tapdata.manager.common.utils.DateUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.changestream.config.ChangeStreamManager;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.cs.NotificationListener;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@WebSocketMessageHandler(type = MessageType.NOTIFICATION)
@Slf4j
public class NotificationHandler implements WebSocketHandler {

	private final MongoTemplate mongoTemplate;

	private static MessageListenerContainer container;

	public static long cacheEmptyTime = 0;

	public static final Map<String, Map<String, String>> notificationMap = new ConcurrentHashMap<>();

	public NotificationHandler(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	@Override
	public void handleMessage(WebSocketContext context) {
		log.info("NotificationHandler context:{}",JsonUtil.toJson(context));
		String userId = context.getUserId();
		if (StringUtils.isBlank(userId)){
			try {
				WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("UserId is blank"));
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}
		if (!notificationMap.containsKey(userId)){
			notificationMap.put(userId, new HashMap<>());
		}
		notificationMap.get(userId).put(context.getSessionId(), context.getSender());
		startChangeStream();
	}

	private  void startChangeStream(){
		if (container == null || !container.isRunning()){
			log.info("Enable changestream monitoring Message");
			container = ChangeStreamManager.start("Message",
					null,
					FullDocument.UPDATE_LOOKUP, mongoTemplate, new NotificationListener());
		}else {
			log.info("Notificationlistenercontainer is already running,collectionName: Message");
		}
		cacheEmptyTime = 0;
	}

	public static void stopChangeStream(){
		if (container != null && container.isRunning() && System.currentTimeMillis() - cacheEmptyTime > 1000 * 60 * 10){
			log.info("Disable changestream monitoring Message");
			try {
				container.stop();
			}catch (Exception e){
				log.error("Disable changestream failed,message: {}", e.getMessage());
			}

		}
	}

	public static void removeSession(String id){
		if (MapUtils.isNotEmpty(notificationMap) && StringUtils.isNotBlank(id)){
			for (Iterator<Map.Entry<String, Map<String, String>>> iterator = notificationMap.entrySet().iterator(); iterator.hasNext(); ) {
				Map.Entry<String, Map<String, String>> entry = iterator.next();
				Map<String, String> value = entry.getValue();
				value.remove(id);
				if (MapUtils.isEmpty(value)) {
					iterator.remove();
				}

			}
		}
		if (MapUtils.isEmpty(notificationMap)){
			cacheEmptyTime = System.currentTimeMillis();
		}
	}

	/**
	 * 发出去的消息格式应该是这样
	 *
	 * {
	 *     "type": "notification",
	 *     "data": [
	 *         {
	 *             "_id": "619db45d9c63770011e8d97d",
	 *             "msg": "connected",
	 *             "sourceId": "619db357c32da6026a68e969",
	 *             "system": "agent",
	 *             "level": "INFO",
	 *             "createTime": "2021-11-24T03:41:17.107Z",
	 *             "serverName": "dfs-agent-17d50048f7c"
	 *         }
	 *     ]
	 * }
	 * @param receiver
	 */
	public static void sendNotification(String receiver, Document document){
		try {
			Map<String, Object> map = new HashMap<>();
			map.put("type", MessageType.NOTIFICATION.getType());


			Map dataMap=new HashMap();
			dataMap.put("id",document.getObjectId("_id").toString());
			dataMap.put("msg",document.getString("msg"));
			dataMap.put("sourceId",document.getString("sourceId"));
			dataMap.put("system",document.getString("system"));
			dataMap.put("level",document.getString("level").toUpperCase());
			dataMap.put("createTime",DateUtil.getISO8601Time(document.getDate("createTime")));
			dataMap.put("title",document.getString("title"));

			String serverName=document.getString("agentName");
			if (org.apache.commons.lang3.StringUtils.isEmpty(serverName)){
				serverName=document.getString("serverName");
			}
			dataMap.put("serverName",serverName);
			map.put("data", dataMap);
			WebSocketManager.sendMessage(receiver, JsonUtil.toJsonUseJackson(map));
		} catch (Exception e) {
			log.error("WebSocket send notification failed,message: {}", e.getMessage());
		}
	}
}
