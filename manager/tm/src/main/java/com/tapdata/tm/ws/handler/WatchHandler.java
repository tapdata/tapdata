/**
 * @title: WatchHandler
 * @description:
 * @author lk
 * @date 2021/9/16
 */
package com.tapdata.tm.ws.handler;

import com.mongodb.client.model.changestream.FullDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.changestream.config.ChangeStreamManager;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.cs.WatchListener;
import com.tapdata.tm.ws.dto.CollectionWatchCache;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.project;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

@WebSocketMessageHandler(type = MessageType.WATCH)
@Slf4j
public class WatchHandler implements WebSocketHandler {

	private final MongoTemplate mongoTemplate;

	public static final Map<String, CollectionWatchCache> watchCacheMap = new ConcurrentHashMap<>();

	public WatchHandler(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
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
		String collectionName = messageInfo.getCollection();
		if (StringUtils.isEmpty(collectionName)){
			try {
				WebSocketManager.sendMessage(context.getSender(), "CollectionName is empty");
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}
		Map<String, Object> filter = messageInfo.getFilter();
		Object in = MapUtils.getValueByPatchPath(filter, "where/fullDocument._id/$in");
		List<String> list = new ArrayList<>();
		if (in instanceof List){
			list.addAll((List)in);
		}
		if (CollectionUtils.isEmpty(list)){
			try {
				WebSocketManager.sendMessage(context.getSender(), "Where is empty");
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}
		List<ObjectId> ids = list.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
		List<DataFlow> dataFlows = mongoTemplate.find(Query.query(Criteria.where("_id").in(ids).and("user_id").is(context.getUserId())), DataFlow.class);
		if (CollectionUtils.isEmpty(dataFlows) || dataFlows.size() != ids.size()){
			List<String> idList = dataFlows.stream().map(dataFlow -> dataFlow.getId().toHexString()).collect(Collectors.toList());
			String idStr = org.apache.commons.lang3.StringUtils.join(idList, ",");
			WebSocketManager.sendMessage(context.getSender(), "Some dataFlow info was not found,Existing ids: " + idStr);
			return;
		}

		Map fields = MapUtils.getAsMap(filter, "fields");
		Set includes = fields.keySet();
		// 当前固定取_id为key
		startChangeStream(collectionName, "_id", list, context.getSender(), includes, context.getSessionId());

	}

	public  void startChangeStream(String collectionName, String key, List<String> list, String receiver, Set<String> includes, String sessionId){
		CollectionWatchCache collectionWatchCache = watchCacheMap.get(collectionName);
		if (collectionWatchCache == null){
			collectionWatchCache = new CollectionWatchCache();
			watchCacheMap.put(collectionName, collectionWatchCache);
		}
		collectionWatchCache.addReceiverInfo(key, list, receiver, sessionId);
		MessageListenerContainer container = collectionWatchCache.getContainer();
		if (container == null || !container.isRunning()){
			Fields fields = null;
			Aggregation aggregation = null;

			HashSet<String> set = new HashSet<>(includes);
			// includes不为空时默认加上fullDocument._id
			set.add("fullDocument._id");
			if (CollectionUtils.isNotEmpty(set)){
				for (String include : set) {
					if (fields == null) {
						fields = Fields.from(Fields.field(include, include));
					}else {
						fields = fields.and(include, include);
					}
				}
			}
			if (fields != null){
				aggregation = newAggregation(project(fields));
			}
			log.info("Enable changestream monitoring {}, include: {}", collectionName, JsonUtil.toJson(set));
			container = ChangeStreamManager.start(collectionName, aggregation,
					FullDocument.UPDATE_LOOKUP, mongoTemplate, new WatchListener());
			collectionWatchCache.setContainer(container);
		}else {
			log.info("Messagelistenercontainer is already running,collectionName: {}", collectionName);
		}
	}

	public static void stopChangeStream(String collectionName){
		if (StringUtils.isNotBlank(collectionName)){
			CollectionWatchCache collectionWatchCache = watchCacheMap.get(collectionName);
			if (collectionWatchCache != null
					&& collectionWatchCache.getContainer() != null
					&& collectionWatchCache.getContainer().isRunning()
					&& System.currentTimeMillis() - collectionWatchCache.getCacheEmptyTime() > 1000 * 60 * 10){
				log.info("Disable changestream monitoring collection: {}", collectionName);
				try {
					collectionWatchCache.getContainer().stop();
					watchCacheMap.remove(collectionName);
				}catch (Exception e){
					log.error("Disable changestream failed,message: {}", e.getMessage());
				}
			}
		}
	}

	public static void removeSession(String id){
		if (MapUtils.isNotEmpty(watchCacheMap) && StringUtils.isNotBlank(id)){
			watchCacheMap.values().forEach(collectionWatchCache -> collectionWatchCache.removeReceiverInfo(id));
		}else {
			log.warn("WatchHandler cache remove seesion skip, id is blank");
		}
	}

	public static void sendWatchMessage(String collectionName, String receiver, Object data){

		try {
			Map<String, Object> map = new HashMap<>();
			map.put("type", "watch");
			map.put("collection", collectionName);
			map.put("data", data);
			WebSocketManager.sendMessage(receiver, JsonUtil.toJson(map));
		} catch (Exception e) {
			log.error("WebSocket send watch message failed,message: {}", e.getMessage());
		}
	}
}
