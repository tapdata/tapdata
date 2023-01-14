/**
 * @title: LogsHandler
 * @description:
 * @author lk
 * @date 2021/9/11
 */
package com.tapdata.tm.ws.handler;

import com.mongodb.client.model.changestream.FullDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.changestream.config.ChangeStreamManager;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.entity.LogEntityElastic;
import com.tapdata.tm.log.service.LogService;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.cs.LogsListener;
import com.tapdata.tm.ws.dto.LogsCache;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@WebSocketMessageHandler(type = MessageType.LOGS)
@Slf4j
public class LogsHandler implements WebSocketHandler {

	private final MongoTemplate mongoTemplate;

	private final LogService logService;

	private static MessageListenerContainer container;

	public static final Map<String, List<LogsCache>> logsMap = new ConcurrentHashMap<>();
	public static final Map<String, List<String>> esLogsMap = new ConcurrentHashMap<>();

	public static AtomicLong cacheEmptyTime = new AtomicLong();

	private static Thread pollingQueryESThread = null;

	public LogsHandler(MongoTemplate mongoTemplate, LogService logService) {
		this.mongoTemplate = mongoTemplate;
		this.logService = logService;
	}

	@Override
	public void handleMessage(WebSocketContext context) throws Exception {
		MessageInfo messageInfo = context.getMessageInfo();
		if (messageInfo == null){
			try {
				WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("Message data cannot be null"));
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage(), e);
			}
			return;
		}

		Map<String, Object> filter = messageInfo.getFilter();
		String dataFlowId = MapUtils.getAsStringByPath(filter, "where/contextMap.dataFlowId/eq");
		if (StringUtils.isBlank(dataFlowId)){
			try {
				WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("DataFlowId is illegal"));
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}else {
			TaskEntity entity = mongoTemplate.findOne(Query.query(Criteria.where("_id").is(toObjectId(dataFlowId)).and("user_id").is(context.getUserId())), TaskEntity.class);
			if (entity == null) {
				WebSocketManager.sendMessage(context.getSender(), "DataFlow info was not found");
				return;
			}
		}

		if (logService.useElastic()) {
			handleMessageElastic(context, filter, dataFlowId);
		} else {
			handleMessageMongo(context, filter, dataFlowId);
		}

	}

	public void handleMessageMongo(WebSocketContext context, Map<String, Object> filter, String dataFlowId) {
		if (!logsMap.containsKey(dataFlowId)){
			logsMap.putIfAbsent(dataFlowId, new ArrayList<>());
		}
		LogsCache logsCache = new LogsCache(context.getSessionId(), context.getSender());
		List<LogsCache> logsCaches = logsMap.get(dataFlowId);
		boolean isExist = logsCaches.stream().anyMatch(logsCach -> context.getSessionId().equals(logsCach.getSessionId()));
		if (isExist){
			log.info("LogsCache always exists,sender: {},dataFlowId: {}, sessionId: {}", context.getSender(), dataFlowId, context.getSessionId());
			logsMap.values().stream()
					.filter(CollectionUtils::isNotEmpty)
					.forEach(value -> value.removeIf(cache -> context.getSessionId().equals(cache.getSessionId())));
		}
		logsCaches.add(logsCache);
		startChangeStream();

		try {
			log.info("Handler message start,context: {}", JsonUtil.toJson(context));
			Query query = Query.query(where("contextMap.dataFlowId").is(dataFlowId));
			String order = MapUtils.getAsString(filter, "order");
			if (StringUtils.isNotBlank(order)){
				query.with(Sort.by(Sort.Direction.DESC, "_id"));
			}
			Long limit = MapUtils.getAsLong(filter, "limit");
			if (limit != null && limit > 0){
				query.limit(limit.intValue());
			}
			List<LogDto> logs = logService.findAll(query);
			sendLogsMessage(dataFlowId, context.getSender(), logs);

			if (CollectionUtils.isNotEmpty(logs)){
				logsCache.setLastTime(logs.get(0).getCreateAt().getTime());
				LinkedBlockingQueue<Document> caches = logsCache.getCaches();
				// after all the logs cached in queue are consumed, the logsCache will be disabled
				// and com.tapdata.tm.ws.cs.LogsListener will send the log to ws session directly
				// when it gets the log change stream.
				while(caches.size() > 0) {
					try {
						Document document = caches.poll();
						if (document != null){
							Date createTime = document.getDate("createTime");
							if (createTime.getTime() - logsCache.getLastTime() > 0) {
								sendLogsMessage(dataFlowId, context.getSender(), document);
							}
						}
					}catch (Exception ignored){

					}
				}
			}
			log.info("Handler message end,sessionId: {}", context.getSessionId());
			logsCache.setEnabled(false);
		} catch (Exception e) {
			log.error("WebSocket send history log message failed,message: {}", e.getMessage());
			logsCache.setEnabled(false);
		}
	}

	public  void startChangeStream(){
		if (container == null || !container.isRunning()){
			log.info("Enable changestream monitoring Logs");
			container = ChangeStreamManager.start("Logs",
					newAggregation(match(where("operationType").in("insert"))),
					FullDocument.UPDATE_LOOKUP, mongoTemplate, new LogsListener());
		}else {
			log.info("Messagelistenercontainer is already running,collectionName: Logs");
		}
		cacheEmptyTime.set(0);
	}

	public static void stopChangeStream(){
		if (container != null && container.isRunning() && System.currentTimeMillis() - cacheEmptyTime.get() > 1000 * 60 * 10){
			log.info("Disable changestream monitoring Logs");
			try {
				container.stop();
			}catch (Exception e){
				log.error("Disable changestream failed,message: {}", e.getMessage());
			}

		}
	}

	/**
	 *
	 * @param context
	 * @param filter
	 * @param dataFlowId
	 */
	public void handleMessageElastic(WebSocketContext context, Map<String, Object> filter, String dataFlowId) {

		// add log cache for client
		if (!logsMap.containsKey(dataFlowId)) {
			logsMap.put(dataFlowId, new ArrayList<>());
		}
		LogsCache logsCache = new LogsCache(context.getSessionId(), context.getSender());
		List<LogsCache> logsCaches = logsMap.get(dataFlowId);
		boolean isExist = logsCaches.stream().anyMatch(logsCache1 -> context.getSessionId().equals(logsCache1.getSessionId()));
		if (isExist){
			log.info("LogsCache always exists,sender: {},dataFlowId: {}, sessionId: {}", context.getSender(), dataFlowId, context.getSessionId());
			logsMap.values().stream()
					.filter(CollectionUtils::isNotEmpty)
					.forEach(value -> value.removeIf(cache -> context.getSessionId().equals(cache.getSessionId())));
		}
		logsCaches.add(logsCache);

		// start polling query logs in es
		startPollingQueryES();

		// query last page logs and send to client
		String order = MapUtils.getAsString(filter, "order");
		Long limit = MapUtils.getAsLong(filter, "limit");
		CriteriaQuery topNQuery = logService.find(dataFlowId, order, limit);
		SearchHits<LogEntityElastic> searchHits = logService.find(topNQuery);
		List<LogDto> logs = convertSearchHintsIntoDtoList(searchHits);
		sendLogsMessage(dataFlowId, context.getSender(), logs);

		// send cache logs to client and disable log cache for client
		try {
			LinkedBlockingQueue<Document> cache = logsCache.getCaches();
			while(cache.size() > 0) {
				try {
					Document document = cache.poll();
					if (document != null){
						Date createTime = document.getDate("createTime");
						if (createTime.getTime() - logsCache.getLastTime() > 0) {
							sendLogsMessage(dataFlowId, context.getSender(), document);
						}
					}
				} catch (Exception ignored){
				}
			}
			logsCache.setEnabled(false);
			log.info("Handler message end,sessionId: {}", context.getSessionId());
		} catch (Throwable e) {
			log.error("WebSocket send history log message failed,message: {}", e.getMessage(), e);
			logsCache.setEnabled(false);
		}

	}

	/**
	 * polling query logs in es
	 */
	private void startPollingQueryES() {

		if (pollingQueryESThread != null) {
			if (Thread.State.TERMINATED == pollingQueryESThread.getState()) {
				// thread recycle
			} else {
				return;
			}
		}

		pollingQueryESThread = new Thread(() -> {
			try {

				AtomicLong cursorTimeForEs = new AtomicLong();

				while (true) {

					long currentPoint = System.currentTimeMillis() - 500;

					if (MapUtils.isEmpty(logsMap)) {
						cursorTimeForEs.set(currentPoint);
						Thread.sleep(2000);
						continue;
					}

					if (cursorTimeForEs.get() == 0) {
						cursorTimeForEs.set(currentPoint - 2000);
					}

					org.springframework.data.elasticsearch.core.query.Criteria criteria =
							org.springframework.data.elasticsearch.core.query.Criteria.where("createAt")
									.greaterThanEqual(new Date(cursorTimeForEs.get())).lessThan(new Date(currentPoint));
					CriteriaQuery query = new CriteriaQuery(criteria);
					query.addSort(Sort.by(Sort.Order.asc("createAt")));

					SearchHits<LogEntityElastic> result = logService.find(query);

					List<LogDto> logs = convertSearchHintsIntoDtoList(result);

					logs.forEach(log -> {

						String dataFlowId = MapUtils.getAsStringByPath(log.getContextMap(), "dataFlowId");
						if (StringUtils.isNotBlank(dataFlowId)){
							List<LogsCache> logsCaches = logsMap.get(dataFlowId);
							if (CollectionUtils.isEmpty(logsCaches)){
								return;
							}
							for (LogsCache logsCache : new ArrayList<>(logsCaches)) {
								LinkedBlockingQueue<Document> caches = logsCache.getCaches();
								if (logsCache.getEnabled()){
									caches.offer(Document.parse(JsonUtil.toJson(log)));
								}else {
									if (caches.size() > 0){
										for (int i = 0; i < caches.size(); i++) {
											Document document = caches.poll();
											if (document != null){
												LogsHandler.sendLogsMessage(dataFlowId, logsCache.getReceiver(), Collections.singletonList(document));
											}
										}
									}
									LogsHandler.sendLogsMessage(dataFlowId, logsCache.getReceiver(), Collections.singletonList(log));
								}
							}
						}

					});


					cursorTimeForEs.set(currentPoint);
					Thread.sleep(2000);
				}

			} catch (Exception e) {
				log.error("Polling query logs in es failed", e);
				pollingQueryESThread = null;
			}
		});

		pollingQueryESThread.start();

	}

	private List<LogDto> convertSearchHintsIntoDtoList(SearchHits<LogEntityElastic> searchHits) {
		List<LogDto> logs = new ArrayList<>();
		searchHits.stream().iterator().forEachRemaining(searchHit -> {
			LogEntityElastic entity = searchHit.getContent();
			try {
				LogDto dto = LogDto.class.getDeclaredConstructor().newInstance();
				BeanUtils.copyProperties(entity, dto);
				logs.add(dto);
			} catch (Exception e) {
				log.error("Convert dto " + LogDto.class + " failed.", e);
			}
		});

		return logs;
	}


	public static void sendLogsMessage(String dataFlowId, String receiver, Object data){

		try {
			Map<String, Object> map = new HashMap<>();
			map.put("type", "logs");
			map.put("dataFlowId", dataFlowId);
			map.put("data", data);
			WebSocketManager.sendMessage(receiver, JsonUtil.toJsonUseJackson(map));
		} catch (Exception e) {
			log.error("WebSocket send log message failed,message: {}", e.getMessage());
		}
	}

	public static void removeSession(String id){
		if (MapUtils.isNotEmpty(logsMap) && StringUtils.isNotBlank(id)){
			logsMap.values().stream()
					.filter(CollectionUtils::isNotEmpty)
					.forEach(value -> value.removeIf(logsCache -> id.equals(logsCache.getSessionId())));
			logsMap.entrySet().removeIf(entry -> CollectionUtils.isEmpty(entry.getValue()));
		}

		if (MapUtils.isNotEmpty(esLogsMap) && StringUtils.isNotBlank(id)){
			esLogsMap.values().stream()
					.filter(CollectionUtils::isNotEmpty)
					.forEach(value -> value.removeIf(id::equals));
			esLogsMap.entrySet().removeIf(entry -> CollectionUtils.isEmpty(entry.getValue()));
		}

		if (MapUtils.isEmpty(logsMap)){
			cacheEmptyTime.set(System.currentTimeMillis());
		}
	}


}
