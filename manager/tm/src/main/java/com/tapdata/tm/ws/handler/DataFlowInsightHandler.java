/**
 * @title: DataFlowInsight
 * @description:
 * @author lk
 * @date 2021/9/15
 */
package com.tapdata.tm.ws.handler;

import cn.hutool.core.thread.ThreadFactoryBuilder;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightDto;
import com.tapdata.tm.dataflowinsight.service.DataFlowInsightService;
import static com.tapdata.tm.dataflowinsight.service.DataFlowInsightService.granularityMap;
import com.tapdata.tm.utils.MapUtils;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

import com.tapdata.tm.ws.dto.DataFlowInsightCache;
import com.tapdata.tm.ws.dto.GranularityInfo;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

//@WebSocketMessageHandler(type = MessageType.DATA_FLOW_INSIGHT)
@Slf4j
public class DataFlowInsightHandler implements WebSocketHandler{

	private final DataFlowInsightService dataFlowInsightService;

	private final MongoTemplate mongoTemplate;

	private static final Map<String, DataFlowInsightCache> dataFlowInsightMap = new ConcurrentHashMap<>();

	public DataFlowInsightHandler(DataFlowInsightService dataFlowInsightService, MongoTemplate mongoTemplate) {
		this.dataFlowInsightService = dataFlowInsightService;
		this.mongoTemplate = mongoTemplate;
		ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(1, ThreadFactoryBuilder.create().setNamePrefix("dataFlowInsight-executor-").build());
		pool.scheduleWithFixedDelay(this::handler, 0, 3, TimeUnit.SECONDS);
	}

	@Override
	public void handleMessage(WebSocketContext context) throws Exception {
		MessageInfo messageInfo = context.getMessageInfo();
		if (messageInfo == null){
			try {
				WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("Message data cannot be null"));
			} catch (Exception e) {
				log.error("WebSocket send message failed, message: {}", e.getMessage());
			}
			return;
		}

		if (StringUtils.isNotBlank(messageInfo.getDataFlowId())){
			DataFlow dataFlow = mongoTemplate.findOne(Query.query(Criteria.where("_id").is(toObjectId(messageInfo.getDataFlowId())).and("user_id").is(context.getUserId())), DataFlow.class);
			if (dataFlow == null){
				WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("DataFlow info was not found"));
				return;
			}
		}
		String sessionId = context.getSessionId();
		dataFlowInsightMap.put(sessionId, new DataFlowInsightCache(sessionId, messageInfo));
		handler();

	}

	private void handler(){
		try {
			List<DataFlowInsightCache> dataFlowInsightCaches = new ArrayList<>(dataFlowInsightMap.values());
			if (CollectionUtils.isEmpty(dataFlowInsightCaches)){
				//log.debug("DataFlowInsightCaches is empty");
				return;
			}
			Map<String, Map<String, List<DataFlowInsightDto>>> resultMap = new HashMap<>();
			granularityMap.forEach((key, value) -> {
				Set<String> dataFlowIdAndStageId = getDataFlowIdAndStageIdByGranularity(value, dataFlowInsightCaches);
				if (CollectionUtils.isNotEmpty(dataFlowIdAndStageId)) {
					if (!resultMap.containsKey(key)){
						resultMap.put(key, new HashMap<>());
					}
					dataFlowIdAndStageId.forEach(ids -> {
						List<DataFlowInsightDto> dataFlowInsight = dataFlowInsightService.findDataFlowInsight(key, ids);
						if (CollectionUtils.isNotEmpty(dataFlowInsight)) {
							resultMap.get(key).put(ids, dataFlowInsight);
						}
					});
				}
			});

			for (DataFlowInsightCache dataFlowInsightCache : dataFlowInsightCaches) {
				GranularityInfo granularityInfo = dataFlowInsightCache.getGranularityInfo();
				String idsStr = getIdsStr(dataFlowInsightCache);
				String stageId = dataFlowInsightCache.getStageId();

				List<Map<String, Object>> throughputResults = dataFlowInsightService.getThroughputResults(resultMap, granularityInfo.getThroughput(), idsStr, stageId);
				List<Map<String, Object>> transTimeResults = dataFlowInsightService.getTransTimeResults(resultMap, granularityInfo.getTransTime(), idsStr, stageId);
				List<Map<String, Object>> replLagResults = dataFlowInsightService.getReplLagResults(resultMap, granularityInfo.getReplLag(), idsStr, stageId);
				Map<String, Object> dataOverviewResult = dataFlowInsightService.getDataOverviewResult(resultMap, granularityInfo.getDataOverview(), idsStr, stageId);
				sendDataFlowInsight(dataFlowInsightCache, throughputResults, transTimeResults, replLagResults, dataOverviewResult);
			}

		}catch (Exception e){
			log.error("DataFlowInsightExecutor error,message: {}", e.getMessage());
		}
	}





	/**
	 * 根据粒度获取需要统计的dataFlowId
	 * granularity:   flow_minute(minute)、
	 *                stage_minute(minute)、
	 *                flow_hour(hour)、
	 *                stage_hour(hour)、
	 *                flow_day(day)、
	 *                stage_day(day)、
	 *                second
	 **/
	private Set<String> getDataFlowIdAndStageIdByGranularity(List<String> granularity, List<DataFlowInsightCache> dataFlowInsightCaches){
		if (CollectionUtils.isEmpty(granularity)){
			return Collections.emptySet();
		}
		dataFlowInsightCaches.sort(Comparator.comparing(DataFlowInsightCache::getDataFlowId));
		return dataFlowInsightCaches.stream()
				.filter(dataFlowInsightCache -> dataFlowInsightCache.getGranularityInfo() != null)
				.filter(dataFlowInsightCache -> granularity.contains(dataFlowInsightCache.getGranularityInfo().getDataOverview())
						|| granularity.contains(dataFlowInsightCache.getGranularityInfo().getReplLag())
						|| granularity.contains(dataFlowInsightCache.getGranularityInfo().getThroughput())
						|| granularity.contains(dataFlowInsightCache.getGranularityInfo().getTransTime()))
				.map(this::getIdsStr)
				.collect(Collectors.toSet());
	}



	private String getIdsStr(DataFlowInsightCache dataFlowInsightCache){
		return (StringUtils.isBlank(dataFlowInsightCache.getDataFlowId()) ? "" : dataFlowInsightCache.getDataFlowId()) + ","
				+ (StringUtils.isBlank(dataFlowInsightCache.getStageId()) ? "" : dataFlowInsightCache.getStageId());
	}

	public static void removeSession(String id){
		if (MapUtils.isNotEmpty(dataFlowInsightMap) && StringUtils.isNotBlank(id)){
			dataFlowInsightMap.remove(id);
		}
	}

	private static void sendDataFlowInsight(DataFlowInsightCache dataFlowInsightCache,
	                                        List<Map<String, Object>> throughputResults,
	                                        List<Map<String, Object>> transTimeResults,
	                                        List<Map<String, Object>> replLagResults,
	                                        Map<String, Object> dataOverviewResult){

		try {
			Map<String, Object> map = new HashMap<>();
			map.put("statsType", dataFlowInsightCache.getStatsType());
			map.put("createTime", System.currentTimeMillis());
			map.put("dataFlowId", dataFlowInsightCache.getDataFlowId());
			map.put("stageId", dataFlowInsightCache.getStageId());
			map.put("granularity", dataFlowInsightCache.getGranularityInfo());
			Map<String, Object> statsData = new HashMap();
			statsData.put("throughput", throughputResults);
			statsData.put("trans_time", transTimeResults);
			statsData.put("repl_lag", replLagResults);
			statsData.put("data_overview", dataOverviewResult);
			map.put("statsData", statsData);
			map.put("type", MessageType.DATA_FLOW_INSIGHT.getType());
			map.put("collection", "DataFlowInsight");
			WebSocketManager.sendMessageBySessionId(dataFlowInsightCache.getSessionId(), JsonUtil.toJson(map));
		} catch (Exception e) {
			log.error("WebSocket send dataFlow insight failed,message: {}", e.getMessage());
		}
	}
}
