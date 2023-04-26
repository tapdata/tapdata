package com.tapdata.cache.memory;

import com.tapdata.cache.ICacheGetter;
import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 任务缓存取数接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/24 下午12:06 Create
 */
public class JobCacheGetter implements ICacheGetter {
	private Logger logger = LogManager.getLogger(JobCacheGetter.class);

	private String dataFlowId;
	private Set<String> whitelist;
	private ICacheService cacheService;

	public JobCacheGetter(String dataFlowId, Set<String> whitelist, ICacheService cacheService) {
		this.dataFlowId = dataFlowId;
		this.whitelist = whitelist;
		this.cacheService = cacheService;
	}

	@Override
	public Map<String, Object> getAndSetCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		if (whitelist.contains(cacheName)) {
			while (true) {
				try {
					return cacheService.getAndSetCache(cacheName, lookup, cacheKeys);
				} catch (RuntimeException e) {
					logger.warn("Waite cache {} recover: {}, stack: {}", cacheName, e.getMessage(), Log4jUtil.getStackString(e));
				}
			}
		}
		throw new RuntimeException("Not found cache '" + cacheName + "', data flow id '" + dataFlowId + "'");
	}

	@Override
	public List<Map<String, Object>> getAndSetCacheArray(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		if (whitelist.contains(cacheName)) {
			while (true) {
				try {
					return cacheService.getAndSetCacheArray(cacheName, lookup, cacheKeys);
				} catch (RuntimeException e) {
					logger.warn("Waite cache {} recover: {}, stack: {}", cacheName, e.getMessage(), Log4jUtil.getStackString(e));
				}
			}
		}
		throw new RuntimeException("Not found cache '" + cacheName + "', data flow id '" + dataFlowId + "'");
	}

	@Override
	public Map<String, Object> getCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		if (whitelist.contains(cacheName)) {
			while (true) {
				try {
					return cacheService.getCache(cacheName, lookup, cacheKeys);
				} catch (RuntimeException e) {
					logger.warn("Waite cache {} recover: {}, stack: {}", cacheName, e.getMessage(), Log4jUtil.getStackString(e));
				}
			}
		}
		throw new RuntimeException("Not found cache '" + cacheName + "', data flow id '" + dataFlowId + "'");
	}

	@Override
	public Object getCacheItem(String cacheName, String field, Object defaultValue, Object... cacheKeys) throws Throwable {
		if (whitelist.contains(cacheName)) {
			while (true) {
				try {
					return cacheService.getCacheItem(cacheName, field, defaultValue, cacheKeys);
				} catch (RuntimeException e) {
					logger.warn("Waite cache {} recover: {}, stack: {}", cacheName, e.getMessage(), Log4jUtil.getStackString(e));
				}
			}
		}
		throw new RuntimeException("Not found cache '" + cacheName + "', data flow id '" + dataFlowId + "'");
	}

	public String[] whitelist() {
		return whitelist.toArray(new String[0]);
	}

	public static JobCacheGetter getInstance(String dataFlowId, ClientMongoOperator clientMongoOperator, ICacheService cacheService) {
		Query query = Query.query(Criteria.where("stages.type").is(Stage.StageTypeEnum.MEM_CACHE.getType()).and("stages.cacheType").is("all"));
		query.fields().include("id").include("stages");
		List<DataFlow> dataFlows = clientMongoOperator.find(query, ConnectorConstant.DATA_FLOW_COLLECTION, DataFlow.class);

		// id 不能放在 $or 中查询，所以拆成两个
		if (!dataFlows.stream().anyMatch(dataFlow -> dataFlowId.equals(dataFlow.getId()))) {
			query = Query.query(Criteria.where("id").is(dataFlowId));
			query.fields().include("id").include("stages");
			DataFlow dataFlow = clientMongoOperator.findOne(query, ConnectorConstant.DATA_FLOW_COLLECTION, DataFlow.class);
			if (null != dataFlow) {
				dataFlows.add(dataFlow);
			}
		}

		Set<String> whitelist = new HashSet<>();
		for (DataFlow dataFlow : dataFlows) {
			if (null == dataFlow.getStages()) continue;
			for (Stage stage : dataFlow.getStages()) {
				if (null != stage
						&& Stage.StageTypeEnum.MEM_CACHE.getType().equals(stage.getType())
						&& ("all".equals(stage.getCacheType()) || dataFlowId.equals(dataFlow.getId()))) {
					whitelist.add(stage.getCacheName());
				}
			}
		}
		return new JobCacheGetter(dataFlowId, whitelist, cacheService);
	}
}
