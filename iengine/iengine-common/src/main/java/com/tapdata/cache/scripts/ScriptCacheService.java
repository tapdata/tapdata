package com.tapdata.cache.scripts;

import com.tapdata.cache.ICacheGetter;
import com.tapdata.cache.ICacheService;
import com.tapdata.cache.IDataSourceRowsGetter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScriptCacheService implements ICacheGetter {

	private final static Logger logger = LogManager.getLogger(ScriptCacheService.class);

	private final String taskId;
	private final String nodeId;
	private final ClientMongoOperator clientMongoOperator;
	private final ICacheService supperCacheService;

	public ScriptCacheService(ClientMongoOperator clientMongoOperator, DataProcessorContext dataProcessorContext) {
		this.taskId = dataProcessorContext.getTaskDto().getId().toHexString();
		this.nodeId = dataProcessorContext.getNode().getId();
		this.clientMongoOperator = clientMongoOperator;
		this.supperCacheService = dataProcessorContext.getCacheService();
	}

	private final Set<String> useInfo = new HashSet<>();

	private void setTaskUsedInfo(String cacheName) {
		if (!useInfo.contains(cacheName)) {
			useInfo.add(cacheName);
			Update update = new Update();
			update.addToSet(String.format("attrs.%s.%s", TaskDto.ATTRS_USED_SHARE_CACHE, cacheName), nodeId);
			clientMongoOperator.update(Query.query(Criteria.where("_id").is(taskId)), update, ConnectorConstant.TASK_COLLECTION);
		}
	}

	@Override
	public Map<String, Object> getAndSetCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		setTaskUsedInfo(cacheName);
		return supperCacheService.getAndSetCache(cacheName, lookup, cacheKeys);
	}

	@Override
	public List<Map<String, Object>> getAndSetCacheArray(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		setTaskUsedInfo(cacheName);
		return supperCacheService.getAndSetCacheArray(cacheName, lookup, cacheKeys);
	}

	@Override
	public Map<String, Object> getCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		setTaskUsedInfo(cacheName);
		return supperCacheService.getCache(cacheName, lookup, cacheKeys);
	}

	@Override
	public Object getCacheItem(String cacheName, String field, Object defaultValue, Object... cacheKeys) throws Throwable {
		setTaskUsedInfo(cacheName);
		return supperCacheService.getCacheItem(cacheName, field, defaultValue, cacheKeys);
	}

	// overwrite
	@Override
	public Map<String, Object> getCache(String cacheName, Object... cacheKeys) throws Throwable {
		setTaskUsedInfo(cacheName);
		return supperCacheService.getCache(cacheName, cacheKeys);
	}

	@Override
	public IDataSourceRowsGetter getDataSourceRowsGetter() {
		return supperCacheService.getDataSourceRowsGetter();
	}

	@Override
	public void close() {
		supperCacheService.close();
	}

}
