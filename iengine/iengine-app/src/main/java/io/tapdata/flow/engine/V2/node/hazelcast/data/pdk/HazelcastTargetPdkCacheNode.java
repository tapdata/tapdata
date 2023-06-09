package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheService;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.error.ShareCacheExCode_20;
import io.tapdata.error.TapEventException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HazelcastTargetPdkCacheNode extends HazelcastTargetPdkBaseNode {

	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkCacheNode.class);

	private final String cacheName;
	private final String referenceId;

	private DataFlowCacheConfig dataFlowCacheConfig;

	private final ConstructIMap<Map<String, Map<String, Object>>> dataMap;

	public HazelcastTargetPdkCacheNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		Node<?> node = dataProcessorContext.getNode();
		if (node instanceof CacheNode) {
			this.cacheName = ((CacheNode) node).getCacheName();
		} else {
			throw new IllegalArgumentException("node must be CacheNode");
		}
		this.referenceId = referenceId(node);
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(node);
		this.dataMap = new DocumentIMap<>(HazelcastUtil.getInstance(), referenceId, CacheUtil.CACHE_NAME_PREFIX + this.cacheName, externalStorage);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		ICacheService cacheService = this.dataProcessorContext.getCacheService();
		this.dataFlowCacheConfig = cacheService.getConfig(cacheName);
	}

	void processEvents(List<TapEvent> tapEvents) {
		for (TapEvent tapEvent : tapEvents) {
			try {
				Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
				Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
				if (MapUtils.isEmpty(before)) {
					before = after;
				}

				String beforeCacheKey = getCacheKey(before);
				String beforePk = CacheUtil.getPk(dataFlowCacheConfig.getPrimaryKeys(), before);
				String afterCacheKey = getCacheKey(after);
				String afterPk = CacheUtil.getPk(dataFlowCacheConfig.getPrimaryKeys(), after);
				if (tapEvent instanceof TapUpdateRecordEvent) {
					CacheUtil.removeRecord(dataMap, beforeCacheKey, beforePk);
					Map<String, Map<String, Object>> recordMap;
					if (dataMap.exists(afterCacheKey)) {
						recordMap = dataMap.find(afterCacheKey);
					} else {
						recordMap = new HashMap<>();
					}
					recordMap.put(afterPk, after);
					dataMap.insert(afterCacheKey, recordMap);

				} else if (tapEvent instanceof TapDeleteRecordEvent) {
					CacheUtil.removeRecord(dataMap, beforeCacheKey, beforePk);
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug("Cache row is not update or delete, will abort it, msg {}", tapEvent);
					}
				}
			} catch (Throwable e) {
				throw new TapEventException(ShareCacheExCode_20.PDK_WRITE_SHARE_CACHE_FAILED, e).addEvent(tapEvent);
			}
		}
	}

	@NotNull
	private String getCacheKey(Map<String, Object> row) {
		final String cacheKeys = dataFlowCacheConfig.getCacheKeys();
		final Object[] cacheKeyValues = CacheUtil.getKeyValues(Arrays.asList(cacheKeys.split(",")), row);
		if (null == cacheKeyValues) {
			throw new RuntimeException("Cache key not in row data: " + cacheKeys);
		}
		return CacheUtil.cacheKey(cacheKeyValues);
	}

	@Override
	public void doClose() throws Exception {
		if (dataMap != null) {
			PersistenceStorage.getInstance().destroy(dataMap.getName());
		}
		super.doClose();
	}

	public static void clearCache(Node<?> node) {
		if (!(node instanceof CacheNode)) return;
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(node);
		String referenceId = referenceId(node);
		recursiveClearCache(externalStorage, referenceId, ((CacheNode) node).getCacheName(), HazelcastUtil.getInstance());
	}

	private static String referenceId(Node<?> node) {
		return String.format("%s-%s-%S", HazelcastTargetPdkCacheNode.class.getSimpleName(), node.getTaskId(), node.getId());
	}

	private static void recursiveClearCache(ExternalStorageDto externalStorageDto, String referenceId, String cacheName, HazelcastInstance hazelcastInstance) {
		if (StringUtils.isEmpty(cacheName)) return;
		cacheName = CacheUtil.CACHE_NAME_PREFIX + cacheName;
		ConstructIMap<Document> imap = new ConstructIMap<>(hazelcastInstance, referenceId, cacheName, externalStorageDto);
		try {
			imap.clear();
			imap.destroy();
		} catch (Exception e) {
			throw new RuntimeException("Clear imap failed, name: " + cacheName + ", error message: " + e.getMessage(), e);
		}
	}

}
