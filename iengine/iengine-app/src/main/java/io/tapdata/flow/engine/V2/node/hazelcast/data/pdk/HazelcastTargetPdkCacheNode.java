package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.jet.core.Inbox;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheService;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class HazelcastTargetPdkCacheNode extends HazelcastPdkBaseNode {

	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkCacheNode.class);

	private final String cacheName;

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
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(node);
		this.dataMap = new DocumentIMap<>(HazelcastUtil.getInstance(), CacheUtil.CACHE_NAME_PREFIX + this.cacheName, externalStorage);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		ICacheService cacheService = this.dataProcessorContext.getCacheService();
		this.dataFlowCacheConfig = cacheService.getConfig(cacheName);
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, 1000);
					if (count > 0) {
						List<TapEvent> tapEvents = new ArrayList<>();
						for (TapdataEvent tapdataEvent : tapdataEvents) {

							if (tapdataEvent.isDML()) {
								TapRecordEvent tapRecordEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
								fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager);
								fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager);
								tapEvents.add(tapRecordEvent);
							} else {
								if (null != tapdataEvent.getTapEvent()) {
									logger.warn("Tap event type does not supported: " + tapdataEvent.getTapEvent().getClass() + ", will ignore it");
									obsLogger.warn("Tap event type does not supported: " + tapdataEvent.getTapEvent().getClass() + ", will ignore it");
								}
							}
						}
						if (CollectionUtils.isNotEmpty(tapEvents)) {
							processEvents(tapEvents);
						}
					} else {
						break;
					}
				}
			}
		} catch (Throwable e) {
			String msg = String.format("Target process failed: %s", e.getMessage());
			errorHandle(new RuntimeException(msg, e), msg);
		}
	}

	void processEvents(List<TapEvent> tapEvents) throws Throwable {
		for (TapEvent tapEvent : tapEvents) {

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

}
