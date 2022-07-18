package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheService;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HazelcastTargetPdkCacheNode extends HazelcastTargetPdkBaseNode {

	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkCacheNode.class);

	private final String cacheName;

	private DataFlowCacheConfig dataFlowCacheConfig;

	private ICacheService cacheService;
	public HazelcastTargetPdkCacheNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		Node<?> node = dataProcessorContext.getNode();
		if (node instanceof CacheNode) {
			this.cacheName = ((CacheNode) node).getCacheName();
		} else {
			throw new IllegalArgumentException("node must be CacheNode");
		}
	}

	@Override
	protected void init(@NotNull Context context) throws Exception {
		super.init(context);
		this.cacheService = this.dataProcessorContext.getCacheService();
		this.dataFlowCacheConfig = cacheService.getConfig(cacheName);
	}

	@Override
	void processEvents(List<TapEvent> tapEvents) {
		for (TapEvent tapEvent : tapEvents) {

			Map<String, Object> row = TapEventUtil.getAfter(tapEvent);
			if (MapUtils.isEmpty(row)) {
				logger.warn("Cache row is empty, will abort it, msg {}", tapEvent);
				continue;
			}

			final String cacheKeys = dataFlowCacheConfig.getCacheKeys();
			final Object[] cacheKeyValues = CacheUtil.getKeyValues(Arrays.asList(cacheKeys.split(",")), row);
			if (null == cacheKeyValues) throw new RuntimeException("Cache key not in row data: " + cacheKeys);

			String cacheKey = CacheUtil.cacheKey(cacheKeyValues);
			if (tapEvent instanceof TapUpdateRecordEvent) {
				List<Map<String, Object>> rows = new ArrayList<>(1);
				rows.add(row);
				cacheService.cacheRow(cacheName, cacheKey, rows);
			} else if (tapEvent instanceof TapDeleteRecordEvent) {
				final Object[] pkKeyValues = CacheUtil.getKeyValues(dataFlowCacheConfig.getPrimaryKeys(), row);
				if (null == pkKeyValues) throw new RuntimeException("Cache primary key not in row data: " + dataFlowCacheConfig.getPrimaryKeys());
				String pkKey = CacheUtil.cacheKey(pkKeyValues);
				cacheService.removeByKey(cacheName, cacheKey, pkKey);

			} else {
				if (logger.isDebugEnabled()) {
					logger.debug("Cache row is not update or delete, will abort it, msg {}", tapEvent);
				}
			}

		}
	}

	@Override
	void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		throw new UnsupportedOperationException();
	}
}
