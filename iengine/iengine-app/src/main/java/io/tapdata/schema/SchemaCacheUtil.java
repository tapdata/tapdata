package io.tapdata.schema;

import com.tapdata.constant.CollectionUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.LRUMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 缓存已推演的节点模型
 */
public class SchemaCacheUtil {

	private final static Logger logger = LogManager.getLogger(SchemaCacheUtil.class);


	/**
	 * key: subTaskId+jsNodeId
	 */
	private static final Map<String, TapTable> tabTableCacheMap = new LRUMap(100);

	/**
	 * key: nodeId
	 */
	private static final Map<String, List<TapEvent>> sampleDataCacheMap = new LRUMap(100);

	public static TapTable getTapTable(String schemaKey) {
		TapTable tapTable = tabTableCacheMap.get(schemaKey);
		tabTableCacheMap.remove(schemaKey);
		return tapTable;
	}

	public static void putTabTable(String schemaKey, TapTable tapTable) {
		tabTableCacheMap.put(schemaKey, tapTable);
	}

	/**
	 * 获取样本数据
	 */
	public static void getSampleData(String sampleDataId, ConnectorNode connectorNode, TapTable tapTable, String TAG, Consumer<List<TapEvent>> action) {
		long startTs = System.currentTimeMillis();
		boolean isCache = true;
		List<TapEvent> tapEventList = sampleDataCacheMap.getOrDefault(sampleDataId, new ArrayList<>());
		if (CollectionUtils.isEmpty(tapEventList)) {
			isCache = false;
			QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(1);
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
					() -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {

						List<Map<String, Object>> results = filterResults.getResults();
						List<TapEvent> events = wrapTapEvent(results, tapTable.getId());
						if (CollectionUtil.isNotEmpty(events)) {
							tapEventList.addAll(events);
						}
					}), TAG);
			sampleDataCacheMap.put(sampleDataId, tapEventList);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("get sample data, cache [{}], cost {}ms", isCache, (System.currentTimeMillis() - startTs));
		}

		List<TapEvent> cloneList = new ArrayList<>();
		for (TapEvent tapEvent : tapEventList) {
			cloneList.add((TapEvent) tapEvent.clone());
		}
		action.accept(cloneList);
	}

	private static List<TapEvent> wrapTapEvent(List<Map<String, Object>> results, String table) {
		List<TapEvent> tapEvents = new ArrayList<>();

		for (Map<String, Object> result : results) {
			tapEvents.add(new TapInsertRecordEvent().init().after(result).table(table));
		}

		return tapEvents;
	}

}
