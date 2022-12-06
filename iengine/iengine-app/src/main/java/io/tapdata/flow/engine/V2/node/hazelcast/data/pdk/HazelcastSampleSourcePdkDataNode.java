package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.SampleMockUtil;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.voovan.tools.collection.CacheMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HazelcastSampleSourcePdkDataNode extends HazelcastPdkBaseNode {

	private final Logger logger = LogManager.getLogger(HazelcastSampleSourcePdkDataNode.class);

	private static final String TAG = HazelcastSampleSourcePdkDataNode.class.getSimpleName();

	private static final CacheMap<String, List<TapEvent>> sampleDataCacheMap = new CacheMap<>();


	static {
		sampleDataCacheMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
	}

	public HazelcastSampleSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		startSourceRunner();
	}

	public void startSourceRunner() {

		try {
			Node<?> node = dataProcessorContext.getNode();
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
			List<String> tables = new ArrayList<>(tapTableMap.keySet());
			int rows = 1;
			if (node instanceof DatabaseNode) {
				rows = ((DatabaseNode) node).getRows() == null ? 1 : ((DatabaseNode) node).getRows();
				tables = ((DatabaseNode) node).getTableNames();
			}

			// 测试任务
			long startTs = System.currentTimeMillis();
			for (String tableName : tables) {
				if (!isRunning()) {
					break;
				}
				TapTable tapTable = tapTableMap.get(tableName);
				String sampleDataId = ((DataParentNode) node).getConnectionId() + "_" + tableName;

				List<TapEvent> tapEventList = sampleDataCacheMap.getOrDefault(sampleDataId, new ArrayList<>());
				boolean isCache = true;
				if (CollectionUtils.isEmpty(tapEventList) || tapEventList.size() < rows) {
					tapEventList.clear();
					isCache = false;
					try {
						createPdkConnectorNode(dataProcessorContext, jetContext.hazelcastInstance());
						connectorNodeInit(dataProcessorContext);
						TapCodecsFilterManager codecsFilterManager = getConnectorNode().getCodecsFilterManager();
						QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();
						TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(rows);
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
								createPdkMethodInvoker().runnable(
										() -> queryByAdvanceFilterFunction.query(getConnectorNode().getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {

											List<Map<String, Object>> results = filterResults.getResults();
											List<TapEvent> events = wrapTapEvent(results, tapTable.getId());
											if (CollectionUtil.isNotEmpty(events)) {
												events.forEach(tapEvent -> {
													tapRecordToTapValue(tapEvent, codecsFilterManager);
													//Simulate null data
													SampleMockUtil.mock(tapTable, TapEventUtil.getAfter(tapEvent));
												});

												tapEventList.addAll(events);
											}
										})).logTag(TAG)
						);
						sampleDataCacheMap.put(sampleDataId, tapEventList);
					} catch (Exception e) {
						logger.warn("Error getting sample data, will try to simulate: {}", e.getMessage());
					}
				}

				if (logger.isDebugEnabled()) {
					logger.debug("get sample data, cache [{}], cost {}ms", isCache, (System.currentTimeMillis() - startTs));
				}

				List<TapEvent> cloneList = new ArrayList<>();
				int count = 0;
				for (TapEvent tapEvent : tapEventList) {
					if (count > rows) {
						break;
					}
					cloneList.add((TapEvent) tapEvent.clone());
					count++;
				}

				List<TapdataEvent> tapdataEvents = wrapTapdataEvent(cloneList);
				if (CollectionUtils.isEmpty(tapdataEvents)) {
					//mock
					tapdataEvents = SampleMockUtil.mock(tapTable, rows);
				}
				for (TapdataEvent tapdataEvent : tapdataEvents) {
					while (true) {
						if (offer(tapdataEvent)) {
							break;
						}
						//try again
					}
				}
				if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
						TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
					logger.info("get data from the following table {} to deduce schema, already obtained from table {}, " +
							"skip other tables", tables, tableName);
					break;
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("query sample data complete, cost {}ms", (System.currentTimeMillis() - startTs));
			}

		} catch (Throwable throwable) {
			errorHandle(throwable, "start source runner failed: " + throwable.getMessage());
		} finally {
			logger.info("source runner complete...");
		}
		//结束
		this.running.set(false);
		logger.info("source runner complete... {}", running.get());
	}

	@Override
	public boolean complete() {
		return !isRunning();
	}

	@Override
	protected PDKMethodInvoker createPdkMethodInvoker() {
		return super.createPdkMethodInvoker().maxRetryTimeMinute(0);
	}

	private List<TapdataEvent> wrapTapdataEvent(List<TapEvent> tapEvents) {

		List<TapdataEvent> tapdataEvents = new ArrayList<>();
		for (TapEvent tapEvent : tapEvents) {
			if (tapEvent instanceof TapRecordEvent) {
				TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
				TapdataEvent tapdataEvent = new TapdataEvent();
				tapdataEvent.setTapEvent(tapRecordEvent);
				tapdataEvent.setSyncStage(SyncStage.INITIAL_SYNC);
				tapdataEvents.add(tapdataEvent);
			}
		}
		return tapdataEvents;
	}

	private static List<TapEvent> wrapTapEvent(List<Map<String, Object>> results, String table) {
		List<TapEvent> tapEvents = new ArrayList<>();

		if (CollectionUtils.isNotEmpty(results)) {
			for (Map<String, Object> result : results) {
				tapEvents.add(new TapInsertRecordEvent().init().after(result).table(table));
			}
		}

		return tapEvents;
	}
}
