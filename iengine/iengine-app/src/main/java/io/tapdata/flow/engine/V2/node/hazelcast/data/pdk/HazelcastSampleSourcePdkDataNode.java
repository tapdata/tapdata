package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.SampleMockUtil;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections4.CollectionUtils;
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
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		startSourceRunner();
	}

	protected TapCodecsFilterManager initNode() {
		HazelcastInstance hazelcastInstance = jetContext.hazelcastInstance();
		createPdkConnectorNode(dataProcessorContext, hazelcastInstance);
		connectorNodeInit(dataProcessorContext);
		ConnectorNode connectorNode = getConnectorNode();
		return connectorNode.getCodecsFilterManager();
	}

	public void startSourceRunner() {

		try {
			Node<?> node = dataProcessorContext.getNode();
			TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
			List<String> tables = new ArrayList<>(tapTableMap.keySet());
			int rows = 1;
			if (node instanceof DatabaseNode) {
				rows = ((DatabaseNode) node).getRows() == null ? 1 : ((DatabaseNode) node).getRows();
				tables = getSourceTables(node,tables);
			} else if (node instanceof TableNode) {
				rows = ((TableNode) node).getRows() == null ? 1 : ((TableNode) node).getRows();
			}
			TapCodecsFilterManager codecsFilterManager = initNode();

			// 测试任务
			long startTs = System.currentTimeMillis();
			for (String tableName : tables) {
				if (!isRunning()) {
					break;
				}
				TapTable tapTable = tapTableMap.get(tableName);
				String sampleDataId = ((DataParentNode) node).getConnectionId() + "_" + tableName;

				List<TapEvent> tapEventList;
				if (processorBaseContext.getTaskDto().isDeduceSchemaTask()) {
					tapEventList = sampleDataCacheMap.getOrDefault(sampleDataId, new ArrayList<>());
				} else {
					tapEventList = new ArrayList<>();
				}
				boolean isCache = true;
				boolean needMock = false;
				if (CollectionUtils.isEmpty(tapEventList) || tapEventList.size() < rows) {
					tapEventList.clear();
					isCache = false;
					try {
						QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();
						if (null == queryByAdvanceFilterFunction){
							throw new CoreException("Can not get test data from source, QueryByAdvanceFilterFunction is not supported.");
						}
						TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(rows);
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
								createPdkMethodInvoker().runnable(
										() -> queryByAdvanceFilterFunction.query(getConnectorNode().getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {

											List<Map<String, Object>> results = filterResults.getResults();
											List<TapEvent> events = wrapTapEvent(results, tapTable.getId());
											if (CollectionUtils.isNotEmpty(events)) {
												events.forEach(tapEvent -> {
													tapRecordToTapValue(tapEvent, codecsFilterManager);
													//Simulate null data
													if (processorBaseContext.getTaskDto().isDeduceSchemaTask()) {
														SampleMockUtil.mock(tapTable, TapEventUtil.getAfter(tapEvent));
													}
												});

												tapEventList.addAll(events);
											}
										})).logTag(TAG)
						);
						if (processorBaseContext.getTaskDto().isDeduceSchemaTask()) {
							sampleDataCacheMap.put(sampleDataId, tapEventList);
						}
						TaskDto taskDto = processorBaseContext.getTaskDto();
						if (null != taskDto && taskDto.isTestTask() && tapEventList.isEmpty()) {
							needMock = true;
							obsLogger.warn("Source table is empty, trying to mock data");
						}
					} catch (Exception e) {
						logger.warn("Error getting sample data, will try to simulate: {}", e.getMessage());
						AspectUtils.executeAspect(new TaskStopAspect().task(processorBaseContext.getTaskDto()).error(new CoreException("Can not get data from source")));
					}
				}

				if (logger.isDebugEnabled()) {
					logger.debug("get sample data, cache [{}], cost {}ms", isCache, (System.currentTimeMillis() - startTs));
				}

				List<TapEvent> cloneList = new ArrayList<>();
				int count = 0;
				for (TapEvent tapEvent : tapEventList) {
					if (count >= rows) {
						break;
					}
					cloneList.add((TapEvent) tapEvent.clone());
					count++;
				}

				List<TapdataEvent> tapdataEvents = wrapTapdataEvent(cloneList);
				if (CollectionUtils.isEmpty(tapdataEvents)) {
					//mock
					try {
						if (processorBaseContext.getTaskDto().isDeduceSchemaTask() || needMock) {
							tapdataEvents = SampleMockUtil.mock(tapTable, rows);
						}
					}catch (Exception e){
						obsLogger.warn("mock data failed, {}, {}", e.getStackTrace(), e.getMessage());
					}
				}
				for (TapdataEvent tapdataEvent : tapdataEvents) {
					while (true) {
						if (offer(tapdataEvent)) {
							break;
						}
						//try again
					}
				}
				if (processorBaseContext.getTaskDto().isDeduceSchemaTask()) {
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
	public PDKMethodInvoker createPdkMethodInvoker() {
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

	protected List<String> getSourceTables(Node node,List<String> tables) {
		if (StringUtils.equalsAnyIgnoreCase(dataProcessorContext.getTaskDto().getSyncType(),
				TaskDto.SYNC_TYPE_TEST_RUN)){
			return ((DatabaseNode) node).getTableNames();
		}
		return tables;
	}
}
