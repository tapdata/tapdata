package io.tapdata.flow.engine.V2.task.preview.node;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataPreviewCompleteEvent;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.ClassHandlersV2;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode;
import io.tapdata.flow.engine.V2.task.preview.*;
import io.tapdata.flow.engine.V2.task.preview.entity.MergeReadData;
import io.tapdata.flow.engine.V2.task.preview.operation.*;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2024-09-23 14:15
 **/
public class HazelcastPreviewSourcePdkDataNode extends HazelcastSourcePdkDataNode {
	public static final String TAG = HazelcastPreviewSourcePdkDataNode.class.getSimpleName();
	public static final String MOCK_METHOD = "MOCK";
	private ClassHandlersV2 previewOperationHandlers;
	private AtomicBoolean finishPreviewRead;
	private TableNode tableNode;

	public HazelcastPreviewSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.finishPreviewRead = new AtomicBoolean(false);
		Node<?> node = getNode();
		if (node instanceof TableNode) {
			this.tableNode = (TableNode) node;
		} else {
			throw new TapCodeException(TaskPreviewExCode_37.NODE_TYPE_INVALID, "Invalid node type: " + node.getClass().getName());
		}
		this.previewOperationHandlers = new ClassHandlersV2();
		this.previewOperationHandlers.register(PreviewReadOperation.class, this::read);
		this.previewOperationHandlers.register(PreviewMergeReadOperation.class, this::read);
		this.previewOperationHandlers.register(PreviewFinishReadOperation.class, this::finishRead);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		initTapLogger();
		createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
		connectorNodeInit(dataProcessorContext);
		initTapCodecsFilterManager();
	}

	@Override
	protected void initTapCodecsFilterManager() {
		this.defaultCodecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> tapValue.getValue().toInstant().toString());
		this.defaultCodecsRegistry.registerFromTapValue(TapDateValue.class, tapValue -> tapValue.getValue().toInstant().toString());
		this.defaultCodecsRegistry.registerFromTapValue(TapTimeValue.class, tapValue -> tapValue.getValue().toTimeStr());
		this.defaultCodecsRegistry.registerFromTapValue(TapYearValue.class, tapValue -> tapValue.getValue().toLocalDateTime().getYear());
		this.defaultCodecsRegistry.registerFromTapValue(TapNumberValue.class, tapValue -> {
			Double value = tapValue.getValue();
			// Determine if the decimal place of value is 0, convert it to Long, otherwise keep Double
			if (null != value && value % 1 == 0) {
				return value.longValue();
			} else {
				return value;
			}
		});
		this.defaultCodecsFilterManager = TapCodecsFilterManager.create(this.defaultCodecsRegistry);
	}

	protected PreviewFinishReadOperation finishRead(PreviewFinishReadOperation previewFinishReadOperation) {
		finishPreviewRead.set(true);
		if (previewFinishReadOperation.isLast()) {
			return previewFinishReadOperation;
		}
		return null;
	}

	protected List<TapInsertRecordEvent> read(PreviewOperation previewOperation) {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		ConnectorNode connectorNode = getConnectorNode();
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		TapAdvanceFilter tapAdvanceFilter;
		if (previewOperation instanceof PreviewReadOperation) {
			tapAdvanceFilter = ((PreviewReadOperation) previewOperation).getTapAdvanceFilter();
		} else if (previewOperation instanceof PreviewMergeReadOperation) {
			tapAdvanceFilter = ((PreviewMergeReadOperation) previewOperation).getTapAdvanceFilter();
		} else {
			tapAdvanceFilter = TapAdvanceFilter.create();
		}
		BatchReadFunction batchReadFunction = connectorFunctions.getBatchReadFunction();
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableNode.getTableName());
		if (null == tapTable) {
			throw new TapCodeException(TaskPreviewExCode_37.NOT_FOUND_SOURCE_TAP_TABLE, tableNode.getTableName());
		}
		AtomicReference<List<TapInsertRecordEvent>> data = new AtomicReference<>(new ArrayList<>());
		String method = "";
		long startMs = System.currentTimeMillis();
		if (null != queryByAdvanceFilterFunction) {
			try {
				queryByAdvanceFilterFunction.query(
						connectorNode.getConnectorContext(),
						tapAdvanceFilter,
						tapTable,
						result -> {
							if (null != result.getError()) {
								throw new TapCodeException(TaskPreviewExCode_37.MERGE_QUERY_ADVANCE_FILTER_ERROR, result.getError());
							}
							List<Map<String, Object>> results = result.getResults();
							for (Map<String, Object> datum : results) {
								TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().after(datum)
										.table(tableNode.getTableName());
								data.get().add(tapInsertRecordEvent);
							}
						}
				);
				method = PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER.name();
			} catch (Throwable e) {
				throw new TapCodeException(TaskPreviewExCode_37.MERGE_QUERY_ADVANCE_FILTER_ERROR, e);
			}
		} else if (null != batchReadFunction) {
			try {
				batchReadFunction.batchRead(
						connectorNode.getConnectorContext(),
						tapTable,
						null,
						taskDto.getPreviewRows(),
						(events, offset) -> {
							for (TapEvent event : events) {
								if (event instanceof TapInsertRecordEvent) {
									TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) event;
									Map<String, Object> after = insertRecordEvent.getAfter();
									if (null != after) {
										data.get().add(insertRecordEvent);
									}
								}
							}
							throw new StopBatchReadException();
						}
				);
			} catch (StopBatchReadException e) {
				// ignore
			} catch (Throwable e) {
				Throwable stopBatchReadException = CommonUtils.matchThrowable(e, StopBatchReadException.class);
				if (null == stopBatchReadException) {
					throw new TapCodeException(TaskPreviewExCode_37.MERGE_BATCH_READ_ERROR, e);
				}
			}
			method = PDKMethod.SOURCE_BATCH_READ.name();
		}
		long endMs = System.currentTimeMillis();
		if (StringUtils.isNotBlank(method)) {
			taskPreviewInstance.getTaskPreviewResultVO().getStats().getReadStats().add(new TaskPreviewReadStatsVO(
					tableNode.getTableName(),
					endMs - startMs,
					tapAdvanceFilter.getLimit(),
					tapAdvanceFilter.getMatch(),
					method,
					data.get().size()
			));
		}
		return data.get();
	}

	private List<TapdataEvent> offerPendingTapdataEvents = new ArrayList<>();

	@Override
	public boolean complete() {
		if (!isRunning()) {
			return true;
		}
		try {
			Node<?> node = getNode();
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			List<TapdataEvent> tapdataEvents = null;
			if (CollectionUtils.isNotEmpty(offerPendingTapdataEvents)) {
				tapdataEvents = offerPendingTapdataEvents;
				offerPendingTapdataEvents.clear();
			} else {
				PreviewReadOperationQueue previewReadOperationQueue = taskPreviewInstance.getPreviewReadOperationQueue();
				PreviewOperation previewOperation = previewReadOperationQueue.poll(node.getId());
				if (null != previewOperation) {
					Object handleResult = previewOperationHandlers.handle(previewOperation);
					mockIfNeed(handleResult, previewOperation);
					try {
						replyPreviewOperationData(handleResult, previewOperation);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return false;
					}
					tapdataEvents = wrapTapdataEvents(handleResult, previewOperation);
				}
			}

			if (CollectionUtils.isNotEmpty(tapdataEvents)) {
				for (int i = 0; i < tapdataEvents.size(); i++) {
					boolean offered = offer(tapdataEvents.get(i));
					if (!offered) {
						offerPendingTapdataEvents = new ArrayList<>(tapdataEvents.subList(i, tapdataEvents.size()));
						return false;
					}
				}
			}

			if (CollectionUtils.isEmpty(offerPendingTapdataEvents)) {
				Map<String, Object> taskGlobalVariablePreview = TaskGlobalVariable.INSTANCE
						.getTaskGlobalVariable(TaskPreviewService.taskPreviewInstanceId(taskDto));
				Object previewComplete = taskGlobalVariablePreview.get(TaskGlobalVariable.PREVIEW_COMPLETE_KEY);
				if (Boolean.TRUE.equals(previewComplete)) {
					this.running.set(false);
				}
			}
		} catch (Exception e) {
			errorHandle(e);
		}
		return false;
	}

	protected void mockIfNeed(Object handleResult, PreviewOperation previewOperation) {
		if (handleResult instanceof List && ((List<TapInsertRecordEvent>) handleResult).isEmpty()) {
			long startMs = System.currentTimeMillis();
			DataMap match = null;
			int limit = 0;
			if (previewOperation instanceof PreviewMergeReadOperation) {
				match = ((PreviewMergeReadOperation) previewOperation).getTapAdvanceFilter().getMatch();
				limit = ((PreviewMergeReadOperation) previewOperation).getTapAdvanceFilter().getLimit();
			} else if (previewOperation instanceof PreviewReadOperation) {
				match = ((PreviewReadOperation) previewOperation).getTapAdvanceFilter().getMatch();
				limit = ((PreviewReadOperation) previewOperation).getTapAdvanceFilter().getLimit();
			}
			TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableNode.getTableName());
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			if (MapUtils.isNotEmpty(nameFieldMap)) {
				Map<String, Object> mockData = new HashMap<>();
				for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
					String fieldName = entry.getKey();
					TapField tapField = entry.getValue();
					mockData.put(fieldName, mockValue(fieldName, tapField, match));
				}
				TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create()
						.after(mockData);
				((List<TapInsertRecordEvent>) handleResult).add(tapInsertRecordEvent);
			}
			long endMs = System.currentTimeMillis();
			taskPreviewInstance.getTaskPreviewResultVO().getStats().getReadStats().add(new TaskPreviewReadStatsVO(
					tableNode.getTableName(),
					endMs - startMs,
					limit,
					match,
					MOCK_METHOD,
					1
			));
		}
	}

	protected Object mockValue(String fieldName, TapField tapField, DataMap match) {
		Object value = null;
		if (null != match) {
			value = match.get(fieldName);
		}
		if (null == value) {
			value = tapField.getDefaultValue();
		}
		if (null == value) {
			TapType tapType = tapField.getTapType();
			if (tapType instanceof TapNumber) {
				value = RandomUtils.nextDouble(0, 100);
			} else if (tapType instanceof TapDateTime) {
				value = Instant.now();
			} else {
				value = RandomStringUtils.randomAlphabetic(10);
			}
		}
		return value;
	}

	protected void replyPreviewOperationData(Object handleResult, PreviewOperation previewOperation) throws InterruptedException {
		if (handleResult instanceof List && previewOperation instanceof PreviewMergeReadOperation) {
			List<TapInsertRecordEvent> events = (List<TapInsertRecordEvent>) handleResult;
			List<Map<String, Object>> afterList = events.stream().map(TapInsertRecordEvent::getAfter).collect(Collectors.toList());
			MergeReadData mergeReadData = new MergeReadData(afterList);
			((PreviewMergeReadOperation) previewOperation).replyData(mergeReadData);
		}
	}

	protected List<TapdataEvent> wrapTapdataEvents(Object handleResult, PreviewOperation previewOperation) {
		List<TapdataEvent> tapdataEvents = new ArrayList<>();
		if (handleResult instanceof List) {
			List<TapInsertRecordEvent> events = (List<TapInsertRecordEvent>) handleResult;
			boolean isLast = false;
			for (int i = 0; i < events.size(); i++) {
				if (i == events.size() - 1) {
					isLast = true;
				}
				TapdataEvent tapdataEvent = new TapdataEvent();
				tapdataEvent.setTapEvent(events.get(i));
				if (isLast) {
					tapdataEvent.addInfo(PreviewOperation.class.getSimpleName(), previewOperation);
				}
				Map<String, Object> after = TapEventUtil.getAfter(tapdataEvent.getTapEvent());
				if (null != after) {
					fromTapValue(after, defaultCodecsFilterManager, TapEventUtil.getTableId(tapdataEvent.getTapEvent()));
				}
				tapdataEvents.add(tapdataEvent);
			}
		} else if (handleResult instanceof PreviewFinishReadOperation) {
			TapdataPreviewCompleteEvent tapdataPreviewCompleteEvent = new TapdataPreviewCompleteEvent();
			// Mock a HeartbeatEvent to prevent it from being skipped when merging nodes are processed
			tapdataPreviewCompleteEvent.setTapEvent(new HeartbeatEvent());
			tapdataEvents.add(tapdataPreviewCompleteEvent);
		}
		return tapdataEvents;
	}

	@Override
	public void doClose() throws TapCodeException {
		super.doClose();
	}

	@Override
	protected void createPdkConnectorNode(DataProcessorContext dataProcessorContext, HazelcastInstance hazelcastInstance) {
		super.createPdkConnectorNode(dataProcessorContext, hazelcastInstance);
	}

	@Override
	protected void connectorNodeInit(DataProcessorContext dataProcessorContext) {
		super.connectorNodeInit(dataProcessorContext);
	}

	@Override
	protected void initTapLogger() {
		super.initTapLogger();
	}

	@Override
	protected boolean isRunning() {
		return super.isRunning();
	}

	@Override
	protected boolean offer(TapdataEvent dataEvent) {
		return super.offer(dataEvent);
	}
}
