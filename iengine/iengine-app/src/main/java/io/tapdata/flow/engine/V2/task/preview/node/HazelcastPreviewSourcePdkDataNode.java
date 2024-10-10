package io.tapdata.flow.engine.V2.task.preview.node;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataPreviewCompleteEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.TapCodecsRegistry;
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
import io.tapdata.flow.engine.V2.task.preview.PreviewReadOperationQueue;
import io.tapdata.flow.engine.V2.task.preview.StopBatchReadException;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewExCode_37;
import io.tapdata.flow.engine.V2.task.preview.operation.*;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
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
		super.doInit(context);
		TapCodecsRegistry codecsRegistry = this.defaultCodecsFilterManager.getCodecsRegistry();
		codecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> tapValue.getValue().toInstant().toString());
		codecsRegistry.registerFromTapValue(TapDateValue.class, tapValue -> tapValue.getValue().toInstant().toString());
		codecsRegistry.registerFromTapValue(TapTimeValue.class, tapValue -> tapValue.getValue().toTimeStr());
		codecsRegistry.registerFromTapValue(TapYearValue.class, tapValue -> tapValue.getValue().toLocalDateTime().getYear());
		codecsRegistry.registerFromTapValue(TapNumberValue.class, tapValue -> {
			Double value = tapValue.getValue();
			// Determine if the decimal place of value is 0, convert it to Long, otherwise keep Double
			if (null != value && value % 1 == 0) {
				return value.longValue();
			} else {
				return value;
			}
		});
	}

	private PreviewFinishReadOperation finishRead(PreviewFinishReadOperation previewFinishReadOperation) {
		finishPreviewRead.set(true);
		if (previewFinishReadOperation.isLast()) {
			return previewFinishReadOperation;
		}
		return null;
	}

	private List<TapInsertRecordEvent> read(PreviewOperation previewOperation) {
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
		}
		return data.get();
	}

	@Override
	public void startSourceRunner() {
		Node<?> node = getNode();
		PreviewReadOperationQueue previewReadOperationQueue = taskPreviewInstance.getPreviewReadOperationQueue();
		while (isRunning() && !finishPreviewRead.get()) {
			PreviewOperation previewOperation;
			try {
				previewOperation = previewReadOperationQueue.take(node.getId());
			} catch (InterruptedException e) {
				break;
			}
			if (null == previewOperation) {
				continue;
			}
			Object handleResult = previewOperationHandlers.handle(previewOperation);
			mockIfNeed(handleResult, previewOperation);
			try {
				replyPreviewOperationData(handleResult, previewOperation);
			} catch (InterruptedException e) {
				break;
			}
			List<TapdataEvent> tapdataEvents = wrapTapdataEvents(handleResult, previewOperation);
			if (CollectionUtils.isNotEmpty(tapdataEvents)) {
				tapdataEvents.forEach(this::enqueue);
			}
		}
	}

	protected void mockIfNeed(Object handleResult, PreviewOperation previewOperation) {
		if (handleResult instanceof List && ((List<TapInsertRecordEvent>) handleResult).isEmpty()) {
			DataMap match = null;
			if (previewOperation instanceof PreviewMergeReadOperation) {
				match = ((PreviewMergeReadOperation) previewOperation).getTapAdvanceFilter().getMatch();
			} else if (previewOperation instanceof PreviewReadOperation) {
				match = ((PreviewReadOperation) previewOperation).getTapAdvanceFilter().getMatch();
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
}
