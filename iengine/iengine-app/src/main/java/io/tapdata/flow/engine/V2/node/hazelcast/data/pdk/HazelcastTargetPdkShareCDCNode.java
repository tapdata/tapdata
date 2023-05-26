package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.ringbuffer.Ringbuffer;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.dataflow.aggregation.PersistentLRUMap;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.construct.HazelcastConstruct;
import io.tapdata.construct.constructImpl.ConstructRingBuffer;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.error.TaskTargetShareCDCProcessorExCode_19;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2022-06-14 17:23
 **/
public class HazelcastTargetPdkShareCDCNode extends HazelcastTargetPdkBaseNode {

	public static final int DEFAULT_SHARE_CDC_TTL_DAY = 3;
	private static final int INSERT_BATCH_SIZE = 1000;
	private static final long MIN_FLUSH_METRICS_INTERVAL_MS = 5000L;
	private static final int FLUSH_METRICS_BATCH_SIZE = 10;
	public static final String TAG = HazelcastTargetPdkShareCDCNode.class.getSimpleName();
	private final static ObjectSerializable OBJECT_SERIALIZABLE = InstanceFactory.instance(ObjectSerializable.class);
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkShareCDCNode.class);
	private final PersistentLRUMap constructMap = new PersistentLRUMap(100, entry -> {
		if (entry instanceof ConstructRingBuffer) {
			try {
				((ConstructRingBuffer<?>) entry).destroy();
			} catch (Exception e) {
				logger.warn("Destroy construct ring buffer failed: {}", e.getMessage());
			}
		}
	});
	private final AtomicReference<String> constructReferenceId = new AtomicReference<>();
	private List<String> tableNames;
	private Map<String, List<Document>> batchCacheData;
	private LinkedBlockingQueue<ShareCdcTableMetricsDto> tableMetricsQueue = new LinkedBlockingQueue<>(1024);
	private ExecutorService flushShareCdcTableMetricsThreadPool;
	private List<ShareCdcTableMetricsDto> cacheMetricsList = new ArrayList<>();
	private final AtomicLong lastFlushMetricsTimeMs = new AtomicLong();
	private Map<String, ShareCdcTableMetricsDto> shareCdcTableMetricsDtoMap;
	private ClassHandlers ddlEventHandlers;
	private final AtomicReference<LogContent> ddlLogContent = new AtomicReference<>();

	public HazelcastTargetPdkShareCDCNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		this.targetBatch = 10000;
		this.targetBatchIntervalMs = 1000;
		this.constructReferenceId.set(String.format("%s-%s-%s", getClass().getSimpleName(), getNode().getTaskId(), getNode().getId()));
		Integer shareCdcTtlDay = getShareCdcTtlDay();
		externalStorageDto.setTtlDay(shareCdcTtlDay);
		LogContent startTimeSign = LogContent.createStartTimeSign();
		Document document = MapUtil.obj2Document(startTimeSign);
		for (String tableName : tableNames) {
			HazelcastConstruct<Document> construct = getConstruct(tableName);
			if (construct.isEmpty()) {
				construct.insert(document);
			}
		}
		this.batchCacheData = new LinkedHashMap<>();
		this.flushShareCdcTableMetricsThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>(),
				r -> new Thread(r, "Flush-Share-Cdc-Table-Metrics-Consumer-"
						+ dataProcessorContext.getTaskDto().getId().toHexString() + "-" + getNode().getId()));
		this.flushShareCdcTableMetricsThreadPool.submit(this::consumeAndFlushTableMetrics);
		this.shareCdcTableMetricsDtoMap = new ConcurrentHashMap<>();
		obsLogger.info("Init log data storage finished, config: " + externalStorageDto);

		ddlEventHandlers = new ClassHandlers();
		ddlEventHandlers.register(TapNewFieldEvent.class, this::writeNewFieldFunction);
		ddlEventHandlers.register(TapAlterFieldNameEvent.class, this::writeAlterFieldNameFunction);
		ddlEventHandlers.register(TapAlterFieldAttributesEvent.class, this::writeAlterFieldAttrFunction);
		ddlEventHandlers.register(TapDropFieldEvent.class, this::writeDropFieldFunction);

		initCodecs();
	}

	private void initCodecs() {
		if (null != codecsFilterManager) {
			TapCodecsRegistry codecsRegistry = codecsFilterManager.getCodecsRegistry();
			codecsRegistry.registerFromTapValue(TapStringValue.class, tapValue -> {
				if (null == tapValue) {
					return null;
				}
				if (tapValue.getOriginType().equals("OBJECT_ID") && tapValue.getOriginValue().getClass().getName().equals(ObjectId.class.getName())) {
					return new ObjectId(tapValue.getValue());
				}
				return tapValue.getValue();
			});
		}
	}

	@Override
	void processEvents(List<TapEvent> tapEvents) {
		throw new UnsupportedOperationException();
	}

	@Override
	void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		try {
			if (CollectionUtils.isEmpty(tapdataShareLogEvents)) return;

			List<TapdataShareLogEvent> cacheTapdataEvents = new ArrayList<>();
			TapEvent lastTapEvent = null;
			for (TapdataShareLogEvent tapdataShareLogEvent : tapdataShareLogEvents) {
				// Dispatch dml and ddl events
				TapEvent tapEvent = tapdataShareLogEvent.getTapEvent();
				if (!tapdataShareLogEvent.isDDL() && !tapdataShareLogEvent.isDML()) {
					continue;
				}
				if (null == lastTapEvent) {
					lastTapEvent = tapEvent;
				}
				if (!(lastTapEvent.getClass().isInstance(tapEvent))) {
					handleTapEvents(cacheTapdataEvents);
				}
				cacheTapdataEvents.add(tapdataShareLogEvent);
				lastTapEvent = tapEvent;
			}

			handleTapEvents(cacheTapdataEvents);
			incrementTableMetrics(tapdataShareLogEvents);
			metricsEnqueue();
		} catch (Exception e) {
			if (!(e instanceof TapCodeException)) {
				throw new TapCodeException(TaskTargetShareCDCProcessorExCode_19.UNKNOWN_ERROR, e);
			} else {
				throw e;
			}
		}
	}

	private void handleTapEvents(List<TapdataShareLogEvent> cacheTapdataEvents) {
		if (CollectionUtils.isEmpty(cacheTapdataEvents)) {
			return;
		}
		TapdataShareLogEvent firstTapdataEvent = cacheTapdataEvents.get(0);
		if (firstTapdataEvent.isDML()) {
			handleTapRecordEvents(cacheTapdataEvents);
		} else if (firstTapdataEvent.isDDL()) {
			handleTapDDLEvents(cacheTapdataEvents);
		}
		cacheTapdataEvents.clear();
	}

	private void handleTapRecordEvents(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		AtomicReference<WriteListResult<TapRecordEvent>> writeListResult = new AtomicReference<>();
		List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
		List<LogContent> logContents = new ArrayList<>();
		for (TapdataShareLogEvent shareLogEvent : tapdataShareLogEvents) {
			LogContent logContent = wrapLogContent(shareLogEvent);
			if (null == logContent) {
				continue;
			}
			logContents.add(logContent);
			TapEvent event = shareLogEvent.getTapEvent();
			tapRecordEvents.add((TapRecordEvent) event);
		}
		executeDataFuncAspect(
				WriteRecordFuncAspect.class,
				() -> new WriteRecordFuncAspect()
						.recordEvents(tapRecordEvents)
						.table(new TapTable(externalStorageDto.getName()))
						.dataProcessorContext(dataProcessorContext)
						.start(),
				writeRecordFuncAspect -> {
					writeListResult.set(writeLogContents(logContents));
					AspectUtils.accept(writeRecordFuncAspect.state(WriteRecordFuncAspect.STATE_WRITING).getConsumers(), tapRecordEvents, writeListResult.get());
				}
		);
	}

	private void handleTapDDLEvents(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		for (TapdataShareLogEvent tapdataShareLogEvent : tapdataShareLogEvents) {
			LogContent logContent = wrapLogContent(tapdataShareLogEvent);
			if (null == logContent) {
				continue;
			}
			ddlLogContent.set(logContent);
			TapEvent tapEvent = tapdataShareLogEvent.getTapEvent();
			ddlEventHandlers.handle(tapEvent);
		}
	}

	private Void writeDropFieldFunction(TapDropFieldEvent tapDropFieldEvent) {
		AspectUtils.executeAspect(new DropFieldFuncAspect()
				.dropFieldEvent(tapDropFieldEvent)
				.dataProcessorContext(dataProcessorContext)
				.state(NewFieldFuncAspect.STATE_START));
		executeDataFuncAspect(
				DropFieldFuncAspect.class,
				() -> new DropFieldFuncAspect()
						.dropFieldEvent(tapDropFieldEvent)
						.dataProcessorContext(dataProcessorContext)
						.start(),
				dropFieldFuncAspect -> writeLogContent(ddlLogContent.get())
		);
		return null;
	}

	private Void writeAlterFieldAttrFunction(TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
		AspectUtils.executeAspect(new AlterFieldAttributesFuncAspect()
				.alterFieldAttributesEvent(tapAlterFieldAttributesEvent)
				.dataProcessorContext(dataProcessorContext)
				.state(AlterFieldAttributesFuncAspect.STATE_START));
		executeDataFuncAspect(
				AlterFieldAttributesFuncAspect.class,
				() -> new AlterFieldAttributesFuncAspect()
						.alterFieldAttributesEvent(tapAlterFieldAttributesEvent)
						.dataProcessorContext(dataProcessorContext)
						.start(),
				alterFieldAttributesFuncAspect -> writeLogContent(ddlLogContent.get())
		);
		return null;
	}

	private Void writeAlterFieldNameFunction(TapAlterFieldNameEvent tapAlterFieldNameEvent) {
		AspectUtils.executeAspect(new AlterFieldNameFuncAspect()
				.alterFieldNameEvent(tapAlterFieldNameEvent)
				.dataProcessorContext(dataProcessorContext)
				.state(AlterFieldNameFuncAspect.STATE_START));
		executeDataFuncAspect(
				AlterFieldNameFuncAspect.class,
				() -> new AlterFieldNameFuncAspect()
						.alterFieldNameEvent(tapAlterFieldNameEvent)
						.dataProcessorContext(dataProcessorContext)
						.state(AlterFieldNameFuncAspect.STATE_START)
						.start(),
				alterFieldNameFuncAspect -> writeLogContent(ddlLogContent.get())
		);
		return null;
	}

	private Void writeNewFieldFunction(TapNewFieldEvent tapNewFieldEvent) {
		AspectUtils.executeAspect(new NewFieldFuncAspect()
				.newFieldEvent(tapNewFieldEvent)
				.dataProcessorContext(dataProcessorContext)
				.state(NewFieldFuncAspect.STATE_START));
		executeDataFuncAspect(
				NewFieldFuncAspect.class,
				() -> new NewFieldFuncAspect()
						.newFieldEvent(tapNewFieldEvent)
						.dataProcessorContext(dataProcessorContext)
						.state(NewFieldFuncAspect.STATE_START)
						.start(),
				newFieldFuncAspect -> writeLogContent(ddlLogContent.get())
		);
		return null;
	}

	private void writeLogContent(LogContent logContent) {
		if (null == logContent) {
			return;
		}
		String tableId = logContent.getFromTable();
		Document document = logContent2Document(logContent);
		HazelcastConstruct<Document> construct = getConstruct(tableId);
		try {
			construct.insert(document);
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.WRITE_ONE_SHARE_LOG_FAILED, "Write document failed: %s", e);
		}
	}

	private <E extends TapEvent> WriteListResult<E> writeLogContents(List<LogContent> logContents) {
		WriteListResult<E> writeListResult = new WriteListResult<>();
		for (LogContent logContent : logContents) {
			String tableId = ShareCdcUtil.getTableId(logContent);
			Document document = logContent2Document(logContent);
			if (!batchCacheData.containsKey(tableId)) {
				batchCacheData.put(tableId, new ArrayList<>());
			}
			batchCacheData.get(tableId).add(document);
			if (batchCacheData.get(tableId).size() >= INSERT_BATCH_SIZE) {
				insertMany(tableId);
				writeListResult.incrementInserted(batchCacheData.get(tableId).size());
				batchCacheData.get(tableId).clear();
			}
		}
		for (Map.Entry<String, List<Document>> entry : batchCacheData.entrySet()) {
			String tableId = entry.getKey();
			List<Document> list = entry.getValue();
			if (CollectionUtils.isNotEmpty(list)) {
				insertMany(tableId);
				writeListResult.incrementInserted(batchCacheData.get(tableId).size());
			}
		}
		batchCacheData.clear();
		return writeListResult;
	}

	@NotNull
	private static Document logContent2Document(LogContent logContent) {
		Document document;
		try {
			document = MapUtil.obj2Document(logContent);
		} catch (Exception e) {
			throw new TapCodeException(TaskTargetShareCDCProcessorExCode_19.CONVERT_LOG_CONTENT_TO_DOCUMENT_FAILED, String.format("Data: %s", logContent), e);
		}
		return document;
	}

	private LogContent wrapLogContent(TapdataShareLogEvent tapdataShareLogEvent) {
		if (null == tapdataShareLogEvent) {
			return null;
		}
		TapEvent tapEvent = tapdataShareLogEvent.getTapEvent();
		String tableId = TapEventUtil.getTableId(tapEvent);
		Long timestamp = TapEventUtil.getTimestamp(tapEvent);
		Object streamOffset = tapdataShareLogEvent.getStreamOffset();
		String offsetStr = "";
		if (null != streamOffset) {
			offsetStr = PdkUtil.encodeOffset(streamOffset);
		}
		LogContent logContent = null;
		String op = TapEventUtil.getOp(tapEvent);
		if (tapdataShareLogEvent.isDML()) {
			Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
			handleData(before);
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			handleData(after);
			verifyDML(tapEvent, tableId, op, timestamp, before, after, offsetStr);
			logContent = LogContent.createDMLLogContent(
					tableId,
					timestamp,
					before,
					after,
					op,
					offsetStr
			);
			logContent.setTableNamespaces(TapEventUtil.getNamespaces(tapEvent));
		} else if (tapdataShareLogEvent.isDDL()) {
			verifyDDL(tapEvent, tableId, op, timestamp, offsetStr);
			byte[] bytes = OBJECT_SERIALIZABLE.fromObject(tapEvent);
			logContent = LogContent.createDDLLogContent(
					tableId,
					timestamp,
					op,
					offsetStr,
					bytes
			);
			logContent.setTableNamespaces(TapEventUtil.getNamespaces(tapEvent));
		}
		return logContent;
	}

	private void consumeAndFlushTableMetrics() {
		while (isRunning()) {
			ShareCdcTableMetricsDto shareCdcTableMetricsDto;
			try {
				shareCdcTableMetricsDto = tableMetricsQueue.poll(1L, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				break;
			}
			if (null != shareCdcTableMetricsDto) {
				cacheMetricsList.add(shareCdcTableMetricsDto);
			}
			if (System.currentTimeMillis() - lastFlushMetricsTimeMs.get() > MIN_FLUSH_METRICS_INTERVAL_MS
					|| cacheMetricsList.size() >= FLUSH_METRICS_BATCH_SIZE) {
				flushShareCdcTableMetrics(cacheMetricsList);
				cacheMetricsList.clear();
				lastFlushMetricsTimeMs.set(System.currentTimeMillis());
			}
		}
	}

	private void flushShareCdcTableMetrics(List<ShareCdcTableMetricsDto> shareCdcTableMetricsDtoList) {
		if (CollectionUtils.isEmpty(shareCdcTableMetricsDtoList)) {
			return;
		}
		clientMongoOperator.insertMany(shareCdcTableMetricsDtoList, ConnectorConstant.SHARE_CDC_TABLE_METRICS_COLLECTION + "/saveOrUpdateDaily",
				unused -> !isRunning());
	}

	@NotNull
	private Integer getShareCdcTtlDay() {
		Integer shareCdcTtlDay = null;
		List<Node<?>> predecessors = GraphUtil.predecessors(processorBaseContext.getNode(), n -> n instanceof LogCollectorNode);
		if (CollectionUtils.isNotEmpty(predecessors)) {
			LogCollectorNode logCollectorNode = (LogCollectorNode) predecessors.get(0);
			shareCdcTtlDay = logCollectorNode.getStorageTime();
			tableNames = ShareCdcUtil.getTableNames(logCollectorNode);
		}
		if (null == shareCdcTtlDay || shareCdcTtlDay.compareTo(0) <= 0) {
			shareCdcTtlDay = DEFAULT_SHARE_CDC_TTL_DAY;
		}
		return shareCdcTtlDay;
	}

	private HazelcastConstruct<Document> getConstruct(String tableName) {
		Object construct = constructMap.get(tableName);
		if (null == construct) {
			synchronized (constructMap) {
				construct = constructMap.computeIfAbsent(tableName, k -> new ConstructRingBuffer<>(
					jetContext.hazelcastInstance(),
					constructReferenceId.get(),
					ShareCdcUtil.getConstructName(processorBaseContext.getTaskDto(), tableName),
					externalStorageDto
				));
			}
		}
		return (HazelcastConstruct<Document>) construct;
	}

	private void incrementTableMetrics(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		for (TapdataShareLogEvent tapdataShareLogEvent : tapdataShareLogEvents) {
			if (null == tapdataShareLogEvent) {
				continue;
			}
			if (!tapdataShareLogEvent.isDML() && !tapdataShareLogEvent.isDDL()) {
				continue;
			}
			String connectionId = "";
			String nodeId = "";
			Object connIdObj = tapdataShareLogEvent.getInfo(TapdataEvent.CONNECTION_ID_INFO_KEY);
			if (connIdObj instanceof String) {
				connectionId = (String) connIdObj;
			}
			List<String> nodeIds = tapdataShareLogEvent.getNodeIds();
			if (CollectionUtils.isNotEmpty(nodeIds)) {
				nodeId = nodeIds.get(0);
			}
			TapEvent tapEvent = tapdataShareLogEvent.getTapEvent();
			String tableId = TapEventUtil.getTableId(tapEvent);
			if (StringUtils.isBlank(connectionId)
					|| StringUtils.isBlank(nodeId)
					|| StringUtils.isBlank(tableId)) {
				return;
			}
			String key = getTableMetricsKey(connectionId, nodeId, tableId);
			ShareCdcTableMetricsDto shareCdcTableMetricsDto;
			if (!shareCdcTableMetricsDtoMap.containsKey(key)) {
				shareCdcTableMetricsDto = new ShareCdcTableMetricsDto();
				shareCdcTableMetricsDto.setTaskId(dataProcessorContext.getTaskDto().getId().toHexString());
				shareCdcTableMetricsDto.setConnectionId(connectionId);
				shareCdcTableMetricsDto.setNodeId(nodeId);
				shareCdcTableMetricsDto.setTableName(tableId);
				shareCdcTableMetricsDto.setCount(1L);
				shareCdcTableMetricsDtoMap.put(key, shareCdcTableMetricsDto);
			} else {
				shareCdcTableMetricsDto = shareCdcTableMetricsDtoMap.get(key);
				shareCdcTableMetricsDto.setCount(shareCdcTableMetricsDto.getCount() + 1L);
			}
			shareCdcTableMetricsDto.setFirstEventTime(tapdataShareLogEvent.getSourceTime());
			shareCdcTableMetricsDto.setCurrentEventTime(tapdataShareLogEvent.getSourceTime());
		}
	}

	private String getTableMetricsKey(String connectionId, String nodeId, String tableId) {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		String taskId = taskDto.getId().toHexString();
		return String.join("-", taskId, connectionId, nodeId, tableId);
	}

	private void metricsEnqueue() {
		if (MapUtils.isEmpty(shareCdcTableMetricsDtoMap)) {
			return;
		}
		Collection<ShareCdcTableMetricsDto> shareCdcTableMetricsDtoList = shareCdcTableMetricsDtoMap.values();
		for (ShareCdcTableMetricsDto shareCdcTableMetricsDto : shareCdcTableMetricsDtoList) {
			while (isRunning()) {
				try {
					if (tableMetricsQueue.offer(shareCdcTableMetricsDto, 1L, TimeUnit.SECONDS)) {
						break;
					}
				} catch (InterruptedException e) {
					break;
				}
			}
		}
		shareCdcTableMetricsDtoMap.clear();
	}

	private void insertMany(String tableId) {
		try {
			HazelcastConstruct<Document> construct = getConstruct(tableId);
			construct.insertMany(batchCacheData.get(tableId), unused -> !isRunning());
			if (logger.isDebugEnabled()) {
				Ringbuffer ringbuffer = ((ConstructRingBuffer) construct).getRingbuffer();
				logger.debug("Write ring buffer, head sequence: {}, tail sequence: {}, last data: {}", ringbuffer.headSequence(), ringbuffer.tailSequence(), ringbuffer.readOne(ringbuffer.tailSequence()));
			}
		} catch (Exception e) {
			throw new TapCodeException(TaskTargetShareCDCProcessorExCode_19.INSERT_MANY_INTO_RINGBUFFER_FAILED,
					String.format("Ring buffer name: %s, table: %s, size: %s", ShareCdcUtil.getConstructName(processorBaseContext.getTaskDto(), tableId), tableId, batchCacheData.get(tableId).size()), e);
		}
	}

	private void handleData(Map<String, Object> data) {
		if (MapUtils.isEmpty(data)) return;
		data.forEach((k, v) -> {
			if (null == v) {
				return;
			}
			String valueClassName = v.getClass().getName();
			if (valueClassName.equals("org.bson.types.ObjectId")) {
				byte[] bytes = v.toString().getBytes();
				byte[] dest = new byte[bytes.length + 2];
				dest[0] = 99;
				dest[dest.length - 1] = 23;
				System.arraycopy(bytes, 0, dest, 1, bytes.length);
				data.put(k, dest);
			}
		});
	}

	private void verifyDML(TapEvent tapEvent, String tableId, String op, Long timestamp, Map<String, Object> before, Map<String, Object> after, String offsetStr) {
		if (!(tapEvent instanceof TapRecordEvent)) {
			throw new RuntimeException(String.format("Expected %s, actual %s", TapRecordEvent.class.getSimpleName(), tapEvent.getClass().getSimpleName()));
		}
		if (StringUtils.isBlank(tableId)) {
			throw new RuntimeException("Missing table id");
		}
		if (StringUtils.isBlank(op)) {
			throw new RuntimeException("Missing operation type");
		}
		if (MapUtils.isEmpty(before) && MapUtils.isEmpty(after)) {
			throw new RuntimeException("Both before and after is empty");
		}
		if (null == timestamp || timestamp.compareTo(0L) <= 0) {
			obsLogger.warn("Invalid timestamp value: " + timestamp);
		}
		if (StringUtils.isBlank(offsetStr)) {
			obsLogger.warn("Invalid offset string: " + offsetStr);
		}
	}

	private void verifyDDL(TapEvent tapEvent, String tableId, String op, Long timestamp, String offsetStr) {
		if (!(tapEvent instanceof TapDDLEvent)) {
			throw new RuntimeException(String.format("Expected %s, actual %s", TapDDLEvent.class.getSimpleName(), tapEvent.getClass().getSimpleName()));
		}
		if (StringUtils.isBlank(tableId)) {
			throw new RuntimeException("Missing table id");
		}
		if (StringUtils.isBlank(op)) {
			throw new RuntimeException("Missing operation type");
		}
		if (null == timestamp || timestamp.compareTo(0L) <= 0) {
			obsLogger.warn("Invalid timestamp value: " + timestamp);
		}
		if (StringUtils.isBlank(offsetStr)) {
			obsLogger.warn("Invalid offset string: " + offsetStr);
		}
	}

	@Override
	public void doClose() throws Exception {
		if (null != flushShareCdcTableMetricsThreadPool) {
			flushShareCdcTableMetricsThreadPool.shutdownNow();
		}
		if (CollectionUtils.isNotEmpty(cacheMetricsList)) {
			CommonUtils.ignoreAnyError(() -> {
				flushShareCdcTableMetrics(cacheMetricsList);
				cacheMetricsList.clear();
			}, TAG);
		}
		if (!tableMetricsQueue.isEmpty()) {
			CommonUtils.ignoreAnyError(() -> {
				cacheMetricsList.addAll(tableMetricsQueue);
				flushShareCdcTableMetrics(cacheMetricsList);
				cacheMetricsList.clear();
				tableMetricsQueue.clear();
				tableMetricsQueue = null;
				cacheMetricsList = null;
			}, TAG);
		}
		constructMap.clear();
		super.doClose();
	}
}
