package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.ringbuffer.Ringbuffer;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import io.tapdata.aspect.WriteRecordFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.construct.HazelcastConstruct;
import io.tapdata.construct.constructImpl.ConstructRingBuffer;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.WriteListResult;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkShareCDCNode.class);
	private LRUMap constructMap;
	private List<String> tableNames;
	private Map<String, List<Document>> batchCacheData;
	private LinkedBlockingQueue<ShareCdcTableMetricsDto> tableMetricsQueue = new LinkedBlockingQueue<>(1024);
	private ExecutorService flushShareCdcTableMetricsThreadPool;
	private List<ShareCdcTableMetricsDto> cacheMetricsList = new ArrayList<>();
	private final AtomicLong lastFlushMetricsTimeMs = new AtomicLong();
	private Map<String, ShareCdcTableMetricsDto> shareCdcTableMetricsDtoMap;

	public HazelcastTargetPdkShareCDCNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		this.targetBatch = 10000;
		this.targetBatchIntervalMs = 10000L;
		Integer shareCdcTtlDay = getShareCdcTtlDay();
		externalStorageDto.setTtlDay(shareCdcTtlDay);
		this.constructMap = new LRUMap();
		LogContent startTimeSign = LogContent.createStartTimeSign();
		Document document = MapUtil.obj2Document(startTimeSign);
		for (String tableName : tableNames) {
			HazelcastConstruct<Document> construct = getConstruct(tableName);
			if (construct.isEmpty()) {
				construct.insert(document);
			}
		}
		this.batchCacheData = new HashMap<>();
		this.flushShareCdcTableMetricsThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>(),
				r -> new Thread(r, "Flush-Share-Cdc-Table-Metrics-Consumer-"
						+ dataProcessorContext.getTaskDto().getId().toHexString() + "-" + getNode().getId()));
		this.flushShareCdcTableMetricsThreadPool.submit(this::consumeAndFlushTableMetrics);
		this.shareCdcTableMetricsDtoMap = new ConcurrentHashMap<>();
		obsLogger.info("Init log data storage finished, config: " + externalStorageDto);
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
			Node<?> firstPreNode = predecessors.get(0);
			shareCdcTtlDay = ((LogCollectorNode) firstPreNode).getStorageTime();
			tableNames = ((LogCollectorNode) firstPreNode).getTableNames();
		}
		if (null == shareCdcTtlDay || shareCdcTtlDay.compareTo(0) <= 0) {
			shareCdcTtlDay = DEFAULT_SHARE_CDC_TTL_DAY;
		}
		return shareCdcTtlDay;
	}

	private HazelcastConstruct<Document> getConstruct(String tableName) {
		if (!constructMap.containsKey(tableName)) {
			HazelcastConstruct<Document> construct = new ConstructRingBuffer<>(
					jetContext.hazelcastInstance(),
					ShareCdcUtil.getConstructName(processorBaseContext.getTaskDto(), tableName),
					externalStorageDto
			);
			constructMap.put(tableName, construct);
		}
		return (HazelcastConstruct<Document>) constructMap.get(tableName);
	}

	@Override
	void processEvents(List<TapEvent> tapEvents) {
		throw new UnsupportedOperationException();
	}

	@Override
	@SneakyThrows
	void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		if (CollectionUtils.isEmpty(tapdataShareLogEvents)) return;

		List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
		for (TapdataShareLogEvent tapdataShareLogEvent : tapdataShareLogEvents) {
			TapEvent tapEvent = tapdataShareLogEvent.getTapEvent();
			if (!(tapEvent instanceof TapRecordEvent)) {
				throw new RuntimeException("Share cdc target expected " + TapRecordEvent.class.getName() + ", actual: " + tapEvent.getClass().getName());
			}
			tapRecordEvents.add((TapRecordEvent) tapEvent);
		}

		WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>();

		executeDataFuncAspect(
				WriteRecordFuncAspect.class,
				() -> new WriteRecordFuncAspect()
						.recordEvents(tapRecordEvents)
						.table(new TapTable(externalStorageDto.getName()))
						.dataProcessorContext(dataProcessorContext)
						.start(),
				writeRecordFuncAspect -> {
					for (TapdataShareLogEvent tapdataShareLogEvent : tapdataShareLogEvents) {
						TapEvent tapEvent = tapdataShareLogEvent.getTapEvent();
						String tableId = TapEventUtil.getTableId(tapEvent);
						String op = TapEventUtil.getOp(tapEvent);
						Long timestamp = TapEventUtil.getTimestamp(tapEvent);
						Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
						handleData(before);
						Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
						handleData(after);
						Object streamOffset = tapdataShareLogEvent.getStreamOffset();
						String offsetStr = "";
						if (null != streamOffset) {
							offsetStr = PdkUtil.encodeOffset(streamOffset);
						}
						verify(tableId, op, before, after, timestamp, offsetStr);
						LogContent logContent = new LogContent(
								tableId,
								timestamp,
								before,
								after,
								op,
								offsetStr
						);
						Document document;
						try {
							document = MapUtil.obj2Document(logContent);
						} catch (Exception e) {
							throw new RuntimeException("Convert map to document failed; Map data: " + logContent + ". Error: " + e.getMessage(), e);
						}
						if (!batchCacheData.containsKey(tableId)) {
							batchCacheData.put(tableId, new ArrayList<>());
						}
						batchCacheData.get(tableId).add(document);
						if (batchCacheData.get(tableId).size() >= INSERT_BATCH_SIZE) {
							insertMany(tableId);
							writeListResult.incrementInserted(batchCacheData.get(tableId).size());
							batchCacheData.get(tableId).clear();
						}
						incrementTableMetrics(tapdataShareLogEvent);
					}
					for (Map.Entry<String, List<Document>> entry : batchCacheData.entrySet()) {
						String tableName = entry.getKey();
						List<Document> list = entry.getValue();
						if (CollectionUtils.isNotEmpty(list)) {
							insertMany(tableName);
							writeListResult.incrementInserted(list.size());
						}
					}
					AspectUtils.accept(writeRecordFuncAspect.state(WriteRecordFuncAspect.STATE_WRITING).getConsumers(), tapRecordEvents, writeListResult);
				}
		);
		metricsEnqueue();
		batchCacheData.clear();
	}

	private void incrementTableMetrics(TapdataShareLogEvent tapdataShareLogEvent) {
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
			throw new RuntimeException("Insert many documents into ringbuffer failed. Table: " + tableId + " Size: " + batchCacheData.get(tableId).size(), e);
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

	private void verify(String tableId, String op, Map<String, Object> before, Map<String, Object> after, Long timestamp, String offsetStr) {
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

	@Override
	public void doClose() throws Exception {
		if (null != flushShareCdcTableMetricsThreadPool) {
			flushShareCdcTableMetricsThreadPool.shutdownNow();
		}
		if (CollectionUtils.isNotEmpty(cacheMetricsList)) {
			flushShareCdcTableMetrics(cacheMetricsList);
			cacheMetricsList.clear();
		}
		if (!tableMetricsQueue.isEmpty()) {
			cacheMetricsList.addAll(tableMetricsQueue);
			flushShareCdcTableMetrics(cacheMetricsList);
			cacheMetricsList.clear();
			tableMetricsQueue.clear();
			tableMetricsQueue = null;
			cacheMetricsList = null;
		}
		super.doClose();
	}
}
