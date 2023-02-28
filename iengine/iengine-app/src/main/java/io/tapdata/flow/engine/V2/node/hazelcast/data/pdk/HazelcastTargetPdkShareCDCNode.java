package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import io.tapdata.HazelcastConstruct;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.constructImpl.ConstructRingBuffer;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-06-14 17:23
 **/
public class HazelcastTargetPdkShareCDCNode extends HazelcastTargetPdkBaseNode {
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkShareCDCNode.class);
	private LRUMap constructMap;
	private Integer shareCdcTtlDay;
	private List<String> tableNames;

	public HazelcastTargetPdkShareCDCNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		List<Node<?>> predecessors = GraphUtil.predecessors(processorBaseContext.getNode(), n -> n instanceof LogCollectorNode);
		if (CollectionUtils.isNotEmpty(predecessors)) {
			Node<?> firstPreNode = predecessors.get(0);
			shareCdcTtlDay = ((LogCollectorNode) firstPreNode).getStorageTime();
			tableNames = ((LogCollectorNode) firstPreNode).getTableNames();
		} else {
			throw new RuntimeException("Cannot found predecessor log collector node");
		}
		this.constructMap = new LRUMap();
		LogContent startTimeSign = LogContent.createStartTimeSign();
		Document document = MapUtil.obj2Document(startTimeSign);
		for (String tableName : tableNames) {
			HazelcastConstruct<Document> construct = getConstruct(tableName);
			if (construct.isEmpty()) {
				construct.insert(document);
			}
		}
	}

	private HazelcastConstruct<Document> getConstruct(String tableName) {
		if (!constructMap.containsKey(tableName)) {
			HazelcastConstruct<Document> construct = new ConstructRingBuffer<>(
					jetContext.hazelcastInstance(),
					ShareCdcUtil.getConstructName(processorBaseContext.getTaskDto(), tableName),
					shareCdcTtlDay
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
		for (TapdataShareLogEvent tapdataShareLogEvent : tapdataShareLogEvents) {
			TapEvent tapEvent = tapdataShareLogEvent.getTapEvent();
			if (!(tapEvent instanceof TapRecordEvent)) {
				throw new RuntimeException("Share cdc target expected " + TapRecordEvent.class.getName() + ", actual: " + tapEvent.getClass().getName());
			}
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
			try {
				getConstruct(logContent.getFromTable()).insert(document);
			} catch (Exception e) {
				throw new RuntimeException("Insert document into ringbuffer failed; Document data: " + document + ". Error: " + e.getMessage(), e);
			}
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
			logger.warn("Invalid timestamp value: " + timestamp);
			obsLogger.warn("Invalid timestamp value: " + timestamp);
		}
		if (StringUtils.isBlank(offsetStr)) {
			obsLogger.warn("Invalid offset string: " + offsetStr);
		}
	}
}
