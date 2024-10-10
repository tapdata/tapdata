package io.tapdata.flow.engine.V2.task.preview.node;

import com.tapdata.constant.MapUtilV2;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataPreviewCompleteEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.task.preview.MemoryMergeData;
import io.tapdata.flow.engine.V2.task.preview.MemoryMergeException;
import io.tapdata.flow.engine.V2.task.preview.MergeCache;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewExCode_37;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewMergeReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewOperation;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2024-09-27 16:26
 **/
public class HazelcastPreviewMergeNode extends HazelcastProcessorBaseNode {
	private MergeTableNode mergeTableNode;
	private Map<String, MergeCache> mergeCacheMap;

	public HazelcastPreviewMergeNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		Node<?> node = getNode();
		if (node instanceof MergeTableNode) {
			this.mergeTableNode = (MergeTableNode) node;
		}
		initMergeCache();
	}

	protected void initMergeCache() {
		this.mergeCacheMap = new HashMap<>();
		TaskDto taskDto = processorBaseContext.getTaskDto();
		DAG dag = taskDto.getDag();
		List<MergeTableProperties> mergeProperties = this.mergeTableNode.getMergeProperties();
		LinkedList<MergeTableProperties> queue = new LinkedList<>();
		for (MergeTableProperties mergeProperty : mergeProperties) {
			queue.offer(mergeProperty);
		}
		while (!queue.isEmpty()) {
			MergeTableProperties property = queue.poll();
			String id = property.getId();
			Node<?> node = dag.getNode(id);
			mergeCacheMap.put(id, MergeCache.create(property).node(node));
			if (CollectionUtils.isNotEmpty(property.getChildren())) {
				for (MergeTableProperties child : property.getChildren()) {
					queue.offer(child);
				}
			}
		}
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		if (tapdataEvent instanceof TapdataPreviewCompleteEvent) {
			List<Map<String, Object>> dataMaps;
			try {
				dataMaps = memoryMerge();
			} catch (MemoryMergeException e) {
				errorHandle(new TapCodeException(TaskPreviewExCode_37.MEMORY_MERGE_ERROR, e));
				return;
			}
			for (Map<String, Object> dataMap : dataMaps) {
				TapdataEvent event = new TapdataEvent();
				event.setNodeIds(tapdataEvent.getNodeIds());
				event.setTapEvent(TapInsertRecordEvent.create().after(dataMap));
				consumer.accept(event, null);
			}
			consumer.accept(tapdataEvent, null);
		} else {
			TapEvent tapEvent = tapdataEvent.getTapEvent();
			if (!(tapEvent instanceof TapInsertRecordEvent)) {
				consumer.accept(tapdataEvent, null);
			}
			TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) tapEvent;
			List<String> nodeIds = tapdataEvent.getNodeIds();
			String preNodeId = nodeIds.get(nodeIds.size() - 1);
			mergeCacheMap.get(preNodeId).data(new HashMap<>(tapInsertRecordEvent.getAfter()));
			Object previewOperation = tapdataEvent.getInfo(PreviewOperation.class.getSimpleName());
			if (previewOperation instanceof PreviewMergeReadOperation) {
				((PreviewMergeReadOperation) previewOperation).getMergeNodeReceived().countDown();
			}
		}
	}

	private List<Map<String, Object>> memoryMerge() throws MemoryMergeException {
		List<MergeTableProperties> mergeProperties = this.mergeTableNode.getMergeProperties();
		List<Map<String, Object>> result = new ArrayList<>();
		LinkedList<MemoryMergeData> queue = new LinkedList<>();
		mergeProperties.forEach(p -> {
			MergeCache mergeCache = mergeCacheMap.get(p.getId());
			queue.offer(MemoryMergeData.create().mergeTableProperties(p)
					.data(mergeCache.getData()));
		});
		while (isRunning() && !queue.isEmpty()) {
			MemoryMergeData memoryMergeData = queue.poll();
			MergeTableProperties mergeTableProperties = memoryMergeData.getMergeTableProperties();
			List<Map<String, Object>> parentMergeData = memoryMergeData.getData();
			List<MergeTableProperties> children = mergeTableProperties.getChildren();
			if (CollectionUtils.isEmpty(children)) {
				result.addAll(parentMergeData);
				continue;
			}
			for (MergeTableProperties childProperty : children) {
				String childPreNodeId = childProperty.getId();
				MergeCache mergeCache = mergeCacheMap.get(childPreNodeId);
				List<Map<String, Object>> mergeCacheData = mergeCache.getData();
				MergeTableProperties.MergeType mergeType = childProperty.getMergeType();
				List<Map<String, String>> joinKeys = childProperty.getJoinKeys();
				String targetPath = childProperty.getTargetPath();
				for (Map<String, Object> parentMergeDatum : parentMergeData) {
					if (MergeTableProperties.MergeType.updateWrite == mergeType) {
						String parentJoinKeyValue = dataJoinKeyValueString(parentMergeDatum, joinKeys, JoinKeyEnum.TARGET);
						Map<String, Object> childData = mergeCacheData.stream()
								.filter(d -> dataJoinKeyValueString(d, joinKeys, JoinKeyEnum.SOURCE).equals(parentJoinKeyValue))
								.findFirst().orElse(null);
						try {
							MapUtilV2.putValueInMap(parentMergeDatum, targetPath, childData);
						} catch (Exception e) {
							throw new MemoryMergeException(e, parentMergeDatum, childData, mergeTableProperties);
						}
					} else if (MergeTableProperties.MergeType.updateIntoArray == mergeType) {
						String parentJoinKeyValue = dataJoinKeyValueString(parentMergeDatum, joinKeys, JoinKeyEnum.TARGET);
						List<Object> childData = mergeCacheData.stream()
								.filter(d -> dataJoinKeyValueString(d, joinKeys, JoinKeyEnum.SOURCE).equals(parentJoinKeyValue))
								.collect(Collectors.toList());
						try {
							MapUtilV2.putValueInMap(parentMergeDatum, targetPath, childData);
						} catch (Exception e) {
							throw new MemoryMergeException(e, parentMergeDatum, childData.stream().map(d -> ((TapMapValue) d).getValue()).collect(Collectors.toList()), mergeTableProperties);
						}
					}
				}
				queue.offer(MemoryMergeData.create().mergeTableProperties(childProperty).data(parentMergeData));
			}
		}
		return result;
	}

	protected String dataJoinKeyValueString(Map<String, Object> data, List<Map<String, String>> joinKeys, JoinKeyEnum joinKeyEnum) {
		List<String> values = new ArrayList<>();
		for (Map<String, String> joinKey : joinKeys) {
			String key = joinKey.get(joinKeyEnum.getKey());
			Object value = MapUtilV2.getValueByKeyV2(data, key);
			if (null == value) {
				value = "null";
			}
			values.add(value.toString());
		}
		return String.join("_", values);
	}

	public enum JoinKeyEnum {
		SOURCE("source"),
		TARGET("target");

		private final String key;

		JoinKeyEnum(String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}
	}

	@Override
	public boolean needTransformValue() {
		return false;
	}

	@Override
	protected boolean supportBatchProcess() {
		return false;
	}
}
