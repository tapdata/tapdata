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
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.task.preview.*;
import io.tapdata.flow.engine.V2.task.preview.entity.MemoryMergeData;
import io.tapdata.flow.engine.V2.task.preview.entity.MergeCache;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewMergeReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewOperation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
	private TaskPreviewNodeMergeResultVO taskPreviewNodeMergeResultVO;

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
		this.taskPreviewNodeMergeResultVO = TaskPreviewNodeMergeResultVO.create();
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
			if (null != node) {
				mergeCacheMap.put(id, MergeCache.create(property).node(node));
			}
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
				taskPreviewNodeMergeResultVO.data(dataMap);
			}
			consumer.accept(tapdataEvent, null);
		} else {
			try {
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
			} catch (Exception e) {
				errorHandle(e);
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
			for (Map<String, Object> datum : mergeCache.getData()) {
				addFieldMapping(p.getId(), "", datum);
			}
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
				if (null == mergeCache) {
					continue;
				}
				List<Map<String, Object>> mergeCacheData = mergeCache.getData();
				MergeTableProperties.MergeType mergeType = childProperty.getMergeType();
				List<Map<String, String>> joinKeys = childProperty.getJoinKeys();
				String targetPath = null == childProperty.getTargetPath() ? "" : childProperty.getTargetPath();
				for (Map<String, Object> parentMergeDatum : parentMergeData) {
					if (MergeTableProperties.MergeType.updateWrite == mergeType) {
						String parentJoinKeyValue = dataJoinKeyValueString(parentMergeDatum, joinKeys, JoinKeyEnum.TARGET);
						Map<String, Object> childData = mergeCacheData.stream()
								.filter(d -> dataJoinKeyValueString(d, joinKeys, JoinKeyEnum.SOURCE).equals(parentJoinKeyValue))
								.findFirst().orElse(null);
						try {
							if (StringUtils.isNotBlank(targetPath)) {
								MapUtilV2.putValueInMap(parentMergeDatum, targetPath, childData);
							} else {
								parentMergeDatum.putAll(childData);
							}
							addFieldMapping(childPreNodeId, targetPath, childData);
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
							for (Object childDatum : childData) {
								addFieldMapping(childPreNodeId, targetPath, childDatum);
							}
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

	@Override
	protected void reportToPreviewIfNeed(TapdataEvent dataEvent) {
		if (processorBaseContext.getTaskDto().isPreviewTask() && null != taskPreviewInstance && null != dataEvent.getTapEvent()) {
			taskPreviewInstance.getTaskPreviewResultVO().nodeResult(getNode().getId(), taskPreviewNodeMergeResultVO);
		}
	}

	protected void addFieldMapping(String nodeId, String prefix, Object data) {
		if (!(data instanceof Map)) {
			return;
		}
		Map<String, Object> map = (Map<String, Object>) data;
		LinkedList<MergeData> queue = new LinkedList<>();
		queue.offer(new MergeData(map));
		while (isRunning() && !queue.isEmpty()) {
			MergeData mergeData = queue.poll();
			Map<String, Object> datum = mergeData.map;
			String loopPrefix = mergeData.prefix;
			for (Map.Entry<String, Object> entry : datum.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				String field = key;
				if (StringUtils.isNotBlank(loopPrefix)) {
					field = String.join(".", loopPrefix, field);
				}
				if (StringUtils.isNotBlank(prefix)) {
					field = String.join(".", prefix, field);
				}
				if (value instanceof Map) {
					queue.offer(new MergeData((Map<String, Object>) value, field));
				} else if (value instanceof Collection) {
					for (Object obj : ((Collection<?>) value)) {
						if (obj instanceof Map) {
							queue.offer(new MergeData((Map<String, Object>) obj, field));
						} else {
							taskPreviewNodeMergeResultVO.addFieldMapping(nodeId, field);
						}
					}
				} else {
					taskPreviewNodeMergeResultVO.addFieldMapping(nodeId, field);
				}
			}
		}
	}

	static class MergeData {
		private final Map<String, Object> map;
		private String prefix;

		public MergeData(Map<String, Object> map, String prefix) {
			this.map = map;
			this.prefix = prefix;
		}

		public MergeData(Map<String, Object> map) {
			this.map = map;
		}
	}
}
