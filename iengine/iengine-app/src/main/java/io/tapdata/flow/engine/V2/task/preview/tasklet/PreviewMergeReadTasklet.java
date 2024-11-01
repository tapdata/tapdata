package io.tapdata.flow.engine.V2.task.preview.tasklet;

import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.NotExistsNode;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.task.preview.entity.NotExistsValue;
import io.tapdata.flow.engine.V2.task.preview.PreviewReadOperationQueue;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewException;
import io.tapdata.flow.engine.V2.task.preview.entity.MergeReadData;
import io.tapdata.flow.engine.V2.task.preview.entity.MergeTableLoopProperty;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewFinishReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewMergeReadOperation;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 11:38
 **/
public class PreviewMergeReadTasklet implements PreviewReadTasklet {

	@Override
	public void execute(TaskDto taskDto, PreviewReadOperationQueue previewReadOperationQueue) throws TaskPreviewException {
		DAG dag = taskDto.getDag();
		List<Node> nodes = dag.getNodes();
		Node<?> foundMergeTableNode = nodes.stream().filter(n -> n instanceof MergeTableNode).findFirst().orElse(null);
		if (foundMergeTableNode == null) {
			throw new TaskPreviewException("No MergeTableNode found in the task");
		}
		List<Node> sourceNodes = new ArrayList<>();
		MergeTableNode mergeTableNode = (MergeTableNode) foundMergeTableNode;
		List<MergeTableProperties> mergeProperties = mergeTableNode.getMergeProperties();
		LinkedList<List<MergeTableLoopProperty>> linkedList = new LinkedList<>();
		final AtomicReference<Integer> level = new AtomicReference<>(1);
		List<MergeTableLoopProperty> rootMergeTableLoopProperties = mergeProperties.stream()
				.map(mergeTableProperties -> new MergeTableLoopProperty(level.get(), mergeTableProperties))
				.collect(Collectors.toList());
		linkedList.offer(rootMergeTableLoopProperties);
		List<PreviewMergeReadOperation> allOperations = new ArrayList<>();

		while (!linkedList.isEmpty()) {
			List<MergeTableLoopProperty> mergeTableLoopProperties = linkedList.poll();
			List<PreviewMergeReadOperation> previewReadOperations = new ArrayList<>();
			for (MergeTableLoopProperty mergeTableLoopProperty : mergeTableLoopProperties) {
				MergeTableProperties mergeTableProperties = mergeTableLoopProperty.getMergeTableProperties();
				String preNodeId = mergeTableProperties.getId();
				Node<?> preNode = dag.getNode(preNodeId);
				List<Node<?>> allSourceNodes = new ArrayList<>();
				if (preNode instanceof TableNode) {
					allSourceNodes.add(preNode);
				} else {
					allSourceNodes.addAll(GraphUtil.predecessors(dag.getNode(preNodeId), node -> node instanceof TableNode));
				}
				sourceNodes.addAll(allSourceNodes);
				for (Node<?> sourceNode : allSourceNodes) {
					PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(
							sourceNode.getId(), mergeTableLoopProperty, taskDto.getPreviewRows()
					);
					if (buildTapAdvanceFilter(taskDto, mergeTableLoopProperty, mergeTableProperties, previewMergeReadOperation))
						continue;
					previewReadOperationQueue.addOperation(sourceNode.getId(), previewMergeReadOperation);
					previewReadOperations.add(previewMergeReadOperation);
					allOperations.add(previewMergeReadOperation);
				}
			}

			level.set(level.get() + 1);
			while (!previewReadOperations.isEmpty()) {
				Iterator<PreviewMergeReadOperation> iterator = previewReadOperations.iterator();
				while (iterator.hasNext()) {
					PreviewMergeReadOperation previewReadOperation = iterator.next();
					MergeTableLoopProperty mergeTableLoopProperty = previewReadOperation.getMergeTableLoopProperty();
					MergeTableProperties mergeTableProperties = mergeTableLoopProperty.getMergeTableProperties();
					MergeReadData mergeReadData = previewReadOperation.replyData();
					if (null == mergeReadData) {
						continue;
					}
					List<Map<String, Object>> data = mergeReadData.getData();
					if (CollectionUtils.isNotEmpty(mergeTableProperties.getChildren()) && CollectionUtils.isNotEmpty(data)) {
						List<MergeTableProperties> children = mergeTableProperties.getChildren();
						List<MergeTableLoopProperty> nextLoopProperties = new ArrayList<>();
						for (MergeTableProperties childMergeTableProperty : children) {
							for (Map<String, Object> datum : data) {
								DataMap dataMap = DataMap.create(datum);
								MergeTableLoopProperty parent = mergeTableLoopProperty.clone();
								parent.setData(dataMap);
								parent.setParentProperties(null);
								MergeTableLoopProperty childMergeTableLoopProperty = new MergeTableLoopProperty(level.get(), childMergeTableProperty, dataMap);
								childMergeTableLoopProperty.addParent(parent);
								nextLoopProperties.add(childMergeTableLoopProperty);
							}
						}
						if (CollectionUtils.isNotEmpty(nextLoopProperties)) {
							linkedList.offer(nextLoopProperties);
						}
					}
					iterator.remove();
				}
			}
		}

		for (PreviewMergeReadOperation previewMergeReadOperation : allOperations) {
			try {
				previewMergeReadOperation.getMergeNodeReceived().await();
			} catch (InterruptedException e) {
				return;
			}
		}
		for (int i = 0; i < sourceNodes.size(); i++) {
			Node node = sourceNodes.get(i);
			boolean isLast = i == sourceNodes.size() - 1;
			previewReadOperationQueue.addOperation(node.getId(), PreviewFinishReadOperation.create().last(isLast));
		}
	}

	private boolean buildTapAdvanceFilter(TaskDto taskDto,
										  MergeTableLoopProperty mergeTableLoopProperty,
										  MergeTableProperties mergeTableProperties,
										  PreviewMergeReadOperation previewMergeReadOperation) {
		TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
		if (mergeTableLoopProperty.getLevel() == 1) {
			tapAdvanceFilter.limit(taskDto.getPreviewRows());
		} else {
			List<MergeTableLoopProperty> parentProperties = mergeTableLoopProperty.getParentProperties();
			DataMap parentData = parentProperties.get(parentProperties.size() - 1).getData();
			if (MapUtils.isEmpty(parentData)) {
				return true;
			}
			List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
			DataMap match = new DataMap();
			boolean finish = false;
			for (Map<String, String> joinKey : joinKeys) {
				String source = joinKey.get("source");
				String target = joinKey.get("target");
				Object value = findTargetKeyValue(mergeTableLoopProperty, target);
				if (value instanceof NotExistsValue) {
					finish = true;
					break;
				}
				match.put(source, value);
			}
			if (finish) {
				return true;
			}
			tapAdvanceFilter.match(match);
			MergeTableProperties.MergeType mergeType = mergeTableProperties.getMergeType();
			if (MergeTableProperties.MergeType.updateIntoArray == mergeType) {
				tapAdvanceFilter.limit(2);
			} else {
				tapAdvanceFilter.limit(1);
			}
		}
		previewMergeReadOperation.setTapAdvanceFilter(tapAdvanceFilter);
		return false;
	}

	protected Object findTargetKeyValue(MergeTableLoopProperty mergeTableLoopProperty, String target) {
		List<MergeTableLoopProperty> parentProperties = mergeTableLoopProperty.getParentProperties();
		for (int i = parentProperties.size() - 1; i >= 0; i--) {
			MergeTableLoopProperty parent = parentProperties.get(i);
			DataMap data = parent.getData();
			Object value = MapUtilV2.getValueByKey(data, target);
			if (value instanceof NotExistsNode) {
				continue;
			}
			return value;
		}
		return new NotExistsValue();
	}
}
