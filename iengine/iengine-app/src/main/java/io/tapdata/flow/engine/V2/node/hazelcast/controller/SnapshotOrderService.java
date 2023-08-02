package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.exception.TapCodeException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2023-05-12 15:54
 **/
public class SnapshotOrderService {
	public static final String SNAPSHOT_ORDER_LIST_KEY = "SNAPSHOT_ORDER_LIST";
	public static final String MERGE_MODE_KEY = "mergeMode";
	private final Map<String, SnapshotOrderController> controllerMap = new ConcurrentHashMap<>();

	public SnapshotOrderController addController(TaskDto taskDto) {
		if (null == taskDto) {
			throw new TapCodeException(SnapshotOrderControllerExCode_21.CREATE_CONTROLLER_TASK_NULL);
		}
		String taskId = taskDto.getId().toHexString();
		Map<String, Object> attrs = taskDto.getAttrs();
		List<NodeControlLayer> snapshotOrderList = null;
		String oldMergeMode = null;
		if (null != attrs && attrs.containsKey(SNAPSHOT_ORDER_LIST_KEY)) {
			Object o = attrs.get(SNAPSHOT_ORDER_LIST_KEY);
			byte[] bytes;
			if (o instanceof byte[]) {
				bytes = (byte[]) o;
			} else if (o instanceof String) {
				bytes = Base64.getDecoder().decode((String) o);
			} else {
				throw new TapCodeException(SnapshotOrderControllerExCode_21.SNAPSHOT_ORDER_LIST_FORMAT_ERROR, o.getClass().toString());
			}
			Object object = InstanceFactory.instance(ObjectSerializable.class).toObject(bytes);
			if (object instanceof List) {
				snapshotOrderList = (List<NodeControlLayer>) object;
				SnapshotOrderController.init(taskDto, snapshotOrderList);
			} else if (object instanceof Map) {
				Map<String, Object> map = (Map<String, Object>) object;
				Object listObj = map.get(SNAPSHOT_ORDER_LIST_KEY);
				if (listObj instanceof List) {
					snapshotOrderList = (List<NodeControlLayer>) listObj;
					SnapshotOrderController.init(taskDto, snapshotOrderList);
				}
				Object mergeModeObj = map.get(MERGE_MODE_KEY);
				if (mergeModeObj instanceof String) {
					oldMergeMode = (String) mergeModeObj;
				}
			}
		}
		snapshotOrderList = resetSnapshotOrderListIfInvalid(snapshotOrderList);
		if (null == snapshotOrderList) {
			oldMergeMode = null;
		}
		SnapshotOrderController snapshotOrderController = SnapshotOrderController.create(taskDto, snapshotOrderList);
		handleSnapshotOrderMode(taskDto, oldMergeMode, snapshotOrderController.getSnapshotOrderList());
		controllerMap.remove(taskId);
		controllerMap.put(taskId, snapshotOrderController);
		return snapshotOrderController;
	}

	@Nullable
	private static List<NodeControlLayer> resetSnapshotOrderListIfInvalid(List<NodeControlLayer> snapshotOrderList) {
		if (null != snapshotOrderList) {
			boolean valid = true;
			for (NodeControlLayer nodeControlLayer : snapshotOrderList) {
				if (CollectionUtils.isEmpty(nodeControlLayer.getNodeControllers())) {
					valid = false;
				}
				for (NodeController nodeController : nodeControlLayer.getNodeControllers()) {
					if (null == nodeController.getNode() || null == nodeController.getStatus()) {
						valid = false;
						break;
					}
				}
				if (!valid) {
					snapshotOrderList = null;
					break;
				}
			}
		}
		return snapshotOrderList;
	}

	private static void handleSnapshotOrderMode(TaskDto taskDto, String oldMergeMode, List<NodeControlLayer> snapshotOrderList) {
		List<Node> nodes = taskDto.getDag().getNodes();
		Node<?> foundNode = nodes.stream().filter(node -> node instanceof MergeTableNode).findFirst().orElse(null);
		if (foundNode instanceof MergeTableNode) {
			MergeTableNode mergeTableNode = (MergeTableNode) foundNode;
			if (null != oldMergeMode && !mergeTableNode.getMergeMode().equals(oldMergeMode)) {
				throw new TapCodeException(SnapshotOrderControllerExCode_21.CANNOT_CHANGE_MERGE_MODE_WITH_OUT_RESET, String.format("Last merge mode: %s, current merge mode: %s", oldMergeMode, mergeTableNode.getMergeMode()));
			}
			String mergeMode = mergeTableNode.getMergeMode();
			if (StringUtils.isBlank(mergeMode) || !StringUtils.equalsAny(mergeMode, MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE, MergeTableNode.SUB_TABLE_FIRST_MERGE_MODE)) {
				mergeMode = MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE;
			}
			switch (mergeMode) {
				case MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE:
					if (CollectionUtils.isNotEmpty(snapshotOrderList)) {
						NodeControlLayer firstLayer = snapshotOrderList.get(0);
						for (NodeController nodeController : firstLayer.getNodeControllers()) {
							nodeController.running();
						}
					}
					break;
				case MergeTableNode.SUB_TABLE_FIRST_MERGE_MODE:
					if (CollectionUtils.isNotEmpty(snapshotOrderList)) {
						Collections.reverse(snapshotOrderList);
						if (snapshotOrderList.size() == 1) {
							for (NodeController nodeController : snapshotOrderList.get(0).getNodeControllers()) {
								nodeController.running();
							}
						} else {
							for (int i = 0; i < snapshotOrderList.size() - 1; i++) {
								NodeControlLayer nodeControlLayer = snapshotOrderList.get(i);
								for (NodeController nodeController : nodeControlLayer.getNodeControllers()) {
									nodeController.running();
								}
							}
						}
					}
					break;
			}
		}
	}

	public SnapshotOrderController getController(String taskId) {
		return controllerMap.get(taskId);
	}

	public boolean removeController(String taskId) {
		return null != controllerMap.remove(taskId);
	}

	public static SnapshotOrderService getInstance() {
		return Singleton.INSTANCE.getInstance();
	}

	private enum Singleton {
		INSTANCE;
		private final SnapshotOrderService instance;

		Singleton() {
			instance = new SnapshotOrderService();
		}

		public SnapshotOrderService getInstance() {
			return instance;
		}
	}
}
