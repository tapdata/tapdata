package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.exception.TapCodeException;

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
	private final Map<String, SnapshotOrderController> controllerMap = new ConcurrentHashMap<>();

	public SnapshotOrderController addController(TaskDto taskDto) {
		if (null == taskDto) {
			throw new TapCodeException(SnapshotOrderControllerExCode_21.CREATE_CONTROLLER_TASK_NULL);
		}
		String taskId = taskDto.getId().toHexString();
		Map<String, Object> attrs = taskDto.getAttrs();
		List<NodeControlLayer> snapshotOrderList = null;
		if (attrs != null && attrs.containsKey(SNAPSHOT_ORDER_LIST_KEY)) {
			Object o = attrs.get(SNAPSHOT_ORDER_LIST_KEY);
			if (o instanceof byte[]) {
				Object object = InstanceFactory.instance(ObjectSerializable.class).toObject((byte[]) o);
				if (object instanceof List) {
					snapshotOrderList = (List<NodeControlLayer>) object;
				}
			}
		}
		SnapshotOrderController snapshotOrderController = SnapshotOrderController.create(taskDto, snapshotOrderList);
		controllerMap.remove(taskId);
		controllerMap.put(taskId, snapshotOrderController);
		return snapshotOrderController;
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
