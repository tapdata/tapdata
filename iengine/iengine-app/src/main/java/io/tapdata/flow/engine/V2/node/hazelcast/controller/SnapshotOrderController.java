package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2023-05-10 17:34
 **/
public class SnapshotOrderController implements Serializable {
	private static final long serialVersionUID = 5790084132356759894L;
	private final ClientMongoOperator clientMongoOperator;
	private final TaskDto taskDto;
	private final ObsLogger obsLogger;
	private List<NodeControlLayer> snapshotOrderList;

	public static SnapshotOrderController create(TaskDto taskDto, List<NodeControlLayer> snapshotOrderList) {
		if (null == taskDto) {
			throw new TapCodeException(SnapshotOrderControllerExCode_21.CREATE_CONTROLLER_TASK_NULL);
		}
		if (null == snapshotOrderList) {
			snapshotOrderList = new ArrayList<>();
			List<Node> nodes = taskDto.getDag().getNodes();
			Node foundNode = nodes.stream().filter(node -> node instanceof MergeTableNode).findFirst().orElse(null);
			if (null != foundNode) {
				MergeTableNode mergeTableNode = (MergeTableNode) foundNode;
				List<MergeTableProperties> mergeProperties = mergeTableNode.getMergeProperties();
				recursiveBuildSnapshotOrderListByMergeNode(mergeProperties, snapshotOrderList, mergeTableNode, 1);
			}
		}
		return new SnapshotOrderController(taskDto, snapshotOrderList);
	}

	private static void recursiveBuildSnapshotOrderListByMergeNode(List<MergeTableProperties> mergeTableProperties, List<NodeControlLayer> snapshotOrderList, Node<?> mergeNode, int level) {
		if (CollectionUtils.isEmpty(mergeTableProperties)) {
			return;
		}
		List<NodeController> nodeControllers = new ArrayList<>();
		List<MergeTableProperties> nextLevelMergeProperties = new ArrayList<>();
		for (MergeTableProperties mergeTableProperty : mergeTableProperties) {
			String preId = mergeTableProperty.getId();
			List<? extends Node<?>> predecessors = mergeNode.predecessors();
			predecessors = predecessors.stream().filter(n -> n.getId().equals(preId)).collect(Collectors.toList());
			List<Node<?>> sourceTableNodes = GraphUtil.predecessors(mergeNode, Node::isDataNode, (List<Node<?>>) predecessors);
			for (Node<?> sourceTableNode : sourceTableNodes) {
				NodeController nodeController = new NodeController(sourceTableNode);
				if (level <= 1) {
					nodeController.running();
				} else {
					nodeController.waitRun();
				}
				nodeControllers.add(nodeController);
			}

			if (CollectionUtils.isNotEmpty(mergeTableProperty.getChildren())) {
				nextLevelMergeProperties.addAll(mergeTableProperty.getChildren());
			}
		}
		if (CollectionUtils.isNotEmpty(nodeControllers)) {
			snapshotOrderList.add(new NodeControlLayer(nodeControllers));
		}
		if (CollectionUtils.isNotEmpty(nextLevelMergeProperties)) {
			recursiveBuildSnapshotOrderListByMergeNode(nextLevelMergeProperties, snapshotOrderList, mergeNode, level + 1);
		}
	}

	private SnapshotOrderController(TaskDto taskDto, List<NodeControlLayer> snapshotOrderList) {
		this.snapshotOrderList = snapshotOrderList;
		if (null == snapshotOrderList) {
			this.snapshotOrderList = Collections.emptyList();
		}
		this.taskDto = taskDto;
		this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		this.obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);
	}

	public void runWithControl(Node<?> node, CommonUtils.AnyError runner) throws Throwable {
		if (null == node) {
			return;
		}
		String nodeId = node.getId();
		if (StringUtils.isBlank(nodeId)) {
			return;
		}
		NodeController nodeController = null;
		for (NodeControlLayer layer : snapshotOrderList) {
			nodeController = layer.getNodeControllers().stream().filter(nc -> nc.getNode().getId().equals(nodeId)).findFirst().orElse(null);
			if (null != nodeController) {
				break;
			}
		}
		if (null == nodeController) {
			nodeController = new NodeController(node);
			nodeController.running();
			throw new TapCodeException(SnapshotOrderControllerExCode_21.NODE_CONTROLLER_NOT_FOUND, String.format("Cannot found node controller by node id: %s[%s]", node.getName(), nodeId));
		}
		AtomicInteger status = nodeController.getStatus();
		boolean needRun;

		synchronized (status) {
			switch (status.get()) {
				case NodeController.WAIT_RUN:
					try {
						obsLogger.info("Node[{}] is waiting for running", node.getName());
						status.wait();
					} catch (InterruptedException ignored) {
					}
					needRun = true;
					break;
				case NodeController.RUNNING:
					needRun = true;
					break;
				case NodeController.FINISH:
					needRun = false;
					break;
				default:
					throw new TapCodeException(SnapshotOrderControllerExCode_21.NONSUPPORT_STATUS, "Invalid snapshot status: " + status.get());
			}
		}

		if (needRun) {
			try {
				runner.run();
			} catch (Throwable e) {
				if (e instanceof TapCodeException) {
					throw e;
				} else {
					throw new TapCodeException(SnapshotOrderControllerExCode_21.RUNNER_ERROR, e);
				}
			}
		}
	}

	public void finish(Node<?> node) {
		NodeControlLayer currentLayer = null;
		NodeControlLayer nextLayer = null;
		Iterator<NodeControlLayer> iterator = snapshotOrderList.iterator();
		while (iterator.hasNext()) {
			currentLayer = iterator.next();
			NodeController nodeController = currentLayer.getNodeControllers().stream().filter(nc -> nc.getNode().getId().equals(node.getId())).findFirst().orElse(null);
			if (null != nodeController) {
				nodeController.finish();
				if (iterator.hasNext()) {
					nextLayer = iterator.next();
				}
				break;
			}
		}
		if (null == currentLayer) {
			throw new TapCodeException(SnapshotOrderControllerExCode_21.NODE_CONTROL_LAYER_NOT_FOUND, String.format("Cannot found node control layer by node id: %s[%s]", node.getName(), node.getId()));
		}

		if (null == currentLayer.getNodeControllers().stream().filter(nc -> nc.getStatus().get() != NodeController.FINISH).findFirst().orElse(null)) {
			// All node finish in current layer
			// Notify next layer to run
			if (currentLayer.finish()) {
				if (null != nextLayer && CollectionUtils.isNotEmpty(nextLayer.getNodeControllers())) {
					obsLogger.info("Node[{}] finish, notify next layer to run", node.getName());
					nextLayer.run();
					obsLogger.info("Next layer have been notified: [{}]", nextLayer.getNodeControllers().stream().map(nc -> nc.getNode().getName()).collect(Collectors.joining(",")));
				}
			}
		}
	}

	public void flush() {
		if (null != clientMongoOperator && CollectionUtils.isNotEmpty(snapshotOrderList)) {
			byte[] bytes = InstanceFactory.instance(ObjectSerializable.class).fromObject(snapshotOrderList);
			Update update = Update.update("attrs." + SnapshotOrderService.SNAPSHOT_ORDER_LIST_KEY, bytes);
			clientMongoOperator.update(Query.query(Criteria.where("_id").is(taskDto.getId())), update, ConnectorConstant.TASK_COLLECTION);
		}
	}

	@Override
	public String toString() {
		if (CollectionUtils.isNotEmpty(snapshotOrderList)) {
			return "Node performs snapshot read by order list: " + snapshotOrderList.stream()
					.map(so -> so.getNodeControllers().stream()
							.map(nc -> nc.getNode().getName())
							.collect(Collectors.joining(",")))
					.collect(Collectors.joining("->"));
		} else {
			return "Node performs snapshot read asynchronously";
		}
	}
}
