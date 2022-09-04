package io.tapdata.proxy.client;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.github.openlg.graphlib.Edge;
import io.tapdata.aspect.PDKNodeInitAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.proxy.constants.NodeType;
import io.tapdata.modules.api.proxy.utils.NodeUtils;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.core.api.ConnectorNode;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC})
public class SubscriptionAspectTask extends AbstractAspectTask {
	@Bean
	ProxySubscriptionManager proxySubscriptionManager;
	private final TaskSubscribeInfo taskSubscribeInfo = new TaskSubscribeInfo();

	public SubscriptionAspectTask() {
		observerHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
	}

	private Void handlePDKNodeInit(PDKNodeInitAspect aspect) {
		DataProcessorContext dataProcessorContext = aspect.getDataProcessorContext();
		Node node = dataProcessorContext.getNode();
		if(node != null) {
			String associateId = dataProcessorContext.getPdkAssociateId();
			if(associateId != null) {
				ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
				if(connectorNode != null) {
					String connectionId = null;
					if(node instanceof DataParentNode) {
						connectionId = ((DataParentNode<?>) node).getConnectionId();
					}
					if(connectionId != null) {
						String type = calculateNodeType(node);
						if(type != null) {
							String typeConnectionId = NodeUtils.typeConnectionId(type, connectionId);
//							io.tapdata.pdk.core.api.Node
							List<io.tapdata.pdk.core.api.Node> pdkNodeList = taskSubscribeInfo.typeConnectionIdPDKNodeMap.get(typeConnectionId);
							if(pdkNodeList == null) {
								pdkNodeList = new CopyOnWriteArrayList<>();
								List<io.tapdata.pdk.core.api.Node> old = taskSubscribeInfo.typeConnectionIdPDKNodeMap.putIfAbsent(typeConnectionId, pdkNodeList);
								if(old != null) {
									pdkNodeList = old;
								}
								pdkNodeList.add(connectorNode);
								taskSubscribeInfo.nodeIdPDKNodeMap.put(node.getId(), connectorNode);
								proxySubscriptionManager.taskSubscribeInfoChanged(taskSubscribeInfo);
							}
						} else {
							TapLogger.warn("Node {} is unknown type, not sure it is source, target or processor", node.getId());
						}
					}
				}
			}
		}
		return null;
	}

	private String calculateNodeType(Node<?> node) {
		String nodeId = node.getId();
		if(nodeId != null && node.getGraph() != null) {
			Collection<Edge> edges = node.getGraph().getEdges();
			if(edges != null) {
				boolean source = false;
				boolean target = false;
				for(Edge edge : edges) {
					if(edge.getSource().equals(nodeId)) {
						source = true;
					} else if(edge.getTarget().equals(nodeId)) {
						target = true;
					}
				}
				if(source && target)
					return NodeType.PROCESSOR;
				if(source)
					return NodeType.SOURCE;
				if(target)
					return NodeType.TARGET;
			}
		}
		return null;
	}

	@Override
	public void onStart(TaskStartAspect startAspect) {
		TaskDto taskDto = startAspect.getTask();
		if(taskDto != null && taskDto.getId() != null && taskDto.getDag() != null) {
			taskSubscribeInfo.taskId = taskDto.getId().toString();
			proxySubscriptionManager.addTaskSubscribeInfo(taskSubscribeInfo);
		}
	}

	@Override
	public void onStop(TaskStopAspect stopAspect) {
		proxySubscriptionManager.removeTaskSubscribeInfo(taskSubscribeInfo);
	}
}
