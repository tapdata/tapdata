package io.tapdata.proxy.client;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.github.openlg.graphlib.Edge;
import io.tapdata.aspect.PDKNodeInitAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.proxy.constants.NodeType;
import io.tapdata.modules.api.proxy.data.FetchNewData;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;
import io.tapdata.modules.api.proxy.utils.NodeUtils;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC}, ignoreErrors = false, order = 1)
public class SubscriptionAspectTask extends AbstractAspectTask {
	private static final String TAG = SubscriptionAspectTask.class.getSimpleName();
	@Bean
	ProxySubscriptionManager proxySubscriptionManager;
	StreamReadConsumer streamReadConsumer;
	Long taskStartTime;
	private final TaskSubscribeInfo taskSubscribeInfo = new TaskSubscribeInfo();

	public SubscriptionAspectTask() {
		observerHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
		observerHandlers.register(StreamReadFuncAspect.class, this::handleStreamRead);
	}

	private Void handleStreamRead(StreamReadFuncAspect streamReadFuncAspect) {
		switch (streamReadFuncAspect.getState()) {
			case StreamReadFuncAspect.STATE_CALLBACK_RAW_DATA:
//				((DataParentNode)streamReadFuncAspect.getDataProcessorContext().getNode()).getConnectionId();
				streamReadConsumer = streamReadFuncAspect.getStreamReadConsumer();
				if(streamReadConsumer != null) {
					streamReadConsumer.streamReadStarted();
					String subscribeId = taskSubscribeInfo.nodeIdTypeConnectionIdMap.get(streamReadFuncAspect.getDataProcessorContext().getNode().getId());
					Object offset = streamReadFuncAspect.getOffsetState();

					BiConsumer<Result, Throwable> biConsumer = (result, throwable) -> {
						handleFetchNewDataResult(streamReadFuncAspect, subscribeId, offset, result, throwable);
					};
					fetchNewData(subscribeId, offset, biConsumer);
				}
				break;
		}
		return null;
	}

	private void handleFetchNewDataResult(StreamReadFuncAspect streamReadFuncAspect, String subscribeId, Object offset, Result result, Throwable throwable) {
		if(throwable != null) {
			streamReadFuncAspect.noMoreWaitRawData(throwable);
			return;
		}
		if(result == null) {
			streamReadFuncAspect.noMoreWaitRawData(new CoreException(NetErrors.RESULT_IS_NULL, "Result/Throwable are both null in handleFetchNewDataResult, subscribeId {} offset {}", subscribeId, offset));
			return;
		}

		FetchNewDataResult fetchNewDataResult = (FetchNewDataResult) result.getMessage();
		if(fetchNewDataResult != null) {
			List<MessageEntity> messages = fetchNewDataResult.getMessages();
			if(messages != null) {
				String associateId = streamReadFuncAspect.getDataProcessorContext().getPdkAssociateId();
				if(associateId != null) {
					ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
					RawDataCallbackFilterFunction function = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunction();
					if(function != null) {
						List<TapEvent> events = new ArrayList<>();

						for(MessageEntity message : messages) {
							PDKInvocationMonitor.invoke(connectorNode, PDKMethod.RAW_DATA_CALLBACK_FILTER, () -> {
								TapEvent tapEvent = function.filter(connectorNode.getConnectorContext(), message.getContent());
								if(tapEvent != null)
									events.add(tapEvent);
							}, TAG);
						}
						streamReadConsumer.accept(events, fetchNewDataResult.getOffset());
						if(!messages.isEmpty()) {
							fetchNewData(subscribeId, offset, (result1, throwable1) -> handleFetchNewDataResult(streamReadFuncAspect, subscribeId, offset, result1, throwable1));
						}
					}
				}
			}
		}
	}

	private void fetchNewData(String subscribeId, Object offset, BiConsumer<Result, Throwable> biConsumer) {
		FetchNewData fetchNewData = new FetchNewData()
				.limit(1)
				.service("engine")
				.subscribeId(subscribeId);
		if(offset == null)
			fetchNewData.taskStartTime(taskStartTime);
		else
			fetchNewData.offset(offset);
		proxySubscriptionManager.getImClient().sendData(new IncomingData().message(fetchNewData)).whenComplete(biConsumer);
	}

	private Void handlePDKNodeInit(PDKNodeInitAspect aspect) {
		DataProcessorContext dataProcessorContext = aspect.getDataProcessorContext();
		Node node = dataProcessorContext.getNode();
		if(node != null) {
			String associateId = dataProcessorContext.getPdkAssociateId();
			if(associateId != null) {
				ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
				if(connectorNode != null) {
//					RawDataCallbackFilterFunction rawDataCallbackFilterFunction = connectorNode.getConnectorFunctions().getDataCallbackFunction();
//					if(rawDataCallbackFilterFunction == null)
//						return null;
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
								taskSubscribeInfo.nodeIdTypeConnectionIdMap.put(node.getId(), typeConnectionId);
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
		taskStartTime = System.currentTimeMillis();
		if(taskDto != null && taskDto.getId() != null && taskDto.getDag() != null) {
			taskSubscribeInfo.taskId = taskDto.getId().toString();
			taskSubscribeInfo.subscriptionAspectTask = this;
			proxySubscriptionManager.addTaskSubscribeInfo(taskSubscribeInfo);
		}
	}

	@Override
	public void onStop(TaskStopAspect stopAspect) {
		proxySubscriptionManager.removeTaskSubscribeInfo(taskSubscribeInfo);
	}
}
