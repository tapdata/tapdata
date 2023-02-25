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
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
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
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunctionV2;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC}, ignoreErrors = false, order = 1)
public class SubscriptionAspectTask extends AbstractAspectTask {
	private static final String TAG = SubscriptionAspectTask.class.getSimpleName();
	@Bean
	ProxySubscriptionManager proxySubscriptionManager;
	StreamReadConsumer streamReadConsumer;
	Long taskStartTime;
	private final TaskSubscribeInfo taskSubscribeInfo = new TaskSubscribeInfo();
	private boolean isStarted;
	private StreamReadFuncAspect streamReadFuncAspect;
	private final AtomicBoolean isFetchingNewData = new AtomicBoolean(false);
	private volatile boolean needFetchingNewData = false;

	private String currentOffset;

	public SubscriptionAspectTask() {
		observerHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
		observerHandlers.register(StreamReadFuncAspect.class, this::handleStreamRead);
	}

	private Void handleStreamRead(StreamReadFuncAspect streamReadFuncAspect) {
		this.streamReadFuncAspect = streamReadFuncAspect;
		switch (streamReadFuncAspect.getState()) {
			case StreamReadFuncAspect.STATE_CALLBACK_RAW_DATA:
//				((DataParentNode)streamReadFuncAspect.getDataProcessorContext().getNode()).getConnectionId();
				if(streamReadConsumer == null) {
					streamReadConsumer = streamReadFuncAspect.getStreamReadConsumer();
					if(streamReadConsumer != null) {
						streamReadConsumer.streamReadStarted();
						String subscribeId = taskSubscribeInfo.nodeIdTypeConnectionIdMap.get(streamReadFuncAspect.getDataProcessorContext().getNode().getId());
						Object theOffset = streamReadFuncAspect.getOffsetState();
						if(theOffset instanceof String) {
							currentOffset = (String) theOffset;
						} else {
							currentOffset = null;
						}

						if(startFetchingNewData()) {
							TapLogger.debug(TAG, "start fetching new data, started {} isFetchingNewData {}", isStarted, isFetchingNewData.get());
							fetchNewData(subscribeId, currentOffset, (result, throwable) -> handleFetchNewDataResult(streamReadFuncAspect, subscribeId, result, throwable));
						}
					}
				} else {
					TapLogger.debug(TAG, "StreamReadFuncAspect.STATE_CALLBACK_RAW_DATA shouldn't enter more than once");
				}
				break;
		}
		return null;
	}

	public synchronized void enableFetchingNewData(String subscribeId) {
		needFetchingNewData = true;
		if(isFetchingNewData.compareAndSet(false, true)) {
			fetchNewData(subscribeId, currentOffset, (result, throwable) -> handleFetchNewDataResult(streamReadFuncAspect, subscribeId, result, throwable));
		}
	}
	private boolean startFetchingNewData() {
		return isStarted && isFetchingNewData.compareAndSet(false, true);
	}

	private void handleFetchNewDataResult(StreamReadFuncAspect streamReadFuncAspect, String subscribeId, Result result, Throwable throwable) {
		if(!isStarted) {
			stopFetchingNewData();
			TapLogger.debug(TAG, "handleFetchNewDataResult will end because isStarted is false. isFetchingNewData {}", isFetchingNewData.get());
			return;
		}
		if(throwable != null) {
			stopFetchingNewData();
			streamReadFuncAspect.noMoreWaitRawData(throwable);
			return;
		}
		if(result == null) {
			stopFetchingNewData();
			streamReadFuncAspect.noMoreWaitRawData(new CoreException(NetErrors.RESULT_IS_NULL, "Result/Throwable are both null in handleFetchNewDataResult, subscribeId {} offset {}", subscribeId, currentOffset));
			return;
		}
		if(result.getCode() != 1) {
			stopFetchingNewData();
			streamReadFuncAspect.noMoreWaitRawData(new CoreException(NetErrors.RESULT_CODE_FAILED, "Result code is not OK, message {}, subscribeId {} offset {}", result.getMessage(), subscribeId, currentOffset));
			return;
		}

		try {
			FetchNewDataResult fetchNewDataResult = (FetchNewDataResult) result.getMessage();
			Object nextOffset = null;
			if(fetchNewDataResult != null) {
				nextOffset = fetchNewDataResult.getOffset();
				List<MessageEntity> messages = fetchNewDataResult.getMessages();
				if(messages != null) {
					String associateId = streamReadFuncAspect.getDataProcessorContext().getPdkAssociateId();
					if(associateId != null) {
						ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
						RawDataCallbackFilterFunction function = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunction();
						RawDataCallbackFilterFunctionV2 functionV2 = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunctionV2();
						if(function != null || functionV2 != null) {
							List<TapEvent> events = new ArrayList<>();

							for(MessageEntity message : messages) {
								PDKInvocationMonitor.invoke(connectorNode, PDKMethod.RAW_DATA_CALLBACK_FILTER, () -> {
									List<TapEvent> tapEvents;
									if(functionV2 != null) {
										tapEvents = functionV2.filter(connectorNode.getConnectorContext(), streamReadFuncAspect.getTables(), message.getContent());
									} else {
										tapEvents = function.filter(connectorNode.getConnectorContext(), message.getContent());
									}
									if(tapEvents != null)
										events.addAll(tapEvents);
								}, TAG);
							}
							if(!messages.isEmpty()) {
								streamReadConsumer.accept(events, fetchNewDataResult.getOffset());
								currentOffset = fetchNewDataResult.getOffset();
							}
							synchronized (this) {
								if(!messages.isEmpty() || needFetchingNewData) {
									needFetchingNewData = false;
									TapLogger.debug(TAG, "Still need fetching new data, messages size {}, needFetchingNewData {}", messages.size(), needFetchingNewData);
									fetchNewData(subscribeId, currentOffset, (result1, throwable1) -> handleFetchNewDataResult(streamReadFuncAspect, subscribeId, result1, throwable1));
									return;
								}
							}
						}
					}
				}
			}
			synchronized (this) {
				if(needFetchingNewData) {
					needFetchingNewData = false;
					Object finalNextOffset = nextOffset;
					TapLogger.debug(TAG, "Still need fetching new data, needFetchingNewData {}", needFetchingNewData);
					fetchNewData(subscribeId, currentOffset, (result1, throwable1) -> handleFetchNewDataResult(streamReadFuncAspect, subscribeId, result1, throwable1));
				} else {
					if(stopFetchingNewData()) {
						TapLogger.debug(TAG, "Stop fetching new data as isFetchingNewData is false");
					}
				}
			}
		} catch (Throwable throwable1) {
			stopFetchingNewData();
			streamReadFuncAspect.noMoreWaitRawData(throwable1);
		}
	}

	private boolean stopFetchingNewData() {
		return isFetchingNewData.compareAndSet(true, false);
	}

	private void fetchNewData(String subscribeId, String offset, BiConsumer<Result, Throwable> biConsumer) {
		FetchNewData fetchNewData = new FetchNewData()
				.limit(1000)
				.service("engine")
				.subscribeId(subscribeId);
		if(offset == null)
			fetchNewData.taskStartTime(taskStartTime);
		else
			fetchNewData.offset(offset);
		if(isStarted) {
			TapLogger.debug(TAG, "fetchNewData subscribeId {}, offset {}, taskStartTime {}", subscribeId, offset, taskStartTime);
			proxySubscriptionManager.getImClient().sendData(new IncomingData().message(fetchNewData)).whenComplete(biConsumer);
		} else {
			TapLogger.debug(TAG, "fetchNewData will not start as task stopped subscribeId {}, offset {}, taskStartTime {}", subscribeId, offset, taskStartTime);
		}
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
								io.tapdata.pdk.core.api.Node oldNode = taskSubscribeInfo.nodeIdPDKNodeMap.put(node.getId(), connectorNode);
								if(oldNode != null) //Node will be recreated when found new table at runtime
									pdkNodeList.remove(oldNode);
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
		isStarted = true;
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
		isStarted = false;
		proxySubscriptionManager.removeTaskSubscribeInfo(taskSubscribeInfo);
		if(streamReadFuncAspect != null)
			streamReadFuncAspect.noMoreWaitRawData();
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		return super.memory(keyRegex, memoryLevel)
				.kv("taskStartTime", taskStartTime != null ? new Date(taskStartTime) : null)
				.kv("taskSubscribeInfo", taskSubscribeInfo);
	}
}
