package io.tapdata.proxy.connection;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.node.NodeRegistryService;
import io.tapdata.modules.api.net.service.node.connection.NodeConnection;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.modules.api.net.service.node.connection.Receiver;
import io.tapdata.wsserver.channels.health.NodeHandler;
import io.tapdata.wsserver.channels.health.NodeHealthManager;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiConsumer;

@Implementation(NodeConnectionFactory.class)
public class NodeConnectionFactoryHttpImpl implements NodeConnectionFactory {
	private static final String TAG = NodeConnectionFactoryHttpImpl.class.getSimpleName();
	private final Map<String, ReceiverEntity<?, ?>> typeNodeIdBiFunctionMap = new ConcurrentHashMap<>();
	private final Map<String, NodeConnection> nodeIdConnectionMap = new ConcurrentHashMap<>();

	private final Set<String> disconnectedNodeIds = new ConcurrentSkipListSet<>();
	@Bean
	private NodeRegistryService nodeRegistryService;
	@Bean
	private NodeHealthManager nodeHealthManager;
	public NodeConnectionFactoryHttpImpl() {
	}

	@Override
	public NodeConnection getNodeConnection(String nodeId) {
		NodeConnection connection = nodeIdConnectionMap.get(nodeId);
		if(connection != null)
			return connection;
		return nodeIdConnectionMap.computeIfAbsent(nodeId, nodeId1 -> {
			NodeConnection nodeConnection = new NodeConnectionHttpImpl();
			NodeHandler nodeHandler = nodeHealthManager.getAliveNode(nodeId1);
			NodeRegistry theNodeRegistry = null;
			if(nodeHandler == null || nodeHandler.getNodeRegistry() == null) {
				theNodeRegistry = nodeRegistryService.get(nodeId1);
			} else {
				theNodeRegistry = nodeHandler.getNodeRegistry();
			}
			if(theNodeRegistry != null) {
				nodeConnection.init(theNodeRegistry, (nodeRegistry, reason) -> {
					if(nodeIdConnectionMap.remove(nodeRegistry.id()) != null) {
						disconnectedNodeIds.add(nodeRegistry.id());
						TapLogger.debug(TAG, "NodeConnection for nodeId {} has been removed, reason {}", reason);
					}
				});
				return nodeConnection;
			} else {
				disconnectedNodeIds.add(nodeId1);
				TapLogger.debug(TAG, "NodeId {} is not alive while create node connection, nodeHandler {}", nodeId1, nodeHandler);
			}
			return null;
		});
	}

	@Override
	public boolean isDisconnected(String nodeId) {
		return disconnectedNodeIds.contains(nodeId);
	}

	@Override
	public NodeConnection removeNodeConnection(String nodeId) {
		return nodeIdConnectionMap.remove(nodeId);
	}

	@Override
	public <Request, Response> void registerReceiver(String type, Receiver<Response, Request> receiver) {
		typeNodeIdBiFunctionMap.put(type, new ReceiverEntity<>(receiver));
	}

	@Override
	public void received(String nodeId, String type, Byte encode, byte[] data, BiConsumer<Object, Throwable> biConsumer) {
		if(type == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "Missing type while receiving request");
		if(nodeId == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "Missing nodeId while receiving request");

		if(disconnectedNodeIds.contains(nodeId)) {
			disconnectedNodeIds.remove(nodeId);
			TapLogger.debug(TAG, "Received nodeId {}, remove it from disconnected list, start connecting...");
			getNodeConnection(nodeId);
		}
		//noinspection rawtypes
		ReceiverEntity receiverEntity = typeNodeIdBiFunctionMap.get(type);
		if(receiverEntity == null)
			throw new CoreException(NetErrors.NO_FUNCTION_ON_TYPE, "No receiver on type {} while receiving request from nodeId {}", type, nodeId);

		//noinspection unchecked
		receiverEntity.receive(nodeId, encode, data, biConsumer);
	}

	public static void main(String[] args) {
		NodeConnectionFactory nodeConnectionFactory = InstanceFactory.instance(NodeConnectionFactory.class);
		nodeConnectionFactory.registerReceiver("aaa", new Receiver<NodeConnectionFactory, String>() {
			@Override
			public void received(String nodeId, String s, BiConsumer<Object, Throwable> biConsumer) {
			}
		});

		nodeConnectionFactory.registerReceiver("aaa", (Receiver<ReceiverEntity, Proxy>) (nodeId, proxy, biConsumer) -> {});

	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
				.kv("disconnectedNodeIds", disconnectedNodeIds)
				;
		DataMap typeNodeIdBiFunctionMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
//		dataMap.kv("typeNodeIdBiFunctionMap", typeNodeIdBiFunctionMap);
//		for(Map.Entry<String, ReceiverEntity<?, ?>> entry : this.typeNodeIdBiFunctionMap.entrySet()) {
//			typeNodeIdBiFunctionMap.kv(entry.getKey(), entry.getValue());
//		}
		DataMap nodeIdConnectionMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
		dataMap.kv("nodeIdConnectionMap", nodeIdConnectionMap);
		for(Map.Entry<String, NodeConnection> entry : this.nodeIdConnectionMap.entrySet()) {
			nodeIdConnectionMap.kv(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
		}
		return dataMap;
	}
}
