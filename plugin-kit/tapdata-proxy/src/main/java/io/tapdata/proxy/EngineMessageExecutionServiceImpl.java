package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.net.service.node.connection.NodeConnection;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.modules.api.net.service.EngineMessageExecutionService;
import io.tapdata.pdk.apis.entity.message.EngineMessage;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.RandomDraw;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;
import io.tapdata.wsserver.channels.health.NodeHandler;
import io.tapdata.wsserver.channels.health.NodeHealthManager;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Implementation(EngineMessageExecutionService.class)
public class EngineMessageExecutionServiceImpl implements EngineMessageExecutionService, MemoryFetcher {
	private static final String TAG = EngineMessageExecutionServiceImpl.class.getSimpleName();
	@Bean
	private GatewaySessionManager gatewaySessionManager;
	@Bean
	private NodeHealthManager nodeHealthManager;
	@Bean
	private NodeConnectionFactory nodeConnectionFactory;

	@Bean
	private SubscribeMap subscribeMap;
	@Bean
	private ProxySubscriptionService proxySubscriptionService;
	private final Map<String, EngineMessageCollector> messageCollectorMap = new ConcurrentHashMap<>();

	public EngineMessageExecutionServiceImpl() {
		PDKIntegration.registerMemoryFetcher(EngineMessageExecutionServiceImpl.class.getSimpleName(), this);
	}
	@Override
	public void callLocal(EngineMessage engineMessage, BiConsumer<Object, Throwable> biConsumer) {
		BiConsumer<Object, Throwable> newConsumer = wrapEngineMessageConsumer(engineMessage, biConsumer, false);
		boolean bool = handleEngineMessageInLocal(engineMessage, newConsumer);
		if(!bool) {
			newConsumer.accept(null, new CoreException(NetErrors.ENGINE_MESSAGE_CALL_LOCAL_FAILED, "Call failed, no session available"));
		}
	}
	@Override
	public void call(EngineMessage engineMessage, BiConsumer<Object, Throwable> biConsumer) {
		BiConsumer<Object, Throwable> newConsumer = wrapEngineMessageConsumer(engineMessage, biConsumer, true);
		if (handleEngineMessageInLocal(engineMessage, newConsumer)) return;

		String currentNodeId = CommonUtils.getProperty("tapdata_node_id");
		Set<String> subscribeIds = engineMessage.getSubscribeIds();
		List<Set<String>> orSubscribeIds = engineMessage.getOrSubscribeIds();

		if((subscribeIds == null || subscribeIds.isEmpty()) && (orSubscribeIds == null || orSubscribeIds.isEmpty())) {
			send(engineMessage.getClass().getSimpleName(), engineMessage, Object.class, newConsumer, null);
		} else {
			List<String> nodeIds = proxySubscriptionService.subscribedNodeIdsByAll("engine", subscribeIds, orSubscribeIds);
			nodeIds.remove(currentNodeId);
			send(engineMessage.getClass().getSimpleName(), engineMessage, Object.class, newConsumer, nodeIds);
		}

	}

	private BiConsumer<Object, Throwable> wrapEngineMessageConsumer(EngineMessage engineMessage, BiConsumer<Object, Throwable> biConsumer, boolean startPlace) {
		String key = engineMessage.key();
		engineMessage.internalRequest(!startPlace);
		EngineMessageCollector collector = messageCollectorMap.get(key);
		if(collector == null) {
			collector = messageCollectorMap.computeIfAbsent(engineMessage.key(), s -> new EngineMessageCollector());
		}
		final String invokeId = CommonUtils.processUniqueId();
		collector.getInvokeIdEngineMessageMap().put(invokeId, engineMessage);
		EngineMessageCollector finalCollector = collector;
		return (o, throwable) -> {
			biConsumer.accept(o, throwable);

			EngineMessage engineMessage1 = finalCollector.getInvokeIdEngineMessageMap().remove(invokeId);
			if(engineMessage1 != null) {
				finalCollector.getCounter().increment();
				long takes = System.currentTimeMillis() - engineMessage1.getCreateTime();
				finalCollector.getTotalTakes().add(takes);

				Integer requestBytes = engineMessage.getRequestBytes();
				if(requestBytes != null)
					finalCollector.getRequestBytes().add(requestBytes);
				Integer responseBytes = engineMessage.getResponseBytes();
				if(responseBytes != null)
					finalCollector.getResponseBytes().add(responseBytes);
				if(throwable != null)
					finalCollector.lastError(throwable);
//				if(error != null && logTag != null) {
//					TapLogger.info(logTag, "methodEnd - {} | message - ({})", method, error.getMessage());//ExceptionUtils.getStackTrace(error)
//					//throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, error.getMessage(), error);
//				} else {
////                    TapLogger.info(logTag, "methodEnd {} invokeId {} successfully, message {} takes {}", method, invokeId, message, takes);
//				}
//				return takes;
			}
			TapLogger.info(TAG, "{}call {} {}, subscribeIds {}, request length {}, response length {}, takes {}{}", (startPlace ? "External " : "Internal "), engineMessage.key(), (throwable != null ? "failed, " + throwable.getMessage() : "successfully"), engineMessage.getSubscribeIds(), engineMessage.getRequestBytes(), engineMessage.getResponseBytes(), System.currentTimeMillis() - engineMessage.getCreateTime(), (engineMessage.getOtherTMIpPort() != null ? " on another TM " + engineMessage.getOtherTMIpPort() : ""));
		};
	}

	private boolean handleEngineMessageInLocal(EngineMessage engineMessage, BiConsumer<Object, Throwable> biConsumer) {
		Set<String> subscribeIds = engineMessage.getSubscribeIds();
		List<Set<String>> orSubscribeIdsList = engineMessage.getOrSubscribeIds();

		if((subscribeIds == null || subscribeIds.isEmpty()) && (orSubscribeIdsList == null || orSubscribeIdsList.isEmpty())) {
			//No subscribeIds, send EngineMessage to any engine which is alive.
			//TODO should use RandomDraw to random the call.
			Collection<GatewaySessionHandler> gatewaySessionHandlers = gatewaySessionManager.getUserIdGatewaySessionHandlerMap().values();
			for(GatewaySessionHandler gatewaySessionHandler : gatewaySessionHandlers) {
				if(gatewaySessionHandler instanceof EngineSessionHandler) {
					EngineSessionHandler engineSessionHandler = (EngineSessionHandler) gatewaySessionHandler;
					boolean bool = executeEngineMessage(engineMessage, biConsumer, engineSessionHandler);
					if(bool)
						return true;
				}
			}
		} else {
			//Has subscribeIds, send EngineMessage to whom subscribed the ids.
			//TODO should use RandomDraw to random the call. otherwise the first one will be called always.
			if ((subscribeIds != null && !subscribeIds.isEmpty()) && executeBySubscribeIds(engineMessage, biConsumer, subscribeIds)) return true;
			if(orSubscribeIdsList != null) {
				for(Set<String> orSubscribeIds : orSubscribeIdsList) {
					if ((orSubscribeIds != null && !orSubscribeIds.isEmpty()) && executeBySubscribeIds(engineMessage, biConsumer, orSubscribeIds)) return true;
				}
			}
		}
		return false;
	}

	private boolean executeBySubscribeIds(EngineMessage engineMessage, BiConsumer<Object, Throwable> biConsumer, Set<String> subscribeIds) {
		Map<EngineSessionHandler, List<String>> orMap = subscribeMap.getSessionSubscribeIdsMapByAll(subscribeIds);
		if(orMap != null && !orMap.isEmpty()) {
			Set<EngineSessionHandler> handlers = orMap.keySet();
			for(EngineSessionHandler engineSessionHandler : handlers) {
				boolean bool = executeEngineMessage(engineMessage, biConsumer, engineSessionHandler);
				if(bool)
					return true;
			}
		}
		return false;
	}

	private boolean executeEngineMessage(EngineMessage engineMessage, BiConsumer<Object, Throwable> biConsumer, EngineSessionHandler engineSessionHandler) {
		boolean bool = false;
		if(engineMessage instanceof CommandInfo) {
			bool = engineSessionHandler.handleCommandInfo((CommandInfo) engineMessage, biConsumer);
		} else if(engineMessage instanceof ServiceCaller) {
			bool = engineSessionHandler.handleServiceCaller((ServiceCaller) engineMessage, biConsumer);
		} else {
			TapLogger.debug(TAG, "No handler for EngineMessage {}", engineMessage);
		}
		return bool;
	}


	public <T> void send(String type, EngineMessage engineMessage, TypeHolder<T> typeHolder, BiConsumer<T, Throwable> biConsumer) {
		//noinspection unchecked
		send(type, engineMessage, typeHolder.getType(), biConsumer, null);
	}
	public <T> void send(String type, EngineMessage engineMessage, Type tClass, BiConsumer<T, Throwable> biConsumer, List<String> list) {
		if(list == null) {
			list = new ArrayList<>();
			Map<String, NodeHandler> healthyNodes = nodeHealthManager.getIdNodeHandlerMap();
			if(healthyNodes != null) {
				for(NodeHandler nodeHandler : healthyNodes.values()) {
					NodeHealth nodeHealth = nodeHandler.getNodeHealth();
					NodeHealth currentNodeHealth = nodeHealthManager.getCurrentNodeHealth();
					if(!currentNodeHealth.getId().equals(nodeHealth.getId()) && nodeHealth.getOnline() != null && nodeHealth.getOnline() > 0) {
						list.add(nodeHealth.getId());
					}
				}
			}
		}

		Throwable error = null;
		if(!list.isEmpty()) {
			RandomDraw randomDraw = new RandomDraw(list.size());
			int index;
			while ((index = randomDraw.next()) != -1) {
				String id = list.get(index);
				NodeConnection nodeConnection = nodeConnectionFactory.getNodeConnection(id);
				if (nodeConnection != null && nodeConnection.isReady() && nodeHealthManager.getAliveNode(id) != null) {
					try {
						engineMessage.setOtherTMIpPort(nodeConnection.getWorkingIpPort());
						//noinspection unchecked
						T response = nodeConnection.send(type, engineMessage, tClass);
						biConsumer.accept(response, null);
						return;
					} catch (Throwable ioException) {
						TapLogger.info(TAG, "Send to nodeId {} failed {} and will try next, command {}", id, ioException.getMessage(), engineMessage);
						error = ioException;
					}
				}
			}
		}
		if(error != null) {
			biConsumer.accept(null, error);
		} else {
			biConsumer.accept(null, new CoreException(NetErrors.NO_AVAILABLE_ENGINE, "No available engine from list {}", list));
		}
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
		for(Map.Entry<String, EngineMessageCollector> entry : messageCollectorMap.entrySet()) {
			if(keyRegex != null && !keyRegex.isEmpty() && !keyRegex.contains(entry.getKey()))
				continue;
			dataMap.kv(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
		}
		return dataMap;
	}
}
