package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;
import io.tapdata.wsserver.channels.health.NodeHealthManager;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

@Implementation(CommandExecutionService.class)
public class CommandExecutionServiceImpl implements CommandExecutionService {
	private static final String TAG = CommandExecutionServiceImpl.class.getSimpleName();
	@Bean
	private GatewaySessionManager gatewaySessionManager;
	@Bean
	private NodeHealthManager nodeHealthManager;
	@Bean
	private NodeConnectionFactory nodeConnectionFactory;

	@Override
	public boolean callLocal(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		return handleCommandInfoInLocal(commandInfo, biConsumer);
	}
	@Override
	public void call(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		if (handleCommandInfoInLocal(commandInfo, biConsumer)) return;
		nodeHealthManager.send(CommandInfo.class.getSimpleName(), commandInfo, new TypeHolder<Map<String, Object>>(){}, biConsumer);
//		List<String> list = new ArrayList<>();
//		Map<String, NodeHandler> healthyNodes = nodeHealthManager.getIdNodeHandlerMap();
//		if(healthyNodes != null) {
//			for(NodeHandler nodeHandler : healthyNodes.values()) {
//				NodeHealth nodeHealth = nodeHandler.getNodeHealth();
//				NodeHealth currentNodeHealth = nodeHealthManager.getCurrentNodeHealth();
//				if(!currentNodeHealth.getId().equals(nodeHealth.getId()) && nodeHealth.getOnline() != null && nodeHealth.getOnline() > 0) {
//					list.add(nodeHealth.getId());
//				}
//			}
//		}
//
//		Throwable error = null;
//		if(!list.isEmpty()) {
//			RandomDraw randomDraw = new RandomDraw(list.size());
//			int index;
//			while((index = randomDraw.next()) != -1) {
//				String id = list.get(index);
//				NodeConnection nodeConnection = nodeConnectionFactory.getNodeConnection(id);
//				if(nodeConnection != null && nodeConnection.isReady()) {
//					try {
//						//noinspection unchecked
//						Map<String, Object> response = nodeConnection.send(CommandInfo.class.getSimpleName(), commandInfo, Map.class);
//						biConsumer.accept(response, null);
//						return;
//					} catch (IOException ioException) {
//						TapLogger.debug(TAG, "Send to nodeId {} failed {} and will try next, command {}", id, ioException.getMessage(), commandInfo);
//						error = ioException;
//					}
//				}
//			}
//		}
//
//		if(error != null) {
//			biConsumer.accept(null, error);
//		} else {
//			throw new CoreException(NetErrors.NO_AVAILABLE_ENGINE, "No available engine to run this command");
//		}
	}

	private boolean handleCommandInfoInLocal(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		Collection<GatewaySessionHandler> gatewaySessionHandlers = gatewaySessionManager.getUserIdGatewaySessionHandlerMap().values();
		for(GatewaySessionHandler gatewaySessionHandler : gatewaySessionHandlers) {
			if(gatewaySessionHandler instanceof EngineSessionHandler) {
				EngineSessionHandler engineSessionHandler = (EngineSessionHandler) gatewaySessionHandler;
				//TODO choose any engine, but should not be the one all the time. if no engine in this proxy, should find one from other proxies.
				if(engineSessionHandler.handleCommandInfo(commandInfo, biConsumer))
					return true;
			}
		}
		return false;
	}
}
