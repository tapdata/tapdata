package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.node.connection.NodeConnection;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.pdk.core.utils.RandomDraw;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;
import io.tapdata.wsserver.channels.health.NodeHandler;
import io.tapdata.wsserver.channels.health.NodeHealthManager;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
		send(CommandInfo.class.getSimpleName(), commandInfo, new TypeHolder<Map<String, Object>>(){}, biConsumer);
	}

	private boolean handleCommandInfoInLocal(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		Collection<GatewaySessionHandler> gatewaySessionHandlers = gatewaySessionManager.getUserIdGatewaySessionHandlerMap().values();
		for(GatewaySessionHandler gatewaySessionHandler : gatewaySessionHandlers) {
			if(gatewaySessionHandler instanceof EngineSessionHandler) {
				EngineSessionHandler engineSessionHandler = (EngineSessionHandler) gatewaySessionHandler;
				if(engineSessionHandler.handleCommandInfo(commandInfo, biConsumer))
					return true;
			}
		}
		return false;
	}


	public <T> void send(String type, CommandInfo commandInfo, TypeHolder<T> typeHolder, BiConsumer<T, Throwable> biConsumer) {
		//noinspection unchecked
		send(type, commandInfo, typeHolder.getType(), biConsumer);
	}
	public <T> void send(String type, CommandInfo commandInfo, Type tClass, BiConsumer<T, Throwable> biConsumer) {
		List<String> list = new ArrayList<>();
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

		Throwable error = null;
		if(!list.isEmpty()) {
			RandomDraw randomDraw = new RandomDraw(list.size());
			int index;
			while ((index = randomDraw.next()) != -1) {
				String id = list.get(index);
				NodeConnection nodeConnection = nodeConnectionFactory.getNodeConnection(id);
				if (nodeConnection != null && nodeConnection.isReady()) {
					try {
						//noinspection unchecked
						T response = nodeConnection.send(type, commandInfo, tClass);
						biConsumer.accept(response, null);
						return;
					} catch (IOException ioException) {
						TapLogger.debug(TAG, "Send to nodeId {} failed {} and will try next, command {}", id, ioException.getMessage(), commandInfo);
						error = ioException;
					}
				}
			}
		}
		if(error != null) {
			biConsumer.accept(null, error);
		} else {
			biConsumer.accept(null, new CoreException(NetErrors.NO_AVAILABLE_ENGINE, "No available engine"));
		}
	}
}
