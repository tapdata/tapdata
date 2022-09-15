package io.tapdata.proxy;

import io.netty.handler.codec.http.HttpUtil;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.pdk.core.utils.RandomDraw;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;
import io.tapdata.wsserver.channels.health.NodeHandler;
import io.tapdata.wsserver.channels.health.NodeHealthManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

@Implementation(CommandExecutionService.class)
public class CommandExecutionServiceImpl implements CommandExecutionService {
	@Bean
	private GatewaySessionManager gatewaySessionManager;
	@Bean
	private NodeHealthManager nodeHealthManager;

	@Override
	public void call(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		Collection<GatewaySessionHandler> gatewaySessionHandlers = gatewaySessionManager.getUserIdGatewaySessionHandlerMap().values();
		for(GatewaySessionHandler gatewaySessionHandler : gatewaySessionHandlers) {
			if(gatewaySessionHandler instanceof EngineSessionHandler) {
				EngineSessionHandler engineSessionHandler = (EngineSessionHandler) gatewaySessionHandler;
				//TODO choose any engine, but should not be the one all the time. if no engine in this proxy, should find one from other proxies.
				if(engineSessionHandler.handleCommandInfo(commandInfo, biConsumer))
					return;
			}
		}

		List<String> list = new ArrayList<>();
		Map<String, NodeHandler> healthyNodes = nodeHealthManager.getIdNodeHandlerMap();
		if(healthyNodes != null) {
			for(NodeHandler nodeHandler : healthyNodes.values()) {
				NodeHealth nodeHealth = nodeHandler.getNodeHealth();
				NodeHealth currentNodeHealth = nodeHealthManager.getCurrentNodeHealth();
				if(!currentNodeHealth.getId().equals(nodeHealth.getId()) && nodeHealth.getHealth() > 0) {
					list.add(nodeHealth.getId());
				}
			}
		}
		RandomDraw randomDraw = new RandomDraw(list.size());
		int index;
		while((index = randomDraw.next()) != -1) {
			String id = list.get(index);

		}

		throw new CoreException(NetErrors.NO_AVAILABLE_ENGINE, "No available engine to run this command");
	}
}
