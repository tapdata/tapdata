package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

@Implementation(CommandExecutionService.class)
public class CommandExecutionServiceImpl implements CommandExecutionService {
	@Bean
	private GatewaySessionManager gatewaySessionManager;

	@Override
	public void call(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		Collection<GatewaySessionHandler> gatewaySessionHandlers = gatewaySessionManager.getUserIdGatewaySessionHandlerMap().values();
		for(GatewaySessionHandler gatewaySessionHandler : gatewaySessionHandlers) {
			if(gatewaySessionHandler instanceof EngineSessionHandler) {
				EngineSessionHandler engineSessionHandler = (EngineSessionHandler) gatewaySessionHandler;
				//TODO choose any engine, but should not be the one all the time. if no engine in this proxy, should find one from other proxies.
				engineSessionHandler.handleCommandInfo(commandInfo, biConsumer);
				return;
			}
		}
		throw new CoreException(NetErrors.NO_AVAILABLE_ENGINE, "No available engine to run this command");
	}
}
