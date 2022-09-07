package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.modules.api.proxy.data.CommandReceived;
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
//				new CommandReceived().commandInfo(commandInfo)
			}
		}
	}
}
