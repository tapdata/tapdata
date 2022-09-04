package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;


@GatewaySession(idType = "engine")
public class MyGatewaySessionHandler extends GatewaySessionHandler {
	private static final String TAG = MyGatewaySessionHandler.class.getSimpleName();

	@Bean
	private ProxySubscriptionService proxySubscriptionService;
	private final TypeHandlers<TapEntity, Result> typeHandlers = new TypeHandlers<>();
	public MyGatewaySessionHandler() {
		typeHandlers.register(NodeSubscribeInfo.class, this::handleNodeSubscribeInfo);
	}

	private Result handleNodeSubscribeInfo(NodeSubscribeInfo nodeSubscribeInfo) {
		String nodeId = CommonUtils.getProperty("tapdata_node_id");
		proxySubscriptionService.syncProxySubscription(new ProxySubscription().service("engine").nodeId(nodeId).subscribeIds(nodeSubscribeInfo.getSubscribeIds()));
		return null;
	}

	@Override
	public void onSessionCreated() {
		TapLogger.debug(TAG, "onSessionCreated id {} token {} ip {}", getId(), getToken(), getUserChannel().getIp());
	}

	@Override
	public void onChannelConnected() {
		TapLogger.debug(TAG, "onChannelConnected");
	}

	@Override
	public void onChannelDisconnected() {
		TapLogger.debug(TAG, "onChannelDisconnected");
	}

	@Override
	public void onSessionDestroyed() {
		TapLogger.debug(TAG, "onSessionDestroyed");
	}

	@Override
	public Result onDataReceived(IncomingData data) {
		TapLogger.debug(TAG, "onDataReceived {}", data);
		return typeHandlers.handle(data.getMessage());
	}

	@Override
	public Result onMessageReceived(IncomingMessage message) {
		TapLogger.debug(TAG, "onMessageReceived {}", message);
		return null;
	}

}
