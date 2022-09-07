package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.proxy.data.FetchNewData;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;

import java.util.Set;


@GatewaySession(idType = "engine")
public class EngineSessionHandler extends GatewaySessionHandler {
	private static final String TAG = EngineSessionHandler.class.getSimpleName();

	@Bean
	private ProxySubscriptionService proxySubscriptionService;
	@Bean
	private MessageEntityService messageEntityService;
	@Bean
	private SubscribeMap subscribeMap;
	private final TypeHandlers<TapEntity, Result> typeHandlers = new TypeHandlers<>();

	private Set<String> cachedSubscribedIds;
	public EngineSessionHandler() {
		typeHandlers.register(NodeSubscribeInfo.class, this::handleNodeSubscribeInfo);
		typeHandlers.register(FetchNewData.class, this::handleFetchNewData);
	}

	private Result handleFetchNewData(FetchNewData fetchNewData) {
		String offset = fetchNewData.getOffset();
		if(offset == null) {
			Long startTime = fetchNewData.getTaskStartTime();
			if(startTime != null) {
				offset = messageEntityService.getOffsetByTimestamp(startTime);
			}
		}
		FetchNewDataResult fetchNewDataResult = messageEntityService.getMessageEntityList(fetchNewData.getService(), fetchNewData.getSubscribeId(), offset, fetchNewData.getLimit());
		return new Result().code(Data.CODE_SUCCESS).message(fetchNewDataResult);
	}

	private Result handleNodeSubscribeInfo(NodeSubscribeInfo nodeSubscribeInfo) {
		String nodeId = CommonUtils.getProperty("tapdata_node_id");
		proxySubscriptionService.syncProxySubscription(new ProxySubscription().service("engine").nodeId(nodeId).subscribeIds(nodeSubscribeInfo.getSubscribeIds()));
		Set<String> newSubscribedIds = nodeSubscribeInfo.getSubscribeIds();
		cachedSubscribedIds = subscribeMap.rebindSubscribeIds(this, newSubscribedIds, cachedSubscribedIds);
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
