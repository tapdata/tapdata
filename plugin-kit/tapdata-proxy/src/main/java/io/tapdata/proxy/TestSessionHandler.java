package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.IncomingMessage;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.proxy.data.FetchNewData;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.modules.api.proxy.data.TestItem;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;
import io.tapdata.wsserver.channels.gateway.modules.GatewayChannelModule;

import java.util.Set;

import static io.tapdata.entity.simplify.TapSimplify.*;


@GatewaySession(idType = "test")
public class TestSessionHandler extends GatewaySessionHandler {
	private static final String TAG = TestSessionHandler.class.getSimpleName();

	private final TypeHandlers<TapEntity, Result> typeHandlers = new TypeHandlers<>();

	@Bean
	private GatewayChannelModule gatewayChannelModule;

	public TestSessionHandler() {
		typeHandlers.register(NodeSubscribeInfo.class, this::handleNodeSubscribeInfo);
		typeHandlers.register(FetchNewData.class, this::handleFetchNewData);
		typeHandlers.register(TestItem.class, this::handleTestItem);
	}

	private Result handleTestItem(TestItem testItem) {
		switch (testItem.getAction()) {
			case "kick":
				gatewayChannelModule.close(getId(), 1234, "kicked as my wish");
				break;
			case "error":
				throw new IllegalArgumentException("This is illegal");
			case "normal":
				return null;
			case "normal1":
				return new Result().code(1).description("my description").serverId("serverid").time(System.currentTimeMillis()).message(new TestItem().action("oops"));
		}
		return null;
	}

	private Result handleFetchNewData(FetchNewData fetchNewData) {
		TapLogger.debug(TAG, "handleFetchNewData {}", fetchNewData);
		FetchNewDataResult fetchNewDataResult = new FetchNewDataResult().offset("adf")
				.messages(list(
						new MessageEntity().service("test").subscribeId("aaaa").content(map(entry("aaa", 1), entry("vbb", "df"))),
						new MessageEntity().service("test").subscribeId("aaaa").content(map(entry("aaa", 1), entry("vbb", "df"))))
				);
		return new Result().code(Data.CODE_SUCCESS).message(fetchNewDataResult);
	}

	private Result handleNodeSubscribeInfo(NodeSubscribeInfo nodeSubscribeInfo) {
		TapLogger.debug(TAG, "handleNodeSubscribeInfo {}", nodeSubscribeInfo);
		if(nodeSubscribeInfo.getSubscribeIds() == null || nodeSubscribeInfo.getSubscribeIds().isEmpty())
			throw new IllegalArgumentException("not subscribeIds");
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
