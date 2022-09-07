package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.modules.api.net.message.CommandResultEntity;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.proxy.data.CommandReceived;
import io.tapdata.modules.api.proxy.data.FetchNewData;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;


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
		typeHandlers.register(CommandReceived.class, this::handleCommandReceived);
	}

	public void handleCommandInfo(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		sendData(new OutgoingData().time(System.currentTimeMillis()).message(new CommandReceived().commandInfo(commandInfo)));
	}

	private Result handleCommandReceived(CommandReceived commandReceived) {
		if(commandReceived == null || commandReceived.getCommandInfo() == null)
			return new Result().code(NetErrors.COMMAND_RECEIVED_ILLEGAL).description("illegal arguments");
		CommandInfo commandInfo = commandReceived.getCommandInfo();;
		ConnectionNode connectionNode = PDKIntegration.createConnectionConnectorBuilder()
				.withConnectionConfig(new DataMap() {{
					commandInfo.getConnectionConfig();
				}})
				.withGroup(commandInfo.getGroup())
				.withPdkId(commandInfo.getPdkId())
				.withAssociateId(UUID.randomUUID().toString())
				.withVersion(commandInfo.getVersion())
				.build();
		CommandCallbackFunction commandCallbackFunction = connectionNode.getConnectionFunctions().getCommandCallbackFunction();
		if(commandCallbackFunction == null) {
			return new Result().code(NetErrors.PDK_NOT_SUPPORT_COMMAND_CALLBACK).description("pdkId " + commandInfo.getPdkId() + " doesn't support CommandCallbackFunction");
		}
		AtomicReference<CommandResultEntity> mapAtomicReference = new AtomicReference<>();
		PDKInvocationMonitor.invoke(connectionNode, PDKMethod.CONNECTION_TEST,
				() -> mapAtomicReference.set(new CommandResultEntity().content(commandCallbackFunction.filter(connectionNode.getConnectionContext(), commandInfo))), TAG);
		return new Result().code(Data.CODE_SUCCESS).message(mapAtomicReference.get());
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
