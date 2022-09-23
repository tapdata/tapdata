package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
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
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
	private final Map<String, CommandInfoExecutor> commandIdExecutorMap = new ConcurrentHashMap<>();
	public EngineSessionHandler() {
		typeHandlers.register(NodeSubscribeInfo.class, this::handleNodeSubscribeInfo);
		typeHandlers.register(FetchNewData.class, this::handleFetchNewData);
		typeHandlers.register(CommandResultEntity.class, this::handleCommandResultEntity);
	}

	private Result handleCommandResultEntity(CommandResultEntity commandResultEntity) {
		if(commandResultEntity == null) {
			return new Result().code(NetErrors.ILLEGAL_PARAMETERS).description("commandResultEntity is null");
		}
		String commandId = commandResultEntity.getCommandId();
		Integer code = commandResultEntity.getCode();
		String message = commandResultEntity.getMessage();
		Map<String, Object> content = commandResultEntity.getContent();
		if(commandId == null) {
			return new Result().code(NetErrors.ILLEGAL_PARAMETERS).description("code {} or commandId {} is null");
		}
		CommandInfoExecutor commandInfoExecutor = commandIdExecutorMap.get(commandId);
		if(code == null || code != Data.CODE_SUCCESS) {
			if(commandInfoExecutor != null)
				commandInfoExecutor.result(null, new CoreException(code, message));
			return new Result().code(code).description(message);
		}

		if(commandInfoExecutor != null) {
			try {
				if(!commandInfoExecutor.result(content, null)) {
					TapLogger.debug(TAG, "Command result was not accept successfully, maybe already handled somewhere else, id {}", commandId);
				}
			} catch (Throwable throwable) {
				int resultCode = NetErrors.CONSUME_COMMAND_RESULT_FAILED;
				if(throwable instanceof CoreException) {
					CoreException coreException = (CoreException) throwable;
					resultCode = coreException.getCode();
				}
				return new Result().code(resultCode).description("Consumer command result failed, " + throwable.getMessage());
			}
		} else {
			return new Result().code(NetErrors.NO_WAITING_COMMAND).description("Command " + commandId + " is expired already");
		}
		return null;
	}

	public static class CommandInfoExecutor {
		private CommandInfo commandInfo;
		private volatile BiConsumer<Map<String, Object>, Throwable> biConsumer;
		private volatile ScheduledFuture<?> scheduledFuture;
		public CommandInfoExecutor(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
			this(commandInfo, biConsumer, null);
		}
		public CommandInfoExecutor(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer, ScheduledFuture<?> scheduledFuture) {
			this.commandInfo = commandInfo;
			this.biConsumer = biConsumer;
			this.scheduledFuture = scheduledFuture;
		}

		public boolean result(Map<String, Object> result, Throwable throwable) {
			if(cancelTimer()) {
				if(biConsumer != null) {
					synchronized (this) {
						if(biConsumer != null) {
							biConsumer.accept(result, throwable);
							biConsumer = null;
							return true;
						}
					}
				}
			}
			return false;
		}

		public boolean cancelTimer() {
			if(scheduledFuture != null) {
				synchronized (this) {
					if(scheduledFuture != null) {
						scheduledFuture.cancel(true);
						scheduledFuture = null;
						return true;
					}
				}
			}
			return false;
		}
	}
	public boolean handleCommandInfo(CommandInfo commandInfo, BiConsumer<Map<String, Object>, Throwable> biConsumer) {
		if(commandInfo == null || biConsumer == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "handleCommandInfo missing parameters, commandInfo {}, biConsumer {}", commandInfo, biConsumer);
		if(isChannelActive()) {
			CommandInfoExecutor commandInfoExecutor = new CommandInfoExecutor(commandInfo, biConsumer);
			commandInfoExecutor.scheduledFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
				commandInfoExecutor.result(null, new TimeoutException("Time out"));
			}, 30, TimeUnit.SECONDS);
			CommandInfoExecutor old = commandIdExecutorMap.put(commandInfo.getId(), commandInfoExecutor);
			if(old != null) {
				TapLogger.debug(TAG, "Command info id {} already exists, will use new one instead, timer will be another 30 seconds", commandInfo.getId());
				old.cancelTimer();
			}
			if(!sendData(new OutgoingData().time(System.currentTimeMillis()).message(new CommandReceived().commandInfo(commandInfo)))) {
				commandInfoExecutor.result(null, new CoreException(NetErrors.ENGINE_CHANNEL_OFFLINE, "Engine channel is offline, please try again"));
			}
			return true;
		} else {
			return false;
//			throw new CoreException(NetErrors.ENGINE_CHANNEL_OFFLINE, "Engine is offline");
		}
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
