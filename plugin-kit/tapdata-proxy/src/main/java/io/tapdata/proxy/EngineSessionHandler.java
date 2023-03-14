package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.*;
import io.tapdata.modules.api.net.message.EngineMessageResultEntity;
import io.tapdata.modules.api.proxy.data.*;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.TapEntity;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.pdk.apis.entity.message.EngineMessage;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import io.tapdata.wsserver.channels.gateway.GatewaySessionHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static io.tapdata.entity.simplify.TapSimplify.toJson;


@GatewaySession(idType = "engine")
public class EngineSessionHandler extends GatewaySessionHandler {
	private static final String TAG = EngineSessionHandler.class.getSimpleName();

	@Bean
	private ProxySubscriptionService proxySubscriptionService;

	@Override
	public Result onRequest(IncomingRequest incomingRequest) {
		return super.onRequest(incomingRequest);
	}

	@Bean
	private MessageEntityService messageEntityService;
	@Bean
	private SubscribeMap subscribeMap;
	private final TypeHandlers<TapEntity, Result> typeHandlers = new TypeHandlers<>();

	private Set<String> cachedSubscribedIds;
	private final Map<String, EngineMessageExecutor<?>> commandIdExecutorMap = new ConcurrentHashMap<>();
	public EngineSessionHandler() {
		typeHandlers.register(NodeSubscribeInfo.class, this::handleNodeSubscribeInfo);
		typeHandlers.register(FetchNewData.class, this::handleFetchNewData);
		typeHandlers.register(EngineMessageResultEntity.class, this::handleEngineMessageResultEntity);
	}

	private AtomicBoolean connected = new AtomicBoolean(false);

	private Result handleEngineMessageResultEntity(EngineMessageResultEntity engineMessageResultEntity) {
		if(engineMessageResultEntity == null) {
			return new Result().code(NetErrors.ILLEGAL_PARAMETERS).description("commandResultEntity is null");
		}
		String id = engineMessageResultEntity.getId();
		Integer code = engineMessageResultEntity.getCode();
		String message = engineMessageResultEntity.getMessage();
		Object content = engineMessageResultEntity.getContent();
		if(id == null) {
			return new Result().code(NetErrors.ILLEGAL_PARAMETERS).description("code {} or commandId {} is null");
		}
		EngineMessageExecutor<?> engineMessageExecutor = commandIdExecutorMap.get(id);
		if(code == null || code != Data.CODE_SUCCESS) {
			if(engineMessageExecutor != null)
				engineMessageExecutor.result(null, new CoreException(code == null ? NetErrors.UNKNOWN_ERROR : code, message));
			return new Result().code(code).description(message);
		}

		if(engineMessageExecutor != null) {
			try {
				if(!engineMessageExecutor.result(content, null)) {
					TapLogger.debug(TAG, "Command result was not accept successfully, maybe already handled somewhere else, id {}", id);
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
			return new Result().code(NetErrors.NO_WAITING_COMMAND).description("Command " + id + " is expired already");
		}
		return null;
	}

	public static class EngineMessageExecutor<T> implements MemoryFetcher {
		private T commandInfo;
		private volatile BiConsumer<Object, Throwable> biConsumer;
		private volatile ScheduledFuture<?> scheduledFuture;
		private Runnable doneRunnable;

		public EngineMessageExecutor(T commandInfo, BiConsumer<Object, Throwable> biConsumer, Runnable doneRunnable) {
			this.commandInfo = commandInfo;
			this.biConsumer = biConsumer;
			this.doneRunnable = doneRunnable;
		}

		public boolean result(Object result, Throwable throwable) {
			boolean done = false;
			if(cancelTimer()) {
				if(biConsumer != null) {
					synchronized (this) {
						if(biConsumer != null) {
							try {
								biConsumer.accept(result, throwable);
							} catch (Throwable throwable1) {
								TapLogger.debug(TAG, "CommandInfoExecutor commandInfo {} accept result {} failed {}", commandInfo, result, throwable1.getMessage());
							} finally {
								biConsumer = null;
								done = true;
							}
						}
					}
				}
			}
			if(done && doneRunnable != null)
				doneRunnable.run();
			return done;
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

		@Override
		public DataMap memory(String keyRegex, String memoryLevel) {
			return DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
					.kv("scheduledFuture", scheduledFuture != null ? scheduledFuture.toString() : null)
					.kv("commandInfo", commandInfo);
		}
	}
	public boolean handleCommandInfo(CommandInfo commandInfo, BiConsumer<Object, Throwable> biConsumer) {
		if(commandInfo == null || biConsumer == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "handleCommandInfo missing parameters, commandInfo {}, biConsumer {}", commandInfo, biConsumer);
		if(isChannelActive()) {
			EngineMessageExecutor<CommandInfo> engineMessageExecutor = new EngineMessageExecutor<>(commandInfo, biConsumer, () -> commandIdExecutorMap.remove(commandInfo.getId()));
			startMessageExecutor(commandInfo, engineMessageExecutor);
			if(!sendData(new OutgoingData().time(System.currentTimeMillis()).message(new CommandReceived().commandInfo(commandInfo)))) {
				engineMessageExecutor.result(null, new CoreException(NetErrors.ENGINE_CHANNEL_OFFLINE, "Engine channel is offline, please try again"));
			}
			return true;
		} else {
			return false;
//			throw new CoreException(NetErrors.ENGINE_CHANNEL_OFFLINE, "Engine is offline");
		}
	}

	private void startMessageExecutor(EngineMessage engineMessage, EngineMessageExecutor<?> engineMessageExecutor) {
		engineMessageExecutor.scheduledFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
			engineMessageExecutor.result(null, new TimeoutException("Time out"));
		}, 65, TimeUnit.SECONDS);
		EngineMessageExecutor<?> old = commandIdExecutorMap.put(engineMessage.getId(), engineMessageExecutor);
		if(old != null) {
			TapLogger.debug(TAG, "Command info id {} already exists, will use new one instead, timer will be another 30 seconds", engineMessage.getId());
			old.cancelTimer();
		}
	}

	public boolean handleServiceCaller(ServiceCaller serviceCaller, BiConsumer<Object, Throwable> biConsumer) {
		if(serviceCaller == null || biConsumer == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "handleCommandInfo missing parameters, commandInfo {}, biConsumer {}", serviceCaller, biConsumer);
		if(isChannelActive()) {
			EngineMessageExecutor<ServiceCaller> engineMessageExecutor = new EngineMessageExecutor<>(serviceCaller, biConsumer, () -> commandIdExecutorMap.remove(serviceCaller.getId()));
			startMessageExecutor(serviceCaller, engineMessageExecutor);
//			TapLogger.info(TAG, "serviceCaller {}", toJson(serviceCaller));
			if(!sendData(new OutgoingData().time(System.currentTimeMillis()).message(new ServiceCallerReceived().serviceCaller(serviceCaller)))) {
				engineMessageExecutor.result(null, new CoreException(NetErrors.ENGINE_CHANNEL_OFFLINE, "Engine channel is offline, please try again"));
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
		connected.set(true);
	}

	@Override
	public void onChannelDisconnected() {
		TapLogger.debug(TAG, "onChannelDisconnected");
		connected.set(false);
		releaseSubscribeIds();
	}

	private void releaseSubscribeIds() {
		subscribeMap.unbindSubscribeIds(this);
		cachedSubscribedIds.clear();
	}

	@Override
	public void onSessionDestroyed() {
		TapLogger.debug(TAG, "onSessionDestroyed");
		if(connected.compareAndSet(true, false)) {
			releaseSubscribeIds();
		}
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

	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
				.kv("touch", new Date(getTouch()))
				.kv("token", getToken())
				.kv("id", getId())
				.kv("userChannel", getUserChannel())
				.kv("isConnected", isChannelActive())
//				.kv("subscribeMap", subscribeMap.memory(keyRegex, memoryLevel))
				.kv("cachedSubscribedIds", cachedSubscribedIds)
				;
		DataMap commandIdExecutorMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
		dataMap.kv("commandIdExecutorMap", commandIdExecutorMap);
		for(Map.Entry<String, EngineMessageExecutor<?>> entry : this.commandIdExecutorMap.entrySet()) {
			commandIdExecutorMap.kv(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
		}
		return dataMap;
	}
}
