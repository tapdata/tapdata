package io.tapdata.proxy.client;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.tapdata.constant.ConfigurationCenter;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.CommandResultEntity;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.modules.api.proxy.data.CommandReceived;
import io.tapdata.modules.api.proxy.data.NewDataReceived;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.timer.MaxFrequencyLimiter;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.IMClientBuilder;
import io.tapdata.wsclient.modules.imclient.impls.websocket.ChannelStatus;
import io.tapdata.wsclient.utils.EventManager;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Bean
public class ProxySubscriptionManager implements MemoryFetcher {
	private static final String TAG = ProxySubscriptionManager.class.getSimpleName();
	private final ConcurrentHashSet<TaskSubscribeInfo> taskSubscribeInfos = new ConcurrentHashSet<>();
	private ConcurrentHashMap<String, List<TaskSubscribeInfo>> typeConnectionIdSubscribeInfosMap = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, TaskSubscribeInfo> taskIdTaskSubscribeInfoMap = new ConcurrentHashMap<>();
//	private ScheduledFuture<?> workingFuture;
//	private final AtomicBoolean needSync = new AtomicBoolean(false);
	private IMClient imClient;

	private MaxFrequencyLimiter maxFrequencyLimiter;

	public ProxySubscriptionManager() {
//		String nodeId = CommonUtils.getProperty("tapdata_node_id");
//		if(nodeId == null)
//			throw new CoreException(NetErrors.CURRENT_NODE_ID_NOT_FOUND, "Current nodeId is not found");
//		proxySubscription = new ProxySubscription().nodeId(nodeId).service("engine");
		maxFrequencyLimiter = new MaxFrequencyLimiter(500, this::syncSubscribeIds);
	}
	public void startIMClient(List<String> baseURLs, String accessToken) {
		if(imClient == null) {
			synchronized (this) {
				if(imClient == null) {
					List<String> newBaseUrls = new ArrayList<>();
					for(String baseUrl : baseURLs) {
						if(!baseUrl.endsWith("/"))
							baseUrl = baseUrl + "/";
						newBaseUrls.add(baseUrl + "proxy?access_token=" + accessToken);
					}
					imClient = new IMClientBuilder()
							.withBaseUrl(newBaseUrls)
							.withService("engine")
							.withPrefix("e")
							.withClientId(ConfigurationCenter.processId + "_" + UUID.randomUUID().toString().replace("-", ""))
							.withTerminal(1)
							.withToken(accessToken)
							.build();
					imClient.start();
					EventManager eventManager = EventManager.getInstance();
					eventManager.registerEventListener(imClient.getPrefix() + ".status", this::handleStatus);
					//prefix + "." + data.getClass().getSimpleName() + "." + data.getContentType()
					eventManager.registerEventListener(imClient.getPrefix() + "." + OutgoingData.class.getSimpleName() + "." + NewDataReceived.class.getSimpleName(), this::handleNewDataReceived);
					eventManager.registerEventListener(imClient.getPrefix() + "." + OutgoingData.class.getSimpleName() + "." + CommandReceived.class.getSimpleName(), this::handleCommandReceived);
				}
			}
		}
	}

	private void handleCommandReceived(String contentType, OutgoingData outgoingData) {
		CommandReceived commandReceived = (CommandReceived) outgoingData.getMessage();
		if(commandReceived == null || commandReceived.getCommandInfo() == null) {
			CommandResultEntity commandResultEntity = new CommandResultEntity()
					.code(NetErrors.MISSING_COMMAND_INFO)
					.message("Missing commandInfo");
			imClient.sendData(new IncomingData().message(commandResultEntity))
					.exceptionally(throwable -> {
						TapLogger.error(TAG, "Send CommandResultEntity(MISSING_COMMAND_INFO) failed, {} CommandResultEntity {}", throwable.getMessage(), commandResultEntity);
						return null;
					});
			return;
		}
		CommandInfo commandInfo = commandReceived.getCommandInfo();
		try {
			PDKUtils pdkUtils = InstanceFactory.instance(PDKUtils.class);
			if(pdkUtils == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "pdkUtils is null");
			if(commandInfo == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "commandInfo is null");
			if(commandInfo.getType() == null || commandInfo.getCommand() == null || commandInfo.getPdkHash() == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "some parameter are null, type {}, command {}, pdkHash {}", commandInfo.getType(), commandInfo.getCommand(), commandInfo.getPdkHash());

			PDKUtils.PDKInfo pdkInfo = pdkUtils.downloadPdkFileIfNeed(commandInfo.getPdkHash());
			ConnectionNode connectionNode = PDKIntegration.createConnectionConnectorBuilder()
//					.withConnectionConfig(DataMap.create(commandInfo.getConnectionConfig()))
					.withGroup(pdkInfo.getGroup())
					.withPdkId(pdkInfo.getPdkId())
					.withAssociateId(UUID.randomUUID().toString())
					.withVersion(pdkInfo.getVersion())
					.build();

			if(commandInfo.getType().equals(CommandInfo.TYPE_NODE) && commandInfo.getConnectionConfig() == null && commandInfo.getConnectionId() != null) {
				commandInfo.setConnectionConfig(pdkUtils.getConnectionConfig(commandInfo.getConnectionId()));
			}
			CommandCallbackFunction commandCallbackFunction = connectionNode.getConnectionFunctions().getCommandCallbackFunction();
			if(commandCallbackFunction == null) {
				CommandResultEntity commandResultEntity = new CommandResultEntity()
						.commandId(commandInfo.getId())
						.code(NetErrors.PDK_NOT_SUPPORT_COMMAND_CALLBACK)
						.message("pdkId " + pdkInfo.getPdkId() + " doesn't support CommandCallbackFunction");
				imClient.sendData(new IncomingData().message(commandResultEntity))
						.exceptionally(throwable -> {
							TapLogger.error(TAG, "Send CommandResultEntity(PDK_NOT_SUPPORT_COMMAND_CALLBACK) failed, {} CommandResultEntity {}", throwable.getMessage(), commandResultEntity);
							return null;
						});
				return;
//			return new Result().code(NetErrors.PDK_NOT_SUPPORT_COMMAND_CALLBACK).description("pdkId " + commandInfo.getPdkId() + " doesn't support CommandCallbackFunction");
			}
			AtomicReference<CommandResultEntity> mapAtomicReference = new AtomicReference<>();
			PDKInvocationMonitor.invoke(connectionNode, PDKMethod.CONNECTION_TEST,
					() -> {
						CommandResult commandResult = commandCallbackFunction.filter(connectionNode.getConnectionContext(), commandInfo);
						mapAtomicReference.set(new CommandResultEntity()
								.content(commandResult != null ? commandResult.getResult() : null)
								.code(Data.CODE_SUCCESS)
								.commandId(commandInfo.getId()));
					}, TAG) ;
			imClient.sendData(new IncomingData().message(mapAtomicReference.get())).exceptionally(throwable -> {
				TapLogger.error(TAG, "Send CommandResultEntity failed, {} CommandResultEntity {}", throwable.getMessage(), mapAtomicReference.get());
				return null;
			});
		} catch(Throwable throwable) {
			CommandResultEntity commandResultEntity = new CommandResultEntity()
					.commandId(commandInfo != null ? commandInfo.getId() : null)
					.code(NetErrors.MISSING_COMMAND_EXECUTE_FAILED)
					.message(throwable.getMessage());
			imClient.sendData(new IncomingData().message(commandResultEntity))
					.exceptionally(throwable1 -> {
						TapLogger.error(TAG, "Send CommandResultEntity(MISSING_COMMAND_INFO) failed, {} CommandResultEntity {}", throwable1.getMessage(), commandResultEntity);
						return null;
					});
		}
	}

	private void handleStatus(String contentType, ChannelStatus channelStatus) {
		if(channelStatus == null)
			return;
		String status = channelStatus.getStatus();
		if(status != null) {
			switch (status) {
				case ChannelStatus.STATUS_CONNECTED:
					handleTaskSubscribeInfoChanged();
					break;
			}
		}
	}

	private void handleNewDataReceived(String contentType, OutgoingData outgoingData) {
		NewDataReceived newDataReceived = (NewDataReceived) outgoingData.getMessage();
		if(newDataReceived != null && newDataReceived.getSubscribeIds() != null) {
			for(String subscribeId : newDataReceived.getSubscribeIds()) {
				List<TaskSubscribeInfo> taskSubscribeInfoList = typeConnectionIdSubscribeInfosMap.get(subscribeId);
				if(taskSubscribeInfoList != null) {
					for(TaskSubscribeInfo taskSubscribeInfo : taskSubscribeInfoList) {
						taskSubscribeInfo.subscriptionAspectTask.enableFetchingNewData(subscribeId);
					}
				}
				//TODO
			}
		}
	}

	public void addTaskSubscribeInfo(TaskSubscribeInfo taskSubscribeInfo) {
		taskSubscribeInfos.add(taskSubscribeInfo);
		if(taskSubscribeInfo.taskId != null) {
			taskIdTaskSubscribeInfoMap.putIfAbsent(taskSubscribeInfo.taskId, taskSubscribeInfo);
		}
		handleTaskSubscribeInfoChanged();
	}

	public void removeTaskSubscribeInfo(TaskSubscribeInfo taskSubscribeInfo) {
		taskSubscribeInfos.remove(taskSubscribeInfo);
		taskIdTaskSubscribeInfoMap.remove(taskSubscribeInfo.taskId);
		handleTaskSubscribeInfoChanged();
	}

	public void taskSubscribeInfoChanged(TaskSubscribeInfo taskSubscribeInfo) {
		handleTaskSubscribeInfoChanged();
	}

	private void handleTaskSubscribeInfoChanged() {
//		if(workingFuture == null && !needSync.get()) {
//			synchronized (this) {
//				if(workingFuture == null && needSync.compareAndSet(false, true)) {
//					workingFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::syncSubscribeIds, 500, TimeUnit.MILLISECONDS);
//				}
//			}
//		} else {
//			TapLogger.debug(TAG, "workingFuture {}", workingFuture);
//		}
		maxFrequencyLimiter.touch();
	}

	private void handleTaskSubscribeInfoAfterComplete() {
//		maxFrequencyLimiter.touch();
//		workingFuture = null;
//		if(needSync.get()) {
//			synchronized (this) {
//				if(workingFuture == null) {
//					workingFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::syncSubscribeIds, 500, TimeUnit.MILLISECONDS);
//				}
//			}
//		}
	}

	private void syncSubscribeIds() {
		boolean enterAsyncProcess = false;
		try {
//			needSync.compareAndSet(true, false);

			ConcurrentHashMap<String, List<TaskSubscribeInfo>> typeConnectionIdSubscribeInfosMap = new ConcurrentHashMap<>();
			for(TaskSubscribeInfo subscribeInfo : taskSubscribeInfos) {
				for(Map.Entry<String, List<Node>> entry : subscribeInfo.typeConnectionIdPDKNodeMap.entrySet()) {
					List<TaskSubscribeInfo> subscribeInfos = typeConnectionIdSubscribeInfosMap.get(entry.getKey());
					if(subscribeInfos == null) {
						subscribeInfos = new CopyOnWriteArrayList<>();
						List<TaskSubscribeInfo> old = typeConnectionIdSubscribeInfosMap.putIfAbsent(entry.getKey(), subscribeInfos);
						if(old != null)
							subscribeInfos = old;
					}
					if(!subscribeInfos.contains(subscribeInfo))
						subscribeInfos.add(subscribeInfo);
				}
			}
			Set<String> keys = typeConnectionIdSubscribeInfosMap.keySet(); //all typeConnectionIds
			this.typeConnectionIdSubscribeInfosMap = typeConnectionIdSubscribeInfosMap;

			IncomingData incomingData = new IncomingData().message(new NodeSubscribeInfo().subscribeIds(keys));
			enterAsyncProcess = true;
			imClient.sendData(incomingData).whenComplete((result1, throwable) -> {
				if(throwable != null)
					TapLogger.error(TAG, "Send NodeSubscribeInfo failed, {}", throwable.getMessage());
				if(result1 != null && result1.getCode() != 1)
					TapLogger.error(TAG, "Send NodeSubscribeInfo failed, code {} message {}", result1.getCode(), result1.getMessage());
//				handleTaskSubscribeInfoAfterComplete();
			});
		} catch(Throwable throwable) {
			TapLogger.error(TAG, "syncSubscribeIds failed, {}", throwable.getMessage());
		} finally {
			if(!enterAsyncProcess) {
				handleTaskSubscribeInfoChanged();
			}

		}
	}

	public IMClient getImClient() {
		return imClient;
	}

	public ConcurrentHashMap<String, List<TaskSubscribeInfo>> getTypeConnectionIdSubscribeInfosMap() {
		return typeConnectionIdSubscribeInfosMap;
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create().keyRegex(keyRegex)
//				.kv("maxFrequencyLimiter", maxFrequencyLimiter.toString())
//				.kv("needSync", needSync.get())
//				.kv("imClient", imClient.memory(keyRegex, memoryLevel))
				;

		//TODO not finished

		return null;
	}
}
