package io.tapdata.proxy.client;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.tapdata.constant.ConfigurationCenter;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.CommandEntity;
import io.tapdata.aspect.supervisor.entity.DiscoverSchemaEntity;
import io.tapdata.aspect.supervisor.entity.DisposableThreadGroupBase;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.EngineMessageResultEntity;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.modules.api.proxy.data.CommandReceived;
import io.tapdata.modules.api.proxy.data.NewDataReceived;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.modules.api.proxy.data.ServiceCallerReceived;
import io.tapdata.modules.api.service.SkeletonService;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.timer.MaxFrequencyLimiter;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.utils.DisposableType;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.IMClientBuilder;
import io.tapdata.wsclient.modules.imclient.impls.websocket.ChannelStatus;
import io.tapdata.wsclient.utils.EventManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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

	private String userId;
	private String processId;

	@Bean
	private SkeletonService skeletonService;

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
					eventManager.registerEventListener(imClient.getPrefix() + "." + OutgoingData.class.getSimpleName() + "." + ServiceCallerReceived.class.getSimpleName(), this::handleServiceCallerReceived);
				}
			}
		}
	}

	private void handleServiceCallerReceived(String contentType, OutgoingData outgoingData) {
		ServiceCallerReceived serviceCallerReceived = (ServiceCallerReceived) outgoingData.getMessage();
		Throwable parseError = serviceCallerReceived.getParseError();
		if(parseError != null) {
			ServiceCaller serviceCaller = serviceCallerReceived.getServiceCaller();
			String id = serviceCaller != null ? serviceCaller.getId() : null;
			EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
					.id(id)
					.code(NetErrors.SERVICE_CALLER_PARSE_FAILED)
					.message(parseError.getMessage());
			imClient.sendData(new IncomingData().message(engineMessageResultEntity))
					.exceptionally(throwable -> {
						TapLogger.error(TAG, "Send EngineMessageResultEntity(SERVICE_CALLER_PARSE_FAILED) failed, {} engineMessageResultEntity {}", throwable.getMessage(), engineMessageResultEntity);
						return null;
					});
			return;
		}
		if(serviceCallerReceived.getServiceCaller() == null) {
			EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
					.code(NetErrors.MISSING_SERVICE_CALLER)
					.message("Missing service caller");
			imClient.sendData(new IncomingData().message(engineMessageResultEntity))
					.exceptionally(throwable -> {
						TapLogger.error(TAG, "Send EngineMessageResultEntity(MISSING_SERVICE_CALLER) failed, {} engineMessageResultEntity {}", throwable.getMessage(), engineMessageResultEntity);
						return null;
					});
			return;
		}
		ServiceCaller serviceCaller = serviceCallerReceived.getServiceCaller();
		try {
			PDKUtils pdkUtils = InstanceFactory.instance(PDKUtils.class);
			if(pdkUtils == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "pdkUtils is null");
			if(serviceCaller == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "serviceCaller is null");
			if(serviceCaller.getClassName() == null || serviceCaller.getMethod() == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "some parameter are null, className {}, method {}, args {}", serviceCaller.getClassName(), serviceCaller.getMethod(), serviceCaller.getArgs());

			skeletonService.call(serviceCaller.getClassName(), serviceCaller.getMethod(), serviceCaller.getArgs()).whenComplete((callResult, throwable) -> {
				EngineMessageResultEntity engineMessageResultEntity;
				if(throwable != null) {
					engineMessageResultEntity = new EngineMessageResultEntity()
							.contentClass(serviceCaller.getReturnClass())
							.code(NetErrors.COMMAND_EXECUTE_FAILED)
							.id(serviceCaller.getId())
							.message(throwable.getMessage());
				} else {
					engineMessageResultEntity = new EngineMessageResultEntity()
							.contentClass(serviceCaller.getReturnClass())
							.content(callResult)
							.code(Data.CODE_SUCCESS)
							.id(serviceCaller.getId());
				}
				imClient.sendData(new IncomingData().message(engineMessageResultEntity)).exceptionally(throwable1 -> {
					TapLogger.error(TAG, "Send EngineMessageResultEntity failed, {} EngineMessageResultEntity {}", throwable1.getMessage(), engineMessageResultEntity);
					return null;
				});
			});
		} catch(Throwable throwable) {
			int code = NetErrors.SERVICE_CALLER_EXECUTE_FAILED;
			if(throwable instanceof CoreException) {
				code = ((CoreException) throwable).getCode();
			}
			EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
					.id(serviceCaller != null ? serviceCaller.getId() : null)
					.code(code)
					.message(throwable.getMessage());
			imClient.sendData(new IncomingData().message(engineMessageResultEntity))
					.exceptionally(throwable1 -> {
						TapLogger.error(TAG, "Send CommandResultEntity(SERVICE_CALLER_EXECUTE_FAILED) failed, {} CommandResultEntity {}", throwable1.getMessage(), engineMessageResultEntity);
						return null;
					});
		}
	}

	private void handleCommandReceived(String contentType, OutgoingData outgoingData) {
		CommandReceived commandReceived = (CommandReceived) outgoingData.getMessage();
		Throwable parseError = commandReceived.getParseError();
		if(parseError != null) {
			CommandInfo commandInfo = commandReceived.getCommandInfo();
			String id = commandInfo != null ? commandInfo.getId() : null;
			EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
					.id(id)
					.code(NetErrors.COMMAND_INFO_PARSE_FAILED)
					.message(parseError.getMessage());
			imClient.sendData(new IncomingData().message(engineMessageResultEntity))
					.exceptionally(throwable -> {
						TapLogger.error(TAG, "Send EngineMessageResultEntity(COMMAND_INFO_PARSE_FAILED) failed, {} engineMessageResultEntity {}", throwable.getMessage(), engineMessageResultEntity);
						return null;
					});
			return;
		}
		if(commandReceived.getCommandInfo() == null) {
			EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
					.code(NetErrors.MISSING_COMMAND_INFO)
					.message("Missing commandInfo");
			imClient.sendData(new IncomingData().message(engineMessageResultEntity))
					.exceptionally(throwable -> {
						TapLogger.error(TAG, "Send EngineMessageResultEntity(MISSING_COMMAND_INFO) failed, {} engineMessageResultEntity {}", throwable.getMessage(), engineMessageResultEntity);
						return null;
					});
			return;
		}
		String associateId = UUID.randomUUID().toString();
		CommandInfo commandInfo;
		PDKUtils pdkUtils;
		PDKUtils.PDKInfo pdkInfo;
		try {
			commandInfo = commandReceived.getCommandInfo();
			pdkUtils = InstanceFactory.instance(PDKUtils.class);
			if(pdkUtils == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "pdkUtils is null");
			if(commandInfo == null)
				throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "commandInfo is null");
			pdkInfo = pdkUtils.downloadPdkFileIfNeed(commandInfo.getPdkHash());
		} catch (Throwable t) {
			EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
					.code(NetErrors.MISSING_DOWNLOAD_PDK_FAILED)
					.message("Download pdk failed");
			imClient.sendData(new IncomingData().message(engineMessageResultEntity))
					.exceptionally(throwable -> {
						TapLogger.error(TAG, "Send EngineMessageResultEntity(DOWNLOAD_PDK_FAILED) failed, {} engineMessageResultEntity {}", throwable.getMessage(), engineMessageResultEntity);
						return null;
					});
			return;
		}

		String threadName = String.format("COMMAND_CALLBACK_%s_%s_%s", pdkInfo.getPdkId(), commandInfo.getCommand(),associateId);
		DisposableThreadGroup threadGroup = new DisposableThreadGroup(DisposableType.COMMAND, threadName);
		DisposableThreadGroupBase entity = new CommandEntity()
				.command(commandInfo.getCommand())
				.time(System.nanoTime())
				.associateId(associateId)
				.connectionName(commandInfo.getConnectionId())
				.type(commandInfo.getType())
				.databaseType(pdkInfo.getPdkId())
				.pdkHash(commandInfo.getPdkHash())
				;
		new Thread(threadGroup, AspectRunnableUtil.aspectRunnable(new DisposableThreadGroupAspect<>(associateId,threadGroup,entity), () -> {
			try {
				if(commandInfo.getType() == null || commandInfo.getCommand() == null || commandInfo.getPdkHash() == null)
					throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "some parameter are null, type {}, command {}, pdkHash {}", commandInfo.getType(), commandInfo.getCommand(), commandInfo.getPdkHash());

				ConnectionNode connectionNode = PDKIntegration.createConnectionConnectorBuilder()
	//					.withConnectionConfig(DataMap.create(commandInfo.getConnectionConfig()))
						.withGroup(pdkInfo.getGroup())
						.withPdkId(pdkInfo.getPdkId())
						.withAssociateId(associateId)
						.withVersion(pdkInfo.getVersion())
						.withLog(new TapLog())
						.build();

				try {
					if(commandInfo.getType().equals(CommandInfo.TYPE_NODE) && commandInfo.getConnectionConfig() == null && commandInfo.getConnectionId() != null) {
						commandInfo.setConnectionConfig(pdkUtils.getConnectionConfig(commandInfo.getConnectionId()));
					}
					CommandCallbackFunction commandCallbackFunction = connectionNode.getConnectionFunctions().getCommandCallbackFunction();
					if(commandCallbackFunction == null) {
						EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
								.id(commandInfo.getId())
								.code(NetErrors.PDK_NOT_SUPPORT_COMMAND_CALLBACK)
								.message("pdkId " + pdkInfo.getPdkId() + " doesn't support CommandCallbackFunction");
						imClient.sendData(new IncomingData().message(engineMessageResultEntity))
								.exceptionally(throwable -> {
									TapLogger.error(TAG, "Send CommandResultEntity(PDK_NOT_SUPPORT_COMMAND_CALLBACK) failed, {} CommandResultEntity {}", throwable.getMessage(), engineMessageResultEntity);
									return null;
								});
						return;
	//			return new Result().code(NetErrors.PDK_NOT_SUPPORT_COMMAND_CALLBACK).description("pdkId " + commandInfo.getPdkId() + " doesn't support CommandCallbackFunction");
					}

						AtomicReference<EngineMessageResultEntity> mapAtomicReference = new AtomicReference<>();
						PDKInvocationMonitor.invoke(connectionNode, PDKMethod.COMMAND_CALLBACK,
								() -> {
									CommandResult commandResult = commandCallbackFunction.filter(connectionNode.getConnectionContext(), commandInfo);
									mapAtomicReference.set(new EngineMessageResultEntity()
											.content(commandResult != null ? (commandResult.getData() != null ? commandResult.getData() : commandResult.getResult()) : null)
											.code(Data.CODE_SUCCESS)
											.id(commandInfo.getId()));
								}, TAG);
						imClient.sendData(new IncomingData().message(mapAtomicReference.get())).exceptionally(throwable -> {
							TapLogger.error(TAG, "Send CommandResultEntity failed, {} CommandResultEntity {}", throwable.getMessage(), mapAtomicReference.get());
							return null;
						});


				} finally {
					connectionNode.unregisterMemoryFetcher();
					PDKIntegration.releaseAssociateId(associateId);
				}
			} catch(Throwable throwable) {
				int code = NetErrors.COMMAND_EXECUTE_FAILED;
				Object data = null;
				if(throwable instanceof CoreException) {
					code = ((CoreException) throwable).getCode();
					data = ((CoreException) throwable).getData();
					if (data instanceof CommandResult){
						data = ((CommandResult)data).getData();
					}
				}
				EngineMessageResultEntity engineMessageResultEntity = new EngineMessageResultEntity()
						.id(commandInfo != null ? commandInfo.getId() : null)
						.code(code)
						.content(data)
						.message(throwable.getMessage());
				imClient.sendData(new IncomingData().message(engineMessageResultEntity))
						.exceptionally(throwable1 -> {
							TapLogger.error(TAG, "Send CommandResultEntity(COMMAND_EXECUTE_FAILED) failed, {} CommandResultEntity {}", throwable1.getMessage(), engineMessageResultEntity);
							return null;
						});
			}
		}), threadName).start();
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
						if(taskSubscribeInfo.subscriptionAspectTask.streamReadConsumer != null)
							taskSubscribeInfo.subscriptionAspectTask.enableFetchingNewData(subscribeId);
						else
							TapLogger.debug(TAG, "streamRead is not started yet, new data request will be ignored for task {}", taskSubscribeInfo.taskId);
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

			HashSet<String> allKeys = new HashSet<>(keys);
			if(processId != null)
				allKeys.add("processId_" + processId);
			if(userId != null)
				allKeys.add("userId_" + userId);

			IncomingData incomingData = new IncomingData().message(new NodeSubscribeInfo().subscribeIds(allKeys));
			enterAsyncProcess = true;
			imClient.sendData(incomingData).whenComplete((result1, throwable) -> {
				if(throwable != null) {
					TapLogger.error(TAG, "Send NodeSubscribeInfo failed, {}", throwable.getMessage());
					handleTaskSubscribeInfoChanged();
				} else if(result1 != null && result1.getCode() != 1) {
					TapLogger.error(TAG, "Send NodeSubscribeInfo failed, code {} message {}", result1.getCode(), result1.getMessage());
					handleTaskSubscribeInfoChanged();
				}
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
		List<DataMap> taskSubscribeInfos = new ArrayList<>();
		DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
				.kv("taskSubscribeInfos", taskSubscribeInfos)
				.kv("userId", userId)
				.kv("processId", processId)
				.kv("imClient", imClient.memory(keyRegex, memoryLevel))
				;
		for(TaskSubscribeInfo taskSubscribeInfo : this.taskSubscribeInfos) {
			taskSubscribeInfos.add(taskSubscribeInfo.memory(keyRegex, memoryLevel));
		}

		return dataMap;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getProcessId() {
		return processId;
	}

	public void setProcessId(String processId) {
		this.processId = processId;
	}
}
