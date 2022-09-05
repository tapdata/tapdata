package io.tapdata.proxy.client;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.tapdata.constant.ConfigurationCenter;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.proxy.data.NewDataReceived;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.IMClientBuilder;
import io.tapdata.wsclient.utils.EventManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Bean
public class ProxySubscriptionManager {
	private static final String TAG = ProxySubscriptionManager.class.getSimpleName();
	private final ConcurrentHashSet<TaskSubscribeInfo> taskSubscribeInfos = new ConcurrentHashSet<>();
	private ConcurrentHashMap<String, List<Node>> typeConnectionIdPDKNodeMap = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, TaskSubscribeInfo> taskIdTaskSubscribeInfoMap = new ConcurrentHashMap<>();
	private ScheduledFuture<?> workingFuture;
	private final AtomicBoolean needSync = new AtomicBoolean(false);
	private IMClient imClient;

	public ProxySubscriptionManager() {
//		String nodeId = CommonUtils.getProperty("tapdata_node_id");
//		if(nodeId == null)
//			throw new CoreException(NetErrors.CURRENT_NODE_ID_NOT_FOUND, "Current nodeId is not found");
//		proxySubscription = new ProxySubscription().nodeId(nodeId).service("engine");
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
							.withClientId(ConfigurationCenter.processId)
							.withTerminal(1)
							.withToken(accessToken)
							.build();
					imClient.start();
					EventManager eventManager = EventManager.getInstance();
					//prefix + "." + data.getClass().getSimpleName() + "." + data.getContentType()
					eventManager.registerEventListener(imClient.getPrefix() + "." + OutgoingData.class.getSimpleName() + "." + NewDataReceived.class.getSimpleName(), this::handleNewDataReceived);
				}
			}
		}
	}

	private void handleNewDataReceived(String contentType, OutgoingData outgoingData) {

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
		if(workingFuture == null && !needSync.get()) {
			synchronized (this) {
				if(workingFuture == null && needSync.compareAndSet(false, true)) {
					workingFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::syncSubscribeIds, 500, TimeUnit.MILLISECONDS);
				}
			}
		} else {
			TapLogger.debug(TAG, "workingFuture {}", workingFuture);
		}
	}

	private void handleTaskSubscribeInfoAfterComplete() {
		workingFuture = null;
		if(needSync.get()) {
			synchronized (this) {
				if(workingFuture == null) {
					workingFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::syncSubscribeIds, 500, TimeUnit.MILLISECONDS);
				}
			}
		}
	}

	private void syncSubscribeIds() {
		boolean enterAsyncProcess = false;
		try {
			needSync.compareAndSet(true, false);

			ConcurrentHashMap<String, List<Node>> typeConnectionIdPDKNodeMap = new ConcurrentHashMap<>();
			for(TaskSubscribeInfo subscribeInfo : taskSubscribeInfos) {
				for(Map.Entry<String, List<Node>> entry : subscribeInfo.typeConnectionIdPDKNodeMap.entrySet()) {
					List<Node> nodes = typeConnectionIdPDKNodeMap.get(entry.getKey());
					if(nodes == null) {
						nodes = new CopyOnWriteArrayList<>();
						List<Node> old = typeConnectionIdPDKNodeMap.putIfAbsent(entry.getKey(), nodes);
						if(old != null)
							nodes = old;
					}
					nodes.addAll(entry.getValue());
				}
			}
			Set<String> keys = typeConnectionIdPDKNodeMap.keySet(); //all typeConnectionIds
			this.typeConnectionIdPDKNodeMap = typeConnectionIdPDKNodeMap;

			IncomingData incomingData = new IncomingData().message(new NodeSubscribeInfo().subscribeIds(keys));
			enterAsyncProcess = true;
			imClient.sendData(incomingData).whenComplete((result1, throwable) -> {
				if(throwable != null)
					TapLogger.error(TAG, "Send NodeSubscribeInfo failed, {}", throwable.getMessage());
				if(result1 != null && result1.getCode() != 1)
					TapLogger.error(TAG, "Send NodeSubscribeInfo failed, code {} message {}", result1.getCode(), result1.getMessage());
				handleTaskSubscribeInfoAfterComplete();
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

	public ConcurrentHashMap<String, List<Node>> getTypeConnectionIdPDKNodeMap() {
		return typeConnectionIdPDKNodeMap;
	}
}
