package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.EventQueueService;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.net.service.node.connection.NodeConnection;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.modules.api.proxy.data.NewDataReceived;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.timer.MaxFrequencyLimiter;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;
import io.tapdata.wsserver.channels.health.NodeHealthManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

@Implementation(value = EventQueueService.class, type = "sync")
public class SyncEventQueueService implements EventQueueService {
	private static final String TAG = SyncEventQueueService.class.getSimpleName();
	@Bean
	private MessageEntityService messageEntityService;

	@Bean
	private SubscribeMap subscribeMap;

	private final MaxFrequencyLimiter maxFrequencyLimiter;
	private Set<String> cachingChangedSubscribeIds = new ConcurrentSkipListSet<>();
	private Set<String> remoteCachingChangedSubscribeIds = new ConcurrentSkipListSet<>();
	@Bean
	private GatewaySessionManager gatewaySessionManager;

	@Bean
	private NodeHealthManager nodeHealthManager;

	@Bean
	private ProxySubscriptionService proxySubscriptionService;

	@Bean
	private NodeConnectionFactory nodeConnectionFactory;
	public SyncEventQueueService() {
		maxFrequencyLimiter = new MaxFrequencyLimiter(500, () -> {
			Set<String> old = cachingChangedSubscribeIds;
			cachingChangedSubscribeIds = new ConcurrentSkipListSet<>();
			Set<String> remoteOld = remoteCachingChangedSubscribeIds;
			remoteCachingChangedSubscribeIds = new ConcurrentSkipListSet<>();

			//combine caching changed subscribe ids and remote caching changed subscribe ids
			remoteOld.addAll(old);

			//match any of subscribe id, then send to all sessions
			Map<EngineSessionHandler, List<String>> sessionSubscribeIdsMap = subscribeMap.getSessionSubscribeIdsMapByAnyOne(remoteOld);

			if(sessionSubscribeIdsMap != null) {
				for(Map.Entry<EngineSessionHandler, List<String>> entry : sessionSubscribeIdsMap.entrySet()) {
					gatewaySessionManager.receiveOutgoingData(entry.getKey().getId(), new OutgoingData().message(new NewDataReceived().subscribeIds(entry.getValue())));
				}
			}

			if(old.isEmpty())
				return;

			String currentNodeId = CommonUtils.getProperty("tapdata_node_id");

			NewDataReceived newDataReceived = new NewDataReceived().subscribeIds(new ArrayList<>(old));
			List<String> nodeIds = proxySubscriptionService.subscribedNodeIdsByAnyOne("engine", old);
			if(nodeIds != null) {
				for(String nodeId : nodeIds) {
					if(currentNodeId.equals(nodeId))
						continue;

					NodeConnection nodeConnection = nodeConnectionFactory.getNodeConnection(nodeId);
					if (nodeConnection != null && nodeConnection.isReady()) {
						try {
							//TODO should use async queue way to send NewDataReceived, to avoid a timeout connection block the others.
							//noinspection unchecked
							nodeConnection.sendAsync(NewDataReceived.class.getSimpleName(), newDataReceived, Void.class, (o, throwable) -> {
								if(throwable != null)
									TapLogger.debug(TAG, "send NewDataReceived {} failed, {}", newDataReceived, throwable.getMessage());
							});
						} catch (IOException ioException) {
							TapLogger.debug(TAG, "Send to nodeId {} failed {} and will try next, newDataReceived {}", nodeId, ioException.getMessage(), newDataReceived);
						}
					} else {
						TapLogger.debug(TAG, "Try to notify node {} failed, state is not ready, as the node hit any of subscribeIds {}", nodeId, old);
					}
				}
			}
		});
	}

	@Override
	public void offer(MessageEntity message) {
		messageEntityService.save(message);
		cachingChangedSubscribeIds.add(message.getSubscribeId());
		maxFrequencyLimiter.touch();
	}

	@Override
	public void newDataReceived(List<String> subscribeIds) {
		if(subscribeIds != null) {
			remoteCachingChangedSubscribeIds.addAll(subscribeIds);
			maxFrequencyLimiter.touch();
		}
	}
}
