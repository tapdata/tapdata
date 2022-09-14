package io.tapdata.wsserver.channels.health;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.NodeHealthService;
import io.tapdata.modules.api.net.service.NodeRegistryService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Bean
public class NodeHealthManager {
	private static final String TAG = NodeHealthManager.class.getSimpleName();
	@Bean
	private NodeHealthService nodeHealthService;

	private final List<HealthWeightListener> healthWeightListeners = new CopyOnWriteArrayList<>();
	private final AtomicBoolean started = new AtomicBoolean(false);

	private NodeHealth nodeHealth;
	private Collection<NodeHealth> healthyNodes;
	private int nodeDeadExpiredSeconds = 120;
	@Bean
	private NodeRegistryService nodeRegistryService;
	@Bean
	private ProxySubscriptionService proxySubscriptionService;

	public void start(HealthWeightListener... listeners) {
		if(started.compareAndSet(false, true)) {
			if(listeners != null)
				healthWeightListeners.addAll(Arrays.asList(listeners));
			int checkHealthPeriodSeconds = CommonUtils.getPropertyInt("tapdata_check_health_period_seconds", 10);
			int cleanUpDeadNodesPeriodSeconds = CommonUtils.getPropertyInt("tapdata_cleanup_dead_nodes_period_seconds", 10);
			nodeDeadExpiredSeconds = CommonUtils.getPropertyInt("tapdata_node_dead_expired_seconds", 120);
			String nodeId = CommonUtils.getProperty("tapdata_node_id");
			if(nodeId == null)
				throw new CoreException(NetErrors.CURRENT_NODE_ID_NOT_FOUND, "Current nodeId for NodeHealthManager#start not found");

			nodeHealth = new NodeHealth().id(nodeId);
			nodeHealthService.save(nodeHealth.health(score()).time(System.currentTimeMillis()));
			ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(() -> {
				CommonUtils.ignoreAnyError(() -> {
					nodeHealthService.save(nodeHealth.health(score()).time(System.currentTimeMillis()));
					healthyNodes = nodeHealthService.getHealthNodes();
				}, TAG);
			}, checkHealthPeriodSeconds, checkHealthPeriodSeconds, TimeUnit.SECONDS);

			ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(() -> {
				CommonUtils.ignoreAnyError(() -> {
					String cleaner = nodeHealthService.getCleaner();
					if(cleaner == null) {
						TapLogger.error(TAG, "Unexpected cleaner, the initial value should be \"none\", not null");
						return;
					}
					NodeHealth cleanerNodeHealth = getAliveNode(cleaner);
					if(cleanerNodeHealth == null) {
						cleanerNodeHealth = nodeHealthService.get(cleaner);
					}
					if(cleaner.equals(nodeId)) {
						cleanUpDeadNodes();
					} else {
						if(!stillAlive(cleanerNodeHealth)) {
							if(nodeHealthService.applyToBeCleaner(cleaner, nodeId)) {
								TapLogger.debug(TAG, "Applied to be cleaner from dead node {} to current nodeId {}", cleaner, nodeId);
								cleanUpDeadNodes();
							}
						}
					}
				}, TAG);
			}, cleanUpDeadNodesPeriodSeconds, cleanUpDeadNodesPeriodSeconds, TimeUnit.SECONDS);
		}
	}

	private boolean isNodeAlive(String nodeId) {
		boolean hit = false;
		for(NodeHealth nodeHealth1 : healthyNodes) {
			if(nodeHealth1.getId().equals(nodeId)) {
				hit = true;
				break;
			}
		}
		return hit;
	}

	private NodeHealth getAliveNode(String nodeId) {
		for(NodeHealth nodeHealth1 : healthyNodes) {
			if(nodeHealth1.getId().equals(nodeId)) {
				return nodeHealth1;
			}
		}
		return null;
	}

	private void cleanUpDeadNodes() {
		if(healthyNodes == null)
			return;
		List<NodeRegistry> nodes = nodeRegistryService.getNodes();
		for(NodeRegistry nodeRegistry : nodes) {
			String id = nodeRegistry.id();
			NodeHealth nodeHealth1 = getAliveNode(id);
			if(stillAlive(nodeHealth1))
				continue; //ignore alive node or dead node which was alive within 120 seconds
			if(nodeRegistryService.delete(id, nodeRegistry.getTime())) {
				TapLogger.debug(TAG, "Found dead node registration {} time {}, deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			} else {
				TapLogger.debug(TAG, "Found dead node registration {} time {}, not deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			}
			if(nodeHealthService.delete(id)) {
				TapLogger.debug(TAG, "Found dead node health {} time {}, deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			} else {
				TapLogger.debug(TAG, "Found dead node health {} time {}, not deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			}
			ProxySubscription proxySubscription = proxySubscriptionService.get(id);
			if(proxySubscription != null) {
				if(proxySubscriptionService.delete(id, proxySubscription.getTime())) {
					TapLogger.debug(TAG, "Found dead proxy subscription {} time {}, deleted", id, proxySubscription.getTime() != null ? new Date(proxySubscription.getTime()) : null);
				} else {
					TapLogger.debug(TAG, "Found dead proxy subscription {} time {}, not deleted", id, proxySubscription.getTime() != null ? new Date(proxySubscription.getTime()) : null);
				}
			}
		}
	}

	private boolean stillAlive(NodeHealth nodeHealth1) {
		return nodeHealth1 != null && nodeHealth1.getTime() != null && (System.currentTimeMillis() - nodeHealth1.getTime()) < TimeUnit.SECONDS.toMillis(nodeDeadExpiredSeconds);
	}

	public int score() {
		int score = 0;
		for(HealthWeightListener healthWeightListener : healthWeightListeners) {
			int weight = healthWeightListener.weight();
			int health = healthWeightListener.health();
			score += health * weight / 100;
		}
		return score;
	}

	public boolean isStarted() {
		return started.get();
	}

	public NodeHealth getNodeHealth() {
		return nodeHealth;
	}

	public Collection<NodeHealth> getHealthyNodes() {
		return healthyNodes;
	}
}
