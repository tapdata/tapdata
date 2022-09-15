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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Bean
public class NodeHealthManager {
	private static final String TAG = NodeHealthManager.class.getSimpleName();
	@Bean
	private NodeHealthService nodeHealthService;
	private final Map<String, NodeHandler> idNodeHandlerMap = new ConcurrentHashMap<>();

	private final List<HealthWeightListener> healthWeightListeners = new CopyOnWriteArrayList<>();
	private final AtomicBoolean started = new AtomicBoolean(false);

	private NodeHealth currentNodeHealth;
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

			currentNodeHealth = new NodeHealth().id(nodeId);
			nodeHealthService.save(currentNodeHealth.health(score()).time(System.currentTimeMillis()));
			ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(() -> {
				CommonUtils.ignoreAnyError(() -> {
					nodeHealthService.save(currentNodeHealth.health(score()).time(System.currentTimeMillis()));
					loadNodes();
				}, TAG);
			}, checkHealthPeriodSeconds, checkHealthPeriodSeconds, TimeUnit.SECONDS);

			ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(() -> {
				CommonUtils.ignoreAnyError(() -> {
					String cleaner = nodeHealthService.getCleaner();
					if(cleaner == null) {
						TapLogger.error(TAG, "Unexpected cleaner, the initial value should be \"none\", not null");
						return;
					}
					NodeHandler nodeHandler = getAliveNode(cleaner);
					NodeHealth cleanerNodeHealth;
					if(nodeHandler == null && !this.currentNodeHealth.getId().equals(cleaner)) {
						cleanerNodeHealth = nodeHealthService.get(cleaner);
						NodeRegistry nodeRegistry = nodeRegistryService.get(cleaner);
						if(cleanerNodeHealth != null && nodeRegistry != null) {
							NodeHandler handler = idNodeHandlerMap.putIfAbsent(cleaner, new NodeHandler().nodeHealth(cleanerNodeHealth).nodeRegistry(nodeRegistry));
							if(handler == null)
								TapLogger.debug(TAG, "Node {} added into healthy node list as cleaner, nodeHealth {}, nodeRegistry {}", cleaner, cleanerNodeHealth, nodeRegistry);
						}
					} else {
						cleanerNodeHealth = nodeHandler.getNodeHealth();
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

	private void loadNodes() {
		Collection<NodeHealth> healthyNodes = nodeHealthService.getHealthNodes();
		Set<String> nodeIds = new HashSet<>();
		for(NodeHealth nodeHealth : healthyNodes) {
			if(nodeHealth.getId() != null && !this.currentNodeHealth.getId().equals(nodeHealth.getId())) {
				nodeIds.add(nodeHealth.getId());
				idNodeHandlerMap.computeIfAbsent(nodeHealth.getId(), id -> {
					NodeRegistry nodeRegistry = nodeRegistryService.get(id);
					if(nodeRegistry != null) {
						TapLogger.debug(TAG, "Node {} added into healthy node list, nodeHealth {}, nodeRegistry {}", id, nodeHealth, nodeRegistry);
						return new NodeHandler().nodeHealth(nodeHealth).nodeRegistry(nodeRegistry);
					} else {
						TapLogger.debug(TAG, "Node id {} can not find NodeRegistry, ignored... nodeHealth {}", id, nodeHealth);
						return null;
					}
				});
			}
		}

		Set<String> deleted = new HashSet<>();
		for(String id : idNodeHandlerMap.keySet()) {
			if(!nodeIds.contains(id)) {
				deleted.add(id);
			}
		}

		for(String deletedId : deleted) {
			NodeHandler nodeHandler = idNodeHandlerMap.remove(deletedId);
			if(nodeHandler != null) {
				TapLogger.debug(TAG, "Node {} has been removed from healthy node list, nodeHealth {}, nodeRegistry {}", deletedId, nodeHandler.getNodeHealth(), nodeHandler.getNodeRegistry());
			}
		}
	}

	private boolean isNodeAlive(String nodeId) {
		return idNodeHandlerMap.containsKey(nodeId);
	}

	private NodeHandler getAliveNode(String nodeId) {
		return idNodeHandlerMap.get(nodeId);
	}

	private void cleanUpDeadNodes() {
		List<NodeRegistry> nodes = nodeRegistryService.getNodes();
		for(NodeRegistry nodeRegistry : nodes) {
			String id = nodeRegistry.id();
			NodeHandler aliveNode = getAliveNode(id);
			if(stillAlive(aliveNode.getNodeHealth()))
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

	private boolean stillAlive(NodeHealth nodeHealth) {
		return nodeHealth != null && nodeHealth.getTime() != null && (System.currentTimeMillis() - nodeHealth.getTime()) < TimeUnit.SECONDS.toMillis(nodeDeadExpiredSeconds);
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

	public NodeHealth getCurrentNodeHealth() {
		return currentNodeHealth;
	}

	public Map<String, NodeHandler> getIdNodeHandlerMap() {
		return idNodeHandlerMap;
	}
}
