package io.tapdata.wsserver.channels.health;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.entity.NodeHealth;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.modules.api.net.entity.ProxySubscription;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.service.node.NodeHealthService;
import io.tapdata.modules.api.net.service.node.NodeRegistryService;
import io.tapdata.modules.api.net.service.ProxySubscriptionService;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsserver.channels.gateway.GatewaySessionManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Bean
public class NodeHealthManager implements MemoryFetcher {
	private static final String TAG = NodeHealthManager.class.getSimpleName();
	@Bean
	private NodeHealthService nodeHealthService;
	private final Map<String, NodeHandler> idNodeHandlerMap = new ConcurrentHashMap<>();

	private final List<HealthWeightListener> healthWeightListeners = new CopyOnWriteArrayList<>();
	private final AtomicBoolean started = new AtomicBoolean(false);

	private NodeHealth currentNodeHealth;
	private int nodeDeadExpiredSeconds = 1800;
	@Bean
	private NodeRegistryService nodeRegistryService;
	@Bean
	private ProxySubscriptionService proxySubscriptionService;

	@Bean
	private GatewaySessionManager gatewaySessionManager;

	private Consumer<NodeRegistry> newNodeConsumer;
	private Consumer<NodeRegistry> deleteNodeConsumer;

	@Bean
	private NodeConnectionFactory nodeConnectionFactory;

	public void start(HealthWeightListener... listeners) {
		if(started.compareAndSet(false, true)) {
			if(listeners != null)
				healthWeightListeners.addAll(Arrays.asList(listeners));
			int checkHealthPeriodSeconds = CommonUtils.getPropertyInt("tapdata_check_health_period_seconds", 10);
			int cleanUpDeadNodesPeriodSeconds = CommonUtils.getPropertyInt("tapdata_cleanup_dead_nodes_period_seconds", 60);
			nodeDeadExpiredSeconds = CommonUtils.getPropertyInt("tapdata_node_dead_expired_seconds", 1800);
			String nodeId = CommonUtils.getProperty("tapdata_node_id");
			if(nodeId == null)
				throw new CoreException(NetErrors.CURRENT_NODE_ID_NOT_FOUND, "Current nodeId for NodeHealthManager#start not found");

			currentNodeHealth = new NodeHealth().id(nodeId);
			nodeHealthService.save(currentNodeHealth.health(score()).online(gatewaySessionManager.roomCounter()).time(System.currentTimeMillis()));
			idNodeHandlerMap.put(nodeId, new NodeHandler().nodeHealth(currentNodeHealth).nodeRegistry(gatewaySessionManager.getNodeRegistry()));
			loadNodes();
			ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(() -> {
				CommonUtils.ignoreAnyError(() -> {
					nodeHealthService.save(currentNodeHealth.health(score()).online(gatewaySessionManager.roomCounter()).time(System.currentTimeMillis()));
					loadNodes();
				}, TAG);
			}, checkHealthPeriodSeconds, checkHealthPeriodSeconds, TimeUnit.SECONDS);
			ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(() -> {
				CommonUtils.ignoreAnyError(this::cleanUpDeadNodes, TAG);
			}, cleanUpDeadNodesPeriodSeconds, cleanUpDeadNodesPeriodSeconds, TimeUnit.SECONDS);
//			ExecutorsManager.getInstance().getScheduledExecutorService().scheduleWithFixedDelay(() -> {
//				CommonUtils.ignoreAnyError(() -> {
//					String cleaner = nodeHealthService.getCleaner();
//					if(cleaner == null) {
//						TapLogger.error(TAG, "Unexpected cleaner, the initial value should be \"none\", not null");
//						return;
//					}
//					NodeHandler nodeHandler = getAliveNode(cleaner);
//					NodeHealth cleanerNodeHealth = null;
//					if(nodeHandler == null && !this.currentNodeHealth.getId().equals(cleaner)) {
//						cleanerNodeHealth = nodeHealthService.get(cleaner);
//						NodeRegistry nodeRegistry = nodeRegistryService.get(cleaner);
//						if(cleanerNodeHealth != null && nodeRegistry != null) {
//							NodeHandler handler = idNodeHandlerMap.putIfAbsent(cleaner, new NodeHandler().nodeHealth(cleanerNodeHealth).nodeRegistry(nodeRegistry));
//							if(handler == null)
//								TapLogger.debug(TAG, "Node {} added into healthy node list as cleaner, nodeHealth {}, nodeRegistry {}", cleaner, cleanerNodeHealth, nodeRegistry);
//						}
//					} else if(nodeHandler != null) {
//						cleanerNodeHealth = nodeHandler.getNodeHealth();
//					}
//					if(cleaner.equals(nodeId)) {
//						cleanUpDeadNodes();
//					} else {
//						if(!stillAlive(cleanerNodeHealth)) {
//							if(nodeHealthService.applyToBeCleaner(cleaner, nodeId)) {
//								TapLogger.debug(TAG, "Applied to be cleaner from dead node {} to current nodeId {}", cleaner, nodeId);
//								cleanUpDeadNodes();
//							}
//						}
//					}
//				}, TAG);
//			}, cleanUpDeadNodesPeriodSeconds, cleanUpDeadNodesPeriodSeconds, TimeUnit.SECONDS);
		}
	}

	private void loadNodes() {
		Collection<NodeHealth> healthyNodes = nodeHealthService.getHealthNodes();
		Set<String> nodeIds = new HashSet<>();
		for(NodeHealth nodeHealth : healthyNodes) {
			if(this.currentNodeHealth.getId().equals(nodeHealth.getId()))
				continue;
			if(nodeHealth.getId() != null) {
				nodeIds.add(nodeHealth.getId());
				AtomicReference<NodeRegistry> newAdded = new AtomicReference<>();
				NodeHandler existingNodeHandler = idNodeHandlerMap.computeIfAbsent(nodeHealth.getId(), id -> {
					NodeRegistry nodeRegistry = nodeRegistryService.get(id);
					if(nodeRegistry != null) {
						TapLogger.info(TAG, "Node {} added into healthy node list, nodeHealth {}, nodeRegistry {}", id, nodeHealth, nodeRegistry);
						newAdded.set(nodeRegistry);
						return new NodeHandler().nodeHealth(nodeHealth).nodeRegistry(nodeRegistry);
					} else {
						nodeHealthService.delete(nodeHealth.getId());
						TapLogger.info(TAG, "Node id {} can not find NodeRegistry, deleted, nodeHealth {}", id, nodeHealth);
						return null;
					}
				});
				if(existingNodeHandler != null && existingNodeHandler.getNodeHealth() != null) {
					NodeHealth existingNodeHealth = existingNodeHandler.getNodeHealth();
					if(existingNodeHealth != null)
						existingNodeHealth.clone(nodeHealth);
				}
				if(newAdded.get() != null && newNodeConsumer != null) {
					newNodeConsumer.accept(newAdded.get());
				}
			}
		}

		Set<String> deleted = new HashSet<>();
		for(String id : idNodeHandlerMap.keySet()) {
			if(this.currentNodeHealth.getId().equals(id))
				continue;
			if(!nodeIds.contains(id)) {
				deleted.add(id);
			}
		}

		for(String deletedId : deleted) {
			NodeHandler nodeHandler = idNodeHandlerMap.remove(deletedId);
			if(nodeHandler != null) {
				TapLogger.info(TAG, "Node {} has been removed from healthy node list, nodeHealth {}, nodeRegistry {}", deletedId, nodeHandler.getNodeHealth(), nodeHandler.getNodeRegistry());
				if(deleteNodeConsumer != null) {
					deleteNodeConsumer.accept(nodeHandler.getNodeRegistry());
				}
			}
		}
	}

	public boolean isNodeAlive(String nodeId) {
		return idNodeHandlerMap.containsKey(nodeId);
	}

	public NodeHandler getAliveNode(String nodeId) {
		return idNodeHandlerMap.get(nodeId);
	}

	public List<String> cleanUpDeadNodes() {
		List<NodeRegistry> nodes = nodeRegistryService.getNodes();
		List<String> deletedNodes = new ArrayList<>();
		for(NodeRegistry nodeRegistry : nodes) {
			String id = nodeRegistry.id();

			if(this.currentNodeHealth.getId().equals(id))
				continue;

			NodeHandler aliveNode = getAliveNode(id);
			if(aliveNode != null)
				continue; //ignore alive node or dead node which was alive within 120 seconds
			NodeHealth nodeHealth = nodeHealthService.get(id);
			if(withinDeadExpiredSeconds(nodeHealth))
				continue;


//			if(!nodeConnectionFactory.isDisconnected(id)) {
//				nodeConnectionFactory.getNodeConnection(id);
//			} else {
//			}
			deletedNodes.add(id);
			if(nodeRegistryService.delete(id, nodeRegistry.getTime())) {
				TapLogger.info(TAG, "Found dead node registration {} time {}, deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			} else {
				TapLogger.info(TAG, "Found dead node registration {} time {}, not deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			}
			if(nodeHealthService.delete(id)) {
				TapLogger.info(TAG, "Found dead node health {} time {}, deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			} else {
				TapLogger.info(TAG, "Found dead node health {} time {}, not deleted", id, nodeRegistry.getTime() != null ? new Date(nodeRegistry.getTime()) : null);
			}
			ProxySubscription proxySubscription = proxySubscriptionService.get(id);
			if(proxySubscription != null) {
				if(proxySubscriptionService.delete(id, proxySubscription.getTime())) {
					TapLogger.info(TAG, "Found dead proxy subscription {} time {}, deleted", id, proxySubscription.getTime() != null ? new Date(proxySubscription.getTime()) : null);
				} else {
					TapLogger.info(TAG, "Found dead proxy subscription {} time {}, not deleted", id, proxySubscription.getTime() != null ? new Date(proxySubscription.getTime()) : null);
				}
			}
		}
		return deletedNodes;
	}

	private boolean withinDeadExpiredSeconds(NodeHealth nodeHealth) {
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

	public Consumer<NodeRegistry> getNewNodeConsumer() {
		return newNodeConsumer;
	}

	public void setNewNodeConsumer(Consumer<NodeRegistry> newNodeConsumer) {
		this.newNodeConsumer = newNodeConsumer;
	}

	public Consumer<NodeRegistry> getDeleteNodeConsumer() {
		return deleteNodeConsumer;
	}

	public void setDeleteNodeConsumer(Consumer<NodeRegistry> deleteNodeConsumer) {
		this.deleteNodeConsumer = deleteNodeConsumer;
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
				.kv("started", started.get())
				.kv("currentNodeHealth", currentNodeHealth)
				.kv("nodeDeadExpiredSeconds", nodeDeadExpiredSeconds)
				.kv("nodeConnectionFactory", nodeConnectionFactory.memory(keyRegex, memoryLevel));
		DataMap idNodeHandlerMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
		dataMap.kv("idNodeHandlerMap", idNodeHandlerMap);
		for(Map.Entry<String, NodeHandler> entry : this.idNodeHandlerMap.entrySet()) {
			idNodeHandlerMap.kv(entry.getKey(), entry.getValue());
		}
		return dataMap;
	}
}
