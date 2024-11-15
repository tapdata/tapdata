package io.tapdata.node.pdk;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.entity.GlobalConnectorResult;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMemoryHashMap;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2022-07-09 15:15
 **/
public class ConnectorNodeService {
	private static final Logger logger = LogManager.getLogger(ConnectorNodeService.class);
	private static final String GLOBAL_ASSOCIATE_ID_PREFIX = "GLOBAL_ASSOCIATE";
	public static final long CLEAN_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(30L);
	public static final long GLOBAL_CLEAN_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5L);
	private final Map<String, ConnectorNodeHolder> connectorNodeMap;
	private final AtomicBoolean cleanGlobalConnectorRunning = new AtomicBoolean(false);
	private ScheduledExecutorService cleanGlobalConnectorThreadPool;

	private ConnectorNodeService() {
		this.connectorNodeMap = new ConcurrentHashMap<>();
	}

	public void startCleanGlobalConnectorThreadPool() {
		if (cleanGlobalConnectorRunning.compareAndSet(false, true)) {
			long cleanInterval = GLOBAL_CLEAN_INTERVAL_MS;
			long cleanTimeout = CLEAN_TIMEOUT_MS;
			logger.info("Global connector thread pool started, interval ms: {}, timeout ms: {}", cleanInterval, cleanTimeout);
			this.cleanGlobalConnectorThreadPool = new ScheduledThreadPoolExecutor(1);
			this.cleanGlobalConnectorThreadPool.scheduleWithFixedDelay(() -> {
				Iterator<Map.Entry<String, ConnectorNodeHolder>> iterator = this.connectorNodeMap.entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry<String, ConnectorNodeHolder> connectorNodeHolderEntry = iterator.next();
					String associateId = connectorNodeHolderEntry.getKey();
					ConnectorNodeHolder connectorNodeHolder = connectorNodeHolderEntry.getValue();
					if (!associateId.startsWith(GLOBAL_ASSOCIATE_ID_PREFIX)) {
						continue;
					}
					long createTimeMs = connectorNodeHolder.getActiveTime();
					if (createTimeMs > 0L && (System.currentTimeMillis() - createTimeMs) > cleanTimeout) {
						ConnectorNode connectorNode = connectorNodeHolder.getConnectorNode();
						try {
							connectorNode.connectorStop();
							PDKIntegration.releaseAssociateId(associateId);
						} catch (Throwable e) {
							logger.error("Failed to release associate id {}", associateId, e);
						} finally {
							iterator.remove();
						}
					}
				}
			}, cleanInterval, cleanInterval, TimeUnit.MILLISECONDS);
		}
	}

	public static ConnectorNodeService getInstance() {
		return ConnectorNodeServiceInstance.SINGLETON.getInstance();
	}

	public static String globalAssociateId(Connections connections) {
		if (null == connections || StringUtils.isBlank(connections.getId())) {
			throw new RuntimeException("connection entity or connection id is empty");
		}
		return String.join("_", GLOBAL_ASSOCIATE_ID_PREFIX, connections.getId());
	}

	public String putConnectorNode(ConnectorNode connectorNode) {
		if (null == connectorNode || StringUtils.isBlank(connectorNode.getAssociateId())) {
			throw new RuntimeException("Store connector node failed, node is null or associateId is blank");
		}
		String associateId = connectorNode.getAssociateId();
		this.connectorNodeMap.put(associateId, new ConnectorNodeHolder(associateId, connectorNode));
		return connectorNode.getAssociateId();
	}

	public void globalConnectorNode(String associateId, Function<String, ConnectorNode> function) {
		if (null == function || StringUtils.isBlank(associateId)) {
			throw new RuntimeException("Store connector node failed, create node function is null or associateId is blank");
		}
		this.connectorNodeMap.computeIfPresent(associateId, (id, node) -> {
			node.setActiveTime(System.currentTimeMillis());
			return node;
		});
		this.connectorNodeMap.computeIfAbsent(associateId, id -> {
			ConnectorNode node = function.apply(id);
			return new ConnectorNodeHolder(id, node);
		});
	}

	public void globalConnectorNode(String connectionId, TapTableMap<String, TapTable> tapTableMap, Log log, BiConsumer<GlobalConnectorResult, RuntimeException> finishCallback) {
		GlobalConnectorResult globalConnectorResult = new GlobalConnectorResult();
		AtomicReference<RuntimeException> exceptionAtomicReference = new AtomicReference<>();
		try {
			Connections connection = ConnectionUtil.getConnection(connectionId, null);
			globalConnectorResult.setConnections(connection);
			Map<String, Object> connectionConfig = connection.getConfig();
			DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(connection.getPdkHash());
			PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
			PdkStateMap pdkStateMap = new PdkStateMemoryHashMap();
			PdkStateMap globalStateMap = new PdkStateMemoryHashMap();
			ConnectorCapabilities connectorCapabilities = ConnectorCapabilities.create();
			Map<String, Object> nodeConfig = new HashMap<>();
			String associateId = ConnectorNodeService.globalAssociateId(connection);
			globalConnectorResult.setAssociateId(associateId);
			ConnectorNodeService.getInstance().globalConnectorNode(associateId, id -> {
				ConnectorNode connectorNode = PdkUtil.createNode(new ObjectId().toHexString(),
						databaseType,
						ConnectorConstant.clientMongoOperator,
						id,
						connectionConfig,
						nodeConfig,
						pdkTableMap,
						pdkStateMap,
						globalStateMap,
						connectorCapabilities,
						null,
						log,
						null
				);
				long currentTimeMillis = System.currentTimeMillis();
				try {
					connectorNode.connectorLightInit();
				} catch (Throwable e) {
					PDKIntegration.releaseAssociateId(associateId);
					exceptionAtomicReference.set(new RuntimeException(String.format("Init connector node failed, connection: %s(%s)", connection.getName(), connectionId), e));
				} finally {
					globalConnectorResult.setConnectorNodeInitTaken(System.currentTimeMillis() - currentTimeMillis);
				}
				return connectorNode;
			});
		} catch (Exception e) {
			exceptionAtomicReference.set(new RuntimeException(e));
		} finally {
			if (null != finishCallback) {
				finishCallback.accept(globalConnectorResult, exceptionAtomicReference.get());
			}
		}
	}

	public ConnectorNode getConnectorNode(String associateId) {
		if (null == associateId) {
			return null;
		}
		ConnectorNodeHolder connectorNodeHolder = this.connectorNodeMap.get(associateId);
		return null == connectorNodeHolder ? null : connectorNodeHolder.connectorNode;
	}

	public ConnectorNode getConnectorNode(ConnectorNode connectorNode) {
		ConnectorNodeHolder connectorNodeHolder = this.connectorNodeMap.get(connectorNode.getAssociateId());
		return null == connectorNodeHolder ? null : connectorNodeHolder.getConnectorNode();
	}

	public void removeConnectorNode(String associateId) {
		if (null != associateId) {
			this.connectorNodeMap.remove(associateId);
		}
	}

	private enum ConnectorNodeServiceInstance {
		SINGLETON;
		private final ConnectorNodeService connectorNodeService;

		ConnectorNodeServiceInstance() {
			this.connectorNodeService = new ConnectorNodeService();
		}

		public ConnectorNodeService getInstance() {
			return connectorNodeService;
		}
	}

	private static class ConnectorNodeHolder {
		private final String associateId;
		private final ConnectorNode connectorNode;
		private long activeTime;

		public ConnectorNodeHolder(String associateId, ConnectorNode connectorNode) {
			this.associateId = associateId;
			this.connectorNode = connectorNode;
			this.activeTime = System.currentTimeMillis();
		}

		public String getAssociateId() {
			return associateId;
		}

		public ConnectorNode getConnectorNode() {
			return connectorNode;
		}

		public long getActiveTime() {
			return activeTime;
		}

		public void setActiveTime(long activeTime) {
			this.activeTime = activeTime;
		}
	}
}
