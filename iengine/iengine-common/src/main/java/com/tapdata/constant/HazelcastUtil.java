package com.tapdata.constant;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.tapdata.cache.hazelcast.HazelcastCacheStats;
import com.tapdata.cache.hazelcast.serializer.HazelcastCacheStatsSerializer;
import com.tapdata.cache.hazelcast.serializer.HazelcastDataFlowCacheConfigSerializer;
import com.tapdata.entity.Connections;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.SyncObjects;
import com.tapdata.entity.hazelcast.HZLoggingType;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.client.properties.ClientProperty.METRICS_ENABLED;

/**
 * @author samuel
 * @Description
 * @create 2021-11-23 16:03
 **/
public class HazelcastUtil {

	private final static String KEYSTORE_DIR = ".keystore";
	private final static String KEYSTORE_SUFFIX = ".keystore";
	private final static String TRUSTSTORE_SUFFIX = ".truststore";
	private static final String DEFAULT_CALL_TIMEOUT = String.valueOf(TimeUnit.MINUTES.toMillis(5L));
	public static final HZLoggingType DEFAULT_HZ_LOGGING_TYPE = HZLoggingType.LOG4J2;
	private static Logger logger = LogManager.getLogger(HazelcastUtil.class);

	public static Config getConfig(String instanceName) {
		return getConfig(instanceName, DEFAULT_HZ_LOGGING_TYPE);
	}

	public static Config getConfig(String instanceName, HZLoggingType hzLoggingType) {
		Config config = new Config();
		config.getJetConfig().setEnabled(true);
		JoinConfig joinConfig = new JoinConfig();
		joinConfig.setTcpIpConfig(new TcpIpConfig().setEnabled(true));
		NetworkConfig networkConfig = new NetworkConfig();
		networkConfig.setJoin(joinConfig);
		config.setNetworkConfig(networkConfig);
		config.setInstanceName(instanceName);
		setSystemProperties(config, hzLoggingType);
		SerializerConfig hazelcastCacheStatsSerializer = new SerializerConfig().setImplementation(new HazelcastCacheStatsSerializer()).setTypeClass(HazelcastCacheStats.class);
		SerializerConfig hazelcastDataFlowCacheConfigSerializer = new SerializerConfig().setImplementation(new HazelcastDataFlowCacheConfigSerializer()).setTypeClass(DataFlowCacheConfig.class);
		config.getSerializationConfig().addSerializerConfig(hazelcastCacheStatsSerializer);
		config.getSerializationConfig().addSerializerConfig(hazelcastDataFlowCacheConfigSerializer);
		return config;
	}

	private static void setSystemProperties(Config config, HZLoggingType hzLoggingType) {
		if (null == config) {
			return;
		}
		config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), CommonUtils.getProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), DEFAULT_CALL_TIMEOUT));
		config.setProperty(ClusterProperty.LOGGING_TYPE.getName(), hzLoggingType.getType());
		Properties properties = config.getProperties();
		StringWriter stringWriter = new StringWriter();
		try (
				PrintWriter printWriter = new PrintWriter(stringWriter)
		) {
			properties.list(printWriter);
			logger.info(stringWriter.toString());
		}
	}

	/**
	 * Get tapdata agent inner hazelcast instance
	 *
	 * @param configurationCenter
	 * @return instance maybe null
	 */
	public static HazelcastInstance getInstance(ConfigurationCenter configurationCenter) {
		String agentId = (String) configurationCenter.getConfig(ConfigurationCenter.AGENT_ID);
		return Hazelcast.getHazelcastInstanceByName(agentId);
	}

	public static HazelcastInstance getInstance() {
		String processId = ConfigurationCenter.processId;
		if (StringUtils.isBlank(processId))
			throw new RuntimeException("Get Hazelcast instance failed, process_id is blank");
		return Hazelcast.getHazelcastInstanceByName(processId);
	}

	public static HazelcastInstance getClient(String clusterName) throws Exception {
		System.setProperty("hazelcast.ignoreXxeProtectionFailures", "true");
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.setClusterName(clusterName);
		clientConfig.getNetworkConfig().setConnectionTimeout(10 * 1000);
		clientConfig.setConnectionStrategyConfig(new ClientConnectionStrategyConfig().setConnectionRetryConfig(new ConnectionRetryConfig().setClusterConnectTimeoutMillis(5 * 1000)));
		clientConfig.setProperty("hazelcast.logging.type", "log4j2");
		return HazelcastClient.newHazelcastClient(clientConfig);
	}

	private static void checkSSLConfig(String sslKey, String sslCA, String sslPass) throws Exception {
		if (StringUtils.isBlank(sslKey)) {
			throw new Exception("Config Hazelcast SSL failed; Keystore file is empty");
		}
		if (StringUtils.isBlank(sslCA)) {
			throw new Exception("Config Hazelcast SSL failed; Truststore file is empty");
		}
		if (StringUtils.isBlank(sslPass)) {
			throw new Exception("Config Hazelcast SSL failed; Store file password is empty");
		}
	}

	public static void closeClient(HazelcastInstance client) {
		if (client != null) {
			try {
				client.shutdown();
			} catch (Exception ignore) {
			}
		}
	}

	public static Stage node2CommonStage(Node node) {
		Stage stage = new Stage();
		if (node.isDataNode() && node instanceof TableNode) {
			TableNode tableNode = (TableNode) node;
			stage.setConnectionId(tableNode.getConnectionId());
			stage.setDatabaseType(tableNode.getDatabaseType());
			if ("keepData".equals(tableNode.getExistDataProcessMode())) {
				stage.setDropType("no_drop");
			} else if ("removeData".equals(tableNode.getExistDataProcessMode())) {
				stage.setDropType("drop_data");
			} else {
				stage.setDropType("dropTable");
			}
			stage.setTableName(((TableNode) node).getTableName());
			//todo
//    stage.setDropTable(tableNode.getDropTable());
//    stage.setEnableInitialOrder(((TableNode) node).getEnableInitialOrder());
//    stage.setInitialSyncOrder(((TableNode) node).getInitialSyncOrder());
//    stage.setInitialOffset(((TableNode) node).getInitialOffset());
		} else if (node instanceof LogCollectorNode) {
			stage.setConnectionId(((LogCollectorNode) node).getConnectionIds().get(0));
		} else if (node instanceof CacheNode) {
			CacheNode cacheNode = (CacheNode) node;
			stage.setCacheKeys(cacheNode.getCacheKeys());
			stage.setCacheName(cacheNode.getCacheName());
			stage.setTtl(cacheNode.getTtl());
			stage.setFields(new HashSet<>(cacheNode.getFields()));
		} else if (node instanceof DatabaseNode) {
			stage.setConnectionId(((DatabaseNode) node).getConnectionId());
			DatabaseNode databaseNode = (DatabaseNode) node;
			List<SyncObjects> dataFlowSyncObjects = new ArrayList<>();
			final List<com.tapdata.tm.commons.dag.vo.SyncObjects> syncObjects = databaseNode.getSyncObjects();
			if (CollectionUtils.isNotEmpty(syncObjects)) {
				for (com.tapdata.tm.commons.dag.vo.SyncObjects syncObject : syncObjects) {
					dataFlowSyncObjects.add(new com.tapdata.entity.dataflow.SyncObjects(
							syncObject.getType(),
							syncObject.getObjectNames()
					));
				}

				stage.setTablePrefix(databaseNode.getTablePrefix());
				stage.setTableSuffix(databaseNode.getTableSuffix());
				stage.setTableNameTransform(databaseNode.getTableNameTransform());
				stage.setSyncObjects(dataFlowSyncObjects);

			}
		}
		stage.setId(node.getId());
		stage.setType(node.getType());
		stage.setName(node.getName());
		stage.setInputLanes(new ArrayList<>());
		stage.setOutputLanes(new ArrayList<>());

		return stage;
	}

	public static List<Stage> node2Stages(Node node) {
		List<Stage> stages = new ArrayList<>();
		Stage stage = node2CommonStage(node);
		stages.add(stage);
		return stages;
	}
}
