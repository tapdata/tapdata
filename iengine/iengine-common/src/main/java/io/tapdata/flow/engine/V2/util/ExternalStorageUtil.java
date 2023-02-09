package io.tapdata.flow.engine.V2.util;

import com.hazelcast.config.Config;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceHttpConfig;
import com.hazelcast.persistence.config.PersistenceInMemConfig;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.mongodb.MongoClientURI;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.json.JsonWriterSettings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-09-14 10:19
 **/
public class ExternalStorageUtil {
	private final static String LOG_PREFIX = "[Hazelcast IMDG Persistence] - ";
	private static final Logger logger = LogManager.getLogger(ExternalStorageUtil.class);
	public static final int DEFAULT_IN_MEM_SIZE = 1;

	public synchronized static void initHZMapStorage(ExternalStorageDto externalStorageDto, String name, Config config) {
		addConfig(externalStorageDto, ConstructType.IMAP, name);
		try {
			PersistenceStorage.getInstance().initMapStoreConfig(config, name);
			logger.info("Init IMap store config succeed, name: " + name);
		} catch (Exception e) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast IMap persistence failed. " + e.getMessage(), e);
		}
	}

	public synchronized static void initHZRingBufferStorage(ExternalStorageDto externalStorageDto, String name, Config config) {
		addConfig(externalStorageDto, ConstructType.RINGBUFFER, name);
		try {
			PersistenceStorage.getInstance().initRingBufferConfig(config, name);
			logger.info("Init RingBuffer store config succeed, name: " + name);
		} catch (Exception e) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast RingBuffer persistence failed. " + e.getMessage(), e);
		}
	}

	private static void addConfig(ExternalStorageDto externalStorageDto, ConstructType constructType, String constructName) {
		if (null == externalStorageDto) throw new IllegalArgumentException("External storage dto cannot be null");
		PersistenceStorageAbstractConfig persistenceConfig = getPersistenceConfig(externalStorageDto, constructType, constructName);
		if (null == persistenceConfig) {
			return;
		}
		checkConfigOverrideAndLogger(persistenceConfig);
		PersistenceStorage.getInstance().addConfig(persistenceConfig);
		logger.info("Added hazelcast persistence config: " + persistenceConfig);
	}

	private static PersistenceStorageAbstractConfig getPersistenceConfig(ExternalStorageDto externalStorageDto, ConstructType constructType, String constructName) {
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig;
		ExternalStorageType externalStorageType;
		try {
			externalStorageType = ExternalStorageType.valueOf(externalStorageDto.getType());
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Nonsupport external storage type: " + externalStorageDto.getType());
		}
		// Set properties
		switch (externalStorageType) {
			case memory:
				persistenceStorageAbstractConfig = PersistenceInMemConfig.create(constructType, constructName);
				persistenceStorageAbstractConfig.setInMemSize(DEFAULT_IN_MEM_SIZE);
				break;
			case mongodb:
				persistenceStorageAbstractConfig = getMongoDBConfig(externalStorageDto, constructType, constructName);
				break;
			case rocksdb:
				persistenceStorageAbstractConfig = getRocksDBConfig(externalStorageDto, constructType, constructName);
				break;
			case httptm:
				persistenceStorageAbstractConfig = getHttpTMConfig(externalStorageDto, constructType, constructName);
				break;
			default:
				throw new RuntimeException("Nonsupport external storage type: " + externalStorageDto.getType());
		}
		return persistenceStorageAbstractConfig;
	}

	private static PersistenceRocksDBConfig getRocksDBConfig(ExternalStorageDto externalStorageDto, ConstructType constructType, String constructName) {
		String rocksdbPath = externalStorageDto.getUri();
		if (StringUtils.isBlank(rocksdbPath)) {
			throw new IllegalArgumentException(LOG_PREFIX + "Init hazelcast persist config failed. RocksDB path cannot be empty");
		}
		PersistenceRocksDBConfig rocksDBConfig = PersistenceRocksDBConfig.create(constructType, constructName)
				.path(rocksdbPath);
		rocksDBConfig.setInMemSize(DEFAULT_IN_MEM_SIZE);
		return rocksDBConfig;
	}

	private static PersistenceMongoDBConfig getMongoDBConfig(ExternalStorageDto externalStorageDto, ConstructType constructType, String constructName) {
		String uri = externalStorageDto.getUri();
		MongoClientURI mongoClientURI;
		try {
			mongoClientURI = MongodbUtil.verifyMongoDBUriWithDB(uri);
		} catch (Exception e) {
			throw new IllegalArgumentException(LOG_PREFIX + "Init hazelcast persistence failed" + e.getMessage());
		}
		String table = externalStorageDto.getTable();
		if (StringUtils.isBlank(table)) {
			throw new IllegalArgumentException(LOG_PREFIX + "Init hazelcast persistence failed. Collection name cannot be empty");
		}
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(constructType, constructName)
				.uri(uri)
				.database(mongoClientURI.getDatabase())
				.collection(table);
		mongoDBConfig.setInMemSize(DEFAULT_IN_MEM_SIZE);
		return mongoDBConfig;
	}

	private static PersistenceHttpConfig getHttpTMConfig(ExternalStorageDto externalStorageDto, ConstructType constructType, String constructName) {
		if (StringUtils.isBlank(externalStorageDto.getBaseUrl())) {
			throw new RuntimeException(LOG_PREFIX + "Base url cannot be empty");
		}
		if (StringUtils.isBlank(externalStorageDto.getAccessToken())) {
			throw new IllegalArgumentException(LOG_PREFIX + "Access token cannot be empty");
		}
		PersistenceHttpConfig httpConfig = PersistenceHttpConfig.create(constructType, constructName, externalStorageDto.getBaseUrl(), externalStorageDto.getAccessToken())
				.connectTimeoutMs(externalStorageDto.getConnectTimeoutMs())
				.readTimeoutMs(externalStorageDto.getReadTimeoutMs());
		httpConfig.setInMemSize(DEFAULT_IN_MEM_SIZE);
		return httpConfig;
	}

	private static void checkConfigOverrideAndLogger(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		PersistenceStorage persistenceStorage = PersistenceStorage.getInstance();
		PersistenceStorageAbstractConfig existingConfig = persistenceStorage.getPersistenceStorageConfig(persistenceStorageAbstractConfig.getConstructType(), persistenceStorageAbstractConfig.getName());
		if (null != existingConfig && !persistenceStorageAbstractConfig.equals(existingConfig)) {
			logger.info(LOG_PREFIX + "Existing persistence config will be override\n old: " + existingConfig + "\n new: " + persistenceStorageAbstractConfig);
		}
	}

	public static Map<String, ExternalStorageDto> getExternalStorageMap(TaskDto taskDto, ClientMongoOperator clientMongoOperator) {
		Map<String, ExternalStorageDto> externalStorageDtoMap = new HashMap<>();
		com.tapdata.tm.commons.dag.DAG dag = taskDto.getDag();
		List<Node> nodes = dag.getNodes();
		if (CollectionUtils.isEmpty(nodes)) {
			logger.warn(String.format("Init external storage config failed. Task [%s] not have any node", taskDto.getName()));
			return externalStorageDtoMap;
		}
		Set<String> ids = new HashSet<>();
		String syncType = taskDto.getSyncType();
		switch (syncType) {
			case TaskDto.SYNC_TYPE_SYNC:
			case TaskDto.SYNC_TYPE_MIGRATE:
				for (Node node : nodes) {
					if (StringUtils.isNotBlank(node.getExternalStorageId())) {
						ids.add(node.getExternalStorageId());
					}
				}
				break;
			case TaskDto.SYNC_TYPE_LOG_COLLECTOR:
				Node logCollectorNode = nodes.stream().filter(node -> node.getType().equals(NodeTypeEnum.LOG_COLLECTOR.type)).findFirst().orElse(null);
				if (null == logCollectorNode) {
					logger.warn("Init external storage config failed. Not found log collector node in task");
					break;
				}
				if (logCollectorNode instanceof LogCollectorNode) {
					List<String> connectionIds = ((LogCollectorNode) logCollectorNode).getConnectionIds();
					if (CollectionUtils.isNotEmpty(connectionIds)) {
						String connectionId = connectionIds.get(0);
						Query connQuery = Query.query(where("_id").is(connectionId));
						connQuery.fields().include("_id").include("shareCDCExternalStorageId");
						Connections connection = clientMongoOperator.findOne(connQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
						String shareCDCExternalStorageId = connection.getShareCDCExternalStorageId();
						if (StringUtils.isNotBlank(shareCDCExternalStorageId)) {
							ids.add(shareCDCExternalStorageId);
						}
					}
				}
				break;
			default:
				break;
		}
		Criteria criteria = new Criteria().orOperator(
				// Get system inner config with constant name. Reference: manager/tm/src/main/resources/init/idaas/2.10-1.json
				where("name").is(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME),
				// Get default config
				where("defaultStorage").is(true),
				where("_id").in(ids)
		);
		List<ExternalStorageDto> externalStorageDtoList = clientMongoOperator.find(Query.query(criteria), ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
		if (CollectionUtils.isEmpty(externalStorageDtoList)) {
			throw new RuntimeException(String.format("Not found any external storage config: %s", criteria.getCriteriaObject().toJson(JsonWriterSettings.builder().indent(true).build())));
		}
		logger.info("Task init external storage configs completed: {}" + externalStorageDtoList.stream().map(ExternalStorageDto::getName).collect(Collectors.joining(",")));
		externalStorageDtoMap = externalStorageDtoList.stream().collect(Collectors.toMap(e -> e.getId().toHexString(), e -> e));
		return externalStorageDtoMap;
	}

	public static ExternalStorageDto getExternalStorage(
			@NotNull Map<String, ExternalStorageDto> externalStorageDtoMap,
			@NotNull Node node,
			@NotNull ClientMongoOperator clientMongoOperator,
			List<Node> nodes,
			Connections connections
	) {
		ExternalStorageDto externalStorageDto;
		if (MapUtils.isEmpty(externalStorageDtoMap)) {
			throw new RuntimeException("External storage map cannot be empty");
		}
		if (node instanceof TableNode || node instanceof DatabaseNode || node instanceof LogCollectorNode) {
			if (null == connections) {
				throw new RuntimeException("Init node " + node.getName() + "(id: " + node.getId() + ", type: " + node.getClass().getSimpleName() + ") external storage failed, connection is null");
			}
			externalStorageDto = getPdkStateMapExternalStorage(externalStorageDtoMap, node, connections);
		} else if (node instanceof HazelCastImdgNode) {
			externalStorageDto = getShareCDCExternalStorage(externalStorageDtoMap, node, clientMongoOperator, nodes);
		} else {
			externalStorageDto = getExternalStorageDto(externalStorageDtoMap, node);
		}
		if (null == externalStorageDto) {
			externalStorageDto = getDefaultExternalStorage(externalStorageDtoMap, node);
		}
		return externalStorageDto;
	}

	public static ExternalStorageDto getExternalStorage(Node node) {
		ClientMongoOperator clientMongoOperator = ConnectorConstant.clientMongoOperator;
		return getExternalStorage(node, null, clientMongoOperator, null);
	}

	public static ExternalStorageDto getExternalStorage(
			@NotNull Node node,
			List<Node> nodes,
			@NotNull ClientMongoOperator clientMongoOperator,
			Connections connections
	) {
		ExternalStorageDto externalStorageDto;
		if (node instanceof TableNode || node instanceof DatabaseNode || node instanceof LogCollectorNode) {
			if (null == connections) {
				throw new RuntimeException("Init node " + node.getName() + "(id: " + node.getId() + ", type: " + node.getClass().getSimpleName() + ") external storage failed, connection is null");
			}
			externalStorageDto = getPdkStateMapExternalStorage(node, connections, clientMongoOperator);
		} else if (node instanceof HazelCastImdgNode) {
			externalStorageDto = getShareCDCExternalStorage(node, nodes, clientMongoOperator);
		} else {
			externalStorageDto = getExternalStorageDto(node, clientMongoOperator);
		}
		if (null == externalStorageDto) {
			externalStorageDto = getDefaultExternalStorage(node, clientMongoOperator);
		}
		return externalStorageDto;
	}

	@Nullable
	private static ExternalStorageDto getDefaultExternalStorage(
			@NotNull Map<String, ExternalStorageDto> externalStorageDtoMap,
			@NotNull Node node
	) {
		ExternalStorageDto externalStorageDto;
		externalStorageDto = externalStorageDtoMap.values().stream().filter(ExternalStorageDto::isDefaultStorage).findFirst().orElse(null);
		if (null == externalStorageDto) {
			externalStorageDto = externalStorageDtoMap.values().stream().filter(e -> e.getName().equals(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME)).findFirst().orElse(null);
		}
		if (null != externalStorageDto) {
			logger.info("Node {}(id: {}, type: {}) use default external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
		}
		return externalStorageDto;
	}

	@Nullable
	private static ExternalStorageDto getDefaultExternalStorage(
			@NotNull Node node,
			@NotNull ClientMongoOperator clientMongoOperator
	) {
		ExternalStorageDto externalStorageDto;
		Query query = Query.query(where("defaultStorage").is(true));
		externalStorageDto = clientMongoOperator.findOne(query, ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
		if (null == externalStorageDto) {
			query = Query.query(where("name").is(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME));
			externalStorageDto = clientMongoOperator.findOne(query, ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
		}
		if (null != externalStorageDto) {
			logger.info("Node {}(id: {}, type: {}) use default external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
		}
		return externalStorageDto;
	}

	@Nullable
	private static ExternalStorageDto getExternalStorageDto(
			@NotNull Map<String, ExternalStorageDto> externalStorageDtoMap,
			@NotNull Node node
	) {
		ExternalStorageDto externalStorageDto = null;
		String externalStorageId = node.getExternalStorageId();
		if (StringUtils.isNotBlank(externalStorageId)) {
			externalStorageDto = externalStorageDtoMap.get(externalStorageId);
			if (null != externalStorageDto) {
				logger.info("Node {}(id: {}, type: {}) use external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
			}
		}
		return externalStorageDto;
	}

	@Nullable
	private static ExternalStorageDto getExternalStorageDto(
			@NotNull Node node,
			@NotNull ClientMongoOperator clientMongoOperator
	) {
		ExternalStorageDto externalStorageDto = null;
		String externalStorageId = node.getExternalStorageId();
		if (StringUtils.isNotBlank(externalStorageId)) {
			Query query = Query.query(where("_id").is(externalStorageId));
			externalStorageDto = clientMongoOperator.findOne(query, ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
			if (null != externalStorageDto) {
				logger.info("Node {}(id: {}, type: {}) use external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
			}
		}
		return externalStorageDto;
	}

	@Nullable
	private static ExternalStorageDto getShareCDCExternalStorage(
			@NotNull Map<String, ExternalStorageDto> externalStorageDtoMap,
			@NotNull Node node,
			@NotNull ClientMongoOperator clientMongoOperator,
			List<Node> nodes
	) {
		ExternalStorageDto externalStorageDto = null;
		// Find source log collector node
		if (CollectionUtils.isEmpty(nodes)) {
			throw new RuntimeException(String.format("Init node %s(id: %s, type: %s) external storage failed, node list is empty", node.getName(), node.getId(), node.getClass().getSimpleName()));
		}
		Node logCollectorNode = nodes.stream().filter(n -> n instanceof LogCollectorNode).findFirst().orElse(null);
		if (logCollectorNode instanceof LogCollectorNode) {
			// Get the external storage config of the pre-log-collector node
			List<String> connectionIds = ((LogCollectorNode) logCollectorNode).getConnectionIds();
			if (CollectionUtils.isNotEmpty(connectionIds)) {
				String connectionId = connectionIds.get(0);
				Query connQuery = Query.query(Criteria.where("_id").is(connectionId));
				connQuery.fields().include("_id").include("shareCDCExternalStorageId");
				Connections logCollectorNodeConn = clientMongoOperator.findOne(connQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
				if (null != logCollectorNodeConn && StringUtils.isNotBlank(logCollectorNodeConn.getShareCDCExternalStorageId())) {
					externalStorageDto = externalStorageDtoMap.get(logCollectorNodeConn.getShareCDCExternalStorageId());
					if (null != externalStorageDto) {
						logger.info("Node {}(id: {}, type: {}) use external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
					}
				}
			}
		}
		return externalStorageDto;
	}

	@Nullable
	private static ExternalStorageDto getShareCDCExternalStorage(
			@NotNull Node node,
			List<Node> nodes,
			@NotNull ClientMongoOperator clientMongoOperator
	) {
		ExternalStorageDto externalStorageDto = null;
		// Find source log collector node
		if (CollectionUtils.isEmpty(nodes)) {
			throw new RuntimeException(String.format("Init node %s(id: %s, type: %s) external storage failed, node list is empty", node.getName(), node.getId(), node.getClass().getSimpleName()));
		}
		Node logCollectorNode = nodes.stream().filter(n -> n instanceof LogCollectorNode).findFirst().orElse(null);
		if (logCollectorNode instanceof LogCollectorNode) {
			// Get the external storage config of the pre-log-collector node
			List<String> connectionIds = ((LogCollectorNode) logCollectorNode).getConnectionIds();
			if (CollectionUtils.isNotEmpty(connectionIds)) {
				String connectionId = connectionIds.get(0);
				Query connQuery = Query.query(Criteria.where("_id").is(connectionId));
				connQuery.fields().include("_id").include("shareCDCExternalStorageId");
				Connections logCollectorNodeConn = clientMongoOperator.findOne(connQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
				if (null != logCollectorNodeConn && StringUtils.isNotBlank(logCollectorNodeConn.getShareCDCExternalStorageId())) {
					Query query = Query.query(where("_id").is(logCollectorNodeConn.getShareCDCExternalStorageId()));
					externalStorageDto = clientMongoOperator.findOne(query, ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
					if (null != externalStorageDto) {
						logger.info("Node {}(id: {}, type: {}) use external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
					}
				}
			}
		}
		return externalStorageDto;
	}

	public static ExternalStorageDto getPdkStateMapExternalStorage(
			@NotNull Map<String, ExternalStorageDto> externalStorageDtoMap,
			@NotNull Node node,
			@NotNull Connections connections
	) {
		ExternalStorageDto externalStorageDto = null;
		if (node instanceof TableNode || node instanceof DatabaseNode || node instanceof LogCollectorNode) {
			if ("pdk".equals(connections.getPdkType())) {
				// External storage for pdk
				externalStorageDto = externalStorageDtoMap.values().stream().filter(e -> e.getName().equals(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME)).findFirst().orElse(null);
			}
			if (null != externalStorageDto) {
				logger.info("Node {}(id: {}, type: {}) use external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
			}
		}
		return externalStorageDto;
	}

	public static ExternalStorageDto getPdkStateMapExternalStorage(
			@NotNull Node node,
			@NotNull Connections connections,
			@NotNull ClientMongoOperator clientMongoOperator
	) {
		ExternalStorageDto externalStorageDto = null;
		if (node instanceof TableNode || node instanceof DatabaseNode || node instanceof LogCollectorNode) {
			if ("pdk".equals(connections.getPdkType())) {
				// External storage for pdk
				Query query = Query.query(where("name").is(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME));
				externalStorageDto = clientMongoOperator.findOne(query, ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
			}
			if (null != externalStorageDto) {
				logger.info("Node {}(id: {}, type: {}) use external storage config: {}", node.getName(), node.getId(), node.getClass().getSimpleName(), externalStorageDto);
			}
		}
		return externalStorageDto;
	}
}
