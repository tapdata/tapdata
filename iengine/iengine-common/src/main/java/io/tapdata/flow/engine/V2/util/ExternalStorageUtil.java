package io.tapdata.flow.engine.V2.util;

import com.hazelcast.config.Config;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.StorageMode;
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

import java.util.*;
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
	public static final int DEFAULT_IN_MEM_SIZE = 100;

	public static void initHazelcastDefaultPersistence(ClientMongoOperator clientMongoOperator, Config config) {
		Query query = Query.query(where("name").is(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME));
		ExternalStorageDto externalStorageDto = clientMongoOperator.findOne(query, ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
		if (null == externalStorageDto) {
			throw new RuntimeException(LOG_PREFIX + String.format("Init hazelcast default persistence failed. Default config name '%s' not exists", ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME));
		}
		initHZPersistenceStorage(externalStorageDto, "", config);
	}

	public synchronized static void initHZPersistenceStorage(ExternalStorageDto externalStorageDto, String name, Config config) {
		initProperty(externalStorageDto);
		// Init hazelcast config
		try {
			if (StringUtils.isBlank(name)) {
				PersistenceStorage.getInstance().initHZConfig(config);
			} else {
				PersistenceStorage.getInstance().initHZConfig(config, name);
			}
		} catch (Exception e) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast persistence failed. " + e.getMessage(), e);
		}
	}

	public synchronized static void initHZMapStorage(ExternalStorageDto externalStorageDto, String name, Config config) {
		initProperty(externalStorageDto);
		// Init hazelcast config
		try {
			if (StringUtils.isBlank(name)) {
				PersistenceStorage.getInstance().initMapStoreConfig(config);
			} else {
				PersistenceStorage.getInstance().initMapStoreConfig(config, name);
			}
		} catch (Exception e) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast IMap persistence failed. " + e.getMessage(), e);
		}
	}

	public synchronized static void initHZRingBufferStorage(ExternalStorageDto externalStorageDto, String name, Config config) {
		initProperty(externalStorageDto);
		// Init hazelcast config
		try {
			if (StringUtils.isBlank(name)) {
				PersistenceStorage.getInstance().initRingBufferConfig(config);
			} else {
				PersistenceStorage.getInstance().initRingBufferConfig(config, name);
			}
		} catch (Exception e) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast RingBuffer persistence failed. " + e.getMessage(), e);
		}
	}

	private static void initProperty(ExternalStorageDto externalStorageDto) {
		if (null == externalStorageDto) throw new IllegalArgumentException("External storage dto cannot be null");
		PersistenceStorage persistenceStorage = PersistenceStorage.getInstance();
		// Set storage mode
		ExternalStorageType externalStorageType = initMode(externalStorageDto, persistenceStorage);
		// Set properties
		switch (externalStorageType) {
			case memory:
				break;
			case mongodb:
				initMongoDBProperty(externalStorageDto, persistenceStorage);
				break;
			case rocksdb:
				initRocksDBProperty(externalStorageDto, persistenceStorage);
				break;
			default:
				break;
		}
		// Set memory size
		persistenceStorage.setInMemSize(DEFAULT_IN_MEM_SIZE);
	}

	private static void initRocksDBProperty(ExternalStorageDto externalStorageDto, PersistenceStorage persistenceStorage) {
		String rocksdbPath = externalStorageDto.getUri();
		if (StringUtils.isBlank(rocksdbPath)) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast default persistence failed. RocksDB path cannot be empty");
		}
		persistenceStorage.setRocksDBPath(rocksdbPath);
		logger.info(LOG_PREFIX + String.format("Hazelcast default persistence RocksDB config\n - path: %s", rocksdbPath));
	}

	private static void initMongoDBProperty(ExternalStorageDto externalStorageDto, PersistenceStorage persistenceStorage) {
		String uri = externalStorageDto.getUri();
		MongoClientURI mongoClientURI;
		try {
			mongoClientURI = MongodbUtil.verifyMongoDBUriWithDB(uri);
		} catch (Exception e) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast default persistence failed. " + e.getMessage());
		}
		String database = mongoClientURI.getDatabase();
		String table = externalStorageDto.getTable();
		if (StringUtils.isBlank(table)) {
			throw new RuntimeException(LOG_PREFIX + "Init hazelcast default persistence failed. Collection name cannot be empty");
		}
		persistenceStorage.setMongoUri(uri);
		persistenceStorage.setDB(database);
		persistenceStorage.setCollection(table);
		logger.info(LOG_PREFIX + String.format("Hazelcast default persistence MongoDB config\n - uri: %s\n - database: %s\n - collection: %s", uri, database, table));
	}

	@NotNull
	private static ExternalStorageType initMode(ExternalStorageDto externalStorageDto, PersistenceStorage persistenceStorage) {
		ExternalStorageType externalStorageType;
		if (null == externalStorageDto.getType()) {
			throw new IllegalArgumentException(LOG_PREFIX + "Init hazelcast default persistence failed. Type cannot be null");
		}
		try {
			externalStorageType = ExternalStorageType.valueOf(externalStorageDto.getType());
		} catch (Throwable e) {
			throw new RuntimeException(LOG_PREFIX + String.format("Init hazelcast default persistence failed. Type '%s' is invalid", externalStorageDto.getType()), e);
		}
		StorageMode storageMode;
		try {
			storageMode = StorageMode.valueOf(externalStorageType.getMode());
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(LOG_PREFIX + String.format("Init hazelcast default persistence failed. Type '%s' is invalid", externalStorageDto.getType()), e);
		}
		persistenceStorage.setStorageMode(storageMode);
		logger.info(LOG_PREFIX + "Hazelcast default persistence mode: " + storageMode);
		return externalStorageType;
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
				// Get system inner config with constant name. Reference: manager/tm/src/main/resources/init/idaas/2.9-1.json
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
