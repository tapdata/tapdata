package io.tapdata.common.sharecdc;

import com.hazelcast.config.Config;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.StorageMode;
import com.mongodb.MongoClientURI;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.hazelcast.HazelcastConstant;
import com.tapdata.entity.hazelcast.PersistenceStorageConfig;
import com.tapdata.entity.sharecdc.ShareCdcConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-01-27 10:35
 **/
public class ShareCdcUtil {

	private final static Logger logger = LogManager.getLogger(ShareCdcUtil.class);
	private final static String LOG_PREFIX = "[Hazelcast IMDG Persistence] - ";
	private final static String DEFAULT_COLLECTION_NAME = "HZPersistence";
	private final static StorageMode DEFAULT_STORAGE_MODE = StorageMode.MongoDB;
	private final static Integer DEFAULT_MEMORY_SIZE = 1;
	private final static String DEFAULT_ROCKSDB_DBPATH = "." + File.separator + "rocksdb-data" + File.separator;
	private final static String SHARE_CDC_KEY_PREFIX = "SHARE_CDC_";

	private static void setDefaultPersistenceConfig(SettingService settingService, ClientMongoOperator clientMongoOperator) {
		settingService.setValue(HazelcastConstant.SETTING_PERSISTENCE_MODE, DEFAULT_STORAGE_MODE.name());
		settingService.setValue(HazelcastConstant.SETTING_PERSISTENCE_MEMORY_SIZE, String.valueOf(DEFAULT_MEMORY_SIZE));
		settingService.setValue(HazelcastConstant.SETTING_PERSISTENCE_MONGODB_URI, clientMongoOperator.getMongoClientURI().getURI());
		settingService.setValue(HazelcastConstant.SETTING_PERSISTENCE_MONGODB_COLLECTION, DEFAULT_COLLECTION_NAME);
	}

	/**
	 * According to the tapdata settings, initialize the Hazelcast IMDG Persistence Storage
	 *
	 * @param config
	 * @param settingService
	 * @param clientMongoOperator
	 */
	public static void initHazelcastPersistenceStorage(Config config, SettingService settingService, ClientMongoOperator clientMongoOperator) {
		try {
			if (config == null || settingService == null || clientMongoOperator == null) {
				throw new IllegalArgumentException(LOG_PREFIX + "Init hazelcast persistence storage failed; Config, SettingService, ClientMongoOperator cannot be null");
			}
//      setDefaultPersistenceConfig(settingService, clientMongoOperator);
			PersistenceStorage persistenceStorage = PersistenceStorage.getInstance();
			PersistenceStorageConfig persistenceStorageConfig = PersistenceStorageConfig.getInstance();
			Map<String, List<String>> changeMap = new HashMap<>();

			String oldMode = persistenceStorageConfig.getStorageMode() == null ? "" : persistenceStorageConfig.getStorageMode().name();
			String newMode = settingService.getString(HazelcastConstant.SETTING_PERSISTENCE_MODE);
			if (!oldMode.equals(newMode)) {
				StorageMode storageMode = setStorageMode(settingService, persistenceStorage);
				persistenceStorageConfig.setStorageMode(storageMode);
				changeMap.put("Storage mode", Arrays.asList(oldMode, newMode));
				switch (storageMode) {
					case Mem:
						PersistenceStorageConfig.clearMongoConfig();
						PersistenceStorageConfig.clearRocksDbConfig();
						break;
					case MongoDB:
						setMongodbStorage(settingService, persistenceStorage, clientMongoOperator, changeMap);
						PersistenceStorageConfig.clearRocksDbConfig();
						break;
					case RocksDB:
						setRocksdbStorage(settingService, persistenceStorage, changeMap);
						PersistenceStorageConfig.clearMongoConfig();
						break;
					default:
						throw new RuntimeException("Unrecognized hazelcast storage persistence mode: " + storageMode);
				}
			}
			setMemorySize(settingService, persistenceStorage, changeMap);
			if (MapUtils.isNotEmpty(changeMap)) {
				persistenceStorage.initHZConfig(config);
				if (persistenceStorageConfig.isFirstTime()) {
					persistenceStorageConfig.setFirstTime(false);
				} else {
					try {
						StringBuilder stringBuilder = new StringBuilder(LOG_PREFIX + "It is found that the Hazelcast IMDG persistence storage config has been modified\n");
						for (Map.Entry<String, List<String>> entry : changeMap.entrySet()) {
							String key = entry.getKey();
							List<String> value = entry.getValue();
							stringBuilder.append(" - ").append(key).append(": ").append(value.get(0)).append(" -> ").append(value.get(1)).append("\n");
						}
						logger.info(stringBuilder);
					} catch (Exception ignore) {
					}
				}
			}
			int shareCdcTtlDay = settingService.getInt("share_cdc_ttl_day", 3);
			persistenceStorageConfig.setShareCdcTtlDay(shareCdcTtlDay);
			PersistenceStorageConfig.getInstance().setEnable(true);
		} catch (Exception e) {
			logger.error("Init hazelcast storage persistence failed; Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			PersistenceStorageConfig.getInstance().setEnable(false);
			PersistenceStorageConfig.getInstance().setThrowable(e);
		}
	}

	private static StorageMode setStorageMode(SettingService settingService, PersistenceStorage persistenceStorage) {
		String persistenceMode = settingService.getString(HazelcastConstant.SETTING_PERSISTENCE_MODE);
		StorageMode storageMode;
		try {
			storageMode = StorageMode.valueOf(persistenceMode);
		} catch (IllegalArgumentException e) {
			logger.warn(LOG_PREFIX + "Persistence mode in setting is invalid: " + persistenceMode + ", will use default mode: " + StorageMode.Mem.name());
			storageMode = DEFAULT_STORAGE_MODE;
		}
		persistenceStorage.setStorageMode(storageMode);
		logger.info(LOG_PREFIX + "Hazelcast IMDG persistence storage mode: " + storageMode);
		return storageMode;
	}

	private static void setMemorySize(SettingService settingService, PersistenceStorage persistenceStorage, Map<String, List<String>> changeMap) {
		PersistenceStorageConfig persistenceStorageConfig = PersistenceStorageConfig.getInstance();
		Integer oldInMemSize = persistenceStorageConfig.getInMemSize();
		int memorySize = settingService.getInt(HazelcastConstant.SETTING_PERSISTENCE_MEMORY_SIZE, DEFAULT_MEMORY_SIZE);
		if (oldInMemSize == null || !oldInMemSize.equals(memorySize)) {
			persistenceStorage.setInMemSize(memorySize);
			persistenceStorageConfig.setInMemSize(memorySize);
			changeMap.put("Storage mem size", Arrays.asList(String.valueOf(oldInMemSize), String.valueOf(memorySize)));
			logger.info(LOG_PREFIX + "Hazelcast IMDG persistence storage mem size: " + memorySize);
		}
	}

	private static void setMongodbStorage(SettingService settingService, PersistenceStorage persistenceStorage, ClientMongoOperator clientMongoOperator, Map<String, List<String>> changeMap) {
		PersistenceStorageConfig persistenceStorageConfig = PersistenceStorageConfig.getInstance();
		String oldMongoUri = persistenceStorageConfig.getMongoUri() == null ? "" : persistenceStorageConfig.getMongoUri();
		String oldMongoDbName = persistenceStorageConfig.getMongoDbName() == null ? "" : persistenceStorageConfig.getMongoDbName();
		String oldMongoCollection = persistenceStorageConfig.getMongoCollection() == null ? "" : persistenceStorageConfig.getMongoCollection();

    /*String mongodbConnectionName = settingService.getString(HazelcastConstant.SETTING_PERSISTENCE_MONGODB_URI);
    Connections mongodbConnection = getMongoConn(settingService, clientMongoOperator);
    if (mongodbConnection == null) {
      throw new RuntimeException(LOG_PREFIX + "Set MongoDB storage failed; Mongodb connection not exists; Connection name: " + mongodbConnectionName);
    }
    String mongodbUri = mongodbConnection.getDatabase_uri();*/
		String mongodbUri = settingService.getString(HazelcastConstant.SETTING_PERSISTENCE_MONGODB_URI);
		MongoClientURI mongoClientURI;
		try {
			mongoClientURI = MongodbUtil.verifyMongoDBUriWithDB(mongodbUri);
		} catch (Exception e) {
			throw new RuntimeException(LOG_PREFIX + e.getMessage());
		}
		String database = mongoClientURI.getDatabase();
		String collectionName = settingService.getString(HazelcastConstant.SETTING_PERSISTENCE_MONGODB_COLLECTION, DEFAULT_COLLECTION_NAME);

		if (!oldMongoUri.equals(mongodbUri)) {
			persistenceStorage.setMongoUri(mongodbUri);
			persistenceStorageConfig.setMongoUri(mongodbUri);
			changeMap.put("MongoDb uri", Arrays.asList(oldMongoUri, mongodbUri));
			logger.info(LOG_PREFIX + "Hazelcast IMDG storage MongoDB uri: " + mongodbUri);
		}
		if (!oldMongoDbName.equals(database)) {
			persistenceStorage.setDB(database);
			persistenceStorageConfig.setMongoDbName(database);
			changeMap.put("MongoDB db", Arrays.asList(oldMongoDbName, database));
			logger.info(LOG_PREFIX + "Hazelcast IMDG storage MongoDB database: " + database);
		}
		if (!oldMongoCollection.equals(collectionName)) {
			persistenceStorage.setCollection(collectionName);
			persistenceStorageConfig.setMongoCollection(collectionName);
			changeMap.put("MongoDB collection", Arrays.asList(oldMongoCollection, collectionName));
			logger.info(LOG_PREFIX + "Hazelcast IMDG storage MongoDB collection: " + collectionName);
		}
	}

	private static Connections getMongoConn(SettingService settingService, ClientMongoOperator clientMongoOperator) {
		String mongodbConnectionId = settingService.getString(HazelcastConstant.SETTING_PERSISTENCE_MONGODB_URI);
		Query query = new Query(Criteria.where("_id").is(new ObjectId(mongodbConnectionId)));
		query.fields().include("database_uri");
		return clientMongoOperator.findOne(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
	}

	private static void setRocksdbStorage(SettingService settingService, PersistenceStorage persistenceStorage, Map<String, List<String>> changeMap) {
		PersistenceStorageConfig persistenceStorageConfig = PersistenceStorageConfig.getInstance();
		String oldRocksDBPath = persistenceStorageConfig.getRocksDBPath() == null ? "" : persistenceStorageConfig.getRocksDBPath();
		String rocksdbPath = settingService.getString(HazelcastConstant.SETTING_PERSISTENCE_ROCKSDB_DBPATH, DEFAULT_ROCKSDB_DBPATH);
		if (!oldRocksDBPath.equals(rocksdbPath)) {
			persistenceStorage.setRocksDBPath(rocksdbPath);
			persistenceStorageConfig.setRocksDBPath(rocksdbPath);
			changeMap.put("RocksDB db path", Arrays.asList(oldRocksDBPath, rocksdbPath));
			logger.info(LOG_PREFIX + "Hazelcast IMDG storage RocksDB db path: " + rocksdbPath);
		}
	}

	public static String getConstructName(TaskDto taskDto) {
		return SHARE_CDC_KEY_PREFIX + taskDto.getName();
	}

	public static String getConstructName(TaskDto taskDto, String tableName) {
		return SHARE_CDC_KEY_PREFIX + taskDto.getName() + "_" + tableName;
	}

	public static boolean shareCdcEnable(SettingService settingService) {
		assert settingService != null;
		settingService.loadSettings(ShareCdcConstant.SETTING_SHARE_CDC_ENABLE);
		String shareCdcEnable = settingService.getString(ShareCdcConstant.SETTING_SHARE_CDC_ENABLE, "true");
		try {
			return Boolean.parseBoolean(shareCdcEnable);
		} catch (Exception e) {
			logger.warn("Get global share cdc enable setting failed, key: " + ShareCdcConstant.SETTING_SHARE_CDC_ENABLE
					+ ", will use default value: true"
					+ "; Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			return true;
		}
	}
}
