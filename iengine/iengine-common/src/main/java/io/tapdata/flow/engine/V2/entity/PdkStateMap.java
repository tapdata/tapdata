package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.AppType;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-21 16:05
 **/
public class PdkStateMap implements KVMap<Object> {
	private static final String TAG = PdkStateMap.class.getSimpleName();
	private static final String GLOBAL_MAP_NAME = "GlobalStateMap";
	public static final int CONNECT_TIMEOUT_MS = 60 * 1000;
	public static final int READ_TIMEOUT_MS = 60 * 1000;
	//	private IMap<String, Document> imap;
	private static final String KEY = PdkStateMap.class.getSimpleName();
	public static final long GLOBAL_MAP_TTL_SECONDS = 604800L;
	private Logger logger = LogManager.getLogger(PdkStateMap.class);
	private static volatile PdkStateMap globalStateMap;
	private DocumentIMap<Document> constructIMap;
	private ExternalStorageDto externalStorage;

	private PdkStateMap() {
	}

	public ExternalStorageDto getExternalStorage() {
		return externalStorage;
	}

	public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance) {
		String name = getStateMapName(nodeId);
		initConstructMap(hazelcastInstance, name);
	}

	private PdkStateMap(HazelcastInstance hazelcastInstance, String mapName) {
		initConstructMap(hazelcastInstance, mapName);
	}

	public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance, TaskDto taskDto, Node<?> node, ClientMongoOperator clientMongoOperator, String func) {
		boolean needUpdateExternalStorageId = false;
		if (null != taskDto && null != node) {
			Map<String, Object> attrs = taskDto.getAttrs();
			if (null != attrs) {
				String externalStorageId = attrs.getOrDefault(getExternalStorageKey(node, func), "").toString();
				if (StringUtils.isNotBlank(externalStorageId)) {
					ExternalStorageDto findExternalStorageDto = clientMongoOperator.findOne(Query.query(Criteria.where("_id").is(externalStorageId)), ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class);
					if (null != findExternalStorageDto) {
						this.externalStorage = findExternalStorageDto;
					} else {
						logger.warn("Task {} node {} found last state map external storage id {}, but cannot found external storage config by this id, will use default config, " +
										"may be cause some problem, recommend reset and restart task",
								taskDto.getName(), node.getName(), externalStorageId);
					}
				} else {
					needUpdateExternalStorageId = true;
				}
			} else {
				needUpdateExternalStorageId = true;
			}
		}
		String name = getStateMapName(nodeId);
		initConstructMap(hazelcastInstance, name);
		if (needUpdateExternalStorageId && null != externalStorage) {
			clientMongoOperator.update(
					Query.query(Criteria.where("_id").is(taskDto.getId().toString())),
					new Update().set("attrs." + getExternalStorageKey(node, func), externalStorage.getId().toHexString()),
					ConnectorConstant.TASK_COLLECTION
			);
		}
		if (null != taskDto && null != node) {
			logger.info("Task {} node {} state map external storage: {}", taskDto.getName(), node.getName(), externalStorage);
		}
	}

	@NotNull
	private static String getExternalStorageKey(Node<?> node, String func) {
		return "state-external-storage-id-" + func + node.getId();
	}

	private void initConstructMap(HazelcastInstance hazelcastInstance, String mapName) {
		initHttpTMStateMap(hazelcastInstance, GlobalConstant.getInstance().getConfigurationCenter(), mapName);
	}

	private void initHttpTMStateMap(HazelcastInstance hazelcastInstance, ConfigurationCenter configurationCenter, String name) {
		if (null == configurationCenter) {
			throw new IllegalArgumentException("Config center cannot be null");
		}
		Object baseURLsObj = configurationCenter.getConfig(ConfigurationCenter.BASR_URLS);
		if (null == baseURLsObj)
			throw new IllegalArgumentException(String.format("Create pdk state map failed, config %s cannot be null", ConfigurationCenter.BASR_URLS));
		List<String> baseURLs;
		if (baseURLsObj instanceof List) {
			try {
				baseURLs = (List<String>) configurationCenter.getConfig(ConfigurationCenter.BASR_URLS);
			} catch (Exception e) {
				throw new IllegalArgumentException(String.format("Create pdk state map failed, config %s type must be List<String>, actual: %s", ConfigurationCenter.BASR_URLS, baseURLsObj.getClass().getSimpleName()));
			}
			if (CollectionUtils.isEmpty(baseURLs)) {
				throw new IllegalArgumentException(String.format("Create pdk state map failed, config %s cannot be empty", ConfigurationCenter.BASR_URLS));
			}
		} else {
			throw new IllegalArgumentException(String.format("Create pdk state map failed, config %s type must be List, actual: %s", ConfigurationCenter.BASR_URLS, baseURLsObj.getClass().getSimpleName()));
		}
		Object accessCodeObj = configurationCenter.getConfig(ConfigurationCenter.ACCESS_CODE);
		if (null == accessCodeObj)
			throw new IllegalArgumentException(String.format("Create pdk state map failed, config %s cannot be null", ConfigurationCenter.ACCESS_CODE));
		String accessCode;
		if (accessCodeObj instanceof String) {
			accessCode = accessCodeObj.toString();
		} else {
			throw new IllegalArgumentException(String.format("Create pdk state map failed, config %s type must be String, actual: %s", ConfigurationCenter.ACCESS_CODE, accessCodeObj.getClass().getSimpleName()));
		}
		ExternalStorageDto externalStorageDto = new ExternalStorageDto();
		externalStorageDto.setType(ExternalStorageType.httptm.name());
		externalStorageDto.setBaseUrl(baseURLs.get(0));
		externalStorageDto.setAccessToken(accessCode);
		externalStorageDto.setConnectTimeoutMs(CONNECT_TIMEOUT_MS);
		externalStorageDto.setReadTimeoutMs(READ_TIMEOUT_MS);
		constructIMap = new DocumentIMap<>(hazelcastInstance, name, externalStorageDto);
	}

	@NotNull
	private static String getStateMapName(String nodeId) {
		return TAG + "_" + nodeId;
	}

	public static PdkStateMap globalStateMap(HazelcastInstance hazelcastInstance) {
		if (globalStateMap == null) {
			synchronized (GLOBAL_MAP_NAME) {
				if (globalStateMap == null) {
					globalStateMap = new PdkStateMap(hazelcastInstance, GLOBAL_MAP_NAME);
					PersistenceStorage.getInstance().setImapTTL(globalStateMap.getConstructIMap().getiMap(), GLOBAL_MAP_TTL_SECONDS);
				}
			}
		}
		return globalStateMap;
	}

	@Override
	public void init(String mapKey, Class<Object> valueClass) {

	}

	@SneakyThrows
	@Override
	public void put(String key, Object o) {
		constructIMap.insert(key, new Document(KEY, o));
	}

	@Override
	public Object putIfAbsent(String key, Object o) {
		return constructIMap.getiMap().putIfAbsent(key, new Document(KEY, o));
	}

	@SneakyThrows
	@Override
	public Object remove(String key) {
		return constructIMap.delete(key);
	}

	@SneakyThrows
	@Override
	public void clear() {
		constructIMap.clear();
	}

	@SneakyThrows
	@Override
	public void reset() {
		constructIMap.destroy();
	}

	@SneakyThrows
	@Override
	public Object get(String key) {
		Document document = constructIMap.find(key);
		if (null == document) return null;
		return document.get(KEY);
	}

	public enum StateMapMode {
		DEFAULT,
		HTTP_TM,
	}

	public DocumentIMap<Document> getConstructIMap() {
		return constructIMap;
	}
}
