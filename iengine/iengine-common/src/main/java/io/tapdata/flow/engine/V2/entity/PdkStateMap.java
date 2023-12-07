package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.AppType;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.error.PdkStateMapExCode_28;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.util.List;

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
	private static final String KEY = PdkStateMap.class.getSimpleName();
	public static final String STATE_MAP_TABLE = "HazelcastPersistence";
	protected static final int[] GLOBAL_STATE_MAP_LOCK = new int[0];
	public static final String SIGN_KEY = "sign";
	private Logger logger = LogManager.getLogger(PdkStateMap.class);
	private static volatile PdkStateMap globalStateMap;
	private DocumentIMap<Document> constructIMap;
	private Node node;
	private String nodeId;
	private StateMapVersion stateMapVersion;

	protected PdkStateMap() {
	}

	public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance) {
		String name = getStateMapName(nodeId);
		this.nodeId = nodeId;
		initConstructMap(hazelcastInstance, name);
	}

	private PdkStateMap(HazelcastInstance hazelcastInstance, String mapName) {
		initConstructMap(hazelcastInstance, mapName);
	}

	public PdkStateMap(HazelcastInstance hazelcastInstance, Node<?> node) {
		if (null == node) throw new IllegalArgumentException("Node cannot be null");
		this.node = node;
		String name = getStateMapName(node.getId());
		initConstructMap(hazelcastInstance, name);
	}

	protected void initConstructMap(HazelcastInstance hazelcastInstance, String mapName) {
		if (AppType.init().isCloud()) {
			initHttpTMStateMap(hazelcastInstance, GlobalConstant.getInstance().getConfigurationCenter(), mapName);
		} else {
			ExternalStorageDto tapdataOrDefaultExternalStorage = ExternalStorageUtil.getTapdataOrDefaultExternalStorage();
			if (mapName.equals(GLOBAL_MAP_NAME)) {
				initGlobalStateMap(hazelcastInstance, mapName, tapdataOrDefaultExternalStorage);
			} else {
				initNodeStateMap(hazelcastInstance, mapName, tapdataOrDefaultExternalStorage);
			}
		}
	}

	protected void initNodeStateMap(HazelcastInstance hazelcastInstance, String mapName, ExternalStorageDto tapdataOrDefaultExternalStorage) {
		tapdataOrDefaultExternalStorage.setTable(null);
		tapdataOrDefaultExternalStorage.setTtlDay(0); // No time to live
		DocumentIMap<Document> documentIMapV2;
		stateMapVersion = StateMapVersion.V2;
		try {
			documentIMapV2 = initDocumentIMapV2(hazelcastInstance, mapName, tapdataOrDefaultExternalStorage);
			if (logger.isDebugEnabled()) {
				logger.debug("Init document imap v2 completed, map name: {}, external storage: {}", mapName, tapdataOrDefaultExternalStorage);
			}
		} catch (Exception e) {
			throw new TapCodeException(PdkStateMapExCode_28.INIT_PDK_STATE_MAP_FAILED, String.format("Map name: %s", mapName), e);
		}
		if (documentIMapV2.isEmpty()) {
			DocumentIMap<Document> documentIMapV1;
			try {
				documentIMapV1 = initDocumentIMapV1(hazelcastInstance, mapName, tapdataOrDefaultExternalStorage);
			} catch (Exception e) {
				constructIMap = documentIMapV2;
				writeStateMapSign();
				return;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("IMap v2 is empty, also need to init IMap v1, map name: {}, external storage: {}", mapName, tapdataOrDefaultExternalStorage);
			}
			if (null == documentIMapV1) {
				constructIMap = documentIMapV2;
			} else {
				if (!documentIMapV1.isEmpty()) {
					constructIMap = documentIMapV1;
					stateMapVersion = StateMapVersion.V1;
					if (logger.isDebugEnabled()) {
						logger.debug("IMap v1 is not empty, use IMap v1 as node pdk state map: {}", documentIMapV1.getName());
					}
					try {
						documentIMapV2.clear();
						documentIMapV2.destroy();
					} catch (Exception e) {
						// ignored
					}
				} else {
					constructIMap = documentIMapV2;
					if (logger.isDebugEnabled()) {
						logger.debug("IMap v1 is empty, use IMap v2 as node pdk state map: {}", documentIMapV2.getName());
					}
					try {
						documentIMapV1.clear();
						documentIMapV1.destroy();
					} catch (Exception e) {
						// ignored
					}
				}
			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("IMap v2 is not empty, use IMap v2 as node pdk state map: {}", documentIMapV2.getName());
			}
			constructIMap = documentIMapV2;
		}
		writeStateMapSign();
	}

	protected void writeStateMapSign() {
		if (null != constructIMap) {
			CommonUtils.ignoreAnyError(() -> {
				Document signDoc = null;
				if (null != node) {
					signDoc = new Document("nodeId", node.getId())
							.append("nodeName", node.getName())
							.append("nodeClass", node.getClass().getName())
							.append("stateMapName", getStateMapName(node.getId()));
				} else if (StringUtils.isNotBlank(nodeId)) {
					signDoc = new Document("nodeId", nodeId)
							.append("stateMapName", getStateMapName(nodeId));
				}
				if (null != signDoc) {
					if (null != stateMapVersion) {
						signDoc.append("stateMapVersion", stateMapVersion.name());
					}
					constructIMap.insert(SIGN_KEY, signDoc);
				}
			}, TAG);
		}
	}

	protected void initGlobalStateMap(HazelcastInstance hazelcastInstance, String mapName, ExternalStorageDto tapdataOrDefaultExternalStorage) {
		tapdataOrDefaultExternalStorage.setTable(STATE_MAP_TABLE);
		tapdataOrDefaultExternalStorage.setTtlDay(0);
		constructIMap = initDocumentIMapV1(hazelcastInstance, mapName, tapdataOrDefaultExternalStorage);
	}

	protected DocumentIMap<Document> initDocumentIMapV1(HazelcastInstance hazelcastInstance, String mapName, ExternalStorageDto externalStorageDto) {
		return initDocumentIMap(hazelcastInstance, mapName, externalStorageDto);
	}

	protected DocumentIMap<Document> initDocumentIMapV2(HazelcastInstance hazelcastInstance, String mapName, ExternalStorageDto externalStorageDto) {
		externalStorageDto.setTable(null);
		String hashMapName = String.valueOf(mapName.hashCode());
		return initDocumentIMap(hazelcastInstance, hashMapName, externalStorageDto);
	}

	protected DocumentIMap<Document> initDocumentIMap(HazelcastInstance hazelcastInstance, String mapName, ExternalStorageDto externalStorageDto) {
		return new DocumentIMap<>(hazelcastInstance, TAG, mapName, externalStorageDto);
	}

	protected void initHttpTMStateMap(HazelcastInstance hazelcastInstance, ConfigurationCenter configurationCenter, String name) {
		if (null == configurationCenter) {
			throw new IllegalArgumentException("Config center cannot be null");
		}
		Object baseURLsObj = configurationCenter.getConfig(ConfigurationCenter.BASR_URLS);
		if (null == baseURLsObj)
			throw new IllegalArgumentException(String.format("Create pdk state map failed, config %s cannot be null", ConfigurationCenter.BASR_URLS));
		List<String> baseURLs;
		if (baseURLsObj instanceof List) {
			baseURLs = (List<String>) baseURLsObj;
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
		externalStorageDto.setBaseURLs(baseURLs);
		externalStorageDto.setAccessToken(accessCode);
		externalStorageDto.setConnectTimeoutMs(CONNECT_TIMEOUT_MS);
		externalStorageDto.setReadTimeoutMs(READ_TIMEOUT_MS);
		constructIMap = initDocumentIMap(hazelcastInstance, name, externalStorageDto);
	}

	@NotNull
	protected static String getStateMapName(String nodeId) {
		if (StringUtils.isBlank(nodeId)) {
			throw new IllegalArgumentException("Node id cannot be empty");
		}
		return TAG + "_" + nodeId;
	}

	public static PdkStateMap globalStateMap(HazelcastInstance hazelcastInstance) {
		if (globalStateMap == null) {
			synchronized (GLOBAL_STATE_MAP_LOCK) {
				if (globalStateMap == null) {
					synchronized (GLOBAL_STATE_MAP_LOCK) {
						globalStateMap = new PdkStateMap(hazelcastInstance, GLOBAL_MAP_NAME);
					}
				}
			}
		}
		return globalStateMap;
	}

	@Override
	public void init(String mapKey, Class<Object> valueClass) {
		// Do nothing
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

	enum StateMapVersion {
		V1, V2,
	}
}
