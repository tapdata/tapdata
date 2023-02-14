package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.AppType;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
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
	//	private IMap<String, Document> imap;
	private static final String KEY = PdkStateMap.class.getSimpleName();
	public static final long GLOBAL_MAP_TTL_SECONDS = 604800L;
	private static volatile PdkStateMap globalStateMap;
	private DocumentIMap<Document> constructIMap;

	private PdkStateMap() {
	}

	public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance, StateMapMode stateMapMode) {
		String name = getStateMapName(nodeId);
		initConstructMap(hazelcastInstance, name, stateMapMode);
	}

	private PdkStateMap(HazelcastInstance hazelcastInstance, String mapName, StateMapMode stateMapMode) {
		initConstructMap(hazelcastInstance, mapName, stateMapMode);
	}

	private void initConstructMap(HazelcastInstance hazelcastInstance, String mapName, StateMapMode stateMapMode) {
		ConfigurationCenter configurationCenter = GlobalConstant.getInstance().getConfigurationCenter();
		Object appTypeObj = configurationCenter.getConfig(ConfigurationCenter.APPTYPE);
		AppType appType = null;
		if (appTypeObj instanceof AppType) {
			appType = (AppType) appTypeObj;
		}
		if (null != appType && appType.isDaas()) {
			stateMapMode = StateMapMode.MONGODB;
		}
		switch (stateMapMode) {
			case DEFAULT:
				constructIMap = new DocumentIMap<>(hazelcastInstance, mapName);
				break;
			case HTTP_TM:
				initHttpTMStateMap(hazelcastInstance, GlobalConstant.getInstance().getConfigurationCenter(), mapName);
				break;
			case MONGODB:
				ExternalStorageDto defaultExternalStorage = ExternalStorageUtil.getDefaultExternalStorage();
				constructIMap = new DocumentIMap<>(hazelcastInstance, mapName, defaultExternalStorage);
				break;
			default:
				throw new IllegalArgumentException("Nonsupport state map storage mode: " + stateMapMode.name());
		}
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
					AppType appType = (AppType) GlobalConstant.getInstance().getConfigurationCenter().getConfig(ConfigurationCenter.APPTYPE);
					StateMapMode stateMapMode = StateMapMode.MONGODB;
					if (appType.isCloud()) {
						stateMapMode = StateMapMode.HTTP_TM;
					}
					globalStateMap = new PdkStateMap(hazelcastInstance, GLOBAL_MAP_NAME, stateMapMode);
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
		MONGODB,
	}

	public DocumentIMap<Document> getConstructIMap() {
		return constructIMap;
	}
}
