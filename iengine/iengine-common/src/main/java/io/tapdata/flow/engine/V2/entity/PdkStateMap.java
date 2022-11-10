package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.StorageMode;
import com.sun.jna.platform.win32.GL;
import com.tapdata.constant.ConfigurationCenter;
import io.tapdata.entity.utils.cache.KVMap;
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
	private IMap<String, Document> imap;
	private static final String KEY = PdkStateMap.class.getSimpleName();
	private static volatile PdkStateMap globalStateMap;

	private PdkStateMap() {
	}

	public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance, StateMapMode stateMapMode) {
		String name = getStateMapName(nodeId);
		switch (stateMapMode) {
			case DEFAULT:
				imap = hazelcastInstance.getMap(name);
				break;
			case HTTP_TM:
				initHttpTMStateMap(hazelcastInstance, GlobalConstant.getInstance().getConfigurationCenter(), name);
				break;
		}
	}

	public PdkStateMap(HazelcastInstance hazelcastInstance, String mapName, StateMapMode stateMapMode) {
		switch (stateMapMode) {
			case DEFAULT:
				imap = hazelcastInstance.getMap(mapName);
				break;
			case HTTP_TM:
				initHttpTMStateMap(hazelcastInstance, GlobalConstant.getInstance().getConfigurationCenter(), mapName);
				break;
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
		new PersistenceStorage()
				.setStorageMode(StorageMode.HTTP_TM)
				.baseUrl(baseURLs.get(0))
				.accessCode(accessCode)
				.connectTimeoutMs(CONNECT_TIMEOUT_MS)
				.readTimeoutMs(READ_TIMEOUT_MS)
				.initMapStoreConfig(hazelcastInstance.getConfig(), name);
		imap = hazelcastInstance.getMap(name);
	}

	@NotNull
	private static String getStateMapName(String nodeId) {
		return TAG + "_" + nodeId;
	}

	public static PdkStateMap globalStateMap(HazelcastInstance hazelcastInstance) {
		if (globalStateMap == null) {
			synchronized (GLOBAL_MAP_NAME) {
				if (globalStateMap == null) {
					globalStateMap = new PdkStateMap(hazelcastInstance, GLOBAL_MAP_NAME, StateMapMode.HTTP_TM);
					PersistenceStorage.getInstance().setImapTTL(GLOBAL_MAP_NAME, 604800L);
				}
			}
		}
		return globalStateMap;
	}

	@Override
	public void init(String mapKey, Class<Object> valueClass) {

	}

	@Override
	public void put(String key, Object o) {
		try {
			documentIMap.insert(key, o);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object putIfAbsent(String key, Object o) {
		try {
			if (null == documentIMap.find(key)) {
				put(key, o);
				return o;
			}
			return null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object remove(String key) {
		try {
			Object o = documentIMap.find(key);
			documentIMap.delete(key);
			return o;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		try {
			documentIMap.clear();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void reset() {
		try {
			documentIMap.clear();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object get(String key) {
		try {
			return documentIMap.find(key);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public IMap<String, Document> getImap() {
		return imap;
	}

	public enum StateMapMode {
		DEFAULT,
		HTTP_TM,
	}
}
