package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.utils.cache.KVMap;

/**
 * @author samuel
 * @Description
 * @create 2022-05-21 16:05
 **/
public class PdkStateMap implements KVMap<Object> {
	private static final String TAG = PdkStateMap.class.getSimpleName();
	private static final String GLOBAL_MAP_NAME = "GlobalStateMap";
	private DocumentIMap<Object> documentIMap;
	private static volatile PdkStateMap globalStateMap;

	private PdkStateMap() {
	}

	public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance) {
		String name = TAG + "_" + nodeId;
		documentIMap = new DocumentIMap<>(hazelcastInstance, name);
	}

	public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance, ExternalStorageDto externalStorageDto) {
		String name = TAG + "_" + nodeId;
		documentIMap = new DocumentIMap<>(hazelcastInstance, name, externalStorageDto);
	}

	public static PdkStateMap globalStateMap(HazelcastInstance hazelcastInstance) {
		if (globalStateMap == null) {
			synchronized (GLOBAL_MAP_NAME) {
				if (globalStateMap == null) {
					globalStateMap = new PdkStateMap();
					globalStateMap.documentIMap = new DocumentIMap<>(hazelcastInstance, GLOBAL_MAP_NAME);
					PersistenceStorage.getInstance().setImapTTL(GLOBAL_MAP_NAME, 604800L);
				}
			}
		}
		return globalStateMap;
	}

	public static PdkStateMap globalStateMap(HazelcastInstance hazelcastInstance, ExternalStorageDto externalStorageDto) {
		if (globalStateMap == null) {
			synchronized (GLOBAL_MAP_NAME) {
				if (globalStateMap == null) {
					globalStateMap = new PdkStateMap();
					globalStateMap.documentIMap = new DocumentIMap<>(hazelcastInstance, GLOBAL_MAP_NAME, externalStorageDto);
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
}
