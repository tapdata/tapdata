package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.PersistenceStorage;
import io.tapdata.entity.utils.cache.KVMap;
import org.bson.Document;

/**
 * @author samuel
 * @Description
 * @create 2022-05-21 16:05
 **/
public class PdkStateMap implements KVMap<Object> {
  private static final String TAG = PdkStateMap.class.getSimpleName();
  private static final String GLOBAL_MAP_NAME = "GlobalStateMap";
  private IMap<String, Document> imap;
  private static final String KEY = PdkStateMap.class.getSimpleName();
  private static volatile PdkStateMap globalStateMap;

  private PdkStateMap() {
  }

  public PdkStateMap(String nodeId, HazelcastInstance hazelcastInstance) {
    String name = TAG + "_" + nodeId;
    imap = hazelcastInstance.getMap(name);
  }

  public static PdkStateMap globalStateMap(HazelcastInstance hazelcastInstance) {
    if (globalStateMap == null) {
      synchronized (GLOBAL_MAP_NAME) {
        if (globalStateMap == null) {
          globalStateMap = new PdkStateMap();
          globalStateMap.imap = hazelcastInstance.getMap(GLOBAL_MAP_NAME);
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
    imap.put(key, new Document(KEY, o));
  }

  @Override
  public Object putIfAbsent(String key, Object o) {
    return imap.putIfAbsent(key, new Document(KEY, o));
  }

  @Override
  public Object remove(String key) {
    return imap.remove(key);
  }

  @Override
  public void clear() {
    imap.clear();
  }

  @Override
  public void reset() {
      imap.clear();
  }

//  @Override
//  public Object get(String key) {
//    Document document = imap.getOrDefault(key, null);
//    if (null == document) return null;
//    return document.get(KEY);
//  }

	@Override
	public Object get(String key) {
		Object value = imap.getOrDefault(key, null);
		if (null == value) return null;
		try {
			return ((Document) value).get(KEY);
		} catch(Throwable throwable) {
			//This is a workaround for resolving Document is different issue. Has performance rick.
			try {
				return value.getClass().getMethod("get", Object.class).invoke(value, KEY);
			} catch(Throwable throwable1) {
				throw new RuntimeException(throwable);
			}
		}
	}

	public IMap<String, Document> getImap() {
		return imap;
	}
}
