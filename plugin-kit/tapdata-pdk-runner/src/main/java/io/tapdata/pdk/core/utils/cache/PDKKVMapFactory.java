package io.tapdata.pdk.core.utils.cache;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("ALL")
@Implementation(value = KVMapFactory.class, buildNumber = 0)
public class PDKKVMapFactory implements KVMapFactory {
    private final Map<String, KVMap<?>> kvMapMap = new ConcurrentHashMap<>();
    @Override
    public <T> KVMap<T> getCacheMap(String mapKey, Class<T> valueClass) {
        return (KVMap<T>) kvMapMap.computeIfAbsent(mapKey, key -> {
            KVMap<T> map = ClassFactory.create(KVMap.class, "ehcache");
            if(map != null)
                map.init(key, valueClass);
            return map;
        });
    }

    @Override
    public <T> KVMap<T> getPersistentMap(String mapKey, Class<T> valueClass) {
        return (KVMap<T>) kvMapMap.computeIfAbsent(mapKey, key -> {
            KVMap<T> map = ClassFactory.create(KVMap.class, "persistent");
            if(map != null)
                map.init(key, valueClass);
            return map;
        });
    }

    @Override
    public <T> KVReadOnlyMap<T> createKVReadOnlyMap(String mapKey) {
        KVMap<T> kvMap = (KVMap<T>) kvMapMap.get(mapKey);
        if(kvMap != null) {
            //Don't want to return kvMap instance as KVReadOnlyMap, because kvMap can be easily forced type conversion. use kvMap reference in KVReadOnlyMap is a better solution.
            return key -> kvMap.get(key);
        }
        return null;
    }

    @Override
    public boolean reset(String mapKey) {
        KVMap<?> map = kvMapMap.remove(mapKey);
        if(map != null) {
            map.reset();
            return true;
        }
        return false;
    }
}
