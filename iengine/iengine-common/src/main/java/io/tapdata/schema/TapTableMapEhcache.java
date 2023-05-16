package io.tapdata.schema;

import io.tapdata.cache.EhcacheService;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.cache.EhcacheKVMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;

/**
 * @author samuel
 * @Description
 * @create 2022-05-10 11:16
 **/
public class TapTableMapEhcache<K extends String, V extends TapTable> extends TapTableMap<K, V> {
    private static final String DIST_CACHE_PATH = "tap_table_ehcache";
    public static final int DEFAULT_OFF_HEAP_MB = 10;
    public static final int DEFAULT_DISK_MB = 1024;
    public static final int MAX_HEAP_ENTRIES = 100;
    public static final String TAP_TABLE_OFF_HEAP_MB_KEY = "TAP_TABLE_OFF_HEAP_MB";
    public static final String TAP_TABLE_DISK_MB_KEY = "TAP_TABLE_DISK_MB";
    public static final String TAP_TABLE_PREFIX = "TAP_TABLE_";

    private final String mapKey;

    public TapTableMapEhcache(String prefix, String nodeId, Long time, Map<K, String> tableNameAndQualifiedNameMap) {
        super(nodeId, time, tableNameAndQualifiedNameMap);
        if (StringUtils.isNotEmpty(prefix)) {
            this.mapKey = prefix + "_" + TAP_TABLE_PREFIX + nodeId;
        } else {
            this.mapKey = TAP_TABLE_PREFIX + nodeId;
        }
        createEhcacheMap();
        EhcacheService.getInstance().getEhcacheKVMap(mapKey).clear();
    }

    private EhcacheKVMap<V> createEhcacheMap() {
        try {
            EhcacheKVMap<TapTable> tapTableMap = EhcacheKVMap.create(this.mapKey, TapTable.class)
                    .cachePath(DIST_CACHE_PATH)
                    .maxHeapEntries(MAX_HEAP_ENTRIES)
                    //				.maxOffHeapMB(CommonUtils.getPropertyInt(TAP_TABLE_OFF_HEAP_MB_KEY, DEFAULT_OFF_HEAP_MB))
                    .maxDiskMB(CommonUtils.getPropertyInt(TAP_TABLE_DISK_MB_KEY, DEFAULT_DISK_MB))
                    .init();
            EhcacheService.getInstance().putEhcacheKVMap(mapKey, tapTableMap);
            return (EhcacheKVMap<V>) tapTableMap;
        } catch (Throwable e) {
            throw new RuntimeException(String.format("Failed to create Ehcache TapTableMap, node id: %s, map name: %s, error: %s", nodeId, mapKey, e.getMessage()), e);
        }
    }

    private EhcacheKVMap<V> getEhcacheKVMap() {
        return EhcacheService.getInstance().getEhcacheKVMap(this.mapKey);
    }

    @Override
    protected V getTapTable(K key) {
        EhcacheKVMap<V> ehcacheKVMap = Optional.ofNullable(getEhcacheKVMap()).orElseGet(() -> {
            try {
                return handleWithLock(() -> {
                    EhcacheKVMap<V> tmp = getEhcacheKVMap();
                    if (null == tmp) {
                        tmp = createEhcacheMap();
                    }
                    return tmp;
                });
            } catch (Throwable e) {
                throw new RuntimeException(String.format("Create TapTableMap failed, node id: %s, map name: %s, error: %s", nodeId, mapKey, e.getMessage()), e);
            }
        });
        if (null == ehcacheKVMap) {
            throw new IllegalArgumentException(String.format("Cannot create TapTableMap, node id: %s, map name: %s", nodeId, mapKey));
        }

        V tapTable = ehcacheKVMap.get(key);
        if (null == tapTable) {
            try {
                tapTable = handleWithLock(() -> {
                    V tmp = ehcacheKVMap.get(key);
                    if (null == tmp) {
                        tmp = findSchema(key);
                        ehcacheKVMap.put(key, tmp);
                    }
                    return tmp;
                });
            } catch (Exception e) {
                throw new RuntimeException("Find schema failed, message: " + e.getMessage(), e);
            }
        }
        return tapTable;
    }

    @Override
    protected void putTapTable(K key, V value) {
        EhcacheService.getInstance().getEhcacheKVMap(mapKey).put(key, value);
    }

    @Override
    protected V removeTapTable(K key) {
        return (V) EhcacheService.getInstance().getEhcacheKVMap(mapKey).remove(key);
    }

    @Override
    protected void clearTapTable() {
        EhcacheService.getInstance().getEhcacheKVMap(mapKey).clear();
    }

    @Override
    protected void resetTapTable() {
        EhcacheService ehcacheService = EhcacheService.getInstance();
        if (StringUtils.isNotBlank(mapKey)) {
            EhcacheKVMap<Object> ehcacheKVMap = ehcacheService.getEhcacheKVMap(mapKey);
            Optional.ofNullable(ehcacheKVMap).ifPresent(EhcacheKVMap::reset);
            ehcacheService.removeEhcacheKVMap(mapKey);
        }
    }
}
