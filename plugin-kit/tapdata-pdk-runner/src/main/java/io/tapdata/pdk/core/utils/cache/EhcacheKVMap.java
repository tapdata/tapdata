package io.tapdata.pdk.core.utils.cache;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import java.io.File;
import java.io.Serializable;

import static io.tapdata.entity.simplify.TapSimplify.*;

@Implementation(value = KVMap.class, buildNumber = 0, type = "ehcache")
public class EhcacheKVMap<T> implements KVMap<T>, Serializable {
    private static final String TAG = EhcacheKVMap.class.getSimpleName();
    private volatile static PersistentCacheManager persistentCacheManager = null;
    private Cache<String, T> cache;
    private String cacheKey;
    private Class<T> valueClass;

    private Integer maxHeapEntries;
    private Integer maxDiskMB;
    private Integer maxOffHeapMB;
    private Boolean clearBeforeInit;

    private String cachePath;

    public EhcacheKVMap<T> clearBeforeInit(Boolean clearBeforeInit) {
        this.clearBeforeInit = clearBeforeInit;
        return this;
    }

    public EhcacheKVMap<T> maxHeapEntries(int maxHeapEntries) {
        this.maxHeapEntries = maxHeapEntries;
        return this;
    }

    public EhcacheKVMap<T> maxDiskMB(int maxDiskMB) {
        this.maxDiskMB = maxDiskMB;
        return this;
    }

    public EhcacheKVMap<T> maxOffHeapMB(int maxOffHeapMB) {
        this.maxOffHeapMB = maxOffHeapMB;
        return this;
    }

    public EhcacheKVMap<T> cachePath(String cachePath) {
        this.cachePath = cachePath;
        return this;
    }

    public static <T> EhcacheKVMap<T> create(String mapKey, Class<T> valueClass) {
        EhcacheKVMap<T> tEhcacheKVMap = new EhcacheKVMap<>();
        tEhcacheKVMap.cacheKey = mapKey;
        tEhcacheKVMap.valueClass = valueClass;
        return tEhcacheKVMap;
    }

    @SuppressWarnings("unchecked")
    public EhcacheKVMap<T> init() {
        init(cacheKey, valueClass);
        return this;
    }
    @SuppressWarnings("unchecked")
    @Override
    public void init(String mapKey, Class<T> valueClass) {
        if(persistentCacheManager == null) {
            synchronized (PersistentCacheManager.class) {
                if(persistentCacheManager == null) {
                    if(cachePath == null) {
                        cachePath = CommonUtils.getProperty("tapcache_ehcache_root_path", "cacheData");
                    }

                    EhcacheKVMap.persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                            .with(CacheManagerBuilder.persistence(new File(getStoragePath(), cachePath)))
                            .withSerializer(Object.class, ObjectSerializer.class)
                            .build(true);
                }
            }
        }
        cache = (Cache<String, T>) persistentCacheManager.getCache(mapKey, String.class, (Class<?>) valueClass);
        if(clearBeforeInit != null && clearBeforeInit && cache != null) {
            CommonUtils.ignoreAnyError(() -> persistentCacheManager.destroyCache(mapKey), TAG);
            cache = null;
        }
        if(cache == null) {
            synchronized (this) {
                cache = (Cache<String, T>) persistentCacheManager.getCache(mapKey, String.class, (Class<?>) valueClass);
                if(cache == null) {
                    cacheKey = mapKey;
                    if(maxHeapEntries == null) {
                        maxHeapEntries = CommonUtils.getPropertyInt("tapcache_ehcache_heap_max_entries", 10);
                    }
//                    if(maxDiskMB == null) {
//                        maxDiskMB = CommonUtils.getPropertyInt("tapcache_ehcache_disk_max_mb", 20);
//                    }
//                    if(maxOffHeapMB == null) {
//                        maxOffHeapMB = CommonUtils.getPropertyInt("tapcache_ehcache_disk_max_mb", 10);
//                    }

//                    CommonUtils.ignoreAnyError(() -> persistentCacheManager.destroyCache(mapKey), TAG);
                    if(cache == null) {
                        ResourcePoolsBuilder resourcePoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder();
                        if(maxHeapEntries != null && maxHeapEntries > 0) {
                            resourcePoolsBuilder = resourcePoolsBuilder.heap(maxHeapEntries, EntryUnit.ENTRIES);
                        }
                        if(maxOffHeapMB != null && maxOffHeapMB > 0) {
                            resourcePoolsBuilder = resourcePoolsBuilder.offheap(maxOffHeapMB, MemoryUnit.MB);
                        }
                        if(maxDiskMB != null && maxDiskMB > 0) {
                            resourcePoolsBuilder = resourcePoolsBuilder.disk(maxDiskMB, MemoryUnit.MB);
                        }

                        if(cache == null) {
                            cache = (Cache<String, T>) persistentCacheManager.createCache(mapKey,
                                    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, (Class<?>) valueClass,
                                            resourcePoolsBuilder));
                        }
                    }
                }
            }
        }
    }



    public static void main(String... args) {
        EhcacheKVMap<TapTable> tableMap = EhcacheKVMap.create("AAAAA", TapTable.class)
                .maxHeapEntries(10)
                .maxDiskMB(5500)
                .init();

//        tableMap.init("AAAAA", TapTable.class);

        tableMap.put("a", table("name").add(field("field", "TapString").tapType(tapString().bytes(100L).fixed(true)).comment("asdkfalskdflskdfj")));

        long putTime = System.currentTimeMillis();
        for(int i = 0; i < 1; i++) {
            String key = String.valueOf(i);
            tableMap.put(key, table(key).add(field("field", "TapString").comment("asdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfj" + key)));
        }
        System.out.println("put takes " + (System.currentTimeMillis() - putTime));
        //put takes 6090

        long getTime = System.currentTimeMillis();
        for(int i = 0; i < 1; i++) {
            TapTable table = tableMap.get(String.valueOf(i % 10));
        }
        System.out.println("get take " + (System.currentTimeMillis() - getTime));
        //get take 61, hit on cache for 1 million read.
        //if missing cache, will take 31 seconds for 1 million read.

        System.out.println(tableMap.get("a"));

//        tableMap.clear();
//        tableMap.reset();
//        tableMap.reset();
    }
    private File getStoragePath() {
        return new File("./");
    }

    @Override
    public void put(String key, T t) {
        cache.put(key, t);
    }

    @Override
    public T putIfAbsent(String key, T t) {
        return cache.putIfAbsent(key, t);
    }

    @Override
    public T get(String key) {
        return cache.get(key);
    }

    @Override
    public T remove(String key) {
        T value = cache.get(key);
        if(value != null) {
            cache.remove(key);
            return value;
        }
        return null;
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void reset() {
        clear();
        CommonUtils.ignoreAnyError(() -> persistentCacheManager.destroyCache(cacheKey), TAG);
    }

    public Integer getMaxHeapEntries() {
        return maxHeapEntries;
    }

    public void setMaxHeapEntries(Integer maxHeapEntries) {
        this.maxHeapEntries = maxHeapEntries;
    }

    public Integer getMaxDiskMB() {
        return maxDiskMB;
    }

    public void setMaxDiskMB(Integer maxDiskMB) {
        this.maxDiskMB = maxDiskMB;
    }

    public Integer getMaxOffHeapMB() {
        return maxOffHeapMB;
    }

    public void setMaxOffHeapMB(Integer maxOffHeapMB) {
        this.maxOffHeapMB = maxOffHeapMB;
    }

    public String getCachePath() {
        return cachePath;
    }

    public void setCachePath(String cachePath) {
        this.cachePath = cachePath;
    }

    public Boolean getClearBeforeInit() {
        return clearBeforeInit;
    }

    public void setClearBeforeInit(Boolean clearBeforeInit) {
        this.clearBeforeInit = clearBeforeInit;
    }
}
