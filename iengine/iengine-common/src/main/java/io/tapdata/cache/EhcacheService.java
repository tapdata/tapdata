package io.tapdata.cache;

import io.tapdata.pdk.core.utils.cache.EhcacheKVMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2022-07-11 10:53
 **/
public class EhcacheService {
	private static final String DIST_CACHE_PATH = "tap_ehcache";
	public static final int DEFAULT_OFF_HEAP_MB = 100;
	public static final int DEFAULT_DISK_MB = 1024;
	public static final int MAX_HEAP_ENTRIES = 100;
	public Map<String, EhcacheKVMap<?>> ehcacheKVMapMap;

	private EhcacheService() {
		this.ehcacheKVMapMap = new ConcurrentHashMap<>();
	}

	public static EhcacheService getInstance() {
		return EhcacheServiceInstance.INSTANCE.getInstance();
	}

	public <E> EhcacheKVMap<E> createEhcacheKVMap(String mapKey, Class<E> eClass) {
		return createEhcacheKVMap(mapKey, eClass, DIST_CACHE_PATH, MAX_HEAP_ENTRIES, DEFAULT_OFF_HEAP_MB, DEFAULT_DISK_MB);
	}

	public <E> EhcacheKVMap<E> createEhcacheKVMap(String mapKey, Class<E> eClass, String cachePath, int maxHeapEntries, int offHeapMB, int diskMB) {
		return EhcacheKVMap.create(mapKey, eClass)
				.cachePath(cachePath)
				.maxHeapEntries(maxHeapEntries)
				.maxOffHeapMB(offHeapMB)
				.maxDiskMB(diskMB)
				.init();
	}

	public void putEhcacheKVMap(String mapKey, EhcacheKVMap<?> ehcacheKVMap) {
		if (null == mapKey || null == ehcacheKVMap) {
			throw new NullPointerException("Map key or ehcache cannot be null");
		}
		this.ehcacheKVMapMap.put(mapKey, ehcacheKVMap);
	}

	public <E> EhcacheKVMap<E> getEhcacheKVMap(String mapKey) {
		if (null == mapKey) {
			throw new NullPointerException("Map name cannot be null");
		}
		return (EhcacheKVMap<E>) ehcacheKVMapMap.get(mapKey);
	}

	public void removeEhcacheKVMap(String mapKey) {
		if (null == mapKey) {
			return;
		}
		this.ehcacheKVMapMap.remove(mapKey);
	}

	private enum EhcacheServiceInstance {
		INSTANCE;
		private final EhcacheService ehcacheService;

		EhcacheServiceInstance() {
			this.ehcacheService = new EhcacheService();
		}

		public EhcacheService getInstance() {
			return ehcacheService;
		}
	}
}
