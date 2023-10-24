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

import static io.tapdata.entity.simplify.TapSimplify.*;

@Implementation(value = KVMap.class, buildNumber = 0, type = "persistent")
public class MongodbKVMap<T> extends EhcacheKVMap<T> {
	public MongodbKVMap() {
		this.maxDiskMB(1024).maxHeapEntries(10);
	}
}
