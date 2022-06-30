package com.tapdata.cache.mapdb;

import com.tapdata.cache.serializer.AbstractSerializerCacheStore;
import com.tapdata.cache.MemoryCacheUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import org.mapdb.DB;
import org.mapdb.Serializer;

public class MapdbCacheStore extends AbstractSerializerCacheStore {

	public MapdbCacheStore(DataFlowCacheConfig cacheConfig, DB db) {
		super(cacheConfig,
				db.hashMap(MemoryCacheUtil.cacheDataKey(cacheConfig.getCacheName()), Serializer.STRING, Serializer.JAVA).counterEnable().createOrOpen(),
				db.hashMap(MemoryCacheUtil.cacheIndexKey(cacheConfig.getCacheName()), Serializer.STRING, Serializer.JAVA).counterEnable().createOrOpen());
	}


}
