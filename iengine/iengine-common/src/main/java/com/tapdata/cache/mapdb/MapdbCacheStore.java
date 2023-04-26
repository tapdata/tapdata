package com.tapdata.cache.mapdb;

import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.serializer.AbstractSerializerCacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import org.mapdb.DB;
import org.mapdb.Serializer;

public class MapdbCacheStore extends AbstractSerializerCacheStore {

	public MapdbCacheStore(DataFlowCacheConfig cacheConfig, DB db) {
		super(cacheConfig,
				db.hashMap(CacheUtil.cacheDataKey(cacheConfig.getCacheName()), Serializer.STRING, Serializer.JAVA).counterEnable().createOrOpen(),
				db.hashMap(CacheUtil.cacheIndexKey(cacheConfig.getCacheName()), Serializer.STRING, Serializer.JAVA).counterEnable().createOrOpen());
	}


}
