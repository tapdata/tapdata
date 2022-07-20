package com.tapdata.cache.mapdb;

import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.cache.serializer.AbstractSerializerCacheGetter;
import com.tapdata.cache.serializer.AbstractSerializerCacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;

public class MapdbCacheGetter extends AbstractSerializerCacheGetter {
	public MapdbCacheGetter(DataFlowCacheConfig cacheConfig, ICacheStore cacheStore,
							ICacheStats cacheStats,
							ClientMongoOperator clientMongoOperator) {
		super(cacheConfig, cacheStore, cacheStats, ((AbstractSerializerCacheStore) cacheStore).getIndexMap(),
				((AbstractSerializerCacheStore) cacheStore).getDataMap(), clientMongoOperator);
	}

}
