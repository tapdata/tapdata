package com.tapdata.cache.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.cache.IDataSourceRowsGetter;
import com.tapdata.cache.PdkDataSourceRowsGetter;
import com.tapdata.cache.serializer.AbstractSerializerCacheGetter;
import com.tapdata.cache.serializer.AbstractSerializerCacheStore;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

public class HazelcastCacheGetter extends AbstractSerializerCacheGetter {

	private final HazelcastInstance hazelcastInstance;


	public HazelcastCacheGetter(DataFlowCacheConfig cacheConfig, ICacheStore cacheStore, ICacheStats cacheStats,
								ClientMongoOperator clientMongoOperator,
								HazelcastInstance hazelcastInstance) {
		super(cacheConfig, cacheStore, cacheStats,
				((AbstractSerializerCacheStore) cacheStore).getIndexMap(),
				((AbstractSerializerCacheStore) cacheStore).getDataMap(), clientMongoOperator);
		this.hazelcastInstance = hazelcastInstance;
	}

	@Override
	protected Connections getSourceConnection(DataFlowCacheConfig cacheConfig) {
		Connections sourceConnection = super.getSourceConnection(cacheConfig);
		if (sourceConnection == null && StringUtils.isNotEmpty(cacheConfig.getSourceConnectionId())) {
			Query query = new Query(Criteria.where("_id").is(cacheConfig.getSourceConnectionId()));
			query.fields().exclude("schema");
			sourceConnection = MongodbUtil.getConnections(query, clientMongoOperator, true);
			cacheConfig.setSourceConnection(sourceConnection);
		}
		return sourceConnection;
	}

	@Override
	public IDataSourceRowsGetter getDataSourceRowsGetter() {
		if (dataSourceRowsGetter == null) {
			logger.info("construct a data source rows getter for cache [{}]", cacheConfig.getCacheName());
			this.dataSourceRowsGetter = new PdkDataSourceRowsGetter(cacheConfig, clientMongoOperator, hazelcastInstance);
		}
		return dataSourceRowsGetter;
	}
}
