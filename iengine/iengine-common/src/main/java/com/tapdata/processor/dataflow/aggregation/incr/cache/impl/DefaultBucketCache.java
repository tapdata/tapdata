package com.tapdata.processor.dataflow.aggregation.incr.cache.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCacheBuilder;

public class DefaultBucketCache<K, V> implements BucketCache<K, V> {

	private final Cache<K, V> cache;

	public DefaultBucketCache(BucketCacheBuilder<K, V> builder) {
		cache = Caffeine.newBuilder().maximumSize(builder.getMaxSize()).build();
	}

	@Override
	public void put(K k, V v) {
		this.cache.put(k, v);
	}

	@Override
	public V get(K k) {
		return this.cache.getIfPresent(k);
	}

	@Override
	public void invalid(K k) {
		this.cache.invalidate(k);
	}

	@Override
	public void invalidAll() {
		this.cache.cleanUp();
	}

}
