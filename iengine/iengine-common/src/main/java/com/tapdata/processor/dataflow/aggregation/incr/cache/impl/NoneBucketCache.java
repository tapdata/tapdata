package com.tapdata.processor.dataflow.aggregation.incr.cache.impl;

import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;

public class NoneBucketCache<K, V> implements BucketCache<K, V> {

	@Override
	public void put(K k, V v) {

	}

	@Override
	public V get(K k) {
		return null;
	}

	@Override
	public void invalid(K k) {

	}

	@Override
	public void invalidAll() {

	}
}
