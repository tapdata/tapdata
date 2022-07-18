package com.tapdata.processor.dataflow.aggregation.incr.cache;

import com.tapdata.processor.dataflow.aggregation.incr.cache.impl.DefaultBucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.cache.impl.NoneBucketCache;

public class BucketCacheBuilder<K, V> {

	private int maxSize;

	public BucketCacheBuilder() {
	}

	public BucketCacheBuilder<K, V> maxSize(int maxSize) {
		this.maxSize = maxSize;
		return this;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public BucketCache<K, V> build() {
		return maxSize > 0 ? new DefaultBucketCache<>(this) : new NoneBucketCache<>();
	}

}
