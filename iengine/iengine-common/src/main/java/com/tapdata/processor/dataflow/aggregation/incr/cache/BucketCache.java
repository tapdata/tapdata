package com.tapdata.processor.dataflow.aggregation.incr.cache;

public interface BucketCache<K, V> {

	void put(K k, V v);

	V get(K k);

	void invalid(K k);

	void invalidAll();

}
