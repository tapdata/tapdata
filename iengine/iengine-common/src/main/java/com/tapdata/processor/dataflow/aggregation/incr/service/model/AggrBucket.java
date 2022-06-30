package com.tapdata.processor.dataflow.aggregation.incr.service.model;

import java.util.Map;

public interface AggrBucket<T extends BucketValue> {

	Map<String, Object> getKey();

	Number getValue();

	long getCount();

	T getBucketValue();

}
