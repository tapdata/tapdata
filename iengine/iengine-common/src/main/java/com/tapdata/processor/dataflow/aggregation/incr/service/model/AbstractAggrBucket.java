package com.tapdata.processor.dataflow.aggregation.incr.service.model;

import java.util.Map;

abstract public class AbstractAggrBucket<T extends BucketValue> implements AggrBucket<T> {

	protected final Map<String, Object> groupByKey;

	public AbstractAggrBucket(Map<String, Object> groupByKey) {
		this.groupByKey = groupByKey;
	}

	@Override
	public Map<String, Object> getKey() {
		return this.groupByKey;
	}
}
