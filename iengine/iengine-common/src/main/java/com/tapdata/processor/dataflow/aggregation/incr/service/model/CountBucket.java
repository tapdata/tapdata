package com.tapdata.processor.dataflow.aggregation.incr.service.model;

import java.util.Map;

public class CountBucket extends AbstractAggrBucket<CountBucket.Value> {

	public static class Value implements BucketValue {
		private final long count;

		public Value(long count) {
			this.count = count;
		}

		public long getCount() {
			return count;
		}
	}

	private final Value bucketValue;

	public CountBucket(Map<String, Object> groupByKey, long count) {
		super(groupByKey);
		this.bucketValue = new Value(count);
	}

	@Override
	public Number getValue() {
		return this.bucketValue.getCount();
	}

	@Override
	public long getCount() {
		return this.bucketValue.getCount();
	}

	@Override
	public Value getBucketValue() {
		return this.bucketValue;
	}


}
