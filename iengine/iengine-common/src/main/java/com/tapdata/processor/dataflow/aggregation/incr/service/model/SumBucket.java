package com.tapdata.processor.dataflow.aggregation.incr.service.model;

import java.util.Map;

public class SumBucket extends AbstractAggrBucket<SumBucket.Value> {

	public static class Value implements BucketValue {
		private final Number value;
		private final long count;

		public Value(Number value, long count) {
			this.value = value;
			this.count = count;
		}

		public Number getValue() {
			return value;
		}

		public long getCount() {
			return count;
		}
	}

	private final Value bucketValue;

	public SumBucket(Map<String, Object> groupByKey, Number value, long count) {
		super(groupByKey);
		this.bucketValue = new Value(value, count);
	}

	@Override
	public Number getValue() {
		return bucketValue.getValue();
	}

	@Override
	public long getCount() {
		return bucketValue.getCount();
	}

	@Override
	public SumBucket.Value getBucketValue() {
		return bucketValue;
	}

}
