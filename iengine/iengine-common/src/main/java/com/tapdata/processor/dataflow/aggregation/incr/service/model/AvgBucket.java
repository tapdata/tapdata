package com.tapdata.processor.dataflow.aggregation.incr.service.model;

import java.util.Map;

public class AvgBucket extends AbstractAggrBucket<AvgBucket.Value> {

	public static class Value implements BucketValue {
		private final Number value;
		private final Number sum;
		private final long count;

		public Value(Number value, Number sum, long count) {
			this.value = value;
			this.sum = sum;
			this.count = count;
		}

		public Number getValue() {
			return value;
		}

		public Number getSum() {
			return sum;
		}

		public long getCount() {
			return count;
		}
	}

	private final Value bucketValue;

	public AvgBucket(Map<String, Object> groupByKey, Number value, Number sum, long count) {
		super(groupByKey);
		this.bucketValue = new Value(value, sum, count);
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
	public Value getBucketValue() {
		return bucketValue;
	}

	public Number getSum() {
		return bucketValue.getSum();
	}

}
