package com.tapdata.processor.dataflow.aggregation.incr.service.model;

import java.util.Map;

public class MaxBucket extends AbstractAggrBucket<MaxBucket.Value> {

	public static class Value implements BucketValue {
		private final Number value;
		private final long count;
		private final Object maxId;

		public Value(Number value, long count, Object maxId) {
			this.value = value;
			this.count = count;
			this.maxId = maxId;
		}

		public Number getValue() {
			return value;
		}

		public long getCount() {
			return count;
		}

		public Object getMaxId() {
			return maxId;
		}
	}

	private final Value bucketValue;

	public MaxBucket(Map<String, Object> groupByKey, Object maxId, Number max, long count) {
		super(groupByKey);
		this.bucketValue = new Value(max, count, maxId);
	}

	@Override
	public Number getValue() {
		return this.bucketValue.getValue();
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
