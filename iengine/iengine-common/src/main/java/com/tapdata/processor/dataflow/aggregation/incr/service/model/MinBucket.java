package com.tapdata.processor.dataflow.aggregation.incr.service.model;

import java.util.Map;

public class MinBucket extends AbstractAggrBucket<MinBucket.Value> {

	public static class Value implements BucketValue {
		private final Number value;
		private final long count;
		private final Object minId;

		public Value(Number value, long count, Object minId) {
			this.value = value;
			this.count = count;
			this.minId = minId;
		}

		public Number getValue() {
			return value;
		}

		public long getCount() {
			return count;
		}

		public Object getMinId() {
			return minId;
		}
	}

	private final Value bucketValue;

	public MinBucket(Map<String, Object> groupByKey, Object minId, Number min, long count) {
		super(groupByKey);
		this.bucketValue = new Value(min, count, minId);
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
