package com.tapdata.processor.dataflow.aggregation.incr.func.impl;

import com.tapdata.entity.dataflow.Aggregation;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.calc.Calculator;
import com.tapdata.processor.dataflow.aggregation.incr.calc.ComposeCalculator;
import com.tapdata.processor.dataflow.aggregation.incr.func.AbstractAggsFunction;
import com.tapdata.processor.dataflow.aggregation.incr.func.Func;
import com.tapdata.processor.dataflow.aggregation.incr.func.FuncCacheKey;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotRecord;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AggrBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.BucketValue;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.MaxBucket;

import java.util.List;
import java.util.Map;

public class MaxFunction extends AbstractAggsFunction {

	public MaxFunction(BucketCache<FuncCacheKey, BucketValue> cache, Aggregation aggregation) throws Throwable {
		super(cache, aggregation);
	}

	@Override
	public Func getFunc() {
		return Func.MAX;
	}

	@Override
	public SnapshotRecord diff(SnapshotRecord newRecord, SnapshotRecord oldRecord) {
		SnapshotRecord clone = newRecord.clone();
		clone.setIgnoreCount(true);
		return clone;
	}

	@Override
	protected AggrBucket doCall(SnapshotService snapshotService, SnapshotRecord snapshotRecord, Map<String, Object> groupByMap) {
		final FuncCacheKey funcCacheKey = new FuncCacheKey(this.processName, groupByMap);
		final MaxBucket.Value cacheValue = (MaxBucket.Value) this.cache.get(funcCacheKey);
		final MaxBucket bucket;
		if (cacheValue == null) { // not in cache
			bucket = snapshotService.max(groupByMap, this.valueField);
		} else if (cacheValue.getCount() <= 0) {
			bucket = new MaxBucket(groupByMap, snapshotRecord.getRecordID(), snapshotRecord.getRecordValue(this.valueField), 1);
		} else {                 //  update in cache
			if (snapshotRecord.isAppend()) {
				bucket = this.handleAppendRecord(cacheValue, snapshotRecord, snapshotService, groupByMap);
			} else {
				bucket = this.handleNonAppendRecord(cacheValue, snapshotRecord, snapshotService, groupByMap);
			}
		}
		this.cache.put(funcCacheKey, bucket.getBucketValue());
		return bucket;
	}

	private MaxBucket handleAppendRecord(MaxBucket.Value cacheValue, SnapshotRecord snapshotRecord, SnapshotService snapshotService, Map<String, Object> groupByMap) {
		Calculator<Number> calculator = ComposeCalculator.getInstance();
		final Object id = snapshotRecord.getRecordID();
		final Number input = snapshotRecord.getRecordValue(this.valueField);
		boolean gt = calculator.gt(input, cacheValue.getValue());
		if (gt) {
			return new MaxBucket(groupByMap, id, input, snapshotRecord.isIngnoreCount() ? cacheValue.getCount() : cacheValue.getCount() + 1);
		} else {
			return snapshotService.max(groupByMap, this.valueField);
		}
	}

	private MaxBucket handleNonAppendRecord(MaxBucket.Value cacheValue, SnapshotRecord snapshotRecord, SnapshotService snapshotService, Map<String, Object> groupByMap) {
		boolean isSameId = snapshotRecord.getRecordID().equals(cacheValue.getMaxId());
		if (isSameId) {
			return snapshotService.max(groupByMap, this.valueField);
		} else {
			return new MaxBucket(groupByMap, cacheValue.getMaxId(), cacheValue.getValue(), cacheValue.getCount() - 1);
		}
	}

	@Override
	public List<AggrBucket> callByGroup(SnapshotService snapshotService) {
		return snapshotService.maxGroup(this.groupByFieldList, valueField);
	}

}
