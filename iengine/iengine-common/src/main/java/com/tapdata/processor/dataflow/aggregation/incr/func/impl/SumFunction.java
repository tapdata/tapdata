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
import com.tapdata.processor.dataflow.aggregation.incr.service.model.SumBucket;

import java.util.List;
import java.util.Map;

public class SumFunction extends AbstractAggsFunction {

	public SumFunction(BucketCache<FuncCacheKey, BucketValue> cache, Aggregation aggregation) throws Throwable {
		super(cache, aggregation);
	}

	@Override
	public Func getFunc() {
		return Func.SUM;
	}

	@Override
	protected AggrBucket doCall(SnapshotService snapshotService, SnapshotRecord snapshotRecord, Map<String, Object> groupByMap) {
		final FuncCacheKey funcCacheKey = new FuncCacheKey(this.processName, groupByMap);
		final SumBucket.Value cacheValue = (SumBucket.Value) this.cache.get(funcCacheKey);
		final SumBucket bucket;
		if (cacheValue == null) { // not in cache
			bucket = snapshotService.sum(groupByMap, this.valueField);
		} else if (cacheValue.getCount() <= 0) {
			bucket = new SumBucket(groupByMap, snapshotRecord.getRecordValue(this.valueField), 1);
		} else {                 //  update in cache
			Calculator<Number> calculator = ComposeCalculator.getInstance();
			final Number input = snapshotRecord.getRecordValue(this.valueField);
			Number sum;
			long count;
			if (snapshotRecord.isAppend()) {
				sum = calculator.add(cacheValue.getValue(), input);
				count = snapshotRecord.isIngnoreCount() ? cacheValue.getCount() : cacheValue.getCount() + 1;
			} else {
				sum = calculator.subtract(cacheValue.getValue(), input);
				count = snapshotRecord.isIngnoreCount() ? cacheValue.getCount() : cacheValue.getCount() - 1;
			}
			bucket = new SumBucket(groupByMap, sum, count);
		}
		this.cache.put(funcCacheKey, bucket.getBucketValue());
		return bucket;
	}

	@Override
	public SnapshotRecord diff(SnapshotRecord newRecord, SnapshotRecord oldRecord) {
		Number v1 = newRecord.getRecordValue(this.valueField);
		Number v2 = oldRecord.getRecordValue(this.valueField);
		Number subtract = ComposeCalculator.getInstance().subtract(v1, v2);
		SnapshotRecord clone = newRecord.clone();
		clone.setRecordValue(this.valueField, subtract);
		clone.setIgnoreCount(true);
		return clone;
	}

	@Override
	public List<AggrBucket> callByGroup(SnapshotService snapshotService) {
		return snapshotService.sumGroup(this.groupByFieldList, this.valueField);
	}
}
