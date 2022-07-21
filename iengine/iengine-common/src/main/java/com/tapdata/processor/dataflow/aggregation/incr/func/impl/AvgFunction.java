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
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AvgBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.BucketValue;

import java.util.List;
import java.util.Map;

public class AvgFunction extends AbstractAggsFunction {

	public AvgFunction(BucketCache<FuncCacheKey, BucketValue> cache, Aggregation aggregation) throws Throwable {
		super(cache, aggregation);
	}

	@Override
	protected AggrBucket doCall(SnapshotService snapshotService, SnapshotRecord snapshotRecord, Map<String, Object> groupByMap) {
		final FuncCacheKey funcCacheKey = new FuncCacheKey(this.processName, groupByMap);
		final AvgBucket.Value cacheValue = (AvgBucket.Value) this.cache.get(funcCacheKey);
		final AvgBucket bucket;
		if (cacheValue == null) { // not in cache
			bucket = snapshotService.avg(groupByMap, this.valueField);
		} else if (cacheValue.getCount() <= 0) {
			final Number input = snapshotRecord.getRecordValue(this.valueField);
			bucket = new AvgBucket(groupByMap, input, input, 1);
		} else {                 //  update in cache
			Calculator<Number> calculator = ComposeCalculator.getInstance();
			final Number input = snapshotRecord.getRecordValue(this.valueField);
			Number sum;
			long count;
			if (snapshotRecord.isAppend()) {
				sum = calculator.add(cacheValue.getSum(), input);
				count = snapshotRecord.isIngnoreCount() ? cacheValue.getCount() : cacheValue.getCount() + 1;
			} else {
				sum = calculator.subtract(cacheValue.getSum(), input);
				count = snapshotRecord.isIngnoreCount() ? cacheValue.getCount() : cacheValue.getCount() - 1;
			}
			Number avg = calculator.divide(sum, count);
			bucket = new AvgBucket(groupByMap, avg, sum, count);
		}
		this.cache.put(funcCacheKey, bucket.getBucketValue());
		return bucket;
	}

	@Override
	public Func getFunc() {
		return Func.AVG;
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
		return snapshotService.avgGroup(this.groupByFieldList, this.valueField);
	}

}


