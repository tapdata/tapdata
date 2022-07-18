package com.tapdata.processor.dataflow.aggregation.incr.func.impl;

import com.tapdata.entity.dataflow.Aggregation;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.func.AbstractAggsFunction;
import com.tapdata.processor.dataflow.aggregation.incr.func.Func;
import com.tapdata.processor.dataflow.aggregation.incr.func.FuncCacheKey;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotRecord;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AggrBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.BucketValue;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.CountBucket;

import java.util.List;
import java.util.Map;

public class CountFunction extends AbstractAggsFunction {

	public CountFunction(BucketCache<FuncCacheKey, BucketValue> cache, Aggregation aggregation) throws Throwable {
		super(cache, aggregation);
	}

	@Override
	public Func getFunc() {
		return Func.COUNT;
	}

	@Override
	public SnapshotRecord diff(SnapshotRecord newRecord, SnapshotRecord oldRecord) {
		return null;
	}

	@Override
	protected AggrBucket doCall(SnapshotService snapshotService, SnapshotRecord snapshotRecord, Map<String, Object> groupByMap) {
		final FuncCacheKey funcCacheKey = new FuncCacheKey(this.processName, groupByMap);
		final CountBucket.Value cacheValue = (CountBucket.Value) this.cache.get(funcCacheKey);
		final CountBucket bucket;
		if (cacheValue == null) { // not in cache
			bucket = snapshotService.count(groupByMap);
		} else {                 //  update in cache
			long count;
			if (snapshotRecord.isAppend()) {
				count = cacheValue.getCount() + 1;
			} else {
				count = cacheValue.getCount() - 1;
			}
			bucket = new CountBucket(groupByMap, count);
		}
		this.cache.put(funcCacheKey, bucket.getBucketValue());
		return bucket;
	}

	@Override
	public List<AggrBucket> callByGroup(SnapshotService snapshotService) {
		return snapshotService.countGroup(this.groupByFieldList);
	}


}
