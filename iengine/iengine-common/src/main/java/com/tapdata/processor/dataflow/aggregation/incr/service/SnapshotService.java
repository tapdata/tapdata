package com.tapdata.processor.dataflow.aggregation.incr.service;


import com.tapdata.processor.dataflow.aggregation.incr.service.model.AvgBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.CountBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.MaxBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.MinBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.SumBucket;

import java.util.List;
import java.util.Map;

public interface SnapshotService<T extends SnapshotRecord> extends LifeCycleService {

	T wrapRecord(List<String> primaryKeyFieldList, Map<String, Object> dataMap);

	T findOneAndReplace(T record);

	T findOneAndModify(T record);

	T findOneAndRemove(T record);

	SumBucket sum(Map<String, Object> groupByMap, String valueField);

	CountBucket count(Map<String, Object> groupByMap);

	MaxBucket max(Map<String, Object> groupByMap, String valueField);

	MinBucket min(Map<String, Object> groupByMap, String valueField);

	AvgBucket avg(Map<String, Object> groupByMap, String valueField);

	List<SumBucket> sumGroup(List<String> groupByFieldList, String valueField);

	List<CountBucket> countGroup(List<String> groupByFieldList);

	List<MaxBucket> maxGroup(List<String> groupByFieldList, String valueField);

	List<MinBucket> minGroup(List<String> groupByFieldList, String valueField);

	List<AvgBucket> avgGroup(List<String> groupByFieldList, String valueField);

	void reset();

	String createIndex(List<String> keyFieldList);

}
