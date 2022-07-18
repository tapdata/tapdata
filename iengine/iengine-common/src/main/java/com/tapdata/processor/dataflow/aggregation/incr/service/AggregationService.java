package com.tapdata.processor.dataflow.aggregation.incr.service;

public interface AggregationService extends LifeCycleService {

	long removeExpire(long version);

}
