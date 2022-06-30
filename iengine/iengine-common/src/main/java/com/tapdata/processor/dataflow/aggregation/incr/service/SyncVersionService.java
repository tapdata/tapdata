package com.tapdata.processor.dataflow.aggregation.incr.service;

public interface SyncVersionService extends LifeCycleService {

	long nextVersion();

	long currentVersion();

}
