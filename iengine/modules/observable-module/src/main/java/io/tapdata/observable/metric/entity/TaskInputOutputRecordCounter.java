package io.tapdata.observable.metric.entity;

import io.tapdata.common.sample.sampler.CounterSampler;

/**
 * @author samuel
 * @Description
 * @create 2024-09-11 16:10
 **/
public class TaskInputOutputRecordCounter {
	private final CounterSampler insertCounter = new CounterSampler(0L);
	private final CounterSampler updateCounter = new CounterSampler(0L);
	private final CounterSampler deleteCounter = new CounterSampler(0L);

	public CounterSampler getInsertCounter() {
		return insertCounter;
	}

	public CounterSampler getUpdateCounter() {
		return updateCounter;
	}

	public CounterSampler getDeleteCounter() {
		return deleteCounter;
	}
}
