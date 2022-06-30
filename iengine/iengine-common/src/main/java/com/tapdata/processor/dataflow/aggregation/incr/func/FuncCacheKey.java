package com.tapdata.processor.dataflow.aggregation.incr.func;

import java.util.Map;
import java.util.Objects;

public class FuncCacheKey {

	private final String processName;

	private final Map<String, Object> groupByMap;

	public FuncCacheKey(String processName, Map<String, Object> groupByMap) {
		this.processName = processName;
		this.groupByMap = groupByMap;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		FuncCacheKey that = (FuncCacheKey) o;
		return processName.equals(that.processName) && groupByMap.equals(that.groupByMap);
	}

	@Override
	public int hashCode() {
		return Objects.hash(processName, groupByMap);
	}
}
