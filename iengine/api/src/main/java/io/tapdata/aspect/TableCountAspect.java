package io.tapdata.aspect;

import io.tapdata.entity.aspect.Aspect;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

public class TableCountAspect extends DataFunctionAspect<TableCountAspect> {
	private Map<String, Long> tableCountMap;
	public TableCountAspect add(String table, long count) {
		if(tableCountMap == null)
			tableCountMap = new ConcurrentHashMap<>();
		tableCountMap.put(table, count);
		return this;
	}

	public Map<String, Long> getTableCountMap() {
		return tableCountMap;
	}

	public void setTableCountMap(Map<String, Long> tableCountMap) {
		this.tableCountMap = tableCountMap;
	}
}
