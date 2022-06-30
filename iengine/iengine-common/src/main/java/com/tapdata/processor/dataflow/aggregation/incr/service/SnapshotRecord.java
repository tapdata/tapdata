package com.tapdata.processor.dataflow.aggregation.incr.service;

import java.util.Map;

public interface SnapshotRecord {

	<T> T getRecord();

	<T> T getRecordID();

	<T> T getRecordValue(String field);

	Object setRecordValue(String field, Object value);

	Map<String, Object> getDataMap();

	boolean isAppend();

	boolean isIngnoreCount();

	void setAppend(boolean isAppend);

	void setIgnoreCount(boolean isIgnoreCount);

	SnapshotRecord clone();

}
