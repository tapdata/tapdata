package com.tapdata.processor.dataflow.aggregation.incr.func;

import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotRecord;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AggrBucket;

import java.util.List;
import java.util.Map;

public interface AggrFunction {

	AggrBucket call(SnapshotService snapshotService, SnapshotRecord snapshotRecord);

	List<AggrBucket> callByGroup(SnapshotService snapshotService);

	Func getFunc();

	String getProcessName();

	boolean isFilter(Map<String, Object> dataMap);

	String getValueField();

	Map<String, Object> formatBucketKey(SnapshotRecord snapshotRecord);

	boolean isBucketKeyEquals(SnapshotRecord r1, SnapshotRecord r2);

	boolean isValueChanged(SnapshotRecord r1, SnapshotRecord r2);

	SnapshotRecord diff(SnapshotRecord newRecord, SnapshotRecord oldRecord);

}
