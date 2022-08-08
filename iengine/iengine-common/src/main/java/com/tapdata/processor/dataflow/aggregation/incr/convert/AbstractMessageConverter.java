package com.tapdata.processor.dataflow.aggregation.incr.convert;

import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.aggregation.incr.func.AggrFunction;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotRecord;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.SyncVersionService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AggrBucket;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tapdata.entity.DataQualityTag.SUB_COLUMN_NAME;

abstract public class AbstractMessageConverter implements MessageConverter {

	protected final SnapshotService snapshotService;
	protected final List<AggrFunction> aggrFunctionList;
	protected final List<String> primaryKeyFieldList;
	protected final SyncVersionService syncVersionService;

	public AbstractMessageConverter(Stage stage, List<AggrFunction> aggrFunctionList, SnapshotService<? extends SnapshotRecord> snapshotService, SyncVersionService syncVersionService) {
		this.aggrFunctionList = aggrFunctionList;
		this.snapshotService = snapshotService;
		this.primaryKeyFieldList = Stream.of(stage.getPrimaryKeys().split(",")).collect(Collectors.toList());
		this.syncVersionService = syncVersionService;
	}

	protected Object fillDataMap(AggrFunction func, AggrBucket<?> bucket, Map<String, Object> dataMap) {
		final Object msgId;
		if (bucket.getKey().size() > 0) {
			msgId = new LinkedHashMap<>(bucket.getKey());
			((Map) msgId).put("_tapd8_sub_name", func.getProcessName());
			bucket.getKey().forEach(dataMap::put);
		} else {
			msgId = func.getProcessName();
		}
		dataMap.put("_id", msgId);
		dataMap.put(func.getFunc().name(), bucket.getValue());
		dataMap.put(SUB_COLUMN_NAME + ".version", syncVersionService.currentVersion());
		return msgId;
	}

}
