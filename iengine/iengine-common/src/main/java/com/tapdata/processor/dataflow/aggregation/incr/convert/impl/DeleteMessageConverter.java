package com.tapdata.processor.dataflow.aggregation.incr.convert.impl;

import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.aggregation.incr.convert.AbstractMessageConverter;
import com.tapdata.processor.dataflow.aggregation.incr.convert.MessageOp;
import com.tapdata.processor.dataflow.aggregation.incr.func.AggrFunction;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotRecord;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.SyncVersionService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AggrBucket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class DeleteMessageConverter extends AbstractMessageConverter {

	public DeleteMessageConverter(Stage stage, List<AggrFunction> aggrFunctionList, SnapshotService snapshotService, SyncVersionService syncVersionService) {
		super(stage, aggrFunctionList, snapshotService, syncVersionService);
	}

	@Override
	public Collection<MessageEntity> convert(MessageEntity originMessage) {
		final SnapshotRecord tempRecord = this.snapshotService.wrapRecord(this.primaryKeyFieldList, originMessage.getBefore());
		// 1. insert snapshot
		final SnapshotRecord oldRecord = this.snapshotService.findOneAndRemove(tempRecord);
		if (oldRecord == null) {
			return Collections.emptyList();
		}
		oldRecord.setAppend(false);
		// 2. update aggregation
		final List<MessageEntity> messageEntityList = new ArrayList<>();
		for (AggrFunction func : aggrFunctionList) {
			// 2.1 record filter
			if (!func.isFilter(oldRecord.getDataMap())) {
				continue;
			}
			// 2.2 call aggregation function
			final AggrBucket<?> bucket = func.call(this.snapshotService, oldRecord);
			// 2.3 convert to MessageEntity
			final HashMap<String, Object> bucketDataMap = new HashMap<>();
			this.fillDataMap(func, bucket, bucketDataMap);
			final MessageEntity messageEntity = new MessageEntity();
			if (bucket.getCount() > 0) {
				messageEntity.setAfter(bucketDataMap);
				messageEntity.setOp(MessageOp.UPDATE.getType());
			} else {
				messageEntity.setBefore(bucketDataMap);
				messageEntity.setOp(this.getOp().getType());
			}
			messageEntity.setTableName(originMessage.getTableName());
			messageEntity.setOffset(originMessage.getOffset());
			messageEntityList.add(messageEntity);
		}
		return messageEntityList;
	}

	@Override
	public MessageOp getOp() {
		return MessageOp.DELETE;
	}

}
