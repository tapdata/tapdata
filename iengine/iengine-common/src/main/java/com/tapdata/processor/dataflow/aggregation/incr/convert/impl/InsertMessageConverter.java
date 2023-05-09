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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class InsertMessageConverter extends AbstractMessageConverter {

	public InsertMessageConverter(Stage stage, List<AggrFunction> aggrFunctionList, SnapshotService<?> snapshotService, SyncVersionService syncVersionService) {
		super(stage, aggrFunctionList, snapshotService, syncVersionService);
	}

	@Override
	public Collection<MessageEntity> convert(MessageEntity originMessage) {
		final SnapshotRecord newRecord = this.snapshotService.wrapRecord(primaryKeyFieldList, originMessage.getAfter());
		// 1. insert snapshot
		final SnapshotRecord oldRecord = this.snapshotService.findOneAndReplace(newRecord);
		// 2. update aggregation
		final List<MessageEntity> messageEntityList = new ArrayList<>();
		for (AggrFunction func : aggrFunctionList) {
			boolean valueChanged = func.isValueChanged(newRecord, oldRecord);
			boolean bucketKeyEquals = func.isBucketKeyEquals(newRecord, oldRecord);
			if (!valueChanged && bucketKeyEquals) {
				continue;
			}
			final List<SnapshotRecord> snapshotRecordList;
			if (bucketKeyEquals) {
				SnapshotRecord diff = func.diff(newRecord, oldRecord);
				snapshotRecordList = diff == null ? Collections.emptyList() : Collections.singletonList(diff);
			} else {
				snapshotRecordList = oldRecord != null ? Arrays.asList(oldRecord, newRecord) : Collections.singletonList(newRecord);
			}
			for (SnapshotRecord record : snapshotRecordList) {
				// 2.1 filter
				if (!func.isFilter(record.getDataMap())) {
					continue;
				}
				// 2.2 call aggregation function
				final AggrBucket<?> bucket = func.call(this.snapshotService, record);
				// 2.3 convert to MessageEntity
				final HashMap<String, Object> bucketDataMap = new HashMap<>();
				this.fillDataMap(func, bucket, bucketDataMap);

				final MessageEntity messageEntity = new MessageEntity();
				if (bucket.getCount() <= 0) {
					messageEntity.setOp(MessageOp.DELETE.getType());
					messageEntity.setBefore(bucketDataMap);
				} else if (record.isAppend() && oldRecord == null && bucket.getCount() == 1) {
					messageEntity.setOp(MessageOp.INSERT.getType());
					messageEntity.setAfter(bucketDataMap);
				} else {
					messageEntity.setOp(MessageOp.UPDATE.getType());
					messageEntity.setAfter(bucketDataMap);
				}
				messageEntity.setTableName(originMessage.getTableName());
				messageEntity.setOffset(originMessage.getOffset());
				messageEntityList.add(messageEntity);
			}
		}
		return messageEntityList;
	}

	@Override
	public MessageOp getOp() {
		return MessageOp.INSERT;
	}

}
