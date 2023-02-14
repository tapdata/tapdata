package io.tapdata.bigquery.service.stream.v2;

import com.google.protobuf.Descriptors;
import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.bigquery.service.bigQuery.BigQueryStart;
import io.tapdata.bigquery.service.stream.WriteCommittedStream;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BigQueryStream extends BigQueryStart {
    public static final String TAG = BigQueryStream.class.getSimpleName();

    private TapTable tapTable;
    private MergeHandel merge;
    private StateMapOperator stateMap;
    private Map<String, String> tableWithTempTable = new ConcurrentHashMap<>();
    private final Object lock = new Object();

    public final Map<String, WriteCommittedStream> streamFactory = new HashMap<>();

    public BigQueryStream stateMap(TapConnectionContext connectorContext) {
        if (!(connectorContext instanceof TapConnectorContext)) {
            throw new CoreException("Cannot get State map in TapConnectionContext. ");
        }
        this.stateMap = StateMapOperator.operator((TapConnectorContext) connectorContext);
        return this;
    }

    public BigQueryStream merge(MergeHandel merge) {
        this.merge = merge.stateMap(this.stateMap);
        return this;
    }

    public TapTable tapTable() {
        return this.tapTable;
    }

    public BigQueryStream tapTable(TapTable tapTable) {
        this.tapTable = tapTable;
        return this;
    }

    /**
     * @deprecated
     */
    public WriteCommittedStream createWriteCommittedStream() throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        String tableName = super.config.isMixedUpdates() ? super.config().tempCursorSchema() : this.tapTable.getName();
        return this.createWriteCommittedStream(tableName);
    }

    public WriteCommittedStream createWriteCommittedStream(String tableName) throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        return this.streamFactory.computeIfAbsent(tableName, key -> {
            try {
                return WriteCommittedStream.writer(
                        super.config().projectId(),
                        super.config().tableSet(),
                        tableName,
                        super.config().serviceAccount())
                        .client(WriteCommittedStream.createClient(super.config().serviceAccount()))
                        .maxStreamCount(super.config().maxStreamAppendCount())
                        .create();
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        });
    }

    private BigQueryStream(TapConnectionContext connectorContext) throws IOException {
        super(connectorContext);
    }

    public static BigQueryStream streamWrite(TapConnectorContext context) throws IOException {
        return new BigQueryStream(context).stateMap(context);
    }

    public synchronized WriteListResult<TapRecordEvent> writeRecord(List<TapRecordEvent> events, TapTable table) throws InterruptedException, IOException, Descriptors.DescriptorValidationException {
        String tableId = table.getId();
        Long streamToBatchTime = this.stateMap.getLong(tableId, MergeHandel.BATCH_TO_STREAM_TIME);
        boolean needCreateTemporaryTable = Objects.isNull(streamToBatchTime);
        Long mergeId = this.stateMap.getLong(tableId, MergeHandel.MERGE_KEY_ID);
        EventAfter eventAfter = new EventAfter(streamToBatchTime)
                .hasMerged(Objects.nonNull(mergeId))
                .table(table);
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        this.tapTable = table;
        eventAfter.convertData(events, this.merge, table);
        streamToBatchTime = Optional.ofNullable(streamToBatchTime).orElse(eventAfter.streamToBatchTime());
        if (!eventAfter.appendData().isEmpty()) {
            synchronized (this.lock) {
                this.createWriteCommittedStream(tableId).append(eventAfter.appendData());
                this.stateMap.saveForTable(tableId, MergeHandel.BATCH_TO_STREAM_TIME, streamToBatchTime);
                //this.batch.close();
            }
            //this.batch.close();
        }
        if (!eventAfter.isAppend()) {
            // 创建临时表
            if (needCreateTemporaryTable) {
                String temporaryTableName = super.tempCursorSchema(tableId, this.stateMap);
                try {
                    this.merge.createTemporaryTable(table, temporaryTableName);
                }catch (Exception e){
                    TapLogger.error(TAG,"Temporary table creation failed. temporary table name: {}, table name: {},  ",temporaryTableName,tableId);
                }
                this.stateMap.saveForTable(tableId, ContextConfig.TEMP_CURSOR_SCHEMA_NAME, temporaryTableName);
                this.stateMap.save(MergeHandel.BATCH_TO_STREAM_TIME, System.nanoTime());
                TapLogger.info(TAG, String.format(" The data has been written in stream mode,and will be written to a temporary table. A temporary table has been created for [ %s ] which name is: %s", tableId, temporaryTableName));
                this.tableWithTempTable.put(tableId, temporaryTableName);
            }
            // 启动merge线程
//            long delay = MergeHandel.FIRST_MERGE_DELAY_SECOND * 1000000000L;
//            long nowTime = System.nanoTime();
//            //mergeId == null,判断此时为首次merge,延时33min
//            if (Objects.isNull(mergeId)) {
//                // -->Time course direction
//                // |------------------|------------------|--------|---------|------------------
//                // T1                 T2                DT1       T3       DT2
//                //may be:
//                // T1: Task start time point .
//                // T2: End of batch processing and start time of outflow processing
//                // T3: Time point of first data consolidation
//                // DT1: Possible recovery time point after task interruption 1, nowTime
//                // DT2:  Possible recovery time point after task interruption 2, nowTime
//                long time = streamToBatchTime + delay;
//                delay = 10 + (nowTime > time ? 0 : time - nowTime);
//            } else {
//                // -->Time course direction
//                // |------------------|---------------------------|---------|----------|--------
//                // T1                 T2                          T3       DT1         T4
//                // may be :
//                // T1: Task start time point .
//                // T2: End of batch processing and start time of outflow processing
//                // T3: Time point of first data merge
//                // T4: Time point of second data merge
//                // DT1: Possible recovery time point after task and after merge data interruption , nowTime
//                //
//                //mergeId != null,非首次merge,是否在上传merge到现在的时间超过了用户配置的时间间隔
//                //超过了就merge异一次，未超过这设置时间差进行延时merge.
//                Long delaySecond = super.config().mergeDelay();
//                long time = nowTime - mergeId;
//                delay = 10 + (time > delaySecond ? 0 : delaySecond - time);
//            }
            this.merge.mergeTemporaryTableToMainTable(table);
            this.merge.needMerge(table);
        }

        if (!eventAfter.mixedAndAppendData().isEmpty()) {
            String tempTableId = this.tableWithTempTable.get(tableId);
            if (Objects.isNull(tempTableId)) {
                Object tempTableIdObj = this.stateMap.getOfTable(tableId, ContextConfig.TEMP_CURSOR_SCHEMA_NAME);
                if (Objects.isNull(tempTableIdObj)) {
                    throw new CoreException(" Coding error: The temporary table was not created or the temporary table name was not saved successfully. Please check the code implementation. ");
                }
                this.tableWithTempTable.put(tableId, tempTableId = String.valueOf(tempTableIdObj));
            }
            synchronized (this.lock) {
                this.createWriteCommittedStream(tempTableId).appendJSON(eventAfter.mixedAndAppendData());
                //this.batch.close();
            }
            //this.stream.close();
            Optional.ofNullable(eventAfter.mergeKeyId()).ifPresent(e -> this.stateMap.saveForTable(tableId, MergeHandel.MERGE_KEY_ID, e));
            Optional.ofNullable(eventAfter.streamToBatchTime()).ifPresent(e -> this.stateMap.saveForTable(tableId, MergeHandel.BATCH_TO_STREAM_TIME, e));
        }
        return result.removedCount(eventAfter.delete())
                .insertedCount(eventAfter.insert())
                .modifiedCount(eventAfter.update());
    }

    public void closeStream() {
        synchronized (this.lock) {
            if (!this.streamFactory.isEmpty()) {
                this.streamFactory.forEach((table, stream) -> Optional.ofNullable(stream).ifPresent(WriteCommittedStream::close));
            }
        }
    }

    public static class EventAfter {
        private long insert;
        private long update;
        private long delete;
        private boolean isAppend = true;
        private final List<Map<String, Object>> appendData = new ArrayList<>();
        private final List<Map<String, Object>> mixedAndAppendData = new ArrayList<>();
        private Object mergeKeyId;
        public Long streamToBatchTime;
        private TapTable table;

        public EventAfter table(TapTable mainTable) {
            this.table = mainTable;
            return this;
        }

        public long insert() {
            return this.insert;
        }

        public long delete() {
            return this.delete;
        }

        public long update() {
            return this.update;
        }

        public List<Map<String, Object>> appendData() {
            return this.appendData;
        }

        public List<Map<String, Object>> mixedAndAppendData() {
            return this.mixedAndAppendData;
        }

        public Object mergeKeyId() {
            return this.mergeKeyId;
        }

        public Long streamToBatchTime() {
            return this.streamToBatchTime;
        }

        public EventAfter(Long streamToBatchTime) {
            if (Objects.nonNull(streamToBatchTime)) {
                this.streamToBatchTime = streamToBatchTime;
                this.isAppend = false;
            }
        }

        public EventAfter hasMerged(boolean hasMerged) {
            this.isAppend = !hasMerged;
            return this;
        }

        public EventAfter convertData(List<TapRecordEvent> events, MergeHandel mergeHandel, TapTable table) {
            for (TapRecordEvent event : events) {
                Map<String, Object> record = new HashMap<>();
                if (event instanceof TapInsertRecordEvent) {
                    this.insert++;
                    record = ((TapInsertRecordEvent) event).getAfter();
                } else {
                    if (this.isAppend) {
                        this.isAppend = false;
                        this.streamToBatchTime = System.nanoTime();
                        TapLogger.info(TAG,"The full quantity has ended and is entering the increment. The data will be written to the temporary table. Please note.");
                    }
                    if (event instanceof TapUpdateRecordEvent) {
                        this.update++;
                    } else if (event instanceof TapDeleteRecordEvent) {
                        this.delete++;
                    } else {
                        TapLogger.warn(TAG, "Unable to process invalid time type, invalid record, ignored. ");
                    }
                }
                if (this.isAppend) {
                    this.appendData.add(record);
                } else {
                    TapInsertRecordEvent temporaryEvent = mergeHandel.temporaryEvent(event, table);
                    Optional.ofNullable(temporaryEvent).ifPresent(e -> {
                        Map<String, Object> after = e.getAfter();
                        this.mixedAndAppendData.add(after);
                        mergeKeyId = after.get(MergeHandel.MERGE_KEY_ID);
                    });
                }
            }
            return this;
        }

        public boolean isAppend() {
            return this.isAppend;
        }
    }
}
