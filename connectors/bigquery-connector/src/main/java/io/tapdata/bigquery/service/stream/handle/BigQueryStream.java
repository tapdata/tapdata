package io.tapdata.bigquery.service.stream.handle;

import com.google.protobuf.Descriptors;
import io.tapdata.bigquery.service.bigQuery.BigQueryStart;
import io.tapdata.bigquery.service.stream.WriteCommittedStream;
import io.tapdata.bigquery.util.bigQueryUtil.SqlValueConvert;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BigQueryStream extends BigQueryStart {
    public static final String TAG = BigQueryStream.class.getSimpleName();

    WriteCommittedStream stream;
    TapTable tapTable;
    MergeHandel merge ;
    public BigQueryStream merge(MergeHandel merge){
        this.merge = merge;
        return this;
    }
    public TapTable tapTable(){
        return this.tapTable;
    }
    public BigQueryStream tapTable(TapTable tapTable){
        this.tapTable = tapTable;
        return this;
    }

    private BigQueryStream(TapConnectionContext connectorContext) throws InterruptedException, IOException, Descriptors.DescriptorValidationException {
        super(connectorContext);
    }

    public static BigQueryStream streamWrite(TapConnectorContext context) throws InterruptedException, Descriptors.DescriptorValidationException, IOException {
        return new BigQueryStream(context);
    }


    public WriteListResult<TapRecordEvent> writeRecord(List<TapRecordEvent> events, TapTable table) throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        AtomicInteger insert = new AtomicInteger();
        AtomicInteger update = new AtomicInteger();
        AtomicInteger delete = new AtomicInteger();
        stream = WriteCommittedStream.writer(super.config().projectId(),super.config().tableSet(),table.getId(),super.config().serviceAccount());
        if (this.config.isMixedUpdates()){
            //混合模式写入数据
            synchronized (merge.mergeLock()){
                stream.appendJSON(records(merge.temporaryEvent(events),insert,update,delete));
            }
        }else {
            //append-only 模式
            stream.append(records(events,insert,update,delete));
        }
        result.setInsertedCount(insert.get());
        result.setModifiedCount(update.get());
        result.setRemovedCount(delete.get());
        return result;
    }


    private List<Map<String,Object>> records(List<TapRecordEvent> events,AtomicInteger insert,AtomicInteger update,AtomicInteger delete){
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (Objects.isNull(nameFieldMap) || nameFieldMap.isEmpty()) {
            throw new CoreException("TapTable not any fields.");
        }
        KVMap<Object> stateMap = ((TapConnectorContext) this.connectorContext).getStateMap();
        return events.stream().filter(Objects::nonNull).map(event->{
            Map<String,Object> record = new HashMap<>();
            if (event instanceof TapInsertRecordEvent){
                insert.getAndIncrement();
                record = ((TapInsertRecordEvent)event).getAfter();
            }else if(event instanceof TapUpdateRecordEvent){
                update.getAndIncrement();
                //record = ((TapUpdateRecordEvent)event).getAfter();
            }else if(event instanceof TapDeleteRecordEvent){
                delete.getAndIncrement();
                //record = ((TapDeleteRecordEvent)event).getBefore();
            }else {

            }
            Object mergeKeyId = record.get(MergeHandel.MERGE_KEY_ID);
            if (Objects.nonNull(mergeKeyId)){
                stateMap.put(MergeHandel.MERGE_KEY_ID,mergeKeyId);
            }
            Map<String,Object> recordMap = new HashMap<>();
            for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
                String key = entry.getKey();
                TapField field = entry.getValue();
                String value = SqlValueConvert.sqlValue(record.get(key),field);
                recordMap.put(key,value);
            }
            return recordMap.isEmpty()?null:recordMap;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }
}
