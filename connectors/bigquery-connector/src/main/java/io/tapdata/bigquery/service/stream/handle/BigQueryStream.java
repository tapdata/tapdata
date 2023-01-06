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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BigQueryStream extends BigQueryStart {
    public static final String TAG = BigQueryStream.class.getSimpleName();

    WriteCommittedStream stream;
    public static final Map<String,WriteCommittedStream> streamMap = new ConcurrentHashMap<>();
    TapTable tapTable;
    MergeHandel merge ;
    Long streamOffset;
    public BigQueryStream streamOffset(Long streamOffset){
        this.streamOffset = streamOffset;
        return this;
    }
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
    public BigQueryStream createWriteCommittedStream() throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        if (Objects.isNull(stream)) {
            String key = super.config().projectId()+"_"+ super.config().tableSet()+"_"+super.config().tempCursorSchema()+"_"+super.config().serviceAccount();
            stream = Optional.ofNullable(streamMap.get(key))
                    .orElse(WriteCommittedStream.writer(super.config().projectId(), super.config().tableSet(), super.config().tempCursorSchema(), super.config().serviceAccount()));
            if (connectorContext instanceof TapConnectorContext){
                TapConnectorContext context = (TapConnectorContext)connectorContext;
                stream.stateMap(context.getStateMap()).streamOffset(streamOffset);
            }
            streamMap.put(key,stream);
        }
        return this;
    }
    public WriteCommittedStream writeCommittedStream(){
        return this.stream;
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
        tapTable = table;
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
        KVMap<Object> stateMap = ((TapConnectorContext) this.connectorContext).getStateMap();
        List<Map<String,Object>> list = new ArrayList<>();
        Object mergeKeyId = null;
        for (TapRecordEvent event : events) {
            if (Objects.isNull(event)) continue;
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
            mergeKeyId = record.get(MergeHandel.MERGE_KEY_ID);
            if (Objects.nonNull(record)){
                list.add(record);
            }
        }
        if (Objects.nonNull(mergeKeyId)){
            stateMap.put(MergeHandel.MERGE_KEY_ID,mergeKeyId);
        }
        return list;

//        return events.stream().filter(Objects::nonNull).map(event->{
//            Map<String,Object> record = new HashMap<>();
//            if (event instanceof TapInsertRecordEvent){
//                insert.getAndIncrement();
//                record = ((TapInsertRecordEvent)event).getAfter();
//            }else if(event instanceof TapUpdateRecordEvent){
//                update.getAndIncrement();
//                //record = ((TapUpdateRecordEvent)event).getAfter();
//            }else if(event instanceof TapDeleteRecordEvent){
//                delete.getAndIncrement();
//                //record = ((TapDeleteRecordEvent)event).getBefore();
//            }else {
//
//            }
//            Object mergeKeyId = record.get(MergeHandel.MERGE_KEY_ID);
//            if (Objects.nonNull(mergeKeyId)){
//                stateMap.put(MergeHandel.MERGE_KEY_ID,mergeKeyId);
//            }
//            Map<String,Object> recordMap = new HashMap<>();
//            Map<String, Object> finalRecord = record;
//            nameFieldMap.forEach((key, f)->recordMap.put(key,SqlValueConvert.streamJsonArrayValue(finalRecord.get(key),f)));
//            return record.isEmpty()?null:record;
//        }).collect(Collectors.toList());
    }
}
