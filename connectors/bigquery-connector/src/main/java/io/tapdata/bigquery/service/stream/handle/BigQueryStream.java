package io.tapdata.bigquery.service.stream.handle;

import io.tapdata.bigquery.service.stream.WriteCommittedStream;
import io.tapdata.bigquery.service.bigQuery.BigQueryStart;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.schema.TapTable;
import com.google.protobuf.Descriptors;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.util.*;

public class BigQueryStream extends BigQueryStart {
    public static final String TAG = BigQueryStream.class.getSimpleName();
    public static final Map<String,WriteCommittedStream> streamMap = new ConcurrentHashMap<>();

    private WriteCommittedStream stream;
    private TapTable tapTable;
    private MergeHandel merge ;
    private AtomicLong streamOffset;

    public BigQueryStream streamOffset(AtomicLong streamOffset){
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
        String tableName = super.config.isMixedUpdates()?super.config().tempCursorSchema():tapTable.getName();
        if (Objects.isNull(stream)) {
            String key = super.config().projectId() +
                    "_" + super.config().tableSet() +
                    "_" + tableName +
                    "_" + super.config().serviceAccount();
            stream = Optional.ofNullable(streamMap.get(key))
                    .orElse(WriteCommittedStream.writer(
                            super.config().projectId(),
                            super.config().tableSet(),
                            tableName,
                            super.config().serviceAccount())
                    );
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

    private BigQueryStream(TapConnectionContext connectorContext) {
        super(connectorContext);
    }

    public static BigQueryStream streamWrite(TapConnectorContext context) {
        return new BigQueryStream(context);
    }

    public WriteListResult<TapRecordEvent> writeRecord(List<TapRecordEvent> events, TapTable table) throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        this.tapTable = table;
        if (this.config.isMixedUpdates()){
            //混合模式写入数据
            synchronized (this.merge.mergeLock()){
                this.stream.appendJSON(this.records(this.merge.temporaryEvent(events), result));
            }
        }else {
            //append-only 模式
            this.stream.append(this.records(events, result));
        }
        return result;
    }

    private List<Map<String,Object>> records(List<TapRecordEvent> events,WriteListResult<TapRecordEvent> result){
        int insert = 0,update = 0, delete = 0;
        KVMap<Object> stateMap = ((TapConnectorContext) this.connectorContext).getStateMap();
        List<Map<String,Object>> list = new ArrayList<>();
        Object mergeKeyId = null;
        for (TapRecordEvent event : events) {
            if (Objects.isNull(event)) continue;
            Map<String,Object> record = new HashMap<>();
            if (event instanceof TapInsertRecordEvent){
                insert++;
                record = ((TapInsertRecordEvent)event).getAfter();
            }
            else if(event instanceof TapUpdateRecordEvent){
                update++;
                //record = ((TapUpdateRecordEvent)event).getAfter();
            }
            else if(event instanceof TapDeleteRecordEvent){
                delete++;
                //record = ((TapDeleteRecordEvent)event).getBefore();
            }
            mergeKeyId = record.get(MergeHandel.MERGE_KEY_ID);
            if (!record.isEmpty()){
                list.add(record);
            }
        }
        result.removedCount(delete).modifiedCount(update).insertedCount(insert);
        if (Objects.nonNull(mergeKeyId)){
            stateMap.put(MergeHandel.MERGE_KEY_ID,mergeKeyId);
        }
        return list;
    }

    public void closeStream(){
        Optional.ofNullable(stream).ifPresent(WriteCommittedStream::close);
    }
}
