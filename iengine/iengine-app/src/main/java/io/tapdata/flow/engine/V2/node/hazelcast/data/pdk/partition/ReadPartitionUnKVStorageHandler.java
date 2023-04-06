package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.async.master.JobContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author GavinXiao
 * @description ReadPartitionUnKVStorageHandler create by Gavin
 * @create 2023/4/6 14:25
 **/
public class ReadPartitionUnKVStorageHandler extends PartitionFieldParentHandler implements ReadPartitionKVStorage {
    private final PDKSourceContext pdkSourceContext;
    private final ReadPartition readPartition;
    private final HazelcastSourcePartitionReadDataNode sourcePdkDataNode;
    private final AtomicBoolean finished = new AtomicBoolean(false);


    public ReadPartitionUnKVStorageHandler(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, HazelcastSourcePartitionReadDataNode sourcePdkDataNode) {
        super(tapTable);
        this.readPartition = readPartition;
        this.pdkSourceContext = pdkSourceContext;
        this.sourcePdkDataNode = sourcePdkDataNode;
    }


    public void writeIntoKVStorage(Map<String, Object> key, Map<String, Object> after, TapRecordEvent recordEvent) {

    }


    public void deleteFromKVStorage(Map<String, Object> key) {

    }


    public void justDeleteFromKVStorage(Map<String, Object> key) {

    }


    public JobContext handleStartCachingStreamData(JobContext jobContext1) {
        return jobContext1;
    }


    public JobContext handleReadPartition(JobContext jobContext) {
        return jobContext;
    }


    public JobContext handleSendingDataFromPartition(JobContext jobContext) {
        return jobContext;
    }


    public JobContext handleFinishedPartition(JobContext jobContext) {
        return jobContext;
    }


    public void passThrough(TapEvent event) {

    }


    public Map<String, Object> getExistDataFromKVMap(Map<String, Object> key) {
        return key;
    }


    public boolean isFinished() {
        return finished.get();
    }

    public void finish() {
        finished.set(true);
    }
}
