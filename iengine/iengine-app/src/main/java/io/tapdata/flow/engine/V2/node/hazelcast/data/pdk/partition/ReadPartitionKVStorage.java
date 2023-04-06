package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import io.tapdata.async.master.JobContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.Map;

/**
 * @author GavinXiao
 * @description ReadPartitionKVStorage create by Gavin
 * @create 2023/4/6 15:05
 **/
public interface ReadPartitionKVStorage {

    public static ReadPartitionKVStorage KVStorage(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, HazelcastSourcePartitionReadDataNode sourcePdkDataNode){
        Node<?> node = pdkSourceContext.getSourcePdkDataNode().getProcessorBaseContext().getNode();
        if (node instanceof DatabaseNode) {
            DatabaseNode databaseNode = (DatabaseNode) node;
            ReadPartitionOptions readPartitionOptions = databaseNode.getReadPartitionOptions();
            if (readPartitionOptions.hasKVStorage()) {
                return new ReadPartitionHandler(pdkSourceContext, tapTable, readPartition, sourcePdkDataNode);
            } else {
                return new ReadPartitionUnKVStorageHandler(pdkSourceContext, tapTable, readPartition, sourcePdkDataNode);
            }
        }
        return new ReadPartitionHandler(pdkSourceContext, tapTable, readPartition, sourcePdkDataNode);
    }

    public void writeIntoKVStorage(Map<String, Object> key, Map<String, Object> after, TapRecordEvent recordEvent);

    public void deleteFromKVStorage(Map<String, Object> key);

    public void justDeleteFromKVStorage(Map<String, Object> key);

    public JobContext handleStartCachingStreamData(JobContext jobContext1);

    public JobContext handleReadPartition(JobContext jobContext);

    public JobContext handleSendingDataFromPartition(JobContext jobContext);

    public JobContext handleFinishedPartition(JobContext jobContext);

    public void passThrough(TapEvent event);

    public Map<String, Object> getExistDataFromKVMap(Map<String, Object> key);

    public boolean isFinished();

    public void finish();
}
