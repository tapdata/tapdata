package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import io.tapdata.async.master.JobContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
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
public interface ReadPartitionHandler {

	public static ReadPartitionHandler createReadPartitionHandler(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, HazelcastSourcePartitionReadDataNode sourcePdkDataNode) {
		Node<?> node = pdkSourceContext.getSourcePdkDataNode().getProcessorBaseContext().getNode();
		if (node instanceof DataParentNode) {
			DataParentNode<?> databaseNode = (DataParentNode<?>) node;
			ReadPartitionOptions readPartitionOptions = databaseNode.getReadPartitionOptions();
			if (readPartitionOptions.hasKVStorage()) {
				return new ReadPartitionKVStorageHandler(pdkSourceContext, tapTable, readPartition, sourcePdkDataNode);
			} else {
				return new ReadPartitionUnKVStorageHandler(pdkSourceContext, tapTable, readPartition, sourcePdkDataNode);
			}
		}
		return new ReadPartitionUnKVStorageHandler(pdkSourceContext, tapTable, readPartition, sourcePdkDataNode);
	}

	public default void writeIntoKVStorage(Map<String, Object> key, Map<String, Object> after, TapRecordEvent recordEvent) {

	}

	public default void deleteFromKVStorage(Map<String, Object> key) {

	}

	public default void justDeleteFromKVStorage(Map<String, Object> key) {

	}

	public default JobContext handleStartCachingStreamData(JobContext jobContext1) {
		return jobContext1;
	}

	public JobContext handleReadPartition(JobContext jobContext);

	public default JobContext handleSendingDataFromPartition(JobContext jobContext) {
		return null;
	}

	public JobContext handleFinishedPartition(JobContext jobContext);

	public void passThrough(TapEvent event);

	public default Map<String, Object> getExistDataFromKVMap(Map<String, Object> key) {
		return null;
	}

	public boolean isFinished();

	public void finish();


	public void handleUpdateRecordEvent(TapUpdateRecordEvent updateRecordEvent, Map<String, Object> after, Map<String, Object> key);

	public void handleInsertRecordEvent(TapInsertRecordEvent insertRecordEvent, Map<String, Object> after, Map<String, Object> key);

	public void handleDeleteRecordEvent(TapDeleteRecordEvent deleteRecordEvent, Map<String, Object> key);

	public void deleteFromPartition(TapDeleteRecordEvent deleteRecordEvent, Map<String, Object> key);
}
