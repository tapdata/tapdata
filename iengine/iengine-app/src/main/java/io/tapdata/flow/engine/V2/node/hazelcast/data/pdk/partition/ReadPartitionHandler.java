package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.async.master.JobContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class ReadPartitionHandler {
	private final PDKSourceContext pdkSourceContext;
	private final TapTable table;
	private final ReadPartition readPartition;
	private final TypeSplitterMap typeSplitterMap;
	private TapStorageFactory storageFactory;
	private String kvStorageId;
	private String sequenceStorageId;
	private String taskId;

	private volatile TapKVStorage kvStorage;
	private volatile TapSequenceStorage sequenceStorage;
	public ReadPartitionHandler(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, TypeSplitterMap typeSplitterMap) {
		this.readPartition = readPartition;
		this.table = tapTable;
		this.pdkSourceContext = pdkSourceContext;
		this.typeSplitterMap = typeSplitterMap;

		this.storageFactory = InstanceFactory.instance(TapStorageFactory.class);
		storageFactory.init(TapStorageFactory.StorageOptions.create().disableJavaSerializable(true).rootPath("./partition_storage"));
		this.taskId = pdkSourceContext.getSourcePdkDataNode().getNode().getTaskId();

		kvStorageId = "stream_" + taskId + "_" + readPartition.getId();
		sequenceStorageId = "batch_" + taskId + "_" + readPartition.getId();
	}


	public JobContext handlePartitionExistOrNot(JobContext jobContext) {
		//TODO check partition id has finished or not from TM.
		if(false)
			return JobContext.create(null).jumpToId("finishedPartition");
		return null;
	}

	public JobContext handleStartCachingStreamData(JobContext jobContext1) {
		if(kvStorage == null) {
			synchronized (this) {
				if(kvStorage == null) {
					kvStorage = storageFactory.getKVStorage(kvStorageId);
				}
			}
		}

		return JobContext.create((Consumer<List<TapEvent>>) tapEvents -> {
			for(TapEvent tapEvent : tapEvents) {
//				kvStorage.put(null, tapEvent);
			}
		});
	}

	public JobContext handleReadPartition(JobContext jobContext) {
		if(sequenceStorage == null) {
			synchronized (this) {
				if(sequenceStorage == null) {
					sequenceStorage = storageFactory.getSequenceStorage(sequenceStorageId);
				}
			}
		}

		return null;
	}

	public JobContext handleSendingDataFromPartition(JobContext jobContext) {
		return null;
	}

	public JobContext handleFinishedPartition(JobContext jobContext) {
		return null;
	}

}
