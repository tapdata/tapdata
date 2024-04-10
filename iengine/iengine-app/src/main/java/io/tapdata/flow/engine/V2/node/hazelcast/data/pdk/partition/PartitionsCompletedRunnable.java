package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.DataFunctionAspect;
import io.tapdata.async.master.AsyncJobCompleted;
import io.tapdata.async.master.ParallelWorker;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class PartitionsCompletedRunnable implements Runnable {
	private final TapTable tapTable;
	private final ParallelWorker partitionsReader;
	private final AspectManager aspectManager;
	private final BatchReadFuncAspect batchReadFuncAspect;
	private final List<ReadPartition> readPartitionList;
	private final HazelcastSourcePartitionReadDataNode sourcePdkDataNodeEx1;
	private final AsyncJobCompleted jobCompleted;

	public PartitionsCompletedRunnable(TapTable tapTable, ParallelWorker partitionsReader, AspectManager aspectManager, BatchReadFuncAspect batchReadFuncAspect, List<ReadPartition> readPartitionList, HazelcastSourcePartitionReadDataNode sourcePdkDataNodeEx1, AsyncJobCompleted jobCompleted) {
		this.tapTable = tapTable;
		this.partitionsReader = partitionsReader;
		this.aspectManager = aspectManager;
		this.batchReadFuncAspect = batchReadFuncAspect;
		this.readPartitionList = readPartitionList;
		this.sourcePdkDataNodeEx1 = sourcePdkDataNodeEx1;
		this.jobCompleted = jobCompleted;
	}

	@Override
	public void run() {
		String tableId = tapTable.getId();
		ObsLogger obsLogger = sourcePdkDataNodeEx1.getObsLogger();
		obsLogger.info("Partitions has been split for table {}, wait until all partitions has been read. readPartition size {} list {}", tableId, readPartitionList.size(), readPartitionList);
		SyncProgress syncProgress = sourcePdkDataNodeEx1.getSyncProgress();
		Object batchOffsetObj = syncProgress.getBatchOffsetObj();
		if (batchOffsetObj instanceof Map) {
			PartitionTableOffset partitionTableOffset = (PartitionTableOffset) syncProgress.getBatchOffsetOfTable(tableId);
			if (partitionTableOffset == null) {
				partitionTableOffset = new PartitionTableOffset();
				syncProgress.updateBatchOffset(tableId, partitionTableOffset, SyncProgress.RUNNING);
			}
			partitionTableOffset.partitions(readPartitionList);
		}
		partitionsReader.finished(this::handleStateChanged);
	}

	protected void handleStateChanged() {
		String tableId = tapTable.getId();
		PartitionTableOffset partitionTableOffset = (PartitionTableOffset) sourcePdkDataNodeEx1.getSyncProgress().getBatchOffsetOfTable(tableId);

		if (partitionTableOffset != null) {
			partitionTableOffset.setTableCompleted(true);
			partitionTableOffset.setPartitions(null);
			partitionTableOffset.setCompletedPartitions(null);
		}

		aspectManager.executeAspect(batchReadFuncAspect.state(DataFunctionAspect.STATE_END));
		sourcePdkDataNodeEx1.enqueue(new TapdataCompleteTableSnapshotEvent(tableId));
		//partition split done and read partitions done, start entering CDC stage.
		sourcePdkDataNodeEx1.handleEnterCDCStage(partitionsReader, tapTable);
		jobCompleted.completed(null, null);
	}
}
