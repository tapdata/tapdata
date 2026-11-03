package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.dataflow.SyncProgress;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.async.master.AsyncJobCompleted;
import io.tapdata.async.master.ParallelWorker;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.partition.ReadPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionsCompletedRunnableTest {

	@Test
	void testCompletedEventCarriesFinalBatchOffsetSnapshot() {
		String tableId = "testTableId";
		TapTable tapTable = new TapTable(tableId);
		ParallelWorker partitionsReader = mock(ParallelWorker.class);
		AspectManager aspectManager = mock(AspectManager.class);
		BatchReadFuncAspect batchReadFuncAspect = new BatchReadFuncAspect();
		HazelcastSourcePartitionReadDataNode sourceNode = mock(HazelcastSourcePartitionReadDataNode.class);
		AsyncJobCompleted jobCompleted = mock(AsyncJobCompleted.class);

		PartitionTableOffset currentOffset = new PartitionTableOffset()
				.partitions(Collections.singletonList(mock(ReadPartition.class)))
				.completedPartitions(new ConcurrentHashMap<>());
		SyncProgress syncProgress = new SyncProgress();
		syncProgress.setBatchOffsetObj(new ConcurrentHashMap<>(Map.of(tableId, currentOffset)));
		when(sourceNode.getSyncProgress()).thenReturn(syncProgress);
		when(sourceNode.getObsLogger()).thenReturn(mock(ObsLogger.class));
		doAnswer(invocation -> {
			((Runnable) invocation.getArgument(0)).run();
			return null;
		}).when(partitionsReader).finished(any(Runnable.class));

		new PartitionsCompletedRunnable(tapTable, partitionsReader, aspectManager, batchReadFuncAspect,
				currentOffset.getPartitions(), sourceNode, jobCompleted).run();

		ArgumentCaptor<TapdataCompleteTableSnapshotEvent> eventCaptor =
				ArgumentCaptor.forClass(TapdataCompleteTableSnapshotEvent.class);
		verify(sourceNode).enqueue(eventCaptor.capture());
		TapdataCompleteTableSnapshotEvent completeEvent = eventCaptor.getValue();
		PartitionTableOffset persistedOffset = (PartitionTableOffset) completeEvent.getBatchOffset();
		assertTrue(completeEvent.getSyncStage() == SyncStage.INITIAL_SYNC);
		assertNotSame(currentOffset, persistedOffset);
		assertTrue(persistedOffset.getTableCompleted());
		assertNull(persistedOffset.getPartitions());
		assertNull(persistedOffset.getCompletedPartitions());
		verify(sourceNode).handleEnterCDCStage(partitionsReader, tapTable);
		verify(jobCompleted).completed(null, null);
	}
}
