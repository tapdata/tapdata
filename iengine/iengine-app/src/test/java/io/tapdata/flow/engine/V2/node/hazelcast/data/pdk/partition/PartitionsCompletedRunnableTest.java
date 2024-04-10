package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.DataFunctionAspect;
import io.tapdata.async.master.AsyncJobCompleted;
import io.tapdata.async.master.ParallelWorker;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.partition.ReadPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

class PartitionsCompletedRunnableTest {
    PartitionsCompletedRunnable instance;

    TapTable tapTable;
    ParallelWorker partitionsReader;
    AspectManager aspectManager;
    BatchReadFuncAspect batchReadFuncAspect;
    List<ReadPartition> readPartitionList;
    HazelcastSourcePartitionReadDataNode sourcePdkDataNodeEx1;
    AsyncJobCompleted jobCompleted;

    ObsLogger obsLogger;

    @BeforeEach
    void init() {
        instance = mock(PartitionsCompletedRunnable.class);

        tapTable = mock(TapTable.class);
        ReflectionTestUtils.setField(instance, "tapTable", tapTable);
        partitionsReader = mock(ParallelWorker.class);
        ReflectionTestUtils.setField(instance, "partitionsReader", partitionsReader);
        aspectManager = mock(AspectManager.class);
        ReflectionTestUtils.setField(instance, "aspectManager", aspectManager);
        batchReadFuncAspect = mock(BatchReadFuncAspect.class);
        ReflectionTestUtils.setField(instance, "batchReadFuncAspect", batchReadFuncAspect);
        readPartitionList = mock(List.class);
        ReflectionTestUtils.setField(instance, "readPartitionList", readPartitionList);
        sourcePdkDataNodeEx1 = mock(HazelcastSourcePartitionReadDataNode.class);
        ReflectionTestUtils.setField(instance, "sourcePdkDataNodeEx1", sourcePdkDataNodeEx1);
        jobCompleted = mock(AsyncJobCompleted.class);
        ReflectionTestUtils.setField(instance, "jobCompleted", jobCompleted);

        obsLogger = mock(ObsLogger.class);
    }

    @Test
    void testCreateObject() {
        Assertions.assertDoesNotThrow(() -> new PartitionsCompletedRunnable(tapTable, partitionsReader, aspectManager, batchReadFuncAspect, readPartitionList, sourcePdkDataNodeEx1, jobCompleted));
    }

    @Nested
    @DisplayName("method run test")
    class RunTest {
        String tableId;
        int size;
        SyncProgress syncProgress;
        Map<String, Object> batchOffsetObj;
        PartitionTableOffset partitionTableOffset;
        @BeforeEach
        void init() {
            tableId = "id";
            size = 0;
            when(tapTable.getId()).thenReturn(tableId);
            when(sourcePdkDataNodeEx1.getObsLogger()).thenReturn(obsLogger);
            when(readPartitionList.size()).thenReturn(size);
            when(readPartitionList.toString()).thenReturn("[]");
            doNothing().when(obsLogger).info("Partitions has been split for table {}, wait until all partitions has been read. readPartition size {} list {}", tableId, size, readPartitionList);
            syncProgress = mock(SyncProgress.class);
            when(sourcePdkDataNodeEx1.getSyncProgress()).thenReturn(syncProgress);
            batchOffsetObj = mock(Map.class);
            when(syncProgress.getBatchOffsetObj()).thenReturn(batchOffsetObj);
            partitionTableOffset = mock(PartitionTableOffset.class);
            when(syncProgress.getBatchOffsetOfTable(tableId)).thenReturn(partitionTableOffset);

            doNothing().when(syncProgress).updateBatchOffset(anyString(), any(PartitionTableOffset.class), anyString());
            when(partitionTableOffset.partitions(readPartitionList)).thenReturn(partitionTableOffset);
            doNothing().when(partitionsReader).finished(any(Runnable.class));

            doCallRealMethod().when(instance).run();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(instance::run);
            verify(tapTable, times(1)).getId();
            verify(sourcePdkDataNodeEx1, times(1)).getObsLogger();
            verify(readPartitionList, times(1)).size();
            verify(obsLogger, times(1)).info("Partitions has been split for table {}, wait until all partitions has been read. readPartition size {} list {}", tableId, size, readPartitionList);
            verify(sourcePdkDataNodeEx1, times(1)).getSyncProgress();
            verify(syncProgress, times(1)).getBatchOffsetObj();
            verify(syncProgress, times(1)).getBatchOffsetOfTable(tableId);
            verify(syncProgress, times(0)).updateBatchOffset(anyString(), any(PartitionTableOffset.class), anyString());
            verify(partitionTableOffset, times(1)).partitions(readPartitionList);
            verify(partitionsReader, times(1)).finished(any(Runnable.class));
        }

        @Test
        void testBatchOffsetObjNotMap() {
            when(syncProgress.getBatchOffsetObj()).thenReturn(0L);
            Assertions.assertDoesNotThrow(instance::run);
            verify(tapTable, times(1)).getId();
            verify(sourcePdkDataNodeEx1, times(1)).getObsLogger();
            verify(readPartitionList, times(1)).size();
            verify(obsLogger, times(1)).info("Partitions has been split for table {}, wait until all partitions has been read. readPartition size {} list {}", tableId, size, readPartitionList);
            verify(sourcePdkDataNodeEx1, times(1)).getSyncProgress();
            verify(syncProgress, times(1)).getBatchOffsetObj();
            verify(syncProgress, times(0)).getBatchOffsetOfTable(tableId);
            verify(syncProgress, times(0)).updateBatchOffset(anyString(), any(PartitionTableOffset.class), anyString());
            verify(partitionTableOffset, times(0)).partitions(readPartitionList);
            verify(partitionsReader, times(1)).finished(any(Runnable.class));
        }
        @Test
        void testPartitionTableOffsetIsNull() {
            when(syncProgress.getBatchOffsetOfTable(tableId)).thenReturn(null);
            Assertions.assertDoesNotThrow(instance::run);
            verify(tapTable, times(1)).getId();
            verify(sourcePdkDataNodeEx1, times(1)).getObsLogger();
            verify(readPartitionList, times(1)).size();
            verify(obsLogger, times(1)).info("Partitions has been split for table {}, wait until all partitions has been read. readPartition size {} list {}", tableId, size, readPartitionList);
            verify(sourcePdkDataNodeEx1, times(1)).getSyncProgress();
            verify(syncProgress, times(1)).getBatchOffsetObj();
            verify(syncProgress, times(1)).getBatchOffsetOfTable(tableId);
            verify(syncProgress, times(1)).updateBatchOffset(anyString(), any(PartitionTableOffset.class), anyString());
            verify(partitionTableOffset, times(0)).partitions(readPartitionList);
            verify(partitionsReader, times(1)).finished(any(Runnable.class));
        }
    }

    @Nested
    @DisplayName("method handleStateChanged test")
    class HandleStateChangedTest {
        String tableId;
        SyncProgress syncProgress;
        PartitionTableOffset partitionTableOffset;
        @BeforeEach
        void init() {
            tableId = "id";
            syncProgress = mock(SyncProgress.class);
            partitionTableOffset = mock(PartitionTableOffset.class);
            when(tapTable.getId()).thenReturn(tableId);
            when(sourcePdkDataNodeEx1.getSyncProgress()).thenReturn(syncProgress);
            when(syncProgress.getBatchOffsetOfTable(tableId)).thenReturn(partitionTableOffset);

            doNothing().when(partitionTableOffset).setTableCompleted(null);
            doNothing().when(partitionTableOffset).setPartitions(null);
            doNothing().when(partitionTableOffset).setCompletedPartitions(null);

            when(batchReadFuncAspect.state(DataFunctionAspect.STATE_END)).thenReturn(mock(BatchReadFuncAspect.class));
            when(aspectManager.executeAspect(any(BatchReadFuncAspect.class))).thenReturn(mock(AspectInterceptResult.class));
            doNothing().when(sourcePdkDataNodeEx1).enqueue(any(TapdataCompleteTableSnapshotEvent.class));
            doNothing().when(sourcePdkDataNodeEx1).handleEnterCDCStage(partitionsReader, tapTable);
            doNothing().when(jobCompleted).completed(null, null);

            doCallRealMethod().when(instance).handleStateChanged();
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(instance::handleStateChanged);
            verify(tapTable, times(1)).getId();
            verify(sourcePdkDataNodeEx1, times(1)).getSyncProgress();
            verify(syncProgress, times(1)).getBatchOffsetOfTable(tableId);

            verify(partitionTableOffset, times(1)).setTableCompleted(true);
            verify(partitionTableOffset, times(1)).setPartitions(null);
            verify(partitionTableOffset, times(1)).setCompletedPartitions(null);

            verify(batchReadFuncAspect, times(1)).state(DataFunctionAspect.STATE_END);
            verify(aspectManager, times(1)).executeAspect(any(BatchReadFuncAspect.class));
            verify(sourcePdkDataNodeEx1, times(1)).enqueue(any(TapdataCompleteTableSnapshotEvent.class));
            verify(sourcePdkDataNodeEx1, times(1)).handleEnterCDCStage(partitionsReader, tapTable);
            verify(jobCompleted, times(1)).completed(null, null);
        }
        @Test
        void testPartitionTableOffsetIsNull() {
            when(syncProgress.getBatchOffsetOfTable(tableId)).thenReturn(null);

            Assertions.assertDoesNotThrow(instance::handleStateChanged);
            verify(tapTable, times(1)).getId();
            verify(sourcePdkDataNodeEx1, times(1)).getSyncProgress();
            verify(syncProgress, times(1)).getBatchOffsetOfTable(tableId);

            verify(partitionTableOffset, times(0)).setTableCompleted(true);
            verify(partitionTableOffset, times(0)).setPartitions(null);
            verify(partitionTableOffset, times(0)).setCompletedPartitions(null);

            verify(batchReadFuncAspect, times(1)).state(DataFunctionAspect.STATE_END);
            verify(aspectManager, times(1)).executeAspect(any(BatchReadFuncAspect.class));
            verify(sourcePdkDataNodeEx1, times(1)).enqueue(any(TapdataCompleteTableSnapshotEvent.class));
            verify(sourcePdkDataNodeEx1, times(1)).handleEnterCDCStage(partitionsReader, tapTable);
            verify(jobCompleted, times(1)).completed(null, null);
        }
    }
}