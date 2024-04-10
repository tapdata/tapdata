package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;


import com.tapdata.entity.dataflow.SyncProgress;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.partition.ReadPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionFieldParentHandlerTest {
    PartitionFieldParentHandler instance;
    ObsLogger obsLogger;
    @BeforeEach
    void init() {
        instance = mock(PartitionFieldParentHandler.class);
        instance.table = "id";

        obsLogger = mock(ObsLogger.class);
    }

    @Nested
    class HandleFinishedPartitionTest {
        HazelcastSourcePartitionReadDataNode sourcePdkDataNode;
        ReadPartition readPartition;
        LongAdder sentEventCount;
        SyncProgress syncProgress;
        PartitionTableOffset batchOffsetOfTable;
        Map<String, Long> completedPartitions;

        @BeforeEach
        void init() {
            sourcePdkDataNode = mock(HazelcastSourcePartitionReadDataNode.class);
            readPartition = mock(ReadPartition.class);
            sentEventCount = mock(LongAdder.class);
            syncProgress = mock(SyncProgress.class);

            when(sourcePdkDataNode.getObsLogger()).thenReturn(obsLogger);
            when(sourcePdkDataNode.getSyncProgress()).thenReturn(syncProgress);

            when(obsLogger.isDebugEnabled()).thenReturn(true);
            doNothing().when(obsLogger).debug("Failed to handle finished partition, SyncProgress not fund");

            batchOffsetOfTable = mock(PartitionTableOffset.class);
            when(syncProgress.getBatchOffsetOfTable("id")).thenReturn(batchOffsetOfTable);

            doNothing().when(syncProgress).updateBatchOffset(anyString(), any(PartitionTableOffset.class), anyString());
            completedPartitions = mock(Map.class);
            when(batchOffsetOfTable.getCompletedPartitions()).thenReturn(completedPartitions);

            doNothing().when(batchOffsetOfTable).setCompletedPartitions(any(ConcurrentHashMap.class));

            when(readPartition.getId()).thenReturn("id");
            when(sentEventCount.longValue()).thenReturn(0L);
            when(completedPartitions.put("id", 0L)).thenReturn(0L);

            when(completedPartitions.size()).thenReturn(1);
            doNothing().when(obsLogger).info("Finished partition {} completed partitions {}", readPartition, 1);

        }
        @Test
        void testNormal() {
            doCallRealMethod().when(instance).handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount);
            Assertions.assertDoesNotThrow(() -> instance.handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount));
            assertVerify(new VerifyEntity()
                    .getObsLogger(1)
                    .getSyncProgress(1)
                    .isDebugEnabled(0)
                    .debug(0)
                    .getBatchOffsetOfTable(1)
                    .updateBatchOffset(0)
                    .getCompletedPartitions(1)
                    .setCompletedPartitions(0)
                    .getId(1)
                    .longValue(1)
                    .put(1)
                    .size(1)
                    .info(1)
            );
        }
        void assertVerify(VerifyEntity entity) {
            verify(sourcePdkDataNode, times(entity.getObsLogger)).getObsLogger();
            verify(sourcePdkDataNode, times(entity.getSyncProgress)).getSyncProgress();
            verify(obsLogger, times(entity.isDebugEnabled)).isDebugEnabled();
            verify(obsLogger, times(entity.debug)).debug("Failed to handle finished partition, SyncProgress not fund");
            verify(syncProgress, times(entity.getBatchOffsetOfTable)).getBatchOffsetOfTable("id");
            verify(syncProgress, times(entity.updateBatchOffset)).updateBatchOffset(anyString(), any(PartitionTableOffset.class), anyString());
            verify(batchOffsetOfTable, times(entity.getCompletedPartitions)).getCompletedPartitions();
            verify(batchOffsetOfTable, times(entity.setCompletedPartitions)).setCompletedPartitions(any(ConcurrentHashMap.class));
            verify(readPartition, times(entity.getId)).getId();
            verify(sentEventCount, times(entity.longValue)).longValue();
            verify(completedPartitions, times(entity.put)).put("id", 0L);
            verify(completedPartitions, times(entity.size)).size();
            verify(obsLogger, times(entity.info)).info("Finished partition {} completed partitions {}", readPartition, 1);
        }
        @Test
        void testBatchOffsetOfTableNotPartitionTableOffset() {
            when(syncProgress.getBatchOffsetOfTable("id")).thenReturn(100L);
            doCallRealMethod().when(instance).handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount);
            Assertions.assertDoesNotThrow(() -> instance.handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount));
            assertVerify(new VerifyEntity()
                    .getObsLogger(1)
                    .getSyncProgress(1)
                    .getBatchOffsetOfTable(1)
            );
        }

        @Test
        void testBatchOffsetOfTableIsNull() {
            when(syncProgress.getBatchOffsetOfTable("id")).thenReturn(null);
            doCallRealMethod().when(instance).handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount);
            Assertions.assertDoesNotThrow(() -> instance.handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount));
            assertVerify(new VerifyEntity()
                    .getObsLogger(1)
                    .getSyncProgress(1)
                    .getBatchOffsetOfTable(1)
                    .updateBatchOffset(1)
                    .getId(1)
                    .longValue(1)
                    .info(1)
            );
        }

        @Test
        void testSyncProgressIsNull() {
            when(sourcePdkDataNode.getSyncProgress()).thenReturn(null);
            doCallRealMethod().when(instance).handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount);
            Assertions.assertDoesNotThrow(() -> instance.handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount));
            assertVerify(new VerifyEntity()
                    .getObsLogger(1)
                    .getSyncProgress(1)
                    .isDebugEnabled(1)
                    .debug(1)
            );
        }

        @Test
        void testSyncProgressIsNullButNotSupportDebug() {
            when(obsLogger.isDebugEnabled()).thenReturn(false);
            when(sourcePdkDataNode.getSyncProgress()).thenReturn(null);
            doCallRealMethod().when(instance).handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount);
            Assertions.assertDoesNotThrow(() -> instance.handleFinishedPartition(sourcePdkDataNode, readPartition, sentEventCount));
            assertVerify(new VerifyEntity()
                    .getObsLogger(1)
                    .getSyncProgress(1)
                    .isDebugEnabled(1)
            );
        }

        @Test
        void testSourcePdkDataNodeIsNull() {
            doCallRealMethod().when(instance).handleFinishedPartition(null, readPartition, sentEventCount);
            Assertions.assertDoesNotThrow(() -> instance.handleFinishedPartition(null, readPartition, sentEventCount));
            assertVerify(new VerifyEntity());
        }

        class VerifyEntity {
            int getObsLogger;
            int getSyncProgress;
            int isDebugEnabled;
            int debug;
            int getBatchOffsetOfTable;
            int updateBatchOffset;
            int getCompletedPartitions;
            int setCompletedPartitions;
            int getId;
            int longValue;
            int put;
            int size;
            int info;
            public VerifyEntity getObsLogger(int times) {
                this.getObsLogger = times;
                return this;
            }
            public VerifyEntity getSyncProgress(int times) {
                this.getSyncProgress = times;
                return this;
            }
            public VerifyEntity isDebugEnabled(int times) {
                this.isDebugEnabled = times;
                return this;
            }
            public VerifyEntity debug(int times) {
                this.debug = times;
                return this;
            }
            public VerifyEntity updateBatchOffset(int times) {
                this.updateBatchOffset = times;
                return this;
            }
            public VerifyEntity getBatchOffsetOfTable(int times) {
                this.getBatchOffsetOfTable = times;
                return this;
            }
            public VerifyEntity setCompletedPartitions(int times) {
                this.setCompletedPartitions = times;
                return this;
            }
            public VerifyEntity getId(int times) {
                this.getId = times;
                return this;
            }
            public VerifyEntity longValue(int times) {
                this.longValue = times;
                return this;
            }
            public VerifyEntity put(int times) {
                this.put = times;
                return this;
            }
            public VerifyEntity size(int times) {
                this.size = times;
                return this;
            }
            public VerifyEntity getCompletedPartitions(int times) {
                this.getCompletedPartitions = times;
                return this;
            }
            public VerifyEntity info(int times) {
                this.info = times;
                return this;
            }
        }
    }
}