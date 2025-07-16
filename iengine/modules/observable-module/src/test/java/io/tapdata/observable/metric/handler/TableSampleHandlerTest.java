package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.Sampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TableSampleHandlerTest {

    @Mock
    private SampleCollector mockCollector;
    
    @Mock
    private CounterSampler mockSnapshotInsertRowCounter;

    
    private TaskDto taskDto;
    private TableSampleHandler tableSampleHandler;
    private Map<String, Number> retrievedTableValues;

    @BeforeEach
    void setUp() {
        taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setTaskRecordId("test-record-id");
        taskDto.setStartTime(new Date());
        
        retrievedTableValues = new HashMap<>();
        retrievedTableValues.put("snapshotInsertRowTotal", 100L);
    }

    @Nested
    class SnapshotSyncRateCalculationTest {
        void testResult (BigDecimal snapshotSyncRate){
            doAnswer(invocation -> {
                String name = invocation.getArgument(0);
                Sampler sampler = invocation.getArgument(1);
                if ("snapshotRowTotal".equals(name)) {
                    assertEquals(1000L, sampler.value());
                }
                return null;
            }).when(mockCollector).addSampler(eq("snapshotRowTotal"), any(Sampler.class));

            doAnswer(invocation -> {
                String name = invocation.getArgument(0);
                Sampler sampler = invocation.getArgument(1);
                if ("snapshotSyncRate".equals(name)) {
                    assertEquals(snapshotSyncRate, sampler.value());
                }
                return null;
            }).when(mockCollector).addSampler(eq("snapshotSyncRate"), any(Sampler.class));
        }

        @BeforeEach
        void setUp() {
            Long snapshotRowTotal = 1000L;
            BigDecimal snapshotSyncRate = new BigDecimal("0.00");
            tableSampleHandler = new TableSampleHandler(taskDto, "test_table", snapshotRowTotal,
                    retrievedTableValues, snapshotSyncRate);
            ReflectionTestUtils.setField(tableSampleHandler, "collector", mockCollector);
        }


        @Test
        void testSnapshotSyncRate_snapshotInsertRowCounterIsNull() {
            when(mockCollector.getCounterSampler(eq("snapshotInsertRowTotal"), anyLong()))
                    .thenReturn(mockSnapshotInsertRowCounter);
            BigDecimal expectedSyncRate = new BigDecimal("0.00");
            testResult(expectedSyncRate);
            tableSampleHandler.doInit(retrievedTableValues);
        }

        @Test
        void testSnapshotSyncRate_snapshotInsertRowCounterIsProgress() {
            when(mockCollector.getCounterSampler(eq("snapshotInsertRowTotal"), anyLong()))
                    .thenReturn(mockSnapshotInsertRowCounter);
            when(mockSnapshotInsertRowCounter.value()).thenReturn(100L);
            BigDecimal expectedSyncRate = new BigDecimal("0.10");
            testResult(expectedSyncRate);
            tableSampleHandler.doInit(retrievedTableValues);

        }

        @Test
        void testSnapshotSyncRate_snapshotInsertRowCounterEqualSnapshotInsertRowTotal() {
            when(mockCollector.getCounterSampler(eq("snapshotInsertRowTotal"), anyLong()))
                    .thenReturn(mockSnapshotInsertRowCounter);
            when(mockSnapshotInsertRowCounter.value()).thenReturn(1000L);
            BigDecimal expectedSyncRate = new BigDecimal("0.99");
            testResult(expectedSyncRate);
            tableSampleHandler.doInit(retrievedTableValues);
        }

        @Test
        void testSnapshotSyncRate_isSnapshotDone() {
            when(mockCollector.getCounterSampler(eq("snapshotInsertRowTotal"), anyLong()))
                    .thenReturn(mockSnapshotInsertRowCounter);
            when(mockSnapshotInsertRowCounter.value()).thenReturn(1000L);
            BigDecimal expectedSyncRate = new BigDecimal("1");
            tableSampleHandler.setSnapshotDone();
            testResult(expectedSyncRate);
            tableSampleHandler.doInit(retrievedTableValues);
        }


    }

}
