package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.Sampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.entity.CountResult;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
            tableSampleHandler = new TableSampleHandler(taskDto, "test_table", new CountResult(snapshotRowTotal,false),
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

    @Nested
    @DisplayName("Constructor Test")
    class ConstructorTest {

        @Test
        @DisplayName("test constructor with normal values")
        void testConstructorWithNormalValues() {
           
            String tableName = "test_table";
            Long count = 1000L;
            Boolean done = false;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = new BigDecimal("0.50");

            
            TableSampleHandler handler = new TableSampleHandler(taskDto, tableName, countResult, retrievedTableValues, inputSyncRate);

            
            assertEquals(tableName, ReflectionTestUtils.getField(handler, "table"));
            assertEquals(count, ReflectionTestUtils.getField(handler, "snapshotRowTotal"));
            assertEquals(inputSyncRate, ReflectionTestUtils.getField(handler, "snapshotSyncRate"));
            assertEquals(retrievedTableValues, ReflectionTestUtils.getField(handler, "retrievedTableValues"));
            assertFalse((Boolean) ReflectionTestUtils.getField(handler, "isSnapshotDone"));
            assertEquals(taskDto, ReflectionTestUtils.getField(handler, "task"));
        }

        @Test
        @DisplayName("test constructor when count is zero")
        void testConstructorWhenCountIsZero() {
           
            String tableName = "empty_table";
            Long count = 0L;
            Boolean done = false;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = new BigDecimal("0.30");

            
            TableSampleHandler handler = new TableSampleHandler(taskDto, tableName, countResult, retrievedTableValues, inputSyncRate);

            
            assertEquals(tableName, ReflectionTestUtils.getField(handler, "table"));
            assertEquals(count, ReflectionTestUtils.getField(handler, "snapshotRowTotal"));
            assertEquals(BigDecimal.ONE, ReflectionTestUtils.getField(handler, "snapshotSyncRate"));
            assertTrue((Boolean) ReflectionTestUtils.getField(handler, "isSnapshotDone"));
        }

        @Test
        @DisplayName("test constructor when done is true")
        void testConstructorWhenDoneIsTrue() {
           
            String tableName = "completed_table";
            Long count = 500L;
            Boolean done = true;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = new BigDecimal("0.75");

            
            TableSampleHandler handler = new TableSampleHandler(taskDto, tableName, countResult, retrievedTableValues, inputSyncRate);

            
            assertEquals(tableName, ReflectionTestUtils.getField(handler, "table"));
            assertEquals(count, ReflectionTestUtils.getField(handler, "snapshotRowTotal"));
            assertEquals(BigDecimal.ONE, ReflectionTestUtils.getField(handler, "snapshotSyncRate"));
            assertTrue((Boolean) ReflectionTestUtils.getField(handler, "isSnapshotDone"));
        }

        @Test
        @DisplayName("test constructor when both count is zero and done is true")
        void testConstructorWhenCountIsZeroAndDoneIsTrue() {
           
            String tableName = "zero_done_table";
            Long count = 0L;
            Boolean done = true;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = new BigDecimal("0.25");

            
            TableSampleHandler handler = new TableSampleHandler(taskDto, tableName, countResult, retrievedTableValues, inputSyncRate);

            
            assertEquals(tableName, ReflectionTestUtils.getField(handler, "table"));
            assertEquals(count, ReflectionTestUtils.getField(handler, "snapshotRowTotal"));
            assertEquals(BigDecimal.ONE, ReflectionTestUtils.getField(handler, "snapshotSyncRate"));
            assertTrue((Boolean) ReflectionTestUtils.getField(handler, "isSnapshotDone"));
        }


        @Test
        @DisplayName("test constructor with large count value")
        void testConstructorWithLargeCountValue() {
           
            String tableName = "large_table";
            Long count = Long.MAX_VALUE;
            Boolean done = false;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = new BigDecimal("0.01");

            
            TableSampleHandler handler = new TableSampleHandler(taskDto, tableName, countResult, retrievedTableValues, inputSyncRate);

            
            assertEquals(tableName, ReflectionTestUtils.getField(handler, "table"));
            assertEquals(count, ReflectionTestUtils.getField(handler, "snapshotRowTotal"));
            assertEquals(inputSyncRate, ReflectionTestUtils.getField(handler, "snapshotSyncRate"));
            assertFalse((Boolean) ReflectionTestUtils.getField(handler, "isSnapshotDone"));
        }

        @Test
        @DisplayName("test constructor with null snapshotSyncRate")
        void testConstructorWithNullSnapshotSyncRate() {
           
            String tableName = "null_rate_table";
            Long count = 300L;
            Boolean done = false;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = null;

            
            TableSampleHandler handler = new TableSampleHandler(taskDto, tableName, countResult, retrievedTableValues, inputSyncRate);

            
            assertEquals(tableName, ReflectionTestUtils.getField(handler, "table"));
            assertEquals(count, ReflectionTestUtils.getField(handler, "snapshotRowTotal"));
            assertNull(ReflectionTestUtils.getField(handler, "snapshotSyncRate"));
            assertFalse((Boolean) ReflectionTestUtils.getField(handler, "isSnapshotDone"));
        }

        @Test
        @DisplayName("test constructor with null retrievedTableValues should throw exception")
        void testConstructorWithNullRetrievedTableValues() {
           
            String tableName = "test_table";
            Long count = 100L;
            Boolean done = false;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = new BigDecimal("0.50");
            Map<String, Number> nullRetrievedTableValues = null;

            assertThrows(NullPointerException.class, () -> {
                new TableSampleHandler(taskDto, tableName, countResult, nullRetrievedTableValues, inputSyncRate);
            });
        }



        @Test
        @DisplayName("test constructor with empty retrievedTableValues")
        void testConstructorWithEmptyRetrievedTableValues() {
           
            String tableName = "test_table";
            Long count = 100L;
            Boolean done = false;
            CountResult countResult = new CountResult(count, done);
            BigDecimal inputSyncRate = new BigDecimal("0.50");
            Map<String, Number> emptyRetrievedTableValues = new HashMap<>();

            
            TableSampleHandler handler = new TableSampleHandler(taskDto, tableName, countResult, emptyRetrievedTableValues, inputSyncRate);

            
            assertEquals(tableName, ReflectionTestUtils.getField(handler, "table"));
            assertEquals(count, ReflectionTestUtils.getField(handler, "snapshotRowTotal"));
            assertEquals(inputSyncRate, ReflectionTestUtils.getField(handler, "snapshotSyncRate"));
            assertEquals(emptyRetrievedTableValues, ReflectionTestUtils.getField(handler, "retrievedTableValues"));
            assertFalse((Boolean) ReflectionTestUtils.getField(handler, "isSnapshotDone"));
        }
    }

}
