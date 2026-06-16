package com.tapdata.tm.task.res;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.task.entity.TaskEntity;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("Class CpuMemoryService Test")
class CpuMemoryServiceTest {

    private CpuMemoryService cpuMemoryService;
    private MongoTemplate mongoOperations;
    private SettingsService settingsService;

    @BeforeEach
    void setUp() {
        mongoOperations = mock(MongoTemplate.class);
        settingsService = mock(SettingsService.class);
        cpuMemoryService = new CpuMemoryService();
        cpuMemoryService.setMongoOperations(mongoOperations);
        cpuMemoryService.setSettingsService(settingsService);
    }

    @Nested
    @DisplayName("Method cpuMemoryUsageOfTask test")
    class cpuMemoryUsageOfTaskTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            List<String> taskIds = Arrays.asList("task1", "task2");
            
            // 构造聚合结果
            List<Map> mappedResults = new ArrayList<>();
            Map<String, Object> result1 = new HashMap<>();
            Map<String, Object> firstRecord1 = new HashMap<>();
            Map<String, Object> tags1 = new HashMap<>();
            tags1.put("taskId", "task1");
            firstRecord1.put("tags", tags1);
            
            List<Map<String, Object>> samples1 = new ArrayList<>();
            Map<String, Object> sample1 = new HashMap<>();
            sample1.put("date", new Date());
            Map<String, Object> vs1 = new HashMap<>();
            vs1.put("cpuUsage", 15.5);
            vs1.put("memoryUsage", 2048L);
            sample1.put("vs", vs1);
            samples1.add(sample1);
            firstRecord1.put("ss", samples1);
            
            result1.put("firstRecord", firstRecord1);
            mappedResults.add(result1);
            
            AggregationResults<Map> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(mappedResults);
            
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class)))
                    .thenReturn(aggregationResults);
            
            Map<String, Map<String, Object>> result = cpuMemoryService.cpuMemoryUsageOfTask(taskIds);
            
            assertNotNull(result);
            assertTrue(result.containsKey("task1"));
            assertEquals(15.5, result.get("task1").get("cpuUsage"));
            assertEquals(2048L, result.get("task1").get("memoryUsage"));
            assertTrue(result.get("task1").containsKey("lastUpdateTime"));
            
            verify(mongoOperations).aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class));
        }

        @Test
        @DisplayName("test with empty results")
        void test2() {
            List<String> taskIds = Arrays.asList("task1");
            
            AggregationResults<Map> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(new ArrayList<>());
            
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class)))
                    .thenReturn(aggregationResults);
            
            Map<String, Map<String, Object>> result = cpuMemoryService.cpuMemoryUsageOfTask(taskIds);
            
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("test with null samples")
        void test3() {
            List<String> taskIds = Arrays.asList("task1");
            
            List<Map> mappedResults = new ArrayList<>();
            Map<String, Object> result1 = new HashMap<>();
            Map<String, Object> firstRecord1 = new HashMap<>();
            Map<String, Object> tags1 = new HashMap<>();
            tags1.put("taskId", "task1");
            firstRecord1.put("tags", tags1);
            firstRecord1.put("ss", null);
            result1.put("firstRecord", firstRecord1);
            mappedResults.add(result1);
            
            AggregationResults<Map> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(mappedResults);
            
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class)))
                    .thenReturn(aggregationResults);
            
            Map<String, Map<String, Object>> result = cpuMemoryService.cpuMemoryUsageOfTask(taskIds);
            
            assertNotNull(result);
            assertTrue(result.containsKey("task1"));
            assertFalse(result.get("task1").containsKey("cpuUsage"));
        }

        @Test
        @DisplayName("test with empty samples")
        void test4() {
            List<String> taskIds = Arrays.asList("task1");
            
            List<Map> mappedResults = new ArrayList<>();
            Map<String, Object> result1 = new HashMap<>();
            Map<String, Object> firstRecord1 = new HashMap<>();
            Map<String, Object> tags1 = new HashMap<>();
            tags1.put("taskId", "task1");
            firstRecord1.put("tags", tags1);
            firstRecord1.put("ss", new ArrayList<>());
            result1.put("firstRecord", firstRecord1);
            mappedResults.add(result1);
            
            AggregationResults<Map> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(mappedResults);
            
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class)))
                    .thenReturn(aggregationResults);
            
            Map<String, Map<String, Object>> result = cpuMemoryService.cpuMemoryUsageOfTask(taskIds);
            
            assertNotNull(result);
            assertTrue(result.containsKey("task1"));
            assertFalse(result.get("task1").containsKey("cpuUsage"));
        }

        @Test
        @DisplayName("test with null date in sample")
        void test5() {
            List<String> taskIds = Arrays.asList("task1");
            
            List<Map> mappedResults = new ArrayList<>();
            Map<String, Object> result1 = new HashMap<>();
            Map<String, Object> firstRecord1 = new HashMap<>();
            Map<String, Object> tags1 = new HashMap<>();
            tags1.put("taskId", "task1");
            firstRecord1.put("tags", tags1);
            
            List<Map<String, Object>> samples1 = new ArrayList<>();
            Map<String, Object> sample1 = new HashMap<>();
            sample1.put("date", null);
            samples1.add(sample1);
            firstRecord1.put("ss", samples1);
            
            result1.put("firstRecord", firstRecord1);
            mappedResults.add(result1);
            
            AggregationResults<Map> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(mappedResults);
            
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class)))
                    .thenReturn(aggregationResults);
            
            Map<String, Map<String, Object>> result = cpuMemoryService.cpuMemoryUsageOfTask(taskIds);
            
            assertNotNull(result);
            assertTrue(result.containsKey("task1"));
            assertFalse(result.get("task1").containsKey("cpuUsage"));
        }

        @Test
        @DisplayName("test with invalid structure")
        void test6() {
            List<String> taskIds = Arrays.asList("task1");
            
            List<Map> mappedResults = new ArrayList<>();
            Map<String, Object> result1 = new HashMap<>();
            result1.put("firstRecord", "invalid");
            mappedResults.add(result1);
            
            AggregationResults<Map> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(mappedResults);
            
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class)))
                    .thenReturn(aggregationResults);
            
            Map<String, Map<String, Object>> result = cpuMemoryService.cpuMemoryUsageOfTask(taskIds);
            
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("test aggregation query structure")
        void test7() {
            List<String> taskIds = Arrays.asList("task1", "task2");
            
            AggregationResults<Map> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(new ArrayList<>());
            
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class)))
                    .thenAnswer(invocation -> {
                        Aggregation aggregation = invocation.getArgument(0);
                        assertNotNull(aggregation);
                        return aggregationResults;
                    });
            
            cpuMemoryService.cpuMemoryUsageOfTask(taskIds);
            
            verify(mongoOperations).aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class));
        }
    }



    @Nested
    class updateTaskCpuMemoryTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            BulkOperations op = mock(BulkOperations.class);
            when(op.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(op.execute()).thenReturn(null);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class)).thenReturn(op);
            Map<String, Map<String, Number>> usageMap = new HashMap<>();
            Map<String, Number> task1Usage = new HashMap<>();
            task1Usage.put("cpuUsage", 15.5);
            task1Usage.put("memoryUsage", 2048L);
            usageMap.put("task1", task1Usage);

            cpuMemoryService.updateTaskCpuMemory(usageMap);

            verify(mongoOperations).bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class);
        }
        void testException() {
            BulkOperations op = mock(BulkOperations.class);
            when(op.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(op.execute()).thenAnswer(w -> {throw new RuntimeException("test");});
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class)).thenReturn(op);
            Map<String, Map<String, Number>> usageMap = new HashMap<>();
            Map<String, Number> task1Usage = new HashMap<>();
            task1Usage.put("cpuUsage", 15.5);
            task1Usage.put("memoryUsage", 2048L);
            usageMap.put("task1", task1Usage);

            cpuMemoryService.updateTaskCpuMemory(usageMap);

            verify(mongoOperations).bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class);
        }

        @Test
        void testEmpty() {
            BulkOperations op = mock(BulkOperations.class);
            when(op.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(op.execute()).thenReturn(null);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class)).thenReturn(op);
            Map<String, Map<String, Number>> usageMap = new HashMap<>();
            cpuMemoryService.updateTaskCpuMemory(usageMap);
            verify(mongoOperations, times(0)).bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class);
        }

        @Test
        void testEmptyInfo() {
            BulkOperations op = mock(BulkOperations.class);
            when(op.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(op.execute()).thenReturn(null);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class)).thenReturn(op);
            Map<String, Map<String, Number>> usageMap = new HashMap<>();
            usageMap.put(new ObjectId().toHexString(), new HashMap<>());
            cpuMemoryService.updateTaskCpuMemory(usageMap);
            verify(mongoOperations, times(1)).bulkOps(BulkOperations.BulkMode.ORDERED, TaskEntity.class);
        }
    }

    @Nested
    @DisplayName("Method hasOpenCpuMemory test")
    class HasOpenCpuMemoryTest {
        @Test
        @DisplayName("should return true when setting value is true")
        void testReturnTrue() {
            Settings settings = new Settings();
            settings.setValue(true);
            when(settingsService.getByKey("cpu_mem_collector")).thenReturn(settings);

            boolean result = cpuMemoryService.hasOpenCpuMemory();

            assertTrue(result);
            verify(settingsService).getByKey("cpu_mem_collector");
        }

        @Test
        @DisplayName("should return false when setting is null")
        void testReturnFalseWhenSettingIsNull() {
            when(settingsService.getByKey("cpu_mem_collector")).thenReturn(null);

            boolean result = cpuMemoryService.hasOpenCpuMemory();

            assertFalse(result);
            verify(settingsService).getByKey("cpu_mem_collector");
        }

        @Test
        @DisplayName("should return false when setting value is false")
        void testReturnFalseWhenSettingValueIsFalse() {
            Settings settings = new Settings();
            settings.setValue(false);
            when(settingsService.getByKey("cpu_mem_collector")).thenReturn(settings);

            boolean result = cpuMemoryService.hasOpenCpuMemory();

            assertFalse(result);
            verify(settingsService).getByKey("cpu_mem_collector");
        }
    }

    @Nested
    @DisplayName("Method ignoreMeasureInfoIfNeed test")
    class IgnoreMeasureInfoIfNeedTest {
        @Test
        @DisplayName("should remove cpu and memory fields when collector is disabled")
        void testRemoveFieldsWhenUsageClosed() {
            Settings settings = new Settings();
            settings.setValue(false);
            when(settingsService.getByKey("cpu_mem_collector")).thenReturn(settings);

            MeasurementQueryParam measurementQueryParam = new MeasurementQueryParam();
            MeasurementQueryParam.MeasurementQuerySample sample = new MeasurementQueryParam.MeasurementQuerySample();
            List<String> fields = new ArrayList<>(Arrays.asList("cpuUsage", "memoryUsage", "qps"));
            sample.setFields(fields);
            sample.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS);
            measurementQueryParam.setSamples(Collections.singletonMap("data", sample));
            Map<String, Object> result = new HashMap<>();

            cpuMemoryService.ignoreMeasureInfoIfNeed(measurementQueryParam, result);

            assertEquals(Boolean.FALSE, result.get("usageOpen"));
            assertEquals(Collections.singletonList("qps"), sample.getFields());
        }

        @Test
        @DisplayName("should keep cpu and memory fields when collector is enabled")
        void testKeepFieldsWhenUsageOpen() {
            Settings settings = new Settings();
            settings.setValue(true);
            when(settingsService.getByKey("cpu_mem_collector")).thenReturn(settings);

            MeasurementQueryParam measurementQueryParam = new MeasurementQueryParam();
            MeasurementQueryParam.MeasurementQuerySample sample = new MeasurementQueryParam.MeasurementQuerySample();
            List<String> fields = new ArrayList<>(Arrays.asList("cpuUsage", "memoryUsage", "qps"));
            sample.setFields(fields);
            sample.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS);
            measurementQueryParam.setSamples(Collections.singletonMap("data", sample));
            Map<String, Object> result = new HashMap<>();

            cpuMemoryService.ignoreMeasureInfoIfNeed(measurementQueryParam, result);

            assertEquals(Boolean.TRUE, result.get("usageOpen"));
            assertEquals(Arrays.asList("cpuUsage", "memoryUsage", "qps"), sample.getFields());
        }

        @Test
        @DisplayName("should ignore non continuous sample")
        void testIgnoreNonContinuousSample() {
            MeasurementQueryParam measurementQueryParam = new MeasurementQueryParam();
            MeasurementQueryParam.MeasurementQuerySample sample = new MeasurementQueryParam.MeasurementQuerySample();
            List<String> fields = new ArrayList<>(Arrays.asList("cpuUsage", "memoryUsage", "qps"));
            sample.setFields(fields);
            sample.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT);
            measurementQueryParam.setSamples(Collections.singletonMap("data", sample));
            Map<String, Object> result = new HashMap<>();

            cpuMemoryService.ignoreMeasureInfoIfNeed(measurementQueryParam, result);

            assertFalse(result.containsKey("usageOpen"));
            assertEquals(Arrays.asList("cpuUsage", "memoryUsage", "qps"), sample.getFields());
            verify(settingsService, never()).getByKey("cpu_mem_collector");
        }
    }
}
