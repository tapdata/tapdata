package com.tapdata.tm.task.res;

import com.tapdata.tm.monitor.entity.MeasurementEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("Class CpuMemoryService Test")
class CpuMemoryServiceTest {

    private CpuMemoryService cpuMemoryService;
    private MongoTemplate mongoOperations;

    @BeforeEach
    void setUp() {
        mongoOperations = mock(MongoTemplate.class);
        cpuMemoryService = new CpuMemoryService();
        cpuMemoryService.setMongoOperations(mongoOperations);
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
}