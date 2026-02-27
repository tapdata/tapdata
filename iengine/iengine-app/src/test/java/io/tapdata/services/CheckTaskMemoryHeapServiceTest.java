package io.tapdata.services;

import com.tapdata.tm.commons.task.dto.CheckTaskMemoryParam;
import com.tapdata.tm.commons.task.dto.CheckTaskMemoryResult;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.service.PdkCountEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CheckTaskMemoryHeapServiceTest {

    CheckTaskMemoryHeapService service;

    @BeforeEach
    void setUp() {
        service = new CheckTaskMemoryHeapService();
    }

    @Nested
    @DisplayName("checkTaskMemoryHeap 方法测试")
    class CheckTaskMemoryHeapTest {

        @Test
        @DisplayName("参数为 null 时返回 null")
        void testNullParams() {
            assertNull(service.checkTaskMemoryHeap(null));
        }

        @Test
        @DisplayName("参数为空列表时返回 null")
        void testEmptyParams() {
            assertNull(service.checkTaskMemoryHeap(Collections.emptyList()));
        }

        @Test
        @DisplayName("列表中全部为 null 元素时返回 null")
        void testAllNullElements() {
            List<CheckTaskMemoryParam> params = new ArrayList<>();
            params.add(null);
            params.add(null);
            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class)) {
                mockHeapMemory(mf, 1024 * 1024 * 100L, 1024 * 1024 * 50L);
                mockCommonUtils(cu);
                assertNull(service.checkTaskMemoryHeap(params));
            }
        }

        @Test
        @DisplayName("param 的 tableMap 为空时返回 null")
        void testEmptyTableMap() {
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100)
                    .nodeSize(2)
                    .writeBatchSize(50)
                    .connectionId("conn1")
                    .tableMap(Collections.emptyMap())
                    .build();
            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class)) {
                mockHeapMemory(mf, 1024 * 1024 * 100L, 1024 * 1024 * 50L);
                mockCommonUtils(cu);
                assertNull(service.checkTaskMemoryHeap(Collections.singletonList(param)));
            }
        }

        @Test
        @DisplayName("PdkCountService 返回 code!=200 时抛出异常")
        void testPdkCountServiceReturnError() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("table1", 512L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100)
                    .nodeSize(2)
                    .writeBatchSize(50)
                    .connectionId("conn1")
                    .tableMap(tableMap)
                    .build();

            PdkCountEntity errorEntity = new PdkCountEntity();
            errorEntity.failed(new RuntimeException("permission denied"));

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString())).thenReturn(errorEntity))) {
                mockHeapMemory(mf, 1024 * 1024 * 100L, 1024 * 1024 * 50L);
                mockCommonUtils(cu);
                assertThrows(RuntimeException.class, () -> service.checkTaskMemoryHeap(Collections.singletonList(param)));
            }
        }

        @Test
        @DisplayName("PdkCountService 返回 rowCount=0 时返回 null")
        void testRowCountZero() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("table1", 512L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100)
                    .nodeSize(2)
                    .writeBatchSize(50)
                    .connectionId("conn1")
                    .tableMap(tableMap)
                    .build();

            PdkCountEntity entity = new PdkCountEntity().success(0L, 256L);

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString())).thenReturn(entity))) {
                mockHeapMemory(mf, 1024 * 1024 * 100L, 1024 * 1024 * 50L);
                mockCommonUtils(cu);
                assertNull(service.checkTaskMemoryHeap(Collections.singletonList(param)));
            }
        }

        @Test
        @DisplayName("正常场景：单表返回 CheckTaskMemoryResult")
        void testNormalSingleTable() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("table1", 512L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100)
                    .nodeSize(2)
                    .writeBatchSize(50)
                    .connectionId("conn1")
                    .tableMap(tableMap)
                    .build();

            PdkCountEntity entity = new PdkCountEntity().success(1000L, 256L);

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString())).thenReturn(entity))) {
                mockHeapMemory(mf, 1024L * 1024 * 1024, 1024L * 1024 * 100);
                mockCommonUtils(cu);
                CheckTaskMemoryResult result = service.checkTaskMemoryHeap(Collections.singletonList(param));
                assertNotNull(result);
                assertNotNull(result.getIsSafe());
                assertEquals("table1", result.getTableName());
                assertEquals(100L, result.getBatchSize());
            }
        }

        @Test
        @DisplayName("正常场景：多表选择最大风险表")
        void testMultipleTablesSelectMaxRisk() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("small_table", 100L);
            tableMap.put("big_table", 2048L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100)
                    .nodeSize(2)
                    .writeBatchSize(50)
                    .connectionId("conn1")
                    .tableMap(tableMap)
                    .build();

            PdkCountEntity smallEntity = new PdkCountEntity().success(500L, 100L);
            PdkCountEntity bigEntity = new PdkCountEntity().success(5000L, 2048L);

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString()))
                                 .thenAnswer(invocation -> {
                                     String tableName = invocation.getArgument(1);
                                     if ("big_table".equals(tableName)) return bigEntity;
                                     return smallEntity;
                                 }))) {
                mockHeapMemory(mf, 1024L * 1024 * 1024, 1024L * 1024 * 100);
                mockCommonUtils(cu);
                CheckTaskMemoryResult result = service.checkTaskMemoryHeap(Collections.singletonList(param));
                assertNotNull(result);
                assertEquals("big_table", result.getTableName());
            }
        }

        @Test
        @DisplayName("多个 CheckTaskMemoryParam 累加 totalEstimatedWithOverhead")
        void testMultipleParams() {
            Map<String, Long> tableMap1 = new HashMap<>();
            tableMap1.put("t1", 512L);
            CheckTaskMemoryParam param1 = CheckTaskMemoryParam.builder()
                    .batchSize(100).nodeSize(2).writeBatchSize(50)
                    .connectionId("conn1").tableMap(tableMap1).build();

            Map<String, Long> tableMap2 = new HashMap<>();
            tableMap2.put("t2", 1024L);
            CheckTaskMemoryParam param2 = CheckTaskMemoryParam.builder()
                    .batchSize(200).nodeSize(3).writeBatchSize(100)
                    .connectionId("conn2").tableMap(tableMap2).build();

            PdkCountEntity entity1 = new PdkCountEntity().success(1000L, 512L);
            PdkCountEntity entity2 = new PdkCountEntity().success(2000L, 1024L);

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString()))
                                 .thenAnswer(invocation -> {
                                     String connId = invocation.getArgument(0);
                                     if ("conn2".equals(connId)) return entity2;
                                     return entity1;
                                 }))) {
                mockHeapMemory(mf, 1024L * 1024 * 1024, 1024L * 1024 * 100);
                mockCommonUtils(cu);
                List<CheckTaskMemoryParam> params = Arrays.asList(param1, param2);
                CheckTaskMemoryResult result = service.checkTaskMemoryHeap(params);
                assertNotNull(result);
                assertNotNull(result.getEstimatedWithOverhead());
            }
        }

        @Test
        @DisplayName("tableMap 中 key 为空字符串时跳过该表")
        void testBlankTableNameSkipped() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("", 512L);
            tableMap.put("valid_table", 256L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100).nodeSize(2).writeBatchSize(50)
                    .connectionId("conn1").tableMap(tableMap).build();

            PdkCountEntity entity = new PdkCountEntity().success(1000L, 256L);

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString())).thenReturn(entity))) {
                mockHeapMemory(mf, 1024L * 1024 * 1024, 1024L * 1024 * 100);
                mockCommonUtils(cu);
                CheckTaskMemoryResult result = service.checkTaskMemoryHeap(Collections.singletonList(param));
                assertNotNull(result);
                assertEquals("valid_table", result.getTableName());
            }
        }

        @Test
        @DisplayName("avgObjSize 为 null 时使用 tableMap 中的 fallback 值")
        void testFallbackAvgSize() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("table1", 1024L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100).nodeSize(2).writeBatchSize(50)
                    .connectionId("conn1").tableMap(tableMap).build();

            PdkCountEntity entity = new PdkCountEntity().success(1000L, null);

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString())).thenReturn(entity))) {
                mockHeapMemory(mf, 1024L * 1024 * 1024, 1024L * 1024 * 100);
                mockCommonUtils(cu);
                CheckTaskMemoryResult result = service.checkTaskMemoryHeap(Collections.singletonList(param));
                assertNotNull(result);
                assertEquals("table1", result.getTableName());
            }
        }

        @Test
        @DisplayName("avgObjSize 和 fallback 都为 0 时跳过该表返回 null")
        void testAvgObjSizeAndFallbackBothZero() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("table1", 0L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(100).nodeSize(2).writeBatchSize(50)
                    .connectionId("conn1").tableMap(tableMap).build();

            PdkCountEntity entity = new PdkCountEntity().success(1000L, 0L);

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class,
                         (mock, context) -> when(mock.count(anyString(), anyString(), anyString())).thenReturn(entity))) {
                mockHeapMemory(mf, 1024L * 1024 * 1024, 1024L * 1024 * 100);
                mockCommonUtils(cu);
                assertNull(service.checkTaskMemoryHeap(Collections.singletonList(param)));
            }
        }

        @Test
        @DisplayName("inFlightUpper 为 0 时返回 null")
        void testInFlightUpperZero() {
            Map<String, Long> tableMap = new HashMap<>();
            tableMap.put("table1", 512L);
            CheckTaskMemoryParam param = CheckTaskMemoryParam.builder()
                    .batchSize(0).nodeSize(0).writeBatchSize(0)
                    .connectionId("conn1").tableMap(tableMap).build();

            try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class);
                 MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class);
                 MockedConstruction<PdkCountService> mc = mockConstruction(PdkCountService.class)) {
                mockHeapMemory(mf, 1024L * 1024 * 1024, 1024L * 1024 * 100);
                mockCommonUtils(cu);
                assertNull(service.checkTaskMemoryHeap(Collections.singletonList(param)));
            }
        }

        private void mockHeapMemory(MockedStatic<ManagementFactory> mf, long maxHeap, long usedHeap) {
            MemoryMXBean memoryMXBean = mock(MemoryMXBean.class);
            MemoryUsage heapUsage = new MemoryUsage(0, usedHeap, maxHeap, maxHeap);
            when(memoryMXBean.getHeapMemoryUsage()).thenReturn(heapUsage);
            mf.when(ManagementFactory::getMemoryMXBean).thenReturn(memoryMXBean);
        }

        private void mockCommonUtils(MockedStatic<CommonUtils> cu) {
            cu.when(() -> CommonUtils.getProperty("HEAP_UTILIZATION_LIMIT")).thenReturn(null);
            cu.when(() -> CommonUtils.getPropertyLong(eq("JVM_BLOAT_FACTOR"), anyLong())).thenReturn(5L);
            cu.when(() -> CommonUtils.getProperty("QUEUE_OVERHEAD_FACTOR")).thenReturn(null);
            cu.when(() -> CommonUtils.getPropertyInt(anyString(), anyInt())).thenReturn(128);
        }
    }
}
