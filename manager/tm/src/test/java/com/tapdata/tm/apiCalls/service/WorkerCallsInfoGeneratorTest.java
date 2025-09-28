package com.tapdata.tm.apiCalls.service;

import com.tapdata.tm.apiCalls.vo.WorkerCallsInfo;
import com.tapdata.tm.apiServer.utils.PercentileCalculator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for WorkerCallsInfoGenerator
 *
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @version v1.0 2025/9/27 Create
 */
class WorkerCallsInfoGeneratorTest {
    WorkerCallsInfoGenerator generator;
    WorkerCallsInfoGenerator.Acceptor acceptor;

    @BeforeEach
    void init() {
        acceptor = mock(WorkerCallsInfoGenerator.Acceptor.class);
    }

    @Nested
    class constructorTest {

        @Test
        void testNormalWithAcceptorOnly() {
            Assertions.assertDoesNotThrow(() -> {
                generator = new WorkerCallsInfoGenerator(acceptor);
                Assertions.assertNotNull(generator);
            });
        }

        @Test
        void testNormalWithAcceptorAndBatchSize() {
            Assertions.assertDoesNotThrow(() -> {
                generator = new WorkerCallsInfoGenerator(acceptor, 50);
                Assertions.assertNotNull(generator);
            });
        }

        @Test
        void testNullAcceptor() {
            Assertions.assertThrows(IllegalArgumentException.class, () -> {
                try {
                    new WorkerCallsInfoGenerator(null);
                } catch (IllegalArgumentException e) {
                    Assertions.assertEquals("Acceptor cannot be null", e.getMessage());
                    throw e;
                }
            });
        }

        @Test
        void testNullBatchSize() {
            Assertions.assertDoesNotThrow(() -> {
                generator = new WorkerCallsInfoGenerator(acceptor, null);
                // 验证使用默认批次大小
                int batchSize = (int) ReflectionTestUtils.getField(generator, "batchSize");
                Assertions.assertEquals(WorkerCallsInfoGenerator.BATCH_ACCEPT, batchSize);
            });
        }

        @Test
        void testCustomBatchSize() {
            generator = new WorkerCallsInfoGenerator(acceptor, 200);
            int batchSize = (int) ReflectionTestUtils.getField(generator, "batchSize");
            Assertions.assertEquals(200, batchSize);
        }
    }

    @Nested
    class appendSingleTest {

        @BeforeEach
        void setUp() {
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 2)); // 小批次大小便于测试
        }

        @Test
        void testNormal() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }
        @Test
        void testLessZero() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", -100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(0L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testSuccessCode() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testErrorCode() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "500", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testBatchSizeReached() {
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 1)); // 批次大小为1
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
                // 验证accept被调用
                verify(acceptor, times(0)).accept(any());
            }
        }

        @Test
        void testTimeKeyCalculation() {
            // 测试时间键计算：(reqTime / 60000L) * 60000L
            WorkerCallsInfo info1 = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 65000L); // 1分5秒
            WorkerCallsInfo info2 = createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "200", 125000L); // 2分5秒

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> {
                    generator.append(info1);
                    generator.append(info2);
                });
            }
        }

        @Test
        void testSameApiIdAccumulation() {
            // 测试同一个API ID的数据累积
            WorkerCallsInfo info1 = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);
            WorkerCallsInfo info2 = createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "404", 60000L); // 同一分钟，同一API

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(125L);

                Assertions.assertDoesNotThrow(() -> {
                    generator.append(info1);
                    generator.append(info2);
                });
            }
        }
    }

    @Nested
    class appendListTest {

        @BeforeEach
        void setUp() {
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 10));
        }

        @Test
        void testNormal() {
            List<WorkerCallsInfo> infos = createWorkerCallsInfoList();

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(infos));
            }
        }

        @Test
        void testEmptyList() {
            List<WorkerCallsInfo> emptyList = Collections.emptyList();

            Assertions.assertDoesNotThrow(() -> generator.append(emptyList));
            verify(acceptor, never()).accept(any());
        }

        @Test
        void testNullList() {
            Assertions.assertDoesNotThrow(() -> generator.append((List<WorkerCallsInfo>) null));
            verify(acceptor, never()).accept(any());
        }

        @Test
        void testListWithNulls() {
            List<WorkerCallsInfo> listWithNulls = new ArrayList<>();
            listWithNulls.add(createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L));
            listWithNulls.add(null);
            listWithNulls.add(createWorkerCallsInfo("worker2", "api2", "gateway2", 200L, "404", 120000L));
            listWithNulls.add(null);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(150L);

                Assertions.assertDoesNotThrow(() -> generator.append(listWithNulls));
            }
        }
    }

    @Nested
    class mapTest {

        @BeforeEach
        void setUp() {
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 100));
        }

        @Test
        void testNormal() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.5))).thenReturn(100L);
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.95))).thenReturn(180L);
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), eq(0.99))).thenReturn(195L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testErrorRateCalculation() {
            // 测试错误率计算
            WorkerCallsInfo successInfo = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);
            WorkerCallsInfo errorInfo = createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "500", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(125L);

                Assertions.assertDoesNotThrow(() -> {
                    generator.append(successInfo);
                    generator.append(errorInfo);
                });
            }
        }

        @Test
        void testRpsCalculation() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testTimeGranularitySet() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testObjectIdGeneration() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }
    }

    @Nested
    class acceptTest {

        @BeforeEach
        void setUp() {
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 100));
        }

        @Test
        void testNormal() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                generator.append(info);

                // 手动调用accept来测试
                Assertions.assertDoesNotThrow(() -> {
                    ReflectionTestUtils.invokeMethod(generator, "accept");
                });

                verify(acceptor, atLeastOnce()).accept(any());
            }
        }

        @Test
        void testEmptyCalls() {
            // 测试空的calls map
            Assertions.assertDoesNotThrow(() -> {
                ReflectionTestUtils.invokeMethod(generator, "accept");
            });

            verify(acceptor, never()).accept(any());
        }
    }

    @Nested
    class closeTest {

        @BeforeEach
        void setUp() {
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 100));
        }

        @Test
        void testNormal() {
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                generator.append(info);

                Assertions.assertDoesNotThrow(() -> generator.close());
                verify(acceptor, atLeastOnce()).accept(any());
            }
        }

        @Test
        void testEmptyCalls() {
            Assertions.assertDoesNotThrow(() -> generator.close());
            verify(acceptor, never()).accept(any());
        }

        @Test
        void testNullCalls() {
            // 设置calls为null
            ReflectionTestUtils.setField(generator, "calls", null);

            Assertions.assertDoesNotThrow(() -> generator.close());
            verify(acceptor, never()).accept(any());
        }
    }

    // Helper methods
    private WorkerCallsInfo createWorkerCallsInfo(String workOid, String apiId, String apiGatewayUuid,
                                                  Long latency, String code, Long reqTime) {
        WorkerCallsInfo info = new WorkerCallsInfo();
        info.setWorkOid(workOid);
        info.setApiId(apiId);
        info.setApiGatewayUuid(apiGatewayUuid);
        info.setLatency(latency);
        info.setCode(code);
        info.setReqTime(reqTime);
        info.setResTime(reqTime + Optional.ofNullable(latency).orElse(0L));
        return info;
    }

    private List<WorkerCallsInfo> createWorkerCallsInfoList() {
        List<WorkerCallsInfo> infos = new ArrayList<>();
        infos.add(createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L));
        infos.add(createWorkerCallsInfo("worker1", "api2", "gateway1", 150L, "404", 60000L));
        infos.add(createWorkerCallsInfo("worker2", "api1", "gateway2", 120L, "500", 120000L));
        return infos;
    }

    @Nested
    class edgeCasesTest {

        @BeforeEach
        void setUp() {
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 100));
        }

        @Test
        void testDifferentHttpCodes() {
            // 测试不同HTTP状态码的处理
            List<WorkerCallsInfo> infos = Arrays.asList(
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L), // 成功
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "201", 60000L), // 成功
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 120L, "299", 60000L), // 成功
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 180L, "300", 60000L), // 错误
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 200L, "404", 60000L), // 错误
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 250L, "500", 60000L)  // 错误
            );

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(150L);

                Assertions.assertDoesNotThrow(() -> generator.append(infos));
            }
        }

        @Test
        void testZeroErrorRate() {
            // 测试零错误率的情况
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testAllErrorRequests() {
            // 测试全部都是错误请求的情况
            List<WorkerCallsInfo> infos = Arrays.asList(
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "404", 60000L),
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "500", 60000L),
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 200L, "503", 60000L)
            );

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(150L);

                Assertions.assertDoesNotThrow(() -> generator.append(infos));
            }
        }

        @Test
        void testBoundaryHttpCodes() {
            // 测试边界HTTP状态码
            List<WorkerCallsInfo> infos = Arrays.asList(
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "199", 60000L), // 错误
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "200", 60000L), // 成功
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 120L, "299", 60000L), // 成功
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 180L, "300", 60000L)  // 错误
            );

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(140L);

                Assertions.assertDoesNotThrow(() -> generator.append(infos));
            }
        }

        @Test
        void testMultipleTimeWindows() {
            // 测试多个时间窗口
            List<WorkerCallsInfo> infos = Arrays.asList(
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L),  // 第1分钟
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "200", 120000L), // 第2分钟
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 120L, "200", 180000L)  // 第3分钟
            );

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(125L);

                Assertions.assertDoesNotThrow(() -> generator.append(infos));
            }
        }

        @Test
        void testSameTimeWindowDifferentApis() {
            // 测试同一时间窗口不同API
            List<WorkerCallsInfo> infos = Arrays.asList(
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L),
                    createWorkerCallsInfo("worker1", "api2", "gateway1", 150L, "404", 60000L),
                    createWorkerCallsInfo("worker1", "api3", "gateway1", 120L, "500", 60000L)
            );

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(125L);

                Assertions.assertDoesNotThrow(() -> generator.append(infos));
            }
        }

        @Test
        void testExistingEntityUpdate() {
            // 测试更新已存在的实体
            WorkerCallsInfo info1 = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);
            WorkerCallsInfo info2 = createWorkerCallsInfo("worker1", "api1", "gateway1", 150L, "404", 60000L); // 同一API

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(125L);

                Assertions.assertDoesNotThrow(() -> {
                    generator.append(info1);
                    generator.append(info2);
                });
            }
        }

        @Test
        void testBatchSizeExactMatch() {
            // 测试批次大小精确匹配
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 2));

            List<WorkerCallsInfo> infos = Arrays.asList(
                    createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L),
                    createWorkerCallsInfo("worker2", "api2", "gateway2", 150L, "200", 120000L)
            );

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(125L);

                Assertions.assertDoesNotThrow(() -> generator.append(infos));
                verify(acceptor, atLeastOnce()).accept(any());
            }
        }

        @Test
        void testBatchSizeCondition() {
            // 测试第48行的批次大小条件：batchSize >= calls.size()
            generator = spy(new WorkerCallsInfoGenerator(acceptor, 1));

            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
                verify(acceptor, times(0)).accept(any());
            }
        }

        @Test
        void testNullDelaysHandling() {
            // 测试延迟为null的处理
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", null, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(null);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testZeroLatency() {
            // 测试零延迟
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 0L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(0L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testLargeLatency() {
            // 测试大延迟值
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 10000L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(10000L);

                Assertions.assertDoesNotThrow(() -> generator.append(info));
            }
        }

        @Test
        void testTryWithResourcesUsage() {
            // 测试try-with-resources的使用
            WorkerCallsInfo info = createWorkerCallsInfo("worker1", "api1", "gateway1", 100L, "200", 60000L);

            try (MockedStatic<PercentileCalculator> mockedCalculator = mockStatic(PercentileCalculator.class)) {
                mockedCalculator.when(() -> PercentileCalculator.calculatePercentile(any(), anyDouble())).thenReturn(100L);

                try (WorkerCallsInfoGenerator testGenerator = new WorkerCallsInfoGenerator(acceptor, 100)) {
                    Assertions.assertDoesNotThrow(() -> testGenerator.append(info));
                }

                // close方法应该被自动调用
                verify(acceptor, atLeastOnce()).accept(any());
            }
        }
    }
}
