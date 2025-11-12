package io.tapdata.threadgroup;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.Usage;
import io.tapdata.pdk.core.executor.ThreadFactory;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.openjdk.jol.info.GraphLayout;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("Class CpuMemoryCollector Test")
class CpuMemoryCollectorTest {

    private CpuMemoryCollector collector;
    private TaskDto taskDto;
    private ThreadFactory threadFactory;

    @BeforeEach
    void setUp() {
        collector = CpuMemoryCollector.COLLECTOR;
        collector.taskWithNode.clear();
        collector.taskDtoMap.clear();
        collector.weakReferenceMap.clear();
        collector.threadGroupMap.clear();
        
        taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        threadFactory = mock(ThreadFactory.class);
        when(threadFactory.getThreadGroup()).thenReturn(new ThreadGroup("test"));
    }

    @Nested
    @DisplayName("Method startTask test")
    class startTaskTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            CpuMemoryCollector.startTask(taskDto);
            
            assertTrue(collector.taskDtoMap.containsKey(taskDto.getId().toHexString()));
            assertEquals(taskDto, collector.taskDtoMap.get(taskDto.getId().toHexString()).get());
        }
    }

    @Nested
    @DisplayName("Method addNode test")
    class addNodeTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            String taskId = "task1";
            String nodeId = "node1";
            
            CpuMemoryCollector.addNode(taskId, nodeId);
            
            assertEquals(taskId, collector.taskWithNode.get(nodeId));
        }

        @Test
        @DisplayName("test when nodeId is blank")
        void test2() {
            CpuMemoryCollector.addNode("task1", "");
            CpuMemoryCollector.addNode("task1", null);
            
            assertTrue(collector.taskWithNode.isEmpty());
        }

        @Test
        @DisplayName("test when taskId is blank")
        void test3() {
            CpuMemoryCollector.addNode("", "node1");
            CpuMemoryCollector.addNode(null, "node1");
            
            assertTrue(collector.taskWithNode.isEmpty());
        }
    }

    @Nested
    @DisplayName("Method registerTask test")
    class registerTaskTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            String nodeId = "node1";
            String taskId = "task1";
            collector.taskWithNode.put(nodeId, taskId);
            
            CpuMemoryCollector.registerTask(nodeId, threadFactory);
            
            assertTrue(collector.threadGroupMap.containsKey(taskId));
            assertEquals(1, collector.threadGroupMap.get(taskId).size());
            assertEquals(threadFactory, collector.threadGroupMap.get(taskId).get(0).get());
        }

        @Test
        @DisplayName("test when threadGroup is null")
        void test2() {
            CpuMemoryCollector.registerTask("node1", null);
            
            assertTrue(collector.threadGroupMap.isEmpty());
        }

        @Test
        @DisplayName("test when taskId not found")
        void test3() {
            CpuMemoryCollector.registerTask("node1", threadFactory);
            
            assertTrue(collector.threadGroupMap.isEmpty());
        }

        @Test
        @DisplayName("test duplicate registration")
        void test4() {
            String nodeId = "node1";
            String taskId = "task1";
            collector.taskWithNode.put(nodeId, taskId);
            
            CpuMemoryCollector.registerTask(nodeId, threadFactory);
            CpuMemoryCollector.registerTask(nodeId, threadFactory);
            
            assertEquals(1, collector.threadGroupMap.get(taskId).size());
        }
    }

    @Nested
    @DisplayName("Method unregisterTask test")
    class unregisterTaskTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            String taskId = "task1";
            String nodeId = "node1";
            
            collector.taskWithNode.put(nodeId, taskId);
            collector.taskWithNode.put("nodeId2", "taskId2");
            collector.threadGroupMap.put(taskId, new ArrayList<>());
            collector.weakReferenceMap.put(taskId, new ArrayList<>());
            collector.taskDtoMap.put(taskId, new WeakReference<>(taskDto));
            collector.taskDtoMap.put("taskId2", new WeakReference<>(taskDto));
            
            CpuMemoryCollector.unregisterTask(taskId);
            
            assertFalse(collector.threadGroupMap.containsKey(taskId));
            assertFalse(collector.weakReferenceMap.containsKey(taskId));
            assertFalse(collector.taskDtoMap.containsKey(taskId));
            assertFalse(collector.taskWithNode.containsKey(nodeId));
        }
        @Test
        @DisplayName("test main process")
        void test2() {
            String taskId = "task1";
            String nodeId = "node1";
            List<WeakReference<Object>> objects = new ArrayList<>();
            objects.add(new WeakReference<>(new Object()));
            collector.weakReferenceMap.put(taskId, objects);

            collector.taskWithNode.put(nodeId, taskId);
            collector.taskWithNode.put("nodeId2", "taskId2");
            collector.threadGroupMap.put(taskId, new ArrayList<>());
            collector.taskDtoMap.put(taskId, new WeakReference<>(taskDto));
            collector.taskDtoMap.put("taskId2", new WeakReference<>(taskDto));

            CpuMemoryCollector.unregisterTask(taskId);

            assertFalse(collector.threadGroupMap.containsKey(taskId));
            assertFalse(collector.weakReferenceMap.containsKey(taskId));
            assertFalse(collector.taskDtoMap.containsKey(taskId));
            assertFalse(collector.taskWithNode.containsKey(nodeId));
        }
    }

    @Nested
    @DisplayName("Method listening test")
    class listeningTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            String nodeId = "node1";
            String taskId = "task1";
            Object info = new Object();
            
            collector.taskWithNode.put(nodeId, taskId);
            
            CpuMemoryCollector.listening(nodeId, info);
            
            assertTrue(collector.weakReferenceMap.containsKey(taskId));
            assertEquals(1, collector.weakReferenceMap.get(taskId).size());
            assertEquals(info, collector.weakReferenceMap.get(taskId).get(0).get());
        }

        @Test
        @DisplayName("test when taskId not found")
        void test2() {
            CpuMemoryCollector.listening("node1", new Object());
            
            assertTrue(collector.weakReferenceMap.isEmpty());
        }
    }

    @Nested
    @DisplayName("Method collectOnce test")
    class collectOnceTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            List<String> taskIds = List.of("task1");
            
            Map<String, Usage> result = CpuMemoryCollector.collectOnce(taskIds);
            
            assertNotNull(result);
        }

        @Test
        @DisplayName("test with null taskIds")
        void test2() {
            Map<String, Usage> result = CpuMemoryCollector.collectOnce(null);
            
            assertNotNull(result);
        }
    }

    @Nested
    @DisplayName("Method eachTaskOnce test")
    class eachTaskOnceTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            List<WeakReference<Object>> weakReferences = new ArrayList<>();
            List<WeakReference<Object>> remove = new ArrayList<>();
            Usage usage = new Usage();
            Object testObj = new Object();
            
            weakReferences.add(new WeakReference<>(testObj));
            
            collector.eachTaskOnce(weakReferences, remove, usage);
            
            assertFalse(usage.getHeapMemoryUsage() > 0);
        }

        @Test
        @DisplayName("test with null reference")
        void test2() {
            List<WeakReference<Object>> weakReferences = new ArrayList<>();
            List<WeakReference<Object>> remove = new ArrayList<>();
            Usage usage = new Usage();
            
            weakReferences.add(new WeakReference<>(null));
            
            collector.eachTaskOnce(weakReferences, remove, usage);
            
            assertEquals(1, remove.size());
        }

        @Test
        @DisplayName("test with null reference")
        void test3() {
            try (MockedStatic<GraphLayout> s = mockStatic(GraphLayout.class)) {
                s.when(() -> GraphLayout.parseInstance(any())).thenAnswer(a -> {throw new RuntimeException("test");});
                List<WeakReference<Object>> weakReferences = new ArrayList<>();
                List<WeakReference<Object>> remove = new ArrayList<>();
                Usage usage = new Usage();

                weakReferences.add(new WeakReference<>(null));

                collector.eachTaskOnce(weakReferences, remove, usage);

                assertEquals(1, remove.size());
            }
        }

        @Test
        @DisplayName("test with null reference")
        void test4() {
            List<WeakReference<Object>> weakReferences = new ArrayList<>();
            List<WeakReference<Object>> remove = new ArrayList<>();
            Usage usage = new Usage();
            WeakReference<Object> ref = new WeakReference<>(new Object());
            weakReferences.add(ref);
            collector.eachTaskOnce(weakReferences, remove, usage);
            assertNotEquals(0, remove.size());
            System.gc();
            collector.eachTaskOnce(weakReferences, remove, usage);
            assertNotEquals(1, remove.size());
        }
    }

    @Nested
    @DisplayName("Method collectMemoryUsage test")
    class collectMemoryUsageTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            String taskId = "task1";
            collector.taskDtoMap.put(taskId, new WeakReference<>(taskDto));
            collector.weakReferenceMap.put(taskId, new ArrayList<>());
            
            Map<String, Usage> usageMap = new java.util.HashMap<>();
            collector.collectMemoryUsage(List.of(taskId), usageMap);
            
            assertFalse(usageMap.containsKey(taskId));
        }
    }

    @Nested
    @DisplayName("Method collectCpuUsage test")
    class collectCpuUsageTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            String taskId = "task1";
            List<WeakReference<ThreadFactory>> threadFactories = new ArrayList<>();
            threadFactories.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, threadFactories);
            
            Map<String, Usage> usageMap = new java.util.HashMap<>();
            collector.collectCpuUsage(List.of(taskId), usageMap);
            
            assertTrue(usageMap.containsKey(taskId));
        }
        @Test
        @DisplayName("test main process")
        void test2() {
            String taskId = "task1";
            List<WeakReference<ThreadFactory>> threadFactories = new ArrayList<>();
            threadFactories.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, threadFactories);

            Map<String, Usage> usageMap = new java.util.HashMap<>();
            collector.collectCpuUsage(List.of(), usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }
        @Test
        @DisplayName("test main process")
        void test3() {
            String taskId = "task1";
            List<WeakReference<ThreadFactory>> threadFactories = new ArrayList<>();
            threadFactories.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, threadFactories);

            Map<String, Usage> usageMap = new java.util.HashMap<>();
            collector.collectCpuUsage(List.of("xxId"), usageMap);

            assertFalse(usageMap.containsKey(taskId));
        }
    }

    @Nested
    @DisplayName("Method ignore test")
    class ignoreTest {
        @Test
        @DisplayName("test normal execution")
        void test1() {
            assertDoesNotThrow(() -> collector.ignore(() -> {}, "test"));
        }

        @Test
        @DisplayName("test exception handling")
        void test2() {
            assertDoesNotThrow(() -> collector.ignore(() -> {
                throw new RuntimeException("test");
            }, "test"));
        }
    }

    @Nested
    @DisplayName("Method eachThreadGroup test")
    class eachThreadGroupTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            LongConsumer consumer = mock(LongConsumer.class);

            ThreadGroup threadGroup = new ThreadGroup("test");
            Thread thread = new Thread(threadGroup, "testThread");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);
            weakReferences.add(new WeakReference<>(threadFactory));

            collector.eachThreadGroup(weakReferences, useless, consumer);

            verify(consumer, times(0)).accept(anyLong());
        }

        @Test
        @DisplayName("test with null threadGroup")
        void test2() {
            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            LongConsumer consumer = mock(LongConsumer.class);

            when(threadFactory.getThreadGroup()).thenReturn(null);
            weakReferences.add(new WeakReference<>(threadFactory));

            collector.eachThreadGroup(weakReferences, useless, consumer);

            assertEquals(0, useless.size());
        }

        @Test
        @DisplayName("test with null reference")
        void test3() {
            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            LongConsumer consumer = mock(LongConsumer.class);

            weakReferences.add(new WeakReference<>(null));

            collector.eachThreadGroup(weakReferences, useless, consumer);

            assertEquals(1, useless.size());
        }
    }

    @Nested
    @DisplayName("Method eachOneTask test")
    class eachOneTaskTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            String taskId = "task1";
            List<WeakReference<ThreadFactory>> threadFactories = new ArrayList<>();
            threadFactories.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, threadFactories);
            Usage usage = new Usage();

            collector.eachOneTask(taskId, usage);

            assertTrue(usage.getCpuUsage() >= 0d);
        }

        @Test
        @DisplayName("test with null threadFactories")
        void test2() {
            String taskId = "task1";
            Usage usage = new Usage();

            collector.eachOneTask(taskId, usage);

            assertFalse(collector.threadGroupMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test with empty threadFactories after cleanup")
        void test3() {
            String taskId = "task1";
            List<WeakReference<ThreadFactory>> threadFactories = new ArrayList<>();
            threadFactories.add(new WeakReference<>(null));
            collector.threadGroupMap.put(taskId, threadFactories);
            Usage usage = new Usage();

            collector.eachOneTask(taskId, usage);

            assertFalse(collector.threadGroupMap.containsKey(taskId));
        }
    }

    @Nested
    @DisplayName("Method asyncCollect test")
    class asyncCollectTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            Consumer<List<CompletableFuture<Void>>> consumer = futures -> {
                futures.add(CompletableFuture.completedFuture(null));
            };

            assertDoesNotThrow(() -> CpuMemoryCollector.asyncCollect(consumer));
        }

        @Test
        @DisplayName("test with exception in future")
        void test2() {
            Consumer<List<CompletableFuture<Void>>> consumer = futures -> {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(new RuntimeException("test"));
                futures.add(future);
            };

            assertDoesNotThrow(() -> CpuMemoryCollector.asyncCollect(consumer));
        }

//        @Test
//        @DisplayName("test with interrupted exception")
//        void test3() {
//            Consumer<List<CompletableFuture<Void>>> consumer = futures -> {
//                CompletableFuture<Void> future = new CompletableFuture<>();
//                Thread.currentThread().interrupt();
//                future.cancel(true);
//                futures.add(future);
//            };
//
//            assertDoesNotThrow(() -> CpuMemoryCollector.asyncCollect(consumer));
//        }
    }

    @Nested
    @DisplayName("Inner class MemInfo test")
    class MemInfoTest {
        @Test
        @DisplayName("test memory calculation")
        void test1() {
            Object testObj = new Object();
            WeakReference<Object> ref = new WeakReference<>(testObj);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);

            Long memory = memInfo.memory();

            assertNotNull(memory);
            assertTrue(memory > 0);
        }

        @Test
        @DisplayName("test with null reference")
        void test2() {
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(null);

            Long memory = memInfo.memory();

            assertNull(memory);
        }

        @Test
        @DisplayName("test with garbage collected object")
        void test3() {
            WeakReference<Object> ref = new WeakReference<>(null);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);

            Long memory = memInfo.memory();

            assertNull(memory);
        }

        @Test
        @DisplayName("test cache mechanism")
        void test4() {
            Object testObj = new Object();
            WeakReference<Object> ref = new WeakReference<>(testObj);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);

            Long memory1 = memInfo.memory();
            Long memory2 = memInfo.memory();

            assertEquals(memory1, memory2);
        }

        @Test
        @DisplayName("test cache mechanism")
        void test5() {
            Object testObj = new Object();
            WeakReference<Object> ref = new WeakReference<>(testObj);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);
            memInfo.lastCalcTime = System.currentTimeMillis() - 10000L;
            memInfo.mem = 100L;
            Long memory1 = memInfo.memory();
            assertEquals(100L, memory1);
        }

        @Test
        @DisplayName("test cache mechanism")
        void test6() {
            Object testObj = new Object();
            WeakReference<Object> ref = new WeakReference<>(testObj);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);
            memInfo.lastCalcTime = System.currentTimeMillis() - 50000L;
            memInfo.mem = 100L;
            Long memory1 = memInfo.memory();
            assertNotEquals(100L, memory1);
        }
    }

    @Nested
    @DisplayName("Inner class Info test")
    class InfoTest {
        @Test
        @DisplayName("test judged with first call")
        void test1() {
            CpuMemoryCollector.Info info = new CpuMemoryCollector.Info();

            boolean result = info.judged(100);

            assertTrue(result);
            assertEquals(100L, info.lastCount);
        }

        @Test
        @DisplayName("test judged with significant change")
        void test2() {
            CpuMemoryCollector.Info info = new CpuMemoryCollector.Info();
            info.lastCount = 100L;

            boolean result = info.judged(150);

            assertTrue(result);
            assertEquals(150L, info.lastCount);
        }

        @Test
        @DisplayName("test judged with minor change")
        void test3() {
            CpuMemoryCollector.Info info = new CpuMemoryCollector.Info();
            info.lastCount = 100L;

            boolean result = info.judged(110);

            assertFalse(result);
            assertEquals(100L, info.lastCount);
        }

        @Test
        @DisplayName("test judged with zero lastCount")
        void test4() {
            CpuMemoryCollector.Info info = new CpuMemoryCollector.Info();
            info.lastCount = 0L;

            boolean result = info.judged(50);

            assertTrue(result);
            assertEquals(50L, info.lastCount);
        }
    }

    @Nested
    @DisplayName("Static fields test")
    class StaticFieldsTest {
        @Test
        @DisplayName("test constants")
        void test1() {
            assertNotNull(CpuMemoryCollector.COLLECTOR);
            assertNotNull(CpuMemoryCollector.THREAD_CPU_TIME);
        }
    }

    @Nested
    @DisplayName("Method collectMemoryUsage test")
    class collectMemoryUsage1Test {
        @Test
        @DisplayName("test with filter task ids")
        void test1() {
            String taskId = "task1";
            List<String> filterTaskIds = Arrays.asList(taskId);
            Map<String, Usage> usageMap = new HashMap<>();

            Object testObj = new Object();
            List<WeakReference<Object>> weakRefs = new ArrayList<>();
            weakRefs.add(new WeakReference<>(testObj));
            collector.weakReferenceMap.put(taskId, weakRefs);
            collector.cacheMemoryMap.put(weakRefs.get(0), new CpuMemoryCollector.MemInfo(weakRefs.get(0)));

            collector.collectMemoryUsage(filterTaskIds, usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test without filter task ids")
        void test2() {
            String taskId = "task1";
            Map<String, Usage> usageMap = new HashMap<>();

            Object testObj = new Object();
            List<WeakReference<Object>> weakRefs = new ArrayList<>();
            weakRefs.add(new WeakReference<>(testObj));
            collector.weakReferenceMap.put(taskId, weakRefs);
            collector.cacheMemoryMap.put(weakRefs.get(0), new CpuMemoryCollector.MemInfo(weakRefs.get(0)));

            collector.collectMemoryUsage(null, usageMap);

            assertFalse(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test with empty weak references")
        void test3() {
            String taskId = "task1";
            Map<String, Usage> usageMap = new HashMap<>();

            collector.weakReferenceMap.put(taskId, new ArrayList<>());

            collector.collectMemoryUsage(null, usageMap);

            assertFalse(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test with null task info")
        void test4() {
            String taskId = "task1";
            Map<String, Usage> usageMap = new HashMap<>();

            Object testObj = new Object();
            List<WeakReference<Object>> weakRefs = new ArrayList<>();
            weakRefs.add(new WeakReference<>(testObj));
            collector.weakReferenceMap.put(taskId, weakRefs);
            collector.cacheMemoryMap.put(weakRefs.get(0), new CpuMemoryCollector.MemInfo(weakRefs.get(0)));
            collector.taskInfo.remove(taskId);

            collector.collectMemoryUsage(null, usageMap);

            assertTrue(collector.taskInfo.containsKey(taskId));
        }

        @Test
        @DisplayName("test with judged false")
        void test5() {
            String taskId = "task1";
            Map<String, Usage> usageMap = new HashMap<>();

            Object testObj = new Object();
            List<WeakReference<Object>> weakRefs = new ArrayList<>();
            weakRefs.add(new WeakReference<>(testObj));
            collector.weakReferenceMap.put(taskId, weakRefs);
            collector.cacheMemoryMap.put(weakRefs.get(0), new CpuMemoryCollector.MemInfo(weakRefs.get(0)));

            CpuMemoryCollector.Info info = new CpuMemoryCollector.Info();
            info.lastCount = 1000L;
            collector.taskInfo.put(taskId, info);

            collector.collectMemoryUsage(null, usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }
    }

    @Nested
    @DisplayName("Method eachTaskOnce test")
    class eachTaskOnce1Test {
        @Test
        @DisplayName("test with valid weak references")
        void test1() {
            List<WeakReference<Object>> weakReferences = new ArrayList<>();
            List<WeakReference<Object>> remove = new ArrayList<>();
            Usage usage = new Usage();

            Object testObj = new Object();
            WeakReference<Object> ref = new WeakReference<>(testObj);
            weakReferences.add(ref);
            collector.cacheMemoryMap.put(ref, new CpuMemoryCollector.MemInfo(ref));

            collector.eachTaskOnce(weakReferences, remove, usage);

            assertTrue(usage.getHeapMemoryUsage() > 0);
        }

        @Test
        @DisplayName("test with null weak reference object")
        void test2() {
            List<WeakReference<Object>> weakReferences = new ArrayList<>();
            List<WeakReference<Object>> remove = new ArrayList<>();
            Usage usage = new Usage();

            WeakReference<Object> ref = new WeakReference<>(null);
            weakReferences.add(ref);

            collector.eachTaskOnce(weakReferences, remove, usage);

            assertEquals(1, remove.size());
        }

        @Test
        @DisplayName("test with null mem info")
        void test3() {
            List<WeakReference<Object>> weakReferences = new ArrayList<>();
            List<WeakReference<Object>> remove = new ArrayList<>();
            Usage usage = new Usage();

            Object testObj = new Object();
            WeakReference<Object> ref = new WeakReference<>(testObj);
            weakReferences.add(ref);

            collector.eachTaskOnce(weakReferences, remove, usage);

            assertEquals(1, remove.size());
        }

        @Test
        @DisplayName("test with null weak reference")
        void test4() {
            List<WeakReference<Object>> weakReferences = new ArrayList<>();
            List<WeakReference<Object>> remove = new ArrayList<>();
            Usage usage = new Usage();

            weakReferences.add(null);

            collector.eachTaskOnce(weakReferences, remove, usage);

            assertEquals(0, remove.size());
        }
    }

    @Nested
    @DisplayName("Method ignore test")
    class ignore1Test {
        @Test
        @DisplayName("test normal execution")
        void test1() {
            Runnable runnable = () -> {
                // normal execution
            };

            assertDoesNotThrow(() -> collector.ignore(runnable, "test message"));
        }

        @Test
        @DisplayName("test with exception")
        void test2() {
            Runnable runnable = () -> {
                throw new RuntimeException("test exception");
            };

            assertDoesNotThrow(() -> collector.ignore(runnable, "test message"));
        }
    }

    @Nested
    @DisplayName("MemInfo memory calculation edge cases")
    class MemInfoEdgeCasesTest {
        @Test
        @DisplayName("test with cache time check")
        void test1() throws InterruptedException {
            Object testObj = new Object();
            WeakReference<Object> ref = new WeakReference<>(testObj);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);

            // First call
            Long memory1 = memInfo.memory();

            // Set lastCalcTime to simulate cache hit
            memInfo.lastCalcTime = System.currentTimeMillis() - 20000L;

            // Second call should use cached value
            Long memory2 = memInfo.memory();

            assertEquals(memory1, memory2);
        }

        @Test
        @DisplayName("test with GraphLayout exception fallback")
        void test2() {
            // Create an object that might cause GraphLayout to fail
            Object testObj = new Object() {
                @Override
                public String toString() {
                    throw new RuntimeException("test");
                }
            };
            WeakReference<Object> ref = new WeakReference<>(testObj);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);

            Long memory = memInfo.memory();

            assertNotNull(memory);
            assertTrue(memory > 0);
        }

        @Test
        @DisplayName("test synchronized removal")
        void test3() {
            WeakReference<Object> ref = new WeakReference<>(null);
            CpuMemoryCollector.MemInfo memInfo = new CpuMemoryCollector.MemInfo(ref);
            collector.cacheMemoryMap.put(ref, memInfo);

            Long memory = memInfo.memory();

            assertNull(memory);
            assertFalse(collector.cacheMemoryMap.containsKey(ref));
        }
    }

    @Nested
    @DisplayName("Static method addNode edge cases")
    class addNodeEdgeCasesTest {
        @Test
        @DisplayName("test with blank nodeId")
        void test1() {
            CpuMemoryCollector.addNode("taskId", "");

            assertFalse(CpuMemoryCollector.COLLECTOR.taskWithNode.containsValue("taskId"));
        }

        @Test
        @DisplayName("test with blank taskId")
        void test2() {
            CpuMemoryCollector.addNode("", "nodeId");

            assertFalse(CpuMemoryCollector.COLLECTOR.taskWithNode.containsKey("nodeId"));
        }
    }

    @Nested
    @DisplayName("Static method registerTask edge cases")
    class registerTaskEdgeCasesTest {
        @Test
        @DisplayName("test with empty taskId")
        void test1() {
            CpuMemoryCollector.COLLECTOR.taskWithNode.put("nodeId", "");

            CpuMemoryCollector.registerTask("nodeId", threadFactory);

            assertFalse(CpuMemoryCollector.COLLECTOR.threadGroupMap.containsKey(""));
        }

        @Test
        @DisplayName("test with duplicate threadFactory")
        void test2() {
            String taskId = "taskId";
            String nodeId = "nodeId";
            CpuMemoryCollector.COLLECTOR.taskWithNode.put(nodeId, taskId);

            CpuMemoryCollector.registerTask(nodeId, threadFactory);
            CpuMemoryCollector.registerTask(nodeId, threadFactory);

            List<WeakReference<ThreadFactory>> refs = CpuMemoryCollector.COLLECTOR.threadGroupMap.get(taskId);
            assertEquals(1, refs.size());
        }

        @Test
        @DisplayName("test cleanup of null references")
        void test3() {
            String taskId = "taskId";
            String nodeId = "nodeId";
            CpuMemoryCollector.COLLECTOR.taskWithNode.put(nodeId, taskId);

            List<WeakReference<ThreadFactory>> refs = new ArrayList<>();
            refs.add(new WeakReference<>(null));
            CpuMemoryCollector.COLLECTOR.threadGroupMap.put(taskId, refs);

            CpuMemoryCollector.registerTask(nodeId, threadFactory);

            refs = CpuMemoryCollector.COLLECTOR.threadGroupMap.get(taskId);
            assertEquals(1, refs.size());
            assertNotNull(refs.get(0).get());
        }
    }

    @Nested
    @DisplayName("Static method listening edge cases")
    class listeningEdgeCasesTest {
        @Test
        @DisplayName("test with empty taskId")
        void test1() {
            CpuMemoryCollector.COLLECTOR.taskWithNode.put("nodeId", "");

            CpuMemoryCollector.listening("nodeId", new Object());

            assertFalse(CpuMemoryCollector.COLLECTOR.weakReferenceMap.containsKey(""));
        }
    }

    @Nested
    @DisplayName("Constructor test")
    class ConstructorTest {
        @Test
        @DisplayName("test private constructor")
        void test1() {
            // Test that COLLECTOR is properly initialized
            assertNotNull(CpuMemoryCollector.COLLECTOR);
            assertNotNull(CpuMemoryCollector.COLLECTOR.taskWithNode);
            assertNotNull(CpuMemoryCollector.COLLECTOR.taskDtoMap);
            assertNotNull(CpuMemoryCollector.COLLECTOR.weakReferenceMap);
            assertNotNull(CpuMemoryCollector.COLLECTOR.threadGroupMap);
            assertNotNull(CpuMemoryCollector.COLLECTOR.taskInfo);
            assertNotNull(CpuMemoryCollector.COLLECTOR.cacheMemoryMap);
        }
    }
}