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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
            
            assertTrue(usage.getHeapMemoryUsage() > 0);
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
            assertEquals(0, remove.size());
            System.gc();
            collector.eachTaskOnce(weakReferences, remove, usage);
            assertEquals(1, remove.size());
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
            
            assertTrue(usageMap.containsKey(taskId));
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
}