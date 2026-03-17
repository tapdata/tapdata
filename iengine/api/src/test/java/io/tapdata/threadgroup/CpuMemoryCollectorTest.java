package io.tapdata.threadgroup;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.Usage;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.core.executor.ThreadFactory;
import io.tapdata.schema.TapTableMap;
import io.tapdata.threadgroup.utils.FixedConcurrentHashMap;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Class CpuMemoryCollector Test")
class CpuMemoryCollectorTest {
    private CpuMemoryCollector collector;

    @BeforeEach
    void setUp() {
        collector = CpuMemoryCollector.COLLECTOR;
        // Clear all maps before each test
        collector.taskWithNode.clear();
        collector.taskDtoMap.clear();
        collector.weakReferenceMap.clear();
        collector.referenceQueue.clear();
        collector.threadGroupMap.clear();
        collector.cleaned.clear();
        CpuMemoryCollector.switchChange(true);
    }

    @AfterEach
    void tearDown() {
        collector.taskWithNode.clear();
        collector.taskDtoMap.clear();
        collector.weakReferenceMap.clear();
        collector.referenceQueue.clear();
        collector.threadGroupMap.clear();
        collector.cleaned.clear();
        CpuMemoryCollector.switchChange(true);
    }

    @Nested
    @DisplayName("Method switchChange test")
    class SwitchChangeTest {
        @Test
        @DisplayName("test switch to false")
        void testSwitchToFalse() {
            CpuMemoryCollector.switchChange(false);
            assertFalse((Boolean) ReflectionTestUtils.getField(collector, "doCollect"));
        }

        @Test
        @DisplayName("test switch to true")
        void testSwitchToTrue() {
            CpuMemoryCollector.switchChange(false);
            CpuMemoryCollector.switchChange(true);
            assertTrue((Boolean) ReflectionTestUtils.getField(collector, "doCollect"));
        }

        @Test
        @DisplayName("test switch same value does nothing")
        void testSwitchSameValue() {
            CpuMemoryCollector.switchChange(true);
            assertTrue((Boolean) ReflectionTestUtils.getField(collector, "doCollect"));
        }
    }

    @Nested
    @DisplayName("Method startTask test")
    class StartTaskTest {
        @Test
        @DisplayName("test start task successfully")
        void testStartTaskSuccess() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            CpuMemoryCollector.startTask(taskDto);
            assertTrue(collector.taskDtoMap.containsKey(taskDto.getId().toHexString()));
        }

        @Test
        @DisplayName("test start task with exception")
        void testStartTaskWithException() {
            TaskDto taskDto = mock(TaskDto.class);
            ObjectId objectId = new ObjectId();
            when(taskDto.getId()).thenReturn(objectId).thenThrow(new RuntimeException("test"));
            assertDoesNotThrow(() -> CpuMemoryCollector.startTask(taskDto));
        }
    }

    @Nested
    @DisplayName("Method addNode test")
    class AddNodeTest {
        @Test
        @DisplayName("test add node successfully")
        void testAddNodeSuccess() {
            String taskId = "task1";
            String nodeId = "node1";
            CpuMemoryCollector.addNode(taskId, nodeId);
            assertEquals(taskId, collector.taskWithNode.get(nodeId));
        }

        @Test
        @DisplayName("test add node with blank nodeId")
        void testAddNodeBlankNodeId() {
            CpuMemoryCollector.addNode("task1", "");
            assertTrue(collector.taskWithNode.isEmpty());
        }

        @Test
        @DisplayName("test add node with blank taskId")
        void testAddNodeBlankTaskId() {
            CpuMemoryCollector.addNode("", "node1");
            assertTrue(collector.taskWithNode.isEmpty());
        }

        @Test
        @DisplayName("test add node with null values")
        void testAddNodeNullValues() {
            CpuMemoryCollector.addNode(null, null);
            assertTrue(collector.taskWithNode.isEmpty());
        }
    }

    @Nested
    @DisplayName("Method registerTask test")
    class RegisterTaskTest {
        @Test
        @DisplayName("test register task with null threadGroup")
        void testRegisterTaskNullThreadGroup() {
            CpuMemoryCollector.registerTask("node1", null);
            assertTrue(collector.threadGroupMap.isEmpty());
        }

        @Test
        @DisplayName("test register task with empty taskId")
        void testRegisterTaskEmptyTaskId() {
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            CpuMemoryCollector.registerTask("node1", threadFactory);
            assertTrue(collector.threadGroupMap.isEmpty());
        }

        @Test
        @DisplayName("test register task successfully")
        void testRegisterTaskSuccess() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            ThreadFactory threadFactory = mock(ThreadFactory.class);

            CpuMemoryCollector.registerTask(nodeId, threadFactory);

            assertTrue(collector.threadGroupMap.containsKey(taskId));
            assertTrue(collector.weakReferenceMap.containsKey(taskId));
            assertTrue(collector.referenceQueue.containsKey(taskId));
        }

        @Test
        @DisplayName("test register task with existing threadGroup")
        void testRegisterTaskExistingThreadGroup() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            ThreadFactory threadFactory = mock(ThreadFactory.class);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            CpuMemoryCollector.registerTask(nodeId, threadFactory);

            assertEquals(1, collector.threadGroupMap.get(taskId).size());
        }

        @Test
        @DisplayName("test register task removes null weak references")
        void testRegisterTaskRemovesNullWeakRefs() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            ThreadFactory threadFactory = mock(ThreadFactory.class);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(null));
            collector.threadGroupMap.put(taskId, list);

            CpuMemoryCollector.registerTask(nodeId, threadFactory);

            assertEquals(1, collector.threadGroupMap.get(taskId).size());
        }
    }

    @Nested
    @DisplayName("Method startClean test")
    class StartCleanTest {
        @Test
        @DisplayName("test startClean when already cleaning")
        void testStartCleanAlreadyCleaning() {
            String taskId = "task1";
            AtomicBoolean cleanTag = new AtomicBoolean(true);
            collector.cleaned.put(taskId, cleanTag);

            collector.startClean(taskId);

            assertTrue(cleanTag.get());
        }

        @Test
        @DisplayName("test startClean starts new clean")
        void testStartCleanStartsNew() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());

            collector.startClean(taskId);

            Thread.sleep(100);
            assertTrue(collector.cleaned.get(taskId).get());

            // Stop the clean loop
            collector.cleaned.get(taskId).set(false);
        }
    }

    @Nested
    @DisplayName("Method unregisterTask test")
    class UnregisterTaskTest {
        @Test
        @DisplayName("test unregister task successfully")
        void testUnregisterTaskSuccess() {
            String taskId = "task1";
            String nodeId = "node1";

            collector.taskWithNode.put(nodeId, taskId);
            collector.taskDtoMap.put(taskId, new WeakReference<>(new TaskDto()));
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.threadGroupMap.put(taskId, new CopyOnWriteArrayList<>());
            collector.cleaned.put(taskId, new AtomicBoolean(true));

            CpuMemoryCollector.unregisterTask(taskId);

            assertFalse(collector.taskWithNode.containsKey(nodeId));
            assertFalse(collector.taskDtoMap.containsKey(taskId));
            assertFalse(collector.weakReferenceMap.containsKey(taskId));
            assertFalse(collector.referenceQueue.containsKey(taskId));
            assertFalse(collector.threadGroupMap.containsKey(taskId));
            assertFalse(collector.cleaned.containsKey(taskId));
        }

        @Test
        @DisplayName("test unregister task with null cleanTag")
        void testUnregisterTaskNullCleanTag() {
            String taskId = "task1";
            assertDoesNotThrow(() -> CpuMemoryCollector.unregisterTask(taskId));
        }
    }

    @Nested
    @DisplayName("Method listeningTables test")
    class ListeningTablesTest {
        @Test
        @DisplayName("test listeningTables when doCollect is false")
        void testListeningTablesDoCollectFalse() {
            CpuMemoryCollector.switchChange(false);
            TapTableMap<?, ?> tableMap = mock(TapTableMap.class);
            CpuMemoryCollector.listeningTables("node1", tableMap);
            assertTrue(collector.weakReferenceMap.isEmpty());
        }

        @Test
        @DisplayName("test listeningTables with null taskId")
        void testListeningTablesNullTaskId() {
            TapTableMap<?, ?> tableMap = mock(TapTableMap.class);
            when(tableMap.keySet()).thenReturn(new java.util.HashSet<>());
            CpuMemoryCollector.listeningTables("node1", tableMap);
            assertTrue(collector.weakReferenceMap.isEmpty());
        }

        @Test
        @DisplayName("test listeningTables with null referenceQueue")
        void testListeningTablesNullReferenceQueue() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);

            TapTableMap<?, ?> tableMap = mock(TapTableMap.class);
            when(tableMap.keySet()).thenReturn(new java.util.HashSet<>());

            CpuMemoryCollector.listeningTables(nodeId, tableMap);
            assertTrue(collector.weakReferenceMap.isEmpty());
        }

        @Test
        @DisplayName("test listeningTables with null weakMap")
        void testListeningTablesNullWeakMap() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());

            TapTableMap<?, ?> tableMap = mock(TapTableMap.class);
            when(tableMap.keySet()).thenReturn(new java.util.HashSet<>());

            CpuMemoryCollector.listeningTables(nodeId, tableMap);
            assertFalse(collector.weakReferenceMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test listeningTables successfully")
        void testListeningTablesSuccess() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            TapTableMap<?, ?> tableMap = mock(TapTableMap.class);
            when(tableMap.keySet()).thenReturn(new java.util.HashSet<>());

            CpuMemoryCollector.listeningTables(nodeId, tableMap);
            assertEquals(1, collector.weakReferenceMap.get(taskId).size());
        }
    }

    @Nested
    @DisplayName("Method listening test")
    class ListeningTest {
        @Test
        @DisplayName("test listening when doCollect is false")
        void testListeningDoCollectFalse() {
            CpuMemoryCollector.switchChange(false);
            CpuMemoryCollector.listening("node1", new HashMap<>());
            assertTrue(collector.weakReferenceMap.isEmpty());
        }

        @Test
        @DisplayName("test listening with unsupported type")
        void testListeningUnsupportedType() {
            CpuMemoryCollector.listening("node1", "string");
            assertTrue(collector.weakReferenceMap.isEmpty());
        }

        @Test
        @DisplayName("test listening with null taskId")
        void testListeningNullTaskId() {
            CpuMemoryCollector.listening("node1", new HashMap<>());
            assertTrue(collector.weakReferenceMap.isEmpty());
        }

        @Test
        @DisplayName("test listening with null referenceQueue")
        void testListeningNullReferenceQueue() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);

            CpuMemoryCollector.listening(nodeId, new HashMap<>());
            assertTrue(collector.weakReferenceMap.isEmpty());
        }

        @Test
        @DisplayName("test listening with null weakMap")
        void testListeningNullWeakMap() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());

            CpuMemoryCollector.listening(nodeId, new HashMap<>());
            assertFalse(collector.weakReferenceMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test listening with Map successfully")
        void testListeningMapSuccess() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            CpuMemoryCollector.listening(nodeId, new HashMap<>());
            assertEquals(1, collector.weakReferenceMap.get(taskId).size());
        }

        @Test
        @DisplayName("test listening with Collection successfully")
        void testListeningCollectionSuccess() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            CpuMemoryCollector.listening(nodeId, new ArrayList<>());
            assertEquals(1, collector.weakReferenceMap.get(taskId).size());
        }

        @Test
        @DisplayName("test listening with TapEvent successfully")
        void testListeningTapEventSuccess() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            TapEvent tapEvent = mock(TapEvent.class);
            CpuMemoryCollector.listening(nodeId, tapEvent);
            assertEquals(1, collector.weakReferenceMap.get(taskId).size());
        }

        @Test
        @DisplayName("test listening with TapdataEvent successfully")
        void testListeningTapdataEventSuccess() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            TapdataEvent tapdataEvent = mock(TapdataEvent.class);
            CpuMemoryCollector.listening(nodeId, tapdataEvent);
            assertEquals(1, collector.weakReferenceMap.get(taskId).size());
        }
    }

    @Nested
    @DisplayName("Method eachTaskOnce test")
    class EachTaskOnceTest {
        @Test
        @DisplayName("test eachTaskOnce with valid weak references")
        void testEachTaskOnceValidRefs() {
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakReferences.put(new WeakReference<>(testObj), 100L);

            Usage usage = new Usage();
            collector.eachTaskOnce(weakReferences, usage);

            assertEquals(100L, usage.getHeapMemoryUsage());
        }

        @Test
        @DisplayName("test eachTaskOnce with null reference")
        void testEachTaskOnceNullRef() {
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = new FixedConcurrentHashMap<>(100);
            weakReferences.put(new WeakReference<>(null), 100L);

            Usage usage = new Usage();
            collector.eachTaskOnce(weakReferences, usage);

            assertEquals(0L, usage.getHeapMemoryUsage());
            assertEquals(0, weakReferences.size());
        }

        @Test
        @DisplayName("test eachTaskOnce with zero size")
        void testEachTaskOnceZeroSize() {
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakReferences.put(new WeakReference<>(testObj), 0L);

            Usage usage = new Usage();
            collector.eachTaskOnce(weakReferences, usage);

            assertTrue(usage.getHeapMemoryUsage() > 0);
        }

        @Test
        @DisplayName("test eachTaskOnce with negative size")
        void testEachTaskOnceNegativeSize() {
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakReferences.put(new WeakReference<>(testObj), -1L);

            Usage usage = new Usage();
            collector.eachTaskOnce(weakReferences, usage);

            assertTrue(usage.getHeapMemoryUsage() > 0);
        }
    }

    @Nested
    @DisplayName("Method collectMemoryUsage test")
    class CollectMemoryUsageTest {
        @Test
        @DisplayName("test collectMemoryUsage with filter")
        void testCollectMemoryUsageWithFilter() {
            String taskId = "task1";
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectMemoryUsage(List.of(taskId), usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test collectMemoryUsage without filter")
        void testCollectMemoryUsageWithoutFilter() {
            String taskId = "task1";
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectMemoryUsage(null, usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test collectMemoryUsage with empty weakReferences")
        void testCollectMemoryUsageEmptyWeakRefs() {
            String taskId = "task1";
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectMemoryUsage(List.of(taskId), usageMap);

            assertFalse(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test collectMemoryUsage with taskDto")
        void testCollectMemoryUsageWithTaskDto() {
            String taskId = "task1";
            TaskDto taskDto = new TaskDto();
            collector.taskDtoMap.put(taskId, new WeakReference<>(taskDto));

            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectMemoryUsage(List.of(taskId), usageMap);

            assertTrue(usageMap.containsKey(taskId));
            assertTrue(usageMap.get(taskId).getHeapMemoryUsage() > 100L);
        }

        @Test
        @DisplayName("test collectMemoryUsage filter not matching")
        void testCollectMemoryUsageFilterNotMatching() {
            String taskId = "task1";
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectMemoryUsage(List.of("task2"), usageMap);

            assertFalse(usageMap.containsKey(taskId));
        }
    }

    @Nested
    @DisplayName("Method collectCpuUsage test")
    class CollectCpuUsageTest {
        @Test
        @DisplayName("test collectCpuUsage with filter")
        void testCollectCpuUsageWithFilter() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectCpuUsage(List.of(taskId), usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test collectCpuUsage without filter")
        void testCollectCpuUsageWithoutFilter() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectCpuUsage(null, usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }
    }

    @Nested
    @DisplayName("Method eachThreadGroup test")
    class EachThreadGroupTest {
        @Test
        @DisplayName("test eachThreadGroup with valid threadFactory")
        void testEachThreadGroupValid() {
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            weakReferences.add(new WeakReference<>(threadFactory));
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            LongConsumer consumer = mock(LongConsumer.class);

            collector.eachThreadGroup(weakReferences, useless, consumer);

            assertTrue(useless.isEmpty());
        }

        @Test
        @DisplayName("test eachThreadGroup with null threadFactory")
        void testEachThreadGroupNullFactory() {
            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            weakReferences.add(new WeakReference<>(null));
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            LongConsumer consumer = mock(LongConsumer.class);

            collector.eachThreadGroup(weakReferences, useless, consumer);

            assertEquals(1, useless.size());
        }

        @Test
        @DisplayName("test eachThreadGroup with empty list")
        void testEachThreadGroupEmptyList() {
            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            LongConsumer consumer = mock(LongConsumer.class);

            collector.eachThreadGroup(weakReferences, useless, consumer);

            assertTrue(useless.isEmpty());
            verify(consumer, never()).accept(anyLong());
        }
    }

    @Nested
    @DisplayName("Method eachOneTask test")
    class EachOneTaskTest {
        @Test
        @DisplayName("test eachOneTask with null weakReferences")
        void testEachOneTaskNullWeakRefs() {
            String taskId = "task1";
            Usage usage = new Usage();

            collector.eachOneTask(taskId, usage);

            assertFalse(collector.threadGroupMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test eachOneTask with empty weakReferences after cleanup")
        void testEachOneTaskEmptyAfterCleanup() {
            String taskId = "task1";
            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(null));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            assertFalse(collector.threadGroupMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test eachOneTask with valid threadFactory")
        void testEachOneTaskValid() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            assertTrue(usage.getCpuUsage() >= 0);
        }
    }

    @Nested
    @DisplayName("Method collectOnce test")
    class CollectOnceTest {
        @Test
        @DisplayName("test collectOnce when doCollect is false")
        void testCollectOnceDoCollectFalse() {
            CpuMemoryCollector.switchChange(false);
            String taskId = "task1";
            collector.threadGroupMap.put(taskId, new CopyOnWriteArrayList<>());

            Map<String, Usage> result = CpuMemoryCollector.collectOnce(List.of(taskId));

            assertTrue(result.containsKey(taskId));
            assertNull(result.get(taskId).getCpuUsage());
            assertNull(result.get(taskId).getHeapMemoryUsage());
        }

        @Test
        @DisplayName("test collectOnce when doCollect is true")
        void testCollectOnceDoCollectTrue() {
            String taskId = "task1";
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);
            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Map<String, Usage> result = CpuMemoryCollector.collectOnce(List.of(taskId));

            assertNotNull(result);
        }

        @Test
        @DisplayName("test collectOnce with empty taskIds")
        void testCollectOnceEmptyTaskIds() {
            Map<String, Usage> result = CpuMemoryCollector.collectOnce(new ArrayList<>());
            assertNotNull(result);
        }
    }

    @Nested
    @DisplayName("Method stopCollect test")
    class StopCollectTest {
        @Test
        @DisplayName("test stopCollect with filter")
        void testStopCollectWithFilter() {
            String taskId = "task1";
            collector.threadGroupMap.put(taskId, new CopyOnWriteArrayList<>());

            Map<String, Usage> usageMap = new HashMap<>();
            collector.stopCollect(List.of(taskId), usageMap);

            assertTrue(usageMap.containsKey(taskId));
            assertNull(usageMap.get(taskId).getCpuUsage());
            assertNull(usageMap.get(taskId).getHeapMemoryUsage());
        }

        @Test
        @DisplayName("test stopCollect without filter")
        void testStopCollectWithoutFilter() {
            String taskId = "task1";
            collector.threadGroupMap.put(taskId, new CopyOnWriteArrayList<>());

            Map<String, Usage> usageMap = new HashMap<>();
            collector.stopCollect(null, usageMap);

            assertTrue(usageMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test stopCollect filter not matching")
        void testStopCollectFilterNotMatching() {
            String taskId = "task1";
            collector.threadGroupMap.put(taskId, new CopyOnWriteArrayList<>());

            Map<String, Usage> usageMap = new HashMap<>();
            collector.stopCollect(List.of("task2"), usageMap);

            assertFalse(usageMap.containsKey(taskId));
        }
    }

    @Nested
    @DisplayName("Method ignore test")
    class IgnoreTest {
        @Test
        @DisplayName("test ignore with successful runnable")
        void testIgnoreSuccess() {
            final boolean[] executed = {false};
            collector.ignore(() -> executed[0] = true, "test {}");
            assertTrue(executed[0]);
        }

        @Test
        @DisplayName("test ignore with exception")
        void testIgnoreException() {
            assertDoesNotThrow(() -> collector.ignore(() -> {
                throw new RuntimeException("test error");
            }, "test {}"));
        }
    }

    @Nested
    @DisplayName("Method asyncCollect test")
    class AsyncCollectTest {
        @Test
        @DisplayName("test asyncCollect with successful futures")
        void testAsyncCollectSuccess() {
            final boolean[] executed = {false};
            CpuMemoryCollector.asyncCollect(futures -> {
                futures.add(java.util.concurrent.CompletableFuture.runAsync(() -> executed[0] = true));
            });
            assertTrue(executed[0]);
        }

        @Test
        @DisplayName("test asyncCollect with empty futures")
        void testAsyncCollectEmpty() {
            assertDoesNotThrow(() -> CpuMemoryCollector.asyncCollect(futures -> {}));
        }
    }

    @Nested
    @DisplayName("Constants test")
    class ConstantsTest {
        @Test
        @DisplayName("test MAX_LISTENING_SIZE")
        void testMaxListeningSize() {
            assertTrue(CpuMemoryCollector.MAX_LISTENING_SIZE > 0);
        }

        @Test
        @DisplayName("test THREAD_CPU_TIME")
        void testThreadCpuTime() {
            assertNotNull(CpuMemoryCollector.THREAD_CPU_TIME);
        }

        @Test
        @DisplayName("test COLLECTOR")
        void testCollector() {
            assertNotNull(CpuMemoryCollector.COLLECTOR);
        }
    }

    @Nested
    @DisplayName("Additional coverage tests")
    class AdditionalCoverageTest {
        @Test
        @DisplayName("test startClean with InterruptedException")
        void testStartCleanInterrupted() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            ReferenceQueue<Object> queue = new ReferenceQueue<>();
            collector.referenceQueue.put(taskId, queue);
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = new FixedConcurrentHashMap<>(100);
            collector.weakReferenceMap.put(taskId, weakMap);

            collector.startClean(taskId);
            Thread.sleep(100);

            // Stop the clean loop
            collector.cleaned.get(taskId).set(false);
            Thread.sleep(600);
        }

        @Test
        @DisplayName("test eachOneTask with sleep interrupted")
        void testEachOneTaskSleepInterrupted() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();

            Thread testThread = new Thread(() -> {
                collector.eachOneTask(taskId, usage);
            });
            testThread.start();

            try {
                Thread.sleep(100);
                testThread.interrupt();
                testThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Test
        @DisplayName("test collectOnce with InterruptedException")
        void testCollectOnceInterrupted() {
            String taskId = "task1";
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Thread testThread = new Thread(() -> {
                CpuMemoryCollector.collectOnce(List.of(taskId));
            });
            testThread.start();

            try {
                Thread.sleep(50);
                testThread.interrupt();
                testThread.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Test
        @DisplayName("test asyncCollect with ExecutionException")
        void testAsyncCollectExecutionException() {
            assertDoesNotThrow(() -> CpuMemoryCollector.asyncCollect(futures -> {
                futures.add(java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    throw new RuntimeException("test error");
                }).thenAccept(v -> {}));
            }));
        }

        @Test
        @DisplayName("test asyncCollect with InterruptedException")
        void testAsyncCollectInterruptedException() {
            Thread testThread = new Thread(() -> {
                CpuMemoryCollector.asyncCollect(futures -> {
                    futures.add(java.util.concurrent.CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }));
                });
            });
            testThread.start();

            try {
                Thread.sleep(50);
                testThread.interrupt();
                testThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Test
        @DisplayName("test eachThreadGroup with threads")
        void testEachThreadGroupWithThreads() {
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("testGroup");
            Thread thread = new Thread(threadGroup, () -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "testThread");
            thread.start();

            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            weakReferences.add(new WeakReference<>(threadFactory));
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            List<Long> threadIds = new ArrayList<>();
            LongConsumer consumer = threadIds::add;

            collector.eachThreadGroup(weakReferences, useless, consumer);

            thread.interrupt();
            assertFalse(threadIds.isEmpty());
        }

        @Test
        @DisplayName("test listeningTables with exception")
        void testListeningTablesException() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());

            FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = spy(new FixedConcurrentHashMap<>(0));
            collector.weakReferenceMap.put(taskId, weakMap);

            TapTableMap<?, ?> tableMap = mock(TapTableMap.class);
            when(tableMap.keySet()).thenReturn(new java.util.HashSet<>());

            assertDoesNotThrow(() -> CpuMemoryCollector.listeningTables(nodeId, tableMap));
        }

        @Test
        @DisplayName("test listening with exception")
        void testListeningException() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());

            FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = spy(new FixedConcurrentHashMap<>(0));
            collector.weakReferenceMap.put(taskId, weakMap);

            assertDoesNotThrow(() -> CpuMemoryCollector.listening(nodeId, new HashMap<>()));
        }

        @Test
        @DisplayName("test eachTaskOnce with exception in calculation")
        void testEachTaskOnceCalculationException() {
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakReferences.put(new WeakReference<>(testObj), -1L);

            Usage usage = new Usage();
            assertDoesNotThrow(() -> collector.eachTaskOnce(weakReferences, usage));
        }

        @Test
        @DisplayName("test eachTaskOnce with object becoming null during update")
        void testEachTaskOnceObjectBecomesNull() {
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = new FixedConcurrentHashMap<>(100);
            WeakReference<Object> weakRef = new WeakReference<>(new Object());
            weakReferences.put(weakRef, -1L);

            // Force GC to clear the weak reference
            System.gc();

            Usage usage = new Usage();
            collector.eachTaskOnce(weakReferences, usage);

            assertEquals(0L, usage.getHeapMemoryUsage());
        }

        @Test
        @DisplayName("test collectMemoryUsage with null taskDto reference")
        void testCollectMemoryUsageNullTaskDtoRef() {
            String taskId = "task1";
            collector.taskDtoMap.put(taskId, new WeakReference<>(null));

            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Map<String, Usage> usageMap = new HashMap<>();
            collector.collectMemoryUsage(List.of(taskId), usageMap);

            assertTrue(usageMap.containsKey(taskId));
            assertEquals(100L, usageMap.get(taskId).getHeapMemoryUsage());
        }

        @Test
        @DisplayName("test eachOneTask with useless references in second pass")
        void testEachOneTaskUselessSecondPass() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            list.add(new WeakReference<>(null));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            assertTrue(usage.getCpuUsage() >= 0);
        }

        @Test
        @DisplayName("test eachOneTask with lead time greater than 1000ms")
        void testEachOneTaskLeadTimeGreater() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            assertNotNull(usage.getCpuUsage());
        }

        @Test
        @DisplayName("test startClean with null maps")
        void testStartCleanNullMaps() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            // Don't add referenceQueue and weakReferenceMap

            collector.startClean(taskId);
            Thread.sleep(200);

            // Stop the clean loop
            collector.cleaned.get(taskId).set(false);
            Thread.sleep(100);
        }

        @Test
        @DisplayName("test startClean removes from cleaned map")
        void testStartCleanRemovesFromCleaned() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            collector.startClean(taskId);
            Thread.sleep(100);

            // Remove from cleaned to stop the loop
            collector.cleaned.remove(taskId);
            Thread.sleep(600);
        }

        @Test
        @DisplayName("test collectOnce with both cpu and memory")
        void testCollectOnceBothCpuAndMemory() {
            String taskId = "task1";

            // Setup memory
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            // Setup CPU
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);
            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Map<String, Usage> result = CpuMemoryCollector.collectOnce(List.of(taskId));

            assertNotNull(result);
            assertTrue(result.containsKey(taskId));
        }

        @Test
        @DisplayName("test eachThreadGroup with exception in consumer")
        void testEachThreadGroupExceptionInConsumer() {
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("testGroup");
            Thread thread = new Thread(threadGroup, () -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "testThread");
            thread.start();

            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            List<WeakReference<ThreadFactory>> weakReferences = new ArrayList<>();
            weakReferences.add(new WeakReference<>(threadFactory));
            List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
            LongConsumer consumer = threadId -> {
                throw new RuntimeException("test exception");
            };

            assertDoesNotThrow(() -> collector.eachThreadGroup(weakReferences, useless, consumer));
            thread.interrupt();
        }

        @Test
        @DisplayName("test eachOneTask removes useless in first pass")
        void testEachOneTaskRemovesUselessFirstPass() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(null)); // This will be marked as useless
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            assertTrue(usage.getCpuUsage() >= 0);
        }

        @Test
        @DisplayName("test registerTask with exception")
        void testRegisterTaskWithException() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);

            // Create a mock that throws exception
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            when(threadFactory.getThreadGroup()).thenThrow(new RuntimeException("test"));

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            assertDoesNotThrow(() -> CpuMemoryCollector.registerTask(nodeId, mock(ThreadFactory.class)));
        }

        @Test
        @DisplayName("test startClean with pull from queue")
        void testStartCleanWithPullFromQueue() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            ReferenceQueue<Object> queue = new ReferenceQueue<>();
            collector.referenceQueue.put(taskId, queue);
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = new FixedConcurrentHashMap<>(100);

            // Add a weak reference that will be enqueued
            Object obj = new Object();
            WeakReference<Object> weakRef = new WeakReference<>(obj, queue);
            weakMap.put(weakRef, 100L);
            collector.weakReferenceMap.put(taskId, weakMap);

            collector.startClean(taskId);
            Thread.sleep(100);

            // Clear the object to trigger GC
            obj = null;
            System.gc();
            Thread.sleep(600);

            // Stop the clean loop
            collector.cleaned.get(taskId).set(false);
            Thread.sleep(100);
        }

        @Test
        @DisplayName("test eachOneTask with null thread in group")
        void testEachOneTaskNullThreadInGroup() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            assertNotNull(usage.getCpuUsage());
        }

        @Test
        @DisplayName("test collectOnce handles exceptions gracefully")
        void testCollectOnceHandlesExceptions() {
            String taskId = "task1";

            // Setup with data that might cause issues
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Map<String, Usage> result = CpuMemoryCollector.collectOnce(List.of(taskId));
            assertNotNull(result);
        }

        @Test
        @DisplayName("test listeningTables adds tables to weak map")
        void testListeningTablesAddsTables() {
            String taskId = "task1";
            String nodeId = "node1";
            collector.taskWithNode.put(nodeId, taskId);
            collector.referenceQueue.put(taskId, new ReferenceQueue<>());
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            TapTableMap<String, io.tapdata.entity.schema.TapTable> tableMap = mock(TapTableMap.class);
            java.util.Set<String> keys = new java.util.HashSet<>();
            keys.add("table1");
            when(tableMap.keySet()).thenReturn(keys);
            when(tableMap.get("table1")).thenReturn(new io.tapdata.entity.schema.TapTable("table1"));

            CpuMemoryCollector.listeningTables(nodeId, tableMap);
            assertEquals(1, collector.weakReferenceMap.get(taskId).size());
        }

        @Test
        @DisplayName("test eachOneTask with before map having null value")
        void testEachOneTaskBeforeMapNullValue() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            Thread thread = new Thread(threadGroup, () -> {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "testThread");
            thread.start();

            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            thread.interrupt();
            assertNotNull(usage.getCpuUsage());
        }

        @Test
        @DisplayName("test startClean with IllegalArgumentException")
        void testStartCleanIllegalArgumentException() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            ReferenceQueue<Object> queue = mock(ReferenceQueue.class);
            try {
                when(queue.remove(anyLong())).thenThrow(new IllegalArgumentException("test"));
            } catch (InterruptedException e) {
                // ignore
            }
            collector.referenceQueue.put(taskId, queue);
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            collector.startClean(taskId);
            Thread.sleep(200);

            // Stop the clean loop
            collector.cleaned.get(taskId).set(false);
            Thread.sleep(100);
        }

        @Test
        @DisplayName("test startClean with InterruptedException in queue remove")
        void testStartCleanInterruptedExceptionInQueueRemove() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            ReferenceQueue<Object> queue = mock(ReferenceQueue.class);
            try {
                when(queue.remove(anyLong())).thenThrow(new InterruptedException("test"));
            } catch (InterruptedException e) {
                // ignore
            }
            collector.referenceQueue.put(taskId, queue);
            collector.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(100));

            collector.startClean(taskId);
            Thread.sleep(200);

            // Stop the clean loop
            AtomicBoolean cleanTag = collector.cleaned.get(taskId);
            if (cleanTag != null) {
                cleanTag.set(false);
            }
            Thread.sleep(100);
        }

        @Test
        @DisplayName("test collectOnce with ExecutionException in cpu future")
        void testCollectOnceExecutionExceptionCpu() {
            String taskId = "task1";

            // Setup CPU with mock that throws
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            when(threadFactory.getThreadGroup()).thenThrow(new RuntimeException("test"));
            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Map<String, Usage> result = CpuMemoryCollector.collectOnce(List.of(taskId));
            assertNotNull(result);
        }

        @Test
        @DisplayName("test eachOneTask removes empty threadGroupMap")
        void testEachOneTaskRemovesEmptyMap() {
            String taskId = "task1";
            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            collector.eachOneTask(taskId, usage);

            assertFalse(collector.threadGroupMap.containsKey(taskId));
        }

        @Test
        @DisplayName("test startClean loop continues when cleanTag is true")
        void testStartCleanLoopContinues() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            ReferenceQueue<Object> queue = new ReferenceQueue<>();
            collector.referenceQueue.put(taskId, queue);
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = new FixedConcurrentHashMap<>(100);
            collector.weakReferenceMap.put(taskId, weakMap);

            collector.startClean(taskId);

            // Let it run for a bit
            Thread.sleep(700);

            // Stop the clean loop
            collector.cleaned.get(taskId).set(false);
            Thread.sleep(100);
        }

        @Test
        @DisplayName("test collectOnce with null filter")
        void testCollectOnceNullFilter() {
            String taskId = "task1";
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakRefs = new FixedConcurrentHashMap<>(100);
            Object testObj = new Object();
            weakRefs.put(new WeakReference<>(testObj), 100L);
            collector.weakReferenceMap.put(taskId, weakRefs);

            Map<String, Usage> result = CpuMemoryCollector.collectOnce(null);
            assertNotNull(result);
        }

        @Test
        @DisplayName("test eachOneTask with sleep less than 1000ms")
        void testEachOneTaskSleepLessThan1000() {
            String taskId = "task1";
            ThreadFactory threadFactory = mock(ThreadFactory.class);
            ThreadGroup threadGroup = new ThreadGroup("test");
            when(threadFactory.getThreadGroup()).thenReturn(threadGroup);

            CopyOnWriteArrayList<WeakReference<ThreadFactory>> list = new CopyOnWriteArrayList<>();
            list.add(new WeakReference<>(threadFactory));
            collector.threadGroupMap.put(taskId, list);

            Usage usage = new Usage();
            long start = System.currentTimeMillis();
            collector.eachOneTask(taskId, usage);
            long elapsed = System.currentTimeMillis() - start;

            // Should take at least 1 second due to sleep
            assertTrue(elapsed >= 900);
        }

        @Test
        @DisplayName("test startClean with multiple pulls from queue")
        void testStartCleanMultiplePulls() throws InterruptedException {
            String taskId = "task1";
            collector.cleaned.put(taskId, new AtomicBoolean(false));
            ReferenceQueue<Object> queue = new ReferenceQueue<>();
            collector.referenceQueue.put(taskId, queue);
            FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = new FixedConcurrentHashMap<>(100);

            // Add multiple weak references
            for (int i = 0; i < 5; i++) {
                Object obj = new Object();
                WeakReference<Object> weakRef = new WeakReference<>(obj, queue);
                weakMap.put(weakRef, 100L);
            }
            collector.weakReferenceMap.put(taskId, weakMap);

            collector.startClean(taskId);
            Thread.sleep(100);

            // Force GC to enqueue weak references
            System.gc();
            Thread.sleep(700);

            // Stop the clean loop
            collector.cleaned.get(taskId).set(false);
            Thread.sleep(100);
        }
    }
}