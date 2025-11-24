package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.aspect.CpuMemUsageAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.TaskAspectManager;
import io.tapdata.entity.Usage;
import io.tapdata.threadgroup.CpuMemoryCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("Class CpuMemoryScheduler Test")
class CpuMemorySchedulerTest {

    private CpuMemoryScheduler scheduler;
    ClientMongoOperator clientMongoOperator;

    @BeforeEach
    void setUp() {
        clientMongoOperator = mock(ClientMongoOperator.class);
        scheduler = new CpuMemoryScheduler();
        ReflectionTestUtils.setField(scheduler, "clientMongoOperator", clientMongoOperator);
    }

    @Nested
    @DisplayName("Method collectCpuUsage test")
    class collectCpuUsageTest {
        @Test
        @DisplayName("test scheduler annotation")
        void test1() throws NoSuchMethodException {
            Method method = CpuMemoryScheduler.class.getDeclaredMethod("collectCpuUsage");
            Scheduled annotation = method.getAnnotation(Scheduled.class);
            
            assertNotNull(annotation);
            assertEquals("0/10 * * * * ?", annotation.cron());
        }
    }

    @Nested
    @DisplayName("Method reportOnce test")
    class reportOnceTest {
        @BeforeEach
        void setUp() {
            when(clientMongoOperator.postOne(anyMap(), anyString(), any(Class.class))).thenReturn(null);
        }
        @Test
        @DisplayName("test main process")
        void test1() {
            List<String> taskIds = List.of("task1", "task2");
            Map<String, Usage> usageMap = new HashMap<>();
            Usage usage1 = new Usage();
            usage1.setCpuUsage(10.5);
            usage1.setHeapMemoryUsage(1024L);
            usageMap.put("task1", usage1);
            
            AspectTask aspectTask = mock(AspectTask.class);
            
            try (MockedStatic<CpuMemoryCollector> mockedCollector = mockStatic(CpuMemoryCollector.class);
                 MockedStatic<TaskAspectManager> mockedManager = mockStatic(TaskAspectManager.class)) {
                
                mockedCollector.when(() -> CpuMemoryCollector.collectOnce(taskIds))
                        .thenReturn(usageMap);
                mockedManager.when(() -> TaskAspectManager.get("task1"))
                        .thenReturn(aspectTask);
                
                scheduler.reportOnce(taskIds);
                
                mockedCollector.verify(() -> CpuMemoryCollector.collectOnce(taskIds));
                mockedManager.verify(() -> TaskAspectManager.get("task1"));
                verify(aspectTask).onObserveAspect(any(CpuMemUsageAspect.class));
            }
        }

        @Test
        @DisplayName("test with null aspectTask")
        void test2() {
            List<String> taskIds = List.of("task1");
            Map<String, Usage> usageMap = new HashMap<>();
            usageMap.put("task1", new Usage());
            
            try (MockedStatic<CpuMemoryCollector> mockedCollector = mockStatic(CpuMemoryCollector.class);
                 MockedStatic<TaskAspectManager> mockedManager = mockStatic(TaskAspectManager.class)) {
                
                mockedCollector.when(() -> CpuMemoryCollector.collectOnce(taskIds))
                        .thenReturn(usageMap);
                mockedManager.when(() -> TaskAspectManager.get("task1"))
                        .thenReturn(null);
                
                assertDoesNotThrow(() -> scheduler.reportOnce(taskIds));
                
                mockedCollector.verify(() -> CpuMemoryCollector.collectOnce(taskIds));
                mockedManager.verify(() -> TaskAspectManager.get("task1"));
            }
        }

        @Test
        @DisplayName("test with null taskIds")
        void test3() {
            Map<String, Usage> usageMap = new HashMap<>();
            
            try (MockedStatic<CpuMemoryCollector> mockedCollector = mockStatic(CpuMemoryCollector.class)) {
                mockedCollector.when(() -> CpuMemoryCollector.collectOnce(null))
                        .thenReturn(usageMap);
                
                assertDoesNotThrow(() -> scheduler.reportOnce(null));
                
                mockedCollector.verify(() -> CpuMemoryCollector.collectOnce(null));
            }
        }

        @Test
        @DisplayName("test with empty usageMap")
        void test4() {
            List<String> taskIds = List.of("task1");
            Map<String, Usage> usageMap = new HashMap<>();
            
            try (MockedStatic<CpuMemoryCollector> mockedCollector = mockStatic(CpuMemoryCollector.class)) {
                mockedCollector.when(() -> CpuMemoryCollector.collectOnce(taskIds))
                        .thenReturn(usageMap);
                
                assertDoesNotThrow(() -> scheduler.reportOnce(taskIds));
                
                mockedCollector.verify(() -> CpuMemoryCollector.collectOnce(taskIds));
            }
        }
    }
}