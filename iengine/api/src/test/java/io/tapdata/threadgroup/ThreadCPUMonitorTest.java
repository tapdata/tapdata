package io.tapdata.threadgroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Class ThreadCPUMonitor Test")
class ThreadCPUMonitorTest {

    private ThreadCPUMonitor monitor;
    private ThreadMXBean threadMXBean;

    @BeforeEach
    void setUp() {
        threadMXBean = mock(ThreadMXBean.class);
        
        try (MockedStatic<ManagementFactory> mockedFactory = mockStatic(ManagementFactory.class)) {
            mockedFactory.when(ManagementFactory::getThreadMXBean).thenReturn(threadMXBean);
            monitor = new ThreadCPUMonitor();
        }
        
        verify(threadMXBean).setThreadCpuTimeEnabled(true);
    }

    @Nested
    @DisplayName("Constructor test")
    class constructorTest {
        @Test
        @DisplayName("test initialization")
        void test1() {
            try (MockedStatic<ManagementFactory> mockedFactory = mockStatic(ManagementFactory.class)) {
                ThreadMXBean mockBean = mock(ThreadMXBean.class);
                mockedFactory.when(ManagementFactory::getThreadMXBean).thenReturn(mockBean);
                
                ThreadCPUMonitor newMonitor = new ThreadCPUMonitor();
                
                verify(mockBean).setThreadCpuTimeEnabled(true);
            }
        }
    }

    @Nested
    @DisplayName("Method getThreadCpuTime test")
    class getThreadCpuTimeTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            long threadId = 123L;
            long expectedTime = 1000000L;
            when(threadMXBean.getThreadCpuTime(threadId)).thenReturn(expectedTime);
            
            long result = monitor.getThreadCpuTime(threadId);
            
            assertEquals(expectedTime, result);
            verify(threadMXBean).getThreadCpuTime(threadId);
        }
    }

    @Nested
    @DisplayName("Method getThreadUserTime test")
    class getThreadUserTimeTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            long threadId = 123L;
            long expectedTime = 800000L;
            when(threadMXBean.getThreadUserTime(threadId)).thenReturn(expectedTime);
            
            long result = monitor.getThreadUserTime(threadId);
            
            assertEquals(expectedTime, result);
            verify(threadMXBean).getThreadUserTime(threadId);
        }
    }

    @Nested
    @DisplayName("Method calculateCpuUsage test")
    class calculateCpuUsageTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            long threadId = 123L;
            Long previousCpuTime = 1000000L;
            long timeIntervalMs = 1000L;
            long currentCpuTime = 2000000L;
            
            when(threadMXBean.getThreadCpuTime(threadId)).thenReturn(currentCpuTime);
            
            double result = monitor.calculateCpuUsage(threadId, previousCpuTime, timeIntervalMs);
            
            assertEquals(0.1, result, 0.001);
            verify(threadMXBean).getThreadCpuTime(threadId);
        }

        @Test
        @DisplayName("test with null previousCpuTime")
        void test2() {
            long threadId = 123L;
            long timeIntervalMs = 1000L;
            long currentCpuTime = 2000000L;
            
            when(threadMXBean.getThreadCpuTime(threadId)).thenReturn(currentCpuTime);
            
            double result = monitor.calculateCpuUsage(threadId, null, timeIntervalMs);
            
            assertEquals(0.2, result, 0.001);
            verify(threadMXBean).getThreadCpuTime(threadId);
        }

        @Test
        @DisplayName("test with zero time interval")
        void test3() {
            long threadId = 123L;
            Long previousCpuTime = 1000000L;
            long timeIntervalMs = 0L;
            long currentCpuTime = 2000000L;
            
            when(threadMXBean.getThreadCpuTime(threadId)).thenReturn(currentCpuTime);
            
            assertDoesNotThrow(() -> monitor.calculateCpuUsage(threadId, previousCpuTime, timeIntervalMs));
        }
    }
}