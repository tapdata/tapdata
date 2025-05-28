package io.tapdata.task.skiperrorevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class SkipErrorEventAspectTaskTest {
    private SkipErrorEventAspectTask skipErrorEventAspectTask;
    private ScheduledExecutorService EXECUTOR;
    private SplitFileLogger logger;

    @BeforeEach
    void setUp() {
        skipErrorEventAspectTask = new SkipErrorEventAspectTask();
        EXECUTOR = spy(Executors.newScheduledThreadPool(1));
        this.logger = mock(SplitFileLogger.class);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "EXECUTOR", EXECUTOR);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "logger", logger);
    }

    @Nested
    class ExecutorShutdownTest {
        @Test
        void shutdownExecutor_WhenExecutorIsNull_ShouldDoNothing() {
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "EXECUTOR", null);

            assertDoesNotThrow(() -> skipErrorEventAspectTask.shutdownExecutor());
        }

        @Test
        void shutdownExecutor_WhenExecutorAlreadyShutdown_ShouldDoNothing() {

            when(EXECUTOR.isShutdown()).thenReturn(true);

            skipErrorEventAspectTask.shutdownExecutor();
            verify(EXECUTOR, never()).shutdown();
            verify(EXECUTOR, never()).shutdownNow();
        }

        @Test
        void shutdownExecutor_WhenNormalShutdown_ShouldSucceed() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

            skipErrorEventAspectTask.shutdownExecutor();

            verify(EXECUTOR).shutdown();
            verify(EXECUTOR, never()).shutdownNow();
        }

        @Test
        void shutdownExecutor_WhenFirstAwaitFails_ShouldCallShutdownNow() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS))
                    .thenReturn(false)
                    .thenReturn(true);

            skipErrorEventAspectTask.shutdownExecutor();

            verify(EXECUTOR).shutdown();
            verify(EXECUTOR).shutdownNow();
        }

        @Test
        void shutdownExecutor_WhenBothAwaitsFail_ShouldLogError() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(false);

            skipErrorEventAspectTask.shutdownExecutor();
            verify(logger).error(eq("shutdown executor failed"));
        }

        @Test
        void shutdownExecutor_WhenInterrupted_ShouldPreserveInterruptStatus() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS))
                    .thenThrow(new InterruptedException());

            skipErrorEventAspectTask.shutdownExecutor();

            verify(EXECUTOR).shutdownNow();
            assertTrue(Thread.currentThread().isInterrupted());

            Thread.interrupted();
        }
    }
}
