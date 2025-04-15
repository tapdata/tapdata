package com.tapdata.tm.taskinspect;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/15 17:22 Create
 */
@ExtendWith(MockitoExtension.class)
class TaskInspectUtilsTest {

    @Mock
    AutoCloseable closeable1;
    @Mock
    AutoCloseable closeable2;
    @Mock
    BooleanSupplier stopSupplier;

    @BeforeEach
    void setUp() {
        reset(closeable1, closeable2, stopSupplier);
    }

    @Nested
    class closeTest {

        @Test
        void testClose_NoCloseables() throws Exception {
            TaskInspectUtils.close();
            verifyNoInteractions(closeable1, closeable2);
        }

        @Test
        void testClose_OneCloseable() throws Exception {
            TaskInspectUtils.close(closeable1);
            verify(closeable1).close();
            verifyNoInteractions(closeable2);
        }

        @Test
        void testClose_MultipleCloseables() throws Exception {
            TaskInspectUtils.close(closeable1, closeable2);
            verify(closeable1).close();
            verify(closeable2).close();
        }

        @Test
        void testClose_ExceptionInFirstCloseable() throws Exception {
            Exception e1 = new Exception("Exception in closeable1");
            doThrow(e1).when(closeable1).close();
            Exception e2 = new Exception("Exception in closeable2");
            doThrow(e2).when(closeable2).close();

            Exception thrown = assertThrows(Exception.class, () -> TaskInspectUtils.close(closeable1, closeable2));
            assertEquals(e1, thrown);
            assertTrue(thrown.getSuppressed()[0] == e2);
        }

        @Test
        void testClose_ExceptionInSecondCloseable() throws Exception {
            Exception e2 = new Exception("Exception in closeable2");
            doThrow(e2).when(closeable2).close();

            Exception thrown = assertThrows(Exception.class, () -> TaskInspectUtils.close(closeable1, closeable2));
            assertEquals(e2, thrown);
            verify(closeable1).close();
        }
    }

    @Nested
    class stopTest {

        @Test
        void testStop_AlreadyStopped() throws InterruptedException {
            when(stopSupplier.getAsBoolean()).thenReturn(true);

            TaskInspectUtils.stop(stopSupplier, 1000);

            verify(stopSupplier).getAsBoolean();
        }

        @Test
        void testStop_StopWithinTimeout() throws InterruptedException {
            when(stopSupplier.getAsBoolean()).thenReturn(false, false, true);

            TaskInspectUtils.stop(stopSupplier, 3000);

            verify(stopSupplier, times(3)).getAsBoolean();
        }

        @Test
        void testStop_Timeout() throws InterruptedException {
            when(stopSupplier.getAsBoolean()).thenReturn(false);

            Exception thrown = assertThrows(RuntimeException.class, () -> TaskInspectUtils.stop(stopSupplier, 1000));
            assertNotNull(thrown.getMessage());
            assertTrue(thrown.getMessage().startsWith("Timeout waiting"));

            verify(stopSupplier, times(2)).getAsBoolean(); // 1000ms / 500ms (sleep time) = 2
        }
    }

    @Nested
    class submitTest {

        @Test
        void testSubmit_Runnable() throws ExecutionException, InterruptedException {
            Runnable runnable = mock(Runnable.class);
            Future<?> future = TaskInspectUtils.submit(runnable);

            assertNotNull(future);
            future.get(); // This will block until the task is complete
            verify(runnable).run();
        }
    }
}
