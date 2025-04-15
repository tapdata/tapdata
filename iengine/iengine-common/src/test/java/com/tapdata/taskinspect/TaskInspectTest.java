package com.tapdata.taskinspect;

import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/14 15:01 Create
 */
class TaskInspectTest {

    interface RunAndThrow<T> {
        void runAndThrow(T v) throws Exception;
    }

    String taskId = "test-task-id";

    @Nested
    class ConstructorTest {

        @Mock
        TaskInspectContext context;
        @Mock
        IOperator operator;
        @Mock
        IMode mode;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.openMocks(this);

            doReturn(taskId).when(context).getTaskId();
            doReturn(true).when(mode).stop();
        }

        @Test
        void test() throws Exception {
            try (TaskInspect taskInspect = new TaskInspect(context, operator)) {

                // Assert
                assertNotNull(taskInspect);
                verify(operator).getConfig(eq(context.getTaskId()));
            }
        }
    }

    @Nested
    class InitTest {
        @Mock
        TaskInspectContext context;
        @Mock
        IOperator operator;
        @Mock
        TaskInspectConfig config;
        @Mock
        IMode mode;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.openMocks(this);

            doReturn(taskId).when(context).getTaskId();
            doReturn(true).when(mode).stop();
        }

        void test(RunAndThrow<TaskInspect> consumer) throws Exception {
            try (TaskInspect taskInspect = mock(TaskInspect.class, CALLS_REAL_METHODS)) {
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "context", context);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "operator", operator);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "modeJob", mode);
                consumer.runAndThrow(taskInspect);
            }
        }

        @Test
        void testInit_Normal() throws Exception {
            // Arrange
            doReturn(config).when(operator).getConfig(anyString());
            doReturn(true).when(context).isStopping();

            test(taskInspect -> {
                // Act
                taskInspect.init();

                // Assert
                verify(operator).getConfig(eq(context.getTaskId()));
                verify(taskInspect).refresh(eq(config));
            });
        }

        @Test
        void testInit_ConfigNull() throws Exception {
            // Arrange
            when(operator.getConfig(anyString())).thenReturn(null);

            test(taskInspect -> {
                // Act
                taskInspect.init();

                // Assert
                verify(operator).getConfig(eq(context.getTaskId()));
                verify(taskInspect, never()).refresh(any());
            });
        }

        @Test
        void testInit_Exception() throws Exception {
            // Arrange
            doReturn(config).when(operator).getConfig(anyString());

            test(taskInspect -> {
                doThrow(new RuntimeException("Test Exception")).when(taskInspect).refresh(eq(config));

                // Act
                taskInspect.init();

                // Assert
                verify(operator).getConfig(eq(context.getTaskId()));
                verify(taskInspect).refresh(any());
            });
        }

        @Test
        void testInit_InterruptedException() throws Exception {
            // Arrange
            doReturn(config).when(operator).getConfig(anyString());

            test(taskInspect -> {
                doThrow(new InterruptedException("Test Interrupted Exception")).when(taskInspect).refresh(eq(config));

                // Act
                taskInspect.init();

                // Assert
                verify(operator).getConfig(eq(context.getTaskId()));
                verify(taskInspect).refresh(any());
                assertTrue(Thread.currentThread().isInterrupted());
            });
        }
    }

    @Nested
    class RefreshTest {
        @Mock
        TaskInspectContext context;
        @Mock
        IOperator operator;
        @Mock
        TaskInspectConfig config;
        @Mock
        IMode mode;
        @Mock
        IMode newMode;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.openMocks(this);

            doReturn(taskId).when(context).getTaskId();
            doReturn(true).when(mode).stop();
            doReturn(true).when(newMode).stop();
        }

        void test(RunAndThrow<TaskInspect> consumer) throws Exception {
            try (TaskInspect taskInspect = mock(TaskInspect.class, CALLS_REAL_METHODS)) {
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "context", context);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "operator", operator);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "modeJob", mode);
                consumer.runAndThrow(taskInspect);
            }
        }

        @Test
        void testRefresh_ModeSame() throws Exception {
            // Arrange
            doReturn(TaskInspectMode.CLOSE).when(mode).getMode();
            doReturn(TaskInspectMode.CLOSE).when(config).getMode();

            try (MockedStatic<TaskInspect> taskInspectMockedStatic = mockStatic(TaskInspect.class)) {
                taskInspectMockedStatic.when(() -> TaskInspect.create(any(TaskInspectMode.class), any(TaskInspectContext.class), any(IOperator.class)))
                    .thenReturn(mode);
                test(taskInspect -> {
                    // Act
                    taskInspect.refresh(config);

                    // Assert
                    verify(config).init(eq(-1));
                    verify(mode, never()).stop();
                    verify(mode).refresh(eq(config));
                });
            }
        }

        @Test
        void testRefresh_ModeDifferent() throws Exception {
            // Arrange
            doReturn(TaskInspectMode.CLOSE).when(mode).getMode();
            doReturn(TaskInspectMode.CUSTOM).when(config).getMode();

            try (MockedStatic<TaskInspect> taskInspectMockedStatic = mockStatic(TaskInspect.class)) {
                taskInspectMockedStatic.when(() -> TaskInspect.create(any(TaskInspectMode.class), any(TaskInspectContext.class), any(IOperator.class)))
                    .thenReturn(newMode);
                test(taskInspect -> {
                    // Act
                    taskInspect.refresh(config);

                    // Assert
                    verify(config).init(eq(-1));
                    verify(mode).stop();
                    verify(newMode).refresh(eq(config));
                });
            }
        }

//        @Test
//        void testRefresh_InterruptedException() throws Exception {
//            // Arrange
//            TaskInspectMode currentMode = TaskInspectMode.CLOSE;
//            TaskInspectMode newMode = TaskInspectMode.OPEN;
//            doReturn(currentMode).when(mode).getMode();
//            doReturn(newMode).when(config).getMode();
//            doThrow(new InterruptedException("Test Interrupted Exception")).when(TaskInspectUtils.class).stop(any(Runnable.class), eq(TaskInspect.MAX_TIMEOUT));
//
//            test(taskInspect -> {
//                // Act
//                taskInspect.refresh(config);
//
//                // Assert
//                verify(mode).stop();
//                verify(TaskInspectUtils.class).stop(any(Runnable.class), eq(TaskInspect.MAX_TIMEOUT));
//                verify(taskInspect, never()).create(any(TaskInspectMode.class), any(TaskInspectContext.class), any(IOperator.class));
//                verify(newMode, never()).refresh(config);
//                assertTrue(Thread.currentThread().isInterrupted());
//            });
//        }
    }

}
