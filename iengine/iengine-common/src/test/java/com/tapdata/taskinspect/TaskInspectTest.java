package com.tapdata.taskinspect;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.taskinspect.mock.SampleMode;
import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/14 15:01 Create
 */
@ExtendWith(MockitoExtension.class)
class TaskInspectTest {

    interface ExConsumer<T> {
        void accept(T v) throws Exception;
    }

    @Mock
    TaskInspectContext context;
    @Mock
    IOperator operator;
    @Mock
    IMode mode;

    String taskId = "test-task-id";

    @BeforeEach
    void setUp() {
        reset(context, operator, mode);
    }


    @Nested
    class ConstructorTest {

        @BeforeEach
        void setUp() {
            doReturn(taskId).when(context).getTaskId();
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
        TaskInspectConfig config;

        @BeforeEach
        void setUp() {
            doReturn(taskId).when(context).getTaskId();
            doReturn(true).when(mode).stop();
            reset(config);
        }

        void test(ExConsumer<TaskInspect> consumer) throws Exception {
            try (TaskInspect taskInspect = mock(TaskInspect.class, CALLS_REAL_METHODS)) {
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "context", context);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "operator", operator);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "modeJob", mode);
                consumer.accept(taskInspect);
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
        TaskInspectConfig config;
        @Mock
        IMode newMode;

        @BeforeEach
        void setUp() {
            doReturn(taskId).when(context).getTaskId();
            doReturn(true).when(mode).stop();
            doReturn(true).when(newMode).stop();
            reset(config, newMode);
        }

        void test(ExConsumer<TaskInspect> consumer) throws Exception {
            try (TaskInspect taskInspect = mock(TaskInspect.class, CALLS_REAL_METHODS)) {
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "context", context);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "operator", operator);
                UnitTestUtils.injectField(TaskInspect.class, taskInspect, "modeJob", mode);
                consumer.accept(taskInspect);
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
                doReturn(true).when(newMode).stop();

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
    }

    @Nested
    class createTest {

        @Mock
        TaskInspectMode taskInspectMode;

        @BeforeEach
        void setUp() {
            reset(taskInspectMode);
        }

        void testFailure(String type) {
            // 预定义
            doReturn(SampleMode.class.getName()).when(taskInspectMode).getImplClassName();
            doReturn(type).when(context).getTaskId();

            // 使用 try-with-resources 来捕获 MockedStatic
            try (MockedStatic<Log4jUtil> log4jUtilMock = mockStatic(Log4jUtil.class)) {
                // 行为
                IMode result = TaskInspect.create(taskInspectMode, context, operator);

                // 预期检查
                assertNotNull(result);
                assertInstanceOf(IMode.class, result);
                log4jUtilMock.verify(() -> Log4jUtil.getStackString(any(Throwable.class)));
            }
        }

        @Test
        void testCreate_ModeClose() {
            // 预定义
            when(taskInspectMode.getImplClassName()).thenReturn(TaskInspectMode.CLOSE.getImplClassName());

            // 行为
            IMode result = TaskInspect.create(taskInspectMode, context, operator);

            // 预期检查
            assertNotNull(result);
        }

        @Test
        void testCreate_ModeCustom_Success() {
            // 预定义
            String className = SampleMode.class.getName();
            when(taskInspectMode.getImplClassName()).thenReturn(className);

            // 行为
            IMode result = TaskInspect.create(taskInspectMode, context, operator);

            // 预期检查
            assertNotNull(result);
            assertInstanceOf(SampleMode.class, result);
        }

        @Test
        void testCreate_ModeCustom_ClassNotFoundException() {
            testFailure("ClassNotFoundException");
        }

        @Test
        void testCreate_ModeCustom_NoSuchMethodException() {
            testFailure("NoSuchMethodException");
        }

        @Test
        void testCreate_ModeCustom_InstantiationException() {
            testFailure("InstantiationException");
        }

        @Test
        void testCreate_ModeCustom_IllegalAccessException() {
            testFailure("IllegalAccessException");
        }

        @Test
        void testCreate_ModeCustom_InvocationTargetException() {
            testFailure("InvocationTargetException");
        }
    }

}
