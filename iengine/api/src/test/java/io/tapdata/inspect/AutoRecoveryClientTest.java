package io.tapdata.inspect;


import com.tapdata.entity.TapdataRecoveryEvent;
import io.tapdata.utils.EngineHelper;
import io.tapdata.utils.VfsHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0   Create
 */
@ExtendWith(MockitoExtension.class)
class AutoRecoveryClientTest {

    String taskId = "test-task-id";
    String inspectTaskId = "test-inspect-task-id";
    String manualId = "test-manual-id";
    @Mock
    Consumer<TapdataRecoveryEvent> enqueueConsumer;
    @Mock
    Consumer<TapdataRecoveryEvent> completedConsumer;

    AutoRecoveryClient ins;
    AutoRecoveryClient spyIns;

    @BeforeEach
    void setUp() {
        ins = new AutoRecoveryClient(taskId, inspectTaskId, enqueueConsumer, completedConsumer) {
            @Override
            public void close() throws Exception {

            }
        };
        spyIns = spy(ins);
    }


    @Nested
    class exportRecoverSqlTest {

        @Test
        void testNullRecoverSqlFile() {
            // 模拟数据
            TapdataRecoveryEvent event = TapdataRecoveryEvent.createBegin(inspectTaskId);

            // 执行逻辑
            spyIns.exportRecoverSql(event);

            // 验证结果
            assertDoesNotThrow(() -> verify(spyIns, never()).appendRecoverSqlBegin(anyString(), anyString()));
        }

        @Test
        void testNullManualId() {
            // 模拟数据
            TapdataRecoveryEvent event = TapdataRecoveryEvent.createBegin(inspectTaskId).ofTaskInspectRecoverSql(taskId, null);

            // 执行逻辑
            spyIns.exportRecoverSql(event);

            // 验证结果
            assertDoesNotThrow(() -> verify(spyIns, never()).appendRecoverSqlBegin(anyString(), anyString()));
        }

        @Test
        void testBegin() {
            // 模拟数据
            TapdataRecoveryEvent event = TapdataRecoveryEvent.createBegin(inspectTaskId).ofTaskInspectRecoverSql(taskId, manualId);

            assertDoesNotThrow(() ->doNothing().when(spyIns).deleteRecoverSqlHistories(anyString(), eq(manualId)));
            assertDoesNotThrow(() ->doNothing().when(spyIns).appendRecoverSqlBegin(anyString(), eq(manualId)));

            // 执行逻辑
            spyIns.exportRecoverSql(event);

            // 验证结果
            assertDoesNotThrow(() -> verify(spyIns).deleteRecoverSqlHistories(anyString(), anyString()));
            assertDoesNotThrow(() -> verify(spyIns).appendRecoverSqlBegin(anyString(), anyString()));
        }

        @Test
        void testEnd() {
            // 模拟数据
            TapdataRecoveryEvent event = TapdataRecoveryEvent.createEnd(inspectTaskId).ofTaskInspectRecoverSql(taskId, manualId);

            assertDoesNotThrow(() ->doNothing().when(spyIns).appendRecoverSqlEnd(anyString()));

            // 执行逻辑
            spyIns.exportRecoverSql(event);

            // 验证结果
            assertDoesNotThrow(() -> verify(spyIns).appendRecoverSqlEnd(anyString()));
        }

        @Test
        void testData() throws Exception {
            // 模拟数据
            String tableId = "test-table-id";
            String rowId = "test-row-id";
            Map<String, Object> data = Map.of("id", 1);
            TapdataRecoveryEvent event = TapdataRecoveryEvent.createInsert(inspectTaskId, tableId, rowId, data).ofTaskInspectRecoverSql(taskId, manualId);

            VfsHelper vfsHelper = mock(VfsHelper.class);

            try (MockedStatic<EngineHelper> mockedStatic = mockStatic(EngineHelper.class)) {
                mockedStatic.when(EngineHelper::vfs).thenReturn(vfsHelper);

                // 执行逻辑
                spyIns.exportRecoverSql(event);

                // 验证结果
                verify(vfsHelper).append(anyString(), anyCollection());
            }
        }
    }

    @Test
    void testAppendRecoverSqlBegin() throws Exception {
        // 模拟数据
        String filepath = "test-file-path.sql";
        VfsHelper vfsHelper = mock(VfsHelper.class);

        try (MockedStatic<EngineHelper> mockedStatic = mockStatic(EngineHelper.class)) {
            mockedStatic.when(EngineHelper::vfs).thenReturn(vfsHelper);

            // 执行逻辑
            spyIns.appendRecoverSqlBegin(filepath, manualId);

            // 验证结果
            verify(vfsHelper).append(eq(filepath), anyCollection());
        }
    }

    @Test
    void testAppendRecoverSqlEnd() throws Exception {
        // 模拟数据
        String filepath = "test-file-path.sql";
        VfsHelper vfsHelper = mock(VfsHelper.class);

        try (MockedStatic<EngineHelper> mockedStatic = mockStatic(EngineHelper.class)) {
            mockedStatic.when(EngineHelper::vfs).thenReturn(vfsHelper);

            // 执行逻辑
            spyIns.appendRecoverSqlEnd(filepath);

            // 验证结果
            verify(vfsHelper).append(eq(filepath), anyCollection());
        }
    }

    @Test
    void testDeleteRecoverSqlHistories0() throws Exception {
        // 模拟数据
        String filepath = "test-file-path.sql";
        VfsHelper vfsHelper = mock(VfsHelper.class);

        try (MockedStatic<EngineHelper> mockedStatic = mockStatic(EngineHelper.class)) {
            mockedStatic.when(EngineHelper::vfs).thenReturn(vfsHelper);

            // 执行逻辑
            spyIns.deleteRecoverSqlHistories(filepath, manualId);

            // 验证结果
            verify(vfsHelper).deleteFrom3DaysAgo(eq(filepath), eq(true));
        }
    }

    @Test
    void testDeleteRecoverSqlHistories1() throws Exception {
        // 模拟数据
        String filepath = "test-file-path.sql";
        VfsHelper vfsHelper = mock(VfsHelper.class);

        try (MockedStatic<EngineHelper> mockedStatic = mockStatic(EngineHelper.class)) {
            mockedStatic.when(EngineHelper::vfs).thenReturn(vfsHelper);
            doReturn(1).when(vfsHelper).deleteFrom3DaysAgo(eq(filepath), eq(true));

            // 执行逻辑
            spyIns.deleteRecoverSqlHistories(filepath, manualId);

            // 验证结果
            verify(vfsHelper).deleteFrom3DaysAgo(eq(filepath), eq(true));
        }
    }
}
