package io.tapdata.task.skiperrortable;

import com.tapdata.entity.SyncStage;
import com.tapdata.tm.skiperrortable.SkipErrorTableStatusEnum;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableReportVo;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;
import io.tapdata.PDKExCode_10;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/10 18:15 Create
 */
class SkipErrorTableTest {
    static TapCodeException skippableEx = new TapCodeException(PDKExCode_10.WRITE_TYPE);
    static RuntimeException unskippableEx = new RuntimeException();
    static String taskId = "test-task-id";
    static String skippedTable = "skipped_table";
    static String newSkippedTable = "new_skipped_table";
    static String recoveringTable = "recovering_table";
    static List<SkipErrorTableStatusVo> lastTableStatusList = List.of(
        SkipErrorTableStatusVo.create().sourceTable(skippedTable).status(SkipErrorTableStatusEnum.SKIPPED),
        SkipErrorTableStatusVo.create().sourceTable(recoveringTable).status(SkipErrorTableStatusEnum.RECOVERING)
    );

    ObsLogger mockLogger;
    SkipErrorTableStorage mockStorage;
    SkipErrorTable instance;
    Consumer<SkipErrorTableStatusVo> mockVoConsumer;

    @BeforeEach
    void setUp() {
        mockLogger = mock(ObsLogger.class);
        mockStorage = mock(SkipErrorTableStorage.class);
        instance = new SkipErrorTable(taskId, mockLogger, mockStorage);
        mockVoConsumer = mock(Consumer.class);
    }

    @Test
    void test_isSkippedOnCompleted() {
        doReturn(lastTableStatusList).when(mockStorage).getAllTableStatus(eq(taskId));
        instance.initTables(mockVoConsumer);

        assertTrue(instance.isSkippedOnCompleted(skippedTable));
        assertFalse(instance.isSkippedOnCompleted(recoveringTable));
        assertFalse(instance.isSkippedOnCompleted(newSkippedTable));
    }

    @Nested
    class skipTableTest {

        @BeforeEach
        void setUp() {
            instance.setSyncStage(SyncStage.INITIAL_SYNC);
            doReturn(lastTableStatusList).when(mockStorage).getAllTableStatus(eq(taskId));
            instance.initTables(mockVoConsumer);
        }

        @Test
        void test_unskippable() {
            assertFalse(instance.skipTable(newSkippedTable, unskippableEx, () -> null));
        }

        @Test
        void test_skippable() {
            SkipErrorTableReportVo mockVo = mock(SkipErrorTableReportVo.class);
            assertTrue(instance.skipTable(newSkippedTable, skippableEx, () -> mockVo));
            verify(mockStorage).reportTableSkipped(eq(taskId), eq(mockVo));
        }

        @Test
        void test_inCDCStage() {
            instance.setSyncStage(SyncStage.CDC);
            assertTrue(instance.skipTable(skippedTable, skippableEx, () -> null), "增量时，已跳过的表返回 true");
            assertFalse(instance.skipTable(newSkippedTable, skippableEx, () -> null), "增量时，未跳过的表返回 false（未支持增量错误表跳过）");
        }
    }

}
