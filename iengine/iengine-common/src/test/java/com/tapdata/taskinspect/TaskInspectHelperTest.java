package com.tapdata.taskinspect;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/26 14:21 Create
 */
class TaskInspectHelperTest {

    @Nested
    class isIgnoreTaskSyncTypeTest {

        @Test
        void testBlank() {
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(null));
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(""));
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(" "));
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(""));
        }

        @Test
        void testIncludes() {
            Assertions.assertFalse(TaskInspectHelper.isIgnoreTaskSyncType(TaskDto.SYNC_TYPE_SYNC));
            Assertions.assertFalse(TaskInspectHelper.isIgnoreTaskSyncType(TaskDto.SYNC_TYPE_MIGRATE));
        }

        @Test
        void testExcludes() {
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR));
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT));
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(TaskDto.SYNC_TYPE_MEM_CACHE));
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(TaskDto.SYNC_TYPE_TEST_RUN));
            Assertions.assertTrue(TaskInspectHelper.isIgnoreTaskSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA));
        }
    }
}
