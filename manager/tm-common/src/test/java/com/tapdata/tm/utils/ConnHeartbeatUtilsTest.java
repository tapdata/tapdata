package com.tapdata.tm.utils;

import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ConnHeartbeatUtilsTest {
    @Nested
    class CheckTask{
        @Test
        void testCacheTask(){
            Assertions.assertTrue(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC_CDC, TaskDto.SYNC_TYPE_MEM_CACHE));
        }

        @Test
        void testCacheTask_initial_sync(){
            Assertions.assertFalse(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC, TaskDto.SYNC_TYPE_MEM_CACHE));
        }

        @Test
        void testMigrateTask(){
            Assertions.assertTrue(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC_CDC, TaskDto.SYNC_TYPE_MIGRATE));
        }

        @Test
        void testMigrateTask_initial_sync(){
            Assertions.assertFalse(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC, TaskDto.SYNC_TYPE_MIGRATE));
        }

        @Test
        void testSyncTask(){
            Assertions.assertTrue(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC_CDC, TaskDto.SYNC_TYPE_SYNC));
        }

        @Test
        void testSyncTask_initial_sync(){
            Assertions.assertFalse(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC, TaskDto.SYNC_TYPE_SYNC));
        }

        @Test
        void testLogCollectorTask(){
            Assertions.assertTrue(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC_CDC, TaskDto.SYNC_TYPE_LOG_COLLECTOR));
        }

        @Test
        void testLogCollectorTask_initial_sync(){
            Assertions.assertFalse(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC, TaskDto.SYNC_TYPE_LOG_COLLECTOR));
        }

        @Test
        void testConnHeartbeatTask(){
            Assertions.assertFalse(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC_CDC, TaskDto.SYNC_TYPE_CONN_HEARTBEAT));
        }

        @Test
        void testTestRunTask(){
            Assertions.assertFalse(ConnHeartbeatUtils.checkTask(ParentTaskDto.TYPE_INITIAL_SYNC_CDC, TaskDto.SYNC_TYPE_TEST_RUN));
        }
    }
}
