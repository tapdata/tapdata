package com.tapdata.taskinspect;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/26 14:21 Create
 */
class TaskInspectHelperTest {

    @Nested
    class createTest {
        @Test
        @DisplayName("多源节点返回 null")
        void testMultiSource() {
            ObjectId taskId = ObjectId.get();
            DAG dag = Mockito.mock(DAG.class);
            List<Node> sourceNodes = new ArrayList<>();
            sourceNodes.add(Mockito.mock(Node.class));
            sourceNodes.add(Mockito.mock(Node.class));
            TaskDto taskDto = Mockito.mock(TaskDto.class);

            Mockito.doReturn(taskId).when(taskDto).getId();
            Mockito.doReturn(TaskDto.SYNC_TYPE_SYNC).when(taskDto).getSyncType();
            Mockito.doReturn(dag).when(taskDto).getDag();
            Mockito.doReturn(sourceNodes).when(dag).getSourceNodes();

            Assertions.assertNull(TaskInspectHelper.create(taskDto, null));
        }

    }

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
