package com.tapdata.tm.task.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.task.repository.TaskRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Real task start and metadata query construction; storage is the external boundary. */
@Timeout(10)
class TaskViewCdcStartIT {
    @Test
    @DisplayName("L2：复制和转换任务启动经源模型查询识别视图，在任何任务持久化之前拒绝增量")
    void shouldRejectViewCdcBeforePersistingStart() {
        TaskRepository tasks = mock(TaskRepository.class);
        TaskServiceImpl service = new TaskServiceImpl(tasks);
        MetadataInstancesServiceImpl metadata = spy(new MetadataInstancesServiceImpl(
                mock(MetadataInstancesRepository.class)));
        ReflectionTestUtils.setField(service, "metadataInstancesService", metadata);
        UserDetail user = mock(UserDetail.class);
        MetadataInstancesDto view = new MetadataInstancesDto();
        view.setOriginalName("v_test");
        view.setMetaType("view");
        doAnswer(call -> {
            Query query = call.getArgument(0);
            String filter = query.getQueryObject().toJson();
            assertTrue(filter.contains("view"));
            assertTrue(filter.contains("mysql-source"));
            assertTrue(filter.contains("v_test"));
            assertEquals(1, query.getFieldsObject().get("original_name"));
            assertEquals(1, query.getFieldsObject().get("meta_type"));
            assertEquals(1, query.getFieldsObject().get("fields.primaryKey"));
            assertEquals(1, query.getFieldsObject().get("indices.unique"));
            assertEquals(1, query.getFieldsObject().get("partitionInfo"));
            assertEquals(1, query.getFieldsObject().get("partitionMasterTableId"));
            assertFalse(query.getFieldsObject().containsKey("fields"));
            return List.of(view);
        }).when(metadata).findAllDto(any(Query.class), eq(user));

        DatabaseNode source = new DatabaseNode();
        source.setConnectionId("mysql-source");
        source.setTableNames(List.of("v_test", "base_table"));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of(source));
        TableNode transformSource = new TableNode();
        transformSource.setConnectionId("mysql-source");
        transformSource.setTableName("v_test");
        for (String syncType : List.of(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)) {
            when(dag.getSources()).thenReturn(TaskDto.SYNC_TYPE_MIGRATE.equals(syncType)
                    ? List.of(source) : List.of(transformSource));
            for (String type : List.of("cdc", "initial_sync+cdc")) {
                TaskDto task = new TaskDto();
                task.setSyncType(syncType);
                task.setStatus(TaskDto.STATUS_EDIT);
                task.setType(type);
                task.setDag(dag);
                BizException error = assertThrows(BizException.class, () -> service.start(task, user, "11"));
                assertEquals("Task.ViewCdcNotSupported", error.getErrorCode());
                assertArrayEquals(new Object[]{"v_test"}, error.getArgs());
                assertNull(task.getLastStartDate());
            }
        }
        verifyNoInteractions(tasks);
    }
}
