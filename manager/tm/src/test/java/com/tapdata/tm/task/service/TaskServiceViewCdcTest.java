package com.tapdata.tm.task.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.task.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskServiceViewCdcTest {
    private TaskServiceImpl service;
    private MetadataInstancesServiceImpl metadata;
    private UserDetail user;
    private TaskDto task;
    private DatabaseNode source;

    @BeforeEach
    void setUp() {
        service = new TaskServiceImpl(mock(TaskRepository.class));
        metadata = mock(MetadataInstancesServiceImpl.class);
        ReflectionTestUtils.setField(service, "metadataInstancesService", metadata);
        user = mock(UserDetail.class);
        task = new TaskDto();
        task.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        task.setType("initial_sync+cdc");
        source = new DatabaseNode();
        source.setConnectionId("mysql");
        source.setTableNames(List.of("v_test"));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of(source));
        task.setDag(dag);
        when(metadata.findSourceSchemaBySourceId(eq("mysql"), any(), eq(user)))
                .thenReturn(List.of(schema("v_test", "view"), schema("test", "table")));
    }

    @Test
    @DisplayName("视图全量加增量在实际启动入口被拒绝，未进入启动副作用")
    void rejectsViewBeforeStartingTask() {
        BizException error = assertThrows(BizException.class, () -> service.start(task, user, "00"));
        assertEquals("Task.ViewCdcNotSupported", error.getErrorCode());
        assertArrayEquals(new Object[]{"v_test"}, error.getArgs());
        assertNull(task.getLastStartDate());
    }

    @Test
    @DisplayName("仅增量的视图任务也被拒绝")
    void rejectsCdcOnly() {
        task.setType("cdc");
        assertThrows(BizException.class, () -> service.validateMigrateViewCdc(task, user));
    }

    @Test
    @DisplayName("普通表和视图混选不能静默跳过视图增量")
    void rejectsMixedSelection() {
        source.setTableNames(List.of("test", "v_test"));
        assertThrows(BizException.class, () -> service.validateMigrateViewCdc(task, user));
    }

    @Test
    @DisplayName("仅全量视图不受增量校验影响")
    void allowsSnapshotOnly() {
        task.setType(TaskDto.TYPE_INITIAL_SYNC);
        assertDoesNotThrow(() -> service.validateMigrateViewCdc(task, user));
        verifyNoInteractions(metadata);
    }

    @Test
    @DisplayName("未选中的视图不阻止普通表增量")
    void allowsTables() {
        source.setTableNames(List.of("test"));
        assertDoesNotThrow(() -> service.validateMigrateViewCdc(task, user));
    }

    @Test
    @DisplayName("表达式匹配到视图时拒绝启动")
    void rejectsExpressionMatchingView() {
        source.setMigrateTableSelectType("expression");
        source.setTableExpression("v_.*");
        assertThrows(BizException.class, () -> service.validateMigrateViewCdc(task, user));
    }

    @Test
    @DisplayName("表达式未匹配视图时允许启动")
    void allowsExpressionMatchingOnlyTables() {
        source.setMigrateTableSelectType("expression");
        source.setTableExpression("test");
        assertDoesNotThrow(() -> service.validateMigrateViewCdc(task, user));
    }

    @Test
    @DisplayName("数据转换字段轮询不被数据复制专用规则拒绝")
    void leavesTransformationUnchanged() {
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        assertDoesNotThrow(() -> service.validateMigrateViewCdc(task, user));
        verifyNoInteractions(metadata);
    }

    private MetadataInstancesDto schema(String name, String type) {
        MetadataInstancesDto schema = new MetadataInstancesDto();
        schema.setOriginalName(name);
        schema.setName(name);
        schema.setMetaType(type);
        return schema;
    }
}
