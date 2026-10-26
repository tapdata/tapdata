package com.tapdata.tm.task.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
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
        task.setStatus(TaskDto.STATUS_EDIT);
        task.setType("initial_sync+cdc");
        source = new DatabaseNode();
        source.setConnectionId("mysql");
        source.setTableNames(List.of("v_test"));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of(source));
        task.setDag(dag);
        when(metadata.findSourceSchemasForStart(eq("mysql"), any(), eq(user)))
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
        assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
    }

    @Test
    @DisplayName("普通表和视图混选不能静默跳过视图增量")
    void rejectsMixedSelection() {
        source.setTableNames(List.of("test", "v_test"));
        assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
    }

    @Test
    @DisplayName("仅全量视图不受增量校验影响")
    void allowsSnapshotOnly() {
        task.setType(TaskDto.TYPE_INITIAL_SYNC);
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
        verifyNoInteractions(metadata);
    }

    @Test
    @DisplayName("未选中的视图不阻止普通表增量")
    void allowsTables() {
        source.setTableNames(List.of("test"));
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
    }

    @Test
    @DisplayName("表达式匹配到视图时拒绝启动")
    void rejectsExpressionMatchingView() {
        source.setMigrateTableSelectType("expression");
        source.setTableExpression("v_.*");
        assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
    }

    @Test
    @DisplayName("表达式未匹配视图时允许启动")
    void allowsExpressionMatchingOnlyTables() {
        source.setMigrateTableSelectType("expression");
        source.setTableExpression("test");
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
    }

    @Test
    @DisplayName("转换视图默认模式和日志模式的仅增量及全量增量均被拒绝")
    void rejectsTransformationViewCdc() {
        for (String type : List.of("cdc", "initial_sync+cdc")) {
            task.setType(type);
            for (String mode : new String[]{null, "", "log"}) {
                transformationSource("v_test", mode);
                BizException error = assertThrows(BizException.class, () -> service.start(task, user, "00"));
                assertEquals("Task.ViewCdcNotSupported", error.getErrorCode());
                assertArrayEquals(new Object[]{"v_test"}, error.getArgs());
                assertNull(task.getLastStartDate());
            }
        }
    }

    @Test
    @DisplayName("转换视图字段轮询在仅增量及全量增量均放行")
    void allowsTransformationPolling() {
        for (String type : List.of("cdc", "initial_sync+cdc")) {
            task.setType(type);
            transformationSource("v_test", "polling");
            assertDoesNotThrow(() -> service.validateViewCdc(task, user));
        }
        verifyNoInteractions(metadata);
    }

    @Test
    void allowsTransformationSnapshotOnly() {
        transformationSource("v_test", null);
        task.setType(TaskDto.TYPE_INITIAL_SYNC);
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
        verifyNoInteractions(metadata);
    }

    @Test
    void allowsTransformationOrdinaryTableCdc() {
        transformationSource("test", null);
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
    }

    @Test
    @DisplayName("轮询源不能掩盖同任务中另一个非法视图增量源")
    void rejectsNonPollingViewAlongsidePollingSource() {
        TableNode polling = transformationSource("v_polling", "polling");
        TableNode log = new TableNode();
        log.setConnectionId("mysql");
        log.setTableName("v_test");
        when(task.getDag().getSources()).thenReturn(List.of(polling, log));
        BizException error = assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
        assertArrayEquals(new Object[]{"v_test"}, error.getArgs());
    }

    @Test
    void invalidStatusTakesPrecedenceWithoutMetadataQuery() {
        for (String status : List.of(TaskDto.STATUS_RUNNING, TaskDto.STATUS_DELETING)) {
            task.setStatus(status);
            BizException error = assertThrows(BizException.class, () -> service.start(task, user, "00"));
            assertEquals(TaskDto.STATUS_DELETING.equals(status) ? "Task.Deleted" : "Task.StartStatusInvalid", error.getErrorCode());
        }
        verifyNoInteractions(metadata);
    }

    @Test
    void missingAndUnknownMetadataAreNotViews() {
        for (List<MetadataInstancesDto> schemas : List.of(List.<MetadataInstancesDto>of(), List.of(schema("v_test", null)), List.of(schema("v_test", "unknown")))) {
            when(metadata.findSourceSchemasForStart(eq("mysql"), any(), eq(user))).thenReturn(schemas);
            assertDoesNotThrow(() -> service.validateViewCdc(task, user));
        }
    }

    @Test
    void invalidRegexIsConfigurationErrorEvenWithoutModels() {
        source.setMigrateTableSelectType("expression");
        source.setTableExpression("[");
        BizException error = assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
        assertEquals("Task.TableExpressionInvalid", error.getErrorCode());
        verifyNoInteractions(metadata);
    }

    @Test
    void batchesTransformationSourcesPerConnection() {
        TableNode first = transformationSource("test", null);
        TableNode second = new TableNode();
        second.setConnectionId("mysql");
        second.setTableName("second");
        TableNode third = new TableNode();
        third.setConnectionId("other");
        third.setTableName("third");
        when(task.getDag().getSources()).thenReturn(List.of(first, second, third));
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
        verify(metadata).findSourceSchemasForStart(eq("mysql"), argThat(names -> names.size() == 2 && names.containsAll(List.of("test", "second"))), eq(user));
        verify(metadata).findSourceSchemasForStart(eq("other"), eq(List.of("third")), eq(user));
        verifyNoMoreInteractions(metadata);
    }

    @Test
    void expressionUsesKeyAndPartitionSelection() {
        source.setMigrateTableSelectType("expression");
        source.setTableExpression(".*");
        MetadataInstancesDto view = schema("v_test", "view");
        com.tapdata.tm.commons.schema.Field key = new com.tapdata.tm.commons.schema.Field();
        key.setPrimaryKey(true);
        view.setFields(List.of(key));
        when(metadata.findSourceSchemasForStart(eq("mysql"), isNull(), eq(user))).thenReturn(List.of(view));
        source.setNoPrimaryKeyTableSelectType(com.tapdata.tm.commons.util.NoPrimaryKeyTableSelectType.HasKeys.name());
        assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
        source.setNoPrimaryKeyTableSelectType(com.tapdata.tm.commons.util.NoPrimaryKeyTableSelectType.NoKeys.name());
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
        key.setPrimaryKey(false);
        com.tapdata.tm.commons.schema.TableIndex unique = new com.tapdata.tm.commons.schema.TableIndex();
        unique.setUnique(true);
        view.setIndices(List.of(unique));
        source.setNoPrimaryKeyTableSelectType(com.tapdata.tm.commons.util.NoPrimaryKeyTableSelectType.OnlyUniqueIndex.name());
        assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
        source.setNoPrimaryKeyTableSelectType(null);
        source.setSyncSourcePartitionTableEnable(true);
        view.setPartitionInfo(mock(io.tapdata.entity.schema.partition.TapPartition.class));
        view.setPartitionMasterTableId("master");
        assertDoesNotThrow(() -> service.validateViewCdc(task, user));
        view.setPartitionMasterTableId("v_test");
        assertThrows(BizException.class, () -> service.validateViewCdc(task, user));
    }

    private TableNode transformationSource(String table, String mode) {
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        TableNode node = new TableNode();
        node.setConnectionId("mysql");
        node.setTableName(table);
        node.setCdcMode(mode);
        when(task.getDag().getSources()).thenReturn(List.of(node));
        return node;
    }

    private MetadataInstancesDto schema(String name, String type) {
        MetadataInstancesDto schema = new MetadataInstancesDto();
        schema.setOriginalName(name);
        schema.setName(name);
        schema.setMetaType(type);
        return schema;
    }
}
