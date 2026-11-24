package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.impl.TaskSaveServiceImpl;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * TAP-13317: saving a stopped migrate task must report the tables newly selected by the edit
 * ({@code ldpNewTables}) so the engine runs their initial sync on restart. Real task save checks,
 * real task setting sync and table expression filtering; metadata storage, run-table measurement
 * and schema deduction are the external boundaries.
 */
@Timeout(10)
class TaskExpressionNewTableIT {
    private static final String CONNECTION_ID = "dameng-source";
    private static final String TASK_RECORD_ID = "record-1";

    private TaskServiceImpl service;
    private MetadataInstancesService metadata;
    private MeasurementServiceV2 measurement;
    private UserDetail user;
    private ObjectId taskId;

    @BeforeEach
    void setUp() {
        service = spy(new TaskServiceImpl(mock(TaskRepository.class)));
        metadata = mock(MetadataInstancesService.class);
        measurement = mock(MeasurementServiceV2.class);
        user = mock(UserDetail.class);
        taskId = new ObjectId();

        TaskSaveServiceImpl taskSaveService = new TaskSaveServiceImpl();
        taskSaveService.setMetadataInstancesService(metadata);
        ReflectionTestUtils.setField(service, "taskSaveService", taskSaveService);
        ReflectionTestUtils.setField(service, "measurementServiceV2", measurement);
        ReflectionTestUtils.setField(service, "dateNodeService", mock(DateNodeService.class));
        ReflectionTestUtils.setField(service, "transformSchemaAsyncService", mock(TransformSchemaAsyncService.class));
        ReflectionTestUtils.setField(service, "transformSchemaService", mock(TransformSchemaService.class));
        ReflectionTestUtils.setField(service, "workerService", mock(WorkerService.class));

        when(metadata.findBySourceIdAndTableNameListNeTaskId(eq(CONNECTION_ID), isNull(), eq(user)))
                .thenReturn(List.of(sourceTable("T1"), sourceTable("T2"), sourceTable("OTHER")));
        when(metadata.findByTaskId(anyString(), any())).thenReturn(List.of());
    }

    private static MetadataInstancesDto sourceTable(String name) {
        MetadataInstancesDto table = new MetadataInstancesDto();
        table.setOriginalName(name);
        table.setName(name);
        table.setMetaType("table");
        return table;
    }

    private static DAG dag(Map<String, Object> source) {
        Map<String, Object> target = Map.of("id", "target", "type", "database", "connectionId", "doris-target");
        String json = JsonUtil.toJsonUseJackson(Map.of(
                "nodes", List.of(source, target),
                "edges", List.of(Map.of("source", "source", "target", "target"))));
        return DAG.build(JsonUtil.parseJsonUseJackson(json, Dag.class));
    }

    private static Map<String, Object> expressionSource(String expression, List<String> staleTableNames) {
        Map<String, Object> source = new HashMap<>();
        source.put("id", "source");
        source.put("type", "database");
        source.put("connectionId", CONNECTION_ID);
        source.put("migrateTableSelectType", "expression");
        source.put("tableExpression", expression);
        source.put("tableNames", staleTableNames);
        return source;
    }

    private TaskDto storedTask(Map<String, Object> source) {
        TaskDto old = new TaskDto();
        old.setId(taskId);
        old.setName("dameng-to-doris");
        old.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        old.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
        old.setTaskRecordId(TASK_RECORD_ID);
        old.setEditVersion("1");
        old.setDag(dag(source));
        doReturn(old).when(service).findById(taskId);
        return old;
    }

    private TaskDto editedTask(Map<String, Object> source) {
        TaskDto edited = new TaskDto();
        edited.setId(taskId);
        edited.setName("dameng-to-doris");
        edited.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
        edited.setDag(dag(source));
        return edited;
    }

    private static Map<String, Object> syncProgressWithCompletedTables(String... tables) {
        Map<String, Object> batchOffset = new HashMap<>();
        for (String table : tables) {
            Map<String, Object> offset = new HashMap<>();
            offset.put("batch_read_connector_status", "OVER");
            batchOffset.put(table, offset);
        }
        byte[] bytes = InstanceFactory.instance(ObjectSerializable.class).fromObject(batchOffset);
        String progress = JsonUtil.toJsonUseJackson(Map.of(
                "syncStage", "CDC",
                "batchOffset", TaskServiceImpl.ENCODE_PREFIX + Base64.getEncoder().encodeToString(bytes)));
        Map<String, Object> syncProgress = new LinkedHashMap<>();
        syncProgress.put("[\"source\",\"target\"]", progress);
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("syncProgress", syncProgress);
        return attrs;
    }

    @Test
    @DisplayName("L1：正则从 ^T1$ 改为 ^T(1|2)$ 且已运行 T1，保存后新增表为 T2，引擎重启据此补全量")
    void shouldReportTableNewlyMatchedByExpressionAgainstRunTables() {
        storedTask(expressionSource("^T1$", List.of("T1")));
        when(measurement.findRunTable(taskId.toHexString(), TASK_RECORD_ID)).thenReturn(new ArrayList<>(List.of("T1")));
        TaskDto edited = editedTask(expressionSource("^T(1|2)$", List.of("T1")));

        service.checkTask(edited, user, false);

        assertEquals(List.of("T2"), edited.getLdpNewTables());
        assertEquals(List.of("T1", "T2"), edited.getDag().getSourceNode().getFirst().getTableNames());
    }

    @Test
    @DisplayName("L1：无表级运行度量时按全量断点 batchOffset 计算，正则新增匹配的 T2 进入新增表")
    void shouldReportTableNewlyMatchedByExpressionAgainstBatchOffset() {
        storedTask(expressionSource("^T1$", List.of("T1")));
        when(measurement.findRunTable(taskId.toHexString(), TASK_RECORD_ID)).thenReturn(new ArrayList<>());
        TaskDto edited = editedTask(expressionSource("^T(1|2)$", List.of("T1")));
        edited.setAttrs(syncProgressWithCompletedTables("T1"));

        service.checkTask(edited, user, false);

        assertEquals(List.of("T2"), edited.getLdpNewTables());
    }

    @Test
    @DisplayName("L1：手动选表模式新增 T2 且已运行 T1，新增表仍为 T2（行为不变）")
    void shouldKeepCustomSelectionNewTables() {
        Map<String, Object> oldSource = new HashMap<>(Map.of(
                "id", "source", "type", "database", "connectionId", CONNECTION_ID,
                "migrateTableSelectType", "custom", "tableNames", List.of("T1")));
        storedTask(oldSource);
        when(measurement.findRunTable(taskId.toHexString(), TASK_RECORD_ID)).thenReturn(new ArrayList<>(List.of("T1")));
        Map<String, Object> newSource = new HashMap<>(oldSource);
        newSource.put("tableNames", List.of("T1", "T2"));
        TaskDto edited = editedTask(newSource);

        service.checkTask(edited, user, false);

        assertEquals(List.of("T2"), edited.getLdpNewTables());
    }
}
