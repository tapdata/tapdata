package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.lineage.analyzer.AnalyzerService;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode;
import com.tapdata.tm.trace.dto.TargetWithLineageDto;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import com.tapdata.tm.trace.param.TaskLineageParam;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.trace.service.log.ChangeLogQuery;
import io.github.openlg.graphlib.Graph;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BloodlineFinderTest {
    private AnalyzerService analyzerCustom;
    private TaskService taskService;
    private JoinStateSetter joinStateSetter;
    private FieldOriginalNameMapping fieldOriginalNameMapping;
    private TrackFieldFilter trackFieldFilter;
    private TableUpdateFieldGetter tableUpdateFieldGetter;
    private BloodlineFinder finder;
    ChangeLogQuery changeLogQuery;

    @BeforeEach
    void setUp() {
        analyzerCustom = mock(AnalyzerService.class);
        taskService = mock(TaskService.class);
        joinStateSetter = mock(JoinStateSetter.class);
        fieldOriginalNameMapping = mock(FieldOriginalNameMapping.class);
        trackFieldFilter = mock(TrackFieldFilter.class);
        tableUpdateFieldGetter = mock(TableUpdateFieldGetter.class);
        changeLogQuery = mock(ChangeLogQuery.class);

        finder = new BloodlineFinder();
        finder.analyzerCustom = analyzerCustom;
        finder.taskService = taskService;
        finder.joinStateSetter = joinStateSetter;
        finder.fieldOriginalNameMapping = fieldOriginalNameMapping;
        finder.trackFieldFilter = trackFieldFilter;
        finder.tableUpdateFieldGetter = tableUpdateFieldGetter;
        finder.changeLogQuery = changeLogQuery;
        doNothing().when(changeLogQuery).shareCDCEnable(any(Dag.class));
    }

    @Test
    void findTaskLineage_shouldWireDependenciesAndPopulateDtoFields() {
        BloodlineFinder spyFinder = spy(finder);
        TaskLineageParam param = TaskLineageParam.instance()
                .connectionId("c1")
                .table("t1")
                .type("UPSTREAM")
                .traceFilterFieldNames(List.of("f1"));

        Dag lineageDag = new Dag();
        lineageDag.setNodes(List.of(new LineageTableNode("t", "c", "cn", "pdk", null)));
        doReturn(lineageDag).when(spyFinder).findLineage(any(TaskLineageParam.class));

        Map<String, DAG> taskDagMap = new HashMap<>();
        doReturn(taskDagMap).when(spyFinder).findTaskDagMap(any(Dag.class));

        Map<String, Map<String, String>> fieldNameMapping = Map.of("n1", Map.of("a", "b"));
        when(fieldOriginalNameMapping.groupFieldOriginalNameMappingByNodeId(anyList())).thenReturn(fieldNameMapping);

        Map<String, List<FieldNameMapping>> updateConditionFieldList = Map.of("n1", List.of(new FieldNameMapping()));
        Map<String, Map<String, String>> traceFilterFieldMap = Map.of("n1", Map.of("f1", "a"));
        when(trackFieldFilter.removeUselessFields(any(Dag.class), anyList(), eq(fieldNameMapping))).thenReturn(traceFilterFieldMap);

        try (MockedStatic<UpdateConditionFieldLoader> mockedStatic = org.mockito.Mockito.mockStatic(UpdateConditionFieldLoader.class)) {
            mockedStatic.when(() -> UpdateConditionFieldLoader.getUpdateConditionFieldList(eq(lineageDag), eq(taskDagMap), eq(fieldNameMapping)))
                    .thenReturn(updateConditionFieldList);

            TaskLineageDto dto = spyFinder.findTaskLineage(param);

            assertSame(lineageDag, dto.getDag());
            assertSame(fieldNameMapping, dto.getFieldNameMapping());
            assertSame(updateConditionFieldList, dto.getUpdateConditionFieldList());
            assertSame(traceFilterFieldMap, dto.getTraceFilterFieldNameMapping());
            verify(joinStateSetter, times(1)).markJoinState(eq(lineageDag), eq(fieldNameMapping), eq(taskDagMap));
            verify(trackFieldFilter, times(1)).removeUselessFields(eq(lineageDag), eq(param.getTraceFilterFieldNames()), eq(fieldNameMapping));
            mockedStatic.verify(() -> UpdateConditionFieldLoader.getUpdateConditionFieldList(eq(lineageDag), eq(taskDagMap), eq(fieldNameMapping)), times(1));
        }
    }

    @Test
    void findTaskLineageSimply_shouldPopulateTargetUpdateFieldsAndTraceFilterMapping() {
        BloodlineFinder spyFinder = spy(finder);
        TaskLineageParam param = TaskLineageParam.instance()
                .connectionId("c1")
                .table("t1")
                .type("UPSTREAM")
                .traceFilterFieldNames(List.of("f1"));

        Dag lineageDag = new Dag();
        lineageDag.setNodes(List.of(new LineageTableNode("t", "c", "cn", "pdk", null)));
        doReturn(lineageDag).when(spyFinder).findLineage(any(TaskLineageParam.class));

        Map<String, DAG> taskDagMap = new HashMap<>();
        doReturn(taskDagMap).when(spyFinder).findTaskDagMap(any(Dag.class));

        Map<String, Map<String, String>> fieldNameMapping = Map.of("n1", Map.of("a", "b"));
        when(fieldOriginalNameMapping.groupFieldOriginalNameMappingByNodeId(anyList())).thenReturn(fieldNameMapping);

        List<String> targetTableUpdateFields = List.of("u1", "u2");
        when(tableUpdateFieldGetter.getTargetTableUpdateFields(eq(lineageDag), eq(taskDagMap))).thenReturn(targetTableUpdateFields);

        Map<String, Map<String, String>> traceFilterFieldMap = Map.of("n1", Map.of("f1", "a"));
        when(trackFieldFilter.removeUselessFields(any(Dag.class), anyList(), eq(fieldNameMapping))).thenReturn(traceFilterFieldMap);

        TargetWithLineageDto dto = spyFinder.findTaskLineageSimply(param);

        assertSame(lineageDag, dto.getDag());
        assertSame(targetTableUpdateFields, dto.getTargetTableUpdateFields());
        assertSame(traceFilterFieldMap, dto.getTraceFilterFieldNameMapping());
        verify(joinStateSetter, times(1)).markJoinState(eq(lineageDag), eq(fieldNameMapping), eq(taskDagMap));
        verify(fieldOriginalNameMapping, times(1)).findUpdateConditionField(eq(lineageDag), eq(taskDagMap), eq(fieldNameMapping));
        verify(trackFieldFilter, times(1)).removeUselessFields(eq(lineageDag), eq(param.getTraceFilterFieldNames()), eq(fieldNameMapping));
    }

    @Test
    void findLineage_shouldReturnDag_whenAnalyzerReturnsNonNullGraph() throws Exception {
        TaskLineageParam param = TaskLineageParam.instance()
                .connectionId("c1")
                .table("t1")
                .type("UPSTREAM");

        when(analyzerCustom.analyzeTable(eq("c1"), eq("t1"), any())).thenReturn(new Graph<>());

        Dag result = finder.findLineage(param);

        assertNotNull(result);
        assertNotNull(result.getNodes());
        assertNotNull(result.getEdges());
    }

    @Test
    void findLineage_shouldReturnDag_whenAnalyzerReturnsNullGraph() throws Exception {
        TaskLineageParam param = TaskLineageParam.instance()
                .connectionId("c1")
                .table("t1")
                .type("UPSTREAM");

        when(analyzerCustom.analyzeTable(eq("c1"), eq("t1"), any())).thenReturn(null);

        Dag result = finder.findLineage(param);

        assertNotNull(result);
        assertNotNull(result.getNodes());
        assertNotNull(result.getEdges());
    }

    @Test
    void findLineage_shouldThrowBizException_whenAnalyzerThrows() throws Exception {
        TaskLineageParam param = TaskLineageParam.instance()
                .connectionId("c1")
                .table("t1")
                .type("UPSTREAM");

        when(analyzerCustom.analyzeTable(eq("c1"), eq("t1"), any())).thenThrow(new RuntimeException("boom"));

        BizException ex = assertThrows(BizException.class, () -> finder.findLineage(param));
        assertEquals("data.trace.findDag.error", ex.getErrorCode());
        assertNotNull(ex.getArgs());
        assertEquals(2, ex.getArgs().length);
        assertEquals("boom", ex.getArgs()[1]);
    }

    @Test
    void loadTaskDagByTaskId_shouldReturnEmptyMap_whenNoValidObjectIds() {
        Map<String, DAG> result = finder.loadTaskDagByTaskId(List.of("notAnObjectId"));
        assertNotNull(result);
        assertEquals(0, result.size());
        verify(taskService, never()).findAll(any(Query.class));
    }
}
