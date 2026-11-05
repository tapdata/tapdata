package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.lineage.analyzer.AnalyzerService;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.entity.LineageType;
import com.tapdata.tm.lineage.util.LineageTypeUtil;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.trace.dto.TargetWithLineageDto;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import com.tapdata.tm.trace.param.TaskLineageParam;
import com.tapdata.tm.trace.service.log.ChangeLogQuery;
import com.tapdata.tm.utils.MongoUtils;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.simplify.TapSimplify;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 13:14 Create
 * @description node.attr.type: <$taskId, 'JOIN' ｜ 'MERGE' ｜ 'APPEND' ｜ 'OTHER'>
 * node.attr.joinKeys: <$taskId, ['xxx'...]>
 * node.attr.tablePk: <$taskId, ['yyy'...]>
 */
@Service
@Slf4j
public class BloodlineFinder {
    @Resource(name = "tableAnalyzerCustom")
    AnalyzerService analyzerCustom;
    @Resource
    TaskService taskService;
    @Resource(name = "joinStateSetter")
    JoinStateSetter joinStateSetter;
    @Resource(name = "fieldOriginalNameMapping")
    FieldOriginalNameMapping fieldOriginalNameMapping;
    @Resource(name = "trackFieldFilter")
    TrackFieldFilter trackFieldFilter;
    @Resource(name = "tableUpdateFieldGetter")
    TableUpdateFieldGetter tableUpdateFieldGetter;
    @Resource(name = "changeLogQuery")
    ChangeLogQuery changeLogQuery;


    public TaskLineageDto findTaskLineage(TaskLineageParam param) {
        TaskLineageDto taskLineage = new TaskLineageDto(findLineage(param));
        Dag dag = taskLineage.getDag();
        Map<String, DAG> taskDagMap = findTaskDagMap(dag);
        Map<String, Map<String, String>> fieldNameMapping = fieldOriginalNameMapping.groupFieldOriginalNameMappingByNodeId(dag.getNodes());
        joinStateSetter.markJoinState(dag, fieldNameMapping, taskDagMap);
        Map<String, List<FieldNameMapping>> updateConditionFieldList = UpdateConditionFieldLoader.getUpdateConditionFieldList(dag, taskDagMap, fieldNameMapping);
        taskLineage.setUpdateConditionFieldList(updateConditionFieldList);
        taskLineage.setFieldNameMapping(fieldNameMapping);
        Map<String, Map<String, String>> traceFilterFielMap = trackFieldFilter.removeUselessFields(dag, param.getTraceFilterFieldNames(), fieldNameMapping);
        taskLineage.setTraceFilterFieldNameMapping(traceFilterFielMap);
        return taskLineage;
    }

    public TargetWithLineageDto findTaskLineageSimply(TaskLineageParam param) {
        TargetWithLineageDto taskLineage = new TargetWithLineageDto(findLineage(param));
        Dag dag = taskLineage.getDag();
        Map<String, DAG> taskDagMap = findTaskDagMap(dag);
        Map<String, Map<String, String>> fieldNameMapping = fieldOriginalNameMapping.groupFieldOriginalNameMappingByNodeId(dag.getNodes());
        joinStateSetter.markJoinState(dag, fieldNameMapping, taskDagMap);
        List<String> targetTableUpdateFields = tableUpdateFieldGetter.getTargetTableUpdateFields(dag, taskDagMap);
        taskLineage.setTargetTableUpdateFields(targetTableUpdateFields);
        fieldOriginalNameMapping.findUpdateConditionField(dag, taskDagMap, fieldNameMapping);
        Map<String, Map<String, String>> traceFilterFielMap = trackFieldFilter.removeUselessFields(dag, param.getTraceFilterFieldNames(), fieldNameMapping);
        taskLineage.setTraceFilterFieldNameMapping(traceFilterFielMap);
        changeLogQuery.shareCDCEnable(taskLineage.getDag());
        return taskLineage;
    }

    public Dag findLineage(TaskLineageParam param) {
        LineageType lineageType = LineageTypeUtil.initLineageType(param.getType(), LineageType.UPSTREAM);
        try {
            Graph<Node, Edge> graph = analyzerCustom.analyzeTable(
                    param.getConnectionId(),
                    param.getTable(),
                    lineageType
            );
            if (null == graph) {
                graph = new Graph<>();
            }
            DAG dag = new DAG(graph);
            return dag.toDag();
        } catch (Exception e) {
            throw new BizException("data.trace.findDag.error", TapSimplify.toJson(param), e.getMessage());
        }
    }

    protected Map<String, DAG> findTaskDagMap(Dag dag) {
        Set<String> taskIds = new HashSet<>();
        dag.getNodes().stream()
                .map(node -> node instanceof LineageTableNode n ? n : null)
                .filter(Objects::nonNull)
                .map(LineageTableNode::getTasks)
                .filter(MapUtils::isNotEmpty)
                .forEach(tasks -> {
                    for (LineageTask lineageTask : tasks.values()) {
                        if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                            continue;
                        }
                        taskIds.add(lineageTask.getId());
                    }
                });
        return loadTaskDagByTaskId(taskIds);
    }

    protected Map<String, DAG> loadTaskDagByTaskId(Collection<String> taskId) {
        List<ObjectId> oIds = taskId.stream().map(MongoUtils::toObjectId).filter(Objects::nonNull).toList();
        if (CollectionUtils.isEmpty(oIds)) {
            return new HashMap<>();
        }
        Query query = new Query(Criteria.where("_id").in(oIds));
        query.fields().include("dag", "_id");
        List<TaskDto> taskDto = taskService.findAll(query);
        return taskDto.stream().filter(Objects::nonNull)
                .collect(Collectors.toMap(e -> e.getId().toHexString(), TaskDto::getDag, (t1, t2) -> t2));
    }

}
