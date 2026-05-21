package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.AnalyzerService;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.entity.LineageType;
import com.tapdata.tm.lineage.util.LineageTypeUtil;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.param.TaskLineageParam;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.simplify.TapSimplify;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.Objects;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 13:14 Create
 * @description
 * node.attr.type: <$taskId, ['JOIN' , 'MERGE' , 'APPEND' , 'OTHER']>
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
    @Resource
    MetadataInstancesRepository metadataInstancesRepository;

    private static final String MAIN_TABLE = "mainTable";
    private static final String SUB_TABLE = "subTable";
    private static final String NODE_ATTR_TYPE_JOIN = "JOIN";
    private static final String NODE_ATTR_TYPE_MERGE = "MERGE";
    private static final String NODE_ATTR_TYPE_APPEND = "APPEND";
    private static final String NODE_ATTR_TYPE_OTHER = "OTHER";

    public TaskLineageDto findTaskLineage(TaskLineageParam param) {
        return simply(findFullTaskLineage(param));
    }

    public TaskLineageDto findFullTaskLineage(TaskLineageParam param) {
        TaskLineageDto taskLineage = null;
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
            taskLineage = new TaskLineageDto(dag.toDag());
        } catch (Exception e) {
            throw new BizException("data.trace.findDag.error", TapSimplify.toJson(param), e.getMessage());
        }
        Dag dag = taskLineage.getDag();
        taskLineage.setFieldNameMapping(groupFieldNameMappingByNodeId(dag.getNodes()));
        markJoinState(dag);
        return taskLineage;
    }

    public void markJoinState(Dag dag) {
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes())) {
            return;
        }
        Set<String> taskIds = new HashSet<>();
        for (Node<?> node : dag.getNodes()) {
            if (!(node instanceof LineageTableNode)) {
                continue;
            }
            LineageMetadataInstance metadata = ((LineageTableNode) node).getMetadata();
            if (null == metadata || !"SOURCE".equalsIgnoreCase(metadata.getSourceType())) {
                continue;
            }
            Map<String, LineageTask> tasks = ((LineageTableNode) node).getTasks();
            if (MapUtils.isEmpty(tasks)) {
                continue;
            }
            for (LineageTask lineageTask : tasks.values()) {
                if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                    continue;
                }
                taskIds.add(lineageTask.getId());
            }
        }
        Map<String, DAG> taskDagMap = loadTaskDagByTaskId(taskIds);
        Map<String, Boolean> hasJoinMap = new HashMap<>();
        Map<String, Boolean> hasMergeMap = new HashMap<>();
        Map<String, Boolean> hasAppendMap = new HashMap<>();
        Map<String, Map<String, TableProperties>> mergeTablePropertiesMap = new HashMap<>();
        taskDagMap.forEach((taskId, taskDag) -> {
            if (null == taskDag || CollectionUtils.isEmpty(taskDag.getNodes())) {
                return;
            }
            Map<String, TableProperties> mergePropertiesMap = mergeTablePropertiesMap.computeIfAbsent(taskId, k -> new HashMap<>());
            Map<String, Node> nodeMap = taskDag.getNodes()
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Node::getId, n -> n, (n1, n2) -> n2));
            for (Node<?> taskNode : nodeMap.values()) {
                if (taskNode instanceof JoinProcessorNode joinNode) {
                    hasJoinMap.put(taskId, true);
                } else if (taskNode instanceof MergeTableNode mergeNode) {
                    hasMergeMap.put(taskId, true);
                    List<MergeTableProperties> mergeProperties = mergeNode.getMergeProperties();
                    mergeProperties.forEach(properties -> {
                        String nodeId = properties.getId();
                        Map<String, TableProperties> mainPropertiesMap = loadRootInfo(nodeId, taskDag, properties);
                        //主表只有一个链路来源
                        mainPropertiesMap.forEach((nId, info) -> info.setTableType(MAIN_TABLE));
                        mergePropertiesMap.putAll(mainPropertiesMap);
                        List<MergeTableProperties> children = properties.getChildren();
                        if (null == children || children.isEmpty()) {
                            return;
                        }
                        children.forEach(child -> {
                            String id = child.getId();
                            mergePropertiesMap.putAll(loadRootInfo(id, taskDag, child));
                        });
                    });
                    if (containsAppendMergeType(mergeNode)) {
                        hasAppendMap.put(taskId, true);
                    }
                }
            }
        });
        for (Node<?> node : dag.getNodes()) {
            if (!(node instanceof LineageTableNode)) {
                continue;
            }
            LineageMetadataInstance metadata = ((LineageTableNode) node).getMetadata();
            if (null == metadata || !"SOURCE".equalsIgnoreCase(metadata.getSourceType())) {
                continue;
            }
            Map<String, LineageTask> tasks = ((LineageTableNode) node).getTasks();
            if (MapUtils.isNotEmpty(tasks)) {
                for (LineageTask lineageTask : tasks.values()) {
                    if (null == lineageTask || StringUtils.isBlank(lineageTask.getId())) {
                        continue;
                    }
                    String taskId = lineageTask.getId();
                    Node taskNode = lineageTask.getTaskNode();
                    String nodeId = taskNode.getId();
                    boolean hasJoin = Optional.ofNullable(hasJoinMap.get(taskId)).orElse(false);
                    boolean hasMerge = Optional.ofNullable(hasMergeMap.get(taskId)).orElse(false);
                    boolean hasAppend = Optional.ofNullable(hasAppendMap.get(taskId)).orElse(false);
                    final String nodeType;
                    Map<String, TableProperties> propertiesMap = mergeTablePropertiesMap.get(taskId);
                    if (hasJoin) {
                        nodeType = NODE_ATTR_TYPE_JOIN;
                    } else if (hasMerge) {
                        nodeType = NODE_ATTR_TYPE_MERGE;
                    } else if (hasAppend) {
                        nodeType = NODE_ATTR_TYPE_APPEND;
                    } else {
                        nodeType = NODE_ATTR_TYPE_OTHER;
                    }
                    Map<String, Object> attrs = node.getAttrs();
                    if (null == attrs) {
                        attrs = new HashMap<>();
                        node.setAttrs(attrs);
                    }
                    Optional.ofNullable(propertiesMap.get(nodeId))
                            .ifPresent(properties -> {
                                properties.setNodeType(nodeType);
                                node.getAttrs().put(taskId, properties);
                            });
                }
            }
        }
    }

    public Map<String, TableProperties> loadRootInfo(String preNodeId, DAG taskDag, MergeTableProperties mergeTableProperties) {
        Map<String, TableProperties> result = new HashMap<>();
        if (StringUtils.isBlank(preNodeId) || null == taskDag || null == taskDag.getTaskId() || null == mergeTableProperties) {
            return result;
        }
        Set<String> rootNodeIds = findRootNodeIds(taskDag, preNodeId);
        String taskId = taskDag.getTaskId().toHexString();
        Map<String, Map<String, Field>> fieldsByNodeIdCache = new HashMap<>();

        if (rootNodeIds.isEmpty() || (rootNodeIds.size() == 1 && rootNodeIds.contains(preNodeId))) {
            TableProperties isSelf = new TableProperties();
            isSelf.setRootNodeId(preNodeId);
            isSelf.setPreNodeId(preNodeId);
            List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
            if (CollectionUtils.isNotEmpty(joinKeys)) {
                List<FieldNameMapping> joinKeyMappings = new ArrayList<>();
                for (Map<String, String> joinKey : joinKeys) {
                    if (MapUtils.isEmpty(joinKey)) {
                        continue;
                    }
                    String sourceName = joinKey.get("source");
                    String targetName = joinKey.get("target");
                    if (StringUtils.isBlank(targetName)) {
                        continue;
                    }
                    if (StringUtils.isBlank(sourceName)) {
                        continue;
                    }
                    String originName = getOriginFieldName(taskId, preNodeId, sourceName, fieldsByNodeIdCache);
                    addFieldNameMapping(joinKeyMappings, originName, targetName);
                }
                isSelf.setJoinKeys(joinKeyMappings);
            }
            List<String> arrayKeys = mergeTableProperties.getArrayKeys();
            if (CollectionUtils.isNotEmpty(arrayKeys)) {
                List<FieldNameMapping> tablePkMappings = new ArrayList<>();
                for (String targetName : arrayKeys) {
                    if (StringUtils.isBlank(targetName)) {
                        continue;
                    }
                    String originName = getOriginFieldName(taskId, preNodeId, targetName, fieldsByNodeIdCache);
                    addFieldNameMapping(tablePkMappings, originName, targetName);
                }
                isSelf.setTablePk(tablePkMappings);
            }
            result.put(preNodeId, isSelf);
            return result;
        }

        List<String> arrayKeys = mergeTableProperties.getArrayKeys();
        if (CollectionUtils.isNotEmpty(arrayKeys)) {
            for (String arrayKey : arrayKeys) {
                if (StringUtils.isBlank(arrayKey)) {
                    continue;
                }
                Optional<TracedField> traced = traceToRoot(taskDag, taskId, preNodeId, arrayKey, fieldsByNodeIdCache);
                traced.ifPresent(tf -> {
                    TableProperties tableProperties = result.computeIfAbsent(tf.rootNodeId, k -> newTableProperties(tf.rootNodeId, preNodeId));
                    if (null == tableProperties.getTablePk()) {
                        tableProperties.setTablePk(new ArrayList<>());
                    }
                    addFieldNameMapping(tableProperties.getTablePk(), tf.rootFieldName, arrayKey);
                });
            }
        }

        List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
        if (CollectionUtils.isNotEmpty(joinKeys)) {
            for (Map<String, String> joinKey : joinKeys) {
                if (MapUtils.isEmpty(joinKey)) {
                    continue;
                }
                String sourceName = joinKey.get("source");
                String targetName = joinKey.get("target");
                if (StringUtils.isBlank(sourceName) || StringUtils.isBlank(targetName)) {
                    continue;
                }
                Optional<TracedField> traced = traceToRoot(taskDag, taskId, preNodeId, sourceName, fieldsByNodeIdCache);
                traced.ifPresent(tf -> {
                    TableProperties tableProperties = result.computeIfAbsent(tf.rootNodeId, k -> newTableProperties(tf.rootNodeId, preNodeId));
                    if (null == tableProperties.getJoinKeys()) {
                        tableProperties.setJoinKeys(new ArrayList<>());
                    }
                    addFieldNameMapping(tableProperties.getJoinKeys(), tf.rootFieldName, targetName);
                });
            }
        }
        return result;
    }

    public Map<String, Map<String, String>> groupFieldNameMappingByNodeId(List<Node> nodes) {
        Map<String, Map<String, String>> mapMap = new HashMap<>();
        if (CollectionUtils.isEmpty(nodes)) {
            return mapMap;
        }
        for (Node<?> node : nodes) {
            if (null == node || StringUtils.isBlank(node.getId())) {
                continue;
            }
            Map<String, String> fieldNameMapping = new HashMap<>();
            if (node instanceof LineageTableNode) {
                LineageMetadataInstance metadata = ((LineageTableNode) node).getMetadata();
                List<Field> fields = null == metadata ? null : metadata.getFields();
                if (CollectionUtils.isNotEmpty(fields)) {
                    for (Field field : fields) {
                        if (null == field || StringUtils.isBlank(field.getFieldName())) {
                            continue;
                        }
                        String originName = field.getOriginalFieldName();
                        if (StringUtils.isBlank(originName)) {
                            originName = field.getPreviousFieldName();
                        }
                        if (StringUtils.isBlank(originName)) {
                            originName = field.getFieldName();
                        }
                        fieldNameMapping.putIfAbsent(field.getFieldName(), originName);
                    }
                    metadata.setFields(null);
                }
            }
            mapMap.put(node.getId(), fieldNameMapping);
        }
        return mapMap;
    }

    private TaskLineageDto simply(TaskLineageDto info) {

        return info;
    }

    private Map<String, DAG> loadTaskDagByTaskId(Collection<String> taskId) {
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

    private boolean containsAppendMergeType(MergeTableNode mergeTableNode) {
        if (null == mergeTableNode || CollectionUtils.isEmpty(mergeTableNode.getMergeProperties())) {
            return false;
        }
        return flattenMergeProperties(mergeTableNode.getMergeProperties()).stream()
                .map(MergeTableProperties::getMergeType)
                .filter(Objects::nonNull)
                .anyMatch(t -> t == MergeTableProperties.MergeType.appendWrite);
    }

    private List<String> extractJoinKeys(JoinProcessorNode joinProcessorNode, String taskNodeId) {
        List<String> keys = new ArrayList<>();
        if (null == joinProcessorNode || StringUtils.isBlank(taskNodeId)) {
            return keys;
        }
        List<JoinProcessorNode.JoinExpression> expressions = joinProcessorNode.getJoinExpressions();
        if (CollectionUtils.isEmpty(expressions)) {
            return keys;
        }
        if (taskNodeId.equals(joinProcessorNode.getLeftNodeId())) {
            for (JoinProcessorNode.JoinExpression expression : expressions) {
                if (null != expression && StringUtils.isNotBlank(expression.getLeft())) {
                    keys.add(expression.getLeft());
                }
            }
        } else if (taskNodeId.equals(joinProcessorNode.getRightNodeId())) {
            for (JoinProcessorNode.JoinExpression expression : expressions) {
                if (null != expression && StringUtils.isNotBlank(expression.getRight())) {
                    keys.add(expression.getRight());
                }
            }
        }
        return keys;
    }

    private List<String> extractJoinKeys(MergeTableNode mergeTableNode, String taskNodeId) {
        List<String> keys = new ArrayList<>();
        if (null == mergeTableNode || StringUtils.isBlank(taskNodeId) || CollectionUtils.isEmpty(mergeTableNode.getMergeProperties())) {
            return keys;
        }
        for (MergeTableProperties properties : flattenMergeProperties(mergeTableNode.getMergeProperties())) {
            if (null == properties || CollectionUtils.isEmpty(properties.getJoinKeys())) {
                continue;
            }
            boolean asSource = taskNodeId.equals(properties.getId());
            boolean asTarget = taskNodeId.equals(properties.getParentId());
            if (!asSource && !asTarget) {
                continue;
            }
            for (Map<String, String> joinKey : properties.getJoinKeys()) {
                if (null == joinKey || joinKey.isEmpty()) {
                    continue;
                }
                String v = asSource ? joinKey.get("source") : joinKey.get("target");
                if (StringUtils.isNotBlank(v)) {
                    keys.add(v);
                }
            }
        }
        return keys;
    }

    private List<MergeTableProperties> flattenMergeProperties(List<MergeTableProperties> mergeProperties) {
        List<MergeTableProperties> list = new ArrayList<>();
        if (CollectionUtils.isEmpty(mergeProperties)) {
            return list;
        }
        for (MergeTableProperties properties : mergeProperties) {
            if (null == properties) {
                continue;
            }
            list.add(properties);
            if (CollectionUtils.isNotEmpty(properties.getChildren())) {
                list.addAll(flattenMergeProperties(properties.getChildren()));
            }
        }
        return list;
    }

    @Data
    public static class TableProperties {
        String rootNodeId;
        String preNodeId;
        String nodeType;
        List<FieldNameMapping> joinKeys;
        List<FieldNameMapping> tablePk;
        String tableType = SUB_TABLE;
    }

    @Data
    public static class FieldNameMapping {
        String originName;
        String targetName;
    }

    @Data
    public static class TracedField {
        private final String rootNodeId;
        private final String rootFieldName;
    }

    private TableProperties newTableProperties(String rootNodeId, String preNodeId) {
        TableProperties tableProperties = new TableProperties();
        tableProperties.setRootNodeId(rootNodeId);
        tableProperties.setPreNodeId(preNodeId);
        return tableProperties;
    }

    private void addFieldNameMapping(List<FieldNameMapping> list, String originName, String targetName) {
        if (null == list || StringUtils.isBlank(originName) || StringUtils.isBlank(targetName)) {
            return;
        }
        for (FieldNameMapping mapping : list) {
            if (null == mapping) {
                continue;
            }
            if (originName.equals(mapping.getOriginName()) && targetName.equals(mapping.getTargetName())) {
                return;
            }
        }
        FieldNameMapping mapping = new FieldNameMapping();
        mapping.setOriginName(originName);
        mapping.setTargetName(targetName);
        list.add(mapping);
    }

    private String getOriginFieldName(String taskId, String nodeId, String targetName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        Field field = getField(taskId, nodeId, targetName, fieldsByNodeIdCache);
        if (null != field && StringUtils.isNotBlank(field.getOriginalFieldName())) {
            return field.getOriginalFieldName();
        }
        return targetName;
    }

    private Set<String> findRootNodeIds(DAG taskDag, String preNodeId) {
        Set<String> roots = new HashSet<>();
        if (null == taskDag || StringUtils.isBlank(preNodeId) || !taskDag.hasNode(preNodeId)) {
            return roots;
        }
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new java.util.ArrayDeque<>();
        queue.add(preNodeId);
        visited.add(preNodeId);
        while (!queue.isEmpty()) {
            String currentId = queue.poll();
            List<Node> predecessors = taskDag.predecessors(currentId);
            if (CollectionUtils.isEmpty(predecessors)) {
                roots.add(currentId);
                continue;
            }
            for (Node<?> predecessor : predecessors) {
                if (null == predecessor || StringUtils.isBlank(predecessor.getId())) {
                    continue;
                }
                String pid = predecessor.getId();
                if (visited.add(pid)) {
                    queue.add(pid);
                }
            }
        }
        return roots;
    }

    private Optional<TracedField> traceToRoot(
            DAG taskDag,
            String taskId,
            String startNodeId,
            String fieldName,
            Map<String, Map<String, Field>> fieldsByNodeIdCache
    ) {
        if (null == taskDag || StringUtils.isBlank(taskId) || StringUtils.isBlank(startNodeId) || StringUtils.isBlank(fieldName)) {
            return Optional.empty();
        }
        String currentNodeId = startNodeId;
        String currentFieldName = fieldName;
        Set<String> visitedNodeIds = new HashSet<>();
        for (int step = 0; step < 64; step++) {
            if (!visitedNodeIds.add(currentNodeId)) {
                return Optional.empty();
            }
            Field field = getField(taskId, currentNodeId, currentFieldName, fieldsByNodeIdCache);
            if (null == field) {
                return Optional.empty();
            }
            List<Node> predecessors = taskDag.predecessors(currentNodeId);
            if (CollectionUtils.isEmpty(predecessors)) {
                String rootFieldName = field.getOriginalFieldName();
                if (StringUtils.isBlank(rootFieldName)) {
                    rootFieldName = currentFieldName;
                }
                return Optional.of(new TracedField(currentNodeId, rootFieldName));
            }
            String previousFieldName = field.getPreviousFieldName();
            if (StringUtils.isBlank(previousFieldName)) {
                return Optional.empty();
            }
            Node<?> nextNode = null;
            for (Node<?> predecessor : predecessors) {
                if (null == predecessor || StringUtils.isBlank(predecessor.getId())) {
                    continue;
                }
                Field preField = getField(taskId, predecessor.getId(), previousFieldName, fieldsByNodeIdCache);
                if (null != preField) {
                    nextNode = predecessor;
                    break;
                }
            }
            if (null == nextNode) {
                return Optional.empty();
            }
            currentNodeId = nextNode.getId();
            currentFieldName = previousFieldName;
        }
        return Optional.empty();
    }

    private Field getField(String taskId, String nodeId, String fieldName, Map<String, Map<String, Field>> fieldsByNodeIdCache) {
        if (StringUtils.isBlank(taskId) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(fieldName)) {
            return null;
        }
        Map<String, Field> fieldMap = fieldsByNodeIdCache.computeIfAbsent(nodeId, nid -> {
            Query query = Query.query(Criteria.where("taskId").is(taskId).and("nodeId").is(nid).and("is_deleted").ne(true));
            query.fields().include("fields");
            MetadataInstancesEntity entity = metadataInstancesRepository.findOne(query).orElse(null);
            List<Field> fields = null == entity ? null : entity.getFields();
            if (CollectionUtils.isEmpty(fields)) {
                return new HashMap<>();
            }
            return fields.stream()
                    .filter(Objects::nonNull)
                    .filter(f -> StringUtils.isNotBlank(f.getFieldName()))
                    .collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));
        });
        return fieldMap.get(fieldName);
    }
}
