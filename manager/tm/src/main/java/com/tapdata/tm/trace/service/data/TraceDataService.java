package com.tapdata.tm.trace.service.data;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.dto.TraceFieldMapping;
import com.tapdata.tm.trace.dto.TraceNodeError;
import com.tapdata.tm.trace.dto.TraceStreamEvent;
import com.tapdata.tm.trace.dto.TraceValue;
import com.tapdata.tm.trace.param.TaskLineageParam;
import com.tapdata.tm.trace.param.WideTableTraceRequest;
import com.tapdata.tm.trace.dto.TraceQueryCondition;
import com.tapdata.tm.trace.service.bloodline.BloodlineFinder;
import com.tapdata.tm.utils.MessageUtil;
import io.tapdata.pdk.apis.entity.QueryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TraceDataService {
    private static final String ERROR_FIELD_MAPPING_NOT_FOUND = "Trace.Field.Mapping.NotFound";
    private static final String ERROR_VALUE_MISMATCH = "Trace.Value.Mismatch";
    private static final String ERROR_FILTER_NOT_BUILT = "Trace.Filter.NotBuilt";
    private static final String ERROR_MERGE_JOIN_KEY_NOT_FOUND = "Trace.Merge.JoinKey.NotFound";

    @Autowired
    private BloodlineFinder bloodlineFinder;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired(required = false)
    private TraceDataQueryRpcAdapter traceDataQueryAdapter;

    public void traceData(WideTableTraceRequest request, OutputStream outputStream) throws IOException {
        traceData(request, "trace_" + UUID.randomUUID().toString().replace("-", ""), outputStream);
    }

    public void traceData(WideTableTraceRequest request, String requestId, OutputStream outputStream) throws IOException {
        TaskLineageParam taskLineageParam = new TaskLineageParam(request.getConnectionId(), request.getTable(), null, request.getTrackedFields());
        TaskLineageDto taskLineageDto = bloodlineFinder.findTaskLineage(taskLineageParam);
        if (taskLineageDto == null || taskLineageDto.getDag() == null) {
            throw new BizException("Trace.LineageDag.NotFound");
        }
        traceDag(request, requestId, outputStream, taskLineageDto);
    }

    private void traceDag(WideTableTraceRequest request, String requestId, OutputStream outputStream, TaskLineageDto taskLineageDto) throws IOException {
        Dag dag = taskLineageDto.getDag();
        List<Node> nodes = dag.getNodes();
        if (CollectionUtils.isEmpty(nodes)) {
            throw new BizException("Trace.LineageDag.NodesNotFound");
        }

        Map<String, Node> nodeMap = nodes.stream()
                .filter(Objects::nonNull)
                .filter(node -> StringUtils.isNotBlank(node.getId()))
                .collect(Collectors.toMap(Node::getId, node -> node, (left, right) -> left));
        Node targetNode = findTargetNode(request, nodes, dag.getEdges());
        if (targetNode == null) {
            throw new BizException("Trace.TargetNode.NotFound");
        }

        Map<String, Map<String, String>> fieldNameMapping = safeFieldNameMapping(taskLineageDto);
        Map<String, String> targetFieldMapping = fieldNameMapping.getOrDefault(targetNode.getId(), Collections.emptyMap());
        Map<String, List<Edge>> incomingEdges = groupIncomingEdges(dag.getEdges());

        Queue<TraceNodeStep> queue = new ArrayDeque<>();
        queue.add(new TraceNodeStep(targetNode, null, buildTargetCondition(request, targetNode), null, null, null, null));
        Set<String> visited = new HashSet<>();
        Map<String, List<BloodlineFinder.FieldNameMapping>> updateConditionFieldList = taskLineageDto.getUpdateConditionFieldList();
        while (!queue.isEmpty()) {
            TraceNodeStep step = queue.poll();
            Node currentNode = step.getCurrentNode();
            if (currentNode == null || !visited.add(currentNode.getId())) {
                continue;
            }
            TraceValue traceValue = emitTraceValue(request, requestId, outputStream, currentNode, step.getDownstreamNode(),
                    step.getCondition(), step.getDownstreamTraceValue(), targetFieldMapping, fieldNameMapping);
            TraceValue filterTraceValue = hasCurrentRecords(traceValue) ? traceValue : step.getFilterTraceValue();
            Node filterNode = hasCurrentRecords(traceValue) ? currentNode : step.getFilterNode();
            TraceQueryCondition filterCondition = hasFilterCondition(step.getCondition()) ? step.getCondition() : step.getFilterCondition();
            for (Edge edge : incomingEdges.getOrDefault(currentNode.getId(), Collections.emptyList())) {
                Node upstreamNode = nodeMap.get(edge.getSource());
                if (upstreamNode != null) {
                    List<BloodlineFinder.FieldNameMapping> updateConditionField = updateConditionFieldList.get(currentNode.getId());
                    TraceConditionBuildResult buildResult = buildUpstreamCondition(request, upstreamNode, currentNode,
                            edge, traceValue, step.getCondition(), filterTraceValue, filterNode, filterCondition, updateConditionField, fieldNameMapping);
                    writeNodeErrors(outputStream, requestId, upstreamNode, buildResult.getErrors());
                    if (buildResult.isTraceable()) {
                        queue.add(new TraceNodeStep(upstreamNode, currentNode, buildResult.getCondition(), traceValue,
                                filterTraceValue, filterNode, filterCondition));
                    }
                }
            }
        }
    }

    private TraceValue emitTraceValue(WideTableTraceRequest request, String requestId, OutputStream outputStream,
                                      Node currentNode, Node downstreamNode, TraceQueryCondition condition,
                                      TraceValue downstreamTraceValue, Map<String, String> targetFieldMapping,
                                      Map<String, Map<String, String>> fieldNameMapping) throws IOException {
        Map<String, String> currentFieldMapping = fieldNameMapping.getOrDefault(currentNode.getId(), Collections.emptyMap());
        Map<String, String> downstreamFieldMapping = downstreamNode == null
                ? Collections.emptyMap()
                : fieldNameMapping.getOrDefault(downstreamNode.getId(), Collections.emptyMap());
        List<Map<String, Object>> currentRecords = queryCurrentRecords(condition);
        List<Map<String, Object>> downStreamRecords = downstreamTraceValue == null
                ? Collections.emptyList()
                : downstreamTraceValue.getCurrentRecords();

        TraceValue traceValue = new TraceValue();
        traceValue.setMatchedCount(currentRecords.size());
        traceValue.setCurrentRecords(currentRecords);
        traceValue.setDownStreamRecords(downStreamRecords);
        List<TraceNodeError> errors = new ArrayList<>();
        traceValue.setTracedFields(buildTracedFields(request.getTrackedFields(), targetFieldMapping, currentFieldMapping, errors));
        traceValue.setResultFieldMapping(buildResultFieldMapping(currentFieldMapping, downstreamFieldMapping,
                resolveTablePath(currentNode)));
        traceValue.setQueryConditions(buildQueryConditions(condition));

        TraceStreamEvent event = TraceStreamEvent.traceValue(
                requestId,
                currentNode.getId(),
                resolveConnectionId(currentNode),
                resolveConnectionName(currentNode),
                resolveTableName(currentNode),
                traceValue
        );
        writeEvent(outputStream, event);
        writeNodeErrors(outputStream, requestId, currentNode, errors);
        return traceValue;
    }

    private List<Map<String, Object>> buildQueryConditions(TraceQueryCondition condition) {
        if (condition == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> queryConditions = new ArrayList<>();
        if (condition.isSqlMode() || StringUtils.isNotBlank(condition.getSql())) {
            Map<String, Object> sqlCondition = new LinkedHashMap<>();
            sqlCondition.put("sqlMode", condition.isSqlMode());
            sqlCondition.put("sql", condition.getSql());
            queryConditions.add(sqlCondition);
        }
        for (Map<String, Object> filter : safeFilters(condition)) {
            Map<String, Object> sanitizedFilter = sanitizeFilter(filter);
            if (MapUtils.isNotEmpty(sanitizedFilter)) {
                queryConditions.add(sanitizedFilter);
            }
        }
        CollectionUtils.emptyIfNull(condition.getQueryOperators()).stream()
                .filter(Objects::nonNull)
                .filter(operator -> StringUtils.isNotBlank(operator.getKey()))
                .map(this::buildQueryOperatorCondition)
                .forEach(queryConditions::add);
        return queryConditions;
    }

    private Map<String, Object> buildQueryOperatorCondition(QueryOperator queryOperator) {
        Map<String, Object> queryCondition = new LinkedHashMap<>();
        queryCondition.put("key", queryOperator.getKey());
        queryCondition.put("value", queryOperator.getValue());
        queryCondition.put("operator", queryOperator.getOperator());
        return queryCondition;
    }

    private void addFilter(TraceQueryCondition condition, String key, Object value) {
        if (condition == null || StringUtils.isBlank(key)) {
            return;
        }
        Map<String, Object> filter = new LinkedHashMap<>();
        filter.put(key, value);
        condition.getFilters().add(filter);
    }

    private List<Map<String, Object>> safeFilters(TraceQueryCondition condition) {
        if (condition == null || CollectionUtils.isEmpty(condition.getFilters())) {
            return Collections.emptyList();
        }
        return condition.getFilters();
    }

    private Map<String, Object> sanitizeFilter(Map<String, Object> filter) {
        if (MapUtils.isEmpty(filter)) {
            return Collections.emptyMap();
        }
        Map<String, Object> sanitized = new LinkedHashMap<>();
        filter.forEach((key, value) -> {
            if (StringUtils.isNotBlank(key)) {
                sanitized.put(key, value);
            }
        });
        return sanitized;
    }

    private List<Map<String, Object>> distinctFilters(List<Map<String, Object>> filters) {
        if (CollectionUtils.isEmpty(filters)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        Set<Map<String, Object>> seen = new HashSet<>();
        for (Map<String, Object> filter : filters) {
            if (MapUtils.isEmpty(filter)) {
                continue;
            }
            Map<String, Object> copy = sanitizeFilter(filter);
            if (MapUtils.isEmpty(copy)) {
                continue;
            }
            if (seen.add(copy)) {
                result.add(copy);
            }
        }
        return result;
    }

    private TraceQueryCondition buildTargetCondition(WideTableTraceRequest request, Node targetNode) {
        TraceQueryCondition condition = new TraceQueryCondition();
        condition.setConnectionId(StringUtils.defaultIfBlank(resolveConnectionId(targetNode), request.getConnectionId()));
        condition.setTable(StringUtils.defaultIfBlank(resolveTableName(targetNode), request.getTable()));
        if (request.getFilters() != null && StringUtils.isNotBlank(request.getFilters().getSql())) {
            condition.setSqlMode(true);
            condition.setSql(request.getFilters().getSql());
            Map<String, Object> filter = JSON.parseObject(request.getFilters().getSql(), Map.class);
            condition.getConditionKeys().addAll(filter.keySet());
        }
        if (request.getFilters() != null && request.getFilters().getCustom() != null
                && StringUtils.isNotBlank(request.getFilters().getCustom().getKey())) {
            QueryOperator custom = request.getFilters().getCustom();
            if (isRangeOperator(custom.getOperator())) {
                condition.getQueryOperators().add(custom);
            } else {
                addFilter(condition, custom.getKey(), custom.getValue());
            }
            condition.getConditionKeys().add(custom.getKey());
        }
        return condition;
    }

    private boolean isRangeOperator(int operator) {
        return operator == QueryOperator.GT || operator == QueryOperator.GTE
                || operator == QueryOperator.LT || operator == QueryOperator.LTE;
    }

    private TraceConditionBuildResult buildUpstreamCondition(WideTableTraceRequest request, Node upstreamNode, Node currentNode,
                                                             Edge edge, TraceValue currentTraceValue, TraceQueryCondition currentCondition,
                                                             TraceValue fallbackTraceValue, Node fallbackNode,
                                                             TraceQueryCondition fallbackCondition,
                                                             List<BloodlineFinder.FieldNameMapping> updateConditionFieldList,
                                                             Map<String, Map<String, String>> fieldNameMapping) {
        List<TraceNodeError> errors = new ArrayList<>();
        TraceQueryCondition condition = new TraceQueryCondition();
        condition.setConnectionId(resolveConnectionId(upstreamNode));
        condition.setTable(resolveTableName(upstreamNode));
        if (isMergeSubTable(upstreamNode, edge)) {
            condition.setFilters(buildMergeJoinFilters(upstreamNode, edge, currentTraceValue,
                    currentCondition, fallbackTraceValue, fallbackCondition, errors));
            addEmptyFilterError(condition, errors, upstreamNode, currentNode);
            return new TraceConditionBuildResult(condition, errors);
        }

        Map<String, String> upstreamFieldMapping = fieldNameMapping.getOrDefault(upstreamNode.getId(), Collections.emptyMap());
        Map<String, String> currentFieldMapping = fieldNameMapping.getOrDefault(currentNode.getId(), Collections.emptyMap());
        Map<String, String> fallbackFieldMapping = fallbackNode == null
                ? Collections.emptyMap()
                : fieldNameMapping.getOrDefault(fallbackNode.getId(), Collections.emptyMap());
        condition.setFilters(buildNormalUpstreamFilters(currentCondition, currentTraceValue,
                upstreamFieldMapping, currentFieldMapping, fallbackCondition, fallbackTraceValue, fallbackFieldMapping, updateConditionFieldList, errors));
        addEmptyFilterError(condition, errors, upstreamNode, currentNode);
        return new TraceConditionBuildResult(condition, errors);
    }

    private List<Map<String, Object>> buildMergeJoinFilters(Node upstreamNode, Edge edge, TraceValue downstreamTraceValue,
                                                            TraceQueryCondition downstreamCondition,
                                                            TraceValue fallbackTraceValue,
                                                            TraceQueryCondition fallbackCondition,
                                                            List<TraceNodeError> errors) {
        List<Map<String, String>> joinKeys = extractJoinKeys(upstreamNode, edge);
        if (CollectionUtils.isEmpty(joinKeys)) {
            errors.add(traceError(ERROR_MERGE_JOIN_KEY_NOT_FOUND,null, null, null, null, null,
                    "table=" + resolveTableName(upstreamNode)));
            return Collections.emptyList();
        }
        List<Map<String, Object>> filters = buildMergeJoinFiltersFromRecords(joinKeys, downstreamTraceValue, errors);
        if (CollectionUtils.isEmpty(filters)) {
            filters = buildMergeJoinFiltersFromCondition(joinKeys, downstreamCondition, errors);
        }
        if (CollectionUtils.isEmpty(filters)) {
            filters = buildMergeJoinFiltersFromRecords(joinKeys, fallbackTraceValue, errors);
        }
        if (CollectionUtils.isEmpty(filters)) {
            filters = buildMergeJoinFiltersFromCondition(joinKeys, fallbackCondition, errors);
        }
        return distinctFilters(filters);
    }

    private List<Map<String, Object>> buildMergeJoinFiltersFromRecords(List<Map<String, String>> joinKeys,
                                                                       TraceValue traceValue,
                                                                       List<TraceNodeError> errors) {
        if (!hasCurrentRecords(traceValue)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> filters = new ArrayList<>();
        for (Map<String, Object> record : traceValue.getCurrentRecords()) {
            Map<String, Object> filter = buildMergeJoinFilter(joinKeys, record, null, errors);
            if (MapUtils.isNotEmpty(filter)) {
                filters.add(filter);
            }
        }
        return filters;
    }

    private List<Map<String, Object>> buildMergeJoinFiltersFromCondition(List<Map<String, String>> joinKeys,
                                                                         TraceQueryCondition condition,
                                                                         List<TraceNodeError> errors) {
        if (!hasFilterCondition(condition)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> filters = new ArrayList<>();
        for (Map<String, Object> sourceFilter : safeFilters(condition)) {
            Map<String, Object> filter = buildMergeJoinFilter(joinKeys, null, sourceFilter, errors);
            if (MapUtils.isNotEmpty(filter)) {
                filters.add(filter);
            }
        }
        return filters;
    }

    private Map<String, Object> buildMergeJoinFilter(List<Map<String, String>> joinKeys,
                                                     Map<String, Object> record,
                                                     Map<String, Object> sourceFilter,
                                                     List<TraceNodeError> errors) {
        Map<String, Object> filter = new LinkedHashMap<>();
        for (Map<String, String> joinKey : joinKeys) {
            String sourceField = firstNotBlank(joinKey.get("originName"));
            String targetField = firstNotBlank(joinKey.get("targetName"));
            if (StringUtils.isAnyBlank(sourceField, targetField)) {
                errors.add(traceError(ERROR_FIELD_MAPPING_NOT_FOUND,null, sourceField, targetField, null, null,
                        "joinKey=" + joinKey));
                continue;
            }
            Object value = record == null ? null : readRecordValue(record, targetField);
            if (value == null && sourceFilter != null) {
                value = sourceFilter.get(targetField);
            }
            if (value != null) {
                filter.put(sourceField, value);
            } else {
                errors.add(traceError(ERROR_VALUE_MISMATCH, targetField, sourceField, targetField, null, null,
                        "joinKey=" + joinKey));
            }
        }
        return filter;
    }

    private List<Map<String, Object>> buildNormalUpstreamFilters(TraceQueryCondition currentCondition,
                                                                 TraceValue currentTraceValue,
                                                                 Map<String, String> upstreamFieldMapping,
                                                                 Map<String, String> currentFieldMapping,
                                                                 TraceQueryCondition fallbackCondition,
                                                                 TraceValue fallbackTraceValue,
                                                                 Map<String, String> fallbackFieldMapping,
                                                                 List<BloodlineFinder.FieldNameMapping> updateConditionFieldList,
                                                                 List<TraceNodeError> errors) {
        List<Map<String, Object>> filters = rewriteFiltersByFieldMapping(currentCondition, currentTraceValue,
                upstreamFieldMapping, currentFieldMapping, updateConditionFieldList, errors);
        if (CollectionUtils.isEmpty(filters)) {
            filters = rewriteRecordValuesByFieldMapping(currentTraceValue, upstreamFieldMapping, currentFieldMapping, errors);
        }
        if (CollectionUtils.isEmpty(filters) && fallbackCondition != null && !fallbackFieldMapping.isEmpty()) {
            filters = rewriteFiltersByFieldMapping(fallbackCondition, fallbackTraceValue,
                    upstreamFieldMapping, fallbackFieldMapping, updateConditionFieldList, errors);
        }
        if (CollectionUtils.isEmpty(filters) && !fallbackFieldMapping.isEmpty()) {
            filters = rewriteRecordValuesByFieldMapping(fallbackTraceValue, upstreamFieldMapping, fallbackFieldMapping, errors);
        }
        return distinctFilters(filters);
    }

    private List<Map<String, Object>> rewriteFiltersByFieldMapping(TraceQueryCondition currentCondition, TraceValue currentTraceValue,
                                                                   Map<String, String> upstreamFieldMapping,
                                                                   Map<String, String> currentFieldMapping,
                                                                   List<BloodlineFinder.FieldNameMapping> updateConditionFieldList,
                                                                   List<TraceNodeError> errors) {
        if (currentCondition == null || upstreamFieldMapping.isEmpty() || currentFieldMapping.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, String> upstreamByOrigin = new HashMap<>();
        upstreamFieldMapping.forEach((fieldName, originName) -> upstreamByOrigin.putIfAbsent(originName, fieldName));

        List<Map<String, Object>> filters = new ArrayList<>();
        if(CollectionUtils.isEmpty(currentCondition.getFilters())){
            currentCondition.getConditionKeys().forEach(key -> {
                if (hasCurrentRecords(currentTraceValue)) {
                    filters.addAll(rewriteFilterWithRecords(null,currentCondition.getConditionKeys(), currentTraceValue, upstreamByOrigin, currentFieldMapping, updateConditionFieldList, errors));
                }
            });
        }else{
            for (Map<String, Object> sourceFilter : safeFilters(currentCondition)) {
                if (hasCurrentRecords(currentTraceValue)) {
                    filters.addAll(rewriteFilterWithRecords(sourceFilter, currentCondition.getConditionKeys(),currentTraceValue, upstreamByOrigin, currentFieldMapping, updateConditionFieldList, errors));
                } else {
                    Map<String, Object> filter = rewriteFilterByFieldMapping(sourceFilter, null,null, upstreamByOrigin, currentFieldMapping, updateConditionFieldList, errors);
                    if (MapUtils.isNotEmpty(filter)) {
                        filters.add(filter);
                    }
                }
            }
        }

        return distinctFilters(filters);
    }

    private List<Map<String, Object>> rewriteFilterWithRecords(Map<String, Object> sourceFilter,
                                                               Set<String> sourceKeys,
                                                               TraceValue currentTraceValue,
                                                               Map<String, String> upstreamByOrigin,
                                                               Map<String, String> currentFieldMapping,
                                                               List<BloodlineFinder.FieldNameMapping> updateConditionFieldList,
                                                               List<TraceNodeError> errors) {
        List<Map<String, Object>> filters = new ArrayList<>();
        for (Map<String, Object> record : currentTraceValue.getCurrentRecords()) {
            Map<String, Object> filter = rewriteFilterByFieldMapping(sourceFilter, sourceKeys,record, upstreamByOrigin, currentFieldMapping, updateConditionFieldList, errors);
            if (MapUtils.isNotEmpty(filter)) {
                filters.add(filter);
            }
        }
        return filters;
    }

    private Map<String, Object> rewriteFilterByFieldMapping(Map<String, Object> sourceFilter,
                                                            Set<String> sourceKeys,
                                                            Map<String, Object> record,
                                                            Map<String, String> upstreamByOrigin,
                                                            Map<String, String> currentFieldMapping,
                                                            List<BloodlineFinder.FieldNameMapping> updateConditionFieldList,
                                                            List<TraceNodeError> errors) {
        Map<String, Object> filter = new LinkedHashMap<>();
        if (CollectionUtils.isNotEmpty(updateConditionFieldList)) {
            updateConditionFieldList.forEach(info -> {
                String originName = info.getOriginName();
                String key = info.getTargetName();
                Object value = readRecordValue(record, key);
                if (value != null) {
                    filter.put(originName, value);
                }
            });
            return filter;
        }
        if (MapUtils.isEmpty(sourceFilter)) {
            sourceKeys.forEach(key -> {
                String originName = currentFieldMapping.get(key);
                Object value = readRecordValue(record, key);
                if (value != null) {
                    filter.put(originName, value);
                }
            });
        }else{
            sourceFilter.forEach((currentField, currentValue) -> {
                String originName = currentFieldMapping.get(currentField);
                Object value = record == null ? currentValue : readRecordValue(record, currentField);
                if (value != null) {
                    filter.put(originName, value);
                }
            });
        }

        return filter;
    }

    private List<Map<String, Object>> rewriteRecordValuesByFieldMapping(TraceValue currentTraceValue,
                                                                        Map<String, String> upstreamFieldMapping,
                                                                        Map<String, String> currentFieldMapping,
                                                                        List<TraceNodeError> errors) {
        if (!hasCurrentRecords(currentTraceValue) || upstreamFieldMapping.isEmpty() || currentFieldMapping.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, String> currentByOrigin = new HashMap<>();
        currentFieldMapping.forEach((fieldName, originName) -> currentByOrigin.putIfAbsent(originName, fieldName));

        List<Map<String, Object>> filters = new ArrayList<>();
        for (Map<String, Object> record : currentTraceValue.getCurrentRecords()) {
            Map<String, Object> filter = new LinkedHashMap<>();
            upstreamFieldMapping.forEach((upstreamField, originName) -> {
                if (StringUtils.isBlank(upstreamField)) {
                    errors.add(traceError(ERROR_FIELD_MAPPING_NOT_FOUND, null, upstreamField, null, null, null,
                            "originName=" + originName));
                    return;
                }
                String currentField = currentByOrigin.get(originName);
                if (StringUtils.isBlank(currentField)) {
                    errors.add(traceError(ERROR_FIELD_MAPPING_NOT_FOUND, null, upstreamField, null, null, null,
                            "originName=" + originName));
                    return;
                }
                Object value = readRecordValue(record, currentField);
                if (value != null) {
                    filter.put(upstreamField, value);
                }
            });
            if (MapUtils.isNotEmpty(filter)) {
                filters.add(filter);
            }
        }
        return distinctFilters(filters);
    }

    private boolean hasCurrentRecords(TraceValue traceValue) {
        return traceValue != null && CollectionUtils.isNotEmpty(traceValue.getCurrentRecords());
    }

    private boolean hasFilterCondition(TraceQueryCondition condition) {
        return condition != null && CollectionUtils.isNotEmpty(condition.getFilters());
    }

    private void addEmptyFilterError(TraceQueryCondition condition, List<TraceNodeError> errors, Node upstreamNode, Node currentNode) {
        if (condition == null || CollectionUtils.isNotEmpty(condition.getFilters()) || CollectionUtils.isNotEmpty(condition.getQueryOperators())
                || StringUtils.isNotBlank(condition.getSql())) {
            return;
        }
        errors.add(traceError(ERROR_FILTER_NOT_BUILT, null, null, null, null, null,
                "upstreamTable=" + resolveTableName(upstreamNode) + ", downstreamTable=" + resolveTableName(currentNode)));
    }

    private List<Map<String, Object>> queryCurrentRecords(TraceQueryCondition condition) {
        if (condition == null || traceDataQueryAdapter == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> records = traceDataQueryAdapter.query(condition);
        return records == null ? Collections.emptyList() : records;
    }

    private Object collectRecordValues(TraceValue traceValue, String fieldName) {
        if (traceValue == null || CollectionUtils.isEmpty(traceValue.getCurrentRecords()) || StringUtils.isBlank(fieldName)) {
            return null;
        }
        List<Object> values = traceValue.getCurrentRecords().stream()
                .map(record -> readRecordValue(record, fieldName))
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
        if (values.isEmpty()) {
            return null;
        }
        return values.size() == 1 ? values.get(0) : values;
    }

    private Object readRecordValue(Map<String, Object> record, String fieldName) {
        if (record == null || StringUtils.isBlank(fieldName)) {
            return null;
        }
        if (record.containsKey(fieldName)) {
            return record.get(fieldName);
        }
        Object current = record;
        for (String part : StringUtils.split(fieldName, '.')) {
            if (!(current instanceof Map)) {
                return null;
            }
            current = ((Map<?, ?>) current).get(part);
        }
        return current;
    }

    private List<TraceFieldMapping> buildTracedFields(List<String> trackedFields, Map<String, String> targetFieldMapping,
                                                      Map<String, String> currentFieldMapping,
                                                      List<TraceNodeError> errors) {
        if (CollectionUtils.isEmpty(trackedFields) || currentFieldMapping.isEmpty()) {
            return Collections.emptyList();
        }
        List<TraceFieldMapping> tracedFields = new ArrayList<>();
        for (String trackedField : trackedFields) {
            if (StringUtils.isBlank(trackedField)) {
                continue;
            }
            String originName = targetFieldMapping.getOrDefault(trackedField, trackedField);
            String currentName = findFieldByOrigin(currentFieldMapping, originName, trackedField);
            if (StringUtils.isBlank(currentName)) {
                continue;
            }
            TraceFieldMapping mapping = new TraceFieldMapping();
            mapping.setOriginName(trackedField);
            mapping.setCurrentName(currentName);
            tracedFields.add(mapping);
        }
        return tracedFields;
    }

    private List<TraceFieldMapping> buildResultFieldMapping(Map<String, String> currentFieldMapping,
                                                            Map<String, String> downstreamFieldMapping,
                                                            String path) {
        if (currentFieldMapping.isEmpty() || downstreamFieldMapping.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, List<String>> downstreamByOrigin = new HashMap<>();
        downstreamFieldMapping.forEach((fieldName, originName) ->
                downstreamByOrigin.computeIfAbsent(originName, key -> new ArrayList<>()).add(fieldName));

        List<TraceFieldMapping> result = new ArrayList<>();
        currentFieldMapping.forEach((currentName, originName) -> {
            String downstreamName = chooseDownstreamFieldName(downstreamByOrigin.get(currentName), path, currentName);
            if (StringUtils.isBlank(downstreamName)) {
                return;
            }
            TraceFieldMapping mapping = new TraceFieldMapping();
            mapping.setCurrentName(currentName);
            mapping.setDownStreamName(downstreamName);
            result.add(mapping);
        });
        return result;
    }

    private String chooseDownstreamFieldName(List<String> downstreamNames, String path, String currentName) {
        if (CollectionUtils.isEmpty(downstreamNames)) {
            return null;
        }
        List<String> candidates = downstreamNames.stream()
                .filter(StringUtils::isNotBlank)
                .distinct()
                .sorted(String::compareTo)
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            return null;
        }
        if (StringUtils.isNotBlank(path)) {
            String prefix = path + ".";
            for (String candidate : candidates) {
                if (StringUtils.equals(candidate, path) || StringUtils.startsWith(candidate, prefix)) {
                    return candidate;
                }
            }
        }
        for (String candidate : candidates) {
            if (!StringUtils.contains(candidate, ".")) {
                return candidate;
            }
        }
        for (String candidate : candidates) {
            if (StringUtils.equals(candidate, currentName)) {
                return candidate;
            }
        }
        return candidates.get(0);
    }

    private String resolveTablePath(Node node) {
        BloodlineFinder.TableProperties properties = resolveTableProperties(node, null);
        return properties == null ? null : properties.getPath();
    }

    private String findFieldByOrigin(Map<String, String> fieldMapping, String originName, String fallbackFieldName) {
        for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
            if (StringUtils.equals(entry.getValue(), originName) || StringUtils.equals(entry.getKey(), fallbackFieldName)) {
                return entry.getKey();
            }
        }
        return null;
    }

    private void writeNodeErrors(OutputStream outputStream, String requestId, Node node, List<TraceNodeError> errors) throws IOException {
        if (CollectionUtils.isEmpty(errors)) {
            return;
        }
        for (TraceNodeError error : errors) {
            writeEvent(outputStream, TraceStreamEvent.nodeError(
                    requestId,
                    node == null ? null : node.getId(),
                    resolveConnectionId(node),
                    resolveConnectionName(node),
                    resolveTableName(node),
                    error
            ));
        }
    }

    private TraceNodeError traceError(String code,String field,
                                      String upstreamField, String downstreamField,
                                      Object expectedValue, Object actualValue,
                                      String detail) {
        TraceNodeError error = new TraceNodeError();
        error.setCode(code);
        error.setMessage(MessageUtil.getMessage(code));
        error.setField(field);
        error.setUpstreamField(upstreamField);
        error.setDownstreamField(downstreamField);
        error.setExpectedValue(expectedValue);
        error.setActualValue(actualValue);
        if (StringUtils.isNotBlank(detail)) {
            error.getDetails().add(detail);
        }
        return error;
    }

    private boolean valueMatches(Object actualValue, Object expectedValue) {
        if (actualValue instanceof Collection) {
            return ((Collection<?>) actualValue).stream().anyMatch(value -> equivalentValue(value, expectedValue));
        }
        if (expectedValue instanceof Collection) {
            return ((Collection<?>) expectedValue).stream().anyMatch(value -> equivalentValue(actualValue, value));
        }
        return equivalentValue(actualValue, expectedValue);
    }

    private boolean equivalentValue(Object left, Object right) {
        return Objects.equals(left, right)
                || (left != null && right != null && StringUtils.equals(String.valueOf(left), String.valueOf(right)));
    }

    private Node findTargetNode(WideTableTraceRequest request, List<Node> nodes, List<Edge> edges) {
        for (Node node : nodes) {
            if (node == null) {
                continue;
            }
            boolean connectionMatched = StringUtils.isBlank(request.getConnectionId())
                    || StringUtils.equals(request.getConnectionId(), resolveConnectionId(node));
            boolean tableMatched = StringUtils.isBlank(request.getTable())
                    || StringUtils.equals(request.getTable(), resolveTableName(node))
                    || StringUtils.equals(request.getTable(), node.getName());
            if (connectionMatched && tableMatched) {
                return node;
            }
        }
        return findSinkNode(nodes, edges);
    }

    private Node findSinkNode(List<Node> nodes, List<Edge> edges) {
        Set<String> sourceNodeIds = CollectionUtils.emptyIfNull(edges).stream()
                .map(Edge::getSource)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
        for (Node node : nodes) {
            if (node != null && StringUtils.isNotBlank(node.getId()) && !sourceNodeIds.contains(node.getId())) {
                return node;
            }
        }
        return null;
    }

    private Map<String, List<Edge>> groupIncomingEdges(List<Edge> edges) {
        Map<String, List<Edge>> incomingEdges = new HashMap<>();
        for (Edge edge : CollectionUtils.emptyIfNull(edges)) {
            if (edge == null || StringUtils.isBlank(edge.getTarget())) {
                continue;
            }
            incomingEdges.computeIfAbsent(edge.getTarget(), key -> new ArrayList<>()).add(edge);
        }
        return incomingEdges;
    }

    private Map<String, Map<String, String>> safeFieldNameMapping(TaskLineageDto taskLineageDto) {
        if (taskLineageDto.getFieldNameMapping() == null) {
            return Collections.emptyMap();
        }
        return taskLineageDto.getFieldNameMapping();
    }

    private boolean isMergeNode(Node node) {
        BloodlineFinder.TableProperties properties = resolveTableProperties(node, null);
        return properties != null
                && StringUtils.equalsAnyIgnoreCase(properties.getNodeType(), "MERGE", "JOIN", "APPEND");
    }

    private boolean isMergeSubTable(Node node, Edge edge) {
        BloodlineFinder.TableProperties properties = resolveTableProperties(node, edge);
        return properties != null && StringUtils.equalsIgnoreCase(properties.getTableType(), "subTable");
    }

    private BloodlineFinder.TableProperties resolveTableProperties(Node node, Edge edge) {
        Map<String, Object> attrs = node == null ? null : node.getAttrs();
        if (attrs == null || attrs.isEmpty()) {
            return null;
        }
        String taskId = resolveTaskId(edge);
        if (StringUtils.isNotBlank(taskId)) {
            BloodlineFinder.TableProperties properties = toTableProperties(attrs.get(taskId));
            if (properties != null) {
                return properties;
            }
        }
        for (Object value : attrs.values()) {
            BloodlineFinder.TableProperties properties = toTableProperties(value);
            if (properties != null) {
                return properties;
            }
        }
        return null;
    }

    private BloodlineFinder.TableProperties toTableProperties(Object value) {
        if (value instanceof BloodlineFinder.TableProperties) {
            return (BloodlineFinder.TableProperties) value;
        }
        if (value instanceof Map && ((Map<?, ?>) value).containsKey("tableType")) {
            try {
                return objectMapper.convertValue(value, BloodlineFinder.TableProperties.class);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return null;
    }

    private List<Map<String, String>> extractJoinKeys(Node node, Edge edge) {
        BloodlineFinder.TableProperties properties = resolveTableProperties(node, edge);
        if (properties == null || CollectionUtils.isEmpty(properties.getJoinKeys())) {
            return Collections.emptyList();
        }
        List<Map<String, String>> joinKeys = new ArrayList<>();
        for (BloodlineFinder.FieldNameMapping mapping : properties.getJoinKeys()) {
            if (mapping == null) {
                continue;
            }
            Map<String, String> joinKey = new HashMap<>();
            if (StringUtils.isNotBlank(mapping.getOriginName())) {
                joinKey.put("originName", mapping.getOriginName());
            }
            if (StringUtils.isNotBlank(mapping.getTargetName())) {
                joinKey.put("targetName", mapping.getTargetName());
            }
            if (!joinKey.isEmpty()) {
                joinKeys.add(joinKey);
            }
        }
        return joinKeys;
    }

    private String resolveTaskId(Edge edge) {
        Map<String, Object> attrs = edge == null ? null : edge.getAttrs();
        if (attrs == null || attrs.isEmpty()) {
            return null;
        }
        Object taskId = firstNonNull(attrs.get("taskId"), attrs.get("task_id"), attrs.get("taskRecordId"), attrs.get("task_record_id"));
        return taskId == null ? null : String.valueOf(taskId);
    }

    private Object firstNonNull(Object... values) {
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String firstNotBlank(String... values) {
        for (String value : values) {
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private String resolveConnectionId(Node node) {
        if (node instanceof LineageTableNode) {
            return ((LineageTableNode) node).getConnectionId();
        }
        return null;
    }

    private String resolveConnectionName(Node node) {
        if (node instanceof LineageTableNode && ((LineageTableNode) node).getConnectionName() != null) {
            return ((LineageTableNode) node).getConnectionName();
        }
        return null;
    }

    private String resolveTableName(Node node) {
        if (node instanceof LineageTableNode) {
            return ((LineageTableNode) node).getTable();
        }
        return node.getName();
    }

    private void writeEvent(OutputStream outputStream, TraceStreamEvent event) throws IOException {
        outputStream.write(objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8));
        outputStream.write('\n');
        outputStream.flush();
    }

    private static class TraceConditionBuildResult {
        private final TraceQueryCondition condition;
        private final List<TraceNodeError> errors;

        private TraceConditionBuildResult(TraceQueryCondition condition, List<TraceNodeError> errors) {
            this.condition = condition;
            this.errors = errors == null ? Collections.emptyList() : errors;
        }

        private TraceQueryCondition getCondition() {
            return condition;
        }

        private List<TraceNodeError> getErrors() {
            return errors;
        }

        private boolean isTraceable() {
            return condition != null && (CollectionUtils.isNotEmpty(condition.getFilters())
                    || CollectionUtils.isNotEmpty(condition.getQueryOperators())
                    || StringUtils.isNotBlank(condition.getSql()));
        }
    }

    private static class TraceNodeStep {
        private final Node currentNode;
        private final Node downstreamNode;
        private final TraceQueryCondition condition;
        private final TraceValue downstreamTraceValue;
        private final TraceValue filterTraceValue;
        private final Node filterNode;
        private final TraceQueryCondition filterCondition;

        private TraceNodeStep(Node currentNode, Node downstreamNode, TraceQueryCondition condition,
                              TraceValue downstreamTraceValue, TraceValue filterTraceValue,
                              Node filterNode, TraceQueryCondition filterCondition) {
            this.currentNode = currentNode;
            this.downstreamNode = downstreamNode;
            this.condition = condition;
            this.downstreamTraceValue = downstreamTraceValue;
            this.filterTraceValue = filterTraceValue;
            this.filterNode = filterNode;
            this.filterCondition = filterCondition;
        }

        private Node getCurrentNode() {
            return currentNode;
        }

        private Node getDownstreamNode() {
            return downstreamNode;
        }

        private TraceQueryCondition getCondition() {
            return condition;
        }

        private TraceValue getDownstreamTraceValue() {
            return downstreamTraceValue;
        }

        private TraceValue getFilterTraceValue() {
            return filterTraceValue;
        }

        private Node getFilterNode() {
            return filterNode;
        }

        private TraceQueryCondition getFilterCondition() {
            return filterCondition;
        }
    }
}
