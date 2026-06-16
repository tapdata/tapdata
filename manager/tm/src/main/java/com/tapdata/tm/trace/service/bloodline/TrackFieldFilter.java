package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
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
 * @version v1.0 2026/5/28 12:31 Create
 * @description
 */
@Service
public class TrackFieldFilter {
    @Resource(name = "fieldOriginalNameMapping")
    FieldOriginalNameMapping fieldOriginalNameMapping;

    public Map<String, Map<String, String>> removeUselessFields(Dag dag, List<String> traceFilterFieldNames, Map<String, Map<String, String>> fieldNameMapping) {
        if (null == dag || null == dag.getNodes()) {
            return new HashMap<>();
        }
        Map<String, Map<String, String>> traceFilterFielMap = removeUnTraceFilterFieldName(dag, traceFilterFieldNames, fieldNameMapping);
        dag.getNodes().forEach(this::removeUselessFields);
        return traceFilterFielMap;
    }

    protected Map<String, Map<String, String>> removeUnTraceFilterFieldName(Dag dag, List<String> traceFilterFieldNames, Map<String, Map<String, String>> fieldNameMapping) {
        if (CollectionUtils.isEmpty(traceFilterFieldNames)) {
            return new HashMap<>();
        }
        //traceFilterFieldNames时整个血缘图中最终目标节点的字段列表
        //利用fieldNameMapping找出traceFilterFieldNames对应在各节点中的字段名映射列表，如果没有就是空列表,这个字段名映射列表就是本方法的返回值
        //找出dag中所有node节点中不包含traceFilterFieldNames任何一个字段的节点，把这些节点从dag的node节点中移除，并把相应在edges中的属性也移除
        if (null == dag || CollectionUtils.isEmpty(dag.getNodes())) {
            return new HashMap<>();
        }

        Map<String, Map<String, String>> fieldNameMappingByNodeId = MapUtils.isEmpty(fieldNameMapping) ? new HashMap<>() : fieldNameMapping;
        LineageTableNode finalTarget = fieldOriginalNameMapping.findFinalTargetLineageTableNode(dag);
        if (null == finalTarget || StringUtils.isBlank(finalTarget.getId())) {
            return new HashMap<>();
        }
        String targetNodeId = finalTarget.getId();

        Set<String> finalTargetFields = traceFilterFieldNames.stream().filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        if (finalTargetFields.isEmpty()) {
            return new HashMap<>();
        }

        Set<String> keptNodeIds = new HashSet<>();
        Map<String, String> nodeFieldToOrigin = fieldNameMappingByNodeId.getOrDefault(targetNodeId, new HashMap<>());
        Set<String> fields = nodeFieldToOrigin.keySet();
        finalTargetFields = merge(finalTargetFields, fields);
        if (CollectionUtils.isEmpty(finalTargetFields)) {
            dag.setNodes(new ArrayList<>());
            dag.setEdges(new ArrayList<>());
            return new HashMap<>();
        }
        keptNodeIds.add(targetNodeId);
        Map<String, Map<String, String>> result = new HashMap<>();

        Map<String, Map<String, String>> targetFieldToOriginNameMap = new HashMap<>();
        Map<String, Set<String>> targetFieldsMap = new HashMap<>();
        targetFieldsMap.put(targetNodeId, finalTargetFields);

        Map<String, Node<?>> nodeMap = new HashMap<>();
        dag.getNodes().forEach(node -> nodeMap.put(node.getId(), node));
        int maxLoopCount = 64;
        while (!nodeMap.isEmpty() && maxLoopCount > 0) {
            maxLoopCount--;
            for (Node<?> node : dag.getNodes()) {
                String id = node.getId();
                Set<String> upStreamIds = upstreamNodeIds(id, dag.getEdges());
                Map<String, String> targetNodeFieldToOrigin = fieldNameMappingByNodeId.getOrDefault(id, new HashMap<>());
                Map<String, String> targetFieldToOriginName = new HashMap<>();
                Set<String> targetFields = targetFieldsMap.get(id);
                if (null == targetFields) {
                    continue;
                }
                for (String targetField : targetFields) {
                    String originName = targetNodeFieldToOrigin.get(targetField);
                    if (StringUtils.isBlank(originName)) {
                        continue;
                    }
                    targetFieldToOriginName.put(targetField, originName);
                }
                targetFieldToOriginNameMap.put(id, targetFieldToOriginName);
                Set<String> originTargetFields = new HashSet<>(targetFieldToOriginName.values());
                upStreamIds.forEach(upStreamId -> targetFieldsMap.put(upStreamId, originTargetFields));
                nodeMap.remove(id);
            }
        }
        for (Node<?> node : dag.getNodes()) {
            String id = node.getId();
            Map<String, String> targetFieldToOriginName = targetFieldToOriginNameMap.getOrDefault(id, new HashMap<>());
            Set<String> targetFields = targetFieldsMap.getOrDefault(id, new HashSet<>());
            eachAllNodes(node, targetNodeId, targetFields, keptNodeIds, fieldNameMappingByNodeId, result, targetFieldToOriginName);
        }

        List<Node> newNodes = dag.getNodes().stream()
                .filter(Objects::nonNull)
                .filter(n -> StringUtils.isNotBlank(n.getId()))
                .filter(n -> keptNodeIds.contains(n.getId()))
                .toList();
        dag.setNodes(newNodes);

        if (CollectionUtils.isNotEmpty(dag.getEdges())) {
            List<Edge> newEdges = dag.getEdges().stream()
                    .filter(Objects::nonNull)
                    .filter(e -> StringUtils.isNotBlank(e.getSource()) && StringUtils.isNotBlank(e.getTarget()))
                    .filter(e -> keptNodeIds.contains(e.getSource()) && keptNodeIds.contains(e.getTarget()))
                    .toList();
            dag.setEdges(newEdges);
        }

        return result;
    }

    Set<String> merge(Set<String> set1, Set<String> set2) {
        if (set1 == null || set2 == null) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>(set1);
        result.retainAll(set2);
        return result;
    }

    Set<String> upstreamNodeIds(String nodeId, List<Edge> edges) {
        Set<String> upstreamNodeIds = new HashSet<>();
        edges.forEach(edge -> {
            if (Objects.equals(edge.getTarget(), nodeId)) {
                upstreamNodeIds.add(edge.getSource());
            }
        });
        return upstreamNodeIds;
    }

    void eachAllNodes(Node<?> n,
                      String targetNodeId,
                      Set<String> targetFields,
                      Set<String> keptNodeIds,
                      Map<String, Map<String, String>> fieldNameMappingByNodeId,
                      Map<String, Map<String, String>> result,
                      Map<String, String> targetFieldToOriginName) {
        if (!(n instanceof LineageTableNode) || StringUtils.isBlank(n.getId())) {
            return;
        }
        String nodeId = n.getId();

        if (targetNodeId.equals(nodeId)) {
            Map<String, String> mapping = new HashMap<>();
            for (String targetField : targetFields) {
                mapping.put(targetField, targetField);
            }
            result.put(nodeId, mapping);
            return;
        }

        Map<String, String> nodeFieldToOrigin = fieldNameMappingByNodeId.get(nodeId);
        if (MapUtils.isEmpty(nodeFieldToOrigin) || MapUtils.isEmpty(targetFieldToOriginName)) {
            return;
        }

        Map<String, String> mapping = new HashMap<>();
        for (Map.Entry<String, String> entry : targetFieldToOriginName.entrySet()) {
            eachTargetFieldToOriginName(entry, nodeFieldToOrigin, mapping);
        }

        if (MapUtils.isNotEmpty(mapping)) {
            keptNodeIds.add(nodeId);
            result.put(nodeId, mapping);
        }
    }

    void eachTargetFieldToOriginName(Map.Entry<String, String> entry, Map<String, String> nodeFieldToOrigin, Map<String, String> mapping) {
        String targetField = entry.getKey();
        String originName = entry.getValue();
        if (StringUtils.isBlank(originName)) {
            return;
        }
        String best = eachNodeFieldToOriginToFindBestName(originName, nodeFieldToOrigin, targetField);
        if (StringUtils.isNotBlank(best)) {
            mapping.put(targetField, best);
        }
    }

    String eachNodeFieldToOriginToFindBestName(String originName, Map<String, String> nodeFieldToOrigin, String targetField) {
        String best = null;
        for (Map.Entry<String, String> e : nodeFieldToOrigin.entrySet()) {
            if (StringUtils.isBlank(e.getKey())
                    || StringUtils.isBlank(e.getValue())
                    || !originName.equals(e.getValue())) {
                continue;
            }
            if (targetField.equals(e.getKey())) {
                return e.getKey();
            }
            if (null == best || e.getKey().compareTo(best) < 0) {
                best = e.getKey();
            }
        }
        return best;
    }

    protected void removeUselessFields(Node<?> node) {
        if (node instanceof LineageTableNode lineageTableNode) {
            LineageMetadataInstance metadata = lineageTableNode.getMetadata();
            if (null == metadata) {
                return;
            }
            metadata.setFields(null);
        }
    }
}
