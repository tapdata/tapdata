package com.tapdata.tm.commons.dag.check;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.task.dto.Message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author gavin
 * @date 2023/10/26 下午4:50
 * @description DAG 检查
 */
public class DAGCheckUtil {

    private DAGCheckUtil() {}
    /**
     * @author gavin
     * @date 2023/10/26 下午4:50
     * @description DAG 检查： 设置节点为禁用状态
     */
    public static void setNodeToDisabled(Node<?> node) {
        if (null == node) return;
        Map<String, Object> attrs = node.getAttrs();
        if (null == attrs) {
            attrs = new HashMap<>();
        }
        attrs.put("disabled", true);
        node.setDisabled(true);
        node.setAttrs(attrs);
    }

    /**
     * @author gavin
     * @date 2023/10/26 下午4:50
     * @description JOIN节点检查： JOIN节点不能只禁用其中一个输入，当禁用其一个输入时，自动禁用其整个JOIN链路
     */
    public static void checkJoinNode(JoinProcessorNode joinNode, List<Edge> edges, List<Message> messageList) {
        if (null == joinNode || joinNode.disabledNode()) return;
        String rightNodeId = joinNode.getRightNodeId();
        String leftNodeId = joinNode.getLeftNodeId();
        if (null == rightNodeId || leftNodeId == null) {
            DAGCheckUtil.setNodeToDisabled(joinNode);
        }
        String currentNodeId = joinNode.getId();
        String joinNodeName = joinNode.getName();
        if (null == edges) return;
        Map<String, Set<String>> sourceNodes = edges.stream().collect(Collectors.groupingBy(Edge::getTarget, Collectors.mapping(Edge::getSource, Collectors.toSet())));
        Set<String> currentNodeSourceNodeIds = sourceNodes.get(currentNodeId);
        final String joinNodeTag = "DAG.JonNode";
        if (null == currentNodeSourceNodeIds || currentNodeSourceNodeIds.size() != 2 ) {
            Message message = new Message();
            message.setCode(joinNodeTag);
            message.setMsg(String.format("An not disabled join node [%s] not contain tow source nodes both should be not disabled", joinNodeName));
            messageList.add(message);
            DAGCheckUtil.setNodeToDisabled(joinNode);
            return;
        }
        for (String currentNodeSourceNodeId : currentNodeSourceNodeIds) {
            Node<?> currentNodeSourceNode = joinNode.getDag().getNode(currentNodeSourceNodeId);
            if (null == currentNodeSourceNode) {
                Message message = new Message();
                message.setCode(joinNodeTag);
                message.setMsg(String.format("An not disabled join node [%s] contain an source node which is empty, source node id is %s", joinNodeName, currentNodeSourceNodeId));
                messageList.add(message);
                DAGCheckUtil.setNodeToDisabled(joinNode);
                continue;
            }
            if (currentNodeSourceNode.disabledNode()) {
                Message message = new Message();
                message.setCode(joinNodeTag);
                message.setMsg(String.format("An not disabled join node [%s] contain an source node which is disabled, source node name is %s", joinNodeName, currentNodeSourceNode.getName()));
                messageList.add(message);
                DAGCheckUtil.setNodeToDisabled(joinNode);
            }
        }
    }

    /**
     * The method needs to return the most target node in the entire DAG graph. 
     * The DAG graph structure and node list can be obtained through the method parameter node. 
     * Currently, it is known that each DAG can only have at most one target node
     * */
    public static Node<?> getTargetNode(Node<?> node) {
        if (null == node) {
            return null;
        }
        DAG dag = node.getDag();
        if (null == dag) {
            return null;
        }
        List<Node> targets = dag.getTargets();
        if (null == targets || targets.isEmpty()) {
            return null;
        }
        if (targets.size() == 1) {
            return (Node<?>) targets.get(0);
        }

        List<Node> dataTargets = targets.stream()
                .filter(Objects::nonNull)
                .filter(n -> Node.NodeCatalog.data == n.getCatalog())
                .collect(Collectors.toList());
        if (dataTargets.size() == 1) {
            return (Node<?>) dataTargets.get(0);
        }

        List<Node> nonVirtualTargets = targets.stream()
                .filter(Objects::nonNull)
                .filter(n -> Node.NodeCatalog.logCollector != n.getCatalog() && Node.NodeCatalog.virtualTarget != n.getCatalog())
                .collect(Collectors.toList());
        List<Node> candidates = nonVirtualTargets.isEmpty() ? targets : nonVirtualTargets;

        Map<String, Integer> depthMemo = new HashMap<>();
        Set<String> visiting = new HashSet<>();
        Node<?> best = null;
        int bestDepth = Integer.MIN_VALUE;
        String bestId = null;
        for (Node<?> candidate : candidates) {
            if (null == candidate || null == candidate.getId()) {
                continue;
            }
            int depth = depthFromSources(dag, candidate.getId(), depthMemo, visiting);
            String nodeId = candidate.getId();
            if (depth > bestDepth || (depth == bestDepth && (bestId == null || nodeId.compareTo(bestId) < 0))) {
                best = candidate;
                bestDepth = depth;
                bestId = nodeId;
            }
        }
        return best;
    }

    private static int depthFromSources(DAG dag, String nodeId, Map<String, Integer> memo, Set<String> visiting) {
        Integer cached = memo.get(nodeId);
        if (cached != null) {
            return cached;
        }
        if (!visiting.add(nodeId)) {
            return 0;
        }
        List<Node> predecessors = dag.predecessors(nodeId);
        int maxDepth = 0;
        if (predecessors != null && !predecessors.isEmpty()) {
            for (Node<?> predecessor : predecessors) {
                if (null == predecessor || null == predecessor.getId()) {
                    continue;
                }
                int d = depthFromSources(dag, predecessor.getId(), memo, visiting) + 1;
                if (d > maxDepth) {
                    maxDepth = d;
                }
            }
        }
        visiting.remove(nodeId);
        memo.put(nodeId, maxDepth);
        return maxDepth;
    }

    public static int preNodeCount(Node<?> node) {
        DAG dag = node.getDag();
        if (null == dag) {
            return 0;
        }
        String id = node.getId();
        LinkedList<Edge> edges = dag.getEdges();
        int count = 0;
        for (Edge edge : edges) {
            String target = edge.getTarget();
            if (target.equals(id)) {
                count++;
            }
        }
        return count;
    }
}
