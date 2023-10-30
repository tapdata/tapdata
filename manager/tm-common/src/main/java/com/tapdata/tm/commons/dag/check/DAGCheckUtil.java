package com.tapdata.tm.commons.dag.check;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.task.dto.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author gavin
 * @date 2023/10/26 下午4:50
 * @description DAG 检查
 */
public class DAGCheckUtil {

    /**
     * @author gavin
     * @date 2023/10/26 下午4:50
     * @description DAG 检查： 保存检查结果到列表
     */
    public static void messageToList(List<Message> messageList, DAGNodeCheckItem code, String message) {
        if (null == messageList) return;
        if (null == code) return;
        Message msg = new Message();
        msg.setCode(code.getCode());
        msg.setMsg(message);
        messageList.add(msg);
    }

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
        if (null == currentNodeSourceNodeIds || currentNodeSourceNodeIds.size() != 2 ) {
            DAGCheckUtil.messageToList(messageList, DAGNodeCheckItem.CHECK_JOIN_NODE, String.format("An not disabled join node [%s] not contain tow source nodes both should be not disabled", joinNodeName));
            DAGCheckUtil.setNodeToDisabled(joinNode);
            return;
        }
        for (String currentNodeSourceNodeId : currentNodeSourceNodeIds) {
            Node<?> currentNodeSourceNode = joinNode.getDag().getNode(currentNodeSourceNodeId);
            if (null == currentNodeSourceNode) {
                DAGCheckUtil.messageToList(messageList, DAGNodeCheckItem.CHECK_JOIN_NODE, String.format("An not disabled join node [%s] contain an source node which is empty, source node id is %s", joinNodeName, currentNodeSourceNodeId));
                DAGCheckUtil.setNodeToDisabled(joinNode);
                continue;
            }
            if (currentNodeSourceNode.disabledNode()) {
                DAGCheckUtil.messageToList(messageList, DAGNodeCheckItem.CHECK_JOIN_NODE, String.format("An not disabled join node [%s] contain an source node which is disabled, source node name is %s", joinNodeName, currentNodeSourceNode.getName()));
                DAGCheckUtil.setNodeToDisabled(joinNode);
            }
        }
    }
}
