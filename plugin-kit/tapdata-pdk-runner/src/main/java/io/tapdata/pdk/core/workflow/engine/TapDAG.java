package io.tapdata.pdk.core.workflow.engine;

import io.tapdata.pdk.core.dag.TapDAGNode;

import java.util.List;
import java.util.Map;

public class TapDAG {
    private String id;
    private List<String> headNodeIds;

    private Map<String, TapDAGNodeEx> nodeMap;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getHeadNodeIds() {
        return headNodeIds;
    }

    public void setHeadNodeIds(List<String> headNodeIds) {
        this.headNodeIds = headNodeIds;
    }

    public Map<String, TapDAGNodeEx> getNodeMap() {
        return nodeMap;
    }

    public void setNodeMap(Map<String, TapDAGNodeEx> nodeMap) {
        this.nodeMap = nodeMap;
    }

    public String dagString() {
        StringBuilder builder = new StringBuilder("\n");
        for(String nodeId : headNodeIds) {
            TapDAGNode node = nodeMap.get(nodeId);
            builder.append("$-> ").append(node.toString());
            describeDag(node, builder, 1);
        }
        return builder.toString();
    }

    private boolean describeDag(TapDAGNode node, StringBuilder builder, int deep) {
        if(node == null)
            return false;
        List<String> nodeIds = node.getChildNodeIds();
        if(nodeIds != null && !nodeIds.isEmpty()) {
            builder.append("\n");
            for(int i = 0; i < deep; i++) {
                builder.append("\t");
            }
            for(String nodeId : nodeIds) {
                TapDAGNode childNode = nodeMap.get(nodeId);
                builder.append(" -> ").append(childNode.toString());
                describeDag(childNode, builder, deep + 1);
                for(int i = 0; i < deep; i++) {
                    builder.append("\t");
                }
            }
            builder.append("\n");
            return true;
        }
        return false;
    }
}
