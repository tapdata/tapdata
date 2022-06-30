package io.tapdata.pdk.core.dag;

import java.util.List;
import java.util.Map;

public class TapDAG {
    private String id;
    private List<TapDAGNode> startNodes;

    private Map<String, TapDAGNode> nodeMap;

    public Map<String, TapDAGNode> getNodeMap() {
        return nodeMap;
    }

    public void setNodeMap(Map<String, TapDAGNode> nodeMap) {
        this.nodeMap = nodeMap;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<TapDAGNode> getStartNodes() {
        return startNodes;
    }

    public void setStartNodes(List<TapDAGNode> startNodes) {
        this.startNodes = startNodes;
    }
}
