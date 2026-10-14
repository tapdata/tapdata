package com.tapdata.tm.fieldorigin.builder;

import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight task dag node used by {@link FieldOriginBuilder}, decoupled from the task DAG so the builder stays pure.
 */
@Data
public class OriginGraphNode {
    private String id;
    private String type;
    private boolean dataNode;
    private String connectionId;
    private List<String> predecessors = new ArrayList<>();
    private List<MergeTableProperties> mergeProperties;

    public static OriginGraphNode data(String id, String type, String connectionId) {
        OriginGraphNode node = new OriginGraphNode();
        node.setId(id);
        node.setType(type);
        node.setDataNode(true);
        node.setConnectionId(connectionId);
        return node;
    }

    public static OriginGraphNode processor(String id, String type) {
        OriginGraphNode node = new OriginGraphNode();
        node.setId(id);
        node.setType(type);
        return node;
    }

    public OriginGraphNode from(String... ids) {
        predecessors.addAll(List.of(ids));
        return this;
    }

    public boolean isMergeNode() {
        return FieldOriginBuilder.MERGE_NODE_TYPE.equals(type);
    }
}
