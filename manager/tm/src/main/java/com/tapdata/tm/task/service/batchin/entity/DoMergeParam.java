package com.tapdata.tm.task.service.batchin.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DoMergeParam {
    String mergeNodeId;
    List<Map<String, Object>> nodes;
    List<Map<String, Object>> edges;
    Map<String, Object> contentMapping;
    Map<String, Object> mergeNode;
    List<String> sourceNodes;
    Map<String, Map<String, Map<String, Object>>> renameFields;
    Map<String, List<Map<String, Object>>> contentRenameOperations;
    Map<String, List<Map<String, Object>>> contentDeleteOperations;
    public DoMergeParam withMergeNodeId(String mergeNodeId) {
        this.mergeNodeId = mergeNodeId;
        return this;
    }
    public DoMergeParam withNodes(List<Map<String, Object>> nodes) {
        this.nodes = nodes;
        return this;
    }
    public DoMergeParam withEdges(List<Map<String, Object>> edges) {
        this.edges = edges;
        return this;
    }
    public DoMergeParam withContentMapping(Map<String, Object> contentMapping) {
        this.contentMapping = contentMapping;
        return this;
    }
    public DoMergeParam withMergeNode(Map<String, Object> mergeNode) {
        this.mergeNode = mergeNode;
        return this;
    }
    public DoMergeParam withSourceNodes(List<String> sourceNodes) {
        this.sourceNodes = sourceNodes;
        return this;
    }
    public DoMergeParam withRenameFields(Map<String, Map<String, Map<String, Object>>> renameFields) {
        this.renameFields = renameFields;
        return this;
    }
    public DoMergeParam withContentRenameOperations(Map<String, List<Map<String, Object>>> contentRenameOperations) {
        this.contentRenameOperations = contentRenameOperations;
        return this;
    }
    public DoMergeParam withContentDeleteOperations(Map<String, List<Map<String, Object>>> contentDeleteOperations) {
        this.contentDeleteOperations = contentDeleteOperations;
        return this;
    }
    public static DoMergeParam of() {
        return new DoMergeParam();
    }
}
