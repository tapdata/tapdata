package com.tapdata.tm.task.service.batchin.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class AddJsNodeParam {
    String sourceConnectionId;
    Map<String, Map<String, Map<String, Object>>> renameFields;
    Map<String, String> sourceToJs;
    Map<String, Object> contentMapping;
    Map<String, List<Map<String, Object>>> contentDeleteOperations;
    Map<String, List<Map<String, Object>>> contentRenameOperations;
    Map<String, Object> task;
    List<Map<String, Object>> nodes;
    List<Map<String, Object>> edges;
    public AddJsNodeParam withSourceConnectionId(String sourceConnectionId) {
        this.sourceConnectionId = sourceConnectionId;
        return this;
    }
    public AddJsNodeParam withRenameFields(Map<String, Map<String, Map<String, Object>>> renameFields) {
        this.renameFields = renameFields;
        return this;
    }
    public AddJsNodeParam withSourceToJs(Map<String, String> sourceToJs) {
        this.sourceToJs = sourceToJs;
        return this;
    }
    public AddJsNodeParam withContentMapping(Map<String, Object> contentMapping) {
        this.contentMapping = contentMapping;
        return this;
    }
    public AddJsNodeParam withContentDeleteOperations(Map<String, List<Map<String, Object>>> contentDeleteOperations) {
        this.contentDeleteOperations = contentDeleteOperations;
        return this;
    }
    public AddJsNodeParam withContentRenameOperations(Map<String, List<Map<String, Object>>> contentRenameOperations) {
        this.contentRenameOperations = contentRenameOperations;
        return this;
    }
    public AddJsNodeParam withTask(Map<String, Object> task) {
        this.task = task;
        return this;
    }
    public AddJsNodeParam withNodes(List<Map<String, Object>> nodes) {
        this.nodes = nodes;
        return this;
    }
    public AddJsNodeParam withEdges(List<Map<String, Object>> edges) {
        this.edges = edges;
        return this;
    }
    public static AddJsNodeParam of() {
        return new AddJsNodeParam();
    }
}
