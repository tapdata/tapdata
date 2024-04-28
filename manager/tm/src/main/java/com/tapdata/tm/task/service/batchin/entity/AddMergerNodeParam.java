package com.tapdata.tm.task.service.batchin.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class AddMergerNodeParam {
    String rootNodeId;
    String mergeNodeId;
    Map<String, Object> mergeNode;
    Map<String, String> sourceToJs;
    Map<String, Object> contentMapping;
    Map<String, Map<String, Map<String, Object>>> renameFields;
    Map<String, List<Map<String, Object>>> contentDeleteOperations;
    Map<String, List<Map<String, Object>>> contentRenameOperations;
    public AddMergerNodeParam withRootNodeId(String rootNodeId) {
        this.rootNodeId = rootNodeId;
        return this;
    }
    public AddMergerNodeParam withMergeNodeId(String mergeNodeId) {
        this.mergeNodeId = mergeNodeId;
        return this;
    }
    public AddMergerNodeParam withRenameFields(Map<String, Map<String, Map<String, Object>>> renameFields) {
        this.renameFields = renameFields;
        return this;
    }
    public AddMergerNodeParam withSourceToJs(Map<String, String> sourceToJs) {
        this.sourceToJs = sourceToJs;
        return this;
    }
    public AddMergerNodeParam withContentMapping(Map<String, Object> contentMapping) {
        this.contentMapping = contentMapping;
        return this;
    }
    public AddMergerNodeParam withContentDeleteOperations(Map<String, List<Map<String, Object>>> contentDeleteOperations) {
        this.contentDeleteOperations = contentDeleteOperations;
        return this;
    }
    public AddMergerNodeParam withContentRenameOperations(Map<String, List<Map<String, Object>>> contentRenameOperations) {
        this.contentRenameOperations = contentRenameOperations;
        return this;
    }
    public AddMergerNodeParam withMergeNode(Map<String, Object> mergeNode) {
        this.mergeNode = mergeNode;
        return this;
    }
    public static AddMergerNodeParam of() {
        return new AddMergerNodeParam();
    }
}
