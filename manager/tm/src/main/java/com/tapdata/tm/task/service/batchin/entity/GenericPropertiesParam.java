package com.tapdata.tm.task.service.batchin.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class GenericPropertiesParam {
    Map<String, Object> parent;
    Map<String, Object> contentMapping;
    Map<String, Object> relationshipsMapping;
    Map<String, Object> full;
    Map<String, String> sourceToJS;
    Map<String, Map<String, Map<String, Object>>> renameFields;
    Map<String, List<Map<String, Object>>> contentDeleteOperations;
    Map<String, List<Map<String, Object>>> contentRenameOperations;
    public GenericPropertiesParam withParent(Map<String, Object> parent) {
        this.parent = parent;
        return this;
    }
    public GenericPropertiesParam withContentMapping(Map<String, Object> contentMapping) {
        this.contentMapping = contentMapping;
        return this;
    }
    public GenericPropertiesParam withRelationshipsMapping(Map<String, Object> relationshipsMapping) {
        this.relationshipsMapping = relationshipsMapping;
        return this;
    }
    public GenericPropertiesParam withFull(Map<String, Object> full) {
        this.full = full;
        return this;
    }
    public GenericPropertiesParam withSourceToJS(Map<String, String> sourceToJS) {
        this.sourceToJS = sourceToJS;
        return this;
    }
    public GenericPropertiesParam withContentDeleteOperations(Map<String, List<Map<String, Object>>> contentDeleteOperations) {
        this.contentDeleteOperations = contentDeleteOperations;
        return this;
    }
    public GenericPropertiesParam withContentRenameOperations(Map<String, List<Map<String, Object>>> contentRenameOperations) {
        this.contentRenameOperations = contentRenameOperations;
        return this;
    }
    public GenericPropertiesParam withRenameFields(Map<String, Map<String, Map<String, Object>>> renameFields) {
        this.renameFields = renameFields;
        return this;
    }
    public static GenericPropertiesParam of() {
        return new GenericPropertiesParam();
    }
}
