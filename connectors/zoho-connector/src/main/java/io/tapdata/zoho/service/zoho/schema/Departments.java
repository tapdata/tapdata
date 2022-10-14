package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;

import java.util.List;
import java.util.Map;

public class Departments implements Schema {
    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignmentDocument(Map<String, Object> obj, TapConnectionContext connectionContext) {
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignmentCsv(Map<String, Object> obj, TapConnectionContext connectionContext, ContextConfig contextConfig) {
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig) {
        return null;
    }
}
