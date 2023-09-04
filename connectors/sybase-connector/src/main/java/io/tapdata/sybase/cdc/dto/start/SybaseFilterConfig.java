package io.tapdata.sybase.cdc.dto.start;

import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;

import java.util.*;

/**
 * @author GavinXiao
 * @description SybaseFilterConfig create by Gavin
 * @create 2023/7/13 10:59
 **/
public class SybaseFilterConfig implements ConfigEntity {
    public static final String configKey = "allow";

    protected String catalog;
    protected String schema;
    private List<String> types;
    private Map<String, Object> allow;

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
    }

    public Map<String, Object> getAllow() {
        return allow;
    }

    public void setAllow(Map<String, Object> allow) {
        this.allow = allow;
    }

    @Override
    public Object toYaml() {
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("catalog", catalog);
        map.put("schema", schema);
        map.put("types", types);
        map.put("allow", allow);
        return map;
    }

    public Map<String, List<String>> blockFieldName(List<String> needBlockFieldName) {
        Map<String, List<String>> hashMap = new HashMap<>();
        hashMap.put("block", Optional.ofNullable(needBlockFieldName).orElse(new ArrayList<String>()));
        return hashMap;
    }

    public Map<String, List<String>> ignoreColumns(List<String> needBlockFieldName) {
        List<String> timestamp = Optional.ofNullable(needBlockFieldName).orElse(new ArrayList<String>());
        timestamp.add("timestamp");
        return blockFieldName(timestamp);
    }

    public boolean isBolField (String dataTypeName) {
        switch (TableTypeEntity.Type.type(dataTypeName)) {
            case TableTypeEntity.Type.BINARY:
            case TableTypeEntity.Type.TEXT:
            case TableTypeEntity.Type.IMAGE:
                return true;
            default:
                return false;
        }
    }
}
