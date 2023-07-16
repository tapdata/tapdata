package io.tapdata.sybase.cdc.dto.start;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author GavinXiao
 * @description SybaseFilterConfig create by Gavin
 * @create 2023/7/13 10:59
 **/
public class SybaseFilterConfig implements ConfigEntity {
    String catalog;
    String schema;
    List<String> types;
    Map<String, Object> allow;

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
        map.put("catalog" , catalog);
        map.put("schema", schema);
        map.put("types", types);
        map.put("allow", allow);
        return map;
    }
}
