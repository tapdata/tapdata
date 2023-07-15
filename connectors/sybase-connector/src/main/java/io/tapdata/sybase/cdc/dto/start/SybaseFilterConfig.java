package io.tapdata.sybase.cdc.dto.start;

import java.util.List;
import java.util.Map;

/**
 * @author GavinXiao
 * @description SybaseFilterConfig create by Gavin
 * @create 2023/7/13 10:59
 **/
public class SybaseFilterConfig {
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
}
