package io.tapdata.sybase.cdc.dto.start;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author GavinXiao
 * @description SybaseReinitConfig create by Gavin
 * @create 2023/8/7 15:45
 **/
public class SybaseReInitConfig implements ConfigEntity {
    public static final String configKey = "re-init";
    private List<String> add_tables;
    protected String catalog;
    protected String schema;

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

    public List<String> getAdd_tables() {
        return add_tables;
    }

    public void setAdd_tables(List<String> add_tables) {
        this.add_tables = add_tables;
    }

    @Override
    public Object toYaml() {
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("catalog", catalog);
        map.put("schema", schema);
        map.put("add-tables", add_tables);
        return map;
    }
}
