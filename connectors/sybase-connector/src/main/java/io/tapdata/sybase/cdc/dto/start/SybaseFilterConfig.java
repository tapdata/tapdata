package io.tapdata.sybase.cdc.dto.start;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

    public static List<SybaseFilterConfig> fromYaml(List<Map<String, Object>> mapList){
        List<SybaseFilterConfig> list = new ArrayList<>();
        if (null != mapList && !mapList.isEmpty()) {
            for (Map<String, Object> map : mapList) {
                SybaseFilterConfig config = new SybaseFilterConfig();
                config.setCatalog(((String) map.get("catalog")));
                config.setSchema(((String) map.get("schema")));
                config.setTypes(((List<String>) map.get("types")));
                config.setAllow((Map<String, Object>)map.get("allow"));
            }
        }
        return list;
    }

    public static final Map<String, List<String>> ignoreColumns = new HashMap<String, List<String>>(){{
        put("block", new ArrayList<String>(){{add("timestamp");}});
    }};
    public static final Map<String, List<String>> unIgnoreColumns = new HashMap<String, List<String>>(){{
        put("block", new ArrayList<String>());
    }};

    public static Map<String, List<String>> unIgnoreColumns(){
        return new HashMap<String, List<String>>(){{
            put("block", new ArrayList<String>());
        }};
    }

    public static Map<String, List<String>> ignoreColumns(){
        return new HashMap<String, List<String>>(){{
            put("block", new ArrayList<String>(){{add("timestamp");}});
        }};
    }

    public static List<Map<String, Object>> fixYaml(List<SybaseFilterConfig> configs) {
        List<Map<String, Object>> list = new ArrayList<>();
        if (null == configs || configs.isEmpty()) return list;
        configs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(SybaseFilterConfig::getCatalog)).forEach((cl, r) -> {
            r.stream().collect(Collectors.groupingBy(SybaseFilterConfig::getSchema)).forEach((s, ri) -> {
                LinkedHashMap<String, Object> map = new LinkedHashMap<>();
                map.put("catalog", cl);
                map.put("schema", s);
                if (ri == null ) ri = new ArrayList<>();
                map.put("types", ri.isEmpty() ? null : ri.get(0).getTypes());
                Map<String, Object> tab = new HashMap<>();
                for (SybaseFilterConfig config : ri) {
                    tab.putAll(config.getAllow());
                }
                map.put("allow", tab);
                list.add(map);
            });
        });
        return list;
    }
}
