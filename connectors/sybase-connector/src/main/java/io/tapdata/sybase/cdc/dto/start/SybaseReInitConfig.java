package io.tapdata.sybase.cdc.dto.start;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * @description SybaseReinitConfig create by Gavin
 * @create 2023/8/7 15:45
 **/
public class SybaseReInitConfig extends SybaseFilterConfig {
    public static final String configKey = "re-init";
    private List<String> add_tables;

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

    public static List<LinkedHashMap<String, Object>> fixYaml0(List<SybaseReInitConfig> configs) {
        List<LinkedHashMap<String, Object>> list = new ArrayList<>();
        if (null == configs || configs.isEmpty()) return list;
        configs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(SybaseFilterConfig::getCatalog)).forEach((cl, r) -> {
            r.stream().collect(Collectors.groupingBy(SybaseFilterConfig::getSchema)).forEach((s, ri) -> {
                LinkedHashMap<String, Object> map = new LinkedHashMap<>();
                map.put("catalog", cl);
                map.put("schema", s);
                Set<String> tab = new HashSet<>();
                for (SybaseReInitConfig sybaseReInitConfig : ri) {
                    tab.addAll(sybaseReInitConfig.getAdd_tables());
                }
                map.put("add-tables", tab);
                list.add(map);
            });
        });
        return list;
    }
}
