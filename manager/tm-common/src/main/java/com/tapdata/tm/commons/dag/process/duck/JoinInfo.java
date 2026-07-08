package com.tapdata.tm.commons.dag.process.duck;

import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Data
public class JoinInfo {
    public static final String TABLE_PROPS = "TABLE_PROPS";
    public static final String PK_INFO = "PK_INFO";
    public static final String JOIN_KEY_INFO = "JOIN_KEY_INFO";
    public static final String TABLE_ALAIN_NAME = "TABLE_ALAIN_NAME";
    public static final String PK = "PK";
    public static final String LEFT = "left";
    public static final String RIGHT = "right";



    private String table;

    private String joinType;

    /**
     * 每组Join条件
     * <p>
     * o.id = d.id
     * o.type = d.type
     */
    private List<JoinKeyPair> joinKeys = new ArrayList<>();

    public static Map<String, List<Map<String, Object>>> toMap(List<JoinInfo> joinKeyInfo) {
        Map<String, List<Map<String, Object>>> joinKeys = new HashMap<>();
        joinKeyInfo.forEach(info -> joinKeys.put(info.getTable(), info.toMap()));
        return joinKeys;
    }

    public List<Map<String, Object>> toMap() {
        List<Map<String, Object>> list = new ArrayList<>();
        getJoinKeys().forEach(joinKey -> {
            Map<String, Object> infoMap = new HashMap<>();
            Optional.ofNullable(joinKey.getLeft()).map(JoinField::toMap).ifPresent(map -> infoMap.put(LEFT, map));
            Optional.ofNullable(joinKey.getRight()).map(JoinField::toMap).ifPresent(map -> infoMap.put(RIGHT, map));
            list.add(infoMap);
        });
        return list;
    }

    public static List<String> getJoinKeys(Map<String, Object> tableAttr, String table) {
        Object props = tableAttr.get(JoinInfo.TABLE_PROPS);
        if (!(props instanceof Map<?, ?> tableProps)) {
            return new ArrayList<>();
        }
        Object info = tableProps.get(JoinInfo.JOIN_KEY_INFO);
        if (!(info instanceof Map<?, ?> joinKeysMap)) {
            return new ArrayList<>();
        }
        Set<String> joinKeys = new HashSet<>();
        joinKeysMap.values().forEach(joinKey -> {
            if (!(joinKey instanceof Collection<?> collection)) {
                return;
            }
            collection.forEach(joinValue -> {
                if (joinValue instanceof Map<?, ?> map) {
                    Optional.ofNullable(JoinField.fieldsOf(map.get(LEFT), table)).ifPresent(joinKeys::add);
                    Optional.ofNullable(JoinField.fieldsOf(map.get(RIGHT), table)).ifPresent(joinKeys::add);
                }
            });
        });
        return new ArrayList<>(joinKeys);
    }
}